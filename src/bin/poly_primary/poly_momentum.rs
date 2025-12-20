//! Polymarket Momentum Front-Runner
//!
//! STRATEGY:
//! - Monitor real-time crypto prices via Polygon.io websocket
//! - Detect rapid price movements (spikes/drops)
//! - Front-run the market by buying YES (price up) or NO (price down)
//! - Execute before market makers adjust their quotes
//!
//! The edge comes from:
//! 1. Faster price feed than most participants
//! 2. Quick execution via FAK orders
//! 3. Market makers slow to adjust quotes after sudden moves
//!
//! Usage:
//!   RUST_LOG=info cargo run --release --bin poly_momentum
//!
//! Environment:
//!   POLY_PRIVATE_KEY - Your Polymarket/Polygon wallet private key
//!   POLY_FUNDER - Your funder address (proxy wallet)
//!   PRICEFEED_API_KEY - Polygon.io API key for price feed

use anyhow::{Context, Result};
use futures_util::{SinkExt, StreamExt};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::RwLock;
use tokio_tungstenite::{connect_async, tungstenite::Message};
use tracing::{error, info, warn};

use arb_bot::polymarket_clob::{PolymarketAsyncClient, SharedAsyncClient, PreparedCreds};
use arb_bot::polymarket_markets::{discover_markets, PolyMarket};

/// CLI Arguments
#[derive(clap::Parser, Debug)]
#[command(author, version, about = "Polymarket Momentum Front-Runner")]
struct Args {
    /// Number of contracts per trade
    #[arg(short = 'c', long, default_value_t = 1.0)]
    contracts: f64,

    /// Price move threshold in basis points per tick (default: 5 = 0.05%)
    #[arg(short, long, default_value_t = 3)]
    threshold_bps: i64,

    /// Minimum edge in cents vs market price (default: 3)
    #[arg(short, long, default_value_t = 3)]
    edge: i64,

    /// Live trading mode (default is dry run)
    #[arg(short, long, default_value_t = false)]
    live: bool,

    /// Specific symbol to trade (BTC, ETH, SOL, XRP) - trades all if not set
    #[arg(long)]
    sym: Option<String>,

    /// Connect directly to Polygon.io instead of local price server
    #[arg(short, long, default_value_t = false)]
    direct: bool,

    /// Max total contracts to hold (default: 10)
    #[arg(short, long, default_value_t = 10.0)]
    max_contracts: f64,

    /// Take profit threshold in cents (sell when bid >= entry + this)
    #[arg(long, default_value_t = 5)]
    take_profit: i64,
}

const POLYMARKET_WS_URL: &str = "wss://ws-subscriptions-clob.polymarket.com/ws/market";
const PRICEFEED_WS_URL: &str = "wss://socket.polygon.io/crypto";
const LOCAL_PRICE_SERVER: &str = "ws://127.0.0.1:9999";

/// Price tracking for momentum detection
#[derive(Debug, Default)]
struct PriceTracker {
    last_price: Option<f64>,
    last_signal_time: Option<Instant>,
    last_signal_direction: Option<Direction>,
}

impl PriceTracker {
    /// Update price and return change in basis points from previous tick
    fn update(&mut self, price: f64) -> Option<i64> {
        let change_bps = self.last_price.map(|old| {
            ((price - old) / old * 10000.0).round() as i64
        });
        self.last_price = Some(price);
        change_bps
    }
}

/// Market state with orderbook and trading data
#[derive(Debug, Clone)]
struct Market {
    // Core market data from PolyMarket
    condition_id: String,
    question: String,
    yes_token: String,
    no_token: String,
    asset: String,
    expiry_minutes: Option<f64>,
    discovered_at: Instant,
    window_start_ts: Option<i64>,
    // Strike price (opening price at window start) - captured from price feed
    strike_price: Option<f64>,
    // Orderbook state
    yes_ask: Option<i64>,
    yes_bid: Option<i64>,
    no_ask: Option<i64>,
    no_bid: Option<i64>,
    // Trading state
    last_trade_time: Option<Instant>,
}

impl Market {
    fn from_polymarket(pm: PolyMarket) -> Self {
        Self {
            condition_id: pm.condition_id,
            question: pm.question,
            yes_token: pm.yes_token,
            no_token: pm.no_token,
            asset: pm.asset,
            expiry_minutes: pm.expiry_minutes,
            discovered_at: Instant::now(),
            window_start_ts: pm.window_start_ts,
            strike_price: None, // Will be set from price feed
            yes_ask: None,
            yes_bid: None,
            no_ask: None,
            no_bid: None,
            last_trade_time: None,
        }
    }

    fn time_remaining_mins(&self) -> Option<f64> {
        self.expiry_minutes.map(|exp| {
            let elapsed_mins = self.discovered_at.elapsed().as_secs_f64() / 60.0;
            (exp - elapsed_mins).max(0.0)
        })
    }

    /// Calculate fair value of YES based on current price vs strike
    /// Returns cents (0-100)
    fn calc_fair_yes(&self, current_price: f64) -> Option<i64> {
        let strike = self.strike_price?;
        let time_left_mins = self.time_remaining_mins()?;

        // Distance from strike in basis points
        let distance_bps = ((current_price - strike) / strike * 10000.0) as i64;

        // Simplified model:
        // - At expiry, if above strike -> 100%, below -> 0%
        // - With time remaining, probability depends on volatility
        // - Assume ~20bps/min volatility for BTC (rough estimate)
        let volatility_bps_per_sqrt_min = 20.0;
        let time_factor = (time_left_mins.max(0.1)).sqrt();
        let std_devs = distance_bps as f64 / (volatility_bps_per_sqrt_min * time_factor);

        // Convert to probability using simple approximation
        // P(above strike) ≈ 0.5 + 0.4 * tanh(std_devs)
        let prob = 0.5 + 0.4 * std_devs.tanh();
        let fair_cents = (prob * 100.0).round() as i64;

        Some(fair_cents.clamp(1, 99))
    }
}

/// Global state
struct State {
    markets: HashMap<String, Market>,
    price_trackers: HashMap<String, PriceTracker>, // asset -> tracker
    pending_signals: Vec<MomentumSignal>,
    // Track open positions for quick exit
    open_positions: Vec<OpenPosition>,
}

#[derive(Debug, Clone)]
struct OpenPosition {
    asset: String,
    token_id: String,
    side: &'static str, // "YES" or "NO"
    entry_price: i64,   // cents
    size: f64,
    opened_at: Instant,
}

#[derive(Debug, Clone)]
struct MomentumSignal {
    asset: String,
    direction: Direction,
    move_bps: i64,
    triggered_at: Instant,
}

#[derive(Debug, Clone, Copy, PartialEq, Default)]
enum Direction {
    #[default]
    Up,
    Down,
}

impl State {
    fn new() -> Self {
        let mut price_trackers = HashMap::new();
        for asset in ["BTC", "ETH", "SOL", "XRP"] {
            price_trackers.insert(asset.to_string(), PriceTracker::default());
        }

        Self {
            markets: HashMap::new(),
            price_trackers,
            pending_signals: Vec::new(),
            open_positions: Vec::new(),
        }
    }

    fn total_position_size(&self) -> f64 {
        self.open_positions.iter().map(|p| p.size).sum()
    }

    fn add_position(&mut self, asset: String, token_id: String, side: &'static str, entry_price: i64, size: f64) {
        self.open_positions.push(OpenPosition {
            asset,
            token_id,
            side,
            entry_price,
            size,
            opened_at: Instant::now(),
        });
    }

    fn remove_position(&mut self, token_id: &str) {
        self.open_positions.retain(|p| p.token_id != token_id);
    }
}


// === Price Feed Messages ===

#[derive(Deserialize, Debug)]
struct PriceFeedMessage {
    btc_price: Option<f64>,
    eth_price: Option<f64>,
    sol_price: Option<f64>,
    xrp_price: Option<f64>,
    timestamp: Option<u64>,
}

// Polygon.io message types
#[derive(Serialize, Debug)]
struct PolygonAuth {
    action: &'static str,
    params: String,
}

#[derive(Serialize, Debug)]
struct PolygonSubscribe {
    action: &'static str,
    params: &'static str,
}

#[derive(Deserialize, Debug)]
struct PolygonStatus {
    status: Option<String>,
    message: Option<String>,
}

#[derive(Deserialize, Debug)]
struct PolygonTrade {
    ev: Option<String>,
    pair: Option<String>,
    p: Option<f64>,
}

/// Run local price server feed and detect momentum signals
async fn run_price_feed(
    state: Arc<RwLock<State>>,
    threshold_bps: i64,
    trade_symbol: Option<String>,
) {
    loop {
        tracing::debug!("[poly_momentum] Connecting to local price server...");

        let ws = match connect_async(LOCAL_PRICE_SERVER).await {
            Ok((ws, _)) => ws,
            Err(e) => {
                error!("[poly_momentum] Connect failed: {} - is price server running?", e);
                tokio::time::sleep(Duration::from_secs(5)).await;
                continue;
            }
        };

        let (mut write, mut read) = ws.split();
        tracing::debug!("[poly_momentum] Connected to local price server");

        let mut ping_interval = tokio::time::interval(Duration::from_secs(30));

        loop {
            tokio::select! {
                _ = ping_interval.tick() => {
                    if let Err(e) = write.send(Message::Ping(vec![])).await {
                        error!("[poly_momentum] Ping failed: {}", e);
                        break;
                    }
                }

                msg = read.next() => {
                    let Some(msg) = msg else { break; };
                    match msg {
                        Ok(Message::Text(text)) => {
                            // Parse local price server format
                            tracing::debug!("[ws] Received: {}", &text[..text.len().min(200)]);
                            match serde_json::from_str::<PriceFeedMessage>(&text) {
                                Ok(msg) => {
                                // Process each available price
                                let prices: Vec<(&str, Option<f64>)> = vec![
                                    ("BTC", msg.btc_price),
                                    ("ETH", msg.eth_price),
                                    ("SOL", msg.sol_price),
                                    ("XRP", msg.xrp_price),
                                ];

                                let mut s = state.write().await;

                                // Current time for strike capture
                                let now_ts = std::time::SystemTime::now()
                                    .duration_since(std::time::UNIX_EPOCH)
                                    .map(|d| d.as_secs() as i64)
                                    .unwrap_or(0);

                                for (asset, price_opt) in prices {
                                    let Some(price) = price_opt else { continue };

                                    // Capture strike price if we don't have one and window has started
                                    if let Some(market) = s.markets.values_mut().find(|m| m.asset == asset) {
                                        if market.strike_price.is_none() {
                                            if let Some(start_ts) = market.window_start_ts {
                                                if now_ts >= start_ts {
                                                    market.strike_price = Some(price);
                                                    info!("[poly_momentum] {} strike captured: ${:.2}", asset, price);
                                                }
                                            }
                                        }
                                    }

                                    // Update price and get tick-to-tick change
                                    if let Some(tracker) = s.price_trackers.get_mut(asset) {
                                        if let Some(change_bps) = tracker.update(price) {
                                            // Log all price updates at debug level
                                            if change_bps != 0 {
                                                tracing::debug!("[price] {} ${:.2} ({:+}bps)", asset, price, change_bps);
                                            }

                                            // React to any significant tick movement
                                            if change_bps.abs() >= threshold_bps {
                                                // Skip if we're trading a specific symbol and this isn't it
                                                if let Some(ref sym) = trade_symbol {
                                                    if !asset.eq_ignore_ascii_case(sym) {
                                                        continue;
                                                    }
                                                }

                                                let direction = if change_bps > 0 {
                                                    Direction::Up
                                                } else {
                                                    Direction::Down
                                                };

                                                // Signal immediately on significant move
                                                // Only cooldown: 100ms to prevent duplicate signals on same move
                                                let should_signal = tracker.last_signal_time
                                                    .map(|t| t.elapsed() >= Duration::from_millis(100))
                                                    .unwrap_or(true);

                                                if should_signal {
                                                    warn!("[poly_momentum] {} {:?} {}bps ${:.2}",
                                                          asset, direction, change_bps.abs(), price);

                                                    tracker.last_signal_time = Some(Instant::now());
                                                    tracker.last_signal_direction = Some(direction);

                                                    s.pending_signals.push(MomentumSignal {
                                                        asset: asset.to_string(),
                                                        direction,
                                                        move_bps: change_bps,
                                                        triggered_at: Instant::now(),
                                                    });
                                                }
                                            }
                                        }
                                    }
                                }
                                }
                                Err(e) => {
                                    tracing::debug!("[ws] Parse error: {} - msg: {}", e, &text[..text.len().min(100)]);
                                }
                            }
                        }
                        Ok(Message::Ping(data)) => {
                            let _ = write.send(Message::Pong(data)).await;
                        }
                        Err(e) => {
                            error!("[poly_momentum] WebSocket error: {}", e);
                            break;
                        }
                        _ => {}
                    }
                }
            }
        }

        tracing::debug!("[poly_momentum] Disconnected, reconnecting...");
        tokio::time::sleep(Duration::from_secs(2)).await;
    }
}

/// Run direct Polygon.io price feed (no local server needed)
async fn run_direct_price_feed(
    state: Arc<RwLock<State>>,
    threshold_bps: i64,
    api_key: String,
    trade_symbol: Option<String>,
) {
    loop {
        tracing::debug!("[poly_momentum] Connecting to Polygon.io...");

        let ws = match connect_async(PRICEFEED_WS_URL).await {
            Ok((ws, _)) => ws,
            Err(e) => {
                error!("[poly_momentum] Polygon.io connect failed: {}", e);
                tokio::time::sleep(Duration::from_secs(5)).await;
                continue;
            }
        };

        let (mut write, mut read) = ws.split();
        tracing::debug!("[poly_momentum] Authenticating with Polygon.io...");

        // Authenticate
        let auth = PolygonAuth {
            action: "auth",
            params: api_key.clone(),
        };
        if let Err(e) = write.send(Message::Text(serde_json::to_string(&auth).unwrap())).await {
            error!("[poly_momentum] Auth send failed: {}", e);
            tokio::time::sleep(Duration::from_secs(5)).await;
            continue;
        }

        // Wait for auth response
        let mut authenticated = false;
        while let Some(msg) = read.next().await {
            if let Ok(Message::Text(text)) = msg {
                if let Ok(statuses) = serde_json::from_str::<Vec<PolygonStatus>>(&text) {
                    if let Some(s) = statuses.first() {
                        if s.status.as_deref() == Some("auth_success") {
                            tracing::debug!("[poly_momentum] Polygon.io authenticated");
                            authenticated = true;
                            break;
                        } else if s.status.as_deref() == Some("auth_failed") {
                            error!("[poly_momentum] Polygon.io auth failed: {:?}", s.message);
                            break;
                        }
                    }
                }
            }
        }

        if !authenticated {
            tokio::time::sleep(Duration::from_secs(5)).await;
            continue;
        }

        // Subscribe to crypto trades
        let subscribe = PolygonSubscribe {
            action: "subscribe",
            params: "XT.BTC-USD,XT.ETH-USD,XT.SOL-USD,XT.XRP-USD",
        };
        if let Err(e) = write.send(Message::Text(serde_json::to_string(&subscribe).unwrap())).await {
            error!("[poly_momentum] Subscribe failed: {}", e);
            tokio::time::sleep(Duration::from_secs(5)).await;
            continue;
        }
        tracing::debug!("[poly_momentum] Subscribed to trades");

        let mut ping_interval = tokio::time::interval(Duration::from_secs(30));

        loop {
            tokio::select! {
                _ = ping_interval.tick() => {
                    if let Err(e) = write.send(Message::Ping(vec![])).await {
                        error!("[poly_momentum] Ping failed: {}", e);
                        break;
                    }
                }

                msg = read.next() => {
                    let Some(msg) = msg else { break; };
                    match msg {
                        Ok(Message::Text(text)) => {
                            // Parse Polygon.io trade messages
                            if let Ok(trades) = serde_json::from_str::<Vec<PolygonTrade>>(&text) {
                                let mut s = state.write().await;

                                // Current time for strike capture
                                let now_ts = std::time::SystemTime::now()
                                    .duration_since(std::time::UNIX_EPOCH)
                                    .map(|d| d.as_secs() as i64)
                                    .unwrap_or(0);

                                for trade in trades {
                                    if trade.ev.as_deref() != Some("XT") {
                                        continue;
                                    }

                                    let Some(pair) = &trade.pair else { continue };
                                    let Some(price) = trade.p else { continue };

                                    let asset = match pair.as_str() {
                                        "BTC-USD" => "BTC",
                                        "ETH-USD" => "ETH",
                                        "SOL-USD" => "SOL",
                                        "XRP-USD" => "XRP",
                                        _ => continue,
                                    };

                                    // Capture strike price if we don't have one and window has started
                                    if let Some(market) = s.markets.values_mut().find(|m| m.asset == asset) {
                                        if market.strike_price.is_none() {
                                            if let Some(start_ts) = market.window_start_ts {
                                                if now_ts >= start_ts {
                                                    market.strike_price = Some(price);
                                                    info!("[poly_momentum] {} strike captured: ${:.2}", asset, price);
                                                }
                                            }
                                        }
                                    }

                                    // Update price and get tick-to-tick change
                                    if let Some(tracker) = s.price_trackers.get_mut(asset) {
                                        if let Some(change_bps) = tracker.update(price) {
                                            if change_bps != 0 {
                                                tracing::debug!("[price] {} ${:.2} ({:+}bps)", asset, price, change_bps);
                                            }

                                            // React to any significant tick movement
                                            if change_bps.abs() >= threshold_bps {
                                                // Skip if we're trading a specific symbol and this isn't it
                                                if let Some(ref sym) = trade_symbol {
                                                    if !asset.eq_ignore_ascii_case(sym) {
                                                        continue;
                                                    }
                                                }

                                                let direction = if change_bps > 0 {
                                                    Direction::Up
                                                } else {
                                                    Direction::Down
                                                };

                                                let should_signal = tracker.last_signal_time
                                                    .map(|t| t.elapsed() >= Duration::from_millis(100))
                                                    .unwrap_or(true);

                                                if should_signal {
                                                    warn!("[poly_momentum] {} {:?} {}bps ${:.2}",
                                                          asset, direction, change_bps.abs(), price);

                                                    tracker.last_signal_time = Some(Instant::now());
                                                    tracker.last_signal_direction = Some(direction);

                                                    s.pending_signals.push(MomentumSignal {
                                                        asset: asset.to_string(),
                                                        direction,
                                                        move_bps: change_bps,
                                                        triggered_at: Instant::now(),
                                                    });
                                                }
                                            }
                                        }
                                    }
                                }
                            }
                        }
                        Ok(Message::Ping(data)) => {
                            let _ = write.send(Message::Pong(data)).await;
                        }
                        Err(e) => {
                            error!("[poly_momentum] WebSocket error: {}", e);
                            break;
                        }
                        _ => {}
                    }
                }
            }
        }

        tracing::debug!("[poly_momentum] Polygon.io disconnected, reconnecting...");
        tokio::time::sleep(Duration::from_secs(2)).await;
    }
}

// === Polymarket WebSocket ===

#[derive(Deserialize, Debug)]
struct BookSnapshot {
    asset_id: String,
    bids: Vec<PriceLevel>,
    asks: Vec<PriceLevel>,
}

#[derive(Deserialize, Debug)]
struct PriceLevel {
    price: String,
    size: String,
}

// Incremental price updates from Polymarket
#[derive(Deserialize, Debug)]
struct PriceChangeMsg {
    price_changes: Vec<PriceChange>,
}

#[derive(Deserialize, Debug)]
struct PriceChange {
    asset_id: String,
    price: String,
}

#[derive(Serialize)]
struct SubscribeCmd {
    assets_ids: Vec<String>,
    #[serde(rename = "type")]
    sub_type: &'static str,
}

fn parse_price_cents(s: &str) -> i64 {
    s.parse::<f64>()
        .map(|p| (p * 100.0).round() as i64)
        .unwrap_or(0)
}

// === Main ===

#[tokio::main]
async fn main() -> Result<()> {
    use clap::Parser;

    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| {
                    tracing_subscriber::EnvFilter::new("poly_momentum=info,arb_bot=info")
                }),
        )
        .with_target(false)
        .init();

    let args = Args::parse();

    let mode = if args.live { "LIVE" } else { "DRY" };
    let sym_str = args.sym.as_deref().unwrap_or("ALL");
    info!("[poly_momentum] {} | {} | {}bps | {}¢ edge | max {} | TP {}¢",
          mode, sym_str, args.threshold_bps, args.edge, args.max_contracts, args.take_profit);

    // Load credentials from project root .env
    let project_root = std::path::Path::new(env!("CARGO_MANIFEST_DIR"));
    let env_path = project_root.join(".env");
    if env_path.exists() {
        dotenvy::from_path(&env_path).ok();
    } else {
        dotenvy::dotenv().ok();
    }

    let private_key = std::env::var("POLY_PRIVATE_KEY")
        .context("POLY_PRIVATE_KEY not set")?;
    let funder = std::env::var("POLY_FUNDER")
        .context("POLY_FUNDER not set")?;

    // Validate private key format
    let pk_len = private_key.len();
    if pk_len < 64 || (pk_len == 66 && !private_key.starts_with("0x")) {
        anyhow::bail!(
            "POLY_PRIVATE_KEY appears invalid (length {}). Expected 64 hex chars or 66 with 0x prefix.",
            pk_len
        );
    }

    // Validate funder address format
    let funder_len = funder.len();
    if funder_len != 42 || !funder.starts_with("0x") {
        anyhow::bail!(
            "POLY_FUNDER appears invalid (length {}). Expected 42 chars with 0x prefix (e.g., 0x123...abc).",
            funder_len
        );
    }

    tracing::debug!("[poly_momentum] Credentials loaded, funder={}", funder);

    // Initialize Polymarket client
    let poly_client = PolymarketAsyncClient::new(
        "https://clob.polymarket.com",
        137,
        &private_key,
        &funder,
    ).context("Failed to create Polymarket client")?;

    tracing::debug!("[poly_momentum] Wallet: {}", poly_client.wallet_address());
    let api_creds = poly_client.derive_api_key(0).await
        .context("derive_api_key failed - check wallet/funder addresses")?;
    let prepared_creds = PreparedCreds::from_api_creds(&api_creds)?;
    let shared_client = Arc::new(SharedAsyncClient::new(poly_client, prepared_creds, 137));
    tracing::debug!("[poly_momentum] API credentials ready");

    if args.live {
        warn!("⚠️  LIVE MODE - Real money at risk! ({} contracts)", args.contracts);
        tokio::time::sleep(Duration::from_secs(3)).await;
    }

    // Discover markets
    let discovered = discover_markets(args.sym.as_deref()).await?;
    for m in &discovered {
        info!("[poly_momentum] {} | {:.0}min left", m.asset, m.expiry_minutes.unwrap_or(0.0));
    }

    if discovered.is_empty() {
        warn!("No markets found!");
        return Ok(());
    }

    // Initialize state - convert PolyMarket to Market with trading state
    let state = Arc::new(RwLock::new({
        let mut s = State::new();
        for pm in discovered {
            let id = pm.condition_id.clone();
            s.markets.insert(id, Market::from_polymarket(pm));
        }
        s
    }));

    // Start price feed with momentum detection
    let state_price = state.clone();
    let threshold = args.threshold_bps;
    let use_direct = args.direct;
    let trade_symbol = args.sym.clone();

    if use_direct {
        // Get Polygon API key for direct connection
        let polygon_key = std::env::var("POLYGON_KEY")
            .or_else(|_| std::env::var("POLYGON_API_KEY"))
            .or_else(|_| std::env::var("PRICEFEED_API_KEY"))
            .context("POLYGON_KEY or PRICEFEED_API_KEY not set (required for --direct)")?;

        tokio::spawn(async move {
            run_direct_price_feed(state_price, threshold, polygon_key, trade_symbol).await;
        });
    } else {
        tokio::spawn(async move {
            run_price_feed(state_price, threshold, trade_symbol).await;
        });
    }

    // Get token IDs for subscription
    let tokens: Vec<String> = {
        let s = state.read().await;
        s.markets.values()
            .flat_map(|m| vec![m.yes_token.clone(), m.no_token.clone()])
            .collect()
    };

    let edge_threshold = args.edge;
    let contracts = args.contracts;
    let dry_run = !args.live;
    let status_symbol = args.sym;
    let max_contracts = args.max_contracts;
    let take_profit = args.take_profit;

    // Main WebSocket loop
    loop {
        tracing::debug!("[poly_momentum] Connecting to Polymarket...");

        let (ws, _) = match connect_async(POLYMARKET_WS_URL).await {
            Ok(ws) => ws,
            Err(e) => {
                error!("[poly_momentum] Connect failed: {}", e);
                tokio::time::sleep(Duration::from_secs(5)).await;
                continue;
            }
        };

        let (mut write, mut read) = ws.split();

        // Subscribe
        let sub = SubscribeCmd {
            assets_ids: tokens.clone(),
            sub_type: "market",
        };
        let _ = write.send(Message::Text(serde_json::to_string(&sub)?)).await;
        tracing::debug!("[poly_momentum] Subscribed to {} tokens", tokens.len());

        let mut ping_interval = tokio::time::interval(Duration::from_secs(30));
        let mut signal_check = tokio::time::interval(Duration::from_millis(100));
        let mut status_interval = tokio::time::interval(Duration::from_secs(10));

        loop {
            tokio::select! {
                _ = ping_interval.tick() => {
                    if let Err(e) = write.send(Message::Ping(vec![])).await {
                        error!("[poly_momentum] Ping failed: {}", e);
                        break;
                    }
                }

                _ = status_interval.tick() => {
                    // Print periodic status
                    let s = state.read().await;
                    let mut status_parts: Vec<String> = Vec::new();

                    for asset in ["BTC", "ETH", "SOL", "XRP"] {
                        // Skip if we're trading a specific symbol and this isn't it
                        if let Some(ref sym) = status_symbol {
                            if !asset.eq_ignore_ascii_case(sym) {
                                continue;
                            }
                        }

                        if let Some(tracker) = s.price_trackers.get(asset) {
                            if let Some(price) = tracker.last_price {
                                // Find market for this asset
                                if let Some(market) = s.markets.values().find(|m| m.asset == asset) {
                                    let yes_ask = market.yes_ask.map(|a| format!("{}¢", a)).unwrap_or_else(|| "-".to_string());
                                    let no_ask = market.no_ask.map(|a| format!("{}¢", a)).unwrap_or_else(|| "-".to_string());
                                    let remaining = market.time_remaining_mins()
                                        .map(|m| format!("{:.0}m", m))
                                        .unwrap_or_else(|| "-".to_string());
                                    let strike_str = market.strike_price
                                        .map(|s| format!("${:.0}", s))
                                        .unwrap_or_else(|| "?".to_string());
                                    let fair_str = market.calc_fair_yes(price)
                                        .map(|f| format!("{}¢", f))
                                        .unwrap_or_else(|| "?".to_string());
                                    status_parts.push(format!("{}: ${:.0} (strike={}) Y={} N={} fair={} [{}]",
                                        asset, price, strike_str, yes_ask, no_ask, fair_str, remaining));
                                } else {
                                    status_parts.push(format!("{}: ${:.2}", asset, price));
                                }
                            }
                        }
                    }

                    if !status_parts.is_empty() {
                        tracing::debug!("[status] {}", status_parts.join(" | "));
                    }

                    // Print positions if any
                    if !s.open_positions.is_empty() {
                        for pos in &s.open_positions {
                            // Find current bid for this position
                            let market = s.markets.values().find(|m|
                                m.yes_token == pos.token_id || m.no_token == pos.token_id
                            );
                            let current_bid = market.and_then(|m| {
                                if pos.side == "YES" { m.yes_bid } else { m.no_bid }
                            });
                            let pnl = current_bid.map(|bid| {
                                let cost = pos.size * pos.entry_price as f64 / 100.0;
                                let value = pos.size * bid as f64 / 100.0;
                                value - cost
                            });
                            let pnl_str = pnl.map(|p| format!("{:+.2}", p)).unwrap_or_else(|| "?".to_string());
                            let bid_str = current_bid.map(|b| format!("{}¢", b)).unwrap_or_else(|| "?".to_string());
                            info!("[pos] {:.1} {} {} @{}¢ | bid={} | P&L ${}",
                                  pos.size, pos.asset, pos.side, pos.entry_price, bid_str, pnl_str);
                        }
                    }
                }

                _ = signal_check.tick() => {
                    // Process pending momentum signals
                    let mut s = state.write().await;

                    // Check for profit-taking opportunities
                    if !dry_run {
                        // (token_id, asset, bid, size, entry_price)
                        let positions_to_close: Vec<(String, String, i64, f64, i64)> = s.open_positions.iter()
                            .filter_map(|pos| {
                                // Find the market for this position
                                let market = s.markets.values().find(|m|
                                    m.yes_token == pos.token_id || m.no_token == pos.token_id
                                )?;

                                // Get current bid for our position
                                let current_bid = if pos.side == "YES" { market.yes_bid? } else { market.no_bid? };

                                // Check if we can take profit
                                let profit = current_bid - pos.entry_price;
                                if profit >= take_profit {
                                    Some((pos.token_id.clone(), pos.asset.clone(), current_bid, pos.size, pos.entry_price))
                                } else {
                                    None
                                }
                            })
                            .collect();

                        for (token_id, asset, bid, size, entry) in positions_to_close {
                            let client = shared_client.clone();
                            let state_clone = state.clone();
                            let token_clone = token_id.clone();

                            let cost = size * entry as f64 / 100.0;
                            let revenue = size * bid as f64 / 100.0;
                            let profit = revenue - cost;
                            warn!("[poly_momentum] 💰 SELL {:.1} {} @{}¢ (cost=${:.2} rev=${:.2} +${:.2})",
                                  size, asset, bid, cost, revenue, profit);

                            tokio::spawn(async move {
                                // Sell 1¢ below bid to ensure fill
                                let sell_price = (bid - 1).max(1) as f64 / 100.0;
                                match client.sell_fak(&token_clone, sell_price, size).await {
                                    Ok(fill) if fill.filled_size > 0.0 => {
                                        warn!("[poly_momentum] ✅ Sold {:.1} @${:.2}", fill.filled_size, fill.fill_cost);
                                        let mut s = state_clone.write().await;
                                        s.remove_position(&token_clone);
                                    }
                                    Ok(_) => {}
                                    Err(e) => {
                                        error!("[poly_momentum] ❌ Sell error: {}", e);
                                    }
                                }
                            });
                        }
                    }

                    // Remove stale signals (>5s old)
                    s.pending_signals.retain(|sig| sig.triggered_at.elapsed() < Duration::from_secs(5));

                    // Process signals
                    let signals: Vec<MomentumSignal> = s.pending_signals.drain(..).collect();

                    // Get position size before signal loop (to avoid borrow conflicts)
                    let current_position = s.total_position_size();

                    for signal in signals {
                        // Check max contracts limit first
                        if current_position >= max_contracts {
                            tracing::debug!("[poly_momentum] Max position reached ({:.0}/{:.0})", current_position, max_contracts);
                            continue;
                        }

                        // Get current price first (before mutable borrow)
                        let current_price = s.price_trackers.get(&signal.asset)
                            .and_then(|t| t.last_price);

                        let Some(current_price) = current_price else {
                            tracing::debug!("[poly_momentum] {} - no current price", signal.asset);
                            continue;
                        };

                        // Find market for this asset
                        let market_entry = s.markets.iter_mut()
                            .find(|(_, m)| m.asset == signal.asset);

                        let Some((market_id, market)) = market_entry else {
                            continue;
                        };

                        // Determine which side to buy
                        let (buy_token, buy_side, ask_price) = match signal.direction {
                            Direction::Up => {
                                // Price went up, buy YES
                                (&market.yes_token, "YES", market.yes_ask)
                            }
                            Direction::Down => {
                                // Price went down, buy NO
                                (&market.no_token, "NO", market.no_ask)
                            }
                        };

                        let Some(ask) = ask_price else {
                            tracing::debug!("[poly_momentum] {} {} - no ask price", signal.asset, buy_side);
                            continue;
                        };

                        // Calculate fair value based on position vs strike
                        let estimated_fair = market.calc_fair_yes(current_price).unwrap_or(50);

                        // For NO side, fair = 100 - YES fair
                        let (fair_for_side, edge) = match signal.direction {
                            Direction::Up => (estimated_fair, estimated_fair - ask),
                            Direction::Down => {
                                let fair_no = 100 - estimated_fair;
                                (fair_no, fair_no - ask)
                            }
                        };

                        if edge < edge_threshold {
                            tracing::debug!("[poly_momentum] {} {} - edge {}¢ < {}¢ (ask={}¢ fair={}¢)",
                                  signal.asset, buy_side, edge, edge_threshold, ask, fair_for_side);
                            continue;
                        }

                        // Calculate remaining capacity
                        let remaining = max_contracts - current_position;

                        let market_id_clone = market_id.clone();
                        let buy_token_clone = buy_token.clone();
                        let asset_clone = signal.asset.clone();
                        let entry_price = ask + 2; // crossing price in cents

                        // Add 2¢ to cross the spread and ensure FAK fills
                        let cross_price = entry_price.min(99) as f64 / 100.0;
                        // Polymarket requires minimum $1 order value - scale up contracts if needed
                        let min_contracts = (1.0 / cross_price).ceil();
                        // Don't exceed remaining capacity
                        let actual_contracts = contracts.max(min_contracts).min(remaining);
                        let cost = actual_contracts * cross_price;

                        // Skip if below $1 minimum or less than 1 contract
                        if cost < 1.0 || actual_contracts < 1.0 {
                            tracing::debug!("[poly_momentum] Skip: cost ${:.2} < $1 min (remaining={:.1})", cost, remaining);
                            continue;
                        }

                        if dry_run {
                            warn!("[poly_momentum] 🎯 Would BUY {:.0} {} {} @{}¢ (${:.2}) | edge={}¢",
                                  actual_contracts, signal.asset, buy_side, entry_price, cost, edge);
                            market.last_trade_time = Some(Instant::now());
                        } else {
                            warn!("[poly_momentum] 🎯 BUY {:.0} {} {} @{}¢ (${:.2}) | edge={}¢",
                                  actual_contracts, signal.asset, buy_side, entry_price, cost, edge);

                            let client = shared_client.clone();
                            let state_clone = state.clone();

                            // Execute trade asynchronously
                            tokio::spawn(async move {
                                match client.buy_fak(&buy_token_clone, cross_price, actual_contracts).await {
                                    Ok(fill) if fill.filled_size > 0.0 => {
                                        warn!("[poly_momentum] ✅ Filled {:.1} @${:.2}",
                                              fill.filled_size, fill.fill_cost);

                                        let mut s = state_clone.write().await;
                                        if let Some(m) = s.markets.get_mut(&market_id_clone) {
                                            m.last_trade_time = Some(Instant::now());
                                        }
                                        // Track position
                                        s.add_position(asset_clone, buy_token_clone, buy_side, entry_price, fill.filled_size);
                                    }
                                    Ok(_) => {
                                        // No fill - FAK killed, already logged in clob
                                    }
                                    Err(e) => {
                                        error!("[poly_momentum] ❌ Order error: {}", e);
                                    }
                                }
                            });

                            market.last_trade_time = Some(Instant::now());
                        }
                    }
                }

                msg = read.next() => {
                    let Some(msg) = msg else { break; };

                    match msg {
                        Ok(Message::Text(text)) => {
                            // Update orderbook
                            tracing::trace!("[ws] Received: {}", &text[..text.len().min(200)]);

                            // Try parsing as full orderbook snapshot
                            if let Ok(books) = serde_json::from_str::<Vec<BookSnapshot>>(&text) {
                                let mut s = state.write().await;

                                for book in books {
                                    // Find market
                                    let market = s.markets.values_mut()
                                        .find(|m| m.yes_token == book.asset_id || m.no_token == book.asset_id);

                                    let Some(market) = market else { continue };

                                    let best_ask = book.asks.iter()
                                        .filter_map(|l| {
                                            let price = parse_price_cents(&l.price);
                                            if price > 0 { Some(price) } else { None }
                                        })
                                        .min();

                                    let best_bid = book.bids.iter()
                                        .filter_map(|l| {
                                            let price = parse_price_cents(&l.price);
                                            if price > 0 { Some(price) } else { None }
                                        })
                                        .max();

                                    let is_yes = book.asset_id == market.yes_token;

                                    if is_yes {
                                        market.yes_ask = best_ask;
                                        market.yes_bid = best_bid;
                                    } else {
                                        market.no_ask = best_ask;
                                        market.no_bid = best_bid;
                                    }
                                }
                            }
                            // Try parsing as incremental price change
                            else if let Ok(msg) = serde_json::from_str::<PriceChangeMsg>(&text) {
                                let mut s = state.write().await;

                                for change in msg.price_changes {
                                    let market = s.markets.values_mut()
                                        .find(|m| m.yes_token == change.asset_id || m.no_token == change.asset_id);

                                    let Some(market) = market else { continue };
                                    let price = parse_price_cents(&change.price);
                                    if price <= 0 { continue; }

                                    let is_yes = change.asset_id == market.yes_token;
                                    let side = if is_yes { "YES" } else { "NO" };

                                    // Use price as approximate mid - bid slightly below, ask slightly above
                                    let bid = (price - 1).max(1);
                                    let ask = (price + 1).min(99);

                                    tracing::debug!("[book] {} {} price={}¢ -> bid={}¢ ask={}¢",
                                        market.asset, side, price, bid, ask);

                                    if is_yes {
                                        market.yes_bid = Some(bid);
                                        market.yes_ask = Some(ask);
                                    } else {
                                        market.no_bid = Some(bid);
                                        market.no_ask = Some(ask);
                                    }
                                }
                            }
                        }
                        Ok(Message::Ping(data)) => {
                            let _ = write.send(Message::Pong(data)).await;
                        }
                        Ok(Message::Close(_)) | Err(_) => break,
                        _ => {}
                    }
                }
            }
        }

        tracing::debug!("[poly_momentum] Polymarket disconnected, reconnecting...");
        tokio::time::sleep(Duration::from_secs(3)).await;
    }
}
