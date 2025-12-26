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
use ethers::prelude::*;
use ethers::providers::{Http, Provider};
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

const USDC_CONTRACT: &str = "0x2791Bca1f2de4661ED88A30C99A7a9449Aa84174";
const RPC_URL: &str = "https://polygon-rpc.com";

/// Query USDC balance for a wallet
async fn get_usdc_balance(wallet: Address) -> Result<f64> {
    let provider = Provider::<Http>::try_from(RPC_URL)?;
    let usdc: Address = USDC_CONTRACT.parse()?;

    // balanceOf(address) selector: 0x70a08231
    let call_data = ethers::abi::encode(&[ethers::abi::Token::Address(wallet)]);
    let mut data = vec![0x70, 0xa0, 0x82, 0x31];
    data.extend(call_data);

    let tx = TransactionRequest::new().to(usdc).data(data);
    let result = provider.call(&tx.into(), None).await?;
    let balance = U256::from_big_endian(&result);

    // USDC has 6 decimals
    Ok(balance.as_u128() as f64 / 1_000_000.0)
}

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

    /// Live trading mode (default is dry run)
    #[arg(short, long, default_value_t = false)]
    live: bool,

    /// Specific symbol to trade (BTC, ETH, SOL, XRP) - trades all if not set
    #[arg(long)]
    sym: Option<String>,

    /// Connect directly to Polygon.io instead of local price server
    #[arg(short, long, default_value_t = false)]
    direct: bool,

    /// Max total dollars to invest (default: $1)
    #[arg(short, long, default_value_t = 1.0)]
    max_dollars: f64,

    /// Take profit threshold in cents (sell when bid >= entry + this)
    #[arg(long, default_value_t = 5)]
    take_profit: i64,

    /// Stop loss: max loss in cents (sell if bid <= entry - this after stop_loss_mins)
    #[arg(long, default_value_t = 3)]
    stop_loss: i64,

    /// Seconds to wait before stop-loss kicks in (0 = immediate)
    #[arg(long, default_value_t = 10)]
    stop_loss_secs: u64,

    /// Sell retry cooldown in milliseconds (default: 1000ms)
    #[arg(long, default_value_t = 1000)]
    sell_retry_ms: u64,
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
    _condition_id: String,
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
            _condition_id: pm.condition_id,
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
    sell_pending: bool, // Track if a sell order is already in-flight
    last_sell_attempt: Option<Instant>, // Cooldown after failed sells
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

    fn total_invested(&self) -> f64 {
        self.open_positions.iter()
            .map(|p| p.size * p.entry_price as f64 / 100.0)
            .sum()
    }

    fn add_position(&mut self, asset: String, token_id: String, side: &'static str, entry_price: i64, size: f64) {
        // Check for existing position with same token_id - update size instead of duplicating
        if let Some(existing) = self.open_positions.iter_mut().find(|p| p.token_id == token_id) {
            let old_size = existing.size;
            existing.size += size;
            // Weighted average entry price
            let old_cost = old_size * existing.entry_price as f64;
            let new_cost = size * entry_price as f64;
            existing.entry_price = ((old_cost + new_cost) / existing.size) as i64;
            tracing::info!("[pos] Updated {} {} position: {:.1} -> {:.1} contracts",
                          existing.asset, existing.side, old_size, existing.size);
            return;
        }

        self.open_positions.push(OpenPosition {
            asset,
            token_id,
            side,
            entry_price,
            size,
            opened_at: Instant::now(),
            sell_pending: false,
            last_sell_attempt: None,
        });
    }

    fn mark_sell_pending(&mut self, token_id: &str) -> bool {
        if let Some(pos) = self.open_positions.iter_mut().find(|p| p.token_id == token_id) {
            if pos.sell_pending {
                tracing::warn!("[state] token {} already pending!", &token_id[..8.min(token_id.len())]);
                return false;
            }
            pos.sell_pending = true;
            tracing::debug!("[state] marked {} as sell_pending", &token_id[..8.min(token_id.len())]);
            true
        } else {
            tracing::warn!("[state] token {} not found for mark_pending!", &token_id[..8.min(token_id.len())]);
            false
        }
    }

    fn clear_sell_pending(&mut self, token_id: &str, failed: bool) {
        if let Some(pos) = self.open_positions.iter_mut().find(|p| p.token_id == token_id) {
            pos.sell_pending = false;
            if failed {
                pos.last_sell_attempt = Some(Instant::now());
            }
        }
    }

    fn get_position(&self, token_id: &str) -> Option<&OpenPosition> {
        self.open_positions.iter().find(|p| p.token_id == token_id)
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
    #[allow(dead_code)]
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
    t: Option<i64>,  // Unix timestamp in milliseconds
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
                                    // Only capture if within 30 seconds of window start (avoid stale strikes on restart)
                                    if let Some(market) = s.markets.values_mut().find(|m| m.asset == asset) {
                                        if market.strike_price.is_none() {
                                            if let Some(start_ts) = market.window_start_ts {
                                                let secs_since_start = now_ts - start_ts;
                                                if now_ts >= start_ts && secs_since_start <= 30 {
                                                    market.strike_price = Some(price);
                                                    info!("[poly_momentum] {} strike captured: ${:.2}", asset, price);
                                                } else if secs_since_start > 30 && secs_since_start < 60 {
                                                    warn!("[poly_momentum] {} skipping strike - joined {}s late",
                                                          asset, secs_since_start);
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
                                                    info!("[poly_momentum] {} {:?} {}bps ${:.2}",
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

                                    // Calculate latency if timestamp available
                                    let latency_ms = trade.t.map(|trade_ts_ms| {
                                        let now_ms = std::time::SystemTime::now()
                                            .duration_since(std::time::UNIX_EPOCH)
                                            .map(|d| d.as_millis() as i64)
                                            .unwrap_or(0);
                                        now_ms - trade_ts_ms
                                    });

                                    // Capture strike price if we don't have one and window has started
                                    // Only capture if within 30 seconds of window start (avoid stale strikes on restart)
                                    if let Some(market) = s.markets.values_mut().find(|m| m.asset == asset) {
                                        if market.strike_price.is_none() {
                                            if let Some(start_ts) = market.window_start_ts {
                                                let secs_since_start = now_ts - start_ts;
                                                if now_ts >= start_ts && secs_since_start <= 30 {
                                                    market.strike_price = Some(price);
                                                    info!("[poly_momentum] {} strike captured: ${:.2}", asset, price);
                                                } else if secs_since_start > 30 && secs_since_start < 60 {
                                                    warn!("[poly_momentum] {} skipping strike - joined {}s late",
                                                          asset, secs_since_start);
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
                                                    let lat_str = latency_ms.map(|l| format!(" ({}ms)", l)).unwrap_or_default();
                                                    info!("[poly_momentum] {} {:?} {}bps ${:.2}{}",
                                                          asset, direction, change_bps.abs(), price, lat_str);

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
    #[allow(dead_code)]
    size: String,
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
    info!("[poly_momentum] {} | {} | {}bps threshold | max ${} | TP {}¢",
          mode, sym_str, args.threshold_bps, args.max_dollars, args.take_profit);

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

    // Parse wallet address for balance checks
    let wallet_address: Address = funder.parse()
        .context("Failed to parse funder address")?;

    // Check and display USDC balance
    let usdc_balance = get_usdc_balance(wallet_address).await.unwrap_or(0.0);
    info!("[poly_momentum] 💰 USDC Balance: ${:.2}", usdc_balance);

    if args.live {
        if usdc_balance < 1.0 {
            warn!("⚠️  LOW BALANCE: Only ${:.2} USDC - need at least $1 for orders!", usdc_balance);
        }
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

    let _contracts = args.contracts;
    let dry_run = !args.live;
    let status_symbol = args.sym;
    let max_dollars = args.max_dollars;
    let take_profit = args.take_profit;
    let stop_loss = args.stop_loss;
    let stop_loss_secs = args.stop_loss_secs;
    let sell_retry_ms = args.sell_retry_ms;

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
        let mut status_interval = tokio::time::interval(Duration::from_secs(5));
        let mut market_refresh = tokio::time::interval(Duration::from_secs(60)); // Refresh markets every 60s

        loop {
            tokio::select! {
                _ = ping_interval.tick() => {
                    if let Err(e) = write.send(Message::Ping(vec![])).await {
                        error!("[poly_momentum] Ping failed: {}", e);
                        break;
                    }
                }

                _ = market_refresh.tick() => {
                    // Refresh if: (1) positions have missing bid data, or (2) no tradeable markets
                    let (missing_bids, has_tradeable_markets) = {
                        let s = state.read().await;
                        let missing = s.open_positions.iter().any(|pos| {
                            let market = s.markets.values().find(|m|
                                m.yes_token == pos.token_id || m.no_token == pos.token_id
                            );
                            let has_bid = market.and_then(|m| {
                                if pos.side == "YES" { m.yes_bid } else { m.no_bid }
                            }).is_some();
                            !has_bid
                        });
                        let tradeable = s.markets.values().any(|m|
                            m.time_remaining_mins().unwrap_or(0.0) > 1.0
                        );
                        (missing, tradeable)
                    };

                    if !missing_bids && has_tradeable_markets {
                        continue; // Skip refresh if everything is fine
                    }

                    let reason = if missing_bids { "missing bid data" } else { "no tradeable markets" };
                    info!("[poly_momentum] 🔄 Refreshing markets ({})", reason);
                    let sym_filter = status_symbol.as_deref();
                    match discover_markets(sym_filter).await {
                        Ok(new_markets) => {
                            let mut s = state.write().await;
                            let mut new_tokens: Vec<String> = Vec::new();

                            // Add/update markets - preserve existing orderbook data!
                            for pm in new_markets {
                                let id = pm.condition_id.clone();
                                if let Some(existing) = s.markets.get_mut(&id) {
                                    // Update expiry but KEEP bid/ask data from websocket
                                    existing.expiry_minutes = pm.expiry_minutes;
                                    existing.discovered_at = Instant::now();
                                } else {
                                    info!("[poly_momentum] 🔄 New market: {} ({:.0}m)", pm.asset, pm.expiry_minutes.unwrap_or(0.0));
                                    new_tokens.push(pm.yes_token.clone());
                                    new_tokens.push(pm.no_token.clone());
                                    s.markets.insert(id, Market::from_polymarket(pm));
                                }
                            }

                            // Always remove expired markets (exp <= 0), even if API returned nothing
                            let before = s.markets.len();
                            s.markets.retain(|_, m| m.time_remaining_mins().unwrap_or(0.0) > 0.0);
                            let removed = before - s.markets.len();
                            if removed > 0 {
                                info!("[poly_momentum] 🗑️ Removed {} expired markets", removed);
                            }

                            // Subscribe websocket to new tokens
                            drop(s); // Release the lock before await
                            if !new_tokens.is_empty() {
                                let sub = SubscribeCmd {
                                    assets_ids: new_tokens.clone(),
                                    sub_type: "market",
                                };
                                if let Err(e) = write.send(Message::Text(serde_json::to_string(&sub).unwrap())).await {
                                    warn!("[poly_momentum] Failed to subscribe new tokens: {}", e);
                                } else {
                                    info!("[poly_momentum] 📡 Subscribed to {} new tokens", new_tokens.len());
                                }
                            }
                        }
                        Err(e) => {
                            warn!("[poly_momentum] Market refresh failed: {}", e);
                        }
                    }
                }

                _ = status_interval.tick() => {
                    // Get balance first (before taking lock)
                    let balance = get_usdc_balance(wallet_address).await.unwrap_or(0.0);

                    // Print periodic status
                    let s = state.read().await;

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
                                    // Show bid/ask for the side we're holding (or YES by default)
                                    let holding_no = s.open_positions.iter().any(|p| p.asset == asset && p.side == "NO");
                                    let (side_label, bid_val, ask_val) = if holding_no {
                                        ("NO", market.no_bid, market.no_ask)
                                    } else {
                                        ("YES", market.yes_bid, market.yes_ask)
                                    };
                                    let bid_str = bid_val.map(|b| format!("{}¢", b)).unwrap_or_else(|| "-".to_string());
                                    let ask_str = ask_val.map(|a| format!("{}¢", a)).unwrap_or_else(|| "-".to_string());
                                    let remaining = market.time_remaining_mins()
                                        .map(|m| format!("{:.0}m left", m))
                                        .unwrap_or_else(|| "-".to_string());
                                    let strike_str = market.strike_price
                                        .map(|s| format!("${:.0}", s))
                                        .unwrap_or_else(|| "?".to_string());
                                    let pct_str = market.strike_price
                                        .map(|strike| {
                                            let pct = (price - strike) / strike * 100.0;
                                            format!("{:+.3}%", pct)
                                        })
                                        .unwrap_or_else(|| "?".to_string());
                                    // Check for position in this asset
                                    let pos_str = s.open_positions.iter()
                                        .find(|p| p.asset == asset)
                                        .map(|pos| {
                                            let current_bid = if pos.side == "YES" {
                                                market.yes_bid
                                            } else {
                                                market.no_bid
                                            };
                                            let pnl_str = current_bid.map(|bid| {
                                                let cost = pos.size * pos.entry_price as f64 / 100.0;
                                                let value = pos.size * bid as f64 / 100.0;
                                                format!(" P&L ${:+.2}", value - cost)
                                            }).unwrap_or_default();
                                            format!(" | POS: {:.0} {} @{}¢{}", pos.size, pos.side, pos.entry_price, pnl_str)
                                        })
                                        .unwrap_or_else(|| " | Pos 0".to_string());

                                    // Shorten market name: extract just the time window like "2:15-2:30"
                                    let short_name = market.question
                                        .split(',')
                                        .nth(1)
                                        .and_then(|s| s.trim().split(" ET").next())
                                        .unwrap_or(&market.question[..market.question.len().min(20)]);

                                    info!("[poly_momentum] [{}] ${:.2} | {} | ${:.0} K={} {} | {} b={} a={} | {}{}",
                                        asset, balance, short_name, price, strike_str, pct_str, side_label, bid_str, ask_str, remaining, pos_str);
                                }
                            }
                        }
                    }
                }

                _ = signal_check.tick() => {
                    // Process pending momentum signals
                    let mut s = state.write().await;

                    // Check for profit-taking opportunities (both live and paper)
                    // Only log tracking when there are actual positions without pending sells
                    let active_positions = s.open_positions.iter()
                        .filter(|p| !p.sell_pending)
                        .count();

                    if active_positions > 0 {
                        let mode_tag = if dry_run { "[DRY]" } else { "[LIVE]" };
                        tracing::debug!("[poly_momentum] {} Tracking {} positions ({} active)",
                                       mode_tag, s.open_positions.len(), active_positions);

                        // Cooldowns: 3s for Polymarket API balance cache sync, configurable retry after failed sell
                        const API_SYNC_DELAY: Duration = Duration::from_secs(3);
                        let sell_retry_cooldown = Duration::from_millis(sell_retry_ms);

                        // (token_id, asset, bid, size, entry_price)
                        let positions_to_close: Vec<(String, String, i64, f64, i64)> = s.open_positions.iter()
                            .filter(|pos| !pos.sell_pending) // Skip positions with pending sells
                            .filter(|pos| pos.opened_at.elapsed() >= API_SYNC_DELAY) // Wait for Polymarket API to sync
                            .filter(|pos| pos.last_sell_attempt
                                .map(|t| t.elapsed() >= sell_retry_cooldown)
                                .unwrap_or(true)) // Cooldown after failed sells
                            .filter_map(|pos| {
                                // Find the market for this position
                                let market = s.markets.values().find(|m|
                                    m.yes_token == pos.token_id || m.no_token == pos.token_id
                                )?;

                                // Get current bid for our position
                                let current_bid = if pos.side == "YES" { market.yes_bid? } else { market.no_bid? };

                                let profit = current_bid - pos.entry_price;
                                let age_secs = pos.opened_at.elapsed().as_secs();

                                // Take profit: sell when profitable enough
                                if profit >= take_profit {
                                    return Some((pos.token_id.clone(), pos.asset.clone(), current_bid, pos.size, pos.entry_price));
                                }

                                // Stop loss: after N secs, sell at up to X cents loss
                                if age_secs >= stop_loss_secs && profit >= -stop_loss {
                                    info!("[poly_momentum] ⏰ STOP-LOSS: {} {}s old, {}¢ loss",
                                          pos.asset, age_secs, -profit);
                                    return Some((pos.token_id.clone(), pos.asset.clone(), current_bid, pos.size, pos.entry_price));
                                }

                                // Emergency exit: if market expires in < 2 mins, sell at any price
                                let time_left = market.time_remaining_mins().unwrap_or(999.0);
                                if time_left < 2.0 {
                                    warn!("[poly_momentum] ⚠️ EMERGENCY EXIT: {} only {:.1}min left! Selling at {}¢ loss",
                                          pos.asset, time_left, -profit);
                                    return Some((pos.token_id.clone(), pos.asset.clone(), current_bid, pos.size, pos.entry_price));
                                }

                                None
                            })
                            .collect();

                        for (token_id, asset, bid, size, entry) in positions_to_close {
                            let cost = size * entry as f64 / 100.0;
                            let revenue = size * bid as f64 / 100.0;
                            let profit = revenue - cost;

                            if dry_run {
                                info!("[poly_momentum] [DRY] 💰 Would SELL {:.1} {} @{}¢ (cost=${:.2} rev=${:.2} P&L=${:+.2})",
                                      size, asset, bid, cost, revenue, profit);
                                s.remove_position(&token_id);
                            } else {
                                // Mark as pending before spawning async task
                                s.mark_sell_pending(&token_id);

                                // Get actual current market bid for logging
                                let market_bid = s.markets.values().find(|m|
                                    m.yes_token == token_id || m.no_token == token_id
                                ).and_then(|m| {
                                    // Check if this token is YES or NO
                                    if m.yes_token == token_id { m.yes_bid } else { m.no_bid }
                                });
                                let market_bid_str = market_bid.map(|b| format!("{}¢", b)).unwrap_or_else(|| "?".to_string());

                                let client = shared_client.clone();
                                let state_clone = state.clone();
                                let token_clone = token_id.clone();

                                // Use actual market bid for sell, not stale bid from filter
                                let actual_bid = market_bid.unwrap_or(bid);

                                // Sell at bid-1 to ensure fill (FAK will match at best available)
                                let sell_cents = (actual_bid - 1).max(1);

                                // Log position state before selling
                                info!("[poly_momentum] [LIVE] 💰 SELL {:.1} {} | entry={}¢ mkt_bid={} | selling @{}¢",
                                      size, asset, entry, market_bid_str, sell_cents);

                                let entry_cents = entry;
                                let asset_clone = asset.clone();
                                tokio::spawn(async move {
                                    let sell_price = sell_cents as f64 / 100.0;
                                    match client.sell_fak(&token_clone, sell_price, size).await {
                                        Ok(fill) if fill.filled_size > 0.0 => {
                                            let price_cents = if fill.filled_size > 0.0 { (fill.fill_cost / fill.filled_size * 100.0).round() as i64 } else { 0 };
                                            let cost = fill.filled_size * entry_cents as f64 / 100.0;
                                            let profit = fill.fill_cost - cost;
                                            info!("[poly_momentum] ✅ SELL Filled {:.1} @{}¢ for ${:.2} (entry {}¢ P&L ${:+.2})",
                                                  fill.filled_size, price_cents, fill.fill_cost, entry_cents, profit);

                                            // Log realized P&L to file
                                            let timestamp = chrono::Utc::now().format("%Y-%m-%d %H:%M:%S UTC");
                                            let log_line = format!("{} | {} | entry={}¢ exit={}¢ | size={:.1} | cost=${:.2} rev=${:.2} | P&L=${:+.2}\n",
                                                timestamp, asset_clone, entry_cents, price_cents, fill.filled_size, cost, fill.fill_cost, profit);
                                            if let Err(e) = std::fs::OpenOptions::new()
                                                .create(true)
                                                .append(true)
                                                .open("pnl_log.csv")
                                                .and_then(|mut f| std::io::Write::write_all(&mut f, log_line.as_bytes()))
                                            {
                                                warn!("[poly_momentum] Failed to write P&L log: {}", e);
                                            }

                                            let mut s = state_clone.write().await;
                                            s.remove_position(&token_clone);
                                        }
                                        Ok(_) => {
                                            // No fill - FAK killed (no liquidity at price)
                                            warn!("[poly_momentum] No liquidity @{}¢ - retrying in {}ms", sell_cents, sell_retry_ms);
                                            let mut s = state_clone.write().await;
                                            s.clear_sell_pending(&token_clone, true); // Apply cooldown
                                        }
                                        Err(e) => {
                                            let err_str = e.to_string();
                                            // Check if market is closed/expired
                                            if err_str.contains("does not exist") || err_str.contains("market closed") {
                                                warn!("[poly_momentum] ⏹️ Market closed - removing position");
                                                let mut s = state_clone.write().await;
                                                s.remove_position(&token_clone);
                                            } else {
                                                error!("[poly_momentum] ❌ Sell error: {}", e);
                                                // Mark as failed to trigger cooldown before retry
                                                let mut s = state_clone.write().await;
                                                s.clear_sell_pending(&token_clone, true);
                                            }
                                        }
                                    }
                                });
                            }
                        }
                    }

                    // Remove stale signals (>5s old)
                    s.pending_signals.retain(|sig| sig.triggered_at.elapsed() < Duration::from_secs(5));

                    // Process signals
                    let signals: Vec<MomentumSignal> = s.pending_signals.drain(..).collect();

                    // Get total dollars invested before signal loop
                    let current_invested = s.total_invested();

                    for signal in signals {
                        // Check max dollars limit first
                        if current_invested >= max_dollars {
                            info!("[poly_momentum] ⏭️ Max invested (${:.2}/${:.0})", current_invested, max_dollars);
                            continue;
                        }

                        // Verify we have a current price for this asset
                        let has_price = s.price_trackers.get(&signal.asset)
                            .and_then(|t| t.last_price)
                            .is_some();

                        if !has_price {
                            info!("[poly_momentum] {} - no current price yet", signal.asset);
                            continue;
                        }

                        // Find market for this asset
                        let market_entry = s.markets.iter_mut()
                            .find(|(_, m)| m.asset == signal.asset);

                        let Some((market_id, market)) = market_entry else {
                            info!("[poly_momentum] {} - no market found", signal.asset);
                            continue;
                        };

                        // Skip if market is expired or about to expire
                        let time_left = market.time_remaining_mins().unwrap_or(0.0);
                        if time_left < 1.0 {
                            tracing::debug!("[poly_momentum] {} - market expired ({:.1}m left)", signal.asset, time_left);
                            continue;
                        }

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
                            info!("[poly_momentum] {} {} - no ask price in orderbook", signal.asset, buy_side);
                            continue;
                        };

                        // Only trade when price is in 30-50¢ range (sweet spot from historical analysis)
                        if ask < 30 || ask > 50 {
                            info!("[poly_momentum] {} {} skip: {}¢ outside 30-50¢ range", signal.asset, buy_side, ask);
                            continue;
                        }

                        // Calculate remaining dollar capacity
                        let remaining_dollars = max_dollars - current_invested;

                        // Log signal conditions
                        let strike_str = market.strike_price.map(|s| format!("${:.0}", s)).unwrap_or("?".into());
                        let time_left = market.time_remaining_mins().map(|t| format!("{:.0}m", t)).unwrap_or("?".into());
                        let mode_str = if dry_run { "[DRY]" } else { "[LIVE]" };
                        info!("[poly_momentum] {} Signal: {} {:?} {}bps | strike={} exp={} | {}={}¢ | max=${:.0}",
                              mode_str, signal.asset, signal.direction, signal.move_bps,
                              strike_str, time_left, buy_side, ask, max_dollars);

                        let market_id_clone = market_id.clone();
                        let buy_token_clone = buy_token.clone();
                        let asset_clone = signal.asset.clone();

                        // Cross spread by +2¢, cap at 99¢
                        let entry_price = (ask + 2).min(99);
                        let cross_price = entry_price as f64 / 100.0;

                        // Polymarket requires minimum $1 order value for marketable orders
                        if remaining_dollars < 1.0 {
                            info!("[poly_momentum] ⏭️ Skip: only ${:.2} remaining, need $1 min order",
                                  remaining_dollars);
                            continue;
                        }

                        // Calculate contracts: use remaining dollars, but at least $1 worth
                        let dollars_to_spend = remaining_dollars.max(1.0);
                        let max_contracts = (dollars_to_spend / cross_price).floor();
                        let min_contracts = (1.0 / cross_price).ceil();
                        let actual_contracts = max_contracts.max(min_contracts);
                        let cost = actual_contracts * cross_price;

                        if dry_run {
                            market.last_trade_time = Some(Instant::now());
                            info!("[poly_momentum] [DRY] 🎯 Would BUY {:.0} {} {} @{}¢ (${:.2}) | {}bps",
                                  actual_contracts, asset_clone, buy_side, entry_price, cost, signal.move_bps);
                        } else {
                            info!("[poly_momentum] [LIVE] 🎯 BUY {:.0} {} {} @{}¢ (${:.2}) | {}bps",
                                  actual_contracts, signal.asset, buy_side, entry_price, cost, signal.move_bps);

                            let client = shared_client.clone();
                            let state_clone = state.clone();
                            let wallet_for_error = wallet_address;
                            let cost_for_error = cost;

                            // Execute trade asynchronously
                            tokio::spawn(async move {
                                match client.buy_fak(&buy_token_clone, cross_price, actual_contracts).await {
                                    Ok(fill) if fill.filled_size > 0.0 => {
                                        // Calculate actual entry price from fill
                                        let actual_entry = (fill.fill_cost / fill.filled_size * 100.0).round() as i64;
                                        info!("[poly_momentum] ✅ BUY Filled {:.1} @{}¢ (${:.2})",
                                              fill.filled_size, actual_entry, fill.fill_cost);

                                        let mut s = state_clone.write().await;
                                        if let Some(m) = s.markets.get_mut(&market_id_clone) {
                                            m.last_trade_time = Some(Instant::now());
                                        }
                                        // Track position with ACTUAL fill price, not expected
                                        s.add_position(asset_clone, buy_token_clone, buy_side, actual_entry, fill.filled_size);
                                    }
                                    Ok(_) => {
                                        // No fill - FAK killed, already logged in clob
                                    }
                                    Err(e) => {
                                        let balance = get_usdc_balance(wallet_for_error).await.unwrap_or(0.0);
                                        error!("[poly_momentum] ❌ BUY error: {} | balance=${:.2} wanted=${:.2}", e, balance, cost_for_error);
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
                            let text_preview = &text[..text.len().min(150)];

                            // Try parsing as full orderbook snapshot (array or single)
                            let books: Option<Vec<BookSnapshot>> = serde_json::from_str::<Vec<BookSnapshot>>(&text).ok()
                                .or_else(|| serde_json::from_str::<BookSnapshot>(&text).ok().map(|b| vec![b]));

                            if let Some(books) = books {
                                tracing::debug!("[ws] BookSnapshot: {} books", books.len());
                                let mut s = state.write().await;

                                for book in books {
                                    // Find market info immutably first
                                    let market_info = s.markets.values()
                                        .find(|m| m.yes_token == book.asset_id || m.no_token == book.asset_id)
                                        .map(|m| (m.asset.clone(), m.yes_token.clone(), m.strike_price, m.question.clone()));

                                    let Some((asset, yes_token, strike_price, question)) = market_info else { continue };

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

                                    let is_yes = book.asset_id == yes_token;

                                    // Now get mutable reference to update
                                    let market = s.markets.values_mut()
                                        .find(|m| m.yes_token == book.asset_id || m.no_token == book.asset_id);

                                    if let Some(market) = market {
                                        if is_yes {
                                            market.yes_ask = best_ask;
                                            market.yes_bid = best_bid;
                                        } else {
                                            market.no_ask = best_ask;
                                            market.no_bid = best_bid;
                                        }
                                    }
                                }
                            }
                            // If we couldn't parse as BookSnapshot, log what we got
                            else {
                                // Log first few chars to help debug
                                if text.len() > 5 && !text.starts_with("[{\"event") {
                                    tracing::debug!("[ws] Unknown msg: {}", text_preview);
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
