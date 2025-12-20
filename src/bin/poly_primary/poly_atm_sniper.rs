//! Polymarket ATM Sniper - Delta 0.50 Strategy
//!
//! STRATEGY:
//! - Only trade when spot price is within 0.0015% of strike (delta ≈ 0.50)
//! - When ATM, bid 45¢ or less on both YES and NO
//! - If both fill: pay 90¢ or less, receive $1 = guaranteed profit
//! - If one fills: hold with ~50% win probability (fair value)
//!
//! This targets the "sweet spot" where binary options are at-the-money,
//! meaning both YES and NO have approximately equal fair value (50¢).
//!
//! Usage:
//!   cargo run --release --bin poly_atm_sniper
//!
//! Environment:
//!   POLY_PRIVATE_KEY - Your Polymarket/Polygon wallet private key
//!   POLY_FUNDER - Your funder address (proxy wallet)
//!   POLYGON_API_KEY - Polygon.io API key for price feed

use anyhow::{Context, Result};
use chrono::{TimeZone, Utc};
use clap::Parser;
use futures_util::{SinkExt, StreamExt};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::RwLock;
use tokio_tungstenite::{connect_async, tungstenite::Message};
use tracing::{debug, error, info, warn};

use arb_bot::polymarket_clob::{
    PolymarketAsyncClient, SharedAsyncClient, PreparedCreds,
};
use arb_bot::polymarket_markets::{discover_all_markets, PolyMarket};

/// Polymarket ATM Sniper - Delta 0.50 Strategy
#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Args {
    /// Size per trade in dollars
    #[arg(short, long, default_value_t = 20.0)]
    size: f64,

    /// Max bid price in cents (bid at this or lower when ATM)
    #[arg(short, long, default_value_t = 45)]
    bid: i64,

    /// Number of contracts per trade
    #[arg(short, long, default_value_t = 1.0)]
    contracts: f64,

    /// ATM threshold: max % distance from strike to be considered ATM (default: 0.0015%)
    /// Price must be within this percentage of strike for delta ≈ 0.50
    #[arg(long, default_value_t = 0.0015)]
    atm_threshold: f64,

    /// Live trading mode (default is dry run)
    #[arg(short, long, default_value_t = false)]
    live: bool,

    /// Specific market slug to monitor (optional, monitors all crypto if not set)
    #[arg(long)]
    market: Option<String>,

    /// Asset symbol filter: btc, eth, sol, xrp (optional, monitors all if not set)
    #[arg(long)]
    sym: Option<String>,

    /// Maximum total contracts to hold across all positions
    #[arg(long, default_value_t = 10.0)]
    max_contracts: f64,

    /// Minimum minutes remaining to trade (default: 2)
    #[arg(long, default_value_t = 2)]
    min_minutes: i64,

    /// Maximum minutes remaining to trade (default: 15)
    #[arg(long, default_value_t = 15)]
    max_minutes: i64,

    /// Connect directly to Polygon.io instead of local price server
    #[arg(short, long, default_value_t = false)]
    direct: bool,
}

const POLYMARKET_WS_URL: &str = "wss://ws-subscriptions-clob.polymarket.com/ws/market";
const POLYGON_WS_URL: &str = "wss://socket.polygon.io/crypto";
const LOCAL_PRICE_SERVER: &str = "ws://127.0.0.1:9999";

/// Market state
#[derive(Debug, Clone)]
struct Market {
    condition_id: String,
    question: String,
    yes_token: String,
    no_token: String,
    asset: String, // "BTC", "ETH", "SOL", "XRP"
    expiry_minutes: Option<f64>,
    discovered_at: std::time::Instant,
    /// Unix timestamp when the 15-minute window starts (from slug)
    window_start_ts: Option<i64>,
    /// Strike price - captured from price feed when window starts
    strike_price: Option<f64>,
    // Orderbook
    yes_ask: Option<i64>,
    yes_bid: Option<i64>,
    no_ask: Option<i64>,
    no_bid: Option<i64>,
    yes_ask_size: f64,
    no_ask_size: f64,
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
            discovered_at: std::time::Instant::now(),
            window_start_ts: pm.window_start_ts,
            strike_price: None, // Will be captured from price feed
            yes_ask: None,
            yes_bid: None,
            no_ask: None,
            no_bid: None,
            yes_ask_size: 0.0,
            no_ask_size: 0.0,
        }
    }

    fn time_remaining_mins(&self) -> Option<f64> {
        self.expiry_minutes.map(|exp| {
            let elapsed_mins = self.discovered_at.elapsed().as_secs_f64() / 60.0;
            (exp - elapsed_mins).max(0.0)
        })
    }
}

/// Position tracking
#[derive(Debug, Clone, Default)]
struct Position {
    yes_qty: f64,
    no_qty: f64,
    yes_cost: f64,
    no_cost: f64,
}

#[allow(dead_code)]
impl Position {
    fn matched(&self) -> f64 {
        self.yes_qty.min(self.no_qty)
    }
    fn profit(&self) -> f64 {
        // Locked profit from matched pairs (payout $1 each)
        self.matched() - self.yes_cost - self.no_cost
    }
}

/// Our resting orders
#[derive(Debug, Clone, Default)]
#[allow(dead_code)]
struct Orders {
    yes_order_id: Option<String>,
    yes_price: Option<i64>,
    no_order_id: Option<String>,
    no_price: Option<i64>,
}

/// Price feed state
#[derive(Debug, Default)]
struct PriceState {
    btc_price: Option<f64>,
    eth_price: Option<f64>,
    sol_price: Option<f64>,
    xrp_price: Option<f64>,
    last_update: Option<std::time::Instant>,
}

/// Global state
struct State {
    markets: HashMap<String, Market>,
    positions: HashMap<String, Position>,
    orders: HashMap<String, Orders>,
    prices: PriceState,
    /// Flag to signal WebSocket needs to resubscribe with new tokens
    needs_resubscribe: bool,
}

impl State {
    fn new() -> Self {
        Self {
            markets: HashMap::new(),
            positions: HashMap::new(),
            orders: HashMap::new(),
            prices: PriceState::default(),
            needs_resubscribe: false,
        }
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

fn parse_size(s: &str) -> f64 {
    s.parse::<f64>().unwrap_or(0.0)
}

// === Price Feed (local server or direct Polygon) ===

#[derive(Deserialize, Debug)]
struct PolygonMessage {
    ev: Option<String>,
    pair: Option<String>,
    p: Option<f64>,
}

#[derive(Deserialize, Debug)]
struct LocalPriceUpdate {
    btc_price: Option<f64>,
    eth_price: Option<f64>,
    sol_price: Option<f64>,
    xrp_price: Option<f64>,
}

/// Try local price server first, fallback to direct Polygon
async fn run_polygon_feed(state: Arc<RwLock<State>>, api_key: &str) {
    loop {
        // Try local price server first
        info!("[PRICES] Trying local price server {}...", LOCAL_PRICE_SERVER);
        match connect_async(LOCAL_PRICE_SERVER).await {
            Ok((ws, _)) => {
                info!("[PRICES] ✅ Connected to local price server");
                if run_local_price_feed(state.clone(), ws).await.is_err() {
                    warn!("[PRICES] Local server disconnected");
                }
            }
            Err(_) => {
                info!("[PRICES] Local server not available, connecting to Polygon directly...");
                if let Err(e) = run_direct_polygon_feed(state.clone(), api_key).await {
                    error!("[POLYGON] Connection error: {}", e);
                }
            }
        }

        warn!("[PRICES] Reconnecting in 3s...");
        tokio::time::sleep(Duration::from_secs(3)).await;
    }
}

/// Connect to local price_feed server
async fn run_local_price_feed(
    state: Arc<RwLock<State>>,
    ws: tokio_tungstenite::WebSocketStream<tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>>,
) -> Result<()> {
    let (mut write, mut read) = ws.split();

    while let Some(msg) = read.next().await {
        match msg {
            Ok(Message::Text(text)) => {
                if let Ok(update) = serde_json::from_str::<LocalPriceUpdate>(&text) {
                    let mut s = state.write().await;

                    // Current time for strike capture
                    let now_ts = std::time::SystemTime::now()
                        .duration_since(std::time::UNIX_EPOCH)
                        .map(|d| d.as_secs() as i64)
                        .unwrap_or(0);

                    // Process each price and capture strikes
                    let prices: Vec<(&str, Option<f64>)> = vec![
                        ("BTC", update.btc_price),
                        ("ETH", update.eth_price),
                        ("SOL", update.sol_price),
                        ("XRP", update.xrp_price),
                    ];

                    for (asset, price_opt) in prices {
                        let Some(price) = price_opt else { continue };

                        // Update price state
                        match asset {
                            "BTC" => s.prices.btc_price = Some(price),
                            "ETH" => s.prices.eth_price = Some(price),
                            "SOL" => s.prices.sol_price = Some(price),
                            "XRP" => s.prices.xrp_price = Some(price),
                            _ => {}
                        }

                        // Capture strike price for markets where window has started
                        for market in s.markets.values_mut() {
                            if market.asset == asset && market.strike_price.is_none() {
                                if let Some(start_ts) = market.window_start_ts {
                                    if now_ts >= start_ts {
                                        market.strike_price = Some(price);
                                        info!("[STRIKE] {} captured: ${:.2}", asset, price);
                                    }
                                }
                            }
                        }
                    }

                    s.prices.last_update = Some(std::time::Instant::now());
                }
            }
            Ok(Message::Ping(data)) => {
                let _ = write.send(Message::Pong(data)).await;
            }
            Ok(Message::Close(_)) | Err(_) => break,
            _ => {}
        }
    }
    Ok(())
}

/// Connect directly to Polygon.io
async fn run_direct_polygon_feed(state: Arc<RwLock<State>>, api_key: &str) -> Result<()> {
    let url = format!("{}?apiKey={}", POLYGON_WS_URL, api_key);
    let (ws, _) = connect_async(&url).await?;
    let (mut write, mut read) = ws.split();

    // Subscribe to all crypto pairs
    let sub = serde_json::json!({
        "action": "subscribe",
        "params": "XT.BTC-USD,XT.ETH-USD,XT.SOL-USD,XT.XRP-USD"
    });
    let _ = write.send(Message::Text(sub.to_string())).await;
    info!("[POLYGON] Subscribed to BTC-USD, ETH-USD, SOL-USD, XRP-USD");

    while let Some(msg) = read.next().await {
        match msg {
            Ok(Message::Text(text)) => {
                if let Ok(messages) = serde_json::from_str::<Vec<PolygonMessage>>(&text) {
                    for m in messages {
                        if m.ev.as_deref() != Some("XT") {
                            continue;
                        }

                        let Some(pair) = m.pair.as_ref() else { continue };
                        let Some(price) = m.p else { continue };

                        let asset = match pair.as_str() {
                            "BTC-USD" => "BTC",
                            "ETH-USD" => "ETH",
                            "SOL-USD" => "SOL",
                            "XRP-USD" => "XRP",
                            _ => continue,
                        };

                        let mut s = state.write().await;

                        // Update price state
                        match asset {
                            "BTC" => s.prices.btc_price = Some(price),
                            "ETH" => s.prices.eth_price = Some(price),
                            "SOL" => s.prices.sol_price = Some(price),
                            "XRP" => s.prices.xrp_price = Some(price),
                            _ => {}
                        }

                        // Current time for strike capture
                        let now_ts = std::time::SystemTime::now()
                            .duration_since(std::time::UNIX_EPOCH)
                            .map(|d| d.as_secs() as i64)
                            .unwrap_or(0);

                        // Capture strike price for markets where window has started
                        for market in s.markets.values_mut() {
                            if market.asset == asset && market.strike_price.is_none() {
                                if let Some(start_ts) = market.window_start_ts {
                                    if now_ts >= start_ts {
                                        market.strike_price = Some(price);
                                        info!("[STRIKE] {} captured: ${:.2}", asset, price);
                                    }
                                }
                            }
                        }

                        s.prices.last_update = Some(std::time::Instant::now());
                    }
                }
            }
            Ok(Message::Ping(data)) => {
                let _ = write.send(Message::Pong(data)).await;
            }
            Err(e) => {
                error!("[POLYGON] WebSocket error: {}", e);
                break;
            }
            _ => {}
        }
    }
    Ok(())
}

/// Calculate the distance from strike as a percentage
fn distance_from_strike_pct(spot: f64, strike: f64) -> f64 {
    ((spot - strike) / strike).abs() * 100.0
}

// === Main ===

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::from_default_env()
                .add_directive("poly_atm_sniper=info".parse().unwrap())
                .add_directive("arb_bot=info".parse().unwrap()),
        )
        .init();

    let args = Args::parse();

    info!("═══════════════════════════════════════════════════════════════════════");
    info!("🎯 POLYMARKET ATM SNIPER - Delta 0.50 Strategy");
    info!("═══════════════════════════════════════════════════════════════════════");
    info!("STRATEGY:");
    info!("   1. Wait for spot to be within {:.4}% of strike (delta ≈ 0.50)", args.atm_threshold);
    info!("   2. When ATM, bid {}¢ on both YES and NO", args.bid);
    info!("   3. If both fill: pay {}¢, receive $1 = {}¢ profit", args.bid * 2, 100 - args.bid * 2);
    info!("═══════════════════════════════════════════════════════════════════════");
    info!("CONFIG:");
    info!("   Mode: {}", if args.live { "🚀 LIVE" } else { "🔍 DRY RUN" });
    info!("   Price Feed: {}", if args.direct { "Direct Polygon.io" } else { "Local server (ws://127.0.0.1:9999)" });
    info!("   Contracts: {} per trade", args.contracts);
    info!("   Max bid: {}¢", args.bid);
    info!("   ATM threshold: {:.4}%", args.atm_threshold);
    info!("   Time window: {}m - {}m before expiry", args.min_minutes, args.max_minutes);
    if let Some(ref market) = args.market {
        info!("   Market filter: {}", market);
    }
    if let Some(ref sym) = args.sym {
        info!("   Asset filter: {}", sym.to_uppercase());
    }
    info!("   Max contracts: {}", args.max_contracts);
    info!("═══════════════════════════════════════════════════════════════════════");

    // Load Polymarket credentials
    dotenvy::dotenv().ok();
    let private_key = std::env::var("POLY_PRIVATE_KEY")
        .context("POLY_PRIVATE_KEY not set")?;
    let funder = std::env::var("POLY_FUNDER")
        .context("POLY_FUNDER not set")?;
    let polygon_api_key = std::env::var("POLYGON_API_KEY")
        .unwrap_or_else(|_| "o2Jm26X52_0tRq2W7V5JbsCUXdMjL7qk".to_string());

    // Initialize Polymarket client
    let poly_client = PolymarketAsyncClient::new(
        "https://clob.polymarket.com",
        137,
        &private_key,
        &funder,
    )?;

    // Derive API credentials
    info!("[POLY] Deriving API credentials...");
    let api_creds = poly_client.derive_api_key(0).await?;
    let prepared_creds = PreparedCreds::from_api_creds(&api_creds)?;
    let shared_client = Arc::new(SharedAsyncClient::new(poly_client, prepared_creds, 137));
    info!("[POLY] API credentials ready");

    if args.live {
        warn!("⚠️  LIVE MODE - Real money!");
        tokio::time::sleep(Duration::from_secs(3)).await;
    }

    // Discover markets using shared module
    info!("[DISCOVER] Searching for crypto markets...");
    let mut discovered = discover_all_markets(args.market.as_deref()).await?;

    // Filter by asset symbol if specified
    if let Some(ref sym) = args.sym {
        let sym_upper = sym.to_uppercase();
        discovered.retain(|m| m.asset == sym_upper);
        info!("[DISCOVER] Filtered to {} {} markets", discovered.len(), sym_upper);
    } else {
        info!("[DISCOVER] Found {} markets", discovered.len());
    }

    for m in &discovered {
        let start_info = m.window_start_ts
            .map(|ts| format!("start_ts={}", ts))
            .unwrap_or_else(|| "no_start".into());
        info!("  • {} | {} | Asset: {} | Expiry: {:.1?}min",
              &m.question[..m.question.len().min(50)],
              start_info, m.asset, m.expiry_minutes);
    }

    if discovered.is_empty() {
        warn!("No markets found! Try different search terms.");
        return Ok(());
    }

    // Initialize state - convert PolyMarket to Market
    let state = Arc::new(RwLock::new({
        let mut s = State::new();
        for pm in discovered {
            let id = pm.condition_id.clone();
            s.positions.insert(id.clone(), Position::default());
            s.orders.insert(id.clone(), Orders::default());
            s.markets.insert(id, Market::from_polymarket(pm));
        }
        s
    }));

    // Start price feed
    let state_clone = state.clone();
    let polygon_key = polygon_api_key.clone();
    let use_direct = args.direct;
    if use_direct {
        tokio::spawn(async move {
            loop {
                if let Err(e) = run_direct_polygon_feed(state_clone.clone(), &polygon_key).await {
                    tracing::error!("[POLYGON] Connection error: {}", e);
                }
                tracing::warn!("[POLYGON] Reconnecting in 3s...");
                tokio::time::sleep(Duration::from_secs(3)).await;
            }
        });
    } else {
        tokio::spawn(async move {
            run_polygon_feed(state_clone, &polygon_key).await;
        });
    }

    // Start periodic market discovery task
    let state_for_discovery = state.clone();
    let discovery_filter = args.sym.clone();
    let market_filter = args.market.clone();
    tokio::spawn(async move {
        // Wait 15 seconds before first refresh (we just discovered markets)
        tokio::time::sleep(Duration::from_secs(15)).await;

        loop {
            debug!("[DISCOVER] Checking for new markets...");
            match discover_all_markets(market_filter.as_deref()).await {
                Ok(mut discovered) => {
                    // Filter by asset symbol if specified
                    if let Some(ref sym) = discovery_filter {
                        let sym_upper = sym.to_uppercase();
                        discovered.retain(|m| m.asset == sym_upper);
                    }

                    let mut s = state_for_discovery.write().await;
                    let mut new_count = 0;
                    let mut expired_count = 0;

                    // Remove expired markets (expiry <= 0)
                    let expired_ids: Vec<String> = s.markets.iter()
                        .filter(|(_, m)| m.time_remaining_mins().unwrap_or(0.0) <= 0.0)
                        .map(|(id, _)| id.clone())
                        .collect();

                    for id in expired_ids {
                        s.markets.remove(&id);
                        s.positions.remove(&id);
                        s.orders.remove(&id);
                        expired_count += 1;
                    }

                    // Add new markets
                    for pm in discovered {
                        let id = pm.condition_id.clone();
                        if !s.markets.contains_key(&id) {
                            info!("[DISCOVER] 🆕 New market: {} | {} | {:.1?}min",
                                  pm.asset, &pm.question[..pm.question.len().min(40)], pm.expiry_minutes);
                            s.positions.insert(id.clone(), Position::default());
                            s.orders.insert(id.clone(), Orders::default());
                            s.markets.insert(id, Market::from_polymarket(pm));
                            new_count += 1;
                        }
                    }

                    if new_count > 0 || expired_count > 0 {
                        info!("[DISCOVER] Added {} new markets, removed {} expired", new_count, expired_count);
                        if new_count > 0 {
                            s.needs_resubscribe = true;
                        }
                    }
                }
                Err(e) => {
                    warn!("[DISCOVER] Market discovery failed: {}", e);
                }
            }

            // Check every 15 seconds for new markets
            tokio::time::sleep(Duration::from_secs(15)).await;
        }
    });

    // Main WebSocket loop
    let bid_price = args.bid;
    let contracts = args.contracts;
    let atm_threshold = args.atm_threshold;
    let dry_run = !args.live;
    let min_minutes = args.min_minutes as f64;
    let max_minutes = args.max_minutes as f64;
    let max_contracts = args.max_contracts;

    loop {
        // Get current token IDs from state (may have new markets)
        let tokens: Vec<String> = {
            let mut s = state.write().await;
            s.needs_resubscribe = false; // Clear flag
            s.markets.values()
                .flat_map(|m| vec![m.yes_token.clone(), m.no_token.clone()])
                .collect()
        };

        if tokens.is_empty() {
            info!("[WS] No markets to subscribe to, waiting...");
            tokio::time::sleep(Duration::from_secs(5)).await;
            continue;
        }

        info!("[WS] Connecting to Polymarket...");

        let (ws, _) = match connect_async(POLYMARKET_WS_URL).await {
            Ok(ws) => ws,
            Err(e) => {
                error!("[WS] Connect failed: {}", e);
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
        info!("[WS] Subscribed to {} tokens ({} markets)", tokens.len(), tokens.len() / 2);

        let mut ping_interval = tokio::time::interval(Duration::from_secs(30));
        let mut status_interval = tokio::time::interval(Duration::from_secs(5));
        let mut resub_check_interval = tokio::time::interval(Duration::from_secs(2));

        loop {
            tokio::select! {
                _ = resub_check_interval.tick() => {
                    // Check if we need to resubscribe for new markets
                    let needs_resub = {
                        let s = state.read().await;
                        s.needs_resubscribe
                    };
                    if needs_resub {
                        info!("[WS] New markets discovered, reconnecting to subscribe...");
                        break;
                    }
                }

                _ = ping_interval.tick() => {
                    if let Err(e) = write.send(Message::Ping(vec![])).await {
                        error!("[WS] Ping failed: {}", e);
                        break;
                    }
                }

                _ = status_interval.tick() => {
                    let s = state.read().await;

                    // Build price string based on which assets we're trading
                    let price_str = if let Some(ref sym) = args.sym {
                        match sym.to_uppercase().as_str() {
                            "BTC" => format!("BTC=${}", s.prices.btc_price.map(|p| format!("{:.2}", p)).unwrap_or("-".into())),
                            "ETH" => format!("ETH=${}", s.prices.eth_price.map(|p| format!("{:.2}", p)).unwrap_or("-".into())),
                            "SOL" => format!("SOL=${}", s.prices.sol_price.map(|p| format!("{:.2}", p)).unwrap_or("-".into())),
                            "XRP" => format!("XRP=${}", s.prices.xrp_price.map(|p| format!("{:.4}", p)).unwrap_or("-".into())),
                            _ => "?".into(),
                        }
                    } else {
                        format!("BTC=${} ETH={}",
                            s.prices.btc_price.map(|p| format!("{:.2}", p)).unwrap_or("-".into()),
                            s.prices.eth_price.map(|p| format!("{:.2}", p)).unwrap_or("-".into()))
                    };

                    // Calculate total position
                    let total_yes: f64 = s.positions.values().map(|p| p.yes_qty).sum();
                    let total_no: f64 = s.positions.values().map(|p| p.no_qty).sum();
                    let total_cost: f64 = s.positions.values().map(|p| p.yes_cost + p.no_cost).sum();
                    let matched = total_yes.min(total_no);
                    let locked_profit = matched - total_cost; // $1 payout per matched pair minus cost

                    info!("[STATUS] {} | Pos: Y={:.1} N={:.1} matched={:.1} | Cost=${:.2} Profit=${:.2} | {:.0}/{:.0}",
                          price_str, total_yes, total_no, matched, total_cost, locked_profit, total_yes + total_no, max_contracts);

                    for (id, market) in &s.markets {
                        let pos = s.positions.get(id).cloned().unwrap_or_default();
                        let orders = s.orders.get(id).cloned().unwrap_or_default();

                        // Get spot price for this asset
                        let spot = match market.asset.as_str() {
                            "BTC" => s.prices.btc_price,
                            "ETH" => s.prices.eth_price,
                            "SOL" => s.prices.sol_price,
                            "XRP" => s.prices.xrp_price,
                            _ => None,
                        };

                        let expiry = market.time_remaining_mins().unwrap_or(0.0);
                        if expiry < min_minutes || expiry > max_minutes {
                            continue;
                        }

                        // Check ATM status using captured strike price
                        let (atm_status, dist_pct) = match (spot, market.strike_price) {
                            (Some(s), Some(k)) => {
                                let dist = distance_from_strike_pct(s, k);
                                let is_atm_now = dist <= atm_threshold;
                                let status = if is_atm_now {
                                    "✅ ATM"
                                } else {
                                    "⏳ OTM"
                                };
                                (status, format!("{:.4}%", dist))
                            }
                            (_, None) => ("⏳ WAIT", "-".into()), // Waiting for strike to be captured
                            _ => ("❓ NO PRICE", "-".into()),
                        };

                        let yes_ask = market.yes_ask.unwrap_or(100);
                        let no_ask = market.no_ask.unwrap_or(100);

                        let order_status = format!(
                            "Y:{} N:{}",
                            orders.yes_order_id.as_ref().map(|_| "📝").unwrap_or("-"),
                            orders.no_order_id.as_ref().map(|_| "📝").unwrap_or("-")
                        );

                        let strike_str = market.strike_price
                            .map(|s| format!("${:.0}", s))
                            .unwrap_or_else(|| "?".into());

                        // Calculate end time (window_start + 15 minutes)
                        let end_time_str = market.window_start_ts
                            .map(|start_ts| {
                                let end_ts = start_ts + 15 * 60; // 15 minutes after start
                                Utc.timestamp_opt(end_ts, 0)
                                    .single()
                                    .map(|dt| dt.format("%H:%M:%S").to_string())
                                    .unwrap_or_else(|| "?".into())
                            })
                            .unwrap_or_else(|| "?".into());

                        info!("  [{}] {} | {:.1}m ends@{} | strike={} | {} dist={} | Y={}¢ N={}¢ | {} | Pos: Y={:.1} N={:.1}",
                              market.asset,
                              &market.question[..market.question.len().min(35)],
                              expiry,
                              end_time_str,
                              strike_str,
                              atm_status, dist_pct,
                              yes_ask, no_ask,
                              order_status,
                              pos.yes_qty, pos.no_qty);
                    }
                }

                msg = read.next() => {
                    let Some(msg) = msg else { break; };

                    match msg {
                        Ok(Message::Text(text)) => {
                            // Try to parse as book snapshot array
                            if let Ok(books) = serde_json::from_str::<Vec<BookSnapshot>>(&text) {
                                for book in books {
                                    let mut s = state.write().await;

                                    // Find which market this token belongs to
                                    let market_id = s.markets.iter()
                                        .find(|(_, m)| m.yes_token == book.asset_id || m.no_token == book.asset_id)
                                        .map(|(id, _)| id.clone());

                                    let Some(market_id) = market_id else { continue };

                                    // Find best ask and bid from orderbook
                                    let best_ask = book.asks.iter()
                                        .filter_map(|l| {
                                            let price = parse_price_cents(&l.price);
                                            if price > 0 { Some((price, parse_size(&l.size))) } else { None }
                                        })
                                        .min_by_key(|(p, _)| *p);

                                    let best_bid = book.bids.iter()
                                        .filter_map(|l| {
                                            let price = parse_price_cents(&l.price);
                                            if price > 0 { Some((price, parse_size(&l.size))) } else { None }
                                        })
                                        .max_by_key(|(p, _)| *p);

                                    // Get prices and market data first
                                    let btc_price = s.prices.btc_price;
                                    let eth_price = s.prices.eth_price;
                                    let sol_price = s.prices.sol_price;
                                    let xrp_price = s.prices.xrp_price;

                                    let Some(market) = s.markets.get_mut(&market_id) else { continue };

                                    let is_yes = book.asset_id == market.yes_token;

                                    if is_yes {
                                        market.yes_ask = best_ask.map(|(p, _)| p);
                                        market.yes_bid = best_bid.map(|(p, _)| p);
                                        market.yes_ask_size = best_ask.map(|(_, s)| s).unwrap_or(0.0);
                                    } else {
                                        market.no_ask = best_ask.map(|(p, _)| p);
                                        market.no_bid = best_bid.map(|(p, _)| p);
                                        market.no_ask_size = best_ask.map(|(_, s)| s).unwrap_or(0.0);
                                    }

                                    // Extract values for trading logic
                                    let asset = market.asset.clone();
                                    let strike = market.strike_price;
                                    let expiry = market.time_remaining_mins();
                                    let yes_ask_price = market.yes_ask;
                                    let no_ask_price = market.no_ask;
                                    let yes_token = market.yes_token.clone();
                                    let no_token = market.no_token.clone();
                                    let question = market.question.clone();

                                    // Get current orders
                                    let orders = s.orders.get(&market_id).cloned().unwrap_or_default();

                                    // Get spot price based on asset
                                    let spot = match asset.as_str() {
                                        "BTC" => btc_price,
                                        "ETH" => eth_price,
                                        "SOL" => sol_price,
                                        "XRP" => xrp_price,
                                        _ => None,
                                    };

                                    // Check if within time window
                                    let mins = expiry.unwrap_or(0.0);
                                    if mins < min_minutes || mins > max_minutes {
                                        continue;
                                    }

                                    // Check ATM condition: spot within threshold of strike
                                    let Some(spot_price) = spot else { continue };
                                    let Some(strike_price) = strike else { continue };

                                    let dist_pct = distance_from_strike_pct(spot_price, strike_price);
                                    let is_atm_now = dist_pct <= atm_threshold;

                                    if !is_atm_now {
                                        // Not ATM - don't trade
                                        debug!("[SKIP] {} not ATM: dist={:.4}% > {:.4}%",
                                               asset, dist_pct, atm_threshold);
                                        continue;
                                    }

                                    // ATM! Check if we should bid
                                    let market_id_clone = market_id.clone();
                                    drop(s);

                                    // Check if we've hit max contracts limit
                                    let total_position = {
                                        let s = state.read().await;
                                        s.positions.values()
                                            .map(|p| p.yes_qty + p.no_qty)
                                            .sum::<f64>()
                                    };

                                    if total_position >= max_contracts {
                                        debug!("[SKIP] Max contracts reached: {:.1} >= {:.1}", total_position, max_contracts);
                                        continue;
                                    }

                                    // Determine if we need to place orders
                                    let need_yes = orders.yes_order_id.is_none();
                                    let need_no = orders.no_order_id.is_none();

                                    if !need_yes && !need_no {
                                        continue;
                                    }

                                    // Only bid if ask is above our bid (otherwise no edge)
                                    let yes_worth_bidding = yes_ask_price.map(|a| a > bid_price).unwrap_or(true);
                                    let no_worth_bidding = no_ask_price.map(|a| a > bid_price).unwrap_or(true);

                                    let spot_str = format!("{:.2}", spot_price);
                                    let strike_str = format!("{:.0}", strike_price);

                                    info!("════════════════════════════════════════════════════════════════");
                                    info!("[ATM] 🎯 {} is AT-THE-MONEY! Spot=${} Strike=${} dist={:.4}%",
                                          asset, spot_str, strike_str, dist_pct);
                                    info!("[ATM] Market: {}", &question[..question.len().min(60)]);
                                    info!("[ATM] {:.1}m remaining | Ask Y={}¢ N={}¢",
                                          mins, yes_ask_price.unwrap_or(0), no_ask_price.unwrap_or(0));
                                    info!("════════════════════════════════════════════════════════════════");

                                    if dry_run {
                                        if need_yes && yes_worth_bidding {
                                            info!("[DRY] Would BID {} contracts YES @{}¢ | delta≈0.50 | {}",
                                                  contracts, bid_price, asset);
                                        }
                                        if need_no && no_worth_bidding {
                                            info!("[DRY] Would BID {} contracts NO @{}¢ | delta≈0.50 | {}",
                                                  contracts, bid_price, asset);
                                        }
                                    } else {
                                        // Place YES bid
                                        if need_yes && yes_worth_bidding {
                                            let price = bid_price as f64 / 100.0;
                                            let current_ask = yes_ask_price.unwrap_or(0);

                                            // Polymarket requires minimum $1 order value - scale up contracts if needed
                                            let min_contracts = (1.0 / price).ceil();
                                            let actual_contracts = contracts.max(min_contracts);
                                            let cost = actual_contracts * price;

                                            // Skip if still below $1 minimum
                                            if cost < 1.0 {
                                                debug!("[SKIP] YES order ${:.2} below $1 minimum", cost);
                                                continue;
                                            }

                                            warn!("[TRADE] 📝 BID {} contracts YES @{}¢ | ask={}¢ | delta≈0.50 | {}",
                                                  actual_contracts, bid_price, current_ask, asset);

                                            match shared_client.buy_fak(&yes_token, price, actual_contracts).await {
                                                Ok(fill) => {
                                                    if fill.filled_size > 0.0 {
                                                        let fill_price_cents = (fill.fill_cost / fill.filled_size * 100.0).round() as i64;
                                                        warn!("[TRADE] ✅ YES Filled {:.2} @{}¢ (total ${:.2}) | ask was {}¢ | order_id={}",
                                                              fill.filled_size, fill_price_cents, fill.fill_cost, current_ask, fill.order_id);
                                                        // Update position
                                                        let mut s = state.write().await;
                                                        if let Some(pos) = s.positions.get_mut(&market_id_clone) {
                                                            pos.yes_qty += fill.filled_size;
                                                            pos.yes_cost += fill.fill_cost;
                                                        }
                                                    } else {
                                                        info!("[TRADE] ⏳ YES order placed (no immediate fill)");
                                                    }
                                                }
                                                Err(e) => error!("[TRADE] ❌ YES bid failed: {}", e),
                                            }
                                        }

                                        // Place NO bid
                                        if need_no && no_worth_bidding {
                                            let price = bid_price as f64 / 100.0;
                                            let current_ask = no_ask_price.unwrap_or(0);

                                            // Polymarket requires minimum $1 order value - scale up contracts if needed
                                            let min_contracts = (1.0 / price).ceil();
                                            let actual_contracts = contracts.max(min_contracts);
                                            let cost = actual_contracts * price;

                                            // Skip if still below $1 minimum
                                            if cost < 1.0 {
                                                debug!("[SKIP] NO order ${:.2} below $1 minimum", cost);
                                                continue;
                                            }

                                            warn!("[TRADE] 📝 BID {} contracts NO @{}¢ | ask={}¢ | delta≈0.50 | {}",
                                                  actual_contracts, bid_price, current_ask, asset);

                                            match shared_client.buy_fak(&no_token, price, actual_contracts).await {
                                                Ok(fill) => {
                                                    if fill.filled_size > 0.0 {
                                                        let fill_price_cents = (fill.fill_cost / fill.filled_size * 100.0).round() as i64;
                                                        warn!("[TRADE] ✅ NO Filled {:.2} @{}¢ (total ${:.2}) | ask was {}¢ | order_id={}",
                                                              fill.filled_size, fill_price_cents, fill.fill_cost, current_ask, fill.order_id);
                                                        // Update position
                                                        let mut s = state.write().await;
                                                        if let Some(pos) = s.positions.get_mut(&market_id_clone) {
                                                            pos.no_qty += fill.filled_size;
                                                            pos.no_cost += fill.fill_cost;
                                                        }
                                                    } else {
                                                        info!("[TRADE] ⏳ NO order placed (no immediate fill)");
                                                    }
                                                }
                                                Err(e) => error!("[TRADE] ❌ NO bid failed: {}", e),
                                            }
                                        }
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

        info!("[WS] Disconnected, reconnecting in 5s...");
        tokio::time::sleep(Duration::from_secs(5)).await;
    }
}
