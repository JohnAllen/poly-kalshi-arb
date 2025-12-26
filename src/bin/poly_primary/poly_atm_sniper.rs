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

    /// ATM threshold: max % distance from strike to be considered ATM (default: 0.035% = ~$1 for ETH)
    /// Price must be within this percentage of strike for delta ≈ 0.50
    #[arg(long, default_value_t = 0.035)]
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

    /// Minimum minutes remaining to trade (default: 2)
    #[arg(long, default_value_t = 2)]
    min_minutes: i64,

    /// Maximum minutes remaining to trade (default: 15)
    #[arg(long, default_value_t = 15)]
    max_minutes: i64,

    /// Connect directly to Polygon.io instead of local price server
    #[arg(short, long, default_value_t = false)]
    direct: bool,

    /// How long to leave orders resting before cancel (milliseconds, default: 1000)
    #[arg(long, default_value_t = 1000)]
    wait_ms: u64,
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
    /// How many seconds after window start we captured the strike (0 = on time)
    strike_delay_secs: Option<i64>,
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
            strike_delay_secs: None,
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

/// Load positions from wallet for given market tokens
async fn load_positions_for_market(
    client: &SharedAsyncClient,
    market_id: &str,
    yes_token: &str,
    no_token: &str,
    asset: &str,
) -> Position {
    let mut pos = Position::default();

    // Check YES token balance and trades
    debug!("[POSITIONS] Checking {} YES token {}...", asset, &yes_token[..12.min(yes_token.len())]);
    match client.get_balance(yes_token).await {
        Ok(yes_bal) if yes_bal > 0.0 => {
            // Get trade history to calculate cost basis
            let cost = match client.get_trades(yes_token).await {
                Ok(trades) => {
                    trades.iter()
                        .filter(|(_, _, side)| side == "BUY")
                        .map(|(price, size, _)| price * size)
                        .sum()
                }
                Err(e) => {
                    warn!("[POSITIONS] Failed to get YES trades for {}: {}", asset, e);
                    0.0
                }
            };
            let avg_price = if cost > 0.0 { cost / yes_bal * 100.0 } else { 0.0 };
            info!("[POSITIONS] ✅ Found {:.1} {} YES @{:.0}¢ (cost ${:.2})", yes_bal, asset, avg_price, cost);
            pos.yes_qty = yes_bal;
            pos.yes_cost = cost;
        }
        Ok(bal) => debug!("[POSITIONS] {} YES balance = {:.2}", asset, bal),
        Err(e) => warn!("[POSITIONS] ❌ Failed to get YES balance for {} ({}): {}", asset, &yes_token[..8], e),
    }

    // Check NO token balance and trades
    debug!("[POSITIONS] Checking {} NO token {}...", asset, &no_token[..12.min(no_token.len())]);
    match client.get_balance(no_token).await {
        Ok(no_bal) if no_bal > 0.0 => {
            let cost = match client.get_trades(no_token).await {
                Ok(trades) => {
                    trades.iter()
                        .filter(|(_, _, side)| side == "BUY")
                        .map(|(price, size, _)| price * size)
                        .sum()
                }
                Err(e) => {
                    warn!("[POSITIONS] Failed to get NO trades for {}: {}", asset, e);
                    0.0
                }
            };
            let avg_price = if cost > 0.0 { cost / no_bal * 100.0 } else { 0.0 };
            info!("[POSITIONS] ✅ Found {:.1} {} NO @{:.0}¢ (cost ${:.2})", no_bal, asset, avg_price, cost);
            pos.no_qty = no_bal;
            pos.no_cost = cost;
        }
        Ok(bal) => debug!("[POSITIONS] {} NO balance = {:.2}", asset, bal),
        Err(e) => warn!("[POSITIONS] ❌ Failed to get NO balance for {} ({}): {}", asset, &no_token[..8], e),
    }

    pos
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

/// Price change message format from Polymarket WebSocket
#[derive(Deserialize, Debug)]
struct PriceChangeMessage {
    #[serde(default)]
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
                                    let secs_until_start = start_ts - now_ts;
                                    if now_ts >= start_ts {
                                        let delay_secs = now_ts - start_ts;
                                        market.strike_price = Some(price);
                                        market.strike_delay_secs = Some(delay_secs);
                                        // Extract time window for logging
                                        let time_window = market.question
                                            .split(',')
                                            .nth(1)
                                            .and_then(|s| s.trim().split(" ET").next())
                                            .unwrap_or("-");
                                        if delay_secs > 10 {
                                            warn!("[STRIKE] ⚠️ {} {} captured: ${:.2} ({}s LATE - using market prices only!)",
                                                  asset, time_window, price, delay_secs);
                                        } else {
                                            info!("[STRIKE] ✅ {} {} captured: ${:.2} ({}s after window start)",
                                                  asset, time_window, price, delay_secs);
                                        }
                                    } else if secs_until_start <= 10 {
                                        info!("[STRIKE] {} waiting: {}s until window starts",
                                              asset, secs_until_start);
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
                                    let secs_until_start = start_ts - now_ts;
                                    if now_ts >= start_ts {
                                        let delay_secs = now_ts - start_ts;
                                        market.strike_price = Some(price);
                                        market.strike_delay_secs = Some(delay_secs);
                                        // Extract time window for logging
                                        let time_window = market.question
                                            .split(',')
                                            .nth(1)
                                            .and_then(|s| s.trim().split(" ET").next())
                                            .unwrap_or("-");
                                        if delay_secs > 10 {
                                            warn!("[STRIKE] ⚠️ {} {} captured: ${:.2} ({}s LATE - using market prices only!)",
                                                  asset, time_window, price, delay_secs);
                                        } else {
                                            info!("[STRIKE] ✅ {} {} captured: ${:.2} ({}s after window start)",
                                                  asset, time_window, price, delay_secs);
                                        }
                                    } else if secs_until_start <= 10 {
                                        info!("[STRIKE] {} waiting: {}s until window starts",
                                              asset, secs_until_start);
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
    info!("");
    info!("📥 ENTRY CONDITIONS (either triggers entry):");
    info!("");
    info!("   🔔 ATM ENTRY (delta ≈ 0.50):");
    info!("      • Spot within {:.4}% of strike", args.atm_threshold);
    info!("      • Time: {}m - {}m before expiry", args.min_minutes, args.max_minutes);
    info!("      • Action: BID {}¢ on both YES and NO", args.bid);
    info!("");
    info!("   💰 ARB ENTRY (guaranteed profit):");
    info!("      • Combined asks < 100¢ (YES + NO < $1)");
    info!("      • Time: {}m - {}m before expiry", args.min_minutes, args.max_minutes);
    info!("      • Action: BUY at ask prices immediately");
    info!("");
    info!("   Contracts: {} per side (min 5)", args.contracts);
    info!("");
    info!("📤 EXIT CONDITIONS:");
    info!("   • Hold until expiry (auto-settles at $1 for winner, $0 for loser)");
    info!("   • Matched pairs (YES+NO) = guaranteed $1 payout");
    info!("");
    info!("═══════════════════════════════════════════════════════════════════════");
    info!("CONFIG:");
    info!("   Mode: {}", if args.live { "🚀 LIVE" } else { "🔍 DRY RUN" });
    info!("   Price Feed: {}", if args.direct { "Direct Polygon.io" } else { "Local server (ws://127.0.0.1:9999)" });
    info!("   Contracts: {} per trade (min for $1)", args.contracts);
    info!("   Max bid: {}¢", args.bid);
    info!("   ATM threshold: {:.4}%", args.atm_threshold);
    info!("   Time window: {}m - {}m before expiry", args.min_minutes, args.max_minutes);
    if let Some(ref market) = args.market {
        info!("   Market filter: {}", market);
    }
    if let Some(ref sym) = args.sym {
        info!("   Asset filter: {}", sym.to_uppercase());
    }
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
    info!("[POLY] API credentials ready, waiting for propagation...");
    tokio::time::sleep(Duration::from_secs(2)).await;

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

    // Load existing positions from Polymarket
    info!("[POSITIONS] Checking for existing positions...");
    {
        // Collect market info first (to avoid borrow issues)
        let market_tokens: Vec<(String, String, String, String)> = {
            let s = state.read().await;
            s.markets.iter()
                .map(|(id, m)| (id.clone(), m.yes_token.clone(), m.no_token.clone(), m.asset.clone()))
                .collect()
        };

        for (market_id, yes_token, no_token, asset) in market_tokens {
            let pos = load_positions_for_market(&shared_client, &market_id, &yes_token, &no_token, &asset).await;
            if pos.yes_qty > 0.0 || pos.no_qty > 0.0 {
                let mut s = state.write().await;
                s.positions.insert(market_id, pos);
            }
        }
    }

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
    let client_for_discovery = shared_client.clone();
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

                    // Collect new markets that need position loading (do this outside write lock)
                    let new_markets_to_load: Vec<_> = {
                        let s = state_for_discovery.read().await;
                        discovered.iter()
                            .filter(|pm| {
                                match s.markets.get(&pm.condition_id) {
                                    Some(existing) => existing.window_start_ts != pm.window_start_ts,
                                    None => true,
                                }
                            })
                            .map(|pm| (pm.condition_id.clone(), pm.yes_token.clone(), pm.no_token.clone(), pm.asset.clone()))
                            .collect()
                    };

                    // Load positions for new markets (outside lock)
                    let mut loaded_positions = Vec::new();
                    for (id, yes_tok, no_tok, asset) in &new_markets_to_load {
                        let pos = load_positions_for_market(&client_for_discovery, id, yes_tok, no_tok, asset).await;
                        loaded_positions.push((id.clone(), pos));
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

                    // Add new markets with loaded positions
                    for pm in discovered {
                        let id = pm.condition_id.clone();
                        if let Some(existing) = s.markets.get(&id) {
                            // Check if window_start_ts changed (new time window for same condition)
                            if existing.window_start_ts != pm.window_start_ts {
                                info!("[DISCOVER] 🔄 Market window changed: {} | {} -> {:?}",
                                      pm.asset,
                                      existing.window_start_ts.map(|t| format!("{}", t)).unwrap_or("-".into()),
                                      pm.window_start_ts);
                                // Remove old entry and re-add
                                s.markets.remove(&id);
                                s.positions.remove(&id);
                                s.orders.remove(&id);

                                // Use loaded position if available, otherwise default
                                let pos = loaded_positions.iter()
                                    .find(|(pid, _)| pid == &id)
                                    .map(|(_, p)| p.clone())
                                    .unwrap_or_default();
                                s.positions.insert(id.clone(), pos);
                                s.orders.insert(id.clone(), Orders::default());
                                s.markets.insert(id, Market::from_polymarket(pm));
                                new_count += 1;
                            }
                        } else {
                            info!("[DISCOVER] 🆕 New market: {} | {} | {:.1?}min | start_ts={:?}",
                                  pm.asset, &pm.question[..pm.question.len().min(40)], pm.expiry_minutes, pm.window_start_ts);

                            // Use loaded position if available, otherwise default
                            let pos = loaded_positions.iter()
                                .find(|(pid, _)| pid == &id)
                                .map(|(_, p)| p.clone())
                                .unwrap_or_default();
                            s.positions.insert(id.clone(), pos);
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
    let wait_ms = args.wait_ms;

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
        let mut status_interval = tokio::time::interval(Duration::from_secs(1)); // Fast 1s checks for ATM
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

                    let mode_str = if dry_run { "🔍 DRY" } else { "🚀 LIVE" };

                    for (id, market) in &s.markets {
                        let pos = s.positions.get(id).cloned().unwrap_or_default();

                        // Get spot price for this asset
                        let spot = match market.asset.as_str() {
                            "BTC" => s.prices.btc_price,
                            "ETH" => s.prices.eth_price,
                            "SOL" => s.prices.sol_price,
                            "XRP" => s.prices.xrp_price,
                            _ => None,
                        };

                        let expiry = market.time_remaining_mins().unwrap_or(0.0);

                        // Skip out-of-window markets silently (we only care about active ones)
                        if expiry < min_minutes || expiry > max_minutes {
                            continue;
                        }

                        // Check ATM status using captured strike price AND market prices
                        let yes_ask = market.yes_ask.unwrap_or(100);
                        let no_ask = market.no_ask.unwrap_or(100);
                        let (atm_status, dist_str) = match (spot, market.strike_price) {
                            (Some(s), Some(k)) => {
                                let dist = distance_from_strike_pct(s, k);
                                let spot_atm = dist <= atm_threshold;
                                // Market must also price it as ATM: both asks between 35-65¢
                                let mkt_atm = yes_ask >= 35 && yes_ask <= 65 && no_ask >= 35 && no_ask <= 65;
                                let status = if spot_atm && mkt_atm {
                                    "ATM✓"  // Both spot and market agree
                                } else if spot_atm && !mkt_atm {
                                    "ATM✗"  // Spot is ATM but market disagrees
                                } else if s > k {
                                    "ITM"
                                } else {
                                    "OTM"
                                };
                                (status, format!("{:.3}%", dist))
                            }
                            (_, None) => ("WAIT", "-".into()),
                            _ => ("?", "-".into()),
                        };

                        let yes_bid_val = market.yes_bid;
                        let no_bid_val = market.no_bid;
                        let yes_bid = yes_bid_val.map(|b| format!("{}¢", b)).unwrap_or("-".into());
                        let no_bid = no_bid_val.map(|b| format!("{}¢", b)).unwrap_or("-".into());

                        let strike_str = market.strike_price
                            .map(|s| format!("${:.2}", s))
                            .unwrap_or_else(|| "?".into());
                        let spot_str = spot.map(|s| format!("${:.2}", s)).unwrap_or("?".into());

                        // Extract time window from question (e.g., "8:00-8:15")
                        let time_window = market.question
                            .split(',')
                            .nth(1)
                            .and_then(|s| s.trim().split(" ET").next())
                            .unwrap_or("-");

                        // Calculate MTM P&L for this market's position
                        let pos_cost = pos.yes_cost + pos.no_cost;
                        let mtm_value = (pos.yes_qty * yes_bid_val.unwrap_or(0) as f64 / 100.0)
                                      + (pos.no_qty * no_bid_val.unwrap_or(0) as f64 / 100.0);
                        let mtm_pnl = mtm_value - pos_cost;

                        // Calculate avg cost per share
                        let yes_avg = if pos.yes_qty > 0.0 { (pos.yes_cost / pos.yes_qty * 100.0).round() as i64 } else { 0 };
                        let no_avg = if pos.no_qty > 0.0 { (pos.no_cost / pos.no_qty * 100.0).round() as i64 } else { 0 };

                        // Build position string with avg cost
                        let pos_str = if pos.yes_qty > 0.0 && pos.no_qty > 0.0 {
                            format!("Y={:.0}@{}¢ N={:.0}@{}¢", pos.yes_qty, yes_avg, pos.no_qty, no_avg)
                        } else if pos.yes_qty > 0.0 {
                            format!("Y={:.0}@{}¢", pos.yes_qty, yes_avg)
                        } else if pos.no_qty > 0.0 {
                            format!("N={:.0}@{}¢", pos.no_qty, no_avg)
                        } else {
                            "none".to_string()
                        };

                        // Build action status: what would we do / why not
                        let action_str = if pos.yes_qty > 0.0 || pos.no_qty > 0.0 {
                            "HOLDING".to_string() // Already have position, not trading more
                        } else if atm_status == "ATM✓" {
                            format!("READY@{}¢", bid_price) // Ready to bid
                        } else {
                            "WAIT".to_string() // Not ATM, waiting
                        };

                        // Single consolidated log line with time window
                        info!("[{}] {} {} | {}={} K={} {} dist={} | Y={}/{}¢ N={}/{}¢ | {:.0}m | {} | Pos {} | Cost=${:.2} MTM=${:.2} PnL=${:+.2}",
                              mode_str,
                              market.asset,
                              time_window,
                              market.asset,
                              spot_str,
                              strike_str,
                              atm_status,
                              dist_str,
                              yes_bid, yes_ask,
                              no_bid, no_ask,
                              expiry,
                              action_str,
                              pos_str,
                              pos_cost, mtm_value, mtm_pnl);
                    }

                    // Proactive trading: attempt trades during status tick, not just on orderbook updates
                    // TWO entry conditions:
                    // 1. ATM Entry: When BOTH spot near strike AND market priced as ATM (asks 35-65¢)
                    // 2. Arb Entry: When combined asks < 100¢, buy at asks (guaranteed profit)
                    const POLY_MIN_CONTRACTS: f64 = 5.0;
                    {
                        // Collect opportunities: (market_id, yes_token, no_token, yes_ask, no_ask, asset, is_arb)
                        let trade_ops: Vec<(String, String, String, i64, i64, String, bool)> = s.markets.iter()
                            .filter_map(|(id, m)| {
                                let exp = m.time_remaining_mins().unwrap_or(0.0);
                                if exp < min_minutes || exp > max_minutes {
                                    // Already logged in status display, skip silently here
                                    return None;
                                }
                                let spot = match m.asset.as_str() {
                                    "BTC" => s.prices.btc_price, "ETH" => s.prices.eth_price,
                                    "SOL" => s.prices.sol_price, "XRP" => s.prices.xrp_price, _ => None,
                                }?;
                                let (ya, na) = (m.yes_ask.unwrap_or(100), m.no_ask.unwrap_or(100));
                                let combined = ya + na;
                                let pos = s.positions.get(id).cloned().unwrap_or_default();

                                // Skip if we already have position (already entered) - we only trade once per market
                                if pos.yes_qty > 0.0 || pos.no_qty > 0.0 {
                                    // Only log occasionally (every 10 seconds approx based on call count)
                                    return None;
                                }

                                // Check ATM condition: BOTH spot near strike AND market prices near 50/50
                                // If strike was captured late, market prices tell us the true state
                                let strike = match m.strike_price {
                                    Some(k) => k,
                                    None => {
                                        // Extract time window for better logging
                                        let tw = m.question.split(',').nth(1)
                                            .and_then(|s| s.trim().split(" ET").next())
                                            .unwrap_or("-");
                                        info!("[TRADE] {} {} skip: no strike captured (window_start_ts={:?})",
                                              m.asset, tw, m.window_start_ts);
                                        return None;
                                    }
                                };
                                let dist = distance_from_strike_pct(spot, strike);
                                let strike_delay = m.strike_delay_secs.unwrap_or(0);
                                let strike_reliable = strike_delay <= 10;

                                // If strike was captured late, it's unreliable - don't trust spot_atm
                                let spot_is_atm = if strike_reliable {
                                    dist <= atm_threshold
                                } else {
                                    false // Strike is unreliable, can't trust spot comparison
                                };

                                // Market must also price it as ATM: both asks between 35-65¢
                                // If market prices YES at 23¢, it's NOT ATM regardless of spot
                                let market_is_atm = ya >= 35 && ya <= 65 && na >= 35 && na <= 65;

                                // ATM logic:
                                // - If strike is reliable: require BOTH spot and market to agree
                                // - If strike is unreliable: rely ONLY on market prices
                                let is_atm = if strike_reliable {
                                    spot_is_atm && market_is_atm
                                } else {
                                    market_is_atm // Only trust market when our strike is late
                                };

                                // Check Arb condition: combined < 100¢
                                let is_arb = combined < 100;

                                // Skip logging handled by status display - no need to duplicate here

                                // Enter if ATM (logic depends on strike reliability) OR if Arb opportunity
                                if is_atm || is_arb {
                                    let mode = if !strike_reliable { "MKT-ONLY" } else { "SPOT+MKT" };
                                    info!("[TRADE] ✅ {} QUALIFIED ({}): spot=${:.2} K=${:.2} delay={}s atm={} arb={} dist={:.4}% Y={}¢ N={}¢",
                                          m.asset, mode, spot, strike, strike_delay, is_atm, is_arb, dist, ya, na);
                                    Some((id.clone(), m.yes_token.clone(), m.no_token.clone(), ya, na, m.asset.clone(), is_arb))
                                } else {
                                    None
                                }
                            }).collect();
                        drop(s);

                        for (mid, ytok, ntok, yask, nask, asset, is_arb) in trade_ops {
                            let combined = yask + nask;
                            let entry_type = if is_arb { "ARB" } else { "ATM" };

                            // For ARB: buy at ask price (guaranteed fill for arb profit)
                            // For ATM: bid at our bid_price
                            let y_price = if is_arb { yask } else { bid_price };
                            let n_price = if is_arb { nask } else { bid_price };

                            // Use contracts directly - minimum 5 for Polymarket
                            let act_c = contracts.max(POLY_MIN_CONTRACTS);
                            let cost_per_pair = (y_price + n_price) as f64 / 100.0;
                            let total_cost = act_c * cost_per_pair;

                            if dry_run {
                                if is_arb {
                                    warn!("💰💰💰 [{}] Would BUY {:.0} YES@{}¢ + NO@{}¢ = {}¢ ({}¢ profit) | {} 💰💰💰",
                                          entry_type, act_c, yask, nask, combined, 100 - combined, asset);
                                } else {
                                    warn!("🔔🔔🔔 [{}] Would BID {:.0} YES@{}¢ + NO@{}¢ | asks Y={}¢ N={}¢ | {} 🔔🔔🔔",
                                          entry_type, act_c, bid_price, bid_price, yask, nask, asset);
                                }
                            } else {
                                if is_arb {
                                    // ARB: Buy BOTH at ask prices
                                    let y_pr = yask as f64 / 100.0;
                                    let n_pr = nask as f64 / 100.0;
                                    let profit = act_c * 1.0 - total_cost;
                                    warn!("💰💰💰 [ARB] 📝 BUY {:.0} YES@{}¢ + NO@{}¢ = ${:.2} (profit ${:.2}) | {} 💰💰💰",
                                          act_c, yask, nask, total_cost, profit, asset);

                                    // Buy YES
                                    if let Ok(f) = shared_client.buy_fak(&ytok, y_pr, act_c).await {
                                        if f.filled_size > 0.0 {
                                            let fp = (f.fill_cost / f.filled_size * 100.0).round() as i64;
                                            warn!("💰 [ARB] ✅ YES Filled {:.2} @{}¢ (${:.2})", f.filled_size, fp, f.fill_cost);
                                            let mut st = state.write().await;
                                            if let Some(p) = st.positions.get_mut(&mid) { p.yes_qty += f.filled_size; p.yes_cost += f.fill_cost; }
                                        }
                                    }
                                    // Buy NO
                                    if let Ok(f) = shared_client.buy_fak(&ntok, n_pr, act_c).await {
                                        if f.filled_size > 0.0 {
                                            let fp = (f.fill_cost / f.filled_size * 100.0).round() as i64;
                                            warn!("💰 [ARB] ✅ NO Filled {:.2} @{}¢ (${:.2})", f.filled_size, fp, f.fill_cost);
                                            let mut st = state.write().await;
                                            if let Some(p) = st.positions.get_mut(&mid) { p.no_qty += f.filled_size; p.no_cost += f.fill_cost; }
                                        }
                                    }
                                } else {
                                    // ATM: Bid at our bid_price with timed order on BOTH sides
                                    let pr = bid_price as f64 / 100.0;

                                    warn!("🔔🔔🔔 [ATM] 📝 BID {:.0} YES@{}¢ + NO@{}¢ (${:.2}) wait={}ms | asks Y={}¢ N={}¢ | {} 🔔🔔🔔",
                                          act_c, bid_price, bid_price, total_cost, wait_ms, yask, nask, asset);

                                    // Get strike price for logging
                                    let strike_price = {
                                        let st = state.read().await;
                                        st.markets.get(&mid).and_then(|m| m.strike_price)
                                    };
                                    let strike_str = strike_price.map(|k| format!("${:.2}", k)).unwrap_or("?".into());
                                    let poly_url = format!("https://polymarket.com/event/{}", mid);

                                    // Place YES order and log price at placement
                                    let yes_result = shared_client.buy_timed(&ytok, pr, act_c, wait_ms).await;
                                    let spot_at_yes = {
                                        let st = state.read().await;
                                        match asset.as_str() {
                                            "BTC" => st.prices.btc_price,
                                            "ETH" => st.prices.eth_price,
                                            "SOL" => st.prices.sol_price,
                                            "XRP" => st.prices.xrp_price,
                                            _ => None,
                                        }
                                    };
                                    info!("[ORDER] YES placed | {}={} K={} | {}", asset, spot_at_yes.map(|p| format!("${:.2}", p)).unwrap_or("?".into()), strike_str, poly_url);

                                    // Place NO order and log price at placement
                                    let no_result = shared_client.buy_timed(&ntok, pr, act_c, wait_ms).await;
                                    let spot_at_no = {
                                        let st = state.read().await;
                                        match asset.as_str() {
                                            "BTC" => st.prices.btc_price,
                                            "ETH" => st.prices.eth_price,
                                            "SOL" => st.prices.sol_price,
                                            "XRP" => st.prices.xrp_price,
                                            _ => None,
                                        }
                                    };
                                    info!("[ORDER] NO placed | {}={} K={} | {}", asset, spot_at_no.map(|p| format!("${:.2}", p)).unwrap_or("?".into()), strike_str, poly_url);

                                    // Track fills
                                    let mut y_filled = 0.0;
                                    let mut n_filled = 0.0;

                                    if let Ok(f) = yes_result {
                                        if f.filled_size > 0.0 {
                                            let fp = (f.fill_cost / f.filled_size * 100.0).round() as i64;
                                            warn!("🔔 [ATM] ✅ YES Filled {:.2} @{}¢ (${:.2})", f.filled_size, fp, f.fill_cost);
                                            let mut st = state.write().await;
                                            if let Some(p) = st.positions.get_mut(&mid) { p.yes_qty += f.filled_size; p.yes_cost += f.fill_cost; }
                                            y_filled = f.filled_size;
                                        } else {
                                            info!("🔔 [ATM] ⏳ YES no fill after {}ms", wait_ms);
                                        }
                                    }

                                    if let Ok(f) = no_result {
                                        if f.filled_size > 0.0 {
                                            let fp = (f.fill_cost / f.filled_size * 100.0).round() as i64;
                                            warn!("🔔 [ATM] ✅ NO Filled {:.2} @{}¢ (${:.2})", f.filled_size, fp, f.fill_cost);
                                            let mut st = state.write().await;
                                            if let Some(p) = st.positions.get_mut(&mid) { p.no_qty += f.filled_size; p.no_cost += f.fill_cost; }
                                            n_filled = f.filled_size;
                                        } else {
                                            info!("🔔 [ATM] ⏳ NO no fill after {}ms", wait_ms);
                                        }
                                    }

                                    // Log summary
                                    let matched = y_filled.min(n_filled);
                                    if matched > 0.0 {
                                        info!("🔔 [ATM] Matched {:.0} pairs | Y={:.0} N={:.0}", matched, y_filled, n_filled);
                                    }
                                }
                            }
                        }
                    }
                }

                msg = read.next() => {
                    let Some(msg) = msg else { break; };

                    match msg {
                        Ok(Message::Text(text)) => {
                            // Try parsing as full orderbook snapshot (array or single object)
                            let books: Option<Vec<BookSnapshot>> = serde_json::from_str::<Vec<BookSnapshot>>(&text).ok()
                                .or_else(|| serde_json::from_str::<BookSnapshot>(&text).ok().map(|b| vec![b]));

                            if let Some(books) = books {
                                debug!("[WS] BookSnapshot: {} books", books.len());
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

                                    let Some(market) = s.markets.get_mut(&market_id) else { continue };

                                    let is_yes = book.asset_id == market.yes_token;
                                    let side = if is_yes { "YES" } else { "NO" };

                                    // Log price updates
                                    debug!("[WS] {} {} bid={:?} ask={:?}", market.asset, side,
                                           best_bid.map(|(p,_)| p), best_ask.map(|(p,_)| p));

                                    if is_yes {
                                        market.yes_ask = best_ask.map(|(p, _)| p);
                                        market.yes_bid = best_bid.map(|(p, _)| p);
                                        market.yes_ask_size = best_ask.map(|(_, s)| s).unwrap_or(0.0);
                                    } else {
                                        market.no_ask = best_ask.map(|(p, _)| p);
                                        market.no_bid = best_bid.map(|(p, _)| p);
                                        market.no_ask_size = best_ask.map(|(_, s)| s).unwrap_or(0.0);
                                    }
                                    // Trading is handled by the status_interval tick (1s)
                                    // to avoid race conditions and duplicate orders
                                }
                            }
                            // Ignore PriceChangeMessage - these are last trade prices, not orderbook
                            // Only BookSnapshot gives us accurate bid/ask data
                            // PriceChangeMessage was causing wild price swings (91¢→47¢→89¢)
                            else if text.contains("price_changes") {
                                // Intentionally ignored
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
