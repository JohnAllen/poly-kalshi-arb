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
use chrono::Utc;
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

    /// Minimum minutes remaining to trade (default: 2)
    #[arg(long, default_value_t = 2)]
    min_minutes: i64,

    /// Maximum minutes remaining to trade (default: 15)
    #[arg(long, default_value_t = 15)]
    max_minutes: i64,
}

const POLYMARKET_WS_URL: &str = "wss://ws-subscriptions-clob.polymarket.com/ws/market";
const GAMMA_API_BASE: &str = "https://gamma-api.polymarket.com";
const POLYGON_WS_URL: &str = "wss://socket.polygon.io/crypto";

/// Market state
#[derive(Debug, Clone)]
struct Market {
    condition_id: String,
    question: String,
    yes_token: String,
    no_token: String,
    strike: Option<f64>,
    asset: String, // "BTC" or "ETH"
    expiry_minutes: Option<f64>,
    // Orderbook
    yes_ask: Option<i64>,
    yes_bid: Option<i64>,
    no_ask: Option<i64>,
    no_bid: Option<i64>,
    yes_ask_size: f64,
    no_ask_size: f64,
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
    last_update: Option<std::time::Instant>,
}

/// Global state
struct State {
    markets: HashMap<String, Market>,
    positions: HashMap<String, Position>,
    orders: HashMap<String, Orders>,
    prices: PriceState,
}

impl State {
    fn new() -> Self {
        Self {
            markets: HashMap::new(),
            positions: HashMap::new(),
            orders: HashMap::new(),
            prices: PriceState::default(),
        }
    }
}

// === Gamma API for market discovery ===

#[derive(Debug, Deserialize)]
struct GammaSeries {
    events: Option<Vec<GammaEvent>>,
}

#[derive(Debug, Deserialize)]
#[allow(dead_code)]
struct GammaEvent {
    slug: Option<String>,
    title: Option<String>,
    closed: Option<bool>,
    markets: Option<Vec<GammaMarket>>,
    #[serde(rename = "endDate")]
    end_date: Option<String>,
    #[serde(rename = "enableOrderBook")]
    enable_order_book: Option<bool>,
}

#[derive(Debug, Deserialize)]
#[allow(dead_code)]
struct GammaMarket {
    #[serde(rename = "conditionId")]
    condition_id: Option<String>,
    question: Option<String>,
    #[serde(rename = "clobTokenIds")]
    clob_token_ids: Option<String>,
    outcomes: Option<String>,
    active: Option<bool>,
    closed: Option<bool>,
    #[serde(rename = "endDate")]
    end_date: Option<String>,
    #[serde(rename = "acceptingOrders")]
    accepting_orders: Option<bool>,
}

/// Crypto series slugs for 15-minute markets
const POLY_SERIES_SLUGS: &[(&str, &str)] = &[
    ("btc-up-or-down-15m", "BTC"),
    ("eth-up-or-down-15m", "ETH"),
];

/// Discover crypto markets on Polymarket with strikes
async fn discover_markets(market_filter: Option<&str>) -> Result<Vec<Market>> {
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(30))
        .build()?;

    let mut markets = Vec::new();

    // Filter series if user specified an asset
    let series_to_check: Vec<(&str, &str)> = if let Some(filter) = market_filter {
        let filter_lower = filter.to_lowercase();
        POLY_SERIES_SLUGS
            .iter()
            .filter(|(slug, asset)| {
                slug.contains(&filter_lower) || asset.to_lowercase().contains(&filter_lower)
            })
            .copied()
            .collect()
    } else {
        POLY_SERIES_SLUGS.to_vec()
    };

    for (series_slug, asset) in series_to_check {
        let url = format!("{}/series?slug={}", GAMMA_API_BASE, series_slug);

        let resp = client
            .get(&url)
            .header("User-Agent", "poly_atm_sniper/1.0")
            .send()
            .await?;

        if !resp.status().is_success() {
            warn!("[DISCOVER] Failed to fetch series '{}': {}", series_slug, resp.status());
            continue;
        }

        let series_list: Vec<GammaSeries> = resp.json().await?;
        let Some(series) = series_list.first() else { continue };
        let Some(events) = &series.events else { continue };

        // Collect event slugs that need fetching
        let event_slugs: Vec<String> = events
            .iter()
            .filter(|e| e.closed != Some(true) && e.enable_order_book == Some(true))
            .filter_map(|e| e.slug.clone())
            .take(20)
            .collect();

        // Fetch each event's market details
        for event_slug in event_slugs {
            let event_url = format!("{}/events?slug={}", GAMMA_API_BASE, event_slug);
            let resp = match client.get(&event_url)
                .header("User-Agent", "poly_atm_sniper/1.0")
                .send()
                .await
            {
                Ok(r) => r,
                Err(_) => continue,
            };

            let event_details: Vec<serde_json::Value> = match resp.json().await {
                Ok(ed) => ed,
                Err(_) => continue,
            };

            let Some(ed) = event_details.first() else { continue };
            let Some(mkts) = ed.get("markets").and_then(|m| m.as_array()) else { continue };

            for mkt in mkts {
                let condition_id = mkt.get("conditionId")
                    .and_then(|v| v.as_str())
                    .map(|s| s.to_string());
                let clob_tokens_str = mkt.get("clobTokenIds")
                    .and_then(|v| v.as_str())
                    .map(|s| s.to_string());
                let question = mkt.get("question")
                    .and_then(|v| v.as_str())
                    .map(|s| s.to_string())
                    .unwrap_or_else(|| event_slug.clone());
                let end_date_str = mkt.get("endDate")
                    .and_then(|v| v.as_str())
                    .map(|s| s.to_string());

                let Some(cid) = condition_id else { continue };
                let Some(cts) = clob_tokens_str else { continue };

                let token_ids: Vec<String> = serde_json::from_str(&cts).unwrap_or_default();
                if token_ids.len() < 2 {
                    continue;
                }

                let expiry_minutes = end_date_str.as_ref().and_then(|d| parse_expiry_minutes(d));

                // Skip expired markets
                if expiry_minutes.map(|m| m <= 0.0).unwrap_or(true) {
                    continue;
                }

                // Parse strike from question
                let strike = parse_strike_from_question(&question);

                markets.push(Market {
                    condition_id: cid,
                    question,
                    yes_token: token_ids[0].clone(),
                    no_token: token_ids[1].clone(),
                    strike,
                    asset: asset.to_string(),
                    expiry_minutes,
                    yes_ask: None,
                    yes_bid: None,
                    no_ask: None,
                    no_bid: None,
                    yes_ask_size: 0.0,
                    no_ask_size: 0.0,
                });
            }
        }
    }

    // Deduplicate by condition_id
    let mut seen = std::collections::HashSet::new();
    markets.retain(|m| seen.insert(m.condition_id.clone()));

    // Sort by expiry (soonest first)
    markets.sort_by(|a, b| {
        a.expiry_minutes
            .unwrap_or(f64::MAX)
            .partial_cmp(&b.expiry_minutes.unwrap_or(f64::MAX))
            .unwrap_or(std::cmp::Ordering::Equal)
    });

    Ok(markets)
}

/// Parse strike price from question text
fn parse_strike_from_question(question: &str) -> Option<f64> {
    // Look for patterns like "$100,000" or "100000"
    let re_patterns = [
        r"\$([0-9,]+(?:\.[0-9]+)?)",
        r"([0-9]+(?:,[0-9]+)*(?:\.[0-9]+)?)\s*(?:dollars?|usd)",
        r"above\s+\$?([0-9,]+)",
        r"below\s+\$?([0-9,]+)",
    ];

    for pattern in re_patterns {
        if let Ok(re) = regex_lite::Regex::new(pattern) {
            if let Some(caps) = re.captures(&question.to_lowercase()) {
                if let Some(m) = caps.get(1) {
                    let num_str = m.as_str().replace(",", "");
                    if let Ok(val) = num_str.parse::<f64>() {
                        return Some(val);
                    }
                }
            }
        }
    }
    None
}

/// Parse expiry time and return minutes remaining
fn parse_expiry_minutes(end_date: &str) -> Option<f64> {
    let dt = chrono::DateTime::parse_from_rfc3339(end_date).ok()?;
    let now = Utc::now();
    let diff = dt.signed_duration_since(now);
    let minutes = diff.num_minutes() as f64;
    if minutes > 0.0 {
        Some(minutes)
    } else {
        None
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

// === Polygon Price Feed ===

#[derive(Deserialize, Debug)]
struct PolygonMessage {
    ev: Option<String>,
    pair: Option<String>,
    p: Option<f64>,
}

async fn run_polygon_feed(state: Arc<RwLock<State>>, api_key: &str) {
    loop {
        info!("[POLYGON] Connecting to price feed...");

        let url = format!("{}?apiKey={}", POLYGON_WS_URL, api_key);
        let ws = match connect_async(&url).await {
            Ok((ws, _)) => ws,
            Err(e) => {
                error!("[POLYGON] Connect failed: {}", e);
                tokio::time::sleep(Duration::from_secs(5)).await;
                continue;
            }
        };

        let (mut write, mut read) = ws.split();

        // Subscribe to BTC and ETH
        let sub = serde_json::json!({
            "action": "subscribe",
            "params": "XT.BTC-USD,XT.ETH-USD"
        });
        let _ = write.send(Message::Text(sub.to_string())).await;
        info!("[POLYGON] Subscribed to BTC-USD, ETH-USD");

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

                            let mut s = state.write().await;
                            if pair == "BTC-USD" {
                                s.prices.btc_price = Some(price);
                            } else if pair == "ETH-USD" {
                                s.prices.eth_price = Some(price);
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

        warn!("[POLYGON] Disconnected, reconnecting in 5s...");
        tokio::time::sleep(Duration::from_secs(5)).await;
    }
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
    info!("   Size: ${:.2} per trade", args.size);
    info!("   Max bid: {}¢", args.bid);
    info!("   ATM threshold: {:.4}%", args.atm_threshold);
    info!("   Time window: {}m - {}m before expiry", args.min_minutes, args.max_minutes);
    if let Some(ref market) = args.market {
        info!("   Filter: {}", market);
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
    info!("[POLY] API credentials ready");

    if args.live {
        warn!("⚠️  LIVE MODE - Real money!");
        tokio::time::sleep(Duration::from_secs(3)).await;
    }

    // Discover markets
    info!("[DISCOVER] Searching for crypto markets with strikes...");
    let discovered = discover_markets(args.market.as_deref()).await?;
    info!("[DISCOVER] Found {} markets", discovered.len());

    for m in &discovered {
        info!("  • {} | Strike: {:?} | Asset: {} | Expiry: {:?}min",
              &m.question[..m.question.len().min(60)],
              m.strike, m.asset, m.expiry_minutes);
    }

    if discovered.is_empty() {
        warn!("No markets found! Try different search terms.");
        return Ok(());
    }

    // Initialize state
    let state = Arc::new(RwLock::new({
        let mut s = State::new();
        for m in discovered {
            let id = m.condition_id.clone();
            s.positions.insert(id.clone(), Position::default());
            s.orders.insert(id.clone(), Orders::default());
            s.markets.insert(id, m);
        }
        s
    }));

    // Start price feed
    let state_clone = state.clone();
    let polygon_key = polygon_api_key.clone();
    tokio::spawn(async move {
        run_polygon_feed(state_clone, &polygon_key).await;
    });

    // Get token IDs for subscription
    let tokens: Vec<String> = {
        let s = state.read().await;
        s.markets.values()
            .flat_map(|m| vec![m.yes_token.clone(), m.no_token.clone()])
            .collect()
    };

    // Main WebSocket loop
    let bid_price = args.bid;
    let size = args.size;
    let atm_threshold = args.atm_threshold;
    let dry_run = !args.live;
    let min_minutes = args.min_minutes as f64;
    let max_minutes = args.max_minutes as f64;

    loop {
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
        info!("[WS] Subscribed to {} tokens", tokens.len());

        let mut ping_interval = tokio::time::interval(Duration::from_secs(30));
        let mut status_interval = tokio::time::interval(Duration::from_secs(5));

        loop {
            tokio::select! {
                _ = ping_interval.tick() => {
                    if let Err(e) = write.send(Message::Ping(vec![])).await {
                        error!("[WS] Ping failed: {}", e);
                        break;
                    }
                }

                _ = status_interval.tick() => {
                    let s = state.read().await;
                    let spot_btc = s.prices.btc_price.map(|p| format!("{:.2}", p)).unwrap_or("-".into());
                    let spot_eth = s.prices.eth_price.map(|p| format!("{:.2}", p)).unwrap_or("-".into());

                    info!("[STATUS] BTC=${} ETH={}", spot_btc, spot_eth);

                    for (id, market) in &s.markets {
                        let pos = s.positions.get(id).cloned().unwrap_or_default();
                        let orders = s.orders.get(id).cloned().unwrap_or_default();

                        // Get spot for this market's asset
                        let spot = if market.asset == "ETH" {
                            s.prices.eth_price
                        } else {
                            s.prices.btc_price
                        };

                        let expiry = market.expiry_minutes.unwrap_or(0.0);
                        if expiry < min_minutes || expiry > max_minutes {
                            continue;
                        }

                        // Check ATM status
                        let (atm_status, dist_pct) = match (spot, market.strike) {
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
                            _ => ("❓ NO DATA", "-".into()),
                        };

                        let yes_ask = market.yes_ask.unwrap_or(100);
                        let no_ask = market.no_ask.unwrap_or(100);

                        let order_status = format!(
                            "Y:{} N:{}",
                            orders.yes_order_id.as_ref().map(|_| "📝").unwrap_or("-"),
                            orders.no_order_id.as_ref().map(|_| "📝").unwrap_or("-")
                        );

                        info!("  [{}] {} | {:.1}m | {} dist={} | Ask Y={}¢ N={}¢ | {} | Pos: Y={:.1} N={:.1}",
                              market.asset,
                              &market.question[..market.question.len().min(40)],
                              expiry,
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
                                    let strike = market.strike;
                                    let expiry = market.expiry_minutes;
                                    let yes_ask_price = market.yes_ask;
                                    let no_ask_price = market.no_ask;
                                    let yes_token = market.yes_token.clone();
                                    let no_token = market.no_token.clone();
                                    let question = market.question.clone();

                                    // Get current orders
                                    let orders = s.orders.get(&market_id).cloned().unwrap_or_default();

                                    // Get spot price based on asset
                                    let spot = if asset == "ETH" { eth_price } else { btc_price };

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
                                            info!("[DRY] Would BID ${:.0} YES @{}¢ | delta≈0.50 | {}",
                                                  size, bid_price, asset);
                                        }
                                        if need_no && no_worth_bidding {
                                            info!("[DRY] Would BID ${:.0} NO @{}¢ | delta≈0.50 | {}",
                                                  size, bid_price, asset);
                                        }
                                    } else {
                                        // Place YES bid
                                        if need_yes && yes_worth_bidding {
                                            let price = bid_price as f64 / 100.0;
                                            let contracts = size / price;

                                            warn!("[TRADE] 📝 BID ${:.0} YES @{}¢ | delta≈0.50 | {}",
                                                  size, bid_price, asset);

                                            match shared_client.buy_fak(&yes_token, price, contracts).await {
                                                Ok(fill) => {
                                                    if fill.filled_size > 0.0 {
                                                        warn!("[TRADE] ✅ YES Filled {:.2} @${:.2} | order_id={}",
                                                              fill.filled_size, fill.fill_cost, fill.order_id);
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
                                            let contracts = size / price;

                                            warn!("[TRADE] 📝 BID ${:.0} NO @{}¢ | delta≈0.50 | {}",
                                                  size, bid_price, asset);

                                            match shared_client.buy_fak(&no_token, price, contracts).await {
                                                Ok(fill) => {
                                                    if fill.filled_size > 0.0 {
                                                        warn!("[TRADE] ✅ NO Filled {:.2} @${:.2} | order_id={}",
                                                              fill.filled_size, fill.fill_cost, fill.order_id);
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
