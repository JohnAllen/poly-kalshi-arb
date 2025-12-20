//! Polymarket Ping Pong Bot
//!
//! STRATEGY:
//! - Buy when price drops to a low threshold (e.g., 30¢)
//! - When price reverts above a higher threshold (e.g., 50¢), look for arb
//! - If YES + NO asks < $1, buy both sides for guaranteed profit
//!
//! Usage:
//!   cargo run --release --bin poly_ping_pong -- --buy-at 30 --arb-above 50 --max-arb-cost 99
//!
//! Environment:
//!   POLY_PRIVATE_KEY - Your Polymarket/Polygon wallet private key
//!   POLY_FUNDER - Your funder address (proxy wallet)
//!   POLYGON_API_KEY - Polygon.io API key for price feed (optional)

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
use tracing::{debug, error, info, trace, warn};

use arb_bot::polymarket_clob::{PolymarketAsyncClient, PreparedCreds, SharedAsyncClient};

/// Polymarket Ping Pong Bot
#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Args {
    /// Symbol to trade: BTC, ETH, SOL, XRP (default: all)
    #[arg(long, default_value = "all")]
    sym: String,

    /// Buy threshold in cents - buy when price drops to this level (e.g., 30 means buy at 30c or below)
    #[arg(short, long, default_value_t = 30)]
    threshold: i64,

    /// Max total cost in cents to buy both sides for arb (default: 97 = 3c profit)
    #[arg(long, default_value_t = 97)]
    max_arb_cost: i64,

    /// Max total dollars to invest (default: $10)
    #[arg(short = 'm', long, default_value_t = 10.0)]
    max_dollars: f64,

    /// Live trading mode (default is dry run)
    #[arg(short, long, default_value_t = false)]
    live: bool,

    /// Minimum minutes remaining to trade (default: 1)
    #[arg(long, default_value_t = 1)]
    min_minutes: i64,

    /// Maximum minutes remaining to trade (default: 14)
    #[arg(long, default_value_t = 14)]
    max_minutes: i64,

    /// Maximum market age in minutes (default: 7 = only trade first 7 mins)
    #[arg(long, default_value_t = 7.0)]
    max_age: f64,
}

const POLYMARKET_WS_URL: &str = "wss://ws-subscriptions-clob.polymarket.com/ws/market";
const GAMMA_API_BASE: &str = "https://gamma-api.polymarket.com";
const LOCAL_PRICE_SERVER: &str = "ws://127.0.0.1:9999";

/// Price feed state
#[derive(Debug, Default)]
struct PriceState {
    btc_price: Option<f64>,
    eth_price: Option<f64>,
    sol_price: Option<f64>,
    xrp_price: Option<f64>,
    last_update: Option<std::time::Instant>,
}

#[derive(Deserialize, Debug)]
struct LocalPriceUpdate {
    btc_price: Option<f64>,
    eth_price: Option<f64>,
    sol_price: Option<f64>,
    xrp_price: Option<f64>,
}

/// Market state
#[derive(Debug, Clone)]
struct Market {
    condition_id: String,
    question: String,
    yes_token: String,
    no_token: String,
    asset: String,
    end_time: Option<chrono::DateTime<chrono::Utc>>,  // Store actual end time, not minutes
    // Orderbook
    yes_ask: Option<i64>,
    yes_bid: Option<i64>,
    no_ask: Option<i64>,
    no_bid: Option<i64>,
    yes_ask_size: f64,
    no_ask_size: f64,
}

impl Market {
    /// Get minutes remaining until expiry (recalculated each call)
    fn minutes_remaining(&self) -> Option<f64> {
        self.end_time.map(|end| {
            let now = Utc::now();
            let diff = end.signed_duration_since(now);
            diff.num_seconds() as f64 / 60.0
        })
    }

    /// Extract strike price from question (e.g., "Will SOL be above $126.00..." -> "$126.00")
    fn pin_name(&self) -> String {
        // Try to extract strike price like "$126.00" or "$97,000.00"
        if let Some(start) = self.question.find('$') {
            let rest = &self.question[start..];
            // Find end of price (space, "at", or end of string)
            let end = rest.find(|c: char| c == ' ' || c == '?')
                .unwrap_or(rest.len());
            return format!("@{}", &rest[..end]);
        }
        // For 15-minute up/down markets, show the end time
        if let Some(end_time) = self.end_time {
            return format!("exp:{}", end_time.format("%H:%M"));
        }
        // Fallback - empty (asset already shown)
        String::new()
    }
}

/// Position tracking
#[derive(Debug, Clone, Default)]
struct Position {
    yes_qty: f64,
    no_qty: f64,
    yes_cost: f64,
    no_cost: f64,
    // Track if we've bought during the crash
    bought_crash: bool,
}

impl Position {
    fn matched(&self) -> f64 {
        self.yes_qty.min(self.no_qty)
    }
    fn total_cost(&self) -> f64 {
        self.yes_cost + self.no_cost
    }
}

/// Global state
struct State {
    markets: HashMap<String, Market>,
    positions: HashMap<String, Position>,
    prices: PriceState,
}

impl State {
    fn new() -> Self {
        Self {
            markets: HashMap::new(),
            positions: HashMap::new(),
            prices: PriceState::default(),
        }
    }
}

/// Connect to local price server
async fn run_price_feed(state: Arc<RwLock<State>>) {
    loop {
        info!("[PRICES] Connecting to local price server {}...", LOCAL_PRICE_SERVER);

        match connect_async(LOCAL_PRICE_SERVER).await {
            Ok((ws, _)) => {
                info!("[PRICES] Connected to local price server");
                let (mut write, mut read) = ws.split();
                let mut msg_count: u64 = 0;

                while let Some(msg) = read.next().await {
                    match msg {
                        Ok(Message::Text(text)) => {
                            msg_count += 1;
                            trace!("[PRICES] Raw message #{}: {}", msg_count, &text[..text.len().min(200)]);

                            match serde_json::from_str::<LocalPriceUpdate>(&text) {
                                Ok(update) => {
                                    let mut s = state.write().await;
                                    let mut changes = Vec::new();

                                    if let Some(btc) = update.btc_price {
                                        let old = s.prices.btc_price;
                                        s.prices.btc_price = Some(btc);
                                        if old != Some(btc) {
                                            changes.push(format!("BTC: ${:.2}", btc));
                                        }
                                    }
                                    if let Some(eth) = update.eth_price {
                                        let old = s.prices.eth_price;
                                        s.prices.eth_price = Some(eth);
                                        if old != Some(eth) {
                                            changes.push(format!("ETH: ${:.2}", eth));
                                        }
                                    }
                                    if let Some(sol) = update.sol_price {
                                        let old = s.prices.sol_price;
                                        s.prices.sol_price = Some(sol);
                                        if old != Some(sol) {
                                            changes.push(format!("SOL: ${:.4}", sol));
                                        }
                                    }
                                    if let Some(xrp) = update.xrp_price {
                                        let old = s.prices.xrp_price;
                                        s.prices.xrp_price = Some(xrp);
                                        if old != Some(xrp) {
                                            changes.push(format!("XRP: ${:.4}", xrp));
                                        }
                                    }
                                    s.prices.last_update = Some(std::time::Instant::now());

                                    if !changes.is_empty() {
                                        debug!("[PRICES] Updated: {}", changes.join(", "));
                                    }
                                }
                                Err(e) => {
                                    debug!("[PRICES] Parse error: {} - msg: {}", e, &text[..text.len().min(100)]);
                                }
                            }
                        }
                        Ok(Message::Ping(data)) => {
                            trace!("[PRICES] Received ping, sending pong");
                            let _ = write.send(Message::Pong(data)).await;
                        }
                        Ok(Message::Pong(_)) => {
                            trace!("[PRICES] Received pong");
                        }
                        Ok(Message::Close(frame)) => {
                            warn!("[PRICES] Received close frame: {:?}", frame);
                            break;
                        }
                        Ok(Message::Binary(data)) => {
                            debug!("[PRICES] Received binary message: {} bytes", data.len());
                        }
                        Ok(Message::Frame(_)) => {
                            trace!("[PRICES] Received raw frame");
                        }
                        Err(e) => {
                            error!("[PRICES] WebSocket error: {}", e);
                            break;
                        }
                    }
                }
                info!("[PRICES] Connection closed after {} messages", msg_count);
            }
            Err(e) => {
                warn!("[PRICES] Failed to connect: {}", e);
            }
        }

        warn!("[PRICES] Disconnected, reconnecting in 3s...");
        tokio::time::sleep(Duration::from_secs(3)).await;
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
    #[serde(rename = "endDate")]
    end_date: Option<String>,
    #[serde(rename = "enableOrderBook")]
    enable_order_book: Option<bool>,
}

/// Crypto series slugs for 15-minute markets
const POLY_SERIES_SLUGS: &[(&str, &str)] = &[
    ("btc-up-or-down-15m", "BTC"),
    ("eth-up-or-down-15m", "ETH"),
    ("sol-up-or-down-15m", "SOL"),
    ("xrp-up-or-down-15m", "XRP"),
];

/// Discover crypto markets on Polymarket
async fn discover_markets(market_filter: Option<&str>) -> Result<Vec<Market>> {
    info!("[DISCOVER] Starting market discovery, filter={:?}", market_filter);

    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(30))
        .build()?;

    let mut markets = Vec::new();

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

    debug!("[DISCOVER] Checking {} series: {:?}", series_to_check.len(), series_to_check);

    for (series_slug, asset) in series_to_check {
        let url = format!("{}/series?slug={}", GAMMA_API_BASE, series_slug);
        debug!("[DISCOVER] Fetching series: {}", url);

        let resp = client
            .get(&url)
            .header("User-Agent", "poly_ping_pong/1.0")
            .send()
            .await?;

        let status = resp.status();
        debug!("[DISCOVER] Series '{}' response: {}", series_slug, status);

        if !status.is_success() {
            warn!(
                "[DISCOVER] Failed to fetch series '{}': {}",
                series_slug,
                status
            );
            continue;
        }

        let series_list: Vec<GammaSeries> = resp.json().await?;
        let Some(series) = series_list.first() else {
            debug!("[DISCOVER] No series data for '{}'", series_slug);
            continue;
        };
        let Some(events) = &series.events else {
            debug!("[DISCOVER] No events in series '{}'", series_slug);
            continue;
        };

        debug!("[DISCOVER] Series '{}' has {} events", series_slug, events.len());

        let event_slugs: Vec<String> = events
            .iter()
            .filter(|e| e.closed != Some(true) && e.enable_order_book == Some(true))
            .filter_map(|e| e.slug.clone())
            .take(20)
            .collect();

        debug!("[DISCOVER] {} open events with orderbook: {:?}", event_slugs.len(), &event_slugs[..event_slugs.len().min(5)]);

        for event_slug in event_slugs {
            let event_url = format!("{}/events?slug={}", GAMMA_API_BASE, event_slug);
            trace!("[DISCOVER] Fetching event: {}", event_url);

            let resp = match client
                .get(&event_url)
                .header("User-Agent", "poly_ping_pong/1.0")
                .send()
                .await
            {
                Ok(r) => r,
                Err(e) => {
                    debug!("[DISCOVER] Failed to fetch event '{}': {}", event_slug, e);
                    continue;
                }
            };

            let event_details: Vec<serde_json::Value> = match resp.json().await {
                Ok(ed) => ed,
                Err(e) => {
                    debug!("[DISCOVER] Failed to parse event '{}': {}", event_slug, e);
                    continue;
                }
            };

            let Some(ed) = event_details.first() else {
                debug!("[DISCOVER] No event details for '{}'", event_slug);
                continue;
            };
            let Some(mkts) = ed.get("markets").and_then(|m| m.as_array()) else {
                debug!("[DISCOVER] No markets in event '{}'", event_slug);
                continue;
            };

            trace!("[DISCOVER] Event '{}' has {} markets", event_slug, mkts.len());

            for mkt in mkts {
                let condition_id = mkt
                    .get("conditionId")
                    .and_then(|v| v.as_str())
                    .map(|s| s.to_string());
                let clob_tokens_str = mkt
                    .get("clobTokenIds")
                    .and_then(|v| v.as_str())
                    .map(|s| s.to_string());
                let question = mkt
                    .get("question")
                    .and_then(|v| v.as_str())
                    .map(|s| s.to_string())
                    .unwrap_or_else(|| event_slug.clone());
                let end_date_str = mkt
                    .get("endDate")
                    .and_then(|v| v.as_str())
                    .map(|s| s.to_string());

                let Some(cid) = condition_id else {
                    trace!("[DISCOVER] Market missing conditionId");
                    continue;
                };
                let Some(cts) = clob_tokens_str else {
                    trace!("[DISCOVER] Market {} missing clobTokenIds", cid);
                    continue;
                };

                let token_ids: Vec<String> = serde_json::from_str(&cts).unwrap_or_default();
                if token_ids.len() < 2 {
                    trace!("[DISCOVER] Market {} has < 2 tokens", cid);
                    continue;
                }

                let end_time = end_date_str.as_ref().and_then(|d| parse_end_time(d));

                if end_time.is_none() {
                    trace!("[DISCOVER] Market {} has no valid end_time (date={})", cid, end_date_str.as_deref().unwrap_or("none"));
                    continue;
                }

                debug!("[DISCOVER] Found market: {} | {} | end={:?} | yes={} no={}",
                       asset, &question[..question.len().min(40)], end_time, &token_ids[0][..8], &token_ids[1][..8]);

                markets.push(Market {
                    condition_id: cid,
                    question,
                    yes_token: token_ids[0].clone(),
                    no_token: token_ids[1].clone(),
                    asset: asset.to_string(),
                    end_time,
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

    let before_dedup = markets.len();
    let mut seen = std::collections::HashSet::new();
    markets.retain(|m| seen.insert(m.condition_id.clone()));
    debug!("[DISCOVER] Deduplicated: {} -> {} markets", before_dedup, markets.len());

    // Only keep markets expiring within 16 minutes
    let before_filter = markets.len();
    markets.retain(|m| {
        let keep = m.minutes_remaining().map(|mins| mins > 0.0 && mins <= 16.0).unwrap_or(false);
        if !keep {
            trace!("[DISCOVER] Filtered out {} (mins={:?})", m.asset, m.minutes_remaining());
        }
        keep
    });
    debug!("[DISCOVER] Time filter: {} -> {} markets (kept those expiring in 0-16 mins)", before_filter, markets.len());

    markets.sort_by(|a, b| {
        a.end_time
            .cmp(&b.end_time)
    });

    info!("[DISCOVER] Discovery complete: {} markets found", markets.len());
    for m in &markets {
        info!("[DISCOVER]   {} | {:.1}m left | {}", m.asset, m.minutes_remaining().unwrap_or(0.0), &m.question[..m.question.len().min(50)]);
    }

    Ok(markets)
}

fn parse_end_time(end_date: &str) -> Option<chrono::DateTime<chrono::Utc>> {
    let dt = chrono::DateTime::parse_from_rfc3339(end_date).ok()?;
    let utc_dt = dt.with_timezone(&chrono::Utc);
    let now = Utc::now();
    if utc_dt > now {
        Some(utc_dt)
    } else {
        None
    }
}

// === Polymarket WebSocket ===

#[derive(Deserialize, Debug)]
struct BookSnapshot {
    #[serde(alias = "market")]
    asset_id: String,
    bids: Vec<PriceLevel>,
    asks: Vec<PriceLevel>,
}

#[derive(Deserialize, Debug)]
struct PriceLevel {
    price: String,
    size: String,
}

/// Wrapped message format that Polymarket might use
#[derive(Deserialize, Debug)]
struct WrappedBookMessage {
    #[serde(default)]
    data: Vec<BookSnapshot>,
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

// === Main ===

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::from_default_env()
                .add_directive("poly_ping_pong=info".parse().unwrap())
                .add_directive("arb_bot=info".parse().unwrap()),
        )
        .with_target(false)
        .with_level(false)
        .with_timer(tracing_subscriber::fmt::time::LocalTime::new(
            time::macros::format_description!("[hour]:[minute]:[second]")
        ))
        .init();

    let args = Args::parse();

    println!("═══════════════════════════════════════════════════════════════════════");
    println!("POLYMARKET PING PONG BOT");
    println!("═══════════════════════════════════════════════════════════════════════");
    println!("Symbol: {}  |  Mode: {}  |  Max: ${:.0}",
             args.sym.to_uppercase(),
             if args.live { "LIVE" } else { "DRY RUN" },
             args.max_dollars);
    println!("Threshold: {}c / {}c  |  Max arb: {}c  |  Time: {}m-{}m  |  Max age: {:.0}m",
             args.threshold, 100 - args.threshold, args.max_arb_cost,
             args.min_minutes, args.max_minutes, args.max_age);
    println!("═══════════════════════════════════════════════════════════════════════");

    dotenvy::dotenv().ok();
    let private_key = std::env::var("POLY_PRIVATE_KEY").context("POLY_PRIVATE_KEY not set")?;
    let funder = std::env::var("POLY_FUNDER").context("POLY_FUNDER not set")?;

    let poly_client = PolymarketAsyncClient::new("https://clob.polymarket.com", 137, &private_key, &funder)?;

    println!("Deriving API credentials...");
    let api_creds = poly_client.derive_api_key(0).await?;
    let prepared_creds = PreparedCreds::from_api_creds(&api_creds)?;
    let shared_client = Arc::new(SharedAsyncClient::new(poly_client, prepared_creds, 137));

    if args.live {
        println!("*** LIVE MODE - Real money! ***");
        tokio::time::sleep(Duration::from_secs(3)).await;
    }

    // Filter by symbol
    let symbol_filter: Option<String> = if args.sym.to_lowercase() == "all" {
        None
    } else {
        Some(args.sym.to_lowercase())
    };

    println!("Discovering markets...");
    let discovered = discover_markets(symbol_filter.as_deref()).await?;
    println!("Found {} markets", discovered.len());

    for m in &discovered {
        println!("  {} | {:.1}m left | {}",
                 m.asset,
                 m.minutes_remaining().unwrap_or(0.0),
                 &m.question[..m.question.len().min(50)]);
    }

    if discovered.is_empty() {
        println!("No markets found!");
        return Ok(());
    }

    let state = Arc::new(RwLock::new({
        let mut s = State::new();
        for m in discovered {
            let id = m.condition_id.clone();
            s.positions.insert(id.clone(), Position::default());
            s.markets.insert(id, m);
        }
        s
    }));

    // Start price feed
    let state_price = state.clone();
    tokio::spawn(async move {
        run_price_feed(state_price).await;
    });

    let threshold = args.threshold;
    let max_arb_cost = args.max_arb_cost;
    let max_dollars = args.max_dollars;
    let dry_run = !args.live;
    let min_minutes = args.min_minutes as f64;
    let max_minutes = args.max_minutes as f64;
    let max_age = args.max_age;
    let log_filter = symbol_filter.clone();

    loop {
        // Get current tokens from state (may have been updated by re-discovery)
        let tokens: Vec<String> = {
            let s = state.read().await;
            s.markets
                .values()
                .flat_map(|m| vec![m.yes_token.clone(), m.no_token.clone()])
                .collect()
        };

        if tokens.is_empty() {
            info!("[DISCOVER] No markets in state, discovering new markets...");
            match discover_markets(symbol_filter.as_deref()).await {
                Ok(discovered) => {
                    if discovered.is_empty() {
                        warn!("[DISCOVER] No markets found, waiting 30s...");
                        tokio::time::sleep(Duration::from_secs(30)).await;
                        continue;
                    }
                    let mut s = state.write().await;
                    for m in discovered {
                        let id = m.condition_id.clone();
                        s.positions.entry(id.clone()).or_insert_with(Position::default);
                        s.markets.insert(id, m);
                    }
                    info!("[DISCOVER] Added {} new markets to state", s.markets.len());
                }
                Err(e) => {
                    error!("[DISCOVER] Discovery failed: {}", e);
                    tokio::time::sleep(Duration::from_secs(10)).await;
                    continue;
                }
            }
            continue; // Re-loop to get the tokens
        }

        info!("[WS] Connecting to Polymarket WebSocket {}...", POLYMARKET_WS_URL);

        let (ws, _) = match connect_async(POLYMARKET_WS_URL).await {
            Ok(ws) => ws,
            Err(e) => {
                error!("[WS] Connect failed: {}", e);
                tokio::time::sleep(Duration::from_secs(5)).await;
                continue;
            }
        };

        let (mut write, mut read) = ws.split();

        let sub = SubscribeCmd {
            assets_ids: tokens.clone(),
            sub_type: "market",
        };
        let _ = write.send(Message::Text(serde_json::to_string(&sub)?)).await;
        info!("[WS] Connected! Subscribed to {} tokens across {} markets", tokens.len(), tokens.len() / 2);
        println!("═════════════════════════════════════════════════════════════════════════════════════════════════════════════");
        println!("STRATEGY: poly_ping_pong - Buy when price crashes to threshold, arb when YES+NO is cheap");
        println!("═════════════════════════════════════════════════════════════════════════════════════════════════════════════");
        println!("Sym PIN spot=price | age=market age | left=time to expiry | YES bid/ask | NO bid/ask | liq=liquidity");
        println!("═════════════════════════════════════════════════════════════════════════════════════════════════════════════");

        let mut ping_interval = tokio::time::interval(Duration::from_secs(30));
        let mut status_interval = tokio::time::interval(Duration::from_secs(1));
        let mut discovery_interval = tokio::time::interval(Duration::from_secs(60)); // Check for new markets every minute
        discovery_interval.tick().await; // Skip first tick

        loop {
            tokio::select! {
                _ = discovery_interval.tick() => {
                    // Periodic discovery to catch new markets when they start
                    info!("[DISCOVER] Checking for new markets...");
                    match discover_markets(symbol_filter.as_deref()).await {
                        Ok(discovered) => {
                            let mut s = state.write().await;
                            let mut new_count = 0;
                            for m in discovered {
                                if !s.markets.contains_key(&m.condition_id) {
                                    info!("[DISCOVER] New market: {} exp:{} ({:.1}m left)",
                                          m.asset,
                                          m.end_time.map(|t| t.format("%H:%M").to_string()).unwrap_or_default(),
                                          m.minutes_remaining().unwrap_or(0.0));
                                    let id = m.condition_id.clone();
                                    s.positions.entry(id.clone()).or_insert_with(Position::default);
                                    s.markets.insert(id, m);
                                    new_count += 1;
                                }
                            }
                            if new_count > 0 {
                                info!("[DISCOVER] Added {} new markets, reconnecting to subscribe...", new_count);
                                break; // Break to reconnect and subscribe to new tokens
                            }
                        }
                        Err(e) => {
                            debug!("[DISCOVER] Periodic discovery failed: {}", e);
                        }
                    }
                }

                _ = ping_interval.tick() => {
                    trace!("[WS] Sending ping...");
                    if let Err(e) = write.send(Message::Ping(vec![])).await {
                        error!("[WS] Ping failed: {}", e);
                        break;
                    }
                    trace!("[WS] Ping sent successfully");
                }

                _ = status_interval.tick() => {
                    // Check for expired markets and remove them
                    let needs_rediscovery = {
                        let mut s = state.write().await;
                        let expired_ids: Vec<String> = s.markets
                            .iter()
                            .filter(|(_, m)| {
                                m.minutes_remaining().map(|mins| mins < -1.0).unwrap_or(true)
                            })
                            .map(|(id, _)| id.clone())
                            .collect();

                        for id in &expired_ids {
                            if let Some(m) = s.markets.remove(id) {
                                info!("[EXPIRE] Removed expired market: {} {} ({})",
                                      m.asset, m.pin_name(), &id[..16]);
                            }
                            s.positions.remove(id);
                        }

                        s.markets.is_empty()
                    };

                    // If all markets expired, break to re-discover
                    if needs_rediscovery {
                        info!("[DISCOVER] All markets expired, breaking to re-discover...");
                        break;
                    }

                    let s = state.read().await;
                    let now = Utc::now().format("%H:%M:%S");

                    // Price feed staleness
                    let price_age_secs = s.prices.last_update
                        .map(|t| t.elapsed().as_secs())
                        .unwrap_or(999);
                    let price_status = if price_age_secs < 5 { "●" } else if price_age_secs < 30 { "○" } else { "✗" };

                    for (id, market) in &s.markets {
                        // Filter by symbol if specified
                        if let Some(ref filter) = log_filter {
                            if !market.asset.to_lowercase().contains(filter) {
                                continue;
                            }
                        }

                        let pos = s.positions.get(id).cloned().unwrap_or_default();
                        let expiry = market.minutes_remaining().unwrap_or(0.0);
                        let market_age = 15.0 - expiry; // How many minutes into the market

                        // Determine filter status (use "left" consistently)
                        let filter_reason = if expiry < min_minutes {
                            Some(format!("EXPIRING (left {:.1}m < {:.0}m)", expiry, min_minutes))
                        } else if expiry > max_minutes {
                            Some(format!("WAITING (left {:.1}m > {:.0}m)", expiry, max_minutes))
                        } else if market_age > max_age {
                            Some(format!("TOO OLD (left {:.1}m, need >{:.0}m)", expiry, 15.0 - max_age))
                        } else {
                            None
                        };

                        // Get spot price for this asset
                        let spot = match market.asset.as_str() {
                            "BTC" => s.prices.btc_price.map(|p| format!("${:.0}", p)),
                            "ETH" => s.prices.eth_price.map(|p| format!("${:.0}", p)),
                            "SOL" => s.prices.sol_price.map(|p| format!("${:.2}", p)),
                            "XRP" => s.prices.xrp_price.map(|p| format!("${:.4}", p)),
                            _ => None,
                        }.unwrap_or_else(|| "-".to_string());

                        let yes_ask = market.yes_ask.unwrap_or(100);
                        let no_ask = market.no_ask.unwrap_or(100);
                        let yes_bid = market.yes_bid.unwrap_or(0);
                        let no_bid = market.no_bid.unwrap_or(0);
                        let combined = yes_ask + no_ask;

                        // Arb analysis
                        let arb_profit = if combined < 100 { 100 - combined } else { 0 };
                        let _arb_flag = if combined <= max_arb_cost {
                            format!("*ARB +{}c*", arb_profit)
                        } else {
                            format!("gap={}c", combined - 100)
                        };

                        // Distance from threshold (use bid to show where market actually IS)
                        // Skip NEAR when no real orderbook (bid=0)
                        let near_flag = if yes_bid > 0 && no_bid > 0 {
                            let yes_bid_to_thresh = yes_bid as i64 - threshold;
                            let no_bid_to_thresh = no_bid as i64 - threshold;
                            if yes_bid_to_thresh <= 5 || no_bid_to_thresh <= 5 {
                                format!(" NEAR(Y{}c N{}c)", yes_bid_to_thresh, no_bid_to_thresh)
                            } else {
                                String::new()
                            }
                        } else {
                            String::new()
                        };

                        // Position display - use dollars
                        let current_invested = pos.total_cost();
                        let pos_str = if current_invested > 0.0 {
                            let matched = pos.matched();
                            let unrealized = matched * 1.0 - current_invested;
                            format!("${:.2}/${:.0} PnL=${:.2}", current_invested, max_dollars, unrealized)
                        } else {
                            format!("$0/${:.0}", max_dollars)
                        };

                        // Mid price implied probability
                        let yes_mid = (yes_bid + yes_ask) as f64 / 2.0;
                        let implied_prob = format!("prob={:.0}%", yes_mid);

                        // Skip markets that are filtered out (too old, expiring, waiting)
                        if filter_reason.is_some() {
                            continue;
                        }

                        // Format pin_name with spacing if non-empty
                        let pin = market.pin_name();
                        let pin_str = if pin.is_empty() { String::new() } else { format!(" {}", pin) };

                        println!(
                            "{} {} [ping_pong] {}{} spot={} | left={:.1}m th={}c | Y {}/{}c N {}/{}c | {} | {}{}",
                            now,
                            price_status,
                            market.asset,
                            pin_str,
                            spot,
                            expiry,
                            threshold,
                            yes_bid, yes_ask,
                            no_bid, no_ask,
                            implied_prob,
                            pos_str,
                            near_flag
                        );
                    }
                }

                msg = read.next() => {
                    let Some(msg) = msg else {
                        info!("[WS] Stream ended (no more messages)");
                        break;
                    };

                    match msg {
                        Ok(Message::Text(text)) => {
                            debug!("[WS] Raw message: {}", &text[..text.len().min(300)]);

                            // Try parsing in multiple formats:
                            // 1. Array of BookSnapshot (initial snapshots)
                            // 2. Single BookSnapshot (individual updates)
                            // 3. Wrapped message with data field
                            // 4. PriceChangeMessage (real-time price updates)
                            let books: Vec<BookSnapshot> = serde_json::from_str::<Vec<BookSnapshot>>(&text)
                                .or_else(|_| serde_json::from_str::<BookSnapshot>(&text).map(|s| vec![s]))
                                .or_else(|_| serde_json::from_str::<WrappedBookMessage>(&text).map(|w| w.data))
                                .unwrap_or_default();

                            // Handle price change messages (real-time updates)
                            let mut handled_price_change = false;
                            let mut trade_signals: Vec<(String, String, String, i64, bool)> = Vec::new(); // (market_id, asset, token, ask, is_yes)

                            if let Ok(price_msg) = serde_json::from_str::<PriceChangeMessage>(&text) {
                                if !price_msg.price_changes.is_empty() {
                                    handled_price_change = true;
                                    let mut s = state.write().await;
                                    for pc in price_msg.price_changes {
                                        // Find market for this asset
                                        let market_entry = s.markets.iter_mut()
                                            .find(|(_, m)| m.yes_token == pc.asset_id || m.no_token == pc.asset_id);

                                        if let Some((_, market)) = market_entry {
                                            let is_yes = pc.asset_id == market.yes_token;
                                            let price_cents = pc.price.parse::<f64>()
                                                .map(|p| (p * 100.0).round() as i64)
                                                .unwrap_or(0);

                                            // Set bid/ask based on the price update (assume 1c spread)
                                            let (old_ask, _old_bid) = if is_yes {
                                                let old = (market.yes_ask, market.yes_bid);
                                                market.yes_bid = Some(price_cents.saturating_sub(1).max(1));
                                                market.yes_ask = Some((price_cents + 1).min(99));
                                                old
                                            } else {
                                                let old = (market.no_ask, market.no_bid);
                                                market.no_bid = Some(price_cents.saturating_sub(1).max(1));
                                                market.no_ask = Some((price_cents + 1).min(99));
                                                old
                                            };

                                            let side = if is_yes { "YES" } else { "NO" };
                                            let new_ask = (price_cents + 1).min(99);
                                            if old_ask != Some(new_ask) {
                                                trace!("[PRICE] {} {} | price={:.2} -> ask={}c",
                                                       market.asset, side, pc.price.parse::<f64>().unwrap_or(0.0), new_ask);
                                            }

                                            // Check for trade opportunity on price update
                                            let mins = market.minutes_remaining().unwrap_or(0.0);
                                            let market_age = 15.0 - mins;
                                            if mins >= min_minutes && mins <= max_minutes && market_age <= max_age
                                               && new_ask <= threshold && new_ask > 0 {
                                                trade_signals.push((
                                                    market.condition_id.clone(),
                                                    market.asset.clone(),
                                                    pc.asset_id.clone(),
                                                    new_ask,
                                                    is_yes
                                                ));
                                            }
                                        }
                                    }
                                }
                            }

                            // Execute trades outside the mutable borrow
                            for (market_id, asset, token, new_ask, is_yes) in trade_signals {
                                let current_invested = {
                                    let s = state.read().await;
                                    s.positions.get(&market_id).map(|p| p.total_cost()).unwrap_or(0.0)
                                };

                                if current_invested >= max_dollars {
                                    continue;
                                }

                                let side = if is_yes { "YES" } else { "NO" };
                                let entry_price = (new_ask + 2).min(99);
                                let cross_price = entry_price as f64 / 100.0;
                                let min_contracts = (1.0 / cross_price).ceil();
                                let cost = min_contracts * cross_price;

                                if !dry_run {
                                    info!("[ping_pong] 🎯 BUYING {} {} | ask {}c < {}c threshold | {:.0} @{}c = ${:.2}",
                                          asset, side, new_ask, threshold, min_contracts, entry_price, cost);
                                    let client = shared_client.clone();
                                    let state_clone = state.clone();
                                    let market_id_spawn = market_id.clone();
                                    let asset_clone = asset.clone();

                                    tokio::spawn(async move {
                                        match client.buy_fak(&token, cross_price, min_contracts).await {
                                            Ok(fill) if fill.filled_size > 0.0 => {
                                                info!("[ping_pong] ✅ {} {} FILLED {:.1} @${:.2}",
                                                      asset_clone, side, fill.filled_size, fill.fill_cost);
                                                let mut s = state_clone.write().await;
                                                if let Some(pos) = s.positions.get_mut(&market_id_spawn) {
                                                    if is_yes {
                                                        pos.yes_qty += fill.filled_size;
                                                        pos.yes_cost += fill.fill_cost;
                                                    } else {
                                                        pos.no_qty += fill.filled_size;
                                                        pos.no_cost += fill.fill_cost;
                                                    }
                                                    pos.bought_crash = true;
                                                }
                                            }
                                            Ok(_) => info!("[ping_pong] ❌ {} {} no fill", asset_clone, side),
                                            Err(e) => error!("[ping_pong] ❌ {} {} error: {}", asset_clone, side, e),
                                        }
                                    });
                                } else {
                                    info!("[ping_pong] 🎯 BUYING {} {} | ask {}c < {}c threshold | {:.0} @{}c = ${:.2} (DRY RUN)",
                                          asset, side, new_ask, threshold, min_contracts, entry_price, cost);
                                }
                            }

                            if !books.is_empty() {
                                debug!("[WS] Parsed {} book snapshots", books.len());

                                    for book in books {
                                        let mut s = state.write().await;

                                        let market_id = s
                                            .markets
                                            .iter()
                                            .find(|(_, m)| {
                                                m.yes_token == book.asset_id || m.no_token == book.asset_id
                                            })
                                            .map(|(id, _)| id.clone());

                                        let Some(market_id) = market_id else {
                                            debug!("[WS] Unknown asset_id: {}", &book.asset_id[..book.asset_id.len().min(16)]);
                                            continue;
                                        };

                                        let best_ask = book
                                            .asks
                                            .iter()
                                            .filter_map(|l| {
                                                let price = parse_price_cents(&l.price);
                                                if price > 0 {
                                                    Some((price, parse_size(&l.size)))
                                                } else {
                                                    None
                                                }
                                            })
                                            .min_by_key(|(p, _)| *p);

                                        let best_bid = book
                                            .bids
                                            .iter()
                                            .filter_map(|l| {
                                                let price = parse_price_cents(&l.price);
                                                if price > 0 {
                                                    Some((price, parse_size(&l.size)))
                                                } else {
                                                    None
                                                }
                                            })
                                            .max_by_key(|(p, _)| *p);

                                        // Total liquidity across all ask levels
                                        let total_ask_size: f64 = book
                                            .asks
                                            .iter()
                                            .map(|l| parse_size(&l.size))
                                            .sum();

                                        // Total bid liquidity
                                        let total_bid_size: f64 = book
                                            .bids
                                            .iter()
                                            .map(|l| parse_size(&l.size))
                                            .sum();

                                        let Some(market) = s.markets.get_mut(&market_id) else {
                                            debug!("[WS] Market {} not found in state", &market_id[..16]);
                                            continue;
                                        };

                                        let is_yes = book.asset_id == market.yes_token;
                                        let side = if is_yes { "YES" } else { "NO" };

                                        // Track old values for change detection
                                        let (old_ask, old_bid, old_size) = if is_yes {
                                            (market.yes_ask, market.yes_bid, market.yes_ask_size)
                                        } else {
                                            (market.no_ask, market.no_bid, market.no_ask_size)
                                        };

                                        if is_yes {
                                            market.yes_ask = best_ask.map(|(p, _)| p);
                                            market.yes_bid = best_bid.map(|(p, _)| p);
                                            market.yes_ask_size = total_ask_size;
                                        } else {
                                            market.no_ask = best_ask.map(|(p, _)| p);
                                            market.no_bid = best_bid.map(|(p, _)| p);
                                            market.no_ask_size = total_ask_size;
                                        }

                                        // Log orderbook changes
                                        let new_ask = best_ask.map(|(p, _)| p);
                                        let new_bid = best_bid.map(|(p, _)| p);
                                        let ask_changed = old_ask != new_ask;
                                        let bid_changed = old_bid != new_bid;
                                        let size_changed = (old_size - total_ask_size).abs() > 0.01;

                                        if ask_changed || bid_changed {
                                            info!("[BOOK] {} {} | bid: {:?}c->{:?}c | ask: {:?}c->{:?}c",
                                                   market.asset, side,
                                                   old_bid, new_bid,
                                                   old_ask, new_ask);
                                        } else if size_changed {
                                            debug!("[BOOK] {} {} | liq: {:.0}->{:.0} (bids={} asks={})",
                                                   market.asset, side,
                                                   old_size, total_ask_size,
                                                   book.bids.len(), book.asks.len());
                                        } else {
                                            trace!("[BOOK] {} {} unchanged | bid={:?}c ask={:?}c liq={:.0}",
                                                   market.asset, side, new_bid, new_ask, total_ask_size);
                                        }

                                        // Log full book depth at trace level
                                        if !book.asks.is_empty() || !book.bids.is_empty() {
                                            trace!("[BOOK] {} {} depth: {} bids (total={:.0}) / {} asks (total={:.0})",
                                                   market.asset, side,
                                                   book.bids.len(), total_bid_size,
                                                   book.asks.len(), total_ask_size);
                                        }

                                        // Extract values
                                        let asset = market.asset.clone();
                                        let mins = market.minutes_remaining().unwrap_or(0.0);
                                        let yes_ask_price = market.yes_ask;
                                        let no_ask_price = market.no_ask;
                                        let yes_token = market.yes_token.clone();
                                        let no_token = market.no_token.clone();
                                        let _question = market.question.clone();

                                        // Get underlying price for this asset
                                        let underlying_price = match asset.as_str() {
                                            "BTC" => s.prices.btc_price,
                                            "ETH" => s.prices.eth_price,
                                            "SOL" => s.prices.sol_price,
                                            "XRP" => s.prices.xrp_price,
                                            _ => None,
                                        };

                                        let market_age = 15.0 - mins; // How many minutes into the market

                                        // Log filtering decisions
                                        if mins < min_minutes {
                                            trace!("[FILTER] {} skipped: mins={:.1} < min_minutes={:.1}", asset, mins, min_minutes);
                                            continue;
                                        }
                                        if mins > max_minutes {
                                            trace!("[FILTER] {} skipped: mins={:.1} > max_minutes={:.1}", asset, mins, max_minutes);
                                            continue;
                                        }
                                        if market_age > max_age {
                                            trace!("[FILTER] {} skipped: age={:.1}m > max_age={:.1}m", asset, market_age, max_age);
                                            continue;
                                        }

                                        let yes_ask = yes_ask_price.unwrap_or(100);
                                        let no_ask = no_ask_price.unwrap_or(100);

                                        let market_id_clone = market_id.clone();
                                        drop(s);

                                        // Format underlying price
                                        let _spot_str = underlying_price
                                            .map(|p| format!("${:.2}", p))
                                            .unwrap_or_else(|| "-".to_string());

                                        // Check current invested dollars
                                        let current_invested = {
                                            let s = state.read().await;
                                            s.positions.get(&market_id_clone)
                                                .map(|p| p.total_cost())
                                                .unwrap_or(0.0)
                                        };

                                        if current_invested >= max_dollars {
                                            debug!("[POSITION] {} max reached (${:.2}/${:.0}), skipping",
                                                   asset, current_invested, max_dollars);
                                            continue;
                                        }

                                        let remaining_dollars = max_dollars - current_invested;

                                        // === STRATEGY 1: Buy on crash ===
                                        // Buy YES if it drops to threshold threshold
                                        if yes_ask <= threshold && yes_ask > 0 {
                                            // Add 2c to cross the spread and ensure FAK fills
                                            let entry_price = (yes_ask + 2).min(99);
                                            let cross_price = entry_price as f64 / 100.0;

                                            // Polymarket requires minimum $1 order value
                                            let min_contracts = (1.0 / cross_price).ceil();

                                            // Check if we have capacity
                                            if remaining_dollars < 1.0 {
                                                debug!("[ping_pong] {} skip: only ${:.2} remaining", asset, remaining_dollars);
                                                continue;
                                            }

                                            // Use minimum contracts to meet $1 threshold
                                            {
                                                let actual_contracts = min_contracts;
                                                let cost = actual_contracts * cross_price;

                                                if dry_run {
                                                    info!("[ping_pong] 🎯 BUYING {} YES | ask {}c < {}c threshold | {:.0} @{}c = ${:.2} | age {:.1}m (DRY RUN)",
                                                          asset, yes_ask, threshold, actual_contracts, entry_price, cost, market_age);
                                                } else {
                                                    info!("[ping_pong] 🎯 BUYING {} YES | ask {}c < {}c threshold | {:.0} @{}c = ${:.2} | age {:.1}m",
                                                          asset, yes_ask, threshold, actual_contracts, entry_price, cost, market_age);
                                                    let client = shared_client.clone();
                                                    let state_clone = state.clone();
                                                    let yes_token_clone = yes_token.clone();
                                                    let market_id_for_spawn = market_id_clone.clone();
                                                    let asset_clone = asset.clone();

                                                    // Execute trade asynchronously
                                                    tokio::spawn(async move {
                                                        match client.buy_fak(&yes_token_clone, cross_price, actual_contracts).await {
                                                            Ok(fill) if fill.filled_size > 0.0 => {
                                                                info!("[ping_pong] ✅ {} YES FILLED {:.1} @${:.2}",
                                                                      asset_clone, fill.filled_size, fill.fill_cost);

                                                                let mut s = state_clone.write().await;
                                                                if let Some(pos) = s.positions.get_mut(&market_id_for_spawn) {
                                                                    pos.yes_qty += fill.filled_size;
                                                                    pos.yes_cost += fill.fill_cost;
                                                                    pos.bought_crash = true;
                                                                }
                                                            }
                                                            Ok(_) => {
                                                                info!("[ping_pong] ❌ {} YES no fill (FAK killed)", asset_clone);
                                                            }
                                                            Err(e) => {
                                                                error!("[ping_pong] ❌ {} YES order error: {}", asset_clone, e);
                                                            }
                                                        }
                                                    });
                                                }
                                            }
                                        }

                                        // Buy NO if it drops to threshold
                                        if no_ask <= threshold && no_ask > 0 {
                                            // Add 2c to cross the spread and ensure FAK fills
                                            let entry_price = (no_ask + 2).min(99);
                                            let cross_price = entry_price as f64 / 100.0;

                                            // Polymarket requires minimum $1 order value
                                            let min_contracts = (1.0 / cross_price).ceil();

                                            // Check if we have capacity
                                            if remaining_dollars < 1.0 {
                                                debug!("[ping_pong] {} skip: only ${:.2} remaining", asset, remaining_dollars);
                                                continue;
                                            }

                                            let actual_contracts = min_contracts;
                                            let cost = actual_contracts * cross_price;

                                            if dry_run {
                                                info!("[ping_pong] 🎯 BUYING {} NO | ask {}c < {}c threshold | {:.0} @{}c = ${:.2} | age {:.1}m (DRY RUN)",
                                                      asset, no_ask, threshold, actual_contracts, entry_price, cost, market_age);
                                            } else {
                                                info!("[ping_pong] 🎯 BUYING {} NO | ask {}c < {}c threshold | {:.0} @{}c = ${:.2} | age {:.1}m",
                                                      asset, no_ask, threshold, actual_contracts, entry_price, cost, market_age);
                                                let client = shared_client.clone();
                                                let state_clone = state.clone();
                                                let no_token_clone = no_token.clone();
                                                let market_id_for_spawn = market_id_clone.clone();
                                                let asset_clone = asset.clone();

                                                // Execute trade asynchronously
                                                tokio::spawn(async move {
                                                    match client.buy_fak(&no_token_clone, cross_price, actual_contracts).await {
                                                        Ok(fill) if fill.filled_size > 0.0 => {
                                                            info!("[ping_pong] ✅ {} NO FILLED {:.1} @${:.2}",
                                                                  asset_clone, fill.filled_size, fill.fill_cost);

                                                            let mut s = state_clone.write().await;
                                                            if let Some(pos) = s.positions.get_mut(&market_id_for_spawn) {
                                                                pos.no_qty += fill.filled_size;
                                                                pos.no_cost += fill.fill_cost;
                                                                pos.bought_crash = true;
                                                            }
                                                        }
                                                        Ok(_) => {
                                                            info!("[ping_pong] ❌ {} NO no fill (FAK killed)", asset_clone);
                                                        }
                                                        Err(e) => {
                                                            error!("[ping_pong] ❌ {} NO order error: {}", asset_clone, e);
                                                        }
                                                    }
                                                });
                                            }
                                        }

                                        // === STRATEGY 2: Arb when both sides cheap ===
                                        // If YES + NO <= max_arb_cost, buy both for guaranteed profit
                                        let combined = yes_ask + no_ask;
                                        if combined <= max_arb_cost && yes_ask > 0 && no_ask > 0 {
                                            let profit = 100 - combined;

                                            // Add 2c to cross spreads
                                            let yes_entry = (yes_ask + 2).min(99);
                                            let no_entry = (no_ask + 2).min(99);
                                            let yes_cross = yes_entry as f64 / 100.0;
                                            let no_cross = no_entry as f64 / 100.0;

                                            // Polymarket requires minimum $1 order value - use min for both sides
                                            let min_contracts = (1.0 / yes_cross.min(no_cross)).ceil();
                                            let actual_contracts = min_contracts;
                                            let total_cost = actual_contracts * (yes_cross + no_cross);

                                            // Check if we have capacity for arb
                                            if remaining_dollars < total_cost {
                                                debug!("[ping_pong] {} skip arb: need ${:.2}, only ${:.2} remaining", asset, total_cost, remaining_dollars);
                                                continue;
                                            }

                                            if dry_run {
                                                info!("[ping_pong] 🎯 ARB {} | Y {}c + N {}c = {}c < $1 | {:.0} each @{}c/{}c = ${:.2} | +{}c profit | age {:.1}m (DRY RUN)",
                                                      asset, yes_ask, no_ask, combined, actual_contracts, yes_entry, no_entry, total_cost, profit, market_age);
                                            } else {
                                                info!("[ping_pong] 🎯 ARB {} | Y {}c + N {}c = {}c < $1 | {:.0} each @{}c/{}c = ${:.2} | +{}c profit | age {:.1}m",
                                                      asset, yes_ask, no_ask, combined, actual_contracts, yes_entry, no_entry, total_cost, profit, market_age);
                                                // Buy YES
                                                let client = shared_client.clone();
                                                let state_clone = state.clone();
                                                let yes_token_clone = yes_token.clone();
                                                let market_id_for_spawn = market_id_clone.clone();
                                                let asset_clone = asset.clone();

                                                tokio::spawn(async move {
                                                    match client.buy_fak(&yes_token_clone, yes_cross, actual_contracts).await {
                                                        Ok(fill) if fill.filled_size > 0.0 => {
                                                            info!("[ping_pong] ✅ {} ARB YES FILLED {:.1} @${:.2}",
                                                                  asset_clone, fill.filled_size, fill.fill_cost);

                                                            let mut s = state_clone.write().await;
                                                            if let Some(pos) = s.positions.get_mut(&market_id_for_spawn) {
                                                                pos.yes_qty += fill.filled_size;
                                                                pos.yes_cost += fill.fill_cost;
                                                            }
                                                        }
                                                        Ok(_) => {
                                                            info!("[ping_pong] ❌ {} ARB YES no fill", asset_clone);
                                                        }
                                                        Err(e) => {
                                                            error!("[ping_pong] ❌ {} ARB YES error: {}", asset_clone, e);
                                                        }
                                                    }
                                                });

                                                // Buy NO
                                                let client = shared_client.clone();
                                                let state_clone = state.clone();
                                                let no_token_clone = no_token.clone();
                                                let market_id_for_spawn = market_id_clone.clone();
                                                let asset_clone = asset.clone();

                                                tokio::spawn(async move {
                                                    match client.buy_fak(&no_token_clone, no_cross, actual_contracts).await {
                                                        Ok(fill) if fill.filled_size > 0.0 => {
                                                            info!("[ping_pong] ✅ {} ARB NO FILLED {:.1} @${:.2}",
                                                                  asset_clone, fill.filled_size, fill.fill_cost);

                                                            let mut s = state_clone.write().await;
                                                            if let Some(pos) = s.positions.get_mut(&market_id_for_spawn) {
                                                                pos.no_qty += fill.filled_size;
                                                                pos.no_cost += fill.fill_cost;
                                                            }
                                                        }
                                                        Ok(_) => {
                                                            info!("[ping_pong] ❌ {} ARB NO no fill", asset_clone);
                                                        }
                                                        Err(e) => {
                                                            error!("[ping_pong] ❌ {} ARB NO error: {}", asset_clone, e);
                                                        }
                                                    }
                                                });
                                            }
                                        }
                                    }
                            } else if !text.is_empty() && !handled_price_change {
                                // Failed to parse as book snapshot or price change - log what we received
                                if text.contains("\"type\"") {
                                    debug!("[WS] Non-book message: {}", &text[..text.len().min(100)]);
                                } else {
                                    debug!("[WS] Unknown message format: {}", &text[..text.len().min(200)]);
                                }
                            }
                        }
                        Ok(Message::Ping(data)) => {
                            trace!("[WS] Received ping, sending pong");
                            let _ = write.send(Message::Pong(data)).await;
                        }
                        Ok(Message::Pong(_)) => {
                            trace!("[WS] Received pong");
                        }
                        Ok(Message::Binary(data)) => {
                            debug!("[WS] Received binary: {} bytes", data.len());
                        }
                        Ok(Message::Close(frame)) => {
                            warn!("[WS] Received close frame: {:?}", frame);
                            break;
                        }
                        Ok(Message::Frame(_)) => {
                            trace!("[WS] Received raw frame");
                        }
                        Err(e) => {
                            error!("[WS] WebSocket error: {}", e);
                            break;
                        }
                    }
                }
            }
        }

        println!("Disconnected, reconnecting in 5s...");
        tokio::time::sleep(Duration::from_secs(5)).await;
    }
}
