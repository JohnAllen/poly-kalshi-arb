//! Polymarket Ping Pong Accumulator Bot
//!
//! STRATEGY:
//! 1. ACCUMULATOR PHASE (market has > endgame_mins left):
//!    - Buy shares on BOTH sides at roughly even sizes
//!    - Buy at regular intervals (every buy_interval seconds)
//!    - Build an average price position as the market progresses
//!
//! 2. ENDGAME PHASE (market has <= endgame_mins left):
//!    - If one side is "way up" (bid >= way_up threshold, e.g., 92c), buy that side
//!    - This captures the likely winner when probability is very high
//!
//! Usage:
//!   cargo run --release --bin poly_ping_pong -- --buy-interval 30 --buy-size 2 --way-up 92 --endgame-mins 2
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

/// Polymarket Ping Pong Accumulator Bot
#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Args {
    /// Symbol to trade: BTC, ETH, SOL, XRP (default: all)
    #[arg(long, default_value = "all")]
    sym: String,

    /// Max total dollars to invest per market (default: $10)
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

    /// Maximum market age in minutes to start accumulating (default: 7)
    #[arg(long, default_value_t = 7.0)]
    max_age: f64,

    // === ACCUMULATOR STRATEGY PARAMS ===

    /// Buy interval in seconds - how often to check and buy (default: 1)
    #[arg(long, default_value_t = 1)]
    buy_interval: u64,

    /// Size per buy in contracts (default: 2)
    #[arg(long, default_value_t = 2.0)]
    buy_size: f64,

    /// Endgame threshold in minutes - when to switch to endgame mode (default: 2)
    #[arg(long, default_value_t = 2.0)]
    endgame_mins: f64,

    /// "Way up" threshold in cents - buy this side in endgame if bid >= this (default: 92)
    #[arg(long, default_value_t = 92)]
    way_up: i64,

    /// Endgame buy size multiplier (default: 3x normal size)
    #[arg(long, default_value_t = 3.0)]
    endgame_mult: f64,
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

/// Accumulator state per market
#[derive(Debug, Clone, Default)]
struct AccumulatorState {
    /// Last time we bought during accumulator phase
    last_accumulator_buy: Option<std::time::Instant>,
    /// Whether we've done the endgame buy
    endgame_bought: bool,
    /// Which side we bought in endgame (true=YES, false=NO)
    endgame_side: Option<bool>,
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
    /// Last order attempt per token (to prevent spam)
    last_order: HashMap<String, std::time::Instant>,
    /// Accumulator state per market
    accumulator: HashMap<String, AccumulatorState>,
}

impl State {
    fn new() -> Self {
        Self {
            markets: HashMap::new(),
            positions: HashMap::new(),
            prices: PriceState::default(),
            last_order: HashMap::new(),
            accumulator: HashMap::new(),
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
                                    info!("[PRICES] Parse error: {} - msg: {}", e, &text[..text.len().min(100)]);
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
                            info!("[PRICES] Received binary message: {} bytes", data.len());
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
    debug!("[DISCOVER] Starting market discovery, filter={:?}", market_filter);

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
            info!("[DISCOVER] No series data for '{}'", series_slug);
            continue;
        };
        let Some(events) = &series.events else {
            info!("[DISCOVER] No events in series '{}'", series_slug);
            continue;
        };

        debug!("[DISCOVER] Series '{}' has {} events", series_slug, events.len());

        // Sort by end_date to get soonest-expiring markets first
        let mut filtered_events: Vec<_> = events
            .iter()
            .filter(|e| e.closed != Some(true) && e.enable_order_book == Some(true))
            .collect();
        filtered_events.sort_by(|a, b| {
            a.end_date.cmp(&b.end_date)
        });
        let event_slugs: Vec<String> = filtered_events
            .into_iter()
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
                    info!("[DISCOVER] Failed to fetch event '{}': {}", event_slug, e);
                    continue;
                }
            };

            let event_details: Vec<serde_json::Value> = match resp.json().await {
                Ok(ed) => ed,
                Err(e) => {
                    info!("[DISCOVER] Failed to parse event '{}': {}", event_slug, e);
                    continue;
                }
            };

            let Some(ed) = event_details.first() else {
                info!("[DISCOVER] No event details for '{}'", event_slug);
                continue;
            };
            let Some(mkts) = ed.get("markets").and_then(|m| m.as_array()) else {
                info!("[DISCOVER] No markets in event '{}'", event_slug);
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

                debug!("[DISCOVER] Found market: {} | {} | end={:?}",
                       asset, &question[..question.len().min(40)], end_time);

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
    println!("POLYMARKET PING PONG ACCUMULATOR BOT");
    println!("═══════════════════════════════════════════════════════════════════════");
    println!("Symbol: {}  |  Mode: {}  |  Max: ${:.0}/market",
             args.sym.to_uppercase(),
             if args.live { "LIVE" } else { "DRY RUN" },
             args.max_dollars);
    println!("Time window: {}m-{}m remaining  |  Max age: {:.0}m",
             args.min_minutes, args.max_minutes, args.max_age);
    println!("ACCUMULATOR: Buy {:.0} contracts every {}s | Build avg price on both sides",
             args.buy_size, args.buy_interval);
    println!("ENDGAME: At {:.1}m left, buy side with bid >= {}c ({:.0}x size)",
             args.endgame_mins, args.way_up, args.endgame_mult);
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

    // Fetch existing positions for discovered markets at startup
    println!("Fetching existing positions from API...");
    let mut positions_map: HashMap<String, Position> = HashMap::new();
    let mut positions_found = 0;
    for m in &discovered {
        let yes_bal = shared_client.get_balance(&m.yes_token).await.unwrap_or(0.0);
        let no_bal = shared_client.get_balance(&m.no_token).await.unwrap_or(0.0);
        if yes_bal > 0.0 || no_bal > 0.0 {
            println!("  {} existing position: Y={:.1} N={:.1}", m.asset, yes_bal, no_bal);
            positions_found += 1;
        }
        let mut pos = Position::default();
        pos.yes_qty = yes_bal;
        pos.no_qty = no_bal;
        // Estimate cost from current mid price (won't be exact but close enough)
        // We'll update with better estimates once we get orderbook data
        pos.yes_cost = yes_bal * 0.5; // Assume 50c avg until we know better
        pos.no_cost = no_bal * 0.5;
        positions_map.insert(m.condition_id.clone(), pos);
    }
    println!("Found {} markets with existing positions", positions_found);

    let state = Arc::new(RwLock::new({
        let mut s = State::new();
        for m in discovered {
            let id = m.condition_id.clone();
            if let Some(pos) = positions_map.remove(&id) {
                s.positions.insert(id.clone(), pos);
            } else {
                s.positions.insert(id.clone(), Position::default());
            }
            s.markets.insert(id, m);
        }
        s
    }));

    // Start price feed
    let state_price = state.clone();
    tokio::spawn(async move {
        run_price_feed(state_price).await;
    });

    let max_dollars = args.max_dollars;
    let dry_run = !args.live;
    let min_minutes = args.min_minutes as f64;
    let max_minutes = args.max_minutes as f64;
    let max_age = args.max_age;
    let log_filter = symbol_filter.clone();
    // Accumulator strategy params
    let buy_interval_secs = args.buy_interval;
    let buy_size = args.buy_size;
    let endgame_mins = args.endgame_mins;
    let way_up_threshold = args.way_up;
    let endgame_mult = args.endgame_mult;

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
                    // Fetch existing positions for discovered markets
                    info!("[STARTUP] Fetching existing positions from API...");
                    let mut positions_found = 0;
                    for m in &discovered {
                        let yes_bal = shared_client.get_balance(&m.yes_token).await.unwrap_or(0.0);
                        let no_bal = shared_client.get_balance(&m.no_token).await.unwrap_or(0.0);
                        if yes_bal > 0.0 || no_bal > 0.0 {
                            info!("[STARTUP] {} existing position: Y={:.1} N={:.1}", m.asset, yes_bal, no_bal);
                            positions_found += 1;
                        }
                        let mut s = state.write().await;
                        let pos = s.positions.entry(m.condition_id.clone()).or_insert_with(Position::default);
                        pos.yes_qty = yes_bal;
                        pos.no_qty = no_bal;
                        // Estimate cost from current prices (won't be exact but close)
                        let yes_mid = m.yes_bid.unwrap_or(50) as f64 / 100.0;
                        let no_mid = m.no_bid.unwrap_or(50) as f64 / 100.0;
                        pos.yes_cost = yes_bal * yes_mid;
                        pos.no_cost = no_bal * no_mid;
                    }
                    info!("[STARTUP] Found {} markets with existing positions", positions_found);

                    let mut s = state.write().await;
                    for m in discovered {
                        let id = m.condition_id.clone();
                        s.markets.insert(id, m);
                    }
                    info!("[DISCOVER] Added {} markets to state", s.markets.len());
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
        println!("STRATEGY: ACCUMULATOR - Buy both sides evenly, then buy 'way up' side in endgame");
        println!("═════════════════════════════════════════════════════════════════════════════════════════════════════════════");
        println!("Sym | spot | left | YES bid/ask | NO bid/ask | Pos | Phase");
        println!("═════════════════════════════════════════════════════════════════════════════════════════════════════════════");

        let mut ping_interval = tokio::time::interval(Duration::from_secs(30));
        let mut status_interval = tokio::time::interval(Duration::from_secs(1));
        let mut discovery_interval = tokio::time::interval(Duration::from_secs(60)); // Check for new markets every minute
        let mut accumulator_interval = tokio::time::interval(Duration::from_secs(buy_interval_secs)); // Accumulator buys
        discovery_interval.tick().await; // Skip first tick
        accumulator_interval.tick().await; // Skip first tick

        loop {
            tokio::select! {
                _ = accumulator_interval.tick() => {
                    // === ACCUMULATOR STRATEGY ===
                    // Phase 1: Buy both sides evenly at intervals
                    // Phase 2 (Endgame): Buy "way up" side when near expiry

                    // Collect markets to trade (with end time and strike for logging)
                    struct MarketInfo {
                        id: String,
                        asset: String,
                        yes_token: String,
                        no_token: String,
                        mins_left: f64,
                        yes_bid: i64,
                        yes_ask: i64,
                        no_bid: i64,
                        no_ask: i64,
                        end_time: Option<chrono::DateTime<chrono::Utc>>,
                        strike: String,
                    }
                    let markets_to_trade: Vec<MarketInfo> = {
                        let s = state.read().await;
                        s.markets.iter()
                            .filter(|(_, m)| {
                                let mins = m.minutes_remaining().unwrap_or(0.0);
                                let age = 15.0 - mins;
                                mins >= min_minutes && mins <= max_minutes && age <= max_age
                            })
                            .map(|(id, m)| MarketInfo {
                                id: id.clone(),
                                asset: m.asset.clone(),
                                yes_token: m.yes_token.clone(),
                                no_token: m.no_token.clone(),
                                mins_left: m.minutes_remaining().unwrap_or(0.0),
                                yes_bid: m.yes_bid.unwrap_or(0),
                                yes_ask: m.yes_ask.unwrap_or(100),
                                no_bid: m.no_bid.unwrap_or(0),
                                no_ask: m.no_ask.unwrap_or(100),
                                end_time: m.end_time,
                                strike: m.pin_name(),
                            })
                            .collect()
                    };

                    for mkt in markets_to_trade {
                        let market_id = mkt.id;
                        let asset = mkt.asset;
                        let yes_token = mkt.yes_token;
                        let no_token = mkt.no_token;
                        let mins_left = mkt.mins_left;
                        let yes_bid_price = mkt.yes_bid;
                        let yes_ask = mkt.yes_ask;
                        let no_bid_price = mkt.no_bid;
                        let no_ask = mkt.no_ask;
                        let end_time_str = mkt.end_time
                            .map(|t| t.with_timezone(&chrono::Local).format("%H:%M").to_string())
                            .unwrap_or_else(|| "??:??".to_string());
                        let strike = mkt.strike;
                        // Get current position and accumulator state
                        let (current_invested, yes_qty, no_qty, acc_state) = {
                            let s = state.read().await;
                            let pos = s.positions.get(&market_id);
                            let acc = s.accumulator.get(&market_id).cloned().unwrap_or_default();
                            (
                                pos.map(|p| p.total_cost()).unwrap_or(0.0),
                                pos.map(|p| p.yes_qty).unwrap_or(0.0),
                                pos.map(|p| p.no_qty).unwrap_or(0.0),
                                acc,
                            )
                        };

                        // Get spot price for logging
                        let spot_price = {
                            let s = state.read().await;
                            match asset.as_str() {
                                "BTC" => s.prices.btc_price.map(|p| format!("${:.0}", p)),
                                "ETH" => s.prices.eth_price.map(|p| format!("${:.0}", p)),
                                "SOL" => s.prices.sol_price.map(|p| format!("${:.2}", p)),
                                "XRP" => s.prices.xrp_price.map(|p| format!("${:.4}", p)),
                                _ => None,
                            }.unwrap_or_else(|| "?".to_string())
                        };

                        let is_endgame = mins_left <= endgame_mins;

                        // Skip if at max investment AND balanced, BUT only during accumulator phase
                        // (endgame always proceeds to buy winning side)
                        let is_balanced = (yes_qty - no_qty).abs() < 0.5;
                        if !is_endgame && current_invested >= max_dollars && is_balanced {
                            debug!("[ACC] {} at max ${:.2} and balanced", asset, current_invested);
                            continue;
                        }

                        if is_endgame {
                            // === ENDGAME PHASE: Buy "way up" side ===
                            if acc_state.endgame_bought {
                                info!("[ENDGAME] {} @{} {} {} done | Y={}c N={}c | Y={:.0} N={:.0}",
                                      asset, end_time_str, strike, spot_price,
                                      yes_bid_price, no_bid_price, yes_qty, no_qty);
                                continue;
                            }

                            // Check if YES is "way up" (bid >= threshold)
                            let yes_way_up = yes_bid_price >= way_up_threshold;
                            let no_way_up = no_bid_price >= way_up_threshold;

                            if !yes_way_up && !no_way_up {
                                info!("[ENDGAME] {} @{} {} {} waiting | Y={}c N={}c (need {}c) | Y={:.0} N={:.0}",
                                       asset, end_time_str, strike, spot_price,
                                       yes_bid_price, no_bid_price, way_up_threshold, yes_qty, no_qty);
                                continue;
                            }

                            // Determine which side to buy
                            let (buy_yes, side_name, token, ask_price) = if yes_way_up && no_way_up {
                                // Both way up? Buy the higher one
                                if yes_bid_price >= no_bid_price {
                                    (true, "YES", yes_token.clone(), yes_ask)
                                } else {
                                    (false, "NO", no_token.clone(), no_ask)
                                }
                            } else if yes_way_up {
                                (true, "YES", yes_token.clone(), yes_ask)
                            } else {
                                (false, "NO", no_token.clone(), no_ask)
                            };

                            let endgame_buy_size = buy_size * endgame_mult;
                            let entry_price = (ask_price + 2).min(99);
                            let cross_price = entry_price as f64 / 100.0;
                            let cost = endgame_buy_size * cross_price;

                            // Mark endgame as bought BEFORE sending order
                            {
                                let mut s = state.write().await;
                                let acc = s.accumulator.entry(market_id.clone()).or_default();
                                acc.endgame_bought = true;
                                acc.endgame_side = Some(buy_yes);
                            }

                            if !dry_run {
                                info!("[ENDGAME] {} @{} {} {} BUY {} | bid={}c >= {}c | {:.0} @{}c = ${:.2}",
                                      asset, end_time_str, strike, spot_price, side_name,
                                      if buy_yes { yes_bid_price } else { no_bid_price },
                                      way_up_threshold, endgame_buy_size, entry_price, cost);

                                let client = shared_client.clone();
                                let state_clone = state.clone();
                                let market_id_clone = market_id.clone();
                                let asset_clone = asset.clone();
                                let side_name_owned = side_name.to_string();

                                tokio::spawn(async move {
                                    match client.buy_fak(&token, cross_price, endgame_buy_size).await {
                                        Ok(fill) if fill.filled_size > 0.0 => {
                                            info!("[ENDGAME] {} {} FILLED {:.1} @${:.2}",
                                                  asset_clone, side_name_owned, fill.filled_size, fill.fill_cost);
                                            let mut s = state_clone.write().await;
                                            if let Some(pos) = s.positions.get_mut(&market_id_clone) {
                                                if buy_yes {
                                                    pos.yes_qty += fill.filled_size;
                                                    pos.yes_cost += fill.fill_cost;
                                                } else {
                                                    pos.no_qty += fill.filled_size;
                                                    pos.no_cost += fill.fill_cost;
                                                }
                                            }
                                        }
                                        Ok(_) => warn!("[ENDGAME] {} {} no fill", asset_clone, side_name_owned),
                                        Err(e) => error!("[ENDGAME] {} {} error: {}", asset_clone, side_name_owned, e),
                                    }
                                });
                            } else {
                                info!("[ENDGAME] {} @{} {} {} BUY {} | bid={}c >= {}c | {:.0} @{}c = ${:.2} (DRY RUN)",
                                      asset, end_time_str, strike, spot_price, side_name,
                                      if buy_yes { yes_bid_price } else { no_bid_price },
                                      way_up_threshold, endgame_buy_size, entry_price, cost);
                            }
                        } else {
                            // === ACCUMULATOR PHASE: Buy side that's behind to stay balanced ===
                            // When equal (including 0/0), buy BOTH sides
                            // When unbalanced, buy only the side that's behind
                            let sides_to_buy: Vec<(&str, &String, i64)> = if yes_qty < no_qty {
                                vec![("YES", &yes_token, yes_ask)]
                            } else if no_qty < yes_qty {
                                vec![("NO", &no_token, no_ask)]
                            } else {
                                // Equal (including 0/0) - buy BOTH sides
                                vec![("YES", &yes_token, yes_ask), ("NO", &no_token, no_ask)]
                            };

                            for (side_name, token, ask_price) in sides_to_buy {
                                let entry_price = (ask_price + 2).min(99);
                                let cross_price = entry_price as f64 / 100.0;

                                // Ensure order meets $1 minimum
                                let min_size = (1.0 / cross_price).ceil();
                                let actual_size = buy_size.max(min_size);
                                let cost = actual_size * cross_price;

                                // Check if we can afford this
                                // Allow first buy even if min order > max
                                // Allow rebalancing buys even if at max (only skip if balanced AND at max)
                                let has_position = yes_qty > 0.0 || no_qty > 0.0;
                                let is_rebalancing = yes_qty != no_qty;
                                let would_exceed = current_invested + cost > max_dollars;

                                if has_position && would_exceed && !is_rebalancing {
                                    debug!("[ACC] {} skip {}: at max and balanced", asset, side_name);
                                    continue;
                                }

                                if !dry_run {
                                    info!("[ACC] {} {} {} BUY {} | Y={:.0} N={:.0} | {:.0} @{}c = ${:.2}",
                                          asset, strike, spot_price, side_name, yes_qty, no_qty, actual_size, entry_price, cost);

                                    let client = shared_client.clone();
                                    let state_clone = state.clone();
                                    let market_id_clone = market_id.clone();
                                    let asset_clone = asset.clone();
                                    let is_yes = side_name == "YES";
                                    let token_clone = token.to_string();
                                    let side_name_clone = side_name.to_string();

                                    tokio::spawn(async move {
                                        match client.buy_fak(&token_clone, cross_price, actual_size).await {
                                            Ok(fill) if fill.filled_size > 0.0 => {
                                                info!("[ACC] {} {} FILLED {:.1} @${:.2}",
                                                      asset_clone, side_name_clone, fill.filled_size, fill.fill_cost);
                                                let mut s = state_clone.write().await;
                                                if let Some(pos) = s.positions.get_mut(&market_id_clone) {
                                                    if is_yes {
                                                        pos.yes_qty += fill.filled_size;
                                                        pos.yes_cost += fill.fill_cost;
                                                    } else {
                                                        pos.no_qty += fill.filled_size;
                                                        pos.no_cost += fill.fill_cost;
                                                    }
                                                }
                                                // Update last buy time
                                                let acc = s.accumulator.entry(market_id_clone).or_default();
                                                acc.last_accumulator_buy = Some(std::time::Instant::now());
                                            }
                                            Ok(_) => info!("[ACC] {} {} no fill", asset_clone, side_name_clone),
                                            Err(e) => info!("[ACC] {} {} error: {}", asset_clone, side_name_clone, e),
                                        }
                                    });
                                } else {
                                    info!("[ACC] {} {} {} BUY {} | Y={:.0} N={:.0} | {:.0} @{}c = ${:.2} (DRY RUN)",
                                          asset, strike, spot_price, side_name, yes_qty, no_qty, actual_size, entry_price, cost);
                                }
                            }
                        }
                    }
                }

                _ = discovery_interval.tick() => {
                    // Periodic discovery to catch new markets when they start
                    debug!("[DISCOVER] Checking for new markets...");
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
                            info!("[DISCOVER] Periodic discovery failed: {}", e);
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
                    // Keep markets 5 minutes after expiry to see resolution
                    let needs_rediscovery = {
                        let mut s = state.write().await;
                        let expired_ids: Vec<String> = s.markets
                            .iter()
                            .filter(|(_, m)| {
                                m.minutes_remaining().map(|mins| mins < -5.0).unwrap_or(true)
                            })
                            .map(|(id, _)| id.clone())
                            .collect();

                        for id in &expired_ids {
                            if let Some(m) = s.markets.remove(id) {
                                info!("[EXPIRE] Removed market (5m after expiry): {} {} ({})",
                                      m.asset, m.pin_name(), &id[..16]);
                            }
                            s.positions.remove(id);
                        }

                        s.markets.is_empty()
                    };

                    // If all markets expired, break to re-discover
                    if needs_rediscovery {
                        info!("[DISCOVER] All markets expired (5m grace), breaking to re-discover...");
                        break;
                    }

                    let s = state.read().await;

                    if s.markets.is_empty() {
                        info!("[ping_pong] No markets in state - waiting for discovery...");
                        continue;
                    }

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

                        // Position quantities and avg costs
                        let pos_yes = pos.yes_qty;
                        let pos_no = pos.no_qty;
                        let total_cost = pos.total_cost();
                        let yes_avg = if pos_yes > 0.0 { (pos.yes_cost / pos_yes * 100.0) as i64 } else { 0 };
                        let no_avg = if pos_no > 0.0 { (pos.no_cost / pos_no * 100.0) as i64 } else { 0 };

                        // Mark-to-market PnL using bid prices (what you'd get if you sold)
                        let yes_bid_dollars = yes_bid as f64 / 100.0;
                        let no_bid_dollars = no_bid as f64 / 100.0;
                        let mtm_value = pos_yes * yes_bid_dollars + pos_no * no_bid_dollars;
                        let pnl = mtm_value - total_cost;
                        let matched = pos_yes.min(pos_no);

                        // Matched pairs indicator
                        let matched_str = if matched > 0.0 {
                            format!("matched:{:.0}", matched)
                        } else {
                            "".to_string()
                        };

                        // Phase indicator with accumulator details
                        let is_endgame = expiry <= endgame_mins;
                        let acc_state = s.accumulator.get(id).cloned().unwrap_or_default();
                        let phase = if is_endgame {
                            let way_up_side = if yes_bid >= way_up_threshold { "YES" }
                                             else if no_bid >= way_up_threshold { "NO" }
                                             else { "-" };
                            if acc_state.endgame_bought {
                                format!("ENDGAME({}) DONE", way_up_side)
                            } else {
                                format!("ENDGAME({})", way_up_side)
                            }
                        } else {
                            // Accumulator phase - show balance status
                            let diff = (pos_yes - pos_no).abs();

                            // Calculate if next order would exceed max
                            let ask_for_behind = if pos_yes < pos_no { yes_ask } else { no_ask };
                            let next_price = ((ask_for_behind + 2).min(99)) as f64 / 100.0;
                            let min_order = (1.0_f64 / next_price).ceil() * next_price;
                            let would_exceed = total_cost + min_order > max_dollars;
                            let is_balanced = (pos_yes - pos_no).abs() < 0.5;

                            let mins_to_endgame = expiry - endgame_mins;
                            let endgame_str = format!("{:.1}m to endgame", mins_to_endgame);

                            if pos_yes == 0.0 && pos_no == 0.0 {
                                // No position yet - buying both sides
                                format!("ACCUM BUYING {}", endgame_str)
                            } else if would_exceed && is_balanced {
                                // At max AND balanced - truly done
                                format!("ACCUM DONE ${:.0} {}", total_cost, endgame_str)
                            } else if pos_yes < pos_no {
                                // Need more YES (will buy even if over max)
                                format!("ACCUM +YES({:.0}) {}", diff, endgame_str)
                            } else if pos_no < pos_yes {
                                // Need more NO (will buy even if over max)
                                format!("ACCUM +NO({:.0}) {}", diff, endgame_str)
                            } else {
                                format!("ACCUM OK({:.0}) {}", pos_yes, endgame_str)
                            }
                        };

                        // Format end time in local timezone
                        let end_time_str = market.end_time
                            .map(|t| t.with_timezone(&chrono::Local).format("%H:%M:%S").to_string())
                            .unwrap_or_else(|| "??:??".to_string());

                        // Format position with avg costs
                        let pos_str = if pos_yes > 0.0 || pos_no > 0.0 {
                            format!("Y={:.0}@{}¢ N={:.0}@{}¢ ${:.2} PnL=${:+.2}",
                                    pos_yes, yes_avg, pos_no, no_avg, total_cost, pnl)
                        } else {
                            "no position".to_string()
                        };

                        // Get strike price for this market
                        let strike = market.pin_name();

                        // Log filtered markets only if we have a position (otherwise silently skip)
                        if let Some(ref reason) = filter_reason {
                            let has_position = pos_yes > 0.0 || pos_no > 0.0;
                            if has_position {
                                info!("[{}] {} {} | {} | Y={}¢ N={}¢ | {:.1}m @{} | {} {}",
                                      market.asset, strike, spot, reason,
                                      yes_bid, no_bid,
                                      expiry, end_time_str,
                                      pos_str, matched_str);
                            }
                            continue;
                        }

                        // Main status line
                        info!("[{}] {} {} | Y={}¢ N={}¢ | {:.1}m @{} | {} | {} {}",
                              market.asset, strike, spot,
                              yes_bid, no_bid,
                              expiry, end_time_str, phase,
                              pos_str, matched_str);
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
                                            if is_yes {
                                                market.yes_bid = Some(price_cents.saturating_sub(1).max(1));
                                                market.yes_ask = Some((price_cents + 1).min(99));
                                            } else {
                                                market.no_bid = Some(price_cents.saturating_sub(1).max(1));
                                                market.no_ask = Some((price_cents + 1).min(99));
                                            };
                                            // Price updates are handled by accumulator_interval, no reactive trading
                                        }
                                    }
                                }
                            }

                            // Book snapshots update the orderbook state
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
                                            info!("[WS] Unknown asset_id: {}", &book.asset_id[..book.asset_id.len().min(16)]);
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
                                            info!("[WS] Market {} not found in state", &market_id[..16]);
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

                                        // Sanity check: YES bid + NO bid should sum close to 100
                                        let y_bid = market.yes_bid.unwrap_or(0);
                                        let n_bid = market.no_bid.unwrap_or(0);
                                        let bid_sum = y_bid + n_bid;
                                        if bid_sum > 0 && (bid_sum < 85 || bid_sum > 115) {
                                            // Prices inconsistent, revert this update
                                            info!("[BOOK] {} {} REJECTED: Y_bid={}c + N_bid={}c = {}c (not ~100c)",
                                                   market.asset, side, y_bid, n_bid, bid_sum);
                                            if is_yes {
                                                market.yes_ask = old_ask;
                                                market.yes_bid = old_bid;
                                                market.yes_ask_size = old_size;
                                            } else {
                                                market.no_ask = old_ask;
                                                market.no_bid = old_bid;
                                                market.no_ask_size = old_size;
                                            }
                                            continue;
                                        }

                                        // Log orderbook changes
                                        let new_ask = best_ask.map(|(p, _)| p);
                                        let new_bid = best_bid.map(|(p, _)| p);
                                        let ask_changed = old_ask != new_ask;
                                        let bid_changed = old_bid != new_bid;
                                        let size_changed = (old_size - total_ask_size).abs() > 0.01;

                                        if ask_changed || bid_changed {
                                            debug!("[BOOK] {} {} | bid: {:?}c->{:?}c | ask: {:?}c->{:?}c",
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

                                        // All trading is handled by accumulator_interval, not reactive to book updates
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
                            info!("[WS] Received binary: {} bytes", data.len());
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
