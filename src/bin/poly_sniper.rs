//! Polymarket Sniper - Fair Value Based Crypto Options Strategy
//!
//! STRATEGY:
//! - Monitor Polymarket crypto binary options (BTC/ETH up/down)
//! - Calculate fair value using Black-Scholes based on spot vs strike
//! - Buy when market price < fair value - edge threshold
//! - Optional: Cross-platform arbitrage with Kalshi
//!
//! The bot:
//! 1. Discovers crypto markets on Polymarket
//! 2. Connects to real-time price feed (Polygon.io)
//! 3. Monitors Polymarket orderbook via WebSocket
//! 4. Optionally monitors Kalshi for cross-platform arbs
//! 5. Executes FAK (Fill-And-Kill) orders when edge detected
//!
//! Usage:
//!   cargo run --release --bin poly_sniper
//!
//! Environment:
//!   POLY_PRIVATE_KEY - Your Polymarket/Polygon wallet private key
//!   POLY_FUNDER - Your funder address (proxy wallet)
//!   POLYGON_API_KEY - Polygon.io API key for price feed
//!   DRY_RUN=1 - Set to 0 to execute trades (default: 1 = monitor only)
//!
//! Optional (for cross-platform arb):
//!   KALSHI_API_KEY_ID - Kalshi API key ID
//!   KALSHI_PRIVATE_KEY_PATH - Path to Kalshi private key

use anyhow::{Context, Result};
use chrono::{Utc, Timelike};
use clap::Parser;
use futures_util::{SinkExt, StreamExt};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::RwLock;
use tokio_tungstenite::{connect_async, tungstenite::Message};
use tracing::{error, info, warn};

use arb_bot::polymarket_clob::{
    PolymarketAsyncClient, SharedAsyncClient, PreparedCreds,
};
use arb_bot::fair_value::calc_fair_value_cents;
use arb_bot::kalshi::{KalshiConfig, KalshiApiClient};
use arb_bot::types::kalshi_fee_cents;

/// Polymarket Sniper - Fair Value Based Strategy
#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Args {
    /// Size per trade in dollars
    #[arg(short, long, default_value_t = 20.0)]
    size: f64,

    /// Edge in cents required to buy (default: 5)
    #[arg(short, long, default_value_t = 5)]
    edge: i64,

    /// Aggressive edge threshold - use IOC at ask (default: 15)
    #[arg(long, default_value_t = 15)]
    aggro_edge: i64,

    /// Annualized volatility % for fair value calc (default: 58%)
    #[arg(short, long, default_value_t = 58.0)]
    vol: f64,

    /// Live trading mode (default is dry run)
    #[arg(short, long, default_value_t = false)]
    live: bool,

    /// Enable cross-platform arbitrage with Kalshi
    #[arg(long, default_value_t = false)]
    arb: bool,

    /// Arb threshold in cents (buy if combined cost < 100 - this)
    #[arg(long, default_value_t = 2)]
    arb_edge: i64,

    /// Specific market slug to monitor (optional, monitors all crypto if not set)
    #[arg(long)]
    market: Option<String>,
}

const POLYMARKET_WS_URL: &str = "wss://ws-subscriptions-clob.polymarket.com/ws/market";
const GAMMA_API_BASE: &str = "https://gamma-api.polymarket.com";
const POLYGON_WS_URL: &str = "wss://socket.polygon.io/crypto";

// Kalshi series tickers for 15-minute crypto markets
const BTC_15M_SERIES: &str = "KXBTC15M";
const ETH_15M_SERIES: &str = "KXETH15M";

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

/// Price feed state
#[derive(Debug, Default)]
struct PriceState {
    btc_price: Option<f64>,
    eth_price: Option<f64>,
    last_update: Option<std::time::Instant>,
}

/// Kalshi 15-minute market state
#[derive(Debug, Clone)]
struct KalshiMarketState {
    ticker: String,
    title: String,
    strike: Option<f64>,
    asset: String, // "BTC" or "ETH"
    yes_ask: Option<i64>,
    yes_bid: Option<i64>,
    no_ask: Option<i64>,
    no_bid: Option<i64>,
    expiry_secs: Option<i64>,
}

impl KalshiMarketState {
    fn new(ticker: String, title: String, strike: Option<f64>, asset: String) -> Self {
        Self {
            ticker,
            title,
            strike,
            asset,
            yes_ask: None,
            yes_bid: None,
            no_ask: None,
            no_bid: None,
            expiry_secs: None,
        }
    }
}

/// Arb opportunity between platforms
#[derive(Debug, Clone)]
struct ArbOpportunity {
    poly_market: String,
    kalshi_ticker: String,
    asset: String,
    strike: f64,
    // Poly YES + Kalshi NO
    poly_yes_ask: i64,
    kalshi_no_ask: i64,
    total_cost_py_kn: i64,
    profit_py_kn: i64,
    // Kalshi YES + Poly NO
    kalshi_yes_ask: i64,
    poly_no_ask: i64,
    total_cost_ky_pn: i64,
    profit_ky_pn: i64,
    // Time remaining
    mins_remaining: i64,
}

/// Global state
struct State {
    markets: HashMap<String, Market>,
    positions: HashMap<String, Position>,
    prices: PriceState,
    kalshi_markets: HashMap<String, KalshiMarketState>,
}

impl State {
    fn new() -> Self {
        Self {
            markets: HashMap::new(),
            positions: HashMap::new(),
            prices: PriceState::default(),
            kalshi_markets: HashMap::new(),
        }
    }
}

// === Gamma API for market discovery ===

/// Series response from Gamma API
#[derive(Debug, Deserialize)]
struct GammaSeries {
    events: Option<Vec<GammaEvent>>,
}

/// Event within a series
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

/// Crypto series slugs for 15-minute up/down markets (these have orderbooks)
const POLY_SERIES_SLUGS: &[(&str, &str)] = &[
    ("btc-up-or-down-15m", "BTC"),
    ("eth-up-or-down-15m", "ETH"),
    ("sol-up-or-down-15m", "SOL"),
    ("xrp-up-or-down-15m", "XRP"),
];

/// Discover crypto markets on Polymarket via series API
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
            .header("User-Agent", "poly_sniper/1.0")
            .send()
            .await?;

        if !resp.status().is_success() {
            warn!("[DISCOVER] Failed to fetch series '{}': {}", series_slug, resp.status());
            continue;
        }

        let series_list: Vec<GammaSeries> = resp.json().await?;
        let Some(series) = series_list.first() else { continue };
        let Some(events) = &series.events else { continue };

        // Collect event slugs that need fetching (events with orderbook enabled and not closed)
        let event_slugs: Vec<String> = events
            .iter()
            .filter(|e| e.closed != Some(true) && e.enable_order_book == Some(true))
            .filter_map(|e| e.slug.clone())
            .take(20) // Limit to avoid too many requests
            .collect();

        // Fetch each event's market details
        for event_slug in event_slugs {
            let event_url = format!("{}/events?slug={}", GAMMA_API_BASE, event_slug);
            let resp = match client.get(&event_url)
                .header("User-Agent", "poly_sniper/1.0")
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

                markets.push(Market {
                    condition_id: cid,
                    question,
                    yes_token: token_ids[0].clone(),
                    no_token: token_ids[1].clone(),
                    strike: None, // Up/Down markets don't have strikes
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
    // Look for patterns like "$100,000" or "100000" or "$100k"
    let re_patterns = [
        r"\$([0-9,]+(?:\.[0-9]+)?)",  // $100,000 or $100,000.50
        r"([0-9]+(?:,[0-9]+)*(?:\.[0-9]+)?)\s*(?:dollars?|usd)",  // 100000 dollars
        r"above\s+\$?([0-9,]+)",  // above $100,000
        r"below\s+\$?([0-9,]+)",  // below $100,000
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
    // Parse ISO 8601 date
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

/// Parse price from "0.XX" format to cents
fn parse_price_cents(s: &str) -> i64 {
    s.parse::<f64>()
        .map(|p| (p * 100.0).round() as i64)
        .unwrap_or(0)
}

/// Parse size from dollar string
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
                    // Polygon sends array of messages
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

// === Kalshi Market Discovery ===

/// Parse expiry time from Kalshi ticker and return seconds remaining
/// Format: KXBTC15M-25DEC171000-00 = year 25, Dec 17, 10:00 EST
fn parse_kalshi_expiry_secs(ticker: &str) -> Option<i64> {
    let parts: Vec<&str> = ticker.split('-').collect();
    if parts.len() < 2 { return None; }
    let datetime_part = parts[1];
    if datetime_part.len() < 11 { return None; }

    let hhmm = &datetime_part[datetime_part.len()-4..];
    let hour: u32 = hhmm[0..2].parse().ok()?;
    let minute: u32 = hhmm[2..4].parse().ok()?;

    // EST to UTC (+5 hours)
    let hour_utc = (hour + 5) % 24;

    let now = Utc::now();
    let expiry_secs = hour_utc * 3600 + minute * 60;
    let current_secs = now.hour() * 3600 + now.minute() * 60 + now.second();

    let diff = expiry_secs as i64 - current_secs as i64;
    if diff < -60 {
        Some(diff + 24 * 3600)
    } else {
        Some(diff)
    }
}

/// Discover Kalshi 15-minute crypto markets
async fn discover_kalshi_markets(client: &KalshiApiClient) -> Result<Vec<KalshiMarketState>> {
    let mut markets = Vec::new();

    for series in [BTC_15M_SERIES, ETH_15M_SERIES] {
        let asset = if series == BTC_15M_SERIES { "BTC" } else { "ETH" };

        match client.get_events(series, 10).await {
            Ok(events) => {
                for event in events {
                    match client.get_markets(&event.event_ticker).await {
                        Ok(event_markets) => {
                            for m in event_markets {
                                let expiry_secs = parse_kalshi_expiry_secs(&m.ticker);
                                // Only include markets with time remaining
                                if expiry_secs.map(|s| s > 0 && s <= 900).unwrap_or(false) {
                                    let mut market = KalshiMarketState::new(
                                        m.ticker.clone(),
                                        m.title.clone(),
                                        m.floor_strike,
                                        asset.to_string(),
                                    );
                                    market.yes_ask = m.yes_ask;
                                    market.yes_bid = m.yes_bid;
                                    market.no_ask = m.no_ask;
                                    market.no_bid = m.no_bid;
                                    market.expiry_secs = expiry_secs;
                                    markets.push(market);
                                }
                            }
                        }
                        Err(e) => warn!("[KALSHI] Failed to get markets for {}: {}", event.event_ticker, e),
                    }
                }
            }
            Err(e) => warn!("[KALSHI] Failed to get events for {}: {}", series, e),
        }
    }

    Ok(markets)
}

/// Find and display arb opportunities between Polymarket and Kalshi
fn find_arb_opportunities(state: &State, arb_edge: i64) -> Vec<ArbOpportunity> {
    let mut arbs = Vec::new();

    // For each Kalshi market, try to find matching Polymarket market
    for (ticker, kalshi) in &state.kalshi_markets {
        let Some(kalshi_strike) = kalshi.strike else { continue };

        // Find Polymarket markets with same asset and similar strike
        for (poly_id, poly) in &state.markets {
            if poly.asset != kalshi.asset { continue; }
            let Some(poly_strike) = poly.strike else { continue };

            // Match if strikes are within 0.1%
            let strike_diff_pct = ((poly_strike - kalshi_strike) / kalshi_strike).abs() * 100.0;
            if strike_diff_pct > 0.1 { continue; }

            // Need prices from both sides
            let (poly_yes, poly_no) = match (poly.yes_ask, poly.no_ask) {
                (Some(y), Some(n)) => (y, n),
                _ => continue,
            };
            let (kalshi_yes, kalshi_no) = match (kalshi.yes_ask, kalshi.no_ask) {
                (Some(y), Some(n)) => (y, n),
                _ => continue,
            };

            // Calculate Kalshi fees
            let kalshi_yes_fee = kalshi_fee_cents(kalshi_yes as u16) as i64;
            let kalshi_no_fee = kalshi_fee_cents(kalshi_no as u16) as i64;

            // Poly YES + Kalshi NO (one resolves YES, one resolves NO = $2 payout)
            // Wait - that's wrong. If they have the SAME strike:
            // - If price > strike: Poly YES pays $1, Kalshi YES pays $1
            // - If price < strike: Poly NO pays $1, Kalshi NO pays $1
            // So we want OPPOSITE sides: Poly YES + Kalshi NO or Kalshi YES + Poly NO
            // This guarantees exactly $1 payout (one wins, one loses)

            // Option 1: Buy Poly YES + Kalshi NO
            // If price > strike: Poly YES wins ($1), Kalshi NO loses (0) = $1
            // If price < strike: Poly YES loses (0), Kalshi NO wins ($1) = $1
            let cost_py_kn = poly_yes + kalshi_no + kalshi_no_fee;
            let profit_py_kn = 100 - cost_py_kn;

            // Option 2: Buy Kalshi YES + Poly NO
            // If price > strike: Kalshi YES wins ($1), Poly NO loses (0) = $1
            // If price < strike: Kalshi YES loses (0), Poly NO wins ($1) = $1
            let cost_ky_pn = kalshi_yes + kalshi_yes_fee + poly_no;
            let profit_ky_pn = 100 - cost_ky_pn;

            let mins_remaining = kalshi.expiry_secs.unwrap_or(0) / 60;

            // Only report if there's an arb opportunity
            if profit_py_kn >= arb_edge || profit_ky_pn >= arb_edge {
                arbs.push(ArbOpportunity {
                    poly_market: poly_id.clone(),
                    kalshi_ticker: ticker.clone(),
                    asset: kalshi.asset.clone(),
                    strike: kalshi_strike,
                    poly_yes_ask: poly_yes,
                    kalshi_no_ask: kalshi_no,
                    total_cost_py_kn: cost_py_kn,
                    profit_py_kn,
                    kalshi_yes_ask: kalshi_yes,
                    poly_no_ask: poly_no,
                    total_cost_ky_pn: cost_ky_pn,
                    profit_ky_pn,
                    mins_remaining,
                });
            }
        }
    }

    arbs
}

/// Print current arb opportunities
fn print_arb_opportunities(arbs: &[ArbOpportunity]) {
    if arbs.is_empty() {
        return;
    }

    info!("═══════════════════════════════════════════════════════════════════════════════════════");
    info!("🔄 CROSS-PLATFORM ARB OPPORTUNITIES (Polymarket ↔ Kalshi)");
    info!("═══════════════════════════════════════════════════════════════════════════════════════");

    for arb in arbs {
        info!("📊 {} @ ${:.0} | {}m remaining", arb.asset, arb.strike, arb.mins_remaining);
        info!("   Poly: {}", &arb.poly_market[..arb.poly_market.len().min(40)]);
        info!("   Kalshi: {}", arb.kalshi_ticker);

        if arb.profit_py_kn > 0 {
            info!("   ✅ BUY Poly YES @{}¢ + Kalshi NO @{}¢ = {}¢ → PROFIT {}¢",
                  arb.poly_yes_ask, arb.kalshi_no_ask, arb.total_cost_py_kn, arb.profit_py_kn);
        } else {
            info!("   ❌ Poly YES @{}¢ + Kalshi NO @{}¢ = {}¢ (no profit)",
                  arb.poly_yes_ask, arb.kalshi_no_ask, arb.total_cost_py_kn);
        }

        if arb.profit_ky_pn > 0 {
            info!("   ✅ BUY Kalshi YES @{}¢ + Poly NO @{}¢ = {}¢ → PROFIT {}¢",
                  arb.kalshi_yes_ask, arb.poly_no_ask, arb.total_cost_ky_pn, arb.profit_ky_pn);
        } else {
            info!("   ❌ Kalshi YES @{}¢ + Poly NO @{}¢ = {}¢ (no profit)",
                  arb.kalshi_yes_ask, arb.poly_no_ask, arb.total_cost_ky_pn);
        }
    }
    info!("═══════════════════════════════════════════════════════════════════════════════════════");
}

/// Kalshi market refresh task - periodically fetches market data
async fn run_kalshi_refresh(
    state: Arc<RwLock<State>>,
    client: Arc<KalshiApiClient>,
    arb_edge: i64,
) {
    let mut interval = tokio::time::interval(Duration::from_secs(10));

    loop {
        interval.tick().await;

        match discover_kalshi_markets(&client).await {
            Ok(markets) => {
                let mut s = state.write().await;

                // Clear old markets and add fresh ones
                s.kalshi_markets.clear();
                for m in markets {
                    s.kalshi_markets.insert(m.ticker.clone(), m);
                }

                // Find and display arb opportunities
                let arbs = find_arb_opportunities(&s, arb_edge);
                drop(s); // Release lock before printing

                print_arb_opportunities(&arbs);
            }
            Err(e) => {
                warn!("[KALSHI] Refresh failed: {}", e);
            }
        }
    }
}

// === Main ===

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::from_default_env()
                .add_directive("poly_sniper=info".parse().unwrap())
                .add_directive("arb_bot=info".parse().unwrap()),
        )
        .init();

    let args = Args::parse();

    info!("═══════════════════════════════════════════════════════════════════════");
    info!("🎯 POLYMARKET SNIPER - Fair Value Based Strategy");
    info!("═══════════════════════════════════════════════════════════════════════");
    info!("STRATEGY:");
    info!("   1. Calculate fair value using Black-Scholes (vol={}%)", args.vol);
    info!("   2. Buy when market_price < fair_value - {}¢", args.edge);
    info!("   3. If edge >= {}¢, use aggressive IOC at ask", args.aggro_edge);
    if args.arb {
        info!("   4. Cross-platform arb with Kalshi (edge >= {}¢)", args.arb_edge);
    }
    info!("═══════════════════════════════════════════════════════════════════════");
    info!("CONFIG:");
    info!("   Mode: {}", if args.live { "🚀 LIVE" } else { "🔍 DRY RUN" });
    info!("   Size: ${:.2} per trade", args.size);
    info!("   Edge: {}¢ | Aggro: {}¢", args.edge, args.aggro_edge);
    info!("   Volatility: {}%", args.vol);
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
        137, // Polygon mainnet
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
    info!("[DISCOVER] Searching for crypto markets...");
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

    // Start Kalshi monitoring for cross-platform arbs (always enabled now)
    let kalshi_client = match KalshiConfig::from_env() {
        Ok(config) => {
            info!("[KALSHI] API credentials loaded, starting arb monitor...");
            Some(Arc::new(KalshiApiClient::new(config)))
        }
        Err(e) => {
            warn!("[KALSHI] No credentials found ({}), cross-platform arbs disabled", e);
            None
        }
    };

    if let Some(kalshi) = kalshi_client {
        let state_kalshi = state.clone();
        let arb_edge = args.arb_edge;
        tokio::spawn(async move {
            run_kalshi_refresh(state_kalshi, kalshi, arb_edge).await;
        });
    }

    // Get token IDs for subscription
    let tokens: Vec<String> = {
        let s = state.read().await;
        s.markets.values()
            .flat_map(|m| vec![m.yes_token.clone(), m.no_token.clone()])
            .collect()
    };

    // Main WebSocket loop
    let vol = args.vol / 100.0;
    let edge = args.edge;
    let aggro_edge = args.aggro_edge;
    let size = args.size;
    let dry_run = !args.live;

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
        let mut status_interval = tokio::time::interval(Duration::from_secs(10));

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
                    let spot_btc = s.prices.btc_price.map(|p| format!("{:.0}", p)).unwrap_or("-".into());
                    let spot_eth = s.prices.eth_price.map(|p| format!("{:.0}", p)).unwrap_or("-".into());

                    info!("[STATUS] BTC=${} ETH=${} | Poly: {} mkts | Kalshi: {} mkts",
                          spot_btc, spot_eth, s.markets.len(), s.kalshi_markets.len());

                    // Show Polymarket markets
                    for (id, market) in &s.markets {
                        let pos = s.positions.get(id).cloned().unwrap_or_default();

                        // Get spot for this market's asset
                        let spot = if market.asset == "ETH" {
                            s.prices.eth_price
                        } else {
                            s.prices.btc_price
                        };

                        // Calculate fair value if we have data
                        let (fair_yes, fair_no) = match (spot, market.strike, market.expiry_minutes) {
                            (Some(s), Some(k), Some(mins)) if mins > 0.0 => {
                                calc_fair_value_cents(s, k, mins, vol)
                            }
                            _ => (50, 50),
                        };

                        let yes_ask = market.yes_ask.unwrap_or(100);
                        let no_ask = market.no_ask.unwrap_or(100);
                        let yes_edge = fair_yes - yes_ask;
                        let no_edge = fair_no - no_ask;

                        let edge_str = if yes_edge >= edge || no_edge >= edge {
                            format!("✓ EDGE Y:{:+}¢ N:{:+}¢", yes_edge, no_edge)
                        } else {
                            format!("Y:{:+}¢ N:{:+}¢", yes_edge, no_edge)
                        };

                        info!("  [POLY] {} | Fair Y={}¢ N={}¢ | Mkt Y={}¢ N={}¢ | {} | Pos: Y={:.1} N={:.1}",
                              market.asset,
                              fair_yes, fair_no,
                              yes_ask, no_ask,
                              edge_str,
                              pos.yes_qty, pos.no_qty);
                    }

                    // Show Kalshi markets
                    for (ticker, kalshi) in &s.kalshi_markets {
                        let mins = kalshi.expiry_secs.map(|s| s / 60).unwrap_or(0);
                        let yes_ask = kalshi.yes_ask.map(|p| format!("{}¢", p)).unwrap_or("-".into());
                        let no_ask = kalshi.no_ask.map(|p| format!("{}¢", p)).unwrap_or("-".into());
                        let strike_str = kalshi.strike.map(|s| format!("${:.0}", s)).unwrap_or("-".into());

                        info!("  [KALSHI] {} | {} @ {} | Y={} N={} | {}m left",
                              ticker, kalshi.asset, strike_str, yes_ask, no_ask, mins);
                    }

                    // Check for cross-platform arb opportunities
                    let arbs = find_arb_opportunities(&s, args.arb_edge);
                    drop(s);
                    if !arbs.is_empty() {
                        print_arb_opportunities(&arbs);
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

                                    // Get prices first
                                    let btc_price = s.prices.btc_price;
                                    let eth_price = s.prices.eth_price;

                                    // Now get mutable reference to market
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

                                    // Extract all values we need
                                    let asset = market.asset.clone();
                                    let strike = market.strike;
                                    let expiry = market.expiry_minutes;
                                    let yes_ask_price = market.yes_ask;
                                    let no_ask_price = market.no_ask;
                                    let yes_token = market.yes_token.clone();
                                    let no_token = market.no_token.clone();

                                    // Get spot price based on asset
                                    let spot = if asset == "ETH" { eth_price } else { btc_price };

                                    drop(s);

                                    // Calculate fair value
                                    // For Up/Down markets (no strike), fair value is 50/50
                                    // For above/below strike markets, use Black-Scholes
                                    let (fair_yes, fair_no) = match (spot, strike, expiry) {
                                        (Some(s), Some(k), Some(mins)) if mins > 0.0 => {
                                            calc_fair_value_cents(s, k, mins, vol)
                                        }
                                        (_, None, _) => (50, 50), // Up/Down markets = 50/50
                                        _ => continue,
                                    };

                                    // Check for edge
                                    let yes_edge_actual = yes_ask_price.map(|a| fair_yes - a).unwrap_or(0);
                                    let no_edge_actual = no_ask_price.map(|a| fair_no - a).unwrap_or(0);

                                    // Trade YES if edge
                                    if yes_edge_actual >= edge {
                                        let ask = yes_ask_price.unwrap();
                                        let is_aggro = yes_edge_actual >= aggro_edge;

                                        if dry_run {
                                            if is_aggro {
                                                info!("[DRY] Would IOC BUY ${:.0} YES @{}¢ | fair={}¢ edge={}¢ | {}",
                                                      size, ask, fair_yes, yes_edge_actual, asset);
                                            } else {
                                                info!("[DRY] Would BUY ${:.0} YES @{}¢ | fair={}¢ edge={}¢ | {}",
                                                      size, ask, fair_yes, yes_edge_actual, asset);
                                            }
                                        } else {
                                            let price = ask as f64 / 100.0;
                                            let contracts = size / price;

                                            warn!("[TRADE] 🎯 BUY ${:.0} YES @{}¢ | fair={}¢ edge={}¢ | {}",
                                                  size, ask, fair_yes, yes_edge_actual, asset);

                                            match shared_client.buy_fak(&yes_token, price, contracts).await {
                                                Ok(fill) => {
                                                    warn!("[TRADE] ✅ Filled {:.2} @${:.2} | order_id={}",
                                                          fill.filled_size, fill.fill_cost, fill.order_id);
                                                    // Update position
                                                    let mut s = state.write().await;
                                                    if let Some(pos) = s.positions.get_mut(&market_id) {
                                                        pos.yes_qty += fill.filled_size;
                                                        pos.yes_cost += fill.fill_cost;
                                                    }
                                                }
                                                Err(e) => error!("[TRADE] ❌ YES buy failed: {}", e),
                                            }
                                        }
                                    }

                                    // Trade NO if edge
                                    if no_edge_actual >= edge {
                                        let Some(ask) = no_ask_price else { continue };
                                        let is_aggro = no_edge_actual >= aggro_edge;

                                        if dry_run {
                                            if is_aggro {
                                                info!("[DRY] Would IOC BUY ${:.0} NO @{}¢ | fair={}¢ edge={}¢ | {}",
                                                      size, ask, fair_no, no_edge_actual, asset);
                                            } else {
                                                info!("[DRY] Would BUY ${:.0} NO @{}¢ | fair={}¢ edge={}¢ | {}",
                                                      size, ask, fair_no, no_edge_actual, asset);
                                            }
                                        } else {
                                            let price = ask as f64 / 100.0;
                                            let contracts = size / price;

                                            warn!("[TRADE] 🎯 BUY ${:.0} NO @{}¢ | fair={}¢ edge={}¢ | {}",
                                                  size, ask, fair_no, no_edge_actual, asset);

                                            match shared_client.buy_fak(&no_token, price, contracts).await {
                                                Ok(fill) => {
                                                    warn!("[TRADE] ✅ Filled {:.2} @${:.2} | order_id={}",
                                                          fill.filled_size, fill.fill_cost, fill.order_id);
                                                    // Update position
                                                    let mut s = state.write().await;
                                                    if let Some(pos) = s.positions.get_mut(&market_id) {
                                                        pos.no_qty += fill.filled_size;
                                                        pos.no_cost += fill.fill_cost;
                                                    }
                                                }
                                                Err(e) => error!("[TRADE] ❌ NO buy failed: {}", e),
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
