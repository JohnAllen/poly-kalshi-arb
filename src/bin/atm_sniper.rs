//! ATM Sniper - Simple ATM Binary Option Strategy
//!
//! STRATEGY:
//! - Find crypto markets where spot price ≈ strike price (ATM = at-the-money)
//! - At ATM, fair value = 50¢ for both YES and NO (delta = 0.5)
//! - Post resting bids at 45¢ for both YES and NO
//! - If both fill: pay 90¢ + fees, get $1 = guaranteed profit
//! - If one fills: hold until expiry (50% win rate at fair value)
//!
//! This is the simplest possible strategy - no complex conditions, just:
//! 1. Is spot ≈ strike? (within 0.05%)
//! 2. Post bids at 45¢ on both sides
//!
//! Usage:
//!   cargo run --release --bin atm_sniper
//!
//! Environment:
//!   KALSHI_API_KEY_ID - Your Kalshi API key ID
//!   KALSHI_PRIVATE_KEY_PATH - Path to your Kalshi private key PEM file
//!   DRY_RUN=0 - Set to execute trades (default: 1 = monitor only)
//!   BID_PRICE=45 - Price in cents to bid (default: 45)
//!   CONTRACTS=5 - Number of contracts per side (default: 5)
//!   ATM_THRESHOLD=0.05 - Max % diff from strike to be "ATM" (default: 0.05%)

use anyhow::{Context, Result};
use chrono::{NaiveTime, Timelike, Utc};
use clap::Parser;
use futures_util::{SinkExt, StreamExt};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tokio::sync::{mpsc, RwLock};
use tokio_tungstenite::{connect_async, tungstenite::{http::Request, Message}};
use tracing::{debug, error, info, warn};

/// ATM Sniper - Simple At-The-Money Binary Option Strategy
#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Args {
    /// Number of contracts per side
    #[arg(short, long, default_value_t = 40)]
    contracts: i64,

    /// Max bid price in cents (won't bid above this even at best bid)
    #[arg(short, long, default_value_t = 45)]
    max_bid: i64,

    /// ATM threshold percentage (0.015 = 0.015%)
    #[arg(short, long, default_value_t = 0.015)]
    threshold: f64,

    /// Live trading mode (default is dry run)
    #[arg(short, long, default_value_t = false)]
    live: bool,
}

use arb_bot::kalshi::{KalshiApiClient, KalshiConfig};

// Reuse modules from kalshi_crypto_arb
mod pricing;
mod polygon_feed;
use polygon_feed::{PriceState, run_polygon_feed};

const KALSHI_WS_URL: &str = "wss://api.elections.kalshi.com/trade-api/ws/v2";
const POLYGON_API_KEY: &str = "o2Jm26X52_0tRq2W7V5JbsCUXdMjL7qk";
const BTC_15M_SERIES: &str = "KXBTC15M";
const ETH_15M_SERIES: &str = "KXETH15M";

/// Simple market state
#[derive(Debug, Clone)]
struct Market {
    ticker: String,
    title: String,
    strike: Option<f64>,
    yes_bid: Option<i64>,
    no_bid: Option<i64>,
}

/// Our resting orders
#[derive(Debug, Clone, Default)]
struct Orders {
    yes_order_id: Option<String>,
    yes_price: Option<i64>,
    no_order_id: Option<String>,
    no_price: Option<i64>,
}

/// Position tracking
#[derive(Debug, Clone, Default)]
struct Position {
    yes_qty: i64,
    no_qty: i64,
    yes_cost: i64,
    no_cost: i64,
}

impl Position {
    fn matched(&self) -> i64 {
        self.yes_qty.min(self.no_qty)
    }
    fn profit(&self) -> i64 {
        // Locked profit from matched pairs
        self.matched() * 100 - self.yes_cost - self.no_cost
    }
    fn unrealized(&self, yes_fair: i64, no_fair: i64) -> i64 {
        // Unrealized P&L based on fair value (50¢ each at ATM)
        let yes_value = self.yes_qty * yes_fair;
        let no_value = self.no_qty * no_fair;
        (yes_value + no_value) - (self.yes_cost + self.no_cost)
    }
}

/// State
struct State {
    markets: HashMap<String, Market>,
    orders: HashMap<String, Orders>,
    positions: HashMap<String, Position>,
}

impl State {
    fn new() -> Self {
        Self {
            markets: HashMap::new(),
            orders: HashMap::new(),
            positions: HashMap::new(),
        }
    }
}

/// Check if spot is near strike (ATM)
fn is_atm(spot: f64, strike: f64, threshold_pct: f64) -> bool {
    let diff_pct = ((spot - strike) / strike).abs() * 100.0;
    diff_pct <= threshold_pct
}

/// Parse seconds remaining from ticker
fn parse_secs_remaining(ticker: &str) -> Option<i64> {
    let parts: Vec<&str> = ticker.split('-').collect();
    if parts.len() < 2 {
        return None;
    }
    let dt = parts[1];
    if dt.len() < 11 {
        return None;
    }

    let hhmm = &dt[dt.len() - 4..];
    let hour: u32 = hhmm[0..2].parse().ok()?;
    let minute: u32 = hhmm[2..4].parse().ok()?;

    // EST to UTC
    let hour_utc = (hour + 5) % 24;
    let now = Utc::now();
    let expiry = NaiveTime::from_hms_opt(hour_utc, minute, 0)?;
    let current = now.time();

    let diff = expiry.num_seconds_from_midnight() as i64 - current.num_seconds_from_midnight() as i64;
    if diff < -60 {
        Some(diff + 86400)
    } else {
        Some(diff)
    }
}

/// Discover BTC and ETH 15-minute markets
async fn discover_markets(client: &KalshiApiClient) -> Result<Vec<Market>> {
    let mut markets = Vec::new();

    // Scrape strikes from Kalshi (fallback if API doesn't have floor_strike)
    let btc_strike = scrape_strike("BTC").await;
    let eth_strike = scrape_strike("ETH").await;

    debug!("[DISCOVER] Scraped strikes: BTC=${:?} ETH=${:?}", btc_strike, eth_strike);

    for series in [BTC_15M_SERIES, ETH_15M_SERIES] {
        let scraped_strike = if series == BTC_15M_SERIES { btc_strike } else { eth_strike };

        let events = client.get_events(series, 100).await?;
        for event in events {
            let event_markets = client.get_markets(&event.event_ticker).await?;
            for m in event_markets {
                // Priority: API floor_strike > scraped strike
                let market_strike = m.floor_strike.or(scraped_strike);
                markets.push(Market {
                    ticker: m.ticker,
                    title: m.title,
                    strike: market_strike,
                    yes_bid: None,
                    no_bid: None,
                });
            }
        }
    }

    Ok(markets)
}

/// Scrape strike from Kalshi page
async fn scrape_strike(asset: &str) -> Option<f64> {
    // Use correct URL slugs - ETH page redirects to a different slug
    let url = if asset == "BTC" {
        "https://kalshi.com/markets/kxbtc15m/bitcoin-price-up-down"
    } else {
        "https://kalshi.com/markets/kxeth15m/eth-15m-price-up-down"
    };

    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(10))
        .redirect(reqwest::redirect::Policy::limited(5))
        .build()
        .ok()?;

    let resp = client
        .get(url)
        .header("User-Agent", "Mozilla/5.0")
        .send()
        .await
        .ok()?;

    let html = resp.text().await.ok()?;
    let marker = "Price to beat:";
    let idx = html.find(marker)?;
    let after = &html[idx + marker.len()..];
    let dollar_idx = after.find('$')?;
    let after_dollar = &after[dollar_idx + 1..];

    let mut num_str = String::new();
    for c in after_dollar.chars() {
        if c.is_ascii_digit() || c == '.' {
            num_str.push(c);
        } else if c == ',' {
            continue;
        } else if !num_str.is_empty() {
            break;
        }
    }

    debug!("[SCRAPE] {} strike: ${}", asset, num_str);
    num_str.parse().ok()
}

/// Process orderbook update
fn process_orderbook(market: &mut Market, yes_levels: &Option<Vec<Vec<i64>>>, no_levels: &Option<Vec<Vec<i64>>>) {
    // Best YES bid
    if let Some(levels) = yes_levels {
        market.yes_bid = levels
            .iter()
            .filter_map(|l| if l.len() >= 2 && l[1] > 0 { Some(l[0]) } else { None })
            .max();
    }
    // Best NO bid
    if let Some(levels) = no_levels {
        market.no_bid = levels
            .iter()
            .filter_map(|l| if l.len() >= 2 && l[1] > 0 { Some(l[0]) } else { None })
            .max();
    }
}

#[derive(Deserialize)]
struct WsMsg {
    #[serde(rename = "type")]
    msg_type: String,
    msg: Option<WsMsgBody>,
}

#[derive(Deserialize)]
struct WsMsgBody {
    market_ticker: Option<String>,
    yes: Option<Vec<Vec<i64>>>,
    no: Option<Vec<Vec<i64>>>,
}

#[derive(Serialize)]
struct SubCmd {
    id: i32,
    cmd: &'static str,
    params: SubParams,
}

#[derive(Serialize)]
struct SubParams {
    channels: Vec<&'static str>,
    market_tickers: Vec<String>,
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::from_default_env()
                .add_directive("atm_sniper=info".parse().unwrap())
                .add_directive("arb_bot=info".parse().unwrap()),
        )
        .init();

    info!("════════════════════════════════════════════════════════════════════════");
    info!("🎯 ATM SNIPER - Simple At-The-Money Strategy");
    // Parse CLI args
    let args = Args::parse();
    let contracts = args.contracts;
    let max_bid = args.max_bid;
    let atm_threshold = args.threshold;
    let dry_run = !args.live;

    info!("════════════════════════════════════════════════════════════════════════");
    info!("STRATEGY:");
    info!("   1. Find markets where spot ≈ strike (ATM, delta ≈ 0.5)");
    info!("   2. Bid at best_bid + 1¢ for YES and NO (max {}¢)", max_bid);
    info!("   3. At ATM, fair value = 50¢, anything below is +EV");
    info!("   4. If both fill at max: {}¢ cost → $1 payout = {}¢ profit", max_bid * 2, 100 - max_bid * 2);
    info!("════════════════════════════════════════════════════════════════════════");

    info!("CONFIG:");
    info!("   Mode: {}", if dry_run { "🔍 DRY RUN" } else { "🚀 LIVE" });
    info!("   Max bid: {}¢ (fair = 50¢ at ATM)", max_bid);
    info!("   Contracts: {} per side", contracts);
    info!("   ATM threshold: {}% from strike", atm_threshold);
    info!("════════════════════════════════════════════════════════════════════════");

    if !dry_run {
        warn!("⚠️  LIVE MODE - Real money!");
        tokio::time::sleep(Duration::from_secs(3)).await;
    }

    let config = KalshiConfig::from_env()?;
    let client = Arc::new(KalshiApiClient::new(KalshiConfig::from_env()?));

    // Discover markets
    let markets = discover_markets(&client).await?;
    info!("Found {} markets", markets.len());

    let state = Arc::new(RwLock::new({
        let mut s = State::new();
        for m in markets {
            let ticker = m.ticker.clone();
            s.markets.insert(ticker.clone(), m);
            s.orders.insert(ticker.clone(), Orders::default());
            s.positions.insert(ticker, Position::default());
        }
        s
    }));

    // Start price feed
    let prices = Arc::new(RwLock::new(PriceState::default()));
    let prices_clone = prices.clone();
    tokio::spawn(async move {
        run_polygon_feed(prices_clone, POLYGON_API_KEY).await;
    });

    // Discovery loop
    let disc_client = client.clone();
    let disc_state = state.clone();
    tokio::spawn(async move {
        let mut interval = tokio::time::interval(Duration::from_secs(30));
        loop {
            interval.tick().await;
            if let Ok(new_markets) = discover_markets(&disc_client).await {
                let mut s = disc_state.write().await;
                for m in new_markets {
                    if !s.markets.contains_key(&m.ticker) {
                        info!("[DISCOVER] New: {}", m.ticker);
                        let ticker = m.ticker.clone();
                        s.markets.insert(ticker.clone(), m);
                        s.orders.insert(ticker.clone(), Orders::default());
                        s.positions.insert(ticker, Position::default());
                    }
                }
            }
        }
    });

    // Main WebSocket loop
    loop {
        info!("[WS] Connecting...");

        let tickers: Vec<String> = {
            let s = state.read().await;
            s.markets.keys().cloned().collect()
        };

        if tickers.is_empty() {
            info!("[WS] No markets, waiting...");
            tokio::time::sleep(Duration::from_secs(5)).await;
            continue;
        }

        let timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)?
            .as_millis()
            .to_string();
        let signature = config.sign(&format!("{}GET/trade-api/ws/v2", timestamp))?;

        let request = Request::builder()
            .uri(KALSHI_WS_URL)
            .header("KALSHI-ACCESS-KEY", &config.api_key_id)
            .header("KALSHI-ACCESS-SIGNATURE", &signature)
            .header("KALSHI-ACCESS-TIMESTAMP", &timestamp)
            .header("Host", "api.elections.kalshi.com")
            .header("Connection", "Upgrade")
            .header("Upgrade", "websocket")
            .header("Sec-WebSocket-Version", "13")
            .header("Sec-WebSocket-Key", tokio_tungstenite::tungstenite::handshake::client::generate_key())
            .body(())?;

        let (ws, _) = match connect_async(request).await {
            Ok(s) => s,
            Err(e) => {
                error!("[WS] Connect failed: {}", e);
                tokio::time::sleep(Duration::from_secs(5)).await;
                continue;
            }
        };

        let (mut write, mut read) = ws.split();

        // Subscribe to orderbook
        let sub = SubCmd {
            id: 1,
            cmd: "subscribe",
            params: SubParams {
                channels: vec!["orderbook_delta"],
                market_tickers: tickers.clone(),
            },
        };
        let _ = write.send(Message::Text(serde_json::to_string(&sub)?)).await;
        info!("[WS] Subscribed to {} markets", tickers.len());

        // Subscribe to fills
        let fill_sub = serde_json::json!({
            "id": 2,
            "cmd": "subscribe",
            "params": { "channels": ["fill"] }
        });
        let _ = write.send(Message::Text(fill_sub.to_string())).await;
        info!("[WS] Subscribed to fill notifications");

        let mut status_interval = tokio::time::interval(Duration::from_secs(5));

        loop {
            tokio::select! {
                _ = status_interval.tick() => {
                    let s = state.read().await;
                    let p = prices.read().await;

                    for (ticker, market) in &s.markets {
                        let secs = parse_secs_remaining(ticker).unwrap_or(0);
                        if secs <= 0 || secs > 900 { continue; }

                        // Use BTC or ETH spot based on ticker
                        let spot = if ticker.contains("ETH") { p.eth_price } else { p.btc_price };

                        let atm = match (spot, market.strike) {
                            (Some(s), Some(k)) => is_atm(s, k, atm_threshold),
                            _ => false,
                        };

                        let orders = s.orders.get(ticker).cloned().unwrap_or_default();
                        let pos = s.positions.get(ticker).cloned().unwrap_or_default();

                        let atm_str = if atm { "✓ ATM" } else { "✗ OTM" };
                        let spot_str = spot.map(|s| format!("{:.0}", s)).unwrap_or("-".into());
                        let strike_str = market.strike.map(|k| format!("{:.0}", k)).unwrap_or("-".into());
                        let asset = if ticker.contains("ETH") { "ETH" } else { "BTC" };

                        // Calculate diff and pct diff
                        let (diff_str, pct_str) = match (spot, market.strike) {
                            (Some(s), Some(k)) => {
                                let diff = s - k;
                                let pct = ((s - k) / k) * 100.0;
                                (format!("{:+.0}", diff), format!("{:+.3}%", pct))
                            }
                            _ => ("-".into(), "-".into()),
                        };

                        let unrealized = pos.unrealized(50, 50); // Fair value at ATM
                        info!("[STATUS] {} | {} | {}m{}s left | {} | Spot={} Strike={} Diff={} ({}) | Bids: Y={} N={} | Pos: Y={} N={} locked={}¢ unreal={}¢",
                              asset, ticker, secs/60, secs%60, atm_str, spot_str, strike_str, diff_str, pct_str,
                              orders.yes_price.map(|p| format!("{}¢", p)).unwrap_or("-".into()),
                              orders.no_price.map(|p| format!("{}¢", p)).unwrap_or("-".into()),
                              pos.yes_qty, pos.no_qty, pos.profit(), unrealized);
                    }
                }

                msg = read.next() => {
                    let Some(msg) = msg else { break; };

                    match msg {
                        Ok(Message::Text(text)) => {
                            if let Ok(ws_msg) = serde_json::from_str::<WsMsg>(&text) {
                                // Handle fill notifications
                                if ws_msg.msg_type == "fill" {
                                    if let Ok(fill_data) = serde_json::from_str::<serde_json::Value>(&text) {
                                        if let Some(msg) = fill_data.get("msg") {
                                            // Try multiple field names for ticker
                                            let fill_ticker = msg.get("market_ticker")
                                                .or_else(|| msg.get("ticker"))
                                                .and_then(|v| v.as_str());
                                            let fill_side = msg.get("side").and_then(|v| v.as_str());
                                            let fill_count = msg.get("count").and_then(|v| v.as_i64()).unwrap_or(0);
                                            let fill_price = msg.get("yes_price")
                                                .or_else(|| msg.get("no_price"))
                                                .or_else(|| msg.get("price"))
                                                .and_then(|v| v.as_i64()).unwrap_or(0);

                                            let ticker_str = fill_ticker.unwrap_or("?");
                                            let side_str = fill_side.unwrap_or("?");

                                            warn!("════════════════════════════════════════════════════════════════");
                                            warn!("[FILL] 🎯 GOT FILLED!");
                                            warn!("[FILL] Ticker: {} | Side: {} | Qty: {} | Price: {}¢",
                                                  ticker_str, side_str, fill_count, fill_price);
                                            warn!("[FILL] Raw: {}", msg);
                                            warn!("════════════════════════════════════════════════════════════════");

                                            // Update position
                                            if let (Some(ticker), Some(side)) = (fill_ticker, fill_side) {
                                                let mut s = state.write().await;
                                                let mut need_completion = false;
                                                let mut completion_side = "";
                                                let mut completion_max_price: i64 = 0;
                                                let mut completion_qty: i64 = 0;

                                                if let Some(pos) = s.positions.get_mut(ticker) {
                                                    let cost = fill_count * fill_price;
                                                    if side == "yes" {
                                                        pos.yes_qty += fill_count;
                                                        pos.yes_cost += cost;
                                                        info!("[FILL] YES position: +{} @{}¢ | Total: Y={} N={} | Locked={}¢",
                                                              fill_count, fill_price, pos.yes_qty, pos.no_qty, pos.profit());

                                                        // Check if we need to complete with NO
                                                        let unmatched = pos.yes_qty - pos.no_qty;
                                                        if unmatched > 0 {
                                                            // Max we can pay for NO to still profit: 100 - avg_yes_cost - 4
                                                            let avg_yes = pos.yes_cost / pos.yes_qty;
                                                            completion_max_price = 96 - avg_yes; // Leave 4¢ profit
                                                            completion_side = "no";
                                                            completion_qty = unmatched;
                                                            need_completion = true;
                                                        }
                                                    } else if side == "no" {
                                                        pos.no_qty += fill_count;
                                                        pos.no_cost += cost;
                                                        info!("[FILL] NO position: +{} @{}¢ | Total: Y={} N={} | Locked={}¢",
                                                              fill_count, fill_price, pos.yes_qty, pos.no_qty, pos.profit());

                                                        // Check if we need to complete with YES
                                                        let unmatched = pos.no_qty - pos.yes_qty;
                                                        if unmatched > 0 {
                                                            // Max we can pay for YES to still profit: 100 - avg_no_cost - 4
                                                            let avg_no = pos.no_cost / pos.no_qty;
                                                            completion_max_price = 96 - avg_no; // Leave 4¢ profit
                                                            completion_side = "yes";
                                                            completion_qty = unmatched;
                                                            need_completion = true;
                                                        }
                                                    }
                                                }
                                                // Clear the filled order from tracking
                                                if let Some(orders) = s.orders.get_mut(ticker) {
                                                    if side == "yes" {
                                                        orders.yes_order_id = None;
                                                        orders.yes_price = None;
                                                    } else if side == "no" {
                                                        orders.no_order_id = None;
                                                        orders.no_price = None;
                                                    }
                                                }

                                                let ticker_owned = ticker.to_string();
                                                drop(s);

                                                // Try to complete the position
                                                if need_completion && completion_max_price > 0 && !dry_run {
                                                    warn!("[COMPLETE] Trying to buy {} {} @{}¢ max to lock profit on {}",
                                                          completion_qty, completion_side, completion_max_price, ticker_owned);

                                                    match client.buy_limit(&ticker_owned, completion_side, completion_max_price, completion_qty).await {
                                                        Ok(resp) => {
                                                            warn!("[COMPLETE] ✅ Order placed: {} | Status: {:?}",
                                                                  resp.order.order_id, resp.order.status);
                                                            // Track this order
                                                            let mut s = state.write().await;
                                                            if let Some(orders) = s.orders.get_mut(&ticker_owned) {
                                                                if completion_side == "yes" {
                                                                    orders.yes_order_id = Some(resp.order.order_id);
                                                                    orders.yes_price = Some(completion_max_price);
                                                                } else {
                                                                    orders.no_order_id = Some(resp.order.order_id);
                                                                    orders.no_price = Some(completion_max_price);
                                                                }
                                                            }
                                                        }
                                                        Err(e) => error!("[COMPLETE] ❌ Failed: {}", e),
                                                    }
                                                } else if need_completion && dry_run {
                                                    warn!("[COMPLETE] [DRY] Would buy {} {} @{}¢ max to lock profit on {}",
                                                          completion_qty, completion_side, completion_max_price, ticker_owned);
                                                }
                                            }
                                        }
                                    }
                                    continue;
                                }

                                let ticker = ws_msg.msg.as_ref().and_then(|m| m.market_ticker.as_ref());
                                let Some(ticker) = ticker else { continue; };

                                if ws_msg.msg_type == "orderbook_snapshot" || ws_msg.msg_type == "orderbook_delta" {
                                    if let Some(body) = &ws_msg.msg {
                                        let mut s = state.write().await;

                                        if let Some(market) = s.markets.get_mut(ticker) {
                                            process_orderbook(market, &body.yes, &body.no);

                                            let secs = parse_secs_remaining(ticker).unwrap_or(0);
                                            if secs <= 0 || secs > 900 { continue; }

                                            let p = prices.read().await;
                                            // Use BTC or ETH spot based on ticker
                                            let spot = if ticker.contains("ETH") { p.eth_price } else { p.btc_price };
                                            let strike = market.strike;
                                            drop(p);

                                            // Extract market data before getting orders (to avoid borrow conflict)
                                            let yes_best_bid = market.yes_bid.unwrap_or(0);
                                            let no_best_bid = market.no_bid.unwrap_or(0);

                                            let atm = match (spot, strike) {
                                                (Some(s), Some(k)) => is_atm(s, k, atm_threshold),
                                                _ => false,
                                            };

                                            // Get current orders
                                            let orders = s.orders.get(ticker).cloned().unwrap_or_default();
                                            let ticker_clone = ticker.clone();
                                            let has_orders = orders.yes_order_id.is_some() || orders.no_order_id.is_some();

                                            // If NOT ATM and we have orders, CANCEL them
                                            if !atm && has_orders {
                                                let spot_str = spot.map(|s| format!("{:.0}", s)).unwrap_or("-".into());
                                                let strike_str = strike.map(|k| format!("{:.0}", k)).unwrap_or("-".into());
                                                warn!("[CANCEL] {} moved OTM | Spot={} Strike={} | Cancelling orders", ticker, spot_str, strike_str);

                                                drop(s);

                                                // Cancel YES order if exists
                                                if let Some(oid) = &orders.yes_order_id {
                                                    match client.cancel_order(oid).await {
                                                        Ok(_) => info!("[CANCEL] ✅ YES order cancelled"),
                                                        Err(e) => warn!("[CANCEL] ❌ YES cancel failed: {}", e),
                                                    }
                                                }
                                                // Cancel NO order if exists
                                                if let Some(oid) = &orders.no_order_id {
                                                    match client.cancel_order(oid).await {
                                                        Ok(_) => info!("[CANCEL] ✅ NO order cancelled"),
                                                        Err(e) => warn!("[CANCEL] ❌ NO cancel failed: {}", e),
                                                    }
                                                }

                                                // Clear order state
                                                let mut s = state.write().await;
                                                if let Some(orders) = s.orders.get_mut(&ticker_clone) {
                                                    orders.yes_order_id = None;
                                                    orders.yes_price = None;
                                                    orders.no_order_id = None;
                                                    orders.no_price = None;
                                                }
                                                continue;
                                            }

                                            if !atm {
                                                debug!("[SKIP] {} not ATM", ticker);
                                                continue;
                                            }

                                            // ATM! Check if we need to post bids

                                            let need_yes = orders.yes_order_id.is_none();
                                            let need_no = orders.no_order_id.is_none();

                                            if !need_yes && !need_no {
                                                continue; // Already have orders
                                            }

                                            // Bid at max_bid - we want to be aggressive when ATM
                                            // If best_bid is higher than max_bid, we won't outbid (capped at max_bid)
                                            let yes_our_bid = max_bid;
                                            let no_our_bid = max_bid;

                                            drop(s);

                                            let spot_str = spot.map(|s| format!("{:.0}", s)).unwrap_or("-".into());
                                            let strike_str = strike.map(|k| format!("{:.0}", k)).unwrap_or("-".into());

                                            if dry_run {
                                                if need_yes {
                                                    info!("[DRY] Would bid {} YES @{}¢ (best={}¢) on {} | Spot={} Strike={}",
                                                          contracts, yes_our_bid, yes_best_bid, ticker_clone, spot_str, strike_str);
                                                }
                                                if need_no {
                                                    info!("[DRY] Would bid {} NO @{}¢ (best={}¢) on {} | Spot={} Strike={}",
                                                          contracts, no_our_bid, no_best_bid, ticker_clone, spot_str, strike_str);
                                                }
                                            } else {
                                                // Post bids
                                                if need_yes {
                                                    info!("[BID] Posting {} YES @{}¢ (best={}¢) on {} | Spot={} Strike={}",
                                                          contracts, yes_our_bid, yes_best_bid, ticker_clone, spot_str, strike_str);
                                                    match client.buy_limit(&ticker_clone, "yes", yes_our_bid, contracts).await {
                                                        Ok(resp) => {
                                                            info!("[BID] ✅ YES order: {}", resp.order.order_id);
                                                            let mut s = state.write().await;
                                                            if let Some(orders) = s.orders.get_mut(&ticker_clone) {
                                                                orders.yes_order_id = Some(resp.order.order_id);
                                                                orders.yes_price = Some(yes_our_bid);
                                                            }
                                                        }
                                                        Err(e) => error!("[BID] ❌ YES failed: {}", e),
                                                    }
                                                }
                                                if need_no {
                                                    info!("[BID] Posting {} NO @{}¢ (best={}¢) on {} | Spot={} Strike={}", contracts, no_our_bid, no_best_bid, ticker_clone, spot_str, strike_str);
                                                    match client.buy_limit(&ticker_clone, "no", no_our_bid, contracts).await {
                                                        Ok(resp) => {
                                                            info!("[BID] ✅ NO order: {}", resp.order.order_id);
                                                            let mut s = state.write().await;
                                                            if let Some(orders) = s.orders.get_mut(&ticker_clone) {
                                                                orders.no_order_id = Some(resp.order.order_id);
                                                                orders.no_price = Some(no_our_bid);
                                                            }
                                                        }
                                                        Err(e) => error!("[BID] ❌ NO failed: {}", e),
                                                    }
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
