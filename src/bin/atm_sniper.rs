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

/// ATM Sniper - Fair Value Based Binary Option Strategy
#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Args {
    /// Number of contracts per side
    #[arg(short, long, default_value_t = 40)]
    contracts: i64,

    /// Max bid price in cents (used when fair value = 50, i.e. ATM)
    #[arg(short, long, default_value_t = 45)]
    max_bid: i64,

    /// Edge in cents required to buy YES (default: 6 - be selective)
    #[arg(long, default_value_t = 6)]
    req_yes_edge: i64,

    /// Edge in cents required to buy NO (default: 3 - be aggressive)
    #[arg(long, default_value_t = 3)]
    req_no_edge: i64,

    /// Edge threshold to buy aggressively IOC (hit the ask) instead of passive bid (default: 20)
    #[arg(long, default_value_t = 20)]
    aggro_edge: i64,

    /// Buy cheap options: buy YES if price <= this with 8+ min left (default: 20)
    #[arg(long, default_value_t = 20)]
    cheap_yes: i64,

    /// Buy cheap options: buy NO if YES price >= this with 8+ min left (default: 80)
    #[arg(long, default_value_t = 80)]
    cheap_no: i64,

    /// Minutes remaining required for cheap option buys (default: 8)
    #[arg(long, default_value_t = 8)]
    cheap_minutes: i64,

    /// Annualized volatility % for fair value calc (default: 58%)
    #[arg(short, long, default_value_t = 58.0)]
    vol: f64,

    /// Live trading mode (default is dry run)
    #[arg(short, long, default_value_t = false)]
    live: bool,

    /// Completion only mode - disable edge/penny/cheap buying, only complete existing positions
    #[arg(long, default_value_t = false)]
    completion_only: bool,
}

use arb_bot::kalshi::{KalshiApiClient, KalshiConfig};
use arb_bot::fair_value::calc_fair_value_cents;

// Reuse modules from kalshi_crypto_arb
mod pricing;
mod polygon_feed;
#[path = "atm_sniper_lib/orderbook.rs"]
mod orderbook;
#[path = "atm_sniper_lib/trading_logic.rs"]
mod trading_logic;
use polygon_feed::{PriceState, run_polygon_feed};
use orderbook::Orderbook;
use trading_logic::calc_bid_price;

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
    book: Orderbook,
}

/// Our resting orders
#[derive(Debug, Clone, Default)]
struct Orders {
    yes_order_id: Option<String>,
    yes_price: Option<i64>,
    no_order_id: Option<String>,
    no_price: Option<i64>,
    // Cooldown for IOC attempts (prevent spam)
    last_yes_ioc: Option<std::time::Instant>,
    last_no_ioc: Option<std::time::Instant>,
    // Cooldown after cancel (prevent rapid re-placing)
    last_yes_cancel: Option<std::time::Instant>,
    last_no_cancel: Option<std::time::Instant>,
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

    for series in [BTC_15M_SERIES] {  // TODO: Add ETH_15M_SERIES back
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
                    book: Orderbook::new(),
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
    info!("🎯 ATM SNIPER - Fair Value Based Strategy");
    // Parse CLI args
    let args = Args::parse();
    let contracts = args.contracts;
    let max_bid = args.max_bid;
    let req_yes_edge = args.req_yes_edge;
    let req_no_edge = args.req_no_edge;
    let aggro_edge = args.aggro_edge;
    let cheap_yes = args.cheap_yes;
    let cheap_no = args.cheap_no;
    let cheap_minutes = args.cheap_minutes;
    let vol = args.vol / 100.0; // Convert % to decimal
    let dry_run = !args.live;
    let completion_only = args.completion_only;

    info!("════════════════════════════════════════════════════════════════════════");
    if completion_only {
        info!("STRATEGY: COMPLETION ONLY MODE");
        info!("   - Edge buying: DISABLED");
        info!("   - Penny bidding: DISABLED");
        info!("   - Cheap buying: DISABLED");
        info!("   - Aggro IOC: DISABLED");
        info!("   - Completion orders: ENABLED (buy opposite side after fills)");
    } else {
        info!("STRATEGY:");
        info!("   1. Calculate fair value using Black-Scholes (vol={}%)", args.vol);
        info!("   2. Buy YES when market_yes < fair_yes - {}¢ (selective)", req_yes_edge);
        info!("   3. Buy NO when market_no < fair_no - {}¢ (aggressive)", req_no_edge);
        info!("   4. If edge >= {}¢, buy IOC at ask (aggressive)", aggro_edge);
        info!("   5. CHEAP: Buy YES if price ≤{}¢ with {}+min left", cheap_yes, cheap_minutes);
        info!("   6. CHEAP: Buy NO if YES price ≥{}¢ with {}+min left", cheap_no, cheap_minutes);
        info!("   7. Max bid at ATM (fair=50¢): {}¢", max_bid);
    }
    info!("════════════════════════════════════════════════════════════════════════");

    info!("CONFIG:");
    info!("   Mode: {}{}", if dry_run { "🔍 DRY RUN" } else { "🚀 LIVE" }, if completion_only { " (COMPLETION ONLY)" } else { "" });
    if !completion_only {
        info!("   YES edge: {}¢ | NO edge: {}¢ | Aggro: {}¢", req_yes_edge, req_no_edge, aggro_edge);
        info!("   Cheap buys: YES≤{}¢ NO≥{}¢ ({}+min)", cheap_yes, cheap_no, cheap_minutes);
        info!("   Max bid (at ATM): {}¢", max_bid);
    }
    info!("   Volatility: {}%", args.vol);
    info!("   Contracts: {} per side", contracts);
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
                        info!("[DISCOVER] New market found: {}", m.ticker);
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

                        let orders = s.orders.get(ticker).cloned().unwrap_or_default();
                        let pos = s.positions.get(ticker).cloned().unwrap_or_default();

                        let spot_str = spot.map(|s| format!("{:.0}", s)).unwrap_or("-".into());
                        let strike_str = market.strike.map(|k| format!("{:.0}", k)).unwrap_or("-".into());
                        let asset = if ticker.contains("ETH") { "ETH" } else { "BTC" };

                        // Calculate fair value
                        let minutes = secs as f64 / 60.0;
                        let (fair_yes, fair_no, diff_str, pct_str) = match (spot, market.strike) {
                            (Some(s), Some(k)) => {
                                let (fy, fn_) = calc_fair_value_cents(s, k, minutes, vol);
                                let diff = s - k;
                                let pct = ((s - k) / k) * 100.0;
                                (fy, fn_, format!("{:+.0}", diff), format!("{:+.3}%", pct))
                            }
                            _ => (50, 50, "-".into(), "-".into()),
                        };

                        // Check for edge opportunities
                        let yes_market = market.book.yes_ask_or_100();
                        let no_market = market.book.no_ask_or_100();
                        let yes_edge_actual = fair_yes - yes_market as i64;
                        let no_edge_actual = fair_no - no_market as i64;

                        let edge_str = if yes_edge_actual >= req_yes_edge || no_edge_actual >= req_no_edge {
                            format!("✓ EDGE Y:{:+}¢ N:{:+}¢", yes_edge_actual, no_edge_actual)
                        } else {
                            format!("Y:{:+}¢ N:{:+}¢", yes_edge_actual, no_edge_actual)
                        };

                        let unrealized = pos.unrealized(fair_yes, fair_no);

                        // Arb condition: can we buy both sides for < 100¢?
                        let yes_ask = market.book.yes_ask_or_100();
                        let no_ask = market.book.no_ask_or_100();
                        let arb_cost = yes_ask + no_ask;
                        let arb_profit = 100 - arb_cost;
                        let arb_str = if arb_profit > 0 {
                            format!("🎯 ARB: {}+{}={}¢ ({}¢ profit)", yes_ask, no_ask, arb_cost, arb_profit)
                        } else {
                            format!("ARB: {}+{}={}¢", yes_ask, no_ask, arb_cost)
                        };

                        // Exposure: how many contracts are unhedged?
                        let exposure = (pos.yes_qty - pos.no_qty).abs();
                        let exposure_side = if pos.yes_qty > pos.no_qty { "Y" } else if pos.no_qty > pos.yes_qty { "N" } else { "-" };

                        info!("[STATUS] {} | {} | {}m{}s | Fair Y={}¢ N={}¢ | Mkt Y={}¢ N={}¢ | {} | {} | Exp: {}{}",
                              asset, ticker, secs/60, secs%60, fair_yes, fair_no,
                              yes_market, no_market, edge_str, arb_str,
                              exposure, exposure_side);
                        info!("         Spot={} K={} ({}) | Pos: Y={} N={} pnl={}¢",
                              spot_str, strike_str, pct_str,
                              pos.yes_qty, pos.no_qty, unrealized);
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

                                            let order_id = msg.get("order_id").and_then(|v| v.as_str()).unwrap_or("?");
                                            let trade_id = msg.get("trade_id").and_then(|v| v.as_str()).unwrap_or("?");
                                            let is_taker = msg.get("is_taker").and_then(|v| v.as_bool()).unwrap_or(false);
                                            let taker_str = if is_taker { "TAKER" } else { "MAKER" };
                                            info!("[FILL] 🎯 {} {} x{} @{}¢ | {} | order={} trade={}",
                                                  ticker_str, side_str.to_uppercase(), fill_count, fill_price, taker_str, order_id, trade_id);

                                            // Update position
                                            if let (Some(ticker), Some(side)) = (fill_ticker, fill_side) {
                                                let mut s = state.write().await;
                                                let mut need_completion = false;
                                                let mut completion_side = "";
                                                let mut completion_max_price: i64 = 0;
                                                let mut completion_qty: i64 = 0;

                                                // Check if this fill was from a completion order
                                                let mut was_completion = false;
                                                if let Some(orders) = s.orders.get(ticker) {
                                                    if side == "yes" && orders.yes_order_id.as_deref() == Some(order_id) {
                                                        was_completion = true;
                                                    } else if side == "no" && orders.no_order_id.as_deref() == Some(order_id) {
                                                        was_completion = true;
                                                    }
                                                }
                                                if was_completion {
                                                    info!("[COMPLETE] ✅ Completion order filled! {} {} x{} @{}¢", ticker, side.to_uppercase(), fill_count, fill_price);
                                                }

                                                // Check if we can immediately flip for profit
                                                let mut can_flip = false;
                                                let mut flip_bid: i64 = 0;
                                                const FLIP_PROFIT: i64 = 5; // Min profit in cents to flip

                                                if let Some(market) = s.markets.get(ticker) {
                                                    let current_bid = if side == "yes" { market.book.yes_bid } else { market.book.no_bid };
                                                    if let Some(bid) = current_bid {
                                                        if bid >= fill_price + FLIP_PROFIT {
                                                            can_flip = true;
                                                            flip_bid = bid;
                                                        }
                                                    }
                                                }

                                                if let Some(pos) = s.positions.get_mut(ticker) {
                                                    let cost = fill_count * fill_price;
                                                    if side == "yes" {
                                                        pos.yes_qty += fill_count;
                                                        pos.yes_cost += cost;
                                                        info!("[FILL] YES position: +{} @{}¢ | Total: Y={} N={} | Locked={}¢",
                                                              fill_count, fill_price, pos.yes_qty, pos.no_qty, pos.profit());

                                                        // Check if we need to complete with NO (only if not flipping)
                                                        if !can_flip {
                                                            let unmatched = pos.yes_qty - pos.no_qty;
                                                            if unmatched > 0 {
                                                                // Max we can pay for NO to break even: 100 - avg_yes_cost
                                                                // Leave 1¢ profit margin
                                                                let avg_yes = pos.yes_cost / pos.yes_qty;
                                                                completion_max_price = 100 - avg_yes - 1;
                                                                info!("[COMPLETE] Calc: yes_cost={}¢ / yes_qty={} = avg_yes={}¢ | max_no = 100 - {} - 1 = {}¢",
                                                                      pos.yes_cost, pos.yes_qty, avg_yes, avg_yes, completion_max_price);
                                                                completion_side = "no";
                                                                completion_qty = unmatched;
                                                                need_completion = true;
                                                            }
                                                        }
                                                    } else if side == "no" {
                                                        pos.no_qty += fill_count;
                                                        pos.no_cost += cost;
                                                        info!("[FILL] NO position: +{} @{}¢ | Total: Y={} N={} | Locked={}¢",
                                                              fill_count, fill_price, pos.yes_qty, pos.no_qty, pos.profit());

                                                        // Check if we need to complete with YES (only if not flipping)
                                                        if !can_flip {
                                                            let unmatched = pos.no_qty - pos.yes_qty;
                                                            if unmatched > 0 {
                                                                // Max we can pay for YES to break even: 100 - avg_no_cost
                                                                // Leave 1¢ profit margin
                                                                let avg_no = pos.no_cost / pos.no_qty;
                                                                completion_max_price = 100 - avg_no - 1;
                                                                info!("[COMPLETE] Calc: no_cost={}¢ / no_qty={} = avg_no={}¢ | max_yes = 100 - {} - 1 = {}¢",
                                                                      pos.no_cost, pos.no_qty, avg_no, avg_no, completion_max_price);
                                                                completion_side = "yes";
                                                                completion_qty = unmatched;
                                                                need_completion = true;
                                                            }
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
                                                let fill_side_owned = side.to_string();
                                                drop(s);

                                                // PRIORITY 1: Try to flip immediately for profit
                                                if can_flip && !dry_run {
                                                    info!("════════════════════════════════════════════════════════════════");
                                                    info!("[FLIP] 💰 FLIPPING {} {} for instant profit!", fill_side_owned.to_uppercase(), ticker_owned);
                                                    info!("[FLIP] Bought @{}¢, selling @{}¢ bid = {}¢ profit/contract!",
                                                          fill_price, flip_bid, flip_bid - fill_price);
                                                    info!("════════════════════════════════════════════════════════════════");

                                                    match client.sell_ioc(&ticker_owned, &fill_side_owned, flip_bid, fill_count).await {
                                                        Ok(resp) => {
                                                            let filled = resp.order.filled_count();
                                                            let profit = filled * (flip_bid - fill_price);
                                                            // Debug: log full order response
                                                            info!("[FLIP] Response: status={:?} filled={} taker={:?} maker={:?} remaining={:?}",
                                                                  resp.order.status, filled,
                                                                  resp.order.taker_fill_count, resp.order.maker_fill_count,
                                                                  resp.order.remaining_count);
                                                            if filled > 0 {
                                                                info!("[FLIP] ✅ SOLD {} @{}¢ | Profit: {}¢ | Status: {:?}",
                                                                      filled, flip_bid, profit, resp.order.status);
                                                                // Update position to remove sold contracts
                                                                let mut s = state.write().await;
                                                                if let Some(pos) = s.positions.get_mut(&ticker_owned) {
                                                                    if fill_side_owned == "yes" {
                                                                        pos.yes_qty -= filled;
                                                                        pos.yes_cost -= filled * fill_price;
                                                                    } else {
                                                                        pos.no_qty -= filled;
                                                                        pos.no_cost -= filled * fill_price;
                                                                    }
                                                                }
                                                            } else {
                                                                info!("[FLIP] ⚠️ No fill - bid may have moved | Status: {:?}", resp.order.status);
                                                            }
                                                        }
                                                        Err(e) => error!("[FLIP] ❌ Sell failed: {}", e),
                                                    }
                                                } else if can_flip && dry_run {
                                                    info!("[FLIP] [DRY] Would sell {} {} @{}¢ (bought @{}¢) = {}¢ profit",
                                                          fill_count, fill_side_owned, flip_bid, fill_price, flip_bid - fill_price);
                                                }

                                                // PRIORITY 2: Try to complete the position (buy opposite side)
                                                if need_completion && completion_max_price > 0 && !dry_run {
                                                    info!("[COMPLETE] Trying to buy {} {} @{}¢ max to lock profit on {}",
                                                          completion_qty, completion_side, completion_max_price, ticker_owned);

                                                    match client.buy_limit(&ticker_owned, completion_side, completion_max_price, completion_qty).await {
                                                        Ok(resp) => {
                                                            info!("[COMPLETE] ✅ Order placed: {} | Status: {:?}",
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
                                                    info!("[COMPLETE] [DRY] Would buy {} {} @{}¢ max to lock profit on {}",
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
                                            // Process snapshot vs delta differently
                                            if ws_msg.msg_type == "orderbook_snapshot" {
                                                market.book.process_snapshot(&body.yes, &body.no);
                                                debug!("[OB] SNAPSHOT {} | yes_bid={:?} no_bid={:?} | yes_levels={} no_levels={}",
                                                       ticker, market.book.yes_bid, market.book.no_bid,
                                                       body.yes.as_ref().map(|v| v.len()).unwrap_or(0),
                                                       body.no.as_ref().map(|v| v.len()).unwrap_or(0));
                                            } else {
                                                market.book.process_delta(&body.yes, &body.no);
                                            }

                                            let secs = parse_secs_remaining(ticker).unwrap_or(0);
                                            if secs <= 0 || secs > 900 { continue; }

                                            let p = prices.read().await;
                                            let spot = if ticker.contains("ETH") { p.eth_price } else { p.btc_price };
                                            let strike = market.strike;
                                            let spike_cooldown = p.is_spike_cooldown();
                                            drop(p);

                                            // Skip trading during spike cooldown (bad tick protection)
                                            if spike_cooldown {
                                                continue;
                                            }

                                            // Calculate fair value
                                            let minutes = secs as f64 / 60.0;
                                            let (fair_yes, fair_no) = match (spot, strike) {
                                                (Some(s), Some(k)) => calc_fair_value_cents(s, k, minutes, vol),
                                                _ => continue,
                                            };

                                            // Market prices from orderbook
                                            let yes_ask = market.book.yes_ask();
                                            let no_ask = market.book.no_ask();

                                            // Calculate actual edge: positive = we can buy below fair value
                                            let yes_edge_actual = yes_ask.map(|a| fair_yes - a as i64);
                                            let no_edge_actual = no_ask.map(|a| fair_no - a as i64);

                                            // Get current orders
                                            let orders = s.orders.get(ticker).cloned().unwrap_or_default();
                                            let ticker_clone = ticker.clone();

                                            // Check if we have unmatched position
                                            let pos = s.positions.get(ticker).cloned().unwrap_or_default();
                                            let unmatched_yes = pos.yes_qty - pos.no_qty;
                                            let unmatched_no = pos.no_qty - pos.yes_qty;

                                            // Determine if we should bid on each side (using asymmetric edge requirements)
                                            let yes_has_edge = yes_edge_actual.map(|e| e >= req_yes_edge).unwrap_or(false);
                                            let no_has_edge = no_edge_actual.map(|e| e >= req_no_edge).unwrap_or(false);

                                            // Cancel orders if edge disappeared
                                            if !yes_has_edge && orders.yes_order_id.is_some() {
                                                if let Some(oid) = &orders.yes_order_id {
                                                    drop(s);
                                                    warn!("[CANCEL] YES edge gone on {} | fair={}¢ ask={:?}¢",
                                                          ticker, fair_yes, yes_ask);
                                                    let _ = client.cancel_order(oid).await;
                                                    let mut s = state.write().await;
                                                    if let Some(orders) = s.orders.get_mut(&ticker_clone) {
                                                        orders.yes_order_id = None;
                                                        orders.yes_price = None;
                                                        orders.last_yes_cancel = Some(std::time::Instant::now());
                                                    }
                                                    continue;
                                                }
                                            }
                                            if !no_has_edge && orders.no_order_id.is_some() {
                                                if let Some(oid) = &orders.no_order_id {
                                                    drop(s);
                                                    warn!("[CANCEL] NO edge gone on {} | fair_yes={}¢ fair_no={}¢ ask={:?}¢ spot={:?} strike={:?}",
                                                          ticker, fair_yes, fair_no, no_ask, spot, strike);
                                                    let _ = client.cancel_order(oid).await;
                                                    let mut s = state.write().await;
                                                    if let Some(orders) = s.orders.get_mut(&ticker_clone) {
                                                        orders.no_order_id = None;
                                                        orders.no_price = None;
                                                        orders.last_no_cancel = Some(std::time::Instant::now());
                                                    }
                                                    continue;
                                                }
                                            }

                                            // Cancel cooldown: 3 seconds after canceling before placing new order
                                            const CANCEL_COOLDOWN: Duration = Duration::from_secs(3);
                                            let now = std::time::Instant::now();
                                            let yes_cancel_ok = orders.last_yes_cancel.map(|t| now.duration_since(t) >= CANCEL_COOLDOWN).unwrap_or(true);
                                            let no_cancel_ok = orders.last_no_cancel.map(|t| now.duration_since(t) >= CANCEL_COOLDOWN).unwrap_or(true);

                                            // Only trade YES/NO if edge exists, no existing order, not on cancel cooldown,
                                            // AND we don't have unhedged positions on that side already
                                            // In completion_only mode, skip all edge-based buying
                                            let mut need_yes = !completion_only && yes_has_edge && orders.yes_order_id.is_none() && yes_cancel_ok && unmatched_yes <= 0;
                                            let mut need_no = !completion_only && no_has_edge && orders.no_order_id.is_none() && no_cancel_ok && unmatched_no <= 0;

                                            // Log when we skip due to unhedged position
                                            if yes_has_edge && unmatched_yes > 0 {
                                                debug!("[SKIP] YES has edge but {} unhedged YES contracts need completion first", unmatched_yes);
                                            }
                                            if no_has_edge && unmatched_no > 0 {
                                                debug!("[SKIP] NO has edge but {} unhedged NO contracts need completion first", unmatched_no);
                                            }

                                            // Log when edge triggers a buy
                                            if need_yes {
                                                info!("[EDGE] YES: fair={}¢ ask={:?}¢ edge={}¢ >= req {}¢ on {}",
                                                      fair_yes, yes_ask, yes_edge_actual.unwrap_or(0), req_yes_edge, ticker);
                                            }
                                            if need_no {
                                                info!("[EDGE] NO: fair={}¢ ask={:?}¢ edge={}¢ >= req {}¢ on {}",
                                                      fair_no, no_ask, no_edge_actual.unwrap_or(0), req_no_edge, ticker);
                                            }

                                            // Debug: log why we're not trading when there's edge
                                            if yes_has_edge && !need_yes {
                                                debug!("[SKIP] YES has edge but skipping: order_id={:?} cancel_ok={}",
                                                       orders.yes_order_id, yes_cancel_ok);
                                            }
                                            if no_has_edge && !need_no {
                                                debug!("[SKIP] NO has edge but skipping: order_id={:?} cancel_ok={}",
                                                       orders.no_order_id, no_cancel_ok);
                                            }

                                            // CHEAP OPTION STRATEGY: buy cheap options with enough time left
                                            // Disabled in completion_only mode
                                            let minutes_left = secs / 60;
                                            let mut cheap_yes_trigger = false;
                                            let mut cheap_no_trigger = false;

                                            if !completion_only && minutes_left >= cheap_minutes && orders.yes_order_id.is_none() && yes_cancel_ok {
                                                // YES is cheap if ask <= cheap_yes threshold
                                                if let Some(ask) = yes_ask {
                                                    if ask <= cheap_yes as i64 {
                                                        need_yes = true;
                                                        cheap_yes_trigger = true;
                                                        info!("[CHEAP] YES: ask={}¢ <= {}¢ threshold, {}min left on {}",
                                                              ask, cheap_yes, minutes_left, ticker);
                                                    }
                                                }
                                            }
                                            if !completion_only && minutes_left >= cheap_minutes && orders.no_order_id.is_none() && no_cancel_ok {
                                                // NO is cheap if NO ask <= (100 - cheap_no) threshold
                                                // e.g., if cheap_no=80, buy NO when NO ask <= 20
                                                if let Some(no_price) = no_ask {
                                                    if no_price <= (100 - cheap_no) as i64 {
                                                        need_no = true;
                                                        cheap_no_trigger = true;
                                                        info!("[CHEAP] NO: ask={}¢ <= {}¢ threshold (YES>={}¢), {}min left on {}",
                                                              no_price, 100 - cheap_no, cheap_no, minutes_left, ticker);
                                                    }
                                                }
                                            }

                                            // NO LIQUIDITY STRATEGY: if no bids exist (ask=100) and fair is reasonable, bid 1¢
                                            // This gets us first in line when markets are empty
                                            // Disabled in completion_only mode
                                            let no_yes_liquidity = yes_ask.map(|a| a >= 100).unwrap_or(true);
                                            let no_no_liquidity = no_ask.map(|a| a >= 100).unwrap_or(true);
                                            let mut penny_yes = false;
                                            let mut penny_no = false;

                                            if !completion_only && no_yes_liquidity && fair_yes >= 20 && fair_yes <= 80 && orders.yes_order_id.is_none() && yes_cancel_ok {
                                                need_yes = true;
                                                penny_yes = true;
                                                info!("[PENNY] YES: no liquidity, fair={}¢, bidding 1¢ on {}", fair_yes, ticker);
                                            }
                                            if !completion_only && no_no_liquidity && fair_no >= 20 && fair_no <= 80 && orders.no_order_id.is_none() && no_cancel_ok {
                                                need_no = true;
                                                penny_no = true;
                                                info!("[PENNY] NO: no liquidity, fair={}¢, bidding 1¢ on {}", fair_no, ticker);
                                            }

                                            // Check if edge is big enough to be aggressive (IOC at ask)
                                            let yes_actual = yes_edge_actual.unwrap_or(0);
                                            let no_actual = no_edge_actual.unwrap_or(0);

                                            // IOC cooldown: 1 second between attempts per side per ticker
                                            // If on cooldown, fall back to passive bid (don't skip)
                                            const IOC_COOLDOWN: Duration = Duration::from_secs(1);
                                            let now = std::time::Instant::now();
                                            let yes_ioc_ok = orders.last_yes_ioc.map(|t| now.duration_since(t) >= IOC_COOLDOWN).unwrap_or(true);
                                            let no_ioc_ok = orders.last_no_ioc.map(|t| now.duration_since(t) >= IOC_COOLDOWN).unwrap_or(true);

                                            // Aggro = edge high enough AND cooldown passed AND not penny bid (no ask to hit)
                                            // If edge is high but cooldown active, we'll fall through to passive bid
                                            let yes_aggro = yes_actual >= aggro_edge && yes_ioc_ok && !penny_yes;
                                            let no_aggro = no_actual >= aggro_edge && no_ioc_ok && !penny_no;

                                            if !need_yes && !need_no {
                                                continue;
                                            }

                                            // Bid price = fair - required_edge (capped at max_bid only near ATM)
                                            // For penny bids (no liquidity), always bid 1¢
                                            let yes_our_bid = if penny_yes { 1 } else { calc_bid_price(fair_yes, req_yes_edge, max_bid) };
                                            let no_our_bid = if penny_no { 1 } else { calc_bid_price(fair_no, req_no_edge, max_bid) };

                                            drop(s);

                                            let spot_str = spot.map(|s| format!("{:.0}", s)).unwrap_or("-".into());

                                            if dry_run {
                                                if need_yes {
                                                    if yes_aggro {
                                                        info!("[DRY] Would IOC BUY {} YES @{}¢ (ask) | fair={}¢ edge={}¢ | {} Spot={}",
                                                              contracts, yes_ask.unwrap_or(0), fair_yes, yes_actual, ticker_clone, spot_str);
                                                    } else {
                                                        info!("[DRY] Would bid {} YES @{}¢ | fair={}¢ edge={}¢ | {} Spot={}",
                                                              contracts, yes_our_bid, fair_yes, yes_actual, ticker_clone, spot_str);
                                                    }
                                                }
                                                if need_no {
                                                    if no_aggro {
                                                        info!("[DRY] Would IOC BUY {} NO @{}¢ (ask) | fair={}¢ edge={}¢ | {} Spot={}",
                                                              contracts, no_ask.unwrap_or(0), fair_no, no_actual, ticker_clone, spot_str);
                                                    } else {
                                                        info!("[DRY] Would bid {} NO @{}¢ | fair={}¢ edge={}¢ | {} Spot={}",
                                                              contracts, no_our_bid, fair_no, no_actual, ticker_clone, spot_str);
                                                    }
                                                }
                                            } else {
                                                if need_yes {
                                                    if yes_aggro {
                                                        // Aggressive: IOC at ask price
                                                        let ask = yes_ask.unwrap_or(99) as i64;
                                                        warn!("[AGGRO] 🎯 IOC BUY {} YES @{}¢ | fair={}¢ edge={}¢ | {}",
                                                              contracts, ask, fair_yes, yes_actual, ticker_clone);
                                                        // Update cooldown timestamp
                                                        {
                                                            let mut s = state.write().await;
                                                            if let Some(orders) = s.orders.get_mut(&ticker_clone) {
                                                                orders.last_yes_ioc = Some(std::time::Instant::now());
                                                            }
                                                        }
                                                        match client.buy_ioc(&ticker_clone, "yes", ask, contracts).await {
                                                            Ok(resp) => {
                                                                let filled = resp.order.filled_count();
                                                                if filled > 0 {
                                                                    warn!("[AGGRO] ✅ Filled {} YES @{}¢", filled, ask);
                                                                    // Position updated by WebSocket fill handler to avoid double-counting
                                                                } else {
                                                                    info!("[AGGRO] No fill - ask moved (cooldown 1s)");
                                                                }
                                                            }
                                                            Err(e) => error!("[AGGRO] ❌ YES IOC failed: {}", e),
                                                        }
                                                    } else {
                                                        // Passive: resting limit bid
                                                        info!("[BID] Posting {} YES @{}¢ | fair={}¢ edge={}¢ | {}",
                                                              contracts, yes_our_bid, fair_yes, yes_actual, ticker_clone);
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
                                                }
                                                if need_no {
                                                    if no_aggro {
                                                        // Aggressive: IOC at ask price
                                                        let ask = no_ask.unwrap_or(99) as i64;
                                                        warn!("[AGGRO] 🎯 IOC BUY {} NO @{}¢ | fair={}¢ edge={}¢ | {}",
                                                              contracts, ask, fair_no, no_actual, ticker_clone);
                                                        // Update cooldown timestamp
                                                        {
                                                            let mut s = state.write().await;
                                                            if let Some(orders) = s.orders.get_mut(&ticker_clone) {
                                                                orders.last_no_ioc = Some(std::time::Instant::now());
                                                            }
                                                        }
                                                        match client.buy_ioc(&ticker_clone, "no", ask, contracts).await {
                                                            Ok(resp) => {
                                                                let filled = resp.order.filled_count();
                                                                if filled > 0 {
                                                                    warn!("[AGGRO] ✅ Filled {} NO @{}¢", filled, ask);
                                                                    // Position updated by WebSocket fill handler to avoid double-counting
                                                                } else {
                                                                    info!("[AGGRO] No fill - ask moved (cooldown 1s)");
                                                                }
                                                            }
                                                            Err(e) => error!("[AGGRO] ❌ NO IOC failed: {}", e),
                                                        }
                                                    } else {
                                                        // Passive: resting limit bid
                                                        info!("[BID] Posting {} NO @{}¢ | fair={}¢ edge={}¢ | {}",
                                                              contracts, no_our_bid, fair_no, no_actual, ticker_clone);
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
