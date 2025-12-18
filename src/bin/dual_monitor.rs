//! Dual Exchange Monitor - Compare Kalshi vs Polymarket Crypto Markets
//!
//! Shows real-time prices from both exchanges:
//! - Kalshi: BTC/ETH 15-minute binary options (short-term)
//! - Polymarket: BTC/ETH price target markets (long-term)
//! - Spot price from Polygon.io
//! - Fair value calculation for Kalshi markets
//!
//! Usage:
//!   cargo run --release --bin dual_monitor
//!
//! Environment:
//!   KALSHI_API_KEY_ID - Kalshi API key
//!   KALSHI_PRIVATE_KEY_PATH - Path to Kalshi private key
//!   POLYGON_API_KEY - Polygon.io API key (optional)

use anyhow::{Context, Result};
use futures_util::{SinkExt, StreamExt};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tokio::sync::RwLock;
use tokio_tungstenite::{connect_async, tungstenite::{http::Request, Message}};
use tracing::{error, info, warn};

use arb_bot::kalshi::{KalshiApiClient, KalshiConfig};
use arb_bot::fair_value::calc_fair_value_cents;

const KALSHI_WS_URL: &str = "wss://api.elections.kalshi.com/trade-api/ws/v2";
const POLYMARKET_WS_URL: &str = "wss://ws-subscriptions-clob.polymarket.com/ws/market";
const GAMMA_API_BASE: &str = "https://gamma-api.polymarket.com";
const POLYGON_WS_URL: &str = "wss://socket.polygon.io/crypto";

const BTC_15M_SERIES: &str = "KXBTC15M";
const ETH_15M_SERIES: &str = "KXETH15M";

// ============================================================================
// Data Structures
// ============================================================================

#[derive(Debug, Clone, Default)]
struct Orderbook {
    yes_bid: Option<i64>,
    yes_ask: Option<i64>,
    no_bid: Option<i64>,
    no_ask: Option<i64>,
}

#[derive(Debug, Clone)]
struct KalshiMarket {
    ticker: String,
    #[allow(dead_code)]
    title: String,
    strike: Option<f64>,
    asset: String,
    book: Orderbook,
}

#[derive(Debug, Clone)]
struct PolyMarket {
    #[allow(dead_code)]
    condition_id: String,
    question: String,
    yes_token: String,
    no_token: String,
    strike: Option<f64>,
    asset: String,
    book: Orderbook,
}

#[derive(Debug, Default)]
struct PriceState {
    btc_price: Option<f64>,
    eth_price: Option<f64>,
}

struct State {
    kalshi_markets: HashMap<String, KalshiMarket>,
    poly_markets: HashMap<String, PolyMarket>,
    prices: PriceState,
    vol: f64,
}

impl State {
    fn new(vol: f64) -> Self {
        Self {
            kalshi_markets: HashMap::new(),
            poly_markets: HashMap::new(),
            prices: PriceState::default(),
            vol,
        }
    }
}

// ============================================================================
// Kalshi Discovery
// ============================================================================

async fn discover_kalshi_markets(client: &KalshiApiClient) -> Result<Vec<KalshiMarket>> {
    let mut markets = Vec::new();

    for series in [BTC_15M_SERIES, ETH_15M_SERIES] {
        let asset = if series == BTC_15M_SERIES { "BTC" } else { "ETH" };

        let events = client.get_events(series, 50).await?;
        for event in events {
            let event_markets = client.get_markets(&event.event_ticker).await?;
            for m in event_markets {
                let strike = m.floor_strike.or_else(|| parse_strike(&m.title));

                markets.push(KalshiMarket {
                    ticker: m.ticker,
                    title: m.title,
                    strike,
                    asset: asset.to_string(),
                    book: Orderbook {
                        yes_bid: m.yes_bid,
                        yes_ask: m.yes_ask,
                        no_bid: m.no_bid,
                        no_ask: m.no_ask,
                    },
                });
            }
        }
    }

    Ok(markets)
}

fn parse_strike(title: &str) -> Option<f64> {
    for word in title.split_whitespace() {
        let clean = word.trim_matches(|c: char| !c.is_ascii_digit() && c != '.');
        if let Ok(val) = clean.parse::<f64>() {
            if val > 1000.0 {
                return Some(val);
            }
        }
    }
    None
}

// ============================================================================
// Polymarket Discovery - Long-term crypto price markets
// ============================================================================

#[derive(Debug, Deserialize)]
#[allow(dead_code)]
struct GammaEvent {
    id: Option<String>,
    title: Option<String>,
    slug: Option<String>,
    markets: Option<Vec<GammaMarket>>,
    active: Option<bool>,
    closed: Option<bool>,
}

#[derive(Debug, Deserialize)]
#[allow(dead_code)]
struct GammaMarket {
    #[serde(rename = "conditionId")]
    condition_id: Option<String>,
    question: Option<String>,
    #[serde(rename = "clobTokenIds")]
    clob_token_ids: Option<String>,
    #[serde(rename = "outcomePrices")]
    outcome_prices: Option<String>,
    #[serde(rename = "bestBid")]
    best_bid: Option<f64>,
    #[serde(rename = "bestAsk")]
    best_ask: Option<f64>,
    active: Option<bool>,
    closed: Option<bool>,
}

async fn discover_poly_markets() -> Result<Vec<PolyMarket>> {
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(30))
        .build()?;

    let mut markets = Vec::new();

    // Fetch Bitcoin and Ethereum price events
    for (slug, asset) in [
        ("what-price-will-bitcoin-hit-in-2025", "BTC"),
        ("what-price-will-ethereum-hit-in-2025", "ETH"),
    ] {
        let url = format!("{}/events?slug={}", GAMMA_API_BASE, slug);

        let resp = match client.get(&url).send().await {
            Ok(r) => r,
            Err(e) => {
                warn!("[POLY] Failed to fetch {}: {}", slug, e);
                continue;
            }
        };

        if !resp.status().is_success() {
            continue;
        }

        let events: Vec<GammaEvent> = match resp.json().await {
            Ok(e) => e,
            Err(_) => continue,
        };

        for event in events {
            let Some(event_markets) = event.markets else { continue };

            for gm in event_markets {
                if gm.closed == Some(true) || gm.active == Some(false) {
                    continue;
                }

                let Some(condition_id) = gm.condition_id else { continue };
                let Some(question) = gm.question else { continue };
                let Some(clob_tokens_str) = gm.clob_token_ids else { continue };

                let token_ids: Vec<String> = serde_json::from_str(&clob_tokens_str).unwrap_or_default();
                if token_ids.len() < 2 {
                    continue;
                }

                let strike = parse_poly_strike(&question);

                // Get initial prices from API
                let yes_ask = gm.best_ask.map(|p| (p * 100.0).round() as i64);
                let yes_bid = gm.best_bid.map(|p| (p * 100.0).round() as i64);

                markets.push(PolyMarket {
                    condition_id,
                    question,
                    yes_token: token_ids[0].clone(),
                    no_token: token_ids[1].clone(),
                    strike,
                    asset: asset.to_string(),
                    book: Orderbook {
                        yes_ask,
                        yes_bid,
                        no_ask: yes_ask.map(|a| 100 - a + 1), // Approximate
                        no_bid: yes_bid.map(|b| 100 - b - 1),
                    },
                });
            }
        }
    }

    // Sort by strike
    markets.sort_by(|a, b| {
        let a_strike = a.strike.unwrap_or(0.0);
        let b_strike = b.strike.unwrap_or(0.0);
        a_strike.partial_cmp(&b_strike).unwrap()
    });

    Ok(markets)
}

fn parse_poly_strike(question: &str) -> Option<f64> {
    // Look for patterns like "$100,000" or "$150,000"
    for word in question.split(|c: char| c.is_whitespace() || c == '?') {
        let clean: String = word.chars()
            .filter(|c| c.is_ascii_digit())
            .collect();
        if clean.len() >= 5 {
            if let Ok(val) = clean.parse::<f64>() {
                if val >= 50000.0 {
                    return Some(val);
                }
            }
        }
    }
    None
}

// ============================================================================
// WebSocket Message Types
// ============================================================================

#[derive(Deserialize)]
struct KalshiWsMsg {
    #[serde(rename = "type")]
    msg_type: String,
    msg: Option<KalshiWsMsgBody>,
}

#[derive(Deserialize)]
struct KalshiWsMsgBody {
    market_ticker: Option<String>,
    yes: Option<Vec<Vec<i64>>>,
    no: Option<Vec<Vec<i64>>>,
}

#[derive(Serialize)]
struct KalshiSubCmd {
    id: i32,
    cmd: &'static str,
    params: KalshiSubParams,
}

#[derive(Serialize)]
struct KalshiSubParams {
    channels: Vec<&'static str>,
    market_tickers: Vec<String>,
}

#[derive(Deserialize, Debug)]
struct PolyBookSnapshot {
    asset_id: String,
    bids: Vec<PolyPriceLevel>,
    asks: Vec<PolyPriceLevel>,
}

#[derive(Deserialize, Debug)]
struct PolyPriceLevel {
    price: String,
    #[allow(dead_code)]
    size: String,
}

#[derive(Serialize)]
struct PolySubCmd {
    assets_ids: Vec<String>,
    #[serde(rename = "type")]
    sub_type: &'static str,
}

#[derive(Deserialize)]
struct PolygonMsg {
    ev: Option<String>,
    pair: Option<String>,
    p: Option<f64>,
}

// ============================================================================
// Time parsing
// ============================================================================

fn parse_secs_remaining(ticker: &str) -> Option<i64> {
    use chrono::{NaiveTime, Timelike, Utc};

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

// ============================================================================
// Display
// ============================================================================

fn display_markets(state: &State) {
    let btc_str = state.prices.btc_price.map(|p| format!("${:.0}", p)).unwrap_or("-".into());
    let eth_str = state.prices.eth_price.map(|p| format!("${:.0}", p)).unwrap_or("-".into());

    println!("\n{}", "═".repeat(90));
    println!("  DUAL EXCHANGE MONITOR");
    println!("  SPOT: BTC {} | ETH {}", btc_str, eth_str);
    println!("{}", "═".repeat(90));

    // ========== KALSHI 15-MINUTE MARKETS ==========
    println!("\n  📊 KALSHI 15-MINUTE MARKETS (short-term binary options)");
    println!("{}", "─".repeat(90));
    println!("  {:5} │ {:>7} │ {:>10} │ {:>8} │ {:>8} │ {:>12}",
             "Asset", "Expiry", "Strike", "YES ask", "NO ask", "Fair Value");
    println!("{}", "─".repeat(90));

    let mut kalshi_list: Vec<_> = state.kalshi_markets.values().collect();
    kalshi_list.sort_by_key(|m| parse_secs_remaining(&m.ticker).unwrap_or(9999));

    for km in kalshi_list {
        let secs = parse_secs_remaining(&km.ticker).unwrap_or(0);
        if secs <= 0 || secs > 900 {
            continue;
        }

        let time_str = format!("{}m{}s", secs / 60, secs % 60);
        let strike_str = km.strike.map(|s| format!("${:.0}", s)).unwrap_or("-".into());

        let yes_ask = km.book.yes_ask.map(|p| format!("{}¢", p)).unwrap_or("-".into());
        let no_ask = km.book.no_ask.map(|p| format!("{}¢", p)).unwrap_or("-".into());

        // Fair value
        let spot = if km.asset == "ETH" { state.prices.eth_price } else { state.prices.btc_price };
        let fair_str = match (spot, km.strike) {
            (Some(s), Some(k)) => {
                let (fy, fn_) = calc_fair_value_cents(s, k, secs as f64 / 60.0, state.vol);
                format!("Y={}¢ N={}¢", fy, fn_)
            }
            _ => "-".into(),
        };

        println!("  {:5} │ {:>7} │ {:>10} │ {:>8} │ {:>8} │ {:>12}",
                 km.asset, time_str, strike_str, yes_ask, no_ask, fair_str);
    }

    // ========== POLYMARKET LONG-TERM MARKETS ==========
    println!("\n  🔮 POLYMARKET CRYPTO MARKETS (2025 price targets)");
    println!("{}", "─".repeat(90));
    println!("  {:5} │ {:>12} │ {:>8} │ {:>8} │ {:>40}",
             "Asset", "Target", "YES", "NO", "Question");
    println!("{}", "─".repeat(90));

    // Group by asset
    let btc_markets: Vec<_> = state.poly_markets.values()
        .filter(|m| m.asset == "BTC")
        .collect();
    let eth_markets: Vec<_> = state.poly_markets.values()
        .filter(|m| m.asset == "ETH")
        .collect();

    for pm in btc_markets.iter().chain(eth_markets.iter()) {
        let strike_str = pm.strike.map(|s| format!("${:.0}", s)).unwrap_or("-".into());
        let yes_str = pm.book.yes_ask.map(|p| format!("{}¢", p)).unwrap_or("-".into());
        let no_str = pm.book.no_ask.map(|p| format!("{}¢", p)).unwrap_or("-".into());

        // Truncate question
        let q = if pm.question.len() > 40 {
            format!("{}...", &pm.question[..37])
        } else {
            pm.question.clone()
        };

        println!("  {:5} │ {:>12} │ {:>8} │ {:>8} │ {:>40}",
                 pm.asset, strike_str, yes_str, no_str, q);
    }

    println!("{}", "═".repeat(90));
    println!("  Note: Kalshi=15min expiry, Polymarket=Dec 31 2025 expiry (different products!)");
}

// ============================================================================
// WebSocket Runners
// ============================================================================

async fn run_kalshi_ws(state: Arc<RwLock<State>>, config: &KalshiConfig) {
    loop {
        let tickers: Vec<String> = {
            let s = state.read().await;
            s.kalshi_markets.keys().cloned().collect()
        };

        if tickers.is_empty() {
            tokio::time::sleep(Duration::from_secs(5)).await;
            continue;
        }

        let timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis()
            .to_string();

        let signature = match config.sign(&format!("{}GET/trade-api/ws/v2", timestamp)) {
            Ok(s) => s,
            Err(e) => {
                error!("[KALSHI] Sign failed: {}", e);
                tokio::time::sleep(Duration::from_secs(5)).await;
                continue;
            }
        };

        let request = match Request::builder()
            .uri(KALSHI_WS_URL)
            .header("KALSHI-ACCESS-KEY", &config.api_key_id)
            .header("KALSHI-ACCESS-SIGNATURE", &signature)
            .header("KALSHI-ACCESS-TIMESTAMP", &timestamp)
            .header("Host", "api.elections.kalshi.com")
            .header("Connection", "Upgrade")
            .header("Upgrade", "websocket")
            .header("Sec-WebSocket-Version", "13")
            .header("Sec-WebSocket-Key", tokio_tungstenite::tungstenite::handshake::client::generate_key())
            .body(())
        {
            Ok(r) => r,
            Err(e) => {
                error!("[KALSHI] Request build failed: {}", e);
                tokio::time::sleep(Duration::from_secs(5)).await;
                continue;
            }
        };

        let (ws, _) = match connect_async(request).await {
            Ok(ws) => ws,
            Err(e) => {
                error!("[KALSHI] Connect failed: {}", e);
                tokio::time::sleep(Duration::from_secs(5)).await;
                continue;
            }
        };

        info!("[KALSHI] Connected");
        let (mut write, mut read) = ws.split();

        let sub = KalshiSubCmd {
            id: 1,
            cmd: "subscribe",
            params: KalshiSubParams {
                channels: vec!["orderbook_delta"],
                market_tickers: tickers,
            },
        };
        let _ = write.send(Message::Text(serde_json::to_string(&sub).unwrap())).await;

        while let Some(msg) = read.next().await {
            match msg {
                Ok(Message::Text(text)) => {
                    if let Ok(ws_msg) = serde_json::from_str::<KalshiWsMsg>(&text) {
                        if ws_msg.msg_type == "orderbook_snapshot" || ws_msg.msg_type == "orderbook_delta" {
                            if let Some(body) = ws_msg.msg {
                                if let Some(ticker) = body.market_ticker {
                                    let mut s = state.write().await;
                                    if let Some(market) = s.kalshi_markets.get_mut(&ticker) {
                                        if let Some(yes_levels) = &body.yes {
                                            if let Some(best) = yes_levels.iter()
                                                .filter(|l| l.len() >= 2 && l[1] > 0)
                                                .max_by_key(|l| l[0])
                                            {
                                                market.book.yes_bid = Some(best[0]);
                                                market.book.no_ask = Some(100 - best[0]);
                                            }
                                        }
                                        if let Some(no_levels) = &body.no {
                                            if let Some(best) = no_levels.iter()
                                                .filter(|l| l.len() >= 2 && l[1] > 0)
                                                .max_by_key(|l| l[0])
                                            {
                                                market.book.no_bid = Some(best[0]);
                                                market.book.yes_ask = Some(100 - best[0]);
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
                Err(e) => {
                    error!("[KALSHI] WS error: {}", e);
                    break;
                }
                _ => {}
            }
        }

        warn!("[KALSHI] Disconnected, reconnecting...");
        tokio::time::sleep(Duration::from_secs(3)).await;
    }
}

async fn run_poly_ws(state: Arc<RwLock<State>>) {
    loop {
        let tokens: Vec<String> = {
            let s = state.read().await;
            s.poly_markets.values()
                .flat_map(|m| vec![m.yes_token.clone(), m.no_token.clone()])
                .collect()
        };

        if tokens.is_empty() {
            tokio::time::sleep(Duration::from_secs(10)).await;
            continue;
        }

        let (ws, _) = match connect_async(POLYMARKET_WS_URL).await {
            Ok(ws) => ws,
            Err(e) => {
                error!("[POLY] Connect failed: {}", e);
                tokio::time::sleep(Duration::from_secs(5)).await;
                continue;
            }
        };

        info!("[POLY] Connected");
        let (mut write, mut read) = ws.split();

        let sub = PolySubCmd {
            assets_ids: tokens,
            sub_type: "market",
        };
        let _ = write.send(Message::Text(serde_json::to_string(&sub).unwrap())).await;

        let mut ping_interval = tokio::time::interval(Duration::from_secs(30));

        loop {
            tokio::select! {
                _ = ping_interval.tick() => {
                    let _ = write.send(Message::Ping(vec![])).await;
                }
                msg = read.next() => {
                    let Some(msg) = msg else { break };
                    match msg {
                        Ok(Message::Text(text)) => {
                            if let Ok(books) = serde_json::from_str::<Vec<PolyBookSnapshot>>(&text) {
                                let mut s = state.write().await;
                                for book in books {
                                    let market = s.poly_markets.values_mut().find(|m| {
                                        m.yes_token == book.asset_id || m.no_token == book.asset_id
                                    });

                                    if let Some(market) = market {
                                        let best_ask = book.asks.iter()
                                            .filter_map(|l| l.price.parse::<f64>().ok())
                                            .map(|p| (p * 100.0).round() as i64)
                                            .filter(|&p| p > 0)
                                            .min();

                                        let best_bid = book.bids.iter()
                                            .filter_map(|l| l.price.parse::<f64>().ok())
                                            .map(|p| (p * 100.0).round() as i64)
                                            .filter(|&p| p > 0)
                                            .max();

                                        if book.asset_id == market.yes_token {
                                            market.book.yes_ask = best_ask;
                                            market.book.yes_bid = best_bid;
                                            // Update NO prices (inverse)
                                            market.book.no_ask = best_bid.map(|b| 100 - b);
                                            market.book.no_bid = best_ask.map(|a| 100 - a);
                                        } else {
                                            market.book.no_ask = best_ask;
                                            market.book.no_bid = best_bid;
                                        }
                                    }
                                }
                            }
                        }
                        Ok(Message::Ping(data)) => {
                            let _ = write.send(Message::Pong(data)).await;
                        }
                        Err(e) => {
                            error!("[POLY] WS error: {}", e);
                            break;
                        }
                        _ => {}
                    }
                }
            }
        }

        warn!("[POLY] Disconnected, reconnecting...");
        tokio::time::sleep(Duration::from_secs(3)).await;
    }
}

async fn run_polygon_feed(state: Arc<RwLock<State>>, api_key: &str) {
    loop {
        let url = format!("{}?apiKey={}", POLYGON_WS_URL, api_key);
        let (ws, _) = match connect_async(&url).await {
            Ok(ws) => ws,
            Err(e) => {
                error!("[POLYGON] Connect failed: {}", e);
                tokio::time::sleep(Duration::from_secs(5)).await;
                continue;
            }
        };

        info!("[POLYGON] Connected");
        let (mut write, mut read) = ws.split();

        let sub = serde_json::json!({
            "action": "subscribe",
            "params": "XT.BTC-USD,XT.ETH-USD"
        });
        let _ = write.send(Message::Text(sub.to_string())).await;

        while let Some(msg) = read.next().await {
            match msg {
                Ok(Message::Text(text)) => {
                    if let Ok(messages) = serde_json::from_str::<Vec<PolygonMsg>>(&text) {
                        let mut s = state.write().await;
                        for m in messages {
                            if m.ev.as_deref() != Some("XT") {
                                continue;
                            }
                            match (m.pair.as_deref(), m.p) {
                                (Some("BTC-USD"), Some(p)) => s.prices.btc_price = Some(p),
                                (Some("ETH-USD"), Some(p)) => s.prices.eth_price = Some(p),
                                _ => {}
                            }
                        }
                    }
                }
                Ok(Message::Ping(data)) => {
                    let _ = write.send(Message::Pong(data)).await;
                }
                Err(_) => break,
                _ => {}
            }
        }

        warn!("[POLYGON] Disconnected, reconnecting...");
        tokio::time::sleep(Duration::from_secs(3)).await;
    }
}

// ============================================================================
// Main
// ============================================================================

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::from_default_env()
                .add_directive("dual_monitor=info".parse().unwrap())
                .add_directive("arb_bot=warn".parse().unwrap()),
        )
        .init();

    println!("═══════════════════════════════════════════════════════════════════════");
    println!("  DUAL EXCHANGE MONITOR");
    println!("  Kalshi (15-min) vs Polymarket (2025 targets)");
    println!("═══════════════════════════════════════════════════════════════════════");

    let config = KalshiConfig::from_env().context("Failed to load Kalshi config")?;
    let kalshi_client = KalshiApiClient::new(KalshiConfig::from_env()?);

    let polygon_api_key = std::env::var("POLYGON_API_KEY")
        .unwrap_or_else(|_| "o2Jm26X52_0tRq2W7V5JbsCUXdMjL7qk".to_string());

    // Discover markets
    println!("[DISCOVER] Finding Kalshi 15-min markets...");
    let kalshi_markets = discover_kalshi_markets(&kalshi_client).await?;
    println!("[DISCOVER] Found {} Kalshi markets", kalshi_markets.len());

    println!("[DISCOVER] Finding Polymarket 2025 price markets...");
    let poly_markets = discover_poly_markets().await?;
    println!("[DISCOVER] Found {} Polymarket markets", poly_markets.len());

    for pm in &poly_markets {
        println!("  • {} ${:?}: {}¢", pm.asset, pm.strike, pm.book.yes_ask.unwrap_or(0));
    }

    // Initialize state
    let state = Arc::new(RwLock::new({
        let mut s = State::new(0.58);
        for m in kalshi_markets {
            s.kalshi_markets.insert(m.ticker.clone(), m);
        }
        for m in poly_markets {
            s.poly_markets.insert(m.yes_token.clone(), m);
        }
        s
    }));

    // Start WebSocket feeds
    let state_kalshi = state.clone();
    let config_clone = KalshiConfig::from_env()?;
    tokio::spawn(async move {
        run_kalshi_ws(state_kalshi, &config_clone).await;
    });

    let state_poly = state.clone();
    tokio::spawn(async move {
        run_poly_ws(state_poly).await;
    });

    let state_polygon = state.clone();
    let polygon_key = polygon_api_key.clone();
    tokio::spawn(async move {
        run_polygon_feed(state_polygon, &polygon_key).await;
    });

    // Periodic Kalshi rediscovery
    let state_disc = state.clone();
    let disc_client = KalshiApiClient::new(KalshiConfig::from_env()?);
    tokio::spawn(async move {
        let mut interval = tokio::time::interval(Duration::from_secs(60));
        loop {
            interval.tick().await;
            if let Ok(new_markets) = discover_kalshi_markets(&disc_client).await {
                let mut s = state_disc.write().await;
                for m in new_markets {
                    if !s.kalshi_markets.contains_key(&m.ticker) {
                        info!("[DISCOVER] New Kalshi market: {}", m.ticker);
                        s.kalshi_markets.insert(m.ticker.clone(), m);
                    }
                }
            }
        }
    });

    // Display loop
    let mut interval = tokio::time::interval(Duration::from_secs(2));
    loop {
        interval.tick().await;
        print!("\x1B[2J\x1B[1;1H"); // Clear screen

        let s = state.read().await;
        display_markets(&s);
    }
}
