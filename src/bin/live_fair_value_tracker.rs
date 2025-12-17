//! Live Fair Value Tracker
//!
//! Monitors live Kalshi crypto markets and compares:
//! - Actual orderbook prices (yes_bid, yes_ask, no_bid, no_ask)
//! - Calculated fair value based on time remaining and ATM assumption
//!
//! Records the difference over time to analyze market efficiency.
//!
//! Usage:
//!   cargo run --release --bin live_fair_value_tracker
//!
//! Output:
//!   Real-time display of market prices vs fair value
//!   CSV log file for later analysis

use anyhow::{Context, Result};
use chrono::{DateTime, Utc};
use futures_util::{SinkExt, StreamExt};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fs::File;
use std::io::Write;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tokio::sync::RwLock;
use tokio_tungstenite::{connect_async, tungstenite::{http::Request, Message}};
use tracing::{info, warn, error};

use arb_bot::kalshi::{KalshiConfig, KalshiApiClient};

const KALSHI_WS_URL: &str = "wss://api.elections.kalshi.com/trade-api/ws/v2";
const BTC_15M_SERIES: &str = "KXBTC15M";
const ETH_15M_SERIES: &str = "KXETH15M";

// ============================================================================
// FAIR VALUE CALCULATION
// ============================================================================

fn norm_cdf(x: f64) -> f64 {
    let a1 = 0.254829592;
    let a2 = -0.284496736;
    let a3 = 1.421413741;
    let a4 = -1.453152027;
    let a5 = 1.061405429;
    let p = 0.3275911;

    let sign = if x < 0.0 { -1.0 } else { 1.0 };
    let x = x.abs();

    let t = 1.0 / (1.0 + p * x);
    let y = 1.0 - (((((a5 * t + a4) * t) + a3) * t + a2) * t + a1) * t * (-x * x / 2.0).exp();

    0.5 * (1.0 + sign * y)
}

/// Calculate fair value for ATM binary option
/// For "BTC price up in next 15 mins?" markets, fair value at launch ≈ 50%
/// As time passes, it converges to actual outcome
fn calc_fair_value_atm(minutes_remaining: f64, annual_vol: f64) -> (i64, i64) {
    if minutes_remaining <= 0.0 {
        return (50, 50); // At expiry, 50/50 until we know result
    }

    // For ATM (spot = strike), d2 ≈ -σ²T/2 / (σ√T) = -σ√T/2
    // This gives a slight edge to NO due to drift
    let time_years = minutes_remaining / 525960.0;
    let sqrt_t = time_years.sqrt();
    let d2 = -0.5 * annual_vol * sqrt_t;

    let yes_prob = norm_cdf(d2);
    let yes_cents = (yes_prob * 100.0).round() as i64;
    let no_cents = 100 - yes_cents;

    (yes_cents, no_cents)
}

// ============================================================================
// DATA STRUCTURES
// ============================================================================

#[derive(Debug, Clone)]
struct MarketState {
    ticker: String,
    title: String,
    /// Best YES ask (what you pay to buy YES)
    yes_ask: Option<i64>,
    /// Best YES bid (what you get to sell YES)
    yes_bid: Option<i64>,
    /// Best NO ask
    no_ask: Option<i64>,
    /// Best NO bid
    no_bid: Option<i64>,
    /// Market close time
    close_time: Option<DateTime<Utc>>,
    /// Minutes remaining until close
    minutes_remaining: f64,
}

impl MarketState {
    fn new(ticker: String, title: String, close_time: Option<DateTime<Utc>>) -> Self {
        Self {
            ticker,
            title,
            yes_ask: None,
            yes_bid: None,
            no_ask: None,
            no_bid: None,
            close_time,
            minutes_remaining: 15.0,
        }
    }

    fn update_time_remaining(&mut self) {
        if let Some(close) = self.close_time {
            let now = Utc::now();
            let diff = close.signed_duration_since(now);
            self.minutes_remaining = (diff.num_seconds() as f64 / 60.0).max(0.0);
        }
    }

    /// Mid price for YES (average of bid and ask)
    fn yes_mid(&self) -> Option<i64> {
        match (self.yes_bid, self.yes_ask) {
            (Some(b), Some(a)) => Some((b + a) / 2),
            (Some(b), None) => Some(b),
            (None, Some(a)) => Some(a),
            _ => None,
        }
    }

    fn spread(&self) -> Option<i64> {
        match (self.yes_bid, self.yes_ask) {
            (Some(b), Some(a)) => Some(a - b),
            _ => None,
        }
    }
}

#[derive(Debug, Clone, Serialize)]
struct PriceSnapshot {
    timestamp: String,
    ticker: String,
    minutes_remaining: f64,
    yes_bid: Option<i64>,
    yes_ask: Option<i64>,
    yes_mid: Option<i64>,
    no_bid: Option<i64>,
    no_ask: Option<i64>,
    fair_yes: i64,
    fair_no: i64,
    /// Difference: yes_mid - fair_yes (positive = market overprices YES)
    mispricing: Option<i64>,
    spread: Option<i64>,
}

// ============================================================================
// WEBSOCKET MESSAGES
// ============================================================================

#[derive(Deserialize, Debug)]
struct KalshiWsMessage {
    #[serde(rename = "type")]
    msg_type: String,
    msg: Option<KalshiWsMsgBody>,
}

#[derive(Deserialize, Debug)]
struct KalshiWsMsgBody {
    market_ticker: Option<String>,
    yes: Option<Vec<Vec<i64>>>,
    no: Option<Vec<Vec<i64>>>,
}

#[derive(Serialize)]
struct SubscribeCmd {
    id: i32,
    cmd: &'static str,
    params: SubscribeParams,
}

#[derive(Serialize)]
struct SubscribeParams {
    channels: Vec<&'static str>,
    market_tickers: Vec<String>,
}

// ============================================================================
// MAIN LOGIC
// ============================================================================

/// Process orderbook snapshot/delta
fn process_orderbook(market: &mut MarketState, body: &KalshiWsMsgBody) {
    // YES levels - best bid determines NO ask, we also get YES bid
    if let Some(levels) = &body.yes {
        // Find best YES bid (highest)
        if let Some((price, _qty)) = levels.iter()
            .filter_map(|l| if l.len() >= 2 && l[1] > 0 { Some((l[0], l[1])) } else { None })
            .max_by_key(|(p, _)| *p)
        {
            market.yes_bid = Some(price);
            market.no_ask = Some(100 - price);
        } else {
            market.yes_bid = None;
        }
    }

    // NO levels - best bid determines YES ask, we also get NO bid
    if let Some(levels) = &body.no {
        if let Some((price, _qty)) = levels.iter()
            .filter_map(|l| if l.len() >= 2 && l[1] > 0 { Some((l[0], l[1])) } else { None })
            .max_by_key(|(p, _)| *p)
        {
            market.no_bid = Some(price);
            market.yes_ask = Some(100 - price);
        } else {
            market.no_bid = None;
        }
    }
}

/// Parse close time from ticker
/// Format: KXBTC15M-25DEC171030-30 -> Dec 17, 2025 10:30 EST = 15:30 UTC
/// The time in ticker is EST (UTC-5)
fn parse_close_time_from_ticker(ticker: &str) -> Option<DateTime<Utc>> {
    // Split by '-' to get parts like ["KXBTC15M", "25DEC171030", "30"]
    let parts: Vec<&str> = ticker.split('-').collect();
    if parts.len() < 2 {
        return None;
    }

    let date_time_part = parts[1]; // e.g., "25DEC171030"
    if date_time_part.len() < 11 {
        return None;
    }

    // Parse: 25DEC171030 = 2025, Dec, 17, 10:30 EST
    let year_suffix = &date_time_part[0..2]; // "25"
    let month = &date_time_part[2..5]; // "DEC"
    let day = &date_time_part[5..7]; // "17"
    let hour = &date_time_part[7..9]; // "10"
    let minute = &date_time_part[9..11]; // "30"

    let year: i32 = format!("20{}", year_suffix).parse().ok()?;
    let month_num: u32 = match month.to_uppercase().as_str() {
        "JAN" => 1, "FEB" => 2, "MAR" => 3, "APR" => 4,
        "MAY" => 5, "JUN" => 6, "JUL" => 7, "AUG" => 8,
        "SEP" => 9, "OCT" => 10, "NOV" => 11, "DEC" => 12,
        _ => return None,
    };
    let day: u32 = day.parse().ok()?;
    let hour: u32 = hour.parse().ok()?;
    let minute: u32 = minute.parse().ok()?;

    // Time is in EST (UTC-5), so add 5 hours to get UTC
    chrono::NaiveDate::from_ymd_opt(year, month_num, day)
        .and_then(|d| d.and_hms_opt(hour + 5, minute, 0))
        .map(|dt| DateTime::<Utc>::from_naive_utc_and_offset(dt, Utc))
}

async fn discover_markets(client: &KalshiApiClient) -> Result<Vec<MarketState>> {
    let mut markets = Vec::new();

    for series in [BTC_15M_SERIES, ETH_15M_SERIES] {
        let events = client.get_events(series, 10).await?;

        for event in events {
            let event_markets = client.get_markets(&event.event_ticker).await?;

            for m in event_markets {
                // Parse close_time from ticker
                let close_time = parse_close_time_from_ticker(&m.ticker);

                let mut market = MarketState::new(m.ticker.clone(), m.title.clone(), close_time);
                market.yes_ask = m.yes_ask;
                market.yes_bid = m.yes_bid;
                market.no_ask = m.no_ask;
                market.no_bid = m.no_bid;
                market.update_time_remaining();

                // Only track markets with time remaining
                if market.minutes_remaining > 0.0 {
                    markets.push(market);
                }
            }
        }
    }

    Ok(markets)
}

fn print_header() {
    println!();
    println!("╔══════════════════════════════════════════════════════════════════════════════════════════════════════════════╗");
    println!("║                              LIVE FAIR VALUE TRACKER                                                         ║");
    println!("╠══════════════════════════════════════════════════════════════════════════════════════════════════════════════╣");
    println!("║  Comparing actual market prices vs theoretical fair value (ATM assumption, 50% annual vol)                   ║");
    println!("╚══════════════════════════════════════════════════════════════════════════════════════════════════════════════╝");
    println!();
}

fn print_market_table(markets: &HashMap<String, MarketState>, vol: f64) {
    println!("┌────────────────────────────────┬────────┬────────┬────────┬────────┬────────┬────────┬────────┬────────┬────────┐");
    println!("│ Ticker                         │ MinRem │ YesBid │ YesAsk │ YesMid │ FairY  │ FairN  │ Misprc │ Spread │ Edge   │");
    println!("├────────────────────────────────┼────────┼────────┼────────┼────────┼────────┼────────┼────────┼────────┼────────┤");

    let mut sorted: Vec<_> = markets.values().collect();
    sorted.sort_by(|a, b| a.minutes_remaining.partial_cmp(&b.minutes_remaining).unwrap());

    for market in sorted {
        let (fair_yes, fair_no) = calc_fair_value_atm(market.minutes_remaining, vol);
        let yes_mid = market.yes_mid();
        let mispricing = yes_mid.map(|m| m - fair_yes);
        let spread = market.spread();

        // Edge = fair - ask (positive = underpriced, good to buy)
        let edge = market.yes_ask.map(|ask| fair_yes - ask);

        let ticker_short = if market.ticker.len() > 30 {
            &market.ticker[..30]
        } else {
            &market.ticker
        };

        println!(
            "│ {:30} │ {:>6.1} │ {:>6} │ {:>6} │ {:>6} │ {:>6} │ {:>6} │ {:>+6} │ {:>6} │ {:>+6} │",
            ticker_short,
            market.minutes_remaining,
            market.yes_bid.map(|p| p.to_string()).unwrap_or("-".to_string()),
            market.yes_ask.map(|p| p.to_string()).unwrap_or("-".to_string()),
            yes_mid.map(|p| p.to_string()).unwrap_or("-".to_string()),
            fair_yes,
            fair_no,
            mispricing.map(|p| p.to_string()).unwrap_or("-".to_string()),
            spread.map(|p| p.to_string()).unwrap_or("-".to_string()),
            edge.map(|p| p.to_string()).unwrap_or("-".to_string()),
        );
    }

    println!("└────────────────────────────────┴────────┴────────┴────────┴────────┴────────┴────────┴────────┴────────┴────────┘");
    println!();
    println!("Legend: Misprc = YesMid - FairYES (+ = market overprices YES)");
    println!("        Edge = FairYES - YesAsk (+ = YES underpriced, good to buy)");
    println!();
}

async fn run_tracker(
    config: &KalshiConfig,
    markets: Arc<RwLock<HashMap<String, MarketState>>>,
    vol: f64,
    csv_file: Option<Arc<RwLock<File>>>,
) -> Result<()> {
    let tickers: Vec<String> = {
        let m = markets.read().await;
        m.keys().cloned().collect()
    };

    if tickers.is_empty() {
        info!("No markets to track");
        return Ok(());
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

    let (ws_stream, _) = connect_async(request).await.context("Failed to connect to Kalshi WS")?;
    info!("Connected to Kalshi WebSocket");

    let (mut write, mut read) = ws_stream.split();

    // Subscribe
    let subscribe_msg = SubscribeCmd {
        id: 1,
        cmd: "subscribe",
        params: SubscribeParams {
            channels: vec!["orderbook_delta"],
            market_tickers: tickers.clone(),
        },
    };

    write.send(Message::Text(serde_json::to_string(&subscribe_msg)?)).await?;
    info!("Subscribed to {} markets", tickers.len());

    // Print initial state
    {
        let m = markets.read().await;
        print_market_table(&m, vol);
    }

    let mut last_print = std::time::Instant::now();
    let print_interval = Duration::from_secs(5);

    while let Some(msg) = read.next().await {
        match msg {
            Ok(Message::Text(text)) => {
                if let Ok(ws_msg) = serde_json::from_str::<KalshiWsMessage>(&text) {
                    if let Some(ticker) = ws_msg.msg.as_ref().and_then(|m| m.market_ticker.as_ref()) {
                        if ws_msg.msg_type == "orderbook_snapshot" || ws_msg.msg_type == "orderbook_delta" {
                            if let Some(body) = &ws_msg.msg {
                                let mut m = markets.write().await;
                                if let Some(market) = m.get_mut(ticker) {
                                    process_orderbook(market, body);
                                    market.update_time_remaining();

                                    // Log to CSV
                                    if let Some(ref csv) = csv_file {
                                        let (fair_yes, fair_no) = calc_fair_value_atm(market.minutes_remaining, vol);
                                        let snapshot = PriceSnapshot {
                                            timestamp: Utc::now().to_rfc3339(),
                                            ticker: ticker.clone(),
                                            minutes_remaining: market.minutes_remaining,
                                            yes_bid: market.yes_bid,
                                            yes_ask: market.yes_ask,
                                            yes_mid: market.yes_mid(),
                                            no_bid: market.no_bid,
                                            no_ask: market.no_ask,
                                            fair_yes,
                                            fair_no,
                                            mispricing: market.yes_mid().map(|m| m - fair_yes),
                                            spread: market.spread(),
                                        };

                                        let mut f = csv.write().await;
                                        writeln!(
                                            f,
                                            "{},{},{:.2},{},{},{},{},{},{},{},{},{}",
                                            snapshot.timestamp,
                                            snapshot.ticker,
                                            snapshot.minutes_remaining,
                                            snapshot.yes_bid.map(|v| v.to_string()).unwrap_or_default(),
                                            snapshot.yes_ask.map(|v| v.to_string()).unwrap_or_default(),
                                            snapshot.yes_mid.map(|v| v.to_string()).unwrap_or_default(),
                                            snapshot.no_bid.map(|v| v.to_string()).unwrap_or_default(),
                                            snapshot.no_ask.map(|v| v.to_string()).unwrap_or_default(),
                                            snapshot.fair_yes,
                                            snapshot.fair_no,
                                            snapshot.mispricing.map(|v| v.to_string()).unwrap_or_default(),
                                            snapshot.spread.map(|v| v.to_string()).unwrap_or_default(),
                                        ).ok();
                                    }
                                }
                            }
                        }
                    }
                }

                // Print update periodically
                if last_print.elapsed() >= print_interval {
                    // Clear screen and reprint
                    print!("\x1B[2J\x1B[1;1H");
                    print_header();
                    let m = markets.read().await;
                    print_market_table(&m, vol);
                    last_print = std::time::Instant::now();
                }
            }
            Ok(Message::Ping(data)) => {
                let _ = write.send(Message::Pong(data)).await;
            }
            Ok(Message::Close(_)) => {
                warn!("WebSocket closed");
                break;
            }
            Err(e) => {
                error!("WebSocket error: {}", e);
                break;
            }
            _ => {}
        }
    }

    Ok(())
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::from_default_env()
                .add_directive("live_fair_value_tracker=info".parse().unwrap())
                .add_directive("arb_bot=info".parse().unwrap()),
        )
        .init();

    let vol: f64 = std::env::var("VOL")
        .ok()
        .and_then(|v| v.parse::<f64>().ok())
        .unwrap_or(50.0) / 100.0;

    let csv_path = std::env::var("CSV_LOG").unwrap_or_else(|_| "fair_value_log.csv".to_string());

    print_header();

    // Load credentials
    let config = KalshiConfig::from_env().context("Failed to load Kalshi credentials")?;
    let client = KalshiApiClient::new(KalshiConfig::from_env()?);

    // Discover markets
    info!("Discovering crypto markets...");
    let market_list = discover_markets(&client).await?;
    info!("Found {} active markets", market_list.len());

    if market_list.is_empty() {
        warn!("No active markets found. Markets may not be open right now.");
        return Ok(());
    }

    // Build state
    let markets = Arc::new(RwLock::new({
        let mut m = HashMap::new();
        for market in market_list {
            info!("  {} ({:.1} min remaining)", market.ticker, market.minutes_remaining);
            m.insert(market.ticker.clone(), market);
        }
        m
    }));

    // Create CSV file
    let csv_file = {
        let mut f = File::create(&csv_path)?;
        writeln!(f, "timestamp,ticker,minutes_remaining,yes_bid,yes_ask,yes_mid,no_bid,no_ask,fair_yes,fair_no,mispricing,spread")?;
        Some(Arc::new(RwLock::new(f)))
    };
    info!("Logging to {}", csv_path);

    // Run tracker with auto-reconnect
    loop {
        match run_tracker(&config, markets.clone(), vol, csv_file.clone()).await {
            Ok(_) => info!("Tracker finished"),
            Err(e) => error!("Tracker error: {}", e),
        }
        info!("Reconnecting in 5 seconds...");
        tokio::time::sleep(Duration::from_secs(5)).await;

        // Rediscover markets (new ones may have opened)
        if let Ok(new_markets) = discover_markets(&client).await {
            let mut m = markets.write().await;
            for market in new_markets {
                if !m.contains_key(&market.ticker) {
                    info!("New market: {} ({:.1} min)", market.ticker, market.minutes_remaining);
                    m.insert(market.ticker.clone(), market);
                }
            }
            // Remove expired markets
            m.retain(|_, v| v.minutes_remaining > 0.0);
        }
    }
}
