//! Kalshi Historical Data API Script
//!
//! Queries historical data for Kalshi crypto markets (BTC/ETH 15-minute markets).
//! Fetches trades, market history, and candlestick data.
//!
//! Usage:
//!   cargo run --release --bin historical_data -- [OPTIONS]
//!
//! Options:
//!   --series <SERIES>     Series ticker (default: KXBTC15M)
//!   --ticker <TICKER>     Specific market ticker (optional)
//!   --trades              Fetch trade history
//!   --history             Fetch market history/candlesticks
//!   --limit <N>           Number of records to fetch (default: 100)
//!   --output <FORMAT>     Output format: json, csv (default: json)
//!   --start <TIMESTAMP>   Start timestamp (ISO 8601 or Unix)
//!   --end <TIMESTAMP>     End timestamp (ISO 8601 or Unix)
//!
//! Environment:
//!   KALSHI_API_KEY_ID - Your Kalshi API key ID
//!   KALSHI_PRIVATE_KEY_PATH - Path to your Kalshi private key PEM file

use anyhow::{Context, Result};
use chrono::{DateTime, TimeZone, Utc};
use clap::Parser;
use serde::{Deserialize, Serialize};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use arb_bot::kalshi::KalshiConfig;

/// Kalshi REST API base URL
const KALSHI_API_BASE: &str = "https://api.elections.kalshi.com/trade-api/v2";

/// API rate limit delay (milliseconds)
const API_DELAY_MS: u64 = 60;

/// Default crypto series
const DEFAULT_SERIES: &str = "KXBTC15M";

// === API Response Types ===

#[derive(Debug, Deserialize, Serialize)]
pub struct TradesResponse {
    pub trades: Vec<Trade>,
    #[serde(default)]
    pub cursor: Option<String>,
}

#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct Trade {
    pub trade_id: String,
    pub ticker: String,
    pub count: i64,
    pub yes_price: i64,
    pub no_price: i64,
    pub taker_side: Option<String>,
    pub created_time: String,
}

#[derive(Debug, Deserialize, Serialize)]
pub struct MarketHistoryResponse {
    pub history: Vec<MarketHistoryPoint>,
    #[serde(default)]
    pub cursor: Option<String>,
}

#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct MarketHistoryPoint {
    pub ts: i64,
    pub yes_price: Option<i64>,
    pub yes_ask: Option<i64>,
    pub yes_bid: Option<i64>,
    pub volume: Option<i64>,
    pub open_interest: Option<i64>,
}

#[derive(Debug, Deserialize, Serialize)]
pub struct CandlesticksResponse {
    pub candlesticks: Vec<Candlestick>,
    #[serde(default)]
    pub cursor: Option<String>,
}

#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct Candlestick {
    pub end_period_ts: i64,
    pub yes_price: CandlestickData,
    pub volume: i64,
    pub open_interest: i64,
}

#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct CandlestickData {
    pub open: i64,
    pub high: i64,
    pub low: i64,
    pub close: i64,
}

#[derive(Debug, Deserialize, Serialize)]
pub struct EventsResponse {
    pub events: Vec<Event>,
    #[serde(default)]
    pub cursor: Option<String>,
}

#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct Event {
    pub event_ticker: String,
    pub title: String,
    #[serde(default)]
    pub sub_title: Option<String>,
    #[serde(default)]
    pub category: Option<String>,
}

#[derive(Debug, Deserialize, Serialize)]
pub struct MarketsResponse {
    pub markets: Vec<Market>,
    #[serde(default)]
    pub cursor: Option<String>,
}

#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct Market {
    pub ticker: String,
    pub event_ticker: String,
    pub title: String,
    pub status: String,
    pub yes_ask: Option<i64>,
    pub yes_bid: Option<i64>,
    pub no_ask: Option<i64>,
    pub no_bid: Option<i64>,
    pub last_price: Option<i64>,
    pub volume: Option<i64>,
    pub volume_24h: Option<i64>,
    pub open_interest: Option<i64>,
    #[serde(default)]
    pub close_time: Option<String>,
    #[serde(default)]
    pub expiration_time: Option<String>,
    #[serde(default)]
    pub result: Option<String>,
}

// === API Client ===

pub struct HistoricalDataClient {
    http: reqwest::Client,
    config: KalshiConfig,
}

impl HistoricalDataClient {
    pub fn new(config: KalshiConfig) -> Self {
        Self {
            http: reqwest::Client::builder()
                .timeout(Duration::from_secs(30))
                .build()
                .expect("Failed to build HTTP client"),
            config,
        }
    }

    /// Authenticated GET request with rate limiting
    async fn get<T: serde::de::DeserializeOwned>(&self, path: &str) -> Result<T> {
        let mut retries = 0;
        const MAX_RETRIES: u32 = 5;

        loop {
            let url = format!("{}{}", KALSHI_API_BASE, path);
            let timestamp_ms = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_millis() as u64;

            let full_path = format!("/trade-api/v2{}", path);
            let signature = self.config.sign(&format!("{}GET{}", timestamp_ms, full_path))?;

            let resp = self.http
                .get(&url)
                .header("KALSHI-ACCESS-KEY", &self.config.api_key_id)
                .header("KALSHI-ACCESS-SIGNATURE", &signature)
                .header("KALSHI-ACCESS-TIMESTAMP", timestamp_ms.to_string())
                .send()
                .await?;

            let status = resp.status();

            if status == reqwest::StatusCode::TOO_MANY_REQUESTS {
                retries += 1;
                if retries > MAX_RETRIES {
                    anyhow::bail!("Rate limited after {} retries", MAX_RETRIES);
                }
                let backoff_ms = 2000 * (1 << retries);
                eprintln!("Rate limited, backing off {}ms...", backoff_ms);
                tokio::time::sleep(Duration::from_millis(backoff_ms)).await;
                continue;
            }

            if !status.is_success() {
                let body = resp.text().await.unwrap_or_default();
                anyhow::bail!("API error {}: {}", status, body);
            }

            let data: T = resp.json().await?;
            tokio::time::sleep(Duration::from_millis(API_DELAY_MS)).await;
            return Ok(data);
        }
    }

    /// Get events for a series
    pub async fn get_events(&self, series_ticker: &str, status: &str, limit: u32) -> Result<Vec<Event>> {
        let path = format!("/events?series_ticker={}&status={}&limit={}", series_ticker, status, limit);
        let resp: EventsResponse = self.get(&path).await?;
        Ok(resp.events)
    }

    /// Get markets for an event
    pub async fn get_markets(&self, event_ticker: &str) -> Result<Vec<Market>> {
        let path = format!("/markets?event_ticker={}", event_ticker);
        let resp: MarketsResponse = self.get(&path).await?;
        Ok(resp.markets)
    }

    /// Get all markets for a series (combines events + markets)
    pub async fn get_series_markets(&self, series_ticker: &str, status: &str, limit: u32) -> Result<Vec<Market>> {
        let events = self.get_events(series_ticker, status, limit).await?;
        let mut all_markets = Vec::new();

        for event in events {
            let markets = self.get_markets(&event.event_ticker).await?;
            all_markets.extend(markets);
        }

        Ok(all_markets)
    }

    /// Get trade history (global trades endpoint)
    pub async fn get_trades(
        &self,
        ticker: Option<&str>,
        limit: u32,
        cursor: Option<&str>,
        min_ts: Option<i64>,
        max_ts: Option<i64>,
    ) -> Result<TradesResponse> {
        let mut path = format!("/markets/trades?limit={}", limit);

        if let Some(t) = ticker {
            path.push_str(&format!("&ticker={}", t));
        }
        if let Some(c) = cursor {
            path.push_str(&format!("&cursor={}", c));
        }
        if let Some(ts) = min_ts {
            path.push_str(&format!("&min_ts={}", ts));
        }
        if let Some(ts) = max_ts {
            path.push_str(&format!("&max_ts={}", ts));
        }

        self.get(&path).await
    }

    /// Get all trades with pagination
    pub async fn get_all_trades(
        &self,
        ticker: Option<&str>,
        limit: u32,
        min_ts: Option<i64>,
        max_ts: Option<i64>,
    ) -> Result<Vec<Trade>> {
        let mut all_trades = Vec::new();
        let mut cursor: Option<String> = None;

        loop {
            let resp = self.get_trades(ticker, limit.min(100), cursor.as_deref(), min_ts, max_ts).await?;
            all_trades.extend(resp.trades);

            if resp.cursor.is_none() || all_trades.len() >= limit as usize {
                break;
            }
            cursor = resp.cursor;
        }

        all_trades.truncate(limit as usize);
        Ok(all_trades)
    }

    /// Get market history (price/volume snapshots)
    pub async fn get_market_history(
        &self,
        ticker: &str,
        limit: u32,
        cursor: Option<&str>,
        min_ts: Option<i64>,
        max_ts: Option<i64>,
    ) -> Result<MarketHistoryResponse> {
        let mut path = format!("/markets/{}/history?limit={}", ticker, limit);

        if let Some(c) = cursor {
            path.push_str(&format!("&cursor={}", c));
        }
        if let Some(ts) = min_ts {
            path.push_str(&format!("&min_ts={}", ts));
        }
        if let Some(ts) = max_ts {
            path.push_str(&format!("&max_ts={}", ts));
        }

        self.get(&path).await
    }

    /// Get candlestick data
    pub async fn get_candlesticks(
        &self,
        series_ticker: &str,
        ticker: &str,
        period_interval: u32, // in minutes
        limit: u32,
    ) -> Result<CandlesticksResponse> {
        let path = format!(
            "/series/{}/markets/{}/candlesticks?period_interval={}&limit={}",
            series_ticker, ticker, period_interval, limit
        );
        self.get(&path).await
    }
}

// === Output Formatting ===

fn format_timestamp(ts: i64) -> String {
    Utc.timestamp_opt(ts, 0)
        .single()
        .map(|dt| dt.format("%Y-%m-%d %H:%M:%S UTC").to_string())
        .unwrap_or_else(|| ts.to_string())
}

fn parse_timestamp(s: &str) -> Result<i64> {
    // Try parsing as ISO 8601
    if let Ok(dt) = DateTime::parse_from_rfc3339(s) {
        return Ok(dt.timestamp());
    }
    // Try parsing as Unix timestamp
    if let Ok(ts) = s.parse::<i64>() {
        return Ok(ts);
    }
    // Try parsing as date only
    if let Ok(dt) = chrono::NaiveDate::parse_from_str(s, "%Y-%m-%d") {
        return Ok(dt.and_hms_opt(0, 0, 0).unwrap().and_utc().timestamp());
    }
    anyhow::bail!("Invalid timestamp format: {}", s)
}

fn trades_to_csv(trades: &[Trade]) -> String {
    let mut csv = String::from("trade_id,ticker,count,yes_price,no_price,taker_side,created_time\n");
    for t in trades {
        csv.push_str(&format!(
            "{},{},{},{},{},{},{}\n",
            t.trade_id,
            t.ticker,
            t.count,
            t.yes_price,
            t.no_price,
            t.taker_side.as_deref().unwrap_or(""),
            t.created_time
        ));
    }
    csv
}

fn history_to_csv(history: &[MarketHistoryPoint]) -> String {
    let mut csv = String::from("timestamp,yes_price,yes_ask,yes_bid,volume,open_interest\n");
    for h in history {
        csv.push_str(&format!(
            "{},{},{},{},{},{}\n",
            format_timestamp(h.ts),
            h.yes_price.map(|p| p.to_string()).unwrap_or_default(),
            h.yes_ask.map(|p| p.to_string()).unwrap_or_default(),
            h.yes_bid.map(|p| p.to_string()).unwrap_or_default(),
            h.volume.map(|v| v.to_string()).unwrap_or_default(),
            h.open_interest.map(|o| o.to_string()).unwrap_or_default(),
        ));
    }
    csv
}

fn markets_to_csv(markets: &[Market]) -> String {
    let mut csv = String::from("ticker,event_ticker,title,status,yes_ask,yes_bid,no_ask,no_bid,last_price,volume,volume_24h,open_interest,close_time,result\n");
    for m in markets {
        csv.push_str(&format!(
            "{},{},\"{}\",{},{},{},{},{},{},{},{},{},{},{}\n",
            m.ticker,
            m.event_ticker,
            m.title.replace('"', "\"\""),
            m.status,
            m.yes_ask.map(|p| p.to_string()).unwrap_or_default(),
            m.yes_bid.map(|p| p.to_string()).unwrap_or_default(),
            m.no_ask.map(|p| p.to_string()).unwrap_or_default(),
            m.no_bid.map(|p| p.to_string()).unwrap_or_default(),
            m.last_price.map(|p| p.to_string()).unwrap_or_default(),
            m.volume.map(|v| v.to_string()).unwrap_or_default(),
            m.volume_24h.map(|v| v.to_string()).unwrap_or_default(),
            m.open_interest.map(|o| o.to_string()).unwrap_or_default(),
            m.close_time.as_deref().unwrap_or(""),
            m.result.as_deref().unwrap_or(""),
        ));
    }
    csv
}

// === CLI ===

#[derive(Parser, Debug)]
#[command(name = "historical_data")]
#[command(about = "Kalshi Historical Data API - fetch trades, markets, and price history")]
struct Args {
    /// Series ticker (e.g., KXBTC15M, KXETH15M)
    #[arg(short, long, default_value = "KXBTC15M")]
    series: String,

    /// Specific market ticker
    #[arg(short, long)]
    ticker: Option<String>,

    /// Fetch trade history
    #[arg(long)]
    trades: bool,

    /// Fetch market price history
    #[arg(long)]
    history: bool,

    /// Fetch market list
    #[arg(short, long)]
    markets: bool,

    /// Number of records to fetch
    #[arg(short, long, default_value = "100")]
    limit: u32,

    /// Output format: json, csv
    #[arg(short, long, default_value = "json")]
    output: String,

    /// Start timestamp (ISO 8601, Unix, or YYYY-MM-DD)
    #[arg(long)]
    start: Option<String>,

    /// End timestamp (ISO 8601, Unix, or YYYY-MM-DD)
    #[arg(long)]
    end: Option<String>,

    /// Market status filter: open, closed, settled
    #[arg(long, default_value = "open")]
    status: String,
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();

    // Parse timestamps if provided
    let start_ts = args.start.as_ref().and_then(|s| parse_timestamp(s).ok());
    let end_ts = args.end.as_ref().and_then(|s| parse_timestamp(s).ok());

    // Default to markets if nothing specified
    let fetch_markets = args.markets || (!args.trades && !args.history);

    // Load Kalshi credentials
    let config = KalshiConfig::from_env().context("Failed to load Kalshi credentials")?;
    let client = HistoricalDataClient::new(config);

    eprintln!("Kalshi Historical Data API");
    eprintln!("==========================");
    eprintln!("Series: {}", args.series);
    if let Some(ref ticker) = args.ticker {
        eprintln!("Ticker: {}", ticker);
    }
    eprintln!("Limit: {}", args.limit);
    eprintln!("Output: {}", args.output);
    eprintln!();

    // Fetch markets
    if fetch_markets {
        eprintln!("Fetching markets for series {}...", args.series);
        let markets = client.get_series_markets(&args.series, &args.status, args.limit).await?;
        eprintln!("Found {} markets", markets.len());

        match args.output.as_str() {
            "csv" => println!("{}", markets_to_csv(&markets)),
            _ => println!("{}", serde_json::to_string_pretty(&markets)?),
        }
    }

    // Fetch trades (ticker optional - will get all trades if not specified)
    if args.trades {
        let ticker_ref = args.ticker.as_deref();

        if let Some(t) = ticker_ref {
            eprintln!("Fetching trades for {}...", t);
        } else {
            eprintln!("Fetching all trades...");
        }
        let trades = client.get_all_trades(ticker_ref, args.limit, start_ts, end_ts).await?;
        eprintln!("Found {} trades", trades.len());

        match args.output.as_str() {
            "csv" => println!("{}", trades_to_csv(&trades)),
            _ => println!("{}", serde_json::to_string_pretty(&trades)?),
        }
    }

    // Fetch history (requires ticker)
    if args.history {
        let ticker = args.ticker.as_ref().ok_or_else(|| {
            anyhow::anyhow!("--history requires --ticker <TICKER>")
        })?;

        eprintln!("Fetching price history for {}...", ticker);
        let resp = client.get_market_history(ticker, args.limit, None, start_ts, end_ts).await?;
        eprintln!("Found {} history points", resp.history.len());

        match args.output.as_str() {
            "csv" => println!("{}", history_to_csv(&resp.history)),
            _ => println!("{}", serde_json::to_string_pretty(&resp.history)?),
        }
    }

    Ok(())
}
