//! Backtest Fair Value Model
//!
//! Tests if the Black-Scholes based fair value calculation is predictive
//! by comparing predictions to actual outcomes on settled markets.
//!
//! Usage:
//!   cargo run --release --bin backtest_fair_value -- --limit 100
//!   cargo run --release --bin backtest_fair_value -- --series KXBTC15M --vol 50
//!   cargo run --release --bin backtest_fair_value -- --series KXETH15M --vol 60

use anyhow::{Context, Result};
use chrono::{DateTime, TimeZone, Utc};
use clap::Parser;
use serde::Deserialize;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use arb_bot::kalshi::KalshiConfig;

const KALSHI_API_BASE: &str = "https://api.elections.kalshi.com/trade-api/v2";
const API_DELAY_MS: u64 = 100;
const POLYGON_API_KEY: &str = "o2Jm26X52_0tRq2W7V5JbsCUXdMjL7qk";

// ============================================================================
// FAIR VALUE CALCULATION (copied from fair_value.rs)
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

pub fn calc_fair_value(spot: f64, strike: f64, minutes_remaining: f64, annual_vol: f64) -> (f64, f64) {
    if minutes_remaining <= 0.0 {
        if spot > strike {
            return (1.0, 0.0);
        } else {
            return (0.0, 1.0);
        }
    }

    if annual_vol <= 0.0 {
        if spot > strike {
            return (1.0, 0.0);
        } else {
            return (0.0, 1.0);
        }
    }

    let time_years = minutes_remaining / 525960.0;
    let sqrt_t = time_years.sqrt();
    let log_ratio = (spot / strike).ln();
    let d2 = (log_ratio - 0.5 * annual_vol.powi(2) * time_years) / (annual_vol * sqrt_t);

    let yes_prob = norm_cdf(d2);
    let no_prob = 1.0 - yes_prob;

    (yes_prob, no_prob)
}

// ============================================================================
// API TYPES
// ============================================================================

#[derive(Debug, Deserialize)]
struct EventsResponse {
    events: Vec<Event>,
    #[serde(default)]
    cursor: Option<String>,
}

#[derive(Debug, Deserialize, Clone)]
struct Event {
    event_ticker: String,
    title: String,
}

#[derive(Debug, Deserialize)]
struct MarketsResponse {
    markets: Vec<Market>,
    #[serde(default)]
    cursor: Option<String>,
}

#[derive(Debug, Deserialize, Clone)]
struct Market {
    ticker: String,
    event_ticker: String,
    title: String,
    status: String,
    #[serde(default)]
    floor_strike: Option<f64>,
    #[serde(default)]
    close_time: Option<String>,
    #[serde(default)]
    expiration_time: Option<String>,
    #[serde(default)]
    result: Option<String>,
}

#[derive(Debug, Deserialize)]
struct TradesResponse {
    trades: Vec<Trade>,
    #[serde(default)]
    cursor: Option<String>,
}

#[derive(Debug, Deserialize, Clone)]
struct Trade {
    ticker: String,
    count: i64,
    yes_price: i64,
    no_price: i64,
    created_time: String,
}

// Polygon aggregates response
#[derive(Debug, Deserialize)]
struct PolygonAggsResponse {
    results: Option<Vec<PolygonBar>>,
    status: String,
}

#[derive(Debug, Deserialize, Clone)]
struct PolygonBar {
    t: i64,  // timestamp in ms
    o: f64,  // open
    h: f64,  // high
    l: f64,  // low
    c: f64,  // close
    v: f64,  // volume
}

// ============================================================================
// API CLIENT
// ============================================================================

struct BacktestClient {
    http: reqwest::Client,
    config: KalshiConfig,
}

impl BacktestClient {
    fn new(config: KalshiConfig) -> Self {
        Self {
            http: reqwest::Client::builder()
                .timeout(Duration::from_secs(30))
                .build()
                .expect("Failed to build HTTP client"),
            config,
        }
    }

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
                let backoff_ms = 500 * (1 << retries);
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

    async fn get_settled_markets(&self, series: &str, limit: u32) -> Result<Vec<Market>> {
        let path = format!("/events?series_ticker={}&status=settled&limit={}", series, limit);
        let resp: EventsResponse = self.get(&path).await?;

        let mut all_markets = Vec::new();
        for event in resp.events {
            let path = format!("/markets?event_ticker={}", event.event_ticker);
            let resp: MarketsResponse = self.get(&path).await?;
            all_markets.extend(resp.markets);
        }

        Ok(all_markets)
    }

    async fn get_trades(&self, ticker: &str, limit: u32) -> Result<Vec<Trade>> {
        let path = format!("/markets/trades?ticker={}&limit={}", ticker, limit);
        let resp: TradesResponse = self.get(&path).await?;
        Ok(resp.trades)
    }
}

// Get spot price from Polygon at a specific timestamp
async fn get_spot_price(http: &reqwest::Client, asset: &str, timestamp_ms: i64) -> Result<f64> {
    let symbol = if asset == "BTC" { "X:BTCUSD" } else { "X:ETHUSD" };

    // Get 1-minute bars around the timestamp
    let from_ms = timestamp_ms - 60_000;
    let to_ms = timestamp_ms + 60_000;

    let url = format!(
        "https://api.polygon.io/v2/aggs/ticker/{}/range/1/minute/{}/{}?apiKey={}",
        symbol, from_ms, to_ms, POLYGON_API_KEY
    );

    let resp: PolygonAggsResponse = http
        .get(&url)
        .send()
        .await?
        .json()
        .await?;

    if let Some(bars) = resp.results {
        if let Some(bar) = bars.first() {
            return Ok(bar.c);
        }
    }

    anyhow::bail!("No price data for {} at {}", asset, timestamp_ms)
}

// Parse strike from market title like "Bitcoin above $104,500?"
fn parse_strike_from_title(title: &str) -> Option<f64> {
    // Look for dollar amount pattern manually
    let dollar_idx = title.find('$')?;
    let after_dollar = &title[dollar_idx + 1..];

    let mut num_str = String::new();
    for c in after_dollar.chars() {
        if c.is_ascii_digit() || c == '.' {
            num_str.push(c);
        } else if c == ',' {
            // Skip commas in numbers
            continue;
        } else if !num_str.is_empty() {
            break;
        }
    }

    if num_str.is_empty() {
        return None;
    }

    num_str.parse().ok()
}

// Parse expiration time from market ticker like "KXBTC15M-25DEC171615-B5"
fn parse_expiry_from_ticker(ticker: &str) -> Option<DateTime<Utc>> {
    // Format: KXBTC15M-25DEC171615-B5
    // The date part is: 25DEC171615 = year 2025, DEC, day 17, 16:15 EST
    let parts: Vec<&str> = ticker.split('-').collect();
    if parts.len() < 2 {
        return None;
    }

    let dt_str = parts[1];
    if dt_str.len() < 11 {
        return None;
    }

    // Parse: YY MMM DD HHMM
    // 25 DEC 17 1615
    let year: i32 = 2000 + dt_str[0..2].parse::<i32>().ok()?;
    let month_str = &dt_str[2..5];
    let day: u32 = dt_str[5..7].parse().ok()?;
    let hour: u32 = dt_str[7..9].parse().ok()?;
    let minute: u32 = dt_str[9..11].parse().ok()?;

    let month = match month_str {
        "JAN" => 1, "FEB" => 2, "MAR" => 3, "APR" => 4,
        "MAY" => 5, "JUN" => 6, "JUL" => 7, "AUG" => 8,
        "SEP" => 9, "OCT" => 10, "NOV" => 11, "DEC" => 12,
        _ => return None,
    };

    // Convert EST to UTC (+5 hours)
    let hour_utc = (hour + 5) % 24;
    let day_adjust = if hour + 5 >= 24 { 1 } else { 0 };

    Utc.with_ymd_and_hms(year, month, day + day_adjust, hour_utc, minute, 0).single()
}

// ============================================================================
// BACKTEST RESULTS
// ============================================================================

#[derive(Debug, Default)]
struct CalibrationBucket {
    count: u32,
    yes_wins: u32,
    total_pred: f64,
}

#[derive(Debug)]
struct BacktestResult {
    ticker: String,
    strike: f64,
    minutes_before: f64,
    spot: f64,
    predicted_yes: f64,
    actual_yes: bool,
    market_yes_price: Option<i64>,
}

fn print_calibration(results: &[BacktestResult]) {
    // Group by prediction bucket (0-10%, 10-20%, etc.)
    let mut buckets: Vec<CalibrationBucket> = (0..10).map(|_| CalibrationBucket::default()).collect();

    for r in results {
        let bucket_idx = ((r.predicted_yes * 10.0).floor() as usize).min(9);
        buckets[bucket_idx].count += 1;
        buckets[bucket_idx].total_pred += r.predicted_yes;
        if r.actual_yes {
            buckets[bucket_idx].yes_wins += 1;
        }
    }

    println!();
    println!("╔══════════════════════════════════════════════════════════════════════════════╗");
    println!("║                         MODEL CALIBRATION ANALYSIS                           ║");
    println!("╠══════════════════════════════════════════════════════════════════════════════╣");
    println!("║  Predicted   │  Count  │  Actual YES%  │  Avg Pred  │  Calibration Error    ║");
    println!("╠══════════════════════════════════════════════════════════════════════════════╣");

    let mut total_brier = 0.0;
    let mut total_count = 0;

    for (i, bucket) in buckets.iter().enumerate() {
        if bucket.count == 0 {
            continue;
        }

        let pred_range = format!("{:>2}-{:>2}%", i * 10, (i + 1) * 10);
        let actual_pct = (bucket.yes_wins as f64 / bucket.count as f64) * 100.0;
        let avg_pred = (bucket.total_pred / bucket.count as f64) * 100.0;
        let error = actual_pct - avg_pred;

        // Brier score contribution
        for r in results {
            let bucket_idx = ((r.predicted_yes * 10.0).floor() as usize).min(9);
            if bucket_idx == i {
                let actual = if r.actual_yes { 1.0 } else { 0.0 };
                total_brier += (r.predicted_yes - actual).powi(2);
                total_count += 1;
            }
        }

        let error_str = if error.abs() < 5.0 {
            format!("{:+6.1}% ✓", error)
        } else if error.abs() < 10.0 {
            format!("{:+6.1}% ~", error)
        } else {
            format!("{:+6.1}% ✗", error)
        };

        println!("║  {:>10}  │  {:>5}  │  {:>10.1}%  │  {:>7.1}%  │  {:>18}   ║",
                 pred_range, bucket.count, actual_pct, avg_pred, error_str);
    }

    let brier_score = if total_count > 0 { total_brier / total_count as f64 } else { 0.0 };

    println!("╠══════════════════════════════════════════════════════════════════════════════╣");
    println!("║  Brier Score: {:.4} (lower is better, 0.25 = random, 0 = perfect)            ║", brier_score);
    println!("║  Total predictions: {}                                                       ║", results.len());
    println!("╚══════════════════════════════════════════════════════════════════════════════╝");
}

fn print_accuracy_by_time(results: &[BacktestResult]) {
    use std::collections::BTreeMap;

    // Group results by minutes_before
    let mut by_time: BTreeMap<i64, Vec<&BacktestResult>> = BTreeMap::new();
    for r in results {
        let key = r.minutes_before as i64;
        by_time.entry(key).or_default().push(r);
    }

    if by_time.len() <= 1 {
        return; // No point showing breakdown for single time point
    }

    println!();
    println!("╔══════════════════════════════════════════════════════════════════════════════╗");
    println!("║                         ACCURACY BY TIME REMAINING                           ║");
    println!("╠══════════════════════════════════════════════════════════════════════════════╣");
    println!("║  T-min │  Count  │  Brier  │  Accuracy  │  Avg Edge  │  P&L/trade           ║");
    println!("╠══════════════════════════════════════════════════════════════════════════════╣");

    for (minutes, group) in &by_time {
        let count = group.len();

        // Brier score
        let brier: f64 = group.iter()
            .map(|r| {
                let actual = if r.actual_yes { 1.0 } else { 0.0 };
                (r.predicted_yes - actual).powi(2)
            })
            .sum::<f64>() / count as f64;

        // Accuracy (prediction > 0.5 matches actual)
        let correct = group.iter()
            .filter(|r| (r.predicted_yes > 0.5) == r.actual_yes)
            .count();
        let accuracy = correct as f64 / count as f64 * 100.0;

        // Edge analysis (only where market price available)
        let with_market: Vec<_> = group.iter()
            .filter(|r| r.market_yes_price.is_some())
            .collect();

        let (avg_edge, pnl_per_trade) = if !with_market.is_empty() {
            let mut total_edge = 0.0;
            let mut total_pnl = 0.0;
            let mut trade_count = 0;

            for r in &with_market {
                let market_price = r.market_yes_price.unwrap() as f64;
                let fair_value = r.predicted_yes * 100.0;
                let edge = (fair_value - market_price).abs();
                total_edge += edge;

                let actual_payout = if r.actual_yes { 100.0 } else { 0.0 };
                if fair_value > market_price + 3.0 {
                    total_pnl += actual_payout - market_price;
                    trade_count += 1;
                } else if fair_value < market_price - 3.0 {
                    total_pnl += (100.0 - actual_payout) - (100.0 - market_price);
                    trade_count += 1;
                }
            }

            let avg = total_edge / with_market.len() as f64;
            let pnl = if trade_count > 0 { total_pnl / trade_count as f64 } else { 0.0 };
            (avg, pnl)
        } else {
            (0.0, 0.0)
        };

        println!("║  {:>4}  │  {:>5}  │  {:.4}  │    {:>5.1}%  │    {:>5.1}¢  │  {:>+6.2}¢             ║",
                 minutes, count, brier, accuracy, avg_edge, pnl_per_trade);
    }

    println!("╚══════════════════════════════════════════════════════════════════════════════╝");
}

fn print_edge_analysis(results: &[BacktestResult]) {
    // Compare model prediction to market price where available
    let with_market: Vec<_> = results.iter()
        .filter(|r| r.market_yes_price.is_some())
        .collect();

    if with_market.is_empty() {
        println!("\nNo market price data available for edge analysis.");
        return;
    }

    println!();
    println!("╔══════════════════════════════════════════════════════════════════════════════╗");
    println!("║                           EDGE ANALYSIS                                      ║");
    println!("╠══════════════════════════════════════════════════════════════════════════════╣");

    let mut total_edge = 0.0;
    for r in &with_market {
        let market_price = r.market_yes_price.unwrap() as f64;
        let fair_value = r.predicted_yes * 100.0;
        total_edge += (fair_value - market_price).abs();
    }
    let avg_edge = total_edge / with_market.len() as f64;
    println!("║  Average |fair - market|: {:.1}¢                                             ║", avg_edge);

    // Test different edge thresholds
    println!("║                                                                              ║");
    println!("║  ASYMMETRIC EDGE ANALYSIS (YES_edge / NO_edge):                             ║");
    println!("║  ────────────────────────────────────────────────────────────────────────── ║");
    println!("║  {:>6} {:>6} │ {:>5} {:>8} │ {:>5} {:>8} │ {:>6} {:>8}              ║",
             "Y_edg", "N_edg", "Y_cnt", "Y_avg", "N_cnt", "N_avg", "Total", "Avg");
    println!("║  ────────────────────────────────────────────────────────────────────────── ║");

    for yes_edge_req in [3, 5, 7, 10, 15] {
        for no_edge_req in [3, 5, 7, 10, 15] {
            let mut buy_yes_profit = 0.0;
            let mut buy_yes_count = 0;
            let mut buy_no_profit = 0.0;
            let mut buy_no_count = 0;

            for r in &with_market {
                let market_price = r.market_yes_price.unwrap() as f64;
                let fair_value = r.predicted_yes * 100.0;
                let actual_payout = if r.actual_yes { 100.0 } else { 0.0 };

                if fair_value > market_price + yes_edge_req as f64 {
                    let profit = actual_payout - market_price;
                    buy_yes_profit += profit;
                    buy_yes_count += 1;
                } else if fair_value < market_price - no_edge_req as f64 {
                    let profit = (100.0 - actual_payout) - (100.0 - market_price);
                    buy_no_profit += profit;
                    buy_no_count += 1;
                }
            }

            let total_profit = buy_yes_profit + buy_no_profit;
            let total_trades = buy_yes_count + buy_no_count;

            if total_trades > 0 {
                let yes_avg = if buy_yes_count > 0 { buy_yes_profit / buy_yes_count as f64 } else { 0.0 };
                let no_avg = if buy_no_count > 0 { buy_no_profit / buy_no_count as f64 } else { 0.0 };
                let total_avg = total_profit / total_trades as f64;

                // Highlight best combinations
                let marker = if total_avg > 8.0 { "★" } else if total_avg > 5.0 { "+" } else { " " };

                println!("║  {:>5}¢ {:>5}¢ │ {:>5} {:>+7.1}¢ │ {:>5} {:>+7.1}¢ │ {:>6} {:>+7.1}¢ {}           ║",
                         yes_edge_req, no_edge_req,
                         buy_yes_count, yes_avg,
                         buy_no_count, no_avg,
                         total_trades, total_avg, marker);
            }
        }
    }

    println!("╚══════════════════════════════════════════════════════════════════════════════╝");
}

// ============================================================================
// CLI
// ============================================================================

#[derive(Parser, Debug)]
#[command(name = "backtest_fair_value")]
#[command(about = "Backtest fair value model against settled Kalshi markets")]
struct Args {
    /// Series ticker (KXBTC15M or KXETH15M)
    #[arg(short, long, default_value = "KXBTC15M")]
    series: String,

    /// Number of markets to test
    #[arg(short, long, default_value = "50")]
    limit: u32,

    /// Annual volatility % (default: 50)
    #[arg(short, long, default_value = "60")]
    vol: f64,

    /// Minutes before expiry to measure (comma-separated, e.g., "1,3,5,10")
    #[arg(short, long, default_value = "1,2,3,5,7,10,15")]
    minutes: String,

    /// Verbose output
    #[arg(long)]
    verbose: bool,
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();
    let vol = args.vol / 100.0;
    let asset = if args.series.contains("ETH") { "ETH" } else { "BTC" };

    // Parse minutes list
    let minutes_list: Vec<f64> = args.minutes
        .split(',')
        .filter_map(|s| s.trim().parse().ok())
        .collect();

    if minutes_list.is_empty() {
        anyhow::bail!("No valid minutes values provided");
    }

    println!("════════════════════════════════════════════════════════════════════════════════");
    println!("  FAIR VALUE MODEL BACKTEST");
    println!("════════════════════════════════════════════════════════════════════════════════");
    println!("  Series: {}", args.series);
    println!("  Asset: {}", asset);
    println!("  Volatility: {}%", args.vol);
    println!("  Time points: {:?} minutes before expiry", minutes_list);
    println!("  Markets to test: {}", args.limit);
    println!("════════════════════════════════════════════════════════════════════════════════");
    println!();

    let config = KalshiConfig::from_env().context("Failed to load Kalshi credentials")?;
    let client = BacktestClient::new(config);
    let http = reqwest::Client::new();

    // Get settled markets
    eprintln!("Fetching settled markets...");
    let markets = client.get_settled_markets(&args.series, args.limit).await?;
    eprintln!("Found {} settled markets", markets.len());

    let mut results = Vec::new();
    let mut skip_no_result = 0;
    let mut skip_no_strike = 0;
    let mut skip_no_expiry = 0;
    let mut skip_no_spot = 0;

    for (i, market) in markets.iter().enumerate() {
        // Need result to know outcome
        let result = match &market.result {
            Some(r) => r.clone(),
            None => {
                skip_no_result += 1;
                continue;
            }
        };

        let actual_yes = result.to_lowercase() == "yes";

        // Get strike price
        let strike = market.floor_strike
            .or_else(|| parse_strike_from_title(&market.title));

        let strike = match strike {
            Some(s) => s,
            None => {
                if args.verbose {
                    eprintln!("  Skipping {} - no strike found in title: {}", market.ticker, market.title);
                }
                skip_no_strike += 1;
                continue;
            }
        };

        // Parse expiry time
        let expiry = match parse_expiry_from_ticker(&market.ticker) {
            Some(e) => e,
            None => {
                if args.verbose {
                    eprintln!("  Skipping {} - can't parse expiry from ticker", market.ticker);
                }
                skip_no_expiry += 1;
                continue;
            }
        };

        // Get market trades once per market
        let trades = client.get_trades(&market.ticker, 200).await.unwrap_or_default();

        // Test each time point
        for &minutes in &minutes_list {
            // Calculate timestamp for X minutes before expiry
            let measure_time = expiry - chrono::Duration::minutes(minutes as i64);
            let measure_ts_ms = measure_time.timestamp_millis();

            // Get spot price at that time
            let spot = match get_spot_price(&http, asset, measure_ts_ms).await {
                Ok(s) => s,
                Err(e) => {
                    if args.verbose {
                        eprintln!("  Skipping {} @{}min - no spot data: {}", market.ticker, minutes, e);
                    }
                    skip_no_spot += 1;
                    continue;
                }
            };

            // Calculate fair value
            let (yes_prob, _) = calc_fair_value(spot, strike, minutes, vol);

            // Find market price closest to measure time
            let market_price = trades.iter()
                .filter(|t| {
                    if let Ok(trade_time) = DateTime::parse_from_rfc3339(&t.created_time) {
                        let diff = (trade_time.timestamp() - measure_time.timestamp()).abs();
                        diff < 120 // Within 2 minutes
                    } else {
                        false
                    }
                })
                .map(|t| t.yes_price)
                .next();

            if args.verbose {
                eprintln!("[{}/{}] {} @{}min | Strike=${:.0} Spot=${:.0} | Pred={:.0}% Actual={} | Market={}¢",
                         i + 1, markets.len(), market.ticker, minutes, strike, spot,
                         yes_prob * 100.0, if actual_yes { "YES" } else { "NO" },
                         market_price.map(|p| p.to_string()).unwrap_or("-".into()));
            }

            results.push(BacktestResult {
                ticker: market.ticker.clone(),
                strike,
                minutes_before: minutes,
                spot,
                predicted_yes: yes_prob,
                actual_yes,
                market_yes_price: market_price,
            });

            tokio::time::sleep(Duration::from_millis(50)).await;
        }

        if !args.verbose {
            eprint!("\r[{}/{}] Processing {} time points...", i + 1, markets.len(), minutes_list.len());
        }
    }

    eprintln!();
    let total_skipped = skip_no_result + skip_no_strike + skip_no_expiry + skip_no_spot;
    eprintln!("Processed {} markets, skipped {}", results.len(), total_skipped);
    if total_skipped > 0 {
        eprintln!("  Skip reasons:");
        if skip_no_result > 0 {
            eprintln!("    - No result field: {}", skip_no_result);
        }
        if skip_no_strike > 0 {
            eprintln!("    - No strike price: {}", skip_no_strike);
        }
        if skip_no_expiry > 0 {
            eprintln!("    - Can't parse expiry: {}", skip_no_expiry);
        }
        if skip_no_spot > 0 {
            eprintln!("    - No spot data from Polygon: {}", skip_no_spot);
        }
    }

    if results.is_empty() {
        eprintln!("No results to analyze!");
        return Ok(());
    }

    print_calibration(&results);

    // Print accuracy breakdown by time remaining
    print_accuracy_by_time(&results);

    // Print edge analysis
    print_edge_analysis(&results);

    println!();
    println!("Sample predictions (first 200):");
    println!("────────────────────────────────────────────────────────────────────────────────────────────");
    println!("{:40} {:>8} {:>8} {:>6} {:>8} {:>8} {:>8}",
             "Ticker", "Strike", "Spot", "T-min", "Pred", "Actual", "Market");
    println!("────────────────────────────────────────────────────────────────────────────────────────────");

    for r in results.iter().take(200) {
        let actual_str = if r.actual_yes { "YES" } else { "NO" };
        let market_str = r.market_yes_price.map(|p| format!("{}¢", p)).unwrap_or("-".into());
        let correct = (r.predicted_yes > 0.5) == r.actual_yes;
        let marker = if correct { "✓" } else { "✗" };

        println!("{:40} {:>8.0} {:>8.0} {:>6.1} {:>6.0}% {:>6} {:>8} {}",
                 r.ticker, r.strike, r.spot, r.minutes_before, r.predicted_yes * 100.0, actual_str, market_str, marker);
    }

    Ok(())
}
