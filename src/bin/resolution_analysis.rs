//! Kalshi Crypto Market Resolution Analysis
//!
//! Analyzes historical crypto markets (BTC/ETH 15-minute) to determine:
//! - What percent of markets resolve YES vs NO
//! - How much this deviates from the expected 50/50
//! - Distribution by strike price relative to settlement
//!
//! Usage:
//!   cargo run --release --bin resolution_analysis
//!
//! Options:
//!   --series <SERIES>   Series to analyze (default: both KXBTC15M and KXETH15M)
//!   --limit <N>         Max events to fetch per series (default: 1000)
//!
//! Environment:
//!   KALSHI_API_KEY_ID - Your Kalshi API key ID
//!   KALSHI_PRIVATE_KEY_PATH - Path to your Kalshi private key PEM file

use anyhow::{Context, Result};
use clap::Parser;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use arb_bot::kalshi::KalshiConfig;

/// Kalshi REST API base URL
const KALSHI_API_BASE: &str = "https://api.elections.kalshi.com/trade-api/v2";

/// API rate limit delay (milliseconds)
const API_DELAY_MS: u64 = 100;

/// Crypto series to analyze
const BTC_SERIES: &str = "KXBTC15M";
const ETH_SERIES: &str = "KXETH15M";

// === API Response Types ===

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
    #[serde(default)]
    pub mutually_exclusive: Option<bool>,
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
    #[serde(default)]
    pub yes_sub_title: Option<String>,
    #[serde(default)]
    pub floor_strike: Option<f64>,
    #[serde(default)]
    pub cap_strike: Option<f64>,
}

// === API Client ===

pub struct AnalysisClient {
    http: reqwest::Client,
    config: KalshiConfig,
}

impl AnalysisClient {
    pub fn new(config: KalshiConfig) -> Self {
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
                let backoff_ms = 2000 * (1 << retries);
                eprintln!("  Rate limited, backing off {}ms...", backoff_ms);
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

    /// Get events with pagination
    pub async fn get_all_events(&self, series_ticker: &str, status: &str, limit: u32) -> Result<Vec<Event>> {
        let mut all_events = Vec::new();
        let mut cursor: Option<String> = None;

        loop {
            let mut path = format!("/events?series_ticker={}&status={}&limit=100", series_ticker, status);
            if let Some(ref c) = cursor {
                path.push_str(&format!("&cursor={}", c));
            }

            let resp: EventsResponse = self.get(&path).await?;
            let count = resp.events.len();
            all_events.extend(resp.events);

            eprint!("\r  Fetched {} events...", all_events.len());

            if resp.cursor.is_none() || count == 0 || all_events.len() >= limit as usize {
                break;
            }
            cursor = resp.cursor;
        }
        eprintln!();

        all_events.truncate(limit as usize);
        Ok(all_events)
    }

    /// Get markets for an event
    pub async fn get_markets(&self, event_ticker: &str) -> Result<Vec<Market>> {
        let path = format!("/markets?event_ticker={}", event_ticker);
        let resp: MarketsResponse = self.get(&path).await?;
        Ok(resp.markets)
    }
}

// === Analysis Types ===

#[derive(Debug, Default)]
struct ResolutionStats {
    total_markets: u32,
    yes_count: u32,
    no_count: u32,
    unknown_count: u32,
    /// Volume-weighted YES rate
    total_volume: i64,
    yes_volume: i64,
    no_volume: i64,
}

impl ResolutionStats {
    fn yes_rate(&self) -> f64 {
        if self.yes_count + self.no_count == 0 {
            return 0.5;
        }
        self.yes_count as f64 / (self.yes_count + self.no_count) as f64
    }

    fn no_rate(&self) -> f64 {
        1.0 - self.yes_rate()
    }

    fn deviation_from_50(&self) -> f64 {
        (self.yes_rate() - 0.5).abs()
    }

    fn volume_weighted_yes_rate(&self) -> f64 {
        if self.yes_volume + self.no_volume == 0 {
            return 0.5;
        }
        self.yes_volume as f64 / (self.yes_volume + self.no_volume) as f64
    }
}

/// Parse strike price from market title
/// Examples:
/// - "BTC above $108,500?" -> 108500.0
/// - "ETH above $4,050?" -> 4050.0
fn parse_strike_from_title(title: &str) -> Option<f64> {
    // Look for dollar amount pattern
    let dollar_idx = title.find('$')?;
    let after_dollar = &title[dollar_idx + 1..];

    // Extract numeric portion (including commas and dots)
    let num_str: String = after_dollar
        .chars()
        .take_while(|c| c.is_ascii_digit() || *c == ',' || *c == '.')
        .filter(|c| *c != ',')
        .collect();

    num_str.parse().ok()
}

/// Determine if market resolved YES or NO
fn get_resolution(market: &Market) -> Option<bool> {
    match market.result.as_deref() {
        Some("yes") => Some(true),
        Some("no") => Some(false),
        _ => None,
    }
}

// === Main ===

#[derive(Parser, Debug)]
#[command(name = "resolution_analysis")]
#[command(about = "Analyze Kalshi crypto market resolution rates (YES vs NO)")]
struct Args {
    /// Series to analyze (can specify multiple, e.g., -s KXBTC15M -s KXETH15M)
    #[arg(short, long)]
    series: Vec<String>,

    /// Max events to fetch per series
    #[arg(short, long, default_value = "1000")]
    limit: u32,

    /// Export raw market data to CSV file
    #[arg(long)]
    csv: Option<String>,
}

#[tokio::main]
async fn main() -> Result<()> {
    let mut args = Args::parse();

    // Default to both series if none specified
    if args.series.is_empty() {
        args.series = vec![BTC_SERIES.to_string(), ETH_SERIES.to_string()];
    }

    println!("╔════════════════════════════════════════════════════════════════════════════╗");
    println!("║           KALSHI CRYPTO MARKET RESOLUTION ANALYSIS                         ║");
    println!("╠════════════════════════════════════════════════════════════════════════════╣");
    println!("║ Analyzing settled markets to determine YES/NO resolution rates             ║");
    println!("╚════════════════════════════════════════════════════════════════════════════╝");
    println!();

    // Load Kalshi credentials
    let config = KalshiConfig::from_env().context("Failed to load Kalshi credentials")?;
    let client = AnalysisClient::new(config);

    let mut overall_stats = ResolutionStats::default();
    let mut series_stats: HashMap<String, ResolutionStats> = HashMap::new();

    // Strike price buckets (relative to hypothetical spot)
    // For crypto, we'll track absolute strike values
    let mut strike_resolution: HashMap<String, (u32, u32)> = HashMap::new(); // strike_bucket -> (yes_count, total)

    // Collect all markets for CSV export
    let mut all_markets: Vec<Market> = Vec::new();

    for series in &args.series {
        println!("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");
        println!("Analyzing series: {}", series);
        println!("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");

        // Fetch settled events
        println!("Fetching settled events...");
        let events = client.get_all_events(series, "settled", args.limit).await?;
        println!("Found {} settled events", events.len());

        let mut stats = ResolutionStats::default();
        let mut processed = 0;

        for event in &events {
            let markets = client.get_markets(&event.event_ticker).await?;

            for market in markets {
                // Markets can be "settled" or "finalized"
                if market.status != "settled" && market.status != "finalized" {
                    continue;
                }

                // Collect for CSV export
                all_markets.push(market.clone());

                stats.total_markets += 1;
                overall_stats.total_markets += 1;

                let volume = market.volume.unwrap_or(0);
                stats.total_volume += volume;
                overall_stats.total_volume += volume;

                match get_resolution(&market) {
                    Some(true) => {
                        stats.yes_count += 1;
                        stats.yes_volume += volume;
                        overall_stats.yes_count += 1;
                        overall_stats.yes_volume += volume;

                        // Track by strike
                        if let Some(strike) = parse_strike_from_title(&market.title) {
                            let bucket = format_strike_bucket(series, strike);
                            let entry = strike_resolution.entry(bucket).or_insert((0, 0));
                            entry.0 += 1;
                            entry.1 += 1;
                        }
                    }
                    Some(false) => {
                        stats.no_count += 1;
                        stats.no_volume += volume;
                        overall_stats.no_count += 1;
                        overall_stats.no_volume += volume;

                        if let Some(strike) = parse_strike_from_title(&market.title) {
                            let bucket = format_strike_bucket(series, strike);
                            let entry = strike_resolution.entry(bucket).or_insert((0, 0));
                            entry.1 += 1;
                        }
                    }
                    None => {
                        stats.unknown_count += 1;
                        overall_stats.unknown_count += 1;
                    }
                }
            }

            processed += 1;
            if processed % 10 == 0 {
                eprint!("\r  Processed {}/{} events...", processed, events.len());
            }
        }
        eprintln!();

        series_stats.insert(series.clone(), stats);
    }

    // Print results
    println!();
    println!("╔════════════════════════════════════════════════════════════════════════════╗");
    println!("║                           RESULTS BY SERIES                                ║");
    println!("╚════════════════════════════════════════════════════════════════════════════╝");

    for series in &args.series {
        if let Some(stats) = series_stats.get(series) {
            println!();
            println!("┌─────────────────────────────────────────────────────────────────────────────┐");
            println!("│ Series: {:68} │", series);
            println!("├─────────────────────────────────────────────────────────────────────────────┤");
            println!("│ Total Settled Markets: {:52} │", stats.total_markets);
            println!("│ YES Resolutions:       {:52} │", stats.yes_count);
            println!("│ NO Resolutions:        {:52} │", stats.no_count);
            println!("│ Unknown/Cancelled:     {:52} │", stats.unknown_count);
            println!("├─────────────────────────────────────────────────────────────────────────────┤");
            println!("│ YES Rate:              {:51.2}% │", stats.yes_rate() * 100.0);
            println!("│ NO Rate:               {:51.2}% │", stats.no_rate() * 100.0);
            println!("│ Deviation from 50%:    {:51.2}% │", stats.deviation_from_50() * 100.0);
            println!("├─────────────────────────────────────────────────────────────────────────────┤");
            println!("│ Total Volume:          {:48} ct │", stats.total_volume);
            println!("│ Volume-Weighted YES:   {:51.2}% │", stats.volume_weighted_yes_rate() * 100.0);
            println!("└─────────────────────────────────────────────────────────────────────────────┘");
        }
    }

    // Overall stats
    println!();
    println!("╔════════════════════════════════════════════════════════════════════════════╗");
    println!("║                          OVERALL STATISTICS                                ║");
    println!("╠════════════════════════════════════════════════════════════════════════════╣");
    println!("║ Total Settled Markets: {:52} ║", overall_stats.total_markets);
    println!("║ YES Resolutions:       {:52} ║", overall_stats.yes_count);
    println!("║ NO Resolutions:        {:52} ║", overall_stats.no_count);
    println!("║ Unknown/Cancelled:     {:52} ║", overall_stats.unknown_count);
    println!("╠════════════════════════════════════════════════════════════════════════════╣");
    println!("║ YES Rate:              {:51.2}% ║", overall_stats.yes_rate() * 100.0);
    println!("║ NO Rate:               {:51.2}% ║", overall_stats.no_rate() * 100.0);
    println!("║ DEVIATION FROM 50%:    {:51.2}% ║", overall_stats.deviation_from_50() * 100.0);
    println!("╠════════════════════════════════════════════════════════════════════════════╣");
    println!("║ Total Volume:          {:48} ct ║", overall_stats.total_volume);
    println!("║ Volume-Weighted YES:   {:51.2}% ║", overall_stats.volume_weighted_yes_rate() * 100.0);
    println!("╚════════════════════════════════════════════════════════════════════════════╝");

    // Statistical significance test
    println!();
    println!("╔════════════════════════════════════════════════════════════════════════════╗");
    println!("║                       STATISTICAL ANALYSIS                                 ║");
    println!("╚════════════════════════════════════════════════════════════════════════════╝");

    let n = overall_stats.yes_count + overall_stats.no_count;
    if n > 0 {
        // Binomial test: is YES rate significantly different from 50%?
        let p_hat = overall_stats.yes_rate();
        let se = (0.5 * 0.5 / n as f64).sqrt();
        let z_score = (p_hat - 0.5) / se;

        // Two-tailed p-value approximation
        let p_value = 2.0 * (1.0 - normal_cdf(z_score.abs()));

        println!();
        println!("Hypothesis Test: Is YES rate significantly different from 50%?");
        println!("───────────────────────────────────────────────────────────────────────────────");
        println!("  Sample size (n):     {}", n);
        println!("  Observed YES rate:   {:.4}", p_hat);
        println!("  Expected (H0):       0.5000");
        println!("  Standard Error:      {:.4}", se);
        println!("  Z-score:             {:.4}", z_score);
        println!("  P-value (two-tail):  {:.6}", p_value);
        println!();

        if p_value < 0.05 {
            println!("  RESULT: STATISTICALLY SIGNIFICANT (p < 0.05)");
            println!("  The YES/NO resolution rate is significantly different from 50%.");
            if p_hat > 0.5 {
                println!("  BIAS: Markets tend to resolve YES more often.");
            } else {
                println!("  BIAS: Markets tend to resolve NO more often.");
            }
        } else {
            println!("  RESULT: NOT STATISTICALLY SIGNIFICANT (p >= 0.05)");
            println!("  The YES/NO resolution rate is not significantly different from 50%.");
            println!("  This is consistent with efficient pricing of ATM binary options.");
        }

        // 95% confidence interval
        let ci_lower = p_hat - 1.96 * se;
        let ci_upper = p_hat + 1.96 * se;
        println!();
        println!("  95% Confidence Interval: [{:.4}, {:.4}]", ci_lower, ci_upper);

        // Expected value analysis for trading
        println!();
        println!("───────────────────────────────────────────────────────────────────────────────");
        println!("TRADING IMPLICATIONS:");
        println!("───────────────────────────────────────────────────────────────────────────────");

        let yes_ev_at_50 = (p_hat * 100.0) - 50.0;
        let no_ev_at_50 = ((1.0 - p_hat) * 100.0) - 50.0;

        println!("  If you always buy YES at 50c:");
        println!("    Expected Value: {:+.2}c per contract", yes_ev_at_50);
        println!("    (Pays $1 with prob {:.2}%, costs 50c)", p_hat * 100.0);

        println!();
        println!("  If you always buy NO at 50c:");
        println!("    Expected Value: {:+.2}c per contract", no_ev_at_50);
        println!("    (Pays $1 with prob {:.2}%, costs 50c)", (1.0 - p_hat) * 100.0);

        // Break-even prices
        let breakeven_yes = p_hat * 100.0;
        let breakeven_no = (1.0 - p_hat) * 100.0;

        println!();
        println!("  Fair prices (excluding fees):");
        println!("    YES fair value: {:.1}c", breakeven_yes);
        println!("    NO fair value:  {:.1}c", breakeven_no);
    }

    // Strike bucket analysis if we have data
    if !strike_resolution.is_empty() {
        println!();
        println!("╔════════════════════════════════════════════════════════════════════════════╗");
        println!("║                    RESOLUTION BY STRIKE BUCKET                             ║");
        println!("╚════════════════════════════════════════════════════════════════════════════╝");
        println!();

        let mut buckets: Vec<_> = strike_resolution.iter().collect();
        buckets.sort_by_key(|(k, _)| k.clone());

        println!("{:30} {:>10} {:>10} {:>12}", "Strike Bucket", "YES", "Total", "YES Rate");
        println!("{}", "─".repeat(65));

        for (bucket, (yes, total)) in buckets {
            let rate = if *total > 0 {
                (*yes as f64 / *total as f64) * 100.0
            } else {
                0.0
            };
            println!("{:30} {:>10} {:>10} {:>11.1}%", bucket, yes, total, rate);
        }
    }

    // Export to CSV if requested
    if let Some(csv_path) = &args.csv {
        use std::io::Write;
        let mut file = std::fs::File::create(csv_path)
            .context(format!("Failed to create CSV file: {}", csv_path))?;

        // Write header
        writeln!(file, "ticker,event_ticker,title,status,result,strike,volume,close_time,expiration_time")?;

        // Write each market
        for market in &all_markets {
            let strike = parse_strike_from_title(&market.title)
                .or(market.floor_strike)
                .map(|s| format!("{:.0}", s))
                .unwrap_or_default();
            let result = market.result.as_deref().unwrap_or("");
            let volume = market.volume.unwrap_or(0);
            let close_time = market.close_time.as_deref().unwrap_or("");
            let expiration_time = market.expiration_time.as_deref().unwrap_or("");

            // Escape title (may contain commas)
            let title_escaped = market.title.replace('"', "\"\"");

            writeln!(
                file,
                "{},{},\"{}\",{},{},{},{},{},{}",
                market.ticker,
                market.event_ticker,
                title_escaped,
                market.status,
                result,
                strike,
                volume,
                close_time,
                expiration_time
            )?;
        }

        println!();
        println!("Exported {} markets to {}", all_markets.len(), csv_path);
    }

    println!();
    println!("Analysis complete.");

    Ok(())
}

/// Format strike into a bucket for grouping
fn format_strike_bucket(series: &str, strike: f64) -> String {
    if series.contains("BTC") {
        // BTC: bucket by $5000
        let bucket = ((strike / 5000.0).floor() as i64) * 5000;
        format!("BTC ${}-${}", bucket, bucket + 5000)
    } else if series.contains("ETH") {
        // ETH: bucket by $500
        let bucket = ((strike / 500.0).floor() as i64) * 500;
        format!("ETH ${}-${}", bucket, bucket + 500)
    } else {
        format!("${:.0}", strike)
    }
}

/// Standard normal CDF approximation
fn normal_cdf(x: f64) -> f64 {
    // Approximation using error function
    0.5 * (1.0 + erf(x / std::f64::consts::SQRT_2))
}

/// Error function approximation (Horner's method)
fn erf(x: f64) -> f64 {
    // Abramowitz and Stegun approximation
    let a1 = 0.254829592;
    let a2 = -0.284496736;
    let a3 = 1.421413741;
    let a4 = -1.453152027;
    let a5 = 1.061405429;
    let p = 0.3275911;

    let sign = if x < 0.0 { -1.0 } else { 1.0 };
    let x = x.abs();

    let t = 1.0 / (1.0 + p * x);
    let y = 1.0 - (((((a5 * t + a4) * t) + a3) * t + a2) * t + a1) * t * (-x * x).exp();

    sign * y
}
