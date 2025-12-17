//! Backtest Pricing Model Against Historical Kalshi Data
//!
//! Fetches settled crypto markets and compares:
//! - Actual market prices (last_price) vs theoretical fair value
//! - Shows distribution of mispricings
//! - Outputs CSV for charting
//!
//! Usage:
//!   cargo run --release --bin backtest_pricing -- [OPTIONS]
//!
//! Options:
//!   --limit <N>       Max events to fetch per series (default: 100)
//!   --vol <PCT>       Annual volatility % (default: 50)
//!   --csv             Output results as CSV
//!   --chart           Output ASCII chart of mispricing distribution

use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use arb_bot::kalshi::KalshiConfig;

const KALSHI_API_BASE: &str = "https://api.elections.kalshi.com/trade-api/v2";
const API_DELAY_MS: u64 = 100;
const BTC_SERIES: &str = "KXBTC15M";
const ETH_SERIES: &str = "KXETH15M";

// ============================================================================
// FAIR VALUE CALCULATION (copied from fair_value.rs for self-contained binary)
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

/// Calculate fair value for a binary option
fn calc_fair_value(spot: f64, strike: f64, minutes_remaining: f64, annual_vol: f64) -> (f64, f64) {
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

fn calc_fair_value_cents(spot: f64, strike: f64, minutes_remaining: f64, annual_vol: f64) -> (i64, i64) {
    let (yes_prob, no_prob) = calc_fair_value(spot, strike, minutes_remaining, annual_vol);
    let yes_cents = (yes_prob * 100.0).round() as i64;
    let no_cents = (no_prob * 100.0).round() as i64;
    (yes_cents, no_cents)
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
}

#[derive(Debug, Deserialize, Clone)]
struct Market {
    ticker: String,
    event_ticker: String,
    title: String,
    status: String,
    yes_ask: Option<i64>,
    yes_bid: Option<i64>,
    no_ask: Option<i64>,
    no_bid: Option<i64>,
    last_price: Option<i64>,
    volume: Option<i64>,
    #[serde(default)]
    result: Option<String>,
}

// ============================================================================
// API CLIENT
// ============================================================================

struct ApiClient {
    http: reqwest::Client,
    config: KalshiConfig,
}

impl ApiClient {
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

    async fn get_events(&self, series_ticker: &str, limit: u32) -> Result<Vec<Event>> {
        let mut all_events = Vec::new();
        let mut cursor: Option<String> = None;

        loop {
            let mut path = format!("/events?series_ticker={}&status=settled&limit=100", series_ticker);
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

    async fn get_markets(&self, event_ticker: &str) -> Result<Vec<Market>> {
        let path = format!("/markets?event_ticker={}", event_ticker);
        let resp: MarketsResponse = self.get(&path).await?;
        Ok(resp.markets)
    }
}

// ============================================================================
// ANALYSIS
// ============================================================================

#[derive(Debug, Clone, Serialize)]
struct MarketAnalysis {
    ticker: String,
    title: String,
    result: String,
    last_price: i64,
    /// Theoretical fair value (assuming ATM at launch, 15 min to expiry)
    fair_value_atm: i64,
    /// Difference: last_price - fair_value
    mispricing: i64,
    volume: i64,
    /// Did the market resolve as expected (YES if price > 50, NO if < 50)?
    correct_side: bool,
}

/// Print ASCII histogram of mispricing distribution
fn print_histogram(mispricings: &[i64]) {
    if mispricings.is_empty() {
        println!("No data to chart.");
        return;
    }

    // Create buckets: -50 to +50 in steps of 5
    let mut buckets: HashMap<i64, u32> = HashMap::new();
    for i in -10..=10 {
        buckets.insert(i * 5, 0);
    }

    for &m in mispricings {
        let bucket = ((m as f64 / 5.0).round() as i64) * 5;
        let bucket = bucket.clamp(-50, 50);
        *buckets.entry(bucket).or_insert(0) += 1;
    }

    let max_count = buckets.values().max().copied().unwrap_or(1) as f64;

    println!();
    println!("Mispricing Distribution (Last Price - Fair Value)");
    println!("══════════════════════════════════════════════════════════════════════════════");
    println!();

    for i in -10..=10 {
        let bucket = i * 5;
        let count = buckets.get(&bucket).copied().unwrap_or(0);
        let bar_len = ((count as f64 / max_count) * 50.0).round() as usize;
        let bar = "█".repeat(bar_len);

        let label = if bucket == 0 {
            format!("{:>4}¢ │", bucket)
        } else if bucket > 0 {
            format!("{:>+4}¢ │", bucket)
        } else {
            format!("{:>4}¢ │", bucket)
        };

        println!("{} {} ({})", label, bar, count);
    }

    println!();
    println!("Legend: Negative = Market underpriced YES, Positive = Market overpriced YES");
}

/// Print summary statistics
fn print_summary(analyses: &[MarketAnalysis]) {
    if analyses.is_empty() {
        println!("No data to analyze.");
        return;
    }

    let mispricings: Vec<i64> = analyses.iter().map(|a| a.mispricing).collect();
    let volumes: Vec<i64> = analyses.iter().map(|a| a.volume).collect();

    let n = mispricings.len() as f64;
    let mean: f64 = mispricings.iter().sum::<i64>() as f64 / n;
    let variance: f64 = mispricings.iter().map(|&x| (x as f64 - mean).powi(2)).sum::<f64>() / n;
    let std_dev = variance.sqrt();

    let mut sorted = mispricings.clone();
    sorted.sort();
    let median = if sorted.len() % 2 == 0 {
        (sorted[sorted.len() / 2 - 1] + sorted[sorted.len() / 2]) as f64 / 2.0
    } else {
        sorted[sorted.len() / 2] as f64
    };

    let min = sorted.first().copied().unwrap_or(0);
    let max = sorted.last().copied().unwrap_or(0);

    // Count underpriced vs overpriced
    let underpriced = mispricings.iter().filter(|&&m| m < -5).count();
    let overpriced = mispricings.iter().filter(|&&m| m > 5).count();
    let fair = mispricings.iter().filter(|&&m| m.abs() <= 5).count();

    // Count correct predictions
    let correct = analyses.iter().filter(|a| a.correct_side).count();
    let correct_pct = (correct as f64 / analyses.len() as f64) * 100.0;

    // Volume-weighted mispricing
    let total_volume: i64 = volumes.iter().sum();
    let vol_weighted_mispricing: f64 = analyses
        .iter()
        .map(|a| a.mispricing as f64 * a.volume as f64)
        .sum::<f64>() / total_volume.max(1) as f64;

    println!();
    println!("╔══════════════════════════════════════════════════════════════════════════════╗");
    println!("║                     PRICING MODEL BACKTEST RESULTS                           ║");
    println!("╠══════════════════════════════════════════════════════════════════════════════╣");
    println!("║ Sample Size:           {:>8} markets                                       ║", analyses.len());
    println!("║ Total Volume:          {:>8} contracts                                     ║", total_volume);
    println!("╠══════════════════════════════════════════════════════════════════════════════╣");
    println!("║ MISPRICING STATISTICS (Last Price - ATM Fair Value):                         ║");
    println!("║   Mean:                {:>+8.2}¢                                              ║", mean);
    println!("║   Median:              {:>+8.2}¢                                              ║", median);
    println!("║   Std Dev:             {:>8.2}¢                                              ║", std_dev);
    println!("║   Min:                 {:>+8}¢                                              ║", min);
    println!("║   Max:                 {:>+8}¢                                              ║", max);
    println!("║   Vol-Weighted Mean:   {:>+8.2}¢                                              ║", vol_weighted_mispricing);
    println!("╠══════════════════════════════════════════════════════════════════════════════╣");
    println!("║ DISTRIBUTION:                                                                ║");
    println!("║   Underpriced (< -5¢): {:>8} ({:>5.1}%)                                      ║", underpriced, underpriced as f64 / n * 100.0);
    println!("║   Fair (±5¢):          {:>8} ({:>5.1}%)                                      ║", fair, fair as f64 / n * 100.0);
    println!("║   Overpriced (> +5¢):  {:>8} ({:>5.1}%)                                      ║", overpriced, overpriced as f64 / n * 100.0);
    println!("╠══════════════════════════════════════════════════════════════════════════════╣");
    println!("║ PREDICTION ACCURACY:                                                         ║");
    println!("║   Last price correctly predicted result: {:>5.1}%                             ║", correct_pct);
    println!("║   (YES if price > 50¢, NO if price ≤ 50¢)                                    ║");
    println!("╚══════════════════════════════════════════════════════════════════════════════╝");

    // Trading implications
    println!();
    println!("TRADING IMPLICATIONS:");
    println!("─────────────────────────────────────────────────────────────────────────────────");

    if mean.abs() > 2.0 {
        if mean > 0.0 {
            println!("  Markets are SYSTEMATICALLY OVERPRICED by ~{:.1}¢ on average.", mean);
            println!("  Strategy: Favor selling YES / buying NO when possible.");
        } else {
            println!("  Markets are SYSTEMATICALLY UNDERPRICED by ~{:.1}¢ on average.", mean.abs());
            println!("  Strategy: Favor buying YES when possible.");
        }
    } else {
        println!("  Markets are EFFICIENTLY PRICED (mean mispricing within ±2¢).");
        println!("  No systematic edge from simple directional bets.");
    }

    if std_dev > 20.0 {
        println!();
        println!("  HIGH VARIANCE ({:.1}¢) suggests frequent mispricings.", std_dev);
        println!("  Opportunity: Look for outliers > 2σ from mean for trades.");
    }
}

/// Print CSV output
fn print_csv(analyses: &[MarketAnalysis]) {
    println!("ticker,title,result,last_price,fair_value_atm,mispricing,volume,correct_side");
    for a in analyses {
        println!(
            "{},\"{}\",{},{},{},{},{},{}",
            a.ticker,
            a.title.replace('"', "\"\""),
            a.result,
            a.last_price,
            a.fair_value_atm,
            a.mispricing,
            a.volume,
            a.correct_side
        );
    }
}

// ============================================================================
// MAIN
// ============================================================================

#[derive(Debug)]
struct Args {
    limit: u32,
    vol: f64,
    csv: bool,
    chart: bool,
}

fn parse_args() -> Args {
    let args: Vec<String> = std::env::args().collect();
    let mut i = 1;

    let mut limit = 100u32;
    let mut vol = 0.50;
    let mut csv = false;
    let mut chart = false;

    while i < args.len() {
        match args[i].as_str() {
            "--limit" | "-l" => {
                i += 1;
                if i < args.len() {
                    limit = args[i].parse().unwrap_or(100);
                }
            }
            "--vol" | "-v" => {
                i += 1;
                if i < args.len() {
                    vol = args[i].parse::<f64>().unwrap_or(50.0) / 100.0;
                }
            }
            "--csv" => {
                csv = true;
            }
            "--chart" => {
                chart = true;
            }
            "--help" | "-h" => {
                println!("Usage: backtest_pricing [OPTIONS]");
                println!("  --limit, -l <N>   Max events per series (default: 100)");
                println!("  --vol, -v <PCT>   Annual volatility % (default: 50)");
                println!("  --csv             Output as CSV");
                println!("  --chart           Show ASCII histogram");
                std::process::exit(0);
            }
            _ => {}
        }
        i += 1;
    }

    if !csv && !chart {
        chart = true; // Default to showing chart
    }

    Args { limit, vol, csv, chart }
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = parse_args();

    if !args.csv {
        eprintln!("╔══════════════════════════════════════════════════════════════════════════════╗");
        eprintln!("║                    PRICING MODEL BACKTEST                                    ║");
        eprintln!("╠══════════════════════════════════════════════════════════════════════════════╣");
        eprintln!("║ Comparing actual market prices vs theoretical ATM fair value                 ║");
        eprintln!("║ Assumption: At launch, spot ≈ strike (ATM), so fair value ≈ 50¢             ║");
        eprintln!("╚══════════════════════════════════════════════════════════════════════════════╝");
        eprintln!();
    }

    let config = KalshiConfig::from_env().context("Failed to load Kalshi credentials")?;
    let client = ApiClient::new(config);

    let mut all_analyses = Vec::new();

    for series in [BTC_SERIES, ETH_SERIES] {
        if !args.csv {
            eprintln!("Analyzing series: {}", series);
        }

        let events = client.get_events(series, args.limit).await?;
        if !args.csv {
            eprintln!("Found {} events", events.len());
        }

        let mut processed = 0;
        for event in &events {
            let markets = client.get_markets(&event.event_ticker).await?;

            for market in markets {
                if market.status != "settled" && market.status != "finalized" {
                    continue;
                }

                let result = market.result.as_deref().unwrap_or("unknown");
                if result == "unknown" {
                    continue;
                }

                let last_price = market.last_price.unwrap_or(50);
                let volume = market.volume.unwrap_or(0);

                // For these "BTC price up in next 15 mins?" markets,
                // ATM fair value is approximately 50¢
                let fair_value_atm = 50;

                let mispricing = last_price - fair_value_atm;

                // Did the last price correctly predict the outcome?
                let predicted_yes = last_price > 50;
                let actual_yes = result == "yes";
                let correct_side = predicted_yes == actual_yes;

                all_analyses.push(MarketAnalysis {
                    ticker: market.ticker.clone(),
                    title: market.title.clone(),
                    result: result.to_string(),
                    last_price,
                    fair_value_atm,
                    mispricing,
                    volume,
                    correct_side,
                });
            }

            processed += 1;
            if !args.csv && processed % 10 == 0 {
                eprint!("\r  Processed {}/{} events...", processed, events.len());
            }
        }
        if !args.csv {
            eprintln!();
        }
    }

    if args.csv {
        print_csv(&all_analyses);
    } else {
        print_summary(&all_analyses);

        if args.chart {
            let mispricings: Vec<i64> = all_analyses.iter().map(|a| a.mispricing).collect();
            print_histogram(&mispricings);
        }
    }

    Ok(())
}
