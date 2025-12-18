//! Backtest Fair Value Model Against Historical Trades
//!
//! Fetches historical trades for BTC/ETH 15-minute markets and compares:
//! - Actual trade prices vs calculated fair value at time of trade
//! - Uses real BTC/ETH prices from Polygon.io to calculate true fair value
//!
//! For "BTC price up/down by $X in 15 mins?" markets:
//! - Strike = opening price ± offset (parsed from ticker)
//! - Fair value calculated using Black-Scholes with real spot price
//!
//! Usage:
//!   cargo run --release --bin backtest_trades -- [OPTIONS]

use anyhow::{Context, Result};
use chrono::{DateTime, Utc, Duration as ChronoDuration};
use clap::Parser;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use arb_bot::kalshi::KalshiConfig;

const KALSHI_API_BASE: &str = "https://api.elections.kalshi.com/trade-api/v2";
const POLYGON_API_BASE: &str = "https://api.polygon.io";
const POLYGON_API_KEY: &str = "o2Jm26X52_0tRq2W7V5JbsCUXdMjL7qk";
const API_DELAY_MS: u64 = 100;

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

/// Calculate fair value for binary option: P(S_T > K)
/// spot: current price
/// strike: strike price
/// minutes_remaining: time until expiry
/// annual_vol: annualized volatility (e.g., 0.50 for 50%)
/// is_above: true for "above" markets, false for "below"
fn calc_fair_value(spot: f64, strike: f64, minutes_remaining: f64, annual_vol: f64, is_above: bool) -> i64 {
    if minutes_remaining <= 0.0 {
        // At expiry
        return if (is_above && spot > strike) || (!is_above && spot < strike) {
            100
        } else {
            0
        };
    }

    let time_years = minutes_remaining / 525960.0;
    let sqrt_t = time_years.sqrt();
    let log_ratio = (spot / strike).ln();

    // d2 = [ln(S/K) - σ²T/2] / (σ√T)
    let d2 = (log_ratio - 0.5 * annual_vol.powi(2) * time_years) / (annual_vol * sqrt_t);

    // P(S_T > K) = N(d2) for above markets
    let prob = if is_above {
        norm_cdf(d2)
    } else {
        1.0 - norm_cdf(d2)
    };

    (prob * 100.0).round() as i64
}

/// For ATM binary option (fallback when we don't have spot price)
fn calc_fair_value_atm(minutes_remaining: f64, annual_vol: f64) -> i64 {
    if minutes_remaining <= 0.0 {
        return 50;
    }

    let time_years = minutes_remaining / 525960.0;
    let sqrt_t = time_years.sqrt();
    let d2 = -0.5 * annual_vol * sqrt_t;
    let yes_prob = norm_cdf(d2);
    (yes_prob * 100.0).round() as i64
}

// ============================================================================
// API TYPES
// ============================================================================

#[derive(Debug, Deserialize)]
struct TradesResponse {
    trades: Vec<Trade>,
    #[serde(default)]
    cursor: Option<String>,
}

#[derive(Debug, Deserialize, Clone)]
struct Trade {
    trade_id: String,
    ticker: String,
    count: i64,
    yes_price: i64,
    no_price: i64,
    taker_side: Option<String>,
    created_time: String,
}

// Polygon.io types for aggregates (bars)
#[derive(Debug, Deserialize)]
struct PolygonAggResponse {
    #[serde(default)]
    results: Vec<PolygonBar>,
    status: Option<String>,
    #[serde(default)]
    results_count: usize,
}

#[derive(Debug, Deserialize, Clone)]
struct PolygonBar {
    t: i64,    // timestamp (ms)
    o: f64,    // open
    h: f64,    // high
    l: f64,    // low
    c: f64,    // close
    v: f64,    // volume
}

// ============================================================================
// API CLIENT
// ============================================================================

struct ApiClient {
    http: reqwest::Client,
    kalshi_config: KalshiConfig,
    polygon_key: String,
}

impl ApiClient {
    fn new(kalshi_config: KalshiConfig, polygon_key: String) -> Self {
        Self {
            http: reqwest::Client::builder()
                .timeout(Duration::from_secs(30))
                .build()
                .expect("Failed to build HTTP client"),
            kalshi_config,
            polygon_key,
        }
    }

    async fn kalshi_get<T: serde::de::DeserializeOwned>(&self, path: &str) -> Result<T> {
        let mut retries = 0;
        const MAX_RETRIES: u32 = 5;

        loop {
            let url = format!("{}{}", KALSHI_API_BASE, path);
            let timestamp_ms = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_millis() as u64;

            let full_path = format!("/trade-api/v2{}", path);
            let signature = self.kalshi_config.sign(&format!("{}GET{}", timestamp_ms, full_path))?;

            let resp = self.http
                .get(&url)
                .header("KALSHI-ACCESS-KEY", &self.kalshi_config.api_key_id)
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

    async fn get_trades(&self, limit: u32, cursor: Option<&str>) -> Result<TradesResponse> {
        let mut path = format!("/markets/trades?limit={}", limit);
        if let Some(c) = cursor {
            path.push_str(&format!("&cursor={}", c));
        }
        self.kalshi_get(&path).await
    }

    async fn get_all_trades(&self, limit: u32) -> Result<Vec<Trade>> {
        let mut all_trades = Vec::new();
        let mut cursor: Option<String> = None;

        loop {
            let resp = self.get_trades(limit.min(1000), cursor.as_deref()).await?;
            let count = resp.trades.len();
            all_trades.extend(resp.trades);

            eprint!("\r  Fetched {} trades...", all_trades.len());

            if resp.cursor.is_none() || count == 0 || all_trades.len() >= limit as usize {
                break;
            }
            cursor = resp.cursor;
        }
        eprintln!();

        all_trades.truncate(limit as usize);
        Ok(all_trades)
    }

    /// Fetch crypto price bars from Polygon.io
    /// Returns minute bars for a time range
    async fn get_crypto_bars(
        &self,
        symbol: &str,  // "X:BTCUSD" or "X:ETHUSD"
        from_ts: i64,  // Unix timestamp in ms
        to_ts: i64,    // Unix timestamp in ms
    ) -> Result<Vec<PolygonBar>> {
        let url = format!(
            "{}/v2/aggs/ticker/{}/range/1/minute/{}/{}?adjusted=true&sort=asc&apiKey={}",
            POLYGON_API_BASE, symbol, from_ts, to_ts, self.polygon_key
        );

        let resp = self.http.get(&url).send().await?;
        let status = resp.status();

        if !status.is_success() {
            let body = resp.text().await.unwrap_or_default();
            anyhow::bail!("Polygon API error {}: {}", status, body);
        }

        let data: PolygonAggResponse = resp.json().await?;
        tokio::time::sleep(Duration::from_millis(50)).await; // Rate limit
        Ok(data.results)
    }

    /// Get opening price at a specific timestamp (finds nearest bar)
    fn find_price_at_time(&self, bars: &[PolygonBar], target_ts_ms: i64) -> Option<f64> {
        // Find the bar closest to but not after the target time
        let mut best: Option<&PolygonBar> = None;
        for bar in bars {
            if bar.t <= target_ts_ms {
                best = Some(bar);
            } else {
                break;
            }
        }
        best.map(|b| b.c) // Use close price
    }
}

// ============================================================================
// TICKER PARSING
// ============================================================================

/// Parsed market info from ticker
#[derive(Debug, Clone)]
struct MarketInfo {
    asset: String,           // "BTC" or "ETH"
    close_time: DateTime<Utc>,
    open_time: DateTime<Utc>,
    strike_offset: i64,      // The offset from opening price (e.g., +30 or -30)
    is_above: bool,          // true = "above" market, false = "below"
}

/// Parse market info from ticker
/// Format examples:
///   KXBTC15M-25DEC171030-B30   -> BTC above +30 (price > open + 30)
///   KXBTC15M-25DEC171030-30    -> BTC above +30 (default is above)
///   KXBTC15M-25DEC171030-T7700 -> BTC within ±7700
fn parse_market_info(ticker: &str) -> Option<MarketInfo> {
    let parts: Vec<&str> = ticker.split('-').collect();
    if parts.len() < 3 {
        return None;
    }

    // Parse asset type
    let asset = if ticker.starts_with("KXBTC15M") {
        "BTC"
    } else if ticker.starts_with("KXETH15M") {
        "ETH"
    } else {
        return None;
    };

    // Parse date/time
    let date_time_part = parts[1];
    if date_time_part.len() < 11 {
        return None;
    }

    let year_suffix = &date_time_part[0..2];
    let month = &date_time_part[2..5];
    let day = &date_time_part[5..7];
    let hour = &date_time_part[7..9];
    let minute = &date_time_part[9..11];

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

    // EST is UTC-5
    let close_time = chrono::NaiveDate::from_ymd_opt(year, month_num, day)
        .and_then(|d| d.and_hms_opt(hour + 5, minute, 0))
        .map(|dt| DateTime::<Utc>::from_naive_utc_and_offset(dt, Utc))?;

    // Open time is 15 minutes before close
    let open_time = close_time - ChronoDuration::minutes(15);

    // Parse strike offset from last part
    let strike_part = parts[2];

    // Determine market type and offset
    let (is_above, strike_offset) = if strike_part.starts_with('B') {
        // "B30" = above +30
        let offset: i64 = strike_part[1..].parse().ok()?;
        (true, offset)
    } else if strike_part.starts_with('T') {
        // "T7700" = within range, skip for now
        return None;
    } else {
        // Plain number like "30" = above +30
        let offset: i64 = strike_part.parse().ok()?;
        (true, offset)
    };

    Some(MarketInfo {
        asset: asset.to_string(),
        close_time,
        open_time,
        strike_offset,
        is_above,
    })
}

/// Check if ticker is a BTC or ETH 15-minute market
fn is_crypto_15m(ticker: &str) -> bool {
    ticker.starts_with("KXBTC15M") || ticker.starts_with("KXETH15M")
}

// ============================================================================
// ANALYSIS
// ============================================================================

#[derive(Debug, Clone, Serialize)]
struct TradeAnalysis {
    ticker: String,
    trade_time: String,
    close_time: String,
    minutes_remaining: f64,
    yes_price: i64,
    fair_value: i64,
    fair_value_atm: i64,
    mispricing: i64,      // yes_price - fair_value
    mispricing_atm: i64,  // yes_price - fair_value_atm
    spot_price: f64,
    opening_price: f64,
    strike: f64,
    closing_price: f64,   // Price at market close (for outcome)
    resolved_yes: bool,   // Did the market resolve YES?
    taker_side: String,
    count: i64,
}

/// Market-level analysis with outcome
#[derive(Debug, Clone)]
struct MarketAnalysis {
    ticker: String,
    asset: String,
    open_time: DateTime<Utc>,
    close_time: DateTime<Utc>,
    opening_price: f64,
    closing_price: f64,
    strike: f64,
    strike_offset: i64,
    resolved_yes: bool,
    trades: Vec<TradeAnalysis>,
}

/// Prepare trade for analysis (without price lookup - just parsing)
struct PendingTrade {
    trade: Trade,
    market_info: MarketInfo,
    trade_time: DateTime<Utc>,
    minutes_remaining: f64,
}

fn prepare_trade(trade: &Trade) -> Option<PendingTrade> {
    if !is_crypto_15m(&trade.ticker) {
        return None;
    }

    let market_info = parse_market_info(&trade.ticker)?;
    let trade_time = DateTime::parse_from_rfc3339(&trade.created_time)
        .ok()?
        .with_timezone(&Utc);

    let duration = market_info.close_time.signed_duration_since(trade_time);
    let minutes_remaining = (duration.num_seconds() as f64 / 60.0).max(0.0);

    // Skip trades after close
    if minutes_remaining <= 0.0 {
        return None;
    }

    Some(PendingTrade {
        trade: trade.clone(),
        market_info,
        trade_time,
        minutes_remaining,
    })
}

fn analyze_trade_with_prices(
    pending: &PendingTrade,
    opening_price: f64,
    spot_price: f64,
    closing_price: f64,
    vol: f64,
) -> TradeAnalysis {
    let strike = opening_price + pending.market_info.strike_offset as f64;

    let fair_value = calc_fair_value(
        spot_price,
        strike,
        pending.minutes_remaining,
        vol,
        pending.market_info.is_above,
    );
    let fair_value_atm = calc_fair_value_atm(pending.minutes_remaining, vol);

    let mispricing = pending.trade.yes_price - fair_value;
    let mispricing_atm = pending.trade.yes_price - fair_value_atm;

    // Determine actual outcome
    let resolved_yes = if pending.market_info.is_above {
        closing_price > strike
    } else {
        closing_price < strike
    };

    TradeAnalysis {
        ticker: pending.trade.ticker.clone(),
        trade_time: pending.trade_time.to_rfc3339(),
        close_time: pending.market_info.close_time.to_rfc3339(),
        minutes_remaining: pending.minutes_remaining,
        yes_price: pending.trade.yes_price,
        fair_value,
        fair_value_atm,
        mispricing,
        mispricing_atm,
        spot_price,
        opening_price,
        strike,
        closing_price,
        resolved_yes,
        taker_side: pending.trade.taker_side.clone().unwrap_or_default(),
        count: pending.trade.count,
    }
}

fn calc_stats(values: &[i64]) -> (f64, f64, f64, i64, i64) {
    if values.is_empty() {
        return (0.0, 0.0, 0.0, 0, 0);
    }
    let n = values.len() as f64;
    let mean = values.iter().sum::<i64>() as f64 / n;
    let variance = values.iter().map(|&x| (x as f64 - mean).powi(2)).sum::<f64>() / n;
    let std_dev = variance.sqrt();
    let mut sorted = values.to_vec();
    sorted.sort();
    let median = if sorted.len() % 2 == 0 {
        (sorted[sorted.len() / 2 - 1] + sorted[sorted.len() / 2]) / 2
    } else {
        sorted[sorted.len() / 2]
    };
    let min = *sorted.first().unwrap_or(&0);
    let max = *sorted.last().unwrap_or(&0);
    (mean, std_dev, median as f64, min, max)
}

fn calc_stats_f64(values: &[f64]) -> (f64, f64, f64, f64, f64) {
    if values.is_empty() {
        return (0.0, 0.0, 0.0, 0.0, 0.0);
    }
    let n = values.len() as f64;
    let mean = values.iter().sum::<f64>() / n;
    let variance = values.iter().map(|&x| (x - mean).powi(2)).sum::<f64>() / n;
    let std_dev = variance.sqrt();
    let mut sorted = values.to_vec();
    sorted.sort_by(|a, b| a.partial_cmp(b).unwrap());
    let median = if sorted.len() % 2 == 0 {
        (sorted[sorted.len() / 2 - 1] + sorted[sorted.len() / 2]) / 2.0
    } else {
        sorted[sorted.len() / 2]
    };
    let min = *sorted.first().unwrap_or(&0.0);
    let max = *sorted.last().unwrap_or(&0.0);
    (mean, std_dev, median, min, max)
}

fn print_summary(analyses: &[TradeAnalysis], markets: &[MarketAnalysis]) {
    if analyses.is_empty() {
        println!("No trades to analyze.");
        return;
    }

    let mispricings: Vec<i64> = analyses.iter().map(|a| a.mispricing).collect();
    let mispricings_atm: Vec<i64> = analyses.iter().map(|a| a.mispricing_atm).collect();

    let (mean, std_dev, median, min, max) = calc_stats(&mispricings);
    let (mean_atm, std_atm, median_atm, min_atm, max_atm) = calc_stats(&mispricings_atm);

    // =========================================================================
    // SECTION 1: BASIC MISPRICING STATISTICS
    // =========================================================================
    println!();
    println!("╔═══════════════════════════════════════════════════════════════════════════════════════╗");
    println!("║              COMPREHENSIVE FAIR VALUE BACKTEST ANALYSIS                               ║");
    println!("╠═══════════════════════════════════════════════════════════════════════════════════════╣");
    println!("║ Real Model: Black-Scholes with actual BTC/ETH spot prices from Polygon.io            ║");
    println!("║ ATM Model:  Assumes fair value ≈ 50¢ (at-the-money approximation)                    ║");
    println!("╚═══════════════════════════════════════════════════════════════════════════════════════╝");

    println!();
    println!("┌─────────────────────────────────────────────────────────────────────────────────────┐");
    println!("│ 1. MISPRICING STATISTICS ({} trades across {} markets)                         │",
             analyses.len(), markets.len());
    println!("└─────────────────────────────────────────────────────────────────────────────────────┘");
    println!();
    println!("Mispricing = Trade Price - Model Fair Value");
    println!("  Positive = Market OVERPRICED YES (edge: sell YES / buy NO)");
    println!("  Negative = Market UNDERPRICED YES (edge: buy YES)");
    println!();
    println!("                         REAL PRICES         ATM MODEL");
    println!("  Mean Mispricing:     {:>+8.2}¢          {:>+8.2}¢", mean, mean_atm);
    println!("  Median Mispricing:   {:>+8.2}¢          {:>+8.2}¢", median, median_atm);
    println!("  Std Dev:             {:>8.2}¢          {:>8.2}¢", std_dev, std_atm);
    println!("  Min:                 {:>+8}¢          {:>+8}¢", min, min_atm);
    println!("  Max:                 {:>+8}¢          {:>+8}¢", max, max_atm);

    // =========================================================================
    // SECTION 2: OUTCOME ANALYSIS - How did markets actually resolve?
    // =========================================================================
    println!();
    println!("┌─────────────────────────────────────────────────────────────────────────────────────┐");
    println!("│ 2. MARKET OUTCOME ANALYSIS                                                          │");
    println!("└─────────────────────────────────────────────────────────────────────────────────────┘");

    let yes_count = markets.iter().filter(|m| m.resolved_yes).count();
    let no_count = markets.len() - yes_count;
    let yes_pct = 100.0 * yes_count as f64 / markets.len() as f64;

    println!();
    println!("  Markets resolved YES: {} ({:.1}%)", yes_count, yes_pct);
    println!("  Markets resolved NO:  {} ({:.1}%)", no_count, 100.0 - yes_pct);
    println!();

    // Show markets with outcomes
    println!("  Market Details:");
    println!("  {:50} {:>10} {:>10} {:>10} {:>8}",
             "Ticker", "Open", "Close", "Strike", "Result");
    for m in markets.iter().take(10) {
        let result = if m.resolved_yes { "YES" } else { "NO" };
        let diff = m.closing_price - m.strike;
        println!("  {:50} {:>10.0} {:>10.0} {:>10.0} {:>8} ({:+.0})",
                 m.ticker, m.opening_price, m.closing_price, m.strike, result, diff);
    }
    if markets.len() > 10 {
        println!("  ... and {} more markets", markets.len() - 10);
    }

    // =========================================================================
    // SECTION 3: MODEL CALIBRATION - Does model predict outcomes correctly?
    // =========================================================================
    println!();
    println!("┌─────────────────────────────────────────────────────────────────────────────────────┐");
    println!("│ 3. MODEL CALIBRATION ANALYSIS                                                       │");
    println!("└─────────────────────────────────────────────────────────────────────────────────────┘");
    println!();
    println!("  Question: When our model says P(YES) = X%, does YES happen X% of the time?");
    println!();

    // Bucket trades by predicted probability
    let mut calibration_buckets: HashMap<i64, (usize, usize)> = HashMap::new(); // (yes_outcomes, total)
    for a in analyses {
        let bucket = ((a.fair_value as f64 / 10.0).round() as i64) * 10;
        let bucket = bucket.clamp(0, 100);
        let entry = calibration_buckets.entry(bucket).or_default();
        entry.1 += 1;
        if a.resolved_yes {
            entry.0 += 1;
        }
    }

    println!("  {:>12} {:>10} {:>12} {:>12} {:>12}",
             "Predicted", "Trades", "YES Count", "Actual %", "Diff");

    let mut total_brier = 0.0;
    let mut brier_count = 0;

    for bucket in (0..=100).step_by(10) {
        if let Some(&(yes, total)) = calibration_buckets.get(&bucket) {
            if total > 0 {
                let actual_pct = 100.0 * yes as f64 / total as f64;
                let diff = actual_pct - bucket as f64;
                println!("  {:>10}% {:>10} {:>12} {:>11.1}% {:>+11.1}%",
                         bucket, total, yes, actual_pct, diff);

                // Brier score component
                for a in analyses.iter().filter(|a| {
                    let b = ((a.fair_value as f64 / 10.0).round() as i64) * 10;
                    b.clamp(0, 100) == bucket
                }) {
                    let pred = a.fair_value as f64 / 100.0;
                    let outcome = if a.resolved_yes { 1.0 } else { 0.0 };
                    total_brier += (pred - outcome).powi(2);
                    brier_count += 1;
                }
            }
        }
    }

    let brier_score = if brier_count > 0 { total_brier / brier_count as f64 } else { 0.0 };
    println!();
    println!("  Brier Score: {:.4} (lower is better, 0.25 = random, 0 = perfect)", brier_score);

    // =========================================================================
    // SECTION 4: PROFITABILITY SIMULATION
    // =========================================================================
    println!();
    println!("┌─────────────────────────────────────────────────────────────────────────────────────┐");
    println!("│ 4. PROFITABILITY SIMULATION                                                         │");
    println!("└─────────────────────────────────────────────────────────────────────────────────────┘");
    println!();
    println!("  Strategy: Buy YES when model says underpriced, sell YES when overpriced");
    println!("  Threshold: Only trade when |mispricing| > threshold");
    println!();

    for threshold in [5, 10, 15, 20] {
        let mut pnl_total = 0.0;
        let mut trades_taken = 0;
        let mut wins = 0;

        for a in analyses {
            if a.mispricing.abs() < threshold {
                continue;
            }
            trades_taken += 1;

            // If mispricing > 0 (market overpriced YES), we sell YES (buy NO)
            // If mispricing < 0 (market underpriced YES), we buy YES
            let bought_yes = a.mispricing < 0;
            let payout = if a.resolved_yes { 100.0 } else { 0.0 };

            let pnl = if bought_yes {
                payout - a.yes_price as f64  // Bought YES at yes_price
            } else {
                (100 - a.yes_price) as f64 - (100.0 - payout)  // Sold YES (bought NO)
            };

            if pnl > 0.0 {
                wins += 1;
            }
            pnl_total += pnl;
        }

        let win_rate = if trades_taken > 0 { 100.0 * wins as f64 / trades_taken as f64 } else { 0.0 };
        let avg_pnl = if trades_taken > 0 { pnl_total / trades_taken as f64 } else { 0.0 };

        println!("  Threshold {:>2}¢: {:>4} trades, {:>5.1}% win rate, {:>+6.2}¢ avg P&L, {:>+8.2}¢ total",
                 threshold, trades_taken, win_rate, avg_pnl, pnl_total);
    }

    // =========================================================================
    // SECTION 5: TRADING EDGE BY MISPRICING BUCKET
    // =========================================================================
    println!();
    println!("┌─────────────────────────────────────────────────────────────────────────────────────┐");
    println!("│ 5. TRADING EDGE BY MISPRICING MAGNITUDE                                             │");
    println!("└─────────────────────────────────────────────────────────────────────────────────────┘");
    println!();
    println!("  Shows average P&L when trading based on mispricing signal");
    println!();

    let mut edge_buckets: HashMap<i64, Vec<f64>> = HashMap::new();
    for a in analyses {
        let bucket = ((a.mispricing as f64 / 10.0).round() as i64) * 10;
        let bucket = bucket.clamp(-50, 50);

        let bought_yes = a.mispricing < 0;
        let payout = if a.resolved_yes { 100.0 } else { 0.0 };
        let pnl = if bought_yes {
            payout - a.yes_price as f64
        } else {
            (100 - a.yes_price) as f64 - (100.0 - payout)
        };

        edge_buckets.entry(bucket).or_default().push(pnl);
    }

    println!("  {:>12} {:>10} {:>12} {:>12}",
             "Mispricing", "Trades", "Avg P&L", "Total P&L");

    for bucket in (-5..=5).map(|i| i * 10) {
        if let Some(pnls) = edge_buckets.get(&bucket) {
            if !pnls.is_empty() {
                let total: f64 = pnls.iter().sum();
                let avg = total / pnls.len() as f64;
                println!("  {:>+10}¢ {:>10} {:>+11.2}¢ {:>+11.2}¢",
                         bucket, pnls.len(), avg, total);
            }
        }
    }

    // =========================================================================
    // SECTION 6: TIME DECAY ANALYSIS
    // =========================================================================
    println!();
    println!("┌─────────────────────────────────────────────────────────────────────────────────────┐");
    println!("│ 6. TIME DECAY ANALYSIS                                                              │");
    println!("└─────────────────────────────────────────────────────────────────────────────────────┘");
    println!();

    let mut by_time: HashMap<String, Vec<&TradeAnalysis>> = HashMap::new();
    for a in analyses {
        let bucket = if a.minutes_remaining > 12.0 {
            "12-15 min"
        } else if a.minutes_remaining > 9.0 {
            "9-12 min"
        } else if a.minutes_remaining > 6.0 {
            "6-9 min"
        } else if a.minutes_remaining > 3.0 {
            "3-6 min"
        } else {
            "0-3 min"
        };
        by_time.entry(bucket.to_string()).or_default().push(a);
    }

    println!("  {:12} {:>8} {:>10} {:>10} {:>10} {:>10}",
             "Time Left", "Trades", "Mean FV", "Mispricing", "YES Rate", "Avg P&L");

    for bucket in ["12-15 min", "9-12 min", "6-9 min", "3-6 min", "0-3 min"] {
        if let Some(trades) = by_time.get(bucket) {
            if !trades.is_empty() {
                let fvs: Vec<i64> = trades.iter().map(|t| t.fair_value).collect();
                let misps: Vec<i64> = trades.iter().map(|t| t.mispricing).collect();
                let yes_count = trades.iter().filter(|t| t.resolved_yes).count();
                let yes_rate = 100.0 * yes_count as f64 / trades.len() as f64;

                // Calculate P&L for trades in this bucket
                let pnls: Vec<f64> = trades.iter().map(|a| {
                    let bought_yes = a.mispricing < 0;
                    let payout = if a.resolved_yes { 100.0 } else { 0.0 };
                    if bought_yes {
                        payout - a.yes_price as f64
                    } else {
                        (100 - a.yes_price) as f64 - (100.0 - payout)
                    }
                }).collect();

                let (mean_fv, _, _, _, _) = calc_stats(&fvs);
                let (mean_misp, _, _, _, _) = calc_stats(&misps);
                let (mean_pnl, _, _, _, _) = calc_stats_f64(&pnls);

                println!("  {:12} {:>8} {:>9.1}¢ {:>+9.1}¢ {:>9.1}% {:>+9.2}¢",
                         bucket, trades.len(), mean_fv, mean_misp, yes_rate, mean_pnl);
            }
        }
    }

    // =========================================================================
    // SECTION 7: VOLATILITY SENSITIVITY
    // =========================================================================
    println!();
    println!("┌─────────────────────────────────────────────────────────────────────────────────────┐");
    println!("│ 7. VOLATILITY SENSITIVITY ANALYSIS                                                  │");
    println!("└─────────────────────────────────────────────────────────────────────────────────────┘");
    println!();
    println!("  How would different volatility assumptions affect fair value and P&L?");
    println!();

    // Sample one trade to show sensitivity
    if let Some(sample) = analyses.first() {
        println!("  Sample trade: {} at {:.1} min remaining", sample.ticker, sample.minutes_remaining);
        println!("  Spot: ${:.0}, Strike: ${:.0}, Market YES: {}¢",
                 sample.spot_price, sample.strike, sample.yes_price);
        println!();
        println!("  {:>8} {:>12} {:>12} {:>12}",
                 "Vol %", "Fair Value", "Mispricing", "Strategy");

        let spot = sample.spot_price;
        let strike = sample.strike;
        let time = sample.minutes_remaining;
        let yes_price = sample.yes_price;

        for vol_pct in [30, 40, 50, 60, 70, 80] {
            let vol = vol_pct as f64 / 100.0;
            let fv = calc_fair_value(spot, strike, time, vol, true);
            let misp = yes_price - fv;
            let strategy = if misp > 5 {
                "Sell YES"
            } else if misp < -5 {
                "Buy YES"
            } else {
                "Hold"
            };
            println!("  {:>7}% {:>11}¢ {:>+11}¢ {:>12}",
                     vol_pct, fv, misp, strategy);
        }
    }

    // =========================================================================
    // SECTION 8: SAMPLE TRADES WITH OUTCOMES
    // =========================================================================
    println!();
    println!("┌─────────────────────────────────────────────────────────────────────────────────────┐");
    println!("│ 8. SAMPLE TRADES WITH OUTCOMES                                                      │");
    println!("└─────────────────────────────────────────────────────────────────────────────────────┘");
    println!();
    println!("  {:6} {:>8} {:>8} {:>6} {:>6} {:>6} {:>8} {:>6}",
             "MinRem", "Spot", "Strike", "Yes", "FV", "Misp", "Outcome", "P&L");

    for a in analyses.iter().take(15) {
        let bought_yes = a.mispricing < 0;
        let payout = if a.resolved_yes { 100.0 } else { 0.0 };
        let pnl = if bought_yes {
            payout - a.yes_price as f64
        } else {
            (100 - a.yes_price) as f64 - (100.0 - payout)
        };
        let action = if bought_yes { "Buy" } else { "Sell" };
        let outcome = if a.resolved_yes { "YES" } else { "NO" };

        println!("  {:>6.1} {:>8.0} {:>8.0} {:>5}¢ {:>5}¢ {:>+5}¢ {:>8} {:>+5.0}¢ ({})",
                 a.minutes_remaining, a.spot_price, a.strike,
                 a.yes_price, a.fair_value, a.mispricing, outcome, pnl, action);
    }

    // =========================================================================
    // SECTION 9: MISPRICING DISTRIBUTION
    // =========================================================================
    println!();
    println!("┌─────────────────────────────────────────────────────────────────────────────────────┐");
    println!("│ 9. MISPRICING DISTRIBUTION                                                          │");
    println!("└─────────────────────────────────────────────────────────────────────────────────────┘");
    println!();

    let mut buckets: HashMap<i64, u32> = HashMap::new();
    for i in -10..=10 {
        buckets.insert(i * 5, 0);
    }
    for &m in &mispricings {
        let bucket = ((m as f64 / 5.0).round() as i64) * 5;
        let bucket = bucket.clamp(-50, 50);
        *buckets.entry(bucket).or_insert(0) += 1;
    }

    let max_count = buckets.values().max().copied().unwrap_or(1) as f64;

    for i in -10..=10 {
        let bucket = i * 5;
        let count = buckets.get(&bucket).copied().unwrap_or(0);
        let bar_len = ((count as f64 / max_count) * 40.0).round() as usize;
        let bar = "█".repeat(bar_len);
        println!("  {:>+4}¢ │ {:40} ({})", bucket, bar, count);
    }

    // =========================================================================
    // SECTION 10: KEY TAKEAWAYS
    // =========================================================================
    println!();
    println!("┌─────────────────────────────────────────────────────────────────────────────────────┐");
    println!("│ 10. KEY TAKEAWAYS                                                                   │");
    println!("└─────────────────────────────────────────────────────────────────────────────────────┘");
    println!();

    // Model accuracy
    println!("  MODEL ACCURACY:");
    if brier_score < 0.20 {
        println!("    ✓ Model is well-calibrated (Brier: {:.4})", brier_score);
    } else if brier_score < 0.25 {
        println!("    ~ Model is marginally calibrated (Brier: {:.4})", brier_score);
    } else {
        println!("    ✗ Model needs improvement (Brier: {:.4})", brier_score);
    }

    // Market efficiency
    println!();
    println!("  MARKET EFFICIENCY:");
    if mean.abs() < 3.0 {
        println!("    ✓ Markets are efficiently priced (mean mispricing: {:+.1}¢)", mean);
    } else if mean > 0.0 {
        println!("    ! Markets OVERPRICE YES by {:.1}¢ on average", mean);
        println!("      Potential edge: Sell YES / Buy NO");
    } else {
        println!("    ! Markets UNDERPRICE YES by {:.1}¢ on average", mean.abs());
        println!("      Potential edge: Buy YES");
    }

    // Volatility
    println!();
    println!("  VOLATILITY:");
    if std_dev < 15.0 {
        println!("    ✓ Low variance suggests good model fit (std: {:.1}¢)", std_dev);
    } else {
        println!("    ~ High variance suggests model tuning needed (std: {:.1}¢)", std_dev);
    }

    println!();
}

fn print_csv(analyses: &[TradeAnalysis]) {
    println!("ticker,trade_time,close_time,minutes_remaining,yes_price,fair_value,fair_value_atm,mispricing,mispricing_atm,spot_price,opening_price,strike,closing_price,resolved_yes,taker_side,count");
    for a in analyses {
        println!(
            "{},{},{},{:.2},{},{},{},{},{},{:.2},{:.2},{:.2},{:.2},{},{},{}",
            a.ticker, a.trade_time, a.close_time, a.minutes_remaining,
            a.yes_price, a.fair_value, a.fair_value_atm, a.mispricing, a.mispricing_atm,
            a.spot_price, a.opening_price, a.strike, a.closing_price, a.resolved_yes,
            a.taker_side, a.count
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
}

fn parse_args() -> Args {
    let args: Vec<String> = std::env::args().collect();
    let mut i = 1;

    let mut limit = 5000u32;
    let mut vol = 0.50;
    let mut csv = false;

    while i < args.len() {
        match args[i].as_str() {
            "--limit" | "-l" => {
                i += 1;
                if i < args.len() {
                    limit = args[i].parse().unwrap_or(5000);
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
            "--help" | "-h" => {
                println!("Usage: backtest_trades [OPTIONS]");
                println!("  --limit, -l <N>   Max trades to fetch (default: 5000)");
                println!("  --vol, -v <PCT>   Annual volatility % (default: 50)");
                println!("  --csv             Output as CSV for charting");
                std::process::exit(0);
            }
            _ => {}
        }
        i += 1;
    }

    Args { limit, vol, csv }
}

#[tokio::main]
async fn main() -> Result<()> {
    // Load .env file
    dotenvy::dotenv().ok();

    let args = parse_args();

    // Load credentials
    let kalshi_config = KalshiConfig::from_env().context("Failed to load Kalshi credentials")?;
    let client = ApiClient::new(kalshi_config, POLYGON_API_KEY.to_string());

    if !args.csv {
        eprintln!("Fetching trades from Kalshi API...");
    }

    let trades = client.get_all_trades(args.limit).await?;

    if !args.csv {
        eprintln!("Total trades fetched: {}", trades.len());
        eprintln!("Filtering for BTC/ETH 15M markets...");
    }

    // Prepare trades (parse market info)
    let pending_trades: Vec<PendingTrade> = trades
        .iter()
        .filter_map(prepare_trade)
        .collect();

    if !args.csv {
        eprintln!("Crypto 15M trades found: {}", pending_trades.len());
    }

    if pending_trades.is_empty() {
        println!("No crypto 15M trades to analyze.");
        return Ok(());
    }

    // Group trades by market (so we can batch price lookups)
    // Key: (asset, open_time_ms) -> trades
    let mut markets: HashMap<(String, i64), Vec<&PendingTrade>> = HashMap::new();
    for pt in &pending_trades {
        let key = (pt.market_info.asset.clone(), pt.market_info.open_time.timestamp_millis());
        markets.entry(key).or_default().push(pt);
    }

    if !args.csv {
        eprintln!("Unique markets: {}", markets.len());
        eprintln!("Fetching crypto prices from Polygon.io...");
    }

    // Fetch prices and analyze
    let mut analyses: Vec<TradeAnalysis> = Vec::new();
    let mut market_analyses: Vec<MarketAnalysis> = Vec::new();
    let mut price_errors = 0;

    for ((asset, open_ts_ms), trades_in_market) in &markets {
        // Get time range for this market (open to close, plus buffer)
        let first = trades_in_market.first().unwrap();
        let close_ts_ms = first.market_info.close_time.timestamp_millis();

        // Fetch price bars for this time range (extend a bit past close to get closing price)
        let symbol = if asset == "BTC" { "X:BTCUSD" } else { "X:ETHUSD" };
        let bars = match client.get_crypto_bars(symbol, *open_ts_ms, close_ts_ms + 60000).await {
            Ok(b) => b,
            Err(e) => {
                if !args.csv {
                    eprintln!("  Warning: Failed to get prices for {} at {}: {}", asset, open_ts_ms, e);
                }
                price_errors += 1;
                continue;
            }
        };

        if bars.is_empty() {
            price_errors += 1;
            continue;
        }

        // Get opening price (first bar at market open)
        let opening_price = match client.find_price_at_time(&bars, *open_ts_ms) {
            Some(p) => p,
            None => {
                price_errors += 1;
                continue;
            }
        };

        // Get closing price (bar at market close)
        let closing_price = client.find_price_at_time(&bars, close_ts_ms)
            .unwrap_or(opening_price);

        let strike = opening_price + first.market_info.strike_offset as f64;
        let resolved_yes = if first.market_info.is_above {
            closing_price > strike
        } else {
            closing_price < strike
        };

        // Build market analysis
        let mut market_trades = Vec::new();

        // Analyze each trade in this market
        let vol = if asset == "BTC" { args.vol } else { args.vol * 1.2 }; // ETH slightly higher vol
        for pt in trades_in_market {
            let trade_ts_ms = pt.trade_time.timestamp_millis();
            if let Some(spot_price) = client.find_price_at_time(&bars, trade_ts_ms) {
                let analysis = analyze_trade_with_prices(pt, opening_price, spot_price, closing_price, vol);
                market_trades.push(analysis.clone());
                analyses.push(analysis);
            } else {
                price_errors += 1;
            }
        }

        // Create market analysis
        market_analyses.push(MarketAnalysis {
            ticker: first.trade.ticker.clone(),
            asset: asset.clone(),
            open_time: first.market_info.open_time,
            close_time: first.market_info.close_time,
            opening_price,
            closing_price,
            strike,
            strike_offset: first.market_info.strike_offset,
            resolved_yes,
            trades: market_trades,
        });
    }

    if !args.csv {
        eprintln!("Successfully analyzed {} trades across {} markets ({} price lookup errors)",
                  analyses.len(), market_analyses.len(), price_errors);
    }

    if args.csv {
        print_csv(&analyses);
    } else {
        print_summary(&analyses, &market_analyses);
    }

    Ok(())
}
