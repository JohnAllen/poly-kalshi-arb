//! Manual Strike Price Input Utility
//!
//! Allows manual keyboard entry of strike prices for markets where
//! the strike cannot be automatically parsed from the market title or API.
//!
//! Features:
//! - Prompts for strike price input every 15 minutes (or configurable interval)
//! - Shows current spot price from Polygon feed to help determine strike
//! - Calculates fair value using the manually entered strike
//! - Persists strikes to a JSON file for reuse
//!
//! Usage:
//!   cargo run --release --bin strike_input
//!
//! Environment:
//!   INTERVAL_MINS - Minutes between prompts (default: 15)
//!   VOL - Annual volatility % (default: 50)
//!   STRIKES_FILE - Path to save strikes (default: .strikes.json)

use anyhow::{Context, Result};
use chrono::{DateTime, Utc, Timelike};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::io::{self, Write};
use std::sync::Arc;
use tokio::sync::RwLock;
use tracing::{info, warn};

use arb_bot::kalshi::{KalshiConfig, KalshiApiClient};

// Polygon feed for live prices
mod polygon_feed;
use polygon_feed::{PriceState, run_polygon_feed};

const POLYGON_API_KEY: &str = "o2Jm26X52_0tRq2W7V5JbsCUXdMjL7qk";
const BTC_15M_SERIES: &str = "KXBTC15M";
const ETH_15M_SERIES: &str = "KXETH15M";

// ============================================================================
// STRIKE STORAGE
// ============================================================================

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
struct StrikeStore {
    /// Map of market ticker -> strike price
    strikes: HashMap<String, f64>,
    /// Last update timestamp
    last_updated: Option<String>,
}

impl StrikeStore {
    fn load(path: &str) -> Self {
        std::fs::read_to_string(path)
            .ok()
            .and_then(|s| serde_json::from_str(&s).ok())
            .unwrap_or_default()
    }

    fn save(&self, path: &str) -> Result<()> {
        let data = serde_json::to_string_pretty(self)?;
        std::fs::write(path, data)?;
        Ok(())
    }

    fn set_strike(&mut self, ticker: &str, strike: f64) {
        self.strikes.insert(ticker.to_string(), strike);
        self.last_updated = Some(Utc::now().to_rfc3339());
    }

    fn get_strike(&self, ticker: &str) -> Option<f64> {
        self.strikes.get(ticker).copied()
    }
}

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

/// Calculate fair value for binary option
/// Returns (yes_cents, no_cents)
fn calc_fair_value(spot: f64, strike: f64, minutes_remaining: f64, annual_vol: f64) -> (i64, i64) {
    if minutes_remaining <= 0.0 {
        return if spot > strike { (100, 0) } else { (0, 100) };
    }

    if annual_vol <= 0.0 {
        return if spot > strike { (100, 0) } else { (0, 100) };
    }

    let time_years = minutes_remaining / 525960.0;
    let sqrt_t = time_years.sqrt();
    let log_ratio = (spot / strike).ln();
    let d2 = (log_ratio - 0.5 * annual_vol.powi(2) * time_years) / (annual_vol * sqrt_t);

    let yes_prob = norm_cdf(d2);
    let yes_cents = (yes_prob * 100.0).round() as i64;
    let no_cents = 100 - yes_cents;

    (yes_cents, no_cents)
}

// ============================================================================
// MARKET DISCOVERY
// ============================================================================

#[derive(Debug, Clone)]
struct Market {
    ticker: String,
    title: String,
    close_time: Option<DateTime<Utc>>,
    yes_ask: Option<i64>,
    no_ask: Option<i64>,
    floor_strike: Option<f64>,
}

fn parse_close_time_from_ticker(ticker: &str) -> Option<DateTime<Utc>> {
    let parts: Vec<&str> = ticker.split('-').collect();
    if parts.len() < 2 {
        return None;
    }

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

    // Time is in EST (UTC-5), add 5 hours
    chrono::NaiveDate::from_ymd_opt(year, month_num, day)
        .and_then(|d| d.and_hms_opt(hour + 5, minute, 0))
        .map(|dt| DateTime::<Utc>::from_naive_utc_and_offset(dt, Utc))
}

/// Parse strike from title like "Bitcoin above $104,250?"
fn parse_strike_from_title(title: &str) -> Option<f64> {
    let dollar_idx = title.find('$')?;
    let after_dollar = &title[dollar_idx + 1..];

    let mut num_str = String::new();
    for c in after_dollar.chars() {
        if c.is_ascii_digit() || c == '.' {
            num_str.push(c);
        } else if c == ',' {
            continue;
        } else {
            break;
        }
    }

    num_str.parse().ok()
}

async fn discover_markets(client: &KalshiApiClient) -> Result<Vec<Market>> {
    let mut markets = Vec::new();

    for series in [BTC_15M_SERIES, ETH_15M_SERIES] {
        let events = client.get_events(series, 10).await?;

        for event in events {
            let event_markets = client.get_markets(&event.event_ticker).await?;

            for m in event_markets {
                let close_time = parse_close_time_from_ticker(&m.ticker);
                let mut market = Market {
                    ticker: m.ticker.clone(),
                    title: m.title.clone(),
                    close_time,
                    yes_ask: m.yes_ask,
                    no_ask: m.no_ask,
                    floor_strike: m.floor_strike,
                };

                // Only include markets with time remaining
                if let Some(close) = market.close_time {
                    let mins_remaining = (close - Utc::now()).num_minutes();
                    if mins_remaining > 0 {
                        markets.push(market);
                    }
                }
            }
        }
    }

    Ok(markets)
}

// ============================================================================
// USER INPUT
// ============================================================================

fn prompt(msg: &str) -> String {
    print!("{}", msg);
    io::stdout().flush().unwrap();
    let mut input = String::new();
    io::stdin().read_line(&mut input).unwrap();
    input.trim().to_string()
}

fn prompt_strike(market: &Market, spot: Option<f64>, existing: Option<f64>) -> Option<f64> {
    println!();
    println!("┌─────────────────────────────────────────────────────────────────┐");
    println!("│ STRIKE INPUT                                                    │");
    println!("├─────────────────────────────────────────────────────────────────┤");
    println!("│ Ticker: {:55} │", &market.ticker);
    println!("│ Title:  {:55} │", &market.title[..market.title.len().min(55)]);
    if let Some(s) = spot {
        println!("│ Current Spot: ${:>12.2}                                    │", s);
    }
    if let Some(e) = existing {
        println!("│ Existing Strike: ${:>12.2} (press Enter to keep)          │", e);
    }
    if let Some(api) = market.floor_strike {
        println!("│ API floor_strike: ${:>12.2}                                │", api);
    }
    if let Some(parsed) = parse_strike_from_title(&market.title) {
        println!("│ Parsed from title: ${:>12.2}                               │", parsed);
    }
    println!("└─────────────────────────────────────────────────────────────────┘");

    loop {
        let input = prompt("Enter strike price (or 's' to skip, 'q' to quit): ");

        if input.to_lowercase() == "q" {
            return None;
        }

        if input.to_lowercase() == "s" || input.is_empty() {
            // Use existing or auto-detected strike
            if let Some(e) = existing {
                return Some(e);
            }
            if let Some(api) = market.floor_strike {
                return Some(api);
            }
            if let Some(parsed) = parse_strike_from_title(&market.title) {
                return Some(parsed);
            }
            if let Some(s) = spot {
                // Use current spot as ATM
                println!("Using current spot ${:.2} as ATM strike", s);
                return Some(s);
            }
            return None;
        }

        // Parse the input
        let cleaned: String = input.chars()
            .filter(|c| c.is_ascii_digit() || *c == '.')
            .collect();

        if let Ok(strike) = cleaned.parse::<f64>() {
            return Some(strike);
        }

        println!("Invalid input. Enter a number like 104250 or 104250.50");
    }
}

// ============================================================================
// DISPLAY
// ============================================================================

fn print_fair_values(
    markets: &[Market],
    strikes: &StrikeStore,
    spot_btc: Option<f64>,
    spot_eth: Option<f64>,
    vol: f64,
) {
    println!();
    println!("╔═══════════════════════════════════════════════════════════════════════════════════════════════════════════════╗");
    println!("║                                      FAIR VALUE CALCULATOR                                                    ║");
    println!("╠═══════════════════════════════════════════════════════════════════════════════════════════════════════════════╣");
    println!("║ BTC Spot: {:>12} │ ETH Spot: {:>12} │ Vol: {:>4.0}%                                              ║",
        spot_btc.map(|s| format!("${:.2}", s)).unwrap_or("-".to_string()),
        spot_eth.map(|s| format!("${:.2}", s)).unwrap_or("-".to_string()),
        vol * 100.0);
    println!("╠═══════════════════════════════════════════════════════════════════════════════════════════════════════════════╣");
    println!("║ {:30} │ {:>7} │ {:>10} │ {:>10} │ {:>6} │ {:>6} │ {:>6} │ {:>6} │ {:>6} ║",
        "Ticker", "MinRem", "Spot", "Strike", "FairY", "FairN", "MktY", "MktN", "Edge");
    println!("╠═══════════════════════════════════════════════════════════════════════════════════════════════════════════════╣");

    for market in markets {
        let mins_remaining = market.close_time
            .map(|c| (c - Utc::now()).num_minutes().max(0) as f64)
            .unwrap_or(15.0);

        let spot = if market.ticker.contains("BTC") { spot_btc } else { spot_eth };

        // Get strike: manual > API > parsed > spot (ATM)
        let strike = strikes.get_strike(&market.ticker)
            .or(market.floor_strike)
            .or_else(|| parse_strike_from_title(&market.title))
            .or(spot);

        let (fair_yes, fair_no, edge) = if let (Some(s), Some(k)) = (spot, strike) {
            let (yes, no) = calc_fair_value(s, k, mins_remaining, vol);
            let mkt_yes = market.yes_ask.unwrap_or(50);
            let mkt_no = market.no_ask.unwrap_or(50);
            let edge = (yes - mkt_yes) + (no - mkt_no);
            (yes, no, edge)
        } else {
            (50, 50, 0)
        };

        let ticker_short = if market.ticker.len() > 30 {
            &market.ticker[..30]
        } else {
            &market.ticker
        };

        println!("║ {:30} │ {:>7.1} │ {:>10} │ {:>10} │ {:>6} │ {:>6} │ {:>6} │ {:>6} │ {:>+6} ║",
            ticker_short,
            mins_remaining,
            spot.map(|s| format!("{:.0}", s)).unwrap_or("-".to_string()),
            strike.map(|s| format!("{:.0}", s)).unwrap_or("-".to_string()),
            fair_yes,
            fair_no,
            market.yes_ask.map(|p| p.to_string()).unwrap_or("-".to_string()),
            market.no_ask.map(|p| p.to_string()).unwrap_or("-".to_string()),
            edge);
    }

    println!("╚═══════════════════════════════════════════════════════════════════════════════════════════════════════════════╝");
    println!();
    println!("Legend: Edge = (FairYES - MktYES) + (FairNO - MktNO). Positive = underpriced.");
    println!();
}

// ============================================================================
// MAIN
// ============================================================================

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::from_default_env()
                .add_directive("strike_input=info".parse().unwrap())
                .add_directive("arb_bot=info".parse().unwrap()),
        )
        .init();

    let vol: f64 = std::env::var("VOL")
        .ok()
        .and_then(|v| v.parse::<f64>().ok())
        .unwrap_or(50.0) / 100.0;

    let interval_mins: u64 = std::env::var("INTERVAL_MINS")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(15);

    let strikes_file = std::env::var("STRIKES_FILE")
        .unwrap_or_else(|_| ".strikes.json".to_string());

    println!();
    println!("╔═══════════════════════════════════════════════════════════════════════════════╗");
    println!("║              MANUAL STRIKE PRICE INPUT UTILITY                                ║");
    println!("╠═══════════════════════════════════════════════════════════════════════════════╣");
    println!("║ This tool lets you manually enter strike prices for markets where             ║");
    println!("║ the strike cannot be automatically determined.                                ║");
    println!("║                                                                               ║");
    println!("║ Commands:                                                                     ║");
    println!("║   Enter number  - Set strike price (e.g., 104250)                             ║");
    println!("║   Press Enter   - Keep existing/auto-detected strike                          ║");
    println!("║   's'           - Skip this market                                            ║");
    println!("║   'r'           - Refresh markets and prices                                  ║");
    println!("║   'q'           - Quit                                                        ║");
    println!("╠═══════════════════════════════════════════════════════════════════════════════╣");
    println!("║ Vol: {:>4.0}% │ Interval: {}min │ Strikes file: {:30} ║",
        vol * 100.0, interval_mins, &strikes_file[..strikes_file.len().min(30)]);
    println!("╚═══════════════════════════════════════════════════════════════════════════════╝");
    println!();

    // Load existing strikes
    let mut strikes = StrikeStore::load(&strikes_file);
    info!("Loaded {} existing strikes from {}", strikes.strikes.len(), strikes_file);

    // Start Polygon feed for live prices
    let spot_prices = Arc::new(RwLock::new(PriceState::default()));
    let spot_clone = spot_prices.clone();
    tokio::spawn(async move {
        run_polygon_feed(spot_clone, POLYGON_API_KEY).await;
    });
    info!("Started BTC/ETH price feed");

    // Load Kalshi client
    let config = KalshiConfig::from_env().context("Failed to load Kalshi credentials")?;
    let client = KalshiApiClient::new(config);
    info!("Kalshi API connected");

    // Wait a moment for price feed
    tokio::time::sleep(std::time::Duration::from_secs(2)).await;

    loop {
        // Discover markets
        info!("Discovering markets...");
        let markets = match discover_markets(&client).await {
            Ok(m) => m,
            Err(e) => {
                warn!("Failed to discover markets: {}", e);
                Vec::new()
            }
        };

        if markets.is_empty() {
            println!("No active markets found. Waiting...");
        } else {
            // Get current spot prices
            let prices = spot_prices.read().await;
            let spot_btc = prices.btc_price;
            let spot_eth = prices.eth_price;
            drop(prices);

            // Show current fair values
            print_fair_values(&markets, &strikes, spot_btc, spot_eth, vol);

            // Prompt for any markets without strikes
            println!("Press 'i' to input strikes, 'r' to refresh, 'q' to quit:");
            let cmd = prompt("> ");

            match cmd.to_lowercase().as_str() {
                "q" => {
                    info!("Quitting...");
                    break;
                }
                "i" => {
                    // Input strikes for each market
                    let prices = spot_prices.read().await;
                    let spot_btc = prices.btc_price;
                    let spot_eth = prices.eth_price;
                    drop(prices);

                    for market in &markets {
                        let spot = if market.ticker.contains("BTC") { spot_btc } else { spot_eth };
                        let existing = strikes.get_strike(&market.ticker);

                        if let Some(strike) = prompt_strike(market, spot, existing) {
                            strikes.set_strike(&market.ticker, strike);
                            info!("Set strike for {} = ${:.2}", market.ticker, strike);
                        }
                    }

                    // Save strikes
                    if let Err(e) = strikes.save(&strikes_file) {
                        warn!("Failed to save strikes: {}", e);
                    } else {
                        info!("Saved strikes to {}", strikes_file);
                    }

                    // Show updated fair values
                    let prices = spot_prices.read().await;
                    let spot_btc = prices.btc_price;
                    let spot_eth = prices.eth_price;
                    drop(prices);
                    print_fair_values(&markets, &strikes, spot_btc, spot_eth, vol);
                }
                "r" => {
                    // Just refresh (loop continues)
                    info!("Refreshing...");
                }
                _ => {
                    // Wait for next interval
                    println!("Waiting {} minutes for next check. Press Ctrl+C to exit.", interval_mins);

                    // Show countdown
                    for remaining in (1..=interval_mins).rev() {
                        print!("\r{} minutes remaining... (press Enter to refresh early)", remaining);
                        io::stdout().flush().unwrap();

                        // Check for early input
                        let timeout = tokio::time::timeout(
                            std::time::Duration::from_secs(60),
                            tokio::task::spawn_blocking(|| {
                                let mut input = String::new();
                                let _ = io::stdin().read_line(&mut input);
                            })
                        );

                        if timeout.await.is_ok() {
                            println!("\nRefreshing early...");
                            break;
                        }
                    }
                    println!();
                }
            }
        }
    }

    Ok(())
}
