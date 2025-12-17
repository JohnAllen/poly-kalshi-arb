//! Kalshi Crypto Arbitrage Scanner
//!
//! Scans Kalshi's 15-minute BTC and ETH price markets for arbitrage opportunities
//! where you can buy both YES and NO for less than $1 total, guaranteeing profit.
//!
//! FEATURES:
//! - Real-time orderbook monitoring via WebSocket
//! - Historical volatility (HV) based option pricing model
//! - Fair value calculation using simplified Black-Scholes for binary options
//! - Automatic market discovery every 60 seconds
//!
//! STRATEGY:
//! 1. Every 15 minutes when new markets open, buy X YES + X NO at target price (default 48¢).
//!    If both fill at 48¢: pay 96¢ + fees (~4¢) = ~100¢, get $1 = breakeven or small profit.
//!
//! 2. ATM INSIGHT: At market launch, strike = spot (ATM), so delta = 0.50.
//!    Fair value = 50¢/50¢. Anything under 50¢ is mathematically underpriced!
//!    - Buy under 50¢ when price is right at strike price
//!    - Even 49¢+49¢ = 98¢ + 2¢ fees = breakeven (but you're buying underpriced options)
//!
//! Usage:
//!   cargo run --release --bin kalshi_crypto_arb
//!
//! Environment:
//!   KALSHI_API_KEY_ID - Your Kalshi API key ID
//!   KALSHI_PRIVATE_KEY_PATH - Path to your Kalshi private key PEM file
//!   DRY_RUN=0 - Set to execute trades (default: 1 = monitor only)
//!   ARB_THRESHOLD=99 - Max total cost in cents (default: 99 for 1% profit)
//!   CONTRACTS=100 - Number of contracts to buy per side (default: 100)
//!   TARGET_PRICE=48 - Price in cents to bid for each side (default: 48)
//!   SECS_INTO_MKT=60 - Seconds into market before requiring price-near-strike check (default: 60)
//!   BTC_HV=50 - BTC annualized volatility % (default: 50)
//!   ETH_HV=60 - ETH annualized volatility % (default: 60)

use anyhow::{Context, Result};
use chrono::{Utc, NaiveTime, Timelike};
use clap::Parser;
use futures_util::{SinkExt, StreamExt};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use std::io::Write;
use tokio::sync::{RwLock, mpsc};
use tokio_tungstenite::{connect_async, tungstenite::{http::Request, Message}};
use tracing::{debug, error, info, warn};

/// Kalshi Crypto Market Maker Bot
#[derive(Parser, Debug)]
#[command(name = "kalshi_crypto_arb")]
#[command(about = "Market-making bot for Kalshi 15-minute crypto binary options")]
struct Args {
    /// Number of contracts to trade per side
    #[arg(short = 'n', long, default_value = "3")]
    contracts: i64,

    /// Enable live trading (default: dry run / monitor only)
    #[arg(short, long)]
    live: bool,

    /// Target profit in cents (bid total = 100 - profit)
    #[arg(short, long, default_value = "15")]
    profit: i64,

    /// Max total bid in cents (for arb threshold)
    #[arg(short, long, default_value = "99")]
    threshold: i64,

    /// Seconds into market before requiring price-near-strike check
    #[arg(short, long, default_value = "60")]
    secs_into_mkt: i64,
}

// Import from the main library
use arb_bot::kalshi::{KalshiConfig, KalshiApiClient};
use arb_bot::types::kalshi_fee_cents;

// Pricing model
mod pricing;
use pricing::{BinaryOptionPricer, PricingConfig, parse_strike_from_title};

// Polygon price feed
mod polygon_feed;
use polygon_feed::{PriceState, run_polygon_feed};

/// Kalshi WebSocket URL
const KALSHI_WS_URL: &str = "wss://api.elections.kalshi.com/trade-api/ws/v2";

/// Polygon API key for crypto price feed
const POLYGON_API_KEY: &str = "o2Jm26X52_0tRq2W7V5JbsCUXdMjL7qk";

/// Series tickers for crypto markets
const BTC_15M_SERIES: &str = "KXBTC15M";
const ETH_15M_SERIES: &str = "KXETH15M";

/// Default seconds into market before requiring price-near-strike check
/// Only buy blindly in first ~1 minute of 15-min market (when strike ≈ spot at market open)
const DEFAULT_SECS_INTO_MKT: i64 = 60;

/// Kalshi URLs for scraping strike price
const KALSHI_BTC_URL: &str = "https://kalshi.com/markets/kxbtc15m/bitcoin-price-up-down";
const KALSHI_ETH_URL: &str = "https://kalshi.com/markets/kxeth15m/ethereum-price-up-down";

/// Log directory for per-market order logs
const LOG_DIR: &str = "logs/orders";

/// Write detailed order log to per-market file
fn log_order(
    ticker: &str,
    event: &str,
    side: &str,
    price: i64,
    qty: i64,
    filled: i64,
    cost: i64,
    status: &str,
    latency_ms: i64,
    spot: Option<f64>,
    strike: Option<f64>,
    secs_remaining: i64,
    condition: &str,
) {
    // Create logs directory if needed
    let _ = std::fs::create_dir_all(LOG_DIR);

    let log_path = format!("{}/{}.log", LOG_DIR, ticker);
    let timestamp = chrono::Utc::now().format("%Y-%m-%d %H:%M:%S%.3f UTC");

    let diff_info = match (spot, strike) {
        (Some(s), Some(k)) => {
            let diff = s - k;
            let pct = (diff / k) * 100.0;
            format!("Spot=${:.2} Strike=${:.0} Diff={:+.0} ({:+.2}%)", s, k, diff, pct)
        }
        _ => "Spot=? Strike=?".to_string(),
    };

    let entry = format!(
        "[{}] {} | {} {} @{}¢ x{} | filled={} cost={}¢ status={} latency={}ms | {}m{}s left | {} | {}\n",
        timestamp,
        event,
        side,
        ticker,
        price,
        qty,
        filled,
        cost,
        status,
        latency_ms,
        secs_remaining / 60,
        secs_remaining % 60,
        diff_info,
        condition,
    );

    if let Ok(mut file) = std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(&log_path)
    {
        let _ = file.write_all(entry.as_bytes());
    }
}

/// Scrape "Price to beat:" strike from Kalshi page
async fn scrape_kalshi_strike(asset: &str) -> Option<f64> {
    let url = if asset.to_uppercase().contains("BTC") {
        KALSHI_BTC_URL
    } else {
        KALSHI_ETH_URL
    };

    debug!("[SCRAPE] Fetching strike from {}", url);

    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(10))
        .build()
        .ok()?;

    let resp = client.get(url)
        .header("User-Agent", "Mozilla/5.0")
        .send()
        .await
        .ok()?;

    let html = resp.text().await.ok()?;

    // Look for "Price to beat:" followed by dollar amount
    let marker = "Price to beat:";
    let idx = html.find(marker)?;
    let after = &html[idx + marker.len()..];

    // Find the $ and extract the number
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

    let strike = num_str.parse::<f64>().ok()?;
    debug!("[SCRAPE] Found {} strike: ${:.2}", asset, strike);
    Some(strike)
}

/// Parse expiry time from ticker like "KXBTC15M-25DEC171000-00" -> seconds remaining
/// Format: 25DEC171000 = year 25, Dec 17, 10:00 EST (Eastern Time)
fn parse_expiry_secs_remaining(ticker: &str) -> Option<i64> {
    let parts: Vec<&str> = ticker.split('-').collect();
    if parts.len() < 2 { return None; }
    let datetime_part = parts[1]; // "25DEC171000"
    if datetime_part.len() < 11 { return None; }

    let hhmm = &datetime_part[datetime_part.len()-4..];
    let hour: u32 = hhmm[0..2].parse().ok()?;
    let minute: u32 = hhmm[2..4].parse().ok()?;

    // Time in ticker is EST (Eastern Time) - convert to UTC by adding 5 hours
    // Note: This assumes EST (UTC-5). During EDT (daylight saving), it would be UTC-4
    // Kalshi crypto markets run 24/7 so we use EST year-round for simplicity
    let hour_utc = (hour + 5) % 24;

    let now = Utc::now();
    let expiry_time = NaiveTime::from_hms_opt(hour_utc, minute, 0)?;
    let current_time = now.time();

    let expiry_secs = expiry_time.num_seconds_from_midnight() as i64;
    let current_secs = current_time.num_seconds_from_midnight() as i64;
    let diff_secs = expiry_secs - current_secs;

    if diff_secs < -60 {
        Some(diff_secs + 24 * 3600)
    } else {
        Some(diff_secs)
    }
}

/// Check if current spot price is near the strike (within threshold %)
fn is_price_near_strike(spot: f64, strike: f64, threshold_pct: f64) -> bool {
    let diff_pct = ((spot - strike) / strike).abs() * 100.0;
    diff_pct <= threshold_pct
}

/// A crypto market with its orderbook state
#[derive(Debug, Clone)]
struct CryptoMarket {
    ticker: String,
    title: String,
    event_ticker: String,
    /// Best YES bid price in cents (what someone will pay for YES)
    yes_bid: Option<i64>,
    /// Best YES ask price in cents (what you pay to buy YES)
    yes_ask: Option<i64>,
    /// Best NO bid price in cents (what someone will pay for NO)
    no_bid: Option<i64>,
    /// Best NO ask price in cents (what you pay to buy NO)
    no_ask: Option<i64>,
    /// YES bid size
    yes_bid_size: i64,
    /// YES ask size
    yes_ask_size: i64,
    /// NO bid size
    no_bid_size: i64,
    /// NO ask size
    no_ask_size: i64,
    /// Strike price parsed from title
    strike: Option<f64>,
    /// Fair value for YES in cents (from pricing model)
    yes_fair_cents: Option<i64>,
    /// Fair value for NO in cents (from pricing model)
    no_fair_cents: Option<i64>,
    /// Full YES orderbook: price -> qty (for delta updates)
    yes_book: std::collections::HashMap<i64, i64>,
    /// Full NO orderbook: price -> qty (for delta updates)
    no_book: std::collections::HashMap<i64, i64>,
}

impl CryptoMarket {
    fn new(ticker: String, title: String, event_ticker: String, floor_strike: Option<f64>) -> Self {
        // Use floor_strike from API if available, otherwise try parsing from title
        let strike = floor_strike.or_else(|| parse_strike_from_title(&title));
        Self {
            ticker,
            title,
            event_ticker,
            yes_bid: None,
            yes_ask: None,
            no_bid: None,
            no_ask: None,
            yes_bid_size: 0,
            yes_ask_size: 0,
            no_bid_size: 0,
            no_ask_size: 0,
            strike,
            yes_fair_cents: Some(50), // Default ATM = 50/50
            no_fair_cents: Some(50),
            yes_book: std::collections::HashMap::new(),
            no_book: std::collections::HashMap::new(),
        }
    }

    /// Update fair values based on current spot price
    fn update_fair_value(&mut self, spot: f64, pricing_config: &PricingConfig) {
        if let Some(strike) = self.strike {
            let hv = pricing_config.get_hv(&self.ticker);
            // Assume 15 minutes to expiry for new markets
            // TODO: Parse actual expiry from ticker (e.g., KXBTC15M-25DEC170915 = 09:15)
            let pricer = BinaryOptionPricer::new(spot, strike, 15.0, hv);
            self.yes_fair_cents = Some(pricer.yes_fair_cents());
            self.no_fair_cents = Some(pricer.no_fair_cents());
        }
    }

    /// Calculate edge in cents (positive = good for us)
    fn edge_cents(&self) -> Option<i64> {
        let yes_fair = self.yes_fair_cents?;
        let no_fair = self.no_fair_cents?;
        let yes_market = self.yes_ask?;
        let no_market = self.no_ask?;

        // Edge = fair value - market price (for each side)
        Some((yes_fair - yes_market) + (no_fair - no_market))
    }

    /// Calculate total cost to buy both sides (including fees)
    fn total_cost_cents(&self) -> Option<i64> {
        let yes = self.yes_ask?;
        let no = self.no_ask?;
        let yes_fee = kalshi_fee_cents(yes as u16) as i64;
        let no_fee = kalshi_fee_cents(no as u16) as i64;
        Some(yes + no + yes_fee + no_fee)
    }

    /// Calculate profit in cents if we buy both sides
    fn profit_cents(&self) -> Option<i64> {
        let cost = self.total_cost_cents()?;
        Some(100 - cost)
    }

    /// Check if there's an arbitrage opportunity
    fn has_arb(&self, threshold_cents: i64) -> bool {
        self.total_cost_cents().map(|c| c < threshold_cents).unwrap_or(false)
    }

    /// Check if both sides are available at target price or less
    fn can_buy_both_at(&self, max_price: i64) -> bool {
        match (self.yes_ask, self.no_ask) {
            (Some(yes), Some(no)) => yes <= max_price && no <= max_price,
            _ => false,
        }
    }
}

/// Global state for all tracked crypto markets
struct CryptoState {
    markets: HashMap<String, CryptoMarket>,
    /// Position tracking per market
    positions: HashMap<String, Position>,
    /// Resting orders per market
    resting_orders: HashMap<String, RestingOrders>,
}

impl CryptoState {
    fn new() -> Self {
        Self {
            markets: HashMap::new(),
            positions: HashMap::new(),
            resting_orders: HashMap::new(),
        }
    }

    fn add_market(&mut self, market: CryptoMarket) {
        let ticker = market.ticker.clone();
        self.markets.insert(ticker.clone(), market);
        self.positions.entry(ticker.clone()).or_insert_with(Position::default);
        self.resting_orders.entry(ticker).or_insert_with(RestingOrders::default);
    }

    fn get_resting_orders(&self, ticker: &str) -> Option<&RestingOrders> {
        self.resting_orders.get(ticker)
    }

    fn get_resting_orders_mut(&mut self, ticker: &str) -> Option<&mut RestingOrders> {
        self.resting_orders.get_mut(ticker)
    }

    fn get_mut(&mut self, ticker: &str) -> Option<&mut CryptoMarket> {
        self.markets.get_mut(ticker)
    }

    fn get_position(&self, ticker: &str) -> Option<&Position> {
        self.positions.get(ticker)
    }

    fn get_position_mut(&mut self, ticker: &str) -> Option<&mut Position> {
        self.positions.get_mut(ticker)
    }

    fn tickers(&self) -> Vec<String> {
        self.markets.keys().cloned().collect()
    }
}

// === WebSocket Message Types ===

#[derive(Deserialize, Debug)]
struct KalshiWsMessage {
    #[serde(rename = "type")]
    msg_type: String,
    msg: Option<KalshiWsMsgBody>,
}

#[derive(Deserialize, Debug)]
struct KalshiWsMsgBody {
    market_ticker: Option<String>,
    /// Snapshot/delta: YES side levels [[price, qty], ...]
    yes: Option<Vec<Vec<i64>>>,
    /// Snapshot/delta: NO side levels [[price, qty], ...]
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

/// Discover all open crypto markets
async fn discover_crypto_markets(client: &KalshiApiClient) -> Result<Vec<CryptoMarket>> {
    let mut markets = Vec::new();

    // Scrape strikes from Kalshi website (same strike for all markets in the 15-min window)
    let btc_strike = scrape_kalshi_strike("BTC").await;
    // let eth_strike = scrape_kalshi_strike("ETH").await;  // ETH disabled for now
    let eth_strike: Option<f64> = None;

    if btc_strike.is_none() {
        error!("[DISCOVER] Could not scrape BTC strike from Kalshi");
    }
    // ETH scraping disabled
    // if eth_strike.is_none() {
    //     error!("[DISCOVER] Could not scrape ETH strike from Kalshi");
    // }

    for series in [BTC_15M_SERIES] {  // ETH disabled for now
        debug!("[DISCOVER] Fetching events for series: {}", series);

        let events = client.get_events(series, 100).await?;
        debug!("[DISCOVER] Found {} events for {}", events.len(), series);

        // Use scraped strike for this asset
        let scraped_strike = if series == BTC_15M_SERIES { btc_strike } else { eth_strike };

        for event in events {
            debug!("[DISCOVER] Event: {} - {}", event.event_ticker, event.title);
            let event_markets = client.get_markets(&event.event_ticker).await?;

            for m in event_markets {
                // Priority: API floor_strike > scraped strike > parsed from title
                let strike = m.floor_strike.or(scraped_strike);

                debug!("[DISCOVER] Market: {} - {} | strike=${:.2} yes_ask={} no_ask={}",
                      m.ticker, m.title,
                      strike.unwrap_or(0.0),
                      m.yes_ask.unwrap_or(0),
                      m.no_ask.unwrap_or(0));
                let market = CryptoMarket::new(
                    m.ticker.clone(),
                    m.title.clone(),
                    event.event_ticker.clone(),
                    strike,
                );
                markets.push(market);
            }
        }
    }

    Ok(markets)
}

/// Process orderbook snapshot from WebSocket
/// Kalshi sends YES bids and NO bids as separate arrays.
///
/// YES orderbook (body.yes):
///   - YES bid = highest price someone will pay for YES
///   - NO ask = 100 - YES bid (to buy NO, you sell YES to the YES bidder)
///
/// NO orderbook (body.no):
///   - NO bid = highest price someone will pay for NO
///   - YES ask = 100 - NO bid (to buy YES, you sell NO to the NO bidder)
/// Process orderbook snapshot - replaces entire book
fn process_snapshot(market: &mut CryptoMarket, body: &KalshiWsMsgBody) {
    // Process YES orderbook - replaces entire YES book
    if let Some(levels) = &body.yes {
        market.yes_book.clear();
        for level in levels {
            if level.len() >= 2 {
                let price = level[0];
                let qty = level[1];
                if qty > 0 {
                    market.yes_book.insert(price, qty);
                }
            }
        }
    }

    // Process NO orderbook - replaces entire NO book
    if let Some(levels) = &body.no {
        market.no_book.clear();
        for level in levels {
            if level.len() >= 2 {
                let price = level[0];
                let qty = level[1];
                if qty > 0 {
                    market.no_book.insert(price, qty);
                }
            }
        }
    }

    // Recalculate best bid/ask from books
    recalc_best_prices(market);
}

/// Process orderbook delta - updates existing book
fn process_delta(market: &mut CryptoMarket, body: &KalshiWsMsgBody) {
    // Process YES orderbook delta
    if let Some(levels) = &body.yes {
        for level in levels {
            if level.len() >= 2 {
                let price = level[0];
                let qty = level[1];
                if qty > 0 {
                    market.yes_book.insert(price, qty);
                } else {
                    market.yes_book.remove(&price);
                }
            }
        }
    }

    // Process NO orderbook delta
    if let Some(levels) = &body.no {
        for level in levels {
            if level.len() >= 2 {
                let price = level[0];
                let qty = level[1];
                if qty > 0 {
                    market.no_book.insert(price, qty);
                } else {
                    market.no_book.remove(&price);
                }
            }
        }
    }

    // Recalculate best bid/ask from books
    recalc_best_prices(market);
}

/// Recalculate best bid/ask from full orderbooks
fn recalc_best_prices(market: &mut CryptoMarket) {
    // YES book: find highest bid
    if let Some((&price, &qty)) = market.yes_book.iter().max_by_key(|(p, _)| *p) {
        market.yes_bid = Some(price);
        market.yes_bid_size = qty;
        // To buy NO, you sell YES to this bidder at 100 - their_bid
        market.no_ask = Some(100 - price);
        market.no_ask_size = qty;
    } else {
        market.yes_bid = None;
        market.yes_bid_size = 0;
        market.no_ask = None;
        market.no_ask_size = 0;
    }

    // NO book: find highest bid
    if let Some((&price, &qty)) = market.no_book.iter().max_by_key(|(p, _)| *p) {
        market.no_bid = Some(price);
        market.no_bid_size = qty;
        // To buy YES, you sell NO to this bidder at 100 - their_bid
        market.yes_ask = Some(100 - price);
        market.yes_ask_size = qty;
    } else {
        market.no_bid = None;
        market.no_bid_size = 0;
        market.yes_ask = None;
        market.yes_ask_size = 0;
    }
}

/// Track position for a market - accumulates as we buy at different prices
#[derive(Debug, Clone, Default)]
struct Position {
    yes_contracts: i64,
    no_contracts: i64,
    yes_cost_cents: i64,
    no_cost_cents: i64,
}

/// Track our resting orders for a market
#[derive(Debug, Clone, Default)]
struct RestingOrders {
    /// Our YES bid order (if any)
    yes_bid_order_id: Option<String>,
    yes_bid_price: Option<i64>,
    yes_bid_qty: i64,
    /// Our NO bid order (if any)
    no_bid_order_id: Option<String>,
    no_bid_price: Option<i64>,
    no_bid_qty: i64,
}

impl Position {
    fn matched_contracts(&self) -> i64 {
        self.yes_contracts.min(self.no_contracts)
    }

    fn unmatched_yes(&self) -> i64 {
        (self.yes_contracts - self.no_contracts).max(0)
    }

    fn unmatched_no(&self) -> i64 {
        (self.no_contracts - self.yes_contracts).max(0)
    }

    fn total_cost(&self) -> i64 {
        self.yes_cost_cents + self.no_cost_cents
    }

    fn guaranteed_value(&self) -> i64 {
        self.matched_contracts() * 100 // $1 per matched pair
    }

    fn locked_profit(&self) -> i64 {
        self.guaranteed_value() - self.total_cost()
    }

    fn add(&mut self, other: &Position) {
        self.yes_contracts += other.yes_contracts;
        self.no_contracts += other.no_contracts;
        self.yes_cost_cents += other.yes_cost_cents;
        self.no_cost_cents += other.no_cost_cents;
    }
}

/// Market-making strategy: Post resting bids on both YES and NO
/// Goal: Get filled on both sides for total < 100¢
///
/// Returns (yes_order_id, no_order_id) if orders were placed
async fn post_market_making_orders(
    client: &KalshiApiClient,
    ticker: &str,
    market: &CryptoMarket,
    resting: &mut RestingOrders,
    contracts: i64,
    max_total: i64,  // e.g., 98 for 2¢ profit target
) -> Result<()> {
    // Need both sides to have visible bids to make market
    let yes_best_bid = match market.yes_bid {
        Some(b) => b,
        None => {
            debug!("[MM] {} | No YES bids visible, skipping", ticker);
            return Ok(());
        }
    };
    let no_best_bid = match market.no_bid {
        Some(b) => b,
        None => {
            debug!("[MM] {} | No NO bids visible, skipping", ticker);
            return Ok(());
        }
    };

    // Strategy: Match the best bid to get queue priority
    // Only bid if total (best_yes + best_no) is profitable
    let target_profit = 15; // Target 15¢ profit per pair
    let max_bid_total = 100 - target_profit; // 85¢ max total

    // Check if market is profitable at best bids
    let market_total = yes_best_bid + no_best_bid;
    let market_profitable = market_total <= max_bid_total;

    if !market_profitable {
        // Market bids are too high - not profitable to match
        // Keep existing orders, don't spam new ones
        info!("[MM] {} | Market not profitable: {}+{}={}¢ > {}¢ target",
               ticker, yes_best_bid, no_best_bid, market_total, max_bid_total);
        return Ok(());
    }

    // Market is profitable - bid 1¢ above best bid to get priority
    // But cap so our total doesn't exceed max_bid_total
    let our_yes_bid = yes_best_bid + 1;
    let our_no_bid = no_best_bid + 1;
    let our_total = our_yes_bid + our_no_bid;

    // Don't bid if our total exceeds max
    if our_total > max_bid_total {
        info!("[MM] {} | Our bids too high: {}+{}={}¢ > {}¢ max",
               ticker, our_yes_bid, our_no_bid, our_total, max_bid_total);
        return Ok(());
    }
    let potential_profit = 100 - our_total;

    info!("[MM] {} | Market: YES bid={}¢ NO bid={}¢ | Total={}¢ | Profit={}¢",
          ticker, yes_best_bid, no_best_bid, our_total, potential_profit);

    // Check if we need to update YES bid:
    // Only cancel/repost if our price differs from what we want
    let need_yes_cancel = match (&resting.yes_bid_order_id, resting.yes_bid_price) {
        (Some(_), Some(p)) if p != our_yes_bid => {
            info!("[MM] YES: updating {}¢ -> {}¢", p, our_yes_bid);
            true
        },
        _ => false,
    };
    let need_yes_post = resting.yes_bid_order_id.is_none();  // No order yet

    // Check if we need to update NO bid
    let need_no_cancel = match (&resting.no_bid_order_id, resting.no_bid_price) {
        (Some(_), Some(p)) if p != our_no_bid => {
            info!("[MM] NO: updating {}¢ -> {}¢", p, our_no_bid);
            true
        },
        _ => false,
    };
    let need_no_post = resting.no_bid_order_id.is_none();

    // Cancel and repost in PARALLEL for speed
    let yes_needs_work = need_yes_cancel || need_yes_post;
    let no_needs_work = need_no_cancel || need_no_post;

    if !yes_needs_work && !no_needs_work {
        debug!("[MM] {} | No changes needed", ticker);
        return Ok(());
    }

    info!("[MM] {} | Updating orders: YES={} NO={}", ticker,
          if yes_needs_work { format!("{}¢", our_yes_bid) } else { "no change".into() },
          if no_needs_work { format!("{}¢", our_no_bid) } else { "no change".into() });

    // Cancel both orders in parallel if needed
    // IMPORTANT: Only clear order ID if cancel succeeds, otherwise we lose track
    let yes_cancel_id = if need_yes_cancel { resting.yes_bid_order_id.clone() } else { None };
    let no_cancel_id = if need_no_cancel { resting.no_bid_order_id.clone() } else { None };

    let mut yes_cancel_ok = !need_yes_cancel;  // true if no cancel needed
    let mut no_cancel_ok = !need_no_cancel;

    if yes_cancel_id.is_some() || no_cancel_id.is_some() {
        let (yes_res, no_res) = tokio::join!(
            async {
                if let Some(oid) = &yes_cancel_id {
                    client.cancel_order(oid).await
                } else {
                    Ok(())
                }
            },
            async {
                if let Some(oid) = &no_cancel_id {
                    client.cancel_order(oid).await
                } else {
                    Ok(())
                }
            }
        );

        // Only clear order tracking if cancel succeeded
        if let Err(e) = &yes_res {
            warn!("[MM] YES cancel failed: {} - keeping old order", e);
        } else if yes_cancel_id.is_some() {
            resting.yes_bid_order_id = None;
            resting.yes_bid_price = None;
            yes_cancel_ok = true;
        }

        if let Err(e) = &no_res {
            warn!("[MM] NO cancel failed: {} - keeping old order", e);
        } else if no_cancel_id.is_some() {
            resting.no_bid_order_id = None;
            resting.no_bid_price = None;
            no_cancel_ok = true;
        }
    }

    // Post both orders in parallel - only if cancel succeeded or no cancel was needed
    let post_yes = (need_yes_post || (need_yes_cancel && yes_cancel_ok)) && resting.yes_bid_order_id.is_none();
    let post_no = (need_no_post || (need_no_cancel && no_cancel_ok)) && resting.no_bid_order_id.is_none();

    if post_yes || post_no {
        let ticker_str = ticker.to_string();
        let (yes_res, no_res) = tokio::join!(
            async {
                if post_yes {
                    Some(client.buy_limit(&ticker_str, "yes", our_yes_bid, contracts).await)
                } else {
                    None
                }
            },
            async {
                if post_no {
                    Some(client.buy_limit(&ticker_str, "no", our_no_bid, contracts).await)
                } else {
                    None
                }
            }
        );

        if let Some(Ok(resp)) = yes_res {
            info!("[MM] ✅ YES @{}¢ x{} | id={}", our_yes_bid, contracts, resp.order.order_id);
            resting.yes_bid_order_id = Some(resp.order.order_id);
            resting.yes_bid_price = Some(our_yes_bid);
            resting.yes_bid_qty = contracts;
        } else if let Some(Err(e)) = yes_res {
            error!("[MM] ❌ YES failed: {}", e);
        }

        if let Some(Ok(resp)) = no_res {
            info!("[MM] ✅ NO @{}¢ x{} | id={}", our_no_bid, contracts, resp.order.order_id);
            resting.no_bid_order_id = Some(resp.order.order_id);
            resting.no_bid_price = Some(our_no_bid);
            resting.no_bid_qty = contracts;
        } else if let Some(Err(e)) = no_res {
            error!("[MM] ❌ NO failed: {}", e);
        }
    }

    Ok(())
}

/// Buy EQUAL quantities of YES and NO simultaneously
/// STRATEGY: Both sides must be below max_price (e.g., 49¢)
/// If both are cheap, buy matching pairs. Guaranteed profit!
async fn execute_matched_pairs(
    client: &KalshiApiClient,
    market: &CryptoMarket,
    position: &mut Position,
    contracts: i64,
    max_price: i64,
) -> Result<bool> {
    let yes_ask = match market.yes_ask {
        Some(p) => p,
        None => return Ok(false),
    };
    let no_ask = match market.no_ask {
        Some(p) => p,
        None => return Ok(false),
    };

    // BOTH sides must be at or below max_price to buy
    if yes_ask > max_price || no_ask > max_price {
        info!("[SKIP] Prices above max: YES={}¢ NO={}¢ (max={}¢)", yes_ask, no_ask, max_price);
        return Ok(false);
    }

    let total_cost = yes_ask + no_ask;
    let profit_per_pair = 100 - total_cost; // $1 payout - cost

    info!("══════════════════════════════════════════════════════════════════════════════");
    info!("[BUY PAIR] 🎯 {} pairs on {}", contracts, market.ticker);
    info!("[BUY PAIR]    Market: {}", market.title);
    info!("[BUY PAIR]    YES @{}¢ + NO @{}¢ = {}¢ per pair", yes_ask, no_ask, total_cost);
    info!("[BUY PAIR]    Profit per pair: {}¢ (before fees)", profit_per_pair);
    info!("══════════════════════════════════════════════════════════════════════════════");

    // Buy YES and NO simultaneously with SAME quantity
    let (yes_result, no_result) = tokio::join!(
        client.buy_ioc(&market.ticker, "yes", yes_ask, contracts),
        client.buy_ioc(&market.ticker, "no", no_ask, contracts)
    );

    let (yes_filled, yes_cost) = match &yes_result {
        Ok(resp) => {
            let f = resp.order.filled_count();
            let c = resp.order.taker_fill_cost.unwrap_or(0);
            info!("[BUY PAIR]    YES: filled {} @{}¢ total", f, c);
            (f, c)
        }
        Err(e) => {
            error!("[BUY PAIR]    YES FAILED: {}", e);
            (0, 0)
        }
    };

    let (no_filled, no_cost) = match &no_result {
        Ok(resp) => {
            let f = resp.order.filled_count();
            let c = resp.order.taker_fill_cost.unwrap_or(0);
            info!("[BUY PAIR]    NO: filled {} @{}¢ total", f, c);
            (f, c)
        }
        Err(e) => {
            error!("[BUY PAIR]    NO FAILED: {}", e);
            (0, 0)
        }
    };

    // Update position
    position.yes_contracts += yes_filled;
    position.no_contracts += no_filled;
    position.yes_cost_cents += yes_cost;
    position.no_cost_cents += no_cost;

    let new_matched = yes_filled.min(no_filled);
    let total_matched = position.matched_contracts();

    info!("══════════════════════════════════════════════════════════════════════════════");
    if new_matched > 0 {
        let this_cost = yes_cost + no_cost;
        let this_value = new_matched * 100;
        let this_profit = this_value - this_cost;
        info!("[BUY PAIR] 💰 Got {} matched pairs this round!", new_matched);
        info!("[BUY PAIR]    Cost: {}¢ | Value: {}¢ | Profit: {}¢", this_cost, this_value, this_profit);
    }

    info!("[POSITION] Total: YES={} NO={} | Matched={} | Locked profit={}¢",
          position.yes_contracts, position.no_contracts,
          total_matched, position.locked_profit());

    if position.unmatched_yes() > 0 {
        warn!("[POSITION] ⚠️ {} unmatched YES", position.unmatched_yes());
    }
    if position.unmatched_no() > 0 {
        warn!("[POSITION] ⚠️ {} unmatched NO", position.unmatched_no());
    }
    info!("══════════════════════════════════════════════════════════════════════════════");

    Ok(new_matched > 0)
}

/// Run WebSocket connection and monitor for arbs (with auto-reconnect)
async fn run_ws(
    config: &KalshiConfig,
    state: Arc<RwLock<CryptoState>>,
    client: Arc<KalshiApiClient>,
    spot_prices: Arc<RwLock<PriceState>>,
    threshold_cents: i64,
    contracts: i64,
    dry_run: bool,
    target_price: i64,
    secs_into_mkt: i64,
    mut new_tickers_rx: mpsc::Receiver<Vec<String>>,
) -> Result<()> {
    // Outer reconnect loop
    loop {
        info!("[WS] 🔌 Connecting to Kalshi WebSocket...");

        let tickers = {
            let s = state.read().await;
            s.tickers()
        };

        if tickers.is_empty() {
            info!("[WS] No markets to monitor, waiting for discovery...");
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

        let ws_result = connect_async(request).await;
        let (ws_stream, _) = match ws_result {
            Ok(stream) => stream,
            Err(e) => {
                error!("[WS] ❌ Failed to connect: {}", e);
                info!("[WS] Reconnecting in 5 seconds...");
                tokio::time::sleep(Duration::from_secs(5)).await;
                continue;
            }
        };
        info!("[WS] Connected to Kalshi WebSocket");

        let (mut write, mut read) = ws_stream.split();

        // Subscribe to orderbook updates for all markets
        let subscribe_msg = SubscribeCmd {
            id: 1,
            cmd: "subscribe",
            params: SubscribeParams {
                channels: vec!["orderbook_delta"],
                market_tickers: tickers.clone(),
            },
        };

        if let Err(e) = write.send(Message::Text(serde_json::to_string(&subscribe_msg)?)).await {
            error!("[WS] Failed to subscribe to orderbook: {}", e);
            info!("[WS] Reconnecting in 5 seconds...");
            tokio::time::sleep(Duration::from_secs(5)).await;
            continue;
        }

        // Subscribe to fill notifications (our order fills)
        let fill_subscribe = serde_json::json!({
            "id": 2,
            "cmd": "subscribe",
            "params": {
                "channels": ["fill"]
            }
        });
        if let Err(e) = write.send(Message::Text(fill_subscribe.to_string())).await {
            error!("[WS] Failed to subscribe to fills: {}", e);
        } else {
            info!("[WS] Subscribed to fill notifications");
        }

        info!("[WS] Subscribed to {} markets", tickers.len());
        for t in &tickers {
            info!("[WS]    - {}", t);
        }

        let mut status_interval = tokio::time::interval(Duration::from_secs(2));
        let mut last_status = std::time::Instant::now();
        let mut ws_closed = false;

        // Inner message loop
        while !ws_closed {
            tokio::select! {
            // Handle new market subscriptions from discovery loop
            Some(new_tickers) = new_tickers_rx.recv() => {
                if !new_tickers.is_empty() {
                    info!("[WS] 📡 Subscribing to {} new markets: {:?}", new_tickers.len(), new_tickers);
                    let subscribe_msg = SubscribeCmd {
                        id: 2,
                        cmd: "subscribe",
                        params: SubscribeParams {
                            channels: vec!["orderbook_delta"],
                            market_tickers: new_tickers,
                        },
                    };
                    if let Err(e) = write.send(Message::Text(serde_json::to_string(&subscribe_msg)?)).await {
                        error!("[WS] Failed to subscribe to new markets: {}", e);
                    }
                }
            }
            _ = status_interval.tick() => {
                // Print periodic status every 2 seconds
                let s = state.read().await;
                let prices = spot_prices.read().await;
                let mut active_markets = 0;
                for (ticker, market) in s.markets.iter() {
                    let secs_remaining = parse_expiry_secs_remaining(ticker).unwrap_or(0);
                    // Skip expired markets (negative or very large time = wrapped around)
                    if secs_remaining <= 0 || secs_remaining > 900 {
                        continue;
                    }
                    active_markets += 1;
                    let spot = if ticker.contains("BTC") { prices.btc_price } else { prices.eth_price };
                    let mins = secs_remaining / 60;
                    let secs = secs_remaining % 60;

                    // Get position and resting orders for this market
                    let pos = s.get_position(ticker).cloned().unwrap_or_default();
                    let resting = s.get_resting_orders(ticker).cloned().unwrap_or_default();

                    // Our resting orders
                    let our_yes_bid = resting.yes_bid_price;
                    let our_no_bid = resting.no_bid_price;

                    // Format each orderbook with our bid shown
                    let yes_str = format!("YES: mkt={}/{} us={}",
                        market.yes_bid.map(|p| format!("{}¢", p)).unwrap_or("-".into()),
                        market.yes_ask.map(|p| format!("{}¢", p)).unwrap_or("-".into()),
                        our_yes_bid.map(|p| format!("{}¢", p)).unwrap_or("-".into()));
                    let no_str = format!("NO: mkt={}/{} us={}",
                        market.no_bid.map(|p| format!("{}¢", p)).unwrap_or("-".into()),
                        market.no_ask.map(|p| format!("{}¢", p)).unwrap_or("-".into()),
                        our_no_bid.map(|p| format!("{}¢", p)).unwrap_or("-".into()));

                    // Combined totals
                    let mkt_bid_total = match (market.yes_bid, market.no_bid) {
                        (Some(yb), Some(nb)) => yb + nb,
                        _ => 0,
                    };
                    let our_bid_total = match (our_yes_bid, our_no_bid) {
                        (Some(yb), Some(nb)) => yb + nb,
                        _ => 0,
                    };

                    // Position info
                    let pos_str = if pos.yes_contracts > 0 || pos.no_contracts > 0 {
                        let yes_avg = if pos.yes_contracts > 0 { pos.yes_cost_cents / pos.yes_contracts } else { 0 };
                        let no_avg = if pos.no_contracts > 0 { pos.no_cost_cents / pos.no_contracts } else { 0 };
                        format!("Pos: Y={}@{}¢ N={}@{}¢ profit={}¢",
                                pos.yes_contracts, yes_avg, pos.no_contracts, no_avg, pos.locked_profit())
                    } else {
                        "".to_string()
                    };

                    info!("[STATUS] {} | {}m{}s | {} | {} | Total: mkt={}¢ us={}¢ {}",
                          ticker, mins, secs, yes_str, no_str, mkt_bid_total, our_bid_total, pos_str);
                }
                if active_markets == 0 {
                    let now = chrono::Utc::now();
                    let mins_to_next = 15 - (now.minute() % 15);
                    info!("[IDLE] No active markets | Next window in ~{}m | BTC=${} | Waiting for discovery...",
                          mins_to_next,
                          prices.btc_price.map(|p| format!("{:.0}", p)).unwrap_or("-".into()));
                }
                last_status = std::time::Instant::now();
            }
            msg = read.next() => {
                let Some(msg) = msg else {
                    info!("[WS] Stream ended");
                    ws_closed = true;
                    continue;
                };
                match msg {
                    Ok(Message::Text(text)) => {
                if let Ok(ws_msg) = serde_json::from_str::<KalshiWsMessage>(&text) {
                    let ticker = ws_msg.msg.as_ref()
                        .and_then(|m| m.market_ticker.as_ref());

                    let Some(ticker) = ticker else { continue };

                    match ws_msg.msg_type.as_str() {
                        "orderbook_snapshot" | "orderbook_delta" => {
                            let is_snapshot = ws_msg.msg_type == "orderbook_snapshot";
                            if let Some(body) = &ws_msg.msg {
                                let mut s = state.write().await;

                                // Get position first (before mutable borrow)
                                let position = s.get_position(ticker).cloned().unwrap_or_default();
                                let yes_held = position.yes_contracts;
                                let no_held = position.no_contracts;

                                if let Some(market) = s.get_mut(ticker) {
                                    let _old_yes = market.yes_ask;
                                    let _old_no = market.no_ask;

                                    // Use snapshot vs delta processing
                                    if is_snapshot {
                                        process_snapshot(market, body);
                                        debug!("[SNAP] {} | YES book={} levels, NO book={} levels",
                                               ticker, market.yes_book.len(), market.no_book.len());
                                    } else {
                                        process_delta(market, body);
                                    }

                                    // Check time remaining
                                    let secs_remaining = parse_expiry_secs_remaining(ticker).unwrap_or(900);

                                    // Skip expired or invalid markets
                                    if secs_remaining <= 0 || secs_remaining > 900 {
                                        debug!("[SKIP] {} expired ({}s remaining)", ticker, secs_remaining);
                                        continue;
                                    }

                                    let mins_remaining = secs_remaining / 60;

                                    // Log orderbook update with bid/ask spread for BOTH orderbooks
                                    // YES orderbook: people bidding to BUY yes
                                    // NO orderbook: people bidding to BUY no
                                    let yes_bid_str = market.yes_bid.map(|v| format!("{}¢", v)).unwrap_or("-".into());
                                    let yes_ask_str = market.yes_ask.map(|v| format!("{}¢", v)).unwrap_or("-".into());
                                    let no_bid_str = market.no_bid.map(|v| format!("{}¢", v)).unwrap_or("-".into());
                                    let no_ask_str = market.no_ask.map(|v| format!("{}¢", v)).unwrap_or("-".into());

                                    // Calculate spread on each side
                                    let yes_spread = match (market.yes_bid, market.yes_ask) {
                                        (Some(b), Some(a)) => format!("{}¢", a - b),
                                        _ => "-".into(),
                                    };
                                    let no_spread = match (market.no_bid, market.no_ask) {
                                        (Some(b), Some(a)) => format!("{}¢", a - b),
                                        _ => "-".into(),
                                    };

                                    info!("[OB] {} | {}m{}s", ticker, mins_remaining, secs_remaining % 60);
                                    info!("     YES book: bid={} ask={} spread={}",
                                          yes_bid_str, yes_ask_str, yes_spread);
                                    info!("     NO  book: bid={} ask={} spread={}",
                                          no_bid_str, no_ask_str, no_spread);

                                    // If we can buy both sides
                                    if let (Some(ya), Some(na)) = (market.yes_ask, market.no_ask) {
                                        let take_total = ya + na;
                                        info!("     TAKER: YES@{}¢ + NO@{}¢ = {}¢ ({})",
                                              ya, na, take_total,
                                              if take_total < 100 { "PROFIT!" } else { "no arb" });
                                    }
                                    if let (Some(yb), Some(nb)) = (market.yes_bid, market.no_bid) {
                                        let bid_total = yb + nb;
                                        info!("     MAKER: bid YES@{}¢ + NO@{}¢ = {}¢ (target <100¢)",
                                              yb, nb, bid_total);
                                    }

                                    // === MARKET MAKING STRATEGY ===
                                    // Post resting bids on both YES and NO orderbooks
                                    // Goal: Get filled on both sides for total < 100¢
                                    if !dry_run {
                                        let market_clone = market.clone();
                                        let ticker_clone = ticker.to_string();
                                        let client_clone = client.clone();

                                        // Get resting orders for this market
                                        if let Some(resting) = s.get_resting_orders_mut(ticker) {
                                            let resting_clone = resting.clone();
                                            drop(s); // Release state lock before async call

                                            // Post/update market-making orders
                                            let mut resting_updated = resting_clone;
                                            if let Err(e) = post_market_making_orders(
                                                &client_clone,
                                                &ticker_clone,
                                                &market_clone,
                                                &mut resting_updated,
                                                contracts,
                                                98,  // Max total = 98¢ (2¢ profit target)
                                            ).await {
                                                error!("[MM] Error: {}", e);
                                            }

                                            // Update state with new resting orders
                                            let mut s = state.write().await;
                                            if let Some(resting) = s.get_resting_orders_mut(&ticker_clone) {
                                                *resting = resting_updated;
                                            }
                                        }
                                        continue; // Skip old IOC logic
                                    }

                                    // === OLD IOC LOGIC (for dry_run mode) ===
                                    let yes_ask = market.yes_ask.unwrap_or(999);
                                    let no_ask = market.no_ask.unwrap_or(999);
                                    let total = yes_ask + no_ask;
                                    let fair_value = 50;
                                    let late_in_market = secs_remaining <= (900 - secs_into_mkt);

                                    let prices = spot_prices.read().await;
                                    let spot = if ticker.contains("BTC") {
                                        prices.btc_price
                                    } else {
                                        prices.eth_price
                                    };
                                    drop(prices);

                                    let (price_near_strike, spot_diff_pct) = match (spot, market.strike) {
                                        (Some(s), Some(k)) => {
                                            let diff_pct = ((s - k) / k).abs() * 100.0;
                                            (diff_pct <= 0.02, Some(diff_pct)) // 0.02% threshold (2/10000)
                                        }
                                        _ => (!late_in_market, None), // If we don't know strike late in market, don't buy
                                    };

                                    // Calculate what we need to stay matched
                                    let need_yes = if no_held > yes_held { no_held - yes_held } else { 0 };
                                    let need_no = if yes_held > no_held { yes_held - no_held } else { 0 };
                                    let balanced = yes_held == no_held;

                                    // Entry conditions:
                                    // 0. VERY early (first 1 min): buy both at ≤40¢ (strike ≈ spot at launch, fair=50¢)
                                    // 1. Early in market (first secs_into_mkt seconds): buy if price < 50¢
                                    // 2. Total ≤ 99¢: ALWAYS buy both (guaranteed arb, any time)
                                    // 3. Later: only buy if spot near strike (delta ≈ 0.5) AND price < 50¢

                                    let total_arb = total <= 99;  // Guaranteed arb - buy anytime!
                                    let yes_cheap = yes_ask < fair_value;  // < 50¢
                                    let no_cheap = no_ask < fair_value;    // < 50¢

                                    // Very early = first 1 minute (60 seconds), when strike ≈ spot
                                    let very_early = secs_remaining > (900 - 60);
                                    // Early buy threshold = 40¢ (good deal when fair = 50¢)
                                    let early_threshold = 40;
                                    let yes_early_cheap = yes_ask <= early_threshold;
                                    let no_early_cheap = no_ask <= early_threshold;

                                    // Time check: early in market OR price near strike
                                    let time_ok = !late_in_market || price_near_strike;

                                    // Log entry conditions (each on separate line)
                                    let cond0 = very_early && (yes_early_cheap || no_early_cheap);  // Very early + ≤40¢
                                    let cond1 = !late_in_market && (yes_cheap || no_cheap);  // Early + cheap
                                    let cond2 = total_arb;                                     // Total ≤ 99¢
                                    let cond3 = late_in_market && price_near_strike && (yes_cheap || no_cheap);  // Late + near strike + cheap
                                    let yn = |b: bool| if b { "✓" } else { "✗" };
                                    info!("[COND] {} | YES={}¢ NO={}¢ Total={}¢", ticker, yes_ask, no_ask, total);
                                    info!("       C0 VeryEarly(1m)+≤40¢: {} | very_early={} ({}m{}s left) ≤40¢=(Y:{} N:{})",
                                          if cond0 { "✓ BUY" } else { "✗ skip" }, yn(very_early), mins_remaining, secs_remaining % 60, yn(yes_early_cheap), yn(no_early_cheap));
                                    info!("       C1 Early+cheap:       {} | early={} cheap=(Y:{} N:{})",
                                          if cond1 { "✓ BUY" } else { "✗ skip" }, yn(!late_in_market), yn(yes_cheap), yn(no_cheap));
                                    info!("       C2 Total≤99¢:         {} | total={}¢",
                                          if cond2 { "✓ BUY" } else { "✗ skip" }, total);
                                    let diff_str = spot_diff_pct.map(|d| format!("{:.3}%", d)).unwrap_or("-".into());
                                    info!("       C3 Late+near+cheap:   {} | late={} near_strike={} (diff={}, thresh=0.02%) cheap=(Y:{} N:{})",
                                          if cond3 { "✓ BUY" } else { "✗ skip" }, yn(late_in_market), yn(price_near_strike), diff_str, yn(yes_cheap), yn(no_cheap));

                                    // Decide what to buy:
                                    // - cond0: very early + ≤40¢ - strike ≈ spot at launch, buy both sides
                                    // - total_arb: buy both sides regardless of time (guaranteed profit)
                                    // - early OR near_strike: buy cheap side
                                    let buy_yes = total_arb
                                        || (very_early && yes_early_cheap && (balanced || need_yes > 0))
                                        || (time_ok && yes_cheap && (balanced || need_yes > 0));
                                    let buy_no = total_arb
                                        || (very_early && no_early_cheap && (balanced || need_no > 0))
                                        || (time_ok && no_cheap && (balanced || need_no > 0));

                                    // How many to buy
                                    let yes_to_buy = if buy_yes {
                                        if total_arb || balanced { contracts } else { need_yes.min(contracts) }
                                    } else { 0 };
                                    let no_to_buy = if buy_no {
                                        if total_arb || balanced { contracts } else { need_no.min(contracts) }
                                    } else { 0 };

                                    // Capture values before dropping the lock
                                    let market_strike = market.strike;
                                    let ticker_clone = ticker.clone();

                                    if buy_yes || buy_no {
                                        let market_clone = market.clone();

                                        warn!("══════════════════════════════════════════════════════════════════════════════");
                                        if total_arb {
                                            warn!("🚨 TOTAL ARB! YES+NO={}¢ < 100¢! {}", total, market_clone.title);
                                        } else {
                                            warn!("🎯 BUYING UNDER 50¢! {}", market_clone.title);
                                        }
                                        warn!("   ⏱️  {}m {}s remaining | Spot: {} | Strike: {}",
                                              mins_remaining, secs_remaining % 60,
                                              spot.map(|s| format!("{:.2}", s)).unwrap_or("-".into()),
                                              market_clone.strike.map(|s| format!("{:.0}", s)).unwrap_or("-".into()));
                                        warn!("   Position: YES={} NO={} | Matched={}", yes_held, no_held, yes_held.min(no_held));
                                        if buy_yes { warn!("   → BUY {} YES @{}¢", yes_to_buy, yes_ask); }
                                        if buy_no { warn!("   → BUY {} NO @{}¢", no_to_buy, no_ask); }
                                        warn!("══════════════════════════════════════════════════════════════════════════════");

                                        let client_clone = client.clone();

                                        // Determine which condition triggered the buy
                                        let trigger_condition = if cond2 { "C2:Total≤99¢" }
                                            else if cond0 { "C0:VeryEarly+≤40¢" }
                                            else if cond1 { "C1:Early+<50¢" }
                                            else if cond3 { "C3:Late+NearStrike+<50¢" }
                                            else { "Unknown" };

                                        drop(s); // Release lock

                                        if !dry_run {
                                            if buy_yes && yes_to_buy > 0 {
                                                let order_time = chrono::Utc::now();
                                                info!("[ORDER] 📤 Sending IOC BUY {} YES @{}¢ on {} at {}",
                                                      yes_to_buy, yes_ask, ticker_clone, order_time.format("%H:%M:%S%.3f"));
                                                match client_clone.buy_ioc(&ticker_clone, "yes", yes_ask, yes_to_buy).await {
                                                    Ok(resp) => {
                                                        let fill_time = chrono::Utc::now();
                                                        let latency_ms = (fill_time - order_time).num_milliseconds();
                                                        let filled = resp.order.filled_count();
                                                        let cost = resp.order.taker_fill_cost.unwrap_or(0);
                                                        let status = &resp.order.status;

                                                        // Write to per-market log file
                                                        log_order(&ticker_clone, "BUY", "YES", yes_ask, yes_to_buy,
                                                                  filled, cost, status, latency_ms,
                                                                  spot, market_strike, secs_remaining, trigger_condition);

                                                        if filled > 0 {
                                                            info!("[FILL] ✅ YES: {} filled @{}¢ total | status={} | latency={}ms",
                                                                  filled, cost, status, latency_ms);
                                                        } else {
                                                            warn!("[FILL] ⚠️ YES: 0 filled (liquidity gone) | wanted {} @{}¢ | status={} | latency={}ms",
                                                                  yes_to_buy, yes_ask, status, latency_ms);
                                                        }
                                                        if filled > 0 {
                                                            let mut s = state.write().await;
                                                            if let Some(pos) = s.get_position_mut(&ticker_clone) {
                                                                pos.yes_contracts += filled;
                                                                pos.yes_cost_cents += cost;
                                                            }
                                                        }
                                                    }
                                                    Err(e) => {
                                                        let latency_ms = (chrono::Utc::now() - order_time).num_milliseconds();
                                                        log_order(&ticker_clone, "BUY_ERROR", "YES", yes_ask, yes_to_buy,
                                                                  0, 0, &format!("error:{}", e), latency_ms,
                                                                  spot, market_strike, secs_remaining, trigger_condition);
                                                        error!("[FILL] ❌ YES failed: {} | latency={}ms", e, latency_ms);
                                                    }
                                                }
                                            }
                                            if buy_no && no_to_buy > 0 {
                                                let order_time = chrono::Utc::now();
                                                info!("[ORDER] 📤 Sending IOC BUY {} NO @{}¢ on {} at {}",
                                                      no_to_buy, no_ask, ticker_clone, order_time.format("%H:%M:%S%.3f"));
                                                match client_clone.buy_ioc(&ticker_clone, "no", no_ask, no_to_buy).await {
                                                    Ok(resp) => {
                                                        let fill_time = chrono::Utc::now();
                                                        let latency_ms = (fill_time - order_time).num_milliseconds();
                                                        let filled = resp.order.filled_count();
                                                        let cost = resp.order.taker_fill_cost.unwrap_or(0);
                                                        let status = &resp.order.status;

                                                        // Write to per-market log file
                                                        log_order(&ticker_clone, "BUY", "NO", no_ask, no_to_buy,
                                                                  filled, cost, status, latency_ms,
                                                                  spot, market_strike, secs_remaining, trigger_condition);

                                                        if filled > 0 {
                                                            info!("[FILL] ✅ NO: {} filled @{}¢ total | status={} | latency={}ms",
                                                                  filled, cost, status, latency_ms);
                                                            let mut s = state.write().await;
                                                            if let Some(pos) = s.get_position_mut(&ticker_clone) {
                                                                pos.no_contracts += filled;
                                                                pos.no_cost_cents += cost;
                                                            }
                                                        } else {
                                                            warn!("[FILL] ⚠️ NO: 0 filled (liquidity gone) | wanted {} @{}¢ | status={} | latency={}ms",
                                                                  no_to_buy, no_ask, status, latency_ms);
                                                        }
                                                    }
                                                    Err(e) => {
                                                        let latency_ms = (chrono::Utc::now() - order_time).num_milliseconds();
                                                        log_order(&ticker_clone, "BUY_ERROR", "NO", no_ask, no_to_buy,
                                                                  0, 0, &format!("error:{}", e), latency_ms,
                                                                  spot, market_strike, secs_remaining, trigger_condition);
                                                        error!("[FILL] ❌ NO failed: {} | latency={}ms", e, latency_ms);
                                                    }
                                                }
                                            }
                                            // Show position
                                            let s = state.read().await;
                                            if let Some(pos) = s.get_position(&ticker_clone) {
                                                info!("[POS] {} | Y={} N={} | Matched={} | Profit={}¢",
                                                      ticker_clone, pos.yes_contracts, pos.no_contracts,
                                                      pos.matched_contracts(), pos.locked_profit());
                                            }
                                        } else {
                                            if buy_yes { info!("[DRY] Would buy {} YES @{}¢ | {} | {}m {}s left | Spot={} Strike={}",
                                                              yes_to_buy, yes_ask, ticker_clone, mins_remaining, secs_remaining % 60,
                                                              spot.map(|s| format!("{:.0}", s)).unwrap_or("?".into()),
                                                              market_strike.map(|s| format!("{:.0}", s)).unwrap_or("?".into())); }
                                            if buy_no { info!("[DRY] Would buy {} NO @{}¢ | {} | {}m {}s left | Spot={} Strike={}",
                                                             no_to_buy, no_ask, ticker_clone, mins_remaining, secs_remaining % 60,
                                                             spot.map(|s| format!("{:.0}", s)).unwrap_or("?".into()),
                                                             market_strike.map(|s| format!("{:.0}", s)).unwrap_or("?".into())); }
                                        }
                                    } else if late_in_market && !price_near_strike && (yes_cheap || no_cheap) {
                                        // Would buy but too late and price moved from strike
                                        let (diff_str, pct_str) = match (spot, market_strike) {
                                            (Some(s), Some(k)) => {
                                                let diff = s - k;
                                                let pct = (diff / k) * 100.0;
                                                (format!("{:+.0}", diff), format!("{:+.2}%", pct))
                                            }
                                            _ => ("-".into(), "-".into()),
                                        };
                                        info!("[SKIP] {}m {}s left, price not near strike | YES={}¢ NO={}¢ | Spot={} Strike={} | Diff={} ({})",
                                              mins_remaining, secs_remaining % 60, yes_ask, no_ask,
                                              spot.map(|s| format!("{:.2}", s)).unwrap_or("-".into()),
                                              market_strike.map(|s| format!("{:.0}", s)).unwrap_or("-".into()),
                                              diff_str, pct_str);
                                    } else {
                                        // Prices not cheap enough
                                        info!("[WAIT] {}m {}s | YES={}¢ NO={}¢ (need <50¢ or total<100¢)",
                                              mins_remaining, secs_remaining % 60, yes_ask, no_ask);
                                    }
                                }
                            }
                        }
                        "fill" => {
                            // Order fill notification from our resting orders
                            info!("══════════════════════════════════════════════════════════════════════════════");
                            info!("[FILL] 🎯 Order filled via WebSocket!");
                            info!("[FILL] Raw message: {}", text);
                            info!("══════════════════════════════════════════════════════════════════════════════");

                            // Try to parse fill details from the raw JSON
                            // Expected fields: ticker, side, count, price, order_id, trade_id
                            if let Ok(fill_data) = serde_json::from_str::<serde_json::Value>(&text) {
                                if let Some(msg) = fill_data.get("msg") {
                                    let fill_ticker = msg.get("ticker").and_then(|v| v.as_str());
                                    let fill_side = msg.get("side").and_then(|v| v.as_str());
                                    let fill_count = msg.get("count").and_then(|v| v.as_i64()).unwrap_or(0);
                                    let fill_price = msg.get("price").and_then(|v| v.as_i64()).unwrap_or(0);
                                    let order_id = msg.get("order_id").and_then(|v| v.as_str());

                                    info!("[FILL] Parsed: ticker={:?} side={:?} count={} price={}¢ order_id={:?}",
                                          fill_ticker, fill_side, fill_count, fill_price, order_id);

                                    // Update position based on fill
                                    if let (Some(fill_ticker), Some(side)) = (fill_ticker, fill_side) {
                                        let ticker_owned = fill_ticker.to_string();
                                        let side_owned = side.to_string();

                                        // Get current market prices for hedging
                                        let market_clone = {
                                            let s = state.read().await;
                                            s.markets.get(fill_ticker).cloned()
                                        };

                                        {
                                            let mut s = state.write().await;
                                            if let Some(pos) = s.get_position_mut(fill_ticker) {
                                                let cost = fill_count * fill_price;
                                                if side == "yes" {
                                                    pos.yes_contracts += fill_count;
                                                    pos.yes_cost_cents += cost;
                                                    info!("[FILL] ✅ YES position updated: +{} @{}¢ = {}¢ | Total: Y={} N={}",
                                                          fill_count, fill_price, cost, pos.yes_contracts, pos.no_contracts);
                                                } else if side == "no" {
                                                    pos.no_contracts += fill_count;
                                                    pos.no_cost_cents += cost;
                                                    info!("[FILL] ✅ NO position updated: +{} @{}¢ = {}¢ | Total: Y={} N={}",
                                                          fill_count, fill_price, cost, pos.yes_contracts, pos.no_contracts);
                                                }

                                                // Check if we have matched pairs
                                                let matched = pos.matched_contracts();
                                                let profit = pos.locked_profit();
                                                if matched > 0 {
                                                    info!("[FILL] 💰 Matched pairs: {} | Locked profit: {}¢", matched, profit);
                                                }
                                            }

                                            // Clear the resting order since it filled
                                            if let Some(resting) = s.get_resting_orders_mut(fill_ticker) {
                                                if let Some(oid) = order_id {
                                                    if resting.yes_bid_order_id.as_deref() == Some(oid) {
                                                        info!("[FILL] Clearing YES resting order");
                                                        resting.yes_bid_order_id = None;
                                                        resting.yes_bid_price = None;
                                                    }
                                                    if resting.no_bid_order_id.as_deref() == Some(oid) {
                                                        info!("[FILL] Clearing NO resting order");
                                                        resting.no_bid_order_id = None;
                                                        resting.no_bid_price = None;
                                                    }
                                                }
                                            }
                                        }

                                        // No immediate hedge - just wait for other side to fill
                                        // Both our bids are resting, so we pay maker fees (0¢)
                                        // When both sides fill, we have a matched pair for profit
                                        info!("[FILL] Waiting for other side to fill (no IOC hedge - all resting orders)");
                                    }
                                }
                            }
                        }
                        other => {
                            debug!("[WS] Message type: {}", other);
                        }
                    }
                }
                    }
                    Ok(Message::Ping(data)) => {
                        debug!("[WS] Ping received");
                        let _ = write.send(Message::Pong(data)).await;
                    }
                    Ok(Message::Close(frame)) => {
                        warn!("[WS] Close frame received: {:?}", frame);
                        ws_closed = true;
                    }
                    Err(e) => {
                        error!("[WS] Error: {}", e);
                        ws_closed = true;
                    }
                    _ => {}
                }
            }
        }
        } // end inner while loop

        // Reconnect after disconnect
        info!("[WS] Reconnecting in 5 seconds...");
        tokio::time::sleep(Duration::from_secs(5)).await;
    } // end outer reconnect loop
}

/// Print current market summary
fn print_summary(state: &CryptoState, threshold_cents: i64, target_price: i64) {
    info!("══════════════════════════════════════════════════════════════════════════════════════════════════════════════");
    let positions_with_contracts: usize = state.positions.values()
        .filter(|p| p.yes_contracts > 0 || p.no_contracts > 0).count();
    info!("📊 MARKET SUMMARY | threshold={}¢ | fair=50¢ | positions={}",
          threshold_cents, positions_with_contracts);
    info!("══════════════════════════════════════════════════════════════════════════════════════════════════════════════");
    info!("{:45} {:>6} {:>6} {:>6} {:>6} {:>6} {:>6} {:>5}   {}",
          "Market", "YES", "NO", "FairY", "FairN", "Total", "Profit", "Edge", "Status");
    info!("──────────────────────────────────────────────────────────────────────────────────────────────────────────────");

    let mut markets: Vec<_> = state.markets.values().collect();
    markets.sort_by_key(|m| m.total_cost_cents().unwrap_or(999));

    for market in markets {
        let yes = market.yes_ask.map(|p| format!("{}", p)).unwrap_or("-".to_string());
        let no = market.no_ask.map(|p| format!("{}", p)).unwrap_or("-".to_string());
        let fair_yes = market.yes_fair_cents.map(|p| format!("{}", p)).unwrap_or("50".to_string());
        let fair_no = market.no_fair_cents.map(|p| format!("{}", p)).unwrap_or("50".to_string());
        let total_str = market.total_cost_cents().map(|c| format!("{}", c)).unwrap_or("-".to_string());
        let profit = market.profit_cents().map(|p| format!("{}", p)).unwrap_or("-".to_string());
        let edge = market.edge_cents().map(|e| format!("{:+}", e)).unwrap_or("-".to_string());

        // If one side is missing, infer from the other (YES + NO ≈ 100¢)
        let yes_ask = market.yes_ask.unwrap_or_else(|| {
            market.no_ask.map(|no| 100 - no).unwrap_or(999)
        });
        let no_ask = market.no_ask.unwrap_or_else(|| {
            market.yes_ask.map(|yes| 100 - yes).unwrap_or(999)
        });
        let total = yes_ask + no_ask;
        let status = if total <= 99 {
            "🚨 ARB!"
        } else if yes_ask < 50 || no_ask < 50 {
            "💰 <50¢"
        } else if market.yes_ask.is_some() && market.no_ask.is_some() {
            "📈 LIVE"
        } else {
            "⏳ WAIT"
        };

        info!("{:45} {:>6} {:>6} {:>6} {:>6} {:>6} {:>6} {:>5} {:>8}",
              &market.title[..market.title.len().min(45)],
              yes, no, fair_yes, fair_no, total_str, profit, edge, status);
    }
    info!("══════════════════════════════════════════════════════════════════════════════════════════════════════════════");
    info!("NOTE: At market launch, strike ≈ spot (ATM), so fair value ≈ 50¢/50¢. Edge = fair - market price.");
}

/// Periodically rediscover markets to find new 15-minute windows
async fn discovery_loop(
    client: Arc<KalshiApiClient>,
    state: Arc<RwLock<CryptoState>>,
    new_tickers_tx: mpsc::Sender<Vec<String>>,
) {
    let mut interval = tokio::time::interval(Duration::from_secs(30)); // Check every 30 seconds
    loop {
        interval.tick().await;
        debug!("[DISCOVERY] Checking for new markets...");

        match discover_crypto_markets(&client).await {
            Ok(new_markets) => {
                let mut s = state.write().await;
                let mut new_tickers = Vec::new();
                for market in new_markets {
                    if !s.markets.contains_key(&market.ticker) {
                        info!("[DISCOVERY] New market found: {} - {}", market.ticker, market.title);
                        new_tickers.push(market.ticker.clone());
                        s.add_market(market);
                    }
                }
                drop(s); // Release lock before sending
                if !new_tickers.is_empty() {
                    info!("[DISCOVERY] Added {} new markets, notifying WebSocket", new_tickers.len());
                    if let Err(e) = new_tickers_tx.send(new_tickers).await {
                        error!("[DISCOVERY] Failed to notify WS of new markets: {}", e);
                    }
                }
            }
            Err(e) => {
                error!("[DISCOVERY] Error: {}", e);
            }
        }
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    // Parse CLI arguments
    let args = Args::parse();

    // Initialize logging - show everything
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::from_default_env()
                .add_directive("kalshi_crypto_arb=info".parse().unwrap())
                .add_directive("arb_bot=info".parse().unwrap()),
        )
        .init();

    info!("════════════════════════════════════════════════════════════════════════");
    info!("🪙 KALSHI CRYPTO MARKET MAKER BOT");
    info!("════════════════════════════════════════════════════════════════════════");
    info!("STRATEGY:");
    info!("   1. Monitor BTC/ETH 15-minute price markets");
    info!("   2. Post resting bids on both YES and NO orderbooks");
    info!("   3. Target profit: {}¢ per matched pair (bid total = {}¢)", args.profit, 100 - args.profit);
    info!("   4. When both sides fill: guaranteed profit!");
    info!("════════════════════════════════════════════════════════════════════════");

    // Configuration from CLI args
    let dry_run = !args.live;
    let threshold_cents = args.threshold;
    let contracts = args.contracts;
    let target_price = (100 - args.profit) / 2;  // Split target evenly
    let secs_into_mkt = args.secs_into_mkt;

    info!("CONFIGURATION:");
    info!("   Mode: {}", if dry_run { "🔍 MONITOR ONLY (use --live to trade)" } else { "🚀 LIVE TRADING" });
    info!("   Contracts: {} per side", contracts);
    info!("   Target profit: {}¢ (bid total ≤ {}¢)", args.profit, 100 - args.profit);
    info!("   Threshold: {}¢ (arb if total cost < {}¢)", threshold_cents, threshold_cents);
    info!("   Blind buy window: {}s (after this, require price near strike)", secs_into_mkt);
    info!("════════════════════════════════════════════════════════════════════════");

    if !dry_run {
        warn!("⚠️  LIVE TRADING ENABLED - REAL MONEY WILL BE USED!");
        warn!("⚠️  Press Ctrl+C within 2.5 seconds to cancel...");
        tokio::time::sleep(Duration::from_millis(2500)).await;
        info!("✅ Starting live trading...");
    }

    // Load Kalshi credentials
    let config = KalshiConfig::from_env()?;
    let client = Arc::new(KalshiApiClient::new(KalshiConfig::from_env()?));
    info!("[KALSHI] ✅ API credentials loaded");

    // Cancel any stale orders from previous runs
    if !dry_run {
        info!("[BOOT] Cancelling all existing orders...");
        match client.cancel_all_orders(None).await {
            Ok(count) => {
                if count > 0 {
                    warn!("[BOOT] ✅ Cancelled {} stale orders from previous run", count);
                } else {
                    info!("[BOOT] ✅ No existing orders to cancel");
                }
            }
            Err(e) => {
                warn!("[BOOT] ⚠️ Failed to cancel existing orders: {}", e);
            }
        }
    }

    // Discover markets
    info!("Discovering crypto markets...");
    let markets = discover_crypto_markets(&client).await?;
    info!("Found {} crypto markets", markets.len());

    if markets.is_empty() {
        warn!("❌ No crypto markets found! Markets may not be open.");
        warn!("   BTC/ETH 15-minute markets open during active trading hours.");
        warn!("   Will keep checking for new markets...");
    }

    // Build state
    let state = Arc::new(RwLock::new({
        let mut s = CryptoState::new();
        for market in markets {
            s.add_market(market);
        }
        s
    }));

    // Start Polygon price feed for live BTC/ETH prices
    let spot_prices = Arc::new(RwLock::new(PriceState::default()));
    let spot_prices_clone = spot_prices.clone();
    tokio::spawn(async move {
        run_polygon_feed(spot_prices_clone, POLYGON_API_KEY).await;
    });
    info!("[POLYGON] 📊 Started BTC/ETH price feed");

    // Print initial summary
    {
        let s = state.read().await;
        print_summary(&s, threshold_cents, target_price);
    }

    // Channel for notifying WebSocket of new markets to subscribe to
    let (new_tickers_tx, new_tickers_rx) = mpsc::channel::<Vec<String>>(10);

    // Start periodic market discovery (finds new 15-min windows)
    let discovery_client = client.clone();
    let discovery_state = state.clone();
    tokio::spawn(async move {
        discovery_loop(discovery_client, discovery_state, new_tickers_tx).await;
    });


    // Heartbeat task - print summary every 30 seconds
    let heartbeat_state = state.clone();
    let heartbeat_spot = spot_prices.clone();
    let heartbeat_threshold = threshold_cents;
    let heartbeat_target = target_price;
    tokio::spawn(async move {
        let mut interval = tokio::time::interval(Duration::from_secs(30));
        loop {
            interval.tick().await;
            let s = heartbeat_state.read().await;
            let prices = heartbeat_spot.read().await;
            if let Some(btc) = prices.btc_price {
                info!("[SPOT] BTC: ${:.2} (last trade)", btc);
            }
            if let Some(eth) = prices.eth_price {
                info!("[SPOT] ETH: ${:.2} (last trade)", eth);
            }
            print_summary(&s, heartbeat_threshold, heartbeat_target);
        }
    });

    // Run WebSocket with auto-reconnect (receiver passed once, reconnects handled inside)
    info!("[WS] 🔌 Starting WebSocket connection manager...");
    run_ws(&config, state.clone(), client.clone(), spot_prices.clone(), threshold_cents, contracts, dry_run, target_price, secs_into_mkt, new_tickers_rx).await?;
    Ok(())
}
