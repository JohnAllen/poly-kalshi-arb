//! Polymarket Pair Accumulator Bot
//!
//! STRATEGY:
//! - Buy YES and NO at market price + 1c (crossing the spread)
//! - Keep equal numbers of shares on each side
//! - Increment by CLI arg amount (e.g., 10 contracts at a time)
//! - Stop when: avg_price <= 99c AND total_contracts >= target
//!
//! Guaranteed profit if YES + NO cost < $1 total.
//!
//! Usage:
//!   cargo run --release --bin poly_pair_accumulator -- --target 100 --increment 10
//!
//! Environment:
//!   POLY_PRIVATE_KEY - Your Polymarket/Polygon wallet private key
//!   POLY_FUNDER - Your funder address (proxy wallet)

use anyhow::{Context, Result};
use clap::Parser;
use futures_util::{SinkExt, StreamExt};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::RwLock;
use tokio_tungstenite::{connect_async, tungstenite::Message};
use tracing::{debug, error, info, warn};

use arb_bot::polymarket_clob::{PolymarketAsyncClient, PreparedCreds, SharedAsyncClient};
use arb_bot::polymarket_markets::{discover_all_markets, PolyMarket};

/// Polymarket Pair Accumulator Bot
#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Args {
    /// Target total contracts (matched pairs) to accumulate
    #[arg(short, long, default_value_t = 100.0)]
    target: f64,

    /// Number of contracts to buy per increment (on each side)
    #[arg(short, long, default_value_t = 10.0)]
    increment: f64,

    /// Max average price in cents to stop (e.g., 99 = stop if avg <= 99c)
    #[arg(long, default_value_t = 99)]
    max_avg_price: i64,

    /// Spread to add on top of market price in cents (default: 1c)
    #[arg(long, default_value_t = 1)]
    spread: i64,

    /// Asset symbol to trade: btc, eth, sol, xrp
    #[arg(long)]
    sym: Option<String>,

    /// Specific market slug (optional)
    #[arg(long)]
    market: Option<String>,

    /// Minimum minutes remaining to trade (default: 2)
    #[arg(long, default_value_t = 2)]
    min_minutes: i64,

    /// Maximum minutes remaining to trade (default: 14)
    #[arg(long, default_value_t = 14)]
    max_minutes: i64,

    /// Delay between buy attempts in milliseconds (default: 2000)
    #[arg(long, default_value_t = 2000)]
    delay_ms: u64,

    /// Live trading mode (default is dry run)
    #[arg(short, long, default_value_t = false)]
    live: bool,
}

const POLYMARKET_WS_URL: &str = "wss://ws-subscriptions-clob.polymarket.com/ws/market";

/// Market state
#[derive(Debug, Clone)]
#[allow(dead_code)]
struct Market {
    condition_id: String,
    question: String,
    yes_token: String,
    no_token: String,
    asset: String,
    expiry_minutes: Option<f64>,
    discovered_at: std::time::Instant,
    // Orderbook
    yes_ask: Option<i64>,
    yes_bid: Option<i64>,
    no_ask: Option<i64>,
    no_bid: Option<i64>,
}

impl Market {
    fn from_polymarket(pm: PolyMarket) -> Self {
        Self {
            condition_id: pm.condition_id,
            question: pm.question,
            yes_token: pm.yes_token,
            no_token: pm.no_token,
            asset: pm.asset,
            expiry_minutes: pm.expiry_minutes,
            discovered_at: std::time::Instant::now(),
            yes_ask: None,
            yes_bid: None,
            no_ask: None,
            no_bid: None,
        }
    }

    fn time_remaining_mins(&self) -> Option<f64> {
        self.expiry_minutes.map(|exp| {
            let elapsed_mins = self.discovered_at.elapsed().as_secs_f64() / 60.0;
            (exp - elapsed_mins).max(0.0)
        })
    }
}

/// Position tracking
#[derive(Debug, Clone, Default)]
#[allow(dead_code)]
struct Position {
    yes_qty: f64,
    no_qty: f64,
    yes_cost: f64,
    no_cost: f64,
}

#[allow(dead_code)]
impl Position {
    fn matched(&self) -> f64 {
        self.yes_qty.min(self.no_qty)
    }

    fn total_cost(&self) -> f64 {
        self.yes_cost + self.no_cost
    }

    fn total_qty(&self) -> f64 {
        self.yes_qty + self.no_qty
    }

    /// Average price per contract in cents (across both sides)
    /// avg_price = total_cost / total_qty * 100
    fn avg_price_cents(&self) -> i64 {
        let total = self.total_qty();
        if total > 0.0 {
            (self.total_cost() / total * 100.0).round() as i64
        } else {
            0
        }
    }

    /// Combined average price: what you pay for 1 YES + 1 NO
    fn combined_avg_cents(&self) -> i64 {
        let matched = self.matched();
        if matched > 0.0 {
            ((self.yes_cost + self.no_cost) / matched * 100.0).round() as i64
        } else {
            0
        }
    }
}

/// Global state
struct State {
    markets: HashMap<String, Market>,
    positions: HashMap<String, Position>,
}

impl State {
    fn new() -> Self {
        Self {
            markets: HashMap::new(),
            positions: HashMap::new(),
        }
    }
}

// === Polymarket WebSocket ===

#[derive(Deserialize, Debug)]
struct BookSnapshot {
    asset_id: String,
    bids: Vec<PriceLevel>,
    asks: Vec<PriceLevel>,
}

#[derive(Deserialize, Debug)]
#[allow(dead_code)]
struct PriceLevel {
    price: String,
    size: String,
}

#[derive(Serialize)]
struct SubscribeCmd {
    assets_ids: Vec<String>,
    #[serde(rename = "type")]
    sub_type: &'static str,
}

fn parse_price_cents(s: &str) -> i64 {
    s.parse::<f64>()
        .map(|p| (p * 100.0).round() as i64)
        .unwrap_or(0)
}

// === Main ===

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::from_default_env()
                .add_directive("poly_pair_accumulator=info".parse().unwrap())
                .add_directive("arb_bot=info".parse().unwrap()),
        )
        .with_target(false)
        .with_level(false)
        .with_timer(tracing_subscriber::fmt::time::LocalTime::new(
            time::macros::format_description!("[hour]:[minute]:[second]"),
        ))
        .init();

    let args = Args::parse();

    println!("═══════════════════════════════════════════════════════════════════════");
    println!("POLYMARKET PAIR ACCUMULATOR BOT");
    println!("═══════════════════════════════════════════════════════════════════════");
    println!("Mode: {}  |  Target: {:.0} matched pairs",
             if args.live { "LIVE" } else { "DRY RUN" }, args.target);
    println!("Increment: {:.0} contracts  |  Spread: +{}c  |  Max avg: {}c",
             args.increment, args.spread, args.max_avg_price);
    println!("Time window: {}m - {}m  |  Delay: {}ms",
             args.min_minutes, args.max_minutes, args.delay_ms);
    if let Some(ref sym) = args.sym {
        println!("Asset filter: {}", sym.to_uppercase());
    }
    println!("═══════════════════════════════════════════════════════════════════════");

    dotenvy::dotenv().ok();
    let private_key = std::env::var("POLY_PRIVATE_KEY").context("POLY_PRIVATE_KEY not set")?;
    let funder = std::env::var("POLY_FUNDER").context("POLY_FUNDER not set")?;

    let poly_client = PolymarketAsyncClient::new("https://clob.polymarket.com", 137, &private_key, &funder)?;

    println!("Deriving API credentials...");
    let api_creds = poly_client.derive_api_key(0).await?;
    let prepared_creds = PreparedCreds::from_api_creds(&api_creds)?;
    let shared_client = Arc::new(SharedAsyncClient::new(poly_client, prepared_creds, 137));

    if args.live {
        println!("*** LIVE MODE - Real money! ***");
        tokio::time::sleep(Duration::from_secs(3)).await;
    }

    // Discover markets
    println!("Discovering markets...");
    let mut discovered = discover_all_markets(args.market.as_deref()).await?;

    // Filter by asset symbol if specified
    if let Some(ref sym) = args.sym {
        let sym_upper = sym.to_uppercase();
        discovered.retain(|m| m.asset == sym_upper);
    }

    println!("Found {} markets", discovered.len());
    for m in &discovered {
        println!("  {} | {:.1?}min | {}", m.asset, m.expiry_minutes, &m.question[..m.question.len().min(50)]);
    }

    if discovered.is_empty() {
        println!("No markets found!");
        return Ok(());
    }

    // Initialize state
    let state = Arc::new(RwLock::new({
        let mut s = State::new();
        for pm in discovered {
            let id = pm.condition_id.clone();
            s.positions.insert(id.clone(), Position::default());
            s.markets.insert(id, Market::from_polymarket(pm));
        }
        s
    }));

    let target_contracts = args.target;
    let increment = args.increment;
    let max_avg_price = args.max_avg_price;
    let spread = args.spread;
    let min_minutes = args.min_minutes as f64;
    let max_minutes = args.max_minutes as f64;
    let delay_ms = args.delay_ms;
    let dry_run = !args.live;

    loop {
        // Get tokens to subscribe
        let tokens: Vec<String> = {
            let s = state.read().await;
            s.markets
                .values()
                .flat_map(|m| vec![m.yes_token.clone(), m.no_token.clone()])
                .collect()
        };

        if tokens.is_empty() {
            warn!("No tokens to subscribe to");
            tokio::time::sleep(Duration::from_secs(5)).await;
            continue;
        }

        info!("[WS] Connecting to Polymarket...");

        let (ws, _) = match connect_async(POLYMARKET_WS_URL).await {
            Ok(ws) => ws,
            Err(e) => {
                error!("[WS] Connect failed: {}", e);
                tokio::time::sleep(Duration::from_secs(5)).await;
                continue;
            }
        };

        let (mut write, mut read) = ws.split();

        // Subscribe
        let sub = SubscribeCmd {
            assets_ids: tokens.clone(),
            sub_type: "market",
        };
        let _ = write.send(Message::Text(serde_json::to_string(&sub)?)).await;
        info!("[WS] Subscribed to {} tokens", tokens.len());

        let mut ping_interval = tokio::time::interval(Duration::from_secs(30));
        let mut status_interval = tokio::time::interval(Duration::from_secs(2));
        let mut trade_interval = tokio::time::interval(Duration::from_millis(delay_ms));
        trade_interval.tick().await; // Skip first tick

        loop {
            tokio::select! {
                _ = ping_interval.tick() => {
                    if let Err(e) = write.send(Message::Ping(vec![])).await {
                        error!("[WS] Ping failed: {}", e);
                        break;
                    }
                }

                _ = status_interval.tick() => {
                    let s = state.read().await;

                    for (id, market) in &s.markets {
                        let pos = s.positions.get(id).cloned().unwrap_or_default();
                        let expiry = market.time_remaining_mins().unwrap_or(0.0);

                        let yes_ask = market.yes_ask.unwrap_or(100);
                        let no_ask = market.no_ask.unwrap_or(100);
                        let yes_bid = market.yes_bid.unwrap_or(0);
                        let no_bid = market.no_bid.unwrap_or(0);
                        let combined_ask = yes_ask + no_ask;

                        let matched = pos.matched();
                        let combined_avg = pos.combined_avg_cents();
                        let profit_cents = if matched > 0.0 { 100 - combined_avg } else { 0 };

                        // Filter by time window
                        let in_window = expiry >= min_minutes && expiry <= max_minutes;
                        let window_str = if in_window { "ACTIVE" } else if expiry < min_minutes { "EXPIRING" } else { "WAITING" };

                        // Check stop condition
                        let stop_reached = matched >= target_contracts && combined_avg <= max_avg_price;
                        let stop_str = if stop_reached { " DONE" } else { "" };

                        info!("[{}] {} | Y={}/{}c N={}/{}c comb={}c | {:.1}m {} | matched={:.0}/{:.0} avg={}c profit=+{}c${}",
                              market.asset,
                              window_str,
                              yes_bid, yes_ask,
                              no_bid, no_ask,
                              combined_ask,
                              expiry,
                              window_str,
                              matched, target_contracts,
                              combined_avg, profit_cents,
                              stop_str);
                    }
                }

                _ = trade_interval.tick() => {
                    // Check each market for trading opportunity
                    let trade_ops: Vec<(String, String, String, i64, i64, String)> = {
                        let s = state.read().await;
                        s.markets.iter().filter_map(|(id, m)| {
                            let exp = m.time_remaining_mins().unwrap_or(0.0);
                            if exp < min_minutes || exp > max_minutes { return None; }

                            let pos = s.positions.get(id).cloned().unwrap_or_default();
                            let matched = pos.matched();
                            let combined_avg = pos.combined_avg_cents();

                            // Check stop condition: matched >= target AND avg <= max_avg
                            if matched >= target_contracts && combined_avg <= max_avg_price {
                                debug!("[{}] Stop reached: {:.0} matched, avg {}c", m.asset, matched, combined_avg);
                                return None;
                            }

                            let ya = m.yes_ask.unwrap_or(100);
                            let na = m.no_ask.unwrap_or(100);

                            // Skip if no valid orderbook
                            if ya == 100 && na == 100 { return None; }

                            Some((id.clone(), m.yes_token.clone(), m.no_token.clone(), ya, na, m.asset.clone()))
                        }).collect()
                    };

                    for (market_id, yes_token, no_token, yes_ask, no_ask, asset) in trade_ops {
                        let combined = yes_ask + no_ask;

                        // Calculate entry prices (market + spread)
                        let yes_entry = (yes_ask + spread).min(99);
                        let no_entry = (no_ask + spread).min(99);
                        let yes_price = yes_entry as f64 / 100.0;
                        let no_price = no_entry as f64 / 100.0;

                        // Polymarket requires minimum $1 order value
                        // Adjust increment if needed to meet minimum
                        let min_contracts_yes = (1.0 / yes_price).ceil();
                        let min_contracts_no = (1.0 / no_price).ceil();
                        let actual_increment = increment.max(min_contracts_yes).max(min_contracts_no);

                        let yes_cost = actual_increment * yes_price;
                        let no_cost = actual_increment * no_price;
                        let total_cost = yes_cost + no_cost;
                        let combined_entry = yes_entry + no_entry;

                        if dry_run {
                            info!("[{}] DRY RUN: BUY {:.0} YES@{}c + {:.0} NO@{}c | asks Y={}c N={}c | comb={}c -> entry={}c | cost=${:.2}",
                                  asset, actual_increment, yes_entry, actual_increment, no_entry,
                                  yes_ask, no_ask, combined, combined_entry, total_cost);
                        } else {
                            info!("[{}] BUY {:.0} YES@{}c + {:.0} NO@{}c | asks Y={}c N={}c | comb={}c -> entry={}c | cost=${:.2}",
                                  asset, actual_increment, yes_entry, actual_increment, no_entry,
                                  yes_ask, no_ask, combined, combined_entry, total_cost);

                            // Buy YES
                            let client = shared_client.clone();
                            let state_clone = state.clone();
                            let market_id_clone = market_id.clone();
                            let asset_clone = asset.clone();
                            let yes_token_clone = yes_token.clone();

                            match client.buy_fak(&yes_token_clone, yes_price, actual_increment).await {
                                Ok(fill) if fill.filled_size > 0.0 => {
                                    let fill_price = (fill.fill_cost / fill.filled_size * 100.0).round() as i64;
                                    info!("[{}] YES FILLED {:.1}@{}c (${:.2})", asset_clone, fill.filled_size, fill_price, fill.fill_cost);
                                    let mut s = state_clone.write().await;
                                    if let Some(pos) = s.positions.get_mut(&market_id_clone) {
                                        pos.yes_qty += fill.filled_size;
                                        pos.yes_cost += fill.fill_cost;
                                    }
                                }
                                Ok(_) => {
                                    info!("[{}] YES no fill @{}c", asset_clone, yes_entry);
                                }
                                Err(e) => {
                                    error!("[{}] YES error: {}", asset_clone, e);
                                }
                            }

                            // Buy NO
                            match shared_client.buy_fak(&no_token, no_price, actual_increment).await {
                                Ok(fill) if fill.filled_size > 0.0 => {
                                    let fill_price = (fill.fill_cost / fill.filled_size * 100.0).round() as i64;
                                    info!("[{}] NO FILLED {:.1}@{}c (${:.2})", asset, fill.filled_size, fill_price, fill.fill_cost);
                                    let mut s = state.write().await;
                                    if let Some(pos) = s.positions.get_mut(&market_id) {
                                        pos.no_qty += fill.filled_size;
                                        pos.no_cost += fill.fill_cost;
                                    }
                                }
                                Ok(_) => {
                                    info!("[{}] NO no fill @{}c", asset, no_entry);
                                }
                                Err(e) => {
                                    error!("[{}] NO error: {}", asset, e);
                                }
                            }

                            // Print updated position
                            let s = state.read().await;
                            if let Some(pos) = s.positions.get(&market_id) {
                                let matched = pos.matched();
                                let combined_avg = pos.combined_avg_cents();
                                let profit = if matched > 0.0 { 100 - combined_avg } else { 0 };
                                info!("[{}] Position: Y={:.0} N={:.0} matched={:.0} avg={}c profit=+{}c | target={:.0}",
                                      asset, pos.yes_qty, pos.no_qty, matched, combined_avg, profit, target_contracts);
                            }
                        }
                    }
                }

                msg = read.next() => {
                    let Some(msg) = msg else { break; };

                    match msg {
                        Ok(Message::Text(text)) => {
                            // Parse orderbook updates
                            let books: Option<Vec<BookSnapshot>> = serde_json::from_str::<Vec<BookSnapshot>>(&text).ok()
                                .or_else(|| serde_json::from_str::<BookSnapshot>(&text).ok().map(|b| vec![b]));

                            if let Some(books) = books {
                                for book in books {
                                    let mut s = state.write().await;

                                    let market_id = s.markets.iter()
                                        .find(|(_, m)| m.yes_token == book.asset_id || m.no_token == book.asset_id)
                                        .map(|(id, _)| id.clone());

                                    let Some(market_id) = market_id else { continue };

                                    let best_ask = book.asks.iter()
                                        .filter_map(|l| {
                                            let price = parse_price_cents(&l.price);
                                            if price > 0 { Some(price) } else { None }
                                        })
                                        .min();

                                    let best_bid = book.bids.iter()
                                        .filter_map(|l| {
                                            let price = parse_price_cents(&l.price);
                                            if price > 0 { Some(price) } else { None }
                                        })
                                        .max();

                                    let Some(market) = s.markets.get_mut(&market_id) else { continue };

                                    let is_yes = book.asset_id == market.yes_token;

                                    if is_yes {
                                        market.yes_ask = best_ask;
                                        market.yes_bid = best_bid;
                                    } else {
                                        market.no_ask = best_ask;
                                        market.no_bid = best_bid;
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

        warn!("[WS] Disconnected, reconnecting in 5s...");
        tokio::time::sleep(Duration::from_secs(5)).await;
    }
}
