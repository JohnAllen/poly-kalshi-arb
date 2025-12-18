//! Polymarket Momentum Front-Runner
//!
//! STRATEGY:
//! - Monitor real-time crypto prices via Polygon.io websocket
//! - Detect rapid price movements (spikes/drops)
//! - Front-run the market by buying YES (price up) or NO (price down)
//! - Execute before market makers adjust their quotes
//!
//! The edge comes from:
//! 1. Faster price feed than most participants
//! 2. Quick execution via FAK orders
//! 3. Market makers slow to adjust quotes after sudden moves
//!
//! Usage:
//!   RUST_LOG=info cargo run --release --bin poly_momentum
//!
//! Environment:
//!   POLY_PRIVATE_KEY - Your Polymarket/Polygon wallet private key
//!   POLY_FUNDER - Your funder address (proxy wallet)
//!   POLYGON_API_KEY - Polygon.io API key for price feed

use anyhow::{Context, Result};
use chrono::Utc;
use futures_util::{SinkExt, StreamExt};
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, VecDeque};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::RwLock;
use tokio_tungstenite::{connect_async, tungstenite::Message};
use tracing::{error, info, warn};

use arb_bot::polymarket_clob::{PolymarketAsyncClient, SharedAsyncClient, PreparedCreds};

/// CLI Arguments
#[derive(clap::Parser, Debug)]
#[command(author, version, about = "Polymarket Momentum Front-Runner")]
struct Args {
    /// Size per trade in dollars
    #[arg(short, long, default_value_t = 25.0)]
    size: f64,

    /// Price move threshold in basis points to trigger (default: 15 = 0.15%)
    #[arg(short, long, default_value_t = 15)]
    threshold_bps: i64,

    /// Lookback window in seconds for detecting moves
    #[arg(short, long, default_value_t = 5)]
    window_secs: u64,

    /// Minimum edge in cents vs market price (default: 3)
    #[arg(short, long, default_value_t = 3)]
    edge: i64,

    /// Cooldown between trades on same market (seconds)
    #[arg(long, default_value_t = 30)]
    cooldown: u64,

    /// Live trading mode (default is dry run)
    #[arg(short, long, default_value_t = false)]
    live: bool,

    /// Specific asset to trade (BTC, ETH, SOL, XRP) - trades all if not set
    #[arg(long)]
    asset: Option<String>,
}

const POLYMARKET_WS_URL: &str = "wss://ws-subscriptions-clob.polymarket.com/ws/market";
const GAMMA_API_BASE: &str = "https://gamma-api.polymarket.com";
const POLYGON_WS_URL: &str = "wss://socket.polygon.io/crypto";

/// Price tick with timestamp
#[derive(Debug, Clone)]
struct PriceTick {
    price: f64,
    timestamp: Instant,
}

/// Price history for momentum detection
#[derive(Debug, Default)]
struct PriceHistory {
    ticks: VecDeque<PriceTick>,
    last_price: Option<f64>,
}

impl PriceHistory {
    fn add_tick(&mut self, price: f64) {
        let tick = PriceTick {
            price,
            timestamp: Instant::now(),
        };
        self.ticks.push_back(tick);
        self.last_price = Some(price);

        // Keep only last 60 seconds of ticks
        let cutoff = Instant::now() - Duration::from_secs(60);
        while self.ticks.front().map(|t| t.timestamp < cutoff).unwrap_or(false) {
            self.ticks.pop_front();
        }
    }

    /// Calculate price change over window in basis points
    fn price_change_bps(&self, window_secs: u64) -> Option<i64> {
        let cutoff = Instant::now() - Duration::from_secs(window_secs);

        // Find oldest tick within window
        let old_tick = self.ticks.iter().find(|t| t.timestamp >= cutoff)?;
        let new_price = self.last_price?;

        let change_pct = (new_price - old_tick.price) / old_tick.price * 10000.0;
        Some(change_pct.round() as i64)
    }

    /// Get price at specific seconds ago
    fn price_at(&self, secs_ago: u64) -> Option<f64> {
        let target = Instant::now() - Duration::from_secs(secs_ago);

        // Find tick closest to target time
        self.ticks.iter()
            .filter(|t| t.timestamp <= target)
            .last()
            .map(|t| t.price)
    }
}

/// Market state
#[derive(Debug, Clone)]
struct Market {
    condition_id: String,
    question: String,
    yes_token: String,
    no_token: String,
    asset: String,
    expiry_minutes: Option<f64>,
    // Orderbook
    yes_ask: Option<i64>,
    yes_bid: Option<i64>,
    no_ask: Option<i64>,
    no_bid: Option<i64>,
    // Trading state
    last_trade_time: Option<Instant>,
}

/// Global state
struct State {
    markets: HashMap<String, Market>,
    price_history: HashMap<String, PriceHistory>, // asset -> history
    pending_signals: Vec<MomentumSignal>,
}

#[derive(Debug, Clone)]
struct MomentumSignal {
    asset: String,
    direction: Direction,
    move_bps: i64,
    triggered_at: Instant,
}

#[derive(Debug, Clone, Copy, PartialEq)]
enum Direction {
    Up,
    Down,
}

impl State {
    fn new() -> Self {
        let mut price_history = HashMap::new();
        for asset in ["BTC", "ETH", "SOL", "XRP"] {
            price_history.insert(asset.to_string(), PriceHistory::default());
        }

        Self {
            markets: HashMap::new(),
            price_history,
            pending_signals: Vec::new(),
        }
    }
}

// === Gamma API for market discovery ===

#[derive(Debug, Deserialize)]
struct GammaSeries {
    events: Option<Vec<GammaEvent>>,
}

#[derive(Debug, Deserialize)]
#[allow(dead_code)]
struct GammaEvent {
    slug: Option<String>,
    closed: Option<bool>,
    #[serde(rename = "enableOrderBook")]
    enable_order_book: Option<bool>,
}

const POLY_SERIES_SLUGS: &[(&str, &str)] = &[
    ("btc-up-or-down-15m", "BTC"),
    ("eth-up-or-down-15m", "ETH"),
    ("sol-up-or-down-15m", "SOL"),
    ("xrp-up-or-down-15m", "XRP"),
];

/// Discover the soonest expiring market for each asset
async fn discover_markets(asset_filter: Option<&str>) -> Result<Vec<Market>> {
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(30))
        .build()?;

    let mut markets = Vec::new();

    let series_to_check: Vec<(&str, &str)> = if let Some(filter) = asset_filter {
        let filter_upper = filter.to_uppercase();
        POLY_SERIES_SLUGS
            .iter()
            .filter(|(_, asset)| *asset == filter_upper)
            .copied()
            .collect()
    } else {
        POLY_SERIES_SLUGS.to_vec()
    };

    for (series_slug, asset) in series_to_check {
        let url = format!("{}/series?slug={}", GAMMA_API_BASE, series_slug);

        let resp = client.get(&url)
            .header("User-Agent", "poly_momentum/1.0")
            .send()
            .await?;

        if !resp.status().is_success() {
            warn!("[DISCOVER] Failed to fetch series '{}': {}", series_slug, resp.status());
            continue;
        }

        let series_list: Vec<GammaSeries> = resp.json().await?;
        let Some(series) = series_list.first() else { continue };
        let Some(events) = &series.events else { continue };

        // Get first active event with orderbook
        let event_slugs: Vec<String> = events
            .iter()
            .filter(|e| e.closed != Some(true) && e.enable_order_book == Some(true))
            .filter_map(|e| e.slug.clone())
            .take(3) // Only need soonest few
            .collect();

        for event_slug in event_slugs {
            let event_url = format!("{}/events?slug={}", GAMMA_API_BASE, event_slug);
            let resp = match client.get(&event_url)
                .header("User-Agent", "poly_momentum/1.0")
                .send()
                .await
            {
                Ok(r) => r,
                Err(_) => continue,
            };

            let event_details: Vec<serde_json::Value> = match resp.json().await {
                Ok(ed) => ed,
                Err(_) => continue,
            };

            let Some(ed) = event_details.first() else { continue };
            let Some(mkts) = ed.get("markets").and_then(|m| m.as_array()) else { continue };

            for mkt in mkts {
                let condition_id = mkt.get("conditionId")
                    .and_then(|v| v.as_str())
                    .map(|s| s.to_string());
                let clob_tokens_str = mkt.get("clobTokenIds")
                    .and_then(|v| v.as_str())
                    .map(|s| s.to_string());
                let question = mkt.get("question")
                    .and_then(|v| v.as_str())
                    .map(|s| s.to_string())
                    .unwrap_or_else(|| event_slug.clone());
                let end_date_str = mkt.get("endDate")
                    .and_then(|v| v.as_str())
                    .map(|s| s.to_string());

                let Some(cid) = condition_id else { continue };
                let Some(cts) = clob_tokens_str else { continue };

                let token_ids: Vec<String> = serde_json::from_str(&cts).unwrap_or_default();
                if token_ids.len() < 2 {
                    continue;
                }

                let expiry_minutes = end_date_str.as_ref().and_then(|d| {
                    let dt = chrono::DateTime::parse_from_rfc3339(d).ok()?;
                    let now = Utc::now();
                    let diff = dt.signed_duration_since(now);
                    let mins = diff.num_minutes() as f64;
                    if mins > 0.0 { Some(mins) } else { None }
                });

                // Skip expired markets
                if expiry_minutes.is_none() {
                    continue;
                }

                markets.push(Market {
                    condition_id: cid,
                    question,
                    yes_token: token_ids[0].clone(),
                    no_token: token_ids[1].clone(),
                    asset: asset.to_string(),
                    expiry_minutes,
                    yes_ask: None,
                    yes_bid: None,
                    no_ask: None,
                    no_bid: None,
                    last_trade_time: None,
                });
            }
        }
    }

    // Keep only soonest market per asset
    let mut best_per_asset: HashMap<String, Market> = HashMap::new();
    for market in markets {
        let expiry = market.expiry_minutes.unwrap_or(f64::MAX);
        let existing = best_per_asset.get(&market.asset);
        if existing.is_none() || existing.unwrap().expiry_minutes.unwrap_or(f64::MAX) > expiry {
            best_per_asset.insert(market.asset.clone(), market);
        }
    }

    Ok(best_per_asset.into_values().collect())
}

// === Polygon Price Feed ===

#[derive(Deserialize, Debug)]
struct PolygonMessage {
    ev: Option<String>,
    pair: Option<String>,
    p: Option<f64>,
}

/// Run Polygon price feed and detect momentum signals
async fn run_price_feed(
    state: Arc<RwLock<State>>,
    api_key: &str,
    threshold_bps: i64,
    window_secs: u64,
) {
    loop {
        info!("[POLYGON] Connecting to price feed...");

        let url = format!("{}?apiKey={}", POLYGON_WS_URL, api_key);
        let ws = match connect_async(&url).await {
            Ok((ws, _)) => ws,
            Err(e) => {
                error!("[POLYGON] Connect failed: {}", e);
                tokio::time::sleep(Duration::from_secs(5)).await;
                continue;
            }
        };

        let (mut write, mut read) = ws.split();

        // Subscribe to crypto pairs
        let sub = serde_json::json!({
            "action": "subscribe",
            "params": "XT.BTC-USD,XT.ETH-USD,XT.SOL-USD,XT.XRP-USD"
        });
        let _ = write.send(Message::Text(sub.to_string())).await;
        info!("[POLYGON] Subscribed to BTC, ETH, SOL, XRP");

        while let Some(msg) = read.next().await {
            match msg {
                Ok(Message::Text(text)) => {
                    if let Ok(messages) = serde_json::from_str::<Vec<PolygonMessage>>(&text) {
                        for m in messages {
                            if m.ev.as_deref() != Some("XT") {
                                continue;
                            }

                            let Some(pair) = m.pair.as_ref() else { continue };
                            let Some(price) = m.p else { continue };

                            let asset = match pair.as_str() {
                                "BTC-USD" => "BTC",
                                "ETH-USD" => "ETH",
                                "SOL-USD" => "SOL",
                                "XRP-USD" => "XRP",
                                _ => continue,
                            };

                            let mut s = state.write().await;

                            // Add tick to history
                            if let Some(history) = s.price_history.get_mut(asset) {
                                let old_price = history.last_price;
                                history.add_tick(price);

                                // Check for momentum signal
                                if let Some(change_bps) = history.price_change_bps(window_secs) {
                                    if change_bps.abs() >= threshold_bps {
                                        let direction = if change_bps > 0 {
                                            Direction::Up
                                        } else {
                                            Direction::Down
                                        };

                                        // Only signal if this is a new move (price crossed threshold)
                                        let should_signal = old_price.map(|op| {
                                            let old_change = ((price - op) / op * 10000.0).round() as i64;
                                            old_change.abs() < threshold_bps
                                        }).unwrap_or(false);

                                        if should_signal || s.pending_signals.iter()
                                            .filter(|sig| sig.asset == asset)
                                            .count() == 0
                                        {
                                            warn!("[SIGNAL] {} {:?} {}bps (${:.2} over {}s window)",
                                                  asset, direction, change_bps.abs(), price, window_secs);

                                            s.pending_signals.push(MomentumSignal {
                                                asset: asset.to_string(),
                                                direction,
                                                move_bps: change_bps,
                                                triggered_at: Instant::now(),
                                            });
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
                    error!("[POLYGON] WebSocket error: {}", e);
                    break;
                }
                _ => {}
            }
        }

        warn!("[POLYGON] Disconnected, reconnecting in 2s...");
        tokio::time::sleep(Duration::from_secs(2)).await;
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
    use clap::Parser;

    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::from_default_env()
                .add_directive("poly_momentum=info".parse().unwrap())
                .add_directive("arb_bot=info".parse().unwrap()),
        )
        .init();

    let args = Args::parse();

    info!("═══════════════════════════════════════════════════════════════════════");
    info!("🚀 POLYMARKET MOMENTUM FRONT-RUNNER");
    info!("═══════════════════════════════════════════════════════════════════════");
    info!("STRATEGY:");
    info!("   1. Detect rapid price moves (>{}bps in {}s)", args.threshold_bps, args.window_secs);
    info!("   2. Buy YES on up moves, NO on down moves");
    info!("   3. Front-run slow market makers");
    info!("═══════════════════════════════════════════════════════════════════════");
    info!("CONFIG:");
    info!("   Mode: {}", if args.live { "🔴 LIVE" } else { "🔍 DRY RUN" });
    info!("   Size: ${:.2} per trade", args.size);
    info!("   Threshold: {}bps ({}%)", args.threshold_bps, args.threshold_bps as f64 / 100.0);
    info!("   Window: {}s", args.window_secs);
    info!("   Min Edge: {}¢", args.edge);
    info!("   Cooldown: {}s", args.cooldown);
    if let Some(ref asset) = args.asset {
        info!("   Asset: {}", asset);
    }
    info!("═══════════════════════════════════════════════════════════════════════");

    // Load credentials
    dotenvy::dotenv().ok();
    let private_key = std::env::var("POLY_PRIVATE_KEY")
        .context("POLY_PRIVATE_KEY not set")?;
    let funder = std::env::var("POLY_FUNDER")
        .context("POLY_FUNDER not set")?;
    let polygon_api_key = std::env::var("POLYGON_API_KEY")
        .unwrap_or_else(|_| "o2Jm26X52_0tRq2W7V5JbsCUXdMjL7qk".to_string());

    // Initialize Polymarket client
    let poly_client = PolymarketAsyncClient::new(
        "https://clob.polymarket.com",
        137,
        &private_key,
        &funder,
    ).context("Failed to create Polymarket client")?;

    info!("[POLY] Deriving API credentials...");
    let api_creds = poly_client.derive_api_key(0).await?;
    let prepared_creds = PreparedCreds::from_api_creds(&api_creds)?;
    let shared_client = Arc::new(SharedAsyncClient::new(poly_client, prepared_creds, 137));
    info!("[POLY] API credentials ready");

    if args.live {
        warn!("⚠️  LIVE MODE - Real money at risk!");
        tokio::time::sleep(Duration::from_secs(3)).await;
    }

    // Discover markets
    info!("[DISCOVER] Searching for markets...");
    let discovered = discover_markets(args.asset.as_deref()).await?;
    info!("[DISCOVER] Found {} markets", discovered.len());

    for m in &discovered {
        info!("  • {} | {} | Expiry: {:.1?}min",
              m.asset,
              &m.question[..m.question.len().min(50)],
              m.expiry_minutes);
    }

    if discovered.is_empty() {
        warn!("No markets found!");
        return Ok(());
    }

    // Initialize state
    let state = Arc::new(RwLock::new({
        let mut s = State::new();
        for m in discovered {
            let id = m.condition_id.clone();
            s.markets.insert(id, m);
        }
        s
    }));

    // Start price feed with momentum detection
    let state_price = state.clone();
    let polygon_key = polygon_api_key.clone();
    let threshold = args.threshold_bps;
    let window = args.window_secs;
    tokio::spawn(async move {
        run_price_feed(state_price, &polygon_key, threshold, window).await;
    });

    // Get token IDs for subscription
    let tokens: Vec<String> = {
        let s = state.read().await;
        s.markets.values()
            .flat_map(|m| vec![m.yes_token.clone(), m.no_token.clone()])
            .collect()
    };

    let edge_threshold = args.edge;
    let size = args.size;
    let dry_run = !args.live;
    let cooldown = Duration::from_secs(args.cooldown);

    // Main WebSocket loop
    loop {
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
        let mut signal_check = tokio::time::interval(Duration::from_millis(100));

        loop {
            tokio::select! {
                _ = ping_interval.tick() => {
                    if let Err(e) = write.send(Message::Ping(vec![])).await {
                        error!("[WS] Ping failed: {}", e);
                        break;
                    }
                }

                _ = signal_check.tick() => {
                    // Process pending momentum signals
                    let mut s = state.write().await;

                    // Remove stale signals (>5s old)
                    s.pending_signals.retain(|sig| sig.triggered_at.elapsed() < Duration::from_secs(5));

                    // Process signals
                    let signals: Vec<MomentumSignal> = s.pending_signals.drain(..).collect();

                    for signal in signals {
                        // Find market for this asset
                        let market_entry = s.markets.iter_mut()
                            .find(|(_, m)| m.asset == signal.asset);

                        let Some((market_id, market)) = market_entry else {
                            continue;
                        };

                        // Check cooldown
                        if let Some(last_trade) = market.last_trade_time {
                            if last_trade.elapsed() < cooldown {
                                info!("[COOLDOWN] {} - {}s remaining",
                                      signal.asset,
                                      (cooldown - last_trade.elapsed()).as_secs());
                                continue;
                            }
                        }

                        // Determine which side to buy
                        let (buy_token, buy_side, ask_price) = match signal.direction {
                            Direction::Up => {
                                // Price went up, buy YES
                                (&market.yes_token, "YES", market.yes_ask)
                            }
                            Direction::Down => {
                                // Price went down, buy NO
                                (&market.no_token, "NO", market.no_ask)
                            }
                        };

                        let Some(ask) = ask_price else {
                            warn!("[SKIP] {} {} - no ask price", signal.asset, buy_side);
                            continue;
                        };

                        // For momentum trades, we expect fair value to be moving
                        // If price moved up 15bps, YES should be worth more than 50¢
                        // Rough estimate: each 10bps move = ~1¢ edge in short-term
                        let estimated_fair = 50 + (signal.move_bps.abs() / 10) as i64;
                        let edge = estimated_fair - ask;

                        if edge < edge_threshold {
                            info!("[SKIP] {} {} - edge {}¢ < {}¢ threshold (ask={}¢, est_fair={}¢)",
                                  signal.asset, buy_side, edge, edge_threshold, ask, estimated_fair);
                            continue;
                        }

                        let market_id_clone = market_id.clone();
                        let buy_token_clone = buy_token.clone();

                        if dry_run {
                            warn!("[DRY] 🎯 Would BUY ${:.0} {} {} @{}¢ | move={}bps | edge={}¢",
                                  size, signal.asset, buy_side, ask, signal.move_bps.abs(), edge);
                            market.last_trade_time = Some(Instant::now());
                        } else {
                            let price = ask as f64 / 100.0;
                            let contracts = size / price;

                            warn!("[TRADE] 🎯 BUY ${:.0} {} {} @{}¢ | move={}bps | edge={}¢",
                                  size, signal.asset, buy_side, ask, signal.move_bps.abs(), edge);

                            let client = shared_client.clone();
                            let state_clone = state.clone();

                            // Execute trade asynchronously
                            tokio::spawn(async move {
                                match client.buy_fak(&buy_token_clone, price, contracts).await {
                                    Ok(fill) => {
                                        warn!("[TRADE] ✅ Filled {:.2} @${:.2} | order_id={}",
                                              fill.filled_size, fill.fill_cost, fill.order_id);

                                        let mut s = state_clone.write().await;
                                        if let Some(m) = s.markets.get_mut(&market_id_clone) {
                                            m.last_trade_time = Some(Instant::now());
                                        }
                                    }
                                    Err(e) => {
                                        error!("[TRADE] ❌ Buy failed: {}", e);
                                    }
                                }
                            });

                            market.last_trade_time = Some(Instant::now());
                        }
                    }
                }

                msg = read.next() => {
                    let Some(msg) = msg else { break; };

                    match msg {
                        Ok(Message::Text(text)) => {
                            // Update orderbook
                            if let Ok(books) = serde_json::from_str::<Vec<BookSnapshot>>(&text) {
                                let mut s = state.write().await;

                                for book in books {
                                    // Find market
                                    let market = s.markets.values_mut()
                                        .find(|m| m.yes_token == book.asset_id || m.no_token == book.asset_id);

                                    let Some(market) = market else { continue };

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

        info!("[WS] Disconnected, reconnecting in 3s...");
        tokio::time::sleep(Duration::from_secs(3)).await;
    }
}
