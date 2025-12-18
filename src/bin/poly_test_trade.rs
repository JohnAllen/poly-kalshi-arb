//! Polymarket Test Trade - Place a single $1 trade to verify connectivity
//!
//! Usage:
//!   cargo run --release --bin poly_test_trade
//!
//! This will:
//! 1. Find an active crypto market on Polymarket
//! 2. Get the current orderbook
//! 3. Place a $1 FAK buy order at the ask price
//!
//! Environment:
//!   POLY_PRIVATE_KEY - Your Polymarket/Polygon wallet private key
//!   POLY_FUNDER - Your funder address (proxy wallet)

use anyhow::{Context, Result};
use clap::Parser;
use serde::Deserialize;
use std::time::Duration;
use tracing::{info, warn, error};

use arb_bot::polymarket_clob::{
    PolymarketAsyncClient, SharedAsyncClient, PreparedCreds,
};

#[derive(Parser, Debug)]
#[command(author, version, about = "Place a single test trade on Polymarket")]
struct Args {
    /// Size in dollars (default: $1)
    #[arg(short, long, default_value_t = 1.0)]
    size: f64,

    /// Side to buy: "yes" or "no" (default: yes)
    #[arg(long, default_value = "yes")]
    side: String,

    /// Dry run mode - don't actually trade
    #[arg(short, long, default_value_t = false)]
    dry_run: bool,

    /// Market asset filter (btc, eth, sol, xrp)
    #[arg(short, long, default_value = "btc")]
    asset: String,
}

const GAMMA_API_BASE: &str = "https://gamma-api.polymarket.com";
const CLOB_API_BASE: &str = "https://clob.polymarket.com";

#[derive(Debug, Deserialize)]
struct GammaSeries {
    events: Option<Vec<GammaEvent>>,
}

#[derive(Debug, Deserialize)]
struct GammaEvent {
    slug: Option<String>,
    closed: Option<bool>,
    #[serde(rename = "enableOrderBook")]
    enable_order_book: Option<bool>,
    #[serde(rename = "endDate")]
    end_date: Option<String>,
}

#[derive(Debug, Deserialize)]
struct BookResponse {
    bids: Option<Vec<PriceLevel>>,
    asks: Option<Vec<PriceLevel>>,
}

#[derive(Debug, Deserialize)]
struct PriceLevel {
    price: String,
    size: String,
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter("poly_test_trade=debug,arb_bot=info")
        .init();

    let args = Args::parse();

    info!("═══════════════════════════════════════════════════════════════════════");
    info!("🧪 POLYMARKET TEST TRADE");
    info!("═══════════════════════════════════════════════════════════════════════");
    info!("Size: ${:.2}", args.size);
    info!("Side: {}", args.side.to_uppercase());
    info!("Asset: {}", args.asset.to_uppercase());
    info!("Mode: {}", if args.dry_run { "DRY RUN" } else { "🚀 LIVE" });
    info!("═══════════════════════════════════════════════════════════════════════");

    // Load credentials
    dotenvy::dotenv().ok();
    let private_key = std::env::var("POLY_PRIVATE_KEY")
        .context("POLY_PRIVATE_KEY not set")?;
    let funder = std::env::var("POLY_FUNDER")
        .context("POLY_FUNDER not set")?;

    info!("[1/5] Initializing Polymarket client...");
    let poly_client = PolymarketAsyncClient::new(
        CLOB_API_BASE,
        137, // Polygon mainnet
        &private_key,
        &funder,
    )?;

    info!("[2/5] Deriving API credentials...");
    let api_creds = poly_client.derive_api_key(0).await?;
    let prepared_creds = PreparedCreds::from_api_creds(&api_creds)?;
    let shared_client = SharedAsyncClient::new(poly_client, prepared_creds, 137);
    info!("  ✓ API key: {}", &api_creds.api_key[..20]);

    // Find a market
    info!("[3/5] Finding active {} market...", args.asset.to_uppercase());
    let http_client = reqwest::Client::builder()
        .timeout(Duration::from_secs(30))
        .build()?;

    let series_slug = match args.asset.to_lowercase().as_str() {
        "btc" => "btc-up-or-down-15m",
        "eth" => "eth-up-or-down-15m",
        "sol" => "sol-up-or-down-15m",
        "xrp" => "xrp-up-or-down-15m",
        _ => "btc-up-or-down-15m",
    };

    let url = format!("{}/series?slug={}", GAMMA_API_BASE, series_slug);
    info!("  Fetching: {}", url);

    let resp = http_client.get(&url)
        .header("User-Agent", "poly_test_trade/1.0")
        .send()
        .await?;

    if !resp.status().is_success() {
        anyhow::bail!("Failed to fetch series: {}", resp.status());
    }

    let series_list: Vec<GammaSeries> = resp.json().await?;
    let series = series_list.first().context("No series found")?;
    let events = series.events.as_ref().context("No events in series")?;

    // Find first active event with orderbook that hasn't ended yet
    let now = chrono::Utc::now();
    let event = events.iter()
        .filter(|e| e.closed != Some(true) && e.enable_order_book == Some(true))
        .filter(|e| {
            // Filter by end_date - must be in the future
            if let Some(end_date_str) = &e.end_date {
                if let Ok(end_date) = chrono::DateTime::parse_from_rfc3339(end_date_str) {
                    return end_date > now;
                }
            }
            false
        })
        .next()
        .context("No active events with orderbook")?;

    let event_slug = event.slug.as_ref().context("Event has no slug")?;
    info!("  Found event: {}", event_slug);

    // Get event details to find market
    let event_url = format!("{}/events?slug={}", GAMMA_API_BASE, event_slug);
    let resp = http_client.get(&event_url)
        .header("User-Agent", "poly_test_trade/1.0")
        .send()
        .await?;

    let event_details: Vec<serde_json::Value> = resp.json().await?;
    let ed = event_details.first().context("No event details")?;
    let markets = ed.get("markets")
        .and_then(|m| m.as_array())
        .context("No markets in event")?;

    let market = markets.first().context("No markets")?;
    let condition_id = market.get("conditionId")
        .and_then(|v| v.as_str())
        .context("No conditionId")?;
    let question = market.get("question")
        .and_then(|v| v.as_str())
        .unwrap_or("Unknown");
    let clob_tokens_str = market.get("clobTokenIds")
        .and_then(|v| v.as_str())
        .context("No clobTokenIds")?;

    let token_ids: Vec<String> = serde_json::from_str(clob_tokens_str)?;
    if token_ids.len() < 2 {
        anyhow::bail!("Need at least 2 token IDs");
    }

    let (yes_token, no_token) = (&token_ids[0], &token_ids[1]);
    info!("  ✓ Market: {}", question);
    info!("  ✓ Condition ID: {}...", &condition_id[..20]);
    info!("  ✓ YES token: {}...", &yes_token[..20]);
    info!("  ✓ NO token: {}...", &no_token[..20]);

    // Get orderbook
    info!("[4/5] Fetching orderbook...");
    let token_to_trade = if args.side.to_lowercase() == "yes" {
        yes_token
    } else {
        no_token
    };

    let book_url = format!("{}/book?token_id={}", CLOB_API_BASE, token_to_trade);
    info!("  Fetching: {}", book_url);

    let resp = http_client.get(&book_url)
        .header("User-Agent", "poly_test_trade/1.0")
        .send()
        .await?;

    let book: BookResponse = resp.json().await?;

    // Find best ask
    let asks = book.asks.unwrap_or_default();
    let bids = book.bids.unwrap_or_default();

    info!("  Orderbook depth: {} bids, {} asks", bids.len(), asks.len());

    if !asks.is_empty() {
        info!("  Top 3 asks:");
        for (i, ask) in asks.iter().take(3).enumerate() {
            let price_cents = (ask.price.parse::<f64>().unwrap_or(0.0) * 100.0).round() as i64;
            info!("    {}. {}¢ x ${}", i + 1, price_cents, ask.size);
        }
    }

    if !bids.is_empty() {
        info!("  Top 3 bids:");
        for (i, bid) in bids.iter().take(3).enumerate() {
            let price_cents = (bid.price.parse::<f64>().unwrap_or(0.0) * 100.0).round() as i64;
            info!("    {}. {}¢ x ${}", i + 1, price_cents, bid.size);
        }
    }

    let best_ask = asks.first().context("No asks available")?;
    let raw_ask_price: f64 = best_ask.price.parse()?;
    let ask_size: f64 = best_ask.size.parse()?;

    // Polymarket price range is 0.01 to 0.99 - cap at 0.99
    let ask_price = raw_ask_price.min(0.99);
    let ask_price_cents = (ask_price * 100.0).round() as i64;

    info!("  ✓ Best ask: {}¢ (${:.2} available)", (raw_ask_price * 100.0).round() as i64, ask_size);
    if raw_ask_price > 0.99 {
        warn!("  ⚠️  Ask price {}¢ exceeds max 99¢, will bid at 99¢", (raw_ask_price * 100.0).round() as i64);
    }

    // Calculate order
    let contracts = args.size / ask_price;
    info!("[5/5] Placing order...");
    info!("  Side: BUY {}", args.side.to_uppercase());
    info!("  Price: {}¢ (${:.4})", ask_price_cents, ask_price);
    info!("  Size: ${:.2} = {:.4} contracts", args.size, contracts);
    info!("  Token: {}...", &token_to_trade[..20]);

    if args.dry_run {
        warn!("═══════════════════════════════════════════════════════════════════════");
        warn!("🔍 DRY RUN - No order placed");
        warn!("  Would buy {:.4} {} contracts @ {}¢", contracts, args.side.to_uppercase(), ask_price_cents);
        warn!("  Max cost: ${:.4}", contracts * ask_price);
        warn!("═══════════════════════════════════════════════════════════════════════");
        return Ok(());
    }

    // Place the trade!
    warn!("═══════════════════════════════════════════════════════════════════════");
    warn!("🚀 PLACING LIVE ORDER...");
    warn!("═══════════════════════════════════════════════════════════════════════");

    match shared_client.buy_fak(token_to_trade, ask_price, contracts).await {
        Ok(fill) => {
            info!("═══════════════════════════════════════════════════════════════════════");
            info!("✅ ORDER RESULT");
            info!("═══════════════════════════════════════════════════════════════════════");
            info!("  Order ID: {}", fill.order_id);
            info!("  Filled: {:.4} contracts", fill.filled_size);
            info!("  Cost: ${:.4}", fill.fill_cost);
            info!("  Avg price: {}¢", if fill.filled_size > 0.0 {
                (fill.fill_cost / fill.filled_size * 100.0).round() as i64
            } else {
                0
            });
            info!("═══════════════════════════════════════════════════════════════════════");
        }
        Err(e) => {
            error!("═══════════════════════════════════════════════════════════════════════");
            error!("❌ ORDER FAILED");
            error!("═══════════════════════════════════════════════════════════════════════");
            error!("  Error: {}", e);
            error!("═══════════════════════════════════════════════════════════════════════");
            return Err(e);
        }
    }

    Ok(())
}
