//! Polymarket Sell All Positions
//!
//! Sells all conditional token holdings for a specific symbol.
//!
//! Usage:
//!   cargo run --release --bin poly_sell_all -- --sym BTC
//!
//! This will:
//! 1. Discover active markets for the symbol
//! 2. Query your token balances for those markets
//! 3. Sell everything at market bid

use anyhow::{Context, Result};
use clap::Parser;
use ethers::prelude::*;
use ethers::providers::{Http, Provider};
use std::sync::Arc;
use tracing::info;

use arb_bot::polymarket_clob::{PolymarketAsyncClient, SharedAsyncClient, PreparedCreds};
use arb_bot::polymarket_markets::discover_markets;

/// CLI Arguments
#[derive(Parser, Debug)]
#[command(author, version, about = "Sell all Polymarket positions for a symbol")]
struct Args {
    /// Symbol to sell (BTC, ETH, SOL, XRP)
    #[arg(long, short = 's')]
    sym: String,

    /// Dry run - don't actually sell
    #[arg(long, short = 'd', default_value_t = false)]
    dry: bool,

    /// Minimum cents to sell at (default: 1)
    #[arg(long, default_value_t = 1)]
    min_price: i64,
}

// Contract addresses
const CTF_CONTRACT: &str = "0x4D97DCd97eC945f40cF65F87097ACe5EA0476045";
const RPC_URL: &str = "https://polygon-rpc.com";

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter("info")
        .with_target(false)
        .init();

    let args = Args::parse();
    let sym = args.sym.to_uppercase();

    info!("Sell All - Symbol: {} | Dry: {}", sym, args.dry);

    // Load credentials
    let project_root = std::path::Path::new(env!("CARGO_MANIFEST_DIR"));
    let env_path = project_root.join(".env");
    if env_path.exists() {
        dotenvy::from_path(&env_path).ok();
    }

    let private_key = std::env::var("POLY_PRIVATE_KEY")
        .context("POLY_PRIVATE_KEY not set")?;
    let funder = std::env::var("POLY_FUNDER")
        .context("POLY_FUNDER not set")?;

    // Initialize provider for balance queries
    let provider = Provider::<Http>::try_from(RPC_URL)?;
    let provider = Arc::new(provider);

    // Initialize Polymarket client
    let poly_client = PolymarketAsyncClient::new(
        "https://clob.polymarket.com",
        137,
        &private_key,
        &funder,
    )?;

    let api_creds = poly_client.derive_api_key(0).await?;
    let prepared_creds = PreparedCreds::from_api_creds(&api_creds)?;
    let shared_client = Arc::new(SharedAsyncClient::new(poly_client, prepared_creds, 137));

    // Discover markets for this symbol
    let markets = discover_markets(Some(&sym)).await?;
    if markets.is_empty() {
        info!("No active markets found for {}", sym);
        return Ok(());
    }

    info!("Found {} markets for {}", markets.len(), sym);

    // Get wallet address
    let wallet_address: Address = funder.parse()?;

    // Query token balances for each market
    for market in &markets {
        info!("Market: {} ({}m left)", market.question, market.expiry_minutes.unwrap_or(0.0));
        info!("  YES token: {}", market.yes_token);
        info!("  NO token: {}", market.no_token);

        // Check YES token balance
        let yes_balance = get_token_balance(&provider, wallet_address, &market.yes_token).await?;
        let no_balance = get_token_balance(&provider, wallet_address, &market.no_token).await?;

        if yes_balance > 0.0 {
            info!("  YES: {:.2} contracts", yes_balance);
            if !args.dry && yes_balance >= 1.0 {
                // Get current bid and sell
                let bid_price = get_best_bid(&market.yes_token).await.unwrap_or(args.min_price);
                let sell_price = ((bid_price - 1).max(args.min_price)) as f64 / 100.0;
                info!("  Selling {:.1} YES @{}¢", yes_balance, (sell_price * 100.0) as i64);

                match shared_client.sell_fak(&market.yes_token, sell_price, yes_balance).await {
                    Ok(fill) if fill.filled_size > 0.0 => {
                        info!("  ✅ Sold {:.1} for ${:.2}", fill.filled_size, fill.fill_cost);
                    }
                    Ok(_) => {
                        info!("  ⚠️ No fill at {}¢", (sell_price * 100.0) as i64);
                    }
                    Err(e) => {
                        info!("  ❌ Error: {}", e);
                    }
                }
            }
        }

        if no_balance > 0.0 {
            info!("  NO: {:.2} contracts", no_balance);
            if !args.dry && no_balance >= 1.0 {
                // Get current bid and sell
                let bid_price = get_best_bid(&market.no_token).await.unwrap_or(args.min_price);
                let sell_price = ((bid_price - 1).max(args.min_price)) as f64 / 100.0;
                info!("  Selling {:.1} NO @{}¢", no_balance, (sell_price * 100.0) as i64);

                match shared_client.sell_fak(&market.no_token, sell_price, no_balance).await {
                    Ok(fill) if fill.filled_size > 0.0 => {
                        info!("  ✅ Sold {:.1} for ${:.2}", fill.filled_size, fill.fill_cost);
                    }
                    Ok(_) => {
                        info!("  ⚠️ No fill at {}¢", (sell_price * 100.0) as i64);
                    }
                    Err(e) => {
                        info!("  ❌ Error: {}", e);
                    }
                }
            }
        }

        if yes_balance == 0.0 && no_balance == 0.0 {
            info!("  (no positions)");
        }
    }

    info!("Done!");
    Ok(())
}

/// Query ERC-1155 balance for a token
async fn get_token_balance(
    provider: &Arc<Provider<Http>>,
    wallet: Address,
    token_id: &str,
) -> Result<f64> {
    // ERC-1155 balanceOf(address,uint256)
    let ctf: Address = CTF_CONTRACT.parse()?;

    // Parse token_id as U256
    let token_id_u256 = U256::from_dec_str(token_id)?;

    // Encode the call
    let call_data = ethers::abi::encode(&[
        ethers::abi::Token::Address(wallet),
        ethers::abi::Token::Uint(token_id_u256),
    ]);

    // balanceOf selector: 0x00fdd58e
    let mut data = vec![0x00, 0xfd, 0xd5, 0x8e];
    data.extend(call_data);

    let tx = TransactionRequest::new()
        .to(ctf)
        .data(data);

    let result = provider.call(&tx.into(), None).await?;

    // Decode result as U256
    let balance = U256::from_big_endian(&result);

    // Convert from micro units (6 decimals) to contracts
    let balance_f64 = balance.as_u128() as f64 / 1_000_000.0;

    Ok(balance_f64)
}

/// Get best bid from orderbook (simplified - just returns a default)
async fn get_best_bid(token_id: &str) -> Option<i64> {
    // Query Polymarket orderbook API
    let url = format!(
        "https://clob.polymarket.com/book?token_id={}",
        token_id
    );

    let resp = reqwest::get(&url).await.ok()?;
    let json: serde_json::Value = resp.json().await.ok()?;

    // Get best bid price
    let bids = json.get("bids")?.as_array()?;
    let best_bid = bids.first()?;
    let price_str = best_bid.get("price")?.as_str()?;
    let price: f64 = price_str.parse().ok()?;

    Some((price * 100.0).round() as i64)
}
