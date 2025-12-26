//! Polymarket Auto-Redemption
//!
//! Redeems resolved positions for USDC.
//! Run this periodically to claim winnings from resolved markets.
//!
//! Usage:
//!   cargo run --release --bin poly_redeem
//!
//! Or with dry run:
//!   cargo run --release --bin poly_redeem -- --dry

use anyhow::{Context, Result};
use clap::Parser;
use ethers::prelude::*;
use ethers::providers::{Http, Provider};
use ethers::utils::hex;
use std::sync::Arc;
use tracing::{info, warn};

/// CLI Arguments
#[derive(Parser, Debug)]
#[command(author, version, about = "Redeem resolved Polymarket positions")]
struct Args {
    /// Dry run - don't actually redeem
    #[arg(long, short = 'd', default_value_t = false)]
    dry: bool,
}

// Contract addresses on Polygon
const CTF_CONTRACT: &str = "0x4D97DCd97eC945f40cF65F87097ACe5EA0476045";
const USDC_CONTRACT: &str = "0x2791Bca1f2de4661ED88A30C99A7a9449Aa84174";
const RPC_URL: &str = "https://polygon-rpc.com";

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter("info")
        .with_target(false)
        .init();

    let args = Args::parse();

    info!("Polymarket Auto-Redemption {}", if args.dry { "(DRY RUN)" } else { "" });

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

    // Initialize provider and wallet
    let provider = Provider::<Http>::try_from(RPC_URL)?;
    let provider = Arc::new(provider);

    let wallet: LocalWallet = private_key.parse::<LocalWallet>()?.with_chain_id(137u64);
    let client = SignerMiddleware::new(provider.clone(), wallet);
    let client = Arc::new(client);

    let wallet_address: Address = funder.parse()?;
    info!("Wallet: {:?}", wallet_address);

    // Get USDC balance before
    let usdc_before = get_usdc_balance(&provider, wallet_address).await?;
    info!("USDC Balance: ${:.2}", usdc_before);

    // Query the Polymarket API for resolved markets we might have positions in
    let resolved = get_resolved_positions(&funder).await?;

    if resolved.is_empty() {
        info!("No resolved positions to redeem");
        return Ok(());
    }

    // Calculate total value (each winning contract = $1)
    let total_value: f64 = resolved.iter().map(|(_, _, bal, _)| bal).sum();
    info!("Found {} resolved position(s) to redeem: ${:.2}", resolved.len(), total_value);

    let ctf: Address = CTF_CONTRACT.parse()?;
    let usdc: Address = USDC_CONTRACT.parse()?;

    for (condition_id, _token_id, balance, outcome) in &resolved {
        if args.dry {
            info!("  {} ({}) - {:.2} contracts", &condition_id[..16], outcome, balance);
            continue;
        }

        info!("Redeeming: {} ({}) - {:.2} contracts",
              &condition_id[..16], outcome, balance);

        // Convert condition_id to bytes32
        let condition_bytes = hex::decode(&condition_id)?;
        let mut condition_arr = [0u8; 32];
        condition_arr.copy_from_slice(&condition_bytes);

        // Index sets: 1 for YES (outcome 0), 2 for NO (outcome 1)
        let index_set = if outcome == "YES" { U256::from(1) } else { U256::from(2) };

        // Encode redeemPositions call
        // redeemPositions(IERC20 collateralToken, bytes32 parentCollectionId, bytes32 conditionId, uint256[] indexSets)
        let parent_collection = [0u8; 32]; // null for top-level

        let call_data = encode_redeem_positions(
            usdc,
            parent_collection,
            condition_arr,
            vec![index_set],
        );

        let tx = TransactionRequest::new()
            .to(ctf)
            .data(call_data)
            .gas(500000u64);

        match client.send_transaction(tx, None).await {
            Ok(pending) => {
                info!("  Submitted tx: {:?}", pending.tx_hash());
                match pending.await {
                    Ok(Some(receipt)) => {
                        if receipt.status == Some(1.into()) {
                            info!("  ✅ Redeemed successfully!");
                        } else {
                            warn!("  ❌ Transaction failed");
                        }
                    }
                    Ok(None) => warn!("  ⚠️ Transaction dropped"),
                    Err(e) => warn!("  ❌ Error waiting for receipt: {}", e),
                }
            }
            Err(e) => {
                warn!("  ❌ Failed to submit: {}", e);
            }
        }
    }

    // Get USDC balance after
    let usdc_after = get_usdc_balance(&provider, wallet_address).await?;
    let gained = usdc_after - usdc_before;
    info!("USDC Balance: ${:.2} ({:+.2})", usdc_after, gained);

    Ok(())
}

/// Query USDC balance
async fn get_usdc_balance(provider: &Arc<Provider<Http>>, wallet: Address) -> Result<f64> {
    let usdc: Address = USDC_CONTRACT.parse()?;

    // balanceOf(address) selector: 0x70a08231
    let call_data = ethers::abi::encode(&[ethers::abi::Token::Address(wallet)]);
    let mut data = vec![0x70, 0xa0, 0x82, 0x31];
    data.extend(call_data);

    let tx = TransactionRequest::new().to(usdc).data(data);
    let result = provider.call(&tx.into(), None).await?;
    let balance = U256::from_big_endian(&result);

    Ok(balance.as_u128() as f64 / 1_000_000.0)
}

/// Get resolved positions from Polymarket API
async fn get_resolved_positions(wallet: &str) -> Result<Vec<(String, String, f64, String)>> {
    // Query the CTF contract for token balances and check which conditions are resolved
    let provider = Provider::<Http>::try_from(RPC_URL)?;
    let wallet_addr: Address = wallet.parse()?;
    let ctf: Address = CTF_CONTRACT.parse()?;

    // First, get list of known condition IDs from recent/active markets
    let markets = get_recent_markets().await?;

    let mut results = Vec::new();

    for (condition_id, yes_token, no_token) in markets {
        // Check YES token balance
        let yes_balance = get_token_balance(&provider, wallet_addr, &yes_token).await?;
        let no_balance = get_token_balance(&provider, wallet_addr, &no_token).await?;

        if yes_balance < 0.01 && no_balance < 0.01 {
            continue; // No position
        }

        // Check if condition is resolved (payoutDenominator > 0)
        let resolved = check_condition_resolved(&provider, ctf, &condition_id).await?;

        if !resolved {
            continue; // Not resolved yet
        }

        // Check payout for each outcome
        if yes_balance >= 0.01 {
            let payout = get_payout(&provider, ctf, &condition_id, 0).await?;
            if payout > 0 {
                results.push((condition_id.clone(), yes_token.clone(), yes_balance, "YES".to_string()));
            }
        }

        if no_balance >= 0.01 {
            let payout = get_payout(&provider, ctf, &condition_id, 1).await?;
            if payout > 0 {
                results.push((condition_id.clone(), no_token.clone(), no_balance, "NO".to_string()));
            }
        }
    }

    Ok(results)
}

/// Get recent markets from Polymarket positions API (more reliable, includes resolved status)
async fn get_recent_markets() -> Result<Vec<(String, String, String)>> {
    // Use data-api which is less rate-limited and already has position info
    let funder = std::env::var("POLY_FUNDER").unwrap_or_default().to_lowercase();
    let url = format!(
        "https://data-api.polymarket.com/positions?user={}&sizeThreshold=0&limit=200",
        funder
    );

    let client = reqwest::Client::new();
    let resp = client
        .get(&url)
        .header("User-Agent", "Mozilla/5.0")
        .send()
        .await?;

    let positions: Vec<serde_json::Value> = resp.json().await?;

    let mut results = Vec::new();
    let mut seen_conditions = std::collections::HashSet::new();

    for pos in positions {
        // Only process redeemable winning positions (curPrice == 1)
        let redeemable = pos.get("redeemable").and_then(|v| v.as_bool()).unwrap_or(false);
        let cur_price = pos.get("curPrice").and_then(|v| v.as_f64()).unwrap_or(0.0);

        if !redeemable || cur_price != 1.0 {
            continue;
        }

        let condition_id = pos.get("conditionId")
            .and_then(|v| v.as_str())
            .map(|s| s.trim_start_matches("0x").to_string());

        let asset = pos.get("asset")
            .and_then(|v| v.as_str())
            .map(|s| s.to_string());

        let opposite_asset = pos.get("oppositeAsset")
            .and_then(|v| v.as_str())
            .map(|s| s.to_string());

        let Some(cid) = condition_id else { continue };
        let Some(token) = asset else { continue };
        let Some(opp_token) = opposite_asset else { continue };

        // Dedupe by condition
        if seen_conditions.contains(&cid) {
            continue;
        }
        seen_conditions.insert(cid.clone());

        // Determine YES/NO order based on outcomeIndex
        let outcome_idx = pos.get("outcomeIndex").and_then(|v| v.as_u64()).unwrap_or(0);
        if outcome_idx == 0 {
            // Our token is YES
            results.push((cid, token, opp_token));
        } else {
            // Our token is NO
            results.push((cid, opp_token, token));
        }
    }

    Ok(results)
}

/// Get ERC-1155 token balance
async fn get_token_balance(provider: &Provider<Http>, wallet: Address, token_id: &str) -> Result<f64> {
    let ctf: Address = CTF_CONTRACT.parse()?;
    let token_id_u256 = U256::from_dec_str(token_id)?;

    // balanceOf(address,uint256) selector: 0x00fdd58e
    let call_data = ethers::abi::encode(&[
        ethers::abi::Token::Address(wallet),
        ethers::abi::Token::Uint(token_id_u256),
    ]);
    let mut data = vec![0x00, 0xfd, 0xd5, 0x8e];
    data.extend(call_data);

    let tx = TransactionRequest::new().to(ctf).data(data);
    let result = provider.call(&tx.into(), None).await?;
    let balance = U256::from_big_endian(&result);

    Ok(balance.as_u128() as f64 / 1_000_000.0)
}

/// Check if a condition has been resolved
async fn check_condition_resolved(provider: &Provider<Http>, ctf: Address, condition_id: &str) -> Result<bool> {
    let condition_bytes = hex::decode(condition_id)?;
    let mut condition_arr = [0u8; 32];
    condition_arr.copy_from_slice(&condition_bytes);

    // payoutDenominator(bytes32) selector: 0xdd34de67
    let call_data = ethers::abi::encode(&[ethers::abi::Token::FixedBytes(condition_arr.to_vec())]);
    let mut data = vec![0xdd, 0x34, 0xde, 0x67];
    data.extend(call_data);

    let tx = TransactionRequest::new().to(ctf).data(data);
    let result = provider.call(&tx.into(), None).await?;
    let denominator = U256::from_big_endian(&result);

    Ok(denominator > U256::zero())
}

/// Get payout numerator for an outcome
async fn get_payout(provider: &Provider<Http>, ctf: Address, condition_id: &str, outcome_index: u32) -> Result<u64> {
    let condition_bytes = hex::decode(condition_id)?;
    let mut condition_arr = [0u8; 32];
    condition_arr.copy_from_slice(&condition_bytes);

    // payoutNumerators(bytes32,uint256) selector: 0x0504c814
    let call_data = ethers::abi::encode(&[
        ethers::abi::Token::FixedBytes(condition_arr.to_vec()),
        ethers::abi::Token::Uint(U256::from(outcome_index)),
    ]);
    let mut data = vec![0x05, 0x04, 0xc8, 0x14];
    data.extend(call_data);

    let tx = TransactionRequest::new().to(ctf).data(data);
    let result = provider.call(&tx.into(), None).await?;
    let numerator = U256::from_big_endian(&result);

    Ok(numerator.as_u64())
}

/// Encode redeemPositions call
fn encode_redeem_positions(
    collateral: Address,
    parent_collection: [u8; 32],
    condition_id: [u8; 32],
    index_sets: Vec<U256>,
) -> Vec<u8> {
    // Function selector: redeemPositions(address,bytes32,bytes32,uint256[])
    // 0x01b7037c
    let mut data = vec![0x01, 0xb7, 0x03, 0x7c];

    // Encode parameters
    let encoded = ethers::abi::encode(&[
        ethers::abi::Token::Address(collateral),
        ethers::abi::Token::FixedBytes(parent_collection.to_vec()),
        ethers::abi::Token::FixedBytes(condition_id.to_vec()),
        ethers::abi::Token::Array(index_sets.into_iter().map(ethers::abi::Token::Uint).collect()),
    ]);

    data.extend(encoded);
    data
}
