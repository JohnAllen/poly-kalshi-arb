//! Polymarket market discovery via Gamma API
//!
//! Provides structs and functions for discovering Polymarket markets,
//! particularly the 15-minute crypto price prediction markets.

use anyhow::Result;
use chrono::Utc;
use serde::Deserialize;
use std::collections::HashMap;
use std::time::Duration;
use tracing::warn;

/// Gamma API base URL
pub const GAMMA_API_BASE: &str = "https://gamma-api.polymarket.com";

/// Series slugs for crypto 15-minute markets
/// Format: (series_slug, asset_symbol)
pub const POLY_SERIES_SLUGS: &[(&str, &str)] = &[
    ("btc-up-or-down-15m", "BTC"),
    ("eth-up-or-down-15m", "ETH"),
    ("sol-up-or-down-15m", "SOL"),
    ("xrp-up-or-down-15m", "XRP"),
];

/// Gamma API series response
#[derive(Debug, Deserialize)]
pub struct GammaSeries {
    pub events: Option<Vec<GammaEvent>>,
}

/// Gamma API event in a series
#[derive(Debug, Deserialize)]
#[allow(dead_code)]
pub struct GammaEvent {
    pub slug: Option<String>,
    pub closed: Option<bool>,
    #[serde(rename = "enableOrderBook")]
    pub enable_order_book: Option<bool>,
}

/// Core Polymarket market data from Gamma API
#[derive(Debug, Clone)]
pub struct PolyMarket {
    /// Unique market identifier
    pub condition_id: String,
    /// Market question/title
    pub question: String,
    /// YES outcome token ID
    pub yes_token: String,
    /// NO outcome token ID
    pub no_token: String,
    /// Underlying asset (BTC, ETH, SOL, XRP)
    pub asset: String,
    /// Minutes until market expires (None if already expired)
    pub expiry_minutes: Option<f64>,
    /// Unix timestamp when the 15-minute window started (extracted from slug)
    pub window_start_ts: Option<i64>,
}

impl PolyMarket {
    /// Get all token IDs for this market
    pub fn token_ids(&self) -> Vec<String> {
        vec![self.yes_token.clone(), self.no_token.clone()]
    }

    /// Check if market is expired
    pub fn is_expired(&self) -> bool {
        self.expiry_minutes.is_none()
    }
}

/// Discover the soonest expiring markets for each asset from Gamma API
///
/// # Arguments
/// * `asset_filter` - Optional filter to only return markets for a specific asset (e.g., "BTC")
///
/// # Returns
/// Vector of discovered markets, one per asset (the soonest expiring for each)
pub async fn discover_markets(asset_filter: Option<&str>) -> Result<Vec<PolyMarket>> {
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
            .header("User-Agent", "poly_arb/1.0")
            .send()
            .await?;

        if !resp.status().is_success() {
            warn!("[market_discovery] Failed to fetch series '{}': {}", series_slug, resp.status());
            continue;
        }

        let series_list: Vec<GammaSeries> = resp.json().await?;
        let Some(series) = series_list.first() else { continue };
        let Some(events) = &series.events else { continue };

        // Get current timestamp to find active/near-future markets
        let now_ts = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_secs() as i64)
            .unwrap_or(0);

        // Find markets that have started (slug timestamp <= now) but not closed
        // Events are sorted oldest-first, so iterate forward to find recent ones
        let event_slugs: Vec<String> = events
            .iter()
            .filter(|e| e.closed != Some(true) && e.enable_order_book == Some(true))
            .filter_map(|e| {
                let slug = e.slug.as_ref()?;
                // Extract timestamp from slug (e.g., "btc-updown-15m-1766190600")
                let ts: i64 = slug.rsplit('-').next()?.parse().ok()?;
                // Keep markets where window has started (ts <= now) or starts within 16 min
                if ts <= now_ts + 960 {
                    Some(slug.clone())
                } else {
                    None
                }
            })
            .collect::<Vec<_>>()
            .into_iter()
            .rev() // Now reverse to get most recent (closest to now) first
            .take(3)
            .collect();

        for event_slug in event_slugs {
            let event_url = format!("{}/events?slug={}", GAMMA_API_BASE, event_slug);
            let resp = match client.get(&event_url)
                .header("User-Agent", "poly_arb/1.0")
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
                let slug = mkt.get("slug")
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

                // Extract window start timestamp from slug (e.g., "btc-updown-15m-1766275200")
                let window_start_ts = slug.as_ref().and_then(|s| {
                    s.rsplit('-').next()?.parse::<i64>().ok()
                });

                // Skip expired markets
                if expiry_minutes.is_none() {
                    continue;
                }

                markets.push(PolyMarket {
                    condition_id: cid,
                    question,
                    yes_token: token_ids[0].clone(),
                    no_token: token_ids[1].clone(),
                    asset: asset.to_string(),
                    expiry_minutes,
                    window_start_ts,
                });
            }
        }
    }

    // Keep only soonest market per asset
    let mut best_per_asset: HashMap<String, PolyMarket> = HashMap::new();
    for market in markets {
        let expiry = market.expiry_minutes.unwrap_or(f64::MAX);
        let existing = best_per_asset.get(&market.asset);
        if existing.is_none() || existing.unwrap().expiry_minutes.unwrap_or(f64::MAX) > expiry {
            best_per_asset.insert(market.asset.clone(), market);
        }
    }

    Ok(best_per_asset.into_values().collect())
}

/// Discover all active markets (not just soonest per asset)
pub async fn discover_all_markets(asset_filter: Option<&str>) -> Result<Vec<PolyMarket>> {
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
            .header("User-Agent", "poly_arb/1.0")
            .send()
            .await?;

        if !resp.status().is_success() {
            warn!("[market_discovery] Failed to fetch series '{}': {}", series_slug, resp.status());
            continue;
        }

        let series_list: Vec<GammaSeries> = resp.json().await?;
        let Some(series) = series_list.first() else { continue };
        let Some(events) = &series.events else { continue };

        // Get current timestamp to find active/near-future markets
        let now_ts = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_secs() as i64)
            .unwrap_or(0);

        // Find markets that have started or start within 16 minutes
        let event_slugs: Vec<String> = events
            .iter()
            .filter(|e| e.closed != Some(true) && e.enable_order_book == Some(true))
            .filter_map(|e| {
                let slug = e.slug.as_ref()?;
                let ts: i64 = slug.rsplit('-').next()?.parse().ok()?;
                if ts <= now_ts + 960 {
                    Some(slug.clone())
                } else {
                    None
                }
            })
            .collect();

        for event_slug in event_slugs {
            let event_url = format!("{}/events?slug={}", GAMMA_API_BASE, event_slug);
            let resp = match client.get(&event_url)
                .header("User-Agent", "poly_arb/1.0")
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
                let slug = mkt.get("slug")
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

                // Extract window start timestamp from slug (e.g., "btc-updown-15m-1766275200")
                let window_start_ts = slug.as_ref().and_then(|s| {
                    s.rsplit('-').next()?.parse::<i64>().ok()
                });

                // Skip expired markets
                if expiry_minutes.is_none() {
                    continue;
                }

                markets.push(PolyMarket {
                    condition_id: cid,
                    question,
                    yes_token: token_ids[0].clone(),
                    no_token: token_ids[1].clone(),
                    asset: asset.to_string(),
                    expiry_minutes,
                    window_start_ts,
                });
            }
        }
    }

    // Sort by expiry time (soonest first)
    markets.sort_by(|a, b| {
        let a_exp = a.expiry_minutes.unwrap_or(f64::MAX);
        let b_exp = b.expiry_minutes.unwrap_or(f64::MAX);
        a_exp.partial_cmp(&b_exp).unwrap_or(std::cmp::Ordering::Equal)
    });

    Ok(markets)
}
