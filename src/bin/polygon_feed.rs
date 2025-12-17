//! Polygon.io Crypto Price Feed
//!
//! Can either:
//! 1. Connect to local price_feed server via WebSocket (preferred, set USE_SHARED_PRICES=1)
//! 2. Connect directly to Polygon WebSocket (fallback)
//!
//! Set USE_SHARED_PRICES=1 to use the shared local server.

use futures_util::{SinkExt, StreamExt};
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use tokio::sync::RwLock;
use tokio_tungstenite::{connect_async, tungstenite::Message};
use tracing::{debug, error, info, warn};

/// Polygon crypto WebSocket URL
const POLYGON_CRYPTO_WS_URL: &str = "wss://socket.polygon.io/crypto";

/// Local price server URL
const LOCAL_PRICE_SERVER: &str = "ws://127.0.0.1:9999";

/// Shared price state
#[derive(Debug, Default, Clone, Serialize, Deserialize)]
pub struct PriceState {
    pub btc_price: Option<f64>,
    pub eth_price: Option<f64>,
    #[serde(default)]
    pub timestamp: Option<i64>,
}

/// Auth message for Polygon
#[derive(Debug, Serialize)]
struct AuthMessage {
    action: String,
    params: String,
}

/// Subscribe message for Polygon
#[derive(Debug, Serialize)]
struct SubscribeMessage {
    action: String,
    params: String,
}

/// Polygon crypto trade message (XT.*)
#[derive(Debug, Deserialize)]
struct CryptoTrade {
    ev: Option<String>,      // Event type: "XT"
    pair: Option<String>,    // e.g., "BTC-USD"
    p: Option<f64>,          // Price
    t: Option<i64>,          // Timestamp
}

/// Polygon crypto quote message (XQ.*)
#[derive(Debug, Deserialize)]
struct CryptoQuote {
    ev: Option<String>,      // Event type: "XQ"
    pair: Option<String>,    // e.g., "BTC-USD"
    bp: Option<f64>,         // Bid price
    ap: Option<f64>,         // Ask price
}

/// Polygon status message
#[derive(Debug, Deserialize)]
struct StatusMessage {
    ev: Option<String>,
    status: Option<String>,
    message: Option<String>,
}

/// Run the Polygon crypto price feed
/// If USE_SHARED_PRICES=1, connects to local price_feed server instead of Polygon directly
pub async fn run_polygon_feed(prices: Arc<RwLock<PriceState>>, api_key: &str) {
    let use_shared = std::env::var("USE_SHARED_PRICES")
        .map(|v| v == "1" || v.to_lowercase() == "true")
        .unwrap_or(false);

    if use_shared {
        info!("[PRICES] Connecting to local price server: {}", LOCAL_PRICE_SERVER);
        loop {
            if let Err(e) = run_local_price_client(prices.clone()).await {
                error!("[PRICES] Connection error: {}", e);
            }
            warn!("[PRICES] Reconnecting to price server in 2 seconds...");
            tokio::time::sleep(tokio::time::Duration::from_secs(2)).await;
        }
    } else {
        info!("[POLYGON] Connecting directly to Polygon WebSocket");
        let key = api_key.to_string();
        loop {
            if let Err(e) = polygon_stream(prices.clone(), &key).await {
                error!("[POLYGON] Connection error: {}", e);
            }
            warn!("[POLYGON] Reconnecting in 5 seconds...");
            tokio::time::sleep(tokio::time::Duration::from_secs(5)).await;
        }
    }
}

/// Connect to local price_feed server via WebSocket
async fn run_local_price_client(prices: Arc<RwLock<PriceState>>) -> anyhow::Result<()> {
    let (ws_stream, _) = connect_async(LOCAL_PRICE_SERVER).await?;
    let (mut write, mut read) = ws_stream.split();

    info!("[PRICES] Connected to local price server");

    while let Some(msg) = read.next().await {
        match msg {
            Ok(Message::Text(text)) => {
                if let Ok(update) = serde_json::from_str::<PriceState>(&text) {
                    let mut state = prices.write().await;
                    if state.btc_price != update.btc_price {
                        debug!("[PRICES] BTC ${:.2}", update.btc_price.unwrap_or(0.0));
                    }
                    if state.eth_price != update.eth_price {
                        debug!("[PRICES] ETH ${:.2}", update.eth_price.unwrap_or(0.0));
                    }
                    state.btc_price = update.btc_price;
                    state.eth_price = update.eth_price;
                    state.timestamp = update.timestamp;
                }
            }
            Ok(Message::Ping(data)) => {
                let _ = write.send(Message::Pong(data)).await;
            }
            Ok(Message::Close(_)) => {
                warn!("[PRICES] Server closed connection");
                break;
            }
            Err(e) => {
                error!("[PRICES] WebSocket error: {}", e);
                break;
            }
            _ => {}
        }
    }

    Ok(())
}

async fn polygon_stream(prices: Arc<RwLock<PriceState>>, api_key: &str) -> anyhow::Result<()> {
    info!("[POLYGON] Connecting to crypto WebSocket...");

    let (ws_stream, _) = connect_async(POLYGON_CRYPTO_WS_URL).await?;
    let (mut write, mut read) = ws_stream.split();

    info!("[POLYGON] Connected, authenticating...");

    // Authenticate
    let auth = AuthMessage {
        action: "auth".to_string(),
        params: api_key.to_string(),
    };
    write.send(Message::Text(serde_json::to_string(&auth)?)).await?;

    // Wait for auth response
    while let Some(msg) = read.next().await {
        if let Ok(Message::Text(text)) = msg {
            if let Ok(status) = serde_json::from_str::<Vec<StatusMessage>>(&text) {
                if let Some(s) = status.first() {
                    if s.status.as_deref() == Some("auth_success") {
                        info!("[POLYGON] Authenticated successfully");
                        break;
                    } else if s.status.as_deref() == Some("auth_failed") {
                        return Err(anyhow::anyhow!("Polygon auth failed: {:?}", s.message));
                    }
                }
            }
        }
    }

    // Subscribe to BTC-USD and ETH-USD trades
    let subscribe = SubscribeMessage {
        action: "subscribe".to_string(),
        params: "XT.BTC-USD,XT.ETH-USD".to_string(),
    };
    write.send(Message::Text(serde_json::to_string(&subscribe)?)).await?;
    info!("[POLYGON] Subscribed to BTC-USD and ETH-USD trades");

    // Use Arc+Mutex for write half to allow ping task access
    let write = Arc::new(tokio::sync::Mutex::new(write));
    let write_clone = write.clone();

    // Spawn keepalive ping task every 10 seconds
    let ping_task = tokio::spawn(async move {
        let mut interval = tokio::time::interval(tokio::time::Duration::from_secs(10));
        loop {
            interval.tick().await;
            let mut w = write_clone.lock().await;
            if w.send(Message::Ping(vec![1, 2, 3, 4])).await.is_err() {
                break;
            }
        }
    });

    while let Some(msg) = read.next().await {
        match msg {
            Ok(Message::Text(text)) => {
                // Polygon sends arrays of messages
                if let Ok(trades) = serde_json::from_str::<Vec<CryptoTrade>>(&text) {
                    for trade in trades {
                        if trade.ev.as_deref() == Some("XT") {
                            if let (Some(pair), Some(price)) = (&trade.pair, trade.p) {
                                let mut state = prices.write().await;
                                match pair.as_str() {
                                    "BTC-USD" => {
                                        state.btc_price = Some(price);
                                    }
                                    "ETH-USD" => {
                                        state.eth_price = Some(price);
                                    }
                                    _ => {}
                                }
                            }
                        }
                    }
                }
            }
            Ok(Message::Ping(data)) => {
                let mut w = write.lock().await;
                let _ = w.send(Message::Pong(data)).await;
            }
            Ok(Message::Pong(_)) => {
                // Keepalive pong received - connection is alive
            }
            Ok(Message::Close(_)) => {
                warn!("[POLYGON] Connection closed by server");
                break;
            }
            Err(e) => {
                error!("[POLYGON] WebSocket error: {}", e);
                break;
            }
            _ => {}
        }
    }

    ping_task.abort();
    Ok(())
}
