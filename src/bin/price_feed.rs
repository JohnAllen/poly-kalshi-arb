//! Shared Price Feed Service
//!
//! Connects to Polygon.io and broadcasts BTC/ETH prices to local clients via WebSocket.
//! Other binaries connect to ws://127.0.0.1:9999 to receive price updates.

use futures_util::{SinkExt, StreamExt};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{broadcast, RwLock};
use tokio_tungstenite::{accept_async, connect_async, tungstenite::Message};
use tracing::{error, info, warn};

const POLYGON_CRYPTO_WS_URL: &str = "wss://socket.polygon.io/crypto";
const LOCAL_WS_PORT: u16 = 9999;

#[derive(Debug, Default, Clone, Serialize, Deserialize)]
pub struct PriceUpdate {
    pub btc_price: Option<f64>,
    pub eth_price: Option<f64>,
    pub timestamp: i64,
}

#[derive(Debug, Serialize)]
struct AuthMessage {
    action: String,
    params: String,
}

#[derive(Debug, Serialize)]
struct SubscribeMessage {
    action: String,
    params: String,
}

#[derive(Debug, Deserialize)]
struct CryptoTrade {
    ev: Option<String>,
    pair: Option<String>,
    p: Option<f64>,
}

#[derive(Debug, Deserialize)]
struct StatusMessage {
    status: Option<String>,
    message: Option<String>,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter("price_feed=info")
        .init();

    let api_key = std::env::var("POLYGON_KEY")
        .expect("POLYGON_KEY environment variable required");

    info!("════════════════════════════════════════════════════════════════════════");
    info!("📊 SHARED PRICE FEED SERVICE");
    info!("════════════════════════════════════════════════════════════════════════");
    info!("Local WebSocket server: ws://127.0.0.1:{}", LOCAL_WS_PORT);
    info!("Other binaries should set USE_SHARED_PRICES=1");
    info!("════════════════════════════════════════════════════════════════════════");

    // Broadcast channel for price updates
    let (tx, _) = broadcast::channel::<String>(100);
    let tx = Arc::new(tx);

    // Current prices state
    let prices = Arc::new(RwLock::new(PriceUpdate::default()));

    // Start local WebSocket server for clients
    let tx_clone = tx.clone();
    let prices_clone = prices.clone();
    tokio::spawn(async move {
        run_local_server(tx_clone, prices_clone).await;
    });

    // Connect to Polygon and broadcast prices
    loop {
        if let Err(e) = run_polygon_feed(prices.clone(), tx.clone(), &api_key).await {
            error!("[POLYGON] Connection error: {}", e);
        }
        warn!("[POLYGON] Reconnecting in 5 seconds...");
        tokio::time::sleep(tokio::time::Duration::from_secs(5)).await;
    }
}

async fn run_local_server(tx: Arc<broadcast::Sender<String>>, prices: Arc<RwLock<PriceUpdate>>) {
    let listener = TcpListener::bind(format!("127.0.0.1:{}", LOCAL_WS_PORT))
        .await
        .expect("Failed to bind local WebSocket server");

    info!("[SERVER] Listening on ws://127.0.0.1:{}", LOCAL_WS_PORT);

    while let Ok((stream, addr)) = listener.accept().await {
        info!("[SERVER] New client connected: {}", addr);
        let rx = tx.subscribe();
        let prices_clone = prices.clone();
        tokio::spawn(handle_client(stream, rx, prices_clone));
    }
}

async fn handle_client(
    stream: TcpStream,
    mut rx: broadcast::Receiver<String>,
    prices: Arc<RwLock<PriceUpdate>>,
) {
    let ws_stream = match accept_async(stream).await {
        Ok(ws) => ws,
        Err(e) => {
            error!("[SERVER] WebSocket handshake failed: {}", e);
            return;
        }
    };

    let (mut write, mut read) = ws_stream.split();

    // Send current prices immediately on connect
    {
        let state = prices.read().await;
        if let Ok(json) = serde_json::to_string(&*state) {
            let _ = write.send(Message::Text(json)).await;
        }
    }

    // Spawn task to forward broadcasts to this client
    let write = Arc::new(tokio::sync::Mutex::new(write));
    let write_clone = write.clone();

    let forward_task = tokio::spawn(async move {
        while let Ok(msg) = rx.recv().await {
            let mut w = write_clone.lock().await;
            if w.send(Message::Text(msg)).await.is_err() {
                break;
            }
        }
    });

    // Handle incoming messages (pings, etc)
    while let Some(msg) = read.next().await {
        match msg {
            Ok(Message::Ping(data)) => {
                let mut w = write.lock().await;
                let _ = w.send(Message::Pong(data)).await;
            }
            Ok(Message::Close(_)) => break,
            Err(_) => break,
            _ => {}
        }
    }

    forward_task.abort();
    info!("[SERVER] Client disconnected");
}

async fn run_polygon_feed(
    prices: Arc<RwLock<PriceUpdate>>,
    tx: Arc<broadcast::Sender<String>>,
    api_key: &str,
) -> anyhow::Result<()> {
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

    // Keepalive ping
    let write = Arc::new(tokio::sync::Mutex::new(write));
    let write_clone = write.clone();
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
                if let Ok(trades) = serde_json::from_str::<Vec<CryptoTrade>>(&text) {
                    let mut updated = false;
                    for trade in trades {
                        if trade.ev.as_deref() == Some("XT") {
                            if let (Some(pair), Some(price)) = (&trade.pair, trade.p) {
                                let mut state = prices.write().await;
                                let now = chrono::Utc::now().timestamp_millis();
                                state.timestamp = now;

                                match pair.as_str() {
                                    "BTC-USD" => {
                                        if state.btc_price != Some(price) {
                                            info!("[BTC] ${:.2}", price);
                                            state.btc_price = Some(price);
                                            updated = true;
                                        }
                                    }
                                    "ETH-USD" => {
                                        if state.eth_price != Some(price) {
                                            info!("[ETH] ${:.2}", price);
                                            state.eth_price = Some(price);
                                            updated = true;
                                        }
                                    }
                                    _ => {}
                                }
                            }
                        }
                    }

                    // Broadcast to clients if updated
                    if updated {
                        let state = prices.read().await;
                        if let Ok(json) = serde_json::to_string(&*state) {
                            let _ = tx.send(json);
                        }
                    }
                }
            }
            Ok(Message::Ping(data)) => {
                let mut w = write.lock().await;
                let _ = w.send(Message::Pong(data)).await;
            }
            Ok(Message::Pong(_)) => {}
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
