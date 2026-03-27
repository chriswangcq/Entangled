//! WS Transport — built-in WebSocket connection management.
//!
//! Handles: connect, auto-reconnect, heartbeat, message routing.
//! Entangled protocol messages (sync/subscribe) are handled internally.
//! Unknown messages are forwarded to the host via callback.

#[cfg(feature = "transport")]
mod ws {
    use std::sync::Arc;
    use std::time::Duration;

    use futures_util::{SinkExt, StreamExt};
    use serde_json::Value;
    use tokio::sync::{Mutex, Notify, broadcast, mpsc};
    use tokio_tungstenite::{
        connect_async,
        tungstenite::{client::IntoClientRequest, Message},
    };

    use crate::auth::AuthProvider;

    /// Outgoing message types (Client → Server).
    #[derive(Debug, serde::Serialize)]
    #[serde(tag = "type", rename_all = "snake_case")]
    pub enum OutMsg {
        Subscribe {
            entity: String,
            #[serde(skip_serializing_if = "Option::is_none")]
            params: Option<Value>,
            #[serde(skip_serializing_if = "Option::is_none")]
            version: Option<u64>,
        },
        Unsubscribe {
            entity: String,
            #[serde(skip_serializing_if = "Option::is_none")]
            params: Option<Value>,
        },
        Request {
            request_id: String,
            action: String,
            #[serde(skip_serializing_if = "Option::is_none")]
            path: Option<String>,
            #[serde(skip_serializing_if = "Option::is_none")]
            data: Option<Value>,
        },
        Pong,
    }

    /// Incoming message classification.
    pub enum InMsg {
        /// Entangled sync frame — handled internally by the engine
        Sync(Value),
        /// Request/response — matched by request_id
        Response { request_id: String, data: Option<Value>, error: Option<String> },
        /// Server ping
        Ping,
        /// Unknown / host-specific push event
        Push { event: String, data: Option<Value> },
        /// Unrecognized
        Unknown,
    }

    impl InMsg {
        pub fn parse(text: &str) -> Self {
            let val: Value = match serde_json::from_str(text) {
                Ok(v) => v,
                Err(_) => return InMsg::Unknown,
            };
            let msg_type = val.get("type").and_then(|t| t.as_str()).unwrap_or("");
            match msg_type {
                "sync" => InMsg::Sync(val),
                "response" => InMsg::Response {
                    request_id: val.get("request_id").and_then(|v| v.as_str()).unwrap_or("").to_string(),
                    data: val.get("data").cloned(),
                    error: val.get("error").and_then(|v| v.as_str()).map(|s| s.to_string()),
                },
                "ping" => InMsg::Ping,
                "push" => InMsg::Push {
                    event: val.get("event").and_then(|v| v.as_str()).unwrap_or("").to_string(),
                    data: val.get("data").cloned(),
                },
                _ => InMsg::Unknown,
            }
        }
    }

    type SplitSink = futures_util::stream::SplitSink<
        tokio_tungstenite::WebSocketStream<
            tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>,
        >,
        Message,
    >;

    /// WS connection manager — owned by EntangledClient.
    pub struct WsTransport {
        sink: Arc<Mutex<Option<SplitSink>>>,
        connected: Arc<std::sync::atomic::AtomicBool>,
        /// Sync frames go here → processed by EntangledClient
        pub(crate) sync_rx: Mutex<Option<mpsc::UnboundedReceiver<Value>>>,
        /// Response frames go here
        pub(crate) response_tx: broadcast::Sender<(String, Result<Value, String>)>,
        /// Push events (host-specific) go here
        pub(crate) push_tx: broadcast::Sender<(String, Option<Value>)>,
        /// Connection state change
        pub(crate) connected_tx: broadcast::Sender<bool>,
    }

    impl WsTransport {
        pub fn new() -> (Self, mpsc::UnboundedSender<Value>) {
            let (sync_tx, sync_rx) = mpsc::unbounded_channel();
            let (response_tx, _) = broadcast::channel(64);
            let (push_tx, _) = broadcast::channel(64);
            let (connected_tx, _) = broadcast::channel(16);

            let transport = Self {
                sink: Arc::new(Mutex::new(None)),
                connected: Arc::new(std::sync::atomic::AtomicBool::new(false)),
                sync_rx: Mutex::new(Some(sync_rx)),
                response_tx,
                push_tx,
                connected_tx,
            };
            (transport, sync_tx)
        }

        pub fn is_connected(&self) -> bool {
            self.connected.load(std::sync::atomic::Ordering::Relaxed)
        }

        /// Send a message to the server.
        pub async fn send(&self, msg: &OutMsg) {
            let mut guard = self.sink.lock().await;
            if let Some(ref mut sink) = *guard {
                if let Ok(json) = serde_json::to_string(msg) {
                    let _ = sink.send(Message::Text(json)).await;
                }
            }
        }

        /// Send a subscribe frame.
        pub async fn subscribe(&self, entity: &str, params: Option<Value>, version: Option<u64>) {
            self.send(&OutMsg::Subscribe {
                entity: entity.to_string(),
                params,
                version,
            }).await;
        }

        /// Send an unsubscribe frame.
        pub async fn unsubscribe(&self, entity: &str, params: Option<Value>) {
            self.send(&OutMsg::Unsubscribe {
                entity: entity.to_string(),
                params,
            }).await;
        }

        /// Send a request and wait for response.
        pub async fn request(&self, action: &str, data: Option<Value>) -> Result<Value, String> {
            let request_id = uuid::Uuid::new_v4().to_string();
            let mut rx = self.response_tx.subscribe();

            self.send(&OutMsg::Request {
                request_id: request_id.clone(),
                action: action.to_string(),
                path: None,
                data,
            }).await;

            // Wait for matching response (15s timeout)
            let deadline = tokio::time::Instant::now() + Duration::from_secs(15);
            loop {
                match tokio::time::timeout_at(deadline, rx.recv()).await {
                    Ok(Ok((rid, result))) if rid == request_id => return result,
                    Ok(Ok(_)) => continue, // not our request
                    Ok(Err(_)) => return Err("Channel closed".into()),
                    Err(_) => return Err("Request timeout (15s)".into()),
                }
            }
        }

        /// Start the connection loop (blocking — run in a spawned task).
        pub async fn run(
            self: Arc<Self>,
            ws_url: &str,
            auth: Arc<dyn AuthProvider>,
            sync_tx: mpsc::UnboundedSender<Value>,
            shutdown: Arc<Notify>,
        ) {
            let ws_base = ws_url.replace("http://", "ws://").replace("https://", "wss://");

            loop {
                tokio::select! {
                    biased;
                    _ = shutdown.notified() => {
                        tracing::info!("[Entangled] Shutdown requested");
                        return;
                    }
                    _ = self.run_single_connection(&ws_base, &auth, &sync_tx) => {
                        self.connected.store(false, std::sync::atomic::Ordering::Relaxed);
                        let _ = self.connected_tx.send(false);
                        tracing::warn!("[Entangled] Disconnected, retrying in 3s...");
                    }
                }

                tokio::select! {
                    biased;
                    _ = shutdown.notified() => return,
                    _ = tokio::time::sleep(Duration::from_secs(3)) => {}
                }
            }
        }

        async fn run_single_connection(
            &self,
            ws_url: &str,
            auth: &Arc<dyn AuthProvider>,
            sync_tx: &mpsc::UnboundedSender<Value>,
        ) {
            // Build request with auth headers
            let req = match ws_url.into_client_request() {
                Ok(mut r) => {
                    for (k, v) in auth.auth_headers() {
                        if let (Ok(name), Ok(val)) = (k.parse::<tokio_tungstenite::tungstenite::http::header::HeaderName>(), v.parse::<tokio_tungstenite::tungstenite::http::header::HeaderValue>()) {
                            r.headers_mut().insert(name, val);
                        }
                    }
                    r
                }
                Err(e) => {
                    tracing::error!("[Entangled] Bad URL: {}", e);
                    return;
                }
            };

            let (ws, _) = match connect_async(req).await {
                Ok(s) => {
                    tracing::info!("[Entangled] Connected to {}", ws_url);
                    s
                }
                Err(e) => {
                    tracing::warn!("[Entangled] Connect failed: {}", e);
                    return;
                }
            };

            self.connected.store(true, std::sync::atomic::Ordering::Relaxed);
            let _ = self.connected_tx.send(true);

            let (sink, mut stream) = ws.split();
            *self.sink.lock().await = Some(sink);

            let mut heartbeat = tokio::time::interval(Duration::from_secs(30));
            heartbeat.tick().await;

            loop {
                let msg = tokio::select! {
                    biased;
                    _ = heartbeat.tick() => {
                        let mut g = self.sink.lock().await;
                        if let Some(ref mut s) = *g {
                            let _ = s.send(Message::Ping(vec![])).await;
                        }
                        continue;
                    }
                    r = tokio::time::timeout(Duration::from_secs(90), stream.next()) => {
                        match r {
                            Err(_) => { tracing::warn!("[Entangled] Read timeout"); return; }
                            Ok(Some(r)) => r,
                            Ok(None) => return,
                        }
                    }
                };

                let text = match msg {
                    Ok(Message::Text(t)) => t,
                    Ok(Message::Close(frame)) => {
                        // Check for auth rejection
                        if let Some(ref f) = frame {
                            if f.code == tokio_tungstenite::tungstenite::protocol::frame::coding::CloseCode::from(4001) {
                                tracing::warn!("[Entangled] Auth rejected by server");
                                auth.on_auth_rejected();
                            }
                        }
                        return;
                    }
                    Ok(Message::Ping(_)) => continue,
                    Err(e) => { tracing::warn!("[Entangled] WS error: {}", e); return; }
                    _ => continue,
                };

                match InMsg::parse(&text) {
                    InMsg::Sync(val) => {
                        let _ = sync_tx.send(val);
                    }
                    InMsg::Response { request_id, data, error } => {
                        let result = if let Some(err) = error {
                            Err(err)
                        } else {
                            Ok(data.unwrap_or(Value::Null))
                        };
                        let _ = self.response_tx.send((request_id, result));
                    }
                    InMsg::Ping => {
                        self.send(&OutMsg::Pong).await;
                    }
                    InMsg::Push { event, data } => {
                        let _ = self.push_tx.send((event, data));
                    }
                    InMsg::Unknown => {}
                }
            }
        }
    }
}

#[cfg(feature = "transport")]
pub use ws::*;
