//! WS Transport — built-in WebSocket connection management.
//!
//! Handles: connect, auto-reconnect, heartbeat, message routing.
//! Entangled protocol messages (sync/entangle) are handled internally.
//! Unknown messages are forwarded to the host via callback.

#[cfg(feature = "transport")]
mod ws {
    use std::sync::Arc;
    use std::time::{Duration, SystemTime, UNIX_EPOCH};

    use futures_util::{SinkExt, StreamExt};
    use serde_json::Value;
    use tokio::sync::{broadcast, mpsc, Mutex, Notify};
    use tokio_tungstenite::{
        connect_async,
        tungstenite::{client::IntoClientRequest, Error as WsError, Message},
    };

    use crate::auth::AuthProvider;

    /// Outgoing message types (Client → Server).
    #[derive(Debug, serde::Serialize)]
    #[serde(tag = "type", rename_all = "snake_case")]
    pub enum OutMsg {
        /// Establish entity entanglement.
        Entangle {
            entity: String,
            #[serde(skip_serializing_if = "Option::is_none")]
            params: Option<Value>,
            #[serde(skip_serializing_if = "Option::is_none")]
            version: Option<u64>,
            #[serde(skip_serializing_if = "Option::is_none")]
            depth: Option<u64>,
            #[serde(skip_serializing_if = "Option::is_none")]
            before_id: Option<String>,
            #[serde(skip_serializing_if = "Option::is_none")]
            limit: Option<u32>,
            #[serde(skip_serializing_if = "Option::is_none")]
            request_id: Option<String>,
        },
        /// Break entity entanglement.
        Disentangle {
            entity: String,
            #[serde(skip_serializing_if = "Option::is_none")]
            params: Option<Value>,
        },
        /// First-class mutation verb: create/update/delete/upsert/custom.
        Action {
            request_id: String,
            entity: String,
            op: String,
            #[serde(skip_serializing_if = "Option::is_none")]
            id: Option<String>,
            #[serde(skip_serializing_if = "Option::is_none")]
            params: Option<Value>,
            #[serde(skip_serializing_if = "Option::is_none")]
            data: Option<Value>,
        },
        Pong,
    }

    /// Incoming message classification.
    pub enum InMsg {
        /// Entangled sync frame — handled internally by the engine
        Sync(Value),
        /// Action acknowledgement — matched by request_id for optimistic update correlation
        Ack {
            request_id: String,
            success: bool,
            data: Option<Value>,
            error: Option<String>,
        },
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
                "error" => InMsg::Ack {
                    request_id: val
                        .get("request_id")
                        .and_then(|v| v.as_str())
                        .unwrap_or("")
                        .to_string(),
                    success: false,
                    data: None,
                    error: val
                        .get("error")
                        .and_then(|v| v.as_str())
                        .map(|s| s.to_string()),
                },
                "ack" => InMsg::Ack {
                    request_id: val
                        .get("request_id")
                        .and_then(|v| v.as_str())
                        .unwrap_or("")
                        .to_string(),
                    success: val
                        .get("success")
                        .and_then(|v| v.as_bool())
                        .unwrap_or(false),
                    data: val.get("data").cloned(),
                    error: val
                        .get("error")
                        .and_then(|v| v.as_str())
                        .map(|s| s.to_string()),
                },
                "ping" | "heartbeat" => InMsg::Ping,
                "push" => InMsg::Push {
                    event: val
                        .get("event")
                        .and_then(|v| v.as_str())
                        .unwrap_or("")
                        .to_string(),
                    data: val.get("data").cloned(),
                },
                _ => InMsg::Unknown,
            }
        }
    }

    const RECONNECT_BASE_DELAY: Duration = Duration::from_secs(3);
    const RECONNECT_MAX_DELAY: Duration = Duration::from_secs(60);
    const RECONNECT_JITTER_PCT: u128 = 20;

    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    enum ConnectFailure {
        AuthRejected(u16),
        Other,
    }

    fn classify_connect_failure(error: &WsError) -> ConnectFailure {
        match error {
            WsError::Http(response) if matches!(response.status().as_u16(), 401 | 403) => {
                ConnectFailure::AuthRejected(response.status().as_u16())
            }
            _ => ConnectFailure::Other,
        }
    }

    fn reconnect_seed() -> u64 {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_nanos() as u64)
            .unwrap_or(0xE17A_6EED)
    }

    fn reconnect_delay(attempt: u32, seed: u64) -> Duration {
        let exponent = attempt.min(8);
        let base_ms = RECONNECT_BASE_DELAY
            .as_millis()
            .saturating_mul(1u128 << exponent);
        let capped_ms = base_ms.min(RECONNECT_MAX_DELAY.as_millis());
        let jitter_window = capped_ms.saturating_mul(RECONNECT_JITTER_PCT) / 100;
        let jitter = if jitter_window == 0 {
            0
        } else {
            deterministic_jitter(seed, attempt) as u128 % (jitter_window + 1)
        };
        Duration::from_millis((capped_ms + jitter).min(RECONNECT_MAX_DELAY.as_millis()) as u64)
    }

    fn deterministic_jitter(seed: u64, attempt: u32) -> u64 {
        let mut x = seed ^ ((attempt as u64).wrapping_mul(0x9E37_79B9_7F4A_7C15));
        x ^= x >> 30;
        x = x.wrapping_mul(0xBF58_476D_1CE4_E5B9);
        x ^= x >> 27;
        x = x.wrapping_mul(0x94D0_49BB_1331_11EB);
        x ^ (x >> 31)
    }

    #[cfg(test)]
    mod tests {
        use super::{
            classify_connect_failure, reconnect_delay, ConnectFailure, InMsg, OutMsg, WsTransport,
            RECONNECT_MAX_DELAY,
        };
        use tokio_tungstenite::tungstenite::{
            http::{Response, StatusCode},
            Error as WsError,
        };

        #[test]
        fn parses_ack_with_canonical_request_id() {
            match InMsg::parse(
                r#"{"type":"ack","request_id":"req-1","success":true,"data":{"ok":true}}"#,
            ) {
                InMsg::Ack {
                    request_id,
                    success,
                    data,
                    error,
                } => {
                    assert_eq!(request_id, "req-1");
                    assert!(success);
                    assert_eq!(data.unwrap()["ok"], true);
                    assert!(error.is_none());
                }
                _ => panic!("expected ack"),
            }
        }

        #[test]
        fn does_not_parse_retired_request_id_alias() {
            match InMsg::parse(r#"{"type":"error","requestId":"legacy","error":"boom"}"#) {
                InMsg::Ack {
                    request_id,
                    success,
                    error,
                    ..
                } => {
                    assert_eq!(request_id, "");
                    assert!(!success);
                    assert_eq!(error.as_deref(), Some("boom"));
                }
                _ => panic!("expected error ack"),
            }
        }

        #[test]
        fn serializes_standard_entangle_depth() {
            let frame = super::OutMsg::Entangle {
                entity: "agent-activity-records".to_string(),
                params: Some(serde_json::json!({"agent_id": "agent-1"})),
                version: None,
                depth: Some(200),
                before_id: None,
                limit: None,
                request_id: None,
            };

            let value = serde_json::to_value(frame).expect("frame should serialize");

            assert_eq!(value["type"], "entangle");
            assert_eq!(value["entity"], "agent-activity-records");
            assert_eq!(value["params"]["agent_id"], "agent-1");
            assert_eq!(value["depth"], 200);
            assert!(value.get("limit").is_none());
        }

        #[test]
        fn reconnect_delay_is_bounded_and_deterministic() {
            assert_eq!(reconnect_delay(0, 7), reconnect_delay(0, 7));
            assert!(reconnect_delay(1, 7) >= reconnect_delay(0, 7));
            assert!(reconnect_delay(20, 7) <= RECONNECT_MAX_DELAY);
        }

        #[tokio::test]
        async fn send_fails_fast_when_disconnected() {
            let (transport, _) = WsTransport::new();

            let error = transport
                .send(&OutMsg::Pong)
                .await
                .expect_err("disconnected send should fail immediately");

            assert!(error.contains("disconnected"));
        }

        #[tokio::test]
        async fn action_fails_fast_when_disconnected() {
            let (transport, _) = WsTransport::new();

            let error = transport
                .send_action("agents", "update", None, None, None)
                .await
                .expect_err("disconnected action should fail immediately");

            assert!(error.contains("disconnected"));
        }

        #[test]
        fn classifies_only_http_401_and_403_as_auth_rejection() {
            let http_error = |status| {
                WsError::Http(
                    Response::builder()
                        .status(status)
                        .body(None)
                        .expect("valid HTTP response"),
                )
            };

            assert_eq!(
                classify_connect_failure(&http_error(StatusCode::UNAUTHORIZED)),
                ConnectFailure::AuthRejected(401)
            );
            assert_eq!(
                classify_connect_failure(&http_error(StatusCode::FORBIDDEN)),
                ConnectFailure::AuthRejected(403)
            );
            assert_eq!(
                classify_connect_failure(&http_error(StatusCode::BAD_REQUEST)),
                ConnectFailure::Other
            );
            assert_eq!(
                classify_connect_failure(&WsError::ConnectionClosed),
                ConnectFailure::Other
            );
        }

        #[tokio::test]
        async fn reconnect_request_keeps_a_permit_until_the_run_loop_observes_it() {
            let (transport, _) = WsTransport::new();

            transport.request_reconnect();

            tokio::time::timeout(
                std::time::Duration::from_millis(100),
                transport.reconnect_requested.notified(),
            )
            .await
            .expect("reconnect request should wake the connection loop");
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
        /// Explicitly interrupts either an active connection or reconnect backoff.
        reconnect_requested: Notify,
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
                reconnect_requested: Notify::new(),
            };
            (transport, sync_tx)
        }

        pub fn is_connected(&self) -> bool {
            self.connected.load(std::sync::atomic::Ordering::Acquire)
        }

        /// Interrupt the current connection/backoff so the next handshake reads
        /// fresh credentials from the shared AuthProvider.
        pub fn request_reconnect(&self) {
            // notify_one stores one permit when the run loop is between select
            // points, so a token update cannot be lost in that race window.
            self.reconnect_requested.notify_one();
        }

        async fn mark_disconnected(&self) -> bool {
            let was_connected = self
                .connected
                .swap(false, std::sync::atomic::Ordering::AcqRel);
            *self.sink.lock().await = None;
            if was_connected {
                let _ = self.connected_tx.send(false);
            }
            was_connected
        }

        /// Send a message to the server.
        pub async fn send(&self, msg: &OutMsg) -> Result<(), String> {
            let mut guard = self.sink.lock().await;
            let sink = guard
                .as_mut()
                .ok_or_else(|| "Entangled transport disconnected".to_string())?;
            let json = serde_json::to_string(msg).map_err(|e| format!("serialize failed: {e}"))?;
            sink.send(Message::Text(json))
                .await
                .map_err(|e| format!("Entangled send failed: {e}"))
        }

        /// Send an entangle frame.
        pub async fn entangle(
            &self,
            entity: &str,
            params: Option<Value>,
            version: Option<u64>,
            depth: Option<u64>,
        ) {
            if let Err(e) = self
                .send(&OutMsg::Entangle {
                    entity: entity.to_string(),
                    params,
                    version,
                    depth,
                    before_id: None,
                    limit: None,
                    request_id: None,
                })
                .await
            {
                tracing::warn!("[Entangled] entangle send failed: {}", e);
            }
        }

        /// Host-facing alias used by embedded App sync bridge.
        pub async fn subscribe(
            &self,
            entity: &str,
            params: Option<Value>,
            version: Option<u64>,
            depth: Option<u64>,
        ) {
            self.entangle(entity, params, version, depth).await;
        }

        /// Send a disentangle frame.
        pub async fn disentangle(&self, entity: &str, params: Option<Value>) {
            if let Err(e) = self
                .send(&OutMsg::Disentangle {
                    entity: entity.to_string(),
                    params,
                })
                .await
            {
                tracing::warn!("[Entangled] disentangle send failed: {}", e);
            }
        }

        /// Send a first-class action (mutation) and wait for ack.
        pub async fn send_action(
            &self,
            entity: &str,
            op: &str,
            id: Option<String>,
            params: Option<Value>,
            data: Option<Value>,
        ) -> Result<Value, String> {
            let request_id = uuid::Uuid::new_v4().to_string();
            let mut rx = self.response_tx.subscribe();

            self.send(&OutMsg::Action {
                request_id: request_id.clone(),
                entity: entity.to_string(),
                op: op.to_string(),
                id,
                params,
                data,
            })
            .await?;

            let deadline = tokio::time::Instant::now() + Duration::from_secs(15);
            loop {
                match tokio::time::timeout_at(deadline, rx.recv()).await {
                    Ok(Ok((rid, result))) if rid == request_id => return result,
                    Ok(Ok(_)) => continue,
                    Ok(Err(_)) => return Err("Channel closed".into()),
                    Err(_) => return Err("Action timeout (15s)".into()),
                }
            }
        }

        /// Request an older page for stream entities.
        pub async fn send_load_more(
            &self,
            entity: &str,
            params: Option<Value>,
            before_id: Option<String>,
            limit: u32,
        ) -> Result<Value, String> {
            let request_id = uuid::Uuid::new_v4().to_string();
            let mut rx = self.response_tx.subscribe();

            self.send(&OutMsg::Entangle {
                entity: entity.to_string(),
                params,
                version: None,
                depth: None,
                before_id,
                limit: Some(limit),
                request_id: Some(request_id.clone()),
            })
            .await?;

            let deadline = tokio::time::Instant::now() + Duration::from_secs(15);
            loop {
                match tokio::time::timeout_at(deadline, rx.recv()).await {
                    Ok(Ok((rid, result))) if rid == request_id => return result,
                    Ok(Ok(_)) => continue,
                    Ok(Err(_)) => return Err("Channel closed".into()),
                    Err(_) => return Err("load_more timeout (15s)".into()),
                }
            }
        }

        /// Take the sync frame receiver (can only be called once).
        pub async fn take_sync_receiver(&self) -> Option<mpsc::UnboundedReceiver<Value>> {
            self.sync_rx.lock().await.take()
        }

        /// Listen to connection state changes.
        pub fn subscribe_connection_state(&self) -> broadcast::Receiver<bool> {
            self.connected_tx.subscribe()
        }

        /// Listen to server push events such as the initial schema frame.
        pub fn subscribe_push(&self) -> broadcast::Receiver<(String, Option<Value>)> {
            self.push_tx.subscribe()
        }

        /// Start the connection loop (blocking — run in a spawned task).
        pub async fn run(
            self: Arc<Self>,
            ws_url: &str,
            auth: Arc<dyn AuthProvider>,
            sync_tx: mpsc::UnboundedSender<Value>,
            shutdown: Arc<Notify>,
        ) {
            let ws_base = ws_url
                .replace("http://", "ws://")
                .replace("https://", "wss://");

            let mut retry_attempt: u32 = 0;
            let retry_seed = reconnect_seed();
            loop {
                let retry_delay = tokio::select! {
                    biased;
                    _ = shutdown.notified() => {
                        self.mark_disconnected().await;
                        tracing::info!("[Entangled] Shutdown requested");
                        return;
                    }
                    _ = self.reconnect_requested.notified() => {
                        self.mark_disconnected().await;
                        retry_attempt = 0;
                        tracing::info!("[Entangled] Reconnect requested");
                        continue;
                    }
                    _ = self.run_single_connection(&ws_base, &auth, &sync_tx) => {
                        let was_connected = self.mark_disconnected().await;
                        let retry_delay = if was_connected {
                            retry_attempt = 0;
                            RECONNECT_BASE_DELAY
                        } else {
                            let delay = reconnect_delay(retry_attempt, retry_seed);
                            retry_attempt = retry_attempt.saturating_add(1);
                            delay
                        };
                        tracing::warn!(
                            retry_delay_ms = retry_delay.as_millis(),
                            was_connected,
                            "[Entangled] Disconnected; retrying after backoff"
                        );
                        retry_delay
                    }
                };

                tokio::select! {
                    biased;
                    _ = shutdown.notified() => {
                        self.mark_disconnected().await;
                        return;
                    },
                    _ = self.reconnect_requested.notified() => {
                        retry_attempt = 0;
                        tracing::info!("[Entangled] Reconnect requested during backoff");
                    },
                    _ = tokio::time::sleep(retry_delay) => {}
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
                        if let (Ok(name), Ok(val)) = (
                            k.parse::<tokio_tungstenite::tungstenite::http::header::HeaderName>(),
                            v.parse::<tokio_tungstenite::tungstenite::http::header::HeaderValue>(),
                        ) {
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
                    if let ConnectFailure::AuthRejected(status) = classify_connect_failure(&e) {
                        tracing::warn!(
                            status,
                            "[Entangled] Authentication rejected during WebSocket handshake"
                        );
                        auth.on_auth_rejected();
                    }
                    tracing::warn!("[Entangled] Connect failed: {}", e);
                    return;
                }
            };

            let (sink, mut stream) = ws.split();
            *self.sink.lock().await = Some(sink);
            self.connected
                .store(true, std::sync::atomic::Ordering::Release);
            // Publish connected only after send() can observe the sink. Reconnect
            // subscribers use this edge to restore subscriptions immediately.
            let _ = self.connected_tx.send(true);

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
                            Err(_) => { tracing::warn!("[Entangled] Read timeout (90s no data)"); return; }
                            Ok(Some(r)) => r,
                            Ok(None) => { tracing::warn!("[Entangled] WS stream ended (server closed)"); return; }
                        }
                    }
                };

                let text = match msg {
                    Ok(Message::Text(t)) => t,
                    Ok(Message::Close(frame)) => {
                        if let Some(ref f) = frame {
                            tracing::warn!(
                                "[Entangled] Server closed connection: code={}, reason={}",
                                u16::from(f.code),
                                f.reason
                            );
                            if f.code == tokio_tungstenite::tungstenite::protocol::frame::coding::CloseCode::from(4001) {
                                auth.on_auth_rejected();
                            }
                        } else {
                            tracing::warn!("[Entangled] Server closed connection (no close frame)");
                        }
                        return;
                    }
                    Ok(Message::Ping(payload)) => {
                        let mut g = self.sink.lock().await;
                        if let Some(ref mut s) = *g {
                            let _ = s.send(Message::Pong(payload)).await;
                        }
                        continue;
                    }
                    Err(e) => {
                        tracing::warn!("[Entangled] WS error: {}", e);
                        return;
                    }
                    _ => continue,
                };

                match InMsg::parse(&text) {
                    InMsg::Sync(val) => {
                        let request_id = val
                            .get("request_id")
                            .and_then(|v| v.as_str())
                            .filter(|s| !s.is_empty())
                            .map(String::from);
                        if let Some(request_id) = request_id {
                            let payload = serde_json::json!({
                                "entries": val.get("data").cloned().unwrap_or(Value::Array(vec![])),
                                "has_more": val.get("hasMore").cloned().unwrap_or(Value::Bool(false)),
                                "success": true,
                            });
                            let _ = self.response_tx.send((request_id, Ok(payload)));
                        } else {
                            let _ = sync_tx.send(val);
                        }
                    }
                    InMsg::Ack {
                        request_id,
                        success,
                        data,
                        error,
                    } => {
                        let result = if !success {
                            Err(error.unwrap_or_else(|| "action failed".into()))
                        } else {
                            Ok(data.unwrap_or(Value::Null))
                        };
                        let _ = self.response_tx.send((request_id, result));
                    }
                    InMsg::Ping => {
                        let _ = self.send(&OutMsg::Pong).await;
                    }
                    InMsg::Push { event, data } => {
                        let _ = self.push_tx.send((event, data));
                    }
                    InMsg::Unknown => {
                        let msg_type = serde_json::from_str::<Value>(&text)
                            .ok()
                            .and_then(|v| v.get("type").and_then(|t| t.as_str()).map(String::from));
                        tracing::debug!(
                            "[Entangled] Unhandled message type: {:?}",
                            msg_type.as_deref().unwrap_or("(no type)")
                        );
                    }
                }
            }
        }
    }
}

#[cfg(feature = "transport")]
pub use ws::*;
