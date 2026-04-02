//! EntangledClient — the unified, self-contained entity sync engine.
//!
//! # Usage (standalone mode — Entangled owns the WS)
//! ```ignore
//! let client = EntangledClient::connect(
//!     "https://api.example.com/ws",
//!     Box::new(JwtAuth::new(token)),
//!     "/data/entangled",
//! ).await;
//!
//! // Subscribe to an entity
//! client.subscribe("todos", None).await;
//!
//! // Read from local cache
//! let todos = client.get_list("todos", None);
//!
//! // Listen for changes
//! let mut rx = client.on_change();
//! while let Ok(changed) = rx.recv().await {
//!     println!("Entity {} changed", changed.entity);
//! }
//! ```
//!
//! # Usage (embedded mode — host owns the WS)
//! ```ignore
//! let client = EntangledClient::embedded("/data/entangled", "user-123");
//!
//! // Host feeds incoming sync frames
//! client.handle_sync_frame(frame_value);
//!
//! // Host sends subscribe/unsubscribe through its own WS
//! ```

use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::Mutex as StdMutex;

use serde_json::Value;
use tokio::sync::broadcast;

use crate::auth::AuthProvider;
use crate::cache::{Cache, CacheKey};
use crate::push::{self, EntityChanged, SyncFrame};

/// Configuration for EntangledClient.
pub struct EntangledConfig {
    /// WebSocket URL (e.g. "https://api.example.com/app/ws")
    pub ws_url: String,
    /// Authentication provider
    pub auth: Box<dyn AuthProvider>,
    /// Base directory for SQLite cache files.
    /// Actual path will be `{db_dir}/{user_id}/entangled.db`
    pub db_dir: PathBuf,
}

/// The unified Entangled engine.
///
/// Two creation modes:
/// - `connect()` — standalone: Entangled manages its own WS
/// - `embedded()` — host manages WS, feeds sync frames manually
pub struct EntangledClient {
    cache: Arc<StdMutex<Cache>>,
    change_tx: broadcast::Sender<EntityChanged>,
    user_id: String,
    db_dir: PathBuf,

    // Transport (only in standalone mode)
    #[cfg(feature = "transport")]
    transport: Option<Arc<crate::transport::WsTransport>>,
    #[cfg(feature = "transport")]
    shutdown: Option<Arc<tokio::sync::Notify>>,
}

impl EntangledClient {
    // ── Standalone mode (Entangled owns WS) ──────────────────────────────────

    /// Create a connected EntangledClient with built-in WS transport.
    #[cfg(feature = "transport")]
    pub async fn connect(config: EntangledConfig) -> Arc<Self> {
        let user_id = config.auth.user_id();
        let cache = Self::open_user_cache(&config.db_dir, &user_id);
        let cache = Arc::new(StdMutex::new(cache));
        let (change_tx, _) = broadcast::channel(128);
        let shutdown = Arc::new(tokio::sync::Notify::new());

        let (transport, sync_tx) = crate::transport::WsTransport::new();
        let transport = Arc::new(transport);

        let client = Arc::new(Self {
            cache,
            change_tx,
            user_id,
            db_dir: config.db_dir.clone(),
            transport: Some(Arc::clone(&transport)),
            shutdown: Some(Arc::clone(&shutdown)),
        });

        // Spawn WS connection loop
        let auth: Arc<dyn AuthProvider> = Arc::from(config.auth);
        let ws_url = config.ws_url.clone();
        let t = Arc::clone(&transport);
        let s = Arc::clone(&shutdown);
        tokio::spawn(async move {
            t.run(&ws_url, auth, sync_tx, s).await;
        });

        // Spawn sync frame processor
        let client_clone = Arc::clone(&client);
        let mut sync_rx = transport.sync_rx.lock().await.take()
            .expect("sync_rx already taken");
        tokio::spawn(async move {
            while let Some(val) = sync_rx.recv().await {
                if let Ok(frame) = serde_json::from_value::<SyncFrame>(val) {
                    let changed = {
                        let cache = client_clone.cache.lock().unwrap();
                        push::process_sync(&cache, &frame)
                    };
                    if let Some(changed) = changed {
                        let _ = client_clone.change_tx.send(changed);
                    }
                }
            }
        });

        client
    }

    // ── Embedded mode (host owns WS) ────────────────────────────────────────

    /// Create an EntangledClient without WS transport.
    /// The host is responsible for feeding sync frames via `handle_sync_frame()`.
    pub fn embedded(db_dir: &Path, user_id: &str) -> Arc<Self> {
        let cache = Self::open_user_cache(db_dir, user_id);
        let (change_tx, _) = broadcast::channel(128);

        Arc::new(Self {
            cache: Arc::new(StdMutex::new(cache)),
            change_tx,
            user_id: user_id.to_string(),
            db_dir: db_dir.to_path_buf(),
            #[cfg(feature = "transport")]
            transport: None,
            #[cfg(feature = "transport")]
            shutdown: None,
        })
    }

    /// Process an incoming sync frame (embedded mode).
    pub fn handle_sync_frame(&self, val: Value) -> Option<EntityChanged> {
        let frame: SyncFrame = serde_json::from_value(val).ok()?;
        let cache = self.cache.lock().unwrap();
        let changed = push::process_sync(&cache, &frame)?;
        let _ = self.change_tx.send(changed.clone());
        Some(changed)
    }

    // ── Per-user cache isolation ─────────────────────────────────────────────

    fn open_user_cache(db_dir: &Path, user_id: &str) -> Cache {
        let user_dir = db_dir.join(user_id);
        std::fs::create_dir_all(&user_dir).ok();
        let db_path = user_dir.join("entangled.db");
        Cache::new(&db_path)
    }

    /// Switch to a different user — closes current DB, opens user's DB.
    pub fn switch_user(&self, new_user_id: &str) -> Result<(), String> {
        let new_cache = Self::open_user_cache(&self.db_dir, new_user_id);
        *self.cache.lock().unwrap() = new_cache;
        tracing::info!("[Entangled] Switched to user {}", new_user_id);
        Ok(())
    }

    // ── Subscribe / Unsubscribe ──────────────────────────────────────────────

    /// Subscribe to an entity. In standalone mode, this sends a WS message.
    #[cfg(feature = "transport")]
    pub async fn subscribe(&self, entity: &str, params: Option<Value>) {
        let key = match &params {
            Some(p) => {
                let map = p.as_object().cloned().unwrap_or_default();
                CacheKey::new(entity, &map)
            }
            None => CacheKey::new_empty(entity),
        };

        // Get version from cache for delta sync
        let version = {
            let cache = self.cache.lock().unwrap();
            cache.get_version(&key)
        };

        if let Some(ref t) = self.transport {
            t.subscribe(entity, params, version).await;
        }
    }

    /// Unsubscribe from an entity.
    #[cfg(feature = "transport")]
    pub async fn unsubscribe(&self, entity: &str, params: Option<Value>) {
        if let Some(ref t) = self.transport {
            t.unsubscribe(entity, params).await;
        }
    }

    /// Send an entity CRUD request (standalone mode).
    #[cfg(feature = "transport")]
    pub async fn request(&self, action: &str, data: Option<Value>) -> Result<Value, String> {
        match &self.transport {
            Some(t) => t.request(action, data).await,
            None => Err("No transport in embedded mode".into()),
        }
    }

    // ── Cache reads (both modes) ─────────────────────────────────────────────

    /// Get a list of items for an entity.
    pub fn get_list(&self, entity: &str, params: Option<&serde_json::Map<String, Value>>) -> Vec<Value> {
        let key = match params {
            Some(p) => CacheKey::new(entity, p),
            None => CacheKey::new_empty(entity),
        };
        let cache = self.cache.lock().unwrap();
        cache.get_list(&key)
    }

    /// Get a single item by ID.
    pub fn get_item(&self, entity: &str, item_id: &str, params: Option<&serde_json::Map<String, Value>>) -> Option<Value> {
        let key = match params {
            Some(p) => CacheKey::new(entity, p),
            None => CacheKey::new_empty(entity),
        };
        let cache = self.cache.lock().unwrap();
        cache.get_item(&key, item_id)
    }

    /// Check if pagination has more data.
    pub fn has_more(&self, entity: &str, params: Option<&serde_json::Map<String, Value>>) -> bool {
        let key = match params {
            Some(p) => CacheKey::new(entity, p),
            None => CacheKey::new_empty(entity),
        };
        let cache = self.cache.lock().unwrap();
        cache.has_more_before(&key)
    }

    /// Get cached version for an entity.
    pub fn get_version(&self, entity: &str, params: Option<&serde_json::Map<String, Value>>) -> Option<u64> {
        let key = match params {
            Some(p) => CacheKey::new(entity, p),
            None => CacheKey::new_empty(entity),
        };
        let cache = self.cache.lock().unwrap();
        cache.get_version(&key)
    }

    // ── Event streams ────────────────────────────────────────────────────────

    /// Subscribe to entity change events.
    pub fn on_change(&self) -> broadcast::Receiver<EntityChanged> {
        self.change_tx.subscribe()
    }

    /// Subscribe to connection state changes (standalone mode).
    #[cfg(feature = "transport")]
    pub fn on_connection_change(&self) -> Option<broadcast::Receiver<bool>> {
        self.transport.as_ref().map(|t| t.connected_tx.subscribe())
    }

    /// Subscribe to host-specific push events (standalone mode).
    #[cfg(feature = "transport")]
    pub fn on_push(&self) -> Option<broadcast::Receiver<(String, Option<Value>)>> {
        self.transport.as_ref().map(|t| t.push_tx.subscribe())
    }

    /// Check if connected (standalone mode).
    #[cfg(feature = "transport")]
    pub fn is_connected(&self) -> bool {
        self.transport.as_ref().map_or(false, |t| t.is_connected())
    }

    /// Raw cache access (for advanced usage / Tauri commands).
    pub fn cache(&self) -> &Arc<StdMutex<Cache>> {
        &self.cache
    }

    pub fn user_id(&self) -> &str {
        &self.user_id
    }

    // ── Shutdown ─────────────────────────────────────────────────────────────

    /// Gracefully shutdown the WS connection.
    #[cfg(feature = "transport")]
    pub fn shutdown(&self) {
        if let Some(ref s) = self.shutdown {
            s.notify_one();
        }
    }
}

impl Drop for EntangledClient {
    fn drop(&mut self) {
        #[cfg(feature = "transport")]
        if let Some(ref s) = self.shutdown {
            s.notify_one();
        }
    }
}
