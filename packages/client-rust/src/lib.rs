//! Entangled — self-contained entity sync engine.
//!
//! A generic middleware for real-time data synchronization between
//! server and client, with persistent SQLite cache and Git-like
//! delta sync protocol.
//!
//! # Architecture
//! ```text
//! ┌─────────────────────────────────────────────┐
//! │             EntangledClient                  │
//! │                                              │
//! │  auth.rs       ← AuthProvider trait (宿主注入) │
//! │  transport.rs  ← WS 连接管理 (可选)           │
//! │  cache.rs      ← SQLite 持久化 (per-user)    │
//! │  push.rs       ← Sync 帧处理                 │
//! │  client.rs     ← 统一 API                    │
//! │                                              │
//! └─────────────────────────────────────────────┘
//! ```
//!
//! # Usage Modes
//!
//! ## Standalone (Entangled owns the WS)
//! ```ignore
//! let client = EntangledClient::connect(EntangledConfig {
//!     ws_url: "https://api.example.com/app/ws".into(),
//!     auth: Box::new(JwtAuth::new(token)),
//!     db_dir: "/data/entangled".into(),
//! }).await;
//! ```
//!
//! ## Embedded (host owns the WS)
//! ```ignore
//! let client = EntangledClient::embedded("/data/entangled", "user-123");
//! // Host calls client.handle_sync_frame(val) on incoming sync messages
//! ```

pub mod auth;
pub mod cache;
pub mod push;
pub mod schema;
pub mod subscription;
pub mod client;

#[cfg(feature = "transport")]
pub mod transport;

#[cfg(feature = "tauri")]
pub mod commands;

// Re-exports for convenience
pub use auth::AuthProvider;
pub use client::{EntangledClient, EntangledConfig};
pub use push::{EntityChanged, SyncFrame};
pub use cache::{Cache, CacheKey};
pub use subscription::{subscription_cache_key, SubscriptionLedger, SubscriptionSchemaStore};
