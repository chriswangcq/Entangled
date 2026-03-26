//! Entangled Client Engine — generic entity cache + push processing + cascade.
//!
//! This crate is the Rust side of the Entangled sync engine.
//! It receives entity schemas dynamically from the server and provides:
//! - Local entity cache (HashMap-based)
//! - Push event processing (update cache in-place)
//! - Cascade invalidation (walk relation graph)
//! - Tauri commands (optional, for Tauri apps)
//!
//! **Zero business logic** — it doesn't know about "todos" or "users".
//! Entity definitions come from the server at runtime.

pub mod schema;
pub mod cache;
pub mod cascade;

#[cfg(feature = "tauri")]
pub mod commands;
