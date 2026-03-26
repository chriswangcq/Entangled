//! Entangled Client Engine — generic entity cache + push processing.
//!
//! This crate is the Rust side of the Entangled sync engine.
//! It receives entity schemas dynamically from the server and provides:
//! - Local entity cache (HashMap-based)
//! - Push event processing (update cache in-place, emit to UI)
//! - Tauri commands (optional, for Tauri apps)
//!
//! **Zero business logic.** Cascade invalidation is handled server-side.
//! The client just receives push events and marks cache entries stale.
//! Entity definitions come from the server at runtime.

pub mod schema;
pub mod cache;
pub mod push;

#[cfg(feature = "tauri")]
pub mod commands;
