//! Auth provider — pluggable authentication for Entangled WS connections.
//!
//! The host application implements `AuthProvider` to inject its own auth mechanism.
//! Entangled never knows whether it's JWT, API Key, or session cookie.

use std::fmt::Debug;

/// Authentication provider trait — implemented by the host application.
///
/// # Example
/// ```ignore
/// struct JwtAuth { token: Arc<RwLock<String>> }
///
/// impl AuthProvider for JwtAuth {
///     fn auth_headers(&self) -> Vec<(String, String)> {
///         let token = self.token.blocking_read().clone();
///         vec![("Authorization".into(), format!("Bearer {}", token))]
///     }
///     fn user_id(&self) -> String { "user-123".into() }
///     fn on_auth_rejected(&self) { /* emit event to UI */ }
/// }
/// ```
pub trait AuthProvider: Send + Sync + Debug + 'static {
    /// Headers to attach to the WS handshake request.
    /// Called each time a connection (or reconnection) is established.
    fn auth_headers(&self) -> Vec<(String, String)>;

    /// Unique user identifier — used to:
    /// - Scope the local SQLite cache file (per-user isolation)
    /// - Identify the user on the server side
    fn user_id(&self) -> String;

    /// Called when the server rejects authentication (4001 close code).
    /// The host should handle token refresh / redirect to login.
    fn on_auth_rejected(&self);
}

/// No-auth provider for testing or local-only usage.
#[derive(Debug, Clone)]
pub struct NoAuth {
    pub user_id: String,
}

impl NoAuth {
    pub fn new(user_id: &str) -> Self {
        Self { user_id: user_id.to_string() }
    }
}

impl AuthProvider for NoAuth {
    fn auth_headers(&self) -> Vec<(String, String)> {
        vec![("X-User-ID".into(), self.user_id.clone())]
    }
    fn user_id(&self) -> String {
        self.user_id.clone()
    }
    fn on_auth_rejected(&self) {
        tracing::warn!("[Entangled] Auth rejected (NoAuth provider)");
    }
}
