//! Connection resolution shared by the live (`#[ignore]`d) integration suites.
//!
//! Every live suite reaches its lane through the same three environment
//! variables — `PYO_TEST_CONNECT_STRING`, `PYO_TEST_MAIN_USER`,
//! `PYO_TEST_MAIN_PASSWORD` — and the same free23 fallbacks. Those blocks used
//! to be copy-pasted per file; the helpers below keep the variable names and
//! the fallback values in one place. Each suite keeps its own `ConnectOptions`
//! / `ClientIdentity` construction; only the env resolution moved here.
//!
//! Three idioms are preserved verbatim so no suite changes behavior:
//!   * [`live_creds_opt`] — `Some` only when all three vars are set, else `None`
//!     (the "skip the live test when unconfigured" path).
//!   * [`live_creds_required`] — panics via `unwrap` when a var is missing.
//!   * [`live_conn_string_or`] / [`live_user_or`] / [`live_password_or`] — take
//!     the caller's lane-specific fallback and apply it only when the var is unset.
//!
//! Loaded with `mod common;`. A given suite exercises only some of these, so
//! the module allows dead code rather than sprinkling per-item attributes.
#![allow(dead_code)]

/// Default `host:port/service` for the free23 lane (`scripts/version_matrix.sh`).
pub const FREE23_CONNECT_STRING: &str = "localhost:1522/FREEPDB1";
/// Default schema user for the free23 lane.
pub const FREE23_USER: &str = "pythontest";
/// Default password for the free23 lane.
pub const FREE23_PASSWORD: &str = "pythontest";

/// Live credentials resolved from the `PYO_TEST_*` environment.
pub struct LiveCreds {
    pub connect_string: String,
    pub user: String,
    pub password: String,
}

/// `Some` only when all three `PYO_TEST_*` variables are set; `None` otherwise.
///
/// Mirrors the `std::env::var(..).ok()?` blocks that let a live suite return
/// `None` and skip when the lane environment is not configured.
pub fn live_creds_opt() -> Option<LiveCreds> {
    Some(LiveCreds {
        connect_string: std::env::var("PYO_TEST_CONNECT_STRING").ok()?,
        user: std::env::var("PYO_TEST_MAIN_USER").ok()?,
        password: std::env::var("PYO_TEST_MAIN_PASSWORD").ok()?,
    })
}

/// All three `PYO_TEST_*` variables, panicking with context if any is unset.
///
/// Mirrors the `std::env::var(..).expect(..)` blocks used by suites that require
/// the lane environment to be present.
pub fn live_creds_required() -> LiveCreds {
    LiveCreds {
        connect_string: std::env::var("PYO_TEST_CONNECT_STRING")
            .expect("PYO_TEST_CONNECT_STRING must be set for a required live test"),
        user: std::env::var("PYO_TEST_MAIN_USER")
            .expect("PYO_TEST_MAIN_USER must be set for a required live test"),
        password: std::env::var("PYO_TEST_MAIN_PASSWORD")
            .expect("PYO_TEST_MAIN_PASSWORD must be set for a required live test"),
    }
}

/// `PYO_TEST_CONNECT_STRING`, or the caller's lane fallback when unset.
pub fn live_conn_string_or(default: &str) -> String {
    std::env::var("PYO_TEST_CONNECT_STRING").unwrap_or_else(|_| default.to_string())
}

/// `PYO_TEST_MAIN_USER`, or the caller's fallback when unset.
pub fn live_user_or(default: &str) -> String {
    std::env::var("PYO_TEST_MAIN_USER").unwrap_or_else(|_| default.to_string())
}

/// `PYO_TEST_MAIN_PASSWORD`, or the caller's fallback when unset.
pub fn live_password_or(default: &str) -> String {
    std::env::var("PYO_TEST_MAIN_PASSWORD").unwrap_or_else(|_| default.to_string())
}
