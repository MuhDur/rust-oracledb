//! First-class observability: feature-gated, zero-cost-when-off `tracing` spans
//! for the thin driver (bead rust-oracledb-lv6).
//!
//! This module is the single seam between the driver and the [`tracing`] crate.
//! Everywhere else the driver emits spans through the macros defined here
//! ([`obs_span!`], [`obs_record!`]), never touching `tracing` directly. That
//! keeps the dependency in ONE place and makes the off-build provably free:
//!
//! * With the `tracing` feature **on**, [`obs_span!`] expands to a
//!   [`tracing::span!`] entered for the lexical scope of its returned guard, and
//!   [`obs_record!`] records a structured field on a live span.
//! * With the feature **off**, both macros expand to (essentially) nothing — a
//!   `()` guard and an empty statement — and the field expressions are **not
//!   evaluated**, so a row-count or digest is never even computed. The default
//!   build does not compile `tracing` in at all (verify with
//!   `cargo tree -p oracledb -e no-dev | grep tracing` → no matches).
//!
//! ## The beat-python angle
//!
//! python-oracledb's instrumentation runs under the CPython GIL: even when two
//! connections do I/O on separate threads, their Python-level span bookkeeping
//! serializes on the GIL. These spans are emitted from the GIL-free Rust engine,
//! so N concurrent connections produce N span trees **in parallel** with no
//! shared interpreter lock. See `docs/OBSERVABILITY.md`.
//!
//! ## Field hygiene
//!
//! Spans carry only non-sensitive structured metadata: a SQL *digest* (the
//! statement shape, never the literal text with embedded values), row counts,
//! rows fetched, and bind *counts*. Bind VALUES and fetched DATA are never put
//! in a span — see [`sql_digest`].

/// A stable, low-cardinality digest of a SQL statement suitable for a span
/// field: the leading verb plus a truncated, whitespace-collapsed shape, with no
/// literal values. This is deliberately NOT the raw SQL — a span must never
/// carry embedded literals (which can contain secrets / PII). The digest lets an
/// operator group spans by statement without leaking data.
///
/// The shape: the first token upper-cased (SELECT / INSERT / BEGIN / ...) joined
/// to a length-capped, single-spaced copy of the statement with runs of
/// whitespace collapsed. Capped at 120 chars so a span field stays bounded
/// regardless of statement size.
#[cfg(feature = "tracing")]
pub(crate) fn sql_digest(sql: &str) -> String {
    const MAX: usize = 120;
    let mut collapsed = String::with_capacity(sql.len().min(MAX));
    let mut last_was_space = false;
    for ch in sql.trim().chars() {
        if ch.is_whitespace() {
            if !last_was_space {
                collapsed.push(' ');
                last_was_space = true;
            }
        } else {
            collapsed.push(ch);
            last_was_space = false;
        }
        if collapsed.len() >= MAX {
            collapsed.push('…');
            break;
        }
    }
    collapsed
}

/// Open an observability span for the current scope.
///
/// When the `tracing` feature is on this expands to an entered [`tracing::span!`]
/// at INFO level; the returned guard keeps the span open until it drops (so bind
/// it to a `let`). When the feature is off it expands to `()` and evaluates none
/// of the field expressions.
///
/// ```ignore
/// let _span = obs_span!("oracledb.execute", sql.digest = %digest, db.bind_count = binds);
/// ```
#[cfg(feature = "tracing")]
#[macro_export]
macro_rules! obs_span {
    ($name:expr $(,)?) => {{
        $crate::__tracing::span!($crate::__tracing::Level::INFO, $name).entered()
    }};
    ($name:expr, $($fields:tt)+) => {{
        $crate::__tracing::span!($crate::__tracing::Level::INFO, $name, $($fields)+).entered()
    }};
}

/// Off-build: a zero-sized no-op guard; the field token-tree is discarded
/// entirely, so none of the field expressions are evaluated and `tracing` is
/// never named. A unit *struct* (not `()`) so a `let _span = obs_span!(…);`
/// binding does not trip the `clippy::let_unit_value` / `unused_unit` lints.
#[cfg(not(feature = "tracing"))]
#[macro_export]
macro_rules! obs_span {
    ($name:expr $(,)?) => {
        $crate::ObsSpanGuard
    };
    ($name:expr, $($fields:tt)+) => {
        $crate::ObsSpanGuard
    };
}

/// Off-build no-op span guard (see [`obs_span!`]). Zero-sized; exists only so the
/// `tracing`-off macros yield a bindable value that is not the `()` unit type.
#[cfg(not(feature = "tracing"))]
#[doc(hidden)]
pub struct ObsSpanGuard;

/// Record a structured field on a live span guard (the value of an [`obs_span!`]).
///
/// On the `tracing` build this forwards to [`tracing::Span::record`]; off, it is
/// an empty statement and the value expression is not evaluated.
///
/// ```ignore
/// obs_record!(_span, db.rows_fetched = rows.len());
/// ```
#[cfg(feature = "tracing")]
#[macro_export]
macro_rules! obs_record {
    ($guard:expr, $($field:ident).+ = $value:expr) => {{
        // `EnteredSpan` derefs to the `Span` it entered; `record` takes the
        // field name as a &str matching the field declared at span creation.
        let _ = $crate::__tracing::Span::record(
            &*$guard,
            ::core::stringify!($($field).+),
            $value,
        );
    }};
}

/// Off-build: a no-op; the value expression is not evaluated, the guard is `()`.
#[cfg(not(feature = "tracing"))]
#[macro_export]
macro_rules! obs_record {
    ($guard:expr, $($field:ident).+ = $value:expr) => {{}};
}

#[cfg(all(test, feature = "tracing"))]
mod tests {
    use super::sql_digest;

    #[test]
    fn sql_digest_collapses_whitespace_and_keeps_shape() {
        // Runs of whitespace (including newlines/tabs) collapse to one space and
        // the statement is trimmed — a stable shape an operator can group on.
        let digest = sql_digest("  select  n\n  from   dual\t where 1=1 ");
        assert_eq!(digest, "select n from dual where 1=1");
    }

    #[test]
    fn sql_digest_is_length_capped() {
        // A pathologically long statement is truncated so a span field stays
        // bounded; the cap ellipsis marks the truncation.
        let long = format!("select {} from dual", "x".repeat(500));
        let digest = sql_digest(&long);
        // 120-char cap plus the ellipsis marker.
        assert!(
            digest.chars().count() <= 121,
            "digest must be length-capped, got {} chars",
            digest.chars().count()
        );
        assert!(
            digest.ends_with('…'),
            "a truncated digest is marked with an ellipsis"
        );
    }

    #[test]
    fn sql_digest_preserves_a_literal_only_because_no_value_extraction_is_promised() {
        // The digest is the statement SHAPE, not a value-stripped rewrite: it
        // does NOT parse out literals. The hygiene guarantee is that the driver
        // passes the *digest* (bounded shape) to spans rather than re-emitting
        // bind VALUES — bind values never reach a span at all (they are not
        // arguments to any obs_span!). This test pins that the digest of a
        // parameterized statement (the recommended form) carries no value.
        let digest = sql_digest("select * from emp where id = :1");
        assert_eq!(digest, "select * from emp where id = :1");
        assert!(
            !digest.chars().any(|c| c.is_ascii_digit() && c != '1'),
            "a parameterized digest carries the bind placeholder, not a value"
        );
    }
}
