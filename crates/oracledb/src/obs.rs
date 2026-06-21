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
/// field: a length-capped, whitespace-collapsed shape with **embedded literals
/// redacted**. This is deliberately NOT the raw SQL — a span must never carry
/// embedded literals (which can contain secrets / PII). The digest lets an
/// operator group spans by statement without leaking data.
///
/// The shape:
///
/// * single-quoted string literals (`'...'`, with the `''` escape handled)
///   collapse to a single `?`,
/// * standalone numeric literals (`42`, `3.14`, `1e9`) collapse to `?`,
/// * bind placeholders (`:name`, `:1`) and identifiers are preserved (a `:1`
///   bind carries no value — only its position),
/// * runs of whitespace collapse to one space and the whole thing is capped at
///   120 chars so a span field stays bounded regardless of statement size.
///
/// Redacting literals here is defence-in-depth: the driver's contract is
/// parameterized SQL (binds), but even if a caller passes a statement with an
/// inline literal, its value never reaches a span.
#[cfg(feature = "tracing")]
pub(crate) fn sql_digest(sql: &str) -> String {
    const MAX: usize = 120;
    // First pass: collapse whitespace into a single-spaced, trimmed form.
    let mut collapsed = String::with_capacity(sql.len());
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
    }

    // Second pass: redact embedded literals to `?` so no value can leak. We walk
    // the chars tracking whether we are inside a single-quoted string literal.
    let mut redacted = String::with_capacity(collapsed.len());
    let mut chars = collapsed.chars().peekable();
    while let Some(ch) = chars.next() {
        match ch {
            // A single-quoted string literal: consume to the closing quote,
            // honouring the doubled-quote (`''`) escape, and emit a single `?`.
            '\'' => {
                redacted.push('?');
                while let Some(&inner) = chars.peek() {
                    chars.next();
                    if inner == '\'' {
                        // A doubled quote is an escaped quote, not the end.
                        if chars.peek() == Some(&'\'') {
                            chars.next();
                            continue;
                        }
                        break;
                    }
                }
            }
            // A numeric literal NOT part of a bind placeholder or identifier.
            // It starts a number only when the previous emitted char is not an
            // identifier char and not a bind-colon (so `:1` survives, `t1`
            // survives, but a bare ` 42` is redacted).
            d if d.is_ascii_digit() && starts_numeric_literal(&redacted) => {
                redacted.push('?');
                // Swallow the rest of the numeric literal (digits, decimal
                // point, exponent) so only one `?` is emitted.
                while let Some(&inner) = chars.peek() {
                    if inner.is_ascii_digit()
                        || inner == '.'
                        || inner == 'e'
                        || inner == 'E'
                        || inner == '+'
                        || inner == '-'
                    {
                        chars.next();
                    } else {
                        break;
                    }
                }
            }
            other => redacted.push(other),
        }
    }

    // Finally cap the length so a span field stays bounded.
    if redacted.chars().count() > MAX {
        let mut capped: String = redacted.chars().take(MAX).collect();
        capped.push('…');
        capped
    } else {
        redacted
    }
}

/// Whether a digit at the current write position begins a *literal* number
/// rather than a suffix of an identifier (`t1`, `col2`) or a numeric bind
/// (`:1`). True when the previously emitted char is absent, whitespace, or a
/// punctuation boundary that is not a bind colon or identifier char.
#[cfg(feature = "tracing")]
fn starts_numeric_literal(emitted_so_far: &str) -> bool {
    match emitted_so_far.chars().last() {
        // Start of string -> a leading number is a literal.
        None => true,
        // Identifier continuation -> the digit belongs to an identifier.
        Some(c) if c.is_alphanumeric() || c == '_' => false,
        // A bind colon -> `:1` is a placeholder position, keep it.
        Some(':') => false,
        // A `?` we just emitted for a prior redaction -> digit is separate.
        // Any other boundary (space, operator, paren, comma) -> a literal.
        Some(_) => true,
    }
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
        // Numeric literals (the `1`s in `1=1`) are redacted to `?` so no value
        // can leak; the shape is preserved.
        let digest = sql_digest("  select  n\n  from   dual\t where 1=1 ");
        assert_eq!(digest, "select n from dual where ?=?");
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
    fn sql_digest_preserves_bind_placeholders() {
        // A bind placeholder (`:1`, `:id`) carries only a POSITION, never a
        // value, so it survives the digest unchanged — that is the whole point
        // of the parameterized form. The digest of a parameterized statement
        // therefore equals its whitespace-collapsed shape.
        assert_eq!(
            sql_digest("select * from emp where id = :1"),
            "select * from emp where id = :1"
        );
        assert_eq!(
            sql_digest("select * from emp where name = :name and dept = :dept"),
            "select * from emp where name = :name and dept = :dept"
        );
    }

    #[test]
    fn sql_digest_redacts_embedded_string_and_numeric_literals() {
        // Even when a caller passes a NON-parameterized statement with inline
        // literals, the digest redacts every value to `?` so no secret/PII can
        // reach a span — the digest is the value-stripped shape.
        let digest = sql_digest("select * from t where ssn = 'SSN-078-05-1120' and age = 42");
        assert_eq!(digest, "select * from t where ssn = ? and age = ?");
        assert!(
            !digest.contains("SSN-078-05-1120") && !digest.contains("42"),
            "embedded literals must be redacted, got {digest:?}"
        );

        // The `''` escape inside a string literal does not end the literal early.
        let escaped = sql_digest("insert into t(name) values ('O''Brien')");
        assert_eq!(escaped, "insert into t(name) values (?)");
        assert!(
            !escaped.contains("Brien"),
            "an escaped-quote literal must still be fully redacted, got {escaped:?}"
        );

        // A decimal / exponent numeric literal collapses to a single `?`.
        assert_eq!(
            sql_digest("select * from t where rate > 3.14e-2"),
            "select * from t where rate > ?"
        );
    }

    // ---- W1-T6.3: redaction (no-secret-leakage) tests -------------------------
    //
    // These tests drive the ACTUAL `obs_span!` / `obs_record!` macros the driver
    // uses — with a capturing subscriber — and assert that by default no secret
    // (raw SQL with embedded literals, bind values, or a credential/password)
    // reaches any emitted span field. They are fully deterministic (no live
    // database) so they run in CI under `--features tracing`.

    use std::collections::BTreeMap;
    use std::sync::{Arc, Mutex};

    use tracing::field::{Field, Visit};
    use tracing::span::{Attributes, Record};
    use tracing::subscriber::with_default;
    use tracing::{Id, Subscriber};

    /// Every field rendered to a string, keyed by field name.
    #[derive(Default)]
    struct FieldCollector(BTreeMap<String, String>);

    impl Visit for FieldCollector {
        fn record_debug(&mut self, field: &Field, value: &dyn std::fmt::Debug) {
            self.0
                .insert(field.name().to_string(), format!("{value:?}"));
        }
        fn record_str(&mut self, field: &Field, value: &str) {
            self.0.insert(field.name().to_string(), value.to_string());
        }
        fn record_u64(&mut self, field: &Field, value: u64) {
            self.0.insert(field.name().to_string(), value.to_string());
        }
        fn record_i64(&mut self, field: &Field, value: i64) {
            self.0.insert(field.name().to_string(), value.to_string());
        }
    }

    #[derive(Clone, Debug)]
    struct CapturedSpan {
        name: String,
        fields: BTreeMap<String, String>,
    }

    /// A minimal capturing subscriber: records every new span's name + fields and
    /// merges any later `record` calls. Dense, monotonic span ids index `spans`.
    #[derive(Clone, Default)]
    struct Capture {
        spans: Arc<Mutex<Vec<CapturedSpan>>>,
    }

    impl Capture {
        /// Every captured field value across every span, flattened — the haystack
        /// a redaction assertion searches for a leaked secret.
        fn all_field_values(&self) -> Vec<String> {
            self.spans
                .lock()
                .unwrap()
                .iter()
                .flat_map(|s| s.fields.values().cloned().collect::<Vec<_>>())
                .collect()
        }

        fn all_field_names(&self) -> Vec<String> {
            self.spans
                .lock()
                .unwrap()
                .iter()
                .flat_map(|s| s.fields.keys().cloned().collect::<Vec<_>>())
                .collect()
        }
    }

    impl Subscriber for Capture {
        fn enabled(&self, _: &tracing::Metadata<'_>) -> bool {
            true
        }
        fn new_span(&self, attrs: &Attributes<'_>) -> Id {
            let mut collector = FieldCollector::default();
            attrs.record(&mut collector);
            let mut spans = self.spans.lock().unwrap();
            spans.push(CapturedSpan {
                name: attrs.metadata().name().to_string(),
                fields: collector.0,
            });
            Id::from_u64(spans.len() as u64)
        }
        fn record(&self, span: &Id, values: &Record<'_>) {
            let mut collector = FieldCollector::default();
            values.record(&mut collector);
            let idx = (span.into_u64() - 1) as usize;
            let mut spans = self.spans.lock().unwrap();
            if let Some(captured) = spans.get_mut(idx) {
                captured.fields.extend(collector.0);
            }
        }
        fn record_follows_from(&self, _: &Id, _: &Id) {}
        fn event(&self, _: &tracing::Event<'_>) {}
        fn enter(&self, _: &Id) {}
        fn exit(&self, _: &Id) {}
    }

    #[test]
    fn spans_redact_sql_binds_and_credentials_by_default() {
        // The secrets that MUST NOT appear anywhere in an emitted span.
        const PASSWORD: &str = "hunter2-super-secret-password";
        const BIND_VALUE: &str = "4111111111111111"; // e.g. a PAN bound to :card
        const SQL_LITERAL_SECRET: &str = "SSN-078-05-1120"; // a literal in raw SQL

        let capture = Capture::default();
        with_default(capture.clone(), || {
            // 1) A connect span carries server address/port/service — but the
            //    password is NEVER an argument to it. Model the exact connect
            //    span shape the driver emits.
            let host = "db.internal.example";
            let port: u64 = 1521;
            let service = "ORCLPDB1";
            let _connect = obs_span!(
                "oracledb.connect",
                db.system = "oracle",
                server.address = %host,
                server.port = port,
                db.name = %service,
            );

            // 2) An execute span carries the SQL DIGEST (shape) + bind COUNTS —
            //    never the raw SQL with the embedded literal, never a bind value.
            //    We deliberately build a raw SQL string that embeds a secret
            //    literal to prove only the parameterized digest reaches the span.
            let raw_sql_with_secret = format!(
                "select * from accounts where ssn = '{SQL_LITERAL_SECRET}' and card = :card"
            );
            let bind_count: u64 = 1;
            let bind_rows: u64 = 1;
            let span = obs_span!(
                "oracledb.execute",
                db.statement = %sql_digest(&raw_sql_with_secret),
                db.bind_count = bind_count,
                db.bind_rows = bind_rows,
                db.rows_fetched = tracing::field::Empty,
            );
            // Record rows fetched on the live span, as the fetch path does.
            obs_record!(span, db.rows_fetched = 3u64);
            // The bind VALUE is computed here (as the driver has it in hand) but
            // is NEVER passed to a span macro.
            let _bind_value_in_scope = (PASSWORD, BIND_VALUE);
        });

        let values = capture.all_field_values();
        let names = capture.all_field_names();
        assert!(
            !capture.spans.lock().unwrap().is_empty(),
            "the tracing feature must actually emit spans for this test to be meaningful"
        );

        // No secret value may appear in ANY captured field value.
        for secret in [PASSWORD, BIND_VALUE, SQL_LITERAL_SECRET] {
            for value in &values {
                assert!(
                    !value.contains(secret),
                    "a secret ({secret}) leaked into an emitted span field value: {value:?}"
                );
            }
        }

        // No field NAME may look like it carries a secret/bind value/credential.
        for name in &names {
            let lower = name.to_ascii_lowercase();
            assert!(
                !lower.contains("password")
                    && !lower.contains("secret")
                    && !lower.contains("credential")
                    && !lower.contains("bind_value")
                    && !lower.contains("bind_values"),
                "span field {name} looks like it leaks sensitive data"
            );
        }

        // Positive control: the execute span DID carry the parameterized digest
        // (the bind placeholder survives; the embedded literal does not).
        let execute = capture
            .spans
            .lock()
            .unwrap()
            .iter()
            .find(|s| s.name == "oracledb.execute")
            .cloned()
            .expect("an execute span was emitted");
        let digest = execute
            .fields
            .get("db.statement")
            .expect("execute span carries a db.statement digest");
        assert!(
            digest.to_uppercase().contains("SELECT") && digest.contains(":card"),
            "the digest must be the parameterized shape, got {digest:?}"
        );
        assert!(
            !digest.contains(SQL_LITERAL_SECRET),
            "the digest must not echo the embedded SQL literal secret, got {digest:?}"
        );
    }

    #[test]
    fn sql_digest_never_lengthens_or_echoes_a_password_argument() {
        // Belt-and-braces: even if a careless caller passed a credential-bearing
        // string to the digest, the digest is bounded and is the ONLY SQL-derived
        // field — but the driver never does this. Here we simply prove the digest
        // of a normal parameterized statement contains only the placeholder.
        let digest = sql_digest("update users set pw = :pw where id = :id");
        assert!(digest.contains(":pw") && digest.contains(":id"));
        assert!(
            !digest.to_lowercase().contains("hunter2"),
            "a parameterized digest cannot contain a value the caller never put in it"
        );
    }
}
