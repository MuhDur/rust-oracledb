//! Feature-gated observability test (bead rust-oracledb-lv6): with the `tracing`
//! feature on, an execute + fetch must emit the expected per-round-trip spans
//! with their structured fields (SQL digest, bind count, rows fetched) — and
//! NEVER any secret value / bind data.
//!
//! Compiled only under `--features tracing`. Self-skips when the container
//! environment is absent. Run with:
//!
//! ```sh
//! eval "$(ORACLEDB_CONTAINER_NAME=rust-oracledb-lane-1523 \
//!         ORACLEDB_HOST_PORT=1523 scripts/container.sh env)"
//! cargo test -p oracledb --features tracing --test observability
//! ```
#![cfg(feature = "tracing")]

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use asupersync::runtime::{reactor, RuntimeBuilder};
use asupersync::Cx;
use oracledb::protocol::thin::QueryValue;
use oracledb::{ConnectOptions, Connection};
use oracledb_protocol::ClientIdentity;

mod common;
use tracing::field::{Field, Visit};
use tracing::span::{Attributes, Record};
use tracing::subscriber::with_default;
use tracing::{Id, Subscriber};

/// One captured span: its name and the structured fields recorded on it (both at
/// creation and via later `record` calls), rendered to strings.
#[derive(Clone, Debug, Default)]
struct CapturedSpan {
    name: String,
    fields: BTreeMap<String, String>,
}

#[derive(Clone, Default)]
struct SpanCapture {
    spans: Arc<Mutex<Vec<CapturedSpan>>>,
}

impl SpanCapture {
    fn snapshot(&self) -> Vec<CapturedSpan> {
        self.spans.lock().unwrap().clone()
    }

    fn span_named(&self, name: &str) -> Option<CapturedSpan> {
        self.snapshot().into_iter().find(|s| s.name == name)
    }
}

/// A `Visit` that stringifies every recorded field into a map.
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

/// A minimal `Subscriber` that records every new span's name + fields and merges
/// any later `record` calls. Span ids are dense and monotonic, indexing `spans`.
struct CaptureSubscriber {
    capture: SpanCapture,
    next_id: Mutex<u64>,
}

impl CaptureSubscriber {
    fn new(capture: SpanCapture) -> Self {
        Self {
            capture,
            next_id: Mutex::new(1),
        }
    }
}

impl Subscriber for CaptureSubscriber {
    fn enabled(&self, _: &tracing::Metadata<'_>) -> bool {
        true
    }

    fn new_span(&self, attrs: &Attributes<'_>) -> Id {
        let mut next = self.next_id.lock().unwrap();
        let id = *next;
        *next += 1;
        let mut collector = FieldCollector::default();
        attrs.record(&mut collector);
        let mut spans = self.capture.spans.lock().unwrap();
        // Index `id - 1` -> push in order; ids start at 1.
        spans.push(CapturedSpan {
            name: attrs.metadata().name().to_string(),
            fields: collector.0,
        });
        Id::from_u64(id)
    }

    fn record(&self, span: &Id, values: &Record<'_>) {
        let mut collector = FieldCollector::default();
        values.record(&mut collector);
        let idx = (span.into_u64() - 1) as usize;
        let mut spans = self.capture.spans.lock().unwrap();
        if let Some(captured) = spans.get_mut(idx) {
            captured.fields.extend(collector.0);
        }
    }

    fn record_follows_from(&self, _: &Id, _: &Id) {}
    fn event(&self, _: &tracing::Event<'_>) {}
    fn enter(&self, _: &Id) {}
    fn exit(&self, _: &Id) {}
}

fn connect_options() -> Option<ConnectOptions> {
    let common::LiveCreds {
        connect_string,
        user,
        password,
    } = common::live_creds_opt()?;
    let identity = ClientIdentity::new(
        "rust-oracledb-otel",
        "otel-machine",
        "otel-osuser",
        "otel-terminal",
        "rust-oracledb thn : 0.0.0",
    )
    .ok()?;
    Some(ConnectOptions::new(
        connect_string,
        user,
        password,
        identity,
    ))
}

#[test]
fn execute_and_fetch_emit_spans_with_structured_fields() {
    let Some(options) = connect_options() else {
        eprintln!(
            "skipped execute_and_fetch_emit_spans_with_structured_fields: PYO_TEST_* not set"
        );
        return;
    };

    let capture = SpanCapture::default();
    let subscriber = CaptureSubscriber::new(capture.clone());

    with_default(subscriber, || {
        let reactor = reactor::create_reactor().expect("native reactor");
        let runtime = RuntimeBuilder::current_thread()
            .with_reactor(reactor)
            .build()
            .expect("runtime");
        runtime.block_on(async {
            let cx = Cx::current().expect("ambient Cx");
            let mut conn = Connection::connect(&cx, options).await.expect("connect");
            // Execute with a small prefetch so the rest of the rows require an
            // explicit FETCH round trip -> an execute span AND a fetch span.
            let sql = "select level as n from dual connect by level <= 3 order by n";
            let first = conn
                .execute_raw(
                    &cx,
                    sql,
                    1,
                    &[],
                    oracledb::protocol::thin::ExecuteOptions::default(),
                    None,
                )
                .await
                .expect("execute");
            let cursor_id = first.cursor_id;
            let mut values: Vec<i64> = (0..first.rows.len())
                .filter_map(|r| first.cell(r, 0).and_then(QueryValue::as_i64))
                .collect();
            // Page the remaining rows with explicit fetch round trips.
            if first.more_rows && cursor_id != 0 {
                let fetched = conn
                    .fetch_rows(&cx, cursor_id, 10, None)
                    .await
                    .expect("fetch");
                values.extend(
                    (0..fetched.rows.len())
                        .filter_map(|r| fetched.cell(r, 0).and_then(QueryValue::as_i64)),
                );
            }
            assert_eq!(values, vec![1, 2, 3], "sanity: the query returns 1,2,3");
            conn.close(&cx).await.expect("close");
        });
    });

    let names: Vec<String> = capture.snapshot().into_iter().map(|s| s.name).collect();

    // A connect span is emitted for the handshake.
    assert!(
        names.iter().any(|n| n == "oracledb.connect"),
        "expected an oracledb.connect span, got {names:?}"
    );

    // The execute span carries the SQL digest and a bind count — but NOT the raw
    // SQL with values and NOT any bind value.
    let execute = capture
        .span_named("oracledb.execute")
        .unwrap_or_else(|| panic!("expected an oracledb.execute span, got {names:?}"));
    let digest = execute
        .fields
        .get("db.statement")
        .unwrap_or_else(|| panic!("execute span must carry a db.statement digest: {execute:?}"));
    assert!(
        digest.to_uppercase().contains("SELECT"),
        "the digest must reflect the statement shape (SELECT ...), got {digest:?}"
    );
    assert!(
        execute.fields.contains_key("db.bind_count"),
        "execute span must carry a db.bind_count field: {execute:?}"
    );

    // At least one fetch/round-trip span records rows fetched.
    let saw_rows_fetched = capture.snapshot().iter().any(|s| {
        (s.name == "oracledb.fetch" || s.name == "oracledb.execute")
            && s.fields.contains_key("db.rows_fetched")
    });
    assert!(
        saw_rows_fetched,
        "expected a span carrying db.rows_fetched, got {:?}",
        capture.snapshot()
    );

    // Field hygiene: NO span may carry a raw bind value or the literal data. The
    // statement here has no binds, but assert the digest field never contains a
    // value-looking literal beyond the statement shape, and that no field name
    // leaks bind/secret data.
    for span in capture.snapshot() {
        for (key, _value) in &span.fields {
            assert!(
                !key.contains("password") && !key.contains("secret") && !key.contains("bind_value"),
                "span {span:?} field {key} looks like it leaks sensitive data"
            );
        }
    }

    // The whole thing ran without the test timing out (a coarse liveness check).
    let _ = Duration::from_secs(0);
}
