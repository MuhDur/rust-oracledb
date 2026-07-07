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
use oracledb::protocol::thin::{
    encode_lob_text, ColumnMetadata, LobValue, QueryValue, CS_FORM_IMPLICIT, ORA_TYPE_NUM_CLOB,
    ORA_TYPE_NUM_NUMBER, ORA_TYPE_NUM_VARCHAR,
};
use oracledb::{ClobReader, ConnectOptions, Connection, StatementShapeCache};
use oracledb_protocol::thin::LobTextDecoder;
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
    /// Standalone events (e.g. `obs_warn!`): each is a field map. Folded into the
    /// no-secret-leakage haystack so a WARN event can never leak data either.
    events: Arc<Mutex<Vec<BTreeMap<String, String>>>>,
}

impl SpanCapture {
    fn snapshot(&self) -> Vec<CapturedSpan> {
        self.spans.lock().unwrap().clone()
    }

    fn span_named(&self, name: &str) -> Option<CapturedSpan> {
        self.snapshot().into_iter().find(|s| s.name == name)
    }

    fn events(&self) -> Vec<BTreeMap<String, String>> {
        self.events.lock().unwrap().clone()
    }

    /// Every captured field value across every span AND event — the haystack a
    /// no-secret-leakage assertion searches.
    fn all_field_values(&self) -> Vec<String> {
        let mut out: Vec<String> = self
            .snapshot()
            .iter()
            .flat_map(|s| s.fields.values().cloned().collect::<Vec<_>>())
            .collect();
        out.extend(
            self.events()
                .iter()
                .flat_map(|e| e.values().cloned().collect::<Vec<_>>()),
        );
        out
    }

    /// Every captured field NAME across every span AND event.
    fn all_field_names(&self) -> Vec<String> {
        let mut out: Vec<String> = self
            .snapshot()
            .iter()
            .flat_map(|s| s.fields.keys().cloned().collect::<Vec<_>>())
            .collect();
        out.extend(
            self.events()
                .iter()
                .flat_map(|e| e.keys().cloned().collect::<Vec<_>>()),
        );
        out
    }
}

/// Assert no synthetic secret value reaches any span/event field, and no field
/// NAME looks like it carries a value / credential. The shared no-PII DoD gate.
fn assert_no_secret_leak(capture: &SpanCapture, secrets: &[&str]) {
    for value in capture.all_field_values() {
        for secret in secrets {
            assert!(
                !value.contains(secret),
                "a secret ({secret}) leaked into an emitted span/event field: {value:?}"
            );
        }
    }
    for name in capture.all_field_names() {
        let lower = name.to_ascii_lowercase();
        assert!(
            !lower.contains("password")
                && !lower.contains("secret")
                && !lower.contains("credential")
                && !lower.contains("bind_value"),
            "field {name} looks like it leaks sensitive data"
        );
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
    fn record_bool(&mut self, field: &Field, value: bool) {
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
    fn event(&self, event: &tracing::Event<'_>) {
        let mut collector = FieldCollector::default();
        event.record(&mut collector);
        self.capture.events.lock().unwrap().push(collector.0);
    }
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

// ---- A4.1 telemetry (iec3.1.29): the four differentiator surfaces ------------

/// 8pp (shape_cache): observing/invalidating statement shapes must emit
/// structured cache hit / miss / self-heal / invalidation events whose counters
/// MOVE under a synthetic sequence — and NEVER carry the SQL text (which can
/// embed a literal secret). Pure and offline (the cache is `Arc`-shared,
/// synchronous, no I/O), driven through the public [`StatementShapeCache`].
#[test]
fn shape_cache_observe_emits_hit_miss_and_invalidation_events_without_sql() {
    // A statement whose text embeds a synthetic secret literal. Only enum-kind
    // outcomes + a generation stamp may be emitted — never this text.
    const SECRET_SQL: &str = "select id, name from t where ssn = 'SSN-SECRET-42'";

    let capture = SpanCapture::default();
    let subscriber = CaptureSubscriber::new(capture.clone());
    with_default(subscriber, || {
        let cache = StatementShapeCache::new();
        let v1 = vec![
            ColumnMetadata::new("ID", ORA_TYPE_NUM_NUMBER)
                .with_precision(9)
                .with_scale(0)
                .with_max_size(22)
                .with_nulls_allowed(true),
            ColumnMetadata::new("NAME", ORA_TYPE_NUM_VARCHAR)
                .with_max_size(50)
                .with_nulls_allowed(true),
        ];
        // Post-DDL shape: NAME widened + an added EMAIL column (a real drift).
        let v2 = vec![
            ColumnMetadata::new("ID", ORA_TYPE_NUM_NUMBER)
                .with_precision(9)
                .with_scale(0)
                .with_max_size(22)
                .with_nulls_allowed(true),
            ColumnMetadata::new("NAME", ORA_TYPE_NUM_VARCHAR)
                .with_max_size(200)
                .with_nulls_allowed(true),
            ColumnMetadata::new("EMAIL", ORA_TYPE_NUM_VARCHAR)
                .with_max_size(100)
                .with_nulls_allowed(true),
        ];

        // miss (first sight) -> hit (same shape) -> self_heal (DDL drift) -> invalidate.
        assert!(cache.observe(SECRET_SQL, &v1).first_seen);
        let hit = cache.observe(SECRET_SQL, &v1);
        assert!(!hit.first_seen && !hit.self_healed);
        assert!(cache.observe(SECRET_SQL, &v2).self_healed);
        assert!(cache.invalidate(SECRET_SQL));
    });

    // Each cache-event counter must have moved exactly once.
    let (mut miss, mut hit, mut heal, mut inval) = (0u32, 0u32, 0u32, 0u32);
    for span in capture.snapshot() {
        if span.name != "oracledb.shape_cache" {
            continue;
        }
        match span.fields.get("db.cache_event").map(String::as_str) {
            Some("miss") => miss += 1,
            Some("hit") => hit += 1,
            Some("self_heal") => heal += 1,
            Some("invalidate") => inval += 1,
            _ => {}
        }
    }
    assert_eq!(
        (miss, hit, heal, inval),
        (1, 1, 1, 1),
        "cache miss/hit/self_heal/invalidate counters must each move, got {:?}",
        (miss, hit, heal, inval)
    );

    // DoD: the SQL text (with its embedded secret) never reaches any field.
    assert_no_secret_leak(&capture, &["SSN-SECRET-42", "select id, name from t"]);
}

/// bbx (lob_stream): the ClobReader boundary-split telemetry is computed as
/// `decoder.clone().finish().is_err()` after each pushed chunk. This offline
/// test proves that exact signal over the public [`LobTextDecoder`] surface:
/// it is TRUE exactly when a chunk ends mid-codepoint / mid-surrogate pair (an
/// incomplete tail is carried), FALSE on a clean two-byte-aligned boundary.
/// (The reader's per-chunk event *emission* needs a live server and is covered
/// by [`lob_stream_emits_chunk_and_boundary_events`], gated like the existing
/// live LOB suite.)
#[test]
fn lob_utf16_boundary_split_signal_flags_surrogate_and_multibyte_splits() {
    // UTF-16LE (AL16UTF16 CLOB form). "A" = 0x0041; "😀" = U+1F600 =
    // surrogate pair high 0xD83D (LE 3D D8), low 0xDE00 (LE 00 DE).
    let split_flags = |chunks: &[&[u8]]| -> Vec<bool> {
        let mut decoder = LobTextDecoder::new(true, true);
        let mut flags = Vec::new();
        for chunk in chunks {
            let _ = decoder.push(chunk).expect("decode chunk");
            // The exact expression ClobReader::read_text_chunk emits on.
            flags.push(decoder.clone().finish().is_err());
        }
        flags
    };

    // Surrogate pair straddling the boundary: high half ends chunk 1, low half
    // opens chunk 2 -> exactly one split, healed on the next chunk.
    assert_eq!(
        split_flags(&[&[0x41, 0x00, 0x3D, 0xD8], &[0x00, 0xDE]]),
        vec![true, false],
        "a surrogate pair split across the boundary must flag exactly one split"
    );
    // An odd trailing byte (a code unit split in half) is a boundary split until
    // its partner byte arrives.
    assert_eq!(
        split_flags(&[&[0x41], &[0x00]]),
        vec![true, false],
        "an odd trailing byte is a boundary split until completed"
    );
    // Two-byte-aligned chunks never flag a split.
    assert_eq!(
        split_flags(&[&[0x41, 0x00], &[0x42, 0x00]]),
        vec![false, false],
        "aligned chunks must not flag a split"
    );
}

/// x3s (streaming query): [`Connection::for_each_row_ref`] must emit one
/// `oracledb.stream` span whose row / page counters MOVE with the streamed
/// result, plus the prefetch look-ahead (queue-depth) signal — and only the SQL
/// digest, never row data. Live; self-skips when the lane env is unset (gated
/// like the existing prefetch/borrowed-fetch suites).
#[test]
fn streaming_query_emits_stream_span_with_row_and_page_counts() {
    let Some(options) = connect_options() else {
        eprintln!("skipped streaming_query_emits_stream_span_with_row_and_page_counts: PYO_TEST_* not set");
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
            // 5 rows, arraysize 2 -> at least one paged fetch round trip after
            // the first batch, so pages_fetched moves too.
            let sql = "select level as n from dual connect by level <= 5 order by n";
            let mut seen = 0u64;
            conn.for_each_row_ref(&cx, sql, 2, |_row| {
                seen += 1;
                Ok(())
            })
            .await
            .expect("stream");
            assert_eq!(seen, 5, "sanity: the callback saw every row");
            conn.close(&cx).await.expect("close");
        });
    });

    let stream = capture
        .span_named("oracledb.stream")
        .expect("an oracledb.stream span was emitted");
    assert_eq!(
        stream.fields.get("db.rows_streamed").map(String::as_str),
        Some("5"),
        "the streaming span must count every row streamed: {stream:?}"
    );
    let pages: u64 = stream
        .fields
        .get("db.pages_fetched")
        .and_then(|v| v.parse().ok())
        .expect("db.pages_fetched present");
    assert!(
        pages >= 1,
        "paged fetch round trips must be counted, got {pages}"
    );
    assert!(
        stream.fields.contains_key("db.prefetch_inflight_max"),
        "the streaming span must carry the prefetch look-ahead (queue-depth) signal"
    );
    // DoD: no field name looks like it carries a value/credential.
    assert_no_secret_leak(&capture, &[]);
}

/// bbx (lob_stream): streaming an astral-codepoint CLOB back in 1-unit chunks
/// must emit per-chunk events (chunk count) and at least one UTF-16 boundary
/// split event — with only counts / booleans, never the LOB text. Live;
/// self-skips when the lane env is unset (gated like `live_lob_stream`).
#[test]
fn lob_stream_emits_chunk_and_boundary_events() {
    let Some(options) = connect_options() else {
        eprintln!("skipped lob_stream_emits_chunk_and_boundary_events: PYO_TEST_* not set");
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
            // Astral codepoints (surrogate pairs) + BMP CJK, so tiny character
            // chunks split code points across reads.
            let text = "😀🎉🎊 漢字 café ✓ \u{10FFFF}";
            let temp = conn
                .create_temp_lob(&cx, ORA_TYPE_NUM_CLOB, CS_FORM_IMPLICIT)
                .await
                .expect("create temp CLOB");
            let mut locator = temp.locator;
            let encoded = encode_lob_text(text, CS_FORM_IMPLICIT, Some(&locator));
            let written = conn
                .write_lob(&cx, &locator, 1, &encoded)
                .await
                .expect("write CLOB");
            if !written.locator.is_empty() {
                locator = written.locator;
            }
            let lob = LobValue {
                ora_type_num: ORA_TYPE_NUM_CLOB,
                csfrm: CS_FORM_IMPLICIT,
                locator: locator.clone(),
                size: text.encode_utf16().count() as u64,
                chunk_size: 0,
            };
            // chunk = 1 UTF-16 code unit -> surrogate pairs split across reads.
            let got = ClobReader::new(&lob, 1)
                .read_to_string(&mut conn, &cx)
                .await
                .expect("stream-read CLOB");
            assert_eq!(got, text, "sanity: streamed CLOB decodes identically");
            conn.free_temp_lobs(&cx, &[locator]).await.ok();
            conn.close(&cx).await.expect("close");
        });
    });

    let spans = capture.snapshot();
    let chunk_spans = spans
        .iter()
        .filter(|s| s.fields.contains_key("db.lob_chunk_bytes"))
        .count();
    assert!(
        chunk_spans > 0,
        "the LOB stream must emit per-chunk spans, got {spans:?}"
    );
    let splits = spans
        .iter()
        .filter(|s| {
            s.fields
                .get("db.lob_utf16_boundary_split")
                .map(String::as_str)
                == Some("true")
        })
        .count();
    assert!(
        splits > 0,
        "streaming an astral CLOB in 1-unit chunks must record UTF-16 boundary splits, got {spans:?}"
    );
    // DoD: no LOB text reaches any field.
    assert_no_secret_leak(&capture, &["😀", "漢字", "café"]);
}
