# Observability — operation spans and connect-phase events

`rust-oracledb` emits structured operation spans through the
[`tracing`](https://docs.rs/tracing) facade when the `tracing` Cargo feature is
enabled. Connect-phase events use that facade in every build; setting
`ORACLEDB_TRACE_CONNECT` also mirrors redacted milestone text to stderr. The
`tracing` crate is part of the default dependency graph, and connect-phase
instrumentation has runtime cost even when operation spans are disabled.

This is the observability story `python-oracledb` cannot match cleanly:
**our spans are emitted from the GIL-free Rust engine**, so N concurrent
connections produce N span trees **in parallel**. python-oracledb's
instrumentation runs under the CPython GIL — even with connections on separate
threads, their Python-level span bookkeeping serializes on the interpreter lock.

---

## 1. Turning it on

The instrumentation lives behind the `tracing` feature (off by default):

```toml
[dependencies]
oracledb = { version = "*", features = ["tracing"] }

# A subscriber to collect the spans. Anything that implements a tracing
# Subscriber works (tracing-subscriber, the OpenTelemetry bridge, etc.).
tracing-subscriber = "0.3"
```

Install a subscriber once at startup, then use the driver normally:

```rust,ignore
use tracing_subscriber::FmtSubscriber;

fn main() {
    // Any tracing subscriber works. For OpenTelemetry, layer
    // `tracing-opentelemetry` here and the spans below become OTLP spans.
    let subscriber = FmtSubscriber::builder()
        .with_max_level(tracing::Level::INFO)
        .finish();
    tracing::subscriber::set_global_default(subscriber)
        .expect("install tracing subscriber");

    // ... open a Connection and run queries; spans are emitted automatically.
}
```

That is the whole integration surface. The driver never names a concrete
subscriber; you choose the backend (pretty console logs, JSON, Jaeger/OTLP via
`tracing-opentelemetry`, …).

---

## 2. What gets traced

Each instrumented wire operation opens an INFO-level span for the duration of
its send/receive. Connect milestones are separate INFO-level events emitted in
every build. The span names and their structured fields:

| Span | Emitted by | Fields |
| --- | --- | --- |
| `oracledb.connect` | `Connection::connect` | `db.system`, `server.address`, `server.port`, `db.name` |
| `oracledb.execute` | `execute_query` / bind & executemany paths | `db.statement` (digest), `db.bind_count`, `db.bind_rows`, `db.rows_fetched` |
| `oracledb.fetch` | `fetch_rows` / paging | `db.cursor_id`, `db.arraysize`, `db.rows_fetched` |
| `oracledb.commit` | `Connection::commit` | — |
| `oracledb.rollback` | `Connection::rollback` | — |
| `oracledb.lob` | `read_lob` / `write_lob` | `db.operation`, `db.lob_offset`, `db.lob_amount` / `db.lob_bytes` |

The `oracledb.connect` span is enabled by the `tracing` feature. Connect
milestone events such as `phase=accept`, `phase=auth_phase_one`, and
`phase=session` are emitted in all builds to tracing subscribers. Set
`ORACLEDB_TRACE_CONNECT=1` to mirror their structured, redacted form to stderr.

### Field hygiene

By default, events and spans carry only **non-sensitive structured metadata**.
In particular:

- `db.statement` is a **digest** — the statement *shape* (leading verb plus a
  whitespace-collapsed, length-capped copy), **never** the raw SQL with embedded
  literals. Use bind variables (`:1`) and the digest carries the placeholder,
  not a value.
- **Bind values and fetched data are never put in a span at all.** Only *counts*
  are recorded (`db.bind_count`, `db.bind_rows`, `db.rows_fetched`).
- Connect events omit the username, password, access token, and raw packet
  contents. `ORACLEDB_TRACE_CONNECT=raw` prints AUTH payloads as hexadecimal
  after an explicit warning. Those payloads can contain credential-derived
  material; keep raw output quarantined and disable raw mode for normal use.

The digest contract is pinned by unit tests in `crates/oracledb/src/obs.rs`.

---

## 3. A sample span tree

A `connect` followed by an `execute` (with a small prefetch) and a paging
`fetch`, then a `commit`, produces a tree like:

```text
oracledb.connect{db.system="oracle" server.address="dbhost" server.port=1521 db.name="FREEPDB1"}
oracledb.execute{db.statement="select n from dual connect by level <= 3 order by n" db.bind_count=0 db.bind_rows=0 db.rows_fetched=1}
oracledb.fetch{db.cursor_id=42 db.arraysize=10 db.rows_fetched=2}
oracledb.commit{}
```

With `tracing-opentelemetry` layered on, each of these becomes an OTLP span with
the same name and attributes, exportable to Jaeger, Tempo, Honeycomb, etc.

A parameterized executemany looks like:

```text
oracledb.execute{db.statement="insert into t(a,b) values (:1,:2)" db.bind_count=2 db.bind_rows=1000 db.rows_fetched=0}
```

— note `db.bind_count=2` (binds per row) and `db.bind_rows=1000` (rows in the
batch), with **no bind value anywhere**.

---

## 4. Feature and environment controls

The driver routes operation spans through two macros in `crates/oracledb/src/obs.rs`,
`obs_span!` and `obs_record!`:

- With `--features tracing`, they expand to `tracing::span!(…).entered()` and
  `Span::record(…)`.
- Without the feature, they expand to a no-op guard and an empty statement, and
  their field expressions are not evaluated.

This control applies to operation spans only. Connect-phase events remain
enabled in every build and go to stderr when `ORACLEDB_TRACE_CONNECT` is set.
The default build includes `tracing` for those events. Verify the dependency
graph and optional operation-span feature directly:

```sh
# Default build: tracing is present for connect-phase events.
cargo tree -p oraclemcp-driver-cx -e no-dev | grep -i tracing

# Feature on: operation spans are enabled as well.
cargo tree -p oraclemcp-driver-cx --features tracing -e no-dev | grep -i '^.*tracing v'
```

The `-e no-dev` edge filter excludes dev-dependencies (the span-capture test's
`tracing-subscriber`), showing the graph a real downstream consumer sees.

---

## 5. The python-oracledb comparison, concretely

| | python-oracledb | rust-oracledb (`tracing` feature) |
| --- | --- | --- |
| Span emission | Under the CPython **GIL** | **GIL-free** Rust engine |
| Concurrency | Span bookkeeping serializes on the GIL | N connections trace **in parallel** |
| Cost when unused | Python-level overhead is always present | Connect events remain instrumented; operation spans are feature-gated |
| Backend | Whatever the Python app wires up | Any `tracing` `Subscriber` / OpenTelemetry |
| Secret safety | App's responsibility | Default events omit credentials; raw connect tracing exposes credential-derived AUTH bytes after a warning |

The parallelism point is the headline: in a service driving many concurrent
Oracle connections, python-oracledb's per-call instrumentation contends on the
interpreter lock, while these spans are produced concurrently by the async Rust
runtime with no shared lock between connections.
