# Observability — first-class, feature-gated `tracing` spans

`rust-oracledb` emits structured **per-round-trip spans** through the
[`tracing`](https://docs.rs/tracing) facade. The instrumentation is **opt-in**
behind a Cargo feature and is **zero-cost when the feature is off**: a default
build does not compile `tracing` in at all, and every span macro expands to
nothing — the off-build is byte-for-byte the pre-feature build.

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

Each wire round trip opens an INFO-level span for the duration of its
send/receive. The span names and their structured fields:

| Span | Emitted by | Fields |
| --- | --- | --- |
| `oracledb.connect` | `Connection::connect` | `db.system`, `server.address`, `server.port`, `db.name` |
| `oracledb.execute` | `execute_query` / bind & executemany paths | `db.statement` (digest), `db.bind_count`, `db.bind_rows`, `db.rows_fetched` |
| `oracledb.fetch` | `fetch_rows` / paging | `db.cursor_id`, `db.arraysize`, `db.rows_fetched` |
| `oracledb.commit` | `Connection::commit` | — |
| `oracledb.rollback` | `Connection::rollback` | — |
| `oracledb.lob` | `read_lob` / `write_lob` | `db.operation`, `db.lob_offset`, `db.lob_amount` / `db.lob_bytes` |

### Field hygiene — no secrets, ever

Spans carry only **non-sensitive structured metadata**. In particular:

- `db.statement` is a **digest** — the statement *shape* (leading verb plus a
  whitespace-collapsed, length-capped copy), **never** the raw SQL with embedded
  literals. Use bind variables (`:1`) and the digest carries the placeholder,
  not a value.
- **Bind values and fetched data are never put in a span at all.** Only *counts*
  are recorded (`db.bind_count`, `db.bind_rows`, `db.rows_fetched`).
- The connect span carries the server address and service name but **never the
  password**.

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

## 4. Zero-cost when off — how, and how to verify

The driver routes every span through two macros in `crates/oracledb/src/obs.rs`,
`obs_span!` and `obs_record!`:

- With `--features tracing`, they expand to `tracing::span!(…).entered()` and
  `Span::record(…)`.
- Without the feature, they expand to a `()` guard and an empty statement, and
  **the field expressions are not evaluated** — a row count or SQL digest is
  never even computed on the off-build.

Because the `tracing` dependency is `optional = true` and gated by the feature,
the default build does not compile it in. Verify directly:

```sh
# Default build: NO tracing in the library dependency graph.
cargo tree -p oracledb -e no-dev | grep -i tracing      # → no matches

# Feature on: tracing appears.
cargo tree -p oracledb --features tracing -e no-dev | grep -i '^.*tracing v'
```

The `-e no-dev` edge filter excludes dev-dependencies (the span-capture test's
`tracing-subscriber`), showing the graph a real downstream consumer sees.

---

## 5. The python-oracledb comparison, concretely

| | python-oracledb | rust-oracledb (`tracing` feature) |
| --- | --- | --- |
| Span emission | Under the CPython **GIL** | **GIL-free** Rust engine |
| Concurrency | Span bookkeeping serializes on the GIL | N connections trace **in parallel** |
| Cost when unused | Python-level overhead is always present | **Zero** — dependency not compiled in |
| Backend | Whatever the Python app wires up | Any `tracing` `Subscriber` / OpenTelemetry |
| Secret safety | App's responsibility | Digest-only by construction; values never reach a span |

The parallelism point is the headline: in a service driving many concurrent
Oracle connections, python-oracledb's per-call instrumentation contends on the
interpreter lock, while these spans are produced concurrently by the async Rust
runtime with no shared lock between connections.
