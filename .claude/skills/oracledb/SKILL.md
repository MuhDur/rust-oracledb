---
name: oracledb
description: >-
  Use the pure-Rust async thin-mode Oracle driver (the `oracledb` crate). Use
  when connecting to Oracle from Rust, running queries / binds / transactions,
  mapping rows to typed structs (FromRow), pooling, or embedding it in another
  app (e.g. a tokio service or oraclemcp) without Instant Client / OCI.
---

# oracledb — pure-Rust thin-mode Oracle driver

`oracledb` is a clean-room async thin-mode Oracle Database driver. It speaks the
TNS/TTC wire protocol directly — **no OCI, no Instant Client, no C**. The
published crates (`oracledb`, `oracledb-protocol`) are `#![forbid(unsafe_code)]`
(the PyO3 conformance harness, which is not published, has FFI `unsafe`). It is the Rust analogue of
python-oracledb's thin mode and passes that project's own thin-mode test suite.

There are **two API surfaces** and picking the right one is the single most
important decision:

| Surface | When | Needs a runtime? |
|---|---|---|
| `BlockingConnection` | sync code, CLI, or a tokio/std app via `spawn_blocking` | No — drives an internal `asupersync` runtime for you |
| `Connection` (async) | you already run on the `asupersync` runtime | Yes — every call takes a `&Cx` |

> **The gotcha:** the async `Connection` runs on **`asupersync`, not tokio**. You
> cannot `.await` it inside a tokio task. From a tokio app (axum, rmcp, oraclemcp)
> use `BlockingConnection` inside `tokio::task::spawn_blocking`. See
> [INTEGRATION.md](references/INTEGRATION.md).

> **Requires nightly Rust.** The async runtime (`asupersync`) is built with
> `#![feature(try_trait_v2)]`, so `oracledb` and anything depending on it only
> compile on a nightly toolchain — a stable build fails with `E0554` before
> reaching this crate's code. This repo currently pins `nightly-2026-05-11`;
> see [docs/TOOLCHAIN.md](../../../docs/TOOLCHAIN.md) before moving that pin.
> There is no stable MSRV.

`Cargo.toml`:

```toml
oracledb = "0.2"
# opt-in bridges & features (off by default; `derive` is already on):
# oracledb = { version = "0.2", features = ["chrono", "serde_json", "arrow"] }
```

## Quick start — blocking (the easy path)

This is a complete, ordinary synchronous `main` — no visible async. (Verbatim
shape of `crates/oracledb/examples/smoke.rs`.)

```rust
use oracledb::protocol::thin::QueryValue;
use oracledb::protocol::ClientIdentity;
use oracledb::{BlockingConnection, ConnectOptions};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    // What the DB records in v$session, in this order:
    // (program, machine, osuser, terminal, driver_name). All are sanitized.
    let identity = ClientIdentity::new("my-app", "host-1", "scott", "term-1", "oracledb")?;

    let mut conn = BlockingConnection::connect(ConnectOptions::new(
        "localhost:1521/FREEPDB1", // EZConnect host:port/service, or a TNS descriptor
        "scott",
        "tiger",
        identity,
    ))?;

    // execute_query(conn, sql, prefetch_rows) -> QueryResult
    let r = BlockingConnection::execute_query(&mut conn, "select 7+5 from dual", 1)?;
    let sum = r.cell(0, 0).and_then(QueryValue::as_i64).unwrap(); // 12

    println!("{sum}");
    BlockingConnection::close(conn)?;
    Ok(())
}
```

`BlockingConnection` is a unit struct of **static methods**. `connect(ConnectOptions)
-> Connection` opens a session and `close(Connection)` takes it by value; the
operations in between — `execute_query`, `query`, `query_named`,
`execute_query_with_binds`, `commit`, `rollback`, `fetch_rows`, `read_lob`,
`write_lob`, `aq_*`, … — take `&mut Connection`.

## Quick start — async (`Connection`)

The async surface is identical except every method takes `cx: &Cx` first and you
must be on an `asupersync` runtime (see the `asupersync-mega-skill` for runtime
setup — `RuntimeBuilder`, `Cx`):

```rust
use oracledb::{Connection, ConnectOptions, params};
// inside an asupersync Cx scope, with `cx: &Cx` in hand:
let mut conn = Connection::connect(cx, ConnectOptions::new(connstr, user, pass, identity)).await?;

// positional binds via params!
let r = conn.query(cx, "select * from emp where deptno = :1", params![20]).await?;

// named binds (reordered to SQL first-appearance order for you)
let r = conn.query_named(cx, "select * from emp where deptno = :d",
                         params!{ ":d" => 20 }).await?;

conn.commit(cx).await?;
```

Verified async signatures:

```rust
Connection::connect(cx: &Cx, options: ConnectOptions) -> Result<Connection>
fn execute_query(&mut self, cx: &Cx, sql: &str, prefetch_rows: u32) -> Result<QueryResult>
fn query(&mut self, cx: &Cx, sql: &str, params: impl IntoBinds) -> Result<QueryResult>
fn query_named(&mut self, cx: &Cx, sql: &str, named: Vec<(String, BindValue)>) -> Result<QueryResult>
fn execute_query_with_binds(&mut self, cx: &Cx, sql: &str, prefetch: u32, binds: &[BindValue]) -> Result<QueryResult>
fn commit(&mut self, cx: &Cx) -> Result<()>   /   fn rollback(&mut self, cx: &Cx) -> Result<()>
```

## Binds: the `params!` macro

```rust
use oracledb::params;
let positional = params![40, "alice", true];        // -> Vec<BindValue>
let named      = params!{ ":id" => 40, ":name" => "alice" }; // -> Vec<(String, BindValue)>
```

Any `ToSql` value works (ints, `&str`/`String`, bool, `Option<T>` for NULL, plus
`chrono`/`uuid`/`rust_decimal`/`serde_json` types under their features).

## Typed rows: `#[derive(FromRow)]`

```rust
use oracledb::FromRow;

#[derive(FromRow)]
struct Emp { id: i64, name: String, hired: Option<chrono::NaiveDate> }
// maps each field BY COLUMN NAME through the real FromSql conversion
// (i64 <- NUMBER, Option<T> <- NULL, NaiveDate <- DATE). Tuple structs map by
// position. #[oracledb(column = "...")] / #[oracledb(rename_all = "...")] adjust names.
```

## Reading results

`QueryResult` gives you cells and typed rows:
- `r.cell(row, col) -> Option<&QueryValue>`, then `QueryValue::as_i64()` /
  `as_text()` / etc.
- typed rows via the `FromRow`/`FromSql` path (`get_by_name`).
- borrowed, zero-copy iteration over a batch via `fetch_rows_ref` /
  `for_each_row_ref` (avoid per-row allocation on hot paths).

## Transactions

DML does not auto-commit. `conn.commit(cx)` / `conn.rollback(cx)` (or the
`BlockingConnection::commit/rollback(&mut conn)` forms). Two-phase commit
(`tpc_*`) and sessionless transactions are also exposed.

## Pooling

`oracledb::pool::PoolEngine<B: PoolBackend>` is a session pool (`start`,
`acquire`, `return_connection`, `close`, plus getmode / wait-timeout /
max-lifetime / ping-interval knobs). For a tokio app, pooling `Connection`s
(each obtained from `BlockingConnection::connect`) with `r2d2` behind
`spawn_blocking` is also fine — see [INTEGRATION.md](references/INTEGRATION.md).

## Errors

`oracledb::Error` is a structured enum; database errors carry the **ORA code** so
you can branch on it (retryable transient errors, ORA-00942, etc.) rather than
string-matching. `Result<T> = Result<T, oracledb::Error>`.

## Feature flags

`derive` is **on** by default; everything else is **off** by default.

| feature | adds |
|---|---|
| `derive` (**on by default**) | `#[derive(FromRow)]` |
| `chrono` / `uuid` / `rust_decimal` / `serde_json` | typed `FromSql`/`ToSql` bridges |
| `arrow` | columnar fetch straight into Arrow builders (analytics) |
| `tracing` | per-round-trip OpenTelemetry-style spans (zero-cost when off) |
| `soda` | experimental thin-mode SODA (python-oracledb thin has none) |
| `cassette` | record/replay the wire stream for deterministic tests |
| `experimental` | `cwallet.sso` auto-login wallet reader |

## Performance levers (when they matter)

- **no-GIL concurrency** is the headline: N connections decode in parallel
  across threads (Python threads serialize on the GIL). This is where Rust wins
  by a large margin; a single query is ~95% server round-trip, so single-thread
  speedups are modest.
- `arrow` feature → columnar decode for analytics.
- native pipelining batches N statements into one round trip (`run_pipeline`).
- borrowed fetch (`fetch_rows_ref`) and prefetch tuning cut client CPU.

See the repo `README.md` "Performance" section and `docs/PERFORMANCE.md` for the
honest, measured numbers and the Amdahl reasoning.

## References

| Topic | File |
|---|---|
| Fuller method catalog (async + blocking, LOB/AQ/CQN/scroll) | [API.md](references/API.md) |
| Embedding in a tokio app / oraclemcp (the runtime bridge) | [INTEGRATION.md](references/INTEGRATION.md) |
| Cutting a release (crates.io + GitHub, in sync) | tag-driven `.github/workflows/release.yml` + `scripts/{release_preflight,publish_crates}.sh` |

## Anti-patterns

| Don't | Do |
|---|---|
| `.await` a `Connection` call inside a tokio task | use `BlockingConnection` in `spawn_blocking` |
| string-match error messages | branch on the ORA code on `oracledb::Error` |
| build SQL by string-concatenating values | use `params!` binds |
| expect 50× single-query speedup over python-oracledb | the win is concurrency / no-GIL, not per-call CPU |
| add Instant Client to deploy | there is none — it's a single static binary |
