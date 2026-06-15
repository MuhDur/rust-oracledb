# Embedding `oracledb` in another app (the runtime bridge)

`oracledb`'s async surface runs on the **`asupersync`** runtime. Most host apps
(axum/hyper/rmcp/tonic and **oraclemcp**) run on **tokio**. You cannot directly
`.await` an `asupersync` future inside a tokio task — they are different reactors.

There are two clean ways to bridge. Pick based on what the host already does.

## Option A — `BlockingConnection` inside `spawn_blocking` (recommended)

`BlockingConnection` already encapsulates an `asupersync` runtime and exposes a
**synchronous** API. So from tokio you treat it exactly like any other blocking
driver (this is the same shape ODPI-C drivers already use):

```rust
// On a tokio runtime:
let row = tokio::task::spawn_blocking(move || -> oracledb::Result<i64> {
    let mut conn = oracledb::BlockingConnection::connect(opts)?;
    let r = oracledb::BlockingConnection::execute_query(&mut conn, sql, 50)?;
    let v = r.cell(0, 0).and_then(|c| c.as_i64()).unwrap_or_default();
    oracledb::BlockingConnection::close(conn)?;
    Ok(v)
}).await??;
```

Pool the `Connection`s with `r2d2`: a `ManageConnection` with
`type Connection = oracledb::Connection`, whose `connect` calls
`BlockingConnection::connect`, `is_valid` calls `BlockingConnection::ping`, and
`has_broken` calls `Connection::is_dead()`. (`BlockingConnection` itself is a
zero-sized static shim — there is nothing to pool; `connect` hands you a
`Connection`.) Keep every DB call inside `spawn_blocking`; no asupersync types
escape into tokio code.

**Why this is the low-friction path:** the host keeps its existing
`spawn_blocking` + pool structure; only the *inner* driver call changes. This is
the recommended route for migrating **oraclemcp** off ODPI-C — it deletes the
Instant Client dependency and makes the server a single static binary, with the
diff contained to `oraclemcp-db`'s `connection.rs` / `pool.rs` / `query.rs`.

## Option B — a dedicated asupersync executor + channel bridge

If you want true async (no blocking pool thread per in-flight query), run the
async `Connection` on its own asupersync runtime thread(s) and hand work in /
results out over channels, exposing a tokio-friendly `async fn` that awaits a
`tokio::sync::oneshot`. More moving parts; only worth it under high concurrency
where a blocking-thread-per-call pool is the bottleneck. See the
`asupersync-mega-skill` `COMPAT-BRIDGE` / `COMPAT-BOUNDARY` references for the
runtime-interop patterns.

## Migrating oraclemcp off ODPI-C — concrete plan

Today `oraclemcp-db` uses the `oracle` crate (ODPI-C, **thick** — needs Instant
Client), pooled with `r2d2`, called via `tokio::spawn_blocking`. It already gates
the whole driver behind an `oracle-driver` feature with a clean
`connection.rs`/`pool.rs`/`query.rs`/`serialize.rs` seam and an offline stub.

1. Add an `oracledb` backend **beside** the ODPI-C one behind a new feature
   (e.g. `thin-driver`), so it is opt-in and reversible — don't rip out ODPI-C
   in one shot.
2. Implement the same internal trait the ODPI-C backend implements, with
   `BlockingConnection` (Option A). Map result columns in `serialize.rs` using
   `QueryValue` accessors / `FromSql`.
3. Map `oracledb::Error` ORA codes into `oraclemcp-error` (branch on code, not
   message text).
4. Keep `r2d2` as the pool manager over `Connection`, or switch to
   `oracledb::pool::PoolEngine`.
5. Run oraclemcp's existing test suite against the thin backend behind the
   feature flag; promote it to default only once it has carried real traffic.

Net result: no OCI / Instant Client, single static binary, and no-GIL parallel
decode instead of r2d2 + `spawn_blocking` thread-pool contention.

## Checklist for any host integration

- [ ] Decide Option A (blocking) vs B (channel bridge). Default to A.
- [ ] Never let an `asupersync` `Cx`/future cross into a tokio `.await`.
- [ ] Pool connections; don't connect per request.
- [ ] Branch on ORA codes from `oracledb::Error`, not strings.
- [ ] Use `params!` binds; never string-concatenate SQL.
- [ ] Pick `prefetch` to match expected result size (round-trip reducer).
- [ ] Gate the new backend behind a feature until it has run real traffic.
