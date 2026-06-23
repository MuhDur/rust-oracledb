# Migrating to `oracledb` 0.3.0

0.3.0 introduces the permanent 1.0 query/execute contract: four operation
families — `query`, `execute`, `execute_many`, `register_query` — plus their
`*_with` builder variants and the row-cardinality helpers `query_one` /
`query_opt` / `query_all`. The old `execute_query*` / `query_named*` /
`execute_query_for_registration` methods still exist in 0.3.0 but are now
`#[deprecated(since = "0.3.0")]`.

> **One release to migrate.** The deprecated shims ship in **0.3.0** and are
> **removed in `0.5.0`** (road-to-1.0 task W4-T1). They delegate to the
> exact same private operation core as the new families, so 0.3.0 is purely
> additive for behavior — your 0.2.x calls keep working (with a deprecation
> warning) until you migrate. Code that must compile clean on 0.3.0 should move
> to the new families now.

This guide is the user-facing form of the internal "nothing lost" map in
[`API_DESIGN.md` §8](API_DESIGN.md#8-the-retained-low-level-surface-kept-as-is--nothing-lost-map);
the old→new table below is generated from / kept consistent with it. The
`api_design_nothing_lost_map_covers_current_surface` test in
`crates/oracledb/src/lib.rs` keeps that map covering the full deprecated surface.

The same migration applies to `BlockingConnection`: every async method named
below has a blocking twin with the same name and the same replacement (the
blocking new families are `BlockingConnection::query` / `query_with` /
`query_one` / `query_opt` / `query_all` / `execute` / `execute_with` /
`execute_many` / `execute_many_with` / `register_query`).

---

## Quick reference: deprecated → replacement

Every method here is deprecated on **both** `Connection` (async) and
`BlockingConnection` (blocking) unless noted.

| 0.2.x method | 0.3.0 replacement |
|---|---|
| `execute_query` | `query` / `query_with` for rows; `execute` / `execute_with` for DML/DDL/PL/SQL |
| `execute_query_collect` | `query` / `query_with` (LOB/JSON/vector cells are materialized by default) |
| `execute_query_with_timeout` | `Query::timeout(d)` with `query_with`, or `Execute::timeout(d)` with `execute_with` |
| `execute_query_with_binds` | `query(cx, sql, params)` for rows; `execute(cx, sql, params)` for DML/DDL/PL/SQL |
| `execute_query_with_binds_and_timeout` | `Query::new(sql).bind(params).timeout(d)` or `Execute::new(sql).bind(params).timeout(d)` |
| `query_named` | `query(cx, sql, params!{ ... })` (named binds via the `params!` macro) |
| `query_named_with_timeout` | `Query::new(sql).bind(params!{ ... }).timeout(d)` with `query_with` |
| `execute_query_with_bind_rows` | `execute_many(cx, sql, rows)` / `Batch::new(sql, rows)` with `execute_many_with` |
| `execute_query_with_bind_rows_and_options` | `execute_raw` (byte-identical raw `QueryResult`), or the curated `Batch::raw_options` / `Execute::raw_options` / `Query` builders (per family) |
| `execute_query_with_bind_rows_and_timeout` | `Batch::new(sql, rows).timeout(d)` with `execute_many_with` (or `execute_raw` with `timeout_ms`) |
| `execute_query_with_bind_rows_options_and_timeout` | `execute_raw` (pass `timeout_ms`) for the raw `QueryResult`, or the curated `Batch::raw_options(opts).timeout(d)` / `Execute::raw_options(opts).timeout(d)` (per family) |
| `execute_query_for_registration` | `Registration::new(sql, registration_id)` with `register_query` |

`query_one` / `query_opt` / `query_all` are new: they replace manual row-count
checks you previously did against a raw `QueryResult`.

---

## Before / after by method

The examples use the async `Connection`. For the blocking driver, call the
same-named methods on `BlockingConnection` (e.g.
`BlockingConnection::query(&mut conn, sql, params)`); the blocking row facade is
`BlockingRows` and the builders are identical.

### `execute_query` → `query` / `execute`

`execute_query` returned a raw first-batch `QueryResult` for *any* statement. In
the four-family API, choose the family by what the statement does: `query` for a
`SELECT`, `execute` for DML/DDL/PL/SQL.

```rust
// 0.2.x
let result = conn.execute_query(cx, "select id, name from emp", 100).await?;
for row in &result.rows { /* Vec<Option<QueryValue>> */ }

// 0.3.0 — rows
let names: Vec<String> = conn
    .query(cx, "select id, name from emp", ())
    .await?
    .collect(cx)
    .await?
    .iter()
    .map(|row| row.get::<String>("name"))
    .collect::<Result<_, _>>()?;

// 0.2.x — DDL via execute_query, result discarded
conn.execute_query(cx, "create table t (id number)", 1).await?;

// 0.3.0 — DML/DDL/PLSQL
let outcome = conn.execute(cx, "create table t (id number)", ()).await?;
let _ = outcome.rows_affected();
```

### `execute_query_collect` → `query`

`execute_query_collect` forced a define-fetch round trip so `CLOB` / `BLOB` /
`VECTOR` / native `JSON` cells were materialized in the first batch. The new
`query` family does this **by default**, so `execute_query_collect` collapses
into a plain `query`.

```rust
// 0.2.x
let result = conn.execute_query_collect(cx, "select doc from t", 50).await?;

// 0.3.0 — materialization is the default
let rows = conn.query(cx, "select doc from t", ()).await?.collect(cx).await?;
```

If you specifically want the old raw, describe-only behavior (a `None` cell for a
define-requiring column, no extra round trip), opt out with
`Query::stream_lobs()`:

```rust
let rows = conn
    .query_with(cx, Query::new("select doc from t").stream_lobs())
    .await?;
```

### `execute_query_with_timeout` → `Query::timeout` / `Execute::timeout`

```rust
use std::time::Duration;
use oracledb::{Query, Execute};

// 0.2.x — timeout in milliseconds
conn.execute_query_with_timeout(cx, "select * from emp", 100, Some(5_000)).await?;

// 0.3.0 — a Duration on the builder (rows)
let rows = conn
    .query_with(cx, Query::new("select * from emp").timeout(Duration::from_secs(5)))
    .await?;

// 0.3.0 — a Duration on the builder (DML/DDL/PLSQL)
conn.execute_with(cx, Execute::new("begin pkg.long_proc; end;").timeout(Duration::from_secs(5)))
    .await?;
```

> **Behavior change — single op deadline.** `*_with_timeout(timeout_ms)` armed a
> *fresh* timeout on every round trip, so an N-batch fetch could run up to N×
> the intended budget. The builder `timeout(Duration)` is translated **once**
> into a single absolute deadline that spans the initial call *and* every
> `Rows::next_batch` / `Rows::collect` continuation and every LOB chunk of the
> one logical operation (API_DESIGN §principle 7). `next_batch` / `collect`
> inherit that deadline and take no timeout of their own. On expiry the driver
> still does BREAK → drain → `Error::CallTimeout` and leaves the session
> `Ready`, exactly as before.

### `execute_query_with_binds` → `query` / `execute` with params

Positional binds become a `Params` argument (a tuple, an array, a `Vec<BindValue>`,
or the `params!` macro).

```rust
// 0.2.x — positional BindValue slice
let binds = vec![BindValue::from(40), BindValue::from("alice")];
let result = conn
    .execute_query_with_binds(cx, "select * from emp where id=:1 and name=:2", 100, &binds)
    .await?;

// 0.3.0 — rows
let rows = conn
    .query(cx, "select * from emp where id=:1 and name=:2", (40, "alice"))
    .await?
    .collect(cx)
    .await?;

// 0.3.0 — DML
let outcome = conn
    .execute(cx, "update emp set name=:2 where id=:1", (40, "alice"))
    .await?;
let _ = outcome.rows_affected();
```

### `execute_query_with_binds_and_timeout` → `bind(..).timeout(..)`

```rust
use std::time::Duration;
use oracledb::Query;

// 0.2.x
conn.execute_query_with_binds_and_timeout(cx, sql, 100, &binds, Some(5_000)).await?;

// 0.3.0 — rows
let rows = conn
    .query_with(cx, Query::new(sql).bind((40, "alice")).timeout(Duration::from_secs(5)))
    .await?;
```

(See the single-op-deadline note above for the timeout semantics change.)

### `query_named` → `query` with `params!`

```rust
use oracledb::params;

// 0.2.x
let result = conn
    .query_named(
        cx,
        "select * from emp where id=:id and name=:name",
        vec![(":id".into(), 40.into()), (":name".into(), "alice".into())],
    )
    .await?;

// 0.3.0 — the params! macro builds the named binds
let rows = conn
    .query(
        cx,
        "select * from emp where id=:id and name=:name",
        params!{ ":id" => 40, ":name" => "alice" },
    )
    .await?
    .collect(cx)
    .await?;
```

Named binds are still reordered to the placeholders' first-appearance order in
the SQL, exactly as `query_named` did.

### `query_named_with_timeout` → `bind(params!{}).timeout(..)`

```rust
use std::time::Duration;
use oracledb::{Query, params};

// 0.2.x
conn.query_named_with_timeout(cx, sql, named, Some(5_000)).await?;

// 0.3.0
let rows = conn
    .query_with(
        cx,
        Query::new(sql)
            .bind(params!{ ":id" => 40 })
            .timeout(Duration::from_secs(5)),
    )
    .await?;
```

### `execute_query_with_bind_rows` → `execute_many`

Array DML (`executemany`) is its own family. Each inner `Vec<BindValue>` is one
bind row.

```rust
// 0.2.x
let rows = vec![
    vec![BindValue::from(1), BindValue::from("a")],
    vec![BindValue::from(2), BindValue::from("b")],
];
let result = conn
    .execute_query_with_bind_rows(cx, "insert into t values (:1, :2)", 0, &rows)
    .await?;
let total = result.row_count;

// 0.3.0
let outcome = conn
    .execute_many(cx, "insert into t values (:1, :2)", rows)
    .await?;
let total = outcome.rows_affected();
```

### `execute_query_with_bind_rows_and_options` → `Batch::raw_options`

The common `ExecuteOptions` flags now have first-class builders
(`Batch::collect_errors` ↔ `batcherrors`, `Batch::row_counts` ↔
`arraydmlrowcounts`, `Execute::parse_only` ↔ `parse_only`,
`Query::scrollable` ↔ `scrollable`). Anything not yet promoted to a builder is
still reachable verbatim through `raw_options`.

```rust
use oracledb::{Batch, ExecuteOptions};

// 0.2.x
let opts = ExecuteOptions::default().with_batcherrors(true);
let result = conn
    .execute_query_with_bind_rows_and_options(cx, sql, 0, &rows, opts)
    .await?;
let errors = result.batch_errors;

// 0.3.0 — promoted builder
let outcome = conn
    .execute_many_with(cx, Batch::new(sql, rows).collect_errors())
    .await?;
let errors = outcome.errors();

// 0.3.0 — exact wire override for any rare/internal flag
let outcome = conn
    .execute_many_with(cx, Batch::new(sql, rows).raw_options(opts))
    .await?;
```

> **Empty-batch semantics differ — pick by intent.** The curated `execute_many`
> family treats an empty row set as a no-op (`execute_many_with(cx, Batch::new(sql,
> vec![]))` returns `BatchOutcome::empty(..)` without a round trip). The deprecated
> raw methods instead ran `sql` *once with no binds* for an empty slice. If you
> relied on that "run once" behavior, migrate to `execute_raw`, which preserves it
> byte-for-byte (`execute_raw(cx, sql, prefetch, &[], opts, timeout_ms)` executes
> `sql` once) — it delegates to the very same core helper the deprecated method
> called, so the returned `QueryResult` is identical. Use `execute_many*` only when
> the no-op-on-empty contract is what you want.

### `execute_query_with_bind_rows_and_timeout` → `Batch::timeout`

```rust
use std::time::Duration;
use oracledb::Batch;

// 0.2.x
conn.execute_query_with_bind_rows_and_timeout(cx, sql, 0, &rows, Some(5_000)).await?;

// 0.3.0
let outcome = conn
    .execute_many_with(cx, Batch::new(sql, rows).timeout(Duration::from_secs(5)))
    .await?;
```

### `execute_query_with_bind_rows_options_and_timeout` → `Batch::raw_options(..).timeout(..)`

```rust
use std::time::Duration;
use oracledb::{Batch, ExecuteOptions};

// 0.2.x
conn.execute_query_with_bind_rows_options_and_timeout(cx, sql, 0, &rows, opts, Some(5_000)).await?;

// 0.3.0
let outcome = conn
    .execute_many_with(
        cx,
        Batch::new(sql, rows).raw_options(opts).timeout(Duration::from_secs(5)),
    )
    .await?;
```

### `execute_query_for_registration` → `register_query`

```rust
use oracledb::Registration;

// 0.2.x
let query_id = conn.execute_query_for_registration(cx, sql, registration_id).await?;

// 0.3.0
let outcome = conn
    .register_query(cx, Registration::new(sql, registration_id))
    .await?;
let query_id = outcome.query_id(); // Option<u64>, same value as before
```

---

## New: row-cardinality helpers

Where 0.2.x code fetched a raw `QueryResult` and hand-checked the row count, use
the cardinality helpers. They are exact about the contract and avoid an
unnecessary fetch budget.

```rust
// exactly one row, else Error::NoRows / Error::TooManyRows
let row = conn.query_one(cx, "select sysdate from dual", ()).await?;

// zero or one row
let maybe = conn.query_opt(cx, "select name from emp where id=:1", (40,)).await?;

// eagerly drain every batch
let all = conn.query_all(cx, "select id from emp", ()).await?;
```

---

## Advanced: the raw execute escape hatch (`execute_raw`)

Almost all code should use the four families above. But if you were relying on a
deprecated `execute_query*` method specifically to get the **raw `QueryResult`**
back — for example a statement-type-agnostic layer that decides query-vs-DML
from `result.columns`, a parse-only describe, or per-bind-row OUT/RETURNING
aggregation — 0.3.0 adds a single low-level primitive that returns the
unprojected wire result, on both `Connection` and `BlockingConnection`:

```rust
use oracledb::ExecuteOptions;

let result /* : QueryResult */ = conn
    .execute_raw(
        cx,
        sql,
        prefetch_rows,                 // first-batch size
        &bind_rows,                    // &[Vec<BindValue>] (empty = no binds)
        ExecuteOptions::default(),     // or .with_parse_only(true), etc.
        timeout_ms,                    // Option<u32>; None = untimed
    )
    .await?;
// result.columns / cursor_id / more_rows / rows / out_values / return_values …
```

`execute_raw` is the execute-side counterpart to the retained low-level fetch
primitives (`fetch_rows*`, `define_and_fetch_rows_with_columns`, `scroll_cursor`,
`fetch_cursor`) and is part of the 1.0 contract — it is **not** deprecated. It is
the byte-identical replacement for
`execute_query_with_bind_rows_options_and_timeout` (and, with an empty
`bind_rows` / default options, the simpler `execute_query_with_*` variants).
Prefer the families for ordinary application code; reach for `execute_raw` only
when you genuinely need the raw result.

---

## Behavior changes to be aware of

These are not method renames — they are semantics you should re-check while
migrating.

### Single operation deadline (timeouts)

Covered above: the new `timeout(Duration)` builders translate to a **single
absolute deadline** for the whole logical operation (initial call + all
`next_batch` / `collect` continuations + LOB chunks), instead of re-arming
`timeout_ms` on each round trip. An N-batch fetch is now bounded by the budget
you set, not N× it. Cancellation is still real (BREAK → drain →
`Error::CallTimeout`, session left `Ready`).

### Structured error classification (W1-T6)

`Error` now carries a stable classification surface you can branch on instead of
string-matching messages. The relevant methods:

- `Error::kind() -> ErrorKind` — top-level category (`Network`, `Timeout`,
  `Cancel`, `Conversion`, `Protocol`, `Database`).
- `Error::ora_code() -> Option<i32>` (alias `oracle_code()`) — the `ORA-NNNNN`
  number when present.
- `Error::is_connection_lost() -> bool` — the connection needs a reconnect
  before retry.
- `Error::is_transient() -> bool` — expected to clear on its own (lock
  contention, deadlock victim, listener hand-off, resource-manager throttle, or a
  call timeout / explicit cancel); safe to retry on the **same** connection.
- `Error::retry_hint() -> RetryHint` — `Never` /
  `RetrySameConnectionIfIdempotent` / `ReconnectThenRetryIfIdempotent` (you still
  own the idempotency decision).
- `Error::is_retryable() -> bool` — `retry_hint() != Never`.
- `Error::resource_limit() -> Option<ResourceLimit>` — structured detail when the
  failure came from a `ProtocolLimits` bound.

```rust
match conn.query_all(cx, sql, ()).await {
    Ok(rows) => { /* ... */ }
    Err(e) if e.is_connection_lost() => { /* reconnect, then retry if idempotent */ }
    Err(e) if e.is_transient()       => { /* back off, retry on same connection */ }
    Err(e) => return Err(e),
}
```

`ErrorKind` and several error/value enums are `#[non_exhaustive]` (W1-T4), so
always include a wildcard arm when matching them.
