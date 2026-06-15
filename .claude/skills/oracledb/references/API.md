# oracledb — fuller method catalog

Most operations exist on both surfaces. The async `Connection` methods take
`cx: &Cx` as the first argument and are `async`. The `BlockingConnection` methods
are **associated functions**: `connect` takes a `ConnectOptions` and returns a
`Connection`, `close` takes the `Connection` by value, and every other method
takes `&mut Connection`. They run the async work on an internal `asupersync`
runtime.

A few operations are async-`Connection`-only (no blocking form): the borrowed-ref
fetch family (`fetch_rows_ref`, `fetch_rows_ref_response`, `for_each_row_ref`) and
`notify_register` / `recv_notification`.

The fallible operations return `oracledb::Result<T>` (i.e.
`Result<T, oracledb::Error>`); the sync getters (`session_id`, `serial_num`,
`identity`, …) return plain values/references.

## Connect / lifecycle

| async `Connection` | blocking `BlockingConnection` |
|---|---|
| `connect(cx, ConnectOptions) -> Connection` | `connect(ConnectOptions) -> Connection` |
| `ping(cx)` / `ping_with_timeout(cx, ms)` | `ping(&mut c)` / `ping_with_timeout(&mut c, ms)` |
| `change_password(...)` | `change_password(...)` |
| (drop) | `close(c)` |
| `is_dead()`, `session_id()`, `serial_num()`, `server_version()`, `sdu()`, `supports_pipelining()`, `cancel_handle()`, `descriptor()`, `identity()` (sync getters on both) | — |

`ConnectOptions::new(connect_string, user, password, identity: ClientIdentity)`.
The connect string is either EZConnect (`host:port/service`) or a full TNS
descriptor `(DESCRIPTION=(ADDRESS=...)(CONNECT_DATA=...))`. TLS/TCPS is driven by
the descriptor / wallet; see `docs/CONNECT_STRINGS.md` and `tls` module.

`ClientIdentity::new(program, machine, osuser, terminal, driver_name)` sets
exactly what the DBA sees in `v$session` — independent of the (possibly empty)
container OS env. Each field is sanitized; empty values are rejected.

## Query / execute

| operation | async | blocking |
|---|---|---|
| simple | `execute_query(cx, sql, prefetch) -> QueryResult` | `execute_query(&mut c, sql, prefetch)` |
| positional binds | `query(cx, sql, impl IntoBinds)` | `query(&mut c, sql, binds)` |
| named binds | `query_named(cx, sql, Vec<(String, BindValue)>)` | `query_named(&mut c, sql, named)` |
| explicit binds | `execute_query_with_binds(cx, sql, prefetch, &[BindValue])` | same |
| with timeout | `execute_query_with_timeout(...)`, `*_with_binds_and_timeout(...)` | same |
| collect all rows | `execute_query_collect(...)` | `execute_query_collect(...)` |
| batch / executemany | `execute_query_with_bind_rows`, `_and_timeout`, `_and_options`, `_options_and_timeout` | `execute_query_with_bind_rows`, `_and_timeout`, `_options_and_timeout` (no plain `_and_options`) |

`prefetch` is the number of rows to ask the server to ship with the execute
(round-trip reducer). `1` is fine for single-row; raise it for large result sets.

## Fetching rows

- `fetch_rows(cx, ...)` / `fetch_rows_with_columns(...)` — owned rows.
- `fetch_rows_ref(...)` / `fetch_rows_ref_response(...)` — **borrowed** rows
  (`QueryValueRef`) that point into the read buffer: zero-copy, no per-cell
  allocation. Use on hot paths. *(async `Connection` only.)*
- `for_each_row_ref(cx, ..., |row_ref| { ... })` — borrowed streaming callback.
  *(async `Connection` only.)*
- `define_and_fetch_rows_with_columns(...)` — supply your own column definitions.
- `scroll_cursor(...)` — scrollable cursors.

## Reading results

`QueryResult`:
- `cell(row, col) -> Option<&QueryValue>`
- `QueryValue` accessors: `as_i64()`, `as_text()`, `as_f64()`, NUMBER access, etc.
- typed projection via `FromRow` / `FromSql` (`get_by_name` by column name).

`OracleNumber` is lossless: integers up to 38 digits are kept as an inline i128
mantissa+scale, with a text fallback for the full Oracle NUMBER range — never
silently lossy via `f64`.

## Transactions

- `commit(cx)` / `rollback(cx)` — DML is **not** auto-committed.
- `transaction_in_progress()` (sync getter).
- Sessionless: `begin_sessionless_transaction`, `resume_*`, `suspend_*`.
- Two-phase commit (XA): `tpc_begin`, `tpc_end`, `tpc_prepare`, `tpc_commit`,
  `tpc_rollback`.

## LOBs

`read_lob` / `read_lob_with_timeout`, `create_temp_lob`, `write_lob` /
`write_lob_with_timeout` (blocking surface; async equivalents exist on
`Connection`).

## Advanced Queuing (AQ)

`aq_enq_one`, `aq_deq_one`, `aq_enq_many`, `aq_deq_many`.

## Change notification (CQN)

`subscribe_register` / `subscribe_unregister`,
`execute_query_for_registration` (both surfaces); `notify_register` /
`recv_notification` (async `Connection` only).

## Cancellation

`cancel_handle() -> CancelHandle` returns a handle you can use from another task
to break an in-flight call (out-of-band where the server supports it). Fetch
cancellation is drain-correct (the connection is left in a clean state).

## Pooling — `oracledb::pool`

`PoolEngine<B: PoolBackend>`:
- `start(backend, PoolConfig) -> PoolEngine`
- `acquire(AcquireOptions) -> conn_id`
- `return_connection(conn_id)` / `drop_connection(conn_id)`
- `close(force)`, `busy_count()`, `open_count()`
- tunables: `getmode`, `wait_timeout_ms`, `timeout_secs`,
  `max_lifetime_session_secs`, `ping_interval_secs`.

`PoolConfig` / `AcquireOptions` are plain config structs. The pool is generic
over a backend so it can be driven from either surface.
