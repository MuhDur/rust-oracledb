# oracledb 1.0 — Public API Design (worked design for W1-T3)

> **Status:** planning design. The worked elaboration of `ROAD_TO_1_0.md` **W1-T3**
> (operation-specific public API) + **W1-T8** (async↔blocking symmetry). Grounded in
> the verified capability inventory (every current public method/type, cited there)
> and the Rust-DB precedent survey (tokio-postgres / sqlx / rusqlite / diesel-async).
> Decisions are the reviewing agent's calls (the user delegated them); each carries
> rationale. Final signature bikeshedding happens at implementation, but the shape,
> the families, and the "nothing lost" mapping are settled here.

This is a **redesign of the execute/query sprawl only** (19 `execute_query*`/`query*`
methods → 4 coherent families). The large *retained* surface (low-level fetch, LOB,
AQ, objects, transactions, pool, pipelining, CQN subscribe/notify, lifecycle, Arrow,
direct-path, SODA, the `oracledb::protocol` re-export) is **kept as-is** and only
touched by the audit passes (W0-T5 ledger visibility, W1-T4 `#[non_exhaustive]`,
W1-T9 module tidy). §8 proves nothing is lost across the whole surface.

---

## 1. Principles (decisions + rationale)

1. **Four operation families over a private `OperationCore`/`ConnectionCore`** —
   `query` (rows), `execute` (DML/DDL/PLSQL, ≤1 bind row), `execute_many` (array DML),
   `register_query` (CQN). No invalid states (batch rows ≠ scalar binds; CQN only on
   `register_query`; fetch/LOB policy only on `query`).
2. **Method-args convenience AND a per-family builder — builder NOT mandatory**
   (precedent: keep `conn.query(sql, params)`; tokio-postgres model). 90% of calls use
   the 3-arg convenience; option-rich calls (`timeout`, `arraysize`, `parse_only`,
   batch flags) use the family builder.
3. **Cardinality siblings** `query_one` / `query_opt` / `query_all` (high-use in
   tokio-postgres & sqlx; omitting them pushes boilerplate onto every caller).
4. **Named binds first-class** (Oracle/python-oracledb are named-primary), positional
   too; **owned `BindValue`** internally (kills the `&[&dyn ToSql]` wart). Keep
   `params!`/`IntoBinds`/`FromRow`/`FromSql`/`ToSql` verbatim.
5. **Owned `Row`**, single `usize`-or-`&str` accessor (sqlx `ColumnIndex` model);
   decouple `Row`'s lifetime from any request borrow.
6. **`Rows` = first batch + lazy continuation** as the v1 cursor (matches the wire
   protocol; no `futures::Stream` in the stable surface; clean blocking mirror) + eager
   `query_all`. A `query_stream` is a reserved *future additive* (bucket-2 `x3s`).
7. **Per-call `Duration` timeout, cancel-safe** — justified (real BREAK/RESET
   cancellation, unlike `tokio::time::timeout` future-drop). On expiry: BREAK→drain→
   `Error::CallTimeout`, session left `Ready` (W1-T2). A `Cx` deadline, if tighter, wins.
   **The `Duration` is translated *once* into a single absolute deadline carried in the
   op/cursor context, spanning *every* round-trip of the one logical operation** — the
   initial call *and* all `Rows::next_batch`/`collect` continuations and every LOB chunk
   — never re-armed per round-trip. (`next_batch`/`collect` take no timeout of their own;
   they inherit the cursor's deadline.) This avoids the per-call-`timeout_ms` pitfall
   where an N-batch fetch could run up to N× the intended budget. The post-timeout
   BREAK→drain runs under its *own* bounded recovery budget (W1-T2), so the expired op
   deadline cannot also cancel the cleanup that keeps the session `Ready`.
8. **`execute` returns a struct** (`rows_affected` + `last_rowid` + OUT/IN-OUT binds +
   RETURNING + implicit result sets), not a bare `u64`. RETURNING is surfaced via OUT
   binds (Oracle-correct), not as query rows.
9. **`execute_many` / `register_query` are designed on python-oracledb's terms** —
   zero Rust precedent (array DML w/ `batch_errors`/`array_dml_row_counts`; CQN).
10. **`BlockingConnection` stays a generated 1:1 mirror** (W1-T8). Every new family
    method gets a sync twin.
11. **Nothing in the retained surface changes shape** — the redesign is scoped to the
    execute/query sprawl; the private core still exposes the execute/fetch/define/cursor
    machinery SODA/Arrow/direct-path facade over.

---

## 2. Binds

```rust
/// Single-row bind payload: positional OR named (multi-row is Batch, §5).
pub enum Params<'a> {
    None,
    Positional(Cow<'a, [BindValue]>),         // :1, :2, …  (binds[0] -> :1)
    Named(Cow<'a, [(String, BindValue)]>),    // reordered to placeholder first-appearance
}
impl<'a, T: IntoBinds> From<T>                    for Params<'a> {} // () , tuples 1-12, [T;N], Vec<T:ToSql>, Vec<BindValue>
impl<'a>             From<Vec<(String,BindValue)>> for Params<'a> {} // params!{}  (Named)
```
- `IntoBinds` (tuples 1–12, slices, `Vec<T: ToSql>`, raw `Vec<BindValue>`, `()`) and the
  two `params!` arms are **kept verbatim**. `params![40,"x"]` → `Positional`;
  `params!{":id"=>40}` → `Named`. Borrowed (`Cow`) so callers can pass `&[BindValue]`
  without moving; owned values live in `BindValue` (no `&dyn ToSql` lifetime friction).
- The full **22 current `BindValue` variants** remain the bind currency
  (`Null`/`TypedNull`/`Output`/`ReturnOutput`/`ObjectOutput`/`ObjectInput`/`Text`/
  `Raw`/`Lob`/`Number`/`BinaryInteger`/`BinaryDouble`/`BinaryFloat`/`Boolean`/
  `IntervalDS`/`IntervalYM`/`DateTime`/`Timestamp`/`Array`/`Vector`/`Json`/
  `Cursor`) — OUT/IN-OUT/RETURNING/object/cursor binds all expressible. Earlier
  planning text said 23; source truth is 22 at this point in W1.

---

## 3. Family 1 — `query` (rows)

```rust
// Convenience (3-arg; sane defaults: arraysize 100, LOB/VECTOR/JSON materialized):
pub async fn query     (&mut self, cx: &Cx, sql: &str, p: impl Into<Params<'_>>) -> Result<Rows>;
pub async fn query_one (&mut self, cx: &Cx, sql: &str, p: impl Into<Params<'_>>) -> Result<Row>;        // exactly 1 (else error)
pub async fn query_opt (&mut self, cx: &Cx, sql: &str, p: impl Into<Params<'_>>) -> Result<Option<Row>>; // 0 or 1
pub async fn query_all (&mut self, cx: &Cx, sql: &str, p: impl Into<Params<'_>>) -> Result<Vec<Row>>;    // eager drain

// Builder (option-rich) — one entry; cardinality via Rows helpers:
pub async fn query_with(&mut self, cx: &Cx, q: Query<'_>) -> Result<Rows>;
```
```rust
#[non_exhaustive]
pub struct Query<'a> { /* private */ }
impl<'a> Query<'a> {
    pub fn new(sql: &'a str) -> Self;
    pub fn bind(self, p: impl Into<Params<'a>>) -> Self;
    pub fn arraysize(self, n: NonZeroU32) -> Self;       // rows/round-trip (default 100)
    pub fn prefetch(self, n: u32) -> Self;               // speculative rows on execute
    pub fn stream_lobs(self) -> Self;                    // opt OUT of auto-materialize
    pub fn scrollable(self) -> Self;                     // -> scrollable cursor (see Rows::scroll)
    pub fn timeout(self, d: Duration) -> Self;           // cancel-safe BREAK timeout
}
```
```rust
#[non_exhaustive]
pub struct Rows { /* private: columns + current batch + open cursor */ }
impl Rows {
    pub fn columns(&self) -> &[ColumnMetadata];
    pub fn batch(&self) -> &[Row];                            // current materialized batch
    pub async fn next_batch(&mut self, cx: &Cx) -> Result<bool>;   // true when batch() was refreshed; inherits the op deadline (§principle 7)
    pub async fn collect(self, cx: &Cx) -> Result<Vec<Row>>;       // drain to the end
    pub fn one(self) -> Result<Row>;                          // exactly-1 (errors if 0/>1)
    pub fn opt(self) -> Result<Option<Row>>;
    pub fn into_typed<T: FromRow>(self) -> Result<Vec<T>>;    // FromRow path (current batch)
    pub fn cursor(&self) -> Option<&Cursor>;                  // REF CURSOR / implicit RS handle
    pub async fn scroll(&mut self, cx: &Cx, to: Scroll) -> Result<()>; // scrollable reposition
}
```
- **Materialize-by-default** resolves oraclemcp #11 (the define-fetch footgun): `query`
  runs the DEFINE-FETCH so CLOB/BLOB/VECTOR/JSON cells are populated, not `None`.
  `.stream_lobs()` opts out for the streaming/low-level path.
- **REF CURSOR / implicit result sets:** a `QueryValue::Cursor` cell yields a `Cursor`;
  `Rows::cursor()` + the retained `fetch_cursor` drain it (§6).
- `arraysize` default **100** (today's `query` hardcodes 1 — a silent multi-row footgun).

---

## 4. Family 2 — `execute` (DML / DDL / PL/SQL, ≤1 bind row)

```rust
pub async fn execute     (&mut self, cx: &Cx, sql: &str, p: impl Into<Params<'_>>) -> Result<ExecuteOutcome>;
pub async fn execute_with(&mut self, cx: &Cx, e: Execute<'_>) -> Result<ExecuteOutcome>;
```
```rust
#[non_exhaustive]
pub struct Execute<'a> { /* private */ }
impl<'a> Execute<'a> {
    pub fn new(sql: &'a str) -> Self;
    pub fn bind(self, p: impl Into<Params<'a>>) -> Self;
    pub fn timeout(self, d: Duration) -> Self;
    pub fn parse_only(self) -> Self;                      // validate without executing
    pub fn raw_options(self, o: ExecuteOptions) -> Self;  // escape hatch: all 13 knobs via builders/getters
}
```
```rust
#[non_exhaustive]
pub struct ExecuteOutcome { /* private */ }
impl ExecuteOutcome {
    pub fn rows_affected(&self) -> u64;
    pub fn last_rowid(&self) -> Option<&str>;
    pub fn out_binds(&self) -> &OutBinds;                 // OUT / IN-OUT (incl. object OUT)
    pub fn returning(&self) -> &ReturningRows;            // RETURNING INTO (per-bind rows)
    pub fn implicit_results(&self) -> &[Cursor];          // DBMS_SQL.RETURN_RESULT
    pub fn compilation_warning(&self) -> Option<&str>;    // PL/SQL "compiled with warnings"
}
```
- **OUT/IN-OUT, RETURNING, implicit result sets** are surfaced as typed accessors over
  what is today `QueryResult.out_values` / `return_values` / `implicit_resultsets`.
- **All 13 `ExecuteOptions` knobs survive:** common ones get builder methods
  (`parse_only`; batch flags live on `Batch`, §5; `registration_id` on `Registration`,
  §6; scroll fields on `Query::scrollable`/`Rows::scroll`); the rest
  (`cursor_id` reuse, `cache_statement`, `no_prefetch`, `token_num`, `suspend_on_success`)
  are driver-internal or other-family, **and** `Execute::raw_options(ExecuteOptions)` is a
  documented method-based escape hatch so power users lose nothing without depending on field layout.
- **DBMS_OUTPUT** stays as `enable_dbms_output` / `read_dbms_output` (retained convenience
  over the OUT-bind machinery; §6).

---

## 5. Family 3 — `execute_many` (array DML / executemany)

```rust
pub async fn execute_many     (&mut self, cx: &Cx, sql: &str, rows: impl Into<BatchRows<'_>>) -> Result<BatchOutcome>;
pub async fn execute_many_with(&mut self, cx: &Cx, b: Batch<'_>) -> Result<BatchOutcome>;
```
```rust
pub enum BatchRows<'a> { Borrowed(&'a [Vec<BindValue>]), Owned(Vec<Vec<BindValue>>) } // iters = rows.len()
#[non_exhaustive]
pub struct Batch<'a> { /* private */ }
impl<'a> Batch<'a> {
    pub fn new(sql: &'a str, rows: impl Into<BatchRows<'a>>) -> Self;
    pub fn collect_errors(self) -> Self;     // ExecuteOptions.batcherrors
    pub fn row_counts(self) -> Self;         // ExecuteOptions.arraydmlrowcounts
    pub fn timeout(self, d: Duration) -> Self;
    pub fn raw_options(self, o: ExecuteOptions) -> Self;
}
#[non_exhaustive]
pub struct BatchOutcome { /* private */ }
impl BatchOutcome {
    pub fn rows_affected(&self) -> u64;
    pub fn per_row_counts(&self) -> Option<&[u64]>;       // when .row_counts()
    pub fn errors(&self) -> &[BatchError];               // {row_index, code, message} when .collect_errors()
    pub fn returning(&self) -> &ReturningRows;           // array RETURNING
}
```
- True server-side array DML (one round trip), not a client loop. `BatchError` exposes
  the row index (today's `BatchServerError{code,offset,message}`).
- Empty batches are a zero-iteration no-op, not a no-bind statement execute; ragged
  bind rows are rejected before any wire work.
- The iterative-PL/SQL helper (`bind_rows_need_iterative_plsql` + `ExecutemanyManager`)
  is the private engine driving this; it stays internal.

---

## 6. Family 4 — `register_query` (CQN) + retained subscribe/notify

```rust
pub async fn register_query(&mut self, cx: &Cx, r: Registration<'_>) -> Result<RegistrationOutcome>;
#[non_exhaustive]
pub struct Registration<'a> { /* sql, params, subscription_id, timeout */ }
#[non_exhaustive]
pub struct RegistrationOutcome { pub fn query_id(&self) -> Option<u64>; }
```
- `register_query` = today's `execute_query_for_registration` (id in → query-id out).
  Server query id `0` is normalized to `None` at this high-level API.
- The CQN lifecycle stays in the **retained** family: `subscribe_register` /
  `subscribe_unregister` / `notify_register` / `recv_notification` →
  `NotificationOutcome` (these are not "queries" and don't fit the four families;
  CapabilityInventory item 11 — preserved as-is).

---

## 7. Cross-cutting: `Row`, typed access, `Error`, blocking mirror

```rust
#[non_exhaustive] pub struct Row { /* owned */ }
impl Row {
    pub fn get<T: FromSql>(&self, i: impl ColumnIndex) -> Result<T>;       // usize OR &str
    pub fn try_get<T: FromSql>(&self, i: impl ColumnIndex) -> Result<Option<T>>;
    pub fn columns(&self) -> &[ColumnMetadata];
    pub fn value(&self, i: impl ColumnIndex) -> Option<&QueryValue>;       // raw escape
}
pub trait ColumnIndex { /* impl for usize and &str */ }
```
- **Typed-mapping stack kept verbatim:** `FromRow` (+ `#[derive(FromRow)]`), `FromSql`,
  `ToSql`, all feature-gated impls (chrono/uuid/serde_json/rust_decimal, always
  Vec<f32>/Vec<f64> for VECTOR), `QueryResultExt`. `TypedRow` becomes the internal basis
  of `Row`. The 16 `QueryValue` variants and their accessors (`as_i64`/`as_text`/…) stay.
- **`Error`** (W1-T6): typed; `kind() -> ErrorKind`, `ora_code()`/`oracle_code()`,
  `offset()`, `caret(sql)`, `connection_disposition() -> {Reusable, Dead}`,
  `retry_hint()`, and the existing `is_connection_lost`/`is_transient`/`is_retryable`
  helpers + curated code sets. `BindError` covers client-side bind-shape
  prevalidation; `SessionlessError`/`ConversionError`/`PoolError` remain the supporting
  public taxonomies (with enum evolution handled by W1-T4).
- **`BlockingConnection`** gets the 1:1 twin of every method above, each
  `block_on`-wrapping its async sibling (W1-T8 verifies completeness):
  `query`, `query_one`, `query_opt`, `query_all`, `query_with`, `execute`,
  `execute_with`, `execute_many`, `execute_many_with`, and `register_query`.
  `query`/`query_with` return `BlockingRows`, the synchronous cursor facade for
  `columns`, `batch`, `next_batch`, `collect`, `one`, `opt`, `into_typed`,
  `cursor`, and `scroll`; sync callers never need to supply a `Cx`.

---

## 8. The retained low-level surface (kept as-is) + "nothing lost" map

The four families cover the *common* path. Everything below is a **distinct capability**
kept verbatim (only audit-tidied), and the private core still exposes the
execute/fetch/define/cursor machinery the SODA/Arrow/direct-path facades sit on.

| CapabilityInventory group | Disposition |
|---|---|
| Low-level fetch/paging: `fetch_rows*`, `fetch_rows_ref*` (zero-copy), `fetch_rows_request`/`_ref_response` (speculative prefetch), `for_each_row_ref`, `define_and_fetch_rows_with_columns`, `fetch_cursor`, `scroll_cursor` | **Retained** (perf + REF CURSOR contract). `Rows`/`Query` are sugar *over* these; the primitives stay public. |
| LOB: read/write/trim/create_temp/free_temp (+ `_with_timeout`) | **Retained** (generic `ora_type_num`/`csfrm`; covers BFILE) |
| AQ: `aq_enq_one/deq_one/enq_many/deq_many` + option/props/payload types | **Retained** |
| Objects: `describe_object_type`, `decode_object`, object binds | **Retained** |
| Transactions: `commit`/`rollback`/`transaction_in_progress`, TPC (`tpc_*`), sessionless (`begin/resume/suspend/prepare_*`) | **Retained** |
| Pooling: `Pool`/`BlockingPool`/`PooledConnection`/`BlockingPooledConnection` guards, `PoolStats`, `PoolBackend`, `PoolConfig` builders/getters, `AcquireOptions` builders/getters, getmode+purity constants, live setters, `PoolError` | **Retained** (async-native facade; sync facade is `block_on`; low-level `PoolEngine` is crate-private) |
| Pipelining: `run_pipeline`/`run_pipeline_decoded` + `PipelineRequest` constructors/getters | **Retained** |
| Lifecycle/accessors: `connect`/`close`/`cancel`/`ping`/`change_password`, `CancelHandle`, `release_cursor`/`close_cursor`, `session_id`/`serial_num`/`server_version[_tuple]`/`sdu`/`descriptor`/`identity`/`supports_pipelining`/`supports_oob`/`is_dead` | **Retained** |
| `ConnectOptions` (all knobs: access-token TCPS-required+redacted, edition, wallet/TLS, app_context, proxy_user, sdu, server_type_emon, statement_cache_size) | **Retained** (audit-tidied to builders/getters per W1-T4) |
| DBMS_OUTPUT: `enable_dbms_output`/`read_dbms_output` | **Retained** |
| Arrow (feature): `fetch_all_record_batch[_columnar]`, `fetch_record_batches`, `ArrowFetchOptions`, helpers | **Retained** (facade over the core) |
| Direct-path load: `direct_path_*` + dpl types | **Retained** |
| SODA (feature): full `SodaDatabase`/`SodaCollection`/… facade | **Retained** (facade over execute/fetch) |
| `supplement_json_column_metadata`, fetch-profiling fns | **Retained** |
| `oracledb::protocol` re-export (codecs/constants/OracleNumber/Vector/OsonValue/EasyConnect/ClientIdentity/…) | **Retained** (public surface; ledger/W0-T5 adjudicates any accidental leaks) |

**Execute/query sprawl → families (the only methods that change), nothing lost:**

| Old async method | New family path | Blocking twin |
|---|---|---|
| `execute_query_for_registration` | `Registration::new(sql, registration_id)` → `register_query` | `BlockingConnection::execute_query_for_registration` deprecated the same way. |
| `execute_query` | `Query::new(sql).stream_lobs().prefetch(n)` → `query_with` for row work; `Execute::new(sql)` → `execute_with` for DML/DDL/PLSQL. The compatibility shim returns the raw first-batch `QueryResult` through the same private operation core used by those families. | `BlockingConnection::execute_query` deprecated; W1-T3.8 adds the blocking family mirror. |
| `execute_query_collect` | `Query::new(sql).prefetch(n)` → `query_with`; materialization is the query default. | `BlockingConnection::execute_query_collect` deprecated. |
| `execute_query_with_timeout` | `Query::new(sql).timeout(d)` or `Execute::new(sql).timeout(d)`. | `BlockingConnection::execute_query_with_timeout` deprecated. |
| `execute_query_with_binds` | `query(cx, sql, Params)` / `Query::bind(..)` for rows; `execute(cx, sql, Params)` / `Execute::bind(..)` for DML/DDL/PLSQL. | `BlockingConnection::execute_query_with_binds` deprecated. |
| `execute_query_with_binds_and_timeout` | `Query::bind(..).timeout(d)` or `Execute::bind(..).timeout(d)`. | `BlockingConnection::execute_query_with_binds_and_timeout` deprecated. |
| `query` | Name reused as the async `Rows` family (`query`/`query_with`) with `Params`. | Old blocking `query` returns raw `QueryResult`; deprecated until W1-T3.8 adds the blocking `Rows` mirror. |
| `query_named` | `query(cx, sql, params!{...})` or `Query::bind(params!{...})`. | `BlockingConnection::query_named` deprecated. |
| `query_named_with_timeout` | `Query::bind(params!{...}).timeout(d)`. | `BlockingConnection::query_named_with_timeout` deprecated. |
| `execute_query_with_bind_rows` | `execute_many(cx, sql, BatchRows)` / `Batch::new(sql, rows)`. Query-style raw first-batch compatibility uses the shared private operation core. | `BlockingConnection::execute_query_with_bind_rows` deprecated. |
| `execute_query_with_bind_rows_and_options` | `Batch::raw_options`, `Execute::raw_options`, or `Query` builders depending on operation family. | `BlockingConnection::execute_query_with_bind_rows_and_options` remains as the deprecated 1:1 compatibility twin. |
| `execute_query_with_bind_rows_and_timeout` | `Batch::timeout(d)` or `Query::timeout(d)` as appropriate. | `BlockingConnection::execute_query_with_bind_rows_and_timeout` deprecated. |
| `execute_query_with_bind_rows_options_and_timeout` | `Batch::raw_options(...).timeout(d)`, `Execute::raw_options(...).timeout(d)`, or `Query` builders. | `BlockingConnection::execute_query_with_bind_rows_options_and_timeout` deprecated. |
| Cardinality checks previously done manually | `query_one`, `query_opt`, `query_all`. | W1-T3.8 mirrors these on `BlockingConnection`. |

**24 capability groups covered by the map:**

| ID | Capability | Disposition |
|---|---|---|
| C01 | Describe-only vs collected/materialized result cells | `Query::stream_lobs()` preserves raw describe behavior; default `Query` materializes LOB/JSON/vector cells. |
| C02 | All `ExecuteOptions` knobs | Common knobs are builders; rare/internal combinations remain reachable through `raw_options`. |
| C03 | OUT binds, DML RETURNING, implicit results, REF CURSOR | `ExecuteOutcome`, `ReturningRows`, `OutBinds`, `Rows::cursor`, and retained `fetch_cursor`. |
| C04 | Batch errors and array DML row counts | `Batch::collect_errors`, `Batch::row_counts`, `BatchOutcome::errors`, `BatchOutcome::per_row_counts`. |
| C05 | Low-level fetch, zero-copy borrowed fetch, speculative fetch | Retained public fetch/paging primitives. |
| C06 | Scrollability | `Query::scrollable`, `Rows::scroll`, and retained `scroll_cursor`. |
| C07 | Timeouts | `Query::timeout`, `Execute::timeout`, `Batch::timeout`, `Registration::timeout`; one logical operation deadline. |
| C08 | Named/positional bind ergonomics | `Params`, `IntoBinds`, and `params!` cover positional and named binds. |
| C09 | Typed read/write stack | `FromSql`, `ToSql`, `FromRow`, derive, `Row::get`, and `Rows::into_typed` retained. |
| C10 | Value enum surface | 22 `BindValue` variants and 16 `QueryValue` variants remain covered below. |
| C11 | Continuous Query Notification | `Registration`/`register_query` plus retained subscribe/notify primitives. |
| C12 | LOB and BFILE operations | Retained read/write/trim/create/free APIs and timeout variants. |
| C13 | AQ | Retained enqueue/dequeue APIs and option/property/payload types. |
| C14 | Oracle objects and collections | Retained describe/decode/object bind capabilities. |
| C15 | Local, TPC, and sessionless transactions | Retained transaction APIs. |
| C16 | Pooling | Retained through W1-T7; public facade revised there. |
| C17 | Pipeline execution | Retained `run_pipeline`, `run_pipeline_decoded`, and `PipelineRequest`. |
| C18 | Lifecycle/accessors | Retained connect/close/ping/change-password/accessor/cursor-release APIs. |
| C19 | Cancellation | Retained `cancel`, `CancelHandle`, cancel-on-drop recovery, and W1-T2 state machine. |
| C20 | Error classification | W1-T6 structured errors build on the retained `Error` surface. |
| C21 | DBMS_OUTPUT | Retained `enable_dbms_output` and `read_dbms_output`. |
| C22 | `ConnectOptions` knobs | Retained and accessorized in W1-T4. |
| C23 | Blocking mirror | W1-T3.8 adds a 1:1 blocking family mirror; deprecated blocking old names remain shims until then. |
| C24 | Arrow, direct-path load, SODA, profiling, protocol re-export | Retained facades and `oracledb::protocol` re-export. |

**`ExecuteOptions` knob mapping (13/13):**

| Field | Family surface |
|---|---|
| `batcherrors` | `Batch::collect_errors` or `Batch::raw_options`. |
| `arraydmlrowcounts` | `Batch::row_counts` or `Batch::raw_options`. |
| `parse_only` | `Execute::parse_only` or `Execute::raw_options`. |
| `token_num` | Pipeline internals / `raw_options` compatibility. |
| `cursor_id` | Retained cursor primitives / `raw_options` compatibility. |
| `cache_statement` | Statement-cache behavior remains in the private operation core; `raw_options` preserves override. |
| `scrollable` | `Query::scrollable`. |
| `fetch_orientation` | `Rows::scroll` and retained `scroll_cursor`; `raw_options` preserves exact wire override. |
| `fetch_pos` | `Rows::scroll` and retained `scroll_cursor`; `raw_options` preserves exact wire override. |
| `scroll_operation` | `Rows::scroll` and retained `scroll_cursor`; `raw_options` preserves exact wire override. |
| `suspend_on_success` | `Execute::raw_options` / `Batch::raw_options` for sessionless piggyback. |
| `no_prefetch` | Private refetch/define logic and `raw_options` compatibility. |
| `registration_id` | `Registration::new(sql, registration_id)` / `register_query`; `raw_options` still exact. |

**Value variants retained:**

| Domain | Variants |
|---|---|
| `BindValue` (22 current variants) | `Null`, `TypedNull`, `Output`, `ReturnOutput`, `ObjectOutput`, `ObjectInput`, `Text`, `Raw`, `Lob`, `Number`, `BinaryInteger`, `BinaryDouble`, `BinaryFloat`, `Boolean`, `IntervalDS`, `IntervalYM`, `DateTime`, `Timestamp`, `Array`, `Vector`, `Json`, `Cursor`. |
| `QueryValue` (16 variants) | `Text`, `TextRaw`, `Raw`, `Rowid`, `BinaryDouble`, `IntervalDS`, `IntervalYM`, `Number`, `Boolean`, `Cursor`, `DateTime`, `Object`, `Lob`, `Vector`, `Json`, `Array`. |

---

## 9. Worked examples

```rust
// rows (typed)
let emps: Vec<Emp> = conn.query_all(cx, "select id,name from emp where dept=:1", (40,)).await?;
let one: Emp       = conn.query_one(cx, "select * from emp where id=:id", params!{":id"=>7}).await?.into(); // via FromRow
let mut rows = conn.query_with(cx, Query::new("select * from big").arraysize(NonZeroU32::new(500).expect("non-zero")).timeout(secs(5))).await?;
while rows.next_batch(cx).await? { for r in rows.batch() { /* … */ } }

// dml + OUT/RETURNING
let r = conn.execute(cx, "update emp set sal=sal*1.1 where dept=:d", params!{":d"=>40}).await?;
println!("{} rows", r.rows_affected());

// executemany with batch errors
let out = conn.execute_many_with(cx, Batch::new("insert into t values(:1,:2)", &rows).collect_errors()).await?;
for e in out.errors() { eprintln!("row {} failed: {}", e.row_index(), e); }

// blocking mirror — identical, no cx
let n = BlockingConnection::execute(&mut conn, "delete from t where id=:1", (9,))?.rows_affected();
let rows = BlockingConnection::query(&mut conn, "select * from t where id>:1", (100,))?.collect()?;
```

---

## 10. Freeze dispositions for former implementation deferrals

Every signature-level item below is resolved for the 0.3.0 freeze. Future
helpers are allowed only when they are additive and do not weaken the four
operation-family contract.

| Item | Disposition for 0.3.0 / 1.0 contract |
|---|---|
| `ColumnIndex` | **Finalized.** Keep the sealed `ColumnIndex` trait with exactly `usize` and `&str` impls for `Row::value`, `Row::get`, and `Row::try_get`. This covers index and case-insensitive Oracle column-name lookup without committing to user-defined index implementations. |
| `Params` / `Cow` ergonomics | **Finalized.** `Params<'a>` remains `None`, `Positional(Cow<'a, [BindValue]>)`, and `Named(Cow<'a, [(String, BindValue)]>)`; builder SQL is borrowed by default and owned only through crate-private convenience constructors. Do not add `&dyn ToSql` bind lifetimes. |
| `NonZeroU32` / `nz(..)` helper | **Finalized as no public helper.** `Query::arraysize` takes `NonZeroU32` directly. A public `nz()` wrapper is not part of the frozen contract; callers can use `NonZeroU32::new(..)` or their own local helper. Adding a helper later would be purely additive. |
| `Scroll` shape | **Finalized.** `Scroll` is `#[non_exhaustive]` with `Current`, `Next`, `Prior`, `First`, `Last`, `Absolute(u32)`, and `Relative(u32)`, consumed by `Rows::scroll` and `BlockingRows::scroll`. |
| `query` / `query_with` unification | **Finalized as split.** Keep `query(sql, params)` as the literal 3-argument convenience path and `query_with(Query)` as the builder path. Do not introduce `impl Into<Query>` before the freeze; it would obscure the simple path without removing real complexity. |
| `query_stream -> impl Stream` | **Parked post-1.0.** No `futures::Stream` enters the stable 1.0 surface. The additive streaming/lending work stays in `rust-oracledb-x3s` and the W3-E10 post-1.0 idea backlog. |
| OUT / RETURNING / batch accessors | **Finalized.** `OutBinds::{len,is_empty,values,get,into_values}`, `ReturningRows::{len,is_empty,values,rows_for,into_values}`, `ExecuteOutcome::{rows_affected,last_rowid,out_binds,returning,implicit_results,compilation_warning}`, `BatchOutcome::{rows_affected,per_row_counts,errors,returning}`, and `BatchError::{row_index,code,message}` are the frozen accessor surface. Extra typed convenience accessors remain additive only. |
| Cursor accessors | **Finalized.** `Rows::cursor` and `BlockingRows::cursor` expose the first REF CURSOR / implicit result-set handle; the lower-level `fetch_cursor` family remains the explicit drain path. |
