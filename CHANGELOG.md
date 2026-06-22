# Changelog

All notable changes to the `oracledb` workspace are documented here. The format
is loosely based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and the project follows the SemVer contract described in
[`docs/adr/0002-semver-contract.md`](docs/adr/0002-semver-contract.md).

## [Unreleased]

### Fixed

- **Pool close race**: a `force`-close racing an in-flight connection open
  failure or an unhealthy ping no longer requeues the associated waiter after
  the pool has begun closing. Previously this could leave a closed pool with a
  stale waiter in its queue, blocking clean close finalization. The in-flight
  failure paths now suppress waiter requeue while closing; the close drain owns
  waiter resolution (the awaiting caller is woken with the pool-closed error).
  Found by exhaustive depth-7 model-checking of the pool lifecycle
  (road-to-1.0 W3-E4).
- **`query_one` / `query_opt` cardinality on single-row LONG results**: these no
  longer raise `Error::TooManyRows` for a query that returns exactly one row
  whose column is `LONG` / `LONG RAW`. The per-row LONG define-fetch ignores the
  requested arraysize and returns one row with `more_rows` still set; the
  cardinality check misread that "end not yet confirmed" flag as a second row.
  `query_one` / `query_opt` now fetch ahead (at most one extra round trip, only
  when a single row is in hand with `more_rows` set) to confirm whether a real
  second row follows. Found by the W3-E1.2 live typed round-trip matrix.
- **`execute_many` RETURNING aggregation**: `BatchOutcome::returning().rows_for(bind)`
  now returns one value per affected input row, instead of only the first
  iteration's value. Array DML decodes `RETURNING` once per iteration, so a single
  RETURNING bind arrives as one group per iteration; the curated `BatchOutcome`
  now coalesces groups that share a bind index (single-statement `RETURNING` is
  unaffected — it already arrives as one group per bind). Found by the W3-E7.4
  live e2e suite.
- **`Query::stream_lobs()` over CLOB/NCLOB**: streamed (locator-only) LOB fetches no
  longer fail with `Protocol(TtcDecode("invalid ub8 length"))`. The LOB column decoder
  unconditionally read the `size` (ub8) and `chunk_size` (ub4) fields, but those are
  present only in LOB-prefetch (define-fetch) responses — a plain streamed locator fetch
  omits them, so the decoder misaligned onto the locator's length prefix. The decoder now
  tracks per-cursor LOB-prefetch state and selects the locator-only vs prefetch decode
  shape accordingly (BFILE always uses the locator-only shape). Default LOB
  materialization is unchanged. Found by the W3-E7.4 live e2e suite (rust-oracledb-jbh9).
- **`f32` conversion overflow** (`FromSql for f32`): a finite NUMBER / BINARY_DOUBLE that
  exceeds the `f32` range now returns `ConversionError::OutOfRange` instead of silently
  yielding `inf` (the `f64` path already rejected non-finite). Found by W3-E8.
- **INTERVAL DAY TO SECOND sub-microsecond precision**: interval encoding is now
  nanosecond-native, so a fractional-seconds value with more than 6 significant digits no
  longer truncates on round-trip (notably OSON/JSON `IntervalDS`). `encode_interval_ds`
  became symmetric with the nanosecond-returning decoder. Found by W3-E8.
- **Borrowed-fetch cancel recovery** (`fetch_rows_ref`): a borrowed (zero-copy) fetch
  future dropped mid-read now arms BREAK → drain recovery like the owned fetch path, so the
  next operation on the connection is not desynchronized by a stranded response. Found by
  W3-E8.
- **Borrowed vs owned NUMBER canonicalization**: the borrowed (zero-copy) and owned fetch
  paths now produce identical canonical text for trailing-zero `NUMBER` values. Found by
  W3-E8.
- **DbObject long attribute values**: a DbObject/collection attribute value longer than 252
  bytes is now decoded correctly. The encoder emits the long form as chunked `ub4` segments
  (matching python-oracledb), but the decoder read a single fixed `u32` length, mis-decoding
  such values on fetch; the decoder now consumes the chunked form. Found by W3-E8.
- **Sparse VECTOR validation**: encoding a sparse VECTOR now validates that the index and
  value counts match and that the dimension count fits the `u16` wire field (fail-closed
  instead of silently wrapping at 65 536). Found by W3-E8.
- **AQ dequeue truncation**: a RAW/JSON AQ dequeue whose declared payload-image length
  exceeds the bytes actually present now returns a decode error instead of silently
  returning truncated data. Found by W3-E8.
- **SODA mixed-case columns**: generated SODA SQL now quotes every descriptor column name
  (not only the media-type column), so collections mapped onto case-sensitive mixed-case
  columns work. (SODA is an experimental feature.) Found by W3-E8.

### Added

- **Deterministic concurrency model-checking** (road-to-1.0 Wave-3 qualification):
  DPOR / exhaustive-enumeration test harnesses over the wire cancel/timeout
  recovery path (W3-E3: cancel maps to `Error::Cancelled`, timeout to
  `Error::CallTimeout`, exactly one BREAK + one RESET, recovery ends at a clean
  `Ready` boundary) and the async pool lifecycle (W3-E4: no missed wakeup, FIFO
  fairness, no double-hand-out, force-close drains all waiters). Test-only; no
  public API change.

## [0.3.0] — 2026-06-21

The migration release: it ships the permanent 1.0 query/execute API (the four
operation families) and deprecates the 0.2.x execute/query names, giving
downstream code one minor release to move before the names are removed ahead of
`1.0.0-rc.1`.

See [`docs/MIGRATING-0.3.md`](docs/MIGRATING-0.3.md) for a method-by-method
old → new map with before/after snippets.

### Added

- **Four operation families** as the permanent 1.0 contract, on both
  `Connection` (async) and `BlockingConnection` (blocking):
  - `query` / `query_with` returning a lazy `Rows` (`BlockingRows`) facade, plus
    the cardinality helpers `query_one`, `query_opt`, and `query_all`.
  - `execute` / `execute_with` returning a structured `ExecuteOutcome`
    (`rows_affected`, `last_rowid`, OUT/IN-OUT binds, RETURNING, implicit result
    sets, compilation warning).
  - `execute_many` / `execute_many_with` returning a `BatchOutcome`
    (`rows_affected`, per-row counts, collected batch errors, RETURNING).
  - `register_query` (CQN) returning a `RegistrationOutcome`.
- **Builders**: `Query`, `Execute`, `Batch`, and `Registration`, with
  `bind`, `timeout`, `prefetch`/`arraysize`, `stream_lobs`, `scrollable`,
  `parse_only`, `collect_errors`, `row_counts`, and `raw_options` as applicable.
- **Structured error classification** on `Error`: `kind() -> ErrorKind`,
  `ora_code()` / `oracle_code()`, `is_connection_lost()`, `is_transient()`,
  `retry_hint() -> RetryHint`, `is_retryable()`, and `resource_limit()`.
- **`execute_raw`** on `Connection` and `BlockingConnection`: a low-level raw
  execute primitive returning the unprojected `QueryResult`, the execute-side
  counterpart to the retained `fetch_rows*` / `define_and_fetch_rows_with_columns`
  / `scroll_cursor` / `fetch_cursor` primitives. For statement-type-agnostic
  dispatch, parse-only describe, or per-bind-row OUT/RETURNING aggregation; the
  four families remain the ergonomic surface for ordinary code.

### Changed

- **Single operation deadline for timeouts.** The new `timeout(Duration)`
  builders translate the duration **once** into a single absolute deadline that
  spans the initial call and every `Rows::next_batch` / `Rows::collect`
  continuation and LOB chunk of the one logical operation, instead of re-arming a
  per-round-trip `timeout_ms`. An N-batch fetch is now bounded by the budget you
  set rather than up to N× it. On expiry the driver still performs
  BREAK → drain → `Error::CallTimeout` and leaves the session `Ready`.
- Several error and value enums (e.g. `ErrorKind`, `BindValue`, `QueryValue`)
  are `#[non_exhaustive]`; match them with a wildcard arm.

### Deprecated

All of the following are `#[deprecated(since = "0.3.0")]` on **both**
`Connection` and `BlockingConnection`, and are scheduled for removal before
`1.0.0-rc.1` (road-to-1.0 W4-T1). Each delegates to the same private operation
core as its replacement, so behavior is unchanged in 0.3.0.

- `execute_query` → `query` / `query_with` (rows) or `execute` / `execute_with`
  (DML/DDL/PL/SQL).
- `execute_query_collect` → `query` / `query_with` (LOB/JSON/vector cells are
  materialized by default; opt out with `Query::stream_lobs()`).
- `execute_query_with_timeout` → `Query::timeout` / `Execute::timeout`.
- `execute_query_with_binds` → `query` / `execute` with a `Params` argument.
- `execute_query_with_binds_and_timeout` → `Query`/`Execute` `bind(..).timeout(..)`.
- `query_named` → `query(cx, sql, params!{ ... })`.
- `query_named_with_timeout` → `Query::new(sql).bind(params!{ ... }).timeout(..)`.
- `execute_query_with_bind_rows` → `execute_many` / `Batch::new`.
- `execute_query_with_bind_rows_and_options` → `Batch::raw_options` (or
  `Execute::raw_options` / `Query` builders, per family).
- `execute_query_with_bind_rows_and_timeout` → `Batch::timeout` (or
  `Query::timeout`).
- `execute_query_with_bind_rows_options_and_timeout` →
  `Batch::raw_options(..).timeout(..)` (or `Execute::raw_options(..).timeout(..)`).
- `execute_query_for_registration` → `register_query` with
  `Registration::new(sql, registration_id)`.

The low-level fetch/paging primitives (`fetch_rows*`,
`define_and_fetch_rows_with_columns`, `scroll_cursor`, `fetch_cursor`, …) and the
LOB/AQ/objects/transactions/pooling/pipeline/SODA/Arrow/direct-path/CQN surfaces
are **retained** — only the execute/query sprawl is consolidated. See
[`docs/API_DESIGN.md` §8](docs/API_DESIGN.md) for the full "nothing lost" map.

### Fixed

Closed all 103 differential-conformance gaps against python-oracledb's own
thin-mode suite — the full suite now diffs to **0 regressions** vs the live
python-oracledb baseline (2578/2578). Three root causes, all pre-existing on
`main` and surfaced by the first clean full conformance run:

- **Bind-shape validation** (66 tests): the raw `execute` path no longer
  enforces SQL placeholder *occurrence* count (it cannot know whether binds were
  supplied by name or by position). A repeated named bind (`:v` used N times) is
  satisfied by a single value — matching python-oracledb — and `parse()` (which
  supplies no binds) is no longer rejected. Positional-count validation is kept
  in the style-aware `Params::Positional` path, and the ragged-batch-row check is
  preserved.
- **Direct path load** (36 tests): the default `batch_size` sentinel
  (`2**32 - 1`, "all rows in one batch") is no longer misread as a row count and
  no longer trips the protocol `max_batch_rows` limit. `batch_size` is a chunking
  upper bound (clamped to the data length), exactly as in python-oracledb.
- **Pool timed-wait acquire** (1 test): a `POOL_GETMODE_TIMEDWAIT` acquire now
  reliably honors its `wait_timeout` and raises `DPY-4005`, via an explicit
  deadline that does not depend on the async runtime's timer wheel; and pool
  teardown no longer risks a deadlock when a finalizer drops the pool while
  holding the embedder's VM lock (e.g. the Python GIL).
