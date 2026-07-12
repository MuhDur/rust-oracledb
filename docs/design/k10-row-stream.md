# K10 Owned Row Stream Design and Delivery Record

Status: implemented in workspace version 0.8.3; public since
[v0.8.2](https://github.com/MuhDur/rust-oracledb/releases/tag/v0.8.2).

The public API landed in
[06244bf](https://github.com/MuhDur/rust-oracledb/commit/06244bfa4c8aab2efb62bfdbd8793dc1e089bd02),
with its release ledger/baseline in
[73231e7](https://github.com/MuhDur/rust-oracledb/commit/73231e710c8262d0b415903c1a8281b03d3c0756)
and tracing-profile correction in
[07f12ef](https://github.com/MuhDur/rust-oracledb/commit/07f12efdea8f4b056efdf0a404a8d2db4e9fe959).
This record distinguishes that tagged implementation from hardening that landed
on the post-release branch.

## Delivered contract

K10 exposes a paged stream of owned Oracle values over one open query cursor:

```rust
Stream<Item = oracledb::Result<Vec<Option<QueryValue>>>>
```

It occupies the ergonomic space between the borrowing `Rows<'conn>` facade and
the callback-oriented `Connection::for_each_row_ref`: callers pull owned rows
with standard stream machinery without holding `&mut Connection` across a
yield. The implementation preserves duplicate-column continuation state,
define-fetch handling for LOB/JSON/VECTOR rows, forward-only cursor lifecycle,
and eager `query_all` result parity.

## Shipped public API

```rust
use futures_core::Stream;
use oracledb::protocol::thin::{ColumnMetadata, QueryValue};

pub struct OwnedRowStream { /* private state */ }

impl Connection {
    pub async fn into_row_stream<'q>(
        self,
        cx: &asupersync::Cx,
        query: Query<'q>,
    ) -> Result<OwnedRowStream>;

    pub async fn into_query_stream<'p>(
        self,
        cx: &asupersync::Cx,
        sql: &str,
        params: impl Into<Params<'p>>,
    ) -> Result<OwnedRowStream>;
}

impl OwnedRowStream {
    pub fn columns(&self) -> &[ColumnMetadata];
    pub fn cursor(&self) -> Option<&Cursor>;
    pub fn into_connection(self) -> Result<Connection>;
}

impl Stream for OwnedRowStream {
    type Item = Result<Vec<Option<QueryValue>>>;
}
```

Both constructors consume the `Connection`. They execute and decode the first
page before returning the stream, which makes column metadata immediately
available. `Query::scrollable` is deliberately ignored because this stream is
forward-only. A constructor failure drops the consumed connection rather than
returning a partially initialized stream.

## Architecture shipped in v0.8.2

The private state machine has four states:

```rust
enum OwnedRowStreamState {
    Buffered(VecDeque<OwnedRow>),
    Fetching(FetchFuture),
    Done,
    Poisoned,
}
```

The stream stores `Option<Connection>` rather than a connection borrow:

1. Buffered rows are yielded without touching the connection.
2. When another page is needed, `poll_next` moves the owned connection into one
   boxed continuation-fetch future.
3. The future returns `(Connection, Result<QueryResult>)`, allowing the state
   machine to restore the connection before yielding the next row or error.
4. The last row from the most recent nonempty page remains the duplicate-column
   seed; an empty continuation page cannot erase it.
5. Re-described cursor metadata and a nonzero replacement cursor id are adopted
   before subsequent pages.

The move-out/move-back design avoids self-referential futures and pin projection.
`OwnedRowStream` remains `Unpin`, and the repository-wide
`#![forbid(unsafe_code)]` invariant holds.

The tagged implementation shared one `QueryDeadline` across the initial execute
and define-fetch bootstrap, but stored a raw timeout duration in the stream and
created a fresh deadline for every continuation page. It also routed a deadline
expiry through BREAK-and-drain recovery without distinguishing whether the
database future had actually been polled. Those were post-release defects, not
part of the intended logical-query deadline contract; both are corrected below.

## Stream-trait decision

K10 selected `futures_core::Stream` and added the trait-only direct dependency:

```toml
futures-core = { version = "0.3", default-features = false }
```

This gives callers standard ecosystem composition without selecting an executor
or pulling `futures-util` into the driver. The constructor `Cx` is cloned into
the stream instead of relying on ambient authority during later polls.

The rejected alternatives were an asupersync-specific `next_row(&Cx)` method
and a custom poll trait. The former would not satisfy libraries expecting
`Stream`; the latter would duplicate the standard trait while retaining the
same state-machine complexity.

The boxed fetch future intentionally has no `Send` bound. With the `tracing`
feature, the fetch path holds a non-`Send` entered-span guard across an await.
The stream is designed for one asupersync lane, so requiring `Send` would reject
a supported feature profile without providing a lifecycle benefit.

## Cursor, drop, and recovery policy at v0.8.2

- Full drain releases the open cursor exactly once and leaves the connection
  available through `into_connection`.
- Early `into_connection` while rows are buffered releases the cursor and
  returns the connection for reuse.
- Idle drop releases the cursor if the stream still owns the connection.
- While a continuation fetch is pending, that future owns the connection. If
  the stream is dropped or consumed then, the future and connection are dropped
  together; `into_connection` fails with `Error::ConnectionClosed` rather than
  claiming that an uncertain connection is reusable.
- A continuation error is yielded once and the stream then terminates. The
  tagged implementation zeroed only the stream cursor id; it did not retire the
  failed cursor from the connection cache and registries. That post-release
  defect is corrected below.

## Post-release hardening on current main

The public API is unchanged, but three verified fixes strengthened the runtime
contract after the v0.8.2 tag:

- [0708550](https://github.com/MuhDur/rust-oracledb/commit/0708550) stores and
  reuses one absolute `QueryDeadline` across initial execute, define-fetch, and
  every continuation page. Consumer pauses consume the same logical-query
  budget; continuation fetches no longer rearm it.
- [b16d42d](https://github.com/MuhDur/rust-oracledb/commit/b16d42d) distinguishes
  expiry before the database future starts from an in-flight expiry. The former
  returns locally with no wire traffic; only the latter performs BREAK/reset and
  drain recovery.
- [02eef5e](https://github.com/MuhDur/rust-oracledb/commit/02eef5e) retires a
  cursor after a continuation or define/fetch error: statement-cache,
  in-use/copied, metadata, and LOB-prefetch state is cleared and one close
  piggyback is queued before the recovered connection can be reused. A
  cancellation proven to occur before operation start still releases the valid
  cached cursor instead.

## Public API and release artifacts

K10 was additive in 0.8.2:

- `OwnedRowStream` and its `Stream`, `Debug`, and `Drop` implementations are
  recorded in `docs/API_LEDGER.md`.
- `Connection::into_row_stream` and `into_query_stream` are present in every
  public-API feature baseline.
- The two owning async constructors are documented exceptions to the
  async/blocking parity rule; blocking callers retain eager `query_all`.
- `futures-core` added a direct dependency edge but no new transitive package,
  because it was already present through asupersync.

## Validation delivered

The release implementation landed with seven offline state-machine tests and
six live tests covering eager parity, small-page continuation, empty results and
reuse, midstream Oracle errors, early-stop reuse, and drop without a hang. The
implementation commit records separate manual live runs on Oracle Free 23ai and
XE 21c. The release also ran the broader five-lane driver matrix, but that matrix
did not invoke the K10 `live_owned_row_stream` suite; it is separate release
evidence rather than K10 behavior coverage.

The 0.8.2 ThreadSanitizer campaign also included the stream lifecycle in its
instrumented offline and live corpora. Public-API/ledger, single-path, feature
matrix, formatting, clippy, dependency, and `forbid(unsafe_code)` gates were
regenerated or rerun for the release tag.

## Residual constraints

- This is a forward-only stream; use the borrowing scroll APIs for cursor
  repositioning.
- The first execute happens before the stream is returned, so K10 is row-streaming
  rather than a lazy-query-start abstraction.
- The in-flight stream state is single-lane and not promised to be `Send`.
- Values are owned per row, trading allocation for freedom from a connection or
  response-buffer borrow.
