# K10 Owned Row Stream Design and Cost

Status: tracked follow-up, not in 0.8.2.

## Goal

Expose an owned-row paged stream over an open query cursor:

```rust
Stream<Item = oracledb::Result<Vec<Option<QueryValue>>>>
```

The stream must preserve current duplicate-column continuation semantics,
define-fetch behavior for LOB/JSON/VECTOR rows, cursor release, and cancellation
cleanup. It must not require the caller to hold `&mut Connection` across each
yielded row.

Today the driver already has two adjacent surfaces:

- `Rows<'conn>`: lazy owned-row paging over `fetch_rows_with_columns`, but it
  stores `&'conn mut Connection` for the lifetime of the row facade.
- `Connection::for_each_row_ref`: callback streaming over borrowed rows, with
  speculative fetch overlap and cancellation cleanup, but no public `Stream`
  type and no owned row item.

K10 fills a different ergonomic slot: caller-controlled row-by-row pull with
owned values.

## Recommended API Shape

Prefer an owning stream that takes `Connection` by value and returns it when the
stream is drained or explicitly recovered:

```rust
use futures_core::Stream;
use oracledb::protocol::thin::QueryValue;

pub struct OwnedRowStream {
    connection: Option<Connection>,
    state: OwnedRowStreamState,
}

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

High-level state:

```rust
enum OwnedRowStreamState {
    PendingFirst,
    Buffered {
        columns: Arc<[ColumnMetadata]>,
        cursor_id: u32,
        more_rows: bool,
        previous_row: Option<Vec<Option<QueryValue>>>,
        batch: VecDeque<Vec<Option<QueryValue>>>,
    },
    Fetching(BoxFuture<'static, Result<(Connection, QueryResult)>>),
    Done,
    Poisoned,
}
```

The constructor executes the query and seeds `batch` from the first page, like
`Rows::from_result`. `poll_next` returns buffered rows without touching the
connection. When the buffer is empty and `more_rows` is true, it moves the owned
`Connection` into an internal future that runs the next
`fetch_rows_with_columns` call and returns both the connection and the fetched
page. That move-out/move-back pattern avoids a self-referential future and keeps
the API `unsafe`-free.

## Stream Trait Options

### `futures-core::Stream` (recommended for a real `Stream`)

Add:

```toml
futures-core = { version = "0.3", default-features = false }
```

Pros:

- Standard Rust async ecosystem trait.
- Dependency is tiny and trait-only; no executor/runtime lock-in.
- Lets users compose with `StreamExt` from `futures-util` if they choose.
- The public type can implement the common trait while still internally using
  asupersync I/O and `Cx`.

Cons:

- New public dependency family and public trait in the API surface.
- `poll_next` does not take `&Cx`; the stream must either clone/store the
  `Cx` captured at construction or require an asupersync ambient context via
  `Cx::current()`. Capturing the constructor `Cx` clone is more explicit and
  avoids ambient-authority surprises.
- The implementation needs a pinned in-flight future state and careful
  cancellation/drop cleanup.

### Asupersync-native pull surface

Shape:

```rust
pub struct OwnedRowStream { /* no Stream impl */ }

impl OwnedRowStream {
    pub async fn next_row(
        &mut self,
        cx: &asupersync::Cx,
    ) -> Result<Option<Vec<Option<QueryValue>>>>;
}
```

Pros:

- Matches existing driver style (`Rows::next_batch`, `RecordBatchFetch::next_batch`,
  `LobReader::read_chunk`).
- No new dependency and no `poll_next` self-reference risk.
- Clean `&mut Connection` avoidance is easy if the stream owns the connection.

Cons:

- It is not literally `Stream<Item = ...>`.
- Users cannot directly use standard stream combinators.
- It satisfies the ergonomic pull need but not libraries that require
  `futures_core::Stream`.

### Custom poll-based trait

Shape:

```rust
pub trait RowStream {
    fn poll_next_row(
        self: Pin<&mut Self>,
        cx: &mut core::task::Context<'_>,
    ) -> Poll<Result<Option<Vec<Option<QueryValue>>>>>;
}
```

Pros:

- No external dependency.
- Keeps a poll-based shape.

Cons:

- Reinvents `futures_core::Stream` with worse ecosystem compatibility.
- Still has the same in-flight future and connection move-out complexity.
- Creates a public trait the driver must support long-term.

Recommendation: use `futures-core::Stream` only if K10 explicitly wants
ecosystem stream compatibility. If the main need is driver-local ergonomics,
prefer an asupersync-native `next_row(&mut self, &Cx)` pull type.

## Public API and Ledger Impact

This is additive, not breaking, if implemented as new items:

- New public type: `OwnedRowStream`.
- New methods on `Connection`: `into_row_stream` and/or `into_query_stream`.
- Potential new method on `OwnedRowStream`: `into_connection`.
- Potential new public dependency: `futures-core`.

No existing method signature needs to change. `Rows<'conn>`,
`BlockingRows<'conn>`, `fetch_rows_with_columns`, and `for_each_row_ref` can
remain unchanged.

Ledger/baseline impact:

- `docs/API_LEDGER.md` needs entries for the new type and methods.
- `docs/baseline/public_api/*.txt` and `docs/baseline/public_api_profiles.tsv`
  will change after `scripts/gen_baseline.sh`.
- `docs/baseline/workspace_packages.tsv` and dependency provenance may change if
  `futures-core` is added.
- Because this changes public API and dependencies, it belongs in a deliberate
  release cut, not in a test-gate-only patch.

## Avoiding `&mut Connection` Across the Yield

The key rule is: do not store `&'conn mut Connection` inside the stream. That is
what `Rows<'conn>` does today, and it prevents the caller from regaining the
connection while the row facade exists.

The owning design stores `Option<Connection>` instead:

1. Buffered rows are yielded from `VecDeque<Vec<Option<QueryValue>>>`; no
   connection borrow is active.
2. When another page is needed, `poll_next` takes the owned `Connection` out of
   the stream state and moves it into the in-flight fetch future.
3. The future calls `fetch_rows_with_columns(cx, cursor_id, arraysize, columns,
   previous_row)` internally.
4. On completion, the future returns `(Connection, QueryResult)` so the stream
   can put the connection back and buffer the page's rows.
5. The yielded item is an owned row, not a borrow into the response buffer.

This keeps every `&mut Connection` borrow inside one async operation. The caller
never receives a row tied to the connection borrow.

Drop/cancellation policy:

- If dropped while idle, release the cursor if the connection is still present.
- If dropped while a fetch future owns the connection, the future drop must use
  the same break/drain recovery machinery that protects `fetch_rows_ref` and
  `for_each_row_ref`. If that cannot be guaranteed, mark the connection poisoned
  and do not return it through `into_connection`.
- `into_connection` should return `Err(Error::ConnectionClosed(...))` or a
  dedicated runtime error if the stream is poisoned by a dropped in-flight
  operation.

## Cost

Estimated implementation size:

- 1 new module, likely `crates/oracledb/src/row_stream.rs`.
- 1 public re-export in `crates/oracledb/src/lib.rs`.
- 2 constructor methods on `Connection`.
- Optional `futures-core` dependency if the real `Stream` trait is selected.
- Unit tests around buffering, cursor release, duplicate-column seed handling,
  and in-flight drop behavior.
- Live/cassette tests for multi-page fetch and reuse after stream drop.

Risk:

- Medium if implemented as asupersync-native pull API.
- Medium-high if implemented as `futures_core::Stream`, because the `poll_next`
  state machine needs to move the connection through an in-flight future without
  self-references and must preserve cancellation cleanup.

This is not a small 0.8.2 patch.

## Test Plan

Offline/unit:

- Construct a synthetic first `QueryResult` and assert buffered rows are yielded
  one at a time before a fetch is attempted.
- Verify duplicate-column continuation seed is the last yielded row from the
  previous page.
- Verify empty pages with `more_rows = true` are skipped without yielding bogus
  empty rows.
- Verify the stream releases a fully drained cursor exactly once.
- Verify `into_connection` works after full drain and after explicit early stop
  when no fetch is in flight.

Cassette/scripted transport:

- Multi-page query: first page from execute, second and third from
  `fetch_rows_with_columns`; output must equal `query_all`.
- Define-fetch query: CLOB/JSON/VECTOR shape must establish client-side define
  before streaming pages.
- Connection reuse after early drop while idle: next query on the returned
  connection decodes correctly.
- Drop during in-flight fetch: next operation breaks/drains or the stream refuses
  to return a reusable connection.

Live:

- `select level from dual connect by level <= N` with `arraysize = 2`, verifying
  row order and cardinality.
- Wide-row duplicate compression case mirroring existing borrowed-fetch
  regressions.
- LOB/JSON/VECTOR result set where first execute returns no rows and a
  define-fetch is required.

Compatibility:

- `cargo public-api` diff is additive only.
- `cargo semver-checks check-release -p oracledb` permits the additive surface.
- Feature matrix still passes with and without optional integration features.
