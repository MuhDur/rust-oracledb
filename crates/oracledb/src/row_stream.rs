//! Owning, row-by-row query stream (K10).
//!
//! [`OwnedRowStream`] is an [`futures_core::Stream`] of owned query rows that
//! takes the [`Connection`] **by value** and hands it back when the stream is
//! drained or explicitly recovered with [`OwnedRowStream::into_connection`].
//!
//! Unlike [`Rows`](crate::Rows) — which borrows `&mut Connection` for the
//! lifetime of the row facade — an `OwnedRowStream` never holds a connection
//! borrow across a yielded row. Buffered rows are served straight from an
//! in-memory page; when the buffer empties and the server has more rows, the
//! stream moves the owned connection into a single in-flight fetch future and
//! moves it back out when the page arrives. That move-out / move-back pattern
//! keeps every `&mut Connection` borrow inside one async fetch and needs no
//! self-referential future, so the whole path stays `#![forbid(unsafe_code)]`.
//!
//! The duplicate-column continuation seed (the last row of the most recent
//! non-empty page) and all define-fetch / LOB-prefetch decode behavior match
//! the eager [`Connection::query`] path exactly: a fully streamed result is
//! byte-identical to [`Connection::query_all`].
//!
//! ```no_run
//! use std::future::poll_fn;
//! use std::pin::Pin;
//!
//! use futures_core::Stream;
//! use oracledb::protocol::thin::QueryValue;
//! use oracledb::{ConnectOptions, Connection};
//!
//! # async fn demo(cx: &asupersync::Cx, options: ConnectOptions) -> oracledb::Result<()> {
//! let conn = Connection::connect(cx, options).await?;
//! let mut stream = conn
//!     .into_query_stream(cx, "select level from dual connect by level <= 5", ())
//!     .await?;
//!
//! // `OwnedRowStream` is `Unpin`, so `Pin::new(&mut stream)` needs no `unsafe`.
//! // `StreamExt::next` from `futures-util` would do the same; pulling by hand
//! // keeps this example free of an extra dependency and gives a real waker.
//! let mut collected: Vec<Vec<Option<QueryValue>>> = Vec::new();
//! while let Some(row) = poll_fn(|task_cx| Pin::new(&mut stream).poll_next(task_cx)).await {
//!     collected.push(row?);
//! }
//!
//! // The stream is drained; take the connection back for reuse.
//! let conn = stream.into_connection()?;
//! conn.close(cx).await?;
//! # Ok(())
//! # }
//! ```

use std::collections::VecDeque;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use asupersync::Cx;
use futures_core::Stream;
use oracledb_protocol::thin::{ColumnMetadata, ExecuteOptions, QueryResult, QueryValue};

use crate::request::{DeadlineExpiry, QueryDeadline};
use crate::rows::first_cursor_from_result;
use crate::{columns_require_define, Connection, Cursor, Error, Params, Query, Result};

/// One owned query row: a value (or SQL `NULL`) per column, in column order.
type OwnedRow = Vec<Option<QueryValue>>;

/// The connection plus the outcome of one continuation fetch. The connection is
/// always returned (whether the fetch succeeded or failed) so the stream can put
/// it back; only a dropped in-flight future loses it (which poisons the stream).
type FetchOutcome = (Connection, Result<QueryResult>);

/// Boxed, `'static` in-flight fetch future. `'static` because it owns
/// everything it touches (the connection, a cloned `Cx`, the columns `Arc`, and
/// the duplicate-column seed), so it holds no borrow of the stream.
///
/// NOT `+ Send`: the fetch path transitively enters an observability span
/// (`ensure_clean_before_request`'s `obs_span!`) whose `tracing::EnteredSpan`
/// guard is held across an await, so under `--features tracing` the future is
/// `!Send` (the driver's other async methods share this shape but are never
/// boxed-as-`Send`, so it was never forced). `OwnedRowStream` is driven on a
/// single asupersync lane (current-thread executor), so `Send` is not required.
type FetchFuture = Pin<Box<dyn Future<Output = FetchOutcome>>>;

/// Internal poll state of an [`OwnedRowStream`].
enum OwnedRowStreamState {
    /// Rows ready to yield from the current page (may be empty when a further
    /// fetch is required).
    Buffered(VecDeque<OwnedRow>),
    /// A continuation fetch owns the connection until its page arrives.
    Fetching(FetchFuture),
    /// The cursor is fully drained (or a fetch failed); the connection, if any,
    /// is back in [`OwnedRowStream::connection`].
    Done,
    /// A fetch future was dropped while it owned the connection, or a fetch was
    /// requested with no connection in hand. The connection cannot be recovered.
    Poisoned,
}

/// A [`Stream`] of owned query rows that owns its [`Connection`] and returns it
/// when drained. Constructed with [`Connection::into_row_stream`] /
/// [`Connection::into_query_stream`].
///
/// See the [module docs](self) for the owning-connection design and a usage
/// example.
#[must_use = "an OwnedRowStream holds the connection until drained or recovered with into_connection()"]
pub struct OwnedRowStream {
    /// `Some` whenever a fetch future does not currently own the connection.
    /// `None` only while a fetch is in flight ([`OwnedRowStreamState::Fetching`])
    /// or after that future was dropped ([`OwnedRowStreamState::Poisoned`]).
    connection: Option<Connection>,
    /// The `Cx` captured at construction, cloned into each continuation fetch.
    /// Its budget / deadline governs the whole stream, mirroring how one
    /// [`Connection::query`] shares a single `Cx` across its paging fetches.
    cx: Cx,
    /// Column metadata for the open cursor. Refreshed if a mid-paging DESCRIBE
    /// re-shapes the cursor (the type-change refetch path).
    columns: Arc<[ColumnMetadata]>,
    /// First REF CURSOR seen in the result set, if any (parity with
    /// [`Rows::cursor`](crate::Rows::cursor)).
    cursor: Option<Cursor>,
    /// Open server cursor id; `0` once released.
    cursor_id: u32,
    /// Rows-per-fetch for continuation pages.
    arraysize: u32,
    /// One absolute deadline for the whole logical query, shared by the first
    /// execute and every continuation page.
    deadline: QueryDeadline,
    /// Whether the server has more rows beyond the current page.
    more_rows: bool,
    /// Duplicate-column continuation seed: the last row of the most recent
    /// non-empty page. The server compresses a page's first row against the
    /// previous page's last row (bit-vector duplicate columns), so this must
    /// survive independently of the buffer — draining the buffer must not lose
    /// the seed, and an empty page must keep the previous one.
    previous_row: Option<OwnedRow>,
    state: OwnedRowStreamState,
}

impl OwnedRowStream {
    /// Seed the stream from the first page (the EXECUTE + first fetch result),
    /// exactly as [`Rows::from_result`](crate::Rows) does for the borrowing
    /// facade. `connection` is `Some` for real streams and `None` only in
    /// offline unit tests that never drive a continuation fetch.
    fn from_first_page(
        connection: Option<Connection>,
        cx: Cx,
        arraysize: u32,
        deadline: QueryDeadline,
        result: QueryResult,
    ) -> Self {
        let cursor_id = result.cursor_id;
        let more_rows = result.more_rows;
        let cursor = first_cursor_from_result(&result);
        let columns: Arc<[ColumnMetadata]> = Arc::from(result.columns.into_boxed_slice());
        let batch: VecDeque<OwnedRow> = result.rows.into_iter().collect();
        let previous_row = batch.back().cloned();
        Self {
            connection,
            cx,
            columns,
            cursor,
            cursor_id,
            arraysize,
            deadline,
            more_rows,
            previous_row,
            state: OwnedRowStreamState::Buffered(batch),
        }
    }

    /// Column metadata of the streamed result set.
    pub fn columns(&self) -> &[ColumnMetadata] {
        &self.columns
    }

    /// The first REF CURSOR carried by the result set, if any — parity with
    /// [`Rows::cursor`](crate::Rows::cursor).
    pub fn cursor(&self) -> Option<&Cursor> {
        self.cursor.as_ref()
    }

    /// Recover the owned [`Connection`], releasing the server cursor first.
    ///
    /// Works after a full drain and after an early stop while rows are still
    /// buffered (no fetch in flight). Returns [`Error::ConnectionClosed`] if the
    /// stream was **poisoned** — a continuation fetch future was dropped while it
    /// owned the connection, so the connection cannot be handed back cleanly.
    pub fn into_connection(mut self) -> Result<Connection> {
        match self.connection.take() {
            Some(mut connection) => {
                if self.cursor_id != 0 {
                    connection.release_cursor(self.cursor_id);
                    self.cursor_id = 0;
                }
                Ok(connection)
            }
            None => Err(Error::ConnectionClosed(
                "owned row stream was poisoned by a dropped in-flight fetch; \
                 the connection cannot be recovered"
                    .to_string(),
            )),
        }
    }

    /// Release the open server cursor if the connection is in hand. A no-op once
    /// the cursor is closed or while a fetch owns the connection.
    fn release_cursor_if_open(&mut self) {
        if self.cursor_id != 0 {
            if let Some(connection) = &mut self.connection {
                connection.release_cursor(self.cursor_id);
            }
            self.cursor_id = 0;
            self.more_rows = false;
        }
    }

    /// Fold a fetched continuation page into the stream state, mirroring
    /// [`Rows::apply_result`](crate::Rows): adopt a re-described cursor
    /// id/columns, refresh `more_rows`, keep the first REF CURSOR, and carry the
    /// duplicate-column seed forward (an empty page keeps the previous seed).
    fn apply_page(&mut self, result: QueryResult) {
        if self.cursor.is_none() {
            if let Some(cursor) = first_cursor_from_result(&result) {
                self.cursor = Some(cursor);
            }
        }
        if result.cursor_id != 0 {
            self.cursor_id = result.cursor_id;
        }
        self.more_rows = result.more_rows;
        let columns = result.columns;
        if !columns.is_empty() {
            self.columns = Arc::from(columns.into_boxed_slice());
        }
        let batch: VecDeque<OwnedRow> = result.rows.into_iter().collect();
        if let Some(last) = batch.back() {
            self.previous_row = Some(last.clone());
        }
        self.state = OwnedRowStreamState::Buffered(batch);
    }
}

/// Build the `'static` in-flight fetch future that owns the connection for one
/// continuation page and returns it (plus the page or the error) when done.
///
/// The query's one absolute [`QueryDeadline`] is reused for every page; a
/// timed-out fetch drains the wire with the same
/// `recover_from_call_timeout` machinery [`Rows::next_batch`](crate::Rows) uses,
/// so the returned connection is left clean (or marked dead) rather than desynced.
fn build_fetch_future(
    mut connection: Connection,
    cx: Cx,
    cursor_id: u32,
    arraysize: u32,
    deadline: QueryDeadline,
    columns: Arc<[ColumnMetadata]>,
    previous_row: Option<OwnedRow>,
) -> FetchFuture {
    Box::pin(async move {
        let outcome = match deadline
            .run(connection.fetch_rows_with_columns(
                &cx,
                cursor_id,
                arraysize,
                &columns,
                previous_row.as_deref(),
            ))
            .await
        {
            Ok(result) => result,
            Err(DeadlineExpiry::BeforeStart) => {
                connection.release_cursor(cursor_id);
                connection.reject_before_operation_start(&cx, deadline.timeout_ms())
            }
            Err(DeadlineExpiry::InFlight) => {
                connection
                    .recover_from_call_timeout(&cx, deadline.timeout_ms())
                    .await
            }
        };
        (connection, outcome)
    })
}

impl Stream for OwnedRowStream {
    type Item = Result<OwnedRow>;

    fn poll_next(self: Pin<&mut Self>, task_cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        // Every field is `Unpin` (the in-flight future is boxed), so the stream
        // is `Unpin` and we can drive it through a plain `&mut Self` — no pin
        // projection, no self-reference.
        let this = self.get_mut();
        loop {
            // Take the state out behind a `Poisoned` placeholder so the arms can
            // freely move the buffer / future and reassign without borrow-check
            // gymnastics; every arm restores a real state before returning.
            match std::mem::replace(&mut this.state, OwnedRowStreamState::Poisoned) {
                OwnedRowStreamState::Buffered(mut batch) => {
                    if let Some(row) = batch.pop_front() {
                        this.state = OwnedRowStreamState::Buffered(batch);
                        return Poll::Ready(Some(Ok(row)));
                    }
                    // Buffer drained: end of stream, or fetch the next page.
                    if !this.more_rows || this.cursor_id == 0 {
                        this.release_cursor_if_open();
                        this.state = OwnedRowStreamState::Done;
                        return Poll::Ready(None);
                    }
                    let Some(connection) = this.connection.take() else {
                        // A page is needed but no connection is in hand: the
                        // stream is unusable. (Not reachable through the public
                        // API; guards the offline test constructor.)
                        this.state = OwnedRowStreamState::Poisoned;
                        return Poll::Ready(Some(Err(Error::ConnectionClosed(
                            "owned row stream needs another page but holds no connection"
                                .to_string(),
                        ))));
                    };
                    let future = build_fetch_future(
                        connection,
                        this.cx.clone(),
                        this.cursor_id,
                        this.arraysize,
                        this.deadline,
                        Arc::clone(&this.columns),
                        this.previous_row.clone(),
                    );
                    this.state = OwnedRowStreamState::Fetching(future);
                    // Loop to poll the freshly-armed fetch future.
                }
                OwnedRowStreamState::Fetching(mut future) => {
                    match future.as_mut().poll(task_cx) {
                        Poll::Pending => {
                            this.state = OwnedRowStreamState::Fetching(future);
                            return Poll::Pending;
                        }
                        Poll::Ready((connection, outcome)) => {
                            this.connection = Some(connection);
                            match outcome {
                                Ok(result) => {
                                    // Buffer the page and carry the seed; loop to
                                    // yield it (an empty page with more rows just
                                    // fetches again — no bogus empty row escapes).
                                    this.apply_page(result);
                                }
                                Err(err) => {
                                    // The connection is back but the cursor may be
                                    // invalid; stop cleanly and surface the error.
                                    this.cursor_id = 0;
                                    this.more_rows = false;
                                    this.state = OwnedRowStreamState::Done;
                                    return Poll::Ready(Some(Err(err)));
                                }
                            }
                        }
                    }
                }
                OwnedRowStreamState::Done => {
                    this.state = OwnedRowStreamState::Done;
                    return Poll::Ready(None);
                }
                OwnedRowStreamState::Poisoned => {
                    this.state = OwnedRowStreamState::Poisoned;
                    return Poll::Ready(None);
                }
            }
        }
    }
}

impl Drop for OwnedRowStream {
    fn drop(&mut self) {
        // Idle drop: release the server cursor if we still hold the connection.
        // If a fetch future owns the connection (Fetching state), the connection
        // drops with the future, closing the socket — nothing to release here.
        self.release_cursor_if_open();
    }
}

impl std::fmt::Debug for OwnedRowStream {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let state = match &self.state {
            OwnedRowStreamState::Buffered(batch) => {
                return f
                    .debug_struct("OwnedRowStream")
                    .field("has_connection", &self.connection.is_some())
                    .field("cursor_id", &self.cursor_id)
                    .field("more_rows", &self.more_rows)
                    .field("buffered_rows", &batch.len())
                    .field("columns", &self.columns.len())
                    .finish();
            }
            OwnedRowStreamState::Fetching(_) => "Fetching",
            OwnedRowStreamState::Done => "Done",
            OwnedRowStreamState::Poisoned => "Poisoned",
        };
        f.debug_struct("OwnedRowStream")
            .field("has_connection", &self.connection.is_some())
            .field("cursor_id", &self.cursor_id)
            .field("more_rows", &self.more_rows)
            .field("state", &state)
            .field("columns", &self.columns.len())
            .finish()
    }
}

impl Connection {
    /// Consume this connection and start an [`OwnedRowStream`] over `query`,
    /// yielding owned rows one at a time and returning the connection when the
    /// stream is drained (or via [`OwnedRowStream::into_connection`]).
    ///
    /// The first page (EXECUTE + first fetch) runs before the stream is
    /// returned, so metadata ([`columns`](OwnedRowStream::columns)) is available
    /// immediately and the streamed rows are byte-identical to
    /// [`Connection::query_all`]. The [`Query`]'s `scrollable` flag is ignored —
    /// the stream is forward-only.
    ///
    /// The connection is consumed by value: if constructing the stream fails
    /// (e.g. a parse error), the connection is dropped with the returned error.
    pub async fn into_row_stream<'q>(self, cx: &Cx, query: Query<'q>) -> Result<OwnedRowStream> {
        let Query {
            sql,
            params,
            arraysize,
            prefetch,
            prefetch_set: _,
            materialize_lobs,
            // Forward-only stream: scrolling is not supported.
            scrollable: _,
            timeout,
        } = query;
        let mut connection = self;
        let sql_owned = sql.into_owned();
        let binds = crate::sql_convert::resolve_params(&sql_owned, params)?;
        let bind_rows = if binds.is_empty() {
            Vec::new()
        } else {
            vec![binds]
        };
        let deadline = QueryDeadline::new(cx, timeout);
        let mut result = match deadline
            .run(connection.execute_query_with_bind_rows_and_options_core(
                cx,
                &sql_owned,
                prefetch,
                &bind_rows,
                ExecuteOptions::default(),
            ))
            .await
        {
            Ok(result) => result?,
            Err(DeadlineExpiry::BeforeStart) => {
                return connection.reject_before_operation_start(cx, deadline.timeout_ms());
            }
            Err(DeadlineExpiry::InFlight) => {
                return connection
                    .recover_from_call_timeout(cx, deadline.timeout_ms())
                    .await
            }
        };
        // Same LOB/JSON/VECTOR define-fetch bootstrap as `query_with`: when the
        // first execute returns no rows because the columns need a client-side
        // define, run one define-and-fetch before the stream starts paging.
        if materialize_lobs
            && columns_require_define(&result.columns)
            && result.cursor_id != 0
            && result.rows.is_empty()
        {
            let cursor_id = result.cursor_id;
            let columns = result.columns.clone();
            let fetched = match deadline
                .run(connection.define_and_fetch_rows_with_columns(
                    cx,
                    cursor_id,
                    prefetch.max(1),
                    &columns,
                    None,
                ))
                .await
            {
                Ok(result) => result?,
                Err(DeadlineExpiry::BeforeStart) => {
                    connection.release_cursor(cursor_id);
                    return connection.reject_before_operation_start(cx, deadline.timeout_ms());
                }
                Err(DeadlineExpiry::InFlight) => {
                    return connection
                        .recover_from_call_timeout(cx, deadline.timeout_ms())
                        .await
                }
            };
            result.rows = fetched.rows;
            result.more_rows = fetched.more_rows;
            if !fetched.columns.is_empty() {
                result.columns = fetched.columns;
            }
            if result.cursor_id == 0 {
                result.cursor_id = cursor_id;
            }
        }
        Ok(OwnedRowStream::from_first_page(
            Some(connection),
            cx.clone(),
            arraysize.get(),
            deadline,
            result,
        ))
    }

    /// Consume this connection and start an [`OwnedRowStream`] over `sql` with
    /// `params`. Convenience wrapper over [`into_row_stream`](Self::into_row_stream)
    /// with the default [`Query`] settings.
    pub async fn into_query_stream<'p>(
        self,
        cx: &Cx,
        sql: &str,
        params: impl Into<Params<'p>>,
    ) -> Result<OwnedRowStream> {
        self.into_row_stream(cx, Query::owned_sql(sql.to_string()).bind(params))
            .await
    }
}

#[cfg(test)]
mod tests {
    use std::future::poll_fn;
    use std::io::{Read, Write};
    use std::pin::{pin, Pin};
    use std::task::{Context, Poll, Waker};
    use std::thread;
    use std::time::Duration;

    use asupersync::net::TcpStream;
    use asupersync::Cx;
    use oracledb_protocol::thin::{
        ColumnMetadata, QueryResult, QueryValue, TNS_DATA_FLAGS_END_OF_RESPONSE,
        TNS_PACKET_TYPE_DATA,
    };
    use oracledb_protocol::wire::{encode_packet, PacketLengthWidth};

    use super::*;

    fn cols(names: &[&str]) -> Vec<ColumnMetadata> {
        names.iter().map(|n| ColumnMetadata::new(*n, 0)).collect()
    }

    fn text_row(values: &[&str]) -> OwnedRow {
        values
            .iter()
            .map(|v| Some(QueryValue::Text((*v).to_string())))
            .collect()
    }

    /// Build a `Cx` off the driver's I/O runtime. The offline tests never fetch,
    /// so no socket is opened; only the ambient `Cx` is needed to seed a stream.
    fn with_cx<R>(body: impl FnOnce(Cx) -> R) -> R {
        let runtime = crate::build_io_runtime().expect("io runtime builds");
        runtime.block_on(async {
            let cx = Cx::current().expect("block_on installs an ambient Cx");
            body(cx)
        })
    }

    /// Poll a `!more_rows` (single-page) stream to exhaustion with a noop waker.
    /// Valid only when no continuation fetch is required (connection may be None).
    fn drain_buffered(stream: &mut OwnedRowStream) -> Vec<Result<OwnedRow>> {
        let mut out = Vec::new();
        let mut stream = pin!(stream);
        let mut task_cx = Context::from_waker(Waker::noop());
        loop {
            match stream.as_mut().poll_next(&mut task_cx) {
                Poll::Ready(Some(item)) => out.push(item),
                Poll::Ready(None) => break,
                Poll::Pending => panic!("buffered-only stream must never park"),
            }
        }
        out
    }

    fn offline_stream(cx: Cx, result: QueryResult) -> OwnedRowStream {
        let deadline = QueryDeadline::new(&cx, None);
        OwnedRowStream::from_first_page(None, cx, 100, deadline, result)
    }

    fn read_packet(socket: &mut std::net::TcpStream) -> std::io::Result<(u8, Vec<u8>)> {
        let mut header = [0u8; 8];
        socket.read_exact(&mut header)?;
        let declared = u32::from_be_bytes([header[0], header[1], header[2], header[3]]) as usize;
        let mut body = vec![0u8; declared - header.len()];
        socket.read_exact(&mut body)?;
        Ok((header[4], body))
    }

    fn data_packet(message: &[u8]) -> Vec<u8> {
        encode_packet(
            TNS_PACKET_TYPE_DATA,
            0,
            Some(TNS_DATA_FLAGS_END_OF_RESPONSE),
            message,
            PacketLengthWidth::Large32,
        )
        .expect("test DATA packet encodes")
    }

    fn marker_packet(marker_type: u8) -> Vec<u8> {
        encode_packet(
            crate::TNS_PACKET_TYPE_MARKER,
            0,
            None,
            &[1, 0, marker_type],
            PacketLengthWidth::Large32,
        )
        .expect("test MARKER packet encodes")
    }

    #[test]
    fn continuation_fetch_does_not_rearm_the_query_timeout() -> Result<()> {
        const QUERY_TIMEOUT: Duration = Duration::from_millis(40);
        const INFLIGHT_BODY: &[u8] = &[0xd1, 0xa1, 0xb1];
        const CANCEL_ERROR: &[u8] = &[0x04, 0x01, 0x0d];

        let listener = std::net::TcpListener::bind("127.0.0.1:0")?;
        let addr = listener.local_addr()?;
        let server = thread::spawn(move || -> std::io::Result<bool> {
            let (mut socket, _) = listener.accept()?;
            socket.set_read_timeout(Some(Duration::from_millis(500)))?;

            let (first_type, first_body) = match read_packet(&mut socket) {
                Ok(packet) => packet,
                Err(err)
                    if matches!(
                        err.kind(),
                        std::io::ErrorKind::UnexpectedEof
                            | std::io::ErrorKind::WouldBlock
                            | std::io::ErrorKind::TimedOut
                    ) =>
                {
                    return Ok(false);
                }
                Err(err) => return Err(err),
            };
            let saw_fetch = first_type == TNS_PACKET_TYPE_DATA;
            let break_body = if saw_fetch {
                let (packet_type, body) = read_packet(&mut socket)?;
                assert_eq!(packet_type, crate::TNS_PACKET_TYPE_MARKER);
                body
            } else {
                assert_eq!(first_type, crate::TNS_PACKET_TYPE_MARKER);
                first_body
            };
            assert_eq!(break_body, [1, 0, crate::TNS_MARKER_TYPE_BREAK]);

            socket.write_all(&data_packet(INFLIGHT_BODY))?;
            socket.write_all(&marker_packet(crate::TNS_MARKER_TYPE_BREAK))?;
            let (reset_type, reset_body) = read_packet(&mut socket)?;
            assert_eq!(reset_type, crate::TNS_PACKET_TYPE_MARKER);
            assert_eq!(reset_body, [1, 0, crate::TNS_MARKER_TYPE_RESET]);
            socket.write_all(&marker_packet(crate::TNS_MARKER_TYPE_RESET))?;
            socket.write_all(&data_packet(CANCEL_ERROR))?;
            socket.flush()?;
            Ok(saw_fetch)
        });

        let runtime = crate::build_io_runtime()?;
        runtime.block_on(async {
            let cx = Cx::current().expect("runtime installs an ambient Cx");
            let socket = TcpStream::connect(addr).await?;
            let (read, write) = crate::transport::plain_split(socket);
            let mut connection = crate::tests::loopback_connection(read, write);
            connection.in_use_cursors.insert(42);
            let result = QueryResult {
                columns: cols(&["A"]),
                rows: vec![text_row(&["first"])],
                cursor_id: 42,
                more_rows: true,
                ..QueryResult::default()
            };
            let deadline = QueryDeadline::new(&cx, Some(QUERY_TIMEOUT));
            let mut stream =
                OwnedRowStream::from_first_page(Some(connection), cx, 1, deadline, result);

            let first = poll_fn(|task_cx| Pin::new(&mut stream).poll_next(task_cx)).await;
            assert!(matches!(first, Some(Ok(_))), "first page must be buffered");

            // Let the one logical query timeout expire before asking for page 2.
            // A continuation must observe that original absolute deadline; it
            // must not get a fresh QUERY_TIMEOUT window of its own.
            thread::sleep(QUERY_TIMEOUT + Duration::from_millis(30));
            let second = poll_fn(|task_cx| Pin::new(&mut stream).poll_next(task_cx)).await;
            assert!(
                matches!(second, Some(Err(Error::CallTimeout(40)))),
                "expired logical query must fail immediately, got {second:?}"
            );
            assert!(
                !stream
                    .connection
                    .as_ref()
                    .expect("connection is returned after the failed fetch")
                    .in_use_cursors
                    .contains(&42),
                "before-start expiry must release the cursor without wire recovery"
            );
            Ok::<_, Error>(())
        })?;

        let saw_fetch = server.join().expect("server thread joins")?;
        assert!(
            !saw_fetch,
            "page 2 was sent after the original query timeout had elapsed; \
             the continuation incorrectly received a fresh timeout window"
        );
        Ok(())
    }

    #[test]
    fn buffered_rows_yield_in_order_then_terminate() {
        with_cx(|cx| {
            let result = QueryResult {
                columns: cols(&["A", "B"]),
                rows: vec![text_row(&["1", "x"]), text_row(&["2", "y"])],
                cursor_id: 7,
                more_rows: false,
                ..QueryResult::default()
            };
            let mut stream = offline_stream(cx, result);

            assert_eq!(stream.columns().len(), 2);
            let drained = drain_buffered(&mut stream);
            assert_eq!(drained.len(), 2, "both buffered rows yield");
            assert_eq!(drained[0].as_ref().unwrap(), &text_row(&["1", "x"]));
            assert_eq!(drained[1].as_ref().unwrap(), &text_row(&["2", "y"]));
            // Cursor released exactly once on drain; a second poll stays None.
            assert_eq!(stream.cursor_id, 0, "cursor released on full drain");
            assert!(matches!(stream.state, OwnedRowStreamState::Done));
        });
    }

    #[test]
    fn empty_single_page_yields_no_rows() {
        with_cx(|cx| {
            let result = QueryResult {
                columns: cols(&["A"]),
                rows: Vec::new(),
                cursor_id: 0,
                more_rows: false,
                ..QueryResult::default()
            };
            let mut stream = offline_stream(cx, result);
            assert!(drain_buffered(&mut stream).is_empty());
            assert!(matches!(stream.state, OwnedRowStreamState::Done));
        });
    }

    #[test]
    fn first_page_seeds_duplicate_column_continuation() {
        with_cx(|cx| {
            let result = QueryResult {
                columns: cols(&["A", "B"]),
                rows: vec![text_row(&["1", "x"]), text_row(&["2", "y"])],
                cursor_id: 7,
                more_rows: true,
                ..QueryResult::default()
            };
            let stream = offline_stream(cx, result);
            // Seed is the LAST row of the first page, independent of the buffer.
            assert_eq!(stream.previous_row, Some(text_row(&["2", "y"])));
        });
    }

    #[test]
    fn empty_continuation_page_keeps_previous_seed() {
        with_cx(|cx| {
            let first = QueryResult {
                columns: cols(&["A"]),
                rows: vec![text_row(&["seed"])],
                cursor_id: 7,
                more_rows: true,
                ..QueryResult::default()
            };
            let mut stream = offline_stream(cx, first);
            assert_eq!(stream.previous_row, Some(text_row(&["seed"])));

            // An empty continuation page (still more rows) must NOT clobber the
            // duplicate-column seed — the ORA-1403 confirmation-fetch shape.
            stream.apply_page(QueryResult {
                columns: Vec::new(),
                rows: Vec::new(),
                cursor_id: 0,
                more_rows: true,
                ..QueryResult::default()
            });
            assert_eq!(
                stream.previous_row,
                Some(text_row(&["seed"])),
                "empty page keeps the previous seed"
            );
            assert!(matches!(stream.state, OwnedRowStreamState::Buffered(ref b) if b.is_empty()));

            // A non-empty page refreshes the seed to ITS last row.
            stream.apply_page(QueryResult {
                columns: Vec::new(),
                rows: vec![text_row(&["p2a"]), text_row(&["p2b"])],
                cursor_id: 0,
                more_rows: false,
                ..QueryResult::default()
            });
            assert_eq!(stream.previous_row, Some(text_row(&["p2b"])));
        });
    }

    #[test]
    fn apply_page_adopts_redescribed_cursor_and_columns() {
        with_cx(|cx| {
            let first = QueryResult {
                columns: cols(&["A"]),
                rows: vec![text_row(&["1"])],
                cursor_id: 7,
                more_rows: true,
                ..QueryResult::default()
            };
            let mut stream = offline_stream(cx, first);
            assert_eq!(stream.cursor_id, 7);

            // A mid-paging DESCRIBE re-shapes the cursor: new id + wider columns.
            stream.apply_page(QueryResult {
                columns: cols(&["A", "B"]),
                rows: vec![text_row(&["2", "z"])],
                cursor_id: 9,
                more_rows: false,
                ..QueryResult::default()
            });
            assert_eq!(stream.cursor_id, 9, "adopts re-described cursor id");
            assert_eq!(stream.columns().len(), 2, "adopts re-described columns");
            assert!(!stream.more_rows);
        });
    }

    #[test]
    fn into_connection_on_poisoned_stream_errors() {
        with_cx(|cx| {
            let result = QueryResult {
                columns: cols(&["A"]),
                rows: vec![text_row(&["1"])],
                cursor_id: 7,
                more_rows: false,
                ..QueryResult::default()
            };
            // connection: None models a stream whose in-flight fetch was dropped.
            let stream = offline_stream(cx, result);
            let err = stream.into_connection().unwrap_err();
            assert!(matches!(err, Error::ConnectionClosed(_)));
        });
    }

    #[test]
    fn fetch_without_connection_poisons_cleanly() {
        with_cx(|cx| {
            // A stream that reports more rows but holds no connection: the first
            // buffered row yields, then the next poll must surface a clean error
            // (not panic) and leave the stream poisoned/terminated.
            let result = QueryResult {
                columns: cols(&["A"]),
                rows: vec![text_row(&["only"])],
                cursor_id: 7,
                more_rows: true,
                ..QueryResult::default()
            };
            let mut stream = offline_stream(cx, result);
            let mut stream = pin!(&mut stream);
            let mut task_cx = Context::from_waker(Waker::noop());

            let first = stream.as_mut().poll_next(&mut task_cx);
            assert!(matches!(first, Poll::Ready(Some(Ok(_)))));

            let second = stream.as_mut().poll_next(&mut task_cx);
            match second {
                Poll::Ready(Some(Err(Error::ConnectionClosed(_)))) => {}
                other => panic!("expected clean ConnectionClosed, got {other:?}"),
            }
            // Terminal thereafter.
            assert!(matches!(
                stream.as_mut().poll_next(&mut task_cx),
                Poll::Ready(None)
            ));
        });
    }
}
