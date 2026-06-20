//! Connection transport: a plain TCP socket or a TLS (TCPS) stream, presented
//! to the rest of the driver as `AsyncRead`/`AsyncWrite` read and write halves.
//!
//! The driver's packet I/O is generic over `AsyncRead`/`AsyncWrite`, and the
//! [`Connection`](crate::Connection) keeps the read half by value and the write
//! half behind an async mutex (so a [`CancelHandle`](crate::CancelHandle) can
//! send an out-of-band break while a read is in flight). To add TCPS without
//! disturbing that shape, both halves become enums:
//!
//! * **Plain** wraps asupersync's `OwnedReadHalf` / `OwnedWriteHalf` (the
//!   pre-TLS behaviour, byte-for-byte).
//! * **Tls** shares a single `TlsStream<TcpStream>` between the read and write
//!   halves. rustls's `ClientConnection` is a unified object that cannot be
//!   split into independent halves the way a raw socket can, so both halves
//!   hold an `Arc<Mutex<..>>` over the one stream. The mutex is a plain
//!   `std::sync::Mutex` held only for the duration of a single non-blocking
//!   `poll_*` call (never across an await), and the driver already serialises
//!   writes through the outer async write mutex and issues at most one read at
//!   a time, so there is no lock contention or ordering hazard.

use std::fmt::Debug;
use std::io;
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll};

use asupersync::io::{AsyncRead, AsyncWrite, ReadBuf};
use asupersync::net::{OwnedReadHalf, OwnedWriteHalf, TcpStream};
use asupersync::tls::TlsStream;

#[cfg(all(test, feature = "cassette"))]
pub(crate) use cassette_seam::replay_split;
#[cfg(feature = "cassette")]
pub use cassette_seam::{
    capture_scope, CaptureScope, CassetteError, CassetteRecorder, ReplayMismatch, ReplayWriteMode,
};

/// A TLS stream shared between the read and write halves.
type SharedTls = Arc<Mutex<TlsStream<TcpStream>>>;

/// Crate-private transport contract beneath the public [`Connection`](crate::Connection).
pub(crate) trait WireTransport {
    type Read: AsyncRead + Debug + Send + Unpin + 'static;
    type Write: AsyncWrite + Debug + Send + Unpin + 'static;
}

pub(crate) type TransportHalves<T> = (<T as WireTransport>::Read, <T as WireTransport>::Write);

/// Crate-private connector contract for producing transport halves.
pub(crate) trait Connector {
    type Transport: WireTransport;

    fn plain_split(&self, stream: TcpStream) -> TransportHalves<Self::Transport>;

    fn tls_split(&self, stream: TlsStream<TcpStream>) -> TransportHalves<Self::Transport>;
}

/// Production Oracle wire transport.
#[derive(Clone, Copy, Debug, Default)]
pub(crate) struct OracleWireTransport;

impl WireTransport for OracleWireTransport {
    type Read = OracleReadHalf;
    type Write = OracleWriteHalf;
}

/// Production connector for TCP and TCPS Oracle transports.
#[derive(Clone, Copy, Debug, Default)]
pub(crate) struct OracleConnector;

impl Connector for OracleConnector {
    type Transport = OracleWireTransport;

    fn plain_split(&self, stream: TcpStream) -> TransportHalves<Self::Transport> {
        plain_split(stream)
    }

    fn tls_split(&self, stream: TlsStream<TcpStream>) -> TransportHalves<Self::Transport> {
        tls_split(stream)
    }
}

#[cfg(test)]
mod tests {
    use super::{Connector, OracleConnector, OracleReadHalf, OracleWriteHalf, WireTransport};

    fn assert_wire_transport<T: WireTransport<Read = OracleReadHalf, Write = OracleWriteHalf>>() {}

    #[test]
    fn oracle_connector_uses_current_transport_halves() {
        assert_wire_transport::<<OracleConnector as Connector>::Transport>();
    }
}

/// The read half of an Oracle connection transport.
pub(crate) enum OracleReadHalf {
    /// Plain TCP read half (non-TLS).
    Plain(OwnedReadHalf),
    /// Shared TLS stream (read side).
    Tls(SharedTls),
    /// Recording wrapper: reads from an inner half and tees every `S->C`
    /// transfer into a [`CassetteRecorder`]. Only present with the `cassette`
    /// feature; when the feature is off this variant does not exist and the
    /// transport path is byte-identical to the plain build.
    #[cfg(feature = "cassette")]
    Recording(cassette_seam::RecordingRead),
    /// Replay source: serves recorded `S->C` bytes to reads in order with no
    /// socket. Only present with the `cassette` feature.
    #[cfg(feature = "cassette")]
    #[allow(dead_code)]
    Replay(cassette_seam::ReplayRead),
}

/// The write half of an Oracle connection transport.
pub(crate) enum OracleWriteHalf {
    /// Plain TCP write half (non-TLS).
    Plain(OwnedWriteHalf),
    /// Shared TLS stream (write side).
    Tls(SharedTls),
    /// Recording wrapper: writes through an inner half and tees every `C->S`
    /// transfer into a [`CassetteRecorder`]. Only present with the `cassette`
    /// feature.
    #[cfg(feature = "cassette")]
    Recording(cassette_seam::RecordingWrite),
    /// Replay sink: checks-or-ignores `C->S` writes against the cassette with
    /// no socket. Only present with the `cassette` feature.
    #[cfg(feature = "cassette")]
    #[allow(dead_code)]
    Replay(cassette_seam::ReplayWrite),
}

impl std::fmt::Debug for OracleReadHalf {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Plain(_) => f.write_str("OracleReadHalf::Plain"),
            Self::Tls(_) => f.write_str("OracleReadHalf::Tls"),
            #[cfg(feature = "cassette")]
            Self::Recording(_) => f.write_str("OracleReadHalf::Recording"),
            #[cfg(feature = "cassette")]
            Self::Replay(_) => f.write_str("OracleReadHalf::Replay"),
        }
    }
}

impl std::fmt::Debug for OracleWriteHalf {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Plain(_) => f.write_str("OracleWriteHalf::Plain"),
            Self::Tls(_) => f.write_str("OracleWriteHalf::Tls"),
            #[cfg(feature = "cassette")]
            Self::Recording(_) => f.write_str("OracleWriteHalf::Recording"),
            #[cfg(feature = "cassette")]
            Self::Replay(_) => f.write_str("OracleWriteHalf::Replay"),
        }
    }
}

/// Build a plain (non-TLS) read/write half pair from a connected TCP stream.
///
/// When the `cassette` feature is enabled and a recorder has been installed for
/// the current thread (see [`capture_scope`]), the halves are transparently
/// wrapped so the session is teed into the recorder. With no recorder installed
/// — and always when the feature is off — this is byte-identical to the plain
/// split.
#[must_use]
pub(crate) fn plain_split(stream: TcpStream) -> (OracleReadHalf, OracleWriteHalf) {
    let (read, write) = stream.into_split();
    let halves = (OracleReadHalf::Plain(read), OracleWriteHalf::Plain(write));
    #[cfg(feature = "cassette")]
    let halves = cassette_seam::wrap_if_capturing(halves);
    halves
}

/// Build a TLS read/write half pair from an established [`TlsStream`].
///
/// Like [`plain_split`], an installed [`capture_scope`] recorder (with the
/// `cassette` feature) transparently wraps the halves for recording.
#[must_use]
pub(crate) fn tls_split(stream: TlsStream<TcpStream>) -> (OracleReadHalf, OracleWriteHalf) {
    let shared: SharedTls = Arc::new(Mutex::new(stream));
    let halves = (
        OracleReadHalf::Tls(Arc::clone(&shared)),
        OracleWriteHalf::Tls(shared),
    );
    #[cfg(feature = "cassette")]
    let halves = cassette_seam::wrap_if_capturing(halves);
    halves
}

fn poisoned() -> io::Error {
    io::Error::other("TLS stream mutex poisoned")
}

impl AsyncRead for OracleReadHalf {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        match self.get_mut() {
            Self::Plain(read) => Pin::new(read).poll_read(cx, buf),
            Self::Tls(shared) => {
                let mut guard = shared.lock().map_err(|_| poisoned())?;
                Pin::new(&mut *guard).poll_read(cx, buf)
            }
            #[cfg(feature = "cassette")]
            Self::Recording(rec) => rec.poll_read(cx, buf),
            #[cfg(feature = "cassette")]
            Self::Replay(replay) => replay.poll_read(buf),
        }
    }
}

impl AsyncWrite for OracleWriteHalf {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        match self.get_mut() {
            Self::Plain(write) => Pin::new(write).poll_write(cx, buf),
            Self::Tls(shared) => {
                let mut guard = shared.lock().map_err(|_| poisoned())?;
                Pin::new(&mut *guard).poll_write(cx, buf)
            }
            #[cfg(feature = "cassette")]
            Self::Recording(rec) => rec.poll_write(cx, buf),
            #[cfg(feature = "cassette")]
            Self::Replay(replay) => replay.poll_write(buf),
        }
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        match self.get_mut() {
            Self::Plain(write) => Pin::new(write).poll_flush(cx),
            Self::Tls(shared) => {
                let mut guard = shared.lock().map_err(|_| poisoned())?;
                Pin::new(&mut *guard).poll_flush(cx)
            }
            #[cfg(feature = "cassette")]
            Self::Recording(rec) => rec.poll_flush(cx),
            #[cfg(feature = "cassette")]
            Self::Replay(_) => Poll::Ready(Ok(())),
        }
    }

    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        match self.get_mut() {
            Self::Plain(write) => Pin::new(write).poll_shutdown(cx),
            Self::Tls(shared) => {
                let mut guard = shared.lock().map_err(|_| poisoned())?;
                Pin::new(&mut *guard).poll_shutdown(cx)
            }
            #[cfg(feature = "cassette")]
            Self::Recording(rec) => rec.poll_shutdown(cx),
            #[cfg(feature = "cassette")]
            Self::Replay(_) => Poll::Ready(Ok(())),
        }
    }
}

/// Record/replay transport seam (`.tns-cassette`). Gated behind the `cassette`
/// feature so the default transport path is byte-identical.
///
/// Two decorators sit on the [`OracleReadHalf`] / [`OracleWriteHalf`] enums:
///
/// * **Recording** wraps the *real* halves and tees every transfer into a
///   [`CassetteRecorder`]. A `C->S` write is recorded as the driver issues it;
///   an `S->C` read is recorded with the exact bytes the socket returned. The
///   live byte stream is untouched — recording is a pure side-effect — so a
///   recorded session is byte-for-byte what a non-recorded one would be.
/// * **Replay** is socket-free. The read half serves the recorded `S->C` bytes
///   to reads in order (splitting a recorded transfer across smaller reads and
///   coalescing is unnecessary — the driver's `read_exact` loop just pulls what
///   it needs); the write half checks-or-ignores `C->S` writes. This drives the
///   real decoder/state-machine offline with no database.
///
/// Replay is byte-deterministic: it never consults a clock or RNG. The recorded
/// timestamps are informational and are ignored on the replay path.
#[cfg(feature = "cassette")]
mod cassette_seam {
    use std::collections::VecDeque;
    use std::io;
    use std::pin::Pin;
    use std::sync::{Arc, Mutex};
    use std::task::{Context, Poll};
    use std::time::Instant;

    use asupersync::io::{AsyncRead, AsyncWrite, ReadBuf};
    use oracledb_protocol::net::cassette::{self, Direction, Frame};

    use super::{OracleReadHalf, OracleWriteHalf};

    /// Re-exported decode error from the cassette wire format.
    pub use oracledb_protocol::net::cassette::CassetteError;

    /// A shared, append-only sink for recorded transfers. Cloning shares the
    /// same underlying buffer (both the recording read and write halves hold a
    /// clone so their transfers interleave in issue order).
    ///
    /// Call [`CassetteRecorder::into_cassette_bytes`] (or
    /// [`to_cassette_bytes`](CassetteRecorder::to_cassette_bytes)) at session
    /// end to serialize a complete `.tns-cassette` (header + frames).
    #[derive(Clone)]
    pub struct CassetteRecorder {
        inner: Arc<Mutex<RecorderState>>,
    }

    struct RecorderState {
        start: Instant,
        frames: Vec<Frame>,
    }

    impl CassetteRecorder {
        /// Create an empty recorder. The session-relative clock starts now.
        #[must_use]
        pub fn new() -> Self {
            Self {
                inner: Arc::new(Mutex::new(RecorderState {
                    start: Instant::now(),
                    frames: Vec::new(),
                })),
            }
        }

        fn record(&self, direction: Direction, bytes: &[u8]) {
            // A poisoned recorder mutex must never take down a live session;
            // drop the recording rather than panic on the I/O path.
            if let Ok(mut state) = self.inner.lock() {
                let micros = u64::try_from(state.start.elapsed().as_micros()).unwrap_or(u64::MAX);
                state.frames.push(Frame {
                    direction,
                    micros,
                    bytes: bytes.to_vec(),
                });
            }
        }

        /// Number of recorded frames so far.
        #[must_use]
        pub fn frame_count(&self) -> usize {
            self.inner.lock().map(|s| s.frames.len()).unwrap_or(0)
        }

        /// Serialize the recorded session into `.tns-cassette` bytes without
        /// consuming the recorder.
        #[must_use]
        pub fn to_cassette_bytes(&self) -> Vec<u8> {
            let mut out = Vec::new();
            cassette::write_header(&mut out);
            if let Ok(state) = self.inner.lock() {
                for frame in &state.frames {
                    cassette::write_frame(&mut out, frame.direction, frame.micros, &frame.bytes);
                }
            }
            out
        }

        /// Serialize and return the `.tns-cassette` bytes.
        #[must_use]
        pub fn into_cassette_bytes(self) -> Vec<u8> {
            self.to_cassette_bytes()
        }
    }

    impl Default for CassetteRecorder {
        fn default() -> Self {
            Self::new()
        }
    }

    /// Wrap a real read/write half pair so every transfer is teed into
    /// `recorder`. The returned halves behave exactly like the inner ones on
    /// the wire; recording is a side-effect only.
    #[must_use]
    pub(crate) fn recording_split(
        read: OracleReadHalf,
        write: OracleWriteHalf,
        recorder: CassetteRecorder,
    ) -> (OracleReadHalf, OracleWriteHalf) {
        (
            OracleReadHalf::Recording(RecordingRead {
                inner: Box::new(read),
                recorder: recorder.clone(),
            }),
            OracleWriteHalf::Recording(RecordingWrite {
                inner: Box::new(write),
                recorder,
            }),
        )
    }

    thread_local! {
        /// The recorder (if any) that [`wrap_if_capturing`] should tee into for
        /// splits performed on this thread. Installed for the duration of a
        /// [`CaptureScope`]. Thread-local rather than global so concurrent
        /// connections on other threads are unaffected, and so capture is opt-in
        /// per `connect` call with no parameter threading through the driver.
        static ACTIVE_RECORDER: std::cell::RefCell<Option<CassetteRecorder>> =
            const { std::cell::RefCell::new(None) };
    }

    /// Transparently wrap freshly-split halves in recording decorators if a
    /// [`CaptureScope`] recorder is installed on the current thread. Called from
    /// [`plain_split`](super::plain_split) / [`tls_split`](super::tls_split) so
    /// `Connection::connect` captures end-to-end with no API change.
    #[must_use]
    pub(super) fn wrap_if_capturing(
        halves: (OracleReadHalf, OracleWriteHalf),
    ) -> (OracleReadHalf, OracleWriteHalf) {
        match ACTIVE_RECORDER.with(|slot| slot.borrow().clone()) {
            Some(recorder) => recording_split(halves.0, halves.1, recorder),
            None => halves,
        }
    }

    /// RAII guard that records every transport split performed on the current
    /// thread for its lifetime. Drop it (or let it fall out of scope) to stop
    /// capturing, then call [`CaptureScope::recorder`] /
    /// [`CassetteRecorder::into_cassette_bytes`] to serialize the cassette.
    ///
    /// Capturing applies to the thread that holds the guard, so the typical use
    /// is: install the scope, run `Connection::connect` + queries + close on the
    /// same task, then serialize.
    #[must_use = "dropping the CaptureScope immediately stops recording"]
    pub struct CaptureScope {
        recorder: CassetteRecorder,
        previous: Option<CassetteRecorder>,
    }

    /// Install a fresh recorder for the current thread and return its guard.
    pub fn capture_scope() -> CaptureScope {
        let recorder = CassetteRecorder::new();
        let previous = ACTIVE_RECORDER.with(|slot| slot.borrow_mut().replace(recorder.clone()));
        CaptureScope { recorder, previous }
    }

    impl CaptureScope {
        /// The recorder collecting this scope's transfers.
        #[must_use]
        pub fn recorder(&self) -> &CassetteRecorder {
            &self.recorder
        }

        /// Serialize the captured session into `.tns-cassette` bytes.
        #[must_use]
        pub fn to_cassette_bytes(&self) -> Vec<u8> {
            self.recorder.to_cassette_bytes()
        }
    }

    impl Drop for CaptureScope {
        fn drop(&mut self) {
            ACTIVE_RECORDER.with(|slot| {
                *slot.borrow_mut() = self.previous.take();
            });
        }
    }

    /// How replay handles the driver's `C->S` writes.
    #[derive(Clone, Copy, Debug, Eq, PartialEq, Default)]
    pub enum ReplayWriteMode {
        /// Accept and discard writes (default). The decoder drives forward with
        /// no socket and no fidelity check on what it sends.
        #[default]
        Ignore,
        /// Compare each write against the recorded `C->S` frames and fail with
        /// [`ReplayMismatch`] on the first divergence. Useful to prove the
        /// driver re-issues the exact captured request stream.
        Check,
    }

    /// A replayed `C->S` write diverged from the recorded request stream
    /// (only raised in [`ReplayWriteMode::Check`]).
    #[derive(Clone, Debug, Eq, PartialEq)]
    pub struct ReplayMismatch {
        /// Index of the recorded `C->S` frame that did not match (or the count
        /// of recorded writes, if the driver wrote more than were recorded).
        pub frame_index: usize,
        /// What the recorded request stream expected at this point.
        pub expected: Vec<u8>,
        /// What the driver actually wrote.
        pub actual: Vec<u8>,
    }

    /// Build a socket-free replay transport from `.tns-cassette` bytes. The read
    /// half serves the recorded `S->C` transfers in order; the write half
    /// handles `C->S` writes per `write_mode`.
    ///
    /// # Errors
    ///
    /// Returns [`CassetteError`] if `data` is not a valid cassette.
    #[allow(dead_code)]
    pub(crate) fn replay_split(
        data: &[u8],
        write_mode: ReplayWriteMode,
    ) -> Result<(OracleReadHalf, OracleWriteHalf), CassetteError> {
        let frames = cassette::decode_all(data)?;
        let mut reads: VecDeque<Vec<u8>> = VecDeque::new();
        let mut writes: VecDeque<Vec<u8>> = VecDeque::new();
        for frame in frames {
            match frame.direction {
                Direction::ServerToClient => reads.push_back(frame.bytes),
                Direction::ClientToServer => writes.push_back(frame.bytes),
            }
        }
        let mismatch = Arc::new(Mutex::new(None));
        Ok((
            OracleReadHalf::Replay(ReplayRead {
                pending: reads,
                offset: 0,
            }),
            OracleWriteHalf::Replay(ReplayWrite {
                expected: writes,
                offset: 0,
                index: 0,
                mode: write_mode,
                mismatch,
            }),
        ))
    }

    /// Recording read half: reads from `inner` and tees `S->C` bytes.
    pub struct RecordingRead {
        inner: Box<OracleReadHalf>,
        recorder: CassetteRecorder,
    }

    impl RecordingRead {
        pub(super) fn poll_read(
            &mut self,
            cx: &mut Context<'_>,
            buf: &mut ReadBuf<'_>,
        ) -> Poll<io::Result<()>> {
            let before = buf.filled().len();
            match Pin::new(self.inner.as_mut()).poll_read(cx, buf) {
                Poll::Ready(Ok(())) => {
                    let new = &buf.filled()[before..];
                    if !new.is_empty() {
                        self.recorder.record(Direction::ServerToClient, new);
                    }
                    Poll::Ready(Ok(()))
                }
                other => other,
            }
        }
    }

    /// Recording write half: writes through `inner` and tees `C->S` bytes.
    pub struct RecordingWrite {
        inner: Box<OracleWriteHalf>,
        recorder: CassetteRecorder,
    }

    impl RecordingWrite {
        pub(super) fn poll_write(
            &mut self,
            cx: &mut Context<'_>,
            buf: &[u8],
        ) -> Poll<io::Result<usize>> {
            match Pin::new(self.inner.as_mut()).poll_write(cx, buf) {
                Poll::Ready(Ok(n)) => {
                    if n > 0 {
                        self.recorder.record(Direction::ClientToServer, &buf[..n]);
                    }
                    Poll::Ready(Ok(n))
                }
                other => other,
            }
        }

        pub(super) fn poll_flush(&mut self, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
            Pin::new(self.inner.as_mut()).poll_flush(cx)
        }

        pub(super) fn poll_shutdown(&mut self, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
            Pin::new(self.inner.as_mut()).poll_shutdown(cx)
        }
    }

    /// Replay read half: serves recorded `S->C` transfers in order, no socket.
    pub struct ReplayRead {
        /// Recorded `S->C` transfers still to be served, oldest first.
        pending: VecDeque<Vec<u8>>,
        /// Byte offset consumed within the front transfer.
        offset: usize,
    }

    impl ReplayRead {
        pub(super) fn poll_read(&mut self, buf: &mut ReadBuf<'_>) -> Poll<io::Result<()>> {
            // Skip any exhausted front transfers.
            while let Some(front) = self.pending.front() {
                if self.offset >= front.len() {
                    self.pending.pop_front();
                    self.offset = 0;
                } else {
                    break;
                }
            }
            let Some(front) = self.pending.front() else {
                // Cassette exhausted: report EOF (zero bytes filled), exactly as
                // a closed socket would. The driver's read_exact then surfaces
                // an UnexpectedEof if it wanted more.
                return Poll::Ready(Ok(()));
            };
            let available = &front[self.offset..];
            let take = available.len().min(buf.remaining());
            buf.put_slice(&available[..take]);
            self.offset += take;
            Poll::Ready(Ok(()))
        }
    }

    /// Replay write half: checks-or-ignores `C->S` writes, no socket.
    pub struct ReplayWrite {
        /// Recorded `C->S` transfers, used only in [`ReplayWriteMode::Check`].
        expected: VecDeque<Vec<u8>>,
        /// Byte offset matched within the front expected transfer.
        offset: usize,
        /// Index of the front expected transfer (for mismatch reporting).
        index: usize,
        mode: ReplayWriteMode,
        /// First mismatch seen, surfaced via [`ReplayWrite::take_mismatch`].
        mismatch: Arc<Mutex<Option<ReplayMismatch>>>,
    }

    impl ReplayWrite {
        pub(super) fn poll_write(&mut self, buf: &[u8]) -> Poll<io::Result<usize>> {
            if matches!(self.mode, ReplayWriteMode::Ignore) {
                return Poll::Ready(Ok(buf.len()));
            }
            // Check mode: match `buf` against the recorded request bytes,
            // flowing across recorded-transfer boundaries (the driver may chunk
            // a logical request differently than it was recorded).
            let mut cursor = 0usize;
            while cursor < buf.len() {
                let Some(front) = self.expected.front() else {
                    self.note_mismatch(self.index, &[], &buf[cursor..]);
                    return Poll::Ready(Err(io::Error::other(
                        "replay: write past recorded stream",
                    )));
                };
                let remaining = &front[self.offset..];
                let chunk = &buf[cursor..];
                let take = remaining.len().min(chunk.len());
                if remaining[..take] != chunk[..take] {
                    self.note_mismatch(self.index, remaining, chunk);
                    return Poll::Ready(Err(io::Error::other("replay: write mismatch")));
                }
                cursor += take;
                self.offset += take;
                if self.offset >= front.len() {
                    self.expected.pop_front();
                    self.offset = 0;
                    self.index += 1;
                }
            }
            Poll::Ready(Ok(buf.len()))
        }

        fn note_mismatch(&self, frame_index: usize, expected: &[u8], actual: &[u8]) {
            if let Ok(mut slot) = self.mismatch.lock() {
                if slot.is_none() {
                    *slot = Some(ReplayMismatch {
                        frame_index,
                        expected: expected.to_vec(),
                        actual: actual.to_vec(),
                    });
                }
            }
        }
    }

    #[cfg(test)]
    mod tests {
        use super::*;
        use asupersync::io::ReadBuf;

        fn read_n(read: &mut OracleReadHalf, n: usize) -> Vec<u8> {
            // Drive poll_read in a loop until n bytes are gathered or EOF.
            let mut out = Vec::new();
            while out.len() < n {
                let mut scratch = vec![0u8; n - out.len()];
                let mut rb = ReadBuf::new(&mut scratch);
                let OracleReadHalf::Replay(replay) = read else {
                    panic!("expected replay read half");
                };
                match replay.poll_read(&mut rb) {
                    Poll::Ready(Ok(())) => {
                        let filled = rb.filled().to_vec();
                        if filled.is_empty() {
                            break; // EOF
                        }
                        out.extend_from_slice(&filled);
                    }
                    _ => break,
                }
            }
            out
        }

        #[test]
        fn replay_serves_server_bytes_in_order() {
            // Hand-craft a cassette: two S->C transfers and one C->S write.
            let recorder = CassetteRecorder::new();
            recorder.record(Direction::ClientToServer, &[0x10, 0x20]);
            recorder.record(Direction::ServerToClient, &[0xAA, 0xBB, 0xCC]);
            recorder.record(Direction::ServerToClient, &[0xDD]);
            let bytes = recorder.into_cassette_bytes();

            let (mut read, _write) =
                replay_split(&bytes, ReplayWriteMode::Ignore).expect("valid cassette");
            // Reads come back as the concatenated S->C stream, in order.
            let got = read_n(&mut read, 4);
            assert_eq!(got, vec![0xAA, 0xBB, 0xCC, 0xDD]);
            // Past the end: EOF (zero fill).
            let eof = read_n(&mut read, 1);
            assert!(eof.is_empty());
        }

        #[test]
        fn replay_splits_one_transfer_across_small_reads() {
            let recorder = CassetteRecorder::new();
            recorder.record(Direction::ServerToClient, &[1, 2, 3, 4, 5]);
            let bytes = recorder.into_cassette_bytes();
            let (mut read, _w) =
                replay_split(&bytes, ReplayWriteMode::Ignore).expect("valid cassette");
            assert_eq!(read_n(&mut read, 2), vec![1, 2]);
            assert_eq!(read_n(&mut read, 2), vec![3, 4]);
            assert_eq!(read_n(&mut read, 2), vec![5]);
        }

        #[test]
        fn replay_write_ignore_accepts_anything() {
            let recorder = CassetteRecorder::new();
            recorder.record(Direction::ClientToServer, &[1, 2, 3]);
            let bytes = recorder.into_cassette_bytes();
            let (_r, mut write) =
                replay_split(&bytes, ReplayWriteMode::Ignore).expect("valid cassette");
            let OracleWriteHalf::Replay(w) = &mut write else {
                panic!("expected replay write half");
            };
            // Ignore mode accepts bytes that don't match the recording.
            assert!(matches!(w.poll_write(&[9, 9, 9, 9]), Poll::Ready(Ok(4))));
        }

        #[test]
        fn replay_write_check_matches_recorded_request_stream() {
            let recorder = CassetteRecorder::new();
            recorder.record(Direction::ClientToServer, &[0xDE, 0xAD, 0xBE, 0xEF]);
            let bytes = recorder.into_cassette_bytes();
            let (_r, mut write) =
                replay_split(&bytes, ReplayWriteMode::Check).expect("valid cassette");
            let OracleWriteHalf::Replay(w) = &mut write else {
                panic!("expected replay write half");
            };
            // Writing the recorded bytes (even chunked) succeeds.
            assert!(matches!(w.poll_write(&[0xDE, 0xAD]), Poll::Ready(Ok(2))));
            assert!(matches!(w.poll_write(&[0xBE, 0xEF]), Poll::Ready(Ok(2))));
            assert!(w.mismatch.lock().expect("lock").is_none());
        }

        #[test]
        fn replay_write_check_flags_mismatch() {
            let recorder = CassetteRecorder::new();
            recorder.record(Direction::ClientToServer, &[1, 2, 3, 4]);
            let bytes = recorder.into_cassette_bytes();
            let (_r, mut write) =
                replay_split(&bytes, ReplayWriteMode::Check).expect("valid cassette");
            let OracleWriteHalf::Replay(w) = &mut write else {
                panic!("expected replay write half");
            };
            assert!(matches!(w.poll_write(&[1, 2]), Poll::Ready(Ok(2))));
            // Diverge on the third byte.
            assert!(matches!(w.poll_write(&[9, 9]), Poll::Ready(Err(_))));
            let mismatch = w
                .mismatch
                .lock()
                .expect("lock")
                .clone()
                .expect("a mismatch");
            assert_eq!(mismatch.frame_index, 0);
        }

        #[test]
        fn recorder_serializes_valid_cassette() {
            let recorder = CassetteRecorder::new();
            recorder.record(Direction::ClientToServer, &[1]);
            recorder.record(Direction::ServerToClient, &[2, 3]);
            assert_eq!(recorder.frame_count(), 2);
            let bytes = recorder.to_cassette_bytes();
            // Round-trips through the wire-format decoder.
            let frames = cassette::decode_all(&bytes).expect("decodes");
            assert_eq!(frames.len(), 2);
            assert_eq!(frames[0].direction, Direction::ClientToServer);
            assert_eq!(frames[1].bytes, vec![2, 3]);
        }

        #[test]
        fn replay_split_rejects_garbage() {
            let err = replay_split(b"not a cassette", ReplayWriteMode::Ignore)
                .expect_err("garbage must fail");
            assert_eq!(err, CassetteError::BadMagic);
        }

        // A trivial valid cassette to feed replay_split when we just need a
        // pair of halves to hand to wrap_if_capturing.
        fn empty_cassette() -> Vec<u8> {
            CassetteRecorder::new().into_cassette_bytes()
        }

        #[test]
        fn capture_scope_wraps_splits_then_restores_on_drop() {
            // No scope installed: splits pass through unwrapped.
            let (r, w) = replay_split(&empty_cassette(), ReplayWriteMode::Ignore).expect("ok");
            let (r, w) = wrap_if_capturing((r, w));
            assert!(matches!(r, OracleReadHalf::Replay(_)));
            assert!(matches!(w, OracleWriteHalf::Replay(_)));

            // Inside a scope: splits are wrapped in Recording decorators.
            {
                let scope = capture_scope();
                let (r, w) = replay_split(&empty_cassette(), ReplayWriteMode::Ignore).expect("ok");
                let (r, w) = wrap_if_capturing((r, w));
                assert!(matches!(r, OracleReadHalf::Recording(_)));
                assert!(matches!(w, OracleWriteHalf::Recording(_)));
                assert_eq!(scope.recorder().frame_count(), 0);
            }

            // After the scope drops: back to pass-through.
            let (r, w) = replay_split(&empty_cassette(), ReplayWriteMode::Ignore).expect("ok");
            let (r, _w) = wrap_if_capturing((r, w));
            assert!(matches!(r, OracleReadHalf::Replay(_)));
        }
    }
}
