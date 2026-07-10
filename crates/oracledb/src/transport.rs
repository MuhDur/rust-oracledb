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

#[cfg(feature = "cassette")]
pub(crate) use cassette_seam::{capture_path_from_env, install_recorder_scope, CaptureGuard};
#[cfg(feature = "cassette")]
pub use cassette_seam::{
    capture_scope, CaptureScope, CassetteCaptureError, CassetteCaptureReport, CassetteError,
    CassetteRecorder, ReplayMismatch, ReplayWriteMode,
};
#[cfg(all(test, feature = "cassette"))]
pub(crate) use cassette_seam::{
    replay_split, replay_split_with_audit, scan_for_secret_fields, scrub_and_gate,
    wrap_if_capturing,
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
    use std::fmt;
    use std::fs;
    use std::io;
    use std::path::{Path, PathBuf};
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

    // ---- secret-free support capture (bead K6) ----------------------------
    //
    // A CassetteRecorder can record the FULL session — including the auth phase
    // (password verifier, session key, tokens). Persisting that raw would leak
    // secrets to disk. `scrub_and_write` is the safe path: it scrubs the
    // auth-phase frames, then runs a fail-closed REFUSE-ON-SECRET gate over the
    // whole artifact, and only writes if nothing secret-shaped survives. The
    // gate is deliberately dumb-but-total (a substring tripwire) and the scrub
    // is scoped to the auth window, so the gate still catches a secret the
    // scrubber missed (e.g. one leaked into post-auth user traffic).

    /// Auth-phase field-name markers that must NEVER survive into a persisted
    /// cassette. On the Oracle wire the secret VALUE (password verifier,
    /// session key, token) is sent right after its ASCII-labelled key, so the
    /// label is a reliable tripwire for a leaked secret. Single source of truth
    /// for both the auth-phase scrubber and the refuse gate; `version_cassettes`
    /// reuses it via [`scan_for_secret_fields`].
    pub(crate) const SECRET_FIELD_NAMES: &[&str] = &[
        "AUTH_PASSWORD",
        "AUTH_SESSKEY",
        "AUTH_VFR_DATA",
        "AUTH_PBKDF2_CSK_SALT",
        "AUTH_PBKDF2_SPEEDY_KEY",
        "AUTH_TOKEN",
        "SESSION_TOKEN",
        "SESSION_KEY",
        "ACCESS_TOKEN",
        "REFRESH_TOKEN",
        "PRIVATE_KEY",
    ];

    fn contains_ascii_case_insensitive(haystack: &[u8], needle: &[u8]) -> bool {
        !needle.is_empty()
            && needle.len() <= haystack.len()
            && haystack
                .windows(needle.len())
                .any(|window| window.eq_ignore_ascii_case(needle))
    }

    fn scan_frame_secret_fields(frames: &[Frame]) -> Vec<&'static str> {
        let mut found = vec![false; SECRET_FIELD_NAMES.len()];
        let mut run_start = 0usize;

        while run_start < frames.len() {
            let direction = frames[run_start].direction;
            let mut run_end = run_start + 1;
            while run_end < frames.len() && frames[run_end].direction == direction {
                run_end += 1;
            }

            // A transport write/read may be split at any byte boundary. Search
            // one contiguous same-direction transfer run so a field name split
            // across recorder frames is still visible to the refuse gate.
            let mut run = Vec::new();
            for frame in &frames[run_start..run_end] {
                run.extend_from_slice(&frame.bytes);
            }
            for (index, field) in SECRET_FIELD_NAMES.iter().enumerate() {
                if contains_ascii_case_insensitive(&run, field.as_bytes()) {
                    found[index] = true;
                }
            }

            run_start = run_end;
        }

        SECRET_FIELD_NAMES
            .iter()
            .copied()
            .zip(found)
            .filter_map(|(field, is_present)| is_present.then_some(field))
            .collect()
    }

    /// Fail-closed refuse gate: return every secret field name that still
    /// appears (case-insensitively) in `bytes`. Cassette payloads are searched
    /// across contiguous same-direction recorder frames, so a transport split
    /// cannot hide a marker behind the next frame header. A non-empty result
    /// means the artifact MUST NOT be persisted.
    #[must_use]
    pub(crate) fn scan_for_secret_fields(bytes: &[u8]) -> Vec<&'static str> {
        if let Ok(frames) = cassette::decode_all(bytes) {
            return scan_frame_secret_fields(&frames);
        }

        SECRET_FIELD_NAMES
            .iter()
            .copied()
            .filter(|field| contains_ascii_case_insensitive(bytes, field.as_bytes()))
            .collect()
    }

    /// The auth handshake always completes within the first few round-trips of
    /// a session, so the scrubber only redacts secret material within this
    /// leading window. Anything beyond it is user traffic that must be
    /// secret-free on its own merit — the refuse gate enforces that fail-closed.
    const AUTH_PHASE_FRAME_LIMIT: usize = 32;

    /// Deterministic redaction fill byte (`0xEE` is not valid UTF-8 leading, so
    /// a redacted region cannot be mistaken for readable field text).
    const REDACTION_BYTE: u8 = 0xEE;

    fn first_secret_marker_before(bytes: &[u8], end: usize) -> Option<usize> {
        SECRET_FIELD_NAMES
            .iter()
            .filter_map(|name| {
                bytes
                    .windows(name.len())
                    .position(|window| window.eq_ignore_ascii_case(name.as_bytes()))
            })
            .filter(|position| *position < end)
            .min()
    }

    /// Scrub the auth phase in place. Recorder frames are arbitrary transport
    /// chunks, so inspect each contiguous same-direction run as one byte stream.
    /// Once a secret marker begins inside the leading auth window, redact from
    /// that marker through the END of the run. This is deliberately fail-closed:
    /// token/password lengths need not be guessed, and a marker or value split
    /// across recorder frames cannot leave an unlabelled secret suffix behind.
    /// Returns the number of frames in which at least one byte was redacted.
    fn scrub_auth_frames(frames: &mut [Frame]) -> usize {
        let mut redacted = 0usize;
        let auth_frame_count = frames.len().min(AUTH_PHASE_FRAME_LIMIT);
        let mut run_start = 0usize;

        while run_start < auth_frame_count {
            let direction = frames[run_start].direction;
            let mut run_end = run_start + 1;
            while run_end < frames.len() && frames[run_end].direction == direction {
                run_end += 1;
            }

            let mut run = Vec::new();
            let mut auth_run_byte_len = 0usize;
            for (offset, frame) in frames[run_start..run_end].iter().enumerate() {
                run.extend_from_slice(&frame.bytes);
                if run_start + offset < auth_frame_count {
                    auth_run_byte_len += frame.bytes.len();
                }
            }

            if let Some(mut marker_offset) = first_secret_marker_before(&run, auth_run_byte_len) {
                for frame in &mut frames[run_start..run_end] {
                    if marker_offset >= frame.bytes.len() {
                        marker_offset -= frame.bytes.len();
                        continue;
                    }

                    let frame_tail = &mut frame.bytes[marker_offset..];
                    if !frame_tail.is_empty() {
                        frame_tail.fill(REDACTION_BYTE);
                        redacted += 1;
                    }
                    marker_offset = 0;
                }
            }

            run_start = run_end;
        }

        redacted
    }

    /// Outcome of a successful secret-free support capture.
    #[derive(Clone, Debug, Eq, PartialEq)]
    pub struct CassetteCaptureReport {
        /// Total frames in the captured session.
        pub frame_count: usize,
        /// Frames in which at least one auth-phase byte was redacted.
        pub redacted_frames: usize,
        /// Size in bytes of the persisted (scrubbed) cassette.
        pub byte_len: usize,
    }

    /// Why a support-capture write was refused or failed.
    #[derive(Debug)]
    pub enum CassetteCaptureError {
        /// The fail-closed refusal: a secret field name survived scrubbing, so
        /// the refuse gate aborted the write. NO file was written.
        SecretLeak {
            /// The secret field name(s) that tripped the gate.
            fields: Vec<&'static str>,
        },
        /// The recorded bytes were not a decodable cassette.
        Decode(CassetteError),
        /// A filesystem error while persisting the gate-passed cassette.
        Io(io::Error),
    }

    impl fmt::Display for CassetteCaptureError {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            match self {
                Self::SecretLeak { fields } => write!(
                    f,
                    "refused to write cassette: secret field(s) survived scrubbing: {fields:?}"
                ),
                Self::Decode(err) => write!(f, "cannot capture: {err}"),
                Self::Io(err) => write!(f, "cassette write failed: {err}"),
            }
        }
    }

    impl std::error::Error for CassetteCaptureError {
        fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
            match self {
                Self::Decode(err) => Some(err),
                Self::Io(err) => Some(err),
                Self::SecretLeak { .. } => None,
            }
        }
    }

    /// Decode `cassette_bytes`, scrub the auth-phase frames, re-encode, and run
    /// the fail-closed refuse gate over the FULL re-encoded artifact. On success
    /// returns the scrubbed cassette bytes and a report; on any surviving secret
    /// returns [`CassetteCaptureError::SecretLeak`] and NO bytes, so the caller
    /// writes nothing. This is the pure (bytes-in, bytes-out) safety core.
    ///
    /// # Errors
    ///
    /// [`CassetteCaptureError::Decode`] if `cassette_bytes` is not a valid
    /// cassette; [`CassetteCaptureError::SecretLeak`] if a secret field name
    /// survived scrubbing.
    pub(crate) fn scrub_and_gate(
        cassette_bytes: &[u8],
    ) -> Result<(Vec<u8>, CassetteCaptureReport), CassetteCaptureError> {
        let mut frames =
            cassette::decode_all(cassette_bytes).map_err(CassetteCaptureError::Decode)?;
        let frame_count = frames.len();
        let redacted_frames = scrub_auth_frames(&mut frames);

        let mut scrubbed = Vec::with_capacity(cassette_bytes.len());
        cassette::write_header(&mut scrubbed);
        for frame in &frames {
            cassette::write_frame(&mut scrubbed, frame.direction, frame.micros, &frame.bytes);
        }

        // Fail-closed: the gate runs over the WHOLE re-encoded artifact, so a
        // secret the scrubber missed (out of the auth window, or a marker it
        // didn't recognize) still aborts the write.
        let leaks = scan_for_secret_fields(&scrubbed);
        if !leaks.is_empty() {
            return Err(CassetteCaptureError::SecretLeak { fields: leaks });
        }

        let report = CassetteCaptureReport {
            frame_count,
            redacted_frames,
            byte_len: scrubbed.len(),
        };
        Ok((scrubbed, report))
    }

    /// Write `bytes` to `path` atomically: write a uniquely-named sibling temp
    /// file, then rename it into place. The gate has already passed by the time
    /// this runs, and a refused capture never reaches here, so no partial or
    /// secret-bearing file is ever left behind.
    fn atomic_write(path: &Path, bytes: &[u8]) -> io::Result<()> {
        let mut tmp_name = path.as_os_str().to_owned();
        tmp_name.push(format!(".{}.tmp", std::process::id()));
        let tmp = PathBuf::from(tmp_name);
        fs::write(&tmp, bytes)?;
        match fs::rename(&tmp, path) {
            Ok(()) => Ok(()),
            Err(err) => {
                let _ = fs::remove_file(&tmp);
                Err(err)
            }
        }
    }

    impl CassetteRecorder {
        /// Scrub the auth phase, run the fail-closed REFUSE-ON-SECRET gate, and
        /// — only if no secret survives — atomically write the shareable,
        /// offline `.tns-cassette` to `path`. On a surviving secret the write is
        /// REFUSED and no file is left behind. This is the safety core of
        /// secret-free support capture: a scrub bug fails closed, never leaks.
        ///
        /// # Errors
        ///
        /// [`CassetteCaptureError::SecretLeak`] if a secret field survived
        /// scrubbing (nothing written); [`CassetteCaptureError::Decode`] if the
        /// recording is not a valid cassette; [`CassetteCaptureError::Io`] on a
        /// filesystem error.
        pub fn scrub_and_write(
            &self,
            path: &Path,
        ) -> Result<CassetteCaptureReport, CassetteCaptureError> {
            let (scrubbed, report) = scrub_and_gate(&self.to_cassette_bytes())?;
            atomic_write(path, &scrubbed).map_err(CassetteCaptureError::Io)?;
            Ok(report)
        }
    }

    /// Read the `ORACLEDB_CAPTURE` support-capture target path from the
    /// environment. `None` (unset or empty) means capture is disabled and the
    /// transport path is byte-identical to a non-capturing session.
    #[must_use]
    pub(crate) fn capture_path_from_env() -> Option<PathBuf> {
        match std::env::var_os("ORACLEDB_CAPTURE") {
            Some(value) if !value.is_empty() => Some(PathBuf::from(value)),
            _ => None,
        }
    }

    /// Install an EXISTING `recorder` as the active recorder for the current
    /// thread and return a scope guard. Unlike [`capture_scope`], the recorder
    /// is supplied by the caller so its frames can be persisted after the
    /// connection ends. Install it around the SYNCHRONOUS transport split only
    /// (never hold it across an await) so the thread-local is observed on the
    /// same thread that performs the split.
    pub(crate) fn install_recorder_scope(recorder: CassetteRecorder) -> CaptureScope {
        let previous = ACTIVE_RECORDER.with(|slot| slot.borrow_mut().replace(recorder.clone()));
        CaptureScope { recorder, previous }
    }

    /// Session-lifetime guard that persists a scrubbed, secret-free cassette
    /// when the connection is dropped or closed. Armed only when
    /// `ORACLEDB_CAPTURE` was set at connect time. Writing is best-effort and
    /// fail-closed: a surviving secret refuses the write (no file) and logs,
    /// never panics on the drop path.
    pub(crate) struct CaptureGuard {
        recorder: CassetteRecorder,
        path: PathBuf,
    }

    impl CaptureGuard {
        pub(crate) fn new(recorder: CassetteRecorder, path: PathBuf) -> Self {
            Self { recorder, path }
        }
    }

    impl fmt::Debug for CaptureGuard {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            // Never render recorded frames (they carry the raw session bytes).
            f.debug_struct("CaptureGuard")
                .field("path", &self.path)
                .field("frames", &self.recorder.frame_count())
                .finish()
        }
    }

    impl Drop for CaptureGuard {
        fn drop(&mut self) {
            if self.recorder.frame_count() == 0 {
                return;
            }
            match self.recorder.scrub_and_write(&self.path) {
                Ok(report) => eprintln!(
                    "oracledb: wrote support cassette {} ({} frames, {} redacted, {} bytes)",
                    self.path.display(),
                    report.frame_count,
                    report.redacted_frames,
                    report.byte_len,
                ),
                Err(CassetteCaptureError::SecretLeak { fields }) => eprintln!(
                    "oracledb: REFUSED to write support cassette {}: secret field(s) survived \
                     scrubbing: {fields:?} (no file written)",
                    self.path.display(),
                ),
                Err(err) => eprintln!(
                    "oracledb: support cassette write failed for {}: {err}",
                    self.path.display(),
                ),
            }
        }
    }

    #[cfg(test)]
    mod capture_tests {
        use super::*;

        /// Encode a list of `(direction, payload)` frames into cassette bytes.
        fn cassette_of(frames: &[(Direction, Vec<u8>)]) -> Vec<u8> {
            let mut out = Vec::new();
            cassette::write_header(&mut out);
            for (i, (direction, bytes)) in frames.iter().enumerate() {
                cassette::write_frame(&mut out, *direction, i as u64, bytes);
            }
            out
        }

        /// A recorder pre-loaded with `frames` (uses the private record path).
        fn recorder_of(frames: &[(Direction, Vec<u8>)]) -> CassetteRecorder {
            let recorder = CassetteRecorder::new();
            for (direction, bytes) in frames {
                recorder.record(*direction, bytes);
            }
            recorder
        }

        fn unique_temp_path(tag: &str) -> PathBuf {
            let nanos = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|d| d.as_nanos())
                .unwrap_or(0);
            std::env::temp_dir().join(format!(
                "oracledb-k6-{tag}-{}-{nanos}.tns-cassette",
                std::process::id()
            ))
        }

        #[test]
        fn auth_window_secret_is_scrubbed_and_gate_passes() {
            // A realistic C->S auth frame carrying the password verifier + a fake
            // session key, followed by a secret-free query + response.
            let mut auth = b"KEY AUTH_PASSWORD=hunter2super AUTH_SESSKEY=".to_vec();
            auth.extend_from_slice(&[0x5A_u8; 48]); // fake session-key blob
            let frames = vec![
                (Direction::ClientToServer, b"CONNECT-DESCRIPTOR".to_vec()),
                (Direction::ServerToClient, b"ACCEPT".to_vec()),
                (Direction::ClientToServer, auth),
                (
                    Direction::ServerToClient,
                    b"AUTH_SVR_RESPONSE SESSION_KEY=deadbeefcafef00d".to_vec(),
                ),
                (Direction::ClientToServer, b"SELECT * FROM nope".to_vec()),
                (
                    Direction::ServerToClient,
                    b"ORA-00942: table or view".to_vec(),
                ),
            ];
            let bytes = cassette_of(&frames);

            let (scrubbed, report) = scrub_and_gate(&bytes).expect("gate must pass after scrub");
            assert!(report.redacted_frames >= 1, "auth frames must be redacted");
            assert_eq!(report.frame_count, frames.len());

            // C4 secret-scan: NO secret field name survives.
            assert!(
                scan_for_secret_fields(&scrubbed).is_empty(),
                "scrubbed artifact must pass the secret scan"
            );
            // The secret VALUES are gone, not just the labels.
            assert!(!contains(&scrubbed, b"hunter2super"));
            assert!(!contains(&scrubbed, b"deadbeefcafef00d"));
            // The post-auth failure is preserved for offline replay.
            assert!(contains(&scrubbed, b"ORA-00942: table or view"));
        }

        #[test]
        fn long_auth_token_is_scrubbed_without_leaving_a_suffix() {
            let leaked_suffix = b"token-tail-that-must-not-survive";
            let mut auth = b"AUTH_TOKEN=".to_vec();
            auth.extend(std::iter::repeat_n(b'x', 64 * 1024));
            auth.extend_from_slice(leaked_suffix);
            let bytes = cassette_of(&[(Direction::ClientToServer, auth)]);

            let (scrubbed, _) = scrub_and_gate(&bytes).expect("long token must be scrubbed");
            assert!(
                !contains(&scrubbed, leaked_suffix),
                "a long token suffix leaked"
            );
        }

        #[test]
        fn long_auth_password_is_scrubbed_without_leaving_a_suffix() {
            let leaked_suffix = b"password-tail-that-must-not-survive";
            let mut auth = b"AUTH_PASSWORD=".to_vec();
            auth.extend(std::iter::repeat_n(b'p', 4096));
            auth.extend_from_slice(leaked_suffix);
            let bytes = cassette_of(&[(Direction::ClientToServer, auth)]);

            let (scrubbed, _) = scrub_and_gate(&bytes).expect("long password must be scrubbed");
            assert!(
                !contains(&scrubbed, leaked_suffix),
                "a long password suffix leaked"
            );
        }

        #[test]
        fn auth_marker_and_value_split_across_frames_are_scrubbed() {
            let leaked_value = b"split-token-value-that-must-not-survive";
            let bytes = cassette_of(&[
                (Direction::ClientToServer, b"AUTH_TOKEN=".to_vec()),
                (Direction::ClientToServer, leaked_value.to_vec()),
                (Direction::ServerToClient, b"AUTH response".to_vec()),
            ]);

            let (scrubbed, _) = scrub_and_gate(&bytes).expect("split token must be scrubbed");
            assert!(
                !contains(&scrubbed, leaked_value),
                "a token value in the next transport frame leaked"
            );
            assert!(
                contains(&scrubbed, b"AUTH response"),
                "redaction must stop at the direction change"
            );
        }

        #[test]
        fn auth_marker_split_across_frames_is_scrubbed() {
            let leaked_value = b"split-marker-token-that-must-not-survive";
            let bytes = cassette_of(&[
                (Direction::ClientToServer, b"AUTH_TO".to_vec()),
                (
                    Direction::ClientToServer,
                    [b"KEN=".as_slice(), leaked_value].concat(),
                ),
                (Direction::ServerToClient, b"AUTH response".to_vec()),
            ]);

            let (scrubbed, _) = scrub_and_gate(&bytes).expect("split marker must be scrubbed");
            assert!(
                !contains(&scrubbed, leaked_value),
                "a token whose marker crosses a transport frame leaked"
            );
            assert!(
                contains(&scrubbed, b"AUTH response"),
                "redaction must stop at the direction change"
            );
        }

        #[test]
        fn auth_token_is_scrubbed_at_every_transport_split_point() {
            let leaked_value = b"every-split-token-value-must-disappear";
            let logical = [b"AUTH_TOKEN=".as_slice(), leaked_value].concat();

            for split in 1..logical.len() {
                let bytes = cassette_of(&[
                    (Direction::ClientToServer, logical[..split].to_vec()),
                    (Direction::ClientToServer, logical[split..].to_vec()),
                    (Direction::ServerToClient, b"AUTH response".to_vec()),
                ]);
                let (scrubbed, _) = scrub_and_gate(&bytes)
                    .unwrap_or_else(|err| panic!("split {split} must scrub cleanly: {err}"));
                assert!(
                    !contains(&scrubbed, leaked_value),
                    "token value leaked when the transport split at byte {split}"
                );
                assert!(
                    scan_for_secret_fields(&scrubbed).is_empty(),
                    "secret marker survived when the transport split at byte {split}"
                );
                assert!(
                    contains(&scrubbed, b"AUTH response"),
                    "opposite-direction response was over-redacted at split {split}"
                );
            }
        }

        #[test]
        fn secret_scanner_does_not_join_bytes_across_direction_changes() {
            let bytes = cassette_of(&[
                (Direction::ClientToServer, b"AUTH_TO".to_vec()),
                (
                    Direction::ServerToClient,
                    b"KEN=not-one-wire-field".to_vec(),
                ),
            ]);

            assert!(
                scan_for_secret_fields(&bytes).is_empty(),
                "opposite directions cannot form one secret field"
            );
        }

        #[test]
        fn split_secret_marker_beyond_auth_window_is_refused() {
            let mut frames: Vec<(Direction, Vec<u8>)> = (0..AUTH_PHASE_FRAME_LIMIT + 3)
                .map(|i| {
                    let direction = if i % 2 == 0 {
                        Direction::ClientToServer
                    } else {
                        Direction::ServerToClient
                    };
                    (direction, format!("benign-frame-{i}").into_bytes())
                })
                .collect();
            frames[AUTH_PHASE_FRAME_LIMIT + 1] = (Direction::ClientToServer, b"AUTH_TO".to_vec());
            frames[AUTH_PHASE_FRAME_LIMIT + 2] =
                (Direction::ClientToServer, b"KEN=post-auth-secret".to_vec());
            let bytes = cassette_of(&frames);

            match scrub_and_gate(&bytes) {
                Err(CassetteCaptureError::SecretLeak { fields }) => {
                    assert!(fields.contains(&"AUTH_TOKEN"), "gate must name the leak");
                }
                other => panic!("split post-auth marker must be REFUSED, got {other:?}"),
            }
        }

        #[test]
        fn plant_secret_beyond_auth_window_is_refused() {
            // Belt-and-suspenders: a secret that the auth-phase scrubber does NOT
            // reach (planted well past the auth window, as if leaked into user
            // traffic) is still caught by the total refuse gate.
            let mut frames: Vec<(Direction, Vec<u8>)> = (0..AUTH_PHASE_FRAME_LIMIT + 4)
                .map(|i| {
                    (
                        Direction::ClientToServer,
                        format!("benign-frame-{i}").into_bytes(),
                    )
                })
                .collect();
            // Plant beyond the auth window.
            frames[AUTH_PHASE_FRAME_LIMIT + 2] = (
                Direction::ServerToClient,
                b"leaked AUTH_TOKEN=eyJ0aGlzIjoiYSBzZWNyZXQifQ".to_vec(),
            );
            let bytes = cassette_of(&frames);

            match scrub_and_gate(&bytes) {
                Err(CassetteCaptureError::SecretLeak { fields }) => {
                    assert!(fields.contains(&"AUTH_TOKEN"), "gate must name the leak");
                }
                other => panic!("planted secret must be REFUSED, got {other:?}"),
            }
        }

        #[test]
        fn scrub_and_write_refuses_planted_secret_and_leaves_no_file() {
            let mut frames: Vec<(Direction, Vec<u8>)> = (0..AUTH_PHASE_FRAME_LIMIT + 4)
                .map(|i| {
                    (
                        Direction::ClientToServer,
                        format!("benign-{i}").into_bytes(),
                    )
                })
                .collect();
            frames[AUTH_PHASE_FRAME_LIMIT + 1] = (
                Direction::ServerToClient,
                b"oops PRIVATE_KEY=-----BEGIN".to_vec(),
            );
            let recorder = recorder_of(&frames);

            let path = unique_temp_path("refuse");
            let err = recorder
                .scrub_and_write(&path)
                .expect_err("planted secret must refuse the write");
            assert!(matches!(err, CassetteCaptureError::SecretLeak { .. }));
            assert!(
                !path.exists(),
                "a refused capture must leave NO file behind"
            );
        }

        #[test]
        fn scrub_and_write_persists_scrubbed_cassette() {
            let mut auth = b"AUTH_PASSWORD=topsecretpw AUTH_VFR_DATA=".to_vec();
            auth.extend_from_slice(&[0x11_u8; 32]);
            let frames = vec![
                (Direction::ClientToServer, b"CONNECT".to_vec()),
                (Direction::ServerToClient, b"ACCEPT".to_vec()),
                (Direction::ClientToServer, auth),
                (Direction::ClientToServer, b"SELECT 1 FROM dual".to_vec()),
                (Direction::ServerToClient, b"ORA-01013: cancelled".to_vec()),
            ];
            let recorder = recorder_of(&frames);

            let path = unique_temp_path("persist");
            let report = recorder.scrub_and_write(&path).expect("write must succeed");
            assert!(report.redacted_frames >= 1);

            let written = fs::read(&path).expect("cassette file must exist");
            assert!(scan_for_secret_fields(&written).is_empty());
            assert!(!contains(&written, b"topsecretpw"));
            assert!(contains(&written, b"ORA-01013: cancelled"));

            let _ = fs::remove_file(&path);
        }

        #[test]
        fn empty_recorder_writes_nothing_via_guard() {
            let path = unique_temp_path("empty");
            {
                let _guard = CaptureGuard::new(CassetteRecorder::new(), path.clone());
            }
            assert!(!path.exists(), "an empty session must not write a cassette");
        }

        fn contains(haystack: &[u8], needle: &[u8]) -> bool {
            !needle.is_empty()
                && needle.len() <= haystack.len()
                && haystack
                    .windows(needle.len())
                    .any(|window| window == needle)
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
    pub(crate) fn wrap_if_capturing(
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

    /// End-of-replay accounting for strict tests. The replay halves update this
    /// shared state as bytes are consumed, letting tests prove that a cassette
    /// was consumed exactly: no unread `S->C` bytes, no unchecked expected
    /// `C->S` writes, and no earlier mismatch.
    #[derive(Clone, Debug)]
    pub(crate) struct ReplayAudit {
        inner: Arc<Mutex<ReplayAuditState>>,
    }

    #[derive(Clone, Debug, Eq, PartialEq)]
    struct ReplayAuditState {
        read_frames_remaining: usize,
        read_bytes_remaining: usize,
        write_frames_remaining: usize,
        write_bytes_remaining: usize,
        mismatch: Option<ReplayMismatch>,
    }

    /// Strict replay completion failure.
    #[derive(Clone, Debug, Eq, PartialEq)]
    #[cfg(test)]
    pub(crate) enum ReplayAuditError {
        /// A replay write diverged from the recorded request stream.
        Mismatch(ReplayMismatch),
        /// Recorded `S->C` bytes remained unread at the end of replay.
        UnreadFrames { frames: usize, bytes: usize },
        /// Recorded `C->S` writes remained unmatched at the end of replay.
        UnwrittenFrames { frames: usize, bytes: usize },
        /// The audit mutex was poisoned.
        Poisoned,
    }

    #[cfg(test)]
    impl fmt::Display for ReplayAuditError {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            match self {
                Self::Mismatch(mismatch) => write!(
                    f,
                    "replay: write mismatch at frame {} (expected {} bytes, got {} bytes)",
                    mismatch.frame_index,
                    mismatch.expected.len(),
                    mismatch.actual.len()
                ),
                Self::UnreadFrames { frames, bytes } => write!(
                    f,
                    "replay: unread server frames remain ({frames} frames, {bytes} bytes)"
                ),
                Self::UnwrittenFrames { frames, bytes } => write!(
                    f,
                    "replay: expected client writes remain ({frames} frames, {bytes} bytes)"
                ),
                Self::Poisoned => f.write_str("replay: audit mutex poisoned"),
            }
        }
    }

    #[cfg(test)]
    impl std::error::Error for ReplayAuditError {}

    impl ReplayAudit {
        fn new(reads: &VecDeque<Vec<u8>>, writes: &VecDeque<Vec<u8>>) -> Self {
            Self {
                inner: Arc::new(Mutex::new(ReplayAuditState {
                    read_frames_remaining: reads.len(),
                    read_bytes_remaining: reads.iter().map(Vec::len).sum(),
                    write_frames_remaining: writes.len(),
                    write_bytes_remaining: writes.iter().map(Vec::len).sum(),
                    mismatch: None,
                })),
            }
        }

        fn note_read_bytes(&self, n: usize) {
            if let Ok(mut state) = self.inner.lock() {
                state.read_bytes_remaining = state.read_bytes_remaining.saturating_sub(n);
            }
        }

        fn note_read_frame_consumed(&self) {
            if let Ok(mut state) = self.inner.lock() {
                state.read_frames_remaining = state.read_frames_remaining.saturating_sub(1);
            }
        }

        fn note_write_bytes(&self, n: usize) {
            if let Ok(mut state) = self.inner.lock() {
                state.write_bytes_remaining = state.write_bytes_remaining.saturating_sub(n);
            }
        }

        fn note_write_frame_consumed(&self) {
            if let Ok(mut state) = self.inner.lock() {
                state.write_frames_remaining = state.write_frames_remaining.saturating_sub(1);
            }
        }

        fn note_mismatch(&self, mismatch: ReplayMismatch) {
            if let Ok(mut state) = self.inner.lock() {
                if state.mismatch.is_none() {
                    state.mismatch = Some(mismatch);
                }
            }
        }

        /// Assert that replay consumed the entire cassette exactly.
        #[cfg(test)]
        pub(crate) fn assert_finished(&self) -> Result<(), ReplayAuditError> {
            let state = self
                .inner
                .lock()
                .map_err(|_| ReplayAuditError::Poisoned)?
                .clone();
            if let Some(mismatch) = state.mismatch {
                return Err(ReplayAuditError::Mismatch(mismatch));
            }
            if state.read_frames_remaining != 0 || state.read_bytes_remaining != 0 {
                return Err(ReplayAuditError::UnreadFrames {
                    frames: state.read_frames_remaining,
                    bytes: state.read_bytes_remaining,
                });
            }
            if state.write_frames_remaining != 0 || state.write_bytes_remaining != 0 {
                return Err(ReplayAuditError::UnwrittenFrames {
                    frames: state.write_frames_remaining,
                    bytes: state.write_bytes_remaining,
                });
            }
            Ok(())
        }
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
        replay_split_inner(data, write_mode).map(|(read, write, _audit)| (read, write))
    }

    #[cfg(test)]
    pub(crate) fn replay_split_with_audit(
        data: &[u8],
        write_mode: ReplayWriteMode,
    ) -> Result<(OracleReadHalf, OracleWriteHalf, ReplayAudit), CassetteError> {
        replay_split_inner(data, write_mode)
    }

    fn replay_split_inner(
        data: &[u8],
        write_mode: ReplayWriteMode,
    ) -> Result<(OracleReadHalf, OracleWriteHalf, ReplayAudit), CassetteError> {
        let frames = cassette::decode_all(data)?;
        let mut reads: VecDeque<Vec<u8>> = VecDeque::new();
        let mut writes: VecDeque<Vec<u8>> = VecDeque::new();
        for frame in frames {
            match frame.direction {
                Direction::ServerToClient => reads.push_back(frame.bytes),
                Direction::ClientToServer => writes.push_back(frame.bytes),
            }
        }
        let audit_writes = if matches!(write_mode, ReplayWriteMode::Check) {
            writes.clone()
        } else {
            VecDeque::new()
        };
        let audit = ReplayAudit::new(&reads, &audit_writes);
        let mismatch = Arc::new(Mutex::new(None));
        Ok((
            OracleReadHalf::Replay(ReplayRead {
                pending: reads,
                offset: 0,
                audit: audit.clone(),
            }),
            OracleWriteHalf::Replay(ReplayWrite {
                expected: writes,
                offset: 0,
                index: 0,
                mode: write_mode,
                mismatch,
                audit: audit.clone(),
            }),
            audit,
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
        audit: ReplayAudit,
    }

    impl ReplayRead {
        pub(super) fn poll_read(&mut self, buf: &mut ReadBuf<'_>) -> Poll<io::Result<()>> {
            // Skip any exhausted front transfers.
            while let Some(front) = self.pending.front() {
                if self.offset >= front.len() {
                    self.pending.pop_front();
                    self.offset = 0;
                    self.audit.note_read_frame_consumed();
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
            self.audit.note_read_bytes(take);
            if self
                .pending
                .front()
                .is_some_and(|front| self.offset >= front.len())
            {
                self.pending.pop_front();
                self.offset = 0;
                self.audit.note_read_frame_consumed();
            }
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
        audit: ReplayAudit,
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
                while let Some(front) = self.expected.front() {
                    if self.offset >= front.len() {
                        self.expected.pop_front();
                        self.offset = 0;
                        self.index += 1;
                        self.audit.note_write_frame_consumed();
                    } else {
                        break;
                    }
                }
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
                self.audit.note_write_bytes(take);
                if self.offset >= front.len() {
                    self.expected.pop_front();
                    self.offset = 0;
                    self.index += 1;
                    self.audit.note_write_frame_consumed();
                }
            }
            Poll::Ready(Ok(buf.len()))
        }

        fn note_mismatch(&self, frame_index: usize, expected: &[u8], actual: &[u8]) {
            let mismatch = ReplayMismatch {
                frame_index,
                expected: expected.to_vec(),
                actual: actual.to_vec(),
            };
            if let Ok(mut slot) = self.mismatch.lock() {
                if slot.is_none() {
                    *slot = Some(mismatch.clone());
                }
            }
            self.audit.note_mismatch(mismatch);
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
        fn replay_audit_rejects_unread_server_frames() {
            let recorder = CassetteRecorder::new();
            recorder.record(Direction::ServerToClient, &[0xAA, 0xBB]);
            let bytes = recorder.into_cassette_bytes();
            let (_read, _write, audit) =
                replay_split_with_audit(&bytes, ReplayWriteMode::Check).expect("valid cassette");

            let err = audit
                .assert_finished()
                .expect_err("strict replay must reject unread server bytes");
            assert!(
                err.to_string().contains("unread server frames"),
                "unexpected error: {err}"
            );
        }

        #[test]
        fn replay_audit_rejects_unmatched_expected_writes() {
            let recorder = CassetteRecorder::new();
            recorder.record(Direction::ClientToServer, &[0x10, 0x20]);
            let bytes = recorder.into_cassette_bytes();
            let (_read, _write, audit) =
                replay_split_with_audit(&bytes, ReplayWriteMode::Check).expect("valid cassette");

            let err = audit
                .assert_finished()
                .expect_err("strict replay must reject unmatched client writes");
            assert!(
                err.to_string().contains("expected client writes"),
                "unexpected error: {err}"
            );
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
