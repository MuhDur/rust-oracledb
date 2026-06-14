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

use std::io;
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll};

use asupersync::io::{AsyncRead, AsyncWrite, ReadBuf};
use asupersync::net::{OwnedReadHalf, OwnedWriteHalf, TcpStream};
use asupersync::tls::TlsStream;

/// A TLS stream shared between the read and write halves.
type SharedTls = Arc<Mutex<TlsStream<TcpStream>>>;

/// The read half of an Oracle connection transport.
pub enum OracleReadHalf {
    /// Plain TCP read half (non-TLS).
    Plain(OwnedReadHalf),
    /// Shared TLS stream (read side).
    Tls(SharedTls),
}

/// The write half of an Oracle connection transport.
pub enum OracleWriteHalf {
    /// Plain TCP write half (non-TLS).
    Plain(OwnedWriteHalf),
    /// Shared TLS stream (write side).
    Tls(SharedTls),
}

impl std::fmt::Debug for OracleReadHalf {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Plain(_) => f.write_str("OracleReadHalf::Plain"),
            Self::Tls(_) => f.write_str("OracleReadHalf::Tls"),
        }
    }
}

impl std::fmt::Debug for OracleWriteHalf {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Plain(_) => f.write_str("OracleWriteHalf::Plain"),
            Self::Tls(_) => f.write_str("OracleWriteHalf::Tls"),
        }
    }
}

/// Build a plain (non-TLS) read/write half pair from a connected TCP stream.
#[must_use]
pub fn plain_split(stream: TcpStream) -> (OracleReadHalf, OracleWriteHalf) {
    let (read, write) = stream.into_split();
    (OracleReadHalf::Plain(read), OracleWriteHalf::Plain(write))
}

/// Build a TLS read/write half pair from an established [`TlsStream`].
#[must_use]
pub fn tls_split(stream: TlsStream<TcpStream>) -> (OracleReadHalf, OracleWriteHalf) {
    let shared: SharedTls = Arc::new(Mutex::new(stream));
    (
        OracleReadHalf::Tls(Arc::clone(&shared)),
        OracleWriteHalf::Tls(shared),
    )
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
        }
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        match self.get_mut() {
            Self::Plain(write) => Pin::new(write).poll_flush(cx),
            Self::Tls(shared) => {
                let mut guard = shared.lock().map_err(|_| poisoned())?;
                Pin::new(&mut *guard).poll_flush(cx)
            }
        }
    }

    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        match self.get_mut() {
            Self::Plain(write) => Pin::new(write).poll_shutdown(cx),
            Self::Tls(shared) => {
                let mut guard = shared.lock().map_err(|_| poisoned())?;
                Pin::new(&mut *guard).poll_shutdown(cx)
            }
        }
    }
}
