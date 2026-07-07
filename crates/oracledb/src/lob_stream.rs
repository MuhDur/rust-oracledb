//! Lazy, on-demand streaming over LOB locators.
//!
//! A fetched LOB column yields a [`LobValue`] locator, not its bytes: the value
//! is pulled from the server separately with [`Connection::read_lob`] /
//! [`Connection::write_lob`]. These wrappers turn that piecewise primitive into
//! a lazy reader/writer that walks a LOB in fixed-size chunks, so a multi-gigabyte
//! LOB never has to be materialised in one buffer.
//!
//! The reader is *pull-based* (`async fn read_chunk`) rather than a poll-based
//! [`AsyncRead`](asupersync::io::AsyncRead): every LOB round trip needs
//! `&mut Connection`, and threading that borrow through a `poll_read(Pin<&mut
//! Self>, ..)` signature would require a self-referential future — i.e. `unsafe`,
//! which this crate forbids. The pull model mirrors python-oracledb's
//! `LOB.read(offset, amount)` and composes cleanly with the connection borrow.
//!
//! Oracle LOB offsets and amounts are **unit**-based: bytes for BLOB/BFILE,
//! characters for CLOB/NCLOB. [`LobReader`] is generic over either (it just
//! forwards the unit counts); [`ClobReader`] layers character decoding on top,
//! stitching multi-byte codepoints and UTF-16 surrogate pairs across chunk
//! boundaries via [`LobTextDecoder`]. [`LobWriter`] is byte-oriented (BLOB): a
//! CLOB writer would have to track character offsets against the server charset,
//! which the caller drives explicitly with `write_lob`.

use asupersync::Cx;
use oracledb_protocol::thin::{LobTextDecoder, LobValue};

use crate::{Connection, Error, Result};

/// A lazy reader over a LOB locator. Pulls the LOB in `chunk`-unit batches on
/// demand, tracking a 1-based cursor. Units are bytes for BLOB and characters
/// for CLOB/NCLOB (Oracle LOB semantics).
#[derive(Clone, Debug)]
pub struct LobReader {
    locator: Vec<u8>,
    /// Total size in units, as reported by the fetched locator.
    total: u64,
    /// 1-based position of the next unit to read.
    pos: u64,
    /// Units requested per round trip (at least 1).
    chunk: u64,
}

impl LobReader {
    /// Build a reader for a fetched LOB, pulling `chunk` units per round trip.
    pub fn new(lob: &LobValue, chunk: u64) -> Self {
        Self::from_parts(lob.locator.clone(), lob.size, chunk)
    }

    /// Build a reader from a raw locator and its unit size (e.g. a temporary
    /// LOB the caller created and wrote).
    pub fn from_parts(locator: Vec<u8>, size: u64, chunk: u64) -> Self {
        LobReader {
            locator,
            total: size,
            pos: 1,
            chunk: chunk.max(1),
        }
    }

    /// Whether the whole LOB has been consumed.
    pub fn is_eof(&self) -> bool {
        self.pos > self.total
    }

    /// The locator this reader walks.
    pub fn locator(&self) -> &[u8] {
        &self.locator
    }

    /// Pull the next chunk of raw LOB bytes (character-set form for CLOB), or
    /// `None` at end of LOB.
    pub async fn read_chunk(&mut self, conn: &mut Connection, cx: &Cx) -> Result<Option<Vec<u8>>> {
        if self.pos > self.total {
            return Ok(None);
        }
        let want = (self.total - self.pos + 1).min(self.chunk);
        let result = conn.read_lob(cx, &self.locator, self.pos, want).await?;
        self.pos += want;
        Ok(Some(result.data.unwrap_or_default()))
    }

    /// Drain the whole LOB into one buffer, one chunk at a time.
    pub async fn read_to_end(&mut self, conn: &mut Connection, cx: &Cx) -> Result<Vec<u8>> {
        let mut out = Vec::with_capacity(usize::try_from(self.total).unwrap_or(0));
        while let Some(chunk) = self.read_chunk(conn, cx).await? {
            out.extend_from_slice(&chunk);
        }
        Ok(out)
    }
}

/// A lazy CLOB/NCLOB reader that decodes streamed character data. Chunk
/// boundaries that split a multi-byte codepoint or a UTF-16 surrogate pair are
/// stitched across reads, so the concatenated text is identical to a whole
/// decode.
#[derive(Clone, Debug)]
pub struct ClobReader {
    inner: LobReader,
    decoder: LobTextDecoder,
}

impl ClobReader {
    /// Build a text reader for a fetched CLOB/NCLOB, pulling `chunk` characters
    /// per round trip.
    pub fn new(lob: &LobValue, chunk: u64) -> Self {
        ClobReader {
            inner: LobReader::new(lob, chunk),
            decoder: LobTextDecoder::from_lob(lob.csfrm, Some(&lob.locator)),
        }
    }

    /// Pull the next chunk and return whatever complete text it decoded to
    /// (possibly empty when a chunk ends mid-codepoint), or `None` at end.
    pub async fn read_text_chunk(
        &mut self,
        conn: &mut Connection,
        cx: &Cx,
    ) -> Result<Option<String>> {
        match self.inner.read_chunk(conn, cx).await? {
            Some(bytes) => Ok(Some(self.decoder.push(&bytes).map_err(Error::Protocol)?)),
            None => Ok(None),
        }
    }

    /// Read the whole CLOB into a `String`, asserting the stream ended on a
    /// codepoint boundary.
    pub async fn read_to_string(mut self, conn: &mut Connection, cx: &Cx) -> Result<String> {
        let mut out = String::new();
        while let Some(piece) = self.read_text_chunk(conn, cx).await? {
            out.push_str(&piece);
        }
        self.decoder.finish().map_err(Error::Protocol)?;
        Ok(out)
    }
}

/// A byte-oriented lazy writer over a (typically temporary) BLOB locator. Each
/// [`write_chunk`](Self::write_chunk) appends at the running byte cursor.
#[derive(Clone, Debug)]
pub struct LobWriter {
    locator: Vec<u8>,
    /// 1-based byte offset of the next write.
    pos: u64,
}

impl LobWriter {
    /// Start a writer at the beginning of `locator`.
    pub fn new(locator: Vec<u8>) -> Self {
        LobWriter { locator, pos: 1 }
    }

    /// The (possibly server-updated) locator.
    pub fn locator(&self) -> &[u8] {
        &self.locator
    }

    /// Consume the writer, returning its locator (e.g. to free the temp LOB).
    pub fn into_locator(self) -> Vec<u8> {
        self.locator
    }

    /// Append `data` at the current byte cursor. A write may return an updated
    /// locator (temporary LOBs), which is adopted for subsequent writes.
    pub async fn write_chunk(&mut self, conn: &mut Connection, cx: &Cx, data: &[u8]) -> Result<()> {
        if data.is_empty() {
            return Ok(());
        }
        let result = conn.write_lob(cx, &self.locator, self.pos, data).await?;
        if !result.locator.is_empty() {
            self.locator = result.locator;
        }
        self.pos += data.len() as u64;
        Ok(())
    }
}
