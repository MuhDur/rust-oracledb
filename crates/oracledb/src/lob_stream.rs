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
use oracledb_protocol::thin::{LobReadResult, LobTextDecoder, LobValue};

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

    fn next_span(&self) -> Option<(u64, u64)> {
        if self.pos > self.total {
            return None;
        }
        let want = (self.total - self.pos + 1).min(self.chunk);
        Some((self.pos, want))
    }

    fn consume_read_result(&mut self, want: u64, result: LobReadResult) -> Result<(Vec<u8>, u64)> {
        let LobReadResult {
            data,
            locator,
            amount,
        } = result;
        if amount == 0 {
            return Err(oracledb_protocol::ProtocolError::TtcDecode(
                "LOB read returned zero units before declared EOF",
            )
            .into());
        }
        if amount > want {
            return Err(oracledb_protocol::ProtocolError::TtcDecode(
                "LOB read returned more units than requested",
            )
            .into());
        }
        let data = data
            .filter(|bytes| !bytes.is_empty())
            .ok_or(Error::Protocol(
                oracledb_protocol::ProtocolError::TtcDecode(
                    "LOB read returned positive progress without data",
                ),
            ))?;
        let next_pos = self.pos.checked_add(amount).ok_or(Error::Protocol(
            oracledb_protocol::ProtocolError::TtcDecode("LOB read position overflow"),
        ))?;

        if !locator.is_empty() {
            self.locator = locator;
        }
        self.pos = next_pos;
        Ok((data, amount))
    }

    /// Pull the next chunk of raw LOB bytes (character-set form for CLOB), or
    /// `None` at end of LOB.
    pub async fn read_chunk(&mut self, conn: &mut Connection, cx: &Cx) -> Result<Option<Vec<u8>>> {
        let Some((offset, want)) = self.next_span() else {
            return Ok(None);
        };
        let result = conn.read_lob(cx, &self.locator, offset, want).await?;
        let (data, actual_amount) = self.consume_read_result(want, result)?;
        let _ = actual_amount;
        // Stream-level chunk span (feature-gated, zero-cost when off): one span
        // per chunk pulled off the locator, carrying the actual unit span +
        // decoded byte size. Only counts — NEVER the LOB bytes or the locator.
        // The `oracledb.lob` round-trip span (in `read_lob`) covers the wire op;
        // this is the stream-reader's own chunk counter. Field expressions are
        // not evaluated in the default build.
        let _span = obs_span!(
            "oracledb.lob_stream",
            db.lob_chunk_units = actual_amount,
            db.lob_chunk_bytes = data.len() as u64,
        );
        Ok(Some(data))
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

#[cfg(test)]
mod tests {
    use std::io::{Read, Write};
    use std::net::TcpListener;
    use std::thread;

    use asupersync::net::TcpStream;
    use oracledb_protocol::thin::{
        build_lob_read_payload_with_seq, ClientCapabilities, TNS_DATA_FLAGS_END_OF_RESPONSE,
        TNS_MSG_TYPE_END_OF_RESPONSE, TNS_MSG_TYPE_LOB_DATA, TNS_MSG_TYPE_PARAMETER,
        TNS_PACKET_TYPE_DATA,
    };
    use oracledb_protocol::wire::{encode_packet, PacketLengthWidth, TtcWriter};

    use super::*;

    fn lob_result(locator: &[u8], amount: u64, data: &[u8]) -> LobReadResult {
        LobReadResult {
            data: Some(data.to_vec()),
            locator: locator.to_vec(),
            amount,
        }
    }

    fn lob_read_response(locator: &[u8], amount: u64, data: &[u8]) -> Vec<u8> {
        let mut payload = TtcWriter::new();
        payload.write_u8(TNS_MSG_TYPE_LOB_DATA);
        payload
            .write_bytes_with_length(data)
            .expect("test LOB data length is encodable");
        payload.write_u8(TNS_MSG_TYPE_PARAMETER);
        payload.write_raw(locator);
        // Positive SB8 values have the same TTC representation as UB8 values.
        payload.write_ub8(amount);
        payload.write_u8(TNS_MSG_TYPE_END_OF_RESPONSE);
        encode_packet(
            TNS_PACKET_TYPE_DATA,
            0,
            Some(TNS_DATA_FLAGS_END_OF_RESPONSE),
            &payload.into_bytes(),
            PacketLengthWidth::Large32,
        )
        .expect("test LOB response packet is encodable")
    }

    fn read_large_packet(stream: &mut std::net::TcpStream) -> Vec<u8> {
        let mut header = [0u8; 8];
        stream
            .read_exact(&mut header)
            .expect("client writes a complete TNS header");
        let length = u32::from_be_bytes(header[..4].try_into().expect("four length bytes"));
        let length = usize::try_from(length).expect("packet length fits usize");
        assert!(length >= header.len() + 2, "DATA packet carries flags");
        assert_eq!(header[4], TNS_PACKET_TYPE_DATA, "client writes DATA packet");
        let mut body = vec![0u8; length - header.len()];
        stream
            .read_exact(&mut body)
            .expect("client writes the complete DATA body");
        assert_eq!(&body[..2], &[0, 0], "request DATA flags are clear");
        body[2..].to_vec()
    }

    #[test]
    fn short_read_uses_actual_units_and_replacement_locator() {
        let mut reader = LobReader::from_parts(vec![0x11; 4], 11, 8);

        let (data, units) = reader
            .consume_read_result(8, lob_result(&[0x22; 4], 3, b"abc"))
            .expect("valid short read must be accepted");

        assert_eq!(data, b"abc");
        assert_eq!(units, 3);
        assert_eq!(reader.locator(), &[0x22; 4]);
        assert_eq!(reader.next_span(), Some((4, 8)));
    }

    #[test]
    fn clob_progress_uses_character_units_not_encoded_bytes() {
        let mut reader = LobReader::from_parts(vec![0x11; 4], 4, 4);
        let utf8 = "é😀".as_bytes();

        let (data, units) = reader
            .consume_read_result(4, lob_result(&[], 2, utf8))
            .expect("two returned character units are valid");

        assert_eq!(data, utf8);
        assert_eq!(units, 2);
        assert_eq!(reader.next_span(), Some((3, 2)));
    }

    #[test]
    fn zero_progress_before_declared_eof_is_rejected_without_mutation() {
        let mut reader = LobReader::from_parts(vec![0x11; 4], 8, 8);

        let err = reader
            .consume_read_result(8, lob_result(&[0x22; 4], 0, b""))
            .expect_err("zero progress before declared EOF must fail closed");

        assert!(matches!(err, Error::Protocol(_)), "got {err:?}");
        assert_eq!(reader.locator(), &[0x11; 4]);
        assert_eq!(reader.next_span(), Some((1, 8)));
    }

    #[test]
    fn oversized_progress_is_rejected_without_mutation() {
        let mut reader = LobReader::from_parts(vec![0x11; 4], 8, 8);

        let err = reader
            .consume_read_result(8, lob_result(&[0x22; 4], 9, b"123456789"))
            .expect_err("server cannot claim more units than requested");

        assert!(matches!(err, Error::Protocol(_)), "got {err:?}");
        assert_eq!(reader.locator(), &[0x11; 4]);
        assert_eq!(reader.next_span(), Some((1, 8)));
    }

    #[test]
    fn positive_progress_without_data_is_rejected_without_mutation() {
        let mut reader = LobReader::from_parts(vec![0x11; 4], 8, 8);

        let err = reader
            .consume_read_result(
                8,
                LobReadResult {
                    data: None,
                    locator: vec![0x22; 4],
                    amount: 3,
                },
            )
            .expect_err("positive progress without a payload must fail closed");

        assert!(matches!(err, Error::Protocol(_)), "got {err:?}");
        assert_eq!(reader.locator(), &[0x11; 4]);
        assert_eq!(reader.next_span(), Some((1, 8)));
    }

    #[test]
    fn position_overflow_is_rejected_without_mutation() {
        let mut reader = LobReader::from_parts(vec![0x11; 4], u64::MAX, 1);
        reader.pos = u64::MAX;

        let err = reader
            .consume_read_result(1, lob_result(&[0x22; 4], 1, b"x"))
            .expect_err("next 1-based position cannot wrap to zero");

        assert!(matches!(err, Error::Protocol(_)), "got {err:?}");
        assert_eq!(reader.locator(), &[0x11; 4]);
        assert_eq!(reader.next_span(), Some((u64::MAX, 1)));
    }

    #[test]
    fn full_chunk_keeps_locator_when_server_returns_no_replacement() {
        let mut reader = LobReader::from_parts(vec![0x11; 4], 8, 8);

        let (data, units) = reader
            .consume_read_result(8, lob_result(&[], 8, b"12345678"))
            .expect("full read must be accepted");

        assert_eq!(data, b"12345678");
        assert_eq!(units, 8);
        assert_eq!(reader.locator(), &[0x11; 4]);
        assert!(reader.is_eof());
    }

    #[test]
    fn read_to_end_preserves_bytes_across_short_read_and_locator_change() -> Result<()> {
        let first_locator = vec![0x11; 4];
        let next_locator = vec![0x22; 4];
        let expected_data = b"abcdefghijk";
        let listener = TcpListener::bind("127.0.0.1:0")?;
        let addr = listener.local_addr()?;
        let responses = [
            lob_read_response(&next_locator, 3, &expected_data[..3]),
            lob_read_response(&next_locator, 8, &expected_data[3..]),
        ];
        let server = thread::spawn(move || {
            let (mut stream, _) = listener.accept().expect("accept LOB test client");
            responses
                .into_iter()
                .map(|response| {
                    let request = read_large_packet(&mut stream);
                    stream
                        .write_all(&response)
                        .expect("write scripted LOB response");
                    request
                })
                .collect::<Vec<_>>()
        });

        let runtime = crate::new_io_runtime()?;
        let (actual_data, final_locator) = runtime.block_on(async {
            let cx = Cx::current()
                .ok_or_else(|| Error::Runtime("missing ambient Cx in LOB test".into()))?;
            let stream = TcpStream::connect(addr).await?;
            let (read, write) = crate::transport::plain_split(stream);
            let mut conn = crate::tests::loopback_connection(read, write);
            let mut reader = LobReader::from_parts(first_locator.clone(), 11, 8);
            let data = reader.read_to_end(&mut conn, &cx).await?;
            Ok::<_, Error>((data, reader.locator().to_vec()))
        })?;
        let requests = server.join().expect("LOB test server joins");
        let ttc_field_version = ClientCapabilities::default().ttc_field_version;

        assert_eq!(actual_data, expected_data);
        assert_eq!(final_locator, next_locator);
        assert_eq!(
            requests,
            vec![
                build_lob_read_payload_with_seq(&first_locator, 1, 8, 1, ttc_field_version)?,
                build_lob_read_payload_with_seq(&next_locator, 4, 8, 2, ttc_field_version)?,
            ],
            "the second round trip must continue from the actual amount with the returned locator"
        );
        Ok(())
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
            Some(bytes) => {
                let text = self.decoder.push(&bytes).map_err(Error::Protocol)?;
                // UTF-16 / multi-byte boundary-split span (feature-gated,
                // zero-cost when off). A split means this chunk ended in the
                // middle of a codepoint or a surrogate pair, so the decoder is
                // carrying an incomplete tail to the next chunk. Detected with a
                // cheap clone + `finish()` probe over the decoder's PUBLIC
                // surface (`finish()` errors iff a partial unit / dangling high
                // surrogate remains) — no decoder internals, no public API
                // change. Because `obs_span!` discards its field expressions in
                // the default build, the clone/`finish()` probe runs ONLY under
                // the feature (zero-cost when off). Only a boolean + a char count
                // are recorded — never the decoded text.
                let _span = obs_span!(
                    "oracledb.lob_stream_text",
                    db.lob_utf16_boundary_split = self.decoder.clone().finish().is_err(),
                    db.lob_chunk_chars = text.chars().count() as u64,
                );
                Ok(Some(text))
            }
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
