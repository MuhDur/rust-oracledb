//! `.tns-cassette` wire format: a sans-I/O, deterministic framing of one Oracle
//! session's raw byte stream, for offline record/replay.
//!
//! A cassette captures the FULL transport byte stream of a session — every
//! client-to-server (`C->S`) write and every server-to-client (`S->C`) read,
//! in the exact order the driver issued them, from connect through close. The
//! format is intentionally trivial and self-describing so a captured production
//! session can be replayed offline with **no database** to reproduce a wire bug.
//!
//! This module is pure: it only encodes and decodes frames in memory. The
//! recording / replay *transports* that tee a live socket into a cassette (or
//! serve a cassette to the decoder with no socket) live in the driver crate's
//! `transport` module, behind the `cassette` feature.
//!
//! # Binary layout
//!
//! ```text
//! magic    : 8 bytes  = b"TNSCASS\0"
//! version  : 1 byte   = CASSETTE_VERSION
//! ----- repeated, one per captured transfer (frame) -----
//! direction: 1 byte   = 0x01 (C->S / client write) | 0x02 (S->C / server read)
//! micros   : 8 bytes  LE = microseconds since the first frame (informational;
//!                          IGNORED on replay so the replay path is clock-free
//!                          and byte-deterministic)
//! length   : 4 bytes  LE = number of payload bytes that follow
//! payload  : length bytes = the raw transport bytes of this transfer
//! ```
//!
//! All integers are little-endian. There is no trailing index or checksum: the
//! frame sequence ends at end-of-file. Decoding is strict — a truncated frame,
//! a bad magic, or an unknown version is an error rather than a silent partial
//! read, so a corrupt cassette fails loudly instead of replaying garbage.

use std::fmt;

/// Magic bytes at the start of every `.tns-cassette` file.
pub const CASSETTE_MAGIC: [u8; 8] = *b"TNSCASS\0";

/// Current cassette format version. Bumped on any incompatible layout change.
pub const CASSETTE_VERSION: u8 = 1;

/// Direction of a captured transfer, from the driver's point of view.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Direction {
    /// Client-to-server: bytes the driver WROTE to the transport.
    ClientToServer,
    /// Server-to-client: bytes the driver READ from the transport.
    ServerToClient,
}

impl Direction {
    /// On-wire tag byte for this direction.
    #[must_use]
    pub fn tag(self) -> u8 {
        match self {
            Self::ClientToServer => 0x01,
            Self::ServerToClient => 0x02,
        }
    }

    /// Parse a direction tag byte.
    fn from_tag(tag: u8) -> Result<Self, CassetteError> {
        match tag {
            0x01 => Ok(Self::ClientToServer),
            0x02 => Ok(Self::ServerToClient),
            other => Err(CassetteError::BadDirection(other)),
        }
    }
}

/// A single captured transfer: a direction, a relative timestamp, and the raw
/// bytes of one read or write.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Frame {
    /// Whether the driver wrote (`C->S`) or read (`S->C`) these bytes.
    pub direction: Direction,
    /// Microseconds since the first frame of the session. Informational only;
    /// the replay path never consults it, so replay is clock-independent.
    pub micros: u64,
    /// The raw transport bytes of this transfer.
    pub bytes: Vec<u8>,
}

/// Errors from decoding a `.tns-cassette` byte stream.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum CassetteError {
    /// The leading magic bytes did not match [`CASSETTE_MAGIC`].
    BadMagic,
    /// The version byte did not match [`CASSETTE_VERSION`].
    UnsupportedVersion(u8),
    /// A direction tag byte was not `0x01` or `0x02`.
    BadDirection(u8),
    /// The stream ended in the middle of a header or payload.
    Truncated {
        /// What the decoder was trying to read when bytes ran out.
        wanted: &'static str,
    },
}

impl fmt::Display for CassetteError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::BadMagic => f.write_str("not a .tns-cassette (bad magic header)"),
            Self::UnsupportedVersion(v) => write!(f, "unsupported cassette version {v}"),
            Self::BadDirection(b) => write!(f, "invalid frame direction tag {b:#04x}"),
            Self::Truncated { wanted } => write!(f, "truncated cassette: expected {wanted}"),
        }
    }
}

impl std::error::Error for CassetteError {}

/// Append the 9-byte cassette header (magic + version) to `out`.
///
/// Call once before writing any frames.
pub fn write_header(out: &mut Vec<u8>) {
    out.extend_from_slice(&CASSETTE_MAGIC);
    out.push(CASSETTE_VERSION);
}

/// Append one encoded frame to `out`.
///
/// The frame is `direction (1) | micros LE (8) | length LE (4) | bytes`.
pub fn write_frame(out: &mut Vec<u8>, direction: Direction, micros: u64, bytes: &[u8]) {
    out.push(direction.tag());
    out.extend_from_slice(&micros.to_le_bytes());
    // A transport read/write never exceeds u32 in practice (TNS SDU is far
    // smaller); clamp defensively so a pathological buffer can't wrap.
    let len = u32::try_from(bytes.len()).unwrap_or(u32::MAX);
    out.extend_from_slice(&len.to_le_bytes());
    out.extend_from_slice(&bytes[..len as usize]);
}

/// A forward cursor over the frames of an encoded cassette byte stream.
///
/// Decoding is lazy and allocation-light: each [`next`](Reader::next) call
/// validates one frame header and copies that frame's payload out.
#[derive(Clone, Debug)]
pub struct Reader<'a> {
    rest: &'a [u8],
}

impl<'a> Reader<'a> {
    /// Validate the header of `data` and return a [`Reader`] positioned at the
    /// first frame.
    pub fn new(data: &'a [u8]) -> Result<Self, CassetteError> {
        let (magic, rest) = data
            .split_at_checked(CASSETTE_MAGIC.len())
            .ok_or(CassetteError::Truncated { wanted: "magic" })?;
        if magic != CASSETTE_MAGIC {
            return Err(CassetteError::BadMagic);
        }
        let (version, rest) = rest
            .split_first()
            .ok_or(CassetteError::Truncated { wanted: "version" })?;
        if *version != CASSETTE_VERSION {
            return Err(CassetteError::UnsupportedVersion(*version));
        }
        Ok(Self { rest })
    }

    /// Decode the next frame, or `None` at clean end-of-stream.
    ///
    /// Returns `Err` if the stream ends partway through a frame.
    #[allow(clippy::should_implement_trait)] // fallible, not std::iter::Iterator
    pub fn next(&mut self) -> Result<Option<Frame>, CassetteError> {
        if self.rest.is_empty() {
            return Ok(None);
        }
        let (tag, rest) = self.rest.split_first().ok_or(CassetteError::Truncated {
            wanted: "direction",
        })?;
        let direction = Direction::from_tag(*tag)?;
        let (micros_bytes, rest) = rest.split_at_checked(8).ok_or(CassetteError::Truncated {
            wanted: "timestamp",
        })?;
        let micros =
            u64::from_le_bytes(
                micros_bytes
                    .try_into()
                    .map_err(|_| CassetteError::Truncated {
                        wanted: "timestamp",
                    })?,
            );
        let (len_bytes, rest) = rest
            .split_at_checked(4)
            .ok_or(CassetteError::Truncated { wanted: "length" })?;
        let len = u32::from_le_bytes(
            len_bytes
                .try_into()
                .map_err(|_| CassetteError::Truncated { wanted: "length" })?,
        ) as usize;
        let (payload, rest) = rest
            .split_at_checked(len)
            .ok_or(CassetteError::Truncated { wanted: "payload" })?;
        self.rest = rest;
        Ok(Some(Frame {
            direction,
            micros,
            bytes: payload.to_vec(),
        }))
    }
}

/// Decode an entire cassette into its frames (eager convenience over
/// [`Reader`]). Returns an error on a bad header or any truncated frame.
pub fn decode_all(data: &[u8]) -> Result<Vec<Frame>, CassetteError> {
    let mut reader = Reader::new(data)?;
    let mut frames = Vec::new();
    while let Some(frame) = reader.next()? {
        frames.push(frame);
    }
    Ok(frames)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn build(frames: &[(Direction, u64, &[u8])]) -> Vec<u8> {
        let mut out = Vec::new();
        write_header(&mut out);
        for (dir, micros, bytes) in frames {
            write_frame(&mut out, *dir, *micros, bytes);
        }
        out
    }

    #[test]
    fn header_is_magic_then_version() {
        let mut out = Vec::new();
        write_header(&mut out);
        assert_eq!(&out[..8], b"TNSCASS\0");
        assert_eq!(out[8], CASSETTE_VERSION);
        assert_eq!(out.len(), 9);
    }

    #[test]
    fn roundtrips_a_couple_of_framed_packets_in_order() {
        // A tiny hand-crafted cassette: one client write, one server read.
        let encoded = build(&[
            (Direction::ClientToServer, 0, &[0xDE, 0xAD]),
            (Direction::ServerToClient, 125, &[0xBE, 0xEF, 0x00]),
        ]);

        let frames = decode_all(&encoded).expect("hand-crafted cassette should decode");
        assert_eq!(frames.len(), 2);
        assert_eq!(frames[0].direction, Direction::ClientToServer);
        assert_eq!(frames[0].micros, 0);
        assert_eq!(frames[0].bytes, vec![0xDE, 0xAD]);
        assert_eq!(frames[1].direction, Direction::ServerToClient);
        assert_eq!(frames[1].micros, 125);
        assert_eq!(frames[1].bytes, vec![0xBE, 0xEF, 0x00]);
    }

    #[test]
    fn record_then_replay_equals_input() {
        // record(x) then replay == x : the encoded frame bytes must decode back
        // to exactly the bytes that were written, preserving direction + order.
        let payloads: Vec<(Direction, Vec<u8>)> = vec![
            (Direction::ClientToServer, vec![1, 2, 3, 4, 5]),
            (Direction::ServerToClient, vec![]), // empty transfer is legal
            (Direction::ServerToClient, vec![9; 300]),
            (Direction::ClientToServer, vec![0xFF, 0x00, 0xFF]),
        ];
        let mut out = Vec::new();
        write_header(&mut out);
        for (dir, bytes) in &payloads {
            write_frame(&mut out, *dir, 0, bytes);
        }

        let frames = decode_all(&out).expect("self-built cassette should decode");
        let replayed: Vec<(Direction, Vec<u8>)> =
            frames.into_iter().map(|f| (f.direction, f.bytes)).collect();
        assert_eq!(replayed, payloads);
    }

    #[test]
    fn reader_yields_frames_lazily_then_none() {
        let encoded = build(&[(Direction::ServerToClient, 7, &[42])]);
        let mut reader = Reader::new(&encoded).expect("valid header");
        let first = reader.next().expect("ok").expect("one frame");
        assert_eq!(first.bytes, vec![42]);
        assert!(reader.next().expect("ok").is_none());
    }

    #[test]
    fn rejects_bad_magic() {
        let err = Reader::new(b"NOTACASS\x01").expect_err("bad magic must fail");
        assert_eq!(err, CassetteError::BadMagic);
    }

    #[test]
    fn rejects_unsupported_version() {
        let mut data = CASSETTE_MAGIC.to_vec();
        data.push(99);
        let err = Reader::new(&data).expect_err("bad version must fail");
        assert_eq!(err, CassetteError::UnsupportedVersion(99));
    }

    #[test]
    fn rejects_truncated_payload() {
        let mut encoded = build(&[(Direction::ClientToServer, 0, &[1, 2, 3, 4])]);
        encoded.truncate(encoded.len() - 2); // chop off last 2 payload bytes
        let err = decode_all(&encoded).expect_err("truncated payload must fail");
        assert_eq!(err, CassetteError::Truncated { wanted: "payload" });
    }

    #[test]
    fn rejects_bad_direction_tag() {
        let mut encoded = Vec::new();
        write_header(&mut encoded);
        encoded.push(0x09); // not a valid direction
        encoded.extend_from_slice(&0u64.to_le_bytes());
        encoded.extend_from_slice(&0u32.to_le_bytes());
        let err = decode_all(&encoded).expect_err("bad direction must fail");
        assert_eq!(err, CassetteError::BadDirection(0x09));
    }
}
