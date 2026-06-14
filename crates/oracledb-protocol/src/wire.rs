#![forbid(unsafe_code)]

use crate::{ProtocolError, Result};

pub const TNS_MAX_SHORT_LENGTH: usize = 252;
pub const TNS_LONG_LENGTH_INDICATOR: u8 = 0xfe;
pub const TNS_NULL_LENGTH_INDICATOR: u8 = 0xff;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum PacketLengthWidth {
    Legacy16,
    Large32,
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct TtcWriter {
    bytes: Vec<u8>,
    seq_num: u8,
}

impl TtcWriter {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn into_bytes(self) -> Vec<u8> {
        self.bytes
    }

    pub fn write_u8(&mut self, value: u8) {
        self.bytes.push(value);
    }

    pub fn write_u16be(&mut self, value: u16) {
        self.bytes.extend_from_slice(&value.to_be_bytes());
    }

    pub fn write_u16le(&mut self, value: u16) {
        self.bytes.extend_from_slice(&value.to_le_bytes());
    }

    pub fn write_u32be(&mut self, value: u32) {
        self.bytes.extend_from_slice(&value.to_be_bytes());
    }

    pub fn write_u64be(&mut self, value: u64) {
        self.bytes.extend_from_slice(&value.to_be_bytes());
    }

    pub fn write_ub2(&mut self, value: u16) {
        if value == 0 {
            self.write_u8(0);
        } else if value <= u16::from(u8::MAX) {
            self.write_u8(1);
            self.write_u8(value as u8);
        } else {
            self.write_u8(2);
            self.write_u16be(value);
        }
    }

    pub fn write_ub4(&mut self, value: u32) {
        if value == 0 {
            self.write_u8(0);
        } else if value <= u32::from(u8::MAX) {
            self.write_u8(1);
            self.write_u8(value as u8);
        } else if value <= u32::from(u16::MAX) {
            self.write_u8(2);
            self.write_u16be(value as u16);
        } else {
            self.write_u8(4);
            self.write_u32be(value);
        }
    }

    pub fn write_ub8(&mut self, value: u64) {
        if value == 0 {
            self.write_u8(0);
        } else if value <= u64::from(u8::MAX) {
            self.write_u8(1);
            self.write_u8(value as u8);
        } else if value <= u64::from(u16::MAX) {
            self.write_u8(2);
            self.write_u16be(value as u16);
        } else if value <= u64::from(u32::MAX) {
            self.write_u8(4);
            self.write_u32be(value as u32);
        } else {
            self.write_u8(8);
            self.write_u64be(value);
        }
    }

    pub fn write_seq_num(&mut self) {
        self.seq_num = self.seq_num.wrapping_add(1);
        if self.seq_num == 0 {
            self.seq_num = 1;
        }
        self.write_u8(self.seq_num);
    }

    pub fn write_raw(&mut self, value: &[u8]) {
        self.bytes.extend_from_slice(value);
    }

    pub fn write_bytes_with_length(&mut self, value: &[u8]) -> Result<()> {
        if value.len() <= TNS_MAX_SHORT_LENGTH {
            self.write_u8(value.len() as u8);
            self.write_raw(value);
            return Ok(());
        }
        self.write_u8(TNS_LONG_LENGTH_INDICATOR);
        for chunk in value.chunks(32_767) {
            self.write_ub4(u32::try_from(chunk.len()).map_err(|_| {
                ProtocolError::InvalidPacketLength {
                    length: chunk.len(),
                    minimum: 0,
                }
            })?);
            self.write_raw(chunk);
        }
        self.write_ub4(0);
        Ok(())
    }

    pub fn write_bytes_with_two_lengths(&mut self, value: Option<&[u8]>) -> Result<()> {
        match value {
            Some(bytes) => {
                self.write_ub4(u32::try_from(bytes.len()).map_err(|_| {
                    ProtocolError::InvalidPacketLength {
                        length: bytes.len(),
                        minimum: 0,
                    }
                })?);
                if !bytes.is_empty() {
                    self.write_bytes_with_length(bytes)?;
                }
            }
            None => self.write_ub4(0),
        }
        Ok(())
    }

    pub fn write_str_two_lengths(&mut self, value: &str) -> Result<()> {
        self.write_bytes_with_two_lengths(Some(value.as_bytes()))
    }

    /// Writes a 32-bit signed integer in Oracle universal (sign-magnitude)
    /// format: a length byte whose high bit (`0x80`) is set for negatives,
    /// followed by the big-endian magnitude bytes. Mirrors the reference
    /// `WriteBuffer.write_sb4` (impl/base/buffer.pyx).
    pub fn write_sb4(&mut self, value: i32) {
        let (sign, magnitude) = if value < 0 {
            (0x80u8, value.unsigned_abs())
        } else {
            (0u8, value as u32)
        };
        if magnitude == 0 {
            self.write_u8(0);
        } else if magnitude <= u32::from(u8::MAX) {
            self.write_u8(1 | sign);
            self.write_u8(magnitude as u8);
        } else if magnitude <= u32::from(u16::MAX) {
            self.write_u8(2 | sign);
            self.write_u16be(magnitude as u16);
        } else {
            self.write_u8(4 | sign);
            self.write_u32be(magnitude);
        }
    }

    /// Writes a keyword/value pair (text and binary values plus a ub2 keyword)
    /// as used by the AQ message-property extension list. Mirrors the reference
    /// `WriteBuffer.write_keyword_value_pair` (impl/thin/packet.pyx:859).
    pub fn write_keyword_value_pair(
        &mut self,
        text_value: Option<&[u8]>,
        binary_value: Option<&[u8]>,
        keyword: u16,
    ) -> Result<()> {
        self.write_bytes_with_two_lengths(text_value)?;
        self.write_bytes_with_two_lengths(binary_value)?;
        self.write_ub2(keyword);
        Ok(())
    }

    pub fn write_function_code(&mut self, function_code: u8) {
        self.write_u8(crate::thin::TNS_MSG_TYPE_FUNCTION);
        self.write_u8(function_code);
        self.write_seq_num();
    }

    pub fn write_function_code_with_seq(&mut self, function_code: u8, seq_num: u8) {
        self.write_u8(crate::thin::TNS_MSG_TYPE_FUNCTION);
        self.write_u8(function_code);
        self.write_u8(seq_num);
    }
}

/// The structural OOM-from-length invariant for every wire decoder.
///
/// A length/count field read from the wire can **never** drive an allocation
/// larger than the bytes actually remaining in the current message buffer: you
/// cannot have `N` elements if fewer than `N * min_bytes_per_elem` bytes remain.
/// Every reader over an untrusted buffer (`TtcReader`, the OSON / DbObject /
/// notification cursors, the VECTOR reader) implements this trait, and every
/// count-driven `Vec::with_capacity` / `reserve` in the decoders routes through
/// one of its two methods instead of trusting a raw `u16`/`u32`/`u64` count.
///
/// This closes the OOM-from-length bug class *by construction*: a new decoder
/// physically cannot pre-allocate from a wire count without going through a
/// bound, because the raw `Vec::with_capacity(count)` shape is the thing we
/// audit against (see `docs/FUZZING.md`).
///
/// Two flavors, both anchored on [`remaining`](Self::remaining):
///
/// * [`alloc_count_checked`](Self::alloc_count_checked) — fail *closed* early:
///   returns an `Err` if the declared count cannot possibly fit, before any
///   allocation. Use where an oversized count is unambiguously malformed.
/// * [`with_capacity_bounded`](Self::with_capacity_bounded) — cap *the
///   pre-allocation* at what the buffer could hold while still returning a
///   normal growable `Vec`. Use where the loop body itself fails closed on the
///   first truncated element read; legitimate large payloads keep working
///   because the cap equals the honest count whenever the bytes are really
///   there.
pub trait BoundedReader {
    /// Bytes still unread in the current message buffer. The ceiling on any
    /// count-driven allocation.
    fn remaining(&self) -> usize;

    /// Validate a server-declared element `count` against the buffer: a run of
    /// `count` elements must carry at least `count * min_bytes_per_elem` bytes,
    /// so a count whose minimum byte footprint exceeds [`remaining`] is a lie.
    /// Returns the (unchanged) `count` when it fits, or a fail-closed
    /// [`ProtocolError::TtcDecode`] otherwise — never a panic, never an OOM.
    ///
    /// `min_bytes_per_elem` is the *minimum* on-wire size of one element (e.g.
    /// 4 for a `u32` index, 8 for an `f64`, 1 for a length-prefixed field whose
    /// shortest legal form is a single length byte). A zero is treated as 1.
    fn alloc_count_checked(&self, count: usize, min_bytes_per_elem: usize) -> Result<usize> {
        let per_elem = min_bytes_per_elem.max(1);
        match count.checked_mul(per_elem) {
            Some(needed) if needed <= self.remaining() => Ok(count),
            _ => Err(ProtocolError::TtcDecode(
                "declared element count exceeds remaining buffer",
            )),
        }
    }

    /// Pre-size a `Vec` for `count` elements *without* trusting `count`: the
    /// reserved capacity is capped at `remaining() / min_bytes_per_elem`, the
    /// largest number of elements the buffer could actually hold. The returned
    /// `Vec` is a normal growable `Vec`, so a legitimately large payload (where
    /// `count` really fits) is pre-sized to the honest count, and a streamed /
    /// chunked field that grows past the initial buffer still appends correctly
    /// — the cap only governs the *speculative* up-front reservation.
    fn with_capacity_bounded<T>(&self, count: usize, min_bytes_per_elem: usize) -> Vec<T> {
        let per_elem = min_bytes_per_elem.max(1);
        Vec::with_capacity(count.min(self.remaining() / per_elem))
    }
}

#[derive(Clone, Debug)]
pub struct TtcReader<'a> {
    bytes: &'a [u8],
    pos: usize,
}

impl BoundedReader for TtcReader<'_> {
    fn remaining(&self) -> usize {
        TtcReader::remaining(self)
    }
}

/// Outcome of [`TtcReader::read_bytes_borrowed`]: a borrowed run of the wire
/// buffer for the common contiguous short-value case, an owned fallback for the
/// non-contiguous chunked long form, or NULL.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum BorrowedBytes<'a> {
    /// SQL NULL (length byte `0` or `0xff`).
    Null,
    /// A contiguous run borrowed directly from the buffer (zero-copy).
    Slice(&'a [u8]),
    /// The chunked long form (`0xfe`), reassembled into an owned `Vec` because
    /// the chunks are not contiguous on the wire. The rare path.
    Chunked(Vec<u8>),
}

impl<'a> TtcReader<'a> {
    pub fn new(bytes: &'a [u8]) -> Self {
        Self { bytes, pos: 0 }
    }

    pub fn remaining(&self) -> usize {
        self.bytes.len().saturating_sub(self.pos)
    }

    pub fn position(&self) -> usize {
        self.pos
    }

    pub fn remaining_slice(&self) -> &[u8] {
        &self.bytes[self.pos.min(self.bytes.len())..]
    }

    pub fn peek_u8(&self) -> Result<u8> {
        self.bytes
            .get(self.pos)
            .copied()
            .ok_or(ProtocolError::TtcDecode("missing u8"))
    }

    pub fn read_u8(&mut self) -> Result<u8> {
        let value = *self
            .bytes
            .get(self.pos)
            .ok_or(ProtocolError::TtcDecode("missing u8"))?;
        self.pos += 1;
        Ok(value)
    }

    pub fn read_i8(&mut self) -> Result<i8> {
        Ok(self.read_u8()? as i8)
    }

    pub fn read_u16be(&mut self) -> Result<u16> {
        let bytes = self.read_raw(2)?;
        Ok(u16::from_be_bytes(
            bytes
                .try_into()
                .map_err(|_| ProtocolError::TtcDecode("invalid u16"))?,
        ))
    }

    pub fn read_u16le(&mut self) -> Result<u16> {
        let bytes = self.read_raw(2)?;
        Ok(u16::from_le_bytes(
            bytes
                .try_into()
                .map_err(|_| ProtocolError::TtcDecode("invalid u16"))?,
        ))
    }

    pub fn read_u32be(&mut self) -> Result<u32> {
        let bytes = self.read_raw(4)?;
        Ok(u32::from_be_bytes(
            bytes
                .try_into()
                .map_err(|_| ProtocolError::TtcDecode("invalid u32"))?,
        ))
    }

    pub fn read_raw(&mut self, len: usize) -> Result<&'a [u8]> {
        let end = self
            .pos
            .checked_add(len)
            .ok_or(ProtocolError::TtcDecode("read offset overflow"))?;
        let bytes = self
            .bytes
            .get(self.pos..end)
            .ok_or(ProtocolError::TtcDecode("truncated TTC payload"))?;
        self.pos = end;
        Ok(bytes)
    }

    pub fn skip(&mut self, len: usize) -> Result<()> {
        self.read_raw(len).map(|_| ())
    }

    pub fn read_ub2(&mut self) -> Result<u16> {
        let len = self.read_u8()?;
        match len {
            0 => Ok(0),
            1 => Ok(u16::from(self.read_u8()?)),
            2 => self.read_u16be(),
            _ => Err(ProtocolError::TtcDecode("invalid ub2 length")),
        }
    }

    pub fn read_ub4(&mut self) -> Result<u32> {
        let len = self.read_u8()?;
        if len == 0 {
            return Ok(0);
        }
        if len > 4 {
            return Err(ProtocolError::TtcDecode("invalid ub4 length"));
        }
        let mut value = 0u32;
        for byte in self.read_raw(usize::from(len))? {
            value = (value << 8) | u32::from(*byte);
        }
        Ok(value)
    }

    pub fn read_sb4(&mut self) -> Result<i32> {
        let len = self.read_u8()?;
        let is_negative = len & 0x80 != 0;
        let len = len & 0x7f;
        if len == 0 {
            return Ok(0);
        }
        if len > 4 {
            return Err(ProtocolError::TtcDecode("invalid sb4 length"));
        }
        // Accumulate in the unsigned width and reinterpret as signed: a server
        // can send four bytes whose high bit is set (so the signed value is
        // i32::MIN) and flag the length as negative. Negating i32::MIN — or even
        // the intermediate `value << 8` — would overflow and panic under the
        // debug/overflow-checked fuzz build. `wrapping_neg` matches the
        // reference C decoder's two's-complement behavior and never panics.
        let mut value = 0u32;
        for byte in self.read_raw(usize::from(len))? {
            value = (value << 8) | u32::from(*byte);
        }
        let value = value as i32;
        Ok(if is_negative {
            value.wrapping_neg()
        } else {
            value
        })
    }

    pub fn read_sb8(&mut self) -> Result<i64> {
        let len = self.read_u8()?;
        let is_negative = len & 0x80 != 0;
        let len = len & 0x7f;
        if len == 0 {
            return Ok(0);
        }
        if len > 8 {
            return Err(ProtocolError::TtcDecode("invalid sb8 length"));
        }
        // See `read_sb4`: unsigned accumulation plus `wrapping_neg` avoids the
        // i64::MIN negate-overflow panic on adversarial input.
        let mut value = 0u64;
        for byte in self.read_raw(usize::from(len))? {
            value = (value << 8) | u64::from(*byte);
        }
        let value = value as i64;
        Ok(if is_negative {
            value.wrapping_neg()
        } else {
            value
        })
    }

    pub fn read_ub8(&mut self) -> Result<u64> {
        let len = self.read_u8()?;
        if len == 0 {
            return Ok(0);
        }
        if len > 8 {
            return Err(ProtocolError::TtcDecode("invalid ub8 length"));
        }
        let mut value = 0u64;
        for byte in self.read_raw(usize::from(len))? {
            value = (value << 8) | u64::from(*byte);
        }
        Ok(value)
    }

    /// Zero-copy companion to [`read_bytes`](Self::read_bytes) for the borrowed
    /// fetch path. The common short-value form (length byte 1..=253) is a single
    /// contiguous run in the buffer, so it is returned as a borrowed slice with
    /// no allocation. The chunked long form (`0xfe`) is *not* contiguous on the
    /// wire (it is a sequence of length-prefixed chunks), so it cannot be
    /// borrowed and falls back to an owned `Vec` — the rare path. `0`/`0xff`
    /// signal SQL NULL.
    ///
    /// Consumes exactly the same number of bytes as `read_bytes` for every
    /// input, so the two are interchangeable mid-stream.
    pub fn read_bytes_borrowed(&mut self) -> Result<BorrowedBytes<'a>> {
        let len = self.read_u8()?;
        if len == TNS_LONG_LENGTH_INDICATOR {
            let mut out = Vec::new();
            loop {
                let chunk_len = self.read_ub4()?;
                if chunk_len == 0 {
                    break;
                }
                let chunk = self.read_raw(chunk_len as usize)?;
                out.extend_from_slice(chunk);
            }
            Ok(BorrowedBytes::Chunked(out))
        } else if len == 0 || len == TNS_NULL_LENGTH_INDICATOR {
            Ok(BorrowedBytes::Null)
        } else {
            Ok(BorrowedBytes::Slice(self.read_raw(usize::from(len))?))
        }
    }

    /// Advance past one length-prefixed TTC byte field (short, NULL, or chunked
    /// long form) **without allocating** — the zero-copy skip used by the
    /// borrowed fetch offset-capture pass. Consumes exactly the bytes
    /// [`read_bytes`](Self::read_bytes) would.
    pub fn skip_bytes_field(&mut self) -> Result<()> {
        let len = self.read_u8()?;
        if len == TNS_LONG_LENGTH_INDICATOR {
            loop {
                let chunk_len = self.read_ub4()?;
                if chunk_len == 0 {
                    break;
                }
                self.skip(chunk_len as usize)?;
            }
            Ok(())
        } else if len == 0 || len == TNS_NULL_LENGTH_INDICATOR {
            Ok(())
        } else {
            self.skip(usize::from(len))
        }
    }

    pub fn read_bytes(&mut self) -> Result<Option<Vec<u8>>> {
        let len = self.read_u8()?;
        if len == TNS_LONG_LENGTH_INDICATOR {
            let mut out = Vec::new();
            loop {
                let chunk_len = self.read_ub4()?;
                if chunk_len == 0 {
                    break;
                }
                let chunk = self.read_raw(chunk_len as usize)?;
                out.extend_from_slice(chunk);
            }
            Ok(Some(out))
        } else if len == 0 || len == TNS_NULL_LENGTH_INDICATOR {
            Ok(None)
        } else {
            Ok(Some(self.read_raw(usize::from(len))?.to_vec()))
        }
    }

    pub fn read_bytes_with_length(&mut self) -> Result<Option<Vec<u8>>> {
        let len =
            usize::try_from(self.read_ub4()?).map_err(|_| ProtocolError::InvalidPacketLength {
                length: usize::MAX,
                minimum: 0,
            })?;
        if len == 0 {
            return Ok(None);
        }
        let value_start = self.pos;
        match self.read_bytes() {
            Ok(Some(bytes)) if bytes.len() == len => Ok(Some(bytes)),
            Ok(_) | Err(_) => {
                self.pos = value_start;
                Ok(Some(self.read_raw(len)?.to_vec()))
            }
        }
    }

    pub fn read_string_with_length(&mut self) -> Result<Option<String>> {
        let Some(bytes) = self.read_bytes_with_length()? else {
            return Ok(None);
        };
        String::from_utf8(bytes)
            .map(Some)
            .map_err(|_| ProtocolError::TtcDecode("server sent non-UTF8 string"))
    }

    pub fn read_string(&mut self) -> Result<Option<String>> {
        let Some(bytes) = self.read_bytes()? else {
            return Ok(None);
        };
        String::from_utf8(bytes)
            .map(Some)
            .map_err(|_| ProtocolError::TtcDecode("server sent non-UTF8 string"))
    }
}

pub fn encode_packet(
    packet_type: u8,
    packet_flags: u8,
    data_flags: Option<u16>,
    payload: &[u8],
    width: PacketLengthWidth,
) -> Result<Vec<u8>> {
    let data_flags_len = usize::from(data_flags.is_some()) * 2;
    let length = crate::packet::TNS_HEADER_LEN + data_flags_len + payload.len();
    let mut out = Vec::with_capacity(length);
    match width {
        PacketLengthWidth::Legacy16 => {
            let wire_length =
                u16::try_from(length).map_err(|_| ProtocolError::PacketTooLarge { length })?;
            out.extend_from_slice(&wire_length.to_be_bytes());
            out.extend_from_slice(&0u16.to_be_bytes());
        }
        PacketLengthWidth::Large32 => {
            let wire_length =
                u32::try_from(length).map_err(|_| ProtocolError::PacketTooLarge { length })?;
            out.extend_from_slice(&wire_length.to_be_bytes());
        }
    }
    out.push(packet_type);
    out.push(packet_flags);
    out.extend_from_slice(&0u16.to_be_bytes());
    if let Some(flags) = data_flags {
        out.extend_from_slice(&flags.to_be_bytes());
    }
    out.extend_from_slice(payload);
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;

    // --- BoundedReader invariant (l2p) -----------------------------------
    // A length/count field read from the wire can NEVER drive an allocation
    // larger than the bytes actually remaining in the buffer. These tests pin
    // both flavors of the bounded-allocation primitive: the early-erroring
    // `alloc_count_checked` and the cap-and-grow `with_capacity_bounded`.

    #[test]
    fn alloc_count_checked_errs_when_count_exceeds_remaining() {
        // 4 bytes left in the buffer, but a declared count of ~4 billion 8-byte
        // elements. The honest minimum is 8 bytes per element, so the claim is
        // a lie and must fail closed rather than reserving ~32 GB.
        let bytes = [0u8; 4];
        let reader = TtcReader::new(&bytes);
        assert!(reader.alloc_count_checked(u32::MAX as usize, 8).is_err());
        // count * min_bytes that overflows usize must also fail closed.
        assert!(reader.alloc_count_checked(usize::MAX, 8).is_err());
    }

    #[test]
    fn alloc_count_checked_ok_when_count_fits() {
        // 16 bytes remaining, two 8-byte elements declared: legitimate.
        let bytes = [0u8; 16];
        let reader = TtcReader::new(&bytes);
        assert_eq!(
            reader.alloc_count_checked(2, 8).expect("fits"),
            2,
            "a count whose bytes fit must pass through unchanged"
        );
        // A zero-minimum element size is treated as 1 byte (defensive) and a
        // zero count is always fine.
        assert_eq!(reader.alloc_count_checked(0, 0).expect("zero"), 0);
    }

    #[test]
    fn with_capacity_bounded_caps_preallocation_but_still_grows() {
        // 8 bytes remaining; a hostile count of ~4 billion 4-byte elements.
        let bytes = [0u8; 8];
        let reader = TtcReader::new(&bytes);
        let v: Vec<u32> = reader.with_capacity_bounded(u32::MAX as usize, 4);
        // The pre-allocation is capped at remaining()/elem = 8/4 = 2, NOT 4e9.
        assert_eq!(
            v.capacity(),
            2,
            "pre-allocation must be capped by remaining"
        );
        // But the vec is still a normal growable Vec: pushing past the cap is
        // fine (legitimate large payloads keep working as chunks arrive).
        let mut v = v;
        for i in 0..100u32 {
            v.push(i);
        }
        assert_eq!(v.len(), 100);
    }

    #[test]
    fn with_capacity_bounded_uses_full_count_when_buffer_is_large() {
        // 400 bytes remaining, 10 four-byte elements: the real count fits, so
        // the pre-allocation is the honest count, not an arbitrary small cap.
        let bytes = [0u8; 400];
        let reader = TtcReader::new(&bytes);
        let v: Vec<u32> = reader.with_capacity_bounded(10, 4);
        assert_eq!(v.capacity(), 10);
    }

    // Regression (w6-fuzz, query_response target): a negative-flagged sb4/sb8
    // whose magnitude is i32::MIN / i64::MIN made `-value` overflow and panic
    // ("attempt to negate with overflow") under the overflow-checked fuzz
    // build. `read_sb4`/`read_sb8` must now wrap instead of panicking.
    #[test]
    fn sb4_sb8_negate_overflow_does_not_panic() {
        // len byte 0x84 => negative, 4 bytes; value bytes 80 00 00 00 => i32::MIN.
        let bytes = [0x84u8, 0x80, 0x00, 0x00, 0x00];
        let mut reader = TtcReader::new(&bytes);
        assert_eq!(reader.read_sb4().expect("sb4 must not panic"), i32::MIN);

        // len byte 0x88 => negative, 8 bytes; 80 00.. => i64::MIN.
        let bytes8 = [0x88u8, 0x80, 0, 0, 0, 0, 0, 0, 0];
        let mut reader8 = TtcReader::new(&bytes8);
        assert_eq!(reader8.read_sb8().expect("sb8 must not panic"), i64::MIN);
    }

    // Round-trip ordinary signed values to confirm the unsigned-accumulation
    // rewrite did not change behavior for the common range.
    #[test]
    fn sb4_decodes_representative_values() {
        // Hand-encoded sign-magnitude: len|0x80 for negatives.
        let cases: [(&[u8], i32); 4] = [
            (&[0x00], 0),
            (&[0x01, 0x2a], 42),
            (&[0x81, 0x2a], -42),
            (&[0x02, 0x01, 0x00], 256),
        ];
        for (bytes, expected) in cases {
            let mut reader = TtcReader::new(bytes);
            assert_eq!(
                reader.read_sb4().expect("sb4 decode"),
                expected,
                "{bytes:?}"
            );
        }
    }

    #[test]
    fn ub4_round_trips_representative_values() {
        for value in [0, 1, 255, 256, 65_535, 65_536, u32::MAX] {
            let mut writer = TtcWriter::new();
            writer.write_ub4(value);
            let bytes = writer.into_bytes();
            let mut reader = TtcReader::new(&bytes);
            assert_eq!(reader.read_ub4().expect("ub4 should decode"), value);
            assert_eq!(reader.remaining(), 0);
        }
    }

    // `read_bytes_borrowed` must borrow the contiguous short-value bytes
    // directly out of the buffer (the zero-copy hot path), signal `Null` for
    // 0/0xff length, and fall back to an owned `Chunked` Vec for the
    // 0xfe long-value form (which is not contiguous on the wire). The borrowed
    // slice must equal what `read_bytes` would return, and consume exactly the
    // same number of bytes.
    #[test]
    fn read_bytes_borrowed_borrows_short_values_and_owns_chunked() {
        // Short value: length byte 3 + "abc".
        let short = [0x03u8, b'a', b'b', b'c'];
        let mut reader = TtcReader::new(&short);
        match reader.read_bytes_borrowed().expect("short decode") {
            BorrowedBytes::Slice(slice) => assert_eq!(slice, b"abc"),
            other => panic!("expected borrowed slice, got {other:?}"),
        }
        assert_eq!(reader.remaining(), 0);

        // NULL value: 0xff.
        let null = [TNS_NULL_LENGTH_INDICATOR];
        let mut reader = TtcReader::new(&null);
        assert!(matches!(
            reader.read_bytes_borrowed().expect("null decode"),
            BorrowedBytes::Null
        ));

        // Zero-length value: 0x00 (also NULL in TTC).
        let zero = [0x00u8];
        let mut reader = TtcReader::new(&zero);
        assert!(matches!(
            reader.read_bytes_borrowed().expect("zero decode"),
            BorrowedBytes::Null
        ));

        // Long/chunked value: 0xfe then ub4 chunk lengths terminated by 0.
        let mut writer = TtcWriter::new();
        writer
            .write_bytes_with_length(&vec![0x5au8; 600]) // forces the 0xfe chunked form
            .expect("chunked encode");
        let long = writer.into_bytes();
        let mut reader = TtcReader::new(&long);
        match reader.read_bytes_borrowed().expect("chunked decode") {
            BorrowedBytes::Chunked(bytes) => assert_eq!(bytes, vec![0x5au8; 600]),
            other => panic!("expected owned chunked bytes, got {other:?}"),
        }
        assert_eq!(reader.remaining(), 0);
    }

    #[test]
    fn bytes_with_length_accepts_nested_ttc_bytes() {
        let mut writer = TtcWriter::new();
        writer
            .write_bytes_with_two_lengths(Some(b"abc"))
            .expect("bytes should encode");
        let bytes = writer.into_bytes();
        let mut reader = TtcReader::new(&bytes);
        assert_eq!(
            reader
                .read_bytes_with_length()
                .expect("bytes should decode"),
            Some(b"abc".to_vec())
        );
        assert_eq!(reader.remaining(), 0);
    }

    #[test]
    fn bytes_with_length_accepts_direct_payload_bytes() {
        let bytes = [1, 3, b'a', b'b', b'c'];
        let mut reader = TtcReader::new(&bytes);
        assert_eq!(
            reader
                .read_bytes_with_length()
                .expect("bytes should decode"),
            Some(b"abc".to_vec())
        );
        assert_eq!(reader.remaining(), 0);
    }

    #[test]
    fn data_packet_uses_four_byte_length_when_negotiated() {
        let packet = encode_packet(
            6,
            0,
            Some(0),
            &[0x03, 0x93, 0x01],
            PacketLengthWidth::Large32,
        )
        .expect("packet should encode");
        assert_eq!(&packet[..10], &[0, 0, 0, 13, 6, 0, 0, 0, 0, 0]);
    }
}
