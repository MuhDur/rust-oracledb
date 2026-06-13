#![forbid(unsafe_code)]

use super::*;

pub struct DbObjectPackedReader<'a> {
    bytes: &'a [u8],
    pos: usize,
}

impl<'a> DbObjectPackedReader<'a> {
    pub fn new(bytes: &'a [u8]) -> Self {
        Self { bytes, pos: 0 }
    }

    pub fn read_u8(&mut self) -> Result<u8> {
        let value = self
            .bytes
            .get(self.pos)
            .copied()
            .ok_or(ProtocolError::TtcDecode("truncated DbObject packed data"))?;
        self.pos += 1;
        Ok(value)
    }

    fn read_raw(&mut self, len: usize) -> Result<&'a [u8]> {
        let end = self.pos.checked_add(len).ok_or(ProtocolError::TtcDecode(
            "DbObject packed data offset overflow",
        ))?;
        let bytes = self
            .bytes
            .get(self.pos..end)
            .ok_or(ProtocolError::TtcDecode("truncated DbObject packed data"))?;
        self.pos = end;
        Ok(bytes)
    }

    fn skip(&mut self, len: usize) -> Result<()> {
        self.read_raw(len).map(|_| ())
    }

    fn read_u32be(&mut self) -> Result<u32> {
        let bytes = self.read_raw(4)?;
        Ok(u32::from_be_bytes(bytes.try_into().map_err(|_| {
            ProtocolError::TtcDecode("invalid DbObject u32")
        })?))
    }

    pub fn read_i32be(&mut self) -> Result<i32> {
        let bytes = self.read_raw(4)?;
        Ok(i32::from_be_bytes(bytes.try_into().map_err(|_| {
            ProtocolError::TtcDecode("invalid DbObject i32")
        })?))
    }

    pub fn read_length(&mut self) -> Result<usize> {
        match self.read_u8()? {
            TNS_LONG_LENGTH_INDICATOR => usize::try_from(self.read_u32be()?)
                .map_err(|_| ProtocolError::TtcDecode("DbObject length overflow")),
            length => Ok(usize::from(length)),
        }
    }

    fn skip_length(&mut self) -> Result<()> {
        if self.read_u8()? == TNS_LONG_LENGTH_INDICATOR {
            self.skip(4)?;
        }
        Ok(())
    }

    pub fn read_value_bytes(&mut self) -> Result<Option<Vec<u8>>> {
        let length = match self.read_u8()? {
            0 | TNS_NULL_LENGTH_INDICATOR => return Ok(None),
            TNS_LONG_LENGTH_INDICATOR => usize::try_from(self.read_u32be()?)
                .map_err(|_| ProtocolError::TtcDecode("DbObject value length overflow"))?,
            length => usize::from(length),
        };
        Ok(Some(self.read_raw(length)?.to_vec()))
    }

    pub fn read_header(&mut self) -> Result<()> {
        let flags = self.read_u8()?;
        let _version = self.read_u8()?;
        self.skip_length()?;
        if flags & TNS_OBJ_IS_DEGENERATE != 0 {
            return Err(ProtocolError::UnsupportedFeature(
                "DbObject stored in a LOB",
            ));
        }
        if flags & TNS_OBJ_NO_PREFIX_SEG == 0 {
            let prefix_len = self.read_length()?;
            self.skip(prefix_len)?;
        }
        Ok(())
    }

    fn bytes_left(&self) -> usize {
        self.bytes.len().saturating_sub(self.pos)
    }

    pub fn read_atomic_null(&mut self, is_collection_context: bool) -> Result<bool> {
        let value = self.read_u8()?;
        match (value, is_collection_context) {
            (TNS_OBJ_ATOMIC_NULL, _) | (TNS_NULL_LENGTH_INDICATOR, true) => Ok(true),
            _ => {
                self.pos = self.pos.saturating_sub(1);
                Ok(false)
            }
        }
    }
}

/// Writes a length-prefixed value into a DbObject pickle image buffer using the
/// inner-buffer scheme (252 short cutoff, 32767 chunks for the long form). This
/// mirrors `Buffer.write_bytes_with_length` used by `_pack_value`
/// (reference impl/thin/packet.pyx) — NOT the 245-cutoff `write_length`.
pub fn image_write_value_bytes(buf: &mut Vec<u8>, value: &[u8]) -> Result<()> {
    if value.len() <= crate::wire::TNS_MAX_SHORT_LENGTH {
        buf.push(value.len() as u8);
        buf.extend_from_slice(value);
        return Ok(());
    }
    buf.push(TNS_LONG_LENGTH_INDICATOR);
    for chunk in value.chunks(32_767) {
        image_write_ub4(
            buf,
            u32::try_from(chunk.len()).map_err(|_| ProtocolError::InvalidPacketLength {
                length: chunk.len(),
                minimum: 0,
            })?,
        );
        buf.extend_from_slice(chunk);
    }
    image_write_ub4(buf, 0);
    Ok(())
}

/// Writes a `ub4` into a pickle image buffer (reference `write_ub4`).
pub(crate) fn image_write_ub4(buf: &mut Vec<u8>, value: u32) {
    if value == 0 {
        buf.push(0);
    } else if value <= u32::from(u8::MAX) {
        buf.push(1);
        buf.push(value as u8);
    } else if value <= u32::from(u16::MAX) {
        buf.push(2);
        buf.extend_from_slice(&(value as u16).to_be_bytes());
    } else {
        buf.push(4);
        buf.extend_from_slice(&value.to_be_bytes());
    }
}

/// Writes a collection/element count length into a pickle image buffer using
/// the 245-cutoff scheme (reference `DbObjectPickleBuffer.write_length`).
pub fn image_write_length(buf: &mut Vec<u8>, length: usize) -> Result<()> {
    if length <= TNS_OBJ_MAX_SHORT_LENGTH {
        buf.push(length as u8);
    } else {
        buf.push(TNS_LONG_LENGTH_INDICATOR);
        buf.extend_from_slice(
            &u32::try_from(length)
                .map_err(|_| ProtocolError::InvalidPacketLength { length, minimum: 0 })?
                .to_be_bytes(),
        );
    }
    Ok(())
}

/// Builds the pickle image header (reference `write_header` + image_flags from
/// `create_new_object`). Returns the buffer pre-seeded with the header; the
/// caller appends the body and then calls [`image_finalize`] to back-patch the
/// total size (4-byte BE at offset 3).
pub fn image_begin(is_collection: bool) -> Vec<u8> {
    let mut image_flags = TNS_OBJ_IS_VERSION_81;
    if is_collection {
        image_flags |= TNS_OBJ_IS_COLLECTION;
    } else {
        image_flags |= TNS_OBJ_NO_PREFIX_SEG;
    }
    let mut buf = Vec::new();
    buf.push(image_flags);
    buf.push(TNS_OBJ_IMAGE_VERSION);
    buf.push(TNS_LONG_LENGTH_INDICATOR);
    buf.extend_from_slice(&0u32.to_be_bytes()); // size placeholder (offset 3)
    if is_collection {
        buf.push(1); // length of prefix segment
        buf.push(1); // prefix segment contents
    }
    buf
}

/// Back-patches the total image size (reference `_get_packed_data`: the 4-byte
/// BE size at offset 3, after flags + version + 0xFE).
pub fn image_finalize(buf: &mut [u8]) -> Result<()> {
    let size = u32::try_from(buf.len()).map_err(|_| ProtocolError::InvalidPacketLength {
        length: buf.len(),
        minimum: 0,
    })?;
    let slot = buf.get_mut(3..7).ok_or(ProtocolError::TtcDecode(
        "DbObject image too short to finalize",
    ))?;
    slot.copy_from_slice(&size.to_be_bytes());
    Ok(())
}

/// Collection flags byte written at the start of a collection body
/// (`TNS_OBJ_HAS_INDEXES` for associative arrays, else 0). Reference
/// `_parse_tds` collection_flags + `_pack_data`.
pub fn collection_flags_for(is_assoc_array: bool) -> u8 {
    if is_assoc_array {
        TNS_OBJ_HAS_INDEXES
    } else {
        0
    }
}

/// Writes a NULL element/attribute marker into the image. Non-collection object
/// attributes use `TNS_OBJ_ATOMIC_NULL` (253); scalars and collection elements
/// use `TNS_NULL_LENGTH_INDICATOR` (255). Reference `_pack_value` None branch.
pub fn image_write_null(buf: &mut Vec<u8>, atomic_null: bool) {
    if atomic_null {
        buf.push(TNS_OBJ_ATOMIC_NULL);
    } else {
        buf.push(TNS_NULL_LENGTH_INDICATOR);
    }
}

/// Packs a single scalar `BindValue` into a DbObject pickle image buffer,
/// mirroring `_pack_value` (reference impl/thin/dbobject.pyx:247-306). Object
/// (nested) and Null/Array values are handled by the caller (the pyshim owns
/// the recursion and null framing); this serves scalar attributes and
/// collection elements only.
pub fn pack_bindvalue_into_image(buf: &mut Vec<u8>, value: &BindValue, csfrm: u8) -> Result<()> {
    match value {
        BindValue::Text(text) => {
            let bytes = encode_text_value(text, csfrm);
            image_write_value_bytes(buf, &bytes)
        }
        BindValue::Raw(bytes) => image_write_value_bytes(buf, bytes),
        BindValue::Number(text) => {
            let bytes = encode_number_text(text)?;
            image_write_value_bytes(buf, &bytes)
        }
        // PLS_INTEGER / BINARY_INTEGER pack as uint8(4) + uint32be (NOT Oracle
        // number text) inside an object image.
        BindValue::BinaryInteger(text) => {
            let value = parse_binary_integer_u32(text)?;
            buf.push(4);
            buf.extend_from_slice(&value.to_be_bytes());
            Ok(())
        }
        // BOOLEAN inside an image is the 4-byte form, NOT [1,1]/[0].
        BindValue::Boolean(value) => {
            buf.push(4);
            buf.extend_from_slice(&u32::from(*value).to_be_bytes());
            Ok(())
        }
        BindValue::BinaryDouble(value) => {
            let bytes = encode_binary_double(*value);
            image_write_value_bytes(buf, &bytes)
        }
        BindValue::BinaryFloat(value) => {
            let bytes = encode_binary_float(*value as f32);
            image_write_value_bytes(buf, &bytes)
        }
        BindValue::DateTime {
            year,
            month,
            day,
            hour,
            minute,
            second,
        } => {
            let bytes = encode_oracle_date(*year, *month, *day, *hour, *minute, *second)?;
            image_write_value_bytes(buf, &bytes)
        }
        BindValue::Timestamp {
            year,
            month,
            day,
            hour,
            minute,
            second,
            nanosecond,
            ora_type_num,
        } => {
            let bytes = if matches!(*ora_type_num, ORA_TYPE_NUM_TIMESTAMP_TZ) {
                encode_oracle_timestamp_tz(
                    *year,
                    *month,
                    *day,
                    *hour,
                    *minute,
                    *second,
                    *nanosecond,
                )?
            } else {
                encode_oracle_timestamp(*year, *month, *day, *hour, *minute, *second, *nanosecond)?
            };
            image_write_value_bytes(buf, &bytes)
        }
        BindValue::Lob { locator, .. } => image_write_value_bytes(buf, locator),
        BindValue::IntervalDS {
            days,
            seconds,
            microseconds,
        } => {
            let bytes = encode_interval_ds(*days, *seconds, *microseconds)?;
            image_write_value_bytes(buf, &bytes)
        }
        BindValue::IntervalYM { years, months } => {
            let bytes = encode_interval_ym(*years, *months)?;
            image_write_value_bytes(buf, &bytes)
        }
        BindValue::Null => {
            image_write_null(buf, false);
            Ok(())
        }
        _ => Err(ProtocolError::UnsupportedFeature(
            "DbObject attribute type not supported for input binding",
        )),
    }
}

pub(crate) fn parse_binary_integer_u32(text: &str) -> Result<u32> {
    let trimmed = text.trim();
    let parsed: i64 = trimmed
        .parse()
        .map_err(|_| ProtocolError::TtcDecode("invalid BINARY_INTEGER value"))?;
    Ok(parsed as u32)
}

/// Frames a fully-packed DbObject pickle `image` into the outgoing data row,
/// replacing the zero stub used for empty OUT binds. Mirrors
/// `WriteBuffer.write_dbobject` (reference impl/thin/packet.pyx:842-857). The
/// `toid` is derived from the type `oid` per `create_new_object` (620-622).
pub fn write_dbobject_bind(writer: &mut TtcWriter, oid: &[u8], image: &[u8]) -> Result<()> {
    let mut toid = Vec::with_capacity(4 + oid.len() + TNS_EXTENT_OID.len());
    toid.extend_from_slice(&[0x00, 0x22, TNS_OBJ_NON_NULL_OID, TNS_OBJ_HAS_EXTENT_OID]);
    toid.extend_from_slice(oid);
    toid.extend_from_slice(&TNS_EXTENT_OID);
    writer.write_bytes_with_two_lengths(Some(&toid))?;
    writer.write_bytes_with_two_lengths(Some(oid))?;
    writer.write_ub4(0); // snapshot
    writer.write_ub4(0); // version
    writer.write_ub4(u32::try_from(image.len()).map_err(|_| {
        ProtocolError::InvalidPacketLength {
            length: image.len(),
            minimum: 0,
        }
    })?);
    writer.write_ub4(TNS_OBJ_TOP_LEVEL);
    writer.write_bytes_with_length(image)
}

pub fn decode_dbobject_text(bytes: &[u8], dbtype_name: &str) -> Result<String> {
    if matches!(dbtype_name, "DB_TYPE_NCHAR" | "DB_TYPE_NVARCHAR") {
        let mut chunks = bytes.chunks_exact(2);
        let units = chunks
            .by_ref()
            .map(|chunk| u16::from_be_bytes([chunk[0], chunk[1]]))
            .collect::<Vec<_>>();
        if !chunks.remainder().is_empty() {
            return Err(ProtocolError::TtcDecode("invalid DbObject UTF-16 text"));
        }
        return String::from_utf16(&units)
            .map_err(|_| ProtocolError::TtcDecode("invalid DbObject UTF-16 text"));
    }
    String::from_utf8(bytes.to_vec())
        .map_err(|_| ProtocolError::TtcDecode("invalid DbObject UTF-8 text"))
}

pub fn decode_dbobject_xmltype_text(bytes: &[u8]) -> Result<Option<String>> {
    let mut reader = DbObjectPackedReader::new(bytes);
    reader.read_header()?;
    reader.skip(1)?;
    let xml_flag = reader.read_u32be()?;
    if xml_flag & TNS_XML_TYPE_FLAG_SKIP_NEXT_4 != 0 {
        reader.skip(4)?;
    }
    let bytes = reader.read_raw(reader.bytes_left())?;
    if xml_flag & TNS_XML_TYPE_STRING != 0 {
        return decode_dbobject_text(bytes, "DB_TYPE_VARCHAR").map(Some);
    }
    if xml_flag & TNS_XML_TYPE_LOB != 0 {
        return Ok(None);
    }
    Err(ProtocolError::TtcDecode("unexpected XMLTYPE flag"))
}

pub fn decode_lob_text(bytes: &[u8], csfrm: u8, locator: Option<&[u8]>) -> Result<String> {
    let (use_utf16, little_endian) = lob_text_uses_utf16(csfrm, locator);
    if !use_utf16 {
        // Validate UTF-8 in place over the borrowed bytes, then allocate the
        // owned String once. Equivalent to `String::from_utf8(bytes.to_vec())`
        // but without the temporary Vec that was copied, validated, and moved.
        return core::str::from_utf8(bytes)
            .map(str::to_owned)
            .map_err(|_| ProtocolError::TtcDecode("invalid LOB UTF-8 text"));
    }
    // UTF-16 (almost always AL16UTF16 from the server for a multi-byte CLOB).
    // An odd byte count is malformed; reject it before decoding, matching the
    // previous `chunks_exact().remainder()` check.
    if bytes.len() % 2 != 0 {
        return Err(ProtocolError::TtcDecode("invalid LOB UTF-16 text"));
    }
    // LOB text is overwhelmingly ASCII/Latin, where every UTF-16 code unit is a
    // single ASCII byte (high byte 0, low byte < 0x80 in big-endian; the mirror
    // in little-endian). Decode those inline — one `String::push` of a 1-byte
    // char, no intermediate buffer — and only on the first non-ASCII or
    // surrogate unit hand the *remaining* bytes to the general
    // `char::decode_utf16` decoder. This skips the old intermediate `Vec<u16>`
    // (a second large allocation filled by a separate byte-swap pass) for the
    // common case while staying byte-for-byte identical to the previous
    // `String::from_utf16` output, including its rejection of lone surrogates.
    // The byte-index walk means the fallback never rescans what was already
    // decoded, so the worst case matches the general decoder rather than
    // doubling it.
    let mut out = String::with_capacity(bytes.len() / 2);
    let mut i = 0;
    while i < bytes.len() {
        let b0 = bytes[i];
        let b1 = bytes[i + 1];
        let is_ascii = if little_endian {
            b1 == 0 && b0 < 0x80
        } else {
            b0 == 0 && b1 < 0x80
        };
        if is_ascii {
            // The non-zero byte is the ASCII code point regardless of endianness.
            let ascii = if little_endian { b0 } else { b1 };
            out.push(ascii as char);
            i += 2;
        } else {
            let units = bytes[i..].chunks_exact(2).map(|chunk| {
                if little_endian {
                    u16::from_le_bytes([chunk[0], chunk[1]])
                } else {
                    u16::from_be_bytes([chunk[0], chunk[1]])
                }
            });
            for unit in char::decode_utf16(units) {
                let ch = unit.map_err(|_| ProtocolError::TtcDecode("invalid LOB UTF-16 text"))?;
                out.push(ch);
            }
            return Ok(out);
        }
    }
    Ok(out)
}

pub fn encode_lob_text(value: &str, csfrm: u8, locator: Option<&[u8]>) -> Vec<u8> {
    let (use_utf16, little_endian) = lob_text_uses_utf16(csfrm, locator);
    if !use_utf16 {
        return value.as_bytes().to_vec();
    }
    let mut bytes = Vec::with_capacity(value.len() * 2);
    for unit in value.encode_utf16() {
        let encoded = if little_endian {
            unit.to_le_bytes()
        } else {
            unit.to_be_bytes()
        };
        bytes.extend_from_slice(&encoded);
    }
    bytes
}

pub fn decode_bfile_locator_name(locator: &[u8]) -> Option<(String, String)> {
    for dir_len_pos in 0..locator.len().saturating_sub(4) {
        let dir_len = u16::from_be_bytes([locator[dir_len_pos], locator[dir_len_pos + 1]]) as usize;
        if dir_len == 0 {
            continue;
        }
        let dir_start = dir_len_pos + 2;
        let dir_end = dir_start.checked_add(dir_len)?;
        let file_len_end = dir_end.checked_add(2)?;
        if file_len_end > locator.len() {
            continue;
        }
        let file_len = u16::from_be_bytes([locator[dir_end], locator[dir_end + 1]]) as usize;
        if file_len == 0 {
            continue;
        }
        let file_start = file_len_end;
        let file_end = file_start.checked_add(file_len)?;
        if file_end != locator.len() {
            continue;
        }
        let dir = std::str::from_utf8(&locator[dir_start..dir_end]).ok()?;
        let file = std::str::from_utf8(&locator[file_start..file_end]).ok()?;
        return Some((dir.to_string(), file.to_string()));
    }
    None
}

pub(crate) fn lob_text_uses_utf16(csfrm: u8, locator: Option<&[u8]>) -> (bool, bool) {
    let use_utf16 = csfrm == CS_FORM_NCHAR
        || locator
            .and_then(|locator| locator.get(TNS_LOB_LOC_OFFSET_FLAG_3))
            .is_some_and(|flags| flags & TNS_LOB_LOC_FLAGS_VAR_LENGTH_CHARSET != 0);
    let little_endian = locator
        .and_then(|locator| locator.get(TNS_LOB_LOC_OFFSET_FLAG_4))
        .is_some_and(|flags| flags & TNS_LOB_LOC_FLAGS_LITTLE_ENDIAN != 0);
    (use_utf16, little_endian)
}

pub fn decode_dbobject_binary_float(bytes: &[u8]) -> Result<f32> {
    let mut bytes: [u8; 4] = bytes
        .try_into()
        .map_err(|_| ProtocolError::TtcDecode("invalid DbObject BINARY_FLOAT"))?;
    if bytes[0] & 0x80 != 0 {
        bytes[0] &= 0x7f;
    } else {
        for byte in &mut bytes {
            *byte = !*byte;
        }
    }
    Ok(f32::from_bits(u32::from_be_bytes(bytes)))
}

pub fn decode_dbobject_binary_double(bytes: &[u8]) -> Result<f64> {
    let mut bytes: [u8; 8] = bytes
        .try_into()
        .map_err(|_| ProtocolError::TtcDecode("invalid DbObject BINARY_DOUBLE"))?;
    if bytes[0] & 0x80 != 0 {
        bytes[0] &= 0x7f;
    } else {
        for byte in &mut bytes {
            *byte = !*byte;
        }
    }
    Ok(f64::from_bits(u64::from_be_bytes(bytes)))
}

#[cfg(test)]
mod decode_lob_text_tests {
    use super::*;

    /// A locator that drives the UTF-16 decode path, with selectable endianness.
    fn utf16_locator(little_endian: bool) -> Vec<u8> {
        let mut loc = vec![0u8; 40];
        loc[TNS_LOB_LOC_OFFSET_FLAG_3] = TNS_LOB_LOC_FLAGS_VAR_LENGTH_CHARSET;
        if little_endian {
            loc[TNS_LOB_LOC_OFFSET_FLAG_4] = TNS_LOB_LOC_FLAGS_LITTLE_ENDIAN;
        }
        loc
    }

    fn encode_utf16(s: &str, little_endian: bool) -> Vec<u8> {
        let mut bytes = Vec::with_capacity(s.len() * 2);
        for unit in s.encode_utf16() {
            let pair = if little_endian {
                unit.to_le_bytes()
            } else {
                unit.to_be_bytes()
            };
            bytes.extend_from_slice(&pair);
        }
        bytes
    }

    /// Reference decoder = the previous implementation, used as the isomorphism
    /// oracle for the optimized `decode_lob_text`.
    fn reference_from_utf16(bytes: &[u8], little_endian: bool) -> Result<String> {
        let mut chunks = bytes.chunks_exact(2);
        let units = chunks
            .by_ref()
            .map(|chunk| {
                if little_endian {
                    u16::from_le_bytes([chunk[0], chunk[1]])
                } else {
                    u16::from_be_bytes([chunk[0], chunk[1]])
                }
            })
            .collect::<Vec<_>>();
        if !chunks.remainder().is_empty() {
            return Err(ProtocolError::TtcDecode("invalid LOB UTF-16 text"));
        }
        String::from_utf16(&units).map_err(|_| ProtocolError::TtcDecode("invalid LOB UTF-16 text"))
    }

    #[test]
    fn utf16_matches_reference_for_varied_text_both_endians() {
        let samples = [
            "",
            "a",
            "the quick brown fox 0123456789",
            "café résumé naïve",           // BMP non-ASCII (Latin-1 supplement)
            "ASCII then 漢字 then more",   // BMP CJK
            "emoji: 😀🎉 mixed with text", // surrogate pairs
            "\u{0000}\u{007f}\u{0080}\u{07ff}\u{0800}\u{ffff}", // boundary code points
        ];
        for sample in samples {
            for little_endian in [false, true] {
                let bytes = encode_utf16(sample, little_endian);
                let loc = utf16_locator(little_endian);
                let got =
                    decode_lob_text(&bytes, CS_FORM_NCHAR, Some(&loc)).expect("optimized decode");
                let expected =
                    reference_from_utf16(&bytes, little_endian).expect("reference decode");
                assert_eq!(got, expected, "sample {sample:?} le={little_endian}");
                assert_eq!(got, sample);
            }
        }
    }

    #[test]
    fn utf16_odd_length_is_rejected_like_reference() {
        let loc = utf16_locator(false);
        // 3 bytes: one full unit plus a dangling byte.
        let bytes = [0x00, 0x41, 0x00];
        assert!(decode_lob_text(&bytes, CS_FORM_NCHAR, Some(&loc)).is_err());
        assert!(reference_from_utf16(&bytes, false).is_err());
    }

    #[test]
    fn utf16_lone_surrogate_is_rejected_like_reference() {
        let loc = utf16_locator(false);
        // ASCII prefix then a lone high surrogate (no following low surrogate).
        let mut bytes = encode_utf16("ok ", false);
        bytes.extend_from_slice(&0xD83Du16.to_be_bytes());
        bytes.extend_from_slice(&encode_utf16("tail", false));
        assert!(decode_lob_text(&bytes, CS_FORM_NCHAR, Some(&loc)).is_err());
        assert!(reference_from_utf16(&bytes, false).is_err());
    }

    #[test]
    fn utf8_path_matches_from_utf8() {
        // csfrm != NCHAR and no UTF-16 locator flag -> UTF-8 path.
        let loc = vec![0u8; 40];
        let sample = "café — utf8 path ✓";
        let bytes = sample.as_bytes();
        let got = decode_lob_text(bytes, 1, Some(&loc)).expect("utf8 decode");
        assert_eq!(got, String::from_utf8(bytes.to_vec()).unwrap());
        assert_eq!(got, sample);
        // invalid UTF-8 errors like String::from_utf8.
        let bad = [0x66, 0x6f, 0xff, 0x6f];
        assert!(decode_lob_text(&bad, 1, Some(&loc)).is_err());
    }
}
