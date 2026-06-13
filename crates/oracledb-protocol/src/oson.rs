//! OSON (Oracle's binary encoding of JSON) codec for `DB_TYPE_JSON`
//! (`ora_type_num` 119).
//!
//! This is a faithful Rust port of the reference implementation
//! `impl/base/oson.pyx` (python-oracledb v4.0.1): `OsonDecoder` / `OsonEncoder`.
//! The on-wire image is the same binary the Oracle server stores for a native
//! JSON column, so this codec must reproduce it byte-for-byte (see the golden
//! images under `tests/golden/oson_golden.json`).
//!
//! Wire-format summary (ground truth, captured from Oracle 23.26):
//!
//! ```text
//! header:
//!   [0..3]  magic        = FF 4A 5A          ('J' 'Z')
//!   [3]     version      = 1 (max field name 255) | 3 (max field name 65535)
//!   [4..6]  primary_flags (uint16be)
//!   -- if IS_SCALAR: optional 2- or 4-byte tree-seg-size, then the single node
//!   -- otherwise the "extended header" follows:
//!        num_short_field_names  (uint8/16/32 per NUM_FNAMES flags)
//!        short_field_names_seg_size (uint16 or uint32 per FNAMES_SEG flag)
//!        -- version 3 only: secondary_flags(u16), num_long_fnames(u32),
//!           long_field_names_seg_size(u32)
//!        tree_seg_size (uint16 or uint32 per TREE_SEG_UINT32 flag)
//!        num_tiny_nodes (uint16, always 0)
//!        short field names segment: hash-id array (1 byte each), offset array
//!           (uint16/32 each), then length-prefixed names
//!        -- version 3 only: long field names segment (hash ids 2 bytes each)
//!        tree segment (the node graph; offsets are relative to tree_seg start)
//! ```
//!
//! Container nodes use the top bit (0x80) of the node-type byte; bit 0x40
//! distinguishes array (set) from object (clear). The 0x18 bits select the
//! number-of-children width (u8/u16/u32) or "shared field ids" mode; the 0x20
//! bit selects 16- vs 32-bit child value offsets. Scalars use a fixed set of
//! type bytes plus three "length inside the node" families (number, integer,
//! short string). See [`OsonValue`] for the decoded shape.

use std::collections::BTreeMap;

use crate::thin::{
    decode_binary_double, decode_binary_float, decode_datetime_value, decode_interval_ds,
    decode_number_value, encode_binary_double, encode_binary_float, encode_interval_ds,
    encode_number_text, encode_oracle_date, encode_oracle_timestamp, QueryValue,
};
use crate::{ProtocolError, Result};

// Magic bytes and versions (reference constants.pxi).
const TNS_JSON_MAGIC_BYTE_1: u8 = 0xff;
const TNS_JSON_MAGIC_BYTE_2: u8 = 0x4a; // 'J'
const TNS_JSON_MAGIC_BYTE_3: u8 = 0x5a; // 'Z'
const TNS_JSON_VERSION_MAX_FNAME_255: u8 = 1;
const TNS_JSON_VERSION_MAX_FNAME_65535: u8 = 3;

// Primary header flags.
const TNS_JSON_FLAG_HASH_ID_UINT8: u16 = 0x0100;
const TNS_JSON_FLAG_NUM_FNAMES_UINT16: u16 = 0x0400;
const TNS_JSON_FLAG_FNAMES_SEG_UINT32: u16 = 0x0800;
const TNS_JSON_FLAG_TINY_NODES_STAT: u16 = 0x2000;
const TNS_JSON_FLAG_TREE_SEG_UINT32: u16 = 0x1000;
const TNS_JSON_FLAG_REL_OFFSET_MODE: u16 = 0x01;
const TNS_JSON_FLAG_INLINE_LEAF: u16 = 0x02;
const TNS_JSON_FLAG_NUM_FNAMES_UINT32: u16 = 0x08;
const TNS_JSON_FLAG_IS_SCALAR: u16 = 0x10;

// Secondary header flag (version 3 long field names segment).
const TNS_JSON_FLAG_SEC_FNAMES_SEG_UINT16: u16 = 0x0100;

// Scalar node type bytes.
const TNS_JSON_TYPE_NULL: u8 = 0x30;
const TNS_JSON_TYPE_TRUE: u8 = 0x31;
const TNS_JSON_TYPE_FALSE: u8 = 0x32;
const TNS_JSON_TYPE_STRING_LENGTH_UINT8: u8 = 0x33;
const TNS_JSON_TYPE_NUMBER_LENGTH_UINT8: u8 = 0x34;
const TNS_JSON_TYPE_BINARY_DOUBLE: u8 = 0x36;
const TNS_JSON_TYPE_STRING_LENGTH_UINT16: u8 = 0x37;
const TNS_JSON_TYPE_STRING_LENGTH_UINT32: u8 = 0x38;
const TNS_JSON_TYPE_TIMESTAMP: u8 = 0x39;
const TNS_JSON_TYPE_BINARY_LENGTH_UINT16: u8 = 0x3a;
const TNS_JSON_TYPE_BINARY_LENGTH_UINT32: u8 = 0x3b;
const TNS_JSON_TYPE_DATE: u8 = 0x3c;
const TNS_JSON_TYPE_INTERVAL_YM: u8 = 0x3d;
const TNS_JSON_TYPE_INTERVAL_DS: u8 = 0x3e;
const TNS_JSON_TYPE_TIMESTAMP_TZ: u8 = 0x7c;
const TNS_JSON_TYPE_TIMESTAMP7: u8 = 0x7d;
const TNS_JSON_TYPE_ID: u8 = 0x7e;
const TNS_JSON_TYPE_BINARY_FLOAT: u8 = 0x7f;
const TNS_JSON_TYPE_OBJECT: u8 = 0x84;
const TNS_JSON_TYPE_ARRAY: u8 = 0xc0;
const TNS_JSON_TYPE_EXTENDED: u8 = 0x7b;
const TNS_JSON_TYPE_VECTOR: u8 = 0x01;

// Oracle scalar wire sizes.
const ORA_TYPE_SIZE_DATE: usize = 7;
const ORA_TYPE_SIZE_TIMESTAMP: usize = 11;
const ORA_TYPE_SIZE_TIMESTAMP_TZ: usize = 13;
const ORA_TYPE_SIZE_INTERVAL_DS: usize = 11;

/// The maximum field name size when the connection does not advertise support
/// for long field names (OSON version 1). With version 3 this rises to 65535.
const MAX_FNAME_SIZE_SHORT: usize = 255;
const MAX_FNAME_SIZE_LONG: usize = 65535;

/// A fully-decoded JSON value preserving every Oracle scalar type that OSON can
/// carry. This is the lossless intermediate the protocol crate produces; the
/// Python-facing layer maps it to `dict`/`list`/`datetime`/`Decimal`/`bytes`.
///
/// We deliberately do not collapse to `serde_json::Value`: OSON distinguishes
/// `BinaryFloat` from `BinaryDouble` from `Number` (an Oracle NUMBER carried as
/// text to preserve arbitrary precision), and carries `Date`/`Timestamp`/
/// `IntervalDS`/`Raw` scalars that JSON cannot represent. Object key order is
/// preserved as insertion order, matching python-oracledb's `dict` semantics.
#[derive(Clone, Debug, PartialEq)]
pub enum OsonValue {
    Null,
    Bool(bool),
    /// An Oracle NUMBER as its canonical decimal text (e.g. "25.25",
    /// "319438950232418390.273596"). Carrying text keeps arbitrary precision.
    Number(String),
    BinaryFloat(f32),
    BinaryDouble(f64),
    /// UTF-8 string.
    String(String),
    /// Raw bytes (`$rawhex` / Python `bytes`).
    Raw(Vec<u8>),
    /// `DATE` / `TIMESTAMP` decoded to civil components (no time zone applied
    /// beyond the OSON normalization done by [`decode_datetime_value`]).
    DateTime {
        year: i32,
        month: u8,
        day: u8,
        hour: u8,
        minute: u8,
        second: u8,
        nanosecond: u32,
    },
    /// `INTERVAL DAY TO SECOND`.
    IntervalDS {
        days: i32,
        hours: i32,
        minutes: i32,
        seconds: i32,
        fseconds: i32,
    },
    /// A VECTOR embedded in JSON (extended node type).
    Vector(crate::vector::Vector),
    Array(Vec<OsonValue>),
    /// Object with insertion-ordered keys.
    Object(Vec<(String, OsonValue)>),
}

// ---------------------------------------------------------------------------
// Decoder
// ---------------------------------------------------------------------------

/// A seekable big-endian reader over the OSON image. Mirrors the random-access
/// `Buffer` the reference decoder relies on (it skips to absolute positions in
/// the tree segment).
struct OsonReader<'a> {
    data: &'a [u8],
    pos: usize,
}

impl<'a> OsonReader<'a> {
    fn new(data: &'a [u8]) -> Self {
        Self { data, pos: 0 }
    }

    fn invalid(reason: &'static str) -> ProtocolError {
        ProtocolError::OsonInvalid(reason)
    }

    fn read_raw(&mut self, len: usize) -> Result<&'a [u8]> {
        let end = self
            .pos
            .checked_add(len)
            .ok_or(ProtocolError::OsonInvalid("length overflow"))?;
        let slice = self
            .data
            .get(self.pos..end)
            .ok_or(ProtocolError::OsonInvalid("read past end of OSON image"))?;
        self.pos = end;
        Ok(slice)
    }

    fn read_u8(&mut self) -> Result<u8> {
        Ok(self.read_raw(1)?[0])
    }

    fn read_u16be(&mut self) -> Result<u16> {
        let raw = self.read_raw(2)?;
        Ok(u16::from_be_bytes([raw[0], raw[1]]))
    }

    fn read_u32be(&mut self) -> Result<u32> {
        let raw = self.read_raw(4)?;
        Ok(u32::from_be_bytes([raw[0], raw[1], raw[2], raw[3]]))
    }

    fn skip(&mut self, len: usize) -> Result<()> {
        self.read_raw(len)?;
        Ok(())
    }

    /// Seek to an absolute position. Unlike `read_raw`, this is allowed to land
    /// exactly at `data.len()` (one-past-end) so callers can restore a saved
    /// cursor at the end of a segment.
    fn seek_to(&mut self, pos: usize) -> Result<()> {
        if pos > self.data.len() {
            return Err(Self::invalid("seek past end of OSON image"));
        }
        self.pos = pos;
        Ok(())
    }
}

/// Header state carried through a full (non-scalar) OSON decode.
struct OsonDecoder<'a> {
    reader: OsonReader<'a>,
    field_names: Vec<String>,
    field_id_length: usize,
    tree_seg_pos: usize,
    relative_offsets: bool,
}

impl<'a> OsonDecoder<'a> {
    /// Reads the field names from a short or long segment. `hash_id_size` is 1
    /// for the short segment and 2 for the long segment; `name_len_size` is the
    /// number of bytes used for the per-name length prefix (1 short, 2 long).
    fn read_field_names(
        &mut self,
        num_fields: usize,
        hash_id_size: usize,
        offsets_size: usize,
        name_len_size: usize,
        seg_size: usize,
    ) -> Result<Vec<String>> {
        // Skip the hash-id array.
        self.reader.skip(num_fields * hash_id_size)?;

        // Remember where the offsets array starts, then skip it and capture the
        // field-names sub-segment.
        let offsets_pos = self.reader.pos;
        self.reader.skip(num_fields * offsets_size)?;
        let seg = self.reader.read_raw(seg_size)?;
        let final_pos = self.reader.pos;

        self.reader.seek_to(offsets_pos)?;
        let mut names = Vec::with_capacity(num_fields);
        for _ in 0..num_fields {
            let offset = if offsets_size == 2 {
                usize::from(self.reader.read_u16be()?)
            } else {
                self.reader.read_u32be()? as usize
            };
            let (name_len, name_start) = if name_len_size == 2 {
                let hi = *seg
                    .get(offset)
                    .ok_or(ProtocolError::OsonInvalid("field name offset out of range"))?;
                let lo = *seg
                    .get(offset + 1)
                    .ok_or(ProtocolError::OsonInvalid("field name offset out of range"))?;
                (usize::from(u16::from_be_bytes([hi, lo])), offset + 2)
            } else {
                let len = *seg
                    .get(offset)
                    .ok_or(ProtocolError::OsonInvalid("field name offset out of range"))?;
                (usize::from(len), offset + 1)
            };
            let end = name_start
                .checked_add(name_len)
                .ok_or(ProtocolError::OsonInvalid("field name length overflow"))?;
            let bytes = seg
                .get(name_start..end)
                .ok_or(ProtocolError::OsonInvalid("field name past end of segment"))?;
            names.push(
                std::str::from_utf8(bytes)
                    .map_err(|_| ProtocolError::OsonInvalid("field name is not valid UTF-8"))?
                    .to_string(),
            );
        }
        self.reader.seek_to(final_pos)?;
        Ok(names)
    }

    /// Reads the number of children of a container, returning `(num, is_shared)`.
    /// The 0x18 bits of the node type select the width; 0x18 means the field ids
    /// are shared with another container whose offset follows.
    fn get_num_children(&mut self, node_type: u8) -> Result<(u32, bool)> {
        let children_bits = node_type & 0x18;
        if children_bits == 0x18 {
            return Ok((0, true));
        }
        let num = match children_bits {
            0x00 => u32::from(self.reader.read_u8()?),
            0x08 => u32::from(self.reader.read_u16be()?),
            0x10 => self.reader.read_u32be()?,
            _ => return Err(ProtocolError::OsonInvalid("invalid container width")),
        };
        Ok((num, false))
    }

    /// Reads a child value offset (16- or 32-bit per the 0x20 bit).
    fn get_offset(&mut self, node_type: u8) -> Result<u32> {
        if node_type & 0x20 != 0 {
            self.reader.read_u32be()
        } else {
            Ok(u32::from(self.reader.read_u16be()?))
        }
    }

    fn decode_container_node(&mut self, node_type: u8) -> Result<OsonValue> {
        let is_object = (node_type & 0x40) == 0;
        // Position of this container relative to the tree segment start (minus
        // the node-type byte we already consumed).
        let container_offset = (self.reader.pos - self.tree_seg_pos - 1) as u32;

        let (mut num_children, is_shared) = self.get_num_children(node_type)?;
        let mut field_ids_pos = 0usize;
        let mut offsets_pos;

        if is_shared {
            // Shared field ids: an offset to another container supplies the
            // field id array and (re-read) the child count.
            let offset = self.get_offset(node_type)?;
            offsets_pos = self.reader.pos;
            self.reader
                .seek_to(self.tree_seg_pos + offset as usize)?;
            let shared_type = self.reader.read_u8()?;
            let (shared_num, _) = self.get_num_children(shared_type)?;
            num_children = shared_num;
            field_ids_pos = self.reader.pos;
        } else if is_object {
            field_ids_pos = self.reader.pos;
            offsets_pos = self.reader.pos + self.field_id_length * num_children as usize;
        } else {
            offsets_pos = self.reader.pos;
        }

        let mut object: Vec<(String, OsonValue)> = Vec::new();
        let mut array: Vec<OsonValue> = Vec::new();
        if is_object {
            object.reserve(num_children as usize);
        } else {
            array.reserve(num_children as usize);
        }

        for _ in 0..num_children {
            let mut name = String::new();
            if is_object {
                self.reader.seek_to(field_ids_pos)?;
                let field_id = match self.field_id_length {
                    1 => u32::from(self.reader.read_u8()?),
                    2 => u32::from(self.reader.read_u16be()?),
                    4 => self.reader.read_u32be()?,
                    _ => return Err(ProtocolError::OsonInvalid("invalid field id length")),
                };
                let index = (field_id as usize)
                    .checked_sub(1)
                    .ok_or(ProtocolError::OsonInvalid("field id out of range"))?;
                name = self
                    .field_names
                    .get(index)
                    .ok_or(ProtocolError::OsonInvalid("field id out of range"))?
                    .clone();
                field_ids_pos = self.reader.pos;
            }
            self.reader.seek_to(offsets_pos)?;
            let mut offset = self.get_offset(node_type)?;
            if self.relative_offsets {
                offset = offset
                    .checked_add(container_offset)
                    .ok_or(ProtocolError::OsonInvalid("relative offset overflow"))?;
            }
            offsets_pos = self.reader.pos;
            self.reader
                .seek_to(self.tree_seg_pos + offset as usize)?;
            let child = self.decode_node()?;
            if is_object {
                object.push((name, child));
            } else {
                array.push(child);
            }
        }

        if is_object {
            Ok(OsonValue::Object(object))
        } else {
            Ok(OsonValue::Array(array))
        }
    }

    fn decode_scalar_with_node_type(&mut self, node_type: u8) -> Result<OsonValue> {
        match node_type {
            TNS_JSON_TYPE_NULL => Ok(OsonValue::Null),
            TNS_JSON_TYPE_TRUE => Ok(OsonValue::Bool(true)),
            TNS_JSON_TYPE_FALSE => Ok(OsonValue::Bool(false)),
            TNS_JSON_TYPE_DATE | TNS_JSON_TYPE_TIMESTAMP7 => {
                self.decode_datetime(ORA_TYPE_SIZE_DATE)
            }
            TNS_JSON_TYPE_TIMESTAMP => self.decode_datetime(ORA_TYPE_SIZE_TIMESTAMP),
            TNS_JSON_TYPE_TIMESTAMP_TZ => self.decode_datetime(ORA_TYPE_SIZE_TIMESTAMP_TZ),
            TNS_JSON_TYPE_BINARY_FLOAT => {
                let raw = self.reader.read_raw(4)?;
                Ok(OsonValue::BinaryFloat(decode_binary_float(raw)?))
            }
            TNS_JSON_TYPE_BINARY_DOUBLE => {
                let raw = self.reader.read_raw(8)?;
                Ok(OsonValue::BinaryDouble(decode_binary_double(raw)?))
            }
            TNS_JSON_TYPE_INTERVAL_DS => {
                let raw = self.reader.read_raw(ORA_TYPE_SIZE_INTERVAL_DS)?;
                match decode_interval_ds(raw)? {
                    QueryValue::IntervalDS {
                        days,
                        hours,
                        minutes,
                        seconds,
                        fseconds,
                    } => Ok(OsonValue::IntervalDS {
                        days,
                        hours,
                        minutes,
                        seconds,
                        fseconds,
                    }),
                    _ => Err(ProtocolError::OsonInvalid("INTERVAL DS decode mismatch")),
                }
            }
            TNS_JSON_TYPE_INTERVAL_YM => {
                Err(ProtocolError::OsonTypeNotSupported("DB_TYPE_INTERVAL_YM"))
            }
            TNS_JSON_TYPE_STRING_LENGTH_UINT8 => {
                let len = usize::from(self.reader.read_u8()?);
                self.decode_string(len)
            }
            TNS_JSON_TYPE_STRING_LENGTH_UINT16 => {
                let len = usize::from(self.reader.read_u16be()?);
                self.decode_string(len)
            }
            TNS_JSON_TYPE_STRING_LENGTH_UINT32 => {
                let len = self.reader.read_u32be()? as usize;
                self.decode_string(len)
            }
            TNS_JSON_TYPE_NUMBER_LENGTH_UINT8 => {
                let len = usize::from(self.reader.read_u8()?);
                self.decode_number(len)
            }
            TNS_JSON_TYPE_ID => {
                let len = usize::from(self.reader.read_u8()?);
                Ok(OsonValue::Raw(self.reader.read_raw(len)?.to_vec()))
            }
            TNS_JSON_TYPE_BINARY_LENGTH_UINT16 => {
                let len = usize::from(self.reader.read_u16be()?);
                Ok(OsonValue::Raw(self.reader.read_raw(len)?.to_vec()))
            }
            TNS_JSON_TYPE_BINARY_LENGTH_UINT32 => {
                let len = self.reader.read_u32be()? as usize;
                Ok(OsonValue::Raw(self.reader.read_raw(len)?.to_vec()))
            }
            TNS_JSON_TYPE_EXTENDED => {
                let extended_type = self.reader.read_u8()?;
                if extended_type == TNS_JSON_TYPE_VECTOR {
                    let len = self.reader.read_u32be()? as usize;
                    let raw = self.reader.read_raw(len)?;
                    let vector = crate::vector::decode_vector(raw)
                        .map_err(|_| ProtocolError::OsonInvalid("invalid embedded VECTOR"))?;
                    Ok(OsonValue::Vector(vector))
                } else {
                    Err(ProtocolError::OsonTypeNotSupported("JSON extended type"))
                }
            }
            _ => self.decode_node_type_with_inline_length(node_type),
        }
    }

    /// Handles the three "length inside the node type byte" scalar families:
    /// number/decimal (0x20/0x60), integer (0x40/0x50), short string (0x00..0x1f).
    fn decode_node_type_with_inline_length(&mut self, node_type: u8) -> Result<OsonValue> {
        match node_type & 0xf0 {
            0x20 | 0x60 => {
                let len = usize::from(node_type & 0x0f) + 1;
                self.decode_number(len)
            }
            0x40 | 0x50 => {
                let len = usize::from(node_type & 0x0f);
                self.decode_number(len)
            }
            _ => {
                if node_type & 0xe0 == 0 {
                    if node_type == 0 {
                        return Ok(OsonValue::String(String::new()));
                    }
                    self.decode_string(usize::from(node_type))
                } else {
                    Err(ProtocolError::OsonInvalid("unsupported OSON node type"))
                }
            }
        }
    }

    fn decode_datetime(&mut self, len: usize) -> Result<OsonValue> {
        let raw = self.reader.read_raw(len)?;
        match decode_datetime_value(raw)? {
            QueryValue::DateTime {
                year,
                month,
                day,
                hour,
                minute,
                second,
                nanosecond,
            } => Ok(OsonValue::DateTime {
                year,
                month,
                day,
                hour,
                minute,
                second,
                nanosecond,
            }),
            _ => Err(ProtocolError::OsonInvalid("datetime decode mismatch")),
        }
    }

    fn decode_string(&mut self, len: usize) -> Result<OsonValue> {
        let raw = self.reader.read_raw(len)?;
        Ok(OsonValue::String(
            std::str::from_utf8(raw)
                .map_err(|_| ProtocolError::OsonInvalid("string is not valid UTF-8"))?
                .to_string(),
        ))
    }

    fn decode_number(&mut self, len: usize) -> Result<OsonValue> {
        let raw = self.reader.read_raw(len)?;
        match decode_number_value(raw)? {
            QueryValue::Number { text, .. } => Ok(OsonValue::Number(text)),
            _ => Err(ProtocolError::OsonInvalid("number decode mismatch")),
        }
    }

    fn decode_node(&mut self) -> Result<OsonValue> {
        let node_type = self.reader.read_u8()?;
        if node_type & 0x80 != 0 {
            return self.decode_container_node(node_type);
        }
        self.decode_scalar_with_node_type(node_type)
    }
}

/// Decodes an OSON binary image into an [`OsonValue`].
///
/// Fails closed: a missing/bad magic or unsupported version yields
/// [`ProtocolError::OsonNotEncoded`] (DPY-5004); structural problems
/// (truncation, out-of-range offsets, non-UTF-8 names) yield
/// [`ProtocolError::OsonInvalid`] (DPY-5006).
pub fn decode_oson(data: &[u8]) -> Result<OsonValue> {
    let mut reader = OsonReader::new(data);

    let magic = reader
        .read_raw(3)
        .map_err(|_| ProtocolError::OsonNotEncoded("image too short for header"))?;
    if magic[0] != TNS_JSON_MAGIC_BYTE_1
        || magic[1] != TNS_JSON_MAGIC_BYTE_2
        || magic[2] != TNS_JSON_MAGIC_BYTE_3
    {
        return Err(ProtocolError::OsonNotEncoded("bad OSON magic"));
    }
    let version = reader
        .read_u8()
        .map_err(|_| ProtocolError::OsonNotEncoded("missing OSON version"))?;
    if version != TNS_JSON_VERSION_MAX_FNAME_255 && version != TNS_JSON_VERSION_MAX_FNAME_65535 {
        return Err(ProtocolError::OsonNotEncoded("unsupported OSON version"));
    }
    let primary_flags = reader
        .read_u16be()
        .map_err(|_| ProtocolError::OsonNotEncoded("missing OSON flags"))?;
    let relative_offsets = primary_flags & TNS_JSON_FLAG_REL_OFFSET_MODE != 0;

    // Scalar fast-path: a small header then a single node.
    if primary_flags & TNS_JSON_FLAG_IS_SCALAR != 0 {
        if primary_flags & TNS_JSON_FLAG_TREE_SEG_UINT32 != 0 {
            reader.skip(4)?;
        } else {
            reader.skip(2)?;
        }
        let mut decoder = OsonDecoder {
            reader,
            field_names: Vec::new(),
            field_id_length: 1,
            tree_seg_pos: 0,
            relative_offsets,
        };
        decoder.tree_seg_pos = decoder.reader.pos;
        return decoder.decode_node();
    }

    // Number of short field names + field id width.
    let (num_short_field_names, field_id_length) =
        if primary_flags & TNS_JSON_FLAG_NUM_FNAMES_UINT32 != 0 {
            (reader.read_u32be()? as usize, 4usize)
        } else if primary_flags & TNS_JSON_FLAG_NUM_FNAMES_UINT16 != 0 {
            (usize::from(reader.read_u16be()?), 2usize)
        } else {
            (usize::from(reader.read_u8()?), 1usize)
        };

    // Short field names segment size + offset width.
    let (short_offsets_size, short_seg_size) =
        if primary_flags & TNS_JSON_FLAG_FNAMES_SEG_UINT32 != 0 {
            (4usize, reader.read_u32be()? as usize)
        } else {
            (2usize, usize::from(reader.read_u16be()?))
        };

    // Version 3 long field names segment metadata.
    let mut num_long_field_names = 0usize;
    let mut long_offsets_size = 0usize;
    let mut long_seg_size = 0usize;
    if version == TNS_JSON_VERSION_MAX_FNAME_65535 {
        let secondary_flags = reader.read_u16be()?;
        long_offsets_size = if secondary_flags & TNS_JSON_FLAG_SEC_FNAMES_SEG_UINT16 != 0 {
            2
        } else {
            4
        };
        num_long_field_names = reader.read_u32be()? as usize;
        long_seg_size = reader.read_u32be()? as usize;
    }

    // Tree segment size.
    let _tree_seg_size = if primary_flags & TNS_JSON_FLAG_TREE_SEG_UINT32 != 0 {
        reader.read_u32be()? as usize
    } else {
        usize::from(reader.read_u16be()?)
    };

    // Number of tiny nodes (always zero in images we produce; ignored).
    let _num_tiny_nodes = reader.read_u16be()?;

    let mut decoder = OsonDecoder {
        reader,
        field_names: Vec::with_capacity(num_short_field_names + num_long_field_names),
        field_id_length,
        tree_seg_pos: 0,
        relative_offsets,
    };

    if num_short_field_names > 0 {
        let names = decoder.read_field_names(
            num_short_field_names,
            1,
            short_offsets_size,
            1,
            short_seg_size,
        )?;
        decoder.field_names.extend(names);
    }
    if num_long_field_names > 0 {
        let names = decoder.read_field_names(
            num_long_field_names,
            2,
            long_offsets_size,
            2,
            long_seg_size,
        )?;
        decoder.field_names.extend(names);
    }

    decoder.tree_seg_pos = decoder.reader.pos;
    decoder.decode_node()
}

// ---------------------------------------------------------------------------
// Encoder
// ---------------------------------------------------------------------------

/// A field name retained during encoding, with its FNV-1a hash id and the
/// offset of its length-prefixed name within the field names segment.
#[derive(Clone)]
struct FieldName {
    name: String,
    name_bytes: Vec<u8>,
    hash_id: u32,
    offset: usize,
    field_id: u32,
}

impl FieldName {
    fn new(name: &str, max_fname_size: usize) -> Result<Self> {
        let name_bytes = name.as_bytes().to_vec();
        if name_bytes.len() > max_fname_size {
            return Err(ProtocolError::OsonInvalid(
                "field name exceeds maximum length for this connection",
            ));
        }
        // Bernstein FNV-1a (reference _calc_hash_id).
        let mut hash_id: u32 = 0x811C_9DC5;
        for &b in &name_bytes {
            hash_id = (hash_id ^ u32::from(b)).wrapping_mul(16_777_619);
        }
        Ok(Self {
            name: name.to_string(),
            name_bytes,
            hash_id,
            offset: 0,
            field_id: 0,
        })
    }

    /// Sort key matching the reference (`OsonFieldName.sort_key`):
    /// (hash_id low byte, name length, name bytes).
    fn sort_key(&self) -> (u8, usize, &[u8]) {
        (
            (self.hash_id & 0xff) as u8,
            self.name_bytes.len(),
            &self.name_bytes,
        )
    }
}

/// A growable field-names segment buffer (short or long).
struct FieldNamesSegment {
    buffer: Vec<u8>,
    field_names: Vec<FieldName>,
    num_field_names: u32,
}

impl FieldNamesSegment {
    fn new() -> Self {
        Self {
            buffer: Vec::new(),
            field_names: Vec::new(),
            num_field_names: 0,
        }
    }

    fn add_name(&mut self, mut field_name: FieldName) {
        field_name.offset = self.buffer.len();
        if field_name.name_bytes.len() <= 255 {
            self.buffer.push(field_name.name_bytes.len() as u8);
        } else {
            self.buffer
                .extend_from_slice(&(field_name.name_bytes.len() as u16).to_be_bytes());
        }
        self.buffer.extend_from_slice(&field_name.name_bytes);
        self.field_names.push(field_name);
    }

    fn process_field_names(&mut self, field_id_offset: u32) {
        self.field_names.sort_by(|a, b| a.sort_key().cmp(&b.sort_key()));
        for (index, field_name) in self.field_names.iter_mut().enumerate() {
            field_name.field_id = field_id_offset + index as u32 + 1;
        }
        self.num_field_names = self.field_names.len() as u32;
    }
}

/// The tree segment buffer; encodes the node graph with 32-bit child offsets.
struct TreeSegment {
    buffer: Vec<u8>,
}

impl TreeSegment {
    fn new() -> Self {
        Self { buffer: Vec::new() }
    }

    fn encode_container_header(&mut self, mut node_type: u8, num_children: usize) {
        node_type |= 0x20; // 32-bit offsets
        if num_children > 65535 {
            node_type |= 0x10;
        } else if num_children > 255 {
            node_type |= 0x08;
        }
        self.buffer.push(node_type);
        if num_children < 256 {
            self.buffer.push(num_children as u8);
        } else if num_children < 65536 {
            self.buffer.extend_from_slice(&(num_children as u16).to_be_bytes());
        } else {
            self.buffer.extend_from_slice(&(num_children as u32).to_be_bytes());
        }
    }

    fn encode_array(&mut self, values: &[OsonValue], encoder: &OsonEncoder) -> Result<()> {
        let num_children = values.len();
        self.encode_container_header(TNS_JSON_TYPE_ARRAY, num_children);
        let mut offset = self.buffer.len();
        self.buffer
            .resize(self.buffer.len() + num_children * 4, 0);
        for element in values {
            let pos = self.buffer.len() as u32;
            self.buffer[offset..offset + 4].copy_from_slice(&pos.to_be_bytes());
            offset += 4;
            self.encode_node(element, encoder)?;
        }
        Ok(())
    }

    fn encode_object(
        &mut self,
        entries: &[(String, OsonValue)],
        encoder: &OsonEncoder,
    ) -> Result<()> {
        let num_children = entries.len();
        self.encode_container_header(TNS_JSON_TYPE_OBJECT, num_children);
        let mut field_id_offset = self.buffer.len();
        let mut value_offset = self.buffer.len() + num_children * encoder.field_id_size;
        let final_offset = value_offset + num_children * 4;
        self.buffer.resize(final_offset, 0);
        for (key, child_value) in entries {
            let field_name = encoder
                .field_names_dict
                .get(key)
                .ok_or(ProtocolError::OsonInvalid("missing field id for key"))?;
            match encoder.field_id_size {
                1 => self.buffer[field_id_offset] = field_name.field_id as u8,
                2 => self.buffer[field_id_offset..field_id_offset + 2]
                    .copy_from_slice(&(field_name.field_id as u16).to_be_bytes()),
                _ => self.buffer[field_id_offset..field_id_offset + 4]
                    .copy_from_slice(&field_name.field_id.to_be_bytes()),
            }
            let pos = self.buffer.len() as u32;
            self.buffer[value_offset..value_offset + 4].copy_from_slice(&pos.to_be_bytes());
            field_id_offset += encoder.field_id_size;
            value_offset += 4;
            self.encode_node(child_value, encoder)?;
        }
        Ok(())
    }

    fn write_string(&mut self, bytes: &[u8]) {
        let len = bytes.len();
        if len < 256 {
            self.buffer.push(TNS_JSON_TYPE_STRING_LENGTH_UINT8);
            self.buffer.push(len as u8);
        } else if len < 65536 {
            self.buffer.push(TNS_JSON_TYPE_STRING_LENGTH_UINT16);
            self.buffer.extend_from_slice(&(len as u16).to_be_bytes());
        } else {
            self.buffer.push(TNS_JSON_TYPE_STRING_LENGTH_UINT32);
            self.buffer.extend_from_slice(&(len as u32).to_be_bytes());
        }
        if len > 0 {
            self.buffer.extend_from_slice(bytes);
        }
    }

    fn encode_node(&mut self, value: &OsonValue, encoder: &OsonEncoder) -> Result<()> {
        match value {
            OsonValue::Null => self.buffer.push(TNS_JSON_TYPE_NULL),
            OsonValue::Bool(true) => self.buffer.push(TNS_JSON_TYPE_TRUE),
            OsonValue::Bool(false) => self.buffer.push(TNS_JSON_TYPE_FALSE),
            OsonValue::Number(text) => {
                let encoded = encode_number_text(text)
                    .map_err(|_| ProtocolError::OsonInvalid("invalid JSON number"))?;
                self.buffer.push(TNS_JSON_TYPE_NUMBER_LENGTH_UINT8);
                self.buffer.push(encoded.len() as u8);
                self.buffer.extend_from_slice(&encoded);
            }
            OsonValue::BinaryFloat(value) => {
                self.buffer.push(TNS_JSON_TYPE_BINARY_FLOAT);
                self.buffer.extend_from_slice(&encode_binary_float(*value));
            }
            OsonValue::BinaryDouble(value) => {
                self.buffer.push(TNS_JSON_TYPE_BINARY_DOUBLE);
                self.buffer.extend_from_slice(&encode_binary_double(*value));
            }
            OsonValue::String(text) => self.write_string(text.as_bytes()),
            OsonValue::Raw(bytes) => {
                let len = bytes.len();
                if len < 65536 {
                    self.buffer.push(TNS_JSON_TYPE_BINARY_LENGTH_UINT16);
                    self.buffer.extend_from_slice(&(len as u16).to_be_bytes());
                } else {
                    self.buffer.push(TNS_JSON_TYPE_BINARY_LENGTH_UINT32);
                    self.buffer.extend_from_slice(&(len as u32).to_be_bytes());
                }
                self.buffer.extend_from_slice(bytes);
            }
            OsonValue::DateTime {
                year,
                month,
                day,
                hour,
                minute,
                second,
                nanosecond,
            } => {
                if *nanosecond == 0 {
                    self.buffer.push(TNS_JSON_TYPE_TIMESTAMP7);
                    let bytes = encode_oracle_date(*year, *month, *day, *hour, *minute, *second)?;
                    self.buffer.extend_from_slice(&bytes);
                } else {
                    self.buffer.push(TNS_JSON_TYPE_TIMESTAMP);
                    let bytes = encode_oracle_timestamp(
                        *year, *month, *day, *hour, *minute, *second, *nanosecond,
                    )?;
                    // TIMESTAMP node is always the full 11-byte form.
                    self.buffer.extend_from_slice(&bytes);
                }
            }
            OsonValue::IntervalDS {
                days,
                hours,
                minutes,
                seconds,
                fseconds,
            } => {
                let total_seconds = hours * 3600 + minutes * 60 + seconds;
                let microseconds = fseconds / 1000;
                let bytes = encode_interval_ds(*days, total_seconds, microseconds)?;
                self.buffer.push(TNS_JSON_TYPE_INTERVAL_DS);
                self.buffer.extend_from_slice(&bytes);
            }
            OsonValue::Vector(vector) => {
                self.buffer.push(TNS_JSON_TYPE_EXTENDED);
                self.buffer.push(TNS_JSON_TYPE_VECTOR);
                let image = crate::vector::encode_vector(vector);
                self.buffer.extend_from_slice(&(image.len() as u32).to_be_bytes());
                self.buffer.extend_from_slice(&image);
            }
            OsonValue::Array(values) => self.encode_array(values, encoder)?,
            OsonValue::Object(entries) => self.encode_object(entries, encoder)?,
        }
        Ok(())
    }
}

/// The OSON encoder. Built once per value via [`encode_oson`].
struct OsonEncoder {
    buffer: Vec<u8>,
    field_names_dict: BTreeMap<String, FieldName>,
    short_fnames_seg: Option<FieldNamesSegment>,
    long_fnames_seg: Option<FieldNamesSegment>,
    num_field_names: u32,
    field_id_size: usize,
    max_fname_size: usize,
    is_scalar: bool,
}

impl OsonEncoder {
    fn new(max_fname_size: usize) -> Self {
        Self {
            buffer: Vec::new(),
            field_names_dict: BTreeMap::new(),
            short_fnames_seg: None,
            long_fnames_seg: None,
            num_field_names: 0,
            field_id_size: 1,
            max_fname_size,
            is_scalar: false,
        }
    }

    fn add_field_name(&mut self, name: &str) -> Result<()> {
        if self.field_names_dict.contains_key(name) {
            return Ok(());
        }
        let field_name = FieldName::new(name, self.max_fname_size)?;
        self.field_names_dict
            .insert(name.to_string(), field_name.clone());
        if field_name.name_bytes.len() <= 255 {
            self.short_fnames_seg
                .get_or_insert_with(FieldNamesSegment::new)
                .add_name(field_name);
        } else {
            self.long_fnames_seg
                .get_or_insert_with(FieldNamesSegment::new)
                .add_name(field_name);
        }
        Ok(())
    }

    /// Recursively collects unique field names (matches `_examine_node`).
    fn examine_node(&mut self, value: &OsonValue) -> Result<()> {
        match value {
            OsonValue::Array(values) => {
                for child in values {
                    self.examine_node(child)?;
                }
            }
            OsonValue::Object(entries) => {
                for (key, child) in entries {
                    self.add_field_name(key)?;
                    self.examine_node(child)?;
                }
            }
            _ => {}
        }
        Ok(())
    }

    /// Determines the header flags. Returns the flag bits.
    fn determine_flags(&mut self, value: &OsonValue) -> Result<u16> {
        let mut flags = TNS_JSON_FLAG_INLINE_LEAF;
        if !matches!(value, OsonValue::Array(_) | OsonValue::Object(_)) {
            self.is_scalar = true;
            flags |= TNS_JSON_FLAG_IS_SCALAR;
            return Ok(flags);
        }

        self.short_fnames_seg = Some(FieldNamesSegment::new());
        self.examine_node(value)?;

        if let Some(seg) = self.short_fnames_seg.as_mut() {
            seg.process_field_names(0);
            self.num_field_names += seg.num_field_names;
        }
        if let Some(seg) = self.long_fnames_seg.as_mut() {
            seg.process_field_names(self.num_field_names);
            self.num_field_names += seg.num_field_names;
        }
        // The field ids in field_names_dict were cloned before sorting assigned
        // ids; re-sync them from the (now processed) segments.
        self.sync_field_ids();

        flags |= TNS_JSON_FLAG_HASH_ID_UINT8 | TNS_JSON_FLAG_TINY_NODES_STAT;
        if self.num_field_names > 65535 {
            flags |= TNS_JSON_FLAG_NUM_FNAMES_UINT32;
            self.field_id_size = 4;
        } else if self.num_field_names > 255 {
            flags |= TNS_JSON_FLAG_NUM_FNAMES_UINT16;
            self.field_id_size = 2;
        } else {
            self.field_id_size = 1;
        }
        if let Some(seg) = self.short_fnames_seg.as_ref() {
            if seg.buffer.len() > 65535 {
                flags |= TNS_JSON_FLAG_FNAMES_SEG_UINT32;
            }
        }
        Ok(flags)
    }

    /// Copies the (post-sort) `field_id` and `offset` from the segment field
    /// names back into `field_names_dict` so object encoding can look them up.
    fn sync_field_ids(&mut self) {
        for seg in [self.short_fnames_seg.as_ref(), self.long_fnames_seg.as_ref()]
            .into_iter()
            .flatten()
        {
            for field_name in &seg.field_names {
                if let Some(entry) = self.field_names_dict.get_mut(&field_name.name) {
                    entry.field_id = field_name.field_id;
                    entry.offset = field_name.offset;
                }
            }
        }
    }

    fn write_u8(&mut self, value: u8) {
        self.buffer.push(value);
    }

    fn write_u16be(&mut self, value: u16) {
        self.buffer.extend_from_slice(&value.to_be_bytes());
    }

    fn write_u32be(&mut self, value: u32) {
        self.buffer.extend_from_slice(&value.to_be_bytes());
    }

    fn write_extended_header(&mut self) {
        let short_num = self
            .short_fnames_seg
            .as_ref()
            .map_or(0, |seg| seg.num_field_names);
        match self.field_id_size {
            1 => self.write_u8(short_num as u8),
            2 => self.write_u16be(short_num as u16),
            _ => self.write_u32be(short_num),
        }
        let short_seg_len = self
            .short_fnames_seg
            .as_ref()
            .map_or(0, |seg| seg.buffer.len());
        if short_seg_len < 65536 {
            self.write_u16be(short_seg_len as u16);
        } else {
            self.write_u32be(short_seg_len as u32);
        }
        if let Some(long_seg) = self.long_fnames_seg.as_ref() {
            let long_seg_len = long_seg.buffer.len();
            let long_num = long_seg.num_field_names;
            let secondary_flags = if long_seg_len < 65536 {
                TNS_JSON_FLAG_SEC_FNAMES_SEG_UINT16
            } else {
                0
            };
            self.write_u16be(secondary_flags);
            self.write_u32be(long_num);
            self.write_u32be(long_seg_len as u32);
        }
    }

    fn write_fnames_seg_for(&mut self, long: bool) {
        // Clone the small per-name metadata we need so we can mutate self.buffer.
        let Some(seg) = (if long {
            self.long_fnames_seg.as_ref()
        } else {
            self.short_fnames_seg.as_ref()
        }) else {
            return;
        };
        let names: Vec<(u32, usize, usize)> = seg
            .field_names
            .iter()
            .map(|f| (f.hash_id, f.name_bytes.len(), f.offset))
            .collect();
        let seg_len = seg.buffer.len();
        let seg_buffer = seg.buffer.clone();

        // Hash ids.
        for (hash_id, name_len, _) in &names {
            if *name_len <= 255 {
                self.write_u8((*hash_id & 0xff) as u8);
            } else {
                self.write_u16be((*hash_id & 0xffff) as u16);
            }
        }
        // Field name offsets.
        for (_, _, offset) in &names {
            if seg_len < 65536 {
                self.write_u16be(*offset as u16);
            } else {
                self.write_u32be(*offset as u32);
            }
        }
        // Field names.
        if seg_len > 0 {
            self.buffer.extend_from_slice(&seg_buffer);
        }
    }

    fn encode(&mut self, value: &OsonValue, supports_long_fnames: bool) -> Result<Vec<u8>> {
        self.max_fname_size = if supports_long_fnames {
            MAX_FNAME_SIZE_LONG
        } else {
            MAX_FNAME_SIZE_SHORT
        };
        let mut flags = self.determine_flags(value)?;

        // Encode the tree segment first so we know its size.
        let mut tree_seg = TreeSegment::new();
        tree_seg.encode_node(value, self)?;
        if tree_seg.buffer.len() > 65535 {
            flags |= TNS_JSON_FLAG_TREE_SEG_UINT32;
        }

        // Initial header.
        self.write_u8(TNS_JSON_MAGIC_BYTE_1);
        self.write_u8(TNS_JSON_MAGIC_BYTE_2);
        self.write_u8(TNS_JSON_MAGIC_BYTE_3);
        if self.long_fnames_seg.is_some() {
            self.write_u8(TNS_JSON_VERSION_MAX_FNAME_65535);
        } else {
            self.write_u8(TNS_JSON_VERSION_MAX_FNAME_255);
        }
        self.write_u16be(flags);

        // Extended header (only when not a bare scalar).
        if self.short_fnames_seg.is_some() {
            self.write_extended_header();
        }

        // Tree segment size.
        let tree_len = tree_seg.buffer.len();
        if tree_len < 65536 {
            self.write_u16be(tree_len as u16);
        } else {
            self.write_u32be(tree_len as u32);
        }

        // Remainder of header and field segments (only when not a bare scalar).
        if self.short_fnames_seg.is_some() {
            self.write_u16be(0); // num tiny nodes
            self.write_fnames_seg_for(false);
            if self.long_fnames_seg.is_some() {
                self.write_fnames_seg_for(true);
            }
        }

        // Tree segment data.
        self.buffer.extend_from_slice(&tree_seg.buffer);
        Ok(std::mem::take(&mut self.buffer))
    }
}

/// Encodes an [`OsonValue`] into an OSON binary image.
///
/// `supports_long_fnames` should be true when the connection advertises support
/// for field names longer than 255 bytes (Oracle 23ai+, selects OSON version 3).
pub fn encode_oson(value: &OsonValue, supports_long_fnames: bool) -> Result<Vec<u8>> {
    let mut encoder = OsonEncoder::new(if supports_long_fnames {
        MAX_FNAME_SIZE_LONG
    } else {
        MAX_FNAME_SIZE_SHORT
    });
    encoder.encode(value, supports_long_fnames)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    use std::fs;
    use std::path::PathBuf;

    fn golden_path() -> PathBuf {
        PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("tests")
            .join("golden")
            .join("oson_golden.json")
    }

    fn hex_to_bytes(s: &str) -> Vec<u8> {
        (0..s.len())
            .step_by(2)
            .map(|i| u8::from_str_radix(&s[i..i + 2], 16).unwrap())
            .collect()
    }

    fn obj(pairs: &[(&str, OsonValue)]) -> OsonValue {
        OsonValue::Object(pairs.iter().map(|(k, v)| (k.to_string(), v.clone())).collect())
    }

    fn num(text: &str) -> OsonValue {
        OsonValue::Number(text.to_string())
    }

    fn s(text: &str) -> OsonValue {
        OsonValue::String(text.to_string())
    }

    /// Build the OsonValue equivalent of each golden case (matching the Python
    /// inputs in gen_oson_golden.py).
    fn golden_value(name: &str) -> Option<OsonValue> {
        Some(match name {
            "scalar_int_42" => num("42"),
            "scalar_str_hello" => s("hello"),
            "scalar_true" => OsonValue::Bool(true),
            "scalar_false" => OsonValue::Bool(false),
            "scalar_null" => OsonValue::Null,
            "scalar_empty_str" => s(""),
            "scalar_float_25_25" => num("25.25"),
            "scalar_decimal" => num("319438950232418390.273596"),
            "scalar_neg_big" => num("-9999999999999999999"),
            "scalar_bytes" => OsonValue::Raw(b"Some Bytes".to_vec()),
            "empty_obj" => obj(&[]),
            "simple_obj" => obj(&[("id", num("6901")), ("value", s("string 6901"))]),
            "name_none" => obj(&[("name", OsonValue::Null)]),
            "nested" => obj(&[(
                "employee",
                obj(&[
                    ("name", s("John")),
                    ("age", num("30")),
                    ("city", s("Delhi")),
                    ("Parmanent", OsonValue::Bool(true)),
                ]),
            )]),
            "list_in_obj" => obj(&[(
                "employees",
                OsonValue::Array(vec![s("John"), s("Matthew"), s("James")]),
            )]),
            "list_of_obj" => obj(&[(
                "employees",
                OsonValue::Array(vec![obj(&[(
                    "employee1",
                    obj(&[("name", s("John")), ("city", s("Delhi"))]),
                )])]),
            )]),
            "obj_3516" => obj(&[
                ("key_1", s("test_3516a")),
                ("key_2", s("test_3516b")),
            ]),
            "timestamp7" => OsonValue::DateTime {
                year: 2004,
                month: 2,
                day: 1,
                hour: 3,
                minute: 4,
                second: 5,
                nanosecond: 0,
            },
            "timestamp_fs" => OsonValue::DateTime {
                year: 2002,
                month: 12,
                day: 13,
                hour: 9,
                minute: 36,
                second: 0,
                nanosecond: 123_000_000,
            },
            "date_only" => OsonValue::DateTime {
                year: 2002,
                month: 12,
                day: 13,
                hour: 0,
                minute: 0,
                second: 0,
                nanosecond: 0,
            },
            "interval_ds" => OsonValue::IntervalDS {
                days: 8,
                hours: 12,
                minutes: 0,
                seconds: 0,
                fseconds: 0,
            },
            "long_fname_256" => obj(&[(&"A".repeat(256), num("6700"))]),
            _ => return None,
        })
    }

    #[test]
    fn golden_encode_matches_byte_for_byte() {
        let raw = fs::read_to_string(golden_path()).expect("golden file");
        let doc: serde_json::Value = serde_json::from_str(&raw).unwrap();
        let cases = doc["cases"].as_array().unwrap();
        let mut checked = 0;
        for case in cases {
            let name = case["name"].as_str().unwrap();
            let expected = hex_to_bytes(case["hex"].as_str().unwrap());
            let Some(value) = golden_value(name) else {
                continue;
            };
            // long_fname_256 needs version 3 (long field names support).
            let supports_long = name == "long_fname_256";
            let encoded = encode_oson(&value, supports_long)
                .unwrap_or_else(|e| panic!("encode {name} failed: {e}"));
            assert_eq!(
                encoded, expected,
                "OSON encode mismatch for golden case {name}\n got: {}\nwant: {}",
                hex(&encoded),
                hex(&expected)
            );
            checked += 1;
        }
        assert!(checked >= 20, "expected to check >=20 golden cases, got {checked}");
    }

    #[test]
    fn golden_decode_round_trips() {
        let raw = fs::read_to_string(golden_path()).expect("golden file");
        let doc: serde_json::Value = serde_json::from_str(&raw).unwrap();
        let cases = doc["cases"].as_array().unwrap();
        for case in cases {
            let name = case["name"].as_str().unwrap();
            let bytes = hex_to_bytes(case["hex"].as_str().unwrap());
            let Some(expected) = golden_value(name) else {
                continue;
            };
            let decoded =
                decode_oson(&bytes).unwrap_or_else(|e| panic!("decode {name} failed: {e}"));
            assert_eq!(decoded, expected, "OSON decode mismatch for {name}");
        }
    }

    fn hex(bytes: &[u8]) -> String {
        bytes.iter().map(|b| format!("{b:02x}")).collect()
    }

    #[test]
    fn round_trip_via_encode_decode() {
        let value = obj(&[
            ("id", num("6903")),
            ("value", s("string 6903")),
            ("flag", OsonValue::Bool(false)),
            ("nothing", OsonValue::Null),
            ("nums", OsonValue::Array(vec![num("1"), num("2.5"), num("-3")])),
            ("bf", OsonValue::BinaryFloat(38.75)),
            ("bd", OsonValue::BinaryDouble(125.875)),
        ]);
        let encoded = encode_oson(&value, false).unwrap();
        let decoded = decode_oson(&encoded).unwrap();
        assert_eq!(decoded, value);
    }

    #[test]
    fn bad_magic_is_dpy_5004() {
        let bytes = b"{'not a previous encoded value': 3}";
        let err = decode_oson(bytes).unwrap_err();
        assert!(matches!(err, ProtocolError::OsonNotEncoded(_)), "got {err:?}");
    }

    #[test]
    fn corrupt_offset_is_dpy_5006() {
        // Encode a small object, then corrupt a byte deep in the tree segment
        // so the structure fails (matches test_3516 which flips byte 15).
        let value = obj(&[("key_1", s("test_3516a")), ("key_2", s("test_3516b"))]);
        let mut encoded = encode_oson(&value, false).unwrap();
        encoded[15] = 0xFF;
        let err = decode_oson(&encoded).unwrap_err();
        assert!(matches!(err, ProtocolError::OsonInvalid(_)), "got {err:?}");
    }

    #[test]
    fn binary_float_double_use_oracle_sign_transform() {
        // Negative values exercise the bitwise-NOT branch of the sign transform;
        // a naive IEEE-754 copy would silently corrupt them.
        for v in [-1.0f64, -123.456, -0.0, f64::MIN] {
            let value = OsonValue::BinaryDouble(v);
            let decoded = decode_oson(&encode_oson(&value, false).unwrap()).unwrap();
            assert_eq!(decoded, OsonValue::BinaryDouble(v));
        }
        for v in [-1.0f32, -123.5, f32::MIN] {
            let value = OsonValue::BinaryFloat(v);
            let decoded = decode_oson(&encode_oson(&value, false).unwrap()).unwrap();
            assert_eq!(decoded, OsonValue::BinaryFloat(v));
        }
    }

    #[test]
    fn long_field_name_round_trips() {
        let key = "Z".repeat(300);
        let value = obj(&[(&key, num("6700")), ("short", s("v"))]);
        let encoded = encode_oson(&value, true).unwrap();
        // Version byte must be 3 when a long field name is present.
        assert_eq!(encoded[3], TNS_JSON_VERSION_MAX_FNAME_65535);
        let decoded = decode_oson(&encoded).unwrap();
        assert_eq!(decoded, value);
    }

    #[test]
    fn json_value_helper_silences_unused_import() {
        // Keep serde_json's json! referenced even if other tests change.
        let _ = json!({"a": 1});
    }
}
