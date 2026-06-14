//! Oracle VECTOR wire codec (reference `impl/base/vector.pyx`).
//!
//! A VECTOR value is serialized as a self-describing binary "image" that the
//! server stores/returns inside a LOB wrapper (see `parse_vector_value` /
//! `write_vector_bind` in `thin.rs`). This module is concerned only with the
//! image itself: the header, the element values, and the optional sparse
//! index list.
//!
//! Image layout (all multi-byte integers are big-endian):
//!
//! ```text
//!   u8   magic byte (0xDB)
//!   u8   version (0 base / 1 binary / 2 sparse)
//!   u16  flags
//!   u8   element format (2=f32, 3=f64, 4=int8, 5=binary)
//!   u32  num_elements  (for binary format: number of *bits*; for sparse:
//!                       the number of dimensions)
//!   [8]  reserved norm space (zero on write; skipped on read when a NORM
//!        flag is set)
//!   -- dense:  num_elements values
//!   -- sparse: u16 num_sparse_elements,
//!              num_sparse_elements * u32 indices,
//!              num_sparse_elements values
//! ```
//!
//! The codec is fail-closed: unknown magic bytes, versions, or element
//! formats produce an error rather than a best-effort guess.

use crate::wire::{BoundedReader, TtcReader, TtcWriter};
use crate::{ProtocolError, Result};

/// VECTOR image magic byte (`TNS_VECTOR_MAGIC_BYTE`).
pub const TNS_VECTOR_MAGIC_BYTE: u8 = 0xDB;

/// VECTOR image versions (`TNS_VECTOR_VERSION_*`).
pub const TNS_VECTOR_VERSION_BASE: u8 = 0;
pub const TNS_VECTOR_VERSION_WITH_BINARY: u8 = 1;
pub const TNS_VECTOR_VERSION_WITH_SPARSE: u8 = 2;

/// VECTOR image flags (`TNS_VECTOR_FLAG_*`).
pub const TNS_VECTOR_FLAG_NORM: u16 = 0x0002;
pub const TNS_VECTOR_FLAG_NORM_RESERVED: u16 = 0x0010;
pub const TNS_VECTOR_FLAG_SPARSE: u16 = 0x0020;

/// VECTOR element storage formats (`VECTOR_FORMAT_*`).
pub const VECTOR_FORMAT_FLOAT32: u8 = 2;
pub const VECTOR_FORMAT_FLOAT64: u8 = 3;
pub const VECTOR_FORMAT_INT8: u8 = 4;
pub const VECTOR_FORMAT_BINARY: u8 = 5;

/// Decoded VECTOR element values, one variant per storage format.
///
/// The variant determines both the wire encoding and the Python `array.array`
/// typecode the shim layer materializes (`f`/`d`/`b`/`B`).
#[derive(Clone, Debug, PartialEq)]
pub enum VectorValues {
    /// FLOAT32 elements (`array.array('f')`).
    Float32(Vec<f32>),
    /// FLOAT64 elements (`array.array('d')`).
    Float64(Vec<f64>),
    /// INT8 elements (`array.array('b')`).
    Int8(Vec<i8>),
    /// BINARY elements: one byte packs 8 dimensions (`array.array('B')`).
    Binary(Vec<u8>),
}

impl VectorValues {
    /// Storage format byte for these values.
    pub fn format(&self) -> u8 {
        match self {
            VectorValues::Float32(_) => VECTOR_FORMAT_FLOAT32,
            VectorValues::Float64(_) => VECTOR_FORMAT_FLOAT64,
            VectorValues::Int8(_) => VECTOR_FORMAT_INT8,
            VectorValues::Binary(_) => VECTOR_FORMAT_BINARY,
        }
    }

    /// Number of stored elements (for BINARY this is the byte count, i.e.
    /// num_dimensions / 8).
    pub fn len(&self) -> usize {
        match self {
            VectorValues::Float32(v) => v.len(),
            VectorValues::Float64(v) => v.len(),
            VectorValues::Int8(v) => v.len(),
            VectorValues::Binary(v) => v.len(),
        }
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

/// A decoded VECTOR value: either dense (values only) or sparse (a dimension
/// count plus parallel index/value arrays of the non-zero entries).
#[derive(Clone, Debug, PartialEq)]
pub enum Vector {
    Dense(VectorValues),
    Sparse {
        num_dimensions: u32,
        indices: Vec<u32>,
        values: VectorValues,
    },
}

/// Decode a VECTOR image (the bytes carried inside the LOB wrapper).
pub fn decode_vector(data: &[u8]) -> Result<Vector> {
    let mut reader = TtcReader::new(data);

    let magic = reader.read_u8()?;
    if magic != TNS_VECTOR_MAGIC_BYTE {
        return Err(ProtocolError::TtcDecode("vector: bad magic byte"));
    }
    let version = reader.read_u8()?;
    if version > TNS_VECTOR_VERSION_WITH_SPARSE {
        return Err(ProtocolError::TtcDecode("vector: unsupported version"));
    }
    let flags = read_u16be(&mut reader)?;
    let format = reader.read_u8()?;
    let mut num_elements = read_u32be(&mut reader)?;
    if flags & TNS_VECTOR_FLAG_NORM_RESERVED != 0 || flags & TNS_VECTOR_FLAG_NORM != 0 {
        reader.skip(8)?;
    }

    if flags & TNS_VECTOR_FLAG_SPARSE != 0 {
        let num_dimensions = num_elements;
        let num_sparse = read_u16be(&mut reader)?;
        // Each sparse index is a 4-byte u32 on the wire, so bound the
        // pre-allocation by the buffer (BoundedReader invariant): a declared
        // count larger than remaining()/4 cannot be honest.
        let mut indices: Vec<u32> = reader.with_capacity_bounded(usize::from(num_sparse), 4);
        for _ in 0..num_sparse {
            indices.push(read_u32be(&mut reader)?);
        }
        let values = decode_values(&mut reader, u32::from(num_sparse), format)?;
        return Ok(Vector::Sparse {
            num_dimensions,
            indices,
            values,
        });
    }

    // dense binary format encodes the bit-count; values are bytes
    if format == VECTOR_FORMAT_BINARY {
        num_elements /= 8;
    }
    let values = decode_values(&mut reader, num_elements, format)?;
    Ok(Vector::Dense(values))
}

fn decode_values(reader: &mut TtcReader<'_>, count: u32, format: u8) -> Result<VectorValues> {
    let count = count as usize;
    // `count` is read straight off the wire (a u32, up to ~4e9). Reserving that
    // many elements up front lets a hostile/buggy server force a multi-gigabyte
    // allocation (OOM) before the first element read even fails on truncation.
    // A legitimate image always carries `count * element_size` value bytes, so
    // `BoundedReader::with_capacity_bounded` caps the reservation by what
    // remains in the buffer — never affecting a valid vector while making the
    // allocation fail-closed. The per-element `read_raw` below still
    // bounds-checks each read.
    match format {
        VECTOR_FORMAT_FLOAT32 => {
            let mut out: Vec<f32> = reader.with_capacity_bounded(count, 4);
            for _ in 0..count {
                let raw = reader.read_raw(4)?;
                out.push(decode_binary_float([raw[0], raw[1], raw[2], raw[3]]));
            }
            Ok(VectorValues::Float32(out))
        }
        VECTOR_FORMAT_FLOAT64 => {
            let mut out: Vec<f64> = reader.with_capacity_bounded(count, 8);
            for _ in 0..count {
                let raw = reader.read_raw(8)?;
                out.push(decode_binary_double([
                    raw[0], raw[1], raw[2], raw[3], raw[4], raw[5], raw[6], raw[7],
                ]));
            }
            Ok(VectorValues::Float64(out))
        }
        VECTOR_FORMAT_INT8 => {
            let mut out: Vec<i8> = reader.with_capacity_bounded(count, 1);
            for _ in 0..count {
                out.push(reader.read_u8()? as i8);
            }
            Ok(VectorValues::Int8(out))
        }
        VECTOR_FORMAT_BINARY => Ok(VectorValues::Binary(reader.read_raw(count)?.to_vec())),
        _ => Err(ProtocolError::TtcDecode(
            "vector: unsupported element format",
        )),
    }
}

/// Encode a VECTOR value into its image (the bytes that go inside the LOB
/// wrapper). Mirrors `VectorEncoder.encode` in `vector.pyx`.
pub fn encode_vector(vector: &Vector) -> Vec<u8> {
    let mut buf = Vec::new();

    let mut flags = TNS_VECTOR_FLAG_NORM_RESERVED;
    let (format, version, num_elements) = match vector {
        Vector::Sparse {
            num_dimensions,
            values,
            ..
        } => {
            flags |= TNS_VECTOR_FLAG_SPARSE | TNS_VECTOR_FLAG_NORM;
            (
                values.format(),
                TNS_VECTOR_VERSION_WITH_SPARSE,
                *num_dimensions,
            )
        }
        Vector::Dense(values) => {
            let format = values.format();
            if format == VECTOR_FORMAT_BINARY {
                (
                    format,
                    TNS_VECTOR_VERSION_WITH_BINARY,
                    (values.len() as u32) * 8,
                )
            } else {
                flags |= TNS_VECTOR_FLAG_NORM;
                (format, TNS_VECTOR_VERSION_BASE, values.len() as u32)
            }
        }
    };

    buf.push(TNS_VECTOR_MAGIC_BYTE);
    buf.push(version);
    buf.extend_from_slice(&flags.to_be_bytes());
    buf.push(format);
    buf.extend_from_slice(&num_elements.to_be_bytes());
    buf.extend_from_slice(&[0u8; 8]); // reserved norm space

    match vector {
        Vector::Dense(values) => encode_values(&mut buf, values),
        Vector::Sparse {
            indices, values, ..
        } => {
            let num_sparse = indices.len() as u16;
            buf.extend_from_slice(&num_sparse.to_be_bytes());
            for index in indices {
                buf.extend_from_slice(&index.to_be_bytes());
            }
            encode_values(&mut buf, values);
        }
    }

    buf
}

fn encode_values(buf: &mut Vec<u8>, values: &VectorValues) {
    match values {
        VectorValues::Float32(v) => {
            for value in v {
                buf.extend_from_slice(&encode_binary_float(*value));
            }
        }
        VectorValues::Float64(v) => {
            for value in v {
                buf.extend_from_slice(&encode_binary_double(*value));
            }
        }
        VectorValues::Int8(v) => {
            for value in v {
                buf.push(*value as u8);
            }
        }
        VectorValues::Binary(v) => buf.extend_from_slice(v),
    }
}

// VECTOR float elements are stored in Oracle's BINARY_FLOAT / BINARY_DOUBLE wire
// form (reference `VectorDecoder._decode_values` / `VectorEncoder._encode_values`
// in `impl/base/vector.pyx`, which call `decode_binary_float` / `encode_binary_double`
// from `decoders.pyx` / `encoders.pyx`), NOT plain IEEE-754 big-endian. The
// transform makes the byte order sort-comparable: a positive value gets its sign
// bit set; a negative value has every bit inverted.

/// Decode an Oracle BINARY_DOUBLE-encoded f64 element.
fn decode_binary_double(bytes: [u8; 8]) -> f64 {
    let mut decoded = bytes;
    if decoded[0] & 0x80 != 0 {
        decoded[0] &= 0x7f;
    } else {
        for byte in &mut decoded {
            *byte = !*byte;
        }
    }
    f64::from_bits(u64::from_be_bytes(decoded))
}

/// Decode an Oracle BINARY_FLOAT-encoded f32 element.
fn decode_binary_float(bytes: [u8; 4]) -> f32 {
    let mut decoded = bytes;
    if decoded[0] & 0x80 != 0 {
        decoded[0] &= 0x7f;
    } else {
        for byte in &mut decoded {
            *byte = !*byte;
        }
    }
    f32::from_bits(u32::from_be_bytes(decoded))
}

/// Encode an f64 element in Oracle BINARY_DOUBLE wire form.
fn encode_binary_double(value: f64) -> [u8; 8] {
    let mut bytes = value.to_bits().to_be_bytes();
    if bytes[0] & 0x80 == 0 {
        bytes[0] |= 0x80;
    } else {
        for byte in &mut bytes {
            *byte = !*byte;
        }
    }
    bytes
}

/// Encode an f32 element in Oracle BINARY_FLOAT wire form.
fn encode_binary_float(value: f32) -> [u8; 4] {
    let mut bytes = value.to_bits().to_be_bytes();
    if bytes[0] & 0x80 == 0 {
        bytes[0] |= 0x80;
    } else {
        for byte in &mut bytes {
            *byte = !*byte;
        }
    }
    bytes
}

// VECTOR images use plain big-endian fixed-width integers in the header (not
// the TTC ubN variable-length forms), so read them directly from raw bytes.
fn read_u16be(reader: &mut TtcReader<'_>) -> Result<u16> {
    let raw = reader.read_raw(2)?;
    Ok(u16::from_be_bytes([raw[0], raw[1]]))
}

fn read_u32be(reader: &mut TtcReader<'_>) -> Result<u32> {
    let raw = reader.read_raw(4)?;
    Ok(u32::from_be_bytes([raw[0], raw[1], raw[2], raw[3]]))
}

/// Convenience for the bind path: a VECTOR image written inside the LOB
/// wrapper is the qlocator (40 bytes, data-length encoded) followed by the
/// raw image bytes-with-length. This helper writes just that pair given a
/// pre-encoded image, mirroring `write_vector` -> `write_qlocator` +
/// `_write_raw_bytes_and_length` in `packet.pyx`.
pub fn write_vector_image(writer: &mut TtcWriter, image: &[u8]) -> Result<()> {
    write_qlocator(writer, image.len() as u64, true);
    writer.write_bytes_with_length(image)?;
    Ok(())
}

/// Writes an OSON image as an AQ JSON payload (reference `write_oson` with
/// `write_length=False`): a QLocator without the 1-byte chunk-length prefix,
/// followed by the OSON bytes as `_write_raw_bytes_and_length`.
pub fn write_oson_aq_payload(writer: &mut TtcWriter, image: &[u8]) -> Result<()> {
    write_qlocator(writer, image.len() as u64, false);
    writer.write_bytes_with_length(image)?;
    Ok(())
}

/// Writes a 40-byte QLocator carrying the data length (reference
/// `write_qlocator` in `packet.pyx`). `write_length` controls the 1-byte
/// chunk-length prefix (present for VECTOR/JSON binds, absent for the AQ JSON
/// payload path).
fn write_qlocator(writer: &mut TtcWriter, data_length: u64, write_length: bool) {
    const TNS_LOB_QLOCATOR_VERSION: u16 = 4;
    const TNS_LOB_LOC_FLAGS_VALUE_BASED: u8 = 0x20;
    const TNS_LOB_LOC_FLAGS_BLOB: u8 = 0x01;
    const TNS_LOB_LOC_FLAGS_ABSTRACT: u8 = 0x40;
    const TNS_LOB_LOC_FLAGS_INIT: u8 = 0x08;

    writer.write_ub4(40); // QLocator length
    if write_length {
        writer.write_u8(40); // chunk length
    }
    writer.write_u16be(38); // QLocator length less 2 bytes
    writer.write_u16be(TNS_LOB_QLOCATOR_VERSION);
    writer.write_u8(
        TNS_LOB_LOC_FLAGS_VALUE_BASED | TNS_LOB_LOC_FLAGS_BLOB | TNS_LOB_LOC_FLAGS_ABSTRACT,
    );
    writer.write_u8(TNS_LOB_LOC_FLAGS_INIT);
    writer.write_u16be(0); // additional flags
    writer.write_u16be(1); // byt1
    writer.write_u64be(data_length);
    writer.write_u16be(0); // unused
    writer.write_u16be(0); // csid
    writer.write_u16be(0); // unused
    writer.write_u64be(0); // unused
    writer.write_u64be(0); // unused
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::Value;

    fn roundtrip(vector: Vector) {
        let image = encode_vector(&vector);
        let decoded = decode_vector(&image).expect("decode");
        assert_eq!(decoded, vector);
    }

    // BoundedReader invariant (l2p), behavior-preservation: a legitimately
    // large vector (where count * element_size really fits the buffer) must
    // still decode in full. The bound is "can't exceed what's in the buffer,"
    // not an arbitrary small cap, so real large results are unaffected.
    #[test]
    fn legitimate_large_vector_still_decodes_fully() {
        let big_f32: Vec<f32> = (0..4096).map(|i| i as f32 * 0.5 - 1024.0).collect();
        roundtrip(Vector::Dense(VectorValues::Float32(big_f32)));
        let big_f64: Vec<f64> = (0..2048).map(|i| i as f64 * 0.25).collect();
        roundtrip(Vector::Dense(VectorValues::Float64(big_f64)));
        // A large sparse vector exercises the bounded sparse-index path.
        roundtrip(Vector::Sparse {
            num_dimensions: 100_000,
            indices: (0..1000).map(|i| i * 7).collect(),
            values: VectorValues::Float32((0..1000).map(|i| i as f32).collect()),
        });
    }

    #[test]
    fn roundtrips_every_dense_format() {
        roundtrip(Vector::Dense(VectorValues::Float32(vec![
            1.5, -2.25, 3.0, 0.0,
        ])));
        roundtrip(Vector::Dense(VectorValues::Float64(vec![
            6501.0, 25.25, 18.125, -3.5,
        ])));
        roundtrip(Vector::Dense(VectorValues::Int8(vec![
            -5, 1, -2, 127, -128,
        ])));
        roundtrip(Vector::Dense(VectorValues::Binary(vec![0xA5, 0x3C])));
    }

    #[test]
    fn roundtrips_every_sparse_format() {
        roundtrip(Vector::Sparse {
            num_dimensions: 8,
            indices: vec![1, 4, 6],
            values: VectorValues::Float64(vec![1.5, -2.0, 9.25]),
        });
        roundtrip(Vector::Sparse {
            num_dimensions: 6,
            indices: vec![0, 3],
            values: VectorValues::Float32(vec![2.5, -7.0]),
        });
        roundtrip(Vector::Sparse {
            num_dimensions: 5,
            indices: vec![2],
            values: VectorValues::Int8(vec![42]),
        });
    }

    // Regression: VECTOR float elements use Oracle's BINARY_FLOAT/DOUBLE
    // sign-transform wire form, NOT plain IEEE-754 big-endian. A positive value
    // gets its sign bit set; a negative value has every bit inverted. Pinning
    // the exact element bytes guards against a regression back to plain
    // `to_be_bytes`/`from_be_bytes`, which silently negates positive values and
    // corrupts negatives (the w3-async P0 bug).
    #[test]
    fn float_elements_use_oracle_binary_transform() {
        // f64 1.0 -> sign bit set -> 0xbff0_0000_0000_0000 (NOT 0x3ff0...).
        let image = encode_vector(&Vector::Dense(VectorValues::Float64(vec![1.0, -2.0])));
        let body = &image[17..]; // 1 magic + 1 ver + 2 flags + 1 fmt + 4 num + 8 norm
        assert_eq!(&body[0..8], &[0xbf, 0xf0, 0, 0, 0, 0, 0, 0], "f64 +1.0");
        // f64 -2.0 -> negative -> every bit inverted from 0xc000... -> 0x3fff...
        assert_eq!(
            &body[8..16],
            &[0x3f, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff],
            "f64 -2.0"
        );

        // f32 1.0 -> 0xbf80_0000 (NOT 0x3f80_0000).
        let image32 = encode_vector(&Vector::Dense(VectorValues::Float32(vec![1.0, -2.0])));
        let body32 = &image32[17..];
        assert_eq!(&body32[0..4], &[0xbf, 0x80, 0, 0], "f32 +1.0");
        assert_eq!(&body32[4..8], &[0x3f, 0xff, 0xff, 0xff], "f32 -2.0");

        // Decoding the same bytes must recover the originals exactly.
        assert_eq!(
            decode_vector(&image).expect("decode f64"),
            Vector::Dense(VectorValues::Float64(vec![1.0, -2.0]))
        );
        assert_eq!(
            decode_vector(&image32).expect("decode f32"),
            Vector::Dense(VectorValues::Float32(vec![1.0, -2.0]))
        );
    }

    #[test]
    fn rejects_bad_magic() {
        let err = decode_vector(&[0x00, 0, 0, 0, 0, 0, 0, 0, 0]).expect_err("bad magic must fail");
        assert!(matches!(err, ProtocolError::TtcDecode(_)));
    }

    #[test]
    fn rejects_unsupported_version() {
        let mut image = encode_vector(&Vector::Dense(VectorValues::Int8(vec![1])));
        image[1] = 99; // bump version past WITH_SPARSE
        let err = decode_vector(&image).expect_err("bad version must fail");
        assert!(matches!(err, ProtocolError::TtcDecode(_)));
    }

    // Regression (w6-fuzz, vector_decoder target): a header advertising a huge
    // FLOAT64 element count (here ~905M via num_elements 0x36000000) made the
    // decoder `Vec::with_capacity` ~7 GB before the first truncated element
    // read failed, tripping libFuzzer's OOM detector. The decoder must now
    // fail closed (truncated payload) without the giant allocation.
    #[test]
    fn fuzz_regression_oom_oversized_element_count() {
        let input = [219, 0, 0, 18, 3, 54, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0];
        let err = decode_vector(&input).expect_err("oversized count must fail closed");
        assert!(matches!(err, ProtocolError::TtcDecode(_)), "got {err:?}");
    }

    // BoundedReader invariant (l2p), VECTOR sparse family: a sparse image
    // declaring a huge num_sparse_elements (0xFFFF u16) but carrying none of the
    // 0xFFFF * 4 = 256 KiB of index bytes must fail closed, not pre-allocate
    // from the count. The `with_capacity_bounded(num_sparse, 4)` cap keeps the
    // reservation at remaining()/4 and the per-index read_u32be then errors.
    #[test]
    fn sparse_oversized_index_count_fails_closed_not_oom() {
        // magic, version=2 (sparse), flags=0x0020 (SPARSE), format=3 (f64),
        // num_elements/num_dimensions = 0 (u32), then num_sparse = 0xFFFF (u16)
        // with NO index/value bytes following.
        let input = [
            TNS_VECTOR_MAGIC_BYTE,
            TNS_VECTOR_VERSION_WITH_SPARSE,
            0x00,
            0x20, // flags: SPARSE
            VECTOR_FORMAT_FLOAT64,
            0x00,
            0x00,
            0x00,
            0x00, // num_elements (u32) = 0
            0xFF,
            0xFF, // num_sparse = 65535, but no indices follow
        ];
        let err = decode_vector(&input).expect_err("oversized sparse count must fail closed");
        assert!(matches!(err, ProtocolError::TtcDecode(_)), "got {err:?}");
    }

    #[test]
    fn binary_dense_bit_count_header() {
        // 2 bytes => 16 dimensions encoded in the header
        let image = encode_vector(&Vector::Dense(VectorValues::Binary(vec![0xA5, 0x3C])));
        let num_elements = u32::from_be_bytes([image[5], image[6], image[7], image[8]]);
        assert_eq!(num_elements, 16);
        assert_eq!(image[1], TNS_VECTOR_VERSION_WITH_BINARY);
    }

    // -- Golden: images captured from the real python-oracledb 4.0.1 driver
    //    (DB-validated round-trips). See tests/golden/vectors.json. --

    fn build_from_golden(entry: &Value) -> Vector {
        let typecode = entry["typecode"].as_str().expect("typecode");
        let f64_at = |x: &Value| x.as_f64().expect("number");
        let i64_at = |x: &Value| x.as_i64().expect("int");
        let u64_at = |x: &Value| x.as_u64().expect("uint");
        let make_values = |arr: &Value| -> VectorValues {
            let v = arr.as_array().expect("array");
            match typecode {
                "f" => VectorValues::Float32(v.iter().map(|x| f64_at(x) as f32).collect()),
                "d" => VectorValues::Float64(v.iter().map(f64_at).collect()),
                "b" => VectorValues::Int8(v.iter().map(|x| i64_at(x) as i8).collect()),
                "B" => VectorValues::Binary(v.iter().map(|x| u64_at(x) as u8).collect()),
                other => panic!("unknown typecode {other}"),
            }
        };
        if entry["kind"] == "sparse" {
            Vector::Sparse {
                num_dimensions: u64_at(&entry["num_dimensions"]) as u32,
                indices: entry["indices"]
                    .as_array()
                    .expect("indices array")
                    .iter()
                    .map(|x| u64_at(x) as u32)
                    .collect(),
                values: make_values(&entry["values"]),
            }
        } else {
            Vector::Dense(make_values(&entry["values"]))
        }
    }

    #[test]
    fn matches_golden_capture() {
        let raw = include_str!("../tests/golden/vectors.json");
        let golden: Value = serde_json::from_str(raw).expect("parse golden json");
        let obj = golden.as_object().expect("golden is an object");
        assert!(!obj.is_empty(), "golden capture must not be empty");
        for (name, entry) in obj {
            let expected_hex = entry["image_hex"].as_str().expect("image_hex");
            let expected = hex::decode(expected_hex).expect("decode golden hex");

            // encode our model -> must equal the captured image byte-for-byte
            let vector = build_from_golden(entry);
            let image = encode_vector(&vector);
            assert_eq!(
                hex::encode(&image),
                expected_hex,
                "encode mismatch for golden case {name}"
            );

            // decode the captured image -> must equal our model
            let decoded = decode_vector(&expected).expect("decode golden image");
            assert_eq!(decoded, vector, "decode mismatch for golden case {name}");
        }
    }
}
