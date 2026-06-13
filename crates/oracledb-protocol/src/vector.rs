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

use crate::wire::{TtcReader, TtcWriter};
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
        let mut indices = Vec::with_capacity(usize::from(num_sparse));
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
    match format {
        VECTOR_FORMAT_FLOAT32 => {
            let mut out = Vec::with_capacity(count);
            for _ in 0..count {
                let raw = reader.read_raw(4)?;
                out.push(f32::from_be_bytes([raw[0], raw[1], raw[2], raw[3]]));
            }
            Ok(VectorValues::Float32(out))
        }
        VECTOR_FORMAT_FLOAT64 => {
            let mut out = Vec::with_capacity(count);
            for _ in 0..count {
                let raw = reader.read_raw(8)?;
                out.push(f64::from_be_bytes([
                    raw[0], raw[1], raw[2], raw[3], raw[4], raw[5], raw[6], raw[7],
                ]));
            }
            Ok(VectorValues::Float64(out))
        }
        VECTOR_FORMAT_INT8 => {
            let mut out = Vec::with_capacity(count);
            for _ in 0..count {
                out.push(reader.read_u8()? as i8);
            }
            Ok(VectorValues::Int8(out))
        }
        VECTOR_FORMAT_BINARY => Ok(VectorValues::Binary(reader.read_raw(count)?.to_vec())),
        _ => Err(ProtocolError::TtcDecode("vector: unsupported element format")),
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
                buf.extend_from_slice(&value.to_be_bytes());
            }
        }
        VectorValues::Float64(v) => {
            for value in v {
                buf.extend_from_slice(&value.to_be_bytes());
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
    write_qlocator(writer, image.len() as u64);
    writer.write_bytes_with_length(image)?;
    Ok(())
}

/// Writes a 40-byte QLocator carrying the data length (reference
/// `write_qlocator` in `packet.pyx`). VECTOR binds always include the
/// 1-byte chunk-length prefix.
fn write_qlocator(writer: &mut TtcWriter, data_length: u64) {
    const TNS_LOB_QLOCATOR_VERSION: u16 = 4;
    const TNS_LOB_LOC_FLAGS_VALUE_BASED: u8 = 0x20;
    const TNS_LOB_LOC_FLAGS_BLOB: u8 = 0x01;
    const TNS_LOB_LOC_FLAGS_ABSTRACT: u8 = 0x40;
    const TNS_LOB_LOC_FLAGS_INIT: u8 = 0x08;

    writer.write_ub4(40); // QLocator length
    writer.write_u8(40); // chunk length
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

    #[test]
    fn roundtrips_every_dense_format() {
        roundtrip(Vector::Dense(VectorValues::Float32(vec![1.5, -2.25, 3.0, 0.0])));
        roundtrip(Vector::Dense(VectorValues::Float64(vec![
            6501.0, 25.25, 18.125, -3.5,
        ])));
        roundtrip(Vector::Dense(VectorValues::Int8(vec![-5, 1, -2, 127, -128])));
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

    #[test]
    fn rejects_bad_magic() {
        let err = decode_vector(&[0x00, 0, 0, 0, 0, 0, 0, 0, 0]).unwrap_err();
        assert!(matches!(err, ProtocolError::TtcDecode(_)));
    }

    #[test]
    fn rejects_unsupported_version() {
        let mut image = encode_vector(&Vector::Dense(VectorValues::Int8(vec![1])));
        image[1] = 99; // bump version past WITH_SPARSE
        let err = decode_vector(&image).unwrap_err();
        assert!(matches!(err, ProtocolError::TtcDecode(_)));
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
        let typecode = entry["typecode"].as_str().unwrap();
        let make_values = |arr: &Value| -> VectorValues {
            let v = arr.as_array().unwrap();
            match typecode {
                "f" => VectorValues::Float32(
                    v.iter().map(|x| x.as_f64().unwrap() as f32).collect(),
                ),
                "d" => VectorValues::Float64(v.iter().map(|x| x.as_f64().unwrap()).collect()),
                "b" => {
                    VectorValues::Int8(v.iter().map(|x| x.as_i64().unwrap() as i8).collect())
                }
                "B" => {
                    VectorValues::Binary(v.iter().map(|x| x.as_u64().unwrap() as u8).collect())
                }
                other => panic!("unknown typecode {other}"),
            }
        };
        if entry["kind"] == "sparse" {
            Vector::Sparse {
                num_dimensions: entry["num_dimensions"].as_u64().unwrap() as u32,
                indices: entry["indices"]
                    .as_array()
                    .unwrap()
                    .iter()
                    .map(|x| x.as_u64().unwrap() as u32)
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
            let expected_hex = entry["image_hex"].as_str().unwrap();
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
