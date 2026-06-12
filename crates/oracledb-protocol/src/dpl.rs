//! Direct Path Load (DPL) protocol support.
//!
//! Implements the three TTC functions used by python-oracledb's
//! `Connection.direct_path_load()`:
//!
//! * function 128 — direct path prepare (send table/column names, receive
//!   server-side column metadata and a direct path cursor id),
//! * function 129 — direct path load stream (column-array piece stream),
//! * function 130 — direct path op (FINISH commits, ABORT discards).
//!
//! The builders/parsers mirror `impl/thin/messages/direct_path_*.pyx` of the
//! python-oracledb v4.0.1 reference and are validated against golden wire
//! captures in `tests/golden/`. The batch state machine mirrors
//! `impl/base/batch_load_manager.pyx`.

use crate::thin::{
    encode_binary_double, encode_binary_float, encode_number_text, encode_oracle_date,
    encode_oracle_timestamp, encode_oracle_timestamp_tz, parse_column_metadata,
    parse_server_error_info, skip_server_side_piggyback, ClientCapabilities, ColumnMetadata,
    CS_FORM_IMPLICIT, CS_FORM_NCHAR, ORA_TYPE_NUM_BINARY_DOUBLE, ORA_TYPE_NUM_BINARY_FLOAT,
    ORA_TYPE_NUM_BINARY_INTEGER, ORA_TYPE_NUM_BLOB, ORA_TYPE_NUM_BOOLEAN, ORA_TYPE_NUM_CHAR,
    ORA_TYPE_NUM_CLOB, ORA_TYPE_NUM_DATE, ORA_TYPE_NUM_LONG, ORA_TYPE_NUM_LONG_RAW,
    ORA_TYPE_NUM_NUMBER, ORA_TYPE_NUM_RAW, ORA_TYPE_NUM_TIMESTAMP, ORA_TYPE_NUM_TIMESTAMP_LTZ,
    ORA_TYPE_NUM_TIMESTAMP_TZ, ORA_TYPE_NUM_VARCHAR, TNS_MSG_TYPE_END_OF_RESPONSE,
    TNS_MSG_TYPE_ERROR, TNS_MSG_TYPE_PARAMETER, TNS_MSG_TYPE_SERVER_SIDE_PIGGYBACK,
    TNS_MSG_TYPE_STATUS,
};
use crate::wire::{TtcReader, TtcWriter};
use crate::{ProtocolError, Result};

pub const TNS_FUNC_DIRECT_PATH_PREPARE: u8 = 128;
pub const TNS_FUNC_DIRECT_PATH_LOAD_STREAM: u8 = 129;
pub const TNS_FUNC_DIRECT_PATH_OP: u8 = 130;

pub const TNS_DP_INTERFACE_VERSION: u32 = 400;
pub const TNS_DP_STREAM_VERSION: u32 = 400;

pub const TNS_DPP_OP_CODE_LOAD: u32 = 1;

pub const TNS_DP_OP_ABORT: u32 = 1;
pub const TNS_DP_OP_FINISH: u32 = 2;

const TNS_DPP_IN_INDEX_INTERFACE_VERSION: usize = 0;
const TNS_DPP_IN_INDEX_STREAM_VERSION: usize = 1;
const TNS_DPP_IN_INDEX_LOCK_WAIT: usize = 14;
const TNS_DPP_KW_INDEX_OBJECT_NAME: u16 = 1;
const TNS_DPP_KW_INDEX_SCHEMA_NAME: u16 = 3;
const TNS_DPP_KW_INDEX_COLUMN_NAME: u16 = 4;
const TNS_DPP_KW_INDEX_NFOBJ_OID_POS: usize = 11;
const TNS_DPP_OUT_INDEX_CURSOR: usize = 3;
// The reference sizes the input array at TNS_DPP_IN_MAX_PARAMS (36) but only
// transmits the first 15 entries: `_initialize_hook` seeds indices 16/17 with
// 0xffff *without* updating `in_values_length`, so they are never sent.
const TNS_DPP_IN_VALUES_SENT: usize = TNS_DPP_IN_INDEX_LOCK_WAIT + 1;

pub const TNS_DPLS_ROW_HEADER_FAST_PIECE: u8 = 0x10;
pub const TNS_DPLS_ROW_HEADER_FAST_ROW: u8 = 0x20;
pub const TNS_DPLS_ROW_HEADER_FIRST: u8 = 0x08;
pub const TNS_DPLS_ROW_HEADER_LAST: u8 = 0x04;
pub const TNS_DPLS_ROW_HEADER_SPLIT_WITH_PREV: u8 = 0x02;
pub const TNS_DPLS_ROW_HEADER_SPLIT_WITH_NEXT: u8 = 0x01;

pub const TNS_DPLS_MAX_MESSAGE_SIZE: u64 = 1_073_728_895;
pub const TNS_DPLS_MAX_SHORT_LENGTH: usize = 0xfa;
pub const TNS_DPLS_MAX_PIECE_SIZE: usize = 0xfff0;

const TNS_DPLS_LONG_LENGTH_INDICATOR: u8 = 0xfe;
const TNS_NULL_LENGTH_INDICATOR: u8 = 0xff;

/// Builds the payload for TTC function 128 (direct path prepare).
///
/// Mirrors `DirectPathPrepareMessage._write_message`.
pub fn build_direct_path_prepare_payload(
    schema_name: &str,
    table_name: &str,
    column_names: &[String],
    seq_num: u8,
) -> Result<Vec<u8>> {
    let keyword_parameters_length =
        u32::try_from(column_names.len() + 2).map_err(|_| ProtocolError::InvalidPacketLength {
            length: column_names.len(),
            minimum: 0,
        })?;

    let mut in_values = [0u32; TNS_DPP_IN_VALUES_SENT];
    in_values[TNS_DPP_IN_INDEX_INTERFACE_VERSION] = TNS_DP_INTERFACE_VERSION;
    in_values[TNS_DPP_IN_INDEX_STREAM_VERSION] = TNS_DP_STREAM_VERSION;
    in_values[TNS_DPP_KW_INDEX_NFOBJ_OID_POS] = 0xffff;
    in_values[TNS_DPP_IN_INDEX_LOCK_WAIT] = 1;

    let mut writer = TtcWriter::new();
    writer.write_function_code_with_seq(TNS_FUNC_DIRECT_PATH_PREPARE, seq_num);
    writer.write_ub8(0); // token number
    writer.write_ub4(TNS_DPP_OP_CODE_LOAD);
    writer.write_u8(1); // keyword parameters (pointer)
    writer.write_ub4(keyword_parameters_length);
    writer.write_u8(1); // input array (pointer)
    writer.write_ub2(TNS_DPP_IN_VALUES_SENT as u16);
    writer.write_u8(1); // metadata (pointer)
    writer.write_u8(1); // metadata length (pointer)
    writer.write_u8(1); // parameters (pointer)
    writer.write_u8(1); // parameters length (pointer)
    writer.write_u8(1); // output array (pointer)
    writer.write_u8(1); // output array length (pointer)
    write_keyword_param(&mut writer, TNS_DPP_KW_INDEX_SCHEMA_NAME, schema_name)?;
    write_keyword_param(&mut writer, TNS_DPP_KW_INDEX_OBJECT_NAME, table_name)?;
    for name in column_names {
        write_keyword_param(&mut writer, TNS_DPP_KW_INDEX_COLUMN_NAME, name)?;
    }
    for value in in_values {
        writer.write_ub4(value);
    }
    Ok(writer.into_bytes())
}

fn write_keyword_param(writer: &mut TtcWriter, index: u16, value: &str) -> Result<()> {
    let bytes = value.as_bytes();
    let len = u16::try_from(bytes.len()).map_err(|_| ProtocolError::InvalidPacketLength {
        length: bytes.len(),
        minimum: 0,
    })?;
    writer.write_ub2(0); // text length
    writer.write_ub2(len);
    writer.write_bytes_with_length(bytes)?;
    writer.write_ub2(index);
    Ok(())
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct DirectPathPrepareResult {
    pub column_metadata: Vec<ColumnMetadata>,
    pub cursor_id: u16,
    pub out_values: Vec<u32>,
}

/// Parses the response to TTC function 128 (direct path prepare).
///
/// `capabilities.charset_id` drives the CLOB metadata override (charset ids
/// of 800 and above are multi-byte, in which case implicit-charset CLOBs
/// switch to the NCHAR form). Mirrors the reference's
/// `DirectPathPrepareMessage._process_metadata`/`_process_return_parameters`.
pub fn parse_direct_path_prepare_response(
    payload: &[u8],
    capabilities: ClientCapabilities,
) -> Result<DirectPathPrepareResult> {
    let mut reader = TtcReader::new(payload);
    let mut result: Option<DirectPathPrepareResult> = None;
    while reader.remaining() > 0 {
        let message_type = reader.read_u8()?;
        match message_type {
            0 => {}
            TNS_MSG_TYPE_PARAMETER => {
                result = Some(parse_prepare_return_parameters(&mut reader, capabilities)?);
            }
            TNS_MSG_TYPE_STATUS => {
                let _call_status = reader.read_ub4()?;
                let _seq = reader.read_ub2()?;
            }
            TNS_MSG_TYPE_SERVER_SIDE_PIGGYBACK => skip_server_side_piggyback(&mut reader)?,
            TNS_MSG_TYPE_END_OF_RESPONSE => break,
            TNS_MSG_TYPE_ERROR => {
                let info = parse_server_error_info(&mut reader, capabilities.ttc_field_version)?;
                if info.number != 0 {
                    return Err(ProtocolError::ServerError(info.message));
                }
            }
            _ => {
                return Err(ProtocolError::UnknownMessageType {
                    message_type,
                    position: reader.position().saturating_sub(1),
                })
            }
        }
    }
    result.ok_or(ProtocolError::TtcDecode(
        "direct path prepare response did not contain return parameters",
    ))
}

fn parse_prepare_return_parameters(
    reader: &mut TtcReader<'_>,
    capabilities: ClientCapabilities,
) -> Result<DirectPathPrepareResult> {
    let num_columns = reader.read_ub4()?;
    let mut column_metadata = Vec::with_capacity(num_columns.min(1_024) as usize);
    for _ in 0..num_columns {
        let mut metadata = parse_column_metadata(reader, capabilities)?;
        apply_direct_path_metadata_overrides(&mut metadata, capabilities.charset_id);
        column_metadata.push(metadata);
    }
    let num_params = reader.read_ub2()?;
    if num_params != 0 {
        return Err(ProtocolError::TtcDecode(
            "unexpected parameters in direct path prepare response",
        ));
    }
    let out_values_length = reader.read_ub2()?;
    let mut out_values = Vec::with_capacity(usize::from(out_values_length));
    for _ in 0..out_values_length {
        out_values.push(reader.read_ub4()?);
    }
    let cursor_id =
        out_values
            .get(TNS_DPP_OUT_INDEX_CURSOR)
            .copied()
            .ok_or(ProtocolError::TtcDecode(
                "direct path prepare response missing cursor id",
            ))?;
    let cursor_id = u16::try_from(cursor_id)
        .map_err(|_| ProtocolError::TtcDecode("direct path cursor id out of range"))?;
    Ok(DirectPathPrepareResult {
        column_metadata,
        cursor_id,
        out_values,
    })
}

/// CLOB/NCLOB and BLOB columns are always streamed as LONG/LONG RAW during a
/// direct path load. Implicit-charset CLOBs switch to the NCHAR form when the
/// database charset is multi-byte (charset ids >= 800).
fn apply_direct_path_metadata_overrides(metadata: &mut ColumnMetadata, charset_id: u16) {
    if metadata.ora_type_num == ORA_TYPE_NUM_CLOB {
        if metadata.csfrm == CS_FORM_IMPLICIT && charset_id >= 800 {
            metadata.csfrm = CS_FORM_NCHAR;
        }
        metadata.ora_type_num = ORA_TYPE_NUM_LONG;
    } else if metadata.ora_type_num == ORA_TYPE_NUM_BLOB {
        metadata.ora_type_num = ORA_TYPE_NUM_LONG_RAW;
        metadata.csfrm = 0;
    }
}

/// Builds the payload for TTC function 130 (direct path op).
///
/// Mirrors `DirectPathOpMessage._write_message`. `op_code` is
/// [`TNS_DP_OP_FINISH`] (commits the load) or [`TNS_DP_OP_ABORT`].
pub fn build_direct_path_op_payload(cursor_id: u16, op_code: u32, seq_num: u8) -> Vec<u8> {
    let mut writer = TtcWriter::new();
    writer.write_function_code_with_seq(TNS_FUNC_DIRECT_PATH_OP, seq_num);
    writer.write_ub8(0); // token number
    writer.write_ub4(op_code);
    writer.write_ub2(cursor_id);
    writer.write_u8(0); // pointer (input values)
    writer.write_ub4(0); // number of input values
    writer.write_u8(1); // pointer (output values)
    writer.write_u8(1); // pointer (output values length)
    writer.into_bytes()
}

/// Parses the response to TTC functions 129 and 130 (both return the same
/// shape: a ub2 count of out values that are each skipped).
pub fn parse_direct_path_simple_response(
    payload: &[u8],
    capabilities: ClientCapabilities,
) -> Result<()> {
    let mut reader = TtcReader::new(payload);
    while reader.remaining() > 0 {
        let message_type = reader.read_u8()?;
        match message_type {
            0 => {}
            TNS_MSG_TYPE_PARAMETER => {
                let num_out_values = reader.read_ub2()?;
                for _ in 0..num_out_values {
                    let _value = reader.read_ub4()?;
                }
            }
            TNS_MSG_TYPE_STATUS => {
                let _call_status = reader.read_ub4()?;
                let _seq = reader.read_ub2()?;
            }
            TNS_MSG_TYPE_SERVER_SIDE_PIGGYBACK => skip_server_side_piggyback(&mut reader)?,
            TNS_MSG_TYPE_END_OF_RESPONSE => break,
            TNS_MSG_TYPE_ERROR => {
                let info = parse_server_error_info(&mut reader, capabilities.ttc_field_version)?;
                if info.number != 0 {
                    return Err(ProtocolError::ServerError(info.message));
                }
            }
            _ => {
                return Err(ProtocolError::UnknownMessageType {
                    message_type,
                    position: reader.position().saturating_sub(1),
                })
            }
        }
    }
    Ok(())
}

pub use parse_direct_path_simple_response as parse_direct_path_load_stream_response;
pub use parse_direct_path_simple_response as parse_direct_path_op_response;

/// One column value of a direct path load row, already converted to the
/// Oracle-facing intermediate form (mirrors the reference's `OracleData`).
///
/// `Bytes` carries the on-the-wire byte payload for VARCHAR/CHAR/LONG (text
/// already encoded per the column's charset form) and RAW/LONG RAW columns.
#[derive(Clone, Debug, PartialEq)]
pub enum DirectPathColumnValue {
    Null,
    Bytes(Vec<u8>),
    Number(String),
    BinaryDouble(f64),
    BinaryFloat(f32),
    DateTime {
        year: i32,
        month: u8,
        day: u8,
        hour: u8,
        minute: u8,
        second: u8,
        nanosecond: u32,
    },
    Boolean(bool),
}

/// A finalized direct path piece, ready to be written to a load stream
/// message.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DirectPathPiece {
    flags: u8,
    num_segments: u8,
    data: Vec<u8>,
}

impl DirectPathPiece {
    pub fn flags(&self) -> u8 {
        self.flags
    }

    pub fn num_segments(&self) -> u8 {
        self.num_segments
    }

    pub fn data(&self) -> &[u8] {
        &self.data
    }

    fn is_fast_row(&self) -> bool {
        self.flags & TNS_DPLS_ROW_HEADER_FAST_ROW != 0
    }

    fn header_length(&self) -> u64 {
        if self.is_fast_row() {
            4
        } else {
            2
        }
    }

    fn write_to(&self, writer: &mut TtcWriter) -> Result<()> {
        writer.write_u8(self.flags);
        if self.is_fast_row() {
            let total = self.data.len() as u64 + self.header_length();
            let total = u16::try_from(total).map_err(|_| {
                ProtocolError::TtcDecode("direct path fast piece exceeds 16-bit length")
            })?;
            writer.write_u16be(total);
        }
        writer.write_u8(self.num_segments);
        writer.write_raw(&self.data);
        Ok(())
    }
}

#[derive(Clone, Copy, Debug, Default)]
struct PieceState {
    is_first: bool,
    is_last: bool,
    is_split_with_prev: bool,
    is_split_with_next: bool,
    is_fast: bool,
    num_segments: u16,
}

/// Streaming encoder for the direct path column-array piece format.
///
/// Port of the reference `PieceBuffer` (direct_path_load_stream.pyx). Usage:
/// `start_row()` / `add_column_value(..)` per column / `finish_row()`, then
/// [`DirectPathPieceBuffer::finish`].
#[derive(Debug, Default)]
pub struct DirectPathPieceBuffer {
    pieces: Vec<DirectPathPiece>,
    total_piece_length: u64,
    data: Vec<u8>,
    current: Option<PieceState>,
}

impl DirectPathPieceBuffer {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn start_row(&mut self) -> Result<()> {
        if self.current.is_some() {
            return Err(ProtocolError::TtcDecode(
                "direct path row started before previous row was finished",
            ));
        }
        self.current = Some(PieceState {
            is_first: true,
            is_fast: true,
            ..PieceState::default()
        });
        Ok(())
    }

    pub fn finish_row(&mut self) -> Result<()> {
        let Some(state) = self.current.as_mut() else {
            return Err(ProtocolError::TtcDecode(
                "direct path row finished without being started",
            ));
        };
        state.is_last = true;
        self.finalize_piece()?;
        self.current = None;
        Ok(())
    }

    pub fn add_column_value(
        &mut self,
        metadata: &ColumnMetadata,
        value: &DirectPathColumnValue,
        row_num: u64,
    ) -> Result<()> {
        let Some(state) = self.current.as_mut() else {
            return Err(ProtocolError::TtcDecode(
                "direct path column value added outside of a row",
            ));
        };

        // at most 255 segments per piece
        if state.num_segments == 255 {
            self.finalize_piece()?;
            self.current = Some(PieceState::default());
        }

        if !is_fast_dbtype(metadata) {
            if let Some(state) = self.current.as_mut() {
                state.is_fast = false;
            }
        }

        match value {
            DirectPathColumnValue::Null => {
                if !metadata.nulls_allowed {
                    return Err(ProtocolError::NullsNotAllowed {
                        column_name: metadata.name.clone(),
                        row_num,
                    });
                }
                self.write_u8_in_piece(TNS_NULL_LENGTH_INDICATOR)?;
                self.bump_segments();
                Ok(())
            }
            DirectPathColumnValue::Bytes(bytes) => {
                if !matches!(
                    metadata.ora_type_num,
                    ORA_TYPE_NUM_VARCHAR
                        | ORA_TYPE_NUM_CHAR
                        | ORA_TYPE_NUM_LONG
                        | ORA_TYPE_NUM_RAW
                        | ORA_TYPE_NUM_LONG_RAW
                ) {
                    return Err(ProtocolError::TtcDecode(
                        "direct path byte value sent for non-character column",
                    ));
                }
                if metadata.max_size > 0 && bytes.len() as u64 > u64::from(metadata.max_size) {
                    return Err(ProtocolError::ValueTooLarge {
                        actual_size: bytes.len(),
                        max_size: metadata.max_size,
                        column_name: metadata.name.clone(),
                        row_num,
                    });
                }
                self.write_raw_bytes_and_length(bytes)
            }
            DirectPathColumnValue::Number(text) => {
                if !matches!(
                    metadata.ora_type_num,
                    ORA_TYPE_NUM_NUMBER | ORA_TYPE_NUM_BINARY_INTEGER
                ) {
                    return Err(ProtocolError::TtcDecode(
                        "direct path number value sent for non-number column",
                    ));
                }
                let encoded = encode_number_text(text)?;
                self.write_raw_bytes_and_length(&encoded)
            }
            DirectPathColumnValue::BinaryDouble(value) => {
                if metadata.ora_type_num != ORA_TYPE_NUM_BINARY_DOUBLE {
                    return Err(ProtocolError::TtcDecode(
                        "direct path binary double sent for other column type",
                    ));
                }
                let encoded = encode_binary_double(*value);
                self.write_raw_bytes_and_length(&encoded)
            }
            DirectPathColumnValue::BinaryFloat(value) => {
                if metadata.ora_type_num != ORA_TYPE_NUM_BINARY_FLOAT {
                    return Err(ProtocolError::TtcDecode(
                        "direct path binary float sent for other column type",
                    ));
                }
                let encoded = encode_binary_float(*value);
                self.write_raw_bytes_and_length(&encoded)
            }
            DirectPathColumnValue::DateTime {
                year,
                month,
                day,
                hour,
                minute,
                second,
                nanosecond,
            } => {
                let encoded = match metadata.ora_type_num {
                    ORA_TYPE_NUM_DATE => {
                        if *nanosecond != 0 {
                            return Err(ProtocolError::TtcDecode(
                                "direct path DATE value has fractional seconds",
                            ));
                        }
                        encode_oracle_date(*year, *month, *day, *hour, *minute, *second)?.to_vec()
                    }
                    // the protocol requires a timestamp with zero fractional
                    // seconds to be transmitted as a 7-byte date
                    ORA_TYPE_NUM_TIMESTAMP | ORA_TYPE_NUM_TIMESTAMP_LTZ => encode_oracle_timestamp(
                        *year,
                        *month,
                        *day,
                        *hour,
                        *minute,
                        *second,
                        *nanosecond,
                    )?,
                    ORA_TYPE_NUM_TIMESTAMP_TZ => encode_oracle_timestamp_tz(
                        *year,
                        *month,
                        *day,
                        *hour,
                        *minute,
                        *second,
                        *nanosecond,
                    )?,
                    _ => {
                        return Err(ProtocolError::TtcDecode(
                            "direct path datetime sent for non-datetime column",
                        ))
                    }
                };
                self.write_raw_bytes_and_length(&encoded)
            }
            DirectPathColumnValue::Boolean(value) => {
                if metadata.ora_type_num != ORA_TYPE_NUM_BOOLEAN {
                    return Err(ProtocolError::TtcDecode(
                        "direct path boolean sent for non-boolean column",
                    ));
                }
                let encoded: &[u8] = if *value { &[1, 1] } else { &[0] };
                self.write_raw_bytes_and_length(encoded)
            }
        }
    }

    /// Finalizes the stream and returns the pieces plus the total piece
    /// length (piece data plus piece headers) for the load stream message.
    pub fn finish(self) -> Result<(Vec<DirectPathPiece>, u32)> {
        if self.current.is_some() {
            return Err(ProtocolError::TtcDecode(
                "direct path stream finished mid-row",
            ));
        }
        let total = u32::try_from(self.total_piece_length)
            .map_err(|_| ProtocolError::DirectPathLoadTooMuchData)?;
        Ok((self.pieces, total))
    }

    fn bump_segments(&mut self) {
        if let Some(state) = self.current.as_mut() {
            state.num_segments = state.num_segments.saturating_add(1);
        }
    }

    fn space_left(&self) -> usize {
        TNS_DPLS_MAX_PIECE_SIZE.saturating_sub(self.data.len())
    }

    fn write_u8_in_piece(&mut self, value: u8) -> Result<()> {
        if self.space_left() < 1 {
            self.finalize_piece()?;
            self.current = Some(PieceState::default());
        }
        self.data.push(value);
        Ok(())
    }

    /// Mirrors `PieceBuffer._write_raw_bytes_and_length`: short values
    /// (<= 0xfa bytes) are written as `u8 length + data`; longer values are
    /// written as one or more `0xfe + u16be length + data` chunks that may
    /// split across pieces with the SPLIT_WITH_PREV/NEXT flags.
    fn write_raw_bytes_and_length(&mut self, bytes: &[u8]) -> Result<()> {
        if bytes.len() <= TNS_DPLS_MAX_SHORT_LENGTH {
            if bytes.len() + 1 > self.space_left() {
                self.finalize_piece()?;
                self.current = Some(PieceState::default());
            }
            self.data.push(bytes.len() as u8);
            self.data.extend_from_slice(bytes);
            self.bump_segments();
            return Ok(());
        }

        let mut remaining = bytes;
        while remaining.len() + 3 > self.space_left() {
            // Fail-closed divergence from the reference: if fewer than four
            // bytes remain in the piece the reference would emit a corrupt
            // zero/negative-length chunk; start a fresh piece instead.
            if self.space_left() < 4 {
                self.finalize_piece()?;
                self.current = Some(PieceState::default());
                continue;
            }
            let chunk_len = self.space_left() - 3;
            let (chunk, rest) = remaining.split_at(chunk_len.min(remaining.len()));
            self.data.push(TNS_DPLS_LONG_LENGTH_INDICATOR);
            self.data
                .extend_from_slice(&(chunk.len() as u16).to_be_bytes());
            self.data.extend_from_slice(chunk);
            remaining = rest;
            if let Some(state) = self.current.as_mut() {
                state.is_split_with_next = true;
            }
            self.bump_segments();
            self.finalize_piece()?;
            self.current = Some(PieceState {
                is_split_with_prev: !remaining.is_empty(),
                ..PieceState::default()
            });
        }
        if !remaining.is_empty() {
            self.bump_segments();
            self.data.push(TNS_DPLS_LONG_LENGTH_INDICATOR);
            self.data
                .extend_from_slice(&(remaining.len() as u16).to_be_bytes());
            self.data.extend_from_slice(remaining);
        }
        Ok(())
    }

    fn finalize_piece(&mut self) -> Result<()> {
        let Some(state) = self.current.take() else {
            return Err(ProtocolError::TtcDecode(
                "direct path piece finalized without an active piece",
            ));
        };
        let mut flags = 0u8;
        if state.is_first {
            flags |= TNS_DPLS_ROW_HEADER_FIRST;
        } else if state.is_split_with_prev {
            flags |= TNS_DPLS_ROW_HEADER_SPLIT_WITH_PREV;
        }
        if state.is_last {
            flags |= TNS_DPLS_ROW_HEADER_LAST;
        } else if state.is_split_with_next {
            flags |= TNS_DPLS_ROW_HEADER_SPLIT_WITH_NEXT;
        }
        let is_fast_row = state.is_first && state.is_last && state.is_fast;
        if is_fast_row {
            flags |= TNS_DPLS_ROW_HEADER_FAST_ROW | TNS_DPLS_ROW_HEADER_FAST_PIECE;
        }
        let num_segments = u8::try_from(state.num_segments)
            .map_err(|_| ProtocolError::TtcDecode("direct path piece segment count overflow"))?;
        let piece = DirectPathPiece {
            flags,
            num_segments,
            data: std::mem::take(&mut self.data),
        };
        let new_length = self.total_piece_length + piece.data.len() as u64 + piece.header_length();
        if new_length > TNS_DPLS_MAX_MESSAGE_SIZE {
            return Err(ProtocolError::DirectPathLoadTooMuchData);
        }
        self.total_piece_length = new_length;
        self.pieces.push(piece);
        // callers decide what the next piece (if any) looks like
        Ok(())
    }
}

/// Fast direct path types per the reference `DbType._is_fast` flags. LONG and
/// LONG RAW (and thus inlined CLOB/BLOB) are not fast.
fn is_fast_dbtype(metadata: &ColumnMetadata) -> bool {
    matches!(
        metadata.ora_type_num,
        ORA_TYPE_NUM_VARCHAR
            | ORA_TYPE_NUM_NUMBER
            | ORA_TYPE_NUM_BINARY_INTEGER
            | ORA_TYPE_NUM_CHAR
            | ORA_TYPE_NUM_DATE
            | ORA_TYPE_NUM_RAW
            | ORA_TYPE_NUM_BINARY_FLOAT
            | ORA_TYPE_NUM_BINARY_DOUBLE
            | ORA_TYPE_NUM_BOOLEAN
            | ORA_TYPE_NUM_TIMESTAMP
            | ORA_TYPE_NUM_TIMESTAMP_TZ
            | ORA_TYPE_NUM_TIMESTAMP_LTZ
    )
}

/// Result of encoding one batch of rows into the piece stream format.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DirectPathStream {
    pub pieces: Vec<DirectPathPiece>,
    pub total_piece_length: u32,
}

/// Encodes a batch of rows into direct path pieces.
///
/// `first_row_num` is the 1-based number of the first row in this batch for
/// error reporting; the reference keeps a running row counter across batches
/// of a single `direct_path_load` call.
pub fn encode_direct_path_rows(
    column_metadata: &[ColumnMetadata],
    rows: &[Vec<DirectPathColumnValue>],
    first_row_num: u64,
) -> Result<DirectPathStream> {
    let mut buffer = DirectPathPieceBuffer::new();
    for (row_index, row) in rows.iter().enumerate() {
        if row.len() != column_metadata.len() {
            return Err(ProtocolError::TtcDecode(
                "direct path row width does not match column metadata",
            ));
        }
        let row_num = first_row_num + row_index as u64;
        buffer.start_row()?;
        for (metadata, value) in column_metadata.iter().zip(row) {
            buffer.add_column_value(metadata, value, row_num)?;
        }
        buffer.finish_row()?;
    }
    let (pieces, total_piece_length) = buffer.finish()?;
    Ok(DirectPathStream {
        pieces,
        total_piece_length,
    })
}

/// Builds the payload for TTC function 129 (direct path load stream).
///
/// Mirrors `DirectPathLoadStreamMessage._write_message`.
pub fn build_direct_path_load_stream_payload(
    cursor_id: u16,
    stream: &DirectPathStream,
    seq_num: u8,
) -> Result<Vec<u8>> {
    let mut writer = TtcWriter::new();
    writer.write_function_code_with_seq(TNS_FUNC_DIRECT_PATH_LOAD_STREAM, seq_num);
    writer.write_ub8(0); // token number
    writer.write_ub2(cursor_id);
    writer.write_u8(1); // pointer (buffer)
    writer.write_ub4(stream.total_piece_length);
    writer.write_ub4(TNS_DP_STREAM_VERSION);
    writer.write_u8(0); // pointer (input values)
    writer.write_ub4(0); // number of input values
    writer.write_u8(1); // pointer (output values)
    writer.write_u8(1); // pointer (output values length)
    for piece in &stream.pieces {
        piece.write_to(&mut writer)?;
    }
    Ok(writer.into_bytes())
}

/// Batch/chunk state machine shared by `executemany` ingestion and direct
/// path load. Port of `BatchLoadManager`/`DataFrameBatchLoadManager`
/// (impl/base/batch_load_manager.pyx).
///
/// The data source is modelled as a list of chunks (an Arrow chunked array
/// has one entry per chunk; a plain list of rows is a single chunk). Batches
/// never span chunk boundaries; `message_offset` is the row offset *within
/// the current chunk* that must accompany the execute/load message.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct BatchLoadState {
    chunk_lengths: Vec<u64>,
    batch_size: u32,
    chunk_index: usize,
    offset: u64,
    message_offset: u64,
    num_rows: u32,
}

impl BatchLoadState {
    pub fn new(chunk_lengths: Vec<u64>, batch_size: u32) -> Result<Self> {
        if batch_size == 0 {
            return Err(ProtocolError::TtcDecode(
                "batch_size must be a positive integer",
            ));
        }
        let mut state = Self {
            chunk_lengths,
            batch_size,
            chunk_index: 0,
            offset: 0,
            message_offset: 0,
            num_rows: 0,
        };
        state.advance_batch();
        Ok(state)
    }

    /// Creates the state machine for a single-chunk source of `total_rows`
    /// rows (a plain list of rows).
    pub fn for_rows(total_rows: u64, batch_size: u32) -> Result<Self> {
        Self::new(vec![total_rows], batch_size)
    }

    /// Number of rows in the current batch; zero means the load is complete.
    pub fn num_rows(&self) -> u32 {
        self.num_rows
    }

    /// Row offset of the current batch within the current chunk.
    pub fn offset(&self) -> u64 {
        self.offset
    }

    /// Offset to send with the execute/load message (row offset within the
    /// current chunk at the time the batch was formed).
    pub fn message_offset(&self) -> u64 {
        self.message_offset
    }

    /// Index of the chunk the current batch draws from.
    pub fn chunk_index(&self) -> usize {
        self.chunk_index
    }

    pub fn is_done(&self) -> bool {
        self.num_rows == 0
    }

    /// Advances to the next batch (mirrors `BatchLoadManager.next_batch`).
    pub fn next_batch(&mut self) {
        self.offset += u64::from(self.num_rows);
        self.advance_batch();
    }

    fn rows_in_current_chunk(&self) -> u64 {
        self.chunk_lengths
            .get(self.chunk_index)
            .copied()
            .unwrap_or(0)
    }

    fn calculate_num_rows_in_batch(&mut self) {
        let remaining = self.rows_in_current_chunk().saturating_sub(self.offset);
        self.num_rows = u32::try_from(remaining.min(u64::from(self.batch_size))).unwrap_or(0);
    }

    fn advance_batch(&mut self) {
        self.message_offset = self.offset;
        self.calculate_num_rows_in_batch();
        if self.num_rows == 0 {
            self.advance_chunk();
        }
    }

    fn advance_chunk(&mut self) {
        while self.chunk_index + 1 < self.chunk_lengths.len() {
            self.offset = 0;
            self.message_offset = 0;
            self.chunk_index += 1;
            self.calculate_num_rows_in_batch();
            if self.num_rows > 0 {
                break;
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn column(name: &str, ora_type_num: u8, max_size: u32, nulls_allowed: bool) -> ColumnMetadata {
        ColumnMetadata {
            name: name.to_string(),
            ora_type_num,
            csfrm: if matches!(
                ora_type_num,
                ORA_TYPE_NUM_VARCHAR | ORA_TYPE_NUM_CHAR | ORA_TYPE_NUM_LONG
            ) {
                CS_FORM_IMPLICIT
            } else {
                0
            },
            precision: 0,
            scale: 0,
            buffer_size: max_size,
            max_size,
            nulls_allowed,
            is_json: false,
            is_oson: false,
            object_schema: None,
            object_type_name: None,
            is_array: false,
        }
    }

    #[test]
    fn prepare_payload_matches_reference_layout() {
        let payload = build_direct_path_prepare_payload(
            "pythontest",
            "dpl_golden",
            &["id".to_string(), "name".to_string()],
            10,
        )
        .expect("payload should build");
        // header: msg type, function code, seq, token
        assert_eq!(&payload[..4], &[3, 128, 10, 0]);
        let mut expected = vec![
            1, 1, // ub4 op code LOAD
            1, // kw pointer
            1, 4, // ub4 kw length = 2 columns + 2
            1, // input array pointer
            1, 15, // ub2 in values length
            1, 1, 1, 1, 1, 1, // six pointers
        ];
        // schema name
        expected.extend_from_slice(&[0, 1, 10]);
        expected.extend_from_slice(&[10]);
        expected.extend_from_slice(b"pythontest");
        expected.extend_from_slice(&[1, 3]);
        // table name
        expected.extend_from_slice(&[0, 1, 10]);
        expected.extend_from_slice(&[10]);
        expected.extend_from_slice(b"dpl_golden");
        expected.extend_from_slice(&[1, 1]);
        // column names
        expected.extend_from_slice(&[0, 1, 2, 2]);
        expected.extend_from_slice(b"id");
        expected.extend_from_slice(&[1, 4]);
        expected.extend_from_slice(&[0, 1, 4, 4]);
        expected.extend_from_slice(b"name");
        expected.extend_from_slice(&[1, 4]);
        // in values: 400, 400, 9 zeros, 0xffff, 0, 0, 1
        expected.extend_from_slice(&[2, 0x01, 0x90, 2, 0x01, 0x90]);
        expected.extend_from_slice(&[0; 9]);
        expected.extend_from_slice(&[2, 0xff, 0xff, 0, 0, 1, 1]);
        assert_eq!(&payload[4..], expected.as_slice());
    }

    #[test]
    fn op_payload_matches_reference_layout() {
        let payload = build_direct_path_op_payload(1, TNS_DP_OP_FINISH, 12);
        assert_eq!(
            payload,
            vec![3, 130, 12, 0, 1, 2, 1, 1, 0, 0, 1, 1],
            "fn code, seq, token, ub4 op, ub2 cursor, ptr 0, ub4 0, ptr 1, ptr 1"
        );
    }

    #[test]
    fn single_fast_row_produces_one_fast_piece() {
        let columns = vec![
            column("ID", ORA_TYPE_NUM_NUMBER, 0, false),
            column("NAME", ORA_TYPE_NUM_VARCHAR, 100, false),
        ];
        let rows = vec![vec![
            DirectPathColumnValue::Number("1".into()),
            DirectPathColumnValue::Bytes(b"alpha".to_vec()),
        ]];
        let stream = encode_direct_path_rows(&columns, &rows, 1).expect("stream should encode");
        assert_eq!(stream.pieces.len(), 1);
        let piece = &stream.pieces[0];
        assert_eq!(
            piece.flags(),
            TNS_DPLS_ROW_HEADER_FIRST
                | TNS_DPLS_ROW_HEADER_LAST
                | TNS_DPLS_ROW_HEADER_FAST_ROW
                | TNS_DPLS_ROW_HEADER_FAST_PIECE
        );
        assert_eq!(piece.num_segments(), 2);
        // number 1 encodes as c1 02; "alpha" as length + bytes
        assert_eq!(
            piece.data(),
            &[2, 0xc1, 0x02, 5, b'a', b'l', b'p', b'h', b'a']
        );
        // total = data + 4-byte fast header
        assert_eq!(stream.total_piece_length, piece.data().len() as u32 + 4);
    }

    #[test]
    fn long_column_clears_fast_flag() {
        let columns = vec![column("WIDE", ORA_TYPE_NUM_LONG, 0, false)];
        let rows = vec![vec![DirectPathColumnValue::Bytes(vec![b'x'; 10])]];
        let stream = encode_direct_path_rows(&columns, &rows, 1).expect("stream should encode");
        assert_eq!(stream.pieces.len(), 1);
        assert_eq!(
            stream.pieces[0].flags(),
            TNS_DPLS_ROW_HEADER_FIRST | TNS_DPLS_ROW_HEADER_LAST
        );
        // 1 length byte + 10 data bytes + 2-byte slow header
        assert_eq!(stream.total_piece_length, 11 + 2);
    }

    #[test]
    fn null_values_encode_as_null_indicator() {
        let columns = vec![column("SALARY", ORA_TYPE_NUM_NUMBER, 0, true)];
        let rows = vec![vec![DirectPathColumnValue::Null]];
        let stream = encode_direct_path_rows(&columns, &rows, 1).expect("stream should encode");
        assert_eq!(stream.pieces[0].data(), &[0xff]);
        assert_eq!(stream.pieces[0].num_segments(), 1);
    }

    #[test]
    fn null_into_not_null_column_raises_dpy_8001() {
        let columns = vec![column("NAME", ORA_TYPE_NUM_VARCHAR, 100, false)];
        let rows = vec![vec![DirectPathColumnValue::Null]];
        let err = encode_direct_path_rows(&columns, &rows, 1).expect_err("nulls must be rejected");
        assert!(
            err.to_string().starts_with("DPY-8001:"),
            "unexpected error: {err}"
        );
        assert!(err.to_string().contains("\"NAME\""), "{err}");
        assert!(err.to_string().contains("row 1"), "{err}");
    }

    #[test]
    fn oversized_value_raises_dpy_8000() {
        let columns = vec![column("NAME", ORA_TYPE_NUM_VARCHAR, 4, false)];
        let rows = vec![vec![DirectPathColumnValue::Bytes(b"toolong".to_vec())]];
        let err = encode_direct_path_rows(&columns, &rows, 3).expect_err("size must be enforced");
        assert!(
            err.to_string().starts_with("DPY-8000:"),
            "unexpected error: {err}"
        );
        assert!(err.to_string().contains("row 3"), "{err}");
    }

    #[test]
    fn long_values_use_fe_chunked_segments() {
        // 600 bytes > 0xfa, must use the 0xfe + u16be length form
        let columns = vec![column("WIDE", ORA_TYPE_NUM_VARCHAR, 1000, false)];
        let value = vec![b'q'; 600];
        let rows = vec![vec![DirectPathColumnValue::Bytes(value.clone())]];
        let stream = encode_direct_path_rows(&columns, &rows, 1).expect("stream should encode");
        assert_eq!(stream.pieces.len(), 1);
        let piece = &stream.pieces[0];
        assert_eq!(piece.num_segments(), 1);
        let mut expected = vec![0xfe, 0x02, 0x58];
        expected.extend_from_slice(&value);
        assert_eq!(piece.data(), expected.as_slice());
    }

    #[test]
    fn values_larger_than_piece_split_across_pieces_with_split_flags() {
        let columns = vec![column("WIDE", ORA_TYPE_NUM_LONG, 0, false)];
        let total = TNS_DPLS_MAX_PIECE_SIZE + 100;
        let rows = vec![vec![DirectPathColumnValue::Bytes(vec![b'z'; total])]];
        let stream = encode_direct_path_rows(&columns, &rows, 1).expect("stream should encode");
        assert_eq!(stream.pieces.len(), 2);
        let first = &stream.pieces[0];
        let second = &stream.pieces[1];
        assert_eq!(
            first.flags(),
            TNS_DPLS_ROW_HEADER_FIRST | TNS_DPLS_ROW_HEADER_SPLIT_WITH_NEXT
        );
        assert_eq!(
            second.flags(),
            TNS_DPLS_ROW_HEADER_SPLIT_WITH_PREV | TNS_DPLS_ROW_HEADER_LAST
        );
        // first piece is filled to the brim: 3-byte chunk header + payload
        assert_eq!(first.data().len(), TNS_DPLS_MAX_PIECE_SIZE);
        assert_eq!(first.data()[0], 0xfe);
        let first_chunk = usize::from(u16::from_be_bytes([first.data()[1], first.data()[2]]));
        assert_eq!(first_chunk, TNS_DPLS_MAX_PIECE_SIZE - 3);
        let second_chunk = usize::from(u16::from_be_bytes([second.data()[1], second.data()[2]]));
        assert_eq!(first_chunk + second_chunk, total);
        assert_eq!(
            stream.total_piece_length as usize,
            first.data().len() + second.data().len() + 2 + 2
        );
    }

    #[test]
    fn segment_count_caps_at_255_per_piece() {
        let columns: Vec<ColumnMetadata> = (0..300)
            .map(|i| column(&format!("C{i}"), ORA_TYPE_NUM_NUMBER, 0, true))
            .collect();
        let row: Vec<DirectPathColumnValue> =
            (0..300).map(|_| DirectPathColumnValue::Null).collect();
        let stream = encode_direct_path_rows(&columns, &[row], 1).expect("stream should encode");
        assert_eq!(stream.pieces.len(), 2);
        assert_eq!(stream.pieces[0].num_segments(), 255);
        assert_eq!(stream.pieces[1].num_segments(), 45);
        // continuation piece created by the segment cap carries neither FIRST
        // nor SPLIT_WITH_PREV (mirrors the reference)
        assert_eq!(stream.pieces[0].flags(), TNS_DPLS_ROW_HEADER_FIRST);
        assert_eq!(stream.pieces[1].flags(), TNS_DPLS_ROW_HEADER_LAST);
    }

    #[test]
    fn timestamp_with_zero_fraction_collapses_to_seven_bytes() {
        let columns = vec![column("TS", ORA_TYPE_NUM_TIMESTAMP, 0, true)];
        let rows = vec![vec![DirectPathColumnValue::DateTime {
            year: 2024,
            month: 1,
            day: 2,
            hour: 3,
            minute: 4,
            second: 5,
            nanosecond: 0,
        }]];
        let stream = encode_direct_path_rows(&columns, &rows, 1).expect("stream should encode");
        assert_eq!(
            stream.pieces[0].data(),
            &[7, 120, 124, 1, 2, 4, 5, 6],
            "7-byte date form expected when fractional seconds are zero"
        );
    }

    #[test]
    fn boolean_values_encode_per_reference() {
        let columns = vec![column("FLAG", ORA_TYPE_NUM_BOOLEAN, 0, true)];
        let rows = vec![
            vec![DirectPathColumnValue::Boolean(true)],
            vec![DirectPathColumnValue::Boolean(false)],
        ];
        let stream = encode_direct_path_rows(&columns, &rows, 1).expect("stream should encode");
        assert_eq!(stream.pieces[0].data(), &[2, 1, 1]);
        assert_eq!(stream.pieces[1].data(), &[1, 0]);
    }

    #[test]
    fn row_width_mismatch_is_rejected() {
        let columns = vec![
            column("A", ORA_TYPE_NUM_NUMBER, 0, true),
            column("B", ORA_TYPE_NUM_NUMBER, 0, true),
        ];
        let rows = vec![vec![DirectPathColumnValue::Null]];
        assert!(encode_direct_path_rows(&columns, &rows, 1).is_err());
    }

    #[test]
    fn metadata_overrides_inline_lobs() {
        let mut clob = column("DOC", ORA_TYPE_NUM_CLOB, 0, true);
        clob.csfrm = CS_FORM_IMPLICIT;
        apply_direct_path_metadata_overrides(&mut clob, 873);
        assert_eq!(clob.ora_type_num, ORA_TYPE_NUM_LONG);
        assert_eq!(clob.csfrm, CS_FORM_NCHAR, "multi-byte charset uses NCHAR");

        let mut clob = column("DOC", ORA_TYPE_NUM_CLOB, 0, true);
        clob.csfrm = CS_FORM_IMPLICIT;
        apply_direct_path_metadata_overrides(&mut clob, 178);
        assert_eq!(
            clob.csfrm, CS_FORM_IMPLICIT,
            "single-byte charset keeps form"
        );

        let mut blob = column("BIN", ORA_TYPE_NUM_BLOB, 0, true);
        apply_direct_path_metadata_overrides(&mut blob, 873);
        assert_eq!(blob.ora_type_num, ORA_TYPE_NUM_LONG_RAW);
        assert_eq!(blob.csfrm, 0);
    }

    #[test]
    fn batch_state_single_chunk_splits_by_batch_size() {
        let mut state = BatchLoadState::for_rows(5, 2).expect("state should build");
        assert_eq!(
            (state.num_rows(), state.offset(), state.message_offset()),
            (2, 0, 0)
        );
        state.next_batch();
        assert_eq!(
            (state.num_rows(), state.offset(), state.message_offset()),
            (2, 2, 2)
        );
        state.next_batch();
        assert_eq!(
            (state.num_rows(), state.offset(), state.message_offset()),
            (1, 4, 4)
        );
        state.next_batch();
        assert!(state.is_done());
    }

    #[test]
    fn batch_state_never_spans_chunks() {
        // chunks of 3 and 2 rows with batch size 2: batches are 2, 1, 2
        let mut state = BatchLoadState::new(vec![3, 2], 2).expect("state should build");
        assert_eq!(
            (
                state.chunk_index(),
                state.num_rows(),
                state.message_offset()
            ),
            (0, 2, 0)
        );
        state.next_batch();
        assert_eq!(
            (
                state.chunk_index(),
                state.num_rows(),
                state.message_offset()
            ),
            (0, 1, 2)
        );
        state.next_batch();
        assert_eq!(
            (
                state.chunk_index(),
                state.num_rows(),
                state.message_offset()
            ),
            (1, 2, 0)
        );
        state.next_batch();
        assert!(state.is_done());
    }

    #[test]
    fn batch_state_skips_empty_chunks() {
        let mut state = BatchLoadState::new(vec![0, 0, 3], 10).expect("state should build");
        assert_eq!((state.chunk_index(), state.num_rows()), (2, 3));
        state.next_batch();
        assert!(state.is_done());
    }

    #[test]
    fn batch_state_rejects_zero_batch_size() {
        assert!(BatchLoadState::for_rows(5, 0).is_err());
    }

    #[test]
    fn batch_state_empty_source_is_done_immediately() {
        let state = BatchLoadState::for_rows(0, 10).expect("state should build");
        assert!(state.is_done());
    }

    #[test]
    fn load_stream_payload_header_matches_reference_layout() {
        let columns = vec![column("ID", ORA_TYPE_NUM_NUMBER, 0, false)];
        let rows = vec![vec![DirectPathColumnValue::Number("1".into())]];
        let stream = encode_direct_path_rows(&columns, &rows, 1).expect("stream should encode");
        let payload =
            build_direct_path_load_stream_payload(1, &stream, 11).expect("payload should build");
        let mut expected = vec![
            3, 129, 11, // fn code + seq
            0,  // token
            1, 1, // ub2 cursor id
            1, // buffer pointer
            1, 7, // ub4 total piece length (3 data + 4 header)
            2, 0x01, 0x90, // ub4 stream version 400
            0,    // input values pointer
            0,    // ub4 input values count
            1, 1, // output pointers
            0x3c, 0, 7, 1, // piece: flags, u16be total, num segments
            2, 0xc1, 0x02, // number 1
        ];
        assert_eq!(payload, std::mem::take(&mut expected));
    }
}
