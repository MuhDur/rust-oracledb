#![forbid(unsafe_code)]

use super::*;
use crate::wire::ProtocolLimits;

/// Validate `slice` as UTF-8 for the hot borrowed-text decode path, returning the
/// borrowed `&str` on success or `()` on rejection (the caller falls back to the
/// owned `TextRaw` carrier — semantics identical regardless of validator).
///
/// With the `simd-decode` feature this uses `simdutf8::basic::from_utf8`, whose
/// accept/reject decision is byte-for-byte identical to `core::str::from_utf8`
/// (it validates the exact same UTF-8 grammar — it only declines to compute the
/// error *position*, which this path never uses). The crate stays
/// `#![forbid(unsafe_code)]`-clean: `simdutf8`'s SIMD `unsafe` is encapsulated
/// inside that dependency and we call only its safe API.
#[inline]
fn validate_utf8(slice: &[u8]) -> core::result::Result<&str, ()> {
    #[cfg(feature = "simd-decode")]
    {
        simdutf8::basic::from_utf8(slice).map_err(|_| ())
    }
    #[cfg(not(feature = "simd-decode"))]
    {
        core::str::from_utf8(slice).map_err(|_| ())
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum LobDecodeMode {
    PlainLocator,
    DefineMetadata,
}

pub fn build_fetch_payload(cursor_id: u32, arraysize: u32, ttc_field_version: u8) -> Vec<u8> {
    build_fetch_payload_with_seq(cursor_id, arraysize, 1, ttc_field_version)
}

pub fn build_fetch_payload_with_seq(
    cursor_id: u32,
    arraysize: u32,
    seq_num: u8,
    ttc_field_version: u8,
) -> Vec<u8> {
    // Fixed tiny payload (function code + ub8 + two ub4 ≈ <=20 bytes). Prealloc
    // so the small pushes do not grow the Vec through doublings; built every
    // fetch page, so this matters on multi-page fetches. Bytes unchanged.
    let mut writer = TtcWriter::with_capacity(32);
    writer.write_function_header(TNS_FUNC_FETCH, seq_num, ttc_field_version);
    writer.write_ub4(cursor_id);
    writer.write_ub4(arraysize);
    writer.into_bytes()
}

pub fn build_define_fetch_payload_with_seq(
    cursor_id: u32,
    arraysize: u32,
    seq_num: u8,
    define_columns: &[ColumnMetadata],
    ttc_field_version: u8,
) -> Result<Vec<u8>> {
    let define_count =
        u32::try_from(define_columns.len()).map_err(|_| ProtocolError::InvalidPacketLength {
            length: define_columns.len(),
            minimum: 0,
        })?;
    let mut writer = TtcWriter::new();
    writer.write_function_header(TNS_FUNC_EXECUTE, seq_num, ttc_field_version);
    writer.write_ub4(TNS_EXEC_OPTION_DEFINE | TNS_EXEC_OPTION_NOT_PLSQL);
    writer.write_ub4(cursor_id);
    writer.write_u8(0);
    writer.write_ub4(0);
    writer.write_u8(1);
    writer.write_ub4(13);
    writer.write_u8(0);
    writer.write_u8(0);
    writer.write_ub4(0);
    writer.write_ub4(arraysize);
    writer.write_ub4(TNS_MAX_LONG_LENGTH);
    writer.write_u8(0);
    writer.write_ub4(0);
    writer.write_u8(0);
    writer.write_u8(0);
    writer.write_u8(0);
    writer.write_u8(0);
    writer.write_u8(0);
    writer.write_u8(1);
    writer.write_ub4(define_count);
    writer.write_ub4(0);
    writer.write_u8(0);
    writer.write_u8(1);
    writer.write_u8(0);
    writer.write_ub4(0);
    writer.write_u8(0);
    writer.write_ub4(0);
    writer.write_ub4(0);
    writer.write_u8(0);
    writer.write_ub4(0);
    writer.write_u8(0);
    writer.write_u8(0);
    writer.write_ub4(0);
    writer.write_ub4(0);
    writer.write_ub4(0);
    writer.write_ub4(0);
    writer.write_ub4(0);
    writer.write_ub4(0);
    writer.write_ub4(0);
    writer.write_ub4(arraysize);
    writer.write_ub4(0);
    writer.write_ub4(0);
    writer.write_ub4(0);
    writer.write_ub4(0);
    writer.write_ub4(0);
    writer.write_ub4(1);
    writer.write_ub4(0);
    writer.write_ub4(0);
    writer.write_ub4(0);
    writer.write_ub4(0);
    writer.write_ub4(0);
    for metadata in define_columns {
        write_define_column_metadata(&mut writer, metadata);
    }
    Ok(writer.into_bytes())
}

pub(crate) fn write_define_column_metadata(writer: &mut TtcWriter, metadata: &ColumnMetadata) {
    // reference base.pyx: VECTOR (and JSON) columns advertise a LOB-prefetch
    // buffer so the server streams the image inline rather than returning a
    // bare temp-LOB locator
    let (mut buffer_size, cont_flags, lob_prefetch_length) = match metadata.ora_type_num {
        ORA_TYPE_NUM_CLOB | ORA_TYPE_NUM_BLOB => (metadata.buffer_size, TNS_LOB_PREFETCH_FLAG, 0),
        ORA_TYPE_NUM_VECTOR => (
            TNS_VECTOR_MAX_LENGTH,
            TNS_LOB_PREFETCH_FLAG,
            TNS_VECTOR_MAX_LENGTH,
        ),
        ORA_TYPE_NUM_JSON => (
            TNS_JSON_MAX_LENGTH,
            TNS_LOB_PREFETCH_FLAG,
            TNS_JSON_MAX_LENGTH,
        ),
        _ => (metadata.buffer_size, 0, 0),
    };
    buffer_size = buffer_size.max(1);
    writer.write_u8(metadata.ora_type_num);
    writer.write_u8(TNS_BIND_USE_INDICATORS);
    writer.write_u8(0);
    writer.write_u8(0);
    writer.write_ub4(buffer_size);
    writer.write_ub4(0);
    writer.write_ub8(cont_flags);
    writer.write_ub4(0);
    writer.write_ub2(0);
    if metadata.csfrm != 0 {
        writer.write_ub2(TNS_CHARSET_UTF8);
    } else {
        writer.write_ub2(0);
    }
    writer.write_u8(metadata.csfrm);
    writer.write_ub4(lob_prefetch_length);
    writer.write_ub4(0);
}

pub fn parse_query_response(
    payload: &[u8],
    capabilities: ClientCapabilities,
) -> Result<QueryResult> {
    parse_query_response_with_previous(payload, capabilities, None)
}

pub fn parse_query_response_with_limits(
    payload: &[u8],
    capabilities: ClientCapabilities,
    limits: ProtocolLimits,
) -> Result<QueryResult> {
    parse_query_response_with_context_binds_options_and_limits(
        payload,
        capabilities,
        &[],
        None,
        &[],
        &[],
        false,
        ExecuteOptions::default(),
        limits,
    )
}

pub fn parse_query_response_with_binds(
    payload: &[u8],
    capabilities: ClientCapabilities,
    binds: &[BindValue],
) -> Result<QueryResult> {
    parse_query_response_with_binds_and_options(
        payload,
        capabilities,
        binds,
        ExecuteOptions::default(),
    )
}

pub fn parse_query_response_with_binds_and_options(
    payload: &[u8],
    capabilities: ClientCapabilities,
    binds: &[BindValue],
    exec_options: ExecuteOptions,
) -> Result<QueryResult> {
    parse_query_response_with_binds_options_and_columns(
        payload,
        capabilities,
        binds,
        exec_options,
        &[],
    )
}

/// `known_columns` carries the fetch metadata of a re-executed statement
/// whose response does not repeat the describe information (reference keeps
/// the statement's fetch vars across executions).
pub fn parse_query_response_with_binds_options_and_columns(
    payload: &[u8],
    capabilities: ClientCapabilities,
    binds: &[BindValue],
    exec_options: ExecuteOptions,
    known_columns: &[ColumnMetadata],
) -> Result<QueryResult> {
    let bind_columns = binds.iter().map(bind_column_metadata).collect::<Vec<_>>();
    let output_bind_indexes = binds
        .iter()
        .enumerate()
        .filter_map(|(index, value)| value.is_return_output().then_some(index))
        .collect::<Vec<_>>();
    parse_query_response_with_context_binds_and_options(
        payload,
        capabilities,
        known_columns,
        None,
        &bind_columns,
        &output_bind_indexes,
        false,
        exec_options,
    )
}

pub fn parse_query_response_with_binds_options_columns_and_limits(
    payload: &[u8],
    capabilities: ClientCapabilities,
    binds: &[BindValue],
    exec_options: ExecuteOptions,
    known_columns: &[ColumnMetadata],
    limits: ProtocolLimits,
) -> Result<QueryResult> {
    limits.check_binds(binds.len())?;
    let bind_columns = binds.iter().map(bind_column_metadata).collect::<Vec<_>>();
    let output_bind_indexes = binds
        .iter()
        .enumerate()
        .filter_map(|(index, value)| value.is_return_output().then_some(index))
        .collect::<Vec<_>>();
    parse_query_response_with_context_binds_options_and_limits(
        payload,
        capabilities,
        known_columns,
        None,
        &bind_columns,
        &output_bind_indexes,
        false,
        exec_options,
        limits,
    )
}

pub fn parse_query_response_with_previous(
    payload: &[u8],
    capabilities: ClientCapabilities,
    previous_row: Option<&[Option<QueryValue>]>,
) -> Result<QueryResult> {
    parse_query_response_with_context(payload, capabilities, &[], previous_row)
}

pub fn parse_query_response_with_context(
    payload: &[u8],
    capabilities: ClientCapabilities,
    previous_columns: &[ColumnMetadata],
    previous_row: Option<&[Option<QueryValue>]>,
) -> Result<QueryResult> {
    parse_query_response_with_context_and_binds(
        payload,
        capabilities,
        previous_columns,
        previous_row,
        &[],
        &[],
        false,
        ProtocolLimits::DEFAULT,
    )
}

pub fn parse_fetch_response_with_context(
    payload: &[u8],
    capabilities: ClientCapabilities,
    previous_columns: &[ColumnMetadata],
    previous_row: Option<&[Option<QueryValue>]>,
) -> Result<QueryResult> {
    parse_fetch_response_with_context_and_limits(
        payload,
        capabilities,
        previous_columns,
        previous_row,
        ProtocolLimits::DEFAULT,
    )
}

pub fn parse_fetch_response_with_context_and_limits(
    payload: &[u8],
    capabilities: ClientCapabilities,
    previous_columns: &[ColumnMetadata],
    previous_row: Option<&[Option<QueryValue>]>,
    limits: ProtocolLimits,
) -> Result<QueryResult> {
    parse_query_response_with_context_binds_options_lob_mode_and_limits(
        payload,
        capabilities,
        previous_columns,
        previous_row,
        &[],
        &[],
        true,
        ExecuteOptions::default(),
        LobDecodeMode::PlainLocator,
        limits,
    )
}

pub fn parse_define_fetch_response_with_context_and_limits(
    payload: &[u8],
    capabilities: ClientCapabilities,
    previous_columns: &[ColumnMetadata],
    previous_row: Option<&[Option<QueryValue>]>,
    limits: ProtocolLimits,
) -> Result<QueryResult> {
    parse_query_response_with_context_binds_options_lob_mode_and_limits(
        payload,
        capabilities,
        previous_columns,
        previous_row,
        &[],
        &[],
        true,
        ExecuteOptions::default(),
        LobDecodeMode::DefineMetadata,
        limits,
    )
}

#[allow(clippy::too_many_arguments)] // mirrors the reference message attribute set
pub(crate) fn parse_query_response_with_context_and_binds(
    payload: &[u8],
    capabilities: ClientCapabilities,
    previous_columns: &[ColumnMetadata],
    previous_row: Option<&[Option<QueryValue>]>,
    bind_columns: &[ColumnMetadata],
    output_bind_indexes: &[usize],
    fetch_long_status: bool,
    limits: ProtocolLimits,
) -> Result<QueryResult> {
    parse_query_response_with_context_binds_options_and_limits(
        payload,
        capabilities,
        previous_columns,
        previous_row,
        bind_columns,
        output_bind_indexes,
        fetch_long_status,
        ExecuteOptions::default(),
        limits,
    )
}

#[allow(clippy::too_many_arguments)] // mirrors the reference message attribute set
pub(crate) fn parse_query_response_with_context_binds_and_options(
    payload: &[u8],
    capabilities: ClientCapabilities,
    previous_columns: &[ColumnMetadata],
    previous_row: Option<&[Option<QueryValue>]>,
    bind_columns: &[ColumnMetadata],
    output_bind_indexes: &[usize],
    fetch_long_status: bool,
    exec_options: ExecuteOptions,
) -> Result<QueryResult> {
    parse_query_response_with_context_binds_options_and_limits(
        payload,
        capabilities,
        previous_columns,
        previous_row,
        bind_columns,
        output_bind_indexes,
        fetch_long_status,
        exec_options,
        ProtocolLimits::DEFAULT,
    )
}

#[allow(clippy::too_many_arguments)] // mirrors the reference message attribute set
pub(crate) fn parse_query_response_with_context_binds_options_and_limits(
    payload: &[u8],
    capabilities: ClientCapabilities,
    previous_columns: &[ColumnMetadata],
    previous_row: Option<&[Option<QueryValue>]>,
    bind_columns: &[ColumnMetadata],
    output_bind_indexes: &[usize],
    fetch_long_status: bool,
    exec_options: ExecuteOptions,
    limits: ProtocolLimits,
) -> Result<QueryResult> {
    parse_query_response_with_context_binds_options_lob_mode_and_limits(
        payload,
        capabilities,
        previous_columns,
        previous_row,
        bind_columns,
        output_bind_indexes,
        fetch_long_status,
        exec_options,
        LobDecodeMode::DefineMetadata,
        limits,
    )
}

#[allow(clippy::too_many_arguments)] // mirrors the reference message attribute set
fn parse_query_response_with_context_binds_options_lob_mode_and_limits(
    payload: &[u8],
    capabilities: ClientCapabilities,
    previous_columns: &[ColumnMetadata],
    previous_row: Option<&[Option<QueryValue>]>,
    bind_columns: &[ColumnMetadata],
    output_bind_indexes: &[usize],
    fetch_long_status: bool,
    exec_options: ExecuteOptions,
    lob_decode_mode: LobDecodeMode,
    limits: ProtocolLimits,
) -> Result<QueryResult> {
    let mut reader = TtcReader::with_limits(payload, limits)?;
    let mut result = QueryResult {
        columns: previous_columns.to_vec(),
        more_rows: true,
        ..QueryResult::default()
    };
    // A re-executed cursor whose column type changed to CLOB/BLOB but was
    // previously fetched as CHAR/VARCHAR/RAW streams the value in LONG/LONG RAW
    // form (see `adjust_refetch_metadata`), which carries the LONG status
    // trailer (null indicator + return code) after each value — even on the
    // execute path that otherwise passes `fetch_long_status = false`. Promote
    // the flag when such an adjustment fires so `parse_row_data` consumes that
    // trailer instead of mis-framing the next message (bead rust-oracledb-f0ad).
    let mut fetch_long_status = fetch_long_status;
    let mut bit_vector: Option<Vec<u8>> = None;
    let mut out_bind_indexes: Vec<usize> = Vec::new();
    while reader.remaining() > 0 {
        let message_type = reader.read_u8()?;
        match message_type {
            0 => {}
            TNS_MSG_TYPE_DESCRIBE_INFO => {
                let _describe_name = reader.read_bytes()?;
                let previous = std::mem::take(&mut result.columns);
                parse_describe_info(&mut reader, capabilities, &mut result)?;
                // re-executing an open cursor whose underlying types changed:
                // the server re-describes mid-response but still streams the
                // row data in the adjusted (LONG/LONG RAW) form expected by
                // the previous fetch metadata (reference `_adjust_metadata`,
                // impl/thin/messages/base.pyx:820-845, applied during
                // `_process_describe_info`).
                for (index, column) in result.columns.iter_mut().enumerate() {
                    if let Some(prev) = previous.get(index) {
                        if adjust_refetch_metadata(prev, column) {
                            // the adjusted column (now LONG / LONG RAW) is
                            // streamed with the LONG status trailer.
                            fetch_long_status = true;
                        }
                    }
                }
            }
            TNS_MSG_TYPE_ROW_HEADER => {
                bit_vector = parse_row_header(&mut reader)?;
            }
            TNS_MSG_TYPE_ROW_DATA => {
                if result.columns.is_empty() && !out_bind_indexes.is_empty() {
                    parse_out_bind_row_data(
                        &mut reader,
                        &mut result,
                        bind_columns,
                        &out_bind_indexes,
                    )?;
                } else if result.columns.is_empty() && !output_bind_indexes.is_empty() {
                    parse_returning_row_data(
                        &mut reader,
                        &mut result,
                        bind_columns,
                        output_bind_indexes,
                    )?;
                } else {
                    parse_row_data(
                        &mut reader,
                        &mut result,
                        bit_vector.as_deref(),
                        previous_row,
                        fetch_long_status,
                        lob_decode_mode,
                    )?;
                }
                bit_vector = None;
            }
            TNS_MSG_TYPE_BIT_VECTOR => {
                bit_vector = Some(parse_bit_vector(&mut reader, result.columns.len())?);
            }
            TNS_MSG_TYPE_PARAMETER => {
                let params =
                    parse_query_return_parameters(&mut reader, exec_options.arraydmlrowcounts)?;
                if exec_options.arraydmlrowcounts {
                    result.array_dml_row_counts = Some(params.row_counts.unwrap_or_default());
                }
                if params.query_id.is_some() {
                    result.query_id = params.query_id;
                }
            }
            TNS_MSG_TYPE_STATUS => {
                let call_status = reader.read_ub4()?;
                let _seq = reader.read_ub2()?;
                result.txn_in_progress = Some(call_status & TNS_EOCS_FLAGS_TXN_IN_PROGRESS != 0);
            }
            TNS_MSG_TYPE_IO_VECTOR => {
                out_bind_indexes = parse_io_vector(&mut reader, bind_columns.len())?
                    .into_iter()
                    .filter(|index| !output_bind_indexes.contains(index))
                    .collect();
            }
            TNS_MSG_TYPE_FLUSH_OUT_BINDS => break,
            TNS_MSG_TYPE_SERVER_SIDE_PIGGYBACK => {
                if let Some(update) = skip_server_side_piggyback(&mut reader)? {
                    result.sessionless_txn_state = Some(update);
                }
            }
            TNS_MSG_TYPE_IMPLICIT_RESULTSET => {
                // reference messages/base.pyx `_process_implicit_result`
                let num_results = reader.read_ub4()?;
                // `num_results` is read straight off the wire (a ub4, up to
                // ~4e9); each resultset consumes at least one byte, so cap the
                // reservation by the bytes left in the payload (BoundedReader).
                // Without this a hostile server forces a multi-gigabyte
                // allocation (OOM) before the truncated read in the loop body
                // fails closed.
                let mut resultsets: Vec<QueryValue> = reader.with_capacity_limited(
                    num_results as usize,
                    1,
                    ProtocolLimits::check_length_prefixed_elements,
                )?;
                for _ in 0..num_results {
                    let num_bytes = reader.read_u8()?;
                    reader.skip(usize::from(num_bytes))?;
                    let mut child = QueryResult::default();
                    parse_describe_info(&mut reader, capabilities, &mut child)?;
                    let child_cursor_id = u32::from(reader.read_ub2()?);
                    resultsets.push(QueryValue::Cursor(Box::new(CursorValue {
                        columns: child.columns,
                        cursor_id: child_cursor_id,
                    })));
                }
                result.implicit_resultsets = Some(resultsets);
            }
            TNS_MSG_TYPE_END_OF_RESPONSE => break,
            // pipeline responses open with the token of the operation they
            // answer (messages/base.pyx:288-293); callers compare it against
            // the expected token (mismatch -> DPY-2052 at the driver layer)
            TNS_MSG_TYPE_TOKEN => {
                result.token_num = Some(reader.read_ub8()?);
            }
            TNS_MSG_TYPE_ERROR => {
                let info = parse_server_error_info(&mut reader, capabilities.ttc_field_version)?;
                // The end-of-call ERROR message (number 0 on success) carries
                // the end-of-call status; sample the transaction-in-progress bit
                // (reference protocol.pyx `_process_call_status`).
                result.txn_in_progress =
                    Some(info.call_status & TNS_EOCS_FLAGS_TXN_IN_PROGRESS != 0);
                if info.cursor_id != 0 {
                    result.cursor_id = u32::from(info.cursor_id);
                }
                result.row_count = info.row_count;
                result.compilation_error_warning |= info.compilation_error_warning;
                result.last_rowid = info.rowid.clone();
                if info.number == TNS_ERR_NO_DATA_FOUND && !result.columns.is_empty() {
                    result.more_rows = false;
                } else if info.number == TNS_ERR_ARRAY_DML_ERRORS {
                    // executemany(batcherrors=True): errors are reported via
                    // the batch error arrays instead of raising ORA-24381
                    // (reference messages/base.pyx `_process_error_info`).
                    result.batch_errors = info.batch_errors;
                } else if info.number != 0 {
                    let mut details = info.into_details();
                    details.array_dml_row_counts = result.array_dml_row_counts.take();
                    return Err(ProtocolError::ServerErrorInfo(Box::new(details)));
                }
            }
            _ => {
                let position = reader.position().saturating_sub(1);
                if let Some(message) =
                    find_embedded_server_error(payload, capabilities.ttc_field_version, position)
                {
                    return Err(ProtocolError::ServerError(message));
                }
                return Err(ProtocolError::UnknownMessageType {
                    message_type,
                    position,
                });
            }
        }
    }
    Ok(result)
}

pub(crate) fn bind_column_metadata(value: &BindValue) -> ColumnMetadata {
    let (ora_type_num, csfrm, buffer_size) = bind_metadata(value);
    let object_schema = match value {
        BindValue::ObjectOutput { schema, .. } | BindValue::ObjectInput { schema, .. } => {
            Some(schema.clone())
        }
        _ => None,
    };
    let object_type_name = match value {
        BindValue::ObjectOutput { type_name, .. } | BindValue::ObjectInput { type_name, .. } => {
            Some(type_name.clone())
        }
        _ => None,
    };
    ColumnMetadata {
        name: String::new(),
        ora_type_num,
        csfrm,
        precision: 0,
        scale: 0,
        buffer_size,
        max_size: buffer_size,
        nulls_allowed: true,
        is_json: false,
        is_oson: false,
        object_schema,
        object_type_name,
        is_array: matches!(value, BindValue::Array { .. }),
        vector_dimensions: None,
        vector_format: 0,
        vector_flags: 0,
        domain_schema: None,
        domain_name: None,
        annotations: None,
    }
}

pub(crate) fn parse_io_vector(reader: &mut TtcReader<'_>, bind_count: usize) -> Result<Vec<usize>> {
    let _flags = reader.read_u8()?;
    let temp16 = reader.read_ub2()?;
    let temp32 = reader.read_ub4()?;
    let num_binds = usize::try_from(temp32)
        .map_err(|_| ProtocolError::InvalidPacketLength {
            length: usize::MAX,
            minimum: 0,
        })?
        .checked_mul(256)
        .and_then(|value| value.checked_add(usize::from(temp16)))
        .ok_or(ProtocolError::InvalidPacketLength {
            length: usize::MAX,
            minimum: 0,
        })?;
    let _num_iters_this_time = reader.read_ub4()?;
    let _uac_buffer_length = reader.read_ub2()?;
    let fast_fetch_len = reader.read_ub2()?;
    if fast_fetch_len > 0 {
        reader.skip(usize::from(fast_fetch_len))?;
    }
    let rowid_len = reader.read_ub2()?;
    if rowid_len > 0 {
        reader.skip(usize::from(rowid_len))?;
    }
    let mut out_indexes = Vec::new();
    reader.limits().check_binds(num_binds)?;
    for index in 0..num_binds {
        let direction = reader.read_u8()?;
        if index < bind_count && direction != TNS_BIND_DIR_INPUT {
            out_indexes.push(index);
        }
    }
    Ok(out_indexes)
}

pub(crate) fn find_embedded_server_error(
    payload: &[u8],
    ttc_field_version: u8,
    position: usize,
) -> Option<String> {
    let start = position.saturating_sub(64);
    for candidate in start..=position {
        if !matches!(payload.get(candidate).copied(), Some(TNS_MSG_TYPE_ERROR)) {
            continue;
        }
        let mut reader = TtcReader::new(payload.get(candidate + 1..)?);
        let info = parse_server_error_info(&mut reader, ttc_field_version).ok()?;
        if info.number != 0 && info.message.starts_with("ORA-") {
            return Some(info.message);
        }
    }
    None
}

pub(crate) fn parse_describe_info(
    reader: &mut TtcReader<'_>,
    capabilities: ClientCapabilities,
    result: &mut QueryResult,
) -> Result<()> {
    let _max_row_size = reader.read_ub4()?;
    let num_columns = reader.read_ub4()?;
    reader.limits().check_columns(num_columns as usize)?;
    result.columns.clear();
    if num_columns > 0 {
        reader.skip(1)?;
    }
    for _ in 0..num_columns {
        result
            .columns
            .push(parse_column_metadata(reader, capabilities)?);
    }
    let _current_date = reader.read_bytes_with_length()?;
    let _dcbflag = reader.read_ub4()?;
    let _dcbmdbz = reader.read_ub4()?;
    let _dcbmnpr = reader.read_ub4()?;
    let _dcbmxpr = reader.read_ub4()?;
    let _dcbqcky = reader.read_bytes_with_length()?;
    Ok(())
}

pub(crate) fn parse_column_metadata(
    reader: &mut TtcReader<'_>,
    capabilities: ClientCapabilities,
) -> Result<ColumnMetadata> {
    let ora_type_num = reader.read_u8()?;
    reader.skip(1)?;
    let precision = reader.read_i8()?;
    let scale = reader.read_i8()?;
    let buffer_size = reader.read_ub4()?;
    let _max_array_elements = reader.read_ub4()?;
    let _cont_flags = reader.read_ub8()?;
    let _oid = reader.read_bytes_with_length()?;
    let _version = reader.read_ub2()?;
    let _charset_id = reader.read_ub2()?;
    let csfrm = reader.read_u8()?;
    let mut max_size = reader.read_ub4()?;
    if ora_type_num == ORA_TYPE_NUM_RAW {
        max_size = buffer_size;
    }
    if version_gates::carries_oaccolid(capabilities.ttc_field_version) {
        let _oaccolid = reader.read_ub4()?;
    }
    let nulls_allowed = reader.read_u8()? != 0;
    reader.skip(1)?;
    let name = reader.read_string_with_length()?.unwrap_or_default();
    let object_schema = reader.read_string_with_length()?;
    let object_type_name = reader.read_string_with_length()?;
    let _column_position = reader.read_ub2()?;
    let uds_flags = reader.read_ub4()?;
    let mut domain_schema = None;
    let mut domain_name = None;
    let mut annotations: Option<Vec<(String, String)>> = None;
    if version_gates::reads_column_domain(capabilities.ttc_field_version) {
        domain_schema = reader.read_string_with_length()?;
        domain_name = reader.read_string_with_length()?;
    }
    if version_gates::reads_column_annotations(capabilities.ttc_field_version) {
        let num_annotations = reader.read_ub4()?;
        if num_annotations > 0 {
            reader.skip(1)?;
            let num_annotations = reader.read_ub4()?;
            reader.skip(1)?;
            // Bound by remaining bytes (BoundedReader): each annotation reads
            // at least a length-prefixed key/value, so a ub4 count larger than
            // the payload is a lie that must not pre-allocate gigabytes.
            let mut collected: Vec<(String, String)> = reader.with_capacity_limited(
                num_annotations as usize,
                1,
                ProtocolLimits::check_object_elements,
            )?;
            for _ in 0..num_annotations {
                let key = reader.read_string_with_length()?.unwrap_or_default();
                // A null annotation value is normalized to "" by the reference
                // driver (python-oracledb base.pyx _process_metadata).
                let value = reader.read_string_with_length()?.unwrap_or_default();
                let _flags = reader.read_ub4()?;
                collected.push((key, value));
            }
            let _flags = reader.read_ub4()?;
            annotations = Some(collected);
        }
    }
    let mut vector_dimensions = None;
    let mut vector_format = 0u8;
    let mut vector_flags = 0u8;
    if version_gates::reads_column_vector_metadata(capabilities.ttc_field_version) {
        // reference metadata.pyx: ub4 dimensions, ub1 format, ub1 flags
        let dims = reader.read_ub4()?;
        reader.limits().check_vector_dimensions(dims as usize)?;
        vector_format = reader.read_u8()?;
        vector_flags = reader.read_u8()?;
        if ora_type_num == ORA_TYPE_NUM_VECTOR {
            vector_dimensions = Some(dims);
        }
    }

    Ok(ColumnMetadata {
        name,
        ora_type_num,
        csfrm,
        precision,
        scale,
        buffer_size,
        max_size,
        nulls_allowed,
        is_json: uds_flags & TNS_UDS_FLAGS_IS_JSON != 0,
        is_oson: uds_flags & TNS_UDS_FLAGS_IS_OSON != 0,
        object_schema,
        object_type_name,
        is_array: false,
        vector_dimensions,
        vector_format,
        vector_flags,
        domain_schema,
        domain_name,
        annotations,
    })
}

pub(crate) fn parse_row_header(reader: &mut TtcReader<'_>) -> Result<Option<Vec<u8>>> {
    reader.skip(1)?;
    let _num_requests = reader.read_ub2()?;
    let _iteration_number = reader.read_ub4()?;
    let _num_iters = reader.read_ub4()?;
    let _buffer_length = reader.read_ub2()?;
    let num_bytes = reader.read_ub4()?;
    let bit_vector = if num_bytes > 0 {
        reader.skip(1)?;
        Some(reader.read_raw(num_bytes as usize)?.to_vec())
    } else {
        None
    };
    let _rxhrid = reader.read_bytes_with_length()?;
    Ok(bit_vector)
}

pub(crate) fn parse_bit_vector(reader: &mut TtcReader<'_>, num_columns: usize) -> Result<Vec<u8>> {
    let _num_columns_sent = reader.read_ub2()?;
    let num_bytes = num_columns.div_ceil(8);
    Ok(reader.read_raw(num_bytes)?.to_vec())
}

pub(crate) fn parse_row_data(
    reader: &mut TtcReader<'_>,
    result: &mut QueryResult,
    bit_vector: Option<&[u8]>,
    previous_row: Option<&[Option<QueryValue>]>,
    fetch_long_status: bool,
    lob_decode_mode: LobDecodeMode,
) -> Result<()> {
    let mut row = Vec::with_capacity(result.columns.len());
    for (index, metadata) in result.columns.iter().enumerate() {
        if is_duplicate_column(bit_vector, index) {
            let previous = result
                .rows
                .last()
                .map(Vec::as_slice)
                .or(previous_row)
                .and_then(|last| last.get(index))
                .cloned()
                .ok_or(ProtocolError::TtcDecode(
                    "duplicate row data without previous row",
                ))?;
            row.push(previous);
            continue;
        }
        row.push(parse_column_value_with_lob_mode(
            reader,
            metadata,
            lob_decode_mode,
        )?);
        if fetch_long_status
            && matches!(
                metadata.ora_type_num,
                ORA_TYPE_NUM_LONG | ORA_TYPE_NUM_LONG_RAW
            )
        {
            let _null_indicator = reader.read_sb4()?;
            let _return_code = reader.read_ub4()?;
        }
    }
    result.rows.push(row);
    Ok(())
}

pub(crate) fn parse_out_bind_row_data(
    reader: &mut TtcReader<'_>,
    result: &mut QueryResult,
    bind_columns: &[ColumnMetadata],
    out_bind_indexes: &[usize],
) -> Result<()> {
    for index in out_bind_indexes {
        let metadata = bind_columns.get(*index).ok_or(ProtocolError::TtcDecode(
            "out bind index without bind metadata",
        ))?;
        if metadata.is_array {
            let num_elements = usize::try_from(reader.read_ub4()?).map_err(|_| {
                ProtocolError::InvalidPacketLength {
                    length: usize::MAX,
                    minimum: 0,
                }
            })?;
            reader.limits().check_batch_rows(num_elements)?;
            // Cap by remaining bytes (BoundedReader): each element consumes
            // wire data, so a ub4 count cannot legitimately exceed the payload.
            let mut values: Vec<Option<QueryValue>> =
                reader.with_capacity_limited(num_elements, 1, ProtocolLimits::check_batch_rows)?;
            for _ in 0..num_elements {
                let value = parse_column_value(reader, metadata)?;
                let actual_num_bytes = reader.read_sb4()?;
                values.push(apply_out_bind_actual_num_bytes(
                    metadata,
                    value,
                    actual_num_bytes,
                    "truncated array OUT bind value",
                )?);
            }
            result
                .out_values
                .push((*index, Some(QueryValue::Array(values))));
            continue;
        }
        let value = parse_column_value(reader, metadata)?;
        let actual_num_bytes = reader.read_sb4()?;
        result.out_values.push((
            *index,
            apply_out_bind_actual_num_bytes(
                metadata,
                value,
                actual_num_bytes,
                "truncated OUT bind value",
            )?,
        ));
    }
    Ok(())
}

pub(crate) fn parse_returning_row_data(
    reader: &mut TtcReader<'_>,
    result: &mut QueryResult,
    bind_columns: &[ColumnMetadata],
    output_bind_indexes: &[usize],
) -> Result<()> {
    for index in output_bind_indexes {
        let metadata = bind_columns.get(*index).ok_or(ProtocolError::TtcDecode(
            "return bind index without bind metadata",
        ))?;
        let num_rows = usize::try_from(reader.read_ub4()?).map_err(|_| {
            ProtocolError::InvalidPacketLength {
                length: usize::MAX,
                minimum: 0,
            }
        })?;
        reader.limits().check_batch_rows(num_rows)?;
        // Cap by remaining bytes (BoundedReader); see the OOM note above.
        let mut values: Vec<Option<QueryValue>> =
            reader.with_capacity_limited(num_rows, 1, ProtocolLimits::check_batch_rows)?;
        for _ in 0..num_rows {
            let value = parse_column_value(reader, metadata)?;
            let actual_num_bytes = reader.read_sb4()?;
            values.push(apply_out_bind_actual_num_bytes(
                metadata,
                value,
                actual_num_bytes,
                "truncated DML RETURNING value",
            )?);
        }
        result.return_values.push((*index, values));
    }
    Ok(())
}

fn apply_out_bind_actual_num_bytes(
    metadata: &ColumnMetadata,
    value: Option<QueryValue>,
    actual_num_bytes: i32,
    truncation_error: &'static str,
) -> Result<Option<QueryValue>> {
    if actual_num_bytes < 0 && metadata.ora_type_num == ORA_TYPE_NUM_BOOLEAN {
        return Ok(None);
    }
    if actual_num_bytes != 0 && value.is_some() {
        return Err(ProtocolError::TtcDecode(truncation_error));
    }
    Ok(value)
}

pub(crate) fn is_duplicate_column(bit_vector: Option<&[u8]>, column_num: usize) -> bool {
    let Some(bit_vector) = bit_vector else {
        return false;
    };
    let byte_num = column_num / 8;
    let bit_num = column_num % 8;
    bit_vector
        .get(byte_num)
        .is_some_and(|byte| byte & (1 << bit_num) == 0)
}

pub(crate) fn parse_column_value(
    reader: &mut TtcReader<'_>,
    metadata: &ColumnMetadata,
) -> Result<Option<QueryValue>> {
    parse_column_value_with_lob_mode(reader, metadata, LobDecodeMode::DefineMetadata)
}

fn parse_column_value_with_lob_mode(
    reader: &mut TtcReader<'_>,
    metadata: &ColumnMetadata,
    lob_decode_mode: LobDecodeMode,
) -> Result<Option<QueryValue>> {
    if metadata.buffer_size == 0
        && !matches!(
            metadata.ora_type_num,
            ORA_TYPE_NUM_LONG | ORA_TYPE_NUM_LONG_RAW | ORA_TYPE_NUM_UROWID
        )
    {
        return Ok(None);
    }
    match metadata.ora_type_num {
        ORA_TYPE_NUM_VARCHAR | ORA_TYPE_NUM_CHAR | ORA_TYPE_NUM_LONG => {
            let Some(bytes) = reader.read_bytes()? else {
                return Ok(None);
            };
            match decode_text_value(&bytes, metadata.csfrm) {
                Ok(value) => Ok(Some(QueryValue::Text(value))),
                // preserve the raw bytes so the caller can honor the
                // configured encoding_errors policy (or raise a Python
                // UnicodeDecodeError as the reference does)
                Err(ProtocolError::TtcDecode(_)) => Ok(Some(QueryValue::TextRaw {
                    bytes,
                    csfrm: metadata.csfrm,
                })),
                Err(err) => Err(err),
            }
        }
        ORA_TYPE_NUM_RAW | ORA_TYPE_NUM_LONG_RAW => Ok(reader.read_bytes()?.map(QueryValue::Raw)),
        ORA_TYPE_NUM_ROWID => parse_rowid_value(reader).map(|value| value.map(QueryValue::Rowid)),
        ORA_TYPE_NUM_UROWID => parse_urowid_value(reader).map(|value| value.map(QueryValue::Rowid)),
        ORA_TYPE_NUM_NUMBER | ORA_TYPE_NUM_BINARY_INTEGER => {
            let Some(bytes) = reader.read_bytes()? else {
                return Ok(None);
            };
            decode_number_value(&bytes).map(Some)
        }
        ORA_TYPE_NUM_BINARY_DOUBLE => {
            let Some(bytes) = reader.read_bytes()? else {
                return Ok(None);
            };
            decode_binary_double(&bytes)
                .map(|value| Some(QueryValue::BinaryDouble(value.to_string())))
        }
        ORA_TYPE_NUM_BINARY_FLOAT => {
            let Some(bytes) = reader.read_bytes()? else {
                return Ok(None);
            };
            // f64-widened text matches Python float semantics for BINARY_FLOAT
            decode_binary_float(&bytes)
                .map(|value| Some(QueryValue::BinaryDouble(f64::from(value).to_string())))
        }
        ORA_TYPE_NUM_BOOLEAN => {
            let Some(bytes) = reader.read_bytes()? else {
                return Ok(None);
            };
            // reference read_bool: last byte == 1 means true; native
            // DB_TYPE_BOOLEAN surfaces as a Python bool.
            let is_true = matches!(bytes.last(), Some(&1));
            Ok(Some(QueryValue::Boolean(is_true)))
        }
        ORA_TYPE_NUM_INTERVAL_DS => {
            let Some(bytes) = reader.read_bytes()? else {
                return Ok(None);
            };
            decode_interval_ds(&bytes).map(Some)
        }
        ORA_TYPE_NUM_INTERVAL_YM => {
            let Some(bytes) = reader.read_bytes()? else {
                return Ok(None);
            };
            decode_interval_ym(&bytes).map(Some)
        }
        ORA_TYPE_NUM_DATE
        | ORA_TYPE_NUM_TIMESTAMP
        | ORA_TYPE_NUM_TIMESTAMP_LTZ
        | ORA_TYPE_NUM_TIMESTAMP_TZ => {
            let Some(bytes) = reader.read_bytes()? else {
                return Ok(None);
            };
            decode_datetime_value(&bytes).map(Some)
        }
        ORA_TYPE_NUM_CLOB | ORA_TYPE_NUM_BLOB | ORA_TYPE_NUM_BFILE => {
            parse_lob_value(reader, metadata, lob_decode_mode)
        }
        ORA_TYPE_NUM_VECTOR => parse_vector_value(reader),
        ORA_TYPE_NUM_JSON => parse_json_value(reader),
        ORA_TYPE_NUM_CURSOR => parse_cursor_value(reader).map(Some),
        ORA_TYPE_NUM_OBJECT => parse_object_value(reader, metadata),
        _ => Err(ProtocolError::UnsupportedFeature("query column type")),
    }
}

/// A column value decoded in pass 1 of the borrowed row decode. Scalar values
/// that borrow the wire buffer are held directly; values that need a small
/// owned arena (synthesized `Number` text, or a cold owned [`QueryValue`]) are
/// recorded as a deferred handle into the per-row arena and resolved in pass 2
/// once the arena is frozen. This two-pass split is what keeps the borrowed
/// path sound under `#![forbid(unsafe_code)]`: no `&str`/`&[u8]` is ever held
/// into an arena that is still being grown.
enum ColumnSlot<'buf> {
    /// SQL NULL.
    Null,
    /// A value that borrows the wire buffer (or is a small `Copy` value).
    Wire(QueryValueRef<'buf>),
    /// A `NUMBER` whose canonical text lives at `arena[range]` in the per-row
    /// number-text arena.
    Number {
        range: core::ops::Range<usize>,
        is_integer: bool,
    },
    /// A cold / non-borrowable value parked at `owned[index]` in the per-row
    /// owned arena.
    Owned(usize),
}

/// Decode one column into a [`ColumnSlot`], borrowing the wire buffer for the
/// hot scalar cases and appending to the per-row arenas for the deferred ones.
/// Mirrors [`parse_column_value`] type-for-type; the produced owned value (via
/// [`QueryValueRef::to_owned_value`]) is identical to the owned path.
///
/// `digits` is a caller-owned scratch buffer reused across all cells so the
/// per-cell `NUMBER` decode allocates nothing of its own (it writes straight
/// into `number_arena`). The hot scalar grid (Text/Raw) borrows the wire buffer
/// directly with zero allocation.
fn parse_column_slot<'buf>(
    reader: &mut TtcReader<'buf>,
    metadata: &ColumnMetadata,
    number_arena: &mut String,
    owned_arena: &mut Vec<QueryValue>,
    digits: &mut Vec<u8>,
    lob_decode_mode: LobDecodeMode,
) -> Result<ColumnSlot<'buf>> {
    // Park an owned QueryValue in the arena and return the deferred slot. Used
    // for the cold / non-borrowable variants so the hot grid stays borrowed.
    fn park(owned_arena: &mut Vec<QueryValue>, value: Option<QueryValue>) -> ColumnSlot<'static> {
        match value {
            None => ColumnSlot::Null,
            Some(value) => {
                owned_arena.push(value);
                ColumnSlot::Owned(owned_arena.len() - 1)
            }
        }
    }

    if metadata.buffer_size == 0
        && !matches!(
            metadata.ora_type_num,
            ORA_TYPE_NUM_LONG | ORA_TYPE_NUM_LONG_RAW | ORA_TYPE_NUM_UROWID
        )
    {
        return Ok(ColumnSlot::Null);
    }
    match metadata.ora_type_num {
        ORA_TYPE_NUM_VARCHAR | ORA_TYPE_NUM_CHAR | ORA_TYPE_NUM_LONG => {
            match reader.read_bytes_borrowed()? {
                BorrowedBytes::Null => Ok(ColumnSlot::Null),
                // Borrow the wire bytes directly when they are valid UTF-8 and
                // not the UTF-16 NCHAR form (which needs re-encoding). Zero copy.
                BorrowedBytes::Slice(slice) if metadata.csfrm != CS_FORM_NCHAR => {
                    match validate_utf8(slice) {
                        Ok(text) => Ok(ColumnSlot::Wire(QueryValueRef::Text(text))),
                        Err(_) => Ok(park(
                            owned_arena,
                            Some(QueryValue::TextRaw {
                                bytes: slice.to_vec(),
                                csfrm: metadata.csfrm,
                            }),
                        )),
                    }
                }
                // NCHAR (UTF-16) or chunked long text: fall back to the owned
                // decode, which re-encodes to UTF-8 / reassembles chunks.
                other => {
                    let bytes = other.into_vec();
                    let value = match decode_text_value(&bytes, metadata.csfrm) {
                        Ok(text) => QueryValue::Text(text),
                        Err(ProtocolError::TtcDecode(_)) => QueryValue::TextRaw {
                            bytes,
                            csfrm: metadata.csfrm,
                        },
                        Err(err) => return Err(err),
                    };
                    Ok(park(owned_arena, Some(value)))
                }
            }
        }
        ORA_TYPE_NUM_RAW | ORA_TYPE_NUM_LONG_RAW => match reader.read_bytes_borrowed()? {
            BorrowedBytes::Null => Ok(ColumnSlot::Null),
            BorrowedBytes::Slice(slice) => Ok(ColumnSlot::Wire(QueryValueRef::Raw(slice))),
            BorrowedBytes::Chunked(bytes) => Ok(park(owned_arena, Some(QueryValue::Raw(bytes)))),
        },
        ORA_TYPE_NUM_NUMBER | ORA_TYPE_NUM_BINARY_INTEGER => {
            // The wire NUMBER is binary; its canonical decimal text is
            // synthesized, so it cannot be borrowed from the buffer. We
            // synthesize it *directly* into the per-row number arena (reusing the
            // `digits` scratch), borrowing from the arena — zero per-cell heap
            // allocation for the common in-range NUMBER.
            with_small_bytes(reader, |bytes| match bytes {
                None => Ok(ColumnSlot::Null),
                Some(bytes) => {
                    let start = number_arena.len();
                    let is_integer = decode_number_text_into(bytes, digits, number_arena)?;
                    Ok(ColumnSlot::Number {
                        range: start..number_arena.len(),
                        is_integer,
                    })
                }
            })
        }
        ORA_TYPE_NUM_BOOLEAN => with_small_bytes(reader, |bytes| match bytes {
            None => Ok(ColumnSlot::Null),
            Some(bytes) => Ok(ColumnSlot::Wire(QueryValueRef::Boolean(matches!(
                bytes.last(),
                Some(&1)
            )))),
        }),
        ORA_TYPE_NUM_INTERVAL_DS => with_small_bytes(reader, |bytes| match bytes {
            None => Ok(ColumnSlot::Null),
            Some(bytes) => match decode_interval_ds(bytes)? {
                QueryValue::IntervalDS {
                    days,
                    hours,
                    minutes,
                    seconds,
                    fseconds,
                } => Ok(ColumnSlot::Wire(QueryValueRef::IntervalDS {
                    days,
                    hours,
                    minutes,
                    seconds,
                    fseconds,
                })),
                other => Ok(park(owned_arena, Some(other))),
            },
        }),
        ORA_TYPE_NUM_INTERVAL_YM => with_small_bytes(reader, |bytes| match bytes {
            None => Ok(ColumnSlot::Null),
            Some(bytes) => match decode_interval_ym(bytes)? {
                QueryValue::IntervalYM { years, months } => {
                    Ok(ColumnSlot::Wire(QueryValueRef::IntervalYM {
                        years,
                        months,
                    }))
                }
                other => Ok(park(owned_arena, Some(other))),
            },
        }),
        ORA_TYPE_NUM_DATE
        | ORA_TYPE_NUM_TIMESTAMP
        | ORA_TYPE_NUM_TIMESTAMP_LTZ
        | ORA_TYPE_NUM_TIMESTAMP_TZ => with_small_bytes(reader, |bytes| match bytes {
            None => Ok(ColumnSlot::Null),
            Some(bytes) => match decode_datetime_value(bytes)? {
                QueryValue::DateTime {
                    year,
                    month,
                    day,
                    hour,
                    minute,
                    second,
                    nanosecond,
                } => Ok(ColumnSlot::Wire(QueryValueRef::DateTime {
                    year,
                    month,
                    day,
                    hour,
                    minute,
                    second,
                    nanosecond,
                })),
                QueryValue::TimestampTz {
                    year,
                    month,
                    day,
                    hour,
                    minute,
                    second,
                    nanosecond,
                    offset_minutes,
                } => Ok(ColumnSlot::Wire(QueryValueRef::TimestampTz {
                    year,
                    month,
                    day,
                    hour,
                    minute,
                    second,
                    nanosecond,
                    offset_minutes,
                })),
                other => Ok(park(owned_arena, Some(other))),
            },
        }),
        // Everything else (Rowid, BinaryDouble/Float, Clob/Blob/Bfile, Vector,
        // Json, Cursor, Object, UROWID) goes through the owned decode and is
        // parked in the owned arena. These are the cold / non-borrowable cases.
        _ => {
            let value = parse_column_value_with_lob_mode(reader, metadata, lob_decode_mode)?;
            Ok(park(owned_arena, value))
        }
    }
}

/// Read one TTC byte field and hand the body to `f` as a borrowed `&[u8]`
/// without allocating in the common contiguous case. `None` is SQL NULL. The
/// rare chunked long form is reassembled into a temporary `Vec` (these small
/// fixed-size scalar types — number/boolean/interval/datetime — are never sent
/// chunked in practice, so this fallback is effectively dead weight).
fn with_small_bytes<'buf, T>(
    reader: &mut TtcReader<'buf>,
    f: impl FnOnce(Option<&[u8]>) -> Result<T>,
) -> Result<T> {
    match reader.read_bytes_borrowed()? {
        BorrowedBytes::Null => f(None),
        BorrowedBytes::Slice(slice) => f(Some(slice)),
        BorrowedBytes::Chunked(owned) => f(Some(&owned)),
    }
}

impl BorrowedBytes<'_> {
    /// Reassemble into an owned `Vec` (zero-copy `Slice` still copies; `Chunked`
    /// reuses its already-owned `Vec`). Used by the non-borrowable text fallback.
    fn into_vec(self) -> Vec<u8> {
        match self {
            BorrowedBytes::Null => Vec::new(),
            BorrowedBytes::Slice(slice) => slice.to_vec(),
            BorrowedBytes::Chunked(owned) => owned,
        }
    }
}

/// A decoded fetch batch that **owns** the wire response buffer and column
/// metadata, and yields rows of borrowed [`QueryValueRef`] that point straight
/// into that buffer. This is the zero-copy fetch fast path: the common scalar
/// grid is decoded with no per-cell allocation.
///
/// ## Soundness
///
/// The buffer is owned by the batch and outlives every borrowed row: rows are
/// only ever surfaced *inside* the [`for_each_row_ref`](Self::for_each_row_ref)
/// callback, whose `&[QueryValueRef]` argument cannot escape (its lifetime is
/// bound to the call). The borrow checker therefore guarantees no
/// `QueryValueRef` can dangle — there is no self-referential struct and no
/// `unsafe`. `Number` text and the cold values borrow per-row arenas that are
/// fully built (pass 1) before any reference into them is taken (pass 2), so an
/// arena is never grown while borrowed.
#[derive(Clone, Debug)]
pub struct BorrowedRowBatch {
    buffer: Vec<u8>,
    columns: Vec<ColumnMetadata>,
    /// Byte offset into `buffer` where each row's column values begin.
    row_starts: Vec<usize>,
    /// Per-row duplicate-column bit vector (server row-compression). `None` (or
    /// an absent entry) means every column is present on the wire for that row.
    /// A zero bit marks a duplicate column whose value repeats the previous
    /// row's value and carries no wire bytes (reference `bit_vector`).
    row_bit_vectors: Vec<Option<Vec<u8>>>,
    /// Whether this batch carried `LONG`/`LONG RAW` status trailers after each
    /// such column (the fetch path sets this; the plain execute path does not).
    fetch_long_status: bool,
    lob_decode_mode: LobDecodeMode,
    /// The caller's previous (owned) row, used to resolve duplicate columns in
    /// the *first* compressed row of the batch (whose duplicates repeat the row
    /// that ended the prior page). `None` outside the compressed-fetch case.
    previous_row_seed: Option<Vec<Option<QueryValue>>>,
}

impl BorrowedRowBatch {
    /// Construct a batch from an owned wire `buffer`, the `columns` describing
    /// each cell, and the per-row start offsets into `buffer`. Use this for
    /// batches with no duplicate-column compression and no LONG trailers (the
    /// common synthetic / test case); the framing-aware
    /// [`parse_query_response_borrowed`] builds the full form.
    pub fn new(buffer: Vec<u8>, columns: Vec<ColumnMetadata>, row_starts: Vec<usize>) -> Self {
        Self {
            buffer,
            columns,
            row_starts,
            row_bit_vectors: Vec::new(),
            fetch_long_status: false,
            lob_decode_mode: LobDecodeMode::PlainLocator,
            previous_row_seed: None,
        }
    }

    /// Number of rows in the batch.
    pub fn row_count(&self) -> usize {
        self.row_starts.len()
    }

    /// The columns describing each cell.
    pub fn columns(&self) -> &[ColumnMetadata] {
        &self.columns
    }

    /// The address range of the owned buffer, for tests asserting that borrowed
    /// scalar cells truly point into it (zero-copy).
    #[cfg(test)]
    pub fn buffer_ptr_range(&self) -> core::ops::Range<usize> {
        let start = self.buffer.as_ptr() as usize;
        start..start + self.buffer.len()
    }

    /// Decode each row and invoke `callback` with the row's borrowed cells. The
    /// `&[Option<QueryValueRef>]` slice borrows the batch buffer and per-row
    /// arenas; it is valid only for the duration of the call (it cannot escape).
    /// `None` cells are SQL NULL. Returns the first decode/callback error.
    ///
    /// Generic over the callback's error type `E` (any error a decode failure
    /// can convert into, e.g. the driver crate's own error) so callers are not
    /// forced through [`ProtocolError`]; a decode failure is surfaced via
    /// `E: From<ProtocolError>`.
    pub fn for_each_row_ref<F, E>(&self, mut callback: F) -> std::result::Result<(), E>
    where
        F: FnMut(&[Option<QueryValueRef<'_>>]) -> std::result::Result<(), E>,
        E: From<ProtocolError>,
    {
        // The two arenas are reused across rows (cleared, not reallocated). They
        // are mutated only in pass 1; pass 2 borrows them immutably and that
        // borrow is confined to a single loop iteration (it ends before the next
        // iteration's `clear()`), which is what keeps the borrow checker — and
        // soundness — happy.
        let mut number_arena = String::new();
        let mut owned_arena: Vec<QueryValue> = Vec::new();
        // Reusable scratch: `digits` for the NUMBER decode, `slots` for pass 1,
        // `row` for the borrowed cells handed to the callback. All cleared and
        // reused across rows so the steady-state per-row decode allocates only
        // when an arena/scratch genuinely grows (amortized). `slots`/`row`/
        // `digits` borrow nothing across iterations beyond the stable buffer.
        let mut digits: Vec<u8> = Vec::new();
        let mut slots: Vec<Option<ColumnSlot<'_>>> = Vec::with_capacity(self.columns.len());
        // Owned snapshot of the previous row, used only to resolve duplicate
        // (bit-vector-compressed) columns, which carry no wire bytes. Empty when
        // the batch has no bit vectors (the common case), so it costs nothing.
        // Seeded from the caller's prior-page row for the first compressed row.
        let mut previous_owned: Vec<Option<QueryValue>> =
            self.previous_row_seed.clone().unwrap_or_default();
        let uses_bit_vectors = !self.row_bit_vectors.is_empty();

        for (row_index, &start) in self.row_starts.iter().enumerate() {
            number_arena.clear();
            owned_arena.clear();
            slots.clear();
            let bit_vector = self
                .row_bit_vectors
                .get(row_index)
                .and_then(|bv| bv.as_deref());

            // Pass 1: decode all columns, growing the per-row arenas. `slots`
            // borrows only the buffer (the deferred Number/Owned slots hold a
            // range/index into the arenas, never a borrow of them). Duplicate
            // columns carry no wire bytes — their owned previous value is parked
            // in the owned arena.
            let mut reader = TtcReader::new(&self.buffer[start..]);
            for (index, metadata) in self.columns.iter().enumerate() {
                if is_duplicate_column(bit_vector, index) {
                    let previous = previous_owned.get(index).and_then(Option::as_ref);
                    match previous {
                        None => slots.push(None),
                        Some(value) => {
                            owned_arena.push(value.clone());
                            slots.push(Some(ColumnSlot::Owned(owned_arena.len() - 1)));
                        }
                    }
                    continue;
                }
                let slot = parse_column_slot(
                    &mut reader,
                    metadata,
                    &mut number_arena,
                    &mut owned_arena,
                    &mut digits,
                    self.lob_decode_mode,
                )?;
                slots.push(match slot {
                    ColumnSlot::Null => None,
                    other => Some(other),
                });
                if self.fetch_long_status
                    && matches!(
                        metadata.ora_type_num,
                        ORA_TYPE_NUM_LONG | ORA_TYPE_NUM_LONG_RAW
                    )
                {
                    let _null_indicator = reader.read_sb4()?;
                    let _return_code = reader.read_ub4()?;
                }
            }

            // Pass 2: arenas are now frozen — resolve deferred slots into
            // borrowed refs. No arena is mutated here, so the borrows are sound.
            // `row` is allocated per row: it carries the per-row arena lifetime,
            // which (unlike `slots`/`digits`, that borrow only the stable buffer)
            // cannot be reused across an arena `clear()`. This is the single
            // remaining per-row allocation, versus the owned path's per-row Vec
            // *plus* a String per scalar cell.
            let row: Vec<Option<QueryValueRef<'_>>> = slots
                .iter()
                .map(|slot| {
                    slot.as_ref().map(|slot| match *slot {
                        ColumnSlot::Null => unreachable!("Null slots are stored as None"),
                        ColumnSlot::Wire(value) => value,
                        ColumnSlot::Number {
                            ref range,
                            is_integer,
                        } => QueryValueRef::Number {
                            text: &number_arena[range.clone()],
                            is_integer,
                        },
                        ColumnSlot::Owned(index) => QueryValueRef::Owned(&owned_arena[index]),
                    })
                })
                .collect();

            callback(&row)?;

            // Snapshot the just-emitted row as owned values for the next row's
            // duplicate-column resolution — but only when the batch actually uses
            // bit-vector compression, so the zero-copy common path pays nothing.
            if uses_bit_vectors {
                previous_owned.clear();
                previous_owned.extend(row.iter().map(|cell| cell.map(|v| v.to_owned_value())));
            }
        }
        Ok(())
    }
}

/// The borrowed counterpart of a fetched [`QueryResult`]: a [`BorrowedRowBatch`]
/// of zero-copy rows plus the response-level fields a caller needs to page and
/// finalize the cursor. Produced by [`parse_query_response_borrowed`].
#[derive(Clone, Debug)]
pub struct BorrowedFetchResult {
    /// The decoded rows, borrowing the response buffer.
    pub batch: BorrowedRowBatch,
    /// Whether the server reports more rows for this cursor.
    pub more_rows: bool,
    /// Server cursor id (for paging / release).
    pub cursor_id: u32,
    /// Total affected/processed row count from the end-of-call error message.
    pub row_count: u64,
}

/// Walk a fetch/query response payload and produce a [`BorrowedFetchResult`]
/// whose rows borrow `payload` (the caller must keep the owned buffer alive —
/// [`BorrowedRowBatch`] owns it). This is the zero-copy companion to
/// [`parse_fetch_response_with_context`]: it walks the exact same message
/// framing (DESCRIBE_INFO / ROW_HEADER / BIT_VECTOR / ROW_DATA / ERROR /
/// END_OF_RESPONSE) but, instead of materializing owned rows, records each
/// row's byte offset and bit vector so [`BorrowedRowBatch::for_each_row_ref`]
/// can decode them lazily and without per-cell allocation.
///
/// Scope: the plain query-row case (the fetch path). Out-bind / DML-returning
/// rows are not part of a fetch response and are left to the owned path.
pub fn parse_query_response_borrowed(
    payload: &[u8],
    capabilities: ClientCapabilities,
    columns: &[ColumnMetadata],
    previous_row: Option<&[Option<QueryValue>]>,
) -> Result<BorrowedFetchResult> {
    parse_query_response_borrowed_with_limits(
        payload,
        capabilities,
        columns,
        previous_row,
        ProtocolLimits::DEFAULT,
    )
}

pub fn parse_query_response_borrowed_with_limits(
    payload: &[u8],
    capabilities: ClientCapabilities,
    columns: &[ColumnMetadata],
    previous_row: Option<&[Option<QueryValue>]>,
    limits: ProtocolLimits,
) -> Result<BorrowedFetchResult> {
    parse_query_response_borrowed_with_lob_mode_and_limits(
        payload,
        capabilities,
        columns,
        previous_row,
        LobDecodeMode::PlainLocator,
        limits,
    )
}

pub fn parse_define_fetch_response_borrowed_with_limits(
    payload: &[u8],
    capabilities: ClientCapabilities,
    columns: &[ColumnMetadata],
    previous_row: Option<&[Option<QueryValue>]>,
    limits: ProtocolLimits,
) -> Result<BorrowedFetchResult> {
    parse_query_response_borrowed_with_lob_mode_and_limits(
        payload,
        capabilities,
        columns,
        previous_row,
        LobDecodeMode::DefineMetadata,
        limits,
    )
}

fn parse_query_response_borrowed_with_lob_mode_and_limits(
    payload: &[u8],
    capabilities: ClientCapabilities,
    columns: &[ColumnMetadata],
    previous_row: Option<&[Option<QueryValue>]>,
    lob_decode_mode: LobDecodeMode,
    limits: ProtocolLimits,
) -> Result<BorrowedFetchResult> {
    let mut reader = TtcReader::with_limits(payload, limits)?;
    reader.limits().check_columns(columns.len())?;
    let mut result_columns = columns.to_vec();
    let mut more_rows = true;
    let mut cursor_id = 0u32;
    let mut row_count = 0u64;
    let mut row_starts: Vec<usize> = Vec::new();
    let mut row_bit_vectors: Vec<Option<Vec<u8>>> = Vec::new();
    let mut any_bit_vector = false;
    let mut pending_bit_vector: Option<Vec<u8>> = None;
    // The fetch path always consumes LONG/LONG RAW status trailers.
    let fetch_long_status = true;

    while reader.remaining() > 0 {
        let message_type = reader.read_u8()?;
        match message_type {
            0 => {}
            TNS_MSG_TYPE_DESCRIBE_INFO => {
                let _describe_name = reader.read_bytes()?;
                let previous = std::mem::take(&mut result_columns);
                let mut described = QueryResult::default();
                parse_describe_info(&mut reader, capabilities, &mut described)?;
                result_columns = described.columns;
                for (index, column) in result_columns.iter_mut().enumerate() {
                    if let Some(prev) = previous.get(index) {
                        adjust_refetch_metadata(prev, column);
                    }
                }
            }
            TNS_MSG_TYPE_ROW_HEADER => {
                pending_bit_vector = parse_row_header(&mut reader)?;
            }
            TNS_MSG_TYPE_BIT_VECTOR => {
                pending_bit_vector = Some(parse_bit_vector(&mut reader, result_columns.len())?);
            }
            TNS_MSG_TYPE_ROW_DATA => {
                // Record where this row's column values begin, then advance the
                // reader past the row (skipping, not materializing).
                reader.limits().check_batch_rows(row_starts.len() + 1)?;
                row_starts.push(reader.position());
                let bit_vector = pending_bit_vector.take();
                any_bit_vector |= bit_vector.is_some();
                row_bit_vectors.push(bit_vector.clone());
                skip_row_data(
                    &mut reader,
                    &result_columns,
                    bit_vector.as_deref(),
                    fetch_long_status,
                    lob_decode_mode,
                )?;
            }
            TNS_MSG_TYPE_PARAMETER => {
                let _params = parse_query_return_parameters(&mut reader, false)?;
            }
            TNS_MSG_TYPE_STATUS => {
                let _call_status = reader.read_ub4()?;
                let _seq = reader.read_ub2()?;
            }
            TNS_MSG_TYPE_SERVER_SIDE_PIGGYBACK => {
                let _ = skip_server_side_piggyback(&mut reader)?;
            }
            TNS_MSG_TYPE_FLUSH_OUT_BINDS | TNS_MSG_TYPE_END_OF_RESPONSE => break,
            TNS_MSG_TYPE_TOKEN => {
                let _token = reader.read_ub8()?;
            }
            TNS_MSG_TYPE_IMPLICIT_RESULTSET => {
                // Mirror the owned parser's framing walk so the reader advances
                // past the implicit-resultset block identically (the borrowed
                // fetch API does not surface child cursors, but it must still
                // consume the bytes). reference messages/base.pyx
                // `_process_implicit_result`.
                let num_results = reader.read_ub4()?;
                reader
                    .limits()
                    .check_length_prefixed_elements(num_results as usize)?;
                for _ in 0..num_results {
                    let num_bytes = reader.read_u8()?;
                    reader.skip(usize::from(num_bytes))?;
                    let mut child = QueryResult::default();
                    parse_describe_info(&mut reader, capabilities, &mut child)?;
                    let _child_cursor_id = reader.read_ub2()?;
                }
            }
            TNS_MSG_TYPE_ERROR => {
                let info = parse_server_error_info(&mut reader, capabilities.ttc_field_version)?;
                if info.cursor_id != 0 {
                    cursor_id = u32::from(info.cursor_id);
                }
                row_count = info.row_count;
                if info.number == TNS_ERR_NO_DATA_FOUND && !result_columns.is_empty() {
                    more_rows = false;
                } else if info.number != 0 && info.number != TNS_ERR_ARRAY_DML_ERRORS {
                    return Err(ProtocolError::ServerErrorInfo(Box::new(
                        info.into_details(),
                    )));
                }
            }
            _ => {
                let position = reader.position().saturating_sub(1);
                if let Some(message) =
                    find_embedded_server_error(payload, capabilities.ttc_field_version, position)
                {
                    return Err(ProtocolError::ServerError(message));
                }
                return Err(ProtocolError::UnknownMessageType {
                    message_type,
                    position,
                });
            }
        }
    }

    // If the batch never used duplicate-column compression, drop the per-row
    // bit-vector vector so iteration takes the zero-copy fast path (no owned
    // previous-row snapshotting).
    if !any_bit_vector {
        row_bit_vectors.clear();
    }

    let batch = BorrowedRowBatch {
        buffer: payload.to_vec(),
        columns: result_columns,
        row_starts,
        row_bit_vectors,
        fetch_long_status,
        lob_decode_mode,
        // Seed the first compressed row's duplicate resolution from the caller's
        // prior-page row (only consulted when the batch uses bit vectors).
        previous_row_seed: any_bit_vector.then(|| {
            previous_row
                .map(<[Option<QueryValue>]>::to_vec)
                .unwrap_or_default()
        }),
    };

    Ok(BorrowedFetchResult {
        batch,
        more_rows,
        cursor_id,
        row_count,
    })
}

/// Advance `reader` past one ROW_DATA row **without materializing owned values**
/// — this is the offset-capture pass, so it must allocate nothing for the hot
/// scalar grid. Mirrors [`parse_row_data`]'s consumption exactly: duplicate
/// (bit-vector) columns carry no wire bytes and are skipped; the hot byte-field
/// scalar types are skipped with a zero-allocation length-prefixed skip; the
/// rare cold types (LOB / Vector / JSON / Cursor / Object / ROWID), whose wire
/// framing is non-trivial, fall back to [`parse_column_value`] (which may
/// allocate, but those are uncommon). `LONG`/`LONG RAW` status trailers are
/// consumed when `fetch_long_status`.
fn skip_row_data(
    reader: &mut TtcReader<'_>,
    columns: &[ColumnMetadata],
    bit_vector: Option<&[u8]>,
    fetch_long_status: bool,
    lob_decode_mode: LobDecodeMode,
) -> Result<()> {
    for (index, metadata) in columns.iter().enumerate() {
        if is_duplicate_column(bit_vector, index) {
            continue;
        }
        let consumed_byte_field = metadata.buffer_size != 0
            && matches!(
                metadata.ora_type_num,
                ORA_TYPE_NUM_VARCHAR
                    | ORA_TYPE_NUM_CHAR
                    | ORA_TYPE_NUM_LONG
                    | ORA_TYPE_NUM_RAW
                    | ORA_TYPE_NUM_LONG_RAW
                    | ORA_TYPE_NUM_NUMBER
                    | ORA_TYPE_NUM_BINARY_INTEGER
                    | ORA_TYPE_NUM_BINARY_DOUBLE
                    | ORA_TYPE_NUM_BINARY_FLOAT
                    | ORA_TYPE_NUM_BOOLEAN
                    | ORA_TYPE_NUM_INTERVAL_DS
                    | ORA_TYPE_NUM_INTERVAL_YM
                    | ORA_TYPE_NUM_DATE
                    | ORA_TYPE_NUM_TIMESTAMP
                    | ORA_TYPE_NUM_TIMESTAMP_LTZ
                    | ORA_TYPE_NUM_TIMESTAMP_TZ
            );
        if consumed_byte_field {
            reader.skip_bytes_field()?;
        } else {
            // Cold / non-byte-field type, or a zero-buffer-size column: defer to
            // the full owned decode purely to advance the reader correctly.
            let _ = parse_column_value_with_lob_mode(reader, metadata, lob_decode_mode)?;
        }
        if fetch_long_status
            && matches!(
                metadata.ora_type_num,
                ORA_TYPE_NUM_LONG | ORA_TYPE_NUM_LONG_RAW
            )
        {
            let _null_indicator = reader.read_sb4()?;
            let _return_code = reader.read_ub4()?;
        }
    }
    Ok(())
}

pub(crate) fn encode_rowid_component(mut value: u32, size: usize, output: &mut String) {
    let mut encoded = vec![b'A'; size];
    for index in 0..size {
        let alphabet_index = usize::try_from(value & 0x3f).unwrap_or(0);
        encoded[size - index - 1] = TNS_BASE64_ALPHABET[alphabet_index];
        value >>= 6;
    }
    output.extend(encoded.into_iter().map(char::from));
}

pub(crate) fn encode_physical_rowid(
    rba: u32,
    partition_id: u16,
    block_num: u32,
    slot_num: u16,
) -> String {
    let mut output = String::with_capacity(ORA_TYPE_SIZE_ROWID as usize);
    encode_rowid_component(rba, 6, &mut output);
    encode_rowid_component(u32::from(partition_id), 3, &mut output);
    encode_rowid_component(block_num, 6, &mut output);
    encode_rowid_component(u32::from(slot_num), 3, &mut output);
    output
}

pub(crate) fn parse_rowid_value(reader: &mut TtcReader<'_>) -> Result<Option<String>> {
    let len = reader.read_u8()?;
    if len == 0 || len == crate::wire::TNS_NULL_LENGTH_INDICATOR {
        return Ok(None);
    }
    let rba = reader.read_ub4()?;
    let partition_id = reader.read_ub2()?;
    reader.skip(1)?;
    let block_num = reader.read_ub4()?;
    let slot_num = reader.read_ub2()?;
    Ok(Some(encode_physical_rowid(
        rba,
        partition_id,
        block_num,
        slot_num,
    )))
}

pub(crate) fn encode_logical_urowid(bytes: &[u8]) -> String {
    let mut input_offset = 1;
    let mut input_len = bytes.len().saturating_sub(1);
    let mut output = String::with_capacity((bytes.len() / 3) * 4 + 4);
    output.push('*');
    while input_len > 0 {
        let mut pos = bytes[input_offset] >> 2;
        output.push(char::from(TNS_BASE64_ALPHABET[usize::from(pos)]));

        pos = (bytes[input_offset] & 0x03) << 4;
        if input_len == 1 {
            output.push(char::from(TNS_BASE64_ALPHABET[usize::from(pos)]));
            break;
        }
        input_offset += 1;
        pos |= (bytes[input_offset] & 0xf0) >> 4;
        output.push(char::from(TNS_BASE64_ALPHABET[usize::from(pos)]));

        pos = (bytes[input_offset] & 0x0f) << 2;
        if input_len == 2 {
            output.push(char::from(TNS_BASE64_ALPHABET[usize::from(pos)]));
            break;
        }
        input_offset += 1;
        pos |= (bytes[input_offset] & 0xc0) >> 6;
        output.push(char::from(TNS_BASE64_ALPHABET[usize::from(pos)]));

        pos = bytes[input_offset] & 0x3f;
        output.push(char::from(TNS_BASE64_ALPHABET[usize::from(pos)]));
        input_offset += 1;
        input_len -= 3;
    }
    output
}

pub(crate) fn parse_urowid_value(reader: &mut TtcReader<'_>) -> Result<Option<String>> {
    if reader.read_bytes()?.is_none() {
        return Ok(None);
    }
    let Some(bytes) = reader.read_bytes()? else {
        return Ok(None);
    };
    if bytes.len() < 13 {
        return Err(ProtocolError::TtcDecode("encoded UROWID too short"));
    }
    if bytes[0] == 1 {
        let rba = u32::from_be_bytes([bytes[1], bytes[2], bytes[3], bytes[4]]);
        let partition_id = u16::from_be_bytes([bytes[5], bytes[6]]);
        let block_num = u32::from_be_bytes([bytes[7], bytes[8], bytes[9], bytes[10]]);
        let slot_num = u16::from_be_bytes([bytes[11], bytes[12]]);
        Ok(Some(encode_physical_rowid(
            rba,
            partition_id,
            block_num,
            slot_num,
        )))
    } else {
        Ok(Some(encode_logical_urowid(&bytes)))
    }
}

pub(crate) fn parse_lob_value(
    reader: &mut TtcReader<'_>,
    metadata: &ColumnMetadata,
    lob_decode_mode: LobDecodeMode,
) -> Result<Option<QueryValue>> {
    let num_bytes = reader.read_ub4()?;
    reader.limits().check_response_bytes(num_bytes as usize)?;
    if num_bytes == 0 {
        return Ok(None);
    }
    let (size, chunk_size) = if matches!(
        (lob_decode_mode, metadata.ora_type_num),
        (_, ORA_TYPE_NUM_BFILE) | (LobDecodeMode::PlainLocator, _)
    ) {
        (0, 0)
    } else {
        (reader.read_ub8()?, reader.read_ub4()?)
    };
    let Some(locator) = reader.read_bytes()? else {
        return Ok(None);
    };
    Ok(Some(QueryValue::Lob(Box::new(LobValue {
        ora_type_num: metadata.ora_type_num,
        csfrm: metadata.csfrm,
        locator,
        size,
        chunk_size,
    }))))
}

#[cfg(test)]
mod lob_fetch_shape_tests {
    use super::*;

    fn clob_column() -> ColumnMetadata {
        ColumnMetadata {
            name: "BODY".into(),
            ora_type_num: ORA_TYPE_NUM_CLOB,
            csfrm: CS_FORM_IMPLICIT,
            precision: 0,
            scale: 0,
            buffer_size: 4000,
            max_size: 4000,
            nulls_allowed: true,
            is_json: false,
            is_oson: false,
            object_schema: None,
            object_type_name: None,
            is_array: false,
            vector_dimensions: None,
            vector_format: 0,
            vector_flags: 0,
            domain_schema: None,
            domain_name: None,
            annotations: None,
        }
    }

    fn blob_column() -> ColumnMetadata {
        ColumnMetadata {
            name: "IMAGE".into(),
            ora_type_num: ORA_TYPE_NUM_BLOB,
            csfrm: 0,
            precision: 0,
            scale: 0,
            buffer_size: 4000,
            max_size: 4000,
            nulls_allowed: true,
            is_json: false,
            is_oson: false,
            object_schema: None,
            object_type_name: None,
            is_array: false,
            vector_dimensions: None,
            vector_format: 0,
            vector_flags: 0,
            domain_schema: None,
            domain_name: None,
            annotations: None,
        }
    }

    fn lob_row_payload(locator: &[u8], metadata: Option<(u64, u32)>) -> Vec<u8> {
        let mut writer = TtcWriter::new();
        writer.write_u8(TNS_MSG_TYPE_ROW_DATA);
        writer.write_ub4(u32::try_from(locator.len()).expect("locator length fits ub4"));
        if let Some((size, chunk_size)) = metadata {
            writer.write_ub8(size);
            writer.write_ub4(chunk_size);
        }
        writer
            .write_bytes_with_length(locator)
            .expect("synthetic locator length is encodable");
        writer.write_u8(TNS_MSG_TYPE_END_OF_RESPONSE);
        writer.into_bytes()
    }

    fn write_define_lob_cell(writer: &mut TtcWriter, locator: &[u8], size: u64, chunk_size: u32) {
        writer.write_ub4(u32::try_from(locator.len()).expect("locator length fits ub4"));
        writer.write_ub8(size);
        writer.write_ub4(chunk_size);
        writer
            .write_bytes_with_length(locator)
            .expect("synthetic locator length is encodable");
    }

    fn first_lob(result: &QueryResult) -> Option<&LobValue> {
        match result.rows.first()?.first()? {
            Some(QueryValue::Lob(lob)) => Some(lob.as_ref()),
            _ => None,
        }
    }

    #[test]
    fn column_metadata_discards_server_charset_id_and_keeps_csfrm_only() {
        let caps = ClientCapabilities {
            ttc_field_version: 0,
            max_string_size: 32_767,
            charset_id: 873,
        };
        let mut writer = TtcWriter::new();
        writer.write_u8(ORA_TYPE_NUM_VARCHAR);
        writer.write_u8(0); // flags
        writer.write_u8(0); // precision
        writer.write_u8(0); // scale
        writer.write_ub4(4000);
        writer.write_ub4(0); // max array elements
        writer.write_ub8(0); // cont flags
        writer
            .write_bytes_with_two_lengths(None)
            .expect("empty oid");
        writer.write_ub2(0); // version
        writer.write_ub2(0); // unusable server charset id: must not drive decoding
        writer.write_u8(CS_FORM_IMPLICIT);
        writer.write_ub4(4000);
        writer.write_u8(1); // nullable
        writer.write_u8(0); // flags
        writer
            .write_bytes_with_two_lengths(Some(b"TXT"))
            .expect("name");
        writer
            .write_bytes_with_two_lengths(None)
            .expect("object schema");
        writer
            .write_bytes_with_two_lengths(None)
            .expect("object type");
        writer.write_ub2(1); // column position
        writer.write_ub4(0); // uds flags

        let bytes = writer.into_bytes();
        let mut reader = TtcReader::new(&bytes);
        let metadata = parse_column_metadata(&mut reader, caps).expect("metadata should parse");
        assert_eq!(metadata.name(), "TXT");
        assert_eq!(metadata.csfrm(), CS_FORM_IMPLICIT);
        assert_eq!(
            decode_text_value("ok".as_bytes(), metadata.csfrm()).expect("decode text"),
            "ok"
        );
    }

    #[test]
    fn plain_fetch_lob_locator_omits_size_and_chunk_fields() {
        let locator: Vec<u8> = (0u8..114).collect();
        let payload = lob_row_payload(&locator, None);
        let columns = [clob_column()];

        let result = parse_fetch_response_with_context(
            &payload,
            ClientCapabilities::default(),
            &columns,
            None,
        )
        .expect("plain fetch CLOB locator should decode");

        let lob = first_lob(&result).expect("plain fetch should return a LOB value");
        assert_eq!(lob.locator, locator);
        assert_eq!(lob.size, 0);
        assert_eq!(lob.chunk_size, 0);
    }

    #[test]
    fn define_fetch_lob_locator_includes_size_and_chunk_fields() {
        let locator: Vec<u8> = (0u8..114).collect();
        let payload = lob_row_payload(&locator, Some((23, 8060)));
        let columns = [clob_column()];

        let result = parse_define_fetch_response_with_context_and_limits(
            &payload,
            ClientCapabilities::default(),
            &columns,
            None,
            ProtocolLimits::DEFAULT,
        )
        .expect("define fetch CLOB locator should decode");

        let lob = first_lob(&result).expect("define fetch should return a LOB value");
        assert_eq!(lob.locator, locator);
        assert_eq!(lob.size, 23);
        assert_eq!(lob.chunk_size, 8060);
    }

    #[test]
    fn borrowed_define_fetch_lob_page_matches_owned_parse() {
        let columns = [clob_column(), blob_column()];
        let clob_locator_a: Vec<u8> = (0u8..114).collect();
        let blob_locator_a: Vec<u8> = (128u8..242).collect();
        let clob_locator_b: Vec<u8> = (32u8..146).collect();
        let blob_locator_b: Vec<u8> = (64u8..178).collect();
        let mut writer = TtcWriter::new();

        writer.write_u8(TNS_MSG_TYPE_ROW_DATA);
        write_define_lob_cell(&mut writer, &clob_locator_a, 23, 8060);
        write_define_lob_cell(&mut writer, &blob_locator_a, 48, 4096);
        writer.write_u8(TNS_MSG_TYPE_ROW_DATA);
        write_define_lob_cell(&mut writer, &clob_locator_b, 31, 8060);
        write_define_lob_cell(&mut writer, &blob_locator_b, 96, 4096);
        writer.write_u8(TNS_MSG_TYPE_END_OF_RESPONSE);
        let payload = writer.into_bytes();

        let owned = parse_define_fetch_response_with_context_and_limits(
            &payload,
            ClientCapabilities::default(),
            &columns,
            None,
            ProtocolLimits::DEFAULT,
        )
        .expect("owned define fetch CLOB/BLOB page should parse");
        let borrowed = parse_define_fetch_response_borrowed_with_limits(
            &payload,
            ClientCapabilities::default(),
            &columns,
            None,
            ProtocolLimits::DEFAULT,
        )
        .expect("borrowed define fetch CLOB/BLOB page should parse");

        let mut borrowed_rows: Vec<Vec<Option<QueryValue>>> = Vec::new();
        borrowed
            .batch
            .for_each_row_ref(|row| {
                borrowed_rows.push(
                    row.iter()
                        .map(|cell| cell.map(|value| value.to_owned_value()))
                        .collect(),
                );
                Ok::<(), ProtocolError>(())
            })
            .expect("borrowed define fetch CLOB/BLOB page should iterate");

        assert_eq!(owned.rows.len(), 2, "owned parse sees both rows");
        assert_eq!(
            borrowed.batch.row_count(),
            2,
            "borrowed parse sees both rows"
        );
        assert_eq!(
            borrowed_rows, owned.rows,
            "borrowed DefineMetadata LOB parse must match owned parse"
        );
    }
}

/// Reads a VECTOR value (reference `ReadBuffer.read_vector` in `packet.pyx`).
/// VECTOR is sent as a fully-prefetched LOB: the image data precedes the
/// (discarded) LOB locator.
pub(crate) fn parse_vector_value(reader: &mut TtcReader<'_>) -> Result<Option<QueryValue>> {
    let num_bytes = reader.read_ub4()?;
    reader.limits().check_response_bytes(num_bytes as usize)?;
    if num_bytes == 0 {
        return Ok(None);
    }
    reader.read_ub8()?; // size (unused)
    reader.read_ub4()?; // chunk size (unused)
    let Some(data) = reader.read_bytes()? else {
        return Ok(None);
    };
    reader.read_bytes()?; // LOB locator (unused)
    if data.is_empty() {
        return Ok(None);
    }
    let vector = crate::vector::decode_vector_with_limits(&data, reader.limits())?;
    Ok(Some(QueryValue::Vector(Box::new(vector))))
}

/// Parses a native JSON (`DB_TYPE_JSON`) column value. Like VECTOR, OSON is sent
/// as a fully-prefetched LOB: `num_bytes`, `size`, `chunk_size`, the OSON image,
/// then a (discarded) LOB locator (reference packet.pyx `read_oson`).
pub(crate) fn parse_json_value(reader: &mut TtcReader<'_>) -> Result<Option<QueryValue>> {
    let num_bytes = reader.read_ub4()?;
    reader.limits().check_response_bytes(num_bytes as usize)?;
    if num_bytes == 0 {
        return Ok(None);
    }
    reader.read_ub8()?; // size (unused)
    reader.read_ub4()?; // chunk size (unused)
    let Some(data) = reader.read_bytes()? else {
        return Ok(None);
    };
    reader.read_bytes()?; // LOB locator (unused)
    if data.is_empty() {
        return Ok(None);
    }
    let value = crate::oson::decode_oson_with_limits(&data, reader.limits())?;
    Ok(Some(QueryValue::Json(Box::new(value))))
}

pub(crate) fn parse_object_value(
    reader: &mut TtcReader<'_>,
    metadata: &ColumnMetadata,
) -> Result<Option<QueryValue>> {
    let _toid = reader.read_bytes_with_length()?;
    let _oid = reader.read_bytes_with_length()?;
    let _snapshot = reader.read_bytes_with_length()?;
    let _version = reader.read_ub2()?;
    let num_bytes = reader.read_ub4()?;
    reader.limits().check_response_bytes(num_bytes as usize)?;
    reader.skip(2)?;
    if num_bytes == 0 {
        return Ok(None);
    }
    let Some(packed_data) = reader.read_bytes()? else {
        return Ok(None);
    };
    Ok(Some(QueryValue::Object(Box::new(ObjectValue {
        schema: metadata.object_schema.clone(),
        type_name: metadata.object_type_name.clone(),
        packed_data,
    }))))
}

pub(crate) fn parse_cursor_value(reader: &mut TtcReader<'_>) -> Result<QueryValue> {
    reader.skip(1)?;
    let mut result = QueryResult::default();
    parse_describe_info(reader, ClientCapabilities::default(), &mut result)?;
    let cursor_id = u32::from(reader.read_ub2()?);
    Ok(QueryValue::Cursor(Box::new(CursorValue {
        columns: result.columns,
        cursor_id,
    })))
}

pub(crate) struct QueryReturnParameters {
    pub row_counts: Option<Vec<u64>>,
    /// CQN registered-query id extracted from the registration-info block
    /// (reference base.pyx:1300-1309); `None` when no block was present.
    pub query_id: Option<u64>,
}

pub(crate) fn parse_query_return_parameters(
    reader: &mut TtcReader<'_>,
    arraydmlrowcounts: bool,
) -> Result<QueryReturnParameters> {
    let num_params = reader.read_ub2()?;
    for _ in 0..num_params {
        let _value = reader.read_ub4()?;
    }
    let num_bytes = reader.read_ub2()?;
    if num_bytes > 0 {
        reader.skip(usize::from(num_bytes))?;
    }
    let num_pairs = reader.read_ub2()?;
    skip_keyword_value_pairs(reader, num_pairs)?;
    // registration info block: the trailing 8 bytes (msb at -4, lsb at -8) are
    // the CQN query id when a registration id was sent (reference base.pyx).
    let num_bytes = usize::from(reader.read_ub2()?);
    let mut query_id = None;
    if num_bytes > 0 {
        let block = reader.read_raw(num_bytes)?;
        if num_bytes >= 8 {
            let msb = u32::from_be_bytes([
                block[num_bytes - 4],
                block[num_bytes - 3],
                block[num_bytes - 2],
                block[num_bytes - 1],
            ]);
            let lsb = u32::from_be_bytes([
                block[num_bytes - 8],
                block[num_bytes - 7],
                block[num_bytes - 6],
                block[num_bytes - 5],
            ]);
            query_id = Some((u64::from(msb) << 32) | u64::from(lsb));
        }
    }
    if arraydmlrowcounts {
        // reference messages/base.pyx `_process_return_parameters` tail
        let num_rows = reader.read_ub4()?;
        reader.limits().check_batch_rows(num_rows as usize)?;
        // Each ub8 row count consumes at least one byte, so cap the reservation
        // by the remaining payload size (BoundedReader).
        let mut row_counts: Vec<u64> =
            reader.with_capacity_limited(num_rows as usize, 1, ProtocolLimits::check_batch_rows)?;
        for _ in 0..num_rows {
            row_counts.push(reader.read_ub8()?);
        }
        return Ok(QueryReturnParameters {
            row_counts: Some(row_counts),
            query_id,
        });
    }
    Ok(QueryReturnParameters {
        row_counts: None,
        query_id,
    })
}

#[cfg(test)]
mod return_parameter_tests {
    use super::*;

    #[test]
    fn registration_info_block_extracts_query_id_from_lsb_msb_tail() {
        let mut writer = TtcWriter::new();
        writer.write_ub2(0); // num params
        writer.write_ub2(0); // parameter bytes
        writer.write_ub2(0); // keyword/value pairs
        writer.write_ub2(8); // registration-info block bytes
        writer.write_raw(&[
            0x55, 0x66, 0x77, 0x88, // lsb
            0x11, 0x22, 0x33, 0x44, // msb
        ]);
        let payload = writer.into_bytes();
        let mut reader = TtcReader::new(&payload);

        let params = parse_query_return_parameters(&mut reader, false).expect("return parameters");

        assert_eq!(params.query_id, Some(0x1122_3344_5566_7788));
        assert_eq!(params.row_counts, None);
    }
}

#[cfg(test)]
mod mutation_decode_tests {
    use super::*;
    use crate::oson::{encode_oson, OsonValue};
    use crate::vector::{encode_vector, Vector, VectorValues};

    fn caps(ttc_field_version: u8) -> ClientCapabilities {
        ClientCapabilities {
            ttc_field_version,
            max_string_size: 32_767,
            charset_id: 873,
        }
    }

    fn column(name: &str, ora_type_num: u8, csfrm: u8, buffer_size: u32) -> ColumnMetadata {
        ColumnMetadata {
            name: name.to_string(),
            ora_type_num,
            csfrm,
            buffer_size,
            max_size: buffer_size,
            nulls_allowed: true,
            ..ColumnMetadata::default()
        }
    }

    struct ColumnRecord<'a> {
        name: &'a str,
        ora_type_num: u8,
        csfrm: u8,
        buffer_size: u32,
        max_size: u32,
        uds_flags: u32,
        vector_dimensions: Option<u32>,
        vector_format: u8,
        vector_flags: u8,
        object_schema: Option<&'a str>,
        object_type_name: Option<&'a str>,
        annotations: &'a [(&'a str, &'a str)],
    }

    impl<'a> ColumnRecord<'a> {
        fn scalar(name: &'a str, ora_type_num: u8, csfrm: u8, buffer_size: u32) -> Self {
            Self {
                name,
                ora_type_num,
                csfrm,
                buffer_size,
                max_size: buffer_size,
                uds_flags: 0,
                vector_dimensions: None,
                vector_format: 0,
                vector_flags: 0,
                object_schema: None,
                object_type_name: None,
                annotations: &[],
            }
        }

        fn write(&self, writer: &mut TtcWriter, field_version: u8) {
            writer.write_u8(self.ora_type_num);
            writer.write_u8(0); // flags
            writer.write_u8(7); // precision
            writer.write_u8(2); // scale
            writer.write_ub4(self.buffer_size);
            writer.write_ub4(0); // max array elements
            writer.write_ub8(0); // cont flags
            writer
                .write_bytes_with_two_lengths(None)
                .expect("column oid");
            writer.write_ub2(0); // version
            writer.write_ub2(0); // server charset id
            writer.write_u8(self.csfrm);
            writer.write_ub4(self.max_size);
            if field_version >= TNS_CCAP_FIELD_VERSION_12_2 {
                writer.write_ub4(0x1122_3344); // oaccolid
            }
            writer.write_u8(1); // nullable
            writer.write_u8(0); // flags
            writer
                .write_bytes_with_two_lengths(Some(self.name.as_bytes()))
                .expect("column name");
            writer
                .write_bytes_with_two_lengths(self.object_schema.map(str::as_bytes))
                .expect("object schema");
            writer
                .write_bytes_with_two_lengths(self.object_type_name.map(str::as_bytes))
                .expect("object type");
            writer.write_ub2(1); // column position
            writer.write_ub4(self.uds_flags);
            if field_version >= TNS_CCAP_FIELD_VERSION_23_1 {
                writer
                    .write_bytes_with_two_lengths(Some(b"DOMSCHEMA"))
                    .expect("domain schema");
                writer
                    .write_bytes_with_two_lengths(Some(b"DOMNAME"))
                    .expect("domain name");
            }
            if field_version >= TNS_CCAP_FIELD_VERSION_23_1_EXT_3 {
                writer.write_ub4(u32::try_from(self.annotations.len()).expect("annotation count"));
                if !self.annotations.is_empty() {
                    writer.write_u8(0); // marker
                    writer.write_ub4(
                        u32::try_from(self.annotations.len()).expect("annotation count"),
                    );
                    writer.write_u8(0); // marker
                    for &(key, value) in self.annotations {
                        writer
                            .write_bytes_with_two_lengths(Some(key.as_bytes()))
                            .expect("annotation key");
                        writer
                            .write_bytes_with_two_lengths(Some(value.as_bytes()))
                            .expect("annotation value");
                        writer.write_ub4(0); // annotation flags
                    }
                    writer.write_ub4(0); // annotation block flags
                }
            }
            if field_version >= TNS_CCAP_FIELD_VERSION_23_4 {
                writer.write_ub4(self.vector_dimensions.unwrap_or(0));
                writer.write_u8(self.vector_format);
                writer.write_u8(self.vector_flags);
            }
        }
    }

    fn write_describe_body(
        writer: &mut TtcWriter,
        field_version: u8,
        columns: &[ColumnRecord<'_>],
    ) {
        writer.write_ub4(4096); // max row size
        writer.write_ub4(u32::try_from(columns.len()).expect("column count"));
        if !columns.is_empty() {
            writer.write_u8(0); // describe column marker
        }
        for column in columns {
            column.write(writer, field_version);
        }
        writer
            .write_bytes_with_two_lengths(None)
            .expect("current date");
        writer.write_ub4(0); // dcbflag
        writer.write_ub4(0); // dcbmdbz
        writer.write_ub4(0); // dcbmnpr
        writer.write_ub4(0); // dcbmxpr
        writer.write_bytes_with_two_lengths(None).expect("dcbqcky");
    }

    fn write_io_vector_body(
        writer: &mut TtcWriter,
        directions: &[u8],
        fast_fetch_bytes: &[u8],
        rowid_bytes: &[u8],
    ) {
        writer.write_u8(0); // flags
        writer.write_ub2(u16::try_from(directions.len()).expect("direction count"));
        writer.write_ub4(0); // high num-binds chunk
        writer.write_ub4(1); // iterations this time
        writer.write_ub2(0); // uac buffer length
        writer.write_ub2(u16::try_from(fast_fetch_bytes.len()).expect("fast fetch len"));
        writer.write_raw(fast_fetch_bytes);
        writer.write_ub2(u16::try_from(rowid_bytes.len()).expect("rowid len"));
        writer.write_raw(rowid_bytes);
        for &direction in directions {
            writer.write_u8(direction);
        }
    }

    struct ErrorInfo<'a> {
        number: u32,
        message: &'a str,
        cursor_id: u16,
        row_count: u64,
        call_status: u32,
        warning_flags: u8,
        rowid: Option<(u32, u16, u32, u16)>,
    }

    fn write_error_info(writer: &mut TtcWriter, info: ErrorInfo<'_>) {
        writer.write_ub4(info.call_status);
        writer.write_ub2(0); // seq
        writer.write_ub4(0); // current row
        writer.write_ub2(0); // obsolete short error number
        writer.write_ub2(0); // array elem error 1
        writer.write_ub2(0); // array elem error 2
        writer.write_ub2(info.cursor_id);
        writer.write_sb4(0); // error position
        writer.write_raw(&[0u8; 5]);
        writer.write_u8(info.warning_flags);
        let (rba, partition_id, block_num, slot_num) = info.rowid.unwrap_or((0, 0, 0, 0));
        writer.write_ub4(rba);
        writer.write_ub2(partition_id);
        writer.write_u8(0);
        writer.write_ub4(block_num);
        writer.write_ub2(slot_num);
        writer.write_ub4(0); // os error
        writer.write_raw(&[0u8; 2]);
        writer.write_ub2(0); // padding
        writer.write_ub4(0); // success iterations
        writer
            .write_bytes_with_two_lengths(None)
            .expect("diagnostic field");
        writer.write_ub2(0); // batch error count
        writer.write_ub4(0); // batch offset count
        writer.write_ub2(0); // batch message count
        writer.write_ub4(info.number);
        writer.write_ub8(info.row_count);
        writer.write_ub4(0); // sql type (field version >= 20.1)
        writer.write_ub4(0); // server checksum
        if info.number != 0 {
            writer
                .write_bytes_with_length(info.message.as_bytes())
                .expect("server error message");
        }
    }

    fn write_return_parameters(
        writer: &mut TtcWriter,
        registration_block: &[u8],
        row_counts: Option<&[u64]>,
    ) {
        writer.write_ub2(0); // num params
        writer.write_ub2(0); // parameter bytes
        writer.write_ub2(0); // keyword/value pairs
        writer.write_ub2(u16::try_from(registration_block.len()).expect("registration len"));
        writer.write_raw(registration_block);
        if let Some(row_counts) = row_counts {
            writer.write_ub4(u32::try_from(row_counts.len()).expect("row count len"));
            for &count in row_counts {
                writer.write_ub8(count);
            }
        }
    }

    fn write_prefetched_lob_value(writer: &mut TtcWriter, data: &[u8]) {
        writer.write_ub4(u32::try_from(data.len()).expect("data len"));
        writer.write_ub8(u64::try_from(data.len()).expect("data size"));
        writer.write_ub4(8192);
        writer
            .write_bytes_with_length(data)
            .expect("prefetched data");
        writer.write_bytes_with_length(b"locator").expect("locator");
    }

    fn write_physical_urowid_cell(writer: &mut TtcWriter) -> String {
        let rba: u32 = 0x0102_0304;
        let partition_id: u16 = 0x0506;
        let block_num: u32 = 0x0708_090a;
        let slot_num: u16 = 0x0b0c;
        let mut encoded = Vec::new();
        encoded.push(1);
        encoded.extend_from_slice(&rba.to_be_bytes());
        encoded.extend_from_slice(&partition_id.to_be_bytes());
        encoded.extend_from_slice(&block_num.to_be_bytes());
        encoded.extend_from_slice(&slot_num.to_be_bytes());
        writer.write_bytes_with_length(&[1]).expect("urowid probe");
        writer
            .write_bytes_with_length(&encoded)
            .expect("physical urowid");
        encode_physical_rowid(rba, partition_id, block_num, slot_num)
    }

    #[test]
    fn response_wrappers_parse_out_binds_returning_status_token_and_error_state() {
        let binds = [
            BindValue::InOut {
                value: Box::new(BindValue::Text("in".to_string())),
                out_buffer_size: 20,
            },
            BindValue::ReturnOutput {
                ora_type_num: ORA_TYPE_NUM_VARCHAR,
                csfrm: CS_FORM_IMPLICIT,
                buffer_size: 20,
            },
        ];

        let mut out_payload = TtcWriter::new();
        out_payload.write_u8(TNS_MSG_TYPE_IO_VECTOR);
        write_io_vector_body(&mut out_payload, &[16, TNS_BIND_DIR_INPUT], b"F", b"R");
        out_payload.write_u8(TNS_MSG_TYPE_ROW_DATA);
        out_payload
            .write_bytes_with_length(b"OUT")
            .expect("out bind value");
        out_payload.write_sb4(0);
        out_payload.write_u8(TNS_MSG_TYPE_STATUS);
        out_payload.write_ub4(TNS_EOCS_FLAGS_TXN_IN_PROGRESS);
        out_payload.write_ub2(0);
        out_payload.write_u8(TNS_MSG_TYPE_SERVER_SIDE_PIGGYBACK);
        out_payload.write_u8(TNS_SERVER_PIGGYBACK_QUERY_CACHE_INVALIDATION);
        out_payload.write_u8(TNS_MSG_TYPE_TOKEN);
        out_payload.write_ub8(0x1122_3344_5566_7788);
        out_payload.write_u8(TNS_MSG_TYPE_ERROR);
        write_error_info(
            &mut out_payload,
            ErrorInfo {
                number: 0,
                message: "",
                cursor_id: 77,
                row_count: 3,
                call_status: 0,
                warning_flags: 0x20,
                rowid: Some((1, 2, 3, 4)),
            },
        );

        let result = parse_query_response_with_binds_options_and_columns(
            &out_payload.into_bytes(),
            caps(TNS_CCAP_FIELD_VERSION_23_4),
            &binds,
            ExecuteOptions::default(),
            &[],
        )
        .expect("owned response with OUT bind/status/token/error");
        assert_eq!(
            result.out_values,
            vec![(0, Some(QueryValue::Text("OUT".to_string())))]
        );
        assert_eq!(result.txn_in_progress, Some(false));
        assert_eq!(result.cursor_id, 77);
        assert_eq!(result.row_count, 3);
        assert!(result.compilation_error_warning);
        assert_eq!(result.last_rowid, Some(encode_physical_rowid(1, 2, 3, 4)));
        assert_eq!(result.token_num, Some(0x1122_3344_5566_7788));

        let mut returning_payload = TtcWriter::new();
        returning_payload.write_u8(TNS_MSG_TYPE_ROW_DATA);
        returning_payload.write_ub4(2);
        returning_payload
            .write_bytes_with_length(b"A")
            .expect("returning value 1");
        returning_payload.write_sb4(0);
        returning_payload
            .write_bytes_with_length(b"B")
            .expect("returning value 2");
        returning_payload.write_sb4(0);
        returning_payload.write_u8(TNS_MSG_TYPE_END_OF_RESPONSE);

        let returning = parse_query_response_with_binds_options_columns_and_limits(
            &returning_payload.into_bytes(),
            caps(TNS_CCAP_FIELD_VERSION_23_4),
            &binds,
            ExecuteOptions::default(),
            &[],
            ProtocolLimits::DEFAULT,
        )
        .expect("owned response with RETURNING values");
        assert_eq!(
            returning.return_values,
            vec![(
                1,
                vec![
                    Some(QueryValue::Text("A".to_string())),
                    Some(QueryValue::Text("B".to_string()))
                ]
            )]
        );
    }

    #[test]
    fn response_flush_breaks_before_trailing_unknown_message() {
        let payload = [TNS_MSG_TYPE_FLUSH_OUT_BINDS, 0xff];
        parse_query_response(&payload, caps(TNS_CCAP_FIELD_VERSION_23_4))
            .expect("flush should stop response parsing before trailing bytes");
    }

    #[test]
    fn no_data_error_marks_more_rows_false_when_columns_are_known() {
        let mut payload = TtcWriter::new();
        payload.write_u8(TNS_MSG_TYPE_ERROR);
        write_error_info(
            &mut payload,
            ErrorInfo {
                number: TNS_ERR_NO_DATA_FOUND,
                message: "ORA-01403: no data found",
                cursor_id: 91,
                row_count: 11,
                call_status: TNS_EOCS_FLAGS_TXN_IN_PROGRESS,
                warning_flags: 0,
                rowid: None,
            },
        );
        let columns = [column("C", ORA_TYPE_NUM_VARCHAR, CS_FORM_IMPLICIT, 30)];

        let result = parse_query_response_with_context(
            &payload.into_bytes(),
            caps(TNS_CCAP_FIELD_VERSION_23_4),
            &columns,
            None,
        )
        .expect("no-data end-of-call should finalize fetch");

        assert!(!result.more_rows);
        assert_eq!(result.cursor_id, 91);
        assert_eq!(result.row_count, 11);
        assert_eq!(result.txn_in_progress, Some(true));
    }

    #[test]
    fn io_vector_boundaries_skip_zero_and_one_byte_payloads_and_filter_returning() {
        let mut zero = TtcWriter::new();
        write_io_vector_body(&mut zero, &[16, 48, TNS_BIND_DIR_INPUT], &[], &[]);
        let zero_bytes = zero.into_bytes();
        let mut reader = TtcReader::new(&zero_bytes);
        assert_eq!(
            parse_io_vector(&mut reader, 2).expect("zero skips"),
            vec![0, 1]
        );
        assert_eq!(reader.remaining(), 0);

        let mut one = TtcWriter::new();
        write_io_vector_body(&mut one, &[TNS_BIND_DIR_INPUT, 16, 48], b"X", b"Y");
        let one_bytes = one.into_bytes();
        let mut reader = TtcReader::new(&one_bytes);
        assert_eq!(
            parse_io_vector(&mut reader, 2).expect("one-byte skips"),
            vec![1]
        );
        assert_eq!(reader.remaining(), 0);
    }

    #[test]
    fn describe_and_column_metadata_decode_zero_columns_annotations_flags_and_vector() {
        let field_version = TNS_CCAP_FIELD_VERSION_23_4;

        let mut zero_body = TtcWriter::new();
        write_describe_body(&mut zero_body, field_version, &[]);
        let zero_bytes = zero_body.into_bytes();
        let mut zero_reader = TtcReader::new(&zero_bytes);
        let mut zero_result = QueryResult::default();
        parse_describe_info(&mut zero_reader, caps(field_version), &mut zero_result)
            .expect("zero-column describe");
        assert!(zero_result.columns.is_empty());
        assert_eq!(zero_reader.remaining(), 0);

        let annotated_vector = ColumnRecord {
            name: "VEC",
            ora_type_num: ORA_TYPE_NUM_VECTOR,
            csfrm: CS_FORM_IMPLICIT,
            buffer_size: 16,
            max_size: 32,
            uds_flags: TNS_UDS_FLAGS_IS_JSON | TNS_UDS_FLAGS_IS_OSON,
            vector_dimensions: Some(1536),
            vector_format: 2,
            vector_flags: 0xa5,
            object_schema: None,
            object_type_name: None,
            annotations: &[("purpose", "mutation")],
        };
        let mut one_body = TtcWriter::new();
        write_describe_body(&mut one_body, field_version, &[annotated_vector]);
        let one_bytes = one_body.into_bytes();
        let mut one_reader = TtcReader::new(&one_bytes);
        let mut one_result = QueryResult::default();
        parse_describe_info(&mut one_reader, caps(field_version), &mut one_result)
            .expect("one-column describe");
        let meta = one_result.columns.first().expect("described column");
        assert_eq!(meta.name(), "VEC");
        assert_eq!(meta.buffer_size(), 16);
        assert_eq!(meta.max_size(), 32);
        assert!(meta.is_json());
        assert!(meta.is_oson());
        assert_eq!(meta.domain_schema(), Some("DOMSCHEMA"));
        assert_eq!(meta.domain_name(), Some("DOMNAME"));
        assert_eq!(
            meta.annotations(),
            Some(&[("purpose".to_string(), "mutation".to_string())][..])
        );
        assert_eq!(meta.vector_dimensions(), Some(1536));
        assert_eq!(meta.vector_format(), 2);
        assert_eq!(meta.vector_flags(), 0xa5);
        assert_eq!(one_reader.remaining(), 0);
    }

    #[test]
    fn row_header_returns_embedded_bit_vector() {
        let mut writer = TtcWriter::new();
        writer.write_u8(0); // skip
        writer.write_ub2(1); // requests
        writer.write_ub4(2); // iteration
        writer.write_ub4(3); // num iters
        writer.write_ub2(4); // buffer length
        writer.write_ub4(1); // bit-vector bytes
        writer.write_u8(0); // marker
        writer.write_raw(&[0b1111_1101]);
        writer
            .write_bytes_with_two_lengths(Some(b"rid"))
            .expect("rxhrid");
        let bytes = writer.into_bytes();
        let mut reader = TtcReader::new(&bytes);

        assert_eq!(
            parse_row_header(&mut reader).expect("row header"),
            Some(vec![0b1111_1101])
        );
        assert_eq!(reader.remaining(), 0);
    }

    #[test]
    fn owned_column_value_decodes_scalar_and_cold_variants() {
        let scalar_cases: Vec<(ColumnMetadata, Vec<u8>, Option<QueryValue>)> = vec![
            (
                column("D", ORA_TYPE_NUM_BINARY_DOUBLE, CS_FORM_IMPLICIT, 8),
                encode_binary_double(-2.5).to_vec(),
                Some(QueryValue::BinaryDouble("-2.5".to_string())),
            ),
            (
                column("F", ORA_TYPE_NUM_BINARY_FLOAT, CS_FORM_IMPLICIT, 4),
                encode_binary_float(3.25).to_vec(),
                Some(QueryValue::BinaryDouble("3.25".to_string())),
            ),
            (
                column("B", ORA_TYPE_NUM_BOOLEAN, CS_FORM_IMPLICIT, 1),
                vec![0, 1],
                Some(QueryValue::Boolean(true)),
            ),
            (
                column("DS", ORA_TYPE_NUM_INTERVAL_DS, CS_FORM_IMPLICIT, 11),
                encode_interval_ds(4, 5 * 3600 + 6 * 60 + 7, 890)
                    .expect("interval ds")
                    .to_vec(),
                Some(QueryValue::IntervalDS {
                    days: 4,
                    hours: 5,
                    minutes: 6,
                    seconds: 7,
                    fseconds: 890,
                }),
            ),
            (
                column("YM", ORA_TYPE_NUM_INTERVAL_YM, CS_FORM_IMPLICIT, 5),
                encode_interval_ym(-3, 2).expect("interval ym").to_vec(),
                Some(QueryValue::IntervalYM {
                    years: -3,
                    months: 2,
                }),
            ),
            (
                column("TS", ORA_TYPE_NUM_TIMESTAMP, CS_FORM_IMPLICIT, 11),
                encode_oracle_timestamp(2026, 7, 8, 9, 10, 11, 123_456_789).expect("timestamp"),
                Some(QueryValue::DateTime {
                    year: 2026,
                    month: 7,
                    day: 8,
                    hour: 9,
                    minute: 10,
                    second: 11,
                    nanosecond: 123_456_789,
                }),
            ),
        ];

        for (metadata, bytes, expected) in scalar_cases {
            let mut writer = TtcWriter::new();
            writer
                .write_bytes_with_length(&bytes)
                .expect("framed scalar");
            let payload = writer.into_bytes();
            let mut reader = TtcReader::new(&payload);
            assert_eq!(
                parse_column_value(&mut reader, &metadata).expect("scalar decode"),
                expected,
                "owned scalar decode for {}",
                metadata.name()
            );
            assert_eq!(reader.remaining(), 0);
        }

        let mut lob_writer = TtcWriter::new();
        lob_writer.write_ub4(3);
        lob_writer.write_ub8(3);
        lob_writer.write_ub4(8192);
        lob_writer
            .write_bytes_with_length(b"lob")
            .expect("lob locator");
        let lob_bytes = lob_writer.into_bytes();
        let mut reader = TtcReader::new(&lob_bytes);
        assert!(matches!(
            parse_column_value(
                &mut reader,
                &column("CLOB", ORA_TYPE_NUM_CLOB, CS_FORM_IMPLICIT, 4000)
            )
            .expect("lob decode"),
            Some(QueryValue::Lob(_))
        ));

        let vector = Vector::Dense(VectorValues::Int8(vec![-1, 0, 7]));
        let vector_image = encode_vector(&vector);
        let mut writer = TtcWriter::new();
        write_prefetched_lob_value(&mut writer, &vector_image);
        let bytes = writer.into_bytes();
        let mut reader = TtcReader::new(&bytes);
        assert_eq!(
            parse_column_value(
                &mut reader,
                &column("VEC", ORA_TYPE_NUM_VECTOR, CS_FORM_IMPLICIT, 4000)
            )
            .expect("vector decode"),
            Some(QueryValue::Vector(Box::new(vector)))
        );

        let json = OsonValue::Object(vec![("k".to_string(), OsonValue::String("v".to_string()))]);
        let json_image = encode_oson(&json, false).expect("encode oson");
        let mut writer = TtcWriter::new();
        write_prefetched_lob_value(&mut writer, &json_image);
        let bytes = writer.into_bytes();
        let mut reader = TtcReader::new(&bytes);
        assert_eq!(
            parse_column_value(
                &mut reader,
                &column("JSON", ORA_TYPE_NUM_JSON, CS_FORM_IMPLICIT, 4000)
            )
            .expect("json decode"),
            Some(QueryValue::Json(Box::new(json)))
        );

        let object_metadata = ColumnMetadata {
            name: "OBJ".to_string(),
            ora_type_num: ORA_TYPE_NUM_OBJECT,
            buffer_size: 4000,
            object_schema: Some("S".to_string()),
            object_type_name: Some("T".to_string()),
            ..ColumnMetadata::default()
        };
        let mut writer = TtcWriter::new();
        writer
            .write_bytes_with_two_lengths(Some(b"toid"))
            .expect("toid");
        writer
            .write_bytes_with_two_lengths(Some(b"oid"))
            .expect("oid");
        writer
            .write_bytes_with_two_lengths(Some(b"snap"))
            .expect("snapshot");
        writer.write_ub2(1);
        writer.write_ub4(4);
        writer.write_raw(&[0, 0]);
        writer
            .write_bytes_with_length(b"PACK")
            .expect("object payload");
        let bytes = writer.into_bytes();
        let mut reader = TtcReader::new(&bytes);
        assert!(matches!(
            parse_column_value(&mut reader, &object_metadata).expect("object decode"),
            Some(QueryValue::Object(object))
                if object.schema.as_deref() == Some("S")
                    && object.type_name.as_deref() == Some("T")
                    && object.packed_data == b"PACK"
        ));

        let mut writer = TtcWriter::new();
        writer.write_u8(0); // cursor flags
        write_describe_body(&mut writer, TNS_CCAP_FIELD_VERSION_23_4, &[]);
        writer.write_ub2(44);
        let bytes = writer.into_bytes();
        let mut reader = TtcReader::new(&bytes);
        assert!(matches!(
            parse_column_value(
                &mut reader,
                &column("CUR", ORA_TYPE_NUM_CURSOR, CS_FORM_IMPLICIT, 1)
            )
            .expect("cursor decode"),
            Some(QueryValue::Cursor(cursor)) if cursor.cursor_id == 44
        ));
    }

    #[test]
    fn urowid_requires_full_physical_payload() {
        let mut truncated = TtcWriter::new();
        truncated
            .write_bytes_with_length(&[1])
            .expect("urowid probe");
        truncated
            .write_bytes_with_length(&[1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12])
            .expect("truncated physical urowid");
        let truncated_bytes = truncated.into_bytes();
        let mut reader = TtcReader::new(&truncated_bytes);
        assert!(parse_urowid_value(&mut reader).is_err());

        let mut ok = TtcWriter::new();
        let expected = write_physical_urowid_cell(&mut ok);
        let ok_bytes = ok.into_bytes();
        let mut reader = TtcReader::new(&ok_bytes);
        assert_eq!(
            parse_urowid_value(&mut reader).expect("physical urowid"),
            Some(expected)
        );
    }

    #[test]
    fn rowid_parser_distinguishes_null_and_physical_rowid() {
        let null_payload = [0u8];
        let mut reader = TtcReader::new(&null_payload);
        assert_eq!(parse_rowid_value(&mut reader).expect("null ROWID"), None);

        let rba: u32 = 0x0102_0304;
        let partition_id: u16 = 0x0506;
        let block_num: u32 = 0x0708_090a;
        let slot_num: u16 = 0x0b0c;
        let mut physical = TtcWriter::new();
        physical.write_u8(1);
        physical.write_ub4(rba);
        physical.write_ub2(partition_id);
        physical.write_u8(0);
        physical.write_ub4(block_num);
        physical.write_ub2(slot_num);
        let physical_bytes = physical.into_bytes();
        let mut reader = TtcReader::new(&physical_bytes);

        assert_eq!(
            parse_rowid_value(&mut reader).expect("physical ROWID"),
            Some(encode_physical_rowid(
                rba,
                partition_id,
                block_num,
                slot_num
            ))
        );
        assert_eq!(reader.remaining(), 0);
    }

    #[test]
    fn borrowed_slots_cover_scalar_variants_and_nchar_fallback() {
        let columns = vec![
            column("TEXT", ORA_TYPE_NUM_VARCHAR, CS_FORM_IMPLICIT, 20),
            column("NCHAR", ORA_TYPE_NUM_VARCHAR, CS_FORM_NCHAR, 20),
            column("RAW", ORA_TYPE_NUM_RAW, CS_FORM_IMPLICIT, 20),
            column("NUM", ORA_TYPE_NUM_NUMBER, CS_FORM_IMPLICIT, 22),
            column("BOOL", ORA_TYPE_NUM_BOOLEAN, CS_FORM_IMPLICIT, 1),
            column("DS", ORA_TYPE_NUM_INTERVAL_DS, CS_FORM_IMPLICIT, 11),
            column("YM", ORA_TYPE_NUM_INTERVAL_YM, CS_FORM_IMPLICIT, 5),
            column("TS", ORA_TYPE_NUM_TIMESTAMP, CS_FORM_IMPLICIT, 11),
        ];
        let mut writer = TtcWriter::new();
        writer.write_bytes_with_length(b"hello").expect("text");
        writer
            .write_bytes_with_length(&[0, b'H', 0, b'i'])
            .expect("nchar utf16");
        writer.write_bytes_with_length(&[0xde, 0xad]).expect("raw");
        let number = encode_number_text("123.45").expect("number");
        writer
            .write_bytes_with_length(&number)
            .expect("number cell");
        writer.write_bytes_with_length(&[1]).expect("boolean");
        writer
            .write_bytes_with_length(
                &encode_interval_ds(1, 2 * 3600 + 3 * 60 + 4, 5).expect("interval ds"),
            )
            .expect("interval ds cell");
        writer
            .write_bytes_with_length(&encode_interval_ym(6, 7).expect("interval ym"))
            .expect("interval ym cell");
        writer
            .write_bytes_with_length(
                &encode_oracle_timestamp(2026, 7, 8, 9, 10, 11, 0).expect("timestamp"),
            )
            .expect("timestamp cell");
        let buffer = writer.into_bytes();
        let ptr_range = {
            let start = buffer.as_ptr() as usize;
            start..start + buffer.len()
        };
        let batch = BorrowedRowBatch::new(buffer, columns, vec![0]);

        batch
            .for_each_row_ref(|row| {
                assert!(matches!(row[0], Some(QueryValueRef::Text("hello"))));
                if let Some(QueryValueRef::Text(text)) = row[0] {
                    assert!(ptr_range.contains(&(text.as_ptr() as usize)));
                }
                assert_eq!(
                    row[1].as_ref().map(QueryValueRef::to_owned_value),
                    Some(QueryValue::Text("Hi".to_string()))
                );
                assert_eq!(
                    row[2].as_ref().and_then(|v| v.as_raw()),
                    Some(&[0xde, 0xad][..])
                );
                assert_eq!(
                    row[3].as_ref().and_then(|v| v.as_number_text()),
                    Some("123.45")
                );
                assert_eq!(
                    row[4].as_ref().map(QueryValueRef::to_owned_value),
                    Some(QueryValue::Boolean(true))
                );
                assert!(matches!(
                    row[5].as_ref().map(QueryValueRef::to_owned_value),
                    Some(QueryValue::IntervalDS {
                        days: 1,
                        hours: 2,
                        minutes: 3,
                        seconds: 4,
                        fseconds: 5
                    })
                ));
                assert_eq!(
                    row[6].as_ref().map(QueryValueRef::to_owned_value),
                    Some(QueryValue::IntervalYM {
                        years: 6,
                        months: 7
                    })
                );
                assert!(matches!(
                    row[7].as_ref().map(QueryValueRef::to_owned_value),
                    Some(QueryValue::DateTime {
                        year: 2026,
                        month: 7,
                        day: 8,
                        hour: 9,
                        minute: 10,
                        second: 11,
                        nanosecond: 0
                    })
                ));
                Ok::<(), ProtocolError>(())
            })
            .expect("borrowed scalar slots");
    }

    #[test]
    fn borrowed_response_dispatches_messages_and_surfaces_non_default_state() {
        let field_version = TNS_CCAP_FIELD_VERSION_23_4;
        let mut payload = TtcWriter::new();
        payload.write_u8(0);
        payload.write_u8(TNS_MSG_TYPE_DESCRIBE_INFO);
        payload
            .write_bytes_with_length(b"describe")
            .expect("describe name");
        write_describe_body(
            &mut payload,
            field_version,
            &[ColumnRecord::scalar(
                "N",
                ORA_TYPE_NUM_NUMBER,
                CS_FORM_IMPLICIT,
                22,
            )],
        );
        payload.write_u8(TNS_MSG_TYPE_ROW_HEADER);
        payload.write_u8(0);
        payload.write_ub2(1);
        payload.write_ub4(1);
        payload.write_ub4(1);
        payload.write_ub2(0);
        payload.write_ub4(0);
        payload
            .write_bytes_with_two_lengths(None)
            .expect("row header rxhrid");
        payload.write_u8(TNS_MSG_TYPE_ROW_DATA);
        let number = encode_number_text("7").expect("number");
        payload
            .write_bytes_with_length(&number)
            .expect("row number");
        payload.write_u8(TNS_MSG_TYPE_PARAMETER);
        write_return_parameters(&mut payload, &[], None);
        payload.write_u8(TNS_MSG_TYPE_STATUS);
        payload.write_ub4(TNS_EOCS_FLAGS_TXN_IN_PROGRESS);
        payload.write_ub2(0);
        payload.write_u8(TNS_MSG_TYPE_SERVER_SIDE_PIGGYBACK);
        payload.write_u8(TNS_SERVER_PIGGYBACK_QUERY_CACHE_INVALIDATION);
        payload.write_u8(TNS_MSG_TYPE_IMPLICIT_RESULTSET);
        payload.write_ub4(0);
        payload.write_u8(TNS_MSG_TYPE_TOKEN);
        payload.write_ub8(99);
        payload.write_u8(TNS_MSG_TYPE_ERROR);
        write_error_info(
            &mut payload,
            ErrorInfo {
                number: TNS_ERR_NO_DATA_FOUND,
                message: "ORA-01403: no data found",
                cursor_id: 123,
                row_count: 1,
                call_status: 0,
                warning_flags: 0,
                rowid: None,
            },
        );

        let borrowed =
            parse_query_response_borrowed(&payload.into_bytes(), caps(field_version), &[], None)
                .expect("borrowed response");
        assert!(!borrowed.more_rows);
        assert_eq!(borrowed.cursor_id, 123);
        assert_eq!(borrowed.row_count, 1);
        assert_eq!(borrowed.batch.columns()[0].name(), "N");
        assert_eq!(borrowed.batch.row_count(), 1);
        let mut rows = Vec::new();
        borrowed
            .batch
            .for_each_row_ref(|row| {
                rows.push(
                    row[0]
                        .as_ref()
                        .and_then(|value| value.as_number_text())
                        .map(str::to_string)
                        .unwrap_or_else(|| "<missing>".to_string()),
                );
                Ok::<(), ProtocolError>(())
            })
            .expect("iterate borrowed response");
        assert_eq!(rows, vec!["7".to_string()]);
    }

    #[test]
    fn borrowed_response_uses_bit_vector_duplicates_and_breaks_on_flush() {
        let columns = vec![
            column("A", ORA_TYPE_NUM_NUMBER, CS_FORM_IMPLICIT, 22),
            column("B", ORA_TYPE_NUM_NUMBER, CS_FORM_IMPLICIT, 22),
        ];
        let previous_row = vec![
            Some(QueryValue::number_from_text("10", true)),
            Some(QueryValue::number_from_text("20", true)),
        ];
        let mut payload = TtcWriter::new();
        payload.write_u8(TNS_MSG_TYPE_BIT_VECTOR);
        payload.write_ub2(2);
        payload.write_raw(&[0b1111_1101]); // column 1 is duplicate
        payload.write_u8(TNS_MSG_TYPE_ROW_DATA);
        let value = encode_number_text("30").expect("number");
        payload
            .write_bytes_with_length(&value)
            .expect("first column");
        payload.write_u8(TNS_MSG_TYPE_FLUSH_OUT_BINDS);
        payload.write_u8(0xff);

        let borrowed = parse_query_response_borrowed(
            &payload.into_bytes(),
            caps(TNS_CCAP_FIELD_VERSION_23_4),
            &columns,
            Some(&previous_row),
        )
        .expect("borrowed bit-vector response");
        assert_eq!(borrowed.batch.row_count(), 1);
        borrowed
            .batch
            .for_each_row_ref(|row| {
                assert_eq!(row[0].as_ref().and_then(|v| v.as_number_text()), Some("30"));
                assert_eq!(
                    row[1]
                        .as_ref()
                        .map(QueryValueRef::to_owned_value)
                        .and_then(|value| value.as_i64()),
                    Some(20)
                );
                Ok::<(), ProtocolError>(())
            })
            .expect("duplicate column resolution");
    }

    #[test]
    fn return_parameters_decode_registration_block_and_row_counts() {
        let mut writer = TtcWriter::new();
        write_return_parameters(
            &mut writer,
            &[
                0xaa, 0xbb, 0xcc, 0xdd, // lsb
                0x11, 0x22, 0x33, 0x44, // msb
            ],
            Some(&[2, 0, 5]),
        );
        let payload = writer.into_bytes();
        let mut reader = TtcReader::new(&payload);

        let params = parse_query_return_parameters(&mut reader, true).expect("return parameters");

        assert_eq!(params.query_id, Some(0x1122_3344_aabb_ccdd));
        assert_eq!(params.row_counts, Some(vec![2, 0, 5]));
        assert_eq!(reader.remaining(), 0);
    }
}

#[cfg(test)]
mod batch_error_continuation_tests {
    use super::*;

    // Offline proof of the `executemany(batcherrors=True, arraydmlrowcounts=True)`
    // continuation contract (bead a4-j1w / rust-oracledb iec3.1.13). The server
    // does NOT abort on the first bad row: it processes the whole bind array,
    // returns a per-iteration affected-row-count vector, and reports the failing
    // rows through the ORA-24381 batch-error arrays instead of raising a fatal
    // error (reference impl/thin/messages/base.pyx `_process_error_info`). This
    // exercises the REAL wire decoder end to end offline — no live array DML — and
    // asserts continuation (every iteration accounted for), the per-row error map
    // (each failure keyed to its input-row offset), and that the surviving rows
    // still report their commit counts.

    /// Writes the `_process_return_parameters` block (num-params / parameter-bytes
    /// / keyword-pairs / registration-block, all empty) followed by the
    /// `arraydmlrowcounts` tail carrying one affected-row count per bind row.
    fn write_return_parameters_with_row_counts(w: &mut TtcWriter, row_counts: &[u64]) {
        w.write_ub2(0); // num params
        w.write_ub2(0); // parameter bytes
        w.write_ub2(0); // keyword/value pairs
        w.write_ub2(0); // registration-info block bytes
        w.write_ub4(u32::try_from(row_counts.len()).expect("row count fits u32"));
        for &count in row_counts {
            w.write_ub8(count);
        }
    }

    /// Writes an `ORA-24381` server-error-info record whose batch-error code /
    /// offset / message arrays mirror `parse_server_error_info`. The fixed header
    /// matches the parser field-for-field (same shape as the errors.rs boundary
    /// fixture); the arrays use the short (non-`0xfe`) packed-length form.
    fn write_batch_error_info(w: &mut TtcWriter, errors: &[(u32, u32, &str)]) {
        // --- fixed header (mirrors parse_server_error_info) ---
        w.write_ub4(0); // call status
        w.write_ub2(0); // seq
        w.write_ub4(0); // current row
        w.write_ub2(0); // error number (obsolete short)
        w.write_ub2(0); // array elem error 1
        w.write_ub2(0); // array elem error 2
        w.write_ub2(0); // cursor id
        w.write_sb4(0); // error position
        w.write_raw(&[0u8; 5]); // skip(5)
        w.write_u8(0); // warning flags
                       // rowid: ub4 rba, ub2 partition, skip(1), ub4 block, ub2 slot
        w.write_ub4(0);
        w.write_ub2(0);
        w.write_u8(0);
        w.write_ub4(0);
        w.write_ub2(0);
        w.write_ub4(0); // os error
        w.write_raw(&[0u8; 2]); // skip(2)
        w.write_ub2(0); // padding
        w.write_ub4(0); // success iters
        w.write_bytes_with_two_lengths(None)
            .expect("empty rowid-diagnostic field"); // read_bytes_with_length

        let count = errors.len();
        // --- batch error CODE array ---
        w.write_ub2(u16::try_from(count).expect("error count fits u16"));
        if count > 0 {
            w.write_u8(u8::try_from(count).expect("packed length fits u8")); // != 0xfe
            for &(code, _, _) in errors {
                w.write_ub2(u16::try_from(code).expect("ORA code fits u16"));
            }
        }
        // --- batch error OFFSET (input-row index) array ---
        w.write_ub4(u32::try_from(count).expect("offset count fits u32"));
        if count > 0 {
            w.write_u8(u8::try_from(count).expect("packed length fits u8"));
            for &(_, offset, _) in errors {
                w.write_ub4(offset);
            }
        }
        // --- batch error MESSAGE array ---
        w.write_ub2(u16::try_from(count).expect("message count fits u16"));
        if count > 0 {
            w.write_u8(0); // packed size (parser skips 1)
            for &(_, _, message) in errors {
                w.write_ub2(u16::try_from(message.len()).expect("message len fits u16")); // discarded chunk len
                w.write_bytes_with_length(message.as_bytes())
                    .expect("batch error message");
                w.write_raw(&[0u8, 0u8]); // 2-byte end marker
            }
        }
        // --- trailing error number / row count / (20.1+) sql-type+checksum / message ---
        w.write_ub4(TNS_ERR_ARRAY_DML_ERRORS);
        w.write_ub8(0); // row count
                        // ClientCapabilities::default() negotiates ttc field version 24 (>= 20.1),
                        // so a modern server sends the sql-type + server-checksum pair here
                        // (reference messages/base.pyx:238). Emit it so the summary message frames.
        w.write_ub4(0); // sql type
        w.write_ub4(0); // server checksum
        w.write_bytes_with_length(b"ORA-24381: error(s) in array DML operation")
            .expect("summary message");
    }

    #[test]
    fn execute_response_decodes_batch_error_continuation_and_row_counts() {
        // A 5-row batch where input rows 1 and 3 violate constraints; rows 0, 2
        // and 4 commit. A pre-fix / abort-on-first-error server would stop at
        // row 1 — the row-count vector and the SECOND error prove it did not.
        let row_counts = [1u64, 0, 1, 0, 1];
        let errors: [(u32, u32, &str); 2] = [
            (1, 1, "ORA-00001: unique constraint (X.PK) violated"),
            (
                1400,
                3,
                "ORA-01400: cannot insert NULL into (\"X\".\"T\".\"C\")",
            ),
        ];

        let mut writer = TtcWriter::new();
        writer.write_u8(TNS_MSG_TYPE_PARAMETER);
        write_return_parameters_with_row_counts(&mut writer, &row_counts);
        writer.write_u8(TNS_MSG_TYPE_ERROR);
        write_batch_error_info(&mut writer, &errors);
        let payload = writer.into_bytes();

        let options = ExecuteOptions::default().with_arraydmlrowcounts(true);
        let result = parse_query_response_with_binds_and_options(
            &payload,
            ClientCapabilities::default(),
            &[],
            options,
        )
        .expect("batch-error execute response decodes");

        // Continuation: every iteration is accounted for — the batch was NOT
        // aborted on the first failing row.
        assert_eq!(
            result.array_dml_row_counts.as_deref(),
            Some(row_counts.as_slice()),
            "per-iteration counts survive: committed rows report 1, failed rows 0"
        );

        // Per-row error map: each failure is keyed to its input-row offset, with
        // its own ORA code and message (not coalesced, not misordered).
        assert_eq!(
            result.batch_errors.len(),
            2,
            "both failing rows are reported, proving continuation past row 1"
        );
        assert_eq!(result.batch_errors[0].code(), 1);
        assert_eq!(result.batch_errors[0].offset(), 1);
        assert_eq!(
            result.batch_errors[0].message(),
            "ORA-00001: unique constraint (X.PK) violated"
        );
        assert_eq!(result.batch_errors[1].code(), 1400);
        assert_eq!(result.batch_errors[1].offset(), 3);
        assert_eq!(
            result.batch_errors[1].message(),
            "ORA-01400: cannot insert NULL into (\"X\".\"T\".\"C\")"
        );

        // The surviving (non-failing) rows still commit-count: 3 rows affected.
        let committed: u64 = result
            .array_dml_row_counts
            .as_deref()
            .expect("row counts present")
            .iter()
            .sum();
        assert_eq!(committed, 3, "the 3 non-failing rows committed");
    }
}

#[cfg(test)]
mod borrowed_fetch_tests {
    use super::*;
    use crate::thin::codecs::encode_number_text;

    // Isomorphism proof for the `simd-decode` feature (bead rust-oracledb-63o):
    // `validate_utf8` must make the SAME accept/reject decision as the canonical
    // `core::str::from_utf8`, AND return the same `&str` on accept, for every
    // input — whether or not the SIMD validator is compiled in. This guards the
    // hot text path against any divergence in UTF-8 grammar handling.
    #[test]
    fn validate_utf8_matches_core_accept_reject() {
        let cases: &[&[u8]] = &[
            b"",
            b"a",
            b"hello world",
            "VARCHAR2 cell".as_bytes(),
            "中文 mixed \u{1f600}".as_bytes(), // CJK + emoji (4-byte)
            "naïve café".as_bytes(),
            &[0x80],                   // lone continuation byte -> reject
            &[0xC0, 0x80],             // overlong NUL -> reject
            &[0xED, 0xA0, 0x80],       // UTF-16 surrogate -> reject
            &[0xF4, 0x90, 0x80, 0x80], // > U+10FFFF -> reject
            &[0xFF],                   // invalid lead -> reject
            &[0xE2, 0x82],             // truncated 3-byte -> reject
        ];
        for &bytes in cases {
            let core = core::str::from_utf8(bytes);
            let ours = validate_utf8(bytes);
            assert_eq!(
                core.is_ok(),
                ours.is_ok(),
                "accept/reject diverged for {bytes:02x?}"
            );
            if let (Ok(a), Ok(b)) = (core, ours) {
                assert_eq!(a, b, "accepted text diverged for {bytes:02x?}");
            }
        }
    }

    // Build a synthetic column metadata for a scalar type.
    fn col(name: &str, ora_type_num: u8, csfrm: u8, buffer_size: u32) -> ColumnMetadata {
        ColumnMetadata {
            name: name.to_string(),
            ora_type_num,
            csfrm,
            buffer_size,
            ..ColumnMetadata::default()
        }
    }

    // Encode one row of [Text, Number, Raw, NULL-text] as the server would frame
    // the column values (each a `write_bytes_with_length` run that `read_bytes`
    // / `read_bytes_borrowed` consume identically), and return the byte offset
    // where the row's column values begin.
    fn encode_mixed_row(writer: &mut TtcWriter, text: &str, number: &str, raw: &[u8]) {
        writer.write_bytes_with_length(text.as_bytes()).unwrap();
        let num = encode_number_text(number).unwrap();
        writer.write_bytes_with_length(&num).unwrap();
        writer.write_bytes_with_length(raw).unwrap();
        writer.write_u8(0); // NULL column (length byte 0)
    }

    // The borrowed batch decode must yield, for every cell, a value whose
    // `to_owned_value()` is bit-for-bit the owned-path `QueryValue`, across a
    // mixed Text/Number/Raw/NULL row. And the Text/Raw cells must genuinely
    // borrow the batch buffer (zero-copy), not a fresh allocation.
    #[test]
    fn borrowed_batch_matches_owned_path_for_mixed_row() {
        let columns = vec![
            col("T", ORA_TYPE_NUM_VARCHAR, CS_FORM_IMPLICIT, 4000),
            col("N", ORA_TYPE_NUM_NUMBER, CS_FORM_IMPLICIT, 22),
            col("R", ORA_TYPE_NUM_RAW, CS_FORM_IMPLICIT, 2000),
            col("Z", ORA_TYPE_NUM_VARCHAR, CS_FORM_IMPLICIT, 4000),
        ];

        let mut writer = TtcWriter::new();
        encode_mixed_row(
            &mut writer,
            "héllo world",
            "-12.5",
            &[0xDE, 0xAD, 0xBE, 0xEF],
        );
        encode_mixed_row(&mut writer, "second", "42", &[0x01]);
        let buffer = writer.into_bytes();
        let row_starts = vec![0, {
            // Find the second row's start by replaying the first row's consumption.
            let mut reader = TtcReader::new(&buffer);
            for c in &columns {
                let _ = parse_column_value(&mut reader, c).unwrap();
            }
            reader.position()
        }];

        // Owned path: decode both rows the existing way for the golden values.
        let owned_rows: Vec<Vec<Option<QueryValue>>> = row_starts
            .iter()
            .map(|&start| {
                let mut reader = TtcReader::new(&buffer[start..]);
                columns
                    .iter()
                    .map(|c| parse_column_value(&mut reader, c).unwrap())
                    .collect()
            })
            .collect();

        // Borrowed path: decode through the batch, collecting owned copies and
        // proving the scalar cells borrow the buffer.
        let batch = BorrowedRowBatch::new(buffer.clone(), columns.clone(), row_starts);
        let buf_ptr_range = batch.buffer_ptr_range();

        let mut seen_rows = 0usize;
        let mut borrowed_owned: Vec<Vec<Option<QueryValue>>> = Vec::new();
        batch
            .for_each_row_ref(|row| {
                seen_rows += 1;
                // Text cell borrows the buffer.
                if let Some(QueryValueRef::Text(t)) = row[0] {
                    let p = t.as_ptr() as usize;
                    assert!(
                        buf_ptr_range.contains(&p),
                        "Text cell must borrow the batch buffer (zero-copy)"
                    );
                }
                // Raw cell borrows the buffer.
                if let Some(QueryValueRef::Raw(r)) = row[2] {
                    let p = r.as_ptr() as usize;
                    assert!(
                        buf_ptr_range.contains(&p),
                        "Raw cell must borrow the batch buffer (zero-copy)"
                    );
                }
                borrowed_owned.push(
                    row.iter()
                        .map(|cell| cell.map(|v| v.to_owned_value()))
                        .collect(),
                );
                Ok::<(), ProtocolError>(())
            })
            .unwrap();

        assert_eq!(seen_rows, 2, "batch yields both rows");
        assert_eq!(
            borrowed_owned, owned_rows,
            "borrowed cells to_owned() must equal the owned-path values"
        );
    }

    #[test]
    fn borrowed_number_to_owned_matches_owned_for_trailing_zero_number() {
        let column = col("N", ORA_TYPE_NUM_NUMBER, CS_FORM_IMPLICIT, 22);
        let number = encode_number_text("1000").expect("encode trailing-zero number");
        let mut writer = TtcWriter::new();
        writer
            .write_bytes_with_length(&number)
            .expect("write framed number");
        let buffer = writer.into_bytes();

        let mut owned_reader = TtcReader::new(&buffer);
        let owned = parse_column_value(&mut owned_reader, &column)
            .expect("owned decode")
            .expect("owned number should be non-null");

        let batch = BorrowedRowBatch::new(buffer, vec![column], vec![0]);
        let mut borrowed_owned = Vec::new();
        batch
            .for_each_row_ref(|row| {
                borrowed_owned.push(
                    row[0]
                        .expect("borrowed number should be non-null")
                        .to_owned_value(),
                );
                Ok::<(), ProtocolError>(())
            })
            .expect("borrowed decode");
        let borrowed = borrowed_owned
            .pop()
            .expect("borrowed decode should yield one row");

        assert_eq!(
            borrowed.as_number_text().as_deref(),
            owned.as_number_text().as_deref(),
            "borrowed and owned paths must expose identical canonical NUMBER text"
        );
        assert_eq!(
            borrowed, owned,
            "borrowed to_owned_value should materialize the same trailing-zero NUMBER"
        );
    }

    #[test]
    fn describe_size_zero_urowid_decodes_and_preserves_following_column_alignment() {
        let columns = vec![
            col("RID", ORA_TYPE_NUM_UROWID, CS_FORM_IMPLICIT, 0),
            col("NEXT", ORA_TYPE_NUM_VARCHAR, CS_FORM_IMPLICIT, 4000),
        ];
        let rba: u32 = 0x0102_0304;
        let partition_id: u16 = 0x0506;
        let block_num: u32 = 0x0708_090a;
        let slot_num: u16 = 0x0b0c;
        let mut encoded_urowid = Vec::new();
        encoded_urowid.push(1);
        encoded_urowid.extend_from_slice(&rba.to_be_bytes());
        encoded_urowid.extend_from_slice(&partition_id.to_be_bytes());
        encoded_urowid.extend_from_slice(&block_num.to_be_bytes());
        encoded_urowid.extend_from_slice(&slot_num.to_be_bytes());
        let expected_rowid = encode_physical_rowid(rba, partition_id, block_num, slot_num);

        let mut writer = TtcWriter::new();
        writer
            .write_bytes_with_length(&[1])
            .expect("write UROWID null probe field");
        writer
            .write_bytes_with_length(&encoded_urowid)
            .expect("write encoded UROWID");
        writer
            .write_bytes_with_length(b"after")
            .expect("write following text column");
        let buffer = writer.into_bytes();

        let mut owned_reader = TtcReader::new(&buffer);
        let owned = columns
            .iter()
            .map(|column| parse_column_value(&mut owned_reader, column).expect("owned decode"))
            .collect::<Vec<_>>();
        assert_eq!(
            owned[0].as_ref().and_then(QueryValue::as_rowid),
            Some(expected_rowid.as_str()),
            "owned decode must not NULL a describe-size-0 UROWID"
        );
        assert_eq!(
            owned[1].as_ref().and_then(QueryValue::as_text),
            Some("after"),
            "owned decode must consume UROWID bytes before the following column"
        );

        let batch = BorrowedRowBatch::new(buffer, columns, vec![0]);
        let mut borrowed_rows = Vec::new();
        batch
            .for_each_row_ref(|row| {
                borrowed_rows.push(
                    row.iter()
                        .map(|cell| cell.map(|value| value.to_owned_value()))
                        .collect::<Vec<_>>(),
                );
                Ok::<(), ProtocolError>(())
            })
            .expect("borrowed decode");
        assert_eq!(borrowed_rows, vec![owned]);
    }

    // The borrowed response parser walks the *same* message framing as the owned
    // `parse_fetch_response_with_context` (ROW_HEADER / BIT_VECTOR / ROW_DATA /
    // END_OF_RESPONSE), but instead of building owned rows it captures each
    // row's byte offset and hands back a `BorrowedRowBatch`. Decoding that batch
    // must reproduce exactly what the owned fetch path produced — duplicate
    // columns (bit vector) and all. Fixture is the same one the owned
    // `fetch_response_decodes_rows_with_previous_cursor_metadata` test uses.
    #[test]
    fn borrowed_response_parse_matches_owned_fetch_path() {
        use hex::FromHex;
        let payload = Vec::from_hex("06020101000205dc0001010101000702c1041d")
            .expect("fixture response should be valid hex");
        let columns = vec![
            col("INTCOL", ORA_TYPE_NUM_NUMBER, CS_FORM_IMPLICIT, 22),
            col("NUMBERCOL", ORA_TYPE_NUM_NUMBER, CS_FORM_IMPLICIT, 22),
        ];
        let previous_row = vec![
            Some(QueryValue::number_from_text("2", true)),
            Some(QueryValue::number_from_text("0.5", false)),
        ];

        // Owned golden.
        let owned = parse_query_response_with_context(
            &payload,
            ClientCapabilities::default(),
            &columns,
            Some(&previous_row),
        )
        .expect("owned fetch decode");

        // Borrowed parse.
        let borrowed = parse_query_response_borrowed(
            &payload,
            ClientCapabilities::default(),
            &columns,
            Some(&previous_row),
        )
        .expect("borrowed fetch decode");

        assert_eq!(borrowed.more_rows, owned.more_rows);
        assert_eq!(borrowed.cursor_id, owned.cursor_id);
        assert_eq!(borrowed.batch.row_count(), owned.rows.len());

        let mut borrowed_owned: Vec<Vec<Option<QueryValue>>> = Vec::new();
        borrowed
            .batch
            .for_each_row_ref(|row| {
                borrowed_owned.push(
                    row.iter()
                        .map(|cell| cell.map(|v| v.to_owned_value()))
                        .collect(),
                );
                Ok::<(), ProtocolError>(())
            })
            .expect("iterate borrowed rows");

        assert_eq!(
            borrowed_owned, owned.rows,
            "borrowed batch must reproduce the owned fetch rows (incl. duplicate columns)"
        );
    }
}

#[cfg(test)]
mod out_bind_boolean_regression_tests {
    use super::*;

    fn boolean_column(is_array: bool) -> ColumnMetadata {
        ColumnMetadata {
            name: "B".to_string(),
            ora_type_num: ORA_TYPE_NUM_BOOLEAN,
            is_array,
            ..ColumnMetadata::default()
        }
    }

    fn boolean_value_with_negative_actual_bytes() -> Vec<u8> {
        let mut writer = TtcWriter::new();
        writer
            .write_bytes_with_length(&[1])
            .expect("write present boolean value");
        writer.write_sb4(-1);
        writer.into_bytes()
    }

    #[test]
    fn scalar_boolean_out_bind_negative_actual_bytes_decodes_null() {
        let bind_columns = [boolean_column(false)];
        let out_bind_indexes = [0usize];
        let payload = boolean_value_with_negative_actual_bytes();
        let mut reader = TtcReader::new(&payload);
        let mut result = QueryResult::default();

        parse_out_bind_row_data(&mut reader, &mut result, &bind_columns, &out_bind_indexes)
            .expect("parse scalar BOOLEAN OUT bind");

        assert_eq!(result.out_values, vec![(0, None)]);
    }

    #[test]
    fn array_boolean_out_bind_negative_actual_bytes_decodes_null_element() {
        let bind_columns = [boolean_column(true)];
        let out_bind_indexes = [0usize];
        let mut writer = TtcWriter::new();
        writer.write_ub4(1);
        writer
            .write_bytes_with_length(&[1])
            .expect("write present boolean array element");
        writer.write_sb4(-1);
        let payload = writer.into_bytes();
        let mut reader = TtcReader::new(&payload);
        let mut result = QueryResult::default();

        parse_out_bind_row_data(&mut reader, &mut result, &bind_columns, &out_bind_indexes)
            .expect("parse array BOOLEAN OUT bind");

        assert_eq!(
            result.out_values,
            vec![(0, Some(QueryValue::Array(vec![None])))]
        );
    }

    #[test]
    fn returning_boolean_negative_actual_bytes_decodes_null() {
        let bind_columns = [boolean_column(false)];
        let output_bind_indexes = [0usize];
        let mut writer = TtcWriter::new();
        writer.write_ub4(1);
        writer
            .write_bytes_with_length(&[1])
            .expect("write present returning boolean value");
        writer.write_sb4(-1);
        let payload = writer.into_bytes();
        let mut reader = TtcReader::new(&payload);
        let mut result = QueryResult::default();

        parse_returning_row_data(
            &mut reader,
            &mut result,
            &bind_columns,
            &output_bind_indexes,
        )
        .expect("parse BOOLEAN RETURNING value");

        assert_eq!(result.return_values, vec![(0, vec![None])]);
    }
}

#[cfg(test)]
mod in_out_io_vector_tests {
    use super::*;

    // The three TNS bind directions the server tags each bind with in the IO
    // vector (reference thin_impl.c: OUTPUT=16, INPUT=32, INPUT_OUTPUT=48). The
    // client emits no direction on the wire — it reads back every bind the server
    // flags as not pure INPUT, i.e. OUT (16) or IN OUT (48). These locals pin the
    // exact wire values this test exercises.
    const TNS_BIND_DIR_OUTPUT: u8 = 16;
    const TNS_BIND_DIR_INPUT_OUTPUT: u8 = 48;

    // Layout consumed by `parse_io_vector`: flags(u8), num_binds low(ub2) /
    // high(ub4), num_iters(ub4), uac_buffer_length(ub2), fast_fetch_len(ub2),
    // rowid_len(ub2), then one direction byte per bind.
    fn io_vector_payload(directions: &[u8]) -> Vec<u8> {
        io_vector_payload_with_skips(directions, &[], &[])
    }

    fn io_vector_payload_with_skips(
        directions: &[u8],
        fast_fetch_bytes: &[u8],
        rowid_bytes: &[u8],
    ) -> Vec<u8> {
        let mut writer = TtcWriter::new();
        writer.write_u8(0);
        writer.write_ub2(u16::try_from(directions.len()).unwrap());
        writer.write_ub4(0);
        writer.write_ub4(1);
        writer.write_ub2(0);
        writer.write_ub2(u16::try_from(fast_fetch_bytes.len()).unwrap());
        writer.write_raw(fast_fetch_bytes);
        writer.write_ub2(u16::try_from(rowid_bytes.len()).unwrap());
        writer.write_raw(rowid_bytes);
        for &direction in directions {
            writer.write_u8(direction);
        }
        writer.into_bytes()
    }

    #[test]
    fn in_out_and_out_directions_read_back_pure_input_does_not() {
        // bind 0 = plain IN (send-only), bind 1 = IN OUT, bind 2 = OUT.
        let payload = io_vector_payload(&[
            TNS_BIND_DIR_INPUT,
            TNS_BIND_DIR_INPUT_OUTPUT,
            TNS_BIND_DIR_OUTPUT,
        ]);
        let mut reader = TtcReader::new(&payload);
        let out_indexes = parse_io_vector(&mut reader, 3).expect("parse io vector");
        assert_eq!(
            out_indexes,
            vec![1, 2],
            "IN OUT (48) and OUT (16) are read back; pure IN (32) is not"
        );
    }

    #[test]
    fn io_vector_skips_optional_payloads_and_ignores_unbound_slots() {
        let payload = io_vector_payload_with_skips(
            &[
                TNS_BIND_DIR_OUTPUT,
                TNS_BIND_DIR_INPUT,
                TNS_BIND_DIR_INPUT_OUTPUT,
            ],
            b"ff",
            b"rid",
        );
        let mut reader = TtcReader::new(&payload);
        let out_indexes = parse_io_vector(&mut reader, 2).expect("parse io vector with skips");

        assert_eq!(
            out_indexes,
            vec![0],
            "optional fast-fetch/rowid payloads are skipped, and directions beyond bind_count are ignored"
        );
        assert_eq!(reader.remaining(), 0);
    }

    // End-to-end read-back: a VARCHAR IN OUT bind whose routine writes back a
    // value longer than the input. The returned bytes decode against the IN OUT
    // bind's own metadata (buffer sized to 200, well beyond the 2-char input), so
    // the longer value survives.
    #[test]
    fn in_out_varchar_reads_back_value_larger_than_input() {
        let inout = BindValue::InOut {
            value: Box::new(BindValue::Text("ab".to_string())),
            out_buffer_size: 200,
        };
        let bind_columns = [bind_column_metadata(&inout)];
        let out_bind_indexes = [0usize];

        // Server OUT-slot response: the routine-modified value "ABCD", then the
        // zero "actual bytes" trailer (present, not truncated).
        let mut writer = TtcWriter::new();
        writer
            .write_bytes_with_length(b"ABCD")
            .expect("write returned varchar");
        writer.write_sb4(0);
        let payload = writer.into_bytes();

        let mut reader = TtcReader::new(&payload);
        let mut result = QueryResult::default();
        parse_out_bind_row_data(&mut reader, &mut result, &bind_columns, &out_bind_indexes)
            .expect("parse IN OUT VARCHAR read-back");

        assert_eq!(
            result.out_values,
            vec![(0, Some(QueryValue::Text("ABCD".to_string())))],
            "the routine-modified IN OUT value is read back against its bind metadata"
        );
    }
}

#[cfg(test)]
mod fuzz_regression_tests {
    use super::*;

    // Regression (w6-fuzz, query_response target): a TNS_MSG_TYPE_IMPLICIT_RESULTSET
    // message (27) whose ub4 result count was ~620M made the dispatch loop
    // `Vec::with_capacity` several gigabytes of `QueryValue::Cursor` before the
    // truncated read failed, tripping libFuzzer's OOM detector. The parser must
    // now fail closed (truncated payload) without the giant allocation.
    #[test]
    fn fuzz_regression_implicit_resultset_oom() {
        // payload: type=27, ub4 length byte 4, value 0x25000000 (~620M), then EOF
        let payload = [27u8, 4, 37, 0, 0, 0];
        let err = parse_query_response(&payload, ClientCapabilities::default())
            .expect_err("oversized implicit-resultset count must fail closed");
        assert!(
            matches!(
                err,
                ProtocolError::TtcDecode(_) | ProtocolError::ResourceLimit { .. }
            ),
            "expected fail-closed protocol error, got {err:?}"
        );
    }

    // BoundedReader invariant (l2p), query-columns family: a DESCRIBE_INFO
    // message (16) declaring a huge num_columns (ub4 ~620M) with no column
    // metadata bytes following must fail closed, not pre-allocate one
    // ColumnMetadata per declared column. parse_describe_info grows the column
    // Vec via push (no speculative with_capacity), and the first
    // parse_column_metadata read past the end errors.
    #[test]
    fn describe_info_oversized_column_count_fails_closed_not_oom() {
        // type=16 DESCRIBE_INFO; describe_name read_bytes len byte 0 (null);
        // max_row_size ub4 = 0; num_columns ub4 (len byte 4) = 0x25000000
        // (~620M); then EOF before the skip(1)/column records.
        let payload = [16u8, 0, 0, 4, 0x25, 0x00, 0x00, 0x00];
        let err = parse_query_response(&payload, ClientCapabilities::default())
            .expect_err("oversized column count must fail closed");
        assert!(
            matches!(
                err,
                ProtocolError::TtcDecode(_) | ProtocolError::ResourceLimit { .. }
            ),
            "expected fail-closed protocol error, got {err:?}"
        );
    }

    #[test]
    fn describe_info_respects_protocol_column_limit() {
        // type=16 DESCRIBE_INFO; describe_name null; max_row_size=0;
        // num_columns=2. A max_columns=1 policy should fail before any column
        // metadata allocation/parsing.
        let payload = [TNS_MSG_TYPE_DESCRIBE_INFO, 0, 0, 1, 2];
        let limits = ProtocolLimits {
            max_columns: 1,
            ..ProtocolLimits::DEFAULT
        };
        let err = parse_query_response_with_limits(&payload, ClientCapabilities::default(), limits)
            .expect_err("column count above policy must fail");
        assert!(
            matches!(
                err,
                ProtocolError::ResourceLimit {
                    limit: "columns",
                    observed: 2,
                    maximum: 1,
                }
            ),
            "expected column ResourceLimit, got {err:?}"
        );
    }

    // BoundedReader invariant (l2p), out-bind array family: an array OUT bind
    // whose ub4 num_elements is enormous (~620M) but carries no element bytes
    // must fail closed via with_capacity_bounded + the per-element read, not
    // reserve gigabytes of Option<QueryValue>.
    #[test]
    fn out_bind_array_oversized_element_count_fails_closed_not_oom() {
        let metadata = ColumnMetadata {
            name: "ARR".to_string(),
            ora_type_num: ORA_TYPE_NUM_NUMBER,
            is_array: true,
            ..ColumnMetadata::default()
        };
        let bind_columns = [metadata];
        let out_bind_indexes = [0usize];
        // ub4 num_elements: len byte 4, value 0x25000000, then no elements.
        let payload = [4u8, 0x25, 0x00, 0x00, 0x00];
        let mut reader = TtcReader::new(&payload);
        let mut result = QueryResult::default();
        let err =
            parse_out_bind_row_data(&mut reader, &mut result, &bind_columns, &out_bind_indexes)
                .expect_err("oversized array OUT bind count must fail closed");
        assert!(
            matches!(
                err,
                ProtocolError::TtcDecode(_) | ProtocolError::ResourceLimit { .. }
            ),
            "expected fail-closed protocol error, got {err:?}"
        );
    }

    // ---- describe-read version-gate boundary tests -------------------------
    //
    // Reference messages/base.pyx `_process_column_metadata` gates four fields
    // on the negotiated ttc field version:
    //   :346  >= 12.2        ub4 oaccolid                (below the 18c floor)
    //   :358  >= 23.1        domain schema + name
    //   :361  >= 23.1 ext 3  annotations block
    //   :376  >= 23.4        vector dimensions/format/flags
    // parse_column_metadata mirrors each. The 12.2 boundary is below our live
    // floor (18c field version 11), so no live server exercises the pre-12.2
    // read path; this offline test pins every boundary by proving the parser
    // consumes *exactly* the bytes for its field version and misaligns (fails
    // closed, or leaves the wire unconsumed) when parsed one version off.

    fn describe_caps(fv: u8) -> ClientCapabilities {
        ClientCapabilities {
            ttc_field_version: fv,
            max_string_size: 32_767,
            charset_id: 873,
        }
    }

    /// One column-metadata record whose optional fields are present iff the
    /// field version gates them in — i.e. exactly what a server at `fv` sends.
    fn describe_column_bytes(fv: u8) -> Vec<u8> {
        let mut w = TtcWriter::new();
        w.write_u8(ORA_TYPE_NUM_VARCHAR);
        w.write_u8(0); // flags
        w.write_u8(0); // precision
        w.write_u8(0); // scale
        w.write_ub4(4000); // buffer size
        w.write_ub4(0); // max array elements
        w.write_ub8(0); // cont flags
        w.write_bytes_with_two_lengths(None).expect("oid");
        w.write_ub2(0); // version
        w.write_ub2(0); // server charset id (ignored)
        w.write_u8(CS_FORM_IMPLICIT);
        w.write_ub4(4000); // max size
        if fv >= TNS_CCAP_FIELD_VERSION_12_2 {
            w.write_ub4(0x1122_3344); // oaccolid (5 wire bytes when present)
        }
        w.write_u8(1); // nullable
        w.write_u8(0); // flags
        w.write_bytes_with_two_lengths(Some(b"TXT")).expect("name");
        w.write_bytes_with_two_lengths(None).expect("object schema");
        w.write_bytes_with_two_lengths(None).expect("object type");
        w.write_ub2(1); // column position
        w.write_ub4(0); // uds flags
        if fv >= TNS_CCAP_FIELD_VERSION_23_1 {
            w.write_bytes_with_two_lengths(None).expect("domain schema");
            w.write_bytes_with_two_lengths(None).expect("domain name");
        }
        if fv >= TNS_CCAP_FIELD_VERSION_23_1_EXT_3 {
            w.write_ub4(0); // num annotations (0 => no sub-block)
        }
        if fv >= TNS_CCAP_FIELD_VERSION_23_4 {
            w.write_ub4(2); // vector dimensions
            w.write_u8(0); // format
            w.write_u8(0); // flags
        }
        w.into_bytes()
    }

    /// `Some((column_name, remaining_bytes))` after a successful parse, else
    /// `None` (failed closed). A version-matched parse decodes the name and
    /// consumes the record exactly => `Some(("TXT", 0))`; any misaligned read
    /// diverges (different name, leftover bytes, or a hard failure).
    fn parse_outcome(bytes: &[u8], fv: u8) -> Option<(String, usize)> {
        let mut reader = TtcReader::new(bytes);
        parse_column_metadata(&mut reader, describe_caps(fv))
            .ok()
            .map(|meta| (meta.name().to_string(), reader.remaining()))
    }

    #[test]
    fn describe_column_metadata_gates_fields_on_field_version() {
        // Each (lo, hi) pair straddles exactly one gate; the other three gates
        // are on the same side of the boundary for both, so only one field moves.
        let boundaries = [
            (TNS_CCAP_FIELD_VERSION_12_2 - 1, TNS_CCAP_FIELD_VERSION_12_2), // oaccolid
            (TNS_CCAP_FIELD_VERSION_23_1 - 1, TNS_CCAP_FIELD_VERSION_23_1), // domain
            (
                TNS_CCAP_FIELD_VERSION_23_1_EXT_3 - 1,
                TNS_CCAP_FIELD_VERSION_23_1_EXT_3,
            ), // annotations
            (TNS_CCAP_FIELD_VERSION_23_4 - 1, TNS_CCAP_FIELD_VERSION_23_4), // vector
        ];
        for (lo, hi) in boundaries {
            let lo_bytes = describe_column_bytes(lo);
            let hi_bytes = describe_column_bytes(hi);
            assert!(
                hi_bytes.len() > lo_bytes.len(),
                "fv {hi}: the gated field must add bytes vs fv {lo}"
            );

            // Version-matched parses decode the name and consume exactly.
            let matched = Some(("TXT".to_string(), 0));
            assert_eq!(parse_outcome(&lo_bytes, lo), matched, "fv {lo} matched");
            assert_eq!(parse_outcome(&hi_bytes, hi), matched, "fv {hi} matched");

            // Parsed one version off, the record misaligns: reading `hi` bytes
            // as `lo` skips the gated field (leftover bytes remain), and reading
            // `lo` bytes as `hi` over-reads (fails closed). Neither matches.
            assert_ne!(
                parse_outcome(&hi_bytes, lo),
                matched,
                "fv {hi} bytes read as fv {lo} must not consume cleanly"
            );
            assert_ne!(
                parse_outcome(&lo_bytes, hi),
                matched,
                "fv {lo} bytes read as fv {hi} must not consume cleanly"
            );
        }
    }
}
