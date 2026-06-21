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

pub fn build_fetch_payload(cursor_id: u32, arraysize: u32) -> Vec<u8> {
    build_fetch_payload_with_seq(cursor_id, arraysize, 1)
}

pub fn build_fetch_payload_with_seq(cursor_id: u32, arraysize: u32, seq_num: u8) -> Vec<u8> {
    // Fixed tiny payload (function code + ub8 + two ub4 ≈ <=20 bytes). Prealloc
    // so the small pushes do not grow the Vec through doublings; built every
    // fetch page, so this matters on multi-page fetches. Bytes unchanged.
    let mut writer = TtcWriter::with_capacity(32);
    writer.write_function_code_with_seq(TNS_FUNC_FETCH, seq_num);
    writer.write_ub8(0);
    writer.write_ub4(cursor_id);
    writer.write_ub4(arraysize);
    writer.into_bytes()
}

pub fn build_define_fetch_payload_with_seq(
    cursor_id: u32,
    arraysize: u32,
    seq_num: u8,
    define_columns: &[ColumnMetadata],
) -> Result<Vec<u8>> {
    let define_count =
        u32::try_from(define_columns.len()).map_err(|_| ProtocolError::InvalidPacketLength {
            length: define_columns.len(),
            minimum: 0,
        })?;
    let mut writer = TtcWriter::new();
    writer.write_function_code_with_seq(TNS_FUNC_EXECUTE, seq_num);
    writer.write_ub8(0);
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
                        adjust_refetch_metadata(prev, column);
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
    if capabilities.ttc_field_version >= TNS_CCAP_FIELD_VERSION_12_2 {
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
    if capabilities.ttc_field_version >= TNS_CCAP_FIELD_VERSION_23_1 {
        domain_schema = reader.read_string_with_length()?;
        domain_name = reader.read_string_with_length()?;
    }
    if capabilities.ttc_field_version >= TNS_CCAP_FIELD_VERSION_23_1_EXT_3 {
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
    if capabilities.ttc_field_version >= TNS_CCAP_FIELD_VERSION_23_4 {
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
                if actual_num_bytes != 0 && value.is_some() {
                    return Err(ProtocolError::TtcDecode("truncated array OUT bind value"));
                }
                values.push(value);
            }
            result
                .out_values
                .push((*index, Some(QueryValue::Array(values))));
            continue;
        }
        let value = parse_column_value(reader, metadata)?;
        let actual_num_bytes = reader.read_sb4()?;
        if actual_num_bytes != 0 && value.is_some() {
            return Err(ProtocolError::TtcDecode("truncated OUT bind value"));
        }
        result.out_values.push((*index, value));
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
            if actual_num_bytes != 0 && value.is_some() {
                return Err(ProtocolError::TtcDecode("truncated DML RETURNING value"));
            }
            values.push(value);
        }
        result.return_values.push((*index, values));
    }
    Ok(())
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
            ORA_TYPE_NUM_LONG | ORA_TYPE_NUM_LONG_RAW
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
            ORA_TYPE_NUM_LONG | ORA_TYPE_NUM_LONG_RAW
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
                    LobDecodeMode::PlainLocator,
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
        lob_decode_mode: LobDecodeMode::PlainLocator,
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

    fn first_lob(result: &QueryResult) -> &LobValue {
        match &result.rows[0][0] {
            Some(QueryValue::Lob(lob)) => lob.as_ref(),
            other => panic!("expected LOB value, got {other:?}"),
        }
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

        let lob = first_lob(&result);
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

        let lob = first_lob(&result);
        assert_eq!(lob.locator, locator);
        assert_eq!(lob.size, 23);
        assert_eq!(lob.chunk_size, 8060);
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
}
