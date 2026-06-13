#![forbid(unsafe_code)]

use super::*;

pub fn build_fetch_payload(cursor_id: u32, arraysize: u32) -> Vec<u8> {
    build_fetch_payload_with_seq(cursor_id, arraysize, 1)
}

pub fn build_fetch_payload_with_seq(cursor_id: u32, arraysize: u32, seq_num: u8) -> Vec<u8> {
    let mut writer = TtcWriter::new();
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
    )
}

pub fn parse_fetch_response_with_context(
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
        true,
    )
}

pub(crate) fn parse_query_response_with_context_and_binds(
    payload: &[u8],
    capabilities: ClientCapabilities,
    previous_columns: &[ColumnMetadata],
    previous_row: Option<&[Option<QueryValue>]>,
    bind_columns: &[ColumnMetadata],
    output_bind_indexes: &[usize],
    fetch_long_status: bool,
) -> Result<QueryResult> {
    parse_query_response_with_context_binds_and_options(
        payload,
        capabilities,
        previous_columns,
        previous_row,
        bind_columns,
        output_bind_indexes,
        fetch_long_status,
        ExecuteOptions::default(),
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
    let mut reader = TtcReader::new(payload);
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
                    )?;
                }
                bit_vector = None;
            }
            TNS_MSG_TYPE_BIT_VECTOR => {
                bit_vector = Some(parse_bit_vector(&mut reader, result.columns.len())?);
            }
            TNS_MSG_TYPE_PARAMETER => {
                let row_counts =
                    parse_query_return_parameters(&mut reader, exec_options.arraydmlrowcounts)?;
                if exec_options.arraydmlrowcounts {
                    result.array_dml_row_counts = Some(row_counts.unwrap_or_default());
                }
            }
            TNS_MSG_TYPE_STATUS => {
                let _call_status = reader.read_ub4()?;
                let _seq = reader.read_ub2()?;
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
                let mut resultsets = Vec::with_capacity(num_results as usize);
                for _ in 0..num_results {
                    let num_bytes = reader.read_u8()?;
                    reader.skip(usize::from(num_bytes))?;
                    let mut child = QueryResult::default();
                    parse_describe_info(&mut reader, capabilities, &mut child)?;
                    let child_cursor_id = u32::from(reader.read_ub2()?);
                    resultsets.push(QueryValue::Cursor {
                        columns: child.columns,
                        cursor_id: child_cursor_id,
                    });
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
            let mut collected = Vec::with_capacity(num_annotations as usize);
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
        row.push(parse_column_value(reader, metadata)?);
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
            let mut values = Vec::with_capacity(num_elements);
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
        let mut values = Vec::with_capacity(num_rows);
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
            parse_lob_value(reader, metadata)
        }
        ORA_TYPE_NUM_VECTOR => parse_vector_value(reader),
        ORA_TYPE_NUM_JSON => parse_json_value(reader),
        ORA_TYPE_NUM_CURSOR => parse_cursor_value(reader).map(Some),
        ORA_TYPE_NUM_OBJECT => parse_object_value(reader, metadata),
        _ => Err(ProtocolError::UnsupportedFeature("query column type")),
    }
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
) -> Result<Option<QueryValue>> {
    let num_bytes = reader.read_ub4()?;
    if num_bytes == 0 {
        return Ok(None);
    }
    let (size, chunk_size) = if matches!(metadata.ora_type_num, ORA_TYPE_NUM_BFILE) {
        (0, 0)
    } else {
        (reader.read_ub8()?, reader.read_ub4()?)
    };
    let Some(locator) = reader.read_bytes()? else {
        return Ok(None);
    };
    Ok(Some(QueryValue::Lob {
        ora_type_num: metadata.ora_type_num,
        csfrm: metadata.csfrm,
        locator,
        size,
        chunk_size,
    }))
}

/// Reads a VECTOR value (reference `ReadBuffer.read_vector` in `packet.pyx`).
/// VECTOR is sent as a fully-prefetched LOB: the image data precedes the
/// (discarded) LOB locator.
pub(crate) fn parse_vector_value(reader: &mut TtcReader<'_>) -> Result<Option<QueryValue>> {
    let num_bytes = reader.read_ub4()?;
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
    let vector = crate::vector::decode_vector(&data)?;
    Ok(Some(QueryValue::Vector(vector)))
}

/// Parses a native JSON (`DB_TYPE_JSON`) column value. Like VECTOR, OSON is sent
/// as a fully-prefetched LOB: `num_bytes`, `size`, `chunk_size`, the OSON image,
/// then a (discarded) LOB locator (reference packet.pyx `read_oson`).
pub(crate) fn parse_json_value(reader: &mut TtcReader<'_>) -> Result<Option<QueryValue>> {
    let num_bytes = reader.read_ub4()?;
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
    let value = crate::oson::decode_oson(&data)?;
    Ok(Some(QueryValue::Json(value)))
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
    reader.skip(2)?;
    if num_bytes == 0 {
        return Ok(None);
    }
    let Some(packed_data) = reader.read_bytes()? else {
        return Ok(None);
    };
    Ok(Some(QueryValue::Object {
        schema: metadata.object_schema.clone(),
        type_name: metadata.object_type_name.clone(),
        packed_data,
    }))
}

pub(crate) fn parse_cursor_value(reader: &mut TtcReader<'_>) -> Result<QueryValue> {
    reader.skip(1)?;
    let mut result = QueryResult::default();
    parse_describe_info(reader, ClientCapabilities::default(), &mut result)?;
    let cursor_id = u32::from(reader.read_ub2()?);
    Ok(QueryValue::Cursor {
        columns: result.columns,
        cursor_id,
    })
}

pub(crate) fn parse_query_return_parameters(
    reader: &mut TtcReader<'_>,
    arraydmlrowcounts: bool,
) -> Result<Option<Vec<u64>>> {
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
    let num_bytes = reader.read_ub2()?;
    if num_bytes > 0 {
        reader.skip(usize::from(num_bytes))?;
    }
    if arraydmlrowcounts {
        // reference messages/base.pyx `_process_return_parameters` tail
        let num_rows = reader.read_ub4()?;
        let mut row_counts = Vec::with_capacity(num_rows as usize);
        for _ in 0..num_rows {
            row_counts.push(reader.read_ub8()?);
        }
        return Ok(Some(row_counts));
    }
    Ok(None)
}
