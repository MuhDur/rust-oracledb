#![forbid(unsafe_code)]

use super::*;

pub fn build_execute_query_payload(sql: &str, prefetch_rows: u32) -> Result<Vec<u8>> {
    build_execute_query_payload_with_seq(sql, prefetch_rows, 1)
}

pub fn build_execute_query_payload_with_seq(
    sql: &str,
    prefetch_rows: u32,
    seq_num: u8,
) -> Result<Vec<u8>> {
    build_execute_payload_with_seq(sql, prefetch_rows, seq_num, true)
}

pub fn build_execute_payload_with_seq(
    sql: &str,
    prefetch_rows: u32,
    seq_num: u8,
    is_query: bool,
) -> Result<Vec<u8>> {
    build_execute_payload_with_binds_with_seq(sql, prefetch_rows, seq_num, is_query, &[])
}

pub fn build_execute_payload_with_binds_with_seq(
    sql: &str,
    prefetch_rows: u32,
    seq_num: u8,
    is_query: bool,
    binds: &[BindValue],
) -> Result<Vec<u8>> {
    let bind_rows = if binds.is_empty() {
        Vec::new()
    } else {
        vec![binds.to_vec()]
    };
    build_execute_payload_with_bind_rows_with_seq(sql, prefetch_rows, seq_num, is_query, &bind_rows)
}

pub fn build_execute_payload_with_bind_rows_with_seq(
    sql: &str,
    prefetch_rows: u32,
    seq_num: u8,
    is_query: bool,
    bind_rows: &[Vec<BindValue>],
) -> Result<Vec<u8>> {
    build_execute_payload_with_bind_rows_and_options_with_seq(
        sql,
        prefetch_rows,
        seq_num,
        is_query,
        bind_rows,
        ExecuteOptions::default(),
    )
}

/// Execute message with an explicit pipeline token; pipelined operations
/// carry tokens 1..N (impl/thin/connection.pyx `_create_messages_for_pipeline`),
/// everything else carries 0.
pub fn build_execute_payload_with_bind_rows_with_seq_and_token(
    sql: &str,
    prefetch_rows: u32,
    seq_num: u8,
    is_query: bool,
    bind_rows: &[Vec<BindValue>],
    token_num: u64,
) -> Result<Vec<u8>> {
    build_execute_payload_with_bind_rows_and_options_with_seq(
        sql,
        prefetch_rows,
        seq_num,
        is_query,
        bind_rows,
        ExecuteOptions {
            token_num,
            ..ExecuteOptions::default()
        },
    )
}

/// Builds a close-cursors piggyback message (reference
/// `_write_close_cursors_piggyback` + `write_cursors_to_close`); it is
/// prepended to the next regular message in the same data packet and
/// consumes a TTC sequence number of its own.
pub fn build_close_cursors_piggyback(cursor_ids: &[u32], seq_num: u8) -> Vec<u8> {
    let mut writer = TtcWriter::new();
    writer.write_u8(TNS_MSG_TYPE_PIGGYBACK);
    writer.write_u8(TNS_FUNC_CLOSE_CURSORS);
    writer.write_u8(seq_num);
    writer.write_ub8(0); // token number (23.1 ext 1+)
    writer.write_u8(1); // pointer
    writer.write_ub4(u32::try_from(cursor_ids.len()).unwrap_or(u32::MAX));
    for cursor_id in cursor_ids {
        writer.write_ub4(*cursor_id);
    }
    writer.into_bytes()
}

pub fn build_execute_payload_with_bind_rows_and_options_with_seq(
    sql: &str,
    prefetch_rows: u32,
    seq_num: u8,
    is_query: bool,
    bind_rows: &[Vec<BindValue>],
    exec_options: ExecuteOptions,
) -> Result<Vec<u8>> {
    let sql_bytes = sql.as_bytes();
    let sql_len =
        u32::try_from(sql_bytes.len()).map_err(|_| ProtocolError::InvalidPacketLength {
            length: sql_bytes.len(),
            minimum: 0,
        })?;
    let bind_count = bind_rows.first().map_or(0, Vec::len);
    for row in bind_rows {
        if row.len() != bind_count {
            return Err(ProtocolError::TtcDecode("inconsistent bind row width"));
        }
    }
    let bind_count = u32::try_from(bind_count).map_err(|_| ProtocolError::InvalidPacketLength {
        length: bind_count,
        minimum: 0,
    })?;
    let bind_row_count =
        u32::try_from(bind_rows.len()).map_err(|_| ProtocolError::InvalidPacketLength {
            length: bind_rows.len(),
            minimum: 0,
        })?;
    // Preallocate the writer so the small per-field `write_*` pushes do not grow
    // the backing `Vec` through several doublings (each a heap allocation). The
    // fixed message header + the inline SQL bytes dominate the no-bind/small-bind
    // common case (e.g. `select 1 from dual` is 87 bytes total); bind columns add
    // their own bytes and may still grow the buffer, but the hot small-statement
    // path now builds in a single allocation. The written bytes are unchanged —
    // this is a pure allocation optimization (see `TtcWriter::with_capacity`).
    let writer_capacity = 96 + sql_bytes.len();
    let mut writer = TtcWriter::with_capacity(writer_capacity);
    writer.write_function_code_with_seq(TNS_FUNC_EXECUTE, seq_num);
    writer.write_ub8(exec_options.token_num);

    let is_plsql = statement_is_plsql(sql);
    let parse_only = exec_options.parse_only;
    // a fresh parse is required when the statement has no open server cursor
    // or is DDL (reference execute.pyx:88-89)
    let needs_parse = exec_options.cursor_id == 0 || crate::sql::statement_is_ddl(sql);
    // a scroll request only repositions the open cursor and fetches; the
    // EXECUTE/BIND options are suppressed (reference execute.pyx:82-84,105)
    let scroll_operation = exec_options.scroll_operation;
    let mut options = 0;
    if needs_parse {
        options |= TNS_EXEC_OPTION_PARSE;
    }
    if !parse_only && !scroll_operation {
        options |= TNS_EXEC_OPTION_EXECUTE;
    }
    if is_query {
        if parse_only {
            options |= TNS_EXEC_OPTION_DESCRIBE;
        } else if !exec_options.no_prefetch {
            // reference execute.pyx:99 gates FETCH on `not stmt._no_prefetch`;
            // a no-prefetch statement (VECTOR columns) leaves the rows to be
            // retrieved by the follow-up define-fetch instead.
            options |= TNS_EXEC_OPTION_FETCH;
        }
    }
    if bind_count > 0 && !scroll_operation {
        options |= TNS_EXEC_OPTION_BIND;
    }
    if is_plsql {
        if bind_count > 0 {
            options |= TNS_EXEC_OPTION_PLSQL_BIND;
        }
    } else if !parse_only {
        options |= TNS_EXEC_OPTION_NOT_PLSQL;
    }
    if exec_options.batcherrors {
        options |= TNS_EXEC_OPTION_BATCH_ERRORS;
    }
    let num_iters = if is_query && !parse_only {
        prefetch_rows
    } else {
        1
    };
    // al8i4[1]: queries report 0 on first execute and the iteration count on
    // re-execute of an open cursor (execute.pyx:187-193)
    let exec_count = if parse_only {
        0
    } else if is_query {
        if exec_options.cursor_id == 0 {
            0
        } else {
            num_iters
        }
    } else {
        bind_row_count.max(1)
    };
    let query_flag = u32::from(is_query);
    // reference sets the implicit-resultset flag on every full execute with
    // SQL (execute.pyx:81-82); anonymous PL/SQL blocks need it for
    // dbms_sql.return_result (ORA-29481 otherwise)
    let mut exec_flags = if parse_only {
        0
    } else {
        TNS_EXEC_FLAGS_IMPLICIT_RESULTSET
    };
    if exec_options.arraydmlrowcounts {
        exec_flags |= TNS_EXEC_FLAGS_DML_ROWCOUNTS;
    }
    // scrollable cursors keep the result set open across fetches and avoid the
    // server cancelling on end-of-fetch (reference execute.pyx:85-87)
    if exec_options.scrollable && !parse_only {
        exec_flags |= TNS_EXEC_FLAGS_SCROLLABLE;
        exec_flags |= TNS_EXEC_FLAGS_NO_CANCEL_ON_EOF;
    }
    writer.write_ub4(options);
    writer.write_ub4(exec_options.cursor_id);
    if needs_parse {
        writer.write_u8(1); // pointer (cursor id)
        writer.write_ub4(sql_len);
    } else {
        writer.write_u8(0); // pointer (cursor id)
        writer.write_ub4(0);
    }
    writer.write_u8(1);
    writer.write_ub4(13);
    writer.write_u8(0);
    writer.write_u8(0);
    writer.write_ub4(0);
    writer.write_ub4(num_iters);
    writer.write_ub4(TNS_MAX_LONG_LENGTH);
    if bind_count == 0 {
        writer.write_u8(0);
        writer.write_ub4(0);
    } else {
        writer.write_u8(1);
        writer.write_ub4(bind_count);
    }
    // CQN registration id (registerquery) split lsb/msb across the al8i4 slots
    // (reference execute.pyx:116-119,156,163). Zero for ordinary executes.
    let registration_id_lsb = (exec_options.registration_id & 0xffff_ffff) as u32;
    let registration_id_msb = ((exec_options.registration_id >> 32) & 0xffff_ffff) as u32;
    writer.write_u8(0);
    writer.write_u8(0);
    writer.write_u8(0);
    writer.write_u8(0);
    writer.write_u8(0);
    writer.write_u8(0);
    writer.write_ub4(0);
    writer.write_ub4(registration_id_lsb); // registration id (lsb)
    writer.write_u8(0); // pointer (al8objlist)
    writer.write_u8(1); // pointer (al8objlen)
    writer.write_u8(0); // pointer (al8blv)
    writer.write_ub4(0); // al8blvl
    writer.write_u8(0); // pointer (al8dnam)
    writer.write_ub4(0); // al8dnaml
    writer.write_ub4(registration_id_msb); // registration id (msb)
    if exec_options.arraydmlrowcounts {
        writer.write_u8(1); // pointer (al8pidmlrc)
        writer.write_ub4(exec_count); // al8pidmlrcbl
        writer.write_u8(1); // pointer (al8pidmlrcl)
    } else {
        writer.write_u8(0); // pointer (al8pidmlrc)
        writer.write_ub4(0); // al8pidmlrcbl
        writer.write_u8(0); // pointer (al8pidmlrcl)
    }
    writer.write_u8(0); // pointer (al8sqlsig)
    writer.write_ub4(0); // SQL signature length
    writer.write_u8(0); // pointer (SQL ID)
    writer.write_ub4(0); // allocated size of SQL ID
    writer.write_u8(0); // pointer (length of SQL ID)
    writer.write_u8(0); // pointer (chunk ids)
    writer.write_ub4(0); // number of chunk ids

    if needs_parse {
        writer.write_bytes_with_length(sql_bytes)?;
        writer.write_ub4(1); // al8i4[0] parse
    } else {
        writer.write_ub4(0); // al8i4[0] parse
    }
    writer.write_ub4(exec_count);
    writer.write_ub4(0);
    writer.write_ub4(0);
    writer.write_ub4(0);
    writer.write_ub4(0);
    writer.write_ub4(0);
    writer.write_ub4(query_flag); // al8i4[7] is query
    writer.write_ub4(0); // al8i4[8]
    writer.write_ub4(exec_flags); // al8i4[9] execute flags
    writer.write_ub4(exec_options.fetch_orientation); // al8i4[10] fetch orientation
    writer.write_ub4(exec_options.fetch_pos); // al8i4[11] fetch pos
    writer.write_ub4(0); // al8i4[12]
                         // a scroll request carries no bind parameters (reference suppresses the
                         // BIND option and never writes bind params for scroll_operation)
    if !bind_rows.is_empty() && !scroll_operation {
        write_bind_params(&mut writer, bind_rows, is_plsql)?;
    }
    Ok(writer.into_bytes())
}

pub(crate) fn write_bind_params(
    writer: &mut TtcWriter,
    bind_rows: &[Vec<BindValue>],
    is_plsql: bool,
) -> Result<()> {
    let Some(first_row) = bind_rows.first() else {
        return Ok(());
    };
    let mut bind_metadata = Vec::with_capacity(first_row.len());
    for index in 0..first_row.len() {
        bind_metadata.push(write_bind_metadata_for_rows(writer, bind_rows, index)?);
    }
    for row in bind_rows {
        if !is_plsql && row.iter().all(BindValue::is_output_only) {
            continue;
        }
        writer.write_u8(TNS_MSG_TYPE_ROW_DATA);
        for index in bind_row_value_order(row, &bind_metadata, is_plsql) {
            let value = &row[index];
            let (_ora_type_num, csfrm, _buffer_size) = bind_metadata
                .get(index)
                .copied()
                .unwrap_or((ORA_TYPE_NUM_VARCHAR, CS_FORM_IMPLICIT, 1));
            write_bind_value(writer, value, csfrm)?;
        }
    }
    Ok(())
}

pub(crate) fn bind_row_value_order(
    row: &[BindValue],
    bind_metadata: &[(u8, u8, u32)],
    is_plsql: bool,
) -> Vec<usize> {
    let mut non_long = Vec::with_capacity(row.len());
    let mut long = Vec::new();
    for (index, value) in row.iter().enumerate() {
        if !is_plsql && value.is_output_only() {
            continue;
        }
        // non-LONG values are written first followed by any LONG values; a
        // value is "long" when its buffer size exceeds the maximum string
        // size (reference messages/base.pyx:1529-1565 keys this off
        // `metadata.buffer_size > buf._caps.max_string_size`)
        if !is_plsql
            && bind_metadata
                .get(index)
                .is_some_and(|(ora_type_num, _, buffer_size)| {
                    matches!(*ora_type_num, ORA_TYPE_NUM_LONG | ORA_TYPE_NUM_LONG_RAW)
                        || *buffer_size > 32_767
                })
        {
            long.push(index);
        } else {
            non_long.push(index);
        }
    }
    non_long.extend(long);
    non_long
}

pub(crate) fn write_bind_metadata_for_rows(
    writer: &mut TtcWriter,
    bind_rows: &[Vec<BindValue>],
    index: usize,
) -> Result<(u8, u8, u32)> {
    let Some(first_row) = bind_rows.first() else {
        return Ok((ORA_TYPE_NUM_VARCHAR, CS_FORM_IMPLICIT, 1));
    };
    let Some(first_value) = first_row.get(index) else {
        return Ok((ORA_TYPE_NUM_VARCHAR, CS_FORM_IMPLICIT, 1));
    };
    let mut metadata_value = first_value;
    let (mut ora_type_num, mut csfrm, mut buffer_size) = bind_metadata(first_value);
    let mut needs_type_inference = matches!(first_value, BindValue::Null);
    for row in bind_rows.iter().skip(1) {
        let Some(value) = row.get(index) else {
            continue;
        };
        if needs_type_inference {
            if matches!(value, BindValue::Null) {
                continue;
            }
            metadata_value = value;
            (ora_type_num, csfrm, buffer_size) = bind_metadata(value);
            needs_type_inference = false;
            continue;
        }
        let (row_ora_type_num, row_csfrm, row_buffer_size) = bind_metadata(value);
        if row_csfrm == csfrm && bind_metadata_types_are_compatible(ora_type_num, row_ora_type_num)
        {
            ora_type_num = promoted_bind_metadata_type(ora_type_num, row_ora_type_num);
            buffer_size = buffer_size.max(row_buffer_size);
        }
    }
    write_bind_metadata_with_type(writer, metadata_value, ora_type_num, csfrm, buffer_size)?;
    Ok((ora_type_num, csfrm, buffer_size))
}

pub(crate) fn bind_metadata_types_are_compatible(left: u8, right: u8) -> bool {
    left == right
        || (matches!(
            left,
            ORA_TYPE_NUM_CHAR | ORA_TYPE_NUM_VARCHAR | ORA_TYPE_NUM_LONG
        ) && matches!(
            right,
            ORA_TYPE_NUM_CHAR | ORA_TYPE_NUM_VARCHAR | ORA_TYPE_NUM_LONG
        ))
        || (matches!(left, ORA_TYPE_NUM_RAW | ORA_TYPE_NUM_LONG_RAW)
            && matches!(right, ORA_TYPE_NUM_RAW | ORA_TYPE_NUM_LONG_RAW))
}

pub(crate) fn promoted_bind_metadata_type(left: u8, right: u8) -> u8 {
    if matches!(left, ORA_TYPE_NUM_LONG | ORA_TYPE_NUM_LONG_RAW) {
        left
    } else if matches!(right, ORA_TYPE_NUM_LONG | ORA_TYPE_NUM_LONG_RAW) {
        right
    } else {
        left
    }
}
