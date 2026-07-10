//! Streaming cursor over the documents matched by a `find()`.

use std::collections::VecDeque;

use asupersync::Cx;
use oracledb_protocol::thin::{
    ColumnMetadata, QueryResult, QueryValue, ORA_TYPE_NUM_BLOB, ORA_TYPE_NUM_CLOB,
    ORA_TYPE_NUM_JSON, ORA_TYPE_NUM_VECTOR,
};

use crate::Connection;

use super::collection::{finish_cursor_operation, SodaCollection};
use super::document::SodaDocument;
use super::error::{Result, SodaError};
use super::operation::SelectColumns;

/// A document cursor: buffers a fetched batch and refills from the server until
/// the result set is exhausted.
pub struct SodaCursor {
    collection: SodaCollection,
    layout: SelectColumns,
    buffer: VecDeque<Vec<Option<QueryValue>>>,
    columns: Vec<ColumnMetadata>,
    cursor_id: u32,
    array_size: u32,
    more_rows: bool,
    previous_row: Option<Vec<Option<QueryValue>>>,
    closed: bool,
}

impl SodaCursor {
    /// Build a cursor from the first batch's QueryResult.
    pub(crate) fn new(
        collection: SodaCollection,
        result: QueryResult,
        layout: SelectColumns,
        array_size: u32,
    ) -> Self {
        let QueryResult {
            columns,
            rows,
            cursor_id,
            more_rows,
            ..
        } = result;
        let previous_row = rows.last().cloned();
        SodaCursor {
            collection,
            layout,
            buffer: rows.into_iter().collect(),
            columns,
            cursor_id,
            array_size,
            more_rows,
            previous_row,
            closed: false,
        }
    }

    /// Return the next document, fetching another batch from the server if the
    /// local buffer is empty and more rows are available.
    pub async fn next_doc(
        &mut self,
        conn: &mut Connection,
        cx: &Cx,
    ) -> Result<Option<SodaDocument>> {
        if self.closed {
            return Err(SodaError::NotSupported("cursor is closed".to_string()));
        }
        if self.buffer.is_empty() && self.more_rows {
            self.fetch_more(conn, cx).await?;
        }
        match self.buffer.pop_front() {
            Some(row) => {
                let doc = self.collection.row_to_document(&row, &self.layout)?;
                Ok(Some(doc))
            }
            None => Ok(None),
        }
    }

    /// Fetch another batch into the buffer. Native JSON / LOB / VECTOR columns
    /// require the define-fetch variant to deliver values.
    async fn fetch_more(&mut self, conn: &mut Connection, cx: &Cx) -> Result<()> {
        // Preserve a cursor when cancellation was already pending before this
        // operation began. Once the low-level call starts, every error below is
        // fail-closed because the server-side cursor state is no longer proven.
        crate::observe_cancellation_between_round_trips(cx).map_err(SodaError::Driver)?;
        let needs_define = self.columns.iter().any(|c| {
            matches!(
                c.ora_type_num(),
                ORA_TYPE_NUM_CLOB | ORA_TYPE_NUM_BLOB | ORA_TYPE_NUM_VECTOR | ORA_TYPE_NUM_JSON
            )
        });
        let result = if needs_define {
            conn.define_and_fetch_rows_with_columns(
                cx,
                self.cursor_id,
                self.array_size,
                &self.columns,
                self.previous_row.as_deref(),
            )
            .await
        } else {
            conn.fetch_rows_with_columns(
                cx,
                self.cursor_id,
                self.array_size,
                &self.columns,
                self.previous_row.as_deref(),
            )
            .await
        };
        let result = match finish_cursor_operation(conn, self.cursor_id, result) {
            Ok(result) => result,
            Err(err) => {
                self.closed = true;
                self.more_rows = false;
                return Err(err);
            }
        };
        self.more_rows = result.more_rows;
        self.previous_row = result.rows.last().cloned();
        self.buffer.extend(result.rows);
        Ok(())
    }

    /// Close the cursor, releasing the server cursor.
    pub async fn close(&mut self, conn: &mut Connection, _cx: &Cx) -> Result<()> {
        if self.closed {
            return Err(SodaError::NotSupported("cursor already closed".to_string()));
        }
        self.release(conn);
        Ok(())
    }

    /// Release an internally-owned cursor without changing the public
    /// double-close contract. This is idempotent so collection helpers can use
    /// it both after a local decoding error and after a fetch failure that has
    /// already retired the cursor fail-closed.
    pub(crate) fn release(&mut self, conn: &mut Connection) {
        if self.closed {
            return;
        }
        self.closed = true;
        self.more_rows = false;
        conn.release_cursor(self.cursor_id);
    }

    /// Whether the cursor has been closed.
    pub fn is_closed(&self) -> bool {
        self.closed
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::{Read as _, Write as _};
    use std::net::TcpListener;
    use std::thread;
    use std::time::Duration;

    use asupersync::{net::TcpStream, CancelKind};
    use oracledb_protocol::thin::{
        ORA_TYPE_NUM_VARCHAR, TNS_DATA_FLAGS_END_OF_RESPONSE, TNS_MSG_TYPE_END_OF_RESPONSE,
        TNS_MSG_TYPE_ROW_DATA, TNS_MSG_TYPE_ROW_HEADER, TNS_PACKET_TYPE_DATA,
    };
    use oracledb_protocol::wire::{encode_packet, PacketLengthWidth, TtcWriter};

    use crate::soda::metadata::{
        ContentSqlType, KeyAssignment, SodaCollectionMetadata, VersionMethod,
    };

    const CURSOR_ID: u32 = 73;
    const SQL: &str = "select soda cursor lifecycle probe";

    fn collection() -> SodaCollection {
        SodaCollection::new(
            "LifecycleProbe".to_string(),
            SodaCollectionMetadata {
                table_name: "LifecycleProbe".to_string(),
                schema_name: None,
                key_column: "RESID".to_string(),
                key_sql_type: "RAW".to_string(),
                key_assignment: KeyAssignment::EmbeddedOid,
                key_path: Some("_id".to_string()),
                content_column: "DOC".to_string(),
                content_sql_type: ContentSqlType::Json,
                version_column: None,
                version_method: VersionMethod::None,
                last_modified_column: None,
                creation_time_column: None,
                media_type_column: None,
                read_only: false,
                native: true,
            },
        )
    }

    fn layout() -> SelectColumns {
        SelectColumns {
            key_idx: 0,
            content_idx: 1,
            version_idx: None,
            created_idx: None,
            last_modified_idx: None,
            media_type_idx: None,
        }
    }

    fn malformed_response_packet() -> Vec<u8> {
        encode_packet(
            TNS_PACKET_TYPE_DATA,
            0,
            Some(TNS_DATA_FLAGS_END_OF_RESPONSE),
            &[0xff],
            PacketLengthWidth::Large32,
        )
        .expect("encode malformed response packet")
    }

    fn successful_scalar_fetch_payload() -> Vec<u8> {
        let mut payload = TtcWriter::new();
        payload.write_u8(TNS_MSG_TYPE_ROW_HEADER);
        payload.write_u8(0); // flags
        payload.write_ub2(1); // request count
        payload.write_ub4(1); // iteration number
        payload.write_ub4(1); // iteration count
        payload.write_ub2(0); // buffer length
        payload.write_ub4(0); // no duplicate-column bit vector
        payload
            .write_bytes_with_two_lengths(None)
            .expect("encode row header id");
        payload.write_u8(TNS_MSG_TYPE_ROW_DATA);
        payload
            .write_bytes_with_length(b"doc-key")
            .expect("encode synthetic SODA key");
        payload
            .write_bytes_with_length(br#"{"ok":true}"#)
            .expect("encode synthetic SODA document");
        payload.write_u8(TNS_MSG_TYPE_END_OF_RESPONSE);
        payload.into_bytes()
    }

    fn successful_scalar_fetch_packet() -> Vec<u8> {
        encode_packet(
            TNS_PACKET_TYPE_DATA,
            0,
            Some(TNS_DATA_FLAGS_END_OF_RESPONSE),
            &successful_scalar_fetch_payload(),
            PacketLengthWidth::Large32,
        )
        .expect("encode successful fetch response packet")
    }

    fn read_one_packet(socket: &mut std::net::TcpStream) -> std::io::Result<()> {
        let mut length = [0u8; 4];
        socket.read_exact(&mut length)?;
        let packet_len = u32::from_be_bytes(length) as usize;
        let mut rest = vec![0u8; packet_len.saturating_sub(length.len())];
        socket.read_exact(&mut rest)
    }

    fn failed_refill_retires_cursor(ora_type_num: u8) -> crate::Result<()> {
        let listener = TcpListener::bind("127.0.0.1:0")?;
        let addr = listener.local_addr()?;
        let server = thread::spawn(move || -> std::io::Result<()> {
            let (mut socket, _) = listener.accept()?;
            socket.set_read_timeout(Some(Duration::from_secs(2)))?;
            read_one_packet(&mut socket)?;
            socket.write_all(&malformed_response_packet())?;
            socket.flush()
        });

        let runtime = crate::build_io_runtime()?;
        let outcome = runtime.block_on(async {
            let cx = Cx::current()
                .ok_or_else(|| crate::Error::Runtime("missing ambient test Cx".to_string()))?;
            let stream = TcpStream::connect(addr).await?;
            let (read, write) = crate::transport::plain_split(stream);
            let mut conn = crate::tests::loopback_connection(read, write);
            let columns = vec![ColumnMetadata::new("DOC", ora_type_num)];

            conn.statement_cache_put(SQL, CURSOR_ID, Vec::new());
            conn.in_use_cursors.insert(CURSOR_ID);
            conn.cursor_columns.insert(CURSOR_ID, columns.clone());
            if ora_type_num == ORA_TYPE_NUM_JSON {
                conn.lob_prefetch_cursors.insert(CURSOR_ID);
            }

            let mut cursor = SodaCursor::new(
                collection(),
                QueryResult {
                    columns,
                    cursor_id: CURSOR_ID,
                    more_rows: true,
                    ..Default::default()
                },
                layout(),
                1,
            );

            let err = cursor
                .next_doc(&mut conn, &cx)
                .await
                .expect_err("malformed continuation response must fail");
            assert!(matches!(err, SodaError::Driver(_)), "{err:?}");
            assert!(
                cursor.is_closed(),
                "a failed cursor operation must make the SODA cursor terminal"
            );
            assert!(!conn.in_use_cursors.contains(&CURSOR_ID));
            assert!(
                conn.statement_cache
                    .iter()
                    .all(|entry| entry.cursor_id != CURSOR_ID),
                "an unproven server cursor must not remain cache-reusable"
            );
            assert!(!conn.cursor_columns.contains_key(&CURSOR_ID));
            assert!(!conn.lob_prefetch_cursors.contains(&CURSOR_ID));
            assert_eq!(
                conn.cursors_to_close
                    .iter()
                    .filter(|cursor_id| **cursor_id == CURSOR_ID)
                    .count(),
                1,
                "the failed SODA cursor must be queued for close exactly once"
            );
            Ok::<_, crate::Error>(())
        });

        server.join().expect("SODA failure server joins")?;
        outcome
    }

    #[test]
    fn failed_plain_fetch_retires_soda_cursor() -> crate::Result<()> {
        failed_refill_retires_cursor(ORA_TYPE_NUM_VARCHAR)
    }

    #[test]
    fn failed_define_fetch_retires_soda_cursor() -> crate::Result<()> {
        failed_refill_retires_cursor(ORA_TYPE_NUM_JSON)
    }

    #[test]
    fn successful_refill_preserves_cursor_until_explicit_close() -> crate::Result<()> {
        let listener = TcpListener::bind("127.0.0.1:0")?;
        let addr = listener.local_addr()?;
        let server = thread::spawn(move || -> std::io::Result<()> {
            let (mut socket, _) = listener.accept()?;
            socket.set_read_timeout(Some(Duration::from_secs(2)))?;
            read_one_packet(&mut socket)?;
            socket.write_all(&successful_scalar_fetch_packet())?;
            socket.flush()
        });

        let runtime = crate::build_io_runtime()?;
        let outcome = runtime.block_on(async {
            let cx = Cx::current()
                .ok_or_else(|| crate::Error::Runtime("missing ambient test Cx".to_string()))?;
            let stream = TcpStream::connect(addr).await?;
            let (read, write) = crate::transport::plain_split(stream);
            let mut conn = crate::tests::loopback_connection(read, write);
            let columns = vec![
                ColumnMetadata::new("KEY", ORA_TYPE_NUM_VARCHAR)
                    .with_csfrm(oracledb_protocol::thin::CS_FORM_IMPLICIT)
                    .with_buffer_size(100)
                    .with_max_size(100),
                ColumnMetadata::new("DOC", ORA_TYPE_NUM_VARCHAR)
                    .with_csfrm(oracledb_protocol::thin::CS_FORM_IMPLICIT)
                    .with_buffer_size(100)
                    .with_max_size(100),
            ];
            let fixture = oracledb_protocol::thin::parse_fetch_response_with_context(
                &successful_scalar_fetch_payload(),
                oracledb_protocol::thin::ClientCapabilities::default(),
                &columns,
                None,
            )
            .expect("successful continuation fixture decodes");
            assert_eq!(fixture.rows.len(), 1);
            conn.statement_cache_put(SQL, CURSOR_ID, Vec::new());
            conn.in_use_cursors.insert(CURSOR_ID);
            conn.cursor_columns.insert(CURSOR_ID, columns.clone());
            let mut cursor = SodaCursor::new(
                collection(),
                QueryResult {
                    columns,
                    cursor_id: CURSOR_ID,
                    more_rows: true,
                    ..Default::default()
                },
                layout(),
                1,
            );

            let doc = cursor
                .next_doc(&mut conn, &cx)
                .await
                .expect("successful continuation fetch")
                .expect("one document returned");
            assert_eq!(doc.key.as_deref(), Some("doc-key"));
            assert_eq!(doc.content_bytes.as_deref(), Some(&br#"{"ok":true}"#[..]));
            assert!(!cursor.is_closed());
            assert!(conn.in_use_cursors.contains(&CURSOR_ID));
            assert!(conn
                .statement_cache
                .iter()
                .any(|entry| entry.cursor_id == CURSOR_ID));
            assert!(conn.cursor_columns.contains_key(&CURSOR_ID));
            assert!(conn.cursors_to_close.is_empty());

            cursor.close(&mut conn, &cx).await.expect("close cursor");
            assert!(cursor.is_closed());
            assert!(!conn.in_use_cursors.contains(&CURSOR_ID));
            assert!(conn
                .statement_cache
                .iter()
                .any(|entry| entry.cursor_id == CURSOR_ID));
            assert!(conn.cursors_to_close.is_empty());
            Ok::<_, crate::Error>(())
        });

        server.join().expect("successful refill server joins")?;
        outcome
    }

    #[test]
    fn cancellation_before_refill_keeps_soda_cursor_retryable() -> crate::Result<()> {
        let listener = TcpListener::bind("127.0.0.1:0")?;
        let addr = listener.local_addr()?;
        let server = thread::spawn(move || -> std::io::Result<Option<u8>> {
            let (mut socket, _) = listener.accept()?;
            socket.set_read_timeout(Some(Duration::from_millis(300)))?;
            let mut byte = [0u8; 1];
            match socket.read(&mut byte) {
                Ok(0) => Ok(None),
                Ok(_) => Ok(Some(byte[0])),
                Err(err)
                    if matches!(
                        err.kind(),
                        std::io::ErrorKind::WouldBlock | std::io::ErrorKind::TimedOut
                    ) =>
                {
                    Ok(None)
                }
                Err(err) => Err(err),
            }
        });

        let runtime = crate::build_io_runtime()?;
        let outcome = runtime.block_on(async {
            let cx = Cx::current()
                .ok_or_else(|| crate::Error::Runtime("missing ambient test Cx".to_string()))?;
            let stream = TcpStream::connect(addr).await?;
            let (read, write) = crate::transport::plain_split(stream);
            let mut conn = crate::tests::loopback_connection(read, write);
            let columns = vec![ColumnMetadata::new("DOC", ORA_TYPE_NUM_VARCHAR)];
            conn.statement_cache_put(SQL, CURSOR_ID, Vec::new());
            conn.in_use_cursors.insert(CURSOR_ID);
            conn.cursor_columns.insert(CURSOR_ID, columns.clone());
            let mut cursor = SodaCursor::new(
                collection(),
                QueryResult {
                    columns,
                    cursor_id: CURSOR_ID,
                    more_rows: true,
                    ..Default::default()
                },
                layout(),
                1,
            );

            cx.cancel_fast(CancelKind::User);
            let err = cursor
                .next_doc(&mut conn, &cx)
                .await
                .expect_err("pending cancellation must stop before refill");
            assert!(matches!(err, SodaError::Driver(crate::Error::Cancelled)));
            assert!(!cursor.is_closed());
            assert!(conn.in_use_cursors.contains(&CURSOR_ID));
            assert!(conn
                .statement_cache
                .iter()
                .any(|entry| entry.cursor_id == CURSOR_ID));
            assert!(conn.cursors_to_close.is_empty());
            Ok::<_, crate::Error>(())
        });

        assert_eq!(
            server.join().expect("pre-cancel server joins")?,
            None,
            "a cancellation observed before refill must not write a packet"
        );
        outcome
    }
}
