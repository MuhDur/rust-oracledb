//! Streaming cursor over the documents matched by a `find()`.

use std::collections::VecDeque;

use asupersync::Cx;
use oracledb_protocol::thin::{
    ColumnMetadata, QueryResult, QueryValue, ORA_TYPE_NUM_BLOB, ORA_TYPE_NUM_CLOB,
    ORA_TYPE_NUM_JSON, ORA_TYPE_NUM_VECTOR,
};

use crate::Connection;

use super::collection::SodaCollection;
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
        let needs_define = self.columns.iter().any(|c| {
            matches!(
                c.ora_type_num,
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
            .map_err(SodaError::Driver)?
        } else {
            conn.fetch_rows_with_columns(
                cx,
                self.cursor_id,
                self.array_size,
                &self.columns,
                self.previous_row.as_deref(),
            )
            .await
            .map_err(SodaError::Driver)?
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
        self.closed = true;
        conn.release_cursor(self.cursor_id);
        Ok(())
    }

    /// Whether the cursor has been closed.
    pub fn is_closed(&self) -> bool {
        self.closed
    }
}
