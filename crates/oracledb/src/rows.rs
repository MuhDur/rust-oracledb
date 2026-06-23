use std::num::NonZeroU32;
use std::sync::Arc;

use asupersync::Cx;
use oracledb_protocol::thin::{BatchServerError, ColumnMetadata, QueryResult, QueryValue};

use crate::recovery::observe_cancellation_between_round_trips;
use crate::request::QueryDeadline;
use crate::{
    block_on_io, ColumnIndex, Connection, Cursor, Error, FromRow, FromSql, Result, Scroll, TypedRow,
};

/// OUT and IN/OUT bind values returned by [`Connection::execute`].
#[derive(Clone, Debug, Default, PartialEq)]
#[non_exhaustive]
pub struct OutBinds {
    values: Vec<(usize, Option<QueryValue>)>,
}

impl OutBinds {
    fn new(values: Vec<(usize, Option<QueryValue>)>) -> Self {
        Self { values }
    }

    pub fn len(&self) -> usize {
        self.values.len()
    }

    pub fn is_empty(&self) -> bool {
        self.values.is_empty()
    }

    pub fn values(&self) -> &[(usize, Option<QueryValue>)] {
        &self.values
    }

    pub fn get(&self, bind_index: usize) -> Option<&Option<QueryValue>> {
        self.values
            .iter()
            .find_map(|(index, value)| (*index == bind_index).then_some(value))
    }

    pub fn into_values(self) -> Vec<(usize, Option<QueryValue>)> {
        self.values
    }
}

/// Per-bind rows returned by DML `RETURNING INTO`.
#[derive(Clone, Debug, Default, PartialEq)]
#[non_exhaustive]
pub struct ReturningRows {
    values: Vec<(usize, Vec<Option<QueryValue>>)>,
}

impl ReturningRows {
    fn new(values: Vec<(usize, Vec<Option<QueryValue>>)>) -> Self {
        Self { values }
    }

    /// Build from raw per-call return-value groups, coalescing groups that share
    /// a bind index. Array DML (`execute_many`) decodes `RETURNING` once per
    /// iteration, emitting one `(bind_index, rows)` group per iteration; without
    /// coalescing `rows_for(bind_index)` - which returns the first matching
    /// group - would expose only the first iteration's value. Coalescing merges
    /// them in input order so `rows_for(bind_index)` returns every affected
    /// row's value, consistent with single-statement `RETURNING`, which already
    /// arrives as one group per bind. (The raw per-iteration grouping is
    /// preserved at the protocol layer for consumers that need it.)
    fn coalesced(raw: Vec<(usize, Vec<Option<QueryValue>>)>) -> Self {
        let mut values: Vec<(usize, Vec<Option<QueryValue>>)> = Vec::new();
        for (index, rows) in raw {
            if let Some((_, existing)) = values.iter_mut().find(|(i, _)| *i == index) {
                existing.extend(rows);
            } else {
                values.push((index, rows));
            }
        }
        Self { values }
    }

    pub fn len(&self) -> usize {
        self.values.len()
    }

    pub fn is_empty(&self) -> bool {
        self.values.is_empty()
    }

    pub fn values(&self) -> &[(usize, Vec<Option<QueryValue>>)] {
        &self.values
    }

    pub fn rows_for(&self, bind_index: usize) -> Option<&[Option<QueryValue>]> {
        self.values
            .iter()
            .find_map(|(index, rows)| (*index == bind_index).then_some(rows.as_slice()))
    }

    pub fn into_values(self) -> Vec<(usize, Vec<Option<QueryValue>>)> {
        self.values
    }
}

/// Result of an [`Execute`](crate::Execute) operation.
#[derive(Clone, Debug, Default, PartialEq)]
#[non_exhaustive]
pub struct ExecuteOutcome {
    rows_affected: u64,
    last_rowid: Option<String>,
    out_binds: OutBinds,
    returning: ReturningRows,
    implicit_results: Vec<Cursor>,
    compilation_warning: bool,
}

impl ExecuteOutcome {
    const COMPILATION_WARNING: &'static str = "PL/SQL compiled with warnings";

    pub(crate) fn from_query_result(result: QueryResult) -> Self {
        let implicit_results = result
            .implicit_resultsets
            .unwrap_or_default()
            .into_iter()
            .filter_map(|value| match value {
                QueryValue::Cursor(cursor) => Some(*cursor),
                _ => None,
            })
            .collect();
        Self {
            rows_affected: result.row_count,
            last_rowid: result.last_rowid,
            out_binds: OutBinds::new(result.out_values),
            returning: ReturningRows::new(result.return_values),
            implicit_results,
            compilation_warning: result.compilation_error_warning,
        }
    }

    pub fn rows_affected(&self) -> u64 {
        self.rows_affected
    }

    pub fn last_rowid(&self) -> Option<&str> {
        self.last_rowid.as_deref()
    }

    pub fn out_binds(&self) -> &OutBinds {
        &self.out_binds
    }

    pub fn returning(&self) -> &ReturningRows {
        &self.returning
    }

    pub fn implicit_results(&self) -> &[Cursor] {
        &self.implicit_results
    }

    pub fn compilation_warning(&self) -> Option<&str> {
        self.compilation_warning
            .then_some(Self::COMPILATION_WARNING)
    }
}

/// One row-level error collected by [`Batch::collect_errors`](crate::Batch::collect_errors).
#[derive(Clone, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub struct BatchError {
    row_index: u32,
    code: u32,
    message: String,
}

impl BatchError {
    fn from_server(error: BatchServerError) -> Self {
        let (code, row_index, message) = error.into_parts();
        Self {
            row_index,
            code,
            message,
        }
    }

    pub fn row_index(&self) -> u32 {
        self.row_index
    }

    pub fn code(&self) -> u32 {
        self.code
    }

    pub fn message(&self) -> &str {
        &self.message
    }
}

impl std::fmt::Display for BatchError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        if self.message.is_empty() {
            write!(f, "ORA-{:05} at batch row {}", self.code, self.row_index)
        } else {
            write!(f, "{} at batch row {}", self.message, self.row_index)
        }
    }
}

/// Result of an [`execute_many`](Connection::execute_many) operation.
#[derive(Clone, Debug, Default, PartialEq)]
#[non_exhaustive]
pub struct BatchOutcome {
    rows_affected: u64,
    per_row_counts: Option<Vec<u64>>,
    errors: Vec<BatchError>,
    returning: ReturningRows,
}

impl BatchOutcome {
    pub(crate) fn empty(array_dml_row_counts: bool) -> Self {
        Self {
            rows_affected: 0,
            per_row_counts: array_dml_row_counts.then(Vec::new),
            errors: Vec::new(),
            returning: ReturningRows::default(),
        }
    }

    pub(crate) fn from_query_result(result: QueryResult) -> Self {
        Self {
            rows_affected: result.row_count,
            per_row_counts: result.array_dml_row_counts,
            errors: result
                .batch_errors
                .into_iter()
                .map(BatchError::from_server)
                .collect(),
            returning: ReturningRows::coalesced(result.return_values),
        }
    }

    pub fn rows_affected(&self) -> u64 {
        self.rows_affected
    }

    pub fn per_row_counts(&self) -> Option<&[u64]> {
        self.per_row_counts.as_deref()
    }

    pub fn errors(&self) -> &[BatchError] {
        &self.errors
    }

    pub fn returning(&self) -> &ReturningRows {
        &self.returning
    }
}

/// Result of a [`register_query`](Connection::register_query) operation.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
#[non_exhaustive]
pub struct RegistrationOutcome {
    query_id: Option<u64>,
}

impl RegistrationOutcome {
    pub(crate) fn from_query_result(result: QueryResult) -> Self {
        Self {
            query_id: result.query_id.filter(|id| *id != 0),
        }
    }

    pub fn query_id(&self) -> Option<u64> {
        self.query_id
    }
}

/// One owned query row.
#[derive(Clone, Debug, PartialEq)]
pub struct Row {
    columns: Arc<[ColumnMetadata]>,
    values: Vec<Option<QueryValue>>,
}

impl Row {
    pub(crate) fn new(columns: Arc<[ColumnMetadata]>, values: Vec<Option<QueryValue>>) -> Self {
        Self { columns, values }
    }

    pub fn len(&self) -> usize {
        self.values.len()
    }

    pub fn is_empty(&self) -> bool {
        self.values.is_empty()
    }

    pub fn columns(&self) -> &[ColumnMetadata] {
        &self.columns
    }

    pub fn values(&self) -> &[Option<QueryValue>] {
        &self.values
    }

    pub fn value(&self, col: impl ColumnIndex) -> Option<&QueryValue> {
        let col = col.resolve(&self.columns).ok()?;
        self.values.get(col).and_then(Option::as_ref)
    }

    pub fn typed_row(&self) -> TypedRow<'_> {
        TypedRow::new(&self.columns, &self.values, 0)
    }

    pub fn get<T: FromSql>(&self, col: impl ColumnIndex) -> Result<T> {
        let col = col.resolve(&self.columns).map_err(Error::Conversion)?;
        self.typed_row().get(col)
    }

    pub fn try_get<T: FromSql>(&self, col: impl ColumnIndex) -> Result<Option<T>> {
        let col = col.resolve(&self.columns).map_err(Error::Conversion)?;
        self.typed_row().try_get_opt(col).map_err(Error::Conversion)
    }

    pub fn get_by_name<T: FromSql>(&self, name: &str) -> Result<T> {
        self.get(name)
    }

    pub fn into_values(self) -> Vec<Option<QueryValue>> {
        self.values
    }
}

/// Lazy result-set facade returned by [`Connection::query`] and
/// [`Connection::query_with`].
#[derive(Debug)]
#[non_exhaustive]
pub struct Rows<'conn> {
    connection: &'conn mut Connection,
    sql: String,
    columns: Arc<[ColumnMetadata]>,
    batch: Vec<Row>,
    cursor_id: u32,
    more_rows: bool,
    arraysize: NonZeroU32,
    deadline: QueryDeadline,
    scrollable: bool,
    cursor: Option<Cursor>,
}

impl Rows<'_> {
    pub(crate) fn from_result<'conn>(
        connection: &'conn mut Connection,
        sql: String,
        arraysize: NonZeroU32,
        deadline: QueryDeadline,
        scrollable: bool,
        result: QueryResult,
    ) -> Rows<'conn> {
        let cursor_id = result.cursor_id;
        let more_rows = result.more_rows;
        let cursor = first_cursor_from_result(&result);
        let columns: Arc<[ColumnMetadata]> = Arc::from(result.columns.into_boxed_slice());
        let batch = result
            .rows
            .into_iter()
            .map(|values| Row::new(Arc::clone(&columns), values))
            .collect();
        Rows {
            connection,
            sql,
            columns,
            batch,
            cursor_id,
            more_rows,
            arraysize,
            deadline,
            scrollable,
            cursor,
        }
    }

    pub fn columns(&self) -> &[ColumnMetadata] {
        &self.columns
    }

    pub fn batch(&self) -> &[Row] {
        &self.batch
    }

    pub async fn next_batch(&mut self, cx: &Cx) -> Result<bool> {
        if !self.more_rows || self.cursor_id == 0 {
            self.release_cursor();
            return Ok(false);
        }
        observe_cancellation_between_round_trips(cx)?;
        let previous_row = self.batch.last().map(|row| row.values.clone());
        let cursor_id = self.cursor_id;
        let arraysize = self.arraysize.get();
        // Cheap refcount bump, not a deep clone: `self.columns` is an
        // `Arc<[ColumnMetadata]>` and `fetch_rows_with_columns` only needs a
        // `&[ColumnMetadata]`, so `&columns` deref-coerces. Cloning the Arc keeps
        // the per-page continuation off the metadata's owned Strings.
        let columns = Arc::clone(&self.columns);
        let result = match self
            .deadline
            .run(self.connection.fetch_rows_with_columns(
                cx,
                cursor_id,
                arraysize,
                &columns,
                previous_row.as_deref(),
            ))
            .await
        {
            Ok(result) => result?,
            Err(()) => {
                self.release_cursor();
                return self
                    .connection
                    .recover_from_call_timeout(cx, self.deadline.timeout_ms())
                    .await;
            }
        };
        self.apply_result(result);
        let batch_available = !self.batch.is_empty() || self.more_rows;
        if !self.more_rows {
            self.release_cursor();
        }
        Ok(batch_available)
    }

    pub async fn collect(mut self, cx: &Cx) -> Result<Vec<Row>> {
        let mut rows = Vec::new();
        rows.append(&mut self.batch);
        while self.more_rows {
            if let Err(err) = self.next_batch(cx).await {
                self.release_cursor();
                return Err(err);
            }
            rows.append(&mut self.batch);
        }
        self.release_cursor();
        Ok(rows)
    }

    /// Fetch ahead until the batch holds at least two rows or the server has
    /// confirmed end-of-data, so the cardinality check in [`one`](Self::one) /
    /// [`opt`](Self::opt) cannot mistake a still-pending `more_rows` flag for a
    /// second row.
    ///
    /// `more_rows` means only "the server has not yet signalled end-of-data",
    /// not ">1 row". A LONG / LONG RAW column forces a per-row define-fetch that
    /// ignores the requested arraysize, so a genuine single-row result comes
    /// back with one row and `more_rows` still set; without this confirmation
    /// `one()` would wrongly raise [`Error::TooManyRows`]. Bounded: at most one
    /// extra round trip for a single-row result, and it stops the moment a
    /// second row is in hand.
    pub(crate) async fn materialize_for_cardinality(&mut self, cx: &Cx) -> Result<()> {
        let mut held: Vec<Row> = Vec::new();
        while held.len() + self.batch.len() < 2 && self.more_rows && self.cursor_id != 0 {
            // `next_batch` keys the LONG/LOB define-fetch continuation off
            // `self.batch.last()` and then REPLACES `self.batch`. Clone the row
            // we already hold into `held` (leaving the original in place as the
            // continuation key) so it survives the fetch.
            if let Some(last) = self.batch.last() {
                held.push(last.clone());
            }
            self.next_batch(cx).await?;
        }
        if !held.is_empty() {
            held.append(&mut self.batch);
            self.batch = held;
        }
        Ok(())
    }

    pub fn one(mut self) -> Result<Row> {
        let too_many = self.more_rows || self.batch.len() > 1;
        self.release_cursor();
        if too_many {
            return Err(Error::TooManyRows);
        }
        self.batch.pop().ok_or(Error::NoRows)
    }

    pub fn opt(mut self) -> Result<Option<Row>> {
        let too_many = self.more_rows || self.batch.len() > 1;
        self.release_cursor();
        if too_many {
            return Err(Error::TooManyRows);
        }
        Ok(self.batch.pop())
    }

    pub async fn into_typed<T: FromRow>(self, cx: &Cx) -> Result<Vec<T>> {
        let rows = self.collect(cx).await?;
        rows.iter()
            .map(|row| T::from_row(&row.typed_row()).map_err(Error::Conversion))
            .collect()
    }

    pub fn cursor(&self) -> Option<&Cursor> {
        self.cursor.as_ref()
    }

    pub async fn scroll(&mut self, cx: &Cx, to: Scroll) -> Result<()> {
        if !self.scrollable {
            return Err(Error::Runtime(
                "Rows::scroll requires Query::scrollable".to_string(),
            ));
        }
        if self.cursor_id == 0 {
            return Err(Error::Runtime(
                "Rows::scroll requires an open cursor".to_string(),
            ));
        }
        observe_cancellation_between_round_trips(cx)?;
        let (orientation, position) = to.into_wire_parts();
        let result = match self
            .deadline
            .run(self.connection.scroll_cursor(
                cx,
                &self.sql,
                self.cursor_id,
                self.arraysize.get(),
                orientation,
                position,
            ))
            .await
        {
            Ok(result) => result?,
            Err(()) => {
                self.release_cursor();
                return self
                    .connection
                    .recover_from_call_timeout(cx, self.deadline.timeout_ms())
                    .await;
            }
        };
        self.apply_result(result);
        Ok(())
    }

    fn apply_result(&mut self, result: QueryResult) {
        let cursor = first_cursor_from_result(&result);
        if result.cursor_id != 0 {
            self.cursor_id = result.cursor_id;
        }
        if !result.columns.is_empty() {
            self.columns = Arc::from(result.columns.into_boxed_slice());
        }
        self.more_rows = result.more_rows;
        if self.cursor.is_none() {
            self.cursor = cursor;
        }
        self.batch = result
            .rows
            .into_iter()
            .map(|values| Row::new(Arc::clone(&self.columns), values))
            .collect();
    }

    fn release_cursor(&mut self) {
        if self.cursor_id == 0 {
            return;
        }
        self.connection.release_cursor(self.cursor_id);
        self.cursor_id = 0;
        self.more_rows = false;
    }
}

impl Drop for Rows<'_> {
    fn drop(&mut self) {
        self.release_cursor();
    }
}

/// Blocking lazy result-set facade returned by [`BlockingConnection::query`](crate::BlockingConnection::query)
/// and [`BlockingConnection::query_with`](crate::BlockingConnection::query_with).
///
/// `BlockingRows` owns the same server cursor state as [`Rows`], but its
/// continuation methods drive the async cursor operations on the blocking
/// facade runtime so synchronous callers never need to pass a [`Cx`].
#[derive(Debug)]
#[non_exhaustive]
pub struct BlockingRows<'conn> {
    inner: Rows<'conn>,
}

impl<'conn> BlockingRows<'conn> {
    pub(crate) fn new(inner: Rows<'conn>) -> Self {
        Self { inner }
    }

    pub fn columns(&self) -> &[ColumnMetadata] {
        self.inner.columns()
    }

    pub fn batch(&self) -> &[Row] {
        self.inner.batch()
    }

    pub fn next_batch(&mut self) -> Result<bool> {
        block_on_io(|cx| async move { self.inner.next_batch(&cx).await })
    }

    pub fn collect(self) -> Result<Vec<Row>> {
        block_on_io(|cx| async move { self.inner.collect(&cx).await })
    }

    pub fn one(self) -> Result<Row> {
        self.inner.one()
    }

    pub fn opt(self) -> Result<Option<Row>> {
        self.inner.opt()
    }

    pub fn into_typed<T: FromRow>(self) -> Result<Vec<T>> {
        self.collect()?
            .iter()
            .map(|row| T::from_row(&row.typed_row()).map_err(Error::Conversion))
            .collect()
    }

    pub fn cursor(&self) -> Option<&Cursor> {
        self.inner.cursor()
    }

    pub fn scroll(&mut self, to: Scroll) -> Result<()> {
        block_on_io(|cx| async move { self.inner.scroll(&cx, to).await })
    }
}

fn first_cursor_from_result(result: &QueryResult) -> Option<Cursor> {
    result
        .implicit_resultsets
        .as_ref()
        .and_then(|values| values.iter().find_map(cursor_from_value))
        .or_else(|| {
            result
                .rows
                .iter()
                .flat_map(|row| row.iter())
                .find_map(|cell| cell.as_ref().and_then(cursor_from_value))
        })
}

fn cursor_from_value(value: &QueryValue) -> Option<Cursor> {
    match value {
        QueryValue::Cursor(cursor) => Some((**cursor).clone()),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use oracledb_protocol::thin::{ColumnMetadata, CursorValue, QueryResult, QueryValue};

    use super::*;
    use crate::{ConversionError, FromRow, TypedRow};

    #[test]
    fn row_reuses_typed_row_conversion_path() {
        #[derive(Debug, Eq, PartialEq)]
        struct Named {
            id: i64,
            name: String,
        }

        impl FromRow for Named {
            fn from_row(row: &TypedRow<'_>) -> std::result::Result<Self, ConversionError> {
                Ok(Self {
                    id: row.try_get_by_name("id")?,
                    name: row.try_get_by_name("name")?,
                })
            }
        }

        let columns: Arc<[ColumnMetadata]> = Arc::from(
            vec![
                ColumnMetadata::new("ID", 0),
                ColumnMetadata::new("NAME", 0),
                ColumnMetadata::new("NICK", 0),
            ]
            .into_boxed_slice(),
        );
        let row = Row::new(
            columns,
            vec![
                Some(QueryValue::number_from_text("42", true)),
                Some(QueryValue::Text("alice".to_string())),
                None,
            ],
        );

        assert_eq!(row.get::<i64>(0).unwrap(), 42);
        assert_eq!(row.get::<i64>("id").unwrap(), 42);
        assert_eq!(row.get_by_name::<i64>("id").unwrap(), 42);
        assert_eq!(row.get::<String>(1).unwrap(), "alice");
        assert_eq!(row.get::<String>("NAME").unwrap(), "alice");
        assert_eq!(
            row.value("name").and_then(QueryValue::as_text),
            Some("alice")
        );
        assert_eq!(row.value(1).and_then(QueryValue::as_text), Some("alice"));
        assert_eq!(row.try_get::<String>(2).unwrap(), None);
        assert_eq!(row.try_get::<String>("nick").unwrap(), None);
        assert!(row.try_get::<String>(99).is_err());
        assert!(row.try_get::<String>("missing").is_err());
        assert_eq!(
            Named::from_row(&row.typed_row()).unwrap(),
            Named {
                id: 42,
                name: "alice".to_string()
            }
        );
    }

    #[test]
    fn execute_outcome_projects_query_result_fields() {
        let result = QueryResult {
            row_count: 7,
            last_rowid: Some("AAABBB".to_string()),
            out_values: vec![(0, Some(QueryValue::Text("out".to_string())))],
            return_values: vec![(1, vec![Some(QueryValue::number_from_text("42", true))])],
            implicit_resultsets: Some(vec![QueryValue::Cursor(Box::new(CursorValue {
                columns: Vec::new(),
                cursor_id: 99,
            }))]),
            compilation_error_warning: true,
            ..QueryResult::default()
        };

        let outcome = ExecuteOutcome::from_query_result(result);

        assert_eq!(outcome.rows_affected(), 7);
        assert_eq!(outcome.last_rowid(), Some("AAABBB"));
        assert_eq!(
            outcome.out_binds().get(0),
            Some(&Some(QueryValue::Text("out".to_string())))
        );
        assert_eq!(
            outcome
                .returning()
                .rows_for(1)
                .and_then(|rows| rows.first())
                .and_then(Option::as_ref)
                .and_then(QueryValue::as_i64),
            Some(42)
        );
        assert_eq!(outcome.implicit_results()[0].cursor_id, 99);
        assert_eq!(
            outcome.compilation_warning(),
            Some(ExecuteOutcome::COMPILATION_WARNING)
        );
    }

    #[test]
    fn batch_outcome_projects_query_result_fields() {
        let result = QueryResult {
            row_count: 3,
            batch_errors: vec![BatchServerError::new(1, 2, "bad row")],
            array_dml_row_counts: Some(vec![1, 0, 1]),
            return_values: vec![(0, vec![Some(QueryValue::Text("AAABBB".to_string()))])],
            ..QueryResult::default()
        };

        let outcome = BatchOutcome::from_query_result(result);

        assert_eq!(outcome.rows_affected(), 3);
        assert_eq!(outcome.per_row_counts(), Some([1, 0, 1].as_slice()));
        assert_eq!(outcome.errors()[0].row_index(), 2);
        assert_eq!(outcome.errors()[0].code(), 1);
        assert_eq!(outcome.errors()[0].message(), "bad row");
        assert_eq!(
            outcome
                .returning()
                .rows_for(0)
                .and_then(|rows| rows.first())
                .and_then(Option::as_ref)
                .and_then(QueryValue::as_text),
            Some("AAABBB")
        );
    }

    #[test]
    fn batch_outcome_coalesces_array_dml_returning_per_bind() {
        // Regression: array DML decodes RETURNING once per iteration, so a
        // single RETURNING bind (index 2) arrives as one group per affected
        // input row. BatchOutcome must coalesce groups that share a bind index
        // so rows_for(2) exposes every affected row's value, not just the first
        // iteration's. (Found by the W3-E7.4 live e2e suite.)
        let result = QueryResult {
            row_count: 2,
            array_dml_row_counts: Some(vec![1, 1]),
            return_values: vec![
                (2, vec![Some(QueryValue::Text("first".to_string()))]),
                (2, vec![Some(QueryValue::Text("second".to_string()))]),
            ],
            ..QueryResult::default()
        };

        let outcome = BatchOutcome::from_query_result(result);

        // One coalesced group for bind index 2 (not one group per iteration).
        assert_eq!(outcome.returning().len(), 1);
        let rows = outcome
            .returning()
            .rows_for(2)
            .expect("returning group for bind index 2");
        assert_eq!(
            rows.len(),
            2,
            "both affected rows' RETURNING values must be present"
        );
        assert_eq!(
            rows[0].as_ref().and_then(QueryValue::as_text),
            Some("first")
        );
        assert_eq!(
            rows[1].as_ref().and_then(QueryValue::as_text),
            Some("second")
        );
    }

    #[test]
    fn empty_batch_outcome_preserves_requested_row_counts_shape() {
        let without_counts = BatchOutcome::empty(false);
        let with_counts = BatchOutcome::empty(true);

        assert_eq!(without_counts.rows_affected(), 0);
        assert_eq!(without_counts.per_row_counts(), None);
        assert_eq!(with_counts.rows_affected(), 0);
        assert_eq!(with_counts.per_row_counts(), Some([].as_slice()));
    }

    #[test]
    fn registration_outcome_projects_query_id() {
        let with_id = RegistrationOutcome::from_query_result(QueryResult {
            query_id: Some(123),
            ..QueryResult::default()
        });
        let zero_id = RegistrationOutcome::from_query_result(QueryResult {
            query_id: Some(0),
            ..QueryResult::default()
        });
        let without_id = RegistrationOutcome::from_query_result(QueryResult::default());

        assert_eq!(with_id.query_id(), Some(123));
        assert_eq!(zero_id.query_id(), None);
        assert_eq!(without_id.query_id(), None);
    }
}
