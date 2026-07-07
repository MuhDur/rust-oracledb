//! Pure-Rust cursor execution policy that operates on statement text and bound
//! values, independent of any Python/PyO3 layer. These helpers were originally
//! inlined in the PyO3 shim; they live here so the `oracledb` crate is a
//! complete standalone driver and the shim can call them directly.

use crate::protocol::sql::statement_is_plsql;
use crate::protocol::thin::BindValue;

/// Error returned when constructing an [`ExecutemanyManager`] with invalid
/// inputs. The two variants mirror the two distinct failure modes the reference
/// batch-load manager rejects, so a Python adapter can raise the matching
/// exception type (`TypeError` for a zero batch size, `RuntimeError` for a row
/// count that overflows the wire's 32-bit field).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ExecutemanyManagerError {
    /// `batch_size` was zero; a batch must contain at least one row.
    ZeroBatchSize,
    /// A row count (the total, or a chunk length) did not fit in `u32`.
    RowCountOverflow,
}

impl std::fmt::Display for ExecutemanyManagerError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::ZeroBatchSize => f.write_str("batch_size must be greater than zero"),
            Self::RowCountOverflow => f.write_str("executemany row count exceeds u32"),
        }
    }
}

impl std::error::Error for ExecutemanyManagerError {}

/// Drives the per-batch row windowing of an `executemany` call: how many rows
/// each server round trip carries and where in the bound-row buffer it starts.
///
/// This is the reference `BatchLoadManager` arithmetic with no Python or PyO3
/// dependency. A batch never exceeds `batch_size` and never crosses a chunk
/// boundary (DataFrame ingestion of an Arrow chunked array splits the binds into
/// chunks; each chunk's trailing partial batch becomes its own round trip).
///
/// The adapter drives it by reading [`Self::num_rows`] / [`Self::message_offset`]
/// for the current batch, executing that window, then calling
/// [`Self::next_batch`] to advance.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExecutemanyManager {
    total_rows: u32,
    batch_size: u32,
    num_rows: u32,
    message_offset: u32,
    /// Cumulative row offsets of chunk boundaries. A batch never spans a
    /// boundary. Empty for plain (single-chunk) binds.
    chunk_ends: Vec<u32>,
}

impl ExecutemanyManager {
    /// Build a manager over `total_rows` rows with the given `batch_size` and no
    /// chunk boundaries (a single contiguous run of rows).
    pub fn new(total_rows: usize, batch_size: u32) -> Result<Self, ExecutemanyManagerError> {
        Self::with_chunks(total_rows, batch_size, Vec::new())
    }

    /// Build a manager whose rows are partitioned into chunks of the given
    /// `chunk_lengths` (cumulative boundaries no batch may cross). `total_rows`
    /// is the overall row count; it should equal the sum of `chunk_lengths` when
    /// chunks are supplied, and is the row count directly when they are empty.
    pub fn with_chunks(
        total_rows: usize,
        batch_size: u32,
        chunk_lengths: Vec<usize>,
    ) -> Result<Self, ExecutemanyManagerError> {
        let total_rows =
            u32::try_from(total_rows).map_err(|_| ExecutemanyManagerError::RowCountOverflow)?;
        if batch_size == 0 {
            return Err(ExecutemanyManagerError::ZeroBatchSize);
        }
        let mut chunk_ends = Vec::with_capacity(chunk_lengths.len());
        let mut acc: u32 = 0;
        for len in chunk_lengths {
            acc = acc.saturating_add(
                u32::try_from(len).map_err(|_| ExecutemanyManagerError::RowCountOverflow)?,
            );
            chunk_ends.push(acc);
        }
        let mut manager = Self {
            total_rows,
            batch_size,
            num_rows: 0,
            message_offset: 0,
            chunk_ends,
        };
        manager.num_rows = manager.batch_len_from(0);
        Ok(manager)
    }

    /// Number of rows in the batch starting at `offset`: at most `batch_size`,
    /// and never crossing the next chunk boundary.
    fn batch_len_from(&self, offset: u32) -> u32 {
        let remaining = self.total_rows.saturating_sub(offset);
        let mut len = remaining.min(self.batch_size);
        if let Some(next_end) = self.chunk_ends.iter().find(|&&end| end > offset) {
            len = len.min(next_end.saturating_sub(offset));
        }
        len
    }

    /// Number of rows in the batch the manager currently points at.
    pub fn num_rows(&self) -> u32 {
        self.num_rows
    }

    /// 0-based starting row offset of the current batch within the bound rows.
    pub fn message_offset(&self) -> u32 {
        self.message_offset
    }

    /// Advance to the next batch. After this, [`Self::num_rows`] is zero once all
    /// rows have been consumed.
    pub fn next_batch(&mut self) {
        self.message_offset = self.message_offset.saturating_add(self.num_rows);
        self.num_rows = self.batch_len_from(self.message_offset);
    }
}

/// Whether a bound value is an output (OUT / IN OUT / RETURNING) placeholder.
///
/// This mirrors the reference's notion of an output bind for the purpose of the
/// executemany strategy decision: explicit OUT binds, IN OUT binds,
/// DML-returning output binds, and DbObject output binds. Plain values and typed
/// NULLs are not outputs. An IN OUT bind returns a value per execution (the
/// server flags it `TNS_BIND_DIR_INPUT_OUTPUT`), so it must force the same
/// per-row accumulation an OUT bind does.
fn bind_value_is_output(value: &BindValue) -> bool {
    matches!(
        value,
        BindValue::Output { .. }
            | BindValue::ReturnOutput { .. }
            | BindValue::ObjectOutput { .. }
            | BindValue::InOut { .. }
    )
}

/// Whether an `executemany` over `bind_rows` must be driven one row at a time
/// (iterative) rather than as a single batched array execute.
///
/// The reference falls back to per-row execution for PL/SQL blocks that have
/// any output binds, because the server returns OUT bind values per execution
/// and they must be accumulated row by row. Plain DML (even with RETURNING)
/// stays on the batched array path.
pub fn bind_rows_need_iterative_plsql(statement: &str, bind_rows: &[Vec<BindValue>]) -> bool {
    statement_is_plsql(statement)
        && bind_rows
            .iter()
            .any(|row| row.iter().any(bind_value_is_output))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn output_bind() -> BindValue {
        BindValue::Output {
            ora_type_num: 1,
            csfrm: 1,
            buffer_size: 32,
        }
    }

    fn input_bind() -> BindValue {
        BindValue::Number("1".to_string())
    }

    #[test]
    fn plsql_with_output_bind_is_iterative() {
        let rows = vec![vec![input_bind(), output_bind()]];
        assert!(bind_rows_need_iterative_plsql(
            "begin :res := pkg.fn(:arg); end;",
            &rows
        ));
    }

    #[test]
    fn plsql_with_output_bind_in_any_row_is_iterative() {
        let rows = vec![
            vec![input_bind(), input_bind()],
            vec![input_bind(), output_bind()],
        ];
        assert!(bind_rows_need_iterative_plsql(
            "begin proc(:a, :b); end;",
            &rows
        ));
    }

    #[test]
    fn plsql_without_output_binds_is_batched() {
        let rows = vec![vec![input_bind()], vec![input_bind()]];
        assert!(!bind_rows_need_iterative_plsql(
            "begin proc(:a); end;",
            &rows
        ));
    }

    #[test]
    fn dml_with_output_binds_stays_batched() {
        // A non-PL/SQL statement is always batched, even with output binds
        // (e.g. DML RETURNING), because the array execute handles it directly.
        let rows = vec![vec![input_bind(), output_bind()]];
        assert!(!bind_rows_need_iterative_plsql(
            "insert into t (c) values (:c) returning id into :id",
            &rows
        ));
        assert!(!bind_rows_need_iterative_plsql(
            "update t set c = :c returning id into :id",
            &rows
        ));
    }

    #[test]
    fn empty_rows_are_batched() {
        assert!(!bind_rows_need_iterative_plsql("begin null; end;", &[]));
    }

    #[test]
    fn return_and_object_output_binds_count_as_outputs() {
        let return_rows = vec![vec![BindValue::ReturnOutput {
            ora_type_num: 1,
            csfrm: 1,
            buffer_size: 32,
        }]];
        assert!(bind_rows_need_iterative_plsql(
            "declare begin :x := 1; end;",
            &return_rows
        ));
    }

    /// Drive the manager to exhaustion, collecting `(message_offset, num_rows)`
    /// for each non-empty batch — exactly how the adapter walks it.
    fn drain(mut manager: ExecutemanyManager) -> Vec<(u32, u32)> {
        let mut batches = Vec::new();
        while manager.num_rows() > 0 {
            batches.push((manager.message_offset(), manager.num_rows()));
            manager.next_batch();
        }
        batches
    }

    #[test]
    fn batches_split_evenly() {
        let manager = ExecutemanyManager::new(6, 2).unwrap();
        assert_eq!(drain(manager), vec![(0, 2), (2, 2), (4, 2)]);
    }

    #[test]
    fn final_batch_carries_the_remainder() {
        let manager = ExecutemanyManager::new(7, 3).unwrap();
        assert_eq!(drain(manager), vec![(0, 3), (3, 3), (6, 1)]);
    }

    #[test]
    fn single_batch_when_batch_size_covers_all_rows() {
        let manager = ExecutemanyManager::new(4, 10).unwrap();
        assert_eq!(manager.num_rows(), 4);
        assert_eq!(manager.message_offset(), 0);
        assert_eq!(drain(manager), vec![(0, 4)]);
    }

    #[test]
    fn zero_total_rows_yields_no_batches() {
        let manager = ExecutemanyManager::new(0, 5).unwrap();
        assert_eq!(manager.num_rows(), 0);
        assert!(drain(manager).is_empty());
    }

    #[test]
    fn zero_batch_size_is_rejected() {
        assert_eq!(
            ExecutemanyManager::new(3, 0),
            Err(ExecutemanyManagerError::ZeroBatchSize)
        );
    }

    #[test]
    fn row_count_overflow_is_rejected() {
        let too_many = (u32::MAX as usize) + 1;
        assert_eq!(
            ExecutemanyManager::new(too_many, 1),
            Err(ExecutemanyManagerError::RowCountOverflow)
        );
    }

    #[test]
    fn batches_never_cross_a_chunk_boundary() {
        // Two chunks of 3 rows each, batch size 2: the first chunk yields a
        // 2-row batch then a trailing 1-row batch (the partial that would have
        // spilled into the second chunk), then the second chunk repeats.
        let manager = ExecutemanyManager::with_chunks(6, 2, vec![3, 3]).unwrap();
        assert_eq!(drain(manager), vec![(0, 2), (2, 1), (3, 2), (5, 1)]);
    }

    #[test]
    fn chunk_boundary_aligned_with_batch_size_does_not_split() {
        // Chunks aligned to the batch size produce no extra partial batches.
        let manager = ExecutemanyManager::with_chunks(4, 2, vec![2, 2]).unwrap();
        assert_eq!(drain(manager), vec![(0, 2), (2, 2)]);
    }
}
