//! Pure-Rust cursor execution policy that operates on statement text and bound
//! values, independent of any Python/PyO3 layer. These helpers were originally
//! inlined in the PyO3 shim; they live here so the `oracledb` crate is a
//! complete standalone driver and the shim can call them directly.

use crate::protocol::sql::statement_is_plsql;
use crate::protocol::thin::BindValue;

/// Whether a bound value is an output (OUT / IN OUT / RETURNING) placeholder.
///
/// This mirrors the reference's notion of an output bind for the purpose of the
/// executemany strategy decision: explicit OUT binds, DML-returning output
/// binds, and DbObject output binds. Plain values and typed NULLs are not
/// outputs.
fn bind_value_is_output(value: &BindValue) -> bool {
    matches!(
        value,
        BindValue::Output { .. } | BindValue::ReturnOutput { .. } | BindValue::ObjectOutput { .. }
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
}
