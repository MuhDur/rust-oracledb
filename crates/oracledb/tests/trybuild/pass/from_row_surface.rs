use oracledb::FromRow;

#[derive(FromRow)]
struct NamedRow {
    id: i64,
    name: String,
    manager_id: Option<i64>,
}

#[derive(FromRow)]
#[oracledb(rename_all = "SCREAMING_SNAKE_CASE")]
struct RenameAllRow {
    employee_id: i64,
    full_name: String,
}

#[derive(FromRow)]
struct FieldOverrideRow {
    #[oracledb(column = "EMPNO")]
    id: i64,
    #[oracledb(rename = "ENAME")]
    name: String,
}

#[derive(FromRow)]
struct TupleRow(i64, Option<String>);

fn assert_from_row<T: oracledb::FromRow>() {}

fn main() {
    assert_from_row::<NamedRow>();
    assert_from_row::<RenameAllRow>();
    assert_from_row::<FieldOverrideRow>();
    assert_from_row::<TupleRow>();
}
