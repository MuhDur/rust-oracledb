use oraclemcp_driver_cx::FromRow;

#[derive(FromRow)]
struct NamedRow {
    id: i64,
    name: String,
    manager_id: Option<i64>,
}

#[derive(FromRow)]
#[driver_cx(rename_all = "SCREAMING_SNAKE_CASE")]
struct RenameAllRow {
    employee_id: i64,
    full_name: String,
}

#[derive(FromRow)]
struct FieldOverrideRow {
    #[driver_cx(column = "EMPNO")]
    id: i64,
    #[driver_cx(rename = "ENAME")]
    name: String,
}

#[derive(FromRow)]
struct TupleRow(i64, Option<String>);

fn assert_from_row<T: oraclemcp_driver_cx::FromRow>() {}

fn main() {
    assert_from_row::<NamedRow>();
    assert_from_row::<RenameAllRow>();
    assert_from_row::<FieldOverrideRow>();
    assert_from_row::<TupleRow>();
}
