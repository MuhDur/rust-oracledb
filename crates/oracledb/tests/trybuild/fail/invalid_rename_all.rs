use oracledb::FromRow;

#[derive(FromRow)]
#[oracledb(rename_all = "kebab-case")]
struct Row {
    employee_id: i64,
}

fn main() {}
