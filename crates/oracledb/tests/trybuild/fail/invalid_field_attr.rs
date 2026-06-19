use oracledb::FromRow;

#[derive(FromRow)]
struct Row {
    #[oracledb(foo = "ID")]
    id: i64,
}

fn main() {}
