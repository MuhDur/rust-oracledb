use oracledb::FromRow;

#[derive(FromRow)]
enum Row {
    Named { id: i64 },
}

fn main() {}
