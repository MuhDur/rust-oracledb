use oracledb::FromRow;

#[derive(FromRow)]
struct Row(#[oracledb(column = "ID")] i64);

fn main() {}
