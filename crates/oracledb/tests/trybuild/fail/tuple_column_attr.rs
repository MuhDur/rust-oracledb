use oraclemcp_driver_cx::FromRow;

#[derive(FromRow)]
struct Row(#[driver_cx(column = "ID")] i64);

fn main() {}
