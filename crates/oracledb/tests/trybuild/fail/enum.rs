use oraclemcp_driver_cx::FromRow;

#[derive(FromRow)]
enum Row {
    Named { id: i64 },
}

fn main() {}
