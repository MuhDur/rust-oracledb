use oraclemcp_driver_cx::FromRow;

#[derive(FromRow)]
struct Row {
    #[driver_cx(foo = "ID")]
    id: i64,
}

fn main() {}
