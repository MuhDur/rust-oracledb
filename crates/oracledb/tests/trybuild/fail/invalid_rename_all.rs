use oraclemcp_driver_cx::FromRow;

#[derive(FromRow)]
#[driver_cx(rename_all = "kebab-case")]
struct Row {
    employee_id: i64,
}

fn main() {}
