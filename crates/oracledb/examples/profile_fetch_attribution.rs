//! Profiling-only: attribute a single-connection multi-page paged fetch into
//! socket-read time vs CPU-decode time, using the crate's `fetch_profile_*`
//! instrumentation. This is the BASELINE measurement that decides whether
//! overlapping the wire read with the decode (bead rust-oracledb-xad / 3oi) can
//! win anything: overlap can only hide whichever of {read, decode} is the
//! smaller per-page cost.
//!
//! Run against the container:
//!
//! ```sh
//! eval "$(ORACLEDB_CONTAINER_NAME=rust-oracledb-lane-1523 \
//!         ORACLEDB_HOST_PORT=1523 scripts/container.sh env)"
//! cargo run -p oracledb --example profile_fetch_attribution --release
//! ```

use asupersync::runtime::{reactor, RuntimeBuilder};
use asupersync::Cx;
use oracledb::protocol::thin::QueryValue;
use oracledb::{ConnectOptions, Connection};
use oracledb_protocol::ClientIdentity;

fn connect_options() -> Option<ConnectOptions> {
    let connect_string = std::env::var("PYO_TEST_CONNECT_STRING").ok()?;
    let user = std::env::var("PYO_TEST_MAIN_USER").ok()?;
    let password = std::env::var("PYO_TEST_MAIN_PASSWORD").ok()?;
    let identity = ClientIdentity::new(
        "rust-oracledb-profile",
        "profile-machine",
        "profile-osuser",
        "profile-terminal",
        "rust-oracledb thn : 0.0.0",
    )
    .ok()?;
    Some(ConnectOptions::new(
        connect_string,
        user,
        password,
        identity,
    ))
}

async fn fetch_all(cx: &Cx, conn: &mut Connection, sql: &str, arraysize: u32) -> usize {
    let first = conn
        .execute_query_with_bind_rows(cx, sql, arraysize, &[])
        .await
        .expect("execute");
    let cursor_id = first.cursor_id;
    let mut total = first.rows.len();
    let mut more_rows = first.more_rows;
    let mut previous_row: Option<Vec<Option<QueryValue>>> = first.rows.last().cloned();
    while more_rows && cursor_id != 0 {
        let batch = conn
            .fetch_rows(cx, cursor_id, arraysize, previous_row.as_deref())
            .await
            .expect("fetch_rows page");
        total += batch.rows.len();
        more_rows = batch.more_rows;
        if let Some(last) = batch.rows.last().cloned() {
            previous_row = Some(last);
        }
    }
    conn.release_cursor(cursor_id);
    total
}

fn main() {
    let Some(options) = connect_options() else {
        eprintln!("skipped: PYO_TEST_* not set");
        return;
    };
    let reactor = reactor::create_reactor().expect("reactor");
    let runtime = RuntimeBuilder::current_thread()
        .with_reactor(reactor)
        .build()
        .expect("runtime");

    runtime.block_on(async {
        let cx = Cx::current().expect("cx");
        let mut conn = Connection::connect(&cx, options).await.expect("connect");

        // Wide-ish, many-page result: 50k rows of a small int, arraysize 1000 =>
        // ~50 pages so the per-page read/decode split is well-sampled.
        let sql = "select level as n from dual connect by level <= 50000";
        let arraysize = 1000u32;

        // Warm once (statement cache + server-side parse) so the measured run is
        // steady-state.
        let _ = fetch_all(&cx, &mut conn, sql, arraysize).await;

        oracledb::fetch_profile_arm(true);
        oracledb::fetch_profile_reset();

        let iters = 20u32;
        let mut total_rows = 0usize;
        let wall_start = asupersync::time::wall_now();
        for _ in 0..iters {
            total_rows += fetch_all(&cx, &mut conn, sql, arraysize).await;
        }
        let wall_ns = asupersync::time::wall_now().duration_since(wall_start);
        oracledb::fetch_profile_arm(false);

        let (read_ns, decode_ns) = oracledb::fetch_profile_read_decode_ns();
        // Pages per iter: ceil(50000/1000) = 50, minus the first page which the
        // execute round trip returns (so 49 paged fetch_rows calls per iter).
        let pages = 49u64 * u64::from(iters);
        println!("=== fetch read/decode attribution (50k rows, arraysize 1000) ===");
        println!(
            "iters={iters}  rows/iter={}  paged_fetch_calls={pages}",
            total_rows / iters as usize
        );
        println!("wall total      : {:.3} ms", wall_ns as f64 / 1e6);
        println!(
            "wall / iter     : {:.3} ms",
            wall_ns as f64 / 1e6 / iters as f64
        );
        println!(
            "read  total     : {:.3} ms  ({:.1}%)",
            read_ns as f64 / 1e6,
            100.0 * read_ns as f64 / (read_ns + decode_ns) as f64
        );
        println!(
            "decode total    : {:.3} ms  ({:.1}%)",
            decode_ns as f64 / 1e6,
            100.0 * decode_ns as f64 / (read_ns + decode_ns) as f64
        );
        println!(
            "read  / page    : {:.1} us",
            read_ns as f64 / 1e3 / pages as f64
        );
        println!(
            "decode / page   : {:.1} us",
            decode_ns as f64 / 1e3 / pages as f64
        );
        println!();
        println!("overlap ceiling : hiding min(read,decode) per page would save up to");
        let min_per_page = read_ns.min(decode_ns) as f64 / 1e3 / pages as f64;
        println!(
            "                  ~{:.1} us/page = {:.1}% of (read+decode)",
            min_per_page,
            100.0 * read_ns.min(decode_ns) as f64 / (read_ns + decode_ns) as f64
        );

        conn.close(&cx).await.expect("close");
    });
}
