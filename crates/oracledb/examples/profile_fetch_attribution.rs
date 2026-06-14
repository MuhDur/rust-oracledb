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
use oracledb::protocol::thin::{QueryValue, QueryValueRef};
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

        // --------------------------------------------------------------------
        // Direct A/B on the borrowed paths: SERIAL (fetch_rows_ref) vs
        // PREFETCHED (for_each_row_ref). The read counter now measures the
        // *read-wait* — how long the read .await blocks once we reach it. If the
        // prefetch genuinely overlaps the server round trip with the decode, the
        // read-wait per page should DROP (the bytes are already in the kernel
        // buffer by the time we await), even though wall time may move little on
        // loopback where the hideable latency is small.
        // --------------------------------------------------------------------
        println!("\n=== borrowed A/B: serial fetch_rows_ref vs prefetched for_each_row_ref ===");
        // Interleave serial/prefetched rounds so any drift hits both equally;
        // collect per-round read-wait so we can report the median (robust on a
        // noisy loopback).
        let rounds = 15u32;
        let ab_iters = 20u32;
        let pages_ab = 49.0 * f64::from(ab_iters);
        let mut s_reads = Vec::new();
        let mut p_reads = Vec::new();
        let mut s_walls = Vec::new();
        let mut p_walls = Vec::new();

        oracledb::fetch_profile_arm(true);
        for _ in 0..rounds {
            // SERIAL round
            oracledb::fetch_profile_reset();
            let s_start = asupersync::time::wall_now();
            for _ in 0..ab_iters {
                let first = conn
                    .execute_query_with_bind_rows(&cx, sql, arraysize, &[])
                    .await
                    .expect("exec");
                let cursor_id = first.cursor_id;
                let mut more = first.more_rows;
                let mut prev: Option<Vec<Option<QueryValue>>> = first.rows.last().cloned();
                while more && cursor_id != 0 {
                    let batch = conn
                        .fetch_rows_ref(&cx, cursor_id, arraysize, prev.as_deref())
                        .await
                        .expect("serial page");
                    more = batch.more_rows;
                    let mut last: Option<Vec<Option<QueryValue>>> = None;
                    batch
                        .batch
                        .for_each_row_ref(|row| {
                            last =
                                Some(row.iter().map(|c| c.map(|v| v.to_owned_value())).collect());
                            std::hint::black_box(&last);
                            Ok::<(), oracledb::Error>(())
                        })
                        .expect("iter");
                    if let Some(l) = last {
                        prev = Some(l);
                    }
                }
                conn.release_cursor(cursor_id);
            }
            s_walls.push(asupersync::time::wall_now().duration_since(s_start) as f64 / 1e6 / f64::from(ab_iters));
            let (s_read, _) = oracledb::fetch_profile_read_decode_ns();
            s_reads.push(s_read as f64 / 1e3 / pages_ab);

            // PREFETCHED round
            oracledb::fetch_profile_reset();
            let p_start = asupersync::time::wall_now();
            for _ in 0..ab_iters {
                conn.for_each_row_ref(&cx, sql, arraysize, |row: &[Option<QueryValueRef<'_>>]| {
                    for cell in row {
                        std::hint::black_box(cell.map(|v| v.to_owned_value()));
                    }
                    Ok(())
                })
                .await
                .expect("prefetched");
            }
            p_walls.push(asupersync::time::wall_now().duration_since(p_start) as f64 / 1e6 / f64::from(ab_iters));
            let (p_read, _) = oracledb::fetch_profile_read_decode_ns();
            p_reads.push(p_read as f64 / 1e3 / pages_ab);
        }
        oracledb::fetch_profile_arm(false);

        let median = |v: &mut Vec<f64>| {
            v.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
            v[v.len() / 2]
        };
        let s_read_med = median(&mut s_reads);
        let p_read_med = median(&mut p_reads);
        let s_wall_med = median(&mut s_walls);
        let p_wall_med = median(&mut p_walls);
        println!("(median of {rounds} interleaved rounds, {ab_iters} iters/round)");
        println!("                    serial        prefetched     delta");
        println!(
            "wall / iter (ms)  : {:>8.3}     {:>8.3}      {:+.1}%",
            s_wall_med,
            p_wall_med,
            100.0 * (p_wall_med - s_wall_med) / s_wall_med
        );
        println!(
            "read-wait / page  : {:>8.1} us  {:>8.1} us   {:+.1}%  <- overlap hides server round trip",
            s_read_med,
            p_read_med,
            100.0 * (p_read_med - s_read_med) / s_read_med
        );

        // --------------------------------------------------------------------
        // REALISTIC CONSUMER: a real caller does work per row (parse/transform/
        // serialize). When the per-row decode+callback work is non-trivial, the
        // overlap hides MORE of the read-wait behind it, so the wall time wins
        // even on loopback. We simulate ~5 us/row of CPU work (a small hash) so
        // a page's worth of decode+work (~5 ms for 1000 rows) comfortably covers
        // the server round trip. This is the regime real network RTT also lands
        // in (read-wait >> bookkeeping), so it is the honest "does the overlap
        // pay off" measurement.
        // --------------------------------------------------------------------
        fn per_row_work(seed: u64) -> u64 {
            // Cheap deterministic CPU work (~a few hundred ns) per call; called a
            // few times per row to reach ~us scale.
            let mut h = seed.wrapping_add(0x9E3779B97F4A7C15);
            for _ in 0..40 {
                h ^= h >> 30;
                h = h.wrapping_mul(0xBF58476D1CE4E5B9);
                h ^= h >> 27;
            }
            h
        }

        println!("\n=== with ~realistic per-row CPU work (consumer does work per row) ===");
        let mut sw_walls = Vec::new();
        let mut pw_walls = Vec::new();
        for _ in 0..rounds {
            // SERIAL + work
            let s_start = asupersync::time::wall_now();
            for _ in 0..ab_iters {
                let first = conn
                    .execute_query_with_bind_rows(&cx, sql, arraysize, &[])
                    .await
                    .expect("exec");
                let cursor_id = first.cursor_id;
                let mut more = first.more_rows;
                let mut prev: Option<Vec<Option<QueryValue>>> = first.rows.last().cloned();
                let mut acc = 0u64;
                while more && cursor_id != 0 {
                    let batch = conn
                        .fetch_rows_ref(&cx, cursor_id, arraysize, prev.as_deref())
                        .await
                        .expect("serial page");
                    more = batch.more_rows;
                    let mut last: Option<Vec<Option<QueryValue>>> = None;
                    batch
                        .batch
                        .for_each_row_ref(|row| {
                            acc = acc.wrapping_add(per_row_work(acc ^ row.len() as u64));
                            last =
                                Some(row.iter().map(|c| c.map(|v| v.to_owned_value())).collect());
                            Ok::<(), oracledb::Error>(())
                        })
                        .expect("iter");
                    if let Some(l) = last {
                        prev = Some(l);
                    }
                }
                std::hint::black_box(acc);
                conn.release_cursor(cursor_id);
            }
            sw_walls.push(asupersync::time::wall_now().duration_since(s_start) as f64 / 1e6 / f64::from(ab_iters));

            // PREFETCHED + work
            let p_start = asupersync::time::wall_now();
            for _ in 0..ab_iters {
                let mut acc = 0u64;
                conn.for_each_row_ref(&cx, sql, arraysize, |row: &[Option<QueryValueRef<'_>>]| {
                    acc = acc.wrapping_add(per_row_work(acc ^ row.len() as u64));
                    std::hint::black_box(&acc);
                    Ok(())
                })
                .await
                .expect("prefetched");
                std::hint::black_box(acc);
            }
            pw_walls.push(asupersync::time::wall_now().duration_since(p_start) as f64 / 1e6 / f64::from(ab_iters));
        }
        let sw_med = median(&mut sw_walls);
        let pw_med = median(&mut pw_walls);
        println!(
            "wall / iter (ms)  : {:>8.3}     {:>8.3}      {:+.1}%  <- overlap pays off when consumer works",
            sw_med,
            pw_med,
            100.0 * (pw_med - sw_med) / sw_med
        );

        conn.close(&cx).await.expect("close");
    });
}
