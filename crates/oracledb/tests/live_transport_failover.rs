//! Live transport tests for bead `rust-oracledb-clvm` (connect-string / HA
//! transport feature gaps):
//!
//! * F1 — a DSN-set transport connect timeout is applied: a short timeout
//!   against a black-hole address actually times out at that bound instead of
//!   hanging on the hard-coded default.
//! * F2 — multi-address failover: a DESCRIPTION whose first ADDRESS is dead and
//!   whose second is a live listener connects via the second; a live-first
//!   descriptor connects via the first with no wasted attempt.
//!
//! All tests are `#[ignore]` and driven by env vars so they only run against a
//! real listener (the `scripts/version_matrix.sh` lanes). The failover tests
//! target the xe18 lane by default:
//!
//! ```bash
//! FAILOVER_HOST=127.0.0.1 FAILOVER_PORT=1518 FAILOVER_SERVICE=XEPDB1 \
//! FAILOVER_USER=testuser FAILOVER_PASSWORD=testpw \
//!   cargo test -p oracledb --test live_transport_failover -- --ignored --nocapture
//! ```

use std::time::{Duration, Instant};

use asupersync::runtime::{reactor, RuntimeBuilder};
use asupersync::Cx;
use oracledb::{ConnectOptions, Connection, Error};
use oracledb_protocol::ClientIdentity;

fn identity() -> ClientIdentity {
    ClientIdentity::new(
        "rust-oracledb",
        "rusthost",
        "rustuser",
        "rustterm",
        "rust-oracledb thn : 0.0.0",
    )
    .expect("identity")
}

fn env(name: &str, default: &str) -> String {
    std::env::var(name).unwrap_or_else(|_| default.to_string())
}

fn block_on<F, T>(fut: F) -> T
where
    F: std::future::Future<Output = T>,
{
    let reactor = reactor::create_reactor().expect("native reactor");
    let runtime = RuntimeBuilder::current_thread()
        .with_reactor(reactor)
        .build()
        .expect("runtime");
    runtime.block_on(async move {
        let _cx = Cx::current().expect("ambient Cx");
        fut.await
    })
}

/// F1: a DSN connect timeout of 2s against an unroutable (black-hole) address
/// (RFC 5737 TEST-NET-1, whose SYNs are dropped) must time out at ~2s, not hang
/// on the 20s default.
#[test]
#[ignore = "requires a network stack that drops SYNs to 192.0.2.1 (RFC5737 TEST-NET-1)"]
fn f1_dsn_connect_timeout_bounds_a_black_hole_dial() {
    let dsn = "(DESCRIPTION=(CONNECT_TIMEOUT=2)\
               (ADDRESS=(PROTOCOL=tcp)(HOST=192.0.2.1)(PORT=1521))\
               (CONNECT_DATA=(SERVICE_NAME=XEPDB1)))";
    let started = Instant::now();
    let result = block_on(async {
        Connection::connect(
            &Cx::current().expect("cx"),
            ConnectOptions::new(dsn, "testuser", "testpw", identity()),
        )
        .await
    });
    let elapsed = started.elapsed();
    let err = result.expect_err("black-hole dial must not connect");
    // The 2s DSN bound must dominate: comfortably under the 20s default, and at
    // least ~1.5s (proving it is the DSN value, not an instant failure).
    assert!(
        elapsed < Duration::from_secs(8),
        "connect should time out near the 2s DSN bound, took {elapsed:?} ({err})"
    );
    assert!(
        elapsed >= Duration::from_millis(1500),
        "should wait for the ~2s bound, took {elapsed:?}"
    );
    // A dropped-SYN dial surfaces as a bounded transport failure, never a
    // hang: the per-address dial times out at the DSN bound and — this being
    // the sole address — the failover exhausts into `AllAddressesFailed`
    // (which names the underlying tcp connect timeout); a stalled-after-accept
    // server would instead surface `CallTimeout`. Either way it is bounded.
    match &err {
        Error::AllAddressesFailed(detail) => assert!(
            detail.contains("timeout"),
            "aggregated failure should name the timeout, got {detail:?}"
        ),
        Error::CallTimeout(_) | Error::Io(_) => {}
        other => panic!("expected a bounded timeout-class error, got {other:?}"),
    }
}

/// F2: first ADDRESS is a dead port (127.0.0.1:1), second is the live lane.
/// Failover must reach the live listener via the second address.
#[test]
#[ignore = "requires the xe18 live lane (scripts/version_matrix.sh up xe18)"]
fn f2_failover_dead_first_live_second() {
    let host = env("FAILOVER_HOST", "127.0.0.1");
    let port = env("FAILOVER_PORT", "1518");
    let service = env("FAILOVER_SERVICE", "XEPDB1");
    let user = env("FAILOVER_USER", "testuser");
    let password = env("FAILOVER_PASSWORD", "testpw");
    let dsn = format!(
        "(DESCRIPTION=(ADDRESS_LIST=\
         (ADDRESS=(PROTOCOL=tcp)(HOST=127.0.0.1)(PORT=1))\
         (ADDRESS=(PROTOCOL=tcp)(HOST={host})(PORT={port})))\
         (CONNECT_DATA=(SERVICE_NAME={service})))"
    );
    let session_id = block_on(async {
        let cx = Cx::current().expect("cx");
        let conn = Connection::connect(&cx, ConnectOptions::new(dsn, user, password, identity()))
            .await
            .expect("failover to the live second address must connect");
        let sid = conn.session_id();
        conn.close(&cx).await.expect("logoff");
        sid
    });
    assert!(session_id > 0, "connected session must have a real SID");
}

/// F2: live-first descriptor connects via the first address (no wasted attempt
/// on the dead trailing one — a successful first dial short-circuits).
#[test]
#[ignore = "requires the xe18 live lane (scripts/version_matrix.sh up xe18)"]
fn f2_failover_live_first_uses_first() {
    let host = env("FAILOVER_HOST", "127.0.0.1");
    let port = env("FAILOVER_PORT", "1518");
    let service = env("FAILOVER_SERVICE", "XEPDB1");
    let user = env("FAILOVER_USER", "testuser");
    let password = env("FAILOVER_PASSWORD", "testpw");
    let dsn = format!(
        "(DESCRIPTION=(ADDRESS_LIST=\
         (ADDRESS=(PROTOCOL=tcp)(HOST={host})(PORT={port}))\
         (ADDRESS=(PROTOCOL=tcp)(HOST=127.0.0.1)(PORT=1)))\
         (CONNECT_DATA=(SERVICE_NAME={service})))"
    );
    let started = Instant::now();
    let session_id = block_on(async {
        let cx = Cx::current().expect("cx");
        let conn = Connection::connect(&cx, ConnectOptions::new(dsn, user, password, identity()))
            .await
            .expect("live first address must connect");
        let sid = conn.session_id();
        conn.close(&cx).await.expect("logoff");
        sid
    });
    let elapsed = started.elapsed();
    assert!(session_id > 0, "connected session must have a real SID");
    // A live-first connect must not pay a failover penalty for the trailing
    // dead address (it is never dialled). Generous bound to tolerate slow DBs.
    assert!(
        elapsed < Duration::from_secs(15),
        "live-first connect should be prompt, took {elapsed:?}"
    );
}
