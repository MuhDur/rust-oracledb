//! Build-only minimal binary for the musl size gate.
//!
//! This constructs the smallest realistic public surface we want represented in
//! a deployable executable: connect options, a query builder, typed binds, and
//! bounded fetch/LOB options. It deliberately does not open a socket.

use std::num::NonZeroU32;

use oracledb::protocol::ClientIdentity;
use oracledb::{params, ConnectOptions, Query};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let identity = ClientIdentity::new(
        "oracledb-min-connect",
        "build-host",
        "build-user",
        "build-term",
        "rust-oracledb",
    )?;
    let options = ConnectOptions::new(
        "localhost:1521/FREEPDB1",
        "app_user",
        "app_password",
        identity,
    )
    .with_statement_cache_size(4);

    let query = Query::new("select :id as id, :label as label from dual")
        .bind(params! { ":id" => 42_i64, ":label" => "probe" })
        .arraysize(NonZeroU32::new(16).expect("sixteen is non-zero"))
        .prefetch(16)
        .stream_lobs();
    let positional = params![42_i64, "probe", true];

    std::hint::black_box((options, query, positional));
    Ok(())
}
