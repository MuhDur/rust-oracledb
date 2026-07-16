# oracledb

**A pure-Rust, async, thin-mode Oracle Database driver.** A clean-room port of
python-oracledb v4.0.1 thin mode that passes the reference's own test suite, with
no Oracle Instant Client, no OCI, and no C library at runtime.

[![License: MIT OR Apache-2.0](https://img.shields.io/badge/license-MIT%20OR%20Apache--2.0-blue.svg)](https://github.com/MuhDur/rust-oracledb#license)
[![Rust: nightly](https://img.shields.io/badge/rust-nightly-orange.svg)](https://www.rust-lang.org)
[![unsafe: forbidden](https://img.shields.io/badge/unsafe-forbidden-success.svg)](https://github.com/MuhDur/rust-oracledb)

`oracledb` speaks the Oracle TNS/TTC wire protocol directly over TCP. You add the
crate, point it at a listener, and connect: no Instant Client to install, no
shared libraries to ship. It is a faithful re-implementation of the
python-oracledb thin client, so its behaviour tracks that reference, verified by
running python-oracledb's **own** thin-mode test suite against the Rust engine.

> This is an independent project and is not affiliated with Oracle. "Oracle" and
> "python-oracledb" are referenced here only to describe what this driver is
> compatible with.

## Highlights

- **GIL-free concurrent decode** — N connections decode the wire on N cores.
- **Typed rows** — `#[derive(FromRow)]` with compile-checked field types.
- **Structured errors and binds** — `Error::ora_code()` / `is_retryable()`,
  `FromSql` / `ToSql`, the `params!` macro, and compiler-style caret diagnostics
  for parse errors (`Error::caret`).
- **Beyond basic queries** — REF CURSOR / implicit result sets
  (`fetch_cursor`), structured ADT object & collection decode
  (`describe_object_type` / `decode_object`), `DBMS_OUTPUT` capture
  (`enable_dbms_output` / `read_dbms_output`), edition selection
  (`with_edition`), per-call timeouts on positional *and* named binds
  (`query_named_with_timeout`), and OCI IAM / OAuth2 token auth
  (`with_access_token`).
- **Tunable connection** — configurable statement-cache size
  (`ConnectOptions::with_statement_cache_size`, `0` disables caching).
- **Tiny deployment** — a single static binary; `FROM scratch` images, no
  interpreter, no native client.
- **`#![forbid(unsafe_code)]`** in the driver and protocol crates; fuzzed and
  OOM-closed by construction.

## Quick example

```rust
use oracledb::{BlockingConnection, ConnectOptions, FromRow, QueryResultExt};
use oracledb::protocol::ClientIdentity;

#[derive(FromRow)]
struct Emp {
    id: i64,
    name: String,
    manager_id: Option<i64>, // nullable column -> Option
}

fn main() -> Result<(), oracledb::Error> {
    let identity = ClientIdentity::new(
        "billing-worker", // program
        "edge-pod-7",     // machine
        "tenant-42",      // osuser
        "shard-a",        // terminal
        "rust-oracledb",  // driver name
    )?;

    let options = ConnectOptions::new(
        "dbhost:1521/FREEPDB1", // EasyConnect string
        "app_user",
        "app_password",
        identity,
    );

    let mut conn = BlockingConnection::connect(options)?;

    let emps: Vec<Emp> = BlockingConnection::query(
        &mut conn,
        "select id, name, manager_id from emp where dept = :1",
        (40,),
    )?
    .into_typed()?;

    for e in &emps {
        println!("{}: {} (mgr {:?})", e.id, e.name, e.manager_id);
    }

    BlockingConnection::close(conn)?;
    Ok(())
}
```

The synchronous `BlockingConnection` facade is an ordinary `main()` with no
visible runtime. The async API is identical minus the blocking wrapper.

## Feature flags

| feature | default | what it adds |
|---|---|---|
| `derive` | yes | `#[derive(FromRow)]` (the `oracledb-derive` proc-macro) |
| `arrow` | no | Apache Arrow row ingest |
| `chrono` / `uuid` / `serde_json` / `rust_decimal` | no | typed `FromSql` / `ToSql` bridges |
| `soda` | no | experimental thin-mode SODA |
| `tracing` | no | OpenTelemetry-style spans (zero-cost when off) |
| `cassette` | no | `.tns-cassette` record / replay transport seam |
| `experimental` | no | legacy compatibility no-op; wallet readers are always available |

## Documentation and source

Full documentation, the parity methodology, performance numbers, deployment
guide, and the safety audit live in the repository:

<https://github.com/MuhDur/rust-oracledb>

## License

Licensed under either of

- Apache License, Version 2.0 (<http://www.apache.org/licenses/LICENSE-2.0>)
- MIT license (<http://opensource.org/licenses/MIT>)

at your option.
