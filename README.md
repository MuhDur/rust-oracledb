# rust-oracledb

**A pure-Rust, async, thin-mode Oracle Database driver. A clean-room port of
python-oracledb v4.0.1 thin mode that passes the reference's own test suite, with
no Oracle Instant Client, no OCI, and no C library at runtime.**

[![License: MIT OR Apache-2.0](https://img.shields.io/badge/license-MIT%20OR%20Apache--2.0-blue.svg)](#license)
[![Rust 1.83+](https://img.shields.io/badge/rust-1.83%2B-orange.svg)](https://www.rust-lang.org)
[![unsafe: forbidden](https://img.shields.io/badge/unsafe-forbidden-success.svg)](#robustness)

`rust-oracledb` speaks the Oracle TNS/TTC wire protocol directly over TCP. You
add the crate, point it at a listener, and connect; no Instant Client to
install, no shared libraries to ship. It is a faithful re-implementation of the
python-oracledb thin client, so its behaviour tracks that reference, verified by
running python-oracledb's **own** thin-mode test suite against the Rust engine.

> This is an independent project and is not affiliated with Oracle. "Oracle" and
> "python-oracledb" are referenced here only to describe what this driver is
> compatible with.

---

## TL;DR

**The problem.** The Oracle client landscape forces a trade-off. The thick
drivers (OCI / Instant Client, and the Rust `rust-oracle` crate that binds to
ODPI-C) pull in hundreds of megabytes of native libraries you must install and
version-match at runtime. python-oracledb's pure-Python thin mode avoids that,
but it runs the wire codec in Python under the GIL and ships an interpreter.

**The solution.** A thin-mode driver written entirely in Rust. It implements the
TNS/TTC protocol itself, so an application using it compiles to a single static
binary, decodes the wire in parallel across cores (no GIL), fails closed against
hostile input (fuzzed), and maps rows into compile-checked Rust types.

**What "parity" means here, precisely.** This is a **conformance** parity, not a
"drop-in / production-ready / Oracle-certified" claim. The yardstick is
python-oracledb's own pytest suite, in thin mode, against Oracle Database 23ai
Free:

| | result | evidence |
|---|---|---|
| Reference thin-mode tests passing through the Rust engine | **2462** | [docs/PARITY_SKIPS.md](docs/PARITY_SKIPS.md) |
| Skipped (identical node IDs to the reference thin driver) | **116** | every skip proven legitimate — see below |
| Skips that hide a Rust-engine defect | **0** | Rust passes all 303 non-skip tests in the skip-bearing modules |
| Regressions vs the recorded baseline | **0** | [docs/RELEASE_CERTIFICATION.md](docs/RELEASE_CERTIFICATION.md) |

Every one of the 116 skips is forced by the thin-mode contract, not by a
shortcoming in this engine: 88 are `requires thick mode` (the reference thin
driver skips them too), 17 are external/OS authentication (**disproven as ours**:
the reference thin driver *fails* all 17 when the gate is removed, because
bequeath auth is thick-only), 4 are a deliberately inverted older-client vector
check, and 7 are hardcoded upstream `@pytest.mark.skip` markers. The full
node-ID taxonomy and the disproof experiment are in
[docs/PARITY_SKIPS.md](docs/PARITY_SKIPS.md).

The green is real, not fabricated: it was adversarially audited (dead-port
offline falsification, `strace` raw-socket capture of server-computed values).
See [docs/FAKE_PARITY_AUDIT.md](docs/FAKE_PARITY_AUDIT.md).

---

## Why use it

| | rust-oracledb | python-oracledb thin | the edge |
|---|---|---|---|
| **Concurrent decode** | GIL-free; N connections decode on N cores | wire codec runs under the CPython GIL | ~4.7× throughput scaling at 8 workers where Python threads regress to 0.5× (measured — see [Performance](#performance)) |
| **Typed rows** | `#[derive(FromRow)]`, compile-checked field types | dynamic Python objects | type errors at compile time, not at row 10,000 |
| **Errors & binds** | structured `Error` (`.ora_code()`, `.is_retryable()`), `FromSql`/`ToSql`, `params!` | bare `.code` int, manual conversion | a curated transient/connection-lost code set ships built in |
| **Deployment** | one 4.26 MB static musl binary, `FROM scratch` image | interpreter + stdlib + wheel (~151 MB deploy) | ~35× smaller image — python-impossible ([docs/DEPLOYMENT.md](docs/DEPLOYMENT.md)) |
| **Connect strings** | full TNS / tnsnames.ora / EZConnect-Plus parser with byte-offset caret diagnostics | terse `DPY-4017` | points the caret at the offending token ([docs/CONNECT_STRINGS.md](docs/CONNECT_STRINGS.md)) |
| **Offline bug repro** | deterministic `.tns-cassette` record/replay (no DB) | no transport seam | a wire bug travels in one self-contained file ([docs/RECORD_REPLAY.md](docs/RECORD_REPLAY.md)) |
| **Observability** | feature-gated `tracing`/OpenTelemetry spans, GIL-free, zero-cost off | GIL-bound instrumentation | N connections trace in parallel; zero dependency when off ([docs/OBSERVABILITY.md](docs/OBSERVABILITY.md)) |
| **SODA** | experimental thin-mode SODA (42 reference SODA tests pass) | none (SODA is thick-only) | the first pure-thin SODA in an Oracle driver ([docs/SODA.md](docs/SODA.md)) |
| **Safety** | `#![forbid(unsafe_code)]`, fuzzed, OOM-closed by construction | C extension surface | one audited FFI module, quarantined to the test harness ([docs/SAFETY_AUDIT.md](docs/SAFETY_AUDIT.md)) |

Each of these is detailed in [The ledger](#the-better-than-python-oracledb-ledger).

---

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
    // The session identity the database records in v$session. Unlike an OCI
    // client (which reports the host process and OS user it runs as), the
    // caller chooses these exactly.
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

    // Bind typed Rust values positionally (:1, :2, ...) and map rows into a struct.
    let result = BlockingConnection::query(
        &mut conn,
        "select id, name, manager_id from emp where dept = :1",
        (40,),
    )?;
    let emps: Vec<Emp> = result.rows_as::<Emp>()?;
    for e in &emps {
        println!("{}: {} (mgr {:?})", e.id, e.name, e.manager_id);
    }

    BlockingConnection::close(conn)?;
    Ok(())
}
```

That uses the synchronous [`BlockingConnection`] facade, so it is an ordinary
`main()` with no visible runtime. The async API is identical minus the blocking
wrapper; see [Quickstart](#quickstart).

---

## Performance

All numbers below are measured; the methodology, host details, and the raw
per-pass spread are in [docs/PERFORMANCE.md](docs/PERFORMANCE.md). Both drivers
speak the same protocol to the same Oracle 23ai Free container over the same
loopback TCP socket, thin mode on both sides, no Instant Client anywhere.

### Concurrent throughput (the no-GIL result)

A decode-bound workload: N workers each drive their own connection, repeatedly
scanning a warmed 1000-row × 20-column table, decoding every cell (NUMBER
base-100 mantissa parsing + UTF-8 VARCHAR2 builds). Scaling factor is
throughput(N) / throughput(1) for that side.

| workers | rust (threads) | python (threads) | python (asyncio) |
|--------:|---------------:|-----------------:|-----------------:|
| 1 | 185k rows/s (1.0×) | 202k (1.0×) | 177k (1.0×) |
| 2 | 420k (2.3×) | 252k (1.3×) | 207k (1.2×) |
| 4 | 870k (4.6×) | 118k (0.6×) | 216k (1.2×) |
| 8 | 870k (**4.7×**) | 109k (**0.5×**) | 207k (**1.2×**) |
| 16 | 780k (4.2×) | 101k (0.5×) | 207k (1.2×) |

Rust scales roughly linearly until the single test container (not the driver)
caps it around 870k rows/s. python-oracledb threads show the textbook GIL
signature: throughput peaks at 2 workers and then falls *below* serial. asyncio
hides connection wait but the decode still runs single-threaded under the GIL, so
it plateaus. At 8 workers Rust's aggregate is roughly 8× the Python-threads
aggregate and 4× the asyncio aggregate.

**Honest qualifier:** at a single connection, python-oracledb is competitive and
sometimes ahead (202k vs 185k at N=1). The Rust win is in *scaling*, not raw
single-connection decode speed.

### Borrowed (zero-copy) fetch path

A `for_each_row_ref` fast path lets a Rust consumer iterate rows as borrowed
`QueryValueRef` slices instead of materialising a `String`/`Vec<u8>` per scalar
cell. Measured on a 5000-row × 4-column batch with an allocation counter:

| | owned `fetch_rows` | borrowed `for_each_row_ref` |
|---|---|---|
| allocations/row | 11.00 | 1.01 (**−91%**) |
| wall time | ~15 ms | ~9 ms (**~37% faster**) |
| bytes allocated | baseline | −21% |

### Pipelined fetch (speculative next-page prefetch)

`for_each_row_ref` issues page *K+1*'s fetch round-trip **before** decoding page
*K*, so the server processes the next page and the kernel buffers its bytes while
the client is still decoding the current one — overlapping wire I/O with decode
on a single connection (something the CPython GIL structurally prevents). Bounded
to one page of look-ahead, and cancellation-safe: a drop mid-prefetch leaves the
stranded page to be broken-and-drained by the next operation (proven by a
deterministic test with a negative control), and the prefetched rows are
byte-identical to the serial path.

| metric (50k rows, arraysize 1000, ~49 pages) | result |
|---|---|
| per-page read-wait | **−5.6% to −24%** (the robust signal) |
| wall time, realistic per-row consumer | **−12.5% to −19.5%** |
| wall time, trivial consumer (loopback) | break-even to −6% |

**Honest caveat:** on loopback the hideable read latency is tiny (~300 µs), so a
trivial consumer is ~break-even; the win is dominated by network RTT, so it grows
on real networks — loopback is the *conservative floor*, not the headline.

### Serial single-call operations

Single connection, serial calls, warm caches. Ratio is python / rust (above 1.0
means Rust faster). Below ~200 µs the host jitter dominates; treat
one-significant-figure differences as ties.

| operation | rust-oracledb | python-oracledb thin | note |
|---|---|---|---|
| `connect` (full handshake) | 32.6 ms | 33.3 ms | tie — network/server-bound; the floor for both |
| `select 1 from dual` | ~123 µs (after opt) | ~80 µs | python edges it on this cheapest op |
| fetch 10k rows | 5.0 ms | 4.7 ms | tie — wire-serialization bound |
| executemany 1000 | 2.2 ms | 2.0 ms | tie (both bimodal under host contention) |
| CLOB read 64 KiB | ~768 µs (after opt) | ~440 µs | python faster; Rust improved −17% via single-pass UTF-16 decode |

The serial single-row and CLOB ops are CPU-bound edges where python-oracledb is
still ahead; this is stated plainly rather than inflated. Two such gaps were
profiled and partially closed (a per-call runtime cache, −59% to −62% on the
blocking facade; a single-pass ASCII-inline UTF-16 LOB decoder, −50% on the
decode phase). The optimization history with before/after criterion deltas is in
[docs/PERFORMANCE.md](docs/PERFORMANCE.md#optimization-history).

**Caveats (the honest part):** these are loopback, single-host, plain-TCP
measurements on a *shared, busy* AMD EPYC box (`schedutil` governor, cores not
pinned), so sub-200 µs numbers carry real run-to-run variance. A real network
with TLS would add latency equally to both drivers and push every serial
operation further toward "network-dominated, therefore a tie". The thick
`rust-oracle` crate is deliberately not benchmarked: it requires Instant Client,
which this project avoids by design.

---

## The better-than-python-oracledb ledger

Each row is a concrete differentiator, with the specific edge.

| feature | rust-oracledb | python-oracledb thin | the edge |
|---|---|---|---|
| **No-GIL concurrent decode** | every connection decodes on its own OS thread, sharing nothing | wire codec holds the CPython GIL | true multicore decode: ~4.7× scaling at N=8 vs python-threads' 0.5× regression ([docs/PERFORMANCE.md](docs/PERFORMANCE.md)) |
| **Compile-checked rows** | `#[derive(FromRow)]` maps a row into a struct with typed fields; `Option<T>` = nullable | runtime Python objects | type mismatches are compile errors |
| **Structured errors + binds** | `Error::ora_code()`, `is_retryable()`, `is_connection_lost()`; `FromSql`/`ToSql`/`params!`; lossless `Decimal` ↔ NUMBER | bare `.code` int, manual conversion | curated transient + connection-lost code sets ship built in |
| **Single static binary** | 4.26 MB stripped musl binary, `FROM scratch` image (one layer, one file) | interpreter + stdlib + wheel | ~35× smaller than python's ~151 MB thin deploy — and python-impossible ([docs/DEPLOYMENT.md](docs/DEPLOYMENT.md)) |
| **Connect-string parser** | full TNS descriptor / tnsnames.ora (+`IFILE`) / EZConnect-Plus, with offset-pointed caret diagnostics | terse `DPY-4017` | a malformed descriptor points the caret at the offending token ([docs/CONNECT_STRINGS.md](docs/CONNECT_STRINGS.md)) |
| **Record/replay** | deterministic `.tns-cassette` capture + offline replay with no socket | no transport seam | reproduce a production wire bug from one file, no DB ([docs/RECORD_REPLAY.md](docs/RECORD_REPLAY.md)) |
| **Observability** | feature-gated `tracing`/OpenTelemetry per-round-trip spans, GIL-free, digest-only (no secrets), zero-cost off | GIL-bound, app-wired | N connections trace in parallel; the dependency isn't compiled in when off ([docs/OBSERVABILITY.md](docs/OBSERVABILITY.md)) |
| **Thin-mode SODA** (experimental) | SODA over the thin TTC protocol — 42 of Oracle's own SODA tests pass | raises `DPI-1050`; SODA is thick-only | the first pure-thin SODA in an Oracle driver ([docs/SODA.md](docs/SODA.md)) |
| **Fail-closed decoder** | OOM-closed by construction (`BoundedReader`); 9 cargo-fuzz targets, billions of execs, 0 crashes | — | a hostile/buggy server cannot OOM or panic the client ([docs/FUZZING.md](docs/FUZZING.md)) |
| **Cancellation-correct fetch** | `cancel()` / scope cancel-on-drop sends a break and drains, leaving a clean connection | — | a cancelled or timed-out fetch never poisons a reused connection |

---

## Robustness

The protocol crate is `#![forbid(unsafe_code)]`, as is the async driver crate.
The only `unsafe` in the entire workspace is one audited module
(`arrow_capsule.rs`, the Arrow C Data Interface PyCapsule export) that lives in
the **PyO3 test harness**, not in either published crate. Every site is
FFI-inherent and reviewed sound ([docs/SAFETY_AUDIT.md](docs/SAFETY_AUDIT.md)).

**Eight real bugs** were found and fixed, each with a regression test, through
multi-pass bug-hunting:

| class | bug |
|---|---|
| Multi-packet framing | the thin decoder mis-framed multi-packet wide-row results past ~1500 rows |
| Error-path wire deadlock | a DML-RETURNING client-side error (`ORA-12899`) left the connection wedged mid-exchange |
| Break/drain state | `call_timeout` sent a BREAK without draining the server response, so a reused connection then read stale packets; a second issue did not drain multiple trailing RESET markers |
| Codec overflow | the NUMBER encoder's `decimal_point_index += exponent` overflowed `i32` on crafted text, panicking under debug assertions |
| Parser recursion DoS | a deeply-nested TNS descriptor recursed without bound — a stack-overflow process abort — now capped |
| Wallet DoS | a malicious `cwallet.sso` could drive an unbounded heap allocation via the PBKDF2 `keyLength` field — now bounded |
| SQL-scan correctness | PL/SQL output-bind detection substring-matched `into`/`returning` inside string literals and comments — now literal/comment-aware |

Separately, the wire decoder is **coverage-guided fuzzed** with 9 cargo-fuzz
targets (one per untrusted decode boundary: packet framing, query response, OSON,
VECTOR, scalar codecs, server-error trailer, direct-path, AQ, CQN/subscription).
Bounded sessions logged billions of executions under ASan/UBSan + overflow-checks
with **zero crashes**. Fuzzing found four denial-of-service bugs early (three
unbounded allocations, one negate-overflow panic), all fixed fail-closed; the
whole OOM-from-wire-length class is now **closed by construction** via the
`BoundedReader` invariant: an allocation can never exceed the bytes remaining in
the message buffer. See [docs/FUZZING.md](docs/FUZZING.md).

---

## Installation

`rust-oracledb` targets Rust 1.83+. It is not yet published to crates.io; depend
on it via git until the first release:

```toml
[dependencies]
oracledb = { git = "https://github.com/MuhDur/rust-oracledb" }
```

### Single static binary (`FROM scratch`)

Because the driver links no native Oracle library, an application can be built as
one fully-static musl binary and shipped in an empty image:

```bash
rustup target add x86_64-unknown-linux-musl
cargo build --release -p oracledb --target x86_64-unknown-linux-musl
```

The end-to-end recipe (musl C toolchain for `ring`, `FROM scratch` Dockerfile,
and the measured 4.26 MB result) is in [docs/DEPLOYMENT.md](docs/DEPLOYMENT.md).

---

## Quickstart

### Synchronous (blocking facade)

See [Quick example](#quick-example) above. `BlockingConnection` wraps the async
driver in a per-thread runtime, so it is an ordinary `main()`.

### Async

The async API mirrors the blocking one; every method takes an asupersync `&Cx`:

```rust
use asupersync::runtime::RuntimeBuilder;
use asupersync::Cx;
use oracledb::{Connection, ConnectOptions};
use oracledb::protocol::ClientIdentity;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let runtime = RuntimeBuilder::current_thread().build()?;
    runtime.block_on(async {
        let cx = Cx::current().expect("Runtime::block_on installs an ambient Cx");

        let identity = ClientIdentity::new(
            "billing-worker", "edge-pod-7", "tenant-42", "shard-a", "rust-oracledb",
        )?;
        let options = ConnectOptions::new(
            "dbhost:1521/FREEPDB1", "app_user", "app_password", identity,
        );

        let mut conn = Connection::connect(&cx, options).await?;
        let result = conn.query(&cx, "select 7 + 5 from dual", ()).await?;
        let sum: i64 = result.get(0, 0)?; // QueryResultExt::get
        assert_eq!(sum, 12);
        conn.close(&cx).await?;
        Ok::<_, oracledb::Error>(())
    })?;
    Ok(())
}
```

### Binds and named parameters

```rust
use oracledb::{QueryResultExt, params};

// Positional: a tuple, a slice, or `params![...]`.
let r = conn.query(&cx, "select :1 + :2 from dual", (40, 2)).await?;

// Named: order-independent; placeholder order in the SQL is resolved for you.
let r = conn
    .query_named(
        &cx,
        "select * from emp where id = :id and name = :name",
        params!{ ":id" => 40, ":name" => "alice" },
    )
    .await?;

// Pull typed cells out by index or by (case-insensitive) column name.
let id: i64 = r.get(0, 0)?;
let name: String = r.get_by_name(0, "name")?;
```

### Typed rows with `#[derive(FromRow)]`

```rust
use oracledb::{FromRow, QueryResultExt};

#[derive(FromRow)]
struct Emp {
    id: i64,
    name: String,
    hired: Option<String>, // NULL -> None; a non-Option NULL is a hard error
}

let result = conn.query(&cx, "select id, name, hired from emp", ()).await?;
let emps: Vec<Emp> = result.rows_as::<Emp>()?;
```

### Feature flags

`default = ["derive"]`. The `derive` proc-macro is build-time-only, so the
default runtime build pulls in nothing extra.

| feature | default | what it adds |
|---|:---:|---|
| `derive` | ✅ | `#[derive(FromRow)]` for compile-checked typed rows |
| `chrono` | | `FromSql`/`ToSql` for `chrono` date/time types |
| `uuid` | | `FromSql`/`ToSql` for `uuid::Uuid` |
| `serde_json` | | `FromSql`/`ToSql` for `serde_json::Value` |
| `rust_decimal` | | lossless `rust_decimal::Decimal` ↔ NUMBER |
| `arrow` | | Arrow `RecordBatch` fetch + C Data Interface export |
| `tracing` | | feature-gated OpenTelemetry-style spans (zero-cost off) |
| `cassette` | | `.tns-cassette` transport record/replay seam |
| `soda` | | **experimental** thin-mode SODA |
| `experimental` | | the `cwallet.sso` SSO auto-login wallet reader |

---

## Architecture

Three crates, plus a test-only harness:

```text
oracledb-protocol   sans-I/O TNS/TTC codec. #![forbid(unsafe_code)].
   (codec core)     Decodes everything an untrusted server puts on the wire;
                    every wire-length-driven allocation is BoundedReader-checked.
        │
        ▼
oracledb            async driver on the asupersync runtime, plus the
   (the driver)     BlockingConnection synchronous facade. #![forbid(unsafe_code)].
                    Connection / execute / fetch / LOB / pool / TLS / SODA.
        │
        ▼
oracledb-pyshim     PyO3 module slotted under python-oracledb's public layer so
   (test harness)   the reference's OWN pytest suite drives the Rust engine.
                    The one quarantined `unsafe` (Arrow FFI) lives here, not in
                    the published crates.
```

`oracledb-derive` is the build-time proc-macro crate behind `#[derive(FromRow)]`.

---

## Honest limitations

This driver is deliberate about what it does *not* yet claim. None of these are
hidden.

- **Conformance parity ≠ production-ready.** The certification is a clean
  zero-regression differential sweep plus an adversarial audit against Oracle
  23ai Free. It is **not** a multi-day statistical soak, and not an
  Oracle-certified or drop-in guarantee. See
  [docs/RELEASE_CERTIFICATION.md](docs/RELEASE_CERTIFICATION.md).
- **Thin-mode SODA is experimental.** It passes 42 of the reference SODA tests
  (python thin passes none), but it is not full thick-mode SODA. Every reference
  failure/skip is explained in [docs/SODA.md](docs/SODA.md) (Oracle Text not in
  the container, native-collection error-code differences, `getDataGuide`,
  mixed-media collections, `JsonId` reconstruction).
- **Live TLS/TCPS needs operator listener infrastructure.** The TCPS client path
  (rustls over the async transport, `ewallet.pem`, DN matching) is implemented and
  unit/handshake-tested with real crypto, but an end-to-end test against a real
  Oracle TCPS listener requires standing one up; the bundled Free container can't
  host it (no `orapki`/Java). The `cwallet.sso` reader is gated behind
  `experimental`. See [docs/TLS_SETUP.md](docs/TLS_SETUP.md).
- **Native single-round-trip pipelining is built but flagged off**
  (`supports_pipelining()` returns `false`); a sequential per-op runner is used,
  each op a real wire round-trip.
- **The shim → crate migration is in progress.** Some SQL/bind/type driver logic
  still lives in the PyO3 shim rather than the standalone crate; suite-green is the
  gate as that logic moves down. The crate is exercised by native (non-shim)
  integration tests today.
- **Wide multi-packet rows.** A known decoder bug in the wide-row multi-packet
  reassembly path (`select *` spanning several packets past ~1500 wide rows) is
  documented in [docs/PERFORMANCE.md](docs/PERFORMANCE.md#a-driver-limitation-this-surfaced).
- **The static binary is x86_64-musl.** ARM64/Windows/macOS need their own
  targets; only Linux musl is `FROM scratch`.

The full negative ledger, with the retry condition for each gap, is in
[docs/RELEASE_CERTIFICATION.md](docs/RELEASE_CERTIFICATION.md#negative-ledger-honest-gaps--retry-conditions-named).

---

## Testing

```bash
# Unit + golden tests, no database needed:
cargo test --workspace

# With optional features:
cargo test --workspace --features cassette
```

Conformance against python-oracledb's own suite, fuzzing, and the live
container-backed integration tests need a local Oracle container; the harness and
docs ([docs/FUZZING.md](docs/FUZZING.md), [docs/PERFORMANCE.md](docs/PERFORMANCE.md))
give the exact commands. Container-dependent benches and tests self-skip cleanly
when no listener is present.

---

## FAQ

**Does it need Oracle Instant Client or OCI?** No. It is pure Rust and speaks the
wire protocol directly. That is the entire point.

**Is it a drop-in replacement for python-oracledb?** No, it is a Rust crate, not
a Python module. It is *behaviour-compatible* with python-oracledb thin mode, and
proves it by passing that project's own test suite.

**Why thin mode only?** Thin mode is what makes the single-static-binary,
no-Instant-Client deployment possible. A thick driver would re-introduce the
native dependency this project exists to avoid.

**What Oracle versions are tested?** Oracle Database 23ai Free (23.26) is the
database under test. The protocol negotiates capabilities and the codecs match
python-oracledb thin's, which targets 12.1+ servers.

**Can I use it synchronously?** Yes. `BlockingConnection` wraps the async driver
in a per-thread runtime, so no async is visible to the caller.

**TLS to Autonomous Database?** The TCPS client path is implemented; you supply a
wallet (`ewallet.pem`, or `cwallet.sso` with `--features experimental`). See
[docs/TLS_SETUP.md](docs/TLS_SETUP.md) for the limitations.

---

## About contributions

Please don't take this the wrong way, but I do not accept outside contributions
for any of my projects. I simply don't have the mental bandwidth to review
anything, and it's my name on the thing, so I'm responsible for any problems it
causes; thus, the risk-reward is highly asymmetric from my perspective. I'd also
have to worry about other "stakeholders," which seems unwise for tools I mostly
make for myself for free. Feel free to submit issues, and even PRs if you want to
illustrate a proposed fix, but know I won't merge them directly. Instead, I'll
have Claude or Codex review submissions via `gh` and independently decide whether
and how to address them. Bug reports in particular are welcome. Sorry if this
offends, but I want to avoid wasted time and hurt feelings. I understand this
isn't in sync with the prevailing open-source ethos that seeks community
contributions, but it's the only way I can move at this velocity and keep my
sanity.

---

## License

Dual-licensed under either of:

- Apache License, Version 2.0 ([LICENSE-APACHE](LICENSE-APACHE))
- MIT license ([LICENSE-MIT](LICENSE-MIT))

at your option.

[`BlockingConnection`]: https://github.com/MuhDur/rust-oracledb
