# Transport record/replay — `.tns-cassette` (offline wire repro)

A **transport seam** that lets the raw Oracle wire byte stream of a session be
(a) **recorded** to a `.tns-cassette` file and (b) **replayed deterministically
offline with no database**. This is the offline-repro differentiator: a captured
production session — including a wire bug — can be replayed against the real
decoder/state-machine on a developer's laptop, no listener required.

> **python-oracledb can't do this.** python-oracledb thin mode has no transport
> seam: there is no supported way to capture a raw thin-mode wire session and
> replay it offline to drive the decoder. Reproducing a wire-level bug there
> means reproducing the *live* conditions (the database, the data, the timing).
> Here the bug travels in a single self-contained file.

Everything below is gated behind the **`cassette` Cargo feature**, which is
**off by default**. With the feature off, the recording/replay enum variants and
the capture hook do not exist and the transport path is byte-identical to the
standard build — parity is unaffected.

---

## 1. Where the seam lives

The driver reads and writes through two enums in
[`crates/oracledb/src/transport.rs`](../crates/oracledb/src/transport.rs):
`OracleReadHalf` and `OracleWriteHalf` (added by the TLS work — `Plain` TCP
halves or a shared `Tls` stream). The `cassette` feature adds two more variants
to each:

| Variant     | Read half (`OracleReadHalf`)            | Write half (`OracleWriteHalf`)              |
| ----------- | --------------------------------------- | ------------------------------------------- |
| `Recording` | reads from the real half, **tees** every `S->C` transfer into a recorder | writes through the real half, **tees** every `C->S` transfer |
| `Replay`    | serves recorded `S->C` bytes in order, **no socket** | checks-or-ignores `C->S` writes, **no socket** |

The `.tns-cassette` **wire format** itself is a pure, sans-I/O module in the
protocol crate:
[`crates/oracledb-protocol/src/net/cassette.rs`](../crates/oracledb-protocol/src/net/cassette.rs).
It only encodes/decodes frames in memory; the I/O-bearing decorators live in the
driver crate's `transport` module.

```
            ┌──────────── Connection::connect ────────────┐
            │                                              │
  RECORD:   socket ──► RecordingRead  ──► decoder          recorder ──► .tns-cassette file
            socket ◄── RecordingWrite ◄── encoder          (tees both directions)

  REPLAY:   (no socket)  ReplayRead   ──► decoder          .tns-cassette file ──► ReplayRead
            (no socket)  ReplayWrite  ◄── encoder          (writes checked or ignored)
```

---

## 2. The `.tns-cassette` binary format

A cassette captures the **full** transport byte stream of one session — every
`C->S` write and every `S->C` read, in the exact order the driver issued them,
from connect through close.

```text
magic    : 8 bytes  = b"TNSCASS\0"
version  : 1 byte   = 1
----- repeated, one per captured transfer (frame) -----
direction: 1 byte   = 0x01 (C->S / client write) | 0x02 (S->C / server read)
micros   : 8 bytes  LE = microseconds since the first frame (informational;
                         IGNORED on replay so the replay path is clock-free)
length   : 4 bytes  LE = number of payload bytes that follow
payload  : length bytes = the raw transport bytes of this transfer
```

* All integers are **little-endian**.
* There is **no trailing index or checksum**; the frame sequence ends at EOF.
* Decoding is **strict**: a bad magic, an unknown version, a bad direction tag,
  or a truncated frame is an error rather than a silent partial read, so a
  corrupt cassette fails loudly instead of replaying garbage.

A **frame is one transport transfer, not one TNS packet.** The driver reads a TNS
packet in two `read_exact` calls (an 8-byte header, then the body), so a single
packet typically spans two `S->C` frames. The replay reader reassembles across
frame boundaries via `read_exact`, exactly as the live driver does — so the
cassette never needs to understand TNS framing; it is a faithful tape of the
socket.

### Determinism

Replay is **byte-deterministic**: the replay path never consults a clock or RNG.
The recorded `micros` timestamps are informational only (useful for eyeballing
latency in a capture) and are never read back during replay. Replaying the same
cassette twice yields identical bytes in identical order.

---

## 3. Recording a session

Recording is wired in transparently. A `capture_scope()` guard installs a
thread-local recorder; while it is alive, the `plain_split` / `tls_split`
helpers that `Connection::connect` already calls auto-wrap the halves in the
`Recording` decorators. No change to `connect` and no extra connect parameter is
needed.

```rust
use oracledb::transport;
use oracledb::{ConnectOptions, Connection};

// Install BEFORE connect so the FULL session (connect, auth, execute, fetch,
// close) is captured.
let scope = transport::capture_scope();

let mut conn = Connection::connect(&cx, options).await?;
let result = conn.execute_query(&cx, "select 7+5 from dual", 2).await?;
conn.close(&cx).await?;

// Serialize and persist the tape.
let cassette: Vec<u8> = scope.to_cassette_bytes();
std::fs::write("session.tns-cassette", &cassette)?;
```

Recording is a pure side-effect: the live byte stream is untouched, so a recorded
session is byte-for-byte what a non-recorded one would be.

You can also drive recording manually around an existing pair of halves with
`transport::recording_split(read, write, recorder)`, or build a recorder
directly with `transport::CassetteRecorder::new()`.

> **Sensitivity note.** A cassette is the raw wire stream, so it contains the
> authentication exchange and any literal data in the session. Treat captures
> from production like any other wire dump — scrub or restrict access before
> sharing.

---

## 4. Replaying a session offline (no database)

Build a socket-free replay transport from the cassette bytes and drive the real
read path. The `ReplayRead` half serves the recorded `S->C` bytes to `read_exact`
in order; the `ReplayWrite` half handles the driver's `C->S` writes per the
chosen mode:

```rust
use oracledb::transport::{self, ReplayWriteMode};

let bytes = std::fs::read("session.tns-cassette")?;
let (mut read, _write) = transport::replay_split(&bytes, ReplayWriteMode::Ignore)?;
// `read` is an OracleReadHalf with no socket. Drive the real TNS packet
// framing + decoder over it; the decoded result matches the recorded session.
```

### Write modes

| `ReplayWriteMode` | Behaviour |
| ----------------- | --------- |
| `Ignore` (default) | Accept and discard `C->S` writes. The decoder drives forward with no fidelity check on what it sends. Use this to step the decoder over the recorded responses. |
| `Check` | Compare each `C->S` write against the recorded request stream and fail (`ReplayMismatch`) on the first divergence — proves the driver re-issues the exact captured request bytes. |

Because the replay halves have **no file descriptor**, replay runs with the
database stopped, the network unreachable, or on a machine that has never seen
the listener. It is purely a function of the cassette.

---

## 5. Worked example & tests

The integration tests in
[`crates/oracledb/tests/cassette_record_replay.rs`](../crates/oracledb/tests/cassette_record_replay.rs)
demonstrate the full loop for `select 7+5 from dual`:

* **Live capture** (`record_select_7_plus_5_session`, `#[ignore]`, needs the
  container): `capture_scope()` around a real connect + `select 7+5 from dual` +
  fetch + close, asserts the **live** result is `12`, and writes the fixture
  [`tests/fixtures/cassettes/select_7_plus_5.tns-cassette`](../crates/oracledb/tests/fixtures/cassettes/select_7_plus_5.tns-cassette).

* **Offline replay** (`replay_select_7_plus_5_offline`, runs everywhere, **no
  DB**): loads the checked-in fixture, builds a `replay_split` transport, drives
  the **real** TNS packet framing over `ReplayRead` and the **real**
  `parse_query_response` decoder, and asserts the decoded value is `12`. With a
  bogus/unreachable DSN in the environment it still passes in milliseconds —
  proof that it never connects.

Unit tests for the wire format live in `net::cassette` (header layout, frame
round-trip, lazy reader, corruption handling) and for the transport decorators
in `transport::cassette_seam` (replay ordering, sub-read splitting, write
check/ignore, mismatch flagging, capture-scope wrap/restore).

### Running it

```bash
# Build & test with the feature on (default build is unaffected).
cargo test --workspace --features cassette

# (Re)capture the fixture against a live listener:
eval "$(ORACLEDB_CONTAINER_NAME=rust-oracledb-free ORACLEDB_HOST_PORT=1521 scripts/container.sh env)"
cargo test -p oracledb --features cassette --test cassette_record_replay \
  record_select_7_plus_5_session -- --ignored --nocapture

# Replay it offline (no DB needed):
cargo test -p oracledb --features cassette --test cassette_record_replay \
  replay_select_7_plus_5_offline
```

---

## 6. Capturing a real bug for repro

1. Reproduce the failing session against the live database with a
   `capture_scope()` installed before `connect` (see §3), and persist the
   cassette the moment the bug manifests.
2. Ship the `.tns-cassette` to whoever debugs it — it is a single self-contained
   file; no database, schema, or credentials travel with it.
3. They `replay_split` it offline (§4) and step the decoder over the exact bytes
   the server sent, as many times as needed, under a debugger, with zero
   database flakiness or timing dependence. Use `ReplayWriteMode::Check` to also
   confirm the request side matches the capture.

The result is a deterministic, offline, file-sized reproduction of a production
wire condition — the kind of repro python-oracledb has no mechanism to produce.

---

## 7. Feature gating & parity

`cassette` is declared in both
[`crates/oracledb-protocol/Cargo.toml`](../crates/oracledb-protocol/Cargo.toml)
(the wire format) and
[`crates/oracledb/Cargo.toml`](../crates/oracledb/Cargo.toml) (the transport
decorators; enabling it also enables `oracledb-protocol/cassette`). It is **off
by default**.

* With the feature **off**, the `Recording`/`Replay` enum variants and the
  `capture_scope` hook are `#[cfg]`-compiled out; the transport `poll_*` match
  arms reduce to exactly the pre-seam `Plain`/`Tls` arms.
* The conformance shim (`oracledb-pyshim`) depends on `oracledb` **without** the
  `cassette` feature, so the parity suite always runs against the byte-identical
  transport path. Parity sentinels `test_1100_connection` (57 passed / 5
  skipped) and `test_2200_number_var` (39 passed) are unchanged by this work.
