# TTC "unknown message type" — classification of the transient desync

Bead: `rust-oracledb-090-b13a-ttc129-desync` (P1).

Field symptom: a metadata read failed with

```
CONNECTION_FAILED: query: unknown TTC message type 129 at position 35
```

and the **identical call succeeded on retry with nothing changed** — a transient
wire condition. This note records what message type 129 is, why the symptom is
transient, what the driver does today, and the one residual risk that needs a
live packet capture to close safely. It exists because a correct fix could not
be *proven* from the protocol code and the reference alone; the analysis below
is deliberately conservative so nobody later "fixes" this by weakening the
unknown-message-type rejection.

## 1. Finding: 129 (0x81) is **not** a real TTC message type

TTC message types are a small, dense enum. The full set this driver recognises
(`crates/oracledb-protocol/src/thin/constants.rs:27-89`):

| value | constant                                  |
|------:|-------------------------------------------|
| 1     | `TNS_MSG_TYPE_PROTOCOL`                    |
| 2     | `TNS_MSG_TYPE_DATA_TYPES`                  |
| 3     | `TNS_MSG_TYPE_FUNCTION`                    |
| 4     | `TNS_MSG_TYPE_ERROR`                       |
| 6     | `TNS_MSG_TYPE_ROW_HEADER`                  |
| 7     | `TNS_MSG_TYPE_ROW_DATA`                    |
| 8     | `TNS_MSG_TYPE_PARAMETER`                   |
| 9     | `TNS_MSG_TYPE_STATUS`                      |
| 11    | `TNS_MSG_TYPE_IO_VECTOR`                   |
| 13/14 | `TNS_MSG_TYPE_OAC` / `..._LOB_DATA`       |
| 16/17 | `TNS_MSG_TYPE_DESCRIBE_INFO` / `PIGGYBACK`|
| 19/21 | `TNS_MSG_TYPE_FLUSH_OUT_BINDS` / `BIT_VECTOR` |
| 23/27 | `..._SERVER_SIDE_PIGGYBACK` / `IMPLICIT_RESULTSET` |
| 29    | `TNS_MSG_TYPE_END_OF_RESPONSE`            |
| 33/34 | `TNS_MSG_TYPE_TOKEN` / `FAST_AUTH`        |

The maximum is 34. **129 (0x81) cannot be a message type** — it is a byte the
decoder read *as* a message type at a message-framing boundary that is not one.

The reference agrees. python-oracledb's thin message loop
(`reference/python-oracledb/.../impl/thin/messages/base.pyx:305-308`) raises
`ERR_MESSAGE_TYPE_UNKNOWN` on exactly this fall-through, and its text
(`.../errors.py:810-813`) is literally *"internal error: unknown protocol
message type {message_type} at position {position}"*. So this is the driver's
option **(b): a genuine transient/unexpected condition, not a real message the
driver is missing.** There is no known type-129 message to parse.

The driver's own error site is the `_ =>` arm of the query/fetch message loop
(`crates/oracledb-protocol/src/thin/fetch.rs:602-613`). Note it first calls
`find_embedded_server_error` (`fetch.rs:604-608`): a genuine Oracle error that
happens to be embedded in the bytes is surfaced as a `ServerError`, **not** as
`UnknownMessageType`. So when `UnknownMessageType` actually fires there is no
recoverable server error hidden in the payload — it is a true framing desync.

## 2. Two distinct desync classes (only one matches this report)

**(a) Deterministic mis-framing — an under-consumed field/trailer.** If a value
carries an optional trailing field that the parse path forgets to consume, the
*next* byte is read as a message type and mis-frames. This is exactly bead
`rust-oracledb-f0ad`, already fixed: a CLOB re-fetched as LONG streams the LONG
status trailer, and before the fix its `0x81` return-code byte was mis-read as
"unknown TTC message type 129 at position 95"
(`crates/oracledb-protocol/src/thin/mod.rs:117-155`). These bugs are
**deterministic** for a given cursor/type state — they do *not* "succeed on
retry with nothing changed," so this class does **not** explain the field
report. (It is the reason `0x81` in particular is a familiar desync byte: it is
the LONG status trailer's return-code marker.)

**(b) Transient wire desync.** The response bytes the decoder framed on differ
run-to-run: a genuinely corrupted byte from the network/server, or the driver
stopping its packet-reassembly at a different point. This is the class the field
report describes ("identical call succeeded on retry, nothing changed").

## 3. What the driver does today with a transient `UnknownMessageType`

The error is already **clean and typed**:
`Error::Protocol(ProtocolError::UnknownMessageType { message_type, position })`.
Its disposition then depends on how the response was read.

### 3a. END_OF_RESPONSE-framed path (23ai and any server that negotiates it)

When `supports_end_of_response` is true, the response is delimited by the
END_OF_RESPONSE packet flag, so the reader consumes **the whole** server
response off the socket regardless of what the parse then makes of the bytes.
The wire is left **aligned**, so the connection stays **reusable**:

- `Error::connection_disposition()` returns `Reusable` for a bare protocol error
  (`crates/oracledb/src/lib.rs:1623-1634`).
- `post_sync_protocol_error_disposition` returns `Ready` for `UnknownMessageType`
  (`crates/oracledb/src/recovery.rs:574-586`), so `note_parse`
  (`lib.rs:3726-3740`) does **not** mark the session dead.

This is a **deliberate, tested** decision, not an oversight. The test
`borrowed_stream_malformed_and_truncated_fetches_retire_before_reuse`
(`lib.rs:17345`) feeds a fully-framed `[0xff]` (unknown message type 255) fetch
response and asserts the connection stays at `SessionRecoveryPhase::Ready` and
successfully executes a **fresh** statement afterward
(`exercise_borrowed_stream_fetch_decode_failure`, `lib.rs:17252-17342`). The
loopback connection used there runs the framed path
(`supports_end_of_response: true`, `lib.rs:12476`).

**This already explains and satisfies the field report:** on the framed path the
identical call, retried on the same (still-aligned) connection, gets a fresh,
well-formed response and succeeds. The transient garbage byte is surfaced once as
a typed error and the session is never silently desynced.

One gap remains even here: `UnknownMessageType` is classified **not retryable**
today — `is_transient()`/`is_retryable()` are false and `retry_hint()` is `Never`
(`lib.rs:1657-1679`). So the successful retry in the field was a *manual /
application-level* retry; an automatic retry layer keyed on `is_retryable()`
would not have replayed it. Making it retryable is **not** safe as a blanket
change (see §5) because retryability is currently framing-agnostic and the
classic path below may leave the wire misaligned.

### 3b. Classic (pre-END_OF_RESPONSE) path — the residual risk

When `supports_end_of_response` is false, there is no end-of-response flag. The
reader decides completion with a **probe over the accumulated payload**:
`response_complete` treats *any* parse result other than `TtcDecode` (the
"ran out of bytes / need more packets" error) as "response complete"
(`lib.rs:9648-9650`), and the classic reassembly loop
(`read_classic_data_response_probed_with_limits`, `lib.rs:9659`) stops on it.

`UnknownMessageType` is **not** `TtcDecode`, so if the accumulated bytes ever
parse far enough to read a bogus message-type byte *before* the server's real
end of response, the reader **stops mid-response**, leaving the remaining packets
stranded in the socket. The connection is then marked `Ready`/`Reusable` (as in
§3a) but the wire is **misaligned**: the next operation reads the stranded tail
and desyncs. Because packet chunking (SDU, server load, prefetch sizing) can vary,
this is plausibly **transient** and "succeeds on retry" — but only because the
retry lands on a *different* pooled connection, while the poisoned one keeps
failing until evicted.

The reference does **not** have this hole. python-oracledb's protocol layer, on
*any* exception from message processing (post-connect, packet already sent),
sends a BREAK marker and `_reset()`s the wire to a clean boundary **before**
re-raising (`.../impl/thin/protocol.pyx:463-470`), and closes the connection
outright when it cannot be left healthy (`_end_request`, `protocol.pyx:434-438`).
The Rust classic path does neither for a post-parse `UnknownMessageType`.

## 4. Why a fix was not landed here

A safe, provable fix could not be written from the code + reference alone:

- **Blanket "mark the session dead on `UnknownMessageType`"** (the obvious patch
  in `post_sync_protocol_error_disposition`) **regresses the deliberate,
  tested framed-path behavior** in §3a — it would discard connections that the
  framed path correctly keeps reusable, breaking
  `borrowed_stream_malformed_and_truncated_fetches_retire_before_reuse`. That is
  a real behavior change dressed up as a bug fix.
- **A classic-path-only fix** (break+drain to resync, mirroring the reference, or
  mark-dead gated on `!supports_end_of_response`) is the *right shape*, but the
  classic path is **not exercised by any loopback test** — `loopback_connection`
  is framed-only (`lib.rs:12476`). Adding classic-path recovery behavior with no
  way to exercise it is guessing, and the report does not say which path
  produced the failure (position 35 / "metadata read" do not disambiguate framed
  vs classic).

Weakening the unknown-message-type rejection to make the symptom disappear is
explicitly out of bounds and would only convert a loud desync into a silent one.

## 5. Safe conservative behavior + how to close this with a repro

To confirm which path fired, capture the failing exchange with the packet-level
connect trace and record the negotiated framing:

```bash
ORACLEDB_TRACE_CONNECT=1   # independent of RUST_LOG; keeps secrets out
```

Record: the server generation, whether `supports_end_of_response` was negotiated
(`Connection::supports_end_of_response`, surfaced at connect), and the raw
response bytes leading to `position 35`.

- **If it fired on the framed path (§3a):** no change is needed for *safety* —
  the wire was aligned and the connection was reusable. The only viable
  enhancement is a **framing-aware** retry hint (surface the error as
  retry-safe-on-the-same-connection *only when the disposition is `Reusable`*),
  never a blanket `is_transient` flag. This is optional polish, not a
  correctness fix.

- **If it fired on the classic path (§3b):** implement the reference-faithful
  behavior — on a post-parse `UnknownMessageType` when `!supports_end_of_response`,
  break + drain the wire to a clean boundary (resync, keep the connection usable),
  **or**, more conservatively, mark the connection dead so the pool evicts it.
  Either MUST be gated on the classic path so it does not regress §3a, and MUST
  ship with a classic-framing loopback test (the reassembly loop stopping
  mid-response on a stranded-packet fixture).

Until such a capture exists, the current behavior is the honest floor: a loud,
typed `Error::Protocol(UnknownMessageType)` that, on the modern framed path,
leaves the session aligned and reusable and lets an identical retry succeed —
which is exactly what the field observed.

## References (verified)

- Error + variant: `crates/oracledb-protocol/src/lib.rs:71`
  (`UnknownMessageType`); message loop fall-through
  `crates/oracledb-protocol/src/thin/fetch.rs:602-613`; embedded-error guard
  `fetch.rs:604-608`.
- Valid message-type set: `crates/oracledb-protocol/src/thin/constants.rs:27-89`.
- Prior deterministic 0x81 desync (fixed): `crates/oracledb-protocol/src/thin/mod.rs:117-155`.
- Completion probe / classic reassembly: `crates/oracledb/src/lib.rs:9648-9650`,
  `lib.rs:9659`.
- Disposition + reuse/retry classification:
  `crates/oracledb/src/recovery.rs:574-590`; `crates/oracledb/src/lib.rs:848-855`,
  `lib.rs:3726-3740`, `lib.rs:1623-1679`.
- Deliberate framed-path reusability test:
  `crates/oracledb/src/lib.rs:17252-17342`, `lib.rs:17345-17350`;
  loopback framing `lib.rs:12476`.
- Reference behavior: `reference/python-oracledb/.../impl/thin/messages/base.pyx:305-308`,
  `.../errors.py:810-813`, `.../impl/thin/protocol.pyx:434-438`, `protocol.pyx:463-470`.
