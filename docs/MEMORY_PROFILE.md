# Bounded Memory Profile

`rust-oracledb` bounds per-operation memory by configuration and protocol
limits, not by total result-set size. A query that returns many rows is paged;
large LOBs are read by requested chunks; batch and pipeline surfaces check row,
bind, and frame counts before building or extending buffers.

## Central Limits

The shared resource policy is `ProtocolLimits`. It names packet, frame,
response, column, bind, batch-row, object, vector, LOB-chunk, and generic
length-prefixed collection caps in one struct
([crates/oracledb-protocol/src/wire.rs](/home/durakovic/projects/rust-oracledb/crates/oracledb-protocol/src/wire.rs:18)).
The default policy is explicit: 16 MiB packets/frames, 256 MiB cumulative
response bytes, 4096 columns, 65,535 binds, 1,000,000 batch rows, and bounded
object/vector/LOB collection counts
([crates/oracledb-protocol/src/wire.rs](/home/durakovic/projects/rust-oracledb/crates/oracledb-protocol/src/wire.rs:49)).
`validate()` rejects internally inconsistent policies before connection use, and
the `check_*` methods centralize enforcement
([crates/oracledb-protocol/src/wire.rs](/home/durakovic/projects/rust-oracledb/crates/oracledb-protocol/src/wire.rs:64)).

`ConnectOptions` carries both the negotiated SDU and the active
`ProtocolLimits`, with default SDU 8192 and `ProtocolLimits::DEFAULT`
([crates/oracledb/src/lib.rs](/home/durakovic/projects/rust-oracledb/crates/oracledb/src/lib.rs:2939),
[crates/oracledb/src/lib.rs](/home/durakovic/projects/rust-oracledb/crates/oracledb/src/lib.rs:3001)).
The statement cache is also count-bounded: default 20 cached statements, and
`with_statement_cache_size()` sets a fixed maximum rather than an unbounded map
([crates/oracledb/src/lib.rs](/home/durakovic/projects/rust-oracledb/crates/oracledb/src/lib.rs:2990),
[crates/oracledb/src/lib.rs](/home/durakovic/projects/rust-oracledb/crates/oracledb/src/lib.rs:3041)).

## Packets And Responses

Writes are packetized by SDU. `send_data_packet_with_flags()` computes
`max_payload = sdu - overhead`, iterates `payload.chunks(max_payload)`, and only
builds one packet per chunk
([crates/oracledb/src/lib.rs](/home/durakovic/projects/rust-oracledb/crates/oracledb/src/lib.rs:8990)).
Reads enforce `max_response_bytes` when combining flush-out-bind continuations
([crates/oracledb/src/lib.rs](/home/durakovic/projects/rust-oracledb/crates/oracledb/src/lib.rs:9369))
and when accumulating multi-packet DATA responses
([crates/oracledb/src/lib.rs](/home/durakovic/projects/rust-oracledb/crates/oracledb/src/lib.rs:9605)).
The single-packet passthrough path still checks the response bound before moving
the owned packet buffer into the response
([crates/oracledb/src/lib.rs](/home/durakovic/projects/rust-oracledb/crates/oracledb/src/lib.rs:9614)).

## Fetch Paging

The public `Query` builder defaults `prefetch` to the configured `arraysize`,
and lets callers set both explicitly
([crates/oracledb/src/lib.rs](/home/durakovic/projects/rust-oracledb/crates/oracledb/src/lib.rs:1614),
[crates/oracledb/src/lib.rs](/home/durakovic/projects/rust-oracledb/crates/oracledb/src/lib.rs:1648)).
LOB materialization is opt-out via `stream_lobs()`, so callers can keep LOB
payloads off the row materialization path
([crates/oracledb/src/lib.rs](/home/durakovic/projects/rust-oracledb/crates/oracledb/src/lib.rs:1662)).
Low-level fetch APIs take an `arraysize` per continuation request; large result
sets are therefore represented as bounded batches rather than one result-size
allocation.

The decoder also bounds server-advertised collection counts before reserving:
OUT bind arrays and DML RETURNING rows check `max_batch_rows`, then allocate with
`with_capacity_limited()` so a dishonest count cannot reserve more than the
remaining payload can justify
([crates/oracledb-protocol/src/thin/fetch.rs](/home/durakovic/projects/rust-oracledb/crates/oracledb-protocol/src/thin/fetch.rs:830),
[crates/oracledb-protocol/src/thin/fetch.rs](/home/durakovic/projects/rust-oracledb/crates/oracledb-protocol/src/thin/fetch.rs:869)).

## LOB Chunks

LOB reads are caller-bounded by `(locator, offset, amount)`: the read payload
serializes the requested `amount`, not the whole logical LOB
([crates/oracledb-protocol/src/thin/lob.rs](/home/durakovic/projects/rust-oracledb/crates/oracledb-protocol/src/thin/lob.rs:6)).
LOB writes check locator and data byte lengths against `max_frame_bytes` before
building the payload
([crates/oracledb/src/lib.rs](/home/durakovic/projects/rust-oracledb/crates/oracledb/src/lib.rs:6376)).
Freeing temporary LOBs checks both locator count (`max_lob_chunks`) and per-locator
frame size before serializing, so a large locator list cannot grow unbounded.

## Batch And Pipeline Buffers

`ExecutemanyManager` is pure batch window arithmetic: a batch never exceeds the
configured `batch_size`, never crosses a chunk boundary, stores only cumulative
chunk ends, and advances by `(message_offset, num_rows)`
([crates/oracledb/src/cursor_logic.rs](/home/durakovic/projects/rust-oracledb/crates/oracledb/src/cursor_logic.rs:36),
[crates/oracledb/src/cursor_logic.rs](/home/durakovic/projects/rust-oracledb/crates/oracledb/src/cursor_logic.rs:66)).
Pipeline execution checks the request count, each bind-row count, and each row's
bind count before appending payload bytes
([crates/oracledb/src/lib.rs](/home/durakovic/projects/rust-oracledb/crates/oracledb/src/lib.rs:7350),
[crates/oracledb/src/lib.rs](/home/durakovic/projects/rust-oracledb/crates/oracledb/src/lib.rs:7386)).

The practical rule is: memory for one operation is bounded by SDU, selected
arraysize/prefetch, statement-cache size, explicit LOB amount/chunk counts, batch
size, and `ProtocolLimits`. It is not proportional to the total row count of a
drained result set unless the caller explicitly collects all pages into its own
application data structure.
