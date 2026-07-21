# 19c parity-version ledger

This is the D11 offline proof for the version surface of the vendored
python-oracledb `v4.0.1` thin reference. The reference root for every source
location below is `reference/python-oracledb/src/oracledb/impl/thin/`.

`docs/reference-gates.tsv` is the machine-checked canonical inventory:
`scripts/extract_reference_gates.sh --check` proves that it contains every
reference gate, and `--check-coverage` proves every non-neutral row names an
existing offline boundary test. This document projects all 45 inventory rows
onto one conservative, synthetic 19c-capability profile so an operator can see
the exact branch selected at 19c without reading the TSV.

## Bound and profile

The profile is deliberately a *reference-derived test fixture*, not a claim
about an observed 19c server handshake:

| Input | Profile value | Source / proof |
| --- | --- | --- |
| TNS protocol | `318` | `constants.pxi:385` is the `MIN_OOB_CHECK` boundary; the synthetic ACCEPT exercises the 318 layout. |
| TTC field version | `13` (`19_1_EXT_1`) | `constants.pxi:503`; python-oracledb writes 13 in `messages/fast_auth.pyx:73-77`. |
| runtime TTC 32K bit | absent | selects 4,000-byte strings, exactly as `capabilities.pyx:146-149`. It is a conservative fixture bit, not an observed 19c claim. |
| fast auth / OOB / EOR flags | all absent | exercises the ordinary classic branch; EOR remains off because its protocol floor is 319. |

The profile is driven three ways:

- `thin::connect::tests::nineteen_c_caps_profile_derives_the_reference_19c_mask` parses a protocol-info message with TTC field version 13 and no 32K bit.
- `version_cassettes::replay_synthetic_19c_caps_cassette_offline` strictly replays a full secret-free `.tns-cassette` CONNECT/ACCEPT exchange through the normal packet writer and replay transport.
- `thin::version_gates::tests::nineteen_c_caps_profile_matches_reference_gate_selection` is the offline reference-gate differential: it checks every implemented TTC gate against the reference's field-13 side. The `query_response` fuzz corpus seed `19c-caps-profile` selects the same field-version 13 decoder profile on each fuzz run.

The existing `harness/differential/diff_oracle.py` is intentionally **not**
called a raw-wire 19c differential: it is a live container round-trip and its
Cython reference decoder cannot accept arbitrary raw TTC bytes. That tool needs
a live server to make behavioral claims. The D11 lanes prove parser and branch
selection only; they do not prove full live-session semantics.

## Exhaustive reference-gate projection

`yes` and `no` in the 19c column are the selected side of the reference gate.
`PARITY-NEUTRAL` is a documented deviation where the feature is not implemented
and the driver conservatively emits no version-specific bytes; it is not a
hidden missing branch. There are no unadjudicated or `MISSING` rows.

| Reference gate | 19c side and behavior | Rust mapping | Disposition |
| --- | --- | --- | --- |
| `capabilities.pyx:126` | no EOR: protocol 318 < 319 | `crates/oracledb-protocol/src/thin/connect.rs` `parse_accept_payload` | OK; synthetic cassette pins false |
| `connection.pyx:680` | no end-user security-context piggyback | no core implementation | PARITY-NEUTRAL; never emitted |
| `connection.pyx:1314` | no pipelined requests | no pipelining implementation | PARITY-NEUTRAL; never emitted |
| `messages/aq_array.pyx:196` | no shard id: 13 < 16 | `thin/aq.rs` array payload | OK; field omitted |
| `messages/aq_base.pyx:129` | no shard-id read | `thin/aq.rs` message-properties parser | OK; field omitted |
| `messages/aq_base.pyx:197` | no shard-id write | `thin/aq.rs` message-properties writer | OK; field omitted |
| `messages/aq_deq.pyx:130` | no JSON-payload byte: 13 < 14 | `thin/aq.rs` dequeue payload | OK; field omitted |
| `messages/aq_deq.pyx:132` | no dequeue shard id | `thin/aq.rs` dequeue payload | OK; field omitted |
| `messages/aq_enq.pyx:115` | no JSON-payload pointer | `thin/aq.rs` enqueue payload | OK; field omitted |
| `messages/auth.pyx:186` | yes: extended five-part `AUTH_VERSION_NO` layout, 13 >= 11 | `crates/oracledb/src/lib.rs` `server_version_number_uses_extended_layout` | OK |
| `messages/base.pyx:238` | no SQL-type/checksum tail: 13 < 14 | `thin/errors.rs` return-info parser | OK; tail not consumed |
| `messages/base.pyx:251` | no EOR error completion | `oracledb/src/lib.rs` response loop | OK; classic completion |
| `messages/base.pyx:297` | no EOR status completion | `oracledb/src/lib.rs` response loop | OK; classic completion |
| `messages/base.pyx:346` | yes: read `oaccolid`, 13 >= 8 | `thin/fetch.rs` column metadata parser | OK |
| `messages/base.pyx:358` | no domain schema/name: 13 < 17 | `thin/fetch.rs` column metadata parser | OK; fields absent |
| `messages/base.pyx:361` | no annotations: 13 < 20 | `thin/fetch.rs` column metadata parser | OK; block absent |
| `messages/base.pyx:376` | no VECTOR metadata: 13 < 24 | `thin/fetch.rs` column metadata parser | OK; fields absent |
| `messages/base.pyx:700` | no function-header pipeline token: 13 < 18 | `thin/connect.rs` and `thin/wire.rs` | OK; token omitted |
| `messages/base.pyx:714` | no piggyback pipeline token | `thin/wire.rs` | OK; token omitted |
| `messages/base.pyx:1429` | yes: write `oaccolid` | `thin/bind.rs` metadata writer | FIXED, boundary test retained |
| `messages/connect.pyx:65` | accept: 318 >= 315 | `thin/connect.rs` `parse_accept_payload` | OK |
| `messages/connect.pyx:75` | yes: parse flags2 at protocol 318 | `thin/connect.rs` `parse_accept_payload` | OK; synthetic cassette covers layout |
| `messages/connect.pyx:111` | no known pre-ACCEPT OOB capability | `thin/connect.rs` CONNECT builder | PARITY-NEUTRAL; fixed `DONT_CARE` path |
| `messages/data_types.pyx:691` | no EOR data-types completion | `oracledb/src/lib.rs` response loop | OK; classic completion |
| `messages/execute.pyx:172` | yes: write `al8sqlsig`, 13 >= 8 | `thin/execute.rs` payload writer | FIXED, boundary test retained |
| `messages/execute.pyx:178` | yes: write chunk ids, 13 >= 9 | `thin/execute.rs` payload writer | FIXED, boundary test retained |
| `messages/protocol.pyx:53` | no EOR protocol completion | `oracledb/src/lib.rs` response loop | OK; classic completion |
| `messages/protocol.pyx:86` | no OSON long field names: 13 < 17 | `oracledb/src/lib.rs` capability derivation | OK; disabled |
| `messages/subscribe.pyx:61` | yes: subscriber name present, 13 >= 7 | `thin/subscr.rs` parser | OK |
| `messages/subscribe.pyx:63` | yes: instance/listener blocks present | `thin/subscr.rs` parser | OK |
| `messages/subscribe.pyx:127` | yes: client-id pointer block written | `thin/subscr.rs` writer | OK |
| `packet.pyx:778` | yes: large-SDU packet framing, 318 >= 315 | `thin/wire.rs` packet encoder | OK |
| `pool.pyx:205` | no request-boundary session state | no DRCP session-state operation | PARITY-NEUTRAL; never emitted |
| `protocol.pyx:65` | no OOB cancellation break | recovery/cancellation path | PARITY-NEUTRAL; no OOB marker |
| `protocol.pyx:195` | no PL/SQL-BOOLEAN capability bit: 13 < 17 | `thin/bind.rs` BOOLEAN binding | PARITY-NEUTRAL; not a conditional wire field |
| `protocol.pyx:196` | same `supports_bool` derivation | `thin/bind.rs` BOOLEAN binding | PARITY-NEUTRAL; same gate as :195 |
| `protocol.pyx:262` | no OOB capability: fixture has no ACCEPT option | `thin/connect.rs` `supports_oob` | OK; false |
| `protocol.pyx:335` | no OOB check probe | no probe implementation | PARITY-NEUTRAL; never emitted |
| `protocol.pyx:347` | no combined fast-auth path: fixture flag absent | `oracledb/src/lib.rs` `Connection::connect` | OK; classic branch selected |
| `protocol.pyx:358` | no EOR toggle around ping | `oracledb/src/lib.rs` response loop | OK; classic branch |
| `protocol.pyx:479` | no in-band OOB fallback marker | cancellation path | PARITY-NEUTRAL; no marker |
| `protocol.pyx:516` | no EOR-gated request-boundary read | `oracledb/src/lib.rs` response loop | OK; classic branch |
| `protocol.pyx:701` | no async combined fast-auth path: fixture flag absent | `oracledb/src/lib.rs` `Connection::connect` | OK; classic branch selected |
| `protocol.pyx:712` | no async EOR toggle around ping | `oracledb/src/lib.rs` response loop | OK; classic branch |
| `protocol.pyx:897` | no async EOR boundary wait | `oracledb/src/lib.rs` response loop | OK; classic branch |

## Evidence boundary

The version matrix supplies real 11g refusal, 18c, 21c, and 23ai lanes; those
generations bracket 19c. This ledger turns that bracket into an enumerated,
machine-checked branch proof. It does **not** upgrade the headline 2462/2578
python-oracledb parity result beyond its recorded 23ai behavioral environment,
and it does not replace an eventual live 19c qualification run.
