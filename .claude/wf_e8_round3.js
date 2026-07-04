export const meta = {
  name: 'e8-bughunt-round',
  description: 'E8 fresh-eyes correctness bug-hunt: fan-out area finders + adversarial multi-skeptic verification, return only majority-confirmed real bugs',
  phases: [
    { title: 'Find', detail: 'one deep finder per driver area' },
    { title: 'Verify', detail: '3 adversarial skeptics per candidate; keep majority-real' },
  ],
}

// Areas already audited + the 12 bugs already found/fixed (do NOT re-report).
const ALREADY = `ALREADY FOUND + FIXED this campaign (do NOT re-report these or close variants):
pool force-close waiter-requeue leak; query_one/query_opt false TooManyRows on single-row LONG;
execute_many RETURNING dropping all-but-first iteration (BatchOutcome coalescing);
stream_lobs() CLOB "invalid ub8 length" (LOB prefetch vs plain-locator decode mode);
f32::from_sql overflow to inf (now OutOfRange); INTERVAL DS sub-microsecond truncation
(encode_interval_ds now ns-native); fetch_rows_ref missing cancel-on-drop recovery;
borrowed-vs-owned trailing-zero NUMBER canonicalization;
sparse VECTOR count as-u16 narrowing + missing length validation;
AQ RAW/JSON dequeue silent truncation on short image; SODA mixed-case column quoting;
DbObject image value format (now SINGLE big-endian u32, 245-cutoff, NO chunking — matches
DbObjectPickleBuffer; do NOT re-report chunked-vs-u32be); long-bind ordering threshold now
uses negotiated max_string_size (not hardcoded 32767); 25 request-sending methods (read_lob,
commit, rollback, ping, AQ, change_password, subscribe, sessionless/TPC, scroll, direct-path,
pipeline) now call ensure_clean_before_request (dirty-wire after dropped cancellable future);
growable pool now grows toward max for all concurrent waiters; NULL native BOOLEAN
OUT/RETURNING (negative actual_num_bytes) now decodes None;
borrowed/zero-copy fetch path (for_each_row_ref, arrow columnar) now selects LOB decode mode
from lob_prefetch_cursors like the owned path (DefineMetadata for CLOB/BLOB prefetch cursors);
ConnectOptions Debug now redacts password + wallet_password + access_token;
UROWID added to the buffer_size==0 describe-NULL short-circuit exemption (owned + borrowed);
number_to_json keeps exact text when f64 is lossy (no precision loss); AUTH_SERIAL_NUM parsed
with ub2 (16-bit) semantics (no connect abort >65535); pool returns-dead-connection now routed
through drop_conn (backend.close_connection runs); pyshim BINARY_INTEGER var-length-BE decode +
pack_element ATOMIC_NULL-by-own-type.
ALSO REFUTED (do NOT report): DbObject read_value_bytes length-0 == EMPTY (NULL is the
TNS_OBJ_ATOMIC_NULL=253 indicator, not length-0); owned-vs-borrowed NUMBER >=1e39 differ only in
internal enum variant (observable canonical/i128/i64/is_integer match); repeated named binds
coalesced to one value (correct Oracle bind-by-name); scroll RELATIVE offset (unverified, no
confirmed reference divergence).
TRACKED EXCEPTIONS (do NOT report as bugs): async Rows::into_typed current-batch semantics
(documented, W4-T1); connect/transport feature gaps (DSN params not applied, no multi-address
failover, listener REDIRECT unsupported/fail-closed, Oracle-SNI degrade) — these are
feature-completeness, not correctness bugs.`

const METHOD = `You are a senior Rust + Oracle TNS/TTC protocol auditor doing a fresh-eyes
correctness pass of the pure-Rust thin driver at /home/durakovic/projects/rust-oracledb.
Read the actual source carefully (use Read/Grep/Bash; do NOT build — analysis only). The
reference for correct behavior is python-oracledb v4.0.1 thin mode (cite it when the right
answer is non-obvious). Report ONLY real correctness defects in IMPLEMENTED behavior:
wrong value/precision/sign/range, encode/decode asymmetry, lost/duplicated/truncated data,
off-by-one, integer overflow/narrowing, wrong NULL handling, a wrong error mapping, a state
machine that can wedge/desync, a missed cancel checkpoint. Do NOT report: style, missing
features/feature-gaps, defensive-validation-of-impossible-server-input unless it causes
silent data corruption, or anything in the ALREADY list. Be precise and conservative:
only report what you can defend with exact file:line + the exact input -> got vs expected.`

const FINDING_SCHEMA = {
  type: 'object',
  additionalProperties: false,
  properties: {
    findings: {
      type: 'array',
      items: {
        type: 'object',
        additionalProperties: false,
        properties: {
          severity: { type: 'string', enum: ['P0', 'P1', 'P2', 'P3'] },
          area: { type: 'string' },
          file: { type: 'string' },
          line: { type: 'integer' },
          title: { type: 'string' },
          bug: { type: 'string', description: 'exact input -> got vs expected, and why it is wrong' },
          reference: { type: 'string', description: 'python-oracledb v4.0.1 behavior or code citation supporting the correct answer' },
        },
        required: ['severity', 'area', 'file', 'line', 'title', 'bug'],
      },
    },
  },
  required: ['findings'],
}

const VERDICT_SCHEMA = {
  type: 'object',
  additionalProperties: false,
  properties: {
    refuted: { type: 'boolean', description: 'true if this is NOT a real correctness bug (intended/documented/matches python-oracledb/feature-gap/already-fixed/cannot-occur)' },
    reasoning: { type: 'string' },
  },
  required: ['refuted', 'reasoning'],
}

const AREAS = [
  { key: 'number', prompt: 'NUMBER decode AND encode: crates/oracledb-protocol/src/thin/number.rs + the number paths in codecs.rs + the i128/rust_decimal bridge in crates/oracledb/src/sql_convert.rs. Edge magnitudes, negative/zero, max precision/scale, scientific/exponent forms, integer-vs-decimal, rounding, the canonical text. (trailing-zero canon already fixed.)' },
  { key: 'temporal', prompt: 'DATE/TIMESTAMP/TIMESTAMP WITH (LOCAL) TIME ZONE/INTERVAL YM/INTERVAL DS in crates/oracledb-protocol/src/thin/codecs.rs and the chrono bridge in sql_convert.rs. Fractional seconds, TZ offset vs named region, sign, leading precision, DST/boundary. (interval ns already fixed.)' },
  { key: 'oson', prompt: 'OSON/JSON in crates/oracledb-protocol/src/oson.rs: every TNS_JSON_TYPE_* scalar + container encode/decode symmetry, nesting depth, large counts, shared/relative offsets, the serde_json bridge.' },
  { key: 'vector', prompt: 'VECTOR in crates/oracledb-protocol/src/vector.rs: dense + sparse, INT8/FLOAT32/FLOAT64 element widths, dimension counts, the flags byte, encode/decode symmetry. (sparse u16 already fixed.)' },
  { key: 'lob', prompt: 'LOB in crates/oracledb/src/lib.rs (read_lob/write_lob/trim/length/free_temp) + the LOB decode in fetch.rs: offset/amount math (chars vs bytes for CLOB), multi-chunk reads, final-chunk handling, temp-LOB create/free, locator reuse. (stream_lobs CLOB decode already fixed.)' },
  { key: 'framing', prompt: 'Multi-packet/framing/borrowed-vs-owned: crates/oracledb-protocol/src/wire.rs + thin/mod.rs + thin/fetch.rs. Packet header/flags/markers, multi-packet reassembly, length-prefix/BoundedReader limits, the borrowed (parse_column_slot/to_owned_value) vs owned (parse_column_value) decode parity for EVERY type. (NUMBER canon already fixed.)' },
  { key: 'cancel', prompt: 'Cancel/timeout/recovery in crates/oracledb/src/lib.rs (recovery phases, observe_cancellation_between_round_trips, ensure_clean_before_request, read_response_cancellable, recover_from_call_timeout) + transport.rs. CancelKind->disposition mapping correctness, exactly-one BREAK/RESET, deferred error surfacing, single-operation deadline spanning all batches/LOB chunks. (fetch_rows_ref already fixed.)' },
  { key: 'pool', prompt: 'Pool in crates/oracledb/src/pool.rs (sans-io lifecycle + async engine): getmode WAIT/NOWAIT/TIMEDWAIT, min/max/increment growth, reaper idle-expiry + ping, acquire fairness/ordering, release, drop order, DPY-4005. (force-close waiter requeue already fixed; do not re-report it.)' },
  { key: 'bind', prompt: 'Bind/define + the four families in crates/oracledb-protocol/src/thin/bind.rs + crates/oracledb/src/lib.rs: positional vs named binds, repeated binds, OUT/IN-OUT, type/size inference, NULL binds, array-DML bind width/occurrence, RETURNING/OUT bind sizing, implicit result sets, scroll. (RETURNING coalescing + query_one cardinality already fixed.)' },
  { key: 'errors-auth', prompt: 'Error classification + auth: the Error::kind/ora_code/is_connection_lost/is_transient/retry_hint/resource_limit mapping in crates/oracledb/src/lib.rs, and the O5LOGON/proxy/change-password auth + secret redaction in crates/oracledb-protocol/src/thin/ (auth/connect) + crates/oracledb/src. Wrong ORA/DPY->kind mapping, a non-retryable marked retryable (or vice-versa), a secret that escapes redaction.' },
  { key: 'dbobject-aq', prompt: 'DbObject/collections + AQ in crates/oracledb-protocol/src/thin/dbobject.rs + aq.rs + the crates/oracledb/src object/AQ surfaces: attribute get/set, collection index, NULL attributes, type metadata, enqueue/dequeue message props, payload encode/decode. (DbObject long-value + AQ truncation already fixed.)' },
]

phase('Find')
log(`E8 bug-hunt round: ${AREAS.length} area finders -> adversarial verify`)
const found = await parallel(AREAS.map(a => () =>
  agent(`${METHOD}\n\nAREA: ${a.prompt}\n\n${ALREADY}\n\nReturn structured findings (possibly empty).`,
    { label: `find:${a.key}`, phase: 'Find', schema: FINDING_SCHEMA, agentType: 'general-purpose' })
))

const candidates = found.filter(Boolean).flatMap((r, i) =>
  (r.findings || []).map(f => ({ ...f, area: f.area || AREAS[i].key })))
log(`candidates before verify: ${candidates.length}`)

phase('Verify')
// LEAN mode (spend/rate-limit budget): one skeptic per candidate (not 3); the orchestrator
// does the final adversarial verification on survivors. Keeps the agent burst small.
const verified = await parallel(candidates.map(c => () =>
  agent(`Adversarially REFUTE this claimed correctness bug in rust-oracledb. Read the ACTUAL code at ${c.file}:${c.line} (and around it), and check against python-oracledb v4.0.1 behavior (reference checkout under reference/python-oracledb/). A claim is REFUTED (refuted=true) if it is intended/documented behavior, matches python-oracledb, is a feature-gap rather than a correctness bug, cannot actually occur (server never sends it AND no silent corruption), is already in the ALREADY list, or is simply wrong about the code. Default to refuted=true when uncertain.\n\nCLAIM [${c.severity}] ${c.file}:${c.line} — ${c.title}\n${c.bug}\nreference: ${c.reference || '(none)'}\n\n${ALREADY}`,
    { label: `verify:${c.area}`, phase: 'Verify', schema: VERDICT_SCHEMA, agentType: 'general-purpose' })
    .then(v => ({ ...c, refuted: v ? v.refuted : null, reasoning: v ? v.reasoning : 'verifier-null' }))))

const survivors = verified.filter(Boolean).filter(c => c.refuted === false || c.refuted === null)
log(`SURVIVORS (not refuted by the single skeptic): ${survivors.length} of ${candidates.length}`)
return { candidates: candidates.length, confirmed: survivors }
