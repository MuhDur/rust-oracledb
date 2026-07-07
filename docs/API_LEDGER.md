# API Ledger

This ledger records the intended disposition for the public API captured under
`docs/baseline/public_api/`. At the 1.0.0-rc.1 freeze, every remaining
`api-ledger` row is expected to be `keep`; the other disposition names are kept
below as historical vocabulary for prior Road to 1.0 cleanup decisions.

`scripts/check_api_ledger.sh` treats the `api-ledger` block below as the source
of truth. Patterns are Bash globs matched, in order, against the exact
`cargo public-api` lines from every supported profile. The first matching row is
the line's disposition, so narrower exceptions must appear before broader module
patterns.

Dispositions:

- `keep`: intended public API.
- `pub(crate)`: currently public, intended to become crate-private before 1.0.
- `rename`: keep the capability, but change the public name.
- `consolidate`: keep the capability, but merge or move it into a smaller
  public surface.
- `deprecate`: leave temporarily with a deprecation path before removal.

## Coverage Rules

```api-ledger
pattern	disposition	reason
pub mod oracledb	keep	Driver crate root.
pub use oracledb::FromRow	keep	Derive output is part of the typed row API.
pub use oracledb::protocol	keep	W1-T9 decision: KEEP. The driver's public API returns protocol-crate types directly (QueryValue, BindValue, ColumnMetadata, ClientIdentity, ...) in 187 signature positions, so users must be able to name them; `oracledb::protocol` is the single canonical path for that without a separate version-coupled oracledb-protocol dependency. Removing it would break the public contract.
pub use oracledb::transport::CassetteError	keep	Cassette diagnostics are part of the record/replay testing surface.
*oracledb::AccessToken*	keep	Public credential wrapper used by token authentication.
*oracledb::AuthCapabilities*	keep	0.5.1 downstream capability-honesty surface: callers can inspect which known auth modes this thin build supports.
*oracledb::AuthModeKind*	keep	Stable classifier for typed authentication modes, used in capability metadata and unsupported-mode diagnostics.
*oracledb::AuthModeSupport*	keep	Machine-classifiable support status for each known authentication mode.
*oracledb::AuthMode*	keep	Typed authentication intent surface, including supported password/proxy/IAM-token modes and fail-closed unsupported external/Kerberos/RADIUS intents.
*oracledb::UnsupportedAuthMode*	keep	Structured diagnostic returned before network I/O when the caller selects a known auth mode unsupported by this thin build.
*oracledb::BlockingConnection::query<'*	keep	Blocking mirror of the query family.
*oracledb::BlockingConnection::query_one*	keep	Blocking mirror of the query-one helper.
*oracledb::BlockingConnection::query_opt*	keep	Blocking mirror of the optional-row query helper.
*oracledb::BlockingConnection::query_all*	keep	Blocking mirror of the eager query helper.
*oracledb::BlockingConnection::query_with*	keep	Blocking mirror of the query builder entry point.
*oracledb::BlockingConnection::execute<'*	keep	Blocking mirror of the execute family.
*oracledb::BlockingConnection::execute_with*	keep	Blocking mirror of the execute builder entry point.
*oracledb::BlockingConnection::execute_many<'*	keep	Blocking mirror of the execute-many family.
*oracledb::BlockingConnection::execute_many_with*	keep	Blocking mirror of the execute-many builder entry point.
*oracledb::BlockingConnection::register_query*	keep	Blocking mirror of the CQN registration family.
*oracledb::BatchError*	keep	Public execute-many row-level error type.
*oracledb::BatchOutcome*	keep	Public execute-many outcome type.
*oracledb::BatchRows*	keep	Public execute-many bind-row payload type.
*oracledb::Batch*	keep	Public execute-many request builder.
*oracledb::BindError*	keep	Public client-side bind prevalidation error taxonomy.
*oracledb::BlockingConnection::execute_raw*	keep	W2-T1: blocking mirror of the low-level raw-execute primitive (returns QueryResult). Execute-side counterpart to the retained fetch_rows*/define_and_fetch/scroll_cursor/fetch_cursor primitives; part of the 1.0 contract. Surfaced during the pyshim migration: the four families project QueryResult into curated outcomes, so a wire-faithful consumer needs an un-deprecated raw entry point. Keep before the broad BlockingConnection consolidate row.
*oracledb::BlockingConnection*	keep	The blocking facade deliberately mirrors the async API; it is the 1.x sync contract.
*oracledb::BlockingRows*	keep	Public blocking lazy row facade returned by the blocking query family.
*oracledb::CancelHandle*	keep	Public cancellation handle.
*oracledb::CollectionElement*	keep	Public object/collection conversion type.
pub fn oracledb::ConnectOptions::*	keep	ConnectOptions fields are already private; the public surface is accessor/builder methods with secret-redacting Debug. Consolidate intent satisfied; the accessor API is the 1.x contract.
*oracledb::ConnectOptions*	keep	Public connection configuration surface.
*oracledb::Connection::execute_raw*	keep	W2-T1: low-level raw-execute primitive (returns the unprojected QueryResult). Execute-side counterpart to the retained fetch_rows*/define_and_fetch/scroll_cursor/fetch_cursor primitives; part of the 1.0 contract. Surfaced during the pyshim migration as the gap W1-T3 missed: the four families project QueryResult into curated outcomes, so a statement-type-agnostic / raw consumer needs an un-deprecated raw entry point.
*oracledb::ConnectionDisposition*	keep	Public connection-reuse classification returned by Error::connection_disposition.
*oracledb::Connection*	keep	Primary async connection API.
*oracledb::ColumnIndex*	keep	Public owned-row index resolver for usize and &str access.
*oracledb::ConversionError*	keep	Public conversion failure taxonomy.
*oracledb::DbmsOutput*	keep	Public DBMS_OUTPUT result type.
*oracledb::DecodedObject*	keep	Public object decoding result type.
*oracledb::ExecutemanyManagerError*	keep	W1-T9: the cursor_logic module is now private, so this is reachable via the single crate-root path only. Kept public because the pyshim conformance harness (executemany_manager_error in cursor.rs) consumes it.
*oracledb::ExecutemanyManager*	keep	W1-T9: cursor_logic is now a private module; the type is reachable via the single crate-root path only. Kept public because the pyshim conformance harness (#[pyclass] ExecutemanyManager wrapping oracledb::ExecutemanyManager) consumes it across the crate boundary.
*oracledb::ExecuteOutcome*	keep	Public execute-family outcome type.
*oracledb::Execute*	keep	Public execute-family request builder.
*oracledb::Result*	keep	Public result alias.
*oracledb::ErrorKind*	keep	Public top-level driver error classification returned by Error::kind.
*oracledb::Error*	keep	Public driver error taxonomy.
*oracledb::FromRow*	keep	Public typed-row conversion trait.
*oracledb::FromSql*	keep	Public inbound SQL conversion trait.
*oracledb::IntoBinds*	keep	Public bind collection trait.
*oracledb::NotificationOutcome*	keep	Public notification receive outcome.
*oracledb::ObjectAttribute*	keep	Public object metadata type.
*oracledb::ObjectType*	keep	Public object metadata type.
*oracledb::OutBinds*	keep	Public execute-family OUT-bind accessor.
*oracledb::Params*	keep	Public single-row bind payload for the operation-family API.
*oracledb::PipelineRequest*	keep	Public pipelining request descriptor.
*oracledb::RegistrationOutcome*	keep	Public register-query outcome type.
*oracledb::Registration*	keep	Public CQN register-query request builder.
*oracledb::ReturningRows*	keep	Public execute-family RETURNING accessor.
*oracledb::RetryHint*	keep	Public conservative retry guidance returned by Error::retry_hint.
*oracledb::Cursor*	keep	Public REF CURSOR handle alias used by Rows.
*oracledb::QueryResultExt*	keep	Public convenience extension for query results.
*oracledb::Query*	keep	Public query-family request builder.
*oracledb::Rows*	keep	Public lazy row-result facade for the query family.
*oracledb::Row*	keep	Public owned row type for the query family.
*oracledb::render_caret*	keep	Public diagnostic helper used to render SQL error offsets.
*oracledb::Scroll*	keep	Public scroll target for scrollable query cursors.
*oracledb::SessionlessError*	keep	Public sessionless transaction error taxonomy.
*oracledb::ToSql*	keep	Public outbound SQL conversion trait.
*oracledb::TypedRow*	keep	Public typed-row accessor.
*oracledb::arrow::*	keep	Feature-gated Arrow integration API.
pub mod oracledb::arrow	keep	Feature-gated Arrow module.
*oracledb::bind_rows_need_iterative_plsql*	keep	W1-T9: cursor_logic is now private; this predicate is reachable via the single crate-root path only. Kept public because the pyshim conformance harness (async_cursor.rs / cursor.rs) consumes it.
*oracledb::fetch_profile*	keep	Explicit diagnostic/profiling knobs exposed by the current crate.
*oracledb::obs_record!*	keep	Public observability macro.
*oracledb::obs_span!*	keep	Public observability macro.
*oracledb::params!*	keep	Public bind helper macro.
*oracledb::prelude::*	keep	W1-T9 prelude: curated glob-import convenience namespace re-exporting the everyday types/traits. Each item's canonical path is its non-prelude home; the prelude is the deliberate convenience exception to single-path.
pub mod oracledb::prelude	keep	W1-T9 prelude module.
*oracledb::pool::PoolBackend*	keep	Public pool extension point: the entire pool API (Pool<B>/BlockingPool<B>/PooledConnection<B>) is generic over this trait and the conformance pyshim implements it (ShimPoolBackend); it is part of the 1.x contract. (Supersedes the early W1-T7 pub(crate) intent.)
*oracledb::pool::*	keep	Pool facades, guarded connection ownership, constants, config, options, and error type stay public.
pub mod oracledb::pool	keep	Pool module remains the public namespace for pool configuration.
*oracledb::soda::*	keep	Feature-gated SODA API.
pub mod oracledb::soda	keep	Feature-gated SODA module.
*oracledb::transport::capture_scope*	keep	Cassette record/replay testing surface.
*oracledb::transport::CaptureScope*	keep	Cassette record/replay testing surface.
*oracledb::transport::Cassette*	keep	Cassette record/replay testing surface (CassetteRecorder).
*oracledb::transport::Replay*	keep	Cassette record/replay testing surface (ReplayMismatch/ReplayWriteMode).
pub mod oracledb::transport	keep	Transport module is already shrunk to cassette record/replay utilities (raw socket halves are pub(crate)); this is the 1.x diagnostics surface.
pub mod oracledb_protocol	keep	Protocol crate root.
*oracledb_protocol::capabilities::*	keep	Public protocol capability negotiation helpers.
pub mod oracledb_protocol::capabilities	keep	Public protocol capability namespace.
*oracledb_protocol::ClientIdentity*	keep	Public client identity metadata.
*oracledb_protocol::crypto::*	keep	EncryptedPassword is a parameter of the retained public thin::build_auth_phase_two_payload* builders; the crypto auth primitives are part of the sans-io protocol contract.
pub mod oracledb_protocol::crypto	keep	Auth crypto module backs the retained thin auth payload builders.
*oracledb_protocol::dpl::BatchLoadState*	keep	Validated batch state is used across the driver/protocol crate boundary and has private fields.
*oracledb_protocol::dpl::DirectPathStream*	keep	Stream payload type is used by driver direct-path APIs.
*oracledb_protocol::dpl::*	keep	Direct-path wire types and pure encode/decode helpers are the protocol crate's public surface.
pub mod oracledb_protocol::dpl	keep	Direct-path protocol namespace.
*oracledb_protocol::net::*	keep	Connect descriptor and cassette protocol helpers are public protocol utilities.
pub mod oracledb_protocol::net	keep	Network descriptor namespace.
*oracledb_protocol::oson::*	keep	OSON codec values and helpers are public protocol utilities.
pub mod oracledb_protocol::oson	keep	OSON namespace.
*oracledb_protocol::packet::*	keep	Sans-io TNS packet primitives are part of the protocol crate's deliberate low-level toolkit.
pub mod oracledb_protocol::packet	keep	Packet namespace.
*oracledb_protocol::ProtocolError*	keep	Public protocol error taxonomy.
*oracledb_protocol::PYTHON_ORACLEDB_REFERENCE*	keep	Public reference-suite provenance constants.
*oracledb_protocol::ResourceLimit*	keep	Public typed protocol resource-limit details for error classification.
*oracledb_protocol::Result*	keep	Public protocol result alias.
*oracledb_protocol::ServerErrorDetails*	keep	Public server error details.
*oracledb_protocol::TNS_VERSION*	keep	Public TNS version constants used by protocol capability tests and diagnostics.
*oracledb_protocol::sql::*	keep	SQL tokenizer and bind-name helpers are public protocol utilities.
pub mod oracledb_protocol::sql	keep	SQL helper namespace.
*oracledb_protocol::thin::*	keep	Thin-protocol message, value, and codec types form the protocol crate's core public API.
pub mod oracledb_protocol::thin	keep	Thin-protocol namespace.
*oracledb_protocol::tls::*	keep	TLS wallet, DN, and SNI helpers are public protocol utilities.
pub mod oracledb_protocol::tls	keep	TLS helper namespace.
*oracledb_protocol::vector::*	keep	Vector codec values and helpers are public protocol utilities.
pub mod oracledb_protocol::vector	keep	Vector namespace.
*oracledb_protocol::wire::*	keep	Sans-io TTC wire reader/writer primitives are part of the protocol crate's deliberate low-level toolkit.
pub mod oracledb_protocol::wire	keep	Wire helper namespace.
*oracledb::RoutineCall*	keep	Driver-native stored procedure/function call builder (0.7.3 a4-plsql-routine): IN, OUT, and function-RETURN binds.
*oracledb::RoutineOutcome*	keep	Driver-native routine OUT/RETURN outcome accessor (0.7.3 a4-plsql-routine).
*oracledb::OutType*	keep	Typed OUT-bind kind selector for RoutineCall (0.7.3 a4-plsql-routine).
*oracledb::LobReader*	keep	Lazy BLOB streaming reader re-export (0.7.3 a4-bbx).
*oracledb::LobWriter*	keep	Lazy LOB streaming writer re-export (0.7.3 a4-bbx).
*oracledb::ClobReader*	keep	UTF-16-aware lazy CLOB streaming reader re-export (0.7.3 a4-bbx).
*oracledb::StatementShapeCache*	keep	Cross-connection statement-shape cache re-export (0.7.3 a4-8pp).
*oracledb::ColumnShape*	keep	Described-column shape fingerprint re-export for the shape cache (0.7.3 a4-8pp).
*oracledb::ShapeObservation*	keep	Per-SQL shape observation (first_seen/generation/self_healed) re-export (0.7.3 a4-8pp).
*oracledb::retry*	keep	Idempotency-gated retry executor over the ORA error taxonomy (0.7.3 a4-r9a).
*oracledb::TokenSource*	keep	IAM/OAuth token-source trait and error taxonomy (env/file/exec) for OCI authentication (0.7.3 A3).
*oracledb::VERSION*	keep	Crate version constant consumed by the server doctor (0.7.3 A6).
*oracledb::BoxFuture*	keep	Boxed-future type alias used in the public async trait surface.
*oracledb::obs_warn*	keep	Observability warning macro exported for the differentiator surfaces.
```

## Accidental-Leak Decisions

| Item | Source | Disposition | Reason |
| --- | --- | --- | --- |
| `ObsSpanGuard` | `crates/oracledb/src/obs.rs:106` | `pub(crate)` | The guard is an RAII implementation detail behind `obs_span!`; exposing it freezes tracing internals. |
| `OracleReadHalf` | `crates/oracledb/src/transport.rs:40` | `pub(crate)` | Socket/TLS split halves should be hidden behind the connector and `ConnectionCore`. |
| `OracleWriteHalf` | `crates/oracledb/src/transport.rs:58` | `pub(crate)` | Same transport-internal disposition as `OracleReadHalf`. |
| `PoolEngine<B>` | `crates/oracledb/src/pool.rs:164` | `pub(crate)` | Low-level sync pool engine is not the intended async-native pool facade. |
| `DirectPathStream` | `crates/oracledb-protocol/src/dpl.rs:722` | `keep` | Driver direct-path APIs accept this payload type directly. |
| `BatchLoadState` | `crates/oracledb-protocol/src/dpl.rs:792` | `keep` | Validated batch state is used across the driver/protocol crate boundary and has private fields. |
| `DirectPathPieceBuffer` | `crates/oracledb-protocol/src/dpl.rs:391` | `pub(crate)` | Piece assembly buffer is an encoder implementation detail. |
| `ExecutemanyManager` | `crates/oracledb/src/cursor_logic.rs:45` | `keep (module privatized)` | W1-T9: the `cursor_logic` module is now private, removing the `oracledb::cursor_logic::…` second path; the type stays `pub` at the crate root because the pyshim conformance harness consumes it across the crate boundary. |
| `ExecutemanyManagerError` | `crates/oracledb/src/cursor_logic.rs:15` | `keep (module privatized)` | W1-T9: same disposition as `ExecutemanyManager` — private module, single crate-root path, kept public for the conformance harness. |

## Follow-Up Use

The `api-ledger` block is now the frozen 1.x surface. Future changes should add
or revise rows deliberately, then regenerate the baseline and run the ledger
gate.
