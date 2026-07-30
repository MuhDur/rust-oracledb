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
pub mod oraclemcp_driver_cx	keep	Driver crate root.
pub use oraclemcp_driver_cx::FromRow	keep	Derive output is part of the typed row API.
pub use oraclemcp_driver_cx::protocol	keep	W1-T9 decision: KEEP. The driver's public API returns protocol-crate types directly (QueryValue, BindValue, ColumnMetadata, ClientIdentity, ...) in 187 signature positions, so users must be able to name them; `oraclemcp_driver_cx::protocol` is the single canonical path for that without a separate version-coupled oraclemcp-driver-cx-protocol dependency. Removing it would break the public contract.
pub use oraclemcp_driver_cx::transport::CassetteError	keep	Cassette diagnostics are part of the record/replay testing surface.
*oraclemcp_driver_cx::AccessToken*	keep	Public credential wrapper used by token authentication.
*oraclemcp_driver_cx::TokenPrivateKey*	keep	Public, redacted credential wrapper for the OCI IAM database-token proof-of-possession private key (paired with AccessToken via with_access_token_and_key).
*oraclemcp_driver_cx::AuthCapabilities*	keep	0.5.1 downstream capability-honesty surface: callers can inspect which known auth modes this thin build supports.
*oraclemcp_driver_cx::AuthModeKind*	keep	Stable classifier for typed authentication modes, used in capability metadata and unsupported-mode diagnostics.
*oraclemcp_driver_cx::AuthModeSupport*	keep	Machine-classifiable support status for each known authentication mode.
*oraclemcp_driver_cx::AuthMode*	keep	Typed authentication intent surface, including supported password/proxy/IAM-token modes and fail-closed unsupported external/Kerberos/RADIUS intents.
*oraclemcp_driver_cx::UnsupportedAuthMode*	keep	Structured diagnostic returned before network I/O when the caller selects a known auth mode unsupported by this thin build.
*oraclemcp_driver_cx::parse_user_and_proxy*	keep	Public, reference-parity parser for bracketed proxy usernames, including the token-auth proxy-only form.
*oraclemcp_driver_cx::BlockingConnection::query<'*	keep	Blocking mirror of the query family.
*oraclemcp_driver_cx::BlockingConnection::query_one*	keep	Blocking mirror of the query-one helper.
*oraclemcp_driver_cx::BlockingConnection::query_opt*	keep	Blocking mirror of the optional-row query helper.
*oraclemcp_driver_cx::BlockingConnection::query_all*	keep	Blocking mirror of the eager query helper.
*oraclemcp_driver_cx::BlockingConnection::query_with*	keep	Blocking mirror of the query builder entry point.
*oraclemcp_driver_cx::BlockingConnection::execute<'*	keep	Blocking mirror of the execute family.
*oraclemcp_driver_cx::BlockingConnection::execute_with*	keep	Blocking mirror of the execute builder entry point.
*oraclemcp_driver_cx::BlockingConnection::execute_many<'*	keep	Blocking mirror of the execute-many family.
*oraclemcp_driver_cx::BlockingConnection::execute_many_with*	keep	Blocking mirror of the execute-many builder entry point.
*oraclemcp_driver_cx::BlockingConnection::register_query*	keep	Blocking mirror of the CQN registration family.
*oraclemcp_driver_cx::BatchError*	keep	Public execute-many row-level error type.
*oraclemcp_driver_cx::BatchOutcome*	keep	Public execute-many outcome type.
*oraclemcp_driver_cx::BatchRows*	keep	Public execute-many bind-row payload type.
*oraclemcp_driver_cx::Batch*	keep	Public execute-many request builder.
*oraclemcp_driver_cx::BindError*	keep	Public client-side bind prevalidation error taxonomy.
*oraclemcp_driver_cx::BlockingConnection::execute_raw*	keep	W2-T1: blocking mirror of the low-level raw-execute primitive (returns QueryResult). Execute-side counterpart to the retained fetch_rows*/define_and_fetch/scroll_cursor/fetch_cursor primitives; part of the 1.0 contract. Surfaced during the pyshim migration: the four families project QueryResult into curated outcomes, so a wire-faithful consumer needs an un-deprecated raw entry point. Keep before the broad BlockingConnection consolidate row.
*oraclemcp_driver_cx::BlockingConnection*	keep	The blocking facade deliberately mirrors the async API; it is the 1.x sync contract.
*oraclemcp_driver_cx::BlockingRows*	keep	Public blocking lazy row facade returned by the blocking query family.
*oraclemcp_driver_cx::CancelHandle*	keep	Public cancellation handle.
*oraclemcp_driver_cx::CollectionElement*	keep	Public object/collection conversion type.
pub fn oraclemcp_driver_cx::ConnectOptions::*	keep	ConnectOptions fields are already private; the public surface is accessor/builder methods with secret-redacting Debug. Consolidate intent satisfied; the accessor API is the 1.x contract.
*oraclemcp_driver_cx::ConnectOptions*	keep	Public connection configuration surface.
*oraclemcp_driver_cx::Connection::execute_raw*	keep	W2-T1: low-level raw-execute primitive (returns the unprojected QueryResult). Execute-side counterpart to the retained fetch_rows*/define_and_fetch/scroll_cursor/fetch_cursor primitives; part of the 1.0 contract. Surfaced during the pyshim migration as the gap W1-T3 missed: the four families project QueryResult into curated outcomes, so a statement-type-agnostic / raw consumer needs an un-deprecated raw entry point.
*oraclemcp_driver_cx::ConnectionDisposition*	keep	Public connection-reuse classification returned by Error::connection_disposition.
*oraclemcp_driver_cx::Connection*	keep	Primary async connection API.
*oraclemcp_driver_cx::resolve_wallet*	keep	0.8.1: public wallet-resolution accessor returning the driver's real precedence decision (chosen file / fell-through / eligibility) so the server doctor reads it instead of mirroring the private load path.
*oraclemcp_driver_cx::WalletResolution*	keep	0.8.1: the wallet-resolution outcome struct returned by resolve_wallet.
*oraclemcp_driver_cx::WalletFile*	keep	0.8.1: enum of wallet file kinds (Pem/P12/Sso) named in WalletResolution.
*oraclemcp_driver_cx::ColumnIndex*	keep	Public owned-row index resolver for usize and &str access.
*oraclemcp_driver_cx::ConversionError*	keep	Public conversion failure taxonomy.
*oraclemcp_driver_cx::DbmsOutput*	keep	Public DBMS_OUTPUT result type.
*oraclemcp_driver_cx::DecodedObject*	keep	Public object decoding result type.
*oraclemcp_driver_cx::ExecutemanyManagerError*	keep	W1-T9: the cursor_logic module is now private, so this is reachable via the single crate-root path only. Kept public because the pyshim conformance harness (executemany_manager_error in cursor.rs) consumes it.
*oraclemcp_driver_cx::ExecutemanyManager*	keep	W1-T9: cursor_logic is now a private module; the type is reachable via the single crate-root path only. Kept public because the pyshim conformance harness (#[pyclass] ExecutemanyManager wrapping oraclemcp_driver_cx::ExecutemanyManager) consumes it across the crate boundary.
*oraclemcp_driver_cx::ExecuteOutcome*	keep	Public execute-family outcome type.
*oraclemcp_driver_cx::Execute*	keep	Public execute-family request builder.
*oraclemcp_driver_cx::Result*	keep	Public result alias.
*oraclemcp_driver_cx::ErrorKind*	keep	Public top-level driver error classification returned by Error::kind.
*oraclemcp_driver_cx::Error*	keep	Public driver error taxonomy.
*oraclemcp_driver_cx::FromRow*	keep	Public typed-row conversion trait.
*oraclemcp_driver_cx::FromSql*	keep	Public inbound SQL conversion trait.
*oraclemcp_driver_cx::IntoBinds*	keep	Public bind collection trait.
*oraclemcp_driver_cx::NotificationOutcome*	keep	Public notification receive outcome.
*oraclemcp_driver_cx::ObjectAttribute*	keep	Public object metadata type.
*oraclemcp_driver_cx::ObjectType*	keep	Public object metadata type.
*oraclemcp_driver_cx::OutBinds*	keep	Public execute-family OUT-bind accessor.
*oraclemcp_driver_cx::Params*	keep	Public single-row bind payload for the operation-family API.
*oraclemcp_driver_cx::PipelineRequest*	keep	Public pipelining request descriptor.
*oraclemcp_driver_cx::RegistrationOutcome*	keep	Public register-query outcome type.
*oraclemcp_driver_cx::Registration*	keep	Public CQN register-query request builder.
*oraclemcp_driver_cx::ReturningRows*	keep	Public execute-family RETURNING accessor.
*oraclemcp_driver_cx::RetryHint*	keep	Public conservative retry guidance returned by Error::retry_hint.
*oraclemcp_driver_cx::Cursor*	keep	Public REF CURSOR handle alias used by Rows.
*oraclemcp_driver_cx::OwnedRowStream*	keep	0.8.2 K10: owned-row futures_core::Stream taking Connection by value (into_row_stream/into_query_stream on Connection); yields Vec<Option<QueryValue>> pages without holding &mut Connection across the yield, enabling true row-by-row streaming downstream. Covers the struct, columns/cursor/into_connection/poll_next accessors, and the Stream/Debug/Drop impls.
*oraclemcp_driver_cx::QueryResultExt*	keep	Public convenience extension for query results.
*oraclemcp_driver_cx::Query*	keep	Public query-family request builder.
*oraclemcp_driver_cx::Rows*	keep	Public lazy row-result facade for the query family.
*oraclemcp_driver_cx::Row*	keep	Public owned row type for the query family.
*oraclemcp_driver_cx::render_caret*	keep	Public diagnostic helper used to render SQL error offsets.
*oraclemcp_driver_cx::Scroll*	keep	Public scroll target for scrollable query cursors.
*oraclemcp_driver_cx::SessionlessError*	keep	Public sessionless transaction error taxonomy.
*oraclemcp_driver_cx::ToSql*	keep	Public outbound SQL conversion trait.
*oraclemcp_driver_cx::TypedRow*	keep	Public typed-row accessor.
*oraclemcp_driver_cx::arrow::*	keep	Feature-gated Arrow integration API.
pub mod oraclemcp_driver_cx::arrow	keep	Feature-gated Arrow module.
*oraclemcp_driver_cx::bind_rows_need_iterative_plsql*	keep	W1-T9: cursor_logic is now private; this predicate is reachable via the single crate-root path only. Kept public because the pyshim conformance harness (async_cursor.rs / cursor.rs) consumes it.
*oraclemcp_driver_cx::fetch_profile*	keep	Explicit diagnostic/profiling knobs exposed by the current crate.
*oraclemcp_driver_cx::obs_record!*	keep	Public observability macro.
*oraclemcp_driver_cx::obs_span!*	keep	Public observability macro.
*oraclemcp_driver_cx::params!*	keep	Public bind helper macro.
*oraclemcp_driver_cx::prelude::*	keep	W1-T9 prelude: curated glob-import convenience namespace re-exporting the everyday types/traits. Each item's canonical path is its non-prelude home; the prelude is the deliberate convenience exception to single-path.
pub mod oraclemcp_driver_cx::prelude	keep	W1-T9 prelude module.
*oraclemcp_driver_cx::pool::PoolBackend*	keep	Public pool extension point: the entire pool API (Pool<B>/BlockingPool<B>/PooledConnection<B>) is generic over this trait and the conformance pyshim implements it (ShimPoolBackend); it is part of the 1.x contract. (Supersedes the early W1-T7 pub(crate) intent.)
*oraclemcp_driver_cx::pool::*	keep	Pool facades, guarded connection ownership, constants, config, options, and error type stay public.
pub mod oraclemcp_driver_cx::pool	keep	Pool module remains the public namespace for pool configuration.
*oraclemcp_driver_cx::soda::*	keep	Feature-gated SODA API.
pub mod oraclemcp_driver_cx::soda	keep	Feature-gated SODA module.
*oraclemcp_driver_cx::transport::capture_scope*	keep	Cassette record/replay testing surface.
*oraclemcp_driver_cx::transport::CaptureScope*	keep	Cassette record/replay testing surface.
*oraclemcp_driver_cx::transport::Cassette*	keep	Cassette record/replay testing surface (CassetteRecorder).
*oraclemcp_driver_cx::transport::Replay*	keep	Cassette record/replay testing surface (ReplayMismatch/ReplayWriteMode).
pub mod oraclemcp_driver_cx::transport	keep	Transport module is already shrunk to cassette record/replay utilities (raw socket halves are pub(crate)); this is the 1.x diagnostics surface.
pub mod oraclemcp_driver_cx_protocol	keep	Protocol crate root.
*oraclemcp_driver_cx_protocol::capabilities::*	keep	Public protocol capability negotiation helpers.
pub mod oraclemcp_driver_cx_protocol::capabilities	keep	Public protocol capability namespace.
*oraclemcp_driver_cx_protocol::ClientIdentity*	keep	Public client identity metadata.
*oraclemcp_driver_cx_protocol::crypto::*	keep	EncryptedPassword is a parameter of the retained public thin::build_auth_phase_two_payload* builders; the crypto auth primitives are part of the sans-io protocol contract.
pub mod oraclemcp_driver_cx_protocol::crypto	keep	Auth crypto module backs the retained thin auth payload builders.
*oraclemcp_driver_cx_protocol::dpl::BatchLoadState*	keep	Validated batch state is used across the driver/protocol crate boundary and has private fields.
*oraclemcp_driver_cx_protocol::dpl::DirectPathStream*	keep	Stream payload type is used by driver direct-path APIs.
*oraclemcp_driver_cx_protocol::dpl::*	keep	Direct-path wire types and pure encode/decode helpers are the protocol crate's public surface.
pub mod oraclemcp_driver_cx_protocol::dpl	keep	Direct-path protocol namespace.
*oraclemcp_driver_cx_protocol::net::*	keep	Connect descriptor and cassette protocol helpers are public protocol utilities.
pub mod oraclemcp_driver_cx_protocol::net	keep	Network descriptor namespace.
*oraclemcp_driver_cx_protocol::oson::*	keep	OSON codec values and helpers are public protocol utilities.
pub mod oraclemcp_driver_cx_protocol::oson	keep	OSON namespace.
*oraclemcp_driver_cx_protocol::packet::*	keep	Sans-io TNS packet primitives are part of the protocol crate's deliberate low-level toolkit.
pub mod oraclemcp_driver_cx_protocol::packet	keep	Packet namespace.
*oraclemcp_driver_cx_protocol::ProtocolError*	keep	Public protocol error taxonomy.
*oraclemcp_driver_cx_protocol::PYTHON_ORACLEDB_REFERENCE*	keep	Public reference-suite provenance constants.
*oraclemcp_driver_cx_protocol::ResourceLimit*	keep	Public typed protocol resource-limit details for error classification.
*oraclemcp_driver_cx_protocol::Result*	keep	Public protocol result alias.
*oraclemcp_driver_cx_protocol::ServerErrorDetails*	keep	Public server error details.
*oraclemcp_driver_cx_protocol::TNS_VERSION*	keep	Public TNS version constants used by protocol capability tests and diagnostics.
*oraclemcp_driver_cx_protocol::sql::*	keep	SQL tokenizer and bind-name helpers are public protocol utilities.
pub mod oraclemcp_driver_cx_protocol::sql	keep	SQL helper namespace.
*oraclemcp_driver_cx_protocol::thin::*	keep	Thin-protocol message, value, and codec types form the protocol crate's core public API.
pub mod oraclemcp_driver_cx_protocol::thin	keep	Thin-protocol namespace.
*oraclemcp_driver_cx_protocol::tls::*	keep	TLS wallet, DN, and SNI helpers are public protocol utilities.
pub mod oraclemcp_driver_cx_protocol::tls	keep	TLS helper namespace.
*oraclemcp_driver_cx_protocol::vector::*	keep	Vector codec values and helpers are public protocol utilities.
pub mod oraclemcp_driver_cx_protocol::vector	keep	Vector namespace.
*oraclemcp_driver_cx_protocol::wire::*	keep	Sans-io TTC wire reader/writer primitives are part of the protocol crate's deliberate low-level toolkit.
pub mod oraclemcp_driver_cx_protocol::wire	keep	Wire helper namespace.
*oraclemcp_driver_cx::RoutineCall*	keep	Driver-native stored procedure/function call builder (0.7.3 a4-plsql-routine): IN, OUT, and function-RETURN binds.
*oraclemcp_driver_cx::RoutineOutcome*	keep	Driver-native routine OUT/RETURN outcome accessor (0.7.3 a4-plsql-routine).
*oraclemcp_driver_cx::OutType*	keep	Typed OUT-bind kind selector for RoutineCall (0.7.3 a4-plsql-routine).
*oraclemcp_driver_cx::LobReader*	keep	Lazy BLOB streaming reader re-export (0.7.3 a4-bbx).
*oraclemcp_driver_cx::LobWriter*	keep	Lazy LOB streaming writer re-export (0.7.3 a4-bbx).
*oraclemcp_driver_cx::ClobReader*	keep	UTF-16-aware lazy CLOB streaming reader re-export (0.7.3 a4-bbx).
*oraclemcp_driver_cx::StatementShapeCache*	keep	Cross-connection statement-shape cache re-export (0.7.3 a4-8pp).
*oraclemcp_driver_cx::ColumnShape*	keep	Described-column shape fingerprint re-export for the shape cache (0.7.3 a4-8pp).
*oraclemcp_driver_cx::ShapeObservation*	keep	Per-SQL shape observation (first_seen/generation/self_healed) re-export (0.7.3 a4-8pp).
*oraclemcp_driver_cx::retry*	keep	Idempotency-gated retry executor over the ORA error taxonomy (0.7.3 a4-r9a).
*oraclemcp_driver_cx::TokenSource*	keep	IAM/OAuth token-source trait and error taxonomy (env/file/exec) for OCI authentication (0.7.3 A3).
*oraclemcp_driver_cx::VERSION*	keep	Crate version constant consumed by the server doctor (0.7.3 A6).
*oraclemcp_driver_cx::BoxFuture*	keep	Boxed-future type alias used in the public async trait surface.
*oraclemcp_driver_cx::obs_warn*	keep	Observability warning macro exported for the differentiator surfaces.
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
| `ExecutemanyManager` | `crates/oracledb/src/cursor_logic.rs:45` | `keep (module privatized)` | W1-T9: the `cursor_logic` module is now private, removing the `oraclemcp_driver_cx::cursor_logic::…` second path; the type stays `pub` at the crate root because the pyshim conformance harness consumes it across the crate boundary. |
| `ExecutemanyManagerError` | `crates/oracledb/src/cursor_logic.rs:15` | `keep (module privatized)` | W1-T9: same disposition as `ExecutemanyManager` — private module, single crate-root path, kept public for the conformance harness. |

## Follow-Up Use

The `api-ledger` block is now the frozen 1.x surface. Future changes should add
or revise rows deliberately, then regenerate the baseline and run the ledger
gate.
