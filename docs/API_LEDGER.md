# API Ledger

This ledger records the intended disposition for the public API captured under
`docs/baseline/public_api/`. It is a planning artifact for the Road to 1.0 API
cleanup: entries marked `pub(crate)`, `rename`, `consolidate`, or `deprecate`
are not changed by this file. Follow-up beads apply those decisions.

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
*oracledb::BlockingConnection::execute_query_for_registration*	rename	Keep the registration capability but rename it into an explicit registration API.
*oracledb::BlockingConnection::execute_query*	consolidate	Query execution overloads should collapse into operation-specific request types.
*oracledb::BlockingConnection::query_named*	consolidate	Named-query overloads should collapse into the same operation-family surface.
*oracledb::BlockingConnection*	consolidate	Keep the sync facade, but W1-T8 should reduce duplicated async/blocking method sprawl.
*oracledb::BlockingRows*	keep	Public blocking lazy row facade returned by the blocking query family.
*oracledb::CancelHandle*	keep	Public cancellation handle.
*oracledb::CollectionElement*	keep	Public object/collection conversion type.
pub fn oracledb::ConnectOptions::*	consolidate	Keep ConnectOptions public but privatize fields behind builders/getters for redaction and SemVer evolution.
*oracledb::ConnectOptions*	keep	Public connection configuration surface.
*oracledb::Connection::execute_query_for_registration*	rename	Keep the registration capability but rename it into an explicit registration API.
*oracledb::Connection::execute_query*	consolidate	Query execution overloads should collapse into operation-specific request types.
*oracledb::Connection::query_named*	consolidate	Named-query overloads should collapse into the same operation-family surface.
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
*oracledb::pool::PoolBackend*	pub(crate)	Backend trait is the internal engine seam; W1-T7 introduces the async pool facade.
*oracledb::pool::*	keep	Pool facades, guarded connection ownership, constants, config, options, and error type stay public.
pub mod oracledb::pool	keep	Pool module remains the public namespace for pool configuration.
*oracledb::soda::qbe::*	pub(crate)	Query-by-example SQL generation is SODA implementation detail.
pub mod oracledb::soda::qbe	pub(crate)	Query-by-example SQL generation is SODA implementation detail.
*oracledb::soda::*	keep	Feature-gated SODA API.
pub mod oracledb::soda	keep	Feature-gated SODA module.
*oracledb::tls::*	pub(crate)	Driver-side TLS handoff types should sit behind ConnectOptions and the connector.
pub mod oracledb::tls	pub(crate)	Driver-side TLS handoff types should sit behind ConnectOptions and the connector.
*oracledb::transport::capture_scope*	consolidate	Keep capture capability, but W3 should expose it as a deliberate cassette API rather than raw transport internals.
*oracledb::transport::CaptureScope*	consolidate	Keep record/replay capability, but W3 should expose it as a deliberate cassette API rather than raw transport internals.
*oracledb::transport::Cassette*	consolidate	Keep record/replay capability, but W3 should expose it as a deliberate cassette API rather than raw transport internals.
*oracledb::transport::Replay*	consolidate	Keep replay capability, but W3 should expose it as a deliberate cassette API rather than raw transport internals.
pub mod oracledb::transport	consolidate	Transport module should shrink to cassette utilities or disappear from the driver surface.
pub mod oracledb_protocol	keep	Protocol crate root.
*oracledb_protocol::capabilities::*	keep	Public protocol capability negotiation helpers.
pub mod oracledb_protocol::capabilities	keep	Public protocol capability namespace.
*oracledb_protocol::ClientIdentity*	keep	Public client identity metadata.
*oracledb_protocol::crypto::*	pub(crate)	Password verifier and encryption details are auth implementation internals.
pub mod oracledb_protocol::crypto	pub(crate)	Password verifier and encryption details are auth implementation internals.
pub oracledb_protocol::dpl::DirectPathStream::*	pub(crate)	DirectPathStream remains public, but raw mutable fields should be hidden behind constructors/accessors.
*oracledb_protocol::dpl::BatchLoadState*	keep	Validated batch state is used across the driver/protocol crate boundary and has private fields.
*oracledb_protocol::dpl::DirectPathPieceBuffer*	pub(crate)	Direct-path piece builder is an encoder implementation detail.
*oracledb_protocol::dpl::DirectPathStream*	keep	Stream payload type is used by driver direct-path APIs.
*oracledb_protocol::dpl::*	keep	Direct-path wire types and pure encode/decode helpers are the protocol crate's public surface.
pub mod oracledb_protocol::dpl	keep	Direct-path protocol namespace.
*oracledb_protocol::net::*	keep	Connect descriptor and cassette protocol helpers are public protocol utilities.
pub mod oracledb_protocol::net	keep	Network descriptor namespace.
*oracledb_protocol::oson::*	keep	OSON codec values and helpers are public protocol utilities.
pub mod oracledb_protocol::oson	keep	OSON namespace.
*oracledb_protocol::packet::*	consolidate	Raw TNS packet helpers need an explicit sans-I/O toolkit contract before 1.0.
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
*oracledb_protocol::wire::*	consolidate	Low-level TTC reader/writer primitives need an explicit sans-I/O toolkit contract before 1.0.
pub mod oracledb_protocol::wire	keep	Wire helper namespace.
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

Wave 1 applies the non-`keep` rows. W0-T5.2 records the expert disposition for
breaking removals or renames; implementation beads apply those decisions without
additional approval gates.
