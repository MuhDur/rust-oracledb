//! Thin-mode SODA (Simple Oracle Document Access).
//!
//! **Experimental surpass feature.** python-oracledb's thin mode has no SODA
//! (it is documented as thick-only). This module implements SODA entirely over
//! the thin TTC wire protocol by generating the SQL and `DBMS_SODA` PL/SQL that
//! the thick OCI client would otherwise generate internally, running it through
//! the existing [`crate::Connection`] execute/fetch surface and the OSON/JSON
//! codecs in `oracledb-protocol`.
//!
//! Scope is the "viable subset": collection lifecycle (create/open/list/drop/
//! truncate), document insert (one/many, with and without returned metadata),
//! find by key(s) / by QBE filter, count, getOne/getDocuments/getCursor,
//! remove, replaceOne, and JSON-search index create/drop. See `docs/SODA.md`
//! for the honest list of supported operations vs. python-oracledb thick and
//! the documented gaps.

mod collection;
mod cursor;
mod database;
mod document;
mod error;
mod metadata;
mod operation;
pub(crate) mod qbe;

pub use collection::SodaCollection;
pub use cursor::SodaCursor;
pub use database::SodaDatabase;
pub use document::SodaDocument;
pub use error::{Result as SodaResult, SodaError};
pub use metadata::{
    parse_metadata, ContentSqlType, KeyAssignment, SodaCollectionMetadata, VersionMethod,
};
pub use operation::{SelectColumns, SodaOperation};
