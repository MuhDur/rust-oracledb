#![no_main]
//! Fuzz target: the driver's `sql.rs` statement-processing surface — bind-name
//! extraction (`scan_bind_names` and everything built on it: unique/per-
//! occurrence binds, PL/SQL output binds, DML `RETURNING ... INTO` binds) and
//! statement parsing (PL/SQL / DDL / DML classification, placeholder
//! rewriting, DML RETURNING-projection rewriting).
//!
//! Entry point: `oracledb_protocol::fuzz_api::fuzz_sql_statement_surface(&[u8])`.
//! No pre-existing `sql_bind_names` target was found in this repo when this
//! target was added, so this is the first `sql.rs` fuzz coverage beyond the
//! `alter_session` target (which drives only `parse_alter_session_value`,
//! `sql.rs`'s one function *not* covered here).
//!
//! `sql.rs` consumes SQL/PL-SQL statement text supplied by the caller, not
//! server wire bytes — but a real application builds dynamic SQL from
//! request-shaped fragments, so this text is not fully trusted either. Every
//! entry point must fail closed: never panic (in particular, never slice
//! across a UTF-8 char boundary while walking a `'...'` string, a `q'{...}'`
//! q-string, a `--`/`/* */` comment, or a bind-name identifier), never hang,
//! never allocate unboundedly — only ever return `Err` / `None` / an empty
//! `Vec` on malformed input (an unterminated quote, a bind name that never
//! closes, adversarial `INTO`/`RETURNING` keyword placement inside literals).
use libfuzzer_sys::fuzz_target;

fuzz_target!(|data: &[u8]| {
    oracledb_protocol::fuzz_api::fuzz_sql_statement_surface(data);
});
