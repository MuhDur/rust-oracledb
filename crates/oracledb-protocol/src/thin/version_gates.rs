#![forbid(unsafe_code)]

//! Centralized TTC field-version gates (the "version surface").
//!
//! Every place the wire format of a message depends on the negotiated TTC field
//! version (`Capabilities.ttc_field_version` in the reference thin driver) is a
//! *gate*: a conditional field that a server below the boundary does not read or
//! send, so emitting it unconditionally shifts every following byte and corrupts
//! the call. Historically these gates were scattered as raw
//! `if ttc_field_version >= TNS_CCAP_FIELD_VERSION_X { .. }` literals next to the
//! bytes they guard, which made a gate easy to forget on the write side (the DPL
//! / TPC / EXECUTE oaccolid bugs were exactly this) and impossible to enumerate.
//!
//! Each predicate here is the **single** definition of one version decision. A
//! call site reaches for the named predicate instead of re-spelling the literal,
//! so the whole surface is greppable (`grep version_gates::`) and every decision
//! is documented against the reference `_caps.ttc_field_version >= ...` check it
//! mirrors, byte-for-byte (same constant, same `>=` direction). Adding a new
//! version-dependent field means adding a named predicate here, which the
//! reference-gate coverage audit (`scripts/extract_reference_gates.sh`) and the
//! offline boundary tests pin to their exact flip point.
//!
//! These are `const fn` and take the raw `ttc_field_version: u8` (the value
//! already threaded through the sans-io codecs) rather than a `&Capabilities`,
//! so they impose no new public surface and no signature churn on the wire
//! builders.

use super::constants::{
    TNS_CCAP_FIELD_VERSION_12_1, TNS_CCAP_FIELD_VERSION_12_2, TNS_CCAP_FIELD_VERSION_12_2_EXT1,
    TNS_CCAP_FIELD_VERSION_20_1, TNS_CCAP_FIELD_VERSION_21_1, TNS_CCAP_FIELD_VERSION_23_1,
    TNS_CCAP_FIELD_VERSION_23_1_EXT_1, TNS_CCAP_FIELD_VERSION_23_1_EXT_3,
    TNS_CCAP_FIELD_VERSION_23_4,
};

/// The `ub8` pipeline-token field on every function / piggyback message header.
///
/// Reference `messages/base.pyx` `_write_function_code` (lines 700 / 714) writes
/// `ub8 token_num` only when `_caps.ttc_field_version >= TNS_CCAP_FIELD_VERSION_23_1_EXT_1`;
/// a pre-23ai server parses a stray token byte as message content and fails the
/// call (observed live: ORA-03120 on Oracle XE 21c). Pipelining (nonzero tokens)
/// only occurs on a 23ai-negotiated connection, so no token is ever dropped.
pub(crate) const fn writes_pipeline_token(ttc_field_version: u8) -> bool {
    ttc_field_version >= TNS_CCAP_FIELD_VERSION_23_1_EXT_1
}

/// The `ub4` `oaccolid` field in a column-metadata record (both the describe
/// read side and the bind-metadata write side carry the same field).
///
/// Reference `messages/base.pyx:346` (read/skip in `_process_column_info`) and
/// `messages/base.pyx:1429` (write in `_write_column_metadata`) gate it on
/// `_caps.ttc_field_version >= TNS_CCAP_FIELD_VERSION_12_2`.
pub(crate) const fn carries_oaccolid(ttc_field_version: u8) -> bool {
    ttc_field_version >= TNS_CCAP_FIELD_VERSION_12_2
}

/// The `al8sqlsig` block (SQL-signature + SQL-ID pointers) in an EXECUTE.
///
/// Reference `messages/execute.pyx:172` gates the block on
/// `_caps.ttc_field_version >= TNS_CCAP_FIELD_VERSION_12_2`. Same constant as
/// [`carries_oaccolid`] but a distinct wire field, so it is a distinct decision.
pub(crate) const fn writes_al8sqlsig(ttc_field_version: u8) -> bool {
    ttc_field_version >= TNS_CCAP_FIELD_VERSION_12_2
}

/// The chunk-ids block (chunk-ids pointer + count) in an EXECUTE, written only
/// inside the `al8sqlsig` block.
///
/// Reference `messages/execute.pyx:178` gates it on
/// `_caps.ttc_field_version >= TNS_CCAP_FIELD_VERSION_12_2_EXT1`.
pub(crate) const fn writes_execute_chunk_ids(ttc_field_version: u8) -> bool {
    ttc_field_version >= TNS_CCAP_FIELD_VERSION_12_2_EXT1
}

/// The `ub4 sql-type` + `ub4 server-checksum` pair in a server error/return
/// info block.
///
/// Reference `messages/base.pyx:238` skips both when
/// `_caps.ttc_field_version >= TNS_CCAP_FIELD_VERSION_20_1`. (Our reader keeps an
/// extra defensive peek for the pre-20.1 layout; only the version half of that
/// condition lives here.)
pub(crate) const fn reads_error_sql_type_and_checksum(ttc_field_version: u8) -> bool {
    ttc_field_version >= TNS_CCAP_FIELD_VERSION_20_1
}

/// The JSON-payload flag/pointer byte in an AQ enqueue / dequeue payload.
///
/// Reference `messages/aq_enq.pyx:115` (enqueue pointer) and
/// `messages/aq_deq.pyx:130` (dequeue flag) gate it on
/// `_caps.ttc_field_version >= TNS_CCAP_FIELD_VERSION_20_1`.
pub(crate) const fn writes_aq_json_payload(ttc_field_version: u8) -> bool {
    ttc_field_version >= TNS_CCAP_FIELD_VERSION_20_1
}

/// The `ub4` shard-id field in AQ message properties / array enqueue+dequeue /
/// single dequeue (write side) and in the dequeue message-properties (read
/// side) — the same field on both directions.
///
/// Reference `messages/aq_base.pyx:129,197`, `messages/aq_array.pyx:196` and
/// `messages/aq_deq.pyx:132` gate it on
/// `_caps.ttc_field_version >= TNS_CCAP_FIELD_VERSION_21_1`.
pub(crate) const fn carries_aq_shard_id(ttc_field_version: u8) -> bool {
    ttc_field_version >= TNS_CCAP_FIELD_VERSION_21_1
}

/// The domain-schema + domain-name strings in a column-metadata describe.
///
/// Reference `messages/base.pyx:358` gates them on
/// `_caps.ttc_field_version >= TNS_CCAP_FIELD_VERSION_23_1`.
pub(crate) const fn reads_column_domain(ttc_field_version: u8) -> bool {
    ttc_field_version >= TNS_CCAP_FIELD_VERSION_23_1
}

/// The column annotations block in a column-metadata describe.
///
/// Reference `messages/base.pyx:361` gates it on
/// `_caps.ttc_field_version >= TNS_CCAP_FIELD_VERSION_23_1_EXT_3`.
pub(crate) const fn reads_column_annotations(ttc_field_version: u8) -> bool {
    ttc_field_version >= TNS_CCAP_FIELD_VERSION_23_1_EXT_3
}

/// The VECTOR column metadata (dimensions / format / flags) in a describe.
///
/// Reference `messages/base.pyx:376` gates it on
/// `_caps.ttc_field_version >= TNS_CCAP_FIELD_VERSION_23_4`.
pub(crate) const fn reads_column_vector_metadata(ttc_field_version: u8) -> bool {
    ttc_field_version >= TNS_CCAP_FIELD_VERSION_23_4
}

/// The `kpninst`/client-id pointer block written into a SUBSCRIBE (register)
/// request.
///
/// Reference `messages/subscribe.pyx:127` gates it on
/// `_caps.ttc_field_version >= TNS_CCAP_FIELD_VERSION_12_1`.
pub(crate) const fn writes_subscribe_client_id_block(ttc_field_version: u8) -> bool {
    ttc_field_version >= TNS_CCAP_FIELD_VERSION_12_1
}

/// The subscriber name and the db-instances / listener-addresses blocks read
/// from a SUBSCRIBE response.
///
/// Reference `messages/subscribe.pyx:61` (subscriber name) and
/// `messages/subscribe.pyx:63` (db instances + listeners) gate them on
/// `_caps.ttc_field_version >= TNS_CCAP_FIELD_VERSION_12_1`.
pub(crate) const fn reads_subscribe_response_details(ttc_field_version: u8) -> bool {
    ttc_field_version >= TNS_CCAP_FIELD_VERSION_12_1
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::thin::constants::TNS_CCAP_FIELD_VERSION_19_1_EXT_1;

    /// Offline 19c capability-profile differential against the reference
    /// `TNS_CCAP_FIELD_VERSION_19_1_EXT_1` branches. This pins branch
    /// selection only; a live 19c lane is still needed for session semantics.
    #[test]
    fn nineteen_c_caps_profile_matches_reference_gate_selection() {
        let field_version = TNS_CCAP_FIELD_VERSION_19_1_EXT_1;

        // Present on both sides of the 19c profile.
        assert!(carries_oaccolid(field_version));
        assert!(writes_al8sqlsig(field_version));
        assert!(writes_execute_chunk_ids(field_version));
        assert!(writes_subscribe_client_id_block(field_version));
        assert!(reads_subscribe_response_details(field_version));

        // 20c, 21c, and 23ai additions must remain absent.
        assert!(!reads_error_sql_type_and_checksum(field_version));
        assert!(!writes_aq_json_payload(field_version));
        assert!(!carries_aq_shard_id(field_version));
        assert!(!reads_column_domain(field_version));
        assert!(!reads_column_annotations(field_version));
        assert!(!reads_column_vector_metadata(field_version));
        assert!(!writes_pipeline_token(field_version));
    }
}
