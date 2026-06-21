//! Allocation-count regression for the inline NUMBER decode (bead
//! rust-oracledb-65w).
//!
//! The owned fetch path used to `malloc` a `String` per NUMBER cell to carry the
//! canonical decimal text. The inline `OracleNumber` (`{ i128 coefficient, i16
//! scale }`) carries the value with no per-cell heap allocation for the common
//! in-range case. This test pins the win: a NUMBER-heavy row's owned decode must
//! allocate at most one heap object per cell (the wire-bytes `Vec` the reader
//! returns) plus the per-row `Vec` — and crucially NOT a second `String` per
//! NUMBER.
//!
//! Allocations are counted with `allocation-counter` (its unsafe lives inside
//! that crate, so the workspace stays `#![forbid(unsafe_code)]`-clean).

use oracledb_protocol::thin::{
    encode_number_text, parse_fetch_response_with_context, ClientCapabilities, ColumnMetadata,
};
use oracledb_protocol::wire::TtcWriter;

const ORA_NUMBER: u8 = 2;
const TNS_MSG_TYPE_ROW_DATA: u8 = 7;
const TNS_MSG_TYPE_END_OF_RESPONSE: u8 = 29;

fn col(name: &str, ora_type_num: u8, buffer_size: u32) -> ColumnMetadata {
    ColumnMetadata::new(name, ora_type_num)
        .with_csfrm(1)
        .with_buffer_size(buffer_size)
}

#[test]
fn owned_number_decode_allocs_at_most_two_per_cell() {
    const ROWS: usize = 2000;
    const COLS: usize = 10;
    let columns: Vec<_> = (0..COLS)
        .map(|i| col(&format!("N{i}"), ORA_NUMBER, 22))
        .collect();

    let mut writer = TtcWriter::new();
    for r in 0..ROWS {
        writer.write_u8(TNS_MSG_TYPE_ROW_DATA);
        for c in 0..COLS {
            // A mix of integers and fractions to exercise the inline form fully.
            let text = if c % 3 == 0 {
                format!("{}", r * 10 + c)
            } else {
                format!("{}.{}", r, c)
            };
            let v = encode_number_text(&text).unwrap();
            writer.write_bytes_with_length(&v).unwrap();
        }
    }
    writer.write_u8(TNS_MSG_TYPE_END_OF_RESPONSE);
    let payload = writer.into_bytes();
    let caps = ClientCapabilities::default();

    // Warm + correctness.
    let warm = parse_fetch_response_with_context(&payload, caps, &columns, None).unwrap();
    assert_eq!(warm.rows.len(), ROWS);

    let measured = allocation_counter::measure(|| {
        let res = parse_fetch_response_with_context(&payload, caps, &columns, None).unwrap();
        std::hint::black_box(res.rows.len());
    });

    let per_row = measured.count_total as f64 / ROWS as f64;
    println!(
        "owned NUMBER decode: {} allocs total, {per_row:.2}/row ({COLS} NUMBER cols)",
        measured.count_total
    );

    // Before the inline form, the owned decode allocated ~2 heap objects per
    // NUMBER cell *just for the value* (canonical-text String + a scratch
    // String/Vec) on top of the wire-bytes Vec and the per-row Vec — ~3/cell.
    // The inline form drops the value-side allocations, so the per-row total
    // must now be at most ~1.5 per cell (the wire-bytes Vec + per-row overhead).
    let max_allowed = (COLS as f64) * 1.5 + 2.0;
    assert!(
        per_row <= max_allowed,
        "owned NUMBER decode allocates {per_row:.2}/row, expected <= {max_allowed:.2}/row \
         (inline NUMBER must not malloc a String per cell)"
    );
}
