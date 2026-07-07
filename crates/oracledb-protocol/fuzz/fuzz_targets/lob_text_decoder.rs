#![no_main]
//! Fuzz target: streaming CLOB/NCLOB text decoder (`LobTextDecoder`, bead a4-bbx).
//!
//! Entry point: `oracledb_protocol::thin::LobTextDecoder::push`/`finish`. This is
//! the new untrusted-decode surface introduced by lazy LOB streaming: LOB bytes
//! arrive off the wire and are fed to the decoder in arbitrary chunk boundaries,
//! which may split a multi-byte UTF-8 sequence or a UTF-16 surrogate pair.
//!
//! The target enforces two invariants, both of which must hold for every input,
//! for both endiannesses and both character-set forms:
//!   * METAMORPHIC: decoding the whole buffer in one push must yield the same
//!     outcome (same `Ok` string, or both `Err`) as decoding it split into
//!     `chunk`-byte pieces — chunking must never change the result.
//!   * DIFFERENTIAL: for the forms reachable through the public
//!     `decode_lob_text` whole-buffer decoder (UTF-8, and big-endian UTF-16 via
//!     `csfrm`), the streaming decoder must agree with it.
//!
//! Any divergence or panic is a finding. The decoder is in a `forbid(unsafe)`
//! crate, so this fuzzes the safe decode logic for fail-closed behaviour.
use libfuzzer_sys::fuzz_target;
use oracledb_protocol::thin::{decode_lob_text, LobTextDecoder, CS_FORM_IMPLICIT, CS_FORM_NCHAR};

/// Decode the whole buffer through a fresh streaming decoder in one push.
fn decode_whole(use_utf16: bool, little_endian: bool, data: &[u8]) -> Result<String, ()> {
    let mut decoder = LobTextDecoder::new(use_utf16, little_endian);
    match decoder.push(data) {
        Ok(text) => decoder.finish().map(|()| text).map_err(|_| ()),
        Err(_) => Err(()),
    }
}

/// Decode the buffer split into `chunk`-byte pieces, concatenating each push.
fn decode_chunked(
    use_utf16: bool,
    little_endian: bool,
    data: &[u8],
    chunk: usize,
) -> Result<String, ()> {
    let mut decoder = LobTextDecoder::new(use_utf16, little_endian);
    let mut out = String::new();
    for piece in data.chunks(chunk.max(1)) {
        match decoder.push(piece) {
            Ok(text) => out.push_str(&text),
            Err(_) => return Err(()),
        }
    }
    decoder.finish().map(|()| out).map_err(|_| ())
}

fuzz_target!(|data: &[u8]| {
    if data.is_empty() || data.len() > 1_000_000 {
        return;
    }
    let selector = data[0];
    let payload = &data[1..];
    let use_utf16 = selector & 0x01 != 0;
    let little_endian = selector & 0x02 != 0;
    let chunk = (usize::from(selector >> 2) & 0x3f) + 1; // 1..=64

    // METAMORPHIC: whole vs chunked must agree.
    let whole = decode_whole(use_utf16, little_endian, payload);
    let chunked = decode_chunked(use_utf16, little_endian, payload, chunk);
    match (&whole, &chunked) {
        (Ok(a), Ok(b)) => assert_eq!(a, b, "streaming decode diverged from whole decode"),
        (Err(_), Err(_)) => {}
        _ => panic!("streaming vs whole decode disagreed: {whole:?} vs {chunked:?}"),
    }

    // DIFFERENTIAL: big-endian / UTF-8 forms are reachable via `decode_lob_text`
    // with a null locator (little-endian needs a locator flag that is crate-
    // private, so only the big-endian side is cross-checked here).
    if !little_endian {
        let csfrm = if use_utf16 {
            CS_FORM_NCHAR
        } else {
            CS_FORM_IMPLICIT
        };
        let oracle = decode_lob_text(payload, csfrm, None).map_err(|_| ());
        match (&oracle, &whole) {
            (Ok(a), Ok(b)) => assert_eq!(a, b, "decoder diverged from decode_lob_text"),
            (Err(_), Err(_)) => {}
            _ => panic!("decoder vs decode_lob_text disagreed: {oracle:?} vs {whole:?}"),
        }
    }
});
