#![no_main]
//! Fuzz target 10: the connect-string parser (descriptor / EZConnect-Plus /
//! tnsnames.ora lexer).
//!
//! Entry point: `oracledb_protocol::fuzz_api::fuzz_connect_string(&str)`, which
//! drives both `net::connectstring::parse` (the TNS connect-descriptor and
//! EZConnect-Plus parser) and the in-memory tnsnames.ora tokenizer. These
//! consume untrusted env / config / user input and only had unit tests; the
//! parser must NEVER panic / OOM / stack-overflow — only return `Err` (or
//! `Ok(None)` for "this is a tnsnames alias").
//!
//! STRUCTURED fuzzing (skill: testing-fuzzing, archetype 5 grammar-based):
//! random bytes almost never reach the deep descriptor / IFILE-recursion /
//! quote-handling states, because the very first byte has to be `(` and every
//! interesting branch is gated behind balanced parens and `KEY=` tokens. So the
//! `Arbitrary` impl below builds three input shapes from the raw bytes:
//!
//!   1. a hand-written nested-paren descriptor generator (reaches arbitrary
//!      nesting depth — guards the `MAX_DESCRIPTOR_DEPTH` fix from bead uf8 —
//!      plus quoted values, container keywords, and the EZConnect host/port/
//!      service grammar);
//!   2. a valid descriptor/EZConnect *prefix* followed by a garbage tail (hits
//!      the "good so far, then malformed" transition states); and
//!   3. the raw bytes verbatim (so libFuzzer's byte-level mutation + the saved
//!      crash corpus still feed the parser directly).
//!
//! The fuzzer picks among them with a selector byte, so one target covers the
//! whole untrusted-input surface.
use libfuzzer_sys::arbitrary::{self, Arbitrary, Unstructured};
use libfuzzer_sys::fuzz_target;
use oracledb_protocol::fuzz_api::fuzz_connect_string;

/// A keyword token: either one of the real connect-descriptor / EZConnect
/// keywords (so the generated tree actually drives the keyword-dispatch arms)
/// or a short arbitrary identifier (so pass-through / unknown-key paths and the
/// `canonical_param_name` table are exercised too).
const KEYWORDS: &[&str] = &[
    "DESCRIPTION",
    "DESCRIPTION_LIST",
    "ADDRESS",
    "ADDRESS_LIST",
    "CONNECT_DATA",
    "SECURITY",
    "PROTOCOL",
    "HOST",
    "PORT",
    "SERVICE_NAME",
    "SID",
    "INSTANCE_NAME",
    "SERVER",
    "LOAD_BALANCE",
    "FAILOVER",
    "SOURCE_ROUTE",
    "RETRY_COUNT",
    "RETRY_DELAY",
    "EXPIRE_TIME",
    "SDU",
    "TRANSPORT_CONNECT_TIMEOUT",
    "POOL_CONNECTION_CLASS",
    "POOL_PURITY",
    "SSL_SERVER_CERT_DN",
    "MY_WALLET_DIRECTORY",
    "USE_SNI",
    "IFILE",
];

/// Atom values that appear after `KEY=` in a real descriptor: protocols, ports,
/// booleans, durations, a quote-bearing string, etc. Picking from these (rather
/// than only random bytes) makes the generated value side actually reach the
/// numeric / boolean / duration / quoted-string parse arms.
const ATOMS: &[&str] = &[
    "tcp", "tcps", "1521", "0", "65536", "on", "off", "yes", "no", "true",
    "dedicated", "pooled", "self", "new", "20sec", "100ms", "5min", "\"q\"",
    "'q'", "a b", "::1", "[::1]", "host.example.com",
];

/// The three input-shaping strategies; the selector byte chooses one.
#[derive(Debug)]
enum ConnectInput {
    /// A generated, mostly-balanced nested descriptor tree.
    Descriptor(String),
    /// A valid-ish prefix glued to an arbitrary garbage tail.
    PrefixPlusGarbage(String),
    /// The raw bytes, interpreted as UTF-8 lossily.
    Raw(String),
}

impl ConnectInput {
    fn as_str(&self) -> &str {
        match self {
            Self::Descriptor(s) | Self::PrefixPlusGarbage(s) | Self::Raw(s) => s,
        }
    }
}

/// Recursively emit a `(KEY=VALUE)` node. `depth` is bounded by the budget so
/// the *generator* itself can't blow the stack while building the string (the
/// parser-under-test has its own MAX_DESCRIPTOR_DEPTH guard, which we
/// deliberately drive past via the explicit deep-nest case below).
fn gen_node(u: &mut Unstructured, out: &mut String, budget: &mut u32) -> arbitrary::Result<()> {
    if *budget == 0 {
        out.push_str("X=x");
        return Ok(());
    }
    *budget -= 1;

    let kw = u.choose(KEYWORDS)?;
    out.push_str(kw);
    // Optional whitespace around the '=' to exercise skip_spaces.
    if u.arbitrary()? {
        out.push(' ');
    }
    out.push('=');
    if u.arbitrary()? {
        out.push(' ');
    }

    // The value is either one-or-more nested parenthesised children, or a
    // simple atom value.
    if u.arbitrary()? {
        let children = u.int_in_range(1u8..=4)?;
        for _ in 0..children {
            out.push('(');
            gen_node(u, out, budget)?;
            out.push(')');
            if *budget == 0 {
                break;
            }
        }
    } else {
        let atom = u.choose(ATOMS)?;
        out.push_str(atom);
    }
    Ok(())
}

impl<'a> Arbitrary<'a> for ConnectInput {
    fn arbitrary(u: &mut Unstructured<'a>) -> arbitrary::Result<Self> {
        // Selector chooses the shape. 0/1 -> structured descriptor, 2 ->
        // prefix+garbage, 3 -> a deliberately over-deep nest (drives the
        // MAX_DESCRIPTOR_DEPTH guard), else -> raw.
        let selector = u.arbitrary::<u8>()? & 0x07;
        match selector {
            0 | 1 => {
                let mut s = String::from("(");
                let mut budget = u.int_in_range(1u32..=64)?;
                gen_node(u, &mut s, &mut budget)?;
                s.push(')');
                Ok(ConnectInput::Descriptor(s))
            }
            2 => {
                // A valid prefix, then an arbitrary tail. The prefix is a real
                // descriptor opening so the parser is in a "deep, valid" state
                // when the garbage arrives.
                let prefix = "(DESCRIPTION=(ADDRESS=(PROTOCOL=tcp)(HOST=h)(PORT=1521))(CONNECT_DATA=(SERVICE_NAME=";
                let tail: &[u8] = u.peek_bytes(u.len().min(256)).unwrap_or(&[]);
                let tail_str = String::from_utf8_lossy(tail).into_owned();
                Ok(ConnectInput::PrefixPlusGarbage(format!("{prefix}{tail_str}")))
            }
            3 => {
                // Deliberately exceed MAX_DESCRIPTOR_DEPTH (128) to keep that
                // fail-closed path hot. The opening parens nest deeper than the
                // guard; the parser must return Err, never overflow the stack.
                let depth = u.int_in_range(100u32..=400)?;
                let mut s = String::new();
                for _ in 0..depth {
                    s.push_str("(A=");
                }
                s.push('x');
                for _ in 0..depth {
                    s.push(')');
                }
                Ok(ConnectInput::Descriptor(s))
            }
            _ => {
                let bytes = u.peek_bytes(u.len()).unwrap_or(&[]);
                Ok(ConnectInput::Raw(String::from_utf8_lossy(bytes).into_owned()))
            }
        }
    }
}

fuzz_target!(|input: ConnectInput| {
    let s = input.as_str();
    // Guard against pathologically large generated strings (the deep-nest case
    // is already bounded; this caps the prefix+garbage / raw shapes so a huge
    // input can't dominate exec time — the parser is O(n) but we want speed).
    if s.len() > 1_000_000 {
        return;
    }
    fuzz_connect_string(s);
});
