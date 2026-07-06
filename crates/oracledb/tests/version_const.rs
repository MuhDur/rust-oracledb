//! A6 — the public `oracledb::VERSION` const.
//!
//! Wrapping crates (notably `oraclemcp-db`'s `doctor`) must surface the
//! driver's real version. `env!("CARGO_PKG_VERSION")` evaluated inside a wrapper
//! yields the wrapper's version, so the driver exposes its own `VERSION`. These
//! tests prove the const exists, is non-empty, is derived from the crate version
//! (not a hand-written literal that could drift), and parses as `major.minor.*`.

#[test]
fn version_const_equals_crate_version() {
    // Evaluated here in an integration test of the `oracledb` crate,
    // `CARGO_PKG_VERSION` is the driver crate's version — so this proves
    // `VERSION` is wired to the crate metadata rather than hardcoded.
    assert_eq!(
        oracledb::VERSION,
        env!("CARGO_PKG_VERSION"),
        "oracledb::VERSION must track the crate's Cargo version"
    );
}

#[test]
fn version_const_is_well_formed_semver_prefix() {
    let v = oracledb::VERSION;
    assert!(!v.is_empty(), "VERSION must not be empty");

    let mut parts = v.split('.');
    let major = parts.next().expect("major component");
    let minor = parts.next().expect("minor component");
    let patch = parts.next().expect("patch component");
    assert!(
        major.chars().all(|c| c.is_ascii_digit()) && !major.is_empty(),
        "major must be numeric, got {major:?} in {v:?}"
    );
    assert!(
        minor.chars().all(|c| c.is_ascii_digit()) && !minor.is_empty(),
        "minor must be numeric, got {minor:?} in {v:?}"
    );
    // The patch component may carry a pre-release/build suffix; require it to
    // start with a digit so `major.minor.patch` is always parseable.
    assert!(
        patch.chars().next().is_some_and(|c| c.is_ascii_digit()),
        "patch must start with a digit, got {patch:?} in {v:?}"
    );
}
