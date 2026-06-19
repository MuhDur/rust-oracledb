#[test]
fn from_row_derive_compile_surface() {
    let t = trybuild::TestCases::new();
    t.pass("tests/trybuild/pass/from_row_surface.rs");
    t.compile_fail("tests/trybuild/fail/*.rs");
}
