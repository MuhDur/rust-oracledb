# Supported Profiles

This file is the support contract for the published Rust crates. It defines the
feature profiles that CI must keep compiling and testing. It does not claim that
every arbitrary feature combination is separately supported.

See [docs/TOOLCHAIN.md](TOOLCHAIN.md) for the Rust toolchain pin and re-pin
procedure.

## Driver Profiles

The `oracledb` crate is nightly-only because the async runtime dependency uses
nightly Rust. The sans-I/O `oracledb-protocol` crate has a separate stable lane
tracked by W0-T3.4.

| profile | command | contract |
|---|---|---|
| minimal | `cargo check -p oracledb --locked --no-default-features` | Driver core without `derive` or optional integrations. |
| default | `cargo check -p oracledb --locked` | Standard user build; includes `derive`. |
| all-features | `cargo check -p oracledb --locked --all-features` | Maximal compile smoke for the driver crate. This does not imply every arbitrary subset is individually supported. |

## Optional Integration Matrix

The supported optional integration slices are:

| feature | purpose |
|---|---|
| `chrono` | `FromSql` / `ToSql` bridge for `chrono` date/time types. |
| `uuid` | `FromSql` / `ToSql` bridge for `uuid::Uuid`. |
| `serde_json` | `FromSql` / `ToSql` bridge for `serde_json::Value`. |
| `rust_decimal` | Lossless `rust_decimal::Decimal` bridge for NUMBER. |
| `arrow` | Arrow `RecordBatch` fetch and ingest helpers. |
| `soda` | Experimental thin-mode SODA facade over the thin protocol. |

CI exercises those slices with `cargo-hack 0.6.45`:

```sh
cargo hack check -p oracledb --locked \
  --feature-powerset --depth 1 \
  --include-features chrono,uuid,serde_json,rust_decimal,arrow,soda

cargo hack test -p oracledb --locked --lib \
  --feature-powerset --depth 1 \
  --include-features chrono,uuid,serde_json,rust_decimal,arrow,soda
```

With these flags, cargo-hack runs `--no-default-features --features <feature>`
for each named integration. The default and all-features profiles above cover
the ordinary `derive` build and maximal compile smoke.

## Documented But Not Matrix Profiles

These features are intentionally outside the W0-T3.1 optional integration
matrix:

| feature | status |
|---|---|
| `derive` | Default feature; covered by the default profile and derive-specific tests. |
| `tracing` | Observability feature; covered by all-features compile smoke and observability tests. |
| `cassette` | Transport record/replay seam; covered by record/replay tests and all-features compile smoke. |
| `experimental` | Enables the cwallet.sso reader; covered by all-features compile smoke, not a stable 1.0 user contract yet. |

Unsupported feature combinations should be documented explicitly before they are
relied on. Do not infer support from `--all-features` alone.
