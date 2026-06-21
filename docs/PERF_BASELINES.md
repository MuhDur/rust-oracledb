# Performance Regression Baselines

This file records the advisory, deterministic E9 baseline. It is intentionally
not a machine-independent performance claim. The check script
`scripts/check_perf_regression.sh` only fails when a selected deterministic
microbench is more than `2.0x` slower than the committed reference in
`docs/baseline/perf_regression_reference.json`.

The gate is suitable for soak / release-qualification evidence, not required PR
CI, because Criterion timings still vary by runner. The selected benches avoid
Oracle and live sockets: scalar codecs, LOB text decode, vector image decode,
typed conversion, bind metadata / execute payload construction, large
multi-packet response reassembly, cassette framing, and the single-packet
response passthrough.

## Baseline Capture

- Date: 2026-06-21
- Toolchain: `rustc 1.97.0-nightly (4b0c9d76a 2026-05-10)`, `cargo 1.97.0-nightly (a343accce 2026-05-08)`
- Host: `durakovic`, Linux `6.17.0-35-generic`, AMD EPYC 7713, 128 logical CPUs, schedutil governor
- Commit at capture: `c453ff0`
- Target dir used locally: `/home/durakovic/.cargo-target-rust-oracledb-e9`
- Threshold: fail only when `current median > reference median * 2.0`

## Representative Medians

The JSON reference is the source consumed by the script. The table below should
match it after the baseline refresh.

| Bench | Criterion path | Median |
|---|---|---:|
| Single-packet passthrough near SDU | `single_packet_8000b/passthrough_drain` | 168.0 ns |
| 4 x 8 KiB response reassembly | `deterministic_reassembly/multi_packet_4x8k_extend` | 626.9 ns |
| NUMBER decode with reused scratch | `deterministic_codec/number_decode_reused_scratch` | 582.9 ns |
| UTF-16 64 KiB LOB decode | `deterministic_codec/lob_utf16_64k_decode` | 77.3 us |
| VECTOR 1024 x f32 image decode | `deterministic_codec/vector_image_decode_1024_f32` | 7.5 us |
| Core typed conversion | `deterministic_binds/typed_conversion_core` | 44.5 ns |
| 256 x 3 bind execute payload | `deterministic_binds/execute_payload_256x3_binds` | 33.0 us |
| 256-frame cassette decode | `deterministic_cassette/decode_256x512b_frames` | 10.8 us |

## Musl Size Baseline

`scripts/check_musl_size.sh` builds the `min_connect` example for
`x86_64-unknown-linux-musl`, strips the example binary, and compares its size to
a documented ceiling. The 2026-06-21 measurement was 463,496 bytes after strip;
the default ceiling is 600,000 bytes, about 29% above the measured size.

To refresh the reference intentionally, run:

```sh
export CARGO_TARGET_DIR=/home/durakovic/.cargo-target-rust-oracledb-e9
export TMPDIR=/home/durakovic/.tmp-rust-oracledb
bash scripts/check_perf_regression.sh
```
