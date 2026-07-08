# Optimization Discipline

This document is the standing D6.6 contract for performance work in the driver
repo. It applies before changing hot decode, fetch, bind, cassette, or response
reassembly paths.

## Baselines

Criterion is the CI-enforced baseline source. The committed reference lives in
`docs/baseline/perf_regression_reference.json` and is checked by
`scripts/check_perf_regression.sh`.

The current deterministic coverage includes:

- `single_packet_8000b/passthrough_drain`
- `deterministic_reassembly/multi_packet_4x8k_extend`
- `deterministic_codec/number_decode_reused_scratch`
- `deterministic_codec/lob_utf16_64k_decode`
- `deterministic_codec/vector_image_decode_1024_f32`
- `deterministic_binds/typed_conversion_core`
- `deterministic_binds/execute_payload_256x3_binds`
- `deterministic_cassette/decode_256x512b_frames`

Hyperfine is advisory wall-clock evidence for local optimization sessions. Use
the command in `docs/PERF_BASELINES.md` when an operator needs process-level
timing evidence alongside Criterion medians.

## CI Gate

The performance regression gate runs in the `soak` and `release-qualification`
profiles via `.github/workflows/_quality.yml`.

The gate fails when a selected deterministic Criterion median is more than the
committed `max_regression_ratio` slower than its reference median. At the time
of this document, that ratio is `2.0x`.

The gate is deliberately not part of required PR CI because GitHub runner timing
variance can be noisy. Required CI still compiles, tests, lints, validates
baselines, checks the API ledger, and runs the release metadata gates.

## Isomorphism Proof Template

Every optimization commit must include this proof in its PR, issue note, or
commit body:

```text
## Change: <short description>
- Hot path: <function/module and benchmark path>
- Baseline command: <criterion or hyperfine command>
- Ordering preserved: <yes/no + why>
- Tie-breaking unchanged: <yes/no/N/A + why>
- Floating-point: <identical/N/A + why>
- RNG/time inputs: <unchanged/N/A + why>
- Error surface: <unchanged + tests proving it>
- Golden outputs: <test/cassette/artifact command and result>
- Perf result: <before median, after median, ratio>
- Rollback: git revert <commit>
```

No optimization is complete without both behavior evidence and a fresh
performance measurement.
