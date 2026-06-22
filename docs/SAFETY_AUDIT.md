# UB + Unsafe Audit — rust-oracledb

Quality lane `q-ubaudit`. Audit performed against worktree HEAD
`77576d2` (Merge fp-tpc: XA/TPC thin mode), which includes the recent
AQ / CQN / TPC thin-mode feature merges.

Toolchain: rustc/cargo 1.97.0-nightly (2026-05-11), miri 0.1.0, cargo-fuzz
0.13.1, cargo-audit 0.22.1, cargo-deny 0.19.7. arrow-array/arrow-schema/
arrow-data 59.0.0, pyo3 0.28.3.

Scope: this is a `forbid-soundness` audit. The two published crates
(`oracledb-protocol`, `oracledb`) declare `#![forbid(unsafe_code)]`; the only
`unsafe` in the workspace lives in one audited module,
`crates/oracledb-pyshim/src/arrow_capsule.rs`, which carries
`#![allow(unsafe_code)]` for the Arrow C Data Interface PyCapsule export.

---

## 1. forbid(unsafe_code) confirmation

### Driver + protocol crates: forbid is structurally airtight

`crates/oracledb-protocol` and `crates/oracledb` apply `#![forbid(unsafe_code)]`
at the crate root and at every module head. The workspace also enforces it via
`[workspace.lints] rust.unsafe_code = "forbid"` (`Cargo.toml:43`), which every
member crate inherits unless it overrides the lint table.

- `grep -rn "unsafe"` over `crates/oracledb-protocol/src` and
  `crates/oracledb/src` returns ZERO usable `unsafe` — only the `forbid`
  attribute lines themselves.
- `#![forbid(unsafe_code)]` is present on the crate root of both crates and is
  repeated per module across the `thin/` tree (auth, fetch, bind, execute,
  connect, codecs, aq, subscr, lob, dbobject, sessionless, types, constants,
  errors), plus `wire.rs`, `crypto.rs`, `capabilities.rs`, `net/mod.rs`,
  `packet/mod.rs`. `forbid` (unlike `deny`) cannot be locally overridden by an
  inner `#[allow]`, so a stray `unsafe` anywhere in these crates is a hard
  compile error.

Verdict: **forbid holds.** The driver and protocol layers contain no `unsafe`.
The recent AQ/CQN/TPC decoders (`thin/aq.rs`, `thin/subscr.rs`,
`thin/sessionless.rs`) are all `#![forbid(unsafe_code)]` and parse server
payloads through the safe, bounds-checked `TtcReader`.

### pyshim crate: deny + one audited allow

`crates/oracledb-pyshim/Cargo.toml` sets `[lints.rust] unsafe_code = "deny"`
(relaxed from the workspace `forbid` so a single module can opt in).
`src/lib.rs` re-asserts `#![deny(unsafe_code)]`. Only `src/arrow_capsule.rs`
carries `#![allow(unsafe_code)]`.

- `grep -rln "unsafe" crates/oracledb-pyshim/src` matches `lib.rs`,
  `vector.rs`, `arrow_capsule.rs` — but in `lib.rs` and `vector.rs` every hit is
  inside a comment/doc-string; neither contains an `unsafe` block, fn, or impl.
- All real `unsafe` constructs live in `arrow_capsule.rs` (15 occurrences,
  enumerated in §3).

cargo-geiger was not installed; the grep + `forbid`/`deny` lint enforcement +
clean `cargo clippy --workspace -D warnings` build (which would reject any
`unsafe` in a `forbid`/`deny` crate) together provide equivalent assurance that
the unsafe surface is exactly the one audited module.

---

## 2. Unsafe-site audit — arrow_capsule.rs

The module implements the Arrow PyCapsule protocol: a heap `Box<FFI_Arrow*>` is
moved into a `PyCapsule`; the capsule destructor reclaims the `Box` exactly
once; the arrow-rs FFI struct's own `Drop` runs the C release callback iff the
consumer has not already moved the struct out. The whole surface is FFI between
Rust, arrow-rs's C Data Interface structs, and CPython via pyo3.

FFI contracts were verified directly against local crate source (arrow 59.0.0,
pyo3 0.28.3). Key confirmed facts that the verdicts rest on:

- `FFI_ArrowArray` / `FFI_ArrowSchema` / `FFI_ArrowArrayStream` `Drop` calls
  `release` only when it is `Some`, and the release callback nulls `release`
  afterward — so drop-after-move and double-drop are no-ops.
- `arrow_array::ffi::to_ffi` and `FFI_ArrowArrayStream::new` are SAFE functions
  returning owned FFI structs.
- `TryFrom<&FFI_ArrowSchema> for Schema/Field` deep-clones by shared reference;
  it never takes ownership and never runs the release callback.
- `ArrowArrayStreamReader::from_raw` / `FFI_ArrowArrayStream::from_raw` MOVE the
  stream out via `std::ptr::replace(raw, empty())`, requiring the pointer be
  valid for reads AND writes, aligned, and initialized; the source slot is left
  with `release = None` so the capsule destructor becomes a no-op.
- pyo3 `PyCapsule::new_with_pointer_and_destructor` requires a `NonNull`
  pointer, a `'static CStr` name, and a destructor that is thread-safe and must
  not panic (a panic aborts the process). CPython invokes the destructor exactly
  once on capsule collection.
- `PyCapsule_GetPointer` returns the stored pointer on name match, or null +
  a set Python error on mismatch. `PyCapsule_GetPointer`, `PyErr_Clear`, and
  `Box::from_raw` are all non-panicking on the Rust side, so the
  `extern "C"` destructor cannot unwind across the FFI boundary.

### Per-site verdicts

| Site (line) | Construct | Invariant upheld | Verdict |
|---|---|---|---|
| L60 | `unsafe extern "C" fn capsule_destructor<T>` | Caller (PyO3/CPython) passes the capsule this fn was registered on; `T` matches the boxed type for `name`. | (A) STRICTLY_UNAVOIDABLE — FFI callback ABI. SOUND. |
| L63 | `PyCapsule_GetPointer(capsule, name)` | `capsule` is the live capsule under destruction; `name` is the `'static CStr` the capsule was created with. Returns the stored ptr or null. | SOUND. |
| L66 | `PyErr_Clear()` | Only reached when GetPointer returned null and set an error; clears it so no stray error leaks into the interpreter. | SOUND. |
| L72 | `Box::from_raw(ptr.cast::<T>())` | `ptr` came from `Box::into_raw(Box::<T>::new(..))` in `new_ffi_capsule` under the same `name`; reclaimed exactly once (CPython calls the destructor once). Runs `T::Drop`, which runs the Arrow C release iff not already moved out. | SOUND — no double-free / use-after-free. |
| L75/77, L80/82, L85/87 | typed `schema/array/stream_destructor` + `capsule_destructor::<T>` call | Each wrapper has the pyo3 `PyCapsule_Destructor` ABI (`fn(*mut PyObject)`) and forwards its own `name` + `T`. Name↔type↔destructor are consistent per `*_capsule` constructor. | (A) FFI ABI. SOUND. |
| L95 | `unsafe extern "C" fn(...)` param type | Type-level only; the value passed is always one of the three audited wrappers. | SOUND. |
| L106 | `PyCapsule::new_with_pointer_and_destructor` | `ptr` is a fresh `NonNull` leaked `Box<T>`; `name` `'static`; `destructor` reclaims exactly this box under this name; ownership transfers to the capsule. On creation failure the box leaks (benign OOM-path leak, never UB). | (A) FFI. SOUND. |
| L263 | `&*ptr.cast::<FFI_ArrowSchema>()` (import_schema) | Capsule owns a valid consumer-side `FFI_ArrowSchema`; we read by shared ref and deep-clone, never taking ownership / running release. The reborrow lifetime is not tied to `capsule_obj` by the compiler but is used only while `capsule_obj` is live. | SOUND — read-only, no dangle. |
| L302 | `ArrowArrayStreamReader::from_raw(ptr.cast::<FFI_ArrowArrayStream>())` (import_arrow_batches) | Capsule owns a valid consumer-side stream; `from_raw` moves it out (ptr valid r/w, aligned, init), leaving the capsule struct released so its destructor no-ops. Each `__arrow_c_stream__()` call yields a fresh capsule, so move-out happens exactly once per capsule. | SOUND — single ownership transfer. |

No unsound block was found. All sites are FFI-inherent: there is no safe-Rust
formulation of the Arrow C Data Interface PyCapsule export (it is a raw-pointer +
C-callback ABI), so they are (A) STRICTLY_UNAVOIDABLE. None are removable via a
safe abstraction.

### Fixes / tightenings made (additive, behavior-preserving)

Two SAFETY comments were hardened to name invariants the compiler does not
enforce (the code was already sound; the comments now document why):

1. **L263 `import_schema`** — added the lifetime invariant: `&*ptr` is reborrowed
   from a raw pointer, so the borrow checker does NOT tie its lifetime to
   `capsule_obj`. The borrow is valid only while the capsule (which owns the
   `FFI_ArrowSchema`) is alive; `ffi_schema` is used solely before the
   function-scoped `capsule_obj` is dropped, so it never dangles.

2. **L106 `new_ffi_capsule`** — documented the capsule-creation failure path:
   on a Python `MemoryError` the destructor is never registered and the box
   leaks; this is a benign OOM-path leak, never a double-free or use-after-free.
   Also recorded the "destructor invoked exactly once by CPython" guarantee.

No `unsafe` block was changed (these are comment-only edits). No code behavior
changed.

### Miri applicability (honest note)

Miri **cannot** meaningfully exercise these unsafe sites: every one is reachable
only through a live CPython interpreter (PyO3 `Python<'py>` token, real
`PyCapsule` objects) and real arrow-rs C Data Interface structs produced by
`to_ffi` / consumed by `from_raw`. Miri does not execute the CPython C runtime
or the Arrow C release callbacks, so a Miri run would either fail to link the
FFI or test nothing of substance. The pure-Rust helpers reachable from this
module (`oracledb::arrow::arrow_type_name`, `decimal128_to_string`) live in a
`#![forbid(unsafe_code)]` crate and contain no unsafe to validate. The soundness
argument for the FFI sites therefore rests on the contract verification in §2
(arrow-rs + pyo3 source), not on a Miri run — stated honestly per the audit
protocol.

---

## 3. Unsafe inventory (exact)

`crates/oracledb-pyshim/src/arrow_capsule.rs` — 15 `unsafe` occurrences:
L19 (`#![allow(unsafe_code)]`), L60 (`unsafe extern "C" fn`), L63, L66, L72
(blocks in `capsule_destructor`), L75/77, L80/82, L85/87 (typed destructors),
L95 (fn-ptr param type), L106 (capsule creation), L263 (schema read), L302
(stream move-out). All other workspace `unsafe` lexemes are in comments only.

---

## 4. Fuzz re-run evidence

The fuzz crate is a standalone workspace at
`crates/oracledb-protocol/fuzz` (libfuzzer-sys, release + debug-assertions +
overflow-checks so the decoders must fail closed, never panic).

### Regression corpus — all pass (no crash)

| File | Target | Result |
|---|---|---|
| oson_oom_oversized_counts.bin | oson_decoder | executed, no crash |
| query_implicit_resultset_oom.bin | query_response | executed, no crash |
| query_sb4_negate_overflow.bin | query_response | executed, no crash |
| vector_oom_oversized_element_count.bin | vector_decoder | executed, no crash |

### New fuzz targets wired for the recent merges (additive)

The AQ and CQN/subscription decoders added by the recent merges had no fuzz
coverage. Two new `#[cfg(fuzzing)]` `fuzz_api` wrappers + targets were added,
following the existing `fuzz_api` pattern exactly:

- `fuzz_api::fuzz_aq_responses` → target `aq_response` — drives
  `parse_aq_enq_response`, `parse_aq_deq_response`, `parse_aq_array_response`
  (leading byte selects TTC field version, payload kind, array op, props count).
- `fuzz_api::fuzz_subscr_responses` → target `subscr_response` — drives
  `parse_subscribe_response` and `parse_notification_stream` (the OAC-record
  parser; leading byte selects TTC field version, namespace, QoS).

TPC/XA (`thin/sessionless.rs`) is request-side encoding plus a thin response
status read; its server-response parse path is covered by the existing
`query_response` target. No separate TPC decoder warranted a new target.

### Short fuzz session — 60s/target, 9 targets

`cargo +nightly fuzz run <target> -- -max_total_time=60` for all 9 targets.
ALL exited 0, zero crashes, zero crash artifacts
(`crates/oracledb-protocol/fuzz/artifacts` has no files). Evidence:
`.unsafe-audit/fuzz_run.log`.

| Target | Runs (≈61s) | cov | corpus | Result |
|---|---|---|---|---|
| packet_framing | 40,739,515 | 41 | 5 | no crash |
| query_response | 3,096,043 | 1675 | 1927 | no crash |
| oson_decoder | 282,641 | 630 | 348 | no crash |
| vector_decoder | 10,605,850 | 210 | 111 | no crash |
| scalar_codecs | 11,336,511 | 220 | 97 | no crash |
| server_error_info | 4,660,879 | 623 | 582 | no crash |
| dpl_response | 3,885,737 | 929 | 1125 | no crash |
| **aq_response** (new) | 4,008,283 | 1140 | 1450 | no crash |
| **subscr_response** (new) | 3,169,380 | 1081 | 1148 | no crash |

The two new targets reached substantive coverage of the recently-merged AQ and
CQN/subscription decoders (cov 1140 / 1081, ft 4997 / 4052) with no
panic / overflow / over-read / over-allocation under release + debug-assertions
+ overflow-checks. No new crashes surfaced after the AQ/TPC/CQN feature merges.

---

## 5. Supply-chain status

### cargo-audit — 2 advisories (both pyo3 0.28.3, both pyshim-only)

| ID | Title | Affected | Reachable in our code? |
|---|---|---|---|
| RUSTSEC-2026-0176 | OOB read in `nth`/`nth_back` for `PyList`/`PyTuple` iterators | pyo3 0.28.3 | NO — we never call `.nth()`/`.nth_back()` on a PyList/PyTuple iterator. |
| RUSTSEC-2026-0177 | Missing `Sync` bound on `PyCFunction::new_closure` closures | pyo3 0.28.3 | NO — we never call `new_closure`. |

Both advisories were published 2026-06-11 (days before this audit). Both are
confined to `oracledb-pyshim` (the PyO3 conformance-test harness) — the
published `oracledb` / `oracledb-protocol` crates do not depend on pyo3.
`grep` over the pyshim source confirms neither vulnerable API is used, so the
practical exposure is nil. Solution: `cargo update -p pyo3` to >=0.29.0 when the
pyshim is next touched (a pyo3 0.28→0.29 bump is a separate, non-trivial change
outside this audit's additive scope).

### cargo-deny

- advisories: FAILED — the same two pyo3 advisories above (no new info).
- bans: ok.
- sources: ok.
- licenses: FAILED — artifact of there being NO `deny.toml`, so the default
  policy has an empty allow-list and rejects every license as "not explicitly
  allowed." This is a missing-config signal, not a real licensing problem (the
  workspace is MIT/Apache-2.0 dual-licensed). Recommend adding a `deny.toml`
  with an `[licenses] allow = [...]` list if cargo-deny is wired into CI.

---

## 6. Beads filed

No unsound code was found, so no UB/soundness beads were filed. Two supply-chain
follow-ups (out of the additive scope of this lane) were filed under parent
`rust-oracledb-j0o`:

- `rust-oracledb-j0o.1` — pyo3 0.28.3 → >=0.29.0 to clear RUSTSEC-2026-0176/0177
  (both pyshim-only, both confirmed unreachable from our code).
- `rust-oracledb-j0o.2` — add a workspace `deny.toml` so the cargo-deny
  license/advisory gate is enforceable in CI.

---

## 7. Gate verification

| Gate | Result |
|---|---|
| `cargo fmt --check` | clean (exit 0) |
| `cargo clippy --workspace --no-deps -- -D warnings` | clean (no warnings) |
| `cargo clippy -p oracledb-protocol` under `--cfg fuzzing` | clean (validates new fuzz_api) |
| `cargo test --workspace` | all green (0 failed) |

Because `arrow_capsule.rs` was touched (comment-only), the shim was rebuilt
(`maturin develop`) against lane container `rust-oracledb-lane-1524` and the FFI
+ sentinel conformance tests were re-run:

| Test | Result | Baseline |
|---|---|---|
| test_8000_dataframe (FFI capsule path) | 82 passed | 82p ✓ |
| test_9100_dataframe_vector (FFI vector path) | 14 passed | 14p ✓ |
| test_1100_connection (sentinel) | 57 passed, 5 skipped | 57p/5s ✓ |

The comment-only edits preserve behavior exactly; all three match baseline.

---

## 8. Verdict summary

- forbid(unsafe_code) on `oracledb-protocol` + `oracledb`: **holds**, structurally
  airtight, including all new AQ/CQN/TPC decoders.
- Unsafe surface = exactly one module (`arrow_capsule.rs`), 11 distinct unsafe
  constructs, all FFI-inherent (A) STRICTLY_UNAVOIDABLE, all **SOUND**.
- No unsound block found; no double-free / UAF / dangling reference. Two SAFETY
  comments hardened (comment-only, behavior-preserving).
- Fuzz: regression corpus clean; existing + 2 new (AQ, subscr/CQN) targets run
  without crashes.
- Supply chain: 2 pyo3 advisories, both pyshim-only and both unreachable from
  our code; recommend a future pyo3 >=0.29 bump. cargo-deny licenses FAILED is a
  missing-`deny.toml` artifact, not a real issue.

---

## 9. Panic-safety addendum (1.0.0-rc.1 readiness, 2026-06-22)

Added during the pre-1.0 hardening pass. §1–§8 above audit `unsafe`/UB; this
section records the **panic-safety** posture (no untrusted server input may
panic the driver) and two deliberate lint decisions. It also supersedes the
stale parts of §5: a workspace `deny.toml` now exists and `cargo deny check` is
exit-0 clean and wired into CI (`ci.yml`, `_quality.yml`, `release.yml`).

### Active panic-relevant lint gates

`[workspace.lints]` with the `-D warnings` clippy gate: `unsafe_code = "forbid"`,
`clippy::unwrap_used = "deny"`, `clippy::todo = "deny"`, `clippy::dbg_macro =
"deny"`. No `.unwrap()` / `todo!` / `unimplemented!` / `dbg!` in shipped code.

### Decode-core panic-safety

Confirmed by the W3-E8 multi-pass bug-hunt (5 adversarial rounds, ~25
correctness bugs fixed) and the pre-1.0 analysis:

- No `.unwrap()` / `.expect()` in production protocol decode paths. The ~636
  `.expect()` in `src` are overwhelmingly in `#[cfg(test)]` modules, where
  `.expect()` is the house style (`unwrap_used` denied, `expect_used` not).
- Every wire read routes through `check_response_bytes` (`wire.rs`), enforcing a
  running byte total against the response bound before any slice; `OsonReader`
  bounds every offset; LOB/chunk reads enforce per-chunk + total caps.
- Decode-path `[i]` / `[a..b]` indexing is preceded by a validated length prefix,
  so it cannot panic on server input.
- Fuzzing: 19 libFuzzer targets over the decode surface ran with **zero crashes**
  in the 1.0.0-rc.1 qualification (`docs/qualification/1.0.0-rc.1/fuzz_summary.txt`).

### Deliberately NOT enabled: `clippy::indexing_slicing` / `clippy::expect_used`

We evaluated escalating both to a gate and chose not to, on purpose:

- `clippy::indexing_slicing` fires ~345× in shipped lib code (≈294 protocol,
  ≈51 driver), almost all on indexing already bounds-checked by an upstream
  length-prefix validation. Mechanically rewriting those to `.get(i)?` would
  change error surfaces and risk regressions for no real safety gain — the
  upstream bound is the actual invariant, covered by the audit + fuzzing above.
- `clippy::expect_used` cannot be scoped to non-test code from `[workspace.lints]`
  (it applies to all targets) and would conflict with the test house style.

Re-audit this decision if the decode surface ever gains a path that indexes
server-controlled data without a prior length check.
