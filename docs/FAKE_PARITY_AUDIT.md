# Fake-Parity Audit — Suite-Green Integrity Verification

**Date:** 2026-06-13 · **Commit audited:** `73524b2` (suite green) · **Verdict: REAL (all slices)**

The filtered python-oracledb thin suite (72 in-scope modules, 2236 tests) passes with
**0 regressions vs the baseline manifest**. Because the whole premise of this port is
"no fake parity," the green was adversarially audited before being claimed.

## Method

Five independent auditors, each tasked to *disprove* the green for a slice of modules, using:
1. **Offline falsification** — point the shim at a dead port; a fabricating shim would still
   "pass". Result: immediate `Connection refused (os error 111)` at connect. No offline path.
2. **Raw socket capture** — `strace` of `recvfrom`/`sendto` on the connection fd, reconstructing
   the actual wire bytes and searching for the codec images / server-computed values.
3. **Data-flow trace** — shim (marshalling) → `oracledb` driver → `oracledb-protocol` codec →
   `asupersync::net::TcpStream` to the real Oracle 23ai container.

## Findings (all REAL, evidence-backed)

| Slice | Modules | Verdict | Evidence |
|---|---|---|---|
| scalars+lob | 1300/1400/1900/2200/2500/2600/2900 | REAL | dead-port → connection refused; no offline fabrication |
| objects+json+vector | 2300/3500/6700/6900/6400/7700 | REAL | OSON/object/vector codec images found in raw socket bytes; codecs run in protocol crate; `#[cfg(test)]` unit data is not in the shipped hot path |
| cursor+execute+pool | 3900/4000/1600/2400/4300/3200 | REAL | executemany inserts rows; server-side `select count(*)` confirms them on the wire |
| async+pipeline+sessionless | 5300/5400/7600/8700/8800 | REAL | async path uses real asupersync driver futures via `Runtime::block_on` + ambient `Cx`; sessionless does real TPC-switch wire ops |
| dataframe+dpl+connect | 8000/9100/9300/9600/1100/4500/7200 | REAL | full TNS handshake + AUTH + `select 7+5 from dual` → server-computed `12`; DPL does real TTC 128/129/130 |

## Honest limitations confirmed (not fakes)

- **Pipelining (test_7600)** passes via the **sequential runner**, with `supports_pipelining()`
  openly returning `false`. Each op is a real wire round-trip (just one-per-op, not batched).
  The native single-round-trip transport is proven at the driver layer
  (`pipeline_round_trips_against_local_container`) but deliberately not wired into the Python
  pipeline path. This is the documented honest fallback, not fabrication.
- The shim contains exactly **10 `not_implemented` sites** — explicit fail-closed errors for
  genuinely-unimplemented edge paths (persistent-LOB write, quoted-identifier edge, a few
  cursor/LOB/DbObject value-conversion corners). They error, never fake.

## Static scan

`scripts/fake_parity_scan.py` flags only legitimate protocol keywords (`tns`/`ttc`/`oson`/
`pbkdf2`) inside the *protocol* crate — no shim-side simulation, no local query/result synthesis,
no hardcoded fixture values. (The earlier dbms_output / v$sql_monitor shim simulations were
removed during Wave 1.)

**Conclusion:** the 2236-test suite-green is genuine end-to-end behavior through the Rust engine
against a real Oracle database. The port passes python-oracledb's own test suite for real.
