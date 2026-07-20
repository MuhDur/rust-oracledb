#!/usr/bin/env python3
"""Differential fuzz oracle: rust-oracledb decoder vs python-oracledb decoder.

WHAT THIS IS (and its fidelity, stated honestly)
-------------------------------------------------
This is a **container round-trip differential**. The Oracle server is the
*encoder*; the SAME server wire bytes for each column are then decoded by BOTH:

  * the reference python-oracledb thin decoder (`impl/base/decoders.pyx`), and
  * rust-oracledb's decoder, surfaced through the `oracledb_pyshim` PyO3 module
    that swaps in for `oracledb.thin_impl`.

For a proptest-style corpus of *extreme / boundary* values, we INSERT the value
once, then FETCH the identical row through each engine and assert the decoded
Python values and their concrete Python types are identical. A divergence is a
real decoder bug of the exact "silent value divergence" class that produced the
8 hand-found bugs
(the bug class that no-panic fuzzing and example tests structurally miss).

FIDELITY CAVEAT (honest): a container round-trip differential is **weaker** than
a pure in-process decoder differential, for two reasons:
  1. The server produces the wire bytes, so we only ever decode *well-formed*
     payloads the server actually emits — we cannot feed an adversarial /
     hand-crafted wire image to both decoders (python-oracledb's `cdef` decoders
     take a C `OracleDataBuffer` and are not callable on raw bytes from Python,
     so a pure decoder differential is impractical without re-exporting Cython).
  2. Both engines share the same server, so a server-side quirk is invisible.
What it DOES prove with high confidence: for every value Oracle can store and
emit, rust and python-oracledb recover the *same Python type and value*. Both
parts are required: Python equality considers values such as `100` and `100.0`
equal even though returning `int` instead of `float` is a parity regression.

To defeat the float-precision blind spot (the default NUMBER->float mapping
collapses the 38-significant-digit cases), NUMBER columns are fetched through an
output type handler that returns the **full decimal string** — so the exact
decoded digit/sign/exponent sequence from `decode_number_value` is compared, not
a lossy f64. DATE/TIMESTAMP keep microsecond precision via `datetime`; we also
fetch the canonical `TO_CHAR(...FF9)` string for nanosecond fidelity.

USAGE
-----
    eval "$(ORACLEDB_CONTAINER_NAME=... ORACLEDB_HOST_PORT=... scripts/container.sh env)"
    .venv-py313/bin/python harness/differential/diff_oracle.py [--cases N] [--seed S]
    python3 harness/differential/diff_oracle.py --self-test

Exit code 0 = all cases agreed; 1 = at least one divergence (printed + a bead
should be filed). The two engines are run in **separate subprocesses** because
the `oracledb.thin_impl` swap is process-global (cannot host both decoders in
one interpreter).
"""

from __future__ import annotations

import argparse
import json
import os
import random
import subprocess
import sys

# ---------------------------------------------------------------------------
# Corpus generation (proptest-style: deterministic from a seed, boundary-dense).
# ---------------------------------------------------------------------------

# 38 significant digits is the Oracle NUMBER precision ceiling; the exponent
# range the wire format must survive is roughly -130..125.
_MAX_NUMBER_DIGITS = 38


def _gen_numbers(rng: random.Random, n: int) -> list[str]:
    """Decimal literals (as SQL text) spanning the full NUMBER domain.

    Always includes the named boundary cases, then fills to `n` with random
    finite decimals at extreme magnitudes / precisions. Returned as SQL numeric
    literals so the server stores the exact value (no client-side rounding).
    """
    fixed = [
        "0",
        "1",
        "-1",
        "0.1",
        "-0.1",
        "100",
        "-100",
        "99",
        "-99",
        "0.01",
        "-0.01",
        # base-100 mantissa edges and single-byte zero region
        "127",
        "128",
        "255",
        "256",
        # max precision integer and fractional, +/-
        "12345678901234567890123456789012345678",
        "-12345678901234567890123456789012345678",
        "0.12345678901234567890123456789012345678",
        "-0.12345678901234567890123456789012345678",
        # near the 21-byte wire NUMBER limit
        "9999999999999999999999999999999999999",
        # extreme exponents (Oracle NUMBER range ~1e-130 .. 9.99e125)
        "1E125",
        "-1E125",
        "1E-130",
        "9.999999999999999999999999999999999999E125",
        # values that exercise trailing-zero stripping / scale
        "100.000",
        "1.0",
        "-0.0",
        "1000000000000000000000",
        "0.00000000000000000001",
    ]
    out = list(fixed)
    while len(out) < n:
        ndigits = rng.randint(1, _MAX_NUMBER_DIGITS)
        digits = "".join(rng.choice("0123456789") for _ in range(ndigits))
        digits = digits.lstrip("0") or "0"
        # random decimal-point position and explicit exponent
        point = rng.randint(0, len(digits))
        exp = rng.randint(-120, 120)
        sign = "-" if rng.random() < 0.5 else ""
        int_part = digits[:point] or "0"
        frac_part = digits[point:]
        mantissa = int_part if not frac_part else f"{int_part}.{frac_part}"
        out.append(f"{sign}{mantissa}E{exp}")
    return out[:n]


def _gen_datetimes(rng: random.Random, n: int) -> list[str]:
    """`YYYY-MM-DD HH24:MI:SS.FF9` strings spanning the representable range."""
    fixed = [
        "0001-01-01 00:00:00.000000000",  # earliest
        "9999-12-31 23:59:59.999999999",  # latest, max fraction
        "1970-01-01 00:00:00.000000000",  # epoch
        "1969-12-31 23:59:59.000000000",  # pre-epoch
        "2000-02-29 12:00:00.500000000",  # leap day
        "2024-02-29 23:59:59.123456789",  # leap day, full fraction
        "1582-10-15 00:00:00.000000000",  # Gregorian start
        "0100-01-01 00:00:00.000000001",  # smallest nonzero fraction
    ]
    out = list(fixed)
    while len(out) < n:
        year = rng.randint(1, 9999)
        month = rng.randint(1, 12)
        day = rng.randint(1, 28)
        hh = rng.randint(0, 23)
        mm = rng.randint(0, 59)
        ss = rng.randint(0, 59)
        ns = rng.randint(0, 999_999_999)
        out.append(f"{year:04d}-{month:02d}-{day:02d} {hh:02d}:{mm:02d}:{ss:02d}.{ns:09d}")
    return out[:n]


def build_corpus(seed: int, cases: int) -> dict:
    rng = random.Random(seed)
    # split the budget across the two highest-divergence-risk codecs
    n_num = max(1, cases // 2)
    n_dt = cases - n_num
    return {
        "numbers": _gen_numbers(rng, n_num),
        "datetimes": _gen_datetimes(rng, n_dt),
    }


# ---------------------------------------------------------------------------
# Fetch worker: run by each subprocess with a chosen decoder engine.
# ---------------------------------------------------------------------------

_FETCH_WORKER = r"""
import importlib, sys, os, json

ENGINE = os.environ["DIFF_ENGINE"]
if ENGINE == "rust":
    sys.modules["oracledb.thin_impl"] = importlib.import_module("oracledb_pyshim")
import oracledb

corpus = json.load(sys.stdin)

def typed_value(value, render=lambda item: item):
    # JSON-safe value plus the exact Python type that the decoder returned.
    value_type = type(value)
    return {
        "type": f"{value_type.__module__}.{value_type.__qualname__}",
        "value": render(value),
    }

def number_as_str(cursor, name, default_type, size, precision, scale):
    # Fetch every NUMBER as its exact decimal string so the full decoded
    # digit/sign/exponent sequence is compared (not a lossy float).
    if default_type == oracledb.DB_TYPE_NUMBER:
        return cursor.var(str, arraysize=cursor.arraysize)
    return None

conn = oracledb.connect(
    user=os.environ["PYO_TEST_MAIN_USER"],
    password=os.environ["PYO_TEST_MAIN_PASSWORD"],
    dsn=os.environ["PYO_TEST_CONNECT_STRING"],
)
cur = conn.cursor()

results = {"numbers": [], "datetimes": []}

# A value the SERVER refuses to store/emit (e.g. ORA-01426 numeric overflow for
# an out-of-range magnitude) is not a decode case — it never produces wire bytes
# for either decoder. We record a "__SERVER_REJECTED__" sentinel so both engines
# skip it identically; the comparator ignores those. This keeps the differential
# focused on values Oracle actually round-trips.
SKIP = "__SERVER_REJECTED__"

# NUMBER: decode the exact stored value as a string via the type handler.
cur.outputtypehandler = number_as_str
for lit in corpus["numbers"]:
    try:
        cur.execute("select cast(" + lit + " as number) from dual")
        (val,) = cur.fetchone()
        results["numbers"].append(typed_value(val, lambda value: str(value)))
    except oracledb.DatabaseError:
        results["numbers"].append(SKIP)
cur.outputtypehandler = None

# DATE/TIMESTAMP: compare both the datetime (microsecond) and the canonical
# TO_CHAR(...FF9) (nanosecond) rendering, so a fractional-second decode bug or a
# civil-field carry bug both surface.
for lit in corpus["datetimes"]:
    try:
        cur.execute(
            "select to_timestamp(:v,'YYYY-MM-DD HH24:MI:SS.FF9'), "
            "to_char(to_timestamp(:v,'YYYY-MM-DD HH24:MI:SS.FF9'),"
            "'YYYY-MM-DD HH24:MI:SS.FF9') from dual",
            {"v": lit},
        )
        dt, txt = cur.fetchone()
        results["datetimes"].append({
            "dt": typed_value(dt, lambda value: None if value is None else value.isoformat()),
            "txt": typed_value(txt),
        })
    except oracledb.DatabaseError:
        results["datetimes"].append(SKIP)

conn.close()
json.dump(results, sys.stdout)
"""


def _run_engine(engine: str, corpus: dict) -> dict:
    env = dict(os.environ)
    env["DIFF_ENGINE"] = engine
    # Ensure the rust shim is importable (harness/ on the path).
    root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    env["PYTHONPATH"] = os.path.join(root, "harness") + os.pathsep + env.get("PYTHONPATH", "")
    proc = subprocess.run(
        [sys.executable, "-c", _FETCH_WORKER],
        input=json.dumps(corpus),
        capture_output=True,
        text=True,
        env=env,
    )
    if proc.returncode != 0:
        raise RuntimeError(
            f"{engine} fetch worker failed (rc={proc.returncode}):\n{proc.stderr}"
        )
    return json.loads(proc.stdout)


# ---------------------------------------------------------------------------
# Comparison / reporting.
# ---------------------------------------------------------------------------


SKIP = "__SERVER_REJECTED__"


def compare(corpus: dict, ref: dict, rust: dict) -> tuple[list[dict], int, int]:
    """Compare exact result shape, Python types, and rendered values."""
    divergences: list[dict] = []
    compared = 0
    skipped = 0

    for kind in ("numbers", "datetimes"):
        expected = len(corpus[kind])
        ref_count = len(ref.get(kind, []))
        rust_count = len(rust.get(kind, []))
        if ref_count != expected or rust_count != expected:
            divergences.append(
                {
                    "type": kind.upper(),
                    "input": "<result-count>",
                    "ref": ref_count,
                    "rust": rust_count,
                    "note": f"expected {expected} results from each engine",
                }
            )

    for lit, r, u in zip(corpus["numbers"], ref["numbers"], rust["numbers"]):
        # If EITHER engine's server rejected the value, skip — but a divergence
        # in *which* engine accepted it would itself be a bug, so flag mismatched
        # acceptance.
        if r == SKIP or u == SKIP:
            if r != u:
                divergences.append(
                    {"type": "NUMBER", "input": lit, "ref": r, "rust": u,
                     "note": "one engine accepted, the other rejected"}
                )
            else:
                skipped += 1
            continue
        compared += 1
        if r != u:
            divergences.append({"type": "NUMBER", "input": lit, "ref": r, "rust": u})

    for lit, r, u in zip(corpus["datetimes"], ref["datetimes"], rust["datetimes"]):
        if r == SKIP or u == SKIP:
            if r != u:
                divergences.append(
                    {"type": "TIMESTAMP", "input": lit, "ref": r, "rust": u,
                     "note": "one engine accepted, the other rejected"}
                )
            else:
                skipped += 1
            continue
        compared += 1
        if r != u:
            divergences.append({"type": "TIMESTAMP", "input": lit, "ref": r, "rust": u})

    return divergences, compared, skipped


def self_test() -> None:
    """Prove the comparator rejects equal-looking wrong types and wrong values."""
    corpus = {
        "numbers": ["100"],
        "datetimes": ["2024-02-29 23:59:59.123456789"],
    }
    baseline = {
        "numbers": [{"type": "builtins.str", "value": "100"}],
        "datetimes": [
            {
                "dt": {
                    "type": "datetime.datetime",
                    "value": "2024-02-29T23:59:59.123456",
                },
                "txt": {
                    "type": "builtins.str",
                    "value": "2024-02-29 23:59:59.123456789",
                },
            }
        ],
    }
    divergences, compared, skipped = compare(corpus, baseline, baseline)
    assert not divergences and compared == 2 and skipped == 0

    # Python says 100 == 100.0. The old value-only assertion shape could let
    # that stale-CONFIRMED regression pass; the type-bearing record must fail.
    wrong_type = json.loads(json.dumps(baseline))
    wrong_type["numbers"][0] = {"type": "builtins.float", "value": "100"}
    divergences, _, _ = compare(corpus, baseline, wrong_type)
    assert len(divergences) == 1, "injected wrong-type regression was not caught"

    wrong_value = json.loads(json.dumps(baseline))
    wrong_value["datetimes"][0]["dt"]["value"] = "2024-02-29T23:59:59.123455"
    divergences, _, _ = compare(corpus, baseline, wrong_value)
    assert len(divergences) == 1, "injected wrong-value regression was not caught"


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--cases", type=int, default=2000, help="total generated cases")
    ap.add_argument(
        "--seed",
        type=lambda s: int(s, 0),
        default=0xC0FFEE,
        help="corpus RNG seed (decimal or 0x-hex)",
    )
    ap.add_argument("--quiet", action="store_true")
    ap.add_argument(
        "--self-test",
        action="store_true",
        help="inject wrong-type and wrong-value results and prove they fail",
    )
    args = ap.parse_args()

    if args.self_test:
        self_test()
        print("OK: differential comparator caught wrong-type and wrong-value injections.")
        return 0

    for var in ("PYO_TEST_CONNECT_STRING", "PYO_TEST_MAIN_USER", "PYO_TEST_MAIN_PASSWORD"):
        if not os.environ.get(var):
            print(f"missing required env {var}; run scripts/container.sh env first", file=sys.stderr)
            return 2

    corpus = build_corpus(args.seed, args.cases)
    n_total = len(corpus["numbers"]) + len(corpus["datetimes"])

    ref = _run_engine("ref", corpus)
    rust = _run_engine("rust", corpus)

    divergences, compared, skipped = compare(corpus, ref, rust)

    if not args.quiet:
        print(
            f"differential oracle: {n_total} generated "
            f"({len(corpus['numbers'])} NUMBER, {len(corpus['datetimes'])} DATE/TIMESTAMP), "
            f"seed={hex(args.seed)}"
        )
        print(
            f"  compared={compared} (both engines round-tripped), "
            f"skipped={skipped} (server rejected the magnitude — not a decode case)"
        )
    if divergences:
        print(f"DIVERGENCES FOUND: {len(divergences)}")
        for d in divergences[:50]:
            print(
                f"  [{d['type']}] input={d['input']!r}{(' — ' + d['note']) if d.get('note') else ''}\n"
                f"      ref ={d['ref']!r}\n"
                f"      rust={d['rust']!r}"
            )
        return 1

    print(f"OK: rust and python-oracledb decoders agreed on all {compared} compared cases.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
