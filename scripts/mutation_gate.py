#!/usr/bin/env python3
"""Turn a cargo-mutants run into mutation-result/v1, with real witnesses.

  scripts/mutation_gate.py --emit --mutants-out DIR --targets FILE... [-o OUT]
  scripts/mutation_gate.py --check FILE [--expected-sha REV]

Why this exists
---------------
A mutation score is the easiest number in testing to state and the hardest to
check. "94% mutation coverage" is unfalsifiable on its own: you cannot tell
whether the denominator excluded timeouts, whether the survivors were ever
looked at, or whether a "kill" was a test that fails on everything.

So this never emits a score alone. It emits the raw counts, the denominator it
declares, and a witness per claimed kill, and the reader recomputes the rate.
The rules live in mutation-result/v1 (docs/EVIDENCE_CONTRACT.md); this is the
producer.

The witnesses are the point
---------------------------
Each kill carries two, and neither is invented:

  mutant_fails_test   a NAMED test that FAILED with the mutant applied, read out
                      of that mutant's own cargo-mutants log.
  head_passes_test    the SAME named test passing on unmutated HEAD, read out of
                      the baseline log cargo-mutants records before it starts.

Both are required because either alone proves nothing. Without the second, a
permanently-red test "kills" every mutant it touches and the score looks superb.

If no test both fails under the mutant and passes at HEAD, this emits NO witness
for that kill rather than a plausible-looking one. The kill count then exceeds
the witnessed kills, mutation-result/v1 rejects the document with
E_MISSING_WITNESS, and the gate fails. That is the intended outcome: an
unwitnessed kill is an assertion, and this refuses to launder it into evidence.
"""

from __future__ import annotations

import argparse
import importlib.util
import json
import re
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent

_spec = importlib.util.spec_from_file_location(
    "validate_evidence", ROOT / "scripts" / "validate_evidence.py"
)
_ve = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(_ve)

# libtest lines: "test tls::sni::tests::sni_basic ... ok" / "... FAILED"
_TEST_OK_RE = re.compile(r"^test ([\w:<>\- ]+?) \.\.\. ok\s*$", re.M)
_TEST_FAIL_RE = re.compile(r"^test ([\w:<>\- ]+?) \.\.\. FAILED\s*$", re.M)

# cargo-mutants summary -> our vocabulary.
CAUGHT = "CaughtMutant"
MISSED = "MissedMutant"


def _read_log(out_dir: Path, rel: str) -> str:
    path = out_dir / rel
    return path.read_text(errors="replace") if path.exists() else ""


def _git(*args: str) -> str:
    return subprocess.run(
        ["git", "-C", str(ROOT), *args], capture_output=True, text=True, check=False
    ).stdout.strip()


def _commit_sha(revision: str) -> str:
    result = subprocess.run(
        ["git", "-C", str(ROOT), "rev-parse", "--verify", f"{revision}^{{commit}}"],
        capture_output=True,
        text=True,
        check=False,
    )
    if result.returncode != 0:
        raise SystemExit(f"mutation-gate: {revision!r} is not a commit in this checkout")
    return result.stdout.strip()


def _budget(profile: str, run_id: str) -> dict:
    out = subprocess.run(
        [
            str(ROOT / "scripts" / "resource_budget.sh"),
            "--profile",
            profile,
            "--run-id",
            run_id,
            "--emit-budget",
        ],
        capture_output=True,
        text=True,
        check=False,
    )
    if out.returncode != 0:
        raise SystemExit(f"mutation-gate: resource_budget.sh failed: {out.stderr.strip()}")
    return json.loads(out.stdout)


def _mutant_id(mutant: dict) -> str:
    """Return a stable identifier that cannot collide across same-named files."""

    span = mutant["span"]["start"]
    return (
        f"{Path(mutant['file']).as_posix()}:"
        f"{span['line']}:"
        f"{span['column']}:"
        f"{mutant['replacement']}"
    )


def emit(args) -> int:
    out_dir = Path(args.mutants_out)
    if out_dir.name != "mutants.out":
        out_dir = out_dir / "mutants.out"
    outcomes = json.loads((out_dir / "outcomes.json").read_text())

    # HEAD-passes witnesses come from the baseline cargo-mutants runs BEFORE any
    # mutant: that is precisely "these tests pass on the unmutated tree".
    baseline_ok = set(_TEST_OK_RE.findall(_read_log(out_dir, "log/baseline.log")))
    if not baseline_ok:
        print(
            "mutation-gate: the baseline log names no passing test, so no kill can "
            "carry a head_passes_test witness. Refusing to emit a document whose "
            "kills would be unwitnessed.",
            file=sys.stderr,
        )
        return 1

    kills, survivors = [], []
    unwitnessed = []

    for outcome in outcomes["outcomes"]:
        scenario = outcome["scenario"]
        if scenario == "Baseline":
            continue
        mutant = scenario["Mutant"]
        location = f"{mutant['file']}:{mutant['span']['start']['line']}"
        mutant_id = _mutant_id(mutant)

        if outcome["summary"] == CAUGHT:
            failed = set(_TEST_FAIL_RE.findall(_read_log(out_dir, outcome["log_path"])))
            # The witness must satisfy BOTH directions, so intersect. sorted()
            # keeps the choice deterministic across runs.
            both = sorted(failed & baseline_ok)
            if not both:
                unwitnessed.append(mutant_id)
                continue
            test = both[0]
            kills.append(
                {
                    "mutant_id": mutant_id,
                    "location": location,
                    "mutant_fails_test": {"test": test, "outcome": "fail"},
                    "head_passes_test": {"test": test, "outcome": "pass"},
                }
            )
        elif outcome["summary"] == MISSED:
            survivors.append(
                {
                    "mutant_id": mutant_id,
                    "location": location,
                    # cargo-mutants cannot know WHY a mutant survived. Saying so
                    # is honest; inventing "equivalent-mutant" would not be.
                    "taxonomy": "triage-pending",
                    "note": f"replacement {mutant['replacement']!r} in {mutant['function']['function_name']!r} survived; not yet triaged",
                }
            )

    if unwitnessed:
        print(
            f"mutation-gate: {len(unwitnessed)} kill(s) have no test that both fails "
            f"under the mutant and passes at HEAD: {', '.join(unwitnessed[:3])}. "
            "Emitting them without a witness would be laundering an assertion into "
            "evidence; the document will be rejected by E_MISSING_WITNESS.",
            file=sys.stderr,
        )

    counts = {
        "caught": outcomes["caught"],
        "missed": outcomes["missed"],
        "timeout": outcomes["timeout"],
        "unviable": outcomes["unviable"],
    }
    denominator = counts["caught"] + counts["missed"]
    rate = (counts["caught"] / denominator) if denominator else 0.0

    doc = {
        "schema": "mutation-result/v1",
        "repo": "rust-oracledb",
        "generated_at": args.generated_at,
        "source": {
            "sha": args.sha,
            "tree_clean": args.tree_clean,
            "branch": args.branch,
        },
        "scope": {
            "claim": args.claim,
            "description": args.desc,
            "targets": list(args.targets),
        },
        "started_at": outcomes["start_time"],
        "ended_at": outcomes["end_time"],
        "resource_budget": _budget(args.budget_profile, args.budget_run_id),
        "shards": [{"id": args.shard_id, "status": "complete"}],
        "counts": counts,
        "denominator": "caught+missed",
        "rate": rate,
        "survivors": survivors,
        "kills": kills,
    }

    # Self-check before writing. Emitting a document our own contract rejects
    # would make this producer the very thing the contract exists to stop.
    findings = _ve.validate_doc(doc)
    if findings:
        print("mutation-gate: REFUSING to emit; the document violates its own contract:", file=sys.stderr)
        for f in findings:
            print(f"  {f}", file=sys.stderr)
        return 1

    text = json.dumps(doc, indent=2) + "\n"
    if args.output:
        Path(args.output).write_text(text)
        print(
            f"mutation-gate: wrote {args.output} "
            f"({counts['caught']} caught / {counts['missed']} missed, rate {rate:.4f}, "
            f"{len(kills)} witnessed kills)"
        )
    else:
        print(text, end="")
    return 0


def _check_findings(doc: dict, expected_sha: str | None = None) -> list:
    findings = _ve.validate_doc(doc)
    if not findings and expected_sha is not None and doc["source"]["sha"] != expected_sha:
        findings.append(
            _ve.Finding(
                "E_STALE_SHA",
                "/source/sha",
                f"mutation evidence is for {doc['source']['sha']} but expected "
                f"{expected_sha}",
            )
        )
    return findings


def check(path: str, expected_revision: str | None = None) -> int:
    doc = json.loads(Path(path).read_text())
    expected_sha = _commit_sha(expected_revision) if expected_revision is not None else None
    findings = _check_findings(doc, expected_sha)
    if findings:
        print(f"mutation-gate: {path} REJECTED", file=sys.stderr)
        for f in findings:
            print(f"  {f}", file=sys.stderr)
        return 1
    c = doc["counts"]
    print(
        f"mutation-gate: {path} OK — {c['caught']} caught, {c['missed']} missed, "
        f"{c['timeout']} timeout, {c['unviable']} unviable; rate {doc['rate']} "
        f"recomputes from {doc['denominator']}; {len(doc['kills'])} kills witnessed, "
        f"{len(doc['survivors'])} survivors classified; scope claim={doc['scope']['claim']}"
    )
    return 0


def self_test() -> int:
    doc = json.loads(
        (ROOT / "schemas" / "evidence" / "fixtures" / "valid" / "mutation-result.json").read_text()
    )
    assert _check_findings(doc, doc["source"]["sha"]) == []
    findings = _check_findings(doc, "0" * 40)
    assert [(finding.code, finding.path) for finding in findings] == [
        ("E_STALE_SHA", "/source/sha")
    ]
    left = {
        "file": "crates/one/shared.rs",
        "span": {"start": {"line": 3, "column": 7}},
        "replacement": "false",
    }
    right = {**left, "file": "crates/two/shared.rs"}
    assert _mutant_id(left) != _mutant_id(right)
    print("mutation-gate: self-test OK (exact source SHA is enforced when requested)")
    return 0


def main() -> int:
    p = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    p.add_argument("--check", metavar="FILE", help="validate an existing mutation-result/v1")
    p.add_argument(
        "--expected-sha",
        metavar="REV",
        help="require --check evidence to name this commit (for example HEAD)",
    )
    p.add_argument("--self-test", action="store_true", help="run offline source-SHA checks")
    p.add_argument("--emit", action="store_true", help="convert a cargo-mutants run")
    p.add_argument("--mutants-out", help="cargo-mutants --output dir (or its mutants.out)")
    p.add_argument("--targets", nargs="+", default=[], help="files this run mutated")
    p.add_argument("--claim", choices=["scoped", "workspace"], default="scoped")
    p.add_argument("--desc", default="", help="what this run covers, in one sentence")
    p.add_argument("--generated-at", dest="generated_at", required=False)
    p.add_argument("--budget-profile", default="mutants")
    p.add_argument("--budget-run-id", default="mutants")
    p.add_argument("--shard-id", default="shard-1of1")
    p.add_argument("-o", "--output")
    args = p.parse_args()

    if args.self_test:
        if args.check or args.emit:
            p.error("--self-test cannot be combined with --check or --emit")
        return self_test()
    if args.check:
        return check(args.check, args.expected_sha)
    if args.expected_sha:
        p.error("--expected-sha requires --check FILE")
    if not args.emit or not args.mutants_out:
        p.error("use --check FILE, or --emit --mutants-out DIR")

    # A producer must record the tree it actually mutated, not caller-supplied
    # metadata. The whole-tree reading is strict because another pane's WIP
    # changes what cargo-mutants actually compiled.
    args.sha = _commit_sha("HEAD")
    args.branch = _git("rev-parse", "--abbrev-ref", "HEAD") or "HEAD"
    args.tree_clean = not _git("status", "--porcelain")
    if not args.generated_at:
        p.error("--generated-at is required (RFC 3339 UTC); pass the run's real time")
    if not args.desc:
        p.error("--desc is required: a scope nobody stated is a scope nobody can check")

    return emit(args)


if __name__ == "__main__":
    sys.exit(main())
