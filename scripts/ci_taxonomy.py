#!/usr/bin/env python3
"""Derive a checked, machine-readable CI job taxonomy from the workflow YAML.

Why this exists
---------------
`gh run list` reports RUN-level conclusions, and a run is reported "success"
even when one of its jobs is red -- because a `continue-on-error: true` job
never fails its run. This repo has two such jobs, and one of them
("fuzz targets compile/smoke (nightly)") sat red for days while every
`gh run list` row said success.

So "is CI green?" cannot be answered from run conclusions. It has to be answered
per check-run, against a list that says which jobs were ever supposed to gate.
This derives that list from the workflow YAML -- nobody maintains it by hand --
and `--check` fails when the committed list drifts from the YAML, so a new job
cannot appear without being classified.

The rule this enforces: **never call CI green while a required job is not a
completed success.** Non-terminal is not success. Missing is not success.
Advisory failures are reported, separately, and never gate.

Tiers
-----
  advisory   continue-on-error: true -- runs on a gating trigger, never blocks.
  required   runs on push-to-branch or pull_request and is not advisory.
             These must be a completed success for a SHA to be releasable.
  scheduled  only fires on a timer (soak, canary, live, tsan).
  release    only fires on a release tag.
  manual     only fires via workflow_dispatch.
"""

from __future__ import annotations

import argparse
import itertools
import json
import re
import subprocess
import sys
from pathlib import Path

import yaml

ROOT = Path(__file__).resolve().parent.parent
WORKFLOW_DIR = ROOT / ".github" / "workflows"
TAXONOMY_PATH = ROOT / "docs" / "ci_taxonomy.json"
SCHEMA = "ci-taxonomy/v1"

# GitHub spells a repository slug the same way in every API call it needs.
REPO_SLUG = "MuhDur/rust-oracledb"

# In YAML 1.1 -- which PyYAML implements -- the bare word `on` is a BOOLEAN.
# GitHub's `on:` trigger block therefore parses to the key True, not "on".
# Every naive workflow parser reads zero triggers and silently classifies every
# job as manual. Resolve it explicitly rather than by luck.
ON_KEYS = (True, "on")

_EXPR_RE = re.compile(r"\$\{\{\s*inputs\.([A-Za-z0-9_-]+)\s*\}\}")
_MATRIX_RE = re.compile(r"\$\{\{\s*matrix\.([A-Za-z0-9_.-]+)\s*\}\}")
_ANY_EXPR_RE = re.compile(r"\$\{\{.*?\}\}")


def _matrix_combos(job: dict) -> list:
    """Expand strategy.matrix into the concrete combinations GitHub will run.

    A matrix job publishes one check-run per combination, each with the job's
    name template expanded against that combination -- so a taxonomy that stops
    at the template names a job that does not exist.
    """
    matrix = (job.get("strategy") or {}).get("matrix")
    if not isinstance(matrix, dict):
        return [{}]

    keys = [k for k in matrix if k not in ("include", "exclude")]
    value_lists = []
    for key in keys:
        values = matrix[key]
        if not isinstance(values, list):
            return [{}]
        value_lists.append(values)

    combos = []
    for combo in itertools.product(*value_lists) if value_lists else [()]:
        combos.append(dict(zip(keys, combo)))
    return combos or [{}]


def _expand_matrix(text: str, combo: dict) -> str:
    def replace(match: re.Match) -> str:
        parts = match.group(1).split(".")
        value = combo
        for part in parts:
            if isinstance(value, dict) and part in value:
                value = value[part]
            else:
                return match.group(0)
        return str(value)

    return _MATRIX_RE.sub(replace, text)


def _assert_resolved(name: str, where: str) -> str:
    """A check-run name containing an unexpanded expression can never match a
    real check-run, so it would silently classify a job as permanently missing.
    Refuse to emit one."""
    if _ANY_EXPR_RE.search(name):
        raise SystemExit(
            f"ci-taxonomy: cannot resolve check-run name {name!r} in {where}. "
            "An unexpanded expression would never match a real check-run. Teach "
            "the deriver this expression rather than shipping a name that "
            "cannot match."
        )
    return name


def _triggers(workflow: dict) -> dict:
    for key in ON_KEYS:
        if key in workflow:
            block = workflow[key]
            if isinstance(block, str):
                return {block: {}}
            if isinstance(block, list):
                return {name: {} for name in block}
            if isinstance(block, dict):
                return block
    return {}


def _expand(text: str, inputs: dict) -> str:
    """Expand ${{ inputs.x }} using the caller's `with:` block.

    A reusable workflow's job name is templated, and the check-run GitHub
    publishes carries the EXPANDED name. Without this, derived names never match
    reality and --status can never find a job.
    """
    return _EXPR_RE.sub(lambda m: str(inputs.get(m.group(1), m.group(0))), text)


def _tier(job: dict, triggers: dict) -> str:
    if job.get("continue-on-error") is True:
        return "advisory"

    push = triggers.get("push") or {}
    push = push if isinstance(push, dict) else {}
    push_branches = "branches" in push
    push_tags = "tags" in push

    if "pull_request" in triggers or push_branches:
        return "required"
    if push_tags:
        return "release"
    if "schedule" in triggers:
        return "scheduled"
    if "workflow_dispatch" in triggers:
        return "manual"
    return "manual"


def derive() -> dict:
    jobs = []
    for path in sorted(WORKFLOW_DIR.glob("*.yml")):
        workflow = yaml.safe_load(path.read_text())
        triggers = _triggers(workflow)

        # A reusable workflow (workflow_call) publishes no check-runs of its
        # own; it is surfaced through its callers, so skip it here.
        if "workflow_call" in triggers and len(triggers) == 1:
            continue

        trigger_names = sorted(str(t) for t in triggers)
        push = triggers.get("push") or {}
        paths = push.get("paths") if isinstance(push, dict) else None

        for job_id, job in (workflow.get("jobs") or {}).items():
            if not isinstance(job, dict):
                continue

            called = job.get("uses")
            if isinstance(called, str) and called.startswith("./.github/workflows/"):
                # Nested job: GitHub names the check-run
                # "<caller job id> / <called job name>", with the called job's
                # name template expanded from the caller's `with:` inputs.
                inputs = job.get("with") or {}
                # removeprefix, not lstrip: lstrip takes a CHARACTER SET, so
                # lstrip("./") eats the dot of ".github" too.
                sub_path = ROOT / called.removeprefix("./")
                sub = yaml.safe_load(sub_path.read_text())
                for sub_id, sub_job in (sub.get("jobs") or {}).items():
                    if not isinstance(sub_job, dict):
                        continue
                    sub_name_template = _expand(str(sub_job.get("name", sub_id)), inputs)
                    # Reusable matrix jobs still publish one check-run per
                    # concrete combination. Expand the called workflow's
                    # matrix after substituting the caller inputs so no
                    # impossible `${{ matrix.* }}` template reaches the
                    # checked taxonomy.
                    for combo in _matrix_combos(sub_job):
                        sub_name = _expand_matrix(sub_name_template, combo)
                        jobs.append(
                            {
                                "check_name": _assert_resolved(
                                    f"{job_id} / {sub_name}", f"{path.name}:{job_id}"
                                ),
                                "tier": _tier(sub_job, triggers),
                                "workflow": str(workflow.get("name", path.stem)),
                                "workflow_file": path.name,
                                "job_id": job_id,
                                "reusable_from": sub_path.name,
                                "triggers": trigger_names,
                                "path_filtered": bool(paths),
                            }
                        )
                continue

            # One check-run per matrix combination; [{}] when there is no matrix.
            for combo in _matrix_combos(job):
                name = _expand_matrix(str(job.get("name", job_id)), combo)
                jobs.append(
                    {
                        "check_name": _assert_resolved(name, f"{path.name}:{job_id}"),
                        "tier": _tier(job, triggers),
                        "workflow": str(workflow.get("name", path.stem)),
                        "workflow_file": path.name,
                        "job_id": job_id,
                        "triggers": trigger_names,
                        "path_filtered": bool(paths),
                    }
                )

    jobs.sort(key=lambda j: (j["workflow_file"], j["check_name"]))

    # `workflows` and `groups` are derived views over `jobs`, never a second
    # source of truth: jobs[] stays the one place a tier is recorded, so a view
    # cannot disagree with it.
    workflows = {}
    for job in jobs:
        entry = workflows.setdefault(
            job["workflow_file"],
            {"name": job["workflow"], "triggers": job["triggers"], "jobs": []},
        )
        entry["jobs"].append(job["check_name"])

    groups: dict = {}
    for job in jobs:
        groups.setdefault(job["tier"], []).append(job["check_name"])

    return {
        "schema": SCHEMA,
        "repo": "rust-oracledb",
        "note": (
            "Generated by scripts/ci_taxonomy.py from .github/workflows/*.yml. "
            "Do not hand-edit: run scripts/ci_taxonomy.py --write. A required "
            "job must be a completed success for a SHA to be releasable; "
            "advisory jobs never gate. `workflows` and `groups` are derived "
            "views over `jobs`, which is the single source of truth for tiers."
        ),
        "jobs": jobs,
        "workflows": dict(sorted(workflows.items())),
        "groups": {tier: sorted(names) for tier, names in sorted(groups.items())},
    }


def _fetch_check_runs(sha: str) -> list:
    """Check-RUNS, deliberately: run conclusions hide continue-on-error reds."""
    out = subprocess.run(
        [
            "gh",
            "api",
            "--paginate",
            f"repos/{REPO_SLUG}/commits/{sha}/check-runs?per_page=100",
            "--jq",
            ".check_runs[] | {name, status, conclusion}",
        ],
        capture_output=True,
        text=True,
        check=False,
    )
    if out.returncode != 0:
        print(f"ci-taxonomy: gh api failed: {out.stderr.strip()}", file=sys.stderr)
        raise SystemExit(2)
    return [json.loads(line) for line in out.stdout.splitlines() if line.strip()]


def status(sha: str) -> dict:
    taxonomy = derive()
    by_name = {j["check_name"]: j for j in taxonomy["jobs"]}
    runs = {r["name"]: r for r in _fetch_check_runs(sha)}

    jobs, unknown = [], []
    for name, run in sorted(runs.items()):
        known = by_name.get(name)
        if known is None:
            unknown.append(name)
            continue
        jobs.append(
            {
                "name": name,
                "tier": known["tier"],
                "status": run["status"],
                "conclusion": run["conclusion"],
            }
        )

    seen = set(runs)
    absent = [
        j
        for j in taxonomy["jobs"]
        if j["tier"] == "required" and j["check_name"] not in seen
    ]
    # A required job can be absent for two very different reasons, and collapsing
    # them would either cry wolf on every docs commit or hide a job that silently
    # stopped running. Both are still not-green -- absence is not success -- but
    # the caller is told which it is.
    #
    #   path_filtered : the workflow has a `paths:` filter, so this commit may
    #                   legitimately not have triggered it. It also means this
    #                   SHA carries no evidence from that job, which is exactly
    #                   why a release needs its artifact recorded AT the release
    #                   SHA (AGENTS.md).
    #   unexpected    : nothing filtered it. It should have run and did not.
    missing_filtered = sorted(j["check_name"] for j in absent if j["path_filtered"])
    missing_unexpected = sorted(j["check_name"] for j in absent if not j["path_filtered"])
    missing = missing_filtered + missing_unexpected

    not_green = [
        j
        for j in jobs
        if j["tier"] == "required"
        and (j["status"] != "completed" or j["conclusion"] != "success")
    ]
    # "not green", not "failures": an advisory job that is hung or queued has not
    # failed, but it has not passed either, and calling the set "failures" would
    # silently drop it. Advisory jobs never gate -- they are reported so a red
    # one cannot sit unnoticed behind a "success" run conclusion, which is the
    # incident this whole command exists for.
    advisory_not_green = [
        j
        for j in jobs
        if j["tier"] == "advisory"
        and not (j["status"] == "completed" and j["conclusion"] == "success")
    ]

    # Fail closed. A required job that never ran has not passed, and this repo's
    # own release rule says a release cannot ship without its evidence recorded
    # AT the release SHA. Absence is not success.
    ci_green = not not_green and not missing and not unknown

    return {
        "schema": SCHEMA,
        "sha": sha,
        "ci_green": ci_green,
        "jobs": jobs,
        "required_not_green": [j["name"] for j in not_green],
        "required_missing_path_filtered": missing_filtered,
        "required_missing_unexpected": missing_unexpected,
        "advisory_not_green": [j["name"] for j in advisory_not_green],
        "unknown_jobs": unknown,
    }


def check() -> int:
    derived = derive()
    if not TAXONOMY_PATH.exists():
        print(
            f"ci-taxonomy: {TAXONOMY_PATH.relative_to(ROOT)} is missing; run "
            "scripts/ci_taxonomy.py --write",
            file=sys.stderr,
        )
        return 1

    committed = json.loads(TAXONOMY_PATH.read_text())
    if committed == derived:
        counts: dict = {}
        for job in derived["jobs"]:
            counts[job["tier"]] = counts.get(job["tier"], 0) + 1
        summary = ", ".join(f"{n} {t}" for t, n in sorted(counts.items()))
        print(f"ci-taxonomy: {len(derived['jobs'])} jobs match the workflow YAML ({summary})")
        return 0

    print(
        "ci-taxonomy: committed taxonomy has DRIFTED from .github/workflows/.\n"
        "A CI job changed tier, appeared, or disappeared. Review the diff, then\n"
        "run scripts/ci_taxonomy.py --write and commit it.",
        file=sys.stderr,
    )
    old = {j["check_name"]: j["tier"] for j in committed.get("jobs", [])}
    new = {j["check_name"]: j["tier"] for j in derived["jobs"]}
    for name in sorted(set(old) | set(new)):
        if old.get(name) != new.get(name):
            print(
                f"  {name}: {old.get(name, '(absent)')} -> {new.get(name, '(absent)')}",
                file=sys.stderr,
            )
    return 1


def verify_names(sha: str) -> int:
    """Every real check-run must be classified by the taxonomy.

    One-directional on purpose: a derived job with no check-run is legitimate (a
    path filter or schedule meant it never ran), but a check-run the taxonomy has
    never heard of means a job is gating, or failing to gate, unclassified.
    """
    taxonomy = derive()
    known = {j["check_name"] for j in taxonomy["jobs"]}
    actual = {r["name"] for r in _fetch_check_runs(sha)}
    unknown = sorted(actual - known)

    for name in sorted(actual):
        print(f"  {'OK      ' if name in known else 'UNKNOWN '} {name}")
    print()
    if unknown:
        print(
            f"ci-taxonomy: {len(unknown)} check-run(s) at {sha[:12]} are not in the "
            "taxonomy; the deriver is wrong or a job is unclassified",
            file=sys.stderr,
        )
        return 1
    print(f"ci-taxonomy: all {len(actual)} check-runs at {sha[:12]} are classified")
    return 0


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("--list", action="store_true", help="print the derived taxonomy")
    group.add_argument("--write", action="store_true", help="regenerate docs/ci_taxonomy.json")
    group.add_argument("--check", action="store_true", help="fail if the committed taxonomy drifted")
    group.add_argument("--status", metavar="SHA", help="classify a SHA's check-runs; non-zero unless green")
    group.add_argument("--verify-names", metavar="SHA", help="assert every real check-run is classified")
    args = parser.parse_args()

    if args.list:
        print(json.dumps(derive(), indent=2))
        return 0

    if args.write:
        TAXONOMY_PATH.write_text(json.dumps(derive(), indent=2) + "\n")
        print(f"ci-taxonomy: wrote {TAXONOMY_PATH.relative_to(ROOT)}")
        return 0

    if args.check:
        return check()

    if args.verify_names:
        return verify_names(args.verify_names)

    result = status(args.status)
    print(json.dumps(result, indent=2))
    # Exit status mirrors the verdict so a caller cannot read "green" off a
    # zero exit without the JSON agreeing.
    return 0 if result["ci_green"] else 1


if __name__ == "__main__":
    sys.exit(main())
