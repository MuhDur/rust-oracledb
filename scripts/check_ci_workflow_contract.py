#!/usr/bin/env python3
"""Fail closed when the release-qualification scheduling contract drifts."""

from __future__ import annotations

import sys
from pathlib import Path
from typing import Any

import yaml

ROOT = Path(__file__).resolve().parent.parent


class ContractError(ValueError):
    """A workflow no longer preserves a load-bearing CI invariant."""


def require(condition: bool, message: str) -> None:
    if not condition:
        raise ContractError(message)


def load_workflow(name: str) -> dict[str, Any]:
    path = ROOT / ".github" / "workflows" / name
    try:
        value = yaml.safe_load(path.read_text())
    except (OSError, yaml.YAMLError) as exc:
        raise ContractError(f"cannot parse {path.relative_to(ROOT)}: {exc}") from exc
    require(isinstance(value, dict), f"{name}: root must be an object")
    require(isinstance(value.get("jobs"), dict), f"{name}: jobs must be an object")
    return value


def steps(job: dict[str, Any], workflow: str, job_name: str) -> list[dict[str, Any]]:
    value = job.get("steps")
    require(isinstance(value, list), f"{workflow}:{job_name}: steps must be an array")
    require(all(isinstance(step, dict) for step in value), f"{workflow}:{job_name}: malformed step")
    return value


def named_step(items: list[dict[str, Any]], name: str, where: str) -> dict[str, Any]:
    matches = [step for step in items if step.get("name") == name]
    require(len(matches) == 1, f"{where}: expected exactly one step named {name!r}")
    return matches[0]


def check_release_qualification() -> None:
    workflow = load_workflow("release-qualification.yml")
    concurrency = workflow.get("concurrency")
    require(isinstance(concurrency, dict), "release qualification: concurrency missing")
    require(
        "protect_for_tag" in str(concurrency.get("group"))
        and "github.run_id" in str(concurrency.get("group")),
        "release qualification: imminent-tag runs are not isolated from superseding candidates",
    )
    require(
        "protect_for_tag" in str(concurrency.get("cancel-in-progress")),
        "release qualification: ordinary candidates are not single-flight",
    )

    jobs = workflow["jobs"]
    independent = ("release-qualification", "emit-required-proof", "emit-version-matrix")
    for job_name in independent:
        require(job_name in jobs, f"release qualification: missing {job_name}")
        require(
            "needs" not in jobs[job_name],
            f"release qualification: {job_name} must start independently",
        )

    candidate = "${{ inputs.candidate_sha }}"
    for job_name in ("emit-required-proof", "emit-version-matrix"):
        checkout = [
            step
            for step in steps(jobs[job_name], "release-qualification.yml", job_name)
            if str(step.get("uses", "")).startswith("actions/checkout@")
        ]
        require(len(checkout) == 1, f"release qualification: {job_name} checkout drift")
        require(
            checkout[0].get("with", {}).get("ref") == candidate,
            f"release qualification: {job_name} does not check out the exact candidate",
        )

    reporter = jobs.get("report-release-qualification-failure", {})
    require(
        set(reporter.get("needs", [])) == set(independent),
        "release qualification: failure reporter does not observe every hard lane",
    )
    reporter_if = str(reporter.get("if", ""))
    for job_name in independent:
        require(job_name in reporter_if, f"release qualification: reporter omits {job_name}")


def check_quality_tools_and_disk() -> None:
    workflow = load_workflow("_quality.yml")
    quality = workflow["jobs"].get("quality")
    require(isinstance(quality, dict), "quality workflow: quality job missing")
    items = steps(quality, "_quality.yml", "quality")
    expected = {
        "Install cargo-hack": "cargo-hack@${{ env.CARGO_HACK_VERSION }}",
        "Install cargo-public-api": "cargo-public-api@${{ env.CARGO_PUBLIC_API_VERSION }}",
        "Install cargo-semver-checks": "cargo-semver-checks@${{ env.CARGO_SEMVER_CHECKS_VERSION }}",
    }
    for name, tool in expected.items():
        step = named_step(items, name, "quality workflow")
        require(
            str(step.get("uses", "")).startswith("taiki-e/install-action@"),
            f"quality workflow: {name} regressed to a cold source install",
        )
        require(step.get("with", {}).get("tool") == tool, f"quality workflow: {name} version drift")
    require(
        not any("cargo install cargo-" in str(step.get("run", "")) for step in items),
        "quality workflow: cold cargo source install reintroduced",
    )
    preflight = named_step(items, "Preflight powerset disk and write/fsync/read canary", "quality workflow")
    feature = named_step(items, "Feature profile matrix", "quality workflow")
    require(
        items.index(preflight) < items.index(feature)
        and "scripts/resource_budget.sh" in str(preflight.get("run", "")),
        "quality workflow: powerset disk preflight must run before compilation",
    )


def check_live_state_machine() -> None:
    workflow = load_workflow("live.yml")
    require(
        workflow.get("permissions", {}).get("actions") == "read",
        "live workflow: prior state may only be read through Actions artifacts",
    )
    live = workflow["jobs"].get("live")
    require(isinstance(live, dict), "live workflow: live job missing")
    items = steps(live, "live.yml", "live")
    test_step = named_step(items, "Driver live tests (serial, ignored)", "live workflow")
    require(test_step.get("continue-on-error") is True, "live workflow: observation cannot be classified")
    advance = named_step(items, "Advance advisory-to-blocking state machine", "live workflow")
    require(
        "python3 scripts/live_gate_state.py transition" in str(advance.get("run", "")),
        "live workflow: deterministic state transition missing",
    )
    enforce = named_step(items, "Enforce automatically re-armed Live gate", "live workflow")
    require(
        "live_gate_state.py enforce" in str(enforce.get("run", ""))
        and enforce.get("continue-on-error") is not True,
        "live workflow: re-armed gate is not hard-blocking",
    )


def main() -> int:
    try:
        check_release_qualification()
        check_quality_tools_and_disk()
        check_live_state_machine()
    except ContractError as exc:
        print(f"ci-workflow-contract: FAIL: {exc}", file=sys.stderr)
        return 1
    print("ci-workflow-contract: OK (parallel exact-SHA proof, pinned tools, bounded Live state)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
