#!/usr/bin/env python3
"""Run the local Required graph and emit a self-validating required-proof/v2.

The graph is derived from `.github/workflows/_quality.yml`, not copied here.
Every workflow step and condition must be classified below.  An unfamiliar step,
action, or condition is a hard error: silently omitting a newly required gate
would make the proof less strict exactly when CI became stricter.
"""

from __future__ import annotations

import argparse
import datetime as dt
import hashlib
import json
import os
import re
import subprocess
import sys
from dataclasses import dataclass
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
QUALITY_WORKFLOW = ROOT / ".github/workflows/_quality.yml"
RELEASE_QUALIFICATION_WORKFLOW = ROOT / ".github/workflows/release-qualification.yml"
RESOURCE_BUDGET = ROOT / "scripts/resource_budget.sh"
VALIDATOR = ROOT / "scripts/validate_evidence.py"

PREP_CONTINUE = "${{ inputs.mode == 'prep' }}"
PREP_ALWAYS = "${{ always() && inputs.mode == 'prep' }}"
STRICT_ONLY = "${{ inputs.mode == 'strict' }}"
FUZZ_STRATEGY = (
    "fail-fast: false",
    "matrix:",
    "  shard: [0, 1, 2, 3]",
)
META_STEPS = {
    "Aggregate quality results",
    "Aggregate prep outcomes",
    "Download prep outcomes",
    "Validate inputs",
    "Select budget",
    "Forced evidence failure",
    "Record prep outcomes",
    "Upload prep outcomes",
    "Show selected target",
}
ALLOWED_ACTION_PREFIXES = (
    "actions/checkout@",
    "dtolnay/rust-toolchain@",
    "Swatinem/rust-cache@",
    "taiki-e/install-action@",
    "actions/download-artifact@",
    "actions/upload-artifact@",
)
CONDITIONS = {
    "": True,
    "${{ inputs.force_failure }}": False,
    "${{ inputs.profile != 'canary' }}": True,
    "${{ inputs.profile == 'release-qualification' }}": False,
    "${{ inputs.profile == 'soak' || inputs.profile == 'release-qualification' }}": False,
    "${{ steps.budget.outputs.package == 'true' }}": False,
    "${{ steps.budget.outputs.fuzz_seconds != '0' }}": False,
    PREP_ALWAYS: False,
    PREP_CONTINUE: False,
    STRICT_ONLY: True,
}
FANOUT_JOBS = (
    "validate",
    "core",
    "contracts",
    "features",
    "release-surface",
    "musl",
    "perf",
    "fuzz",
)
AGGREGATE_JOB = "quality"
AGGREGATE_CONDITION = "${{ always() }}"
EXPECTED_QUALITY_COMMANDS = (
    "Format",
    "Clippy",
    "Test workspace",
    "Test cassette replay",
    "Test docs",
    "Build docs",
    "Install stable Rust",
    "Stable protocol tests",
    "Install baseline tools",
    "Verify checked entry traces and release/evidence contracts",
    "Install cargo-public-api",
    "Baseline drift check",
    "API ledger coverage",
    "Single public path per type",
    "Async/blocking coverage",
    "Connect-trace secret exclusion",
    "Confidentiality secret scan (C4)",
    "Pin clean-room reference checkout",
    "Reference version-gate parity coverage",
    "Reference version-gate boundary-test coverage",
    "Provenance artifacts drift check (SBOM + dep/action inventory)",
    "Install cargo-hack",
    "Feature profile matrix",
    "Validate release metadata",
    "Inter-crate version-pin guard test",
    "Standalone packaged-crate build",
    "Install cargo-semver-checks",
    "SemVer advisory checks",
    "Supply-chain checks",
    "Package crates",
    "Musl binary size gate",
    "Deterministic performance regression gate",
    "Fuzz targets (bounded shard)",
)


class ContractError(RuntimeError):
    """The workflow cannot be translated without weakening the local proof."""


@dataclass
class Step:
    name: str
    job: str = ""
    condition: str = ""
    uses: str | None = None
    run: str | None = None
    shell: str | None = None
    working_directory: str | None = None
    environment: dict[str, str] | None = None
    identifier: str = ""
    continue_on_error: str = ""


@dataclass
class Job:
    identifier: str
    name: str = ""
    condition: str = ""
    needs: tuple[str, ...] = ()
    step_count: int = 0
    strategy: tuple[str, ...] = ()
    environment: dict[str, str] | None = None


@dataclass
class QualityWorkflow:
    jobs: dict[str, Job]
    steps: list[Step]
    environment: dict[str, str]


def utc_now() -> str:
    return dt.datetime.now(dt.timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def command_output(argv: list[str]) -> str:
    try:
        return subprocess.check_output(argv, cwd=ROOT, text=True, stderr=subprocess.STDOUT).strip()
    except (OSError, subprocess.CalledProcessError):
        return "unavailable"


def parse_needs(value: str, job: str) -> tuple[str, ...]:
    """Parse the scalar/inline-list `needs` forms used by the quality graph."""

    if not value:
        raise ContractError(f"job {job!r}: multiline needs is unsupported")
    if value.startswith("["):
        if not value.endswith("]"):
            raise ContractError(f"job {job!r}: malformed inline needs list")
        values = tuple(item.strip() for item in value[1:-1].split(",") if item.strip())
    else:
        values = (value,)
    if not values or len(values) != len(set(values)):
        raise ContractError(f"job {job!r}: needs must be a non-empty unique list")
    return values


def parse_environment_value(value: str, scope: str) -> str:
    """Parse the scalar subset accepted for an environment-variable value."""

    if not value:
        return ""
    if value[0] == '"':
        try:
            decoded = json.loads(value)
        except json.JSONDecodeError as exc:
            raise ContractError(f"{scope}: invalid double-quoted environment value") from exc
        if not isinstance(decoded, str):
            raise ContractError(f"{scope}: environment value must be a string")
        return decoded
    if value[0] == "'":
        if len(value) < 2 or not value.endswith("'"):
            raise ContractError(f"{scope}: invalid single-quoted environment value")
        return value[1:-1].replace("''", "'")
    if value.startswith(("[", "{")):
        raise ContractError(f"{scope}: inline environment collections are unsupported")
    return value


def parse_environment_entry(raw: str, indent: int, scope: str) -> tuple[str, str] | None:
    """Parse one indented ``KEY: value`` mapping entry, failing closed on drift."""

    entry = re.match(rf"^ {{{indent}}}(?P<key>[A-Za-z_][A-Za-z0-9_]*):(?P<value>.*)$", raw)
    if entry is None:
        return None
    key = entry.group("key")
    return key, parse_environment_value(entry.group("value").strip(), f"{scope}.{key}")


def merged_environment(workflow: "QualityWorkflow", step: Step) -> dict[str, str]:
    """Apply GitHub Actions environment precedence for one quality command."""

    job = workflow.jobs[step.job]
    return {
        **workflow.environment,
        **(job.environment or {}),
        **(step.environment or {}),
    }


def validate_environment(environment: dict[str, str], command_name: str) -> None:
    """Reject values that the local runner cannot faithfully resolve."""

    unresolved = {
        key: value
        for key, value in environment.items()
        if "${{" in value or "}}" in value
    }
    if unresolved:
        names = ", ".join(sorted(unresolved))
        raise ContractError(f"{command_name}: unresolved GitHub expression in active environment: {names}")


def execution_environment(environment: dict[str, str], command_name: str) -> dict[str, str]:
    """Build the child environment without pretending to resolve GitHub contexts."""

    validate_environment(environment, command_name)
    return {**os.environ, **environment}


def parse_quality_workflow(text: str) -> QualityWorkflow:
    """Parse the deliberately small YAML subset used by `jobs.<id>.steps`.

    A dependency on a general YAML parser would make the proof depend on ambient
    Python packages. This parser accepts only the workflow shape we audit and
    fails closed on job-level delegation, matrices, demotion, or unfamiliar
    fields that could make the local Required projection diverge from CI.
    """

    jobs: dict[str, Job] = {}
    steps: list[Step] = []
    workflow_environment: dict[str, str] = {}
    current: Step | None = None
    current_job: Job | None = None
    collecting_run = False
    run_lines: list[str] = []
    in_jobs = False
    in_steps = False
    in_strategy = False
    strategy_lines: list[str] = []
    collecting_workflow_environment = False
    collecting_job_environment = False
    collecting_step_environment = False

    def finish_current() -> None:
        nonlocal current, collecting_run, run_lines
        if current is not None:
            if collecting_run:
                current.run = "\n".join(run_lines).strip()
            steps.append(current)
            jobs[current.job].step_count += 1
        current = None
        collecting_run = False
        run_lines = []

    def finish_strategy() -> None:
        nonlocal in_strategy, strategy_lines
        if in_strategy:
            if current_job is None:
                raise ContractError("strategy appeared outside a quality job")
            current_job.strategy = tuple(strategy_lines)
        in_strategy = False
        strategy_lines = []

    for raw in text.splitlines():
        if not in_jobs:
            if collecting_workflow_environment:
                entry = parse_environment_entry(raw, 2, "workflow env")
                if entry is not None:
                    key, value = entry
                    if key in workflow_environment:
                        raise ContractError(f"workflow env: duplicate variable {key!r}")
                    workflow_environment[key] = value
                    continue
                if raw.startswith("  ") and raw.strip() and not raw.lstrip().startswith("#"):
                    raise ContractError(f"workflow env: unclassified mapping line {raw.strip()!r}")
                collecting_workflow_environment = False
            if raw == "env:":
                collecting_workflow_environment = True
                continue
            if raw == "jobs:":
                in_jobs = True
                continue
            continue

        if collecting_job_environment:
            entry = parse_environment_entry(
                raw,
                6,
                f"job {current_job.identifier if current_job is not None else '<unknown>'} env",
            )
            if entry is not None:
                if current_job is None or current_job.environment is None:
                    raise ContractError("job environment appeared outside a quality job")
                key, value = entry
                if key in current_job.environment:
                    raise ContractError(
                        f"job {current_job.identifier!r}: duplicate environment variable {key!r}"
                    )
                current_job.environment[key] = value
                continue
            if raw.startswith("      ") and raw.strip() and not raw.lstrip().startswith("#"):
                raise ContractError(f"job environment: unclassified mapping line {raw.strip()!r}")
            collecting_job_environment = False

        if collecting_step_environment:
            entry = parse_environment_entry(
                raw,
                10,
                f"{current.name if current is not None else '<unknown>'} env",
            )
            if entry is not None:
                if current is None or current.environment is None:
                    raise ContractError("step environment appeared outside a quality step")
                key, value = entry
                if key in current.environment:
                    raise ContractError(f"{current.name}: duplicate environment variable {key!r}")
                current.environment[key] = value
                continue
            if raw.startswith("          ") and raw.strip() and not raw.lstrip().startswith("#"):
                raise ContractError(f"step environment: unclassified mapping line {raw.strip()!r}")
            collecting_step_environment = False

        if raw and not raw.startswith(" "):
            finish_current()
            finish_strategy()
            in_jobs = False
            in_steps = False
            current_job = None
            continue

        job_start = re.match(r"^  (?P<job>[A-Za-z0-9_-]+):\s*$", raw)
        if job_start:
            finish_current()
            finish_strategy()
            in_steps = False
            identifier = job_start.group("job")
            if identifier in jobs:
                raise ContractError(f"duplicate quality job {identifier!r}")
            current_job = Job(identifier=identifier)
            jobs[identifier] = current_job
            continue

        if in_strategy:
            if raw.startswith("      "):
                strategy_lines.append(raw[6:])
                continue
            finish_strategy()

        if in_steps and raw and not raw.startswith("      "):
            finish_current()
            in_steps = False

        if not in_steps:
            if not raw.strip() or raw.lstrip().startswith("#"):
                continue
            job_field = re.match(r"^    (?P<key>[A-Za-z0-9_-]+):(?P<value>.*)$", raw)
            if not job_field or current_job is None:
                raise ContractError(f"unclassified jobs block line {raw.strip()!r}")
            key = job_field.group("key")
            value = job_field.group("value").strip()
            if key == "name":
                current_job.name = value
            elif key == "needs":
                current_job.needs = parse_needs(value, current_job.identifier)
            elif key == "if":
                current_job.condition = value
            elif key == "steps":
                if value:
                    raise ContractError(
                        f"job {current_job.identifier!r}: unsupported steps shape {raw.strip()!r}"
                    )
                in_steps = True
            elif key == "strategy":
                if value:
                    raise ContractError(
                        f"job {current_job.identifier!r}: inline strategy is unsupported"
                    )
                in_strategy = True
            elif key == "env":
                if value:
                    raise ContractError(
                        f"job {current_job.identifier!r}: inline environment is unsupported"
                    )
                current_job.environment = {}
                collecting_job_environment = True
            elif key in ("runs-on", "timeout-minutes"):
                if not value:
                    raise ContractError(f"job {current_job.identifier!r}: empty {key!r}")
            else:
                raise ContractError(
                    f"job {current_job.identifier!r}: unsupported job-level field {key!r}"
                )
            continue

        start = re.match(r"^      - (?:(?:name: (?P<name>.+))|(?:uses: (?P<uses>.+)))$", raw)
        if start:
            finish_current()
            uses = start.group("uses")
            if current_job is None:
                raise ContractError("workflow step appeared outside a quality job")
            current = Step(
                name=start.group("name") or f"uses: {uses}",
                job=current_job.identifier,
                uses=uses,
            )
            continue
        if current is None:
            continue
        if collecting_run:
            if not raw:
                run_lines.append("")
                continue
            if raw.startswith("          "):
                run_lines.append(raw[10:])
                continue
            current.run = "\n".join(run_lines).strip()
            collecting_run = False
            run_lines = []

        field = re.match(
            r"^        (?P<key>name|if|uses|run|shell|working-directory|env|id|with|continue-on-error):(?P<value>.*)$",
            raw,
        )
        if not field:
            if raw.startswith("        ") and not raw.startswith("          "):
                raise ContractError(f"{current.name}: unclassified workflow field {raw.strip()!r}")
            continue
        key, value = field.group("key"), field.group("value").strip()
        if key == "name":
            current.name = value
        elif key == "if":
            current.condition = value
        elif key == "uses":
            current.uses = value
            if current.name.startswith("uses: "):
                current.name = f"uses: {value}"
        elif key == "run":
            if value == "|":
                collecting_run = True
            elif value:
                current.run = value
            else:
                raise ContractError(f"{current.name}: empty run field")
        elif key == "shell":
            current.shell = value
        elif key == "working-directory":
            current.working_directory = value
        elif key == "env":
            if value:
                raise ContractError(f"{current.name}: inline environment is unsupported")
            current.environment = {}
            collecting_step_environment = True
        elif key == "id":
            current.identifier = value
        elif key == "continue-on-error":
            current.continue_on_error = value
    finish_current()
    finish_strategy()
    if not steps:
        raise ContractError("could not find any jobs.<id>.steps in _quality.yml")
    return QualityWorkflow(jobs=jobs, steps=steps, environment=workflow_environment)


def parse_quality_steps(text: str) -> list[Step]:
    """Compatibility wrapper used by the focused parser self-tests."""

    return parse_quality_workflow(text).steps


def validate_quality_job_graph(workflow: QualityWorkflow) -> None:
    """Prove the fan-out and stable aggregate cannot omit or demote a shard."""

    expected_jobs = (*FANOUT_JOBS, AGGREGATE_JOB)
    actual_jobs = tuple(workflow.jobs)
    if actual_jobs != expected_jobs:
        raise ContractError(
            f"quality job order/identity drift: expected {expected_jobs!r}, got {actual_jobs!r}"
        )

    for identifier in FANOUT_JOBS:
        job = workflow.jobs[identifier]
        expected_name = f"quality-{identifier} (${{{{ inputs.profile }}}}/${{{{ inputs.budget }}}})"
        if identifier == "fuzz":
            expected_name += " [shard ${{ matrix.shard }}]"
        if job.name != expected_name:
            raise ContractError(
                f"job {identifier!r}: stable check name changed from {expected_name!r} to {job.name!r}"
            )
        expected_needs = () if identifier == "validate" else ("validate",)
        if job.needs != expected_needs:
            raise ContractError(
                f"job {identifier!r}: expected needs {expected_needs!r}, got {job.needs!r}"
            )
        if job.condition:
            raise ContractError(
                f"job {identifier!r}: job-level if could create a phantom/skipped check"
            )
        if job.step_count == 0:
            raise ContractError(f"job {identifier!r}: quality shard has no steps")
        expected_strategy = FUZZ_STRATEGY if identifier == "fuzz" else ()
        if job.strategy != expected_strategy:
            raise ContractError(
                f"job {identifier!r}: strategy must be {expected_strategy!r}, got {job.strategy!r}"
            )

    fanout_steps = [step for step in workflow.steps if step.job in FANOUT_JOBS]
    for step in fanout_steps:
        if step.name in ("Record prep outcomes", "Upload prep outcomes"):
            if step.condition != PREP_ALWAYS:
                raise ContractError(f"{step.job}/{step.name}: prep reporter condition drifted")
            if step.continue_on_error:
                raise ContractError(f"{step.job}/{step.name}: prep reporter must fail hard")
            continue
        if step.continue_on_error != PREP_CONTINUE:
            raise ContractError(
                f"{step.job}/{step.name}: every prep prerequisite and gate must continue on error"
            )

    gate_steps = [
        step
        for step in fanout_steps
        if step.uses is None and step.name not in META_STEPS
    ]
    actual_gate_names = tuple(step.name for step in gate_steps)
    if actual_gate_names != EXPECTED_QUALITY_COMMANDS:
        raise ContractError(
            "quality command inventory drift: "
            f"expected {EXPECTED_QUALITY_COMMANDS!r}, got {actual_gate_names!r}"
        )
    missing_gate_ids = [step.name for step in gate_steps if not step.identifier]
    if missing_gate_ids:
        raise ContractError(f"quality commands missing outcome ids: {missing_gate_ids!r}")

    for identifier in FANOUT_JOBS:
        job_steps = [step for step in fanout_steps if step.job == identifier]
        reporters = [step for step in job_steps if step.name == "Record prep outcomes"]
        uploads = [step for step in job_steps if step.name == "Upload prep outcomes"]
        if len(reporters) != 1 or len(uploads) != 1:
            raise ContractError(f"job {identifier!r}: prep outcomes require one recorder and one upload")
        expected_ids = tuple(
            step.identifier
            for step in gate_steps
            if step.job == identifier
        )
        if identifier == "validate":
            expected_ids = ("validate_inputs",)
        recorded_ids = tuple(
            re.findall(r"\$\{\{ steps\.([A-Za-z0-9_-]+)\.outcome \}\}", reporters[0].run or "")
        )
        if recorded_ids != expected_ids:
            raise ContractError(
                f"job {identifier!r}: prep outcome inventory must be {expected_ids!r}, "
                f"got {recorded_ids!r}"
            )

    fuzz_step = next(step for step in gate_steps if step.job == "fuzz")
    fuzz_run = fuzz_step.run or ""
    required_fuzz_tokens = (
        'shard_count=4',
        'shard_index="${{ matrix.shard }}"',
        "target_index % shard_count == shard_index",
        '-max_total_time="${{ steps.budget.outputs.fuzz_seconds }}"',
    )
    missing_fuzz_tokens = [token for token in required_fuzz_tokens if token not in fuzz_run]
    forbidden_fuzz_tokens = [
        token for token in ("GITHUB_RUN_NUMBER", "fuzz_target_cap", "target_cap") if token in fuzz_run
    ]
    if missing_fuzz_tokens or forbidden_fuzz_tokens:
        raise ContractError(
            "fuzz shard is not the exact four-way round-robin contract"
            f"; missing={missing_fuzz_tokens!r}; forbidden={forbidden_fuzz_tokens!r}"
        )

    aggregate = workflow.jobs[AGGREGATE_JOB]
    stable_name = "quality (${{ inputs.profile }}/${{ inputs.budget }})"
    if aggregate.name != stable_name:
        raise ContractError(
            f"aggregate job must preserve stable check name {stable_name!r}, got {aggregate.name!r}"
        )
    if aggregate.condition != AGGREGATE_CONDITION:
        raise ContractError(
            f"aggregate job must run under {AGGREGATE_CONDITION!r}, got {aggregate.condition!r}"
        )
    if aggregate.needs != FANOUT_JOBS:
        raise ContractError(
            f"aggregate job must need every quality shard {FANOUT_JOBS!r}, got {aggregate.needs!r}"
        )

    aggregate_steps = [step for step in workflow.steps if step.job == AGGREGATE_JOB]
    if tuple(step.name for step in aggregate_steps) != (
        "Aggregate quality results",
        "Download prep outcomes",
        "Aggregate prep outcomes",
    ):
        raise ContractError("aggregate job must contain the strict check and prep result collector")
    strict_step, download_step, prep_step = aggregate_steps
    if strict_step.condition != STRICT_ONLY:
        raise ContractError("strict aggregate result check must be strict-only")
    if download_step.condition != PREP_CONTINUE or prep_step.condition != PREP_CONTINUE:
        raise ContractError("prep aggregation steps must be prep-only")
    aggregate_run = strict_step.run or ""
    result_tokens = {
        "release-surface": "${{ needs['release-surface'].result }}",
        **{
            identifier: f"${{{{ needs.{identifier}.result }}}}"
            for identifier in FANOUT_JOBS
            if identifier != "release-surface"
        },
    }
    missing_tokens = sorted(
        identifier for identifier, token in result_tokens.items() if token not in aggregate_run
    )
    if missing_tokens or 'if [[ "$result" != "success" ]]' not in aggregate_run or "exit 1" not in aggregate_run:
        raise ContractError(
            "aggregate result step does not fail closed over every shard"
            + (f": missing {missing_tokens}" if missing_tokens else "")
        )

    prep_run = prep_step.run or ""
    prep_tokens = (
        "validate core contracts features release-surface musl perf",
        "fuzz-0 fuzz-1 fuzz-2 fuzz-3",
        "Prep diagnostics only; no qualification proof or release evidence was emitted.",
        'if [[ "$outcome" != "success" ]]',
        "exit 1",
    )
    missing_prep_tokens = [token for token in prep_tokens if token not in prep_run]
    if missing_prep_tokens:
        raise ContractError(f"prep aggregate is incomplete: missing {missing_prep_tokens!r}")


def validate_d4_workflow_text(text: str) -> None:
    """Pin D4 details that live inside action inputs rather than the YAML subset."""

    fuzz_match = re.search(r"^  fuzz:\n(?P<body>.*?)(?=^  quality:\n)", text, re.MULTILINE | re.DOTALL)
    if fuzz_match is None:
        raise ContractError("could not isolate the fuzz quality job")
    fuzz = fuzz_match.group("body")
    cache_contract = (
        "          workspaces: |\n"
        "            . -> target\n"
        "            crates/oracledb-protocol/fuzz -> target\n"
    )
    if cache_contract not in fuzz:
        raise ContractError("fuzz rust-cache must include the standalone fuzz workspace target")
    if 'echo "fuzz_seconds=120"' not in fuzz:
        raise ContractError("fuzz release budget must retain 120 seconds per target")
    if any(token in fuzz for token in ("fuzz_target_cap", "GITHUB_RUN_NUMBER")):
        raise ContractError("fuzz workflow retained the serial/rotating target-cap implementation")


def validate_release_qualification_workflow(text: str) -> None:
    """Prove prep executes the shared graph while strict alone may emit proof."""

    required = (
        "          - strict\n          - prep",
        "  release-qualification:\n"
        "    uses: ./.github/workflows/_quality.yml\n"
        "    with:\n"
        "      profile: release-qualification\n"
        "      budget: release\n"
        "      candidate_sha: ${{ inputs.candidate_sha }}\n"
        "      mode: ${{ inputs.mode }}",
        "if: ${{ inputs.mode == 'strict' && needs.release-qualification.result == 'success' }}",
        "if: ${{ always() && inputs.mode == 'strict'",
    )
    missing = [token for token in required if token not in text]
    if missing:
        raise ContractError(f"release qualification prep/strict contract drift: missing {missing!r}")
    forbidden = (
        "prepare-release-qualification:",
        "Warm dependency and tool caches",
        "if: ${{ inputs.mode == 'prep' }}\n    uses: ./.github/workflows/_quality.yml",
    )
    present = [token for token in forbidden if token in text]
    if present:
        raise ContractError(f"release qualification retained a warm-only or prep-only call: {present!r}")

    evidence_jobs: dict[str, str] = {}
    for job_name in ("emit-required-proof", "emit-version-matrix"):
        job_match = re.search(
            rf"^  {job_name}:\n(?P<body>.*?)(?=^  [A-Za-z0-9_-]+:\n|\Z)",
            text,
            re.MULTILINE | re.DOTALL,
        )
        if job_match is None:
            raise ContractError(f"missing strict evidence job {job_name!r}")
        job_body = job_match.group("body")
        if "inputs.mode == 'strict'" not in job_body:
            raise ContractError(f"{job_name}: evidence upload is reachable from prep mode")
        evidence_jobs[job_name] = job_body

    required_proof = evidence_jobs["emit-required-proof"]
    version_matrix = evidence_jobs["emit-version-matrix"]
    if required_proof.count("uses: actions/upload-artifact@") != 2:
        raise ContractError("required-proof job must upload the proof and diagnostic command logs")
    if version_matrix.count("uses: actions/upload-artifact@") != 1:
        raise ContractError("version-matrix job must upload exactly one exact-SHA evidence artifact")
    required_proof_tokens = (
        "        if: ${{ always() }}\n"
        "        with:\n"
        "          name: release-required-proof-${{ inputs.candidate_sha }}\n"
        "          path: ${{ runner.temp }}/required-proof-${{ inputs.candidate_sha }}.json\n"
        "          if-no-files-found: error",
        "      - name: Upload required-proof command logs\n"
        "        if: ${{ always() }}\n"
        "        uses: actions/upload-artifact@",
        "          name: release-required-logs-${{ inputs.candidate_sha }}\n"
        "          path: ${{ runner.temp }}/logs/${{ inputs.candidate_sha }}\n"
        "          if-no-files-found: warn",
    )
    missing = [token for token in required_proof_tokens if token not in required_proof]
    if missing:
        raise ContractError(f"required-proof diagnostic artifact contract drift: missing {missing!r}")


def effective_plan(workflow: Path = QUALITY_WORKFLOW) -> list[dict[str, object]]:
    entries: list[dict[str, object]] = []
    workflow_text = workflow.read_text()
    parsed = parse_quality_workflow(workflow_text)
    validate_quality_job_graph(parsed)
    validate_d4_workflow_text(workflow_text)
    validate_release_qualification_workflow(RELEASE_QUALIFICATION_WORKFLOW.read_text())
    for step in parsed.steps:
        if step.condition not in CONDITIONS:
            raise ContractError(f"{step.name}: unclassified condition {step.condition!r}")
        enabled = CONDITIONS[step.condition]
        record: dict[str, object] = {
            "job": step.job,
            "name": step.name,
            "condition": step.condition or None,
            "enabled_for_required": enabled,
        }
        if step.name in META_STEPS:
            record["classification"] = "ci-meta"
            record["reason"] = "github-expression-or-reporting-step-is-not-a-quality-gate"
        elif step.uses is not None:
            if not step.uses.startswith(ALLOWED_ACTION_PREFIXES):
                raise ContractError(f"{step.name}: unclassified setup action {step.uses!r}")
            record["classification"] = "setup-action"
            record["reason"] = "local-runner-uses-existing-checkout-toolchain-and-cache"
        elif step.run is None:
            if step.name not in META_STEPS:
                raise ContractError(f"{step.name}: neither a classified action nor a runnable command")
            record["classification"] = "ci-meta"
            record["reason"] = "github-expression-or-reporting-step-has-no-local-command"
        elif not enabled:
            record["classification"] = "profile-excluded"
            record["reason"] = "condition-is-false-for-profile-required"
        elif step.shell not in (None, "bash"):
            raise ContractError(f"{step.name}: unsupported active shell {step.shell!r}")
        elif step.working_directory is not None:
            raise ContractError(
                f"{step.name}: active working-directory {step.working_directory!r} is not yet replayable"
            )
        else:
            environment = merged_environment(parsed, step)
            validate_environment(environment, step.name)
            record["classification"] = "required-command"
            record["argv"] = ["bash", "-lc", step.run]
            # Execution-only metadata: it is deliberately omitted from the
            # required-proof command records so a workflow secret cannot become
            # release evidence merely because a command needs it at runtime.
            record["environment"] = environment
        entries.append(record)
    if not any(row["classification"] == "required-command" for row in entries):
        raise ContractError("required profile has no executable commands")
    return entries


def git_clean() -> bool:
    return subprocess.run(["git", "status", "--porcelain"], cwd=ROOT, text=True, capture_output=True).stdout == ""


def git_sha() -> str:
    return command_output(["git", "rev-parse", "HEAD"])


def default_output(sha: str) -> Path:
    return ROOT / "tests/artifacts/evidence/required" / f"required-proof-{sha}.json"


def command_id(name: str) -> str:
    """Return the stable evidence identifier for one workflow command."""

    return name.lower().replace(" ", "-").replace("/", "-")


def expected_command_records(plan: list[dict[str, object]]) -> dict[str, dict[str, object]]:
    """Describe the complete Required command graph this runner must record.

    The generic evidence schema intentionally cannot know every repository's
    workflow.  The runner can: it derived this exact plan from `_quality.yml`.
    Keep that local knowledge explicit so dropping a command record is a hard
    error instead of an apparently green but incomplete proof.
    """

    expected: dict[str, dict[str, object]] = {}
    for row in plan:
        if row["classification"] != "required-command":
            continue
        name = str(row["name"])
        identifier = command_id(name)
        argv = row["argv"]
        if identifier in expected:
            raise ContractError(f"duplicate required command identifier {identifier!r}")
        expected[identifier] = {"tier": "required", "argv": argv}

    expected["live-matrix"] = {
        "tier": "advisory",
        "argv": ["scripts/version_matrix.sh", "full", "all"],
    }
    return expected


def validate_command_coverage(commands: list[dict[str, object]], plan: list[dict[str, object]]) -> None:
    """Reject a proof record set that differs from the derived Required graph."""

    expected = expected_command_records(plan)
    actual: dict[str, dict[str, object]] = {}
    for command in commands:
        identifier = str(command["id"])
        if identifier in actual:
            raise ContractError(f"duplicate command record {identifier!r}")
        actual[identifier] = command

    missing = sorted(set(expected) - set(actual))
    if missing:
        raise ContractError(f"missing Required command records: {', '.join(missing)}")
    unexpected = sorted(set(actual) - set(expected))
    if unexpected:
        raise ContractError(f"unexpected command records: {', '.join(unexpected)}")

    for identifier, expected_record in expected.items():
        actual_record = actual[identifier]
        for field in ("tier", "argv"):
            if actual_record[field] != expected_record[field]:
                raise ContractError(
                    f"{identifier}: recorded {field} differs from the derived Required graph"
                )


def command_graph_commitment(plan: list[dict[str, object]]) -> dict[str, object]:
    """Commit the exact command IDs the independent validator must see."""

    command_ids = sorted(expected_command_records(plan))
    canonical = json.dumps(command_ids, ensure_ascii=False, separators=(",", ":"))
    return {
        "command_ids": command_ids,
        "sha256": hashlib.sha256(canonical.encode()).hexdigest(),
    }


def required_verdict(commands: list[dict[str, object]]) -> str:
    """A Required skip or failure is never promoted to a passing proof."""

    return "pass" if all(
        command["outcome"] == "pass"
        for command in commands
        if command["tier"] == "required"
    ) else "fail"


def source_ref(sha: str, branch: str) -> dict[str, object]:
    source: dict[str, object] = {"sha": sha, "tree_clean": True}
    if branch:
        source["branch"] = branch
    return source


def emitted_budget(run_id: str) -> dict[str, object]:
    raw = subprocess.check_output(
        [str(RESOURCE_BUDGET), "--profile", "test", "--run-id", run_id, "--emit-budget"],
        cwd=ROOT,
        text=True,
    )
    return json.loads(raw)


def run_required(plan: list[dict[str, object]], sha: str, output: Path, run_id: str) -> int:
    commands: list[dict[str, object]] = []
    logs = output.parent / "logs" / sha
    logs.mkdir(parents=True, exist_ok=True)

    for row in plan:
        if row["classification"] != "required-command":
            continue
        argv = row["argv"]
        assert isinstance(argv, list)
        environment = row.get("environment", {})
        if not isinstance(environment, dict) or not all(
            isinstance(key, str) and isinstance(value, str) for key, value in environment.items()
        ):
            raise ContractError(f"{row['name']}: invalid replay environment")
        started = utc_now()
        try:
            completed = subprocess.run(
                argv,
                cwd=ROOT,
                env=execution_environment(environment, str(row["name"])),
                text=True,
                capture_output=True,
                check=False,
            )
            outcome = "pass" if completed.returncode == 0 else "fail"
            exit_code: int | None = completed.returncode
            output_text = completed.stdout + completed.stderr
        except FileNotFoundError as exc:
            outcome = "skip"
            exit_code = None
            output_text = str(exc)
        (logs / f"{command_id(str(row['name']))}.log").write_text(output_text)
        record: dict[str, object] = {
            "id": command_id(str(row["name"])),
            "tier": "required",
            "argv": argv,
            "sha": sha,
            "outcome": outcome,
            "exit_code": exit_code,
            "started_at": started,
            "ended_at": utc_now() if outcome != "skip" else None,
        }
        if outcome == "skip":
            record["skip_reason"] = "required-tool-unavailable"
        commands.append(record)
        if outcome != "pass":
            print(
                "required-command: "
                f"id={record['id']} outcome={outcome} exit_code={json.dumps(exit_code)} "
                f"argv={json.dumps(argv)}",
                flush=True,
            )

    commands.append(
        {
            "id": "live-matrix",
            "tier": "advisory",
            "argv": ["scripts/version_matrix.sh", "full", "all"],
            "sha": sha,
            "outcome": "skip",
            "skip_reason": "not-run-by-required-local",
            "exit_code": None,
            "started_at": utc_now(),
            "ended_at": None,
        }
    )
    validate_command_coverage(commands, plan)
    verdict = required_verdict(commands)
    proof = {
        "schema": "required-proof/v2",
        "repo": "rust-oracledb",
        "generated_at": utc_now(),
        "source": source_ref(sha, command_output(["git", "branch", "--show-current"])),
        "tool_versions": {
            "rustc": command_output(["rustc", "--version"]),
            "cargo": command_output(["cargo", "--version"]),
            "cargo-deny": command_output(["cargo-deny", "--version"]),
            "runner": "scripts/verify_required_local.py",
        },
        "resource_budget": emitted_budget(run_id),
        "command_graph": command_graph_commitment(plan),
        "commands": commands,
        "verdict": verdict,
    }
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(json.dumps(proof, indent=2) + "\n")
    validated = subprocess.run([sys.executable, str(VALIDATOR), str(output), "--json"], cwd=ROOT, check=False)
    if validated.returncode != 0:
        return validated.returncode
    print(f"required-proof: wrote {output} (verdict={verdict})")
    return 0 if verdict == "pass" else 1


def self_test() -> None:
    sha = "0" * 40
    assert source_ref(sha, "") == {"sha": sha, "tree_clean": True}
    assert source_ref(sha, "main")["branch"] == "main"
    unknown = "jobs:\n  quality:\n    steps:\n      - name: New required gate\n        run: echo hi\n        if: ${{ inputs.not_known }}\n"
    try:
        parse_quality_steps(unknown)
        # Parsing is deliberately separate from classification.
        if "${{ inputs.not_known }}" not in CONDITIONS:
            raise ContractError("New required gate: unclassified condition '${{ inputs.not_known }}'")
    except ContractError:
        pass
    else:
        raise AssertionError("unknown conditions must fail closed")
    unknown_action = "jobs:\n  quality:\n    steps:\n      - uses: unreviewed/new-action@v1\n"
    try:
        for step in parse_quality_steps(unknown_action):
            if step.uses is not None and not step.uses.startswith(ALLOWED_ACTION_PREFIXES):
                raise ContractError(f"{step.name}: unclassified setup action {step.uses!r}")
    except ContractError:
        pass
    else:
        raise AssertionError("unknown setup actions must fail closed")

    quality_text = QUALITY_WORKFLOW.read_text()
    rq_text = RELEASE_QUALIFICATION_WORKFLOW.read_text()
    parsed = parse_quality_workflow(quality_text)
    validate_quality_job_graph(parsed)
    validate_d4_workflow_text(quality_text)
    validate_release_qualification_workflow(rq_text)
    assert parsed.environment == {
        "CARGO_TERM_COLOR": "always",
        "CARGO_PUBLIC_API_VERSION": "0.52.0",
        "CARGO_HACK_VERSION": "0.6.45",
        "CARGO_SEMVER_CHECKS_VERSION": "0.48.0",
    }
    assert tuple(parsed.jobs) == (*FANOUT_JOBS, AGGREGATE_JOB)
    assert parsed.jobs[AGGREGATE_JOB].needs == FANOUT_JOBS
    parsed_gate_names = tuple(
        step.name
        for step in parsed.steps
        if step.job in FANOUT_JOBS and step.uses is None and step.name not in META_STEPS
    )
    assert parsed_gate_names == EXPECTED_QUALITY_COMMANDS
    assert len(parsed_gate_names) == 33

    required_plan = effective_plan()
    for name, version_key in (
        ("Install cargo-public-api", "CARGO_PUBLIC_API_VERSION"),
        ("Install cargo-hack", "CARGO_HACK_VERSION"),
        ("Install cargo-semver-checks", "CARGO_SEMVER_CHECKS_VERSION"),
    ):
        row = next(item for item in required_plan if item["name"] == name)
        assert row["classification"] == "required-command"
        environment = row["environment"]
        assert isinstance(environment, dict)
        assert environment[version_key] == parsed.environment[version_key]

    environment_fixture = """\
env:
  SHARED: workflow
  WORKFLOW_ONLY: workflow-only
jobs:
  sample:
    env:
      SHARED: job
      JOB_ONLY: job-only
    steps:
      - name: Environment precedence
        env:
          SHARED: step
          STEP_ONLY: step-only
        run: echo ok
"""
    environment_workflow = parse_quality_workflow(environment_fixture)
    environment_step = environment_workflow.steps[0]
    assert merged_environment(environment_workflow, environment_step) == {
        "SHARED": "step",
        "WORKFLOW_ONLY": "workflow-only",
        "JOB_ONLY": "job-only",
        "STEP_ONLY": "step-only",
    }
    try:
        validate_environment({"RESULTS_DIR": "${{ runner.temp }}"}, "Environment precedence")
    except ContractError as exc:
        assert "unresolved GitHub expression" in str(exc), exc
    else:
        raise AssertionError("unresolved active environment must fail closed")

    strategy_on_core = parse_quality_workflow(quality_text)
    strategy_on_core.jobs["core"].strategy = FUZZ_STRATEGY
    try:
        validate_quality_job_graph(strategy_on_core)
    except ContractError as exc:
        assert "job 'core': strategy must be" in str(exc), exc
    else:
        raise AssertionError("a matrix outside the fuzz job must fail closed")

    for broken_strategy in (
        ("fail-fast: true", "matrix:", "  shard: [0, 1, 2, 3]"),
        ("fail-fast: false", "matrix:", "  shard: [0, 1]"),
        (*FUZZ_STRATEGY, "  os: [ubuntu-latest]"),
    ):
        broken_fuzz = parse_quality_workflow(quality_text)
        broken_fuzz.jobs["fuzz"].strategy = broken_strategy
        try:
            validate_quality_job_graph(broken_fuzz)
        except ContractError as exc:
            assert "job 'fuzz': strategy must be" in str(exc), exc
        else:
            raise AssertionError(f"fuzz strategy {broken_strategy!r} must fail closed")

    broken_condition = parse_quality_workflow(QUALITY_WORKFLOW.read_text())
    broken_condition.jobs["core"].condition = "${{ inputs.profile != 'canary' }}"
    try:
        validate_quality_job_graph(broken_condition)
    except ContractError as exc:
        assert "phantom/skipped check" in str(exc), exc
    else:
        raise AssertionError("a conditional quality shard must fail closed")

    broken_aggregate = parse_quality_workflow(QUALITY_WORKFLOW.read_text())
    broken_aggregate.jobs[AGGREGATE_JOB].needs = FANOUT_JOBS[:-1]
    try:
        validate_quality_job_graph(broken_aggregate)
    except ContractError as exc:
        assert "must need every quality shard" in str(exc), exc
    else:
        raise AssertionError("the stable aggregate must cover every shard")

    broken_result_check = parse_quality_workflow(QUALITY_WORKFLOW.read_text())
    aggregate_step = next(step for step in broken_result_check.steps if step.job == AGGREGATE_JOB)
    aggregate_step.run = (aggregate_step.run or "").replace("exit 1", "true")
    try:
        validate_quality_job_graph(broken_result_check)
    except ContractError as exc:
        assert "does not fail closed" in str(exc), exc
    else:
        raise AssertionError("a non-failing aggregate result step must fail closed")

    for broken_text, expected_error in (
        (
            quality_text.replace("            crates/oracledb-protocol/fuzz -> target\n", "", 1),
            "standalone fuzz workspace target",
        ),
        (
            quality_text.replace('shard_index="${{ matrix.shard }}"', "shard_index=$GITHUB_RUN_NUMBER", 1),
            "four-way round-robin contract",
        ),
        (
            quality_text.replace("${{ steps.format.outcome }}", "${{ steps.clippy.outcome }}", 1),
            "prep outcome inventory",
        ),
        (
            quality_text.replace("continue-on-error: ${{ inputs.mode == 'prep' }}", "continue-on-error: false", 1),
            "must continue on error",
        ),
    ):
        try:
            broken_parsed = parse_quality_workflow(broken_text)
            validate_quality_job_graph(broken_parsed)
            validate_d4_workflow_text(broken_text)
        except ContractError as exc:
            assert expected_error in str(exc), exc
        else:
            raise AssertionError(f"{expected_error!r} must fail closed")

    for broken_rq in (
        rq_text.replace("      mode: ${{ inputs.mode }}", "      mode: strict", 1),
        rq_text.replace(
            "if: ${{ inputs.mode == 'strict' && needs.release-qualification.result == 'success' }}",
            "if: ${{ needs.release-qualification.result == 'success' }}",
            1,
        ),
        rq_text.replace(
            "          name: release-required-logs-${{ inputs.candidate_sha }}",
            "          name: release-required-proof-logs-${{ inputs.candidate_sha }}",
            1,
        ),
    ):
        try:
            validate_release_qualification_workflow(broken_rq)
        except ContractError:
            pass
        else:
            raise AssertionError("prep must never skip the shared graph or reach strict evidence")

    test_plan: list[dict[str, object]] = [
        {
            "classification": "required-command",
            "name": "Format",
            "argv": ["bash", "-lc", "cargo fmt --all -- --check"],
        },
        {
            "classification": "required-command",
            "name": "Test workspace",
            "argv": ["bash", "-lc", "cargo test --workspace"],
        },
    ]
    passing_commands: list[dict[str, object]] = [
        {
            "id": "format",
            "tier": "required",
            "argv": ["bash", "-lc", "cargo fmt --all -- --check"],
            "outcome": "pass",
        },
        {
            "id": "test-workspace",
            "tier": "required",
            "argv": ["bash", "-lc", "cargo test --workspace"],
            "outcome": "pass",
        },
        {
            "id": "live-matrix",
            "tier": "advisory",
            "argv": ["scripts/version_matrix.sh", "full", "all"],
            "outcome": "skip",
        },
    ]
    validate_command_coverage(passing_commands, test_plan)
    graph = command_graph_commitment(test_plan)
    assert graph["command_ids"] == ["format", "live-matrix", "test-workspace"]
    assert re.fullmatch(r"[0-9a-f]{64}", str(graph["sha256"]))
    assert required_verdict(passing_commands) == "pass"

    for records, expected_error in (
        (passing_commands[1:], "missing Required command records"),
        (passing_commands + [passing_commands[0]], "duplicate command record"),
        (
            [
                {**passing_commands[0], "argv": ["bash", "-lc", "cargo fmt"]},
                *passing_commands[1:],
            ],
            "recorded argv differs",
        ),
    ):
        try:
            validate_command_coverage(records, test_plan)
        except ContractError as exc:
            assert expected_error in str(exc), exc
        else:
            raise AssertionError(f"{expected_error!r} must fail closed")

    assert required_verdict([{**passing_commands[0], "outcome": "skip"}]) == "fail"
    assert required_verdict([{**passing_commands[0], "outcome": "fail"}]) == "fail"
    print("verify-required-local: self-test OK")


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path)
    parser.add_argument("--run-id")
    parser.add_argument("--internal", action="store_true", help=argparse.SUPPRESS)
    parser.add_argument("--plan", action="store_true")
    parser.add_argument("--self-test", action="store_true")
    args = parser.parse_args()

    if args.self_test:
        self_test()
        return 0
    plan = effective_plan()
    if args.plan:
        public_plan = [
            {key: value for key, value in row.items() if key != "environment"}
            for row in plan
        ]
        print(json.dumps({"profile": "required", "steps": public_plan}, indent=2))
        return 0

    sha = git_sha()
    if not re.fullmatch(r"[0-9a-f]{40}", sha):
        raise ContractError(f"could not resolve an exact HEAD SHA: {sha!r}")
    output = args.output or default_output(sha)
    run_id = args.run_id or f"required-{sha[:12]}"

    if not args.internal:
        command = [
            str(RESOURCE_BUDGET),
            "--profile",
            "test",
            "--run-id",
            run_id,
            "--",
            sys.executable,
            str(Path(__file__).resolve()),
            "--internal",
            "--run-id",
            run_id,
            "--output",
            str(output),
        ]
        return subprocess.run(command, cwd=ROOT, check=False).returncode

    if not git_clean():
        print(
            "required-proof: REFUSING to run on a dirty tree; use a detached clean worktree at HEAD "
            "so every command and the emitted source.sha describe the same code",
            file=sys.stderr,
        )
        return 78
    return run_required(plan, sha, output, run_id)


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except ContractError as exc:
        print(f"required-proof: {exc}", file=sys.stderr)
        raise SystemExit(2)
