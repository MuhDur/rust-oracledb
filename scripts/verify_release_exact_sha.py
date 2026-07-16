#!/usr/bin/env python3
"""Validate one prospective release tag without creating the tag.

The release workflow is deliberately tag-driven.  This command is its
read-only counterpart: it checks a clean, exact commit already reachable from
``origin/main`` and emits a ``release-candidate-proof/v1`` only when the
candidate's local proof, CI check-runs, and live version-matrix artifact all
refer to that same commit.  It never creates a tag, modifies a ref, pushes, or
publishes.
"""

from __future__ import annotations

import argparse
import datetime as dt
import json
import re
import subprocess
import sys
import tomllib
from dataclasses import dataclass
from pathlib import Path
from typing import Protocol

from validate_evidence import validate_doc


ROOT = Path(__file__).resolve().parents[1]
CI_TAXONOMY = ROOT / "scripts" / "ci_taxonomy.py"
SHA_RE = re.compile(r"^[0-9a-f]{40}$")
TAG_RE = re.compile(r"^v([0-9]+\.[0-9]+\.[0-9]+(?:-[0-9A-Za-z.-]+)?)$")
MATRIX_LANES = ("xe11", "xe18", "xe21", "free23", "octcps")


class ReleaseValidationError(RuntimeError):
    """A candidate failed a precondition and must not produce a proof."""

    def __init__(self, code: str, message: str) -> None:
        super().__init__(message)
        self.code = code


@dataclass(frozen=True)
class CommandResult:
    returncode: int
    stdout: str = ""
    stderr: str = ""


class CommandRunner(Protocol):
    def run(self, argv: list[str]) -> CommandResult:
        """Run an inspection command from the repository root."""


class SubprocessRunner:
    def __init__(self, root: Path) -> None:
        self.root = root

    def run(self, argv: list[str]) -> CommandResult:
        try:
            completed = subprocess.run(argv, cwd=self.root, text=True, capture_output=True, check=False)
        except OSError as error:
            return CommandResult(127, stderr=str(error))
        return CommandResult(completed.returncode, completed.stdout, completed.stderr)


class FakeRunner:
    """Small command transcript used by ``--self-test`` without a GitHub call."""

    def __init__(self, responses: dict[tuple[str, ...], CommandResult]) -> None:
        self.responses = responses

    def run(self, argv: list[str]) -> CommandResult:
        try:
            return self.responses[tuple(argv)]
        except KeyError as error:
            raise AssertionError(f"unplanned inspection command: {argv!r}") from error


def utc_now() -> str:
    return dt.datetime.now(dt.timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def fail_if_error(result: CommandResult, code: str, action: str) -> str:
    if result.returncode != 0:
        detail = result.stderr.strip() or result.stdout.strip() or f"exit {result.returncode}"
        raise ReleaseValidationError(code, f"{action}: {detail}")
    return result.stdout.strip()


def require_clean_tree(runner: CommandRunner) -> None:
    output = fail_if_error(runner.run(["git", "status", "--porcelain"]), "E_GIT", "could not inspect tree")
    if output:
        raise ReleaseValidationError(
            "E_TREE_DIRTY",
            "refusing to validate a dirty tree; the candidate must be reproduced from an exact commit",
        )


def require_exact_candidate(runner: CommandRunner, sha: str) -> None:
    if not SHA_RE.fullmatch(sha):
        raise ReleaseValidationError("E_UNKNOWN_SHA", "--sha must be a full 40-character lowercase commit SHA")
    resolved = fail_if_error(
        runner.run(["git", "rev-parse", "--verify", f"{sha}^{{commit}}"]),
        "E_UNKNOWN_SHA",
        f"candidate {sha} is not a locally known commit",
    )
    if resolved != sha:
        raise ReleaseValidationError(
            "E_UNKNOWN_SHA", f"candidate {sha} resolved unexpectedly to {resolved!r}; refusing an ambiguous SHA"
        )
    head = fail_if_error(runner.run(["git", "rev-parse", "HEAD"]), "E_GIT", "could not resolve HEAD")
    if head != sha:
        raise ReleaseValidationError(
            "E_SHA_NOT_HEAD",
            f"HEAD is {head}, not requested candidate {sha}; check out the exact candidate before validation",
        )


def require_main_ancestry(runner: CommandRunner, sha: str) -> None:
    result = runner.run(["git", "merge-base", "--is-ancestor", sha, "origin/main"])
    if result.returncode == 0:
        return
    if result.returncode == 1:
        raise ReleaseValidationError(
            "E_NOT_ON_MAIN", f"candidate {sha} is not contained in the locally available origin/main"
        )
    fail_if_error(result, "E_GIT", "could not check origin/main ancestry")


def require_absent_tag(runner: CommandRunner, tag: str) -> None:
    if not TAG_RE.fullmatch(tag):
        raise ReleaseValidationError("E_TAG_FORMAT", f"tag {tag!r} is not a supported vX.Y.Z tag")
    result = runner.run(["git", "show-ref", "--verify", "--quiet", f"refs/tags/{tag}"])
    if result.returncode == 0:
        raise ReleaseValidationError("E_TAG_EXISTS", f"tag {tag!r} already exists; this command never moves tags")
    if result.returncode != 1:
        fail_if_error(result, "E_GIT", f"could not inspect tag {tag!r}")


def workspace_version(root: Path) -> str:
    try:
        cargo = tomllib.loads((root / "Cargo.toml").read_text())
        version = cargo["workspace"]["package"]["version"]
    except (FileNotFoundError, KeyError, TypeError, tomllib.TOMLDecodeError) as error:
        raise ReleaseValidationError("E_WORKSPACE_VERSION", f"could not read workspace version: {error}") from error
    if not isinstance(version, str) or TAG_RE.fullmatch(f"v{version}") is None:
        raise ReleaseValidationError("E_WORKSPACE_VERSION", f"workspace version {version!r} is not release-tag compatible")
    return version


def require_tag_version(tag: str, version: str) -> None:
    if tag != f"v{version}":
        raise ReleaseValidationError(
            "E_TAG_VERSION_MISMATCH", f"tag {tag!r} does not match workspace version {version!r}"
        )


def load_json(path: Path, code: str, description: str) -> dict:
    try:
        value = json.loads(path.read_text(), parse_constant=lambda value: (_ for _ in ()).throw(ValueError(value)))
    except (FileNotFoundError, OSError, ValueError, json.JSONDecodeError) as error:
        raise ReleaseValidationError(code, f"{description} at {path}: {error}") from error
    if not isinstance(value, dict):
        raise ReleaseValidationError(code, f"{description} at {path} must be a JSON object")
    return value


def validate_required_proof(proof: dict, sha: str, path: Path) -> None:
    findings = validate_doc(proof)
    if findings:
        raise ReleaseValidationError(
            "E_REQUIRED_PROOF_INVALID",
            f"required proof {path} is invalid: {findings[0]}",
        )
    if proof.get("schema") != "required-proof/v1":
        raise ReleaseValidationError("E_REQUIRED_PROOF_INVALID", f"{path} is not required-proof/v1")
    if proof.get("verdict") != "pass":
        raise ReleaseValidationError("E_REQUIRED_PROOF_NOT_GREEN", f"{path} does not record a passing Required graph")
    source = proof.get("source")
    if not isinstance(source, dict) or source.get("sha") != sha:
        raise ReleaseValidationError("E_STALE_SHA", f"required proof {path} does not describe candidate {sha}")


def matrix_artifact(root: Path, sha: str) -> tuple[Path, dict]:
    path = root / "tests" / "artifacts" / "version_matrix" / f"results-{sha}.json"
    return path, load_json(path, "E_MISSING_LIVE_ARTIFACT", "missing exact-SHA live version-matrix artifact")


def validate_matrix_artifact(artifact: dict, sha: str, path: Path) -> None:
    if artifact.get("sha") != sha:
        raise ReleaseValidationError(
            "E_ARTIFACT_SHA_MISMATCH",
            f"matrix artifact {path} records {artifact.get('sha')!r}, not candidate {sha}; parent artifacts are not substitutes",
        )
    if artifact.get("dirty") is not False:
        raise ReleaseValidationError("E_LIVE_ARTIFACT_DIRTY", f"matrix artifact {path} was not recorded on a clean tree")
    if artifact.get("overall") != "PASS":
        raise ReleaseValidationError("E_LIVE_ARTIFACT_NOT_GREEN", f"matrix artifact {path} is not all-green")
    lanes = artifact.get("lanes")
    if not isinstance(lanes, dict):
        raise ReleaseValidationError("E_LIVE_ARTIFACT_INVALID", f"matrix artifact {path} has no lane verdict map")
    for lane in MATRIX_LANES:
        if lanes.get(lane) != "PASS":
            raise ReleaseValidationError(
                "E_LIVE_ARTIFACT_NOT_GREEN", f"matrix artifact {path} lane {lane!r} is not PASS"
            )


def ci_status(runner: CommandRunner, sha: str) -> dict:
    result = runner.run([sys.executable, str(CI_TAXONOMY), "--status", sha])
    if result.returncode not in (0, 1):
        detail = result.stderr.strip() or result.stdout.strip() or f"exit {result.returncode}"
        raise ReleaseValidationError("E_CI_STATUS_UNAVAILABLE", f"could not obtain CI check-run status: {detail}")
    try:
        report = json.loads(result.stdout)
    except json.JSONDecodeError as error:
        detail = result.stderr.strip() or result.stdout.strip() or f"exit {result.returncode}"
        raise ReleaseValidationError("E_CI_STATUS_UNAVAILABLE", f"could not obtain CI check-run status: {detail}") from error
    if not isinstance(report, dict) or report.get("sha") != sha:
        raise ReleaseValidationError("E_CI_STATUS_UNAVAILABLE", "CI status report did not describe the requested SHA")
    return report


def validate_ci_status(report: dict, sha: str) -> list[dict]:
    missing = (
        report.get("required_missing_path_filtered"),
        report.get("required_missing_unexpected"),
        report.get("unknown_jobs"),
        report.get("required_not_green"),
    )
    jobs = report.get("jobs")
    if report.get("ci_green") is not True or any(missing) or not isinstance(jobs, list):
        raise ReleaseValidationError(
            "E_REQUIRED_CI_NOT_GREEN",
            f"required CI is not a complete completed/success set for candidate {sha}",
        )
    recorded = [
        job
        for job in jobs
        if isinstance(job, dict) and job.get("tier") in ("required", "advisory")
    ]
    required = [job for job in recorded if job["tier"] == "required"]
    if not required:
        raise ReleaseValidationError("E_REQUIRED_CI_NOT_GREEN", "CI report contains no required check-runs")
    for job in required:
        if job.get("status") != "completed" or job.get("conclusion") != "success":
            raise ReleaseValidationError(
                "E_REQUIRED_CI_NOT_GREEN",
                f"required check {job.get('name')!r} is not completed/success",
            )
    # The evidence contract intentionally models the gating and advisory
    # check-runs only. Scheduled, manual, and tag-only jobs are classified by
    # ci-taxonomy but neither gate this candidate nor fit ciJob's closed enum.
    return recorded


def default_required_proof(root: Path, sha: str) -> Path:
    return root / "tests" / "artifacts" / "evidence" / "required" / f"required-proof-{sha}.json"


def default_output(root: Path, sha: str) -> Path:
    return root / "tests" / "artifacts" / "evidence" / "release-candidate" / f"release-candidate-proof-{sha}.json"


def resolve_repo_path(root: Path, supplied: Path) -> Path:
    path = supplied if supplied.is_absolute() else root / supplied
    try:
        path.relative_to(root)
    except ValueError as error:
        raise ReleaseValidationError("E_OUTPUT_PATH", f"path {path} must stay within the repository") from error
    return path


def build_proof(root: Path, tag: str, sha: str, required_path: Path, runner: CommandRunner) -> dict:
    require_clean_tree(runner)
    require_exact_candidate(runner, sha)
    require_main_ancestry(runner, sha)
    require_absent_tag(runner, tag)
    version = workspace_version(root)
    require_tag_version(tag, version)

    required = load_json(required_path, "E_MISSING_REQUIRED_PROOF", "missing exact-SHA required proof")
    validate_required_proof(required, sha, required_path)
    matrix_path, matrix = matrix_artifact(root, sha)
    validate_matrix_artifact(matrix, sha, matrix_path)
    jobs = validate_ci_status(ci_status(runner, sha), sha)

    proof = {
        "schema": "release-candidate-proof/v1",
        "repo": root.name,
        "generated_at": utc_now(),
        "candidate": {"tag": tag, "version": version},
        "source": {"sha": sha, "tree_clean": True, "branch": "main"},
        "required_proof": {
            "schema": "required-proof/v1",
            "path": str(required_path.relative_to(root)),
            "sha": sha,
        },
        "required_ci": {"sha": sha, "jobs": jobs},
        "artifacts": [
            {"kind": "version-matrix", "path": str(matrix_path.relative_to(root)), "sha": sha}
        ],
        "verdict": "pass",
    }
    findings = validate_doc(proof)
    if findings:
        raise ReleaseValidationError("E_PROOF_INVALID", f"generated proof violates its contract: {findings[0]}")
    return proof


def assert_rejected(action, code: str) -> None:
    try:
        action()
    except ReleaseValidationError as error:
        assert error.code == code, error
    else:
        raise AssertionError(f"expected {code} rejection")


def self_test() -> None:
    sha = "a" * 40
    parent = "b" * 40
    version = workspace_version(ROOT)
    tag = f"v{version}"
    green_report = {
        "sha": sha,
        "ci_green": True,
        "jobs": [
            {"name": "required / quality", "tier": "required", "status": "completed", "conclusion": "success"},
            {"name": "nightly discovery", "tier": "scheduled", "status": "completed", "conclusion": "failure"},
        ],
        "required_not_green": [],
        "required_missing_path_filtered": [],
        "required_missing_unexpected": [],
        "unknown_jobs": [],
    }
    responses = {
        ("git", "status", "--porcelain"): CommandResult(0),
        ("git", "rev-parse", "--verify", f"{sha}^{{commit}}") : CommandResult(0, f"{sha}\n"),
        ("git", "rev-parse", "HEAD"): CommandResult(0, f"{sha}\n"),
        ("git", "merge-base", "--is-ancestor", sha, "origin/main"): CommandResult(0),
        ("git", "show-ref", "--verify", "--quiet", f"refs/tags/{tag}"): CommandResult(1),
        (sys.executable, str(CI_TAXONOMY), "--status", sha): CommandResult(0, json.dumps(green_report)),
    }
    runner = FakeRunner(responses)
    require_clean_tree(runner)
    require_exact_candidate(runner, sha)
    require_main_ancestry(runner, sha)
    require_absent_tag(runner, tag)
    assert workspace_version(ROOT) == version
    require_tag_version(tag, version)
    green_jobs = validate_ci_status(ci_status(runner, sha), sha)
    assert green_jobs == green_report["jobs"][:1]

    required = json.loads((ROOT / "schemas/evidence/fixtures/valid/required-proof-pass.json").read_text())
    required["source"]["sha"] = sha
    for command in required["commands"]:
        command["sha"] = sha
    validate_required_proof(required, sha, Path("required-proof.json"))

    green_matrix = {"sha": sha, "dirty": False, "overall": "PASS", "lanes": {lane: "PASS" for lane in MATRIX_LANES}}
    validate_matrix_artifact(green_matrix, sha, Path("matrix.json"))
    generated = {
        "schema": "release-candidate-proof/v1",
        "repo": ROOT.name,
        "generated_at": "2026-07-16T00:00:00Z",
        "candidate": {"tag": tag, "version": version},
        "source": {"sha": sha, "tree_clean": True, "branch": "main"},
        "required_proof": {"schema": "required-proof/v1", "path": "required-proof.json", "sha": sha},
        "required_ci": {"sha": sha, "jobs": green_jobs},
        "artifacts": [{"kind": "version-matrix", "path": "matrix.json", "sha": sha}],
        "verdict": "pass",
    }
    generated_findings = validate_doc(generated)
    assert not generated_findings, [str(finding) for finding in generated_findings]

    assert_rejected(lambda: require_exact_candidate(runner, "not-a-sha"), "E_UNKNOWN_SHA")
    unknown = dict(responses)
    unknown[("git", "rev-parse", "--verify", f"{sha}^{{commit}}") ] = CommandResult(128, stderr="unknown revision")
    assert_rejected(lambda: require_exact_candidate(FakeRunner(unknown), sha), "E_UNKNOWN_SHA")
    dirty = dict(responses)
    dirty[("git", "status", "--porcelain")] = CommandResult(0, " M Cargo.toml\n")
    assert_rejected(lambda: require_clean_tree(FakeRunner(dirty)), "E_TREE_DIRTY")
    non_main = dict(responses)
    non_main[("git", "merge-base", "--is-ancestor", sha, "origin/main")] = CommandResult(1)
    assert_rejected(lambda: require_main_ancestry(FakeRunner(non_main), sha), "E_NOT_ON_MAIN")
    tagged = dict(responses)
    tagged[("git", "show-ref", "--verify", "--quiet", f"refs/tags/{tag}")] = CommandResult(0)
    assert_rejected(lambda: require_absent_tag(FakeRunner(tagged), tag), "E_TAG_EXISTS")
    assert_rejected(lambda: require_tag_version("v0.0.0", version), "E_TAG_VERSION_MISMATCH")
    assert_rejected(
        lambda: validate_ci_status({**green_report, "ci_green": False, "required_not_green": ["required / quality"]}, sha),
        "E_REQUIRED_CI_NOT_GREEN",
    )
    assert_rejected(
        lambda: validate_ci_status(
            {**green_report, "jobs": [{"name": "required / quality", "tier": "required", "status": "in_progress", "conclusion": None}]},
            sha,
        ),
        "E_REQUIRED_CI_NOT_GREEN",
    )
    assert_rejected(
        lambda: matrix_artifact(ROOT / "missing-release-proof-fixture", sha),
        "E_MISSING_LIVE_ARTIFACT",
    )
    assert_rejected(lambda: validate_matrix_artifact({**green_matrix, "sha": parent}, sha, Path("matrix.json")), "E_ARTIFACT_SHA_MISMATCH")
    assert_rejected(lambda: validate_matrix_artifact({**green_matrix, "lanes": {}}, sha, Path("matrix.json")), "E_LIVE_ARTIFACT_NOT_GREEN")
    print("verify-release-exact-sha: self-test OK")


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.split("\n", 1)[0])
    parser.add_argument("--tag", help="candidate tag vX.Y.Z; this command never creates it")
    parser.add_argument("--sha", help="full 40-character candidate commit SHA")
    parser.add_argument("--required-proof", type=Path, help="exact-SHA required-proof/v1 path")
    parser.add_argument("--output", type=Path, help="where to write release-candidate-proof/v1")
    parser.add_argument("--self-test", action="store_true", help="run deterministic offline negative controls")
    args = parser.parse_args()

    if args.self_test:
        self_test()
        return 0
    if not args.tag or not args.sha:
        parser.error("--tag and --sha are both required unless --self-test is used")

    try:
        required_path = resolve_repo_path(ROOT, args.required_proof or default_required_proof(ROOT, args.sha))
        output = resolve_repo_path(ROOT, args.output or default_output(ROOT, args.sha))
        if output.exists():
            raise ReleaseValidationError("E_OUTPUT_EXISTS", f"refusing to overwrite existing proof {output}")
        proof = build_proof(ROOT, args.tag, args.sha, required_path, SubprocessRunner(ROOT))
        output.parent.mkdir(parents=True, exist_ok=True)
        output.write_text(json.dumps(proof, indent=2) + "\n")
    except ReleaseValidationError as error:
        print(f"release-candidate-proof: {error.code}: {error}", file=sys.stderr)
        return 1
    print(f"release-candidate-proof: wrote {output.relative_to(ROOT)} for {args.tag} at {args.sha}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
