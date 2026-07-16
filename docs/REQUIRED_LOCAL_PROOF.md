# Local Required proof

`scripts/verify_required_local.sh` is the local counterpart to the effective
`required` profile in `.github/workflows/_quality.yml`. It emits
`required-proof/v2`: an exact SHA, tool versions, enforced resource budget, and
one outcome record for every effective Required command. It never claims that
GitHub CI is green and it never tags, pushes, publishes, or creates a release.

The command derives its plan from the workflow. Every workflow step, action, and
condition must have an explicit classification; an unrecognised addition fails
closed rather than quietly falling out of the local graph. Before it writes a
proof, the runner compares its records with that derived plan: a missing,
duplicate, or altered Required command record is rejected. The DB-free contract
test also executes the shared invalid fixtures for stale SHA, an unfinished
command, and a skipped Required command presented as a pass.

The producer writes a sorted command-ID witness with its SHA-256 commitment.
`validate_evidence.py` requires every record to match that witness exactly, so a
missing, duplicate, or extra command is `E_COMMAND_GRAPH_MISMATCH`. The
exact-SHA release consumer additionally derives the candidate's Required plan
from the audited workflow and checks IDs, tiers, and argv before accepting a
passing proof.

## Run it honestly

The proof requires a clean tree. In this shared checkout that normally means a
detached clean worktree at the exact SHA being proved. Running a workspace graph
against another pane's uncommitted code and recording `HEAD` would be false
evidence, so the runner exits 78 before it starts in that state.

Then run:

```bash
scripts/verify_required_local.sh
```

The runner re-execs under `scripts/resource_budget.sh --profile test`, which
creates an isolated disk-backed target directory and enforces both memory and
PID/task ceilings. Its output is written to
`tests/artifacts/evidence/required/required-proof-<sha>.json` and immediately
checked by `scripts/validate_evidence.py`.

Required commands that cannot run are records with `outcome: "skip"`; the v2
semantic validator treats that as a failing proof. The advisory live matrix is
recorded separately as a typed skip (`not-run-by-required-local`), never rounded
up to a pass.

For a DB-free graph/contract check without running the heavy commands:

```bash
scripts/test_verify_required_local.sh
```
