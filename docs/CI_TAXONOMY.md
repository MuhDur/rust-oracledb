# CI taxonomy

A machine-readable answer to one question that turns out to be surprisingly hard:
**is CI actually green on this commit?**

## Why `gh run list` cannot answer it

`gh run list` reports **run-level** conclusions. A run is reported `success` even
when one of its jobs is red — because a job marked `continue-on-error: true`
never fails its run.

This repo has two such jobs. One of them, `fuzz targets compile/smoke (nightly)`,
**sat red for days** (a fuzz target added in `54757a6` was never registered in
`targets.toml`) while every `gh run list` row said `success`. Nothing lied; the
question was wrong.

So "green" has to be answered **per check-run**, against a list that says which
jobs were ever supposed to gate. That list is what this is.

```bash
scripts/check_ci_taxonomy.sh            # drift gate: YAML vs committed list
scripts/ci_taxonomy.py --list           # derived taxonomy, JSON
scripts/ci_taxonomy.py --write          # regenerate docs/ci_taxonomy.json
scripts/ci_taxonomy.py --status  <SHA>  # classify a SHA; exit 1 unless green
scripts/ci_taxonomy.py --verify-names <SHA>   # every real check-run is classified
```

The list is **derived from `.github/workflows/*.yml`, never hand-maintained**, and
`--check` fails when the committed copy drifts — so a new job cannot appear
without being classified, and a required job cannot quietly become advisory.

## Tiers

| tier | derived from | gates? |
|---|---|---|
| `required` | runs on push-to-branch or `pull_request`, not advisory | **yes** — must be `completed`/`success` for a SHA to be releasable |
| `advisory` | `continue-on-error: true` | never — reported separately |
| `scheduled` | only fires on a timer (`schedule`) | no |
| `release` | only fires on a release tag (`push: tags:`) | no |
| `manual` | only `workflow_dispatch` | no |

Current: 12 required, 2 advisory, 6 scheduled, 4 release, 5 manual (29 jobs).
The authoritative list is [`ci_taxonomy.json`](ci_taxonomy.json). `workflows` and
`groups` in that file are **derived views** over `jobs[]`, which stays the single
place a tier is recorded, so a view cannot disagree with it.

## The rule

**Never call CI green while a required job is not a completed success.**

- Non-terminal is not success. A required job still `in_progress` is not green.
- `skipped` and `neutral` are not success.
- **Absent is not success** — see below.
- Advisory jobs are reported in `advisory_not_green` and never affect `ci_green`.
  The set is "not green" rather than "failures" on purpose: an advisory job that
  is hung or queued has not failed, but it has not passed either, and calling the
  set "failures" would quietly drop it.

## Absence, and why it fails closed

A required job with no check-run at a SHA is reported in one of two buckets,
because the two mean very different things:

- **`required_missing_path_filtered`** — the workflow has a `paths:` filter, so
  this commit may legitimately never have triggered it. The version-matrix lanes
  only run on `crates/**`, so a docs-only commit shows all four here.
- **`required_missing_unexpected`** — nothing filtered it. It should have run and
  did not. This is the alarming one.

Both leave `ci_green: false`. That is deliberate: absence is not success, and this
repo's own release rule already says a release cannot ship without its evidence
recorded **at the release SHA** (AGENTS.md). A path filter explains why a job did
not run; it does not conjure evidence that it passed.

This matters more than it sounds. A report that only iterates the check-runs that
*exist* finds "0 required not-green" on a docs-only commit and calls it green —
while four required version-matrix lanes never ran at all. Modelling absence is
what stops the overclaim.

An `unknown_jobs` entry — a check-run the taxonomy has never heard of — also
forces non-green, because an unclassified job is one nobody decided about.

## How names are derived

The derived `check_name` must match the check-run GitHub publishes exactly, or a
job can never be found and would look permanently missing. Three things make that
non-trivial, and all three are handled:

- **The `on:` trap.** In YAML 1.1 — which PyYAML implements — the bare word `on`
  is a **boolean**. GitHub's `on:` block parses to the key `True`, not `"on"`. A
  naive parser reads zero triggers and silently calls every job manual.
- **Reusable workflows.** `required.yml` calls `_quality.yml`, and GitHub prefixes
  every called job with the caller job id. The parallel quality shards therefore
  publish names such as `required / quality-core (required/required)`. A final
  fail-closed aggregate deliberately preserves the established
  `required / quality (required/required)` check name. In both cases the suffix is
  the called job's `name:` with `${{ inputs.* }}` expanded from the caller's
  `with:` block.
- **Matrix jobs.** `version-matrix.yml` publishes one check-run per lane
  (`xe18 full suite`, …), so the taxonomy expands `${{ matrix.lane.name }}` into
  the concrete combinations.

If the deriver meets an expression it cannot expand, it **refuses to emit the
name** rather than shipping one that could never match. A name that never matches
does not fail loudly; it reports "missing" forever, which is worse.

`--verify-names <SHA>` closes the loop against reality: every real check-run at a
SHA must be classified. It is one-directional on purpose — a derived job with no
check-run is legitimate (a filter or schedule), but a check-run the taxonomy has
never heard of means the deriver is wrong or a job is unclassified.

## Cross-repo shape

`ci-taxonomy/v1` is mirrored with the sibling `oraclemcp` repo (`yg4x.8`). The
*content* is necessarily per-repo — different workflows — but the **shape and
semantics are shared**: `schema`, `jobs[]` (with `tier`), the derived `workflows`
and `groups` views, and the report keys `ci_green`, `required_not_green`,
`advisory_not_green`, `required_missing_path_filtered`,
`required_missing_unexpected`, `unknown_jobs`.

`schema` (not `schema_version`) matches the discriminator every schema in
[EVIDENCE_CONTRACT.md](EVIDENCE_CONTRACT.md) already uses, so the whole contract
family spells the same concept one way; the value carries the version.

## Relationship to the evidence contract

`--status` emits jobs in the same `{name, tier, status, conclusion}` shape as the
`required_ci.jobs` block of `release-candidate-proof/v1`
([EVIDENCE_CONTRACT.md](EVIDENCE_CONTRACT.md)), so the release-candidate gate can
consume this directly rather than re-deriving CI state. That gate independently
re-checks the same invariant (`E_REQUIRED_CI_NOT_GREEN`), because a proof is only
evidence if a reader can re-check it rather than trust the producer.

This command **never triggers a release and never claims CI success on its own**;
it reports what the check-runs say, and refuses to round any of it up.
