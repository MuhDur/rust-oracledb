# ADR-0003: Release Evidence Is Per Exact Candidate SHA

Status: Accepted

Date: 2026-06-19

## Context

Scheduled CI lanes are useful discovery tools. They run on the moving default
branch and can find breakage from toolchain drift, fuzzing, model checks, live
Oracle behavior, or performance changes. They do not prove that a specific
release candidate is qualified.

The 1.0 release needs evidence for the frozen commit that will be released.

## Decision

Release qualification evidence is tied to one exact candidate SHA. Scheduled
canary and soak lanes are discovery only. The definitive 1.0 gate is the manual
`release-qualification` workflow with an explicit `candidate_sha`.

If code changes after qualification, the previous evidence no longer qualifies
the release. Cut a new release candidate and qualify that new exact SHA.

## Consequences

- W0-T2 creates reusable CI so required, canary, soak, and release qualification
  lanes run the same command families at different budgets.
- W4-T2 runs `release-qualification` at soak-equivalent budget against the frozen
  release-candidate commit.
- The release evidence bundle must record the candidate SHA and the exact gate
  results.
- Passing scheduled canary or soak on `main` is never a substitute for the manual
  exact-SHA qualification run.

## Review Triggers

Revisit this decision only if release mechanics change so the released artifact
can be cryptographically tied to an equivalent immutable input other than a Git
commit SHA. Any replacement must still make moving-branch discovery distinct from
release-candidate qualification.

Changing this decision requires a new ADR.
