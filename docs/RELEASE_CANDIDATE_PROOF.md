# Exact-SHA release-candidate proof

`scripts/verify_release_exact_sha.sh --tag vX.Y.Z --sha <40-lowercase-hex>`
is a read-only check for a prospective release. It never creates a tag, changes
a ref, pushes, publishes a crate, or creates a GitHub release. Its success
output is `release-candidate-proof/v2`, a validation record rather than release
authorization.

The command fails closed unless all of these describe the same exact candidate:

- the worktree is clean, checked out at `--sha`, and that commit is contained in
  the locally available `origin/main`;
- the candidate tag is well formed, absent, and matches the workspace version;
- the required-local proof is a valid passing `required-proof/v2` for `--sha`,
  with every record matching its self-declared graph record and the candidate's
  independently derived Required graph;
- `scripts/ci_taxonomy.py --status <sha>` reports every required check-run as
  `completed` / `success`, with no missing or unknown checks; and
- `tests/artifacts/version_matrix/results-<sha>.json` is a clean, all-PASS live
  matrix result for that same SHA, including `xe11`, `xe18`, `xe21`, `free23`,
  and `octcps`.

The artifact rule is intentionally stricter than the legacy tag preflight:
**a parent matrix artifact is rejected**, even if its commit changed only the
artifact directory. `release-candidate-proof/v2` requires `artifacts[].sha` to
equal `source.sha`; accepting the parent would turn an exact-candidate claim
into an inference. Until the matrix evidence producer can supply an artifact
for the exact candidate, this validator correctly produces no proof.

By default, successful output is written to
`tests/artifacts/evidence/release-candidate/release-candidate-proof-<sha>.json`.
The command refuses to overwrite an existing path. It verifies tree cleanliness
before writing, so the output documents the clean input tree; it does not claim
that the generated report itself has been committed.

For the DB-free regression and contract checks:

```bash
scripts/test_verify_release_exact_sha.sh
```

The self-test includes negative controls for a dirty tree, unknown SHA,
off-main candidate, existing tag, tag/version mismatch, non-terminal required
CI, missing matrix lanes, and a parent-artifact SHA substitution.
