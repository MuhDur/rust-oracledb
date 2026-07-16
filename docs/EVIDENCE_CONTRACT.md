# Cross-repo evidence contract

Six versioned JSON schemas that define what counts as *evidence* in this repo
and in the sibling `oraclemcp` repo. They exist because of a specific, repeated
failure: work was closed, and releases were cut, on claims that nothing could
check — a rate with no counts behind it, a green that was really a skip, an
artifact recorded for some other commit.

The rule of the contract is one sentence: **a claim that cannot be checked is
not evidence.** Every field below exists because some claim escaped once.

| schema | answers |
|---|---|
| `required-proof/v1` | Legacy local Required proof shape, accepted for historical evidence. |
| `required-proof/v2` | Did this repo's complete Required command graph actually run, on exactly this tree, to completion? |
| `release-candidate-proof/v1` | Legacy exact-SHA candidate proof shape, accepted for historical evidence. |
| `release-candidate-proof/v2` | Does a candidate tag at an exact SHA satisfy every release precondition using a Required proof checked against its own workflow graph? |
| `mutation-result/v1` | Did a mutation run measure what it claims, and can its rate be recomputed? |
| `bead-close-evidence/v1` | Is what this bead closes on actually backed by artifacts at this SHA? |

Files live in [`schemas/evidence/`](../schemas/evidence). Fixtures and their
manifest live in [`schemas/evidence/fixtures/`](../schemas/evidence/fixtures).

## Running the gate

```bash
scripts/check_evidence_contract.sh          # the whole fixture suite
scripts/check_evidence_contract.sh --mirror-root ../oraclemcp  # dual-release schema inventory
scripts/validate_evidence.py FILE.json      # validate one document
scripts/validate_evidence.py --json F.json  # machine-readable findings
```

Offline, no database, no network, Python standard library only. A gate whose job
is to say "no" must not depend on a package install that can fail; a gate that
cannot run tends to get skipped, and a skipped gate is the thing this contract
exists to catch.

## Two layers, and why

**Structural** — the versioned `.schema.json` files (JSON Schema draft 2020-12).
This is the cross-repo contract, mirrored byte-for-byte.

**Semantic** — cross-field invariants, reported as stable rule codes.

The second layer is not an afterthought. **JSON Schema cannot express two of the
five mandated negative cases.** It has no arithmetic, so it cannot check that a
declared kill rate matches the counts printed beside it; and it has no
comparison between fields, so it cannot check that an artifact's SHA equals the
source SHA. A repo that mirrored only the `.json` files would accept an
`arithmetic-mismatch` document silently — the document is perfectly well-formed.
It is simply false.

**The rule codes are contract; this implementation is not.** The sibling repo may
re-implement the semantic layer in any language, so long as the same document
yields the same code.

## Rule codes

| code | fires when |
|---|---|
| `E_SCHEMA` | Structural violation (type, enum, pattern, required, unknown property). |
| `E_UNSUPPORTED_SCHEMA` | The document declares no schema, or one this contract does not define. |
| `E_TREE_DIRTY` | Evidence was produced from a tree with uncommitted changes, so it describes code at no commit. |
| `E_STALE_SHA` | A command, proof, or CI status is recorded against a different SHA than the document is for. |
| `E_UNFINISHED` | Something claims an outcome but recorded no end time or completed exit status, so its completion cannot be verified. |
| `E_EXIT_STATUS_MISMATCH` | A completed pass/fail command's declared outcome contradicts its process exit status. |
| `E_COMMAND_GRAPH_MISMATCH` | A v2 Required-proof has an invalid canonical graph record or records differ from it. |
| `E_SKIP_WITHOUT_REASON` | A skip carries no machine-readable reason, making it indistinguishable from a gap. |
| `E_SKIPPED_AS_PASS` | A required command was skipped and the proof still declares pass. |
| `E_VERDICT_MISMATCH` | The declared verdict is not what the records derive. |
| `E_TAG_VERSION_MISMATCH` | The candidate tag does not name the version the tree ships. |
| `E_REQUIRED_CI_NOT_GREEN` | A required job is non-terminal or non-success. |
| `E_ARTIFACT_SHA_MISMATCH` | An artifact was recorded for another commit and offered as evidence for this one. |
| `E_RATE_MISMATCH` | The declared rate is not what the counts and denominator produce. |
| `E_SHARD_INCOMPLETE` | A shard did not finish, so the population was never fully evaluated. |
| `E_SURVIVOR_COUNT_MISMATCH` | Fewer survivor records than survivors claimed: a count, not a taxonomy. |
| `E_MISSING_WITNESS` | Fewer witnessed kills than kills claimed. |
| `E_DUPLICATE_MUTANT` | A mutant ID repeats within killed or surviving records, inflating the claimed population. |
| `E_MUTANT_POPULATION_OVERLAP` | A mutant ID is recorded as both killed and surviving. |
| `E_LIVE_CLAIM_WITHOUT_ARTIFACT` | A live claim points at no artifact. |
| `E_DEFECT_WITHOUT_BEAD` | A defect known at close time has no bead tracking it. |
| `E_SCOPED_TEST_CANNOT_MARK_READY` | A `ready` claim rests on a scoped test. |
| `E_INSUFFICIENT_READINESS_BASIS` | A `ready` claim rests on manual review. |

## A scoped test cannot mark a bead ready

This is the contract's headline rule, and it is **executable**, not advice.

A scoped test exercises the part of a change you were thinking about. It is real
evidence about that part, and it says **nothing at all** about the rest. Reading
a scoped green as "the bead is done" is how a bead gets closed over an untested
remainder — which is one of the documented root causes this epic was opened for.

So `bead-close-evidence/v1` makes readiness a *pair*: a `claim` and the `basis`
it rests on. The pairs are checked:

| basis | may claim `ready`? |
|---|---|
| `required-proof` | yes — the full Required graph ran at this SHA |
| `live-evidence` | yes — an exact-SHA live artifact exists |
| `scoped-test` | **no** → `E_SCOPED_TEST_CANNOT_MARK_READY` |
| `manual-review` | **no** → `E_INSUFFICIENT_READINESS_BASIS` |

Declaring `not-ready` on a scoped test is perfectly valid, and is the honest way
to record partial work. The rule only bites the claim, never the evidence.

A rule that lives only in prose is a rule someone has to remember. This epic
exists because that did not work.

## Design decisions worth knowing

**Nullable, then rejected.** `ended_at`, a completed command's `exit_code`, and
`known_defects[].bead_id` are nullable in the schema and rejected by a named
semantic rule where null would make a claim unverifiable. Forbidding null
structurally would also work, but it yields "expected string, got null". The
contract would rather say *"this command reports pass but never finished"*. An
unfinished command must be **representable** so it can be **named**.

**A skip is not a failure, and not a pass.** `outcome` is tri-state. A skipped
command legitimately has no end time — it never ran — so `E_UNFINISHED` exempts
skips. What it must carry is a machine-readable reason. An advisory skip does
not gate; a required skip means the proof cannot pass.

**A verdict is for the whole graph.** `required-proof/v2` carries a sorted
command-ID record and the SHA-256 of its compact JSON representation. The
offline validator rejects malformed records and missing, duplicate, or extra
records with `E_COMMAND_GRAPH_MISMATCH`. That in-document checksum is not an
authenticity boundary: the exact-SHA release consumer independently derives the
effective Required plan from the candidate's audited `.github/workflows/_quality.yml`
and compares IDs, tiers, and argv. A green subset therefore cannot become release
evidence by rewriting its own graph record and checksum.

**`release-candidate-proof.verdict` is `const: "pass"`.** The document exists
only for a candidate that passed, so a failing one is a contradiction rather than
a value. A failing candidate produces findings and no document. Producing this
document performs no tag, push, publish or registry mutation: it is a validation
record, not an authorisation.

**Unviable mutants are in no denominator.** A mutant that did not compile tested
nothing. The `denominator` is declared explicitly because the choice changes the
number, and an undeclared denominator is how a rate becomes unfalsifiable.

**Both kill witnesses are required.** A kill records that the mutant *failed* a
named test **and** that the same test *passes* on unmutated HEAD. Without the
second witness, a permanently-red test would "kill" every mutant it touched.

**Mutant populations are sets, not tallies.** Each `mutant_id` may occur once
in `kills` or once in `survivors`, never twice and never in both. Repeating a
witnessed kill must not inflate `counts.caught`, and a mutant cannot be both a
successful detection and an unclassified survivor.

**Every negative fixture produces exactly one finding.** Each is a valid document
with one defect planted in it. Demanding a single finding proves the rule fires
*for its declared reason* — "it was rejected" and "this rule rejected it" are
different claims, and only the second detects a rule going dead.

## Mirroring with oraclemcp

Each versioned `.schema.json` file is **byte-identical** across both repos. Two
consequences follow, and both are deliberate departures from `oraclemcp`'s
existing `schemas/` conventions:

- **`$id` is repo-neutral**: `urn:cross-repo-evidence-contract:vN:<name>`. A
  repo-scoped `$id` (`https://github.com/MuhDur/oraclemcp/...`) would make byte
  identity impossible by construction.
- **Extension keys carry no repo prefix**: `x-evidence-contract`, not
  `x-oraclemcp-*`. The repo name belongs in the payload's `repo` field, never in
  the schema.

Each schema is **self-contained** — local `$defs`, no cross-file `$ref` — so
mirroring is a file copy. Definitions shared between schemas (`sha1`,
`sourceRef`, `timestamp`, `nullableTimestamp`, `resourceBudget`, `artifactRef`)
must be identical in every file that has them; `check_shared_defs()` enforces
that, so schema versions cannot drift into incompatible dialects.

For a coordinated release, run `scripts/check_evidence_contract.sh
--mirror-root ../oraclemcp` from either checkout. It rejects a missing,
unexpected, or byte-different `*.schema.json` file while leaving normal
single-repository CI independent of a sibling checkout.

Changing a document shape is a new version. These documents are read by tooling
in two repos, so `v1` remains readable but frozen once a proof-producing command
depends on it. `required-proof/v2` and `release-candidate-proof/v2` add an
internally checked graph record and independent release-side comparison without
widening their v1 predecessors.

## Consumers

These beads import this contract rather than inventing their own shape:

| bead | uses |
|---|---|
| `f1cl.2` required-local-json-proof | emits `required-proof/v2` |
| `f1cl.3` verify-release-exact-sha | emits `release-candidate-proof/v2` |
| `f1cl.4` release-surface-manifest | `E_STALE_SHA` / surface drift |
| `f1cl.5` bead-close-evidence-audit | emits + audits `bead-close-evidence/v1` |
| `f1cl.6` entry-trace-tristate-e2e | the `pass`/`skip`+reason/`fail` tri-state; `integration_evidence.entry_points` |
| `f1cl.7` mutation gate | emits `mutation-result/v1` |
| `f1cl.9` resource-budget harness | `resource_budget` in `required-proof` and `mutation-result` |

Two seams are left open on purpose. `integration_evidence.entry_points` may be
empty with an `entry_points_excluded_reason`, but this contract does not yet
enforce which changes must have an entry point — that rule belongs to `f1cl.6`,
which owns the tri-state. And `source.branch` is recorded but branch policy
(e.g. "must be `main`") is enforced by `f1cl.3`, which knows the release branch.
Both are fields here and rules there, so the contract does not guess at
decisions its consumers own.
