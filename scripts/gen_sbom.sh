#!/usr/bin/env bash
# W4-T3.3: generate supply-chain provenance artifacts deterministically (no
# external SBOM tooling required — derived from `cargo metadata` + the workspace).
# Produces, under docs/provenance/ (override with SBOM_OUT_DIR):
#   - cyclonedx.json     CycloneDX 1.5 SBOM of the published crates' non-dev
#                        dependency closure
#   - dependencies.tsv   human dep inventory (name, version, license, source)
#   - github-actions.tsv pinned GitHub Action inventory (action, ref, sha)
# Pass --check to fail if regenerating changes any committed artifact (drift).
set -uo pipefail

# Deterministic collation: inventory ordering must not depend on the
# generating machine's locale (see the same guard in gen_baseline.sh).
export LC_ALL=C

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT" || exit 2
OUT="${SBOM_OUT_DIR:-$ROOT/docs/provenance}"
CHECK=false
[ "${1:-}" = "--check" ] && CHECK=true
mkdir -p "$OUT"

command -v cargo >/dev/null 2>&1 || { echo "gen-sbom: missing cargo" >&2; exit 2; }
command -v jq >/dev/null 2>&1 || { echo "gen-sbom: missing jq" >&2; exit 2; }
command -v python3 >/dev/null 2>&1 || { echo "gen-sbom: missing python3" >&2; exit 2; }

# --- SBOM + dependency inventory (non-dev closure of the published crates) ---
# NOTE: the heredoc feeds the Python source on stdin, while process substitution
# passes the cargo-metadata JSON as argv[2]. This avoids temp-file cleanup in
# no-delete automation sessions.
python3 - "$OUT" <(cargo metadata --format-version 1 --all-features) <<'PY'
import json, sys
from pathlib import Path

out = Path(sys.argv[1])
md = json.load(open(sys.argv[2]))
packages = {p["id"]: p for p in md["packages"]}
nodes = {n["id"]: n for n in md["resolve"]["nodes"]}

PUBLISHED = {"oracledb", "oracledb-protocol", "oracledb-derive"}
roots = [p["id"] for p in md["packages"]
         if p["name"] in PUBLISHED and p.get("source") is None]

# Walk the resolve graph from the published roots following normal + build edges
# only (exclude dev-dependencies; they are not part of the shipped artifact).
reach, stack = set(), list(roots)
while stack:
    pid = stack.pop()
    if pid in reach:
        continue
    reach.add(pid)
    for dep in nodes.get(pid, {}).get("deps", []):
        kinds = {dk.get("kind") for dk in dep.get("dep_kinds", [])}
        if kinds & {None, "build"}:          # None == normal
            stack.append(dep["pkg"])

# Exclude the publish=false pyshim harness if it ever sneaks in.
def keep(pid):
    return packages[pid]["name"] != "oracledb-pyshim"

comps = sorted((packages[pid] for pid in reach if keep(pid)),
               key=lambda p: (p["name"], p["version"]))

# Dependency inventory TSV (deterministic).
inv = ["name\tversion\tlicense\tsource"]
for p in comps:
    src = p.get("source") or "local"
    inv.append(f'{p["name"]}\t{p["version"]}\t{p.get("license") or ""}\t{src}')
(out / "dependencies.tsv").write_text("\n".join(inv) + "\n")

# CycloneDX 1.5 SBOM (no timestamp/serial -> reproducible + diffable).
def comp(p):
    c = {
        "type": "library",
        "name": p["name"],
        "version": p["version"],
        "purl": f'pkg:cargo/{p["name"]}@{p["version"]}',
    }
    if p.get("license"):
        c["licenses"] = [{"license": {"name": p["license"]}}]
    return c

root = packages[roots[0]] if roots else {"name": "oracledb", "version": "0"}
bom = {
    "bomFormat": "CycloneDX",
    "specVersion": "1.5",
    "version": 1,
    "metadata": {"component": comp(root)},
    "components": [comp(p) for p in comps if p["name"] != root["name"]],
}
(out / "cyclonedx.json").write_text(json.dumps(bom, indent=2, sort_keys=True) + "\n")
print(f"gen-sbom: {len(comps)} components in the published non-dev closure")
PY

# --- Pinned GitHub Action inventory (supply-chain: every external `uses:` is
# SHA-pinned). Local reusable workflows (uses: ./...) are excluded — they are
# in-repo references, not pinnable external actions. ---
{
  echo -e "action\tpinned_ref"
  grep -rhoE 'uses:[[:space:]]*[^[:space:]]+' .github/workflows/ \
    | sed -E 's/uses:[[:space:]]*//' \
    | grep -v '^\./' \
    | sort -u \
    | while IFS= read -r u; do
        printf '%s\t%s\n' "${u%@*}" "${u##*@}"
      done
} > "$OUT/github-actions.tsv"

# Flag any external action NOT pinned to a 40-char commit SHA (tag/branch pins are weaker).
unpinned="$(awk -F '\t' 'NR>1 && $2 !~ /^[0-9a-f]{40}$/ {print $1" -> "$2}' "$OUT/github-actions.tsv")"
if [ -n "$unpinned" ]; then
  echo "gen-sbom: NOTE — GitHub Actions not pinned to a 40-hex commit SHA:" >&2
  printf '  %s\n' "$unpinned" >&2
fi

if [ "$CHECK" = true ]; then
  if ! git -C "$ROOT" diff --exit-code -- "$(realpath --relative-to="$ROOT" "$OUT")" >/dev/null 2>&1; then
    echo "gen-sbom: --check failed; provenance artifacts are stale, re-run scripts/gen_sbom.sh" >&2
    git -C "$ROOT" diff --stat -- "$(realpath --relative-to="$ROOT" "$OUT")" >&2
    exit 1
  fi
fi

echo "gen-sbom: wrote $OUT/{cyclonedx.json,dependencies.tsv,github-actions.tsv}"
