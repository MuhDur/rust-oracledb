#!/usr/bin/env bash
#
# extract_reference_gates.sh — enumerate every version/capability gate in the
# vendored python-oracledb thin protocol (reference/python-oracledb/.../thin).
#
# A "gate" is a point where the reference conditions a wire field (a byte it
# reads or writes) on a negotiated capability or protocol/field version:
#
#     if buf._caps.ttc_field_version >= TNS_CCAP_FIELD_VERSION_23_1: ...
#     if self._caps.protocol_version  >= TNS_VERSION_MIN_LARGE_SDU:  ...
#     if buf._caps.supports_end_of_response:                         ...
#
# Every such point in our port must branch on the SAME capability with the SAME
# constant and direction, or the emitted/parsed bytes diverge on some server
# version (the class of bug tracked by epic rust-oracledb-xver-parity-so3w).
#
# The reference is a maintained external (new gates such as 23_1_EXT_3 / 23_4
# keep appearing), so this script is the re-runnable spine of the L1 audit:
#
#   scripts/extract_reference_gates.sh            # print the live inventory (TSV)
#   scripts/extract_reference_gates.sh --check    # diff live inventory against
#                                                 # docs/reference-gates.tsv;
#                                                 # exit 3 if a gate appeared or
#                                                 # disappeared (i.e. the mapping
#                                                 # table needs revisiting)
#
# Columns (tab-separated):
#   reference_file            path under impl/thin (e.g. messages/execute.pyx)
#   line                      line number of the gate
#   capability_or_constant    the predicate (constant name or supports_* flag)
#   direction                 ">=" | "<" | "flag" (boolean capability test)
#   conditional_note          the trailing comment / first guarded line (what
#                             byte/field the gate controls)
#
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
thin_dir="${repo_root}/reference/python-oracledb/src/oracledb/impl/thin"
inventory="${repo_root}/docs/reference-gates.tsv"

if [[ ! -d "${thin_dir}" ]]; then
  echo "reference thin dir not found: ${thin_dir}" >&2
  exit 2
fi

emit_inventory() {
  # grep every gate line, then for each derive the constant, direction and a
  # short note (trailing comment on the gate line, else the comment on the next
  # non-blank guarded line). Deterministic sort so diffs are stable.
  grep -rnE \
    '(ttc_field_version|protocol_version)[[:space:]]*(>=|<|>)[[:space:]]*TNS_|_caps\.supports_[a-z_]+|supports_bool[[:space:]]*=' \
    "${thin_dir}" \
  | grep -vE 'cdef|bint |def _adjust|self\.supports_[a-z_]+ =|self\.supports_[a-z_]+=|supports_[a-z_]+ = (True|False)|supports_[a-z_]+ = supports' \
  | while IFS= read -r hit; do
      file="${hit%%:*}"
      rest="${hit#*:}"
      line="${rest%%:*}"
      code="${rest#*:}"
      rel="${file#"${thin_dir}"/}"

      # capability + direction
      if [[ "${code}" =~ (ttc_field_version|protocol_version)[[:space:]]*(\>=|\<|\>)[[:space:]]*(TNS_[A-Z0-9_]+) ]]; then
        cap="${BASH_REMATCH[3]}"
        dir="${BASH_REMATCH[2]}"
      elif [[ "${code}" =~ supports_bool[[:space:]]*= ]]; then
        cap="supports_bool<-TNS_CCAP_FIELD_VERSION_23_1"
        dir=">="
      elif [[ "${code}" =~ (supports_[a-z_]+) ]]; then
        cap="${BASH_REMATCH[1]}"
        dir="flag"
      else
        cap="?"
        dir="?"
      fi

      # note: trailing comment on the gate line, else next guarded line's comment
      note="$(sed -n "${line}p" "${file}" | sed -n 's/.*#[[:space:]]*\(.*\)$/\1/p')"
      if [[ -z "${note}" ]]; then
        note="$(sed -n "$((line+1)),$((line+1))p" "${file}" \
                | sed -e 's/^[[:space:]]*//' -e 's/[[:space:]]*$//')"
      fi
      [[ -z "${note}" ]] && note="(see source)"

      printf '%s\t%s\t%s\t%s\t%s\n' "${rel}" "${line}" "${cap}" "${dir}" "${note}"
    done \
  | sort -t$'\t' -k1,1 -k2,2n
}

header=$'reference_file\tline\tcapability_or_constant\tdirection\tconditional_note'

if [[ "${1:-}" == "--check" ]]; then
  if [[ ! -f "${inventory}" ]]; then
    echo "inventory not found: ${inventory} (run without --check to seed it)" >&2
    exit 2
  fi
  # compare on stable gate identity only: reference_file + line + capability.
  # the note column is derived (and the committed file adds mapping columns),
  # so a reworded comment or a new mapping must not trip the drift check — only
  # a gate that appeared, vanished or changed its capability/version should.
  live="$(emit_inventory | cut -f1-3)"
  recorded="$(grep -vE '^[[:space:]]*#' "${inventory}" | tail -n +2 | cut -f1-3 | sort -t$'\t' -k1,1 -k2,2n)"
  if diff <(printf '%s\n' "${recorded}") <(printf '%s\n' "${live}") >/tmp/reference-gates.diff 2>&1; then
    echo "OK: docs/reference-gates.tsv covers every reference gate ($(printf '%s\n' "${live}" | grep -c . ) gates)."
    exit 0
  else
    echo "DRIFT: reference gates changed — update docs/reference-gates.tsv + coverage." >&2
    echo "  (< recorded, > live)" >&2
    cat /tmp/reference-gates.diff >&2
    exit 3
  fi
fi

printf '%s\n' "${header}"
emit_inventory
