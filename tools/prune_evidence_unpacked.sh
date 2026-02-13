#!/usr/bin/env bash
set -euo pipefail

usage() {
  echo "usage: $0 [--dry-run]" >&2
}

DRY_RUN=0
if [[ "${1:-}" == "--dry-run" ]]; then
  DRY_RUN=1
  shift
fi
if [[ $# -ne 0 ]]; then
  usage
  exit 2
fi

REPO="/home/deploy/quantlab-backend"
EVIDENCE_ROOT="${REPO}/evidence"

if [[ ! -d "$EVIDENCE_ROOT" ]]; then
  echo "EVIDENCE_ROOT_MISSING ${EVIDENCE_ROOT}"
  echo "deleted_dirs=0"
  echo "orphan_dirs=0"
  exit 0
fi

has_triple_for_dir() {
  local base="$1"
  local exact_prefix="${EVIDENCE_ROOT}/${base}"

  if [[ -f "${exact_prefix}.tar.gz" && -f "${exact_prefix}.tar.gz.sha256" && -f "${exact_prefix}.moved_to.txt" ]]; then
    return 0
  fi

  shopt -s nullglob
  local tar
  for tar in "${EVIDENCE_ROOT}/${base}"*.tar.gz; do
    local prefix="${tar%.tar.gz}"
    local pref_base
    pref_base="$(basename "$prefix")"
    if [[ "$pref_base" == "$base" || "$pref_base" == "$base"-* || "$pref_base" == "$base"_* || "$pref_base" == "$base".* ]]; then
      if [[ -f "${prefix}.tar.gz.sha256" && -f "${prefix}.moved_to.txt" ]]; then
        shopt -u nullglob
        return 0
      fi
    fi
  done
  shopt -u nullglob
  return 1
}

declare -a unpacked_dirs=()
while IFS= read -r d; do
  unpacked_dirs+=("$d")
done < <(find "$EVIDENCE_ROOT" -maxdepth 1 -mindepth 1 -type d | sort)

declare -a orphan_dirs=()
declare -a safe_dirs=()
for d in "${unpacked_dirs[@]}"; do
  b="$(basename "$d")"
  if has_triple_for_dir "$b"; then
    safe_dirs+=("$d")
  else
    orphan_dirs+=("$d")
  fi
done

if [[ "${#orphan_dirs[@]}" -gt 0 ]]; then
  for d in "${orphan_dirs[@]}"; do
    echo "ORPHAN_UNPACKED ${d}"
  done
  echo "deleted_dirs=0"
  echo "orphan_dirs=${#orphan_dirs[@]}"
  exit 2
fi

deleted_dirs=0
for d in "${safe_dirs[@]}"; do
  echo "DELETE_UNPACKED_CANDIDATE ${d}"
  if [[ "$DRY_RUN" -eq 0 ]]; then
    rm -rf -- "$d"
  fi
  deleted_dirs=$((deleted_dirs + 1))
done

echo "deleted_dirs=${deleted_dirs}"
echo "orphan_dirs=0"

