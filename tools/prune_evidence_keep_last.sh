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

KEEP_N="${EVIDENCE_KEEP_N:-50}"
if ! [[ "$KEEP_N" =~ ^[0-9]+$ ]]; then
  echo "invalid EVIDENCE_KEEP_N=${KEEP_N}" >&2
  exit 3
fi

REPO="/home/deploy/quantlab-backend"
EVIDENCE_ROOT="${REPO}/evidence"

if [[ ! -d "$EVIDENCE_ROOT" ]]; then
  echo "EVIDENCE_ROOT_MISSING ${EVIDENCE_ROOT}"
  echo "slim_triples_kept=0"
  echo "slim_triples_deleted=0"
  echo "skipped_incomplete=0"
  exit 0
fi

declare -a candidates=()
declare -a skipped=()

shopt -s nullglob
for tar in "${EVIDENCE_ROOT}"/*.tar.gz; do
  prefix="${tar%.tar.gz}"
  sha="${prefix}.tar.gz.sha256"
  moved="${prefix}.moved_to.txt"
  if [[ -f "$sha" && -f "$moved" ]]; then
    base="$(basename "$prefix")"
    epoch=""
    if [[ "$base" =~ ([0-9]{8})_([0-9]{6}) ]]; then
      ymd="${BASH_REMATCH[1]}"
      hms="${BASH_REMATCH[2]}"
      d="${ymd:0:4}-${ymd:4:2}-${ymd:6:2} ${hms:0:2}:${hms:2:2}:${hms:4:2} UTC"
      epoch="$(date -u -d "$d" +%s 2>/dev/null || true)"
    fi
    if [[ -z "$epoch" ]]; then
      epoch="$(stat -c %Y "$tar")"
    fi
    candidates+=("$(printf "%014d\t%s" "$epoch" "$prefix")")
  else
    skipped+=("$tar")
  fi
done
shopt -u nullglob

IFS=$'\n' sorted=($(printf "%s\n" "${candidates[@]}" | sort -r))
unset IFS

kept=0
deleted=0

for i in "${!sorted[@]}"; do
  rec="${sorted[$i]}"
  prefix="${rec#*$'\t'}"
  if (( i < KEEP_N )); then
    echo "KEEP_SLIM_PREFIX ${prefix}"
    kept=$((kept + 1))
    continue
  fi
  echo "DELETE_SLIM_PREFIX ${prefix}"
  if [[ "$DRY_RUN" -eq 0 ]]; then
    rm -f -- "${prefix}.tar.gz" "${prefix}.tar.gz.sha256" "${prefix}.moved_to.txt"
  fi
  deleted=$((deleted + 1))
done

for s in "${skipped[@]}"; do
  echo "SKIP_INCOMPLETE_TRIPLE ${s}"
done

echo "slim_triples_kept=${kept}"
echo "slim_triples_deleted=${deleted}"
echo "skipped_incomplete=${#skipped[@]}"

