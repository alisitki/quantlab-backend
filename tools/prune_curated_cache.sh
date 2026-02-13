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

CURATED_ROOT="${QUANTLAB_CURATED_ROOT:-/home/deploy/quantlab-cache/curated}"
CURATED_TTL_DAYS="${CURATED_TTL_DAYS:-7}"
CURATED_MAX_GB="${CURATED_MAX_GB:-30}"

if ! [[ "$CURATED_TTL_DAYS" =~ ^[0-9]+$ ]]; then
  echo "invalid CURATED_TTL_DAYS=${CURATED_TTL_DAYS}" >&2
  exit 3
fi
if ! [[ "$CURATED_MAX_GB" =~ ^[0-9]+$ ]]; then
  echo "invalid CURATED_MAX_GB=${CURATED_MAX_GB}" >&2
  exit 4
fi

bytes_of_root() {
  local root="$1"
  if [[ -d "$root" ]]; then
    du -sb "$root" 2>/dev/null | awk '{print $1}'
  else
    echo "0"
  fi
}

before_bytes="$(bytes_of_root "$CURATED_ROOT")"
deleted_count=0
deleted_ttl=0
deleted_size=0

if [[ ! -d "$CURATED_ROOT" ]]; then
  echo "ROOT_MISSING ${CURATED_ROOT}"
  echo "before_bytes=${before_bytes}"
  echo "after_bytes=${before_bytes}"
  echo "deleted_count=0"
  exit 0
fi

echo "CURATED_ROOT=${CURATED_ROOT}"
echo "DRY_RUN=${DRY_RUN}"
echo "CURATED_TTL_DAYS=${CURATED_TTL_DAYS}"
echo "CURATED_MAX_GB=${CURATED_MAX_GB}"

while IFS= read -r -d '' p; do
  echo "TTL_PRUNE_CANDIDATE ${p}"
  if [[ "$DRY_RUN" -eq 0 ]] && [[ -e "$p" || -L "$p" ]]; then
    rm -rf -- "$p"
    deleted_count=$((deleted_count + 1))
    deleted_ttl=$((deleted_ttl + 1))
  elif [[ "$DRY_RUN" -eq 1 ]]; then
    deleted_count=$((deleted_count + 1))
    deleted_ttl=$((deleted_ttl + 1))
  fi
done < <(find "$CURATED_ROOT" -mindepth 1 -mtime +"$CURATED_TTL_DAYS" -print0 | sort -z)

cap_bytes=$((CURATED_MAX_GB * 1024 * 1024 * 1024))
current_bytes="$(bytes_of_root "$CURATED_ROOT")"
sim_bytes="$current_bytes"

if (( sim_bytes > cap_bytes )); then
  while IFS= read -r -d '' rec; do
    path="${rec#*$'\t'}"
    if [[ ! -e "$path" && ! -L "$path" ]]; then
      continue
    fi
    path_bytes="$(du -sb "$path" 2>/dev/null | awk '{print $1}')"
    path_bytes="${path_bytes:-0}"
    echo "SIZE_PRUNE_CANDIDATE ${path} bytes=${path_bytes}"
    if [[ "$DRY_RUN" -eq 0 ]]; then
      rm -rf -- "$path"
      current_bytes="$(bytes_of_root "$CURATED_ROOT")"
    else
      current_bytes=$((current_bytes - path_bytes))
      if (( current_bytes < 0 )); then
        current_bytes=0
      fi
    fi
    deleted_count=$((deleted_count + 1))
    deleted_size=$((deleted_size + 1))
    if (( current_bytes <= cap_bytes )); then
      break
    fi
  done < <(find "$CURATED_ROOT" -mindepth 1 -maxdepth 1 -printf '%T@\t%p\0' | sort -z -n)
fi

after_bytes="$(bytes_of_root "$CURATED_ROOT")"
echo "before_bytes=${before_bytes}"
echo "after_bytes=${after_bytes}"
echo "deleted_count=${deleted_count}"
echo "deleted_ttl=${deleted_ttl}"
echo "deleted_size=${deleted_size}"

