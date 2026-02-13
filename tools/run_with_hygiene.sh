#!/usr/bin/env bash
set -euo pipefail

usage() {
  echo "usage: $0 -- <command> [args...]" >&2
}

if [[ $# -lt 1 ]]; then
  usage
  exit 2
fi

if [[ "${1:-}" == "--" ]]; then
  shift
fi
if [[ $# -lt 1 ]]; then
  usage
  exit 2
fi

REPO="/home/deploy/quantlab-backend"
cd "$REPO"

# Start hygiene is non-fatal by policy.
if [[ -x tools/prune_curated_cache.sh ]]; then
  tools/prune_curated_cache.sh || true
fi

set +e
"$@"
CMD_EC=$?
set -e

UNPACKED_EC=0
KEEP_EC=0
if [[ -x tools/prune_evidence_unpacked.sh ]]; then
  tools/prune_evidence_unpacked.sh || UNPACKED_EC=$?
fi
if [[ -x tools/prune_evidence_keep_last.sh ]]; then
  tools/prune_evidence_keep_last.sh || KEEP_EC=$?
fi

if [[ $CMD_EC -ne 0 ]]; then
  exit "$CMD_EC"
fi
if [[ $UNPACKED_EC -ne 0 ]]; then
  exit "$UNPACKED_EC"
fi
if [[ $KEEP_EC -ne 0 ]]; then
  exit "$KEEP_EC"
fi

exit 0

