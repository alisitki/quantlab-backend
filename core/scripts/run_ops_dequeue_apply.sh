#!/bin/bash
# scripts/run_ops_dequeue_apply.sh: Apply runner for ops dequeue outbox (Telegram) with failure visibility.
set -euo pipefail

APP_DIR="/home/deploy/quantlab/core"
LOGDIR="$APP_DIR/logs"
mkdir -p "$LOGDIR"

LOGFILE="$LOGDIR/ops_dequeue_apply_cron.log"
LOCKFILE="/tmp/quantlab_ops_dequeue_apply.lock"
FAILURES="$APP_DIR/ops/outbox/failures.jsonl"

# Locking: FD 200
exec 200>"$LOCKFILE"
if ! flock -n 200; then
    echo "[$(date -Iseconds)] ABORT: Another instance running" >> "$LOGFILE"
    exit 0
fi

echo "[$(date -Iseconds)] START: Ops Dequeue APPLY (telegram)" >> "$LOGFILE"

cd "$APP_DIR"

# Load .env for cron (cron does not have login shell environment)
if [ -f ".env" ]; then
  export $(grep -v '^#' .env | xargs)
fi

# Record failures.jsonl line count (missing => 0)
BEFORE=0
if [ -f "$FAILURES" ]; then
  BEFORE=$(wc -l < "$FAILURES" | tr -d ' ')
fi

set +e
node scheduler/dequeue_ops_outbox.js --apply --adapter telegram --max 20 >> "$LOGFILE" 2>&1
EXIT_CODE=$?
set -e

AFTER=0
if [ -f "$FAILURES" ]; then
  AFTER=$(wc -l < "$FAILURES" | tr -d ' ')
fi

# Hard failure path (exit code != 0)
if [ $EXIT_CODE -ne 0 ]; then
  echo "[$(date -Iseconds)] ERROR: dequeue runner exit=$EXIT_CODE" >> "$LOGFILE"
  node scheduler/alert_hook.js --type OPS_DEQUEUE_FAILURE --exit-code "$EXIT_CODE" >> "$LOGFILE" 2>&1 || true
  echo "[$(date -Iseconds)] END: Ops Dequeue APPLY (exit=$EXIT_CODE)" >> "$LOGFILE"
  exit 0
fi

# Soft failure path (exit 0 but failures grew)
if [ "$AFTER" -gt "$BEFORE" ]; then
  echo "[$(date -Iseconds)] SOFT-FAIL: failures.jsonl grew ($BEFORE -> $AFTER)" >> "$LOGFILE"
  # Use exit code 2 for soft failures
  node scheduler/alert_hook.js --type OPS_DEQUEUE_FAILURE --exit-code 2 >> "$LOGFILE" 2>&1 || true
fi

echo "[$(date -Iseconds)] END: Ops Dequeue APPLY (exit=$EXIT_CODE failures=$BEFORE->$AFTER)" >> "$LOGFILE"
exit 0
