#!/bin/bash
# scripts/run_ops_dequeue.sh: Dry-run runner for ops dequeue outbox.
set -euo pipefail

APP_DIR="/home/deploy/quantlab/core"
LOGDIR="$APP_DIR/logs"
mkdir -p "$LOGDIR"

LOGFILE="$LOGDIR/ops_dequeue_cron.log"
LOCKFILE="/tmp/quantlab_ops_dequeue.lock"

# Locking: FD 200
exec 200>"$LOCKFILE"
if ! flock -n 200; then
    # Silent exit on lock collision
    exit 0
fi

echo "[$(date -Iseconds)] START: Ops Dequeue (Dry-run)" >> "$LOGFILE"

cd "$APP_DIR"

# Load .env for cron (cron does not have login shell environment)
if [ -f ".env" ]; then
  export $(grep -v '^#' .env | xargs)
fi

set +e
node scheduler/dequeue_ops_outbox.js --adapter stub --max 20 >> "$LOGFILE" 2>&1
EXIT=$?
set -e

if [ $EXIT -ne 0 ]; then
    echo "[$(date -Iseconds)] ERROR: Script failed with exit code $EXIT" >> "$LOGFILE"
    node scheduler/alert_hook.js --type OPS_DEQUEUE_FAILURE --exit-code "$EXIT" >> "$LOGFILE" 2>&1 || true
fi

echo "[$(date -Iseconds)] END: Exit $EXIT" >> "$LOGFILE"
exit 0
