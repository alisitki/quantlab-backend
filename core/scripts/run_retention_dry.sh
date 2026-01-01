#!/bin/bash
set -euo pipefail

LOCKFILE="/tmp/quantlab_retention.lock"
APP_DIR="/home/deploy/quantlab/api/core"
LOGDIR="$APP_DIR/logs"
LOGFILE="$LOGDIR/retention_cron.log"

# Ensure log directory exists
mkdir -p "$LOGDIR"

# Lock protection - using FD 200 for lock maintenance
exec 200>"$LOCKFILE"
flock -n 200 || { echo "[$(date -Iseconds)] ABORT: Another instance running" >> "$LOGFILE"; exit 0; }

cd "$APP_DIR"

# Load .env for cron (cron does not have login shell environment)
if [ -f ".env" ]; then
  export $(grep -v '^#' .env | xargs)
fi

echo "[$(date -Iseconds)] START: Retention Dry-Run" >> "$LOGFILE"

# Run in DRY-RUN mode (no --apply). set +e to catch exit code reliably without script terminating.
set +e
node scheduler/cleanup_retention.js >> "$LOGFILE" 2>&1
EXIT_CODE=$?
set -e

if [ $EXIT_CODE -ne 0 ]; then
  echo "[$(date -Iseconds)] ERROR: Retention runner failed (exit=$EXIT_CODE)" >> "$LOGFILE"
  # Alert only on unexpected runner errors. Failure here shouldn't stop log end marking.
  node scheduler/alert_hook.js --type RETENTION_RUNNER_FAILURE --exit-code "$EXIT_CODE" --log-path "$LOGFILE" >> "$LOGFILE" 2>&1 || true
fi

echo "[$(date -Iseconds)] END: Retention Dry-Run (exit=$EXIT_CODE)" >> "$LOGFILE"
exit 0
