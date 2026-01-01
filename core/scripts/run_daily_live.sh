#!/bin/bash
# run_daily_live.sh: Production cron wrapper for daily ML LIVE runs.
# 
# Features:
#   - STATE-GATED: Checks compact readiness before running
#   - Lockfile prevents overlapping runs
#   - Logs stdout/stderr to file
#   - Triggers alert on failure (not on SKIP)
#
# Cron example:
#   0 2 * * * /home/deploy/quantlab-backend/core/scripts/run_daily_live.sh >> /home/deploy/quantlab-backend/core/logs/cron_daily.log 2>&1

set -uo pipefail

WORKDIR="/home/deploy/quantlab-backend/core"
LOCKFILE="/tmp/quantlab_daily_live.lock"
LOGDIR="$WORKDIR/logs"
SYMBOL="${1:-btcusdt}"

# Compute target date (yesterday UTC)
TARGET_DATE=$(date -u -d yesterday +%Y%m%d)

# Ensure log directory
mkdir -p "$LOGDIR"

# Lockfile: prevent overlapping runs
exec 200>"$LOCKFILE"
if ! flock -n 200; then
    echo "[$(date -Iseconds)] ABORT: Another instance running (lockfile: $LOCKFILE)"
    exit 1
fi

echo "============================================================"
echo "[$(date -Iseconds)] Daily LIVE Cron - Target: $TARGET_DATE ($SYMBOL)"
echo "============================================================"

cd "$WORKDIR"

# ─────────────────────────────────────────────────────────────────
# PHASE 1: Check compact state readiness
# ─────────────────────────────────────────────────────────────────
echo "[$(date -Iseconds)] Checking compact readiness for $TARGET_DATE..."
READINESS=$(node scheduler/check_compact_ready.js --date "$TARGET_DATE" 2>&1 | head -1)

if [ "$READINESS" = "FAILURE" ]; then
    echo "[$(date -Iseconds)] FAILURE: Compact state missing or invalid"
    node scheduler/alert_hook.js --type STATE_MISSING --symbol "$SYMBOL" --date "$TARGET_DATE" || true
    exit 1
fi

if [ "$READINESS" = "NOT_READY" ]; then
    echo "[$(date -Iseconds)] SKIP: Compact not ready for $TARGET_DATE (no alert)"
    echo "============================================================"
    exit 0  # NOT a failure - just skip
fi

if [ "$READINESS" != "READY" ]; then
    echo "[$(date -Iseconds)] UNEXPECTED: Got '$READINESS' from readiness check"
    node scheduler/alert_hook.js --type CRON_FAILURE --symbol "$SYMBOL" --message "Unexpected readiness: $READINESS" || true
    exit 1
fi

# ─────────────────────────────────────────────────────────────────
# PHASE 2: Execute ML training
# ─────────────────────────────────────────────────────────────────
echo "[$(date -Iseconds)] READY: Proceeding with ML run for $TARGET_DATE"

PSEUDO_PROBA=0 node scheduler/run_daily_prod.js \
    --mode live \
    --symbol "$SYMBOL" \
    --date "$TARGET_DATE" \
    --ensure-features

EXIT_CODE=$?

echo "============================================================"
echo "[$(date -Iseconds)] END daily LIVE run (exit=$EXIT_CODE)"
echo "============================================================"

# Trigger alert on failure
if [ $EXIT_CODE -ne 0 ]; then
    echo "[$(date -Iseconds)] ALERT: Triggering failure alert..."
    node scheduler/alert_hook.js --type CRON_FAILURE --exit-code "$EXIT_CODE" --symbol "$SYMBOL" --date "$TARGET_DATE" || true
fi

exit $EXIT_CODE
