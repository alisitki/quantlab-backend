#!/usr/bin/env bash
set -euo pipefail

BASE_DIR="$(cd "$(dirname "$0")" && pwd)"
ART_DIR="$BASE_DIR/artifacts"
SNAPSHOT_REPORT="$ART_DIR/snapshot_report.json"
TICK_REPORT="$ART_DIR/tick_report.json"
VALIDATION_REPORT="$ART_DIR/validation_report.json"
STRATEGY_FILE="$ART_DIR/strategy.json"

echo "[REPRO] Running snapshot eval"
node "$BASE_DIR/../../eval-run.js" \
  --mode snapshot \
  --strategy ema_cross \
  --symbol BTCUSDT \
  --date 2024-01-19 \
  --params '{"ema_fast":1,"ema_slow":3,"fastPeriod":1,"slowPeriod":3}' \
  --strategy_file "$STRATEGY_FILE" \
  --out "$SNAPSHOT_REPORT"

node - <<'NODE'
const fs = require('fs');
const expected = JSON.parse(fs.readFileSync('/home/deploy/quantlab-backend/services/strategyd/candidates/9bd0eb5615e13b1111b521cf77acc92fb6b3d1211338d4655b1c07b0c7788510/candidate.json', 'utf8'));
const report = JSON.parse(fs.readFileSync('/home/deploy/quantlab-backend/services/strategyd/candidates/9bd0eb5615e13b1111b521cf77acc92fb6b3d1211338d4655b1c07b0c7788510/artifacts/snapshot_report.json', 'utf8'));
if (report.determinism.state_hash !== expected.expected.snapshot.state_hash) {
  console.error('SNAPSHOT state_hash mismatch');
  process.exit(1);
}
if (report.determinism.fills_hash !== expected.expected.snapshot.fills_hash) {
  console.error('SNAPSHOT fills_hash mismatch');
  process.exit(1);
}
console.log('SNAPSHOT hash match');
NODE

if [ -f "$STRATEGY_FILE" ]; then
  EXPECTED_SHA=$(node -e "const c=require('/home/deploy/quantlab-backend/services/strategyd/candidates/9bd0eb5615e13b1111b521cf77acc92fb6b3d1211338d4655b1c07b0c7788510/candidate.json'); console.log(c.strategy_checksum || '');")
  ACTUAL_SHA=$(sha256sum "$STRATEGY_FILE" | awk '{print $1}')
  if [ "$EXPECTED_SHA" != "$ACTUAL_SHA" ]; then
    echo "STRATEGY checksum mismatch"
    exit 1
  fi
  echo "STRATEGY checksum match"
fi

if [ -f "$VALIDATION_REPORT" ]; then
  echo "[REPRO] Running tick eval"
  node "$BASE_DIR/../../eval-run.js" \
    --mode tick \
    --strategy ema_cross \
    --symbol BTCUSDT \
    --date 2024-01-19 \
    --params '{"ema_fast":1,"ema_slow":3,"fastPeriod":1,"slowPeriod":3}' \
    --strategy_file "$STRATEGY_FILE" \
    --out "$TICK_REPORT"

  node - <<'NODE'
const fs = require('fs');
const expected = JSON.parse(fs.readFileSync('/home/deploy/quantlab-backend/services/strategyd/candidates/9bd0eb5615e13b1111b521cf77acc92fb6b3d1211338d4655b1c07b0c7788510/candidate.json', 'utf8'));
const report = JSON.parse(fs.readFileSync('/home/deploy/quantlab-backend/services/strategyd/candidates/9bd0eb5615e13b1111b521cf77acc92fb6b3d1211338d4655b1c07b0c7788510/artifacts/tick_report.json', 'utf8'));
if (report.determinism.state_hash !== expected.expected.tick.state_hash) {
  console.error('TICK state_hash mismatch');
  process.exit(1);
}
if (report.determinism.fills_hash !== expected.expected.tick.fills_hash) {
  console.error('TICK fills_hash mismatch');
  process.exit(1);
}
console.log('TICK hash match');
NODE
fi
