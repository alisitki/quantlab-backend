#!/usr/bin/env bash
set -euo pipefail

ROOT="/home/deploy/quantlab-backend"
cd "$ROOT"

OUT_ROOT="tools/phase7_microstructure_feeaware_output"
mkdir -p "$OUT_ROOT/evidence_pack"

stdout="$OUT_ROOT/evidence_pack/0001_full_run_live_stdout.log"
stderr="$OUT_ROOT/evidence_pack/0001_full_run_live_stderr.log"
timelog="$OUT_ROOT/evidence_pack/0001_full_run_live_time.log"

/usr/bin/time -v -o "$timelog" python3 tools/phase7_microstructure_feeaware_v0.py \
  --mode full \
  --output-dir tools/phase7_microstructure_feeaware_output/full_linkusdt \
  --result-json tools/phase7_microstructure_feeaware_v0.json \
  --report-md tools/phase7_microstructure_feeaware_output/phase7_microstructure_feeaware_report_v0.md \
  --run-max-duration-sec 21600 \
  --per-run-timeout-sec 21900 \
  --heartbeat-ms 5000 \
  --progress-interval-sec 60 >"$stdout" 2>"$stderr"
