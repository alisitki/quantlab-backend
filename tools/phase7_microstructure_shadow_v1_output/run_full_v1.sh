#!/usr/bin/env bash
set -uo pipefail

TIME_LOG="tools/phase7_microstructure_shadow_v1_output/evidence_pack/0020_full_run_time.log"
STDOUT_LOG="tools/phase7_microstructure_shadow_v1_output/evidence_pack/0020_full_run_live_stdout.log"
STDERR_LOG="tools/phase7_microstructure_shadow_v1_output/evidence_pack/0020_full_run_live_stderr.log"

/usr/bin/time -v -o "$TIME_LOG" \
  python3 tools/phase7_microstructure_shadow_validation_v1.py \
    --output-dir tools/phase7_microstructure_shadow_v1_output/full_run \
    --result-json tools/phase7_microstructure_shadow_result_v1.json \
    --report-md tools/phase7_microstructure_shadow_v1_output/phase7_microstructure_shadow_report_v1.md \
    --max-parallel 3 \
    --run-max-duration-sec 21600 \
    --per-run-timeout-sec 21900 \
    --subprocess-timeout-sec 22500 \
    --heartbeat-ms 5000 \
    --progress-interval-sec 60 \
    --max-continuation 3 \
    >"$STDOUT_LOG" 2>"$STDERR_LOG"

code=$?
printf 'exit_code=%s\n' "$code" >> "$TIME_LOG"
exit "$code"
