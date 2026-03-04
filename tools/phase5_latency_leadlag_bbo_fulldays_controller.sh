#!/usr/bin/env bash
set -euo pipefail

REPO="/home/deploy/quantlab-backend"
cd "$REPO"

python3 tools/phase5_latency_leadlag_bbo_fulldays_controller.py "$@"
