#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat >&2 <<'USAGE'
usage: tools/prepare-mini-fullscan-campaign.sh \
  --exchange <exchange> --stream <stream> --start <YYYYMMDD> --end <YYYYMMDD> \
  [--max-symbols <n>] [--core-symbols <csv>] [--per-run-timeout-min <n>] \
  [--state-bucket <bucket>] [--state-key <key>] [--archive-root <path>] [--run-id <id>]
USAGE
}

EXCHANGE=""
STREAM=""
START=""
END=""
MAX_SYMBOLS="20"
CORE_SYMBOLS="btcusdt,ethusdt,solusdt,xrpusdt"
PER_RUN_TIMEOUT_MIN="12"
STATE_BUCKET="${S3_COMPACT_BUCKET:-quantlab-compact}"
STATE_KEY="${S3_COMPACT_STATE_KEY:-compacted/_state.json}"
ARCHIVE_ROOT="/home/deploy/quantlab-evidence-archive/$(date -u +%Y%m%d)_slim"
RUN_ID=""

while [[ $# -gt 0 ]]; do
  case "$1" in
    --exchange) EXCHANGE="$2"; shift 2;;
    --stream) STREAM="$2"; shift 2;;
    --start) START="$2"; shift 2;;
    --end) END="$2"; shift 2;;
    --max-symbols) MAX_SYMBOLS="$2"; shift 2;;
    --core-symbols) CORE_SYMBOLS="$2"; shift 2;;
    --per-run-timeout-min) PER_RUN_TIMEOUT_MIN="$2"; shift 2;;
    --state-bucket) STATE_BUCKET="$2"; shift 2;;
    --state-key) STATE_KEY="$2"; shift 2;;
    --archive-root) ARCHIVE_ROOT="$2"; shift 2;;
    --run-id) RUN_ID="$2"; shift 2;;
    -h|--help) usage; exit 0;;
    *) echo "unknown arg: $1" >&2; usage; exit 2;;
  esac
done

if [[ -z "$EXCHANGE" || -z "$STREAM" || -z "$START" || -z "$END" ]]; then
  usage
  exit 2
fi

if [[ -z "$RUN_ID" ]]; then
  RUN_ID="multi-hypothesis-phase5-mini-fullscan-${STREAM}-$(date -u +%Y%m%d_%H%M%S)__FULLSCAN_MAJOR"
fi

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
PACK_DIR="${REPO_ROOT}/evidence/${RUN_ID}"
STATE_DIR="${PACK_DIR}/state_selection"
STATE_JSON="/tmp/compacted__state.json"
MERGED_OBJECT_KEYS_TSV="${STATE_DIR}/object_keys_selected.tsv"
SYMBOLS_FILE="${PACK_DIR}/symbols_selected.txt"
CAMPAIGN_META="${PACK_DIR}/campaign_meta.tsv"
ENV_OUT="/tmp/fullscan_mini_env.sh"

mkdir -p "${PACK_DIR}/runs" "${STATE_DIR}"

python3 /tmp/s3_compact_tool.py get "${STATE_BUCKET}" "${STATE_KEY}" "${STATE_JSON}" >/dev/null

python3 "${REPO_ROOT}/tools/state_selection_from_compacted_state.py" \
  --state-json "${STATE_JSON}" \
  --exchange "${EXCHANGE}" \
  --stream "${STREAM}" \
  --start "${START}" \
  --end "${END}" \
  --max-symbols "${MAX_SYMBOLS}" \
  --core-symbols "${CORE_SYMBOLS}" \
  --bucket "${STATE_BUCKET}" \
  --output-tsv "${MERGED_OBJECT_KEYS_TSV}" \
  --selected-symbols-out "${SYMBOLS_FILE}"

SYMBOL_COUNT="$(wc -l < "${SYMBOLS_FILE}" | tr -d ' ')"
SYMBOLS_CSV="$(paste -sd, "${SYMBOLS_FILE}")"

{
  printf 'run_id\tcategory\texchange\tstream\tstart\tend\tsymbol_count\tsymbols_csv\tobject_keys_tsv_path\tper_run_timeout_min\tselection_strategy\tstate_bucket\tstate_key\tstate_json\n'
  printf '%s\tFULLSCAN_MAJOR\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\tstate_from_compacted_json\t%s\t%s\t%s\n' \
    "${RUN_ID}" "${EXCHANGE}" "${STREAM}" "${START}" "${END}" "${SYMBOL_COUNT}" "${SYMBOLS_CSV}" "${MERGED_OBJECT_KEYS_TSV}" "${PER_RUN_TIMEOUT_MIN}" "${STATE_BUCKET}" "${STATE_KEY}" "${STATE_JSON}"
} > "${CAMPAIGN_META}"

cat > "${ENV_OUT}" <<EOF
RUN_ID=${RUN_ID}
PACK_DIR=${PACK_DIR}
ARCHIVE_ROOT=${ARCHIVE_ROOT}
MERGED_OBJECT_KEYS_TSV=${MERGED_OBJECT_KEYS_TSV}
START=${START}
END=${END}
PER_RUN_TIMEOUT_MIN=${PER_RUN_TIMEOUT_MIN}
SYMBOLS_CSV=${SYMBOLS_CSV}
EOF

echo "RUN_ID=${RUN_ID}"
echo "PACK_DIR=${PACK_DIR}"
echo "MERGED_OBJECT_KEYS_TSV=${MERGED_OBJECT_KEYS_TSV}"
echo "SYMBOL_COUNT=${SYMBOL_COUNT}"
echo "SYMBOLS_SELECTED=${SYMBOLS_CSV}"
echo "CAMPAIGN_META=${CAMPAIGN_META}"
echo "ENV_OUT=${ENV_OUT}"
