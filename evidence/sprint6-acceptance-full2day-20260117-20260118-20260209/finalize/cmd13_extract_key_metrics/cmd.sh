set -euo pipefail
EVD_ROOT="evidence/sprint6-acceptance-full2day-20260117-20260118-20260209"

echo "== summary.json =="
jq -r '.status as $s | "status=\($s)"' "$EVD_ROOT/summary.json"
jq -r '.window | "target_window=\(.start)-\(.end)"' "$EVD_ROOT/summary.json"

echo ""
echo "== candidates (top3) =="
# Print only rank,start,end,rows_total
awk -F'\t' 'NR==1{next} NR==2{print $0; next} NR>2{print $1"\t"$2"\t"$3"\t"$4}' "$EVD_ROOT/inventory/smoke_aday_listesi.txt"

echo ""
echo "== candidate sha256 proof =="
sed -n '1,120p' "$EVD_ROOT/inventory/candidate_sha256_proof.txt"

echo ""
echo "== selected target inputs sha256 proof =="
sed -n '1,120p' "$EVD_ROOT/sha256/meta_sha256_proof.txt"

echo ""
echo "== smoke attempts summary (from summary.json) =="
jq -r '.smoke_sweep.attempts[] | "try=\(.try) window=\(.window.start)-\(.window.end) exit=\(.exit) patterns_scanned=\(.patterns_scanned) wall_s=\(.wall_s) max_rss_kb=\(.max_rss_kb)"' "$EVD_ROOT/summary.json"

echo ""
echo "== integrity check result =="
rg -n "INTEGRITY_CHECK:" "$EVD_ROOT/finalize/cmd11_integrity_check/stdout.log" || true
