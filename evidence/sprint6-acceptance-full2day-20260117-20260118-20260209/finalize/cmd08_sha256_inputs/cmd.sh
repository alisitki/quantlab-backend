set -euo pipefail
EVD_ROOT="evidence/sprint6-acceptance-full2day-20260117-20260118-20260209"
mkdir -p "$EVD_ROOT/sha256"

# Computed parquet sha256
(
  cd "$EVD_ROOT/inputs"
  sha256sum adausdt_20260117.parquet adausdt_20260118.parquet
) >"$EVD_ROOT/sha256/sha256sum_inputs_parquet.txt"

h1=$(awk '/adausdt_20260117\.parquet$/{print $1}' "$EVD_ROOT/sha256/sha256sum_inputs_parquet.txt" | tail -n 1)
h2=$(awk '/adausdt_20260118\.parquet$/{print $1}' "$EVD_ROOT/sha256/sha256sum_inputs_parquet.txt" | tail -n 1)

rows1=$(jq -r '.rows // "UNKNOWN"' "$EVD_ROOT/inputs/adausdt_20260117_meta.json")
rows2=$(jq -r '.rows // "UNKNOWN"' "$EVD_ROOT/inputs/adausdt_20260118_meta.json")
dq1=$(jq -r '.day_quality // "MISSING"' "$EVD_ROOT/inputs/adausdt_20260117_meta.json")
dq2=$(jq -r '.day_quality // "MISSING"' "$EVD_ROOT/inputs/adausdt_20260118_meta.json")
meta_h1=$(jq -r '.sha256 // "MISSING"' "$EVD_ROOT/inputs/adausdt_20260117_meta.json")
meta_h2=$(jq -r '.sha256 // "MISSING"' "$EVD_ROOT/inputs/adausdt_20260118_meta.json")

sha_equal="false"
if [ -n "$h1" ] && [ -n "$h2" ] && [ "$h1" = "$h2" ]; then
  sha_equal="true"
fi

cat >"$EVD_ROOT/sha256/meta_sha256_proof.txt" <<EOF
20260117.sha256_computed=$h1
20260118.sha256_computed=$h2
sha_equal=$sha_equal
20260117.meta.sha256=$meta_h1
20260118.meta.sha256=$meta_h2
20260117.rows=$rows1
20260118.rows=$rows2
20260117.day_quality=$dq1
20260118.day_quality=$dq2
FULL_DEFINITION_APPLIED=day_quality==GOOD
EOF

# Show proof lines
sed -n '1,120p' "$EVD_ROOT/sha256/meta_sha256_proof.txt"
