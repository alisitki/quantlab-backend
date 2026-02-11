set -euo pipefail
EVD_ROOT="evidence/sprint6-acceptance-full2day-__PENDING__-__PENDING__-20260209"
out="$EVD_ROOT/inventory/adausdt_bbo_daily_inventory.tsv"
tmp="$(mktemp)"

# Curated (meta.json)
if [ -d "data/curated" ]; then
  find data/curated -type f -path "*/stream=bbo/symbol=adausdt/date=*/data.parquet" | sort | while read -r pq; do
    meta="${pq%/data.parquet}/meta.json"
    [ -f "$meta" ] || continue
    date="$(printf "%s" "$pq" | sed -n 's|.*date=\([0-9]\{8\}\)/data\.parquet|\1|p')"
    dq="$(jq -r '.day_quality // "MISSING"' "$meta")"
    rows="$(jq -r '.rows // "UNKNOWN"' "$meta")"
    st="$(jq -r '.stream_type // "UNKNOWN"' "$meta")"
    [ "$st" = "bbo" ] || continue
    printf "%s\t%s\t%s\t%s\t%s\n" "$date" "$dq" "$rows" "$pq" "$meta" >>"$tmp"
  done
fi

# Legacy sprint2/test naming (adausdt_YYYYMMDD.parquet + adausdt_YYYYMMDD_meta.json)
for dir in data/sprint2 data/test; do
  [ -d "$dir" ] || continue
  find "$dir" -maxdepth 1 -type f -name "adausdt_[0-9][0-9][0-9][0-9][0-9][0-9][0-9][0-9].parquet" | sort | while read -r pq; do
    meta="${pq%.parquet}_meta.json"
    [ -f "$meta" ] || continue
    date="$(basename "$pq" | sed -n 's/^adausdt_\([0-9]\{8\}\)\.parquet$/\1/p')"
    dq="$(jq -r '.day_quality // "MISSING"' "$meta")"
    rows="$(jq -r '.rows // "UNKNOWN"' "$meta")"
    st="$(jq -r '.stream_type // "UNKNOWN"' "$meta")"
    [ "$st" = "bbo" ] || continue
    printf "%s\t%s\t%s\t%s\t%s\n" "$date" "$dq" "$rows" "$pq" "$meta" >>"$tmp"
  done
done

printf "date\tday_quality\trows\tparquet_path\tmeta_path\n" >"$out"
sort -u "$tmp" | sort -k1,1 >>"$out"
rm -f "$tmp"

# Also emit missing-day_quality list for the mandatory FAIL branch check
awk -F'\t' 'NR>1 && $2=="MISSING"{print $1"\t"$4"\t"$5}' "$out" >"${out%.tsv}_missing_day_quality.tsv"

# Optional: small summary lines (for quick scan)
rows_total_good=$(awk -F'\t' 'NR>1 && $2=="GOOD" && $3 ~ /^[0-9]+$/ {sum+=$3} END {print sum+0}' "$out")
days_total=$(awk 'END{print NR-1}' "$out")
days_good=$(awk -F'\t' 'NR>1 && $2=="GOOD" {c++} END{print c+0}' "$out")
echo "inventory_days_total=${days_total}"
echo "inventory_days_good=${days_good}"
echo "inventory_rows_total_good=${rows_total_good}"
