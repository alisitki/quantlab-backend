set -euo pipefail
EVD_ROOT="evidence/sprint6-acceptance-full2day-20260117-20260118-20260209"

src1_pq="data/curated/exchange=binance/stream=bbo/symbol=adausdt/date=20260117/data.parquet"
src1_meta="data/curated/exchange=binance/stream=bbo/symbol=adausdt/date=20260117/meta.json"
src2_pq="data/curated/exchange=binance/stream=bbo/symbol=adausdt/date=20260118/data.parquet"
src2_meta="data/curated/exchange=binance/stream=bbo/symbol=adausdt/date=20260118/meta.json"

mkdir -p "$EVD_ROOT/inputs"

cp -a "$src1_pq" "$EVD_ROOT/inputs/adausdt_20260117.parquet"
cp -a "$src1_meta" "$EVD_ROOT/inputs/adausdt_20260117_meta.json"
cp -a "$src2_pq" "$EVD_ROOT/inputs/adausdt_20260118.parquet"
cp -a "$src2_meta" "$EVD_ROOT/inputs/adausdt_20260118_meta.json"

ls -la "$EVD_ROOT/inputs"
