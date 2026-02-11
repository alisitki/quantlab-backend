set -euo pipefail
inv="evidence/sprint6-acceptance-full2day-__PENDING__-__PENDING__-20260209/inventory/adausdt_bbo_daily_inventory.tsv"
out="evidence/sprint6-acceptance-full2day-__PENDING__-__PENDING__-20260209/inventory/smoke_aday_listesi.txt"

python3 - <<'PY' "$inv" "$out"
import sys, datetime
inv_path, out_path = sys.argv[1], sys.argv[2]

rows = {}
paths = {}
dq = {}

with open(inv_path, "r", encoding="utf-8") as f:
  header = f.readline()
  for line in f:
    date, dayq, r, pq, mp = line.rstrip("\n").split("\t")
    dq[date] = dayq
    try:
      rows[date] = int(r)
    except:
      rows[date] = None
    paths[date] = (pq, mp)

def next_day(d):
  dt = datetime.datetime.strptime(d, "%Y%m%d").date()
  return (dt + datetime.timedelta(days=1)).strftime("%Y%m%d")

cands = []
for d in sorted(dq.keys()):
  nd = next_day(d)
  if nd not in dq: continue
  if dq.get(d) != "GOOD" or dq.get(nd) != "GOOD": continue
  r1, r2 = rows.get(d), rows.get(nd)
  if r1 is None or r2 is None:
    total = None
  else:
    total = r1 + r2
  cands.append((total, d, nd))

def sortkey(x):
  total, d, nd = x
  return (10**30 if total is None else total, d, nd)

cands.sort(key=sortkey)
top = cands[:3]

with open(out_path, "w", encoding="utf-8") as o:
  o.write("FULL_DEFINITION_APPLIED=day_quality==GOOD\n")
  o.write("rank\tstart\tend\trows_total\tday1_rows\tday2_rows\tday1_parquet\tday2_parquet\tday1_meta\tday2_meta\n")
  for i,(total, d, nd) in enumerate(top, start=1):
    pq1, mp1 = paths[d]
    pq2, mp2 = paths[nd]
    o.write(f"{i}\t{d}\t{nd}\t{'' if total is None else total}\t{rows[d]}\t{rows[nd]}\t{pq1}\t{pq2}\t{mp1}\t{mp2}\n")

print(f"candidates_selected={len(top)}")
for i,(_,d,nd) in enumerate(top, start=1):
  print(f"cand{i}={d}-{nd}")
PY
