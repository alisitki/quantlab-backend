set -euo pipefail
root="evidence/sprint6-acceptance-full2day-__PENDING__-__PENDING__-20260209"
cands="$root/inventory/smoke_aday_listesi.txt"
sumout="$root/inventory/sha256sum_candidate_parquets.txt"
proof="$root/inventory/candidate_sha256_proof.txt"

# Collect parquet paths (fields 7 and 8), unique
mkdir -p "$root/inventory"
tail -n +3 "$cands" | cut -f7-8 | tr '\t' '\n' | sort -u >"$root/inventory/_candidate_parquets.list"

# sha256 of candidate parquets
sha256sum $(cat "$root/inventory/_candidate_parquets.list") >"$sumout"

# Per-window sha_equal proof
python3 - <<'PY' "$cands" "$sumout" "$proof"
import sys
cands_path, sum_path, out_path = sys.argv[1], sys.argv[2], sys.argv[3]
sha = {}
with open(sum_path,"r",encoding="utf-8") as f:
  for line in f:
    h, p = line.strip().split(None,1)
    sha[p]=h
lines=[]
with open(cands_path,"r",encoding="utf-8") as f:
  for line in f:
    if line.startswith("rank\t") or line.startswith("FULL_DEFINITION") or not line.strip():
      continue
    parts=line.rstrip("\n").split("\t")
    start,end = parts[1], parts[2]
    pq1,pq2 = parts[6], parts[7]
    h1,h2 = sha.get(pq1,"MISSING"), sha.get(pq2,"MISSING")
    lines.append((start,end,h1,h2, "true" if h1==h2 else "false"))
with open(out_path,"w",encoding="utf-8") as o:
  for start,end,h1,h2,eq in lines:
    o.write(f"{start}-{end}.day1_sha256={h1}\n")
    o.write(f"{start}-{end}.day2_sha256={h2}\n")
    o.write(f"{start}-{end}.sha_equal={eq}\n")
PY
