#!/usr/bin/env bash
set -euo pipefail

REPO="/home/deploy/quantlab-backend"
cd "$REPO"

MAX_WALL=7200
FAMILY_ID="latency_leadlag_v1"
EXCHANGES_CSV="binance,bybit,okx"
STREAM="bbo"
TOL_MS=20
DELTA_LIST="0,10,25,50,100,250,500"
H_LIST="50,100,250"

TS="$(date -u +%Y%m%d_%H%M%S)"
PACK="multi-hypothesis-phase5-latency-leadlag-v1-${TS}"
PACK_DIR="$REPO/evidence/$PACK"
ARCHIVE_ROOT="/home/deploy/quantlab-evidence-archive/$(date -u +%Y%m%d)_slim"

mkdir -p "$PACK_DIR"/{analysis,state_selection,attempts/attempt_1,controller_commands,finalize}

CMD_INDEX="$PACK_DIR/command_index.tsv"
TIME_SUMMARY="$PACK_DIR/time_v_summary.tsv"
printf 'step\texit_code\tmax_rss_kb\telapsed_s\tcumulative_wall_s\tcmd_relpath\tstdout_relpath\tstderr_relpath\ttime_v_relpath\texit_relpath\n' > "$CMD_INDEX"
printf 'step\texit_code\telapsed_s\tmax_rss_kb\tcumulative_wall_s\n' > "$TIME_SUMMARY"

CUM_WALL="0"

extract_elapsed() {
  local f="$1"
  python3 - <<PY
import re
text=open(r"$f","r",encoding="utf-8",errors="replace").read()
m=re.search(r"Elapsed \(wall clock\) time \(h:mm:ss or m:ss\):\s*([^\n]+)",text)
if not m:
    print("0.0")
    raise SystemExit(0)
raw=m.group(1).strip()
parts=raw.split(":")
vals=[float(x) for x in parts]
if len(vals)==3:
    sec=vals[0]*3600+vals[1]*60+vals[2]
elif len(vals)==2:
    sec=vals[0]*60+vals[1]
else:
    sec=vals[0]
print(f"{sec:.6f}")
PY
}

extract_rss() {
  local f="$1"
  python3 - <<PY
import re
text=open(r"$f","r",encoding="utf-8",errors="replace").read()
m=re.search(r"Maximum resident set size \(kbytes\):\s*([0-9]+)",text)
print(m.group(1) if m else "0")
PY
}

run_step() {
  local step="$1"
  local cmd="$2"
  local dir="$PACK_DIR/controller_commands/$step"
  mkdir -p "$dir"
  local cmdf="$dir/cmd.sh"
  local outf="$dir/stdout.log"
  local errf="$dir/stderr.log"
  local timef="$dir/time-v.log"
  local exitf="$dir/exit_code.txt"

  printf '%s\n' "$cmd" > "$cmdf"
  set +e
  /usr/bin/time -v -o "$timef" -- bash -lc "$cmd" >"$outf" 2>"$errf"
  local ec=$?
  set -e

  # After slim finalize, PACK_DIR can move to archive root. Remap paths if needed.
  if [[ ! -f "$timef" && -f "$REPO/evidence/$PACK.moved_to.txt" ]]; then
    local moved
    moved="$(cat "$REPO/evidence/$PACK.moved_to.txt")"
    PACK_DIR="$moved"
    CMD_INDEX="$PACK_DIR/command_index.tsv"
    TIME_SUMMARY="$PACK_DIR/time_v_summary.tsv"
    dir="$PACK_DIR/controller_commands/$step"
    cmdf="$dir/cmd.sh"
    outf="$dir/stdout.log"
    errf="$dir/stderr.log"
    timef="$dir/time-v.log"
    exitf="$dir/exit_code.txt"
  fi

  mkdir -p "$(dirname "$exitf")"
  printf '%s\n' "$ec" > "$exitf"

  local elapsed
  elapsed="$(extract_elapsed "$timef")"
  local rss
  rss="$(extract_rss "$timef")"
  CUM_WALL="$(python3 - <<PY
c=float("$CUM_WALL")
e=float("$elapsed")
print(f"{c+e:.6f}")
PY
)"

  local rel_cmd rel_out rel_err rel_time rel_exit
  rel_cmd="${cmdf#$PACK_DIR/}"
  rel_out="${outf#$PACK_DIR/}"
  rel_err="${errf#$PACK_DIR/}"
  rel_time="${timef#$PACK_DIR/}"
  rel_exit="${exitf#$PACK_DIR/}"

  printf '%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\n' \
    "$step" "$ec" "$rss" "$elapsed" "$CUM_WALL" "$rel_cmd" "$rel_out" "$rel_err" "$rel_time" "$rel_exit" >> "$CMD_INDEX"
  printf '%s\t%s\t%s\t%s\t%s\n' "$step" "$ec" "$elapsed" "$rss" "$CUM_WALL" >> "$TIME_SUMMARY"

  python3 - <<PY
import sys
if float("$CUM_WALL") >= float("$MAX_WALL"):
    print("MAX_WALL_REACHED")
    sys.exit(90)
sys.exit(0)
PY

  if [[ "$ec" -ne 0 ]]; then
    echo "STEP_FAILED:$step" >&2
    return "$ec"
  fi
  return 0
}

cat > "$PACK_DIR/analysis/pre_note.txt" <<EOF
Phase-5 latency_leadlag_v1 diagnostic run.
Why: verify deterministic lead-lag forward-return signal grid on single window.
Done: PASS/<LABEL>:detail + SLIM triple + no_new_runs=true.
EOF

run_step "precheck" "test -x tools/slim_finalize.sh && test -f tools/hypotheses/latency_leadlag_v1.py && test -x /tmp/s3_compact_tool.py"
run_step "code_version" "git rev-parse HEAD > '$PACK_DIR/analysis/code_version_ref.txt'"
run_step "state_fetch" "python3 /tmp/s3_compact_tool.py get quantlab-compact compacted/_state.json /tmp/quantlab_compacted_state_latency_v1_${TS}.json"

run_step "selection_proof" "python3 - <<'PY'
import json, csv, datetime, collections
from pathlib import Path

state_path=Path('/tmp/quantlab_compacted_state_latency_v1_${TS}.json')
pack=Path('$PACK_DIR')
sel_dir=pack/'state_selection'
sel_dir.mkdir(parents=True, exist_ok=True)

state=json.loads(state_path.read_text(encoding='utf-8'))
parts=state.get('partitions',{})
exs=['binance','bybit','okx']
stream='bbo'

rows_map={}
for k,v in parts.items():
    p=k.split('/')
    if len(p)!=4:
        continue
    ex,st,sym,date=p
    if ex not in exs or st!=stream:
        continue
    if str(v.get('status','')).lower()!='success' or v.get('day_quality_post')!='GOOD':
        continue
    try:
        rows=int(v.get('rows'))
    except Exception:
        rows=None
    rows_map[(ex,sym,date)]={
        'rows': rows,
        'status': v.get('status',''),
        'day_quality_post': v.get('day_quality_post',''),
        'total_size_bytes': v.get('total_size_bytes',''),
        'updated_at': v.get('updated_at',''),
    }

sym_to_windows=collections.defaultdict(list)
all_syms=sorted({sym for (_,sym,_) in rows_map.keys()})
for sym in all_syms:
    common=sorted(
        {d for (e,s,d) in rows_map if e=='binance' and s==sym}
        & {d for (e,s,d) in rows_map if e=='bybit' and s==sym}
        & {d for (e,s,d) in rows_map if e=='okx' and s==sym}
    )
    for i in range(len(common)-1):
        d1=common[i]; d2=common[i+1]
        t1=datetime.datetime.strptime(d1,'%Y%m%d')
        t2=datetime.datetime.strptime(d2,'%Y%m%d')
        if (t2-t1).days!=1:
            continue
        total=0
        ok=True
        for ex in exs:
            r1=rows_map[(ex,sym,d1)]['rows']
            r2=rows_map[(ex,sym,d2)]['rows']
            if r1 is None or r2 is None:
                ok=False
                break
            total += r1 + r2
        if ok:
            sym_to_windows[sym].append({'start':d1,'end':d2,'rows_total':total})

if not sym_to_windows:
    raise SystemExit('no_common_3exchange_windows')

rank=[]
for sym,ws in sym_to_windows.items():
    rank.append((sym, len(ws), min(w['rows_total'] for w in ws)))
rank.sort(key=lambda x:(-x[1], x[2], x[0]))
sel_sym=rank[0][0]
sel_ws=sorted(sym_to_windows[sel_sym], key=lambda w:(w['rows_total'], w['start']))[0]
start=sel_ws['start']; end=sel_ws['end']

if not (sel_sym=='ltcusdt' and start=='20260127' and end=='20260128'):
    raise SystemExit(f'selection_mismatch got {sel_sym} {start}..{end} expected ltcusdt 20260127..20260128')

with (sel_dir/'selected_window.tsv').open('w',encoding='utf-8',newline='') as f:
    w=csv.writer(f,delimiter='\t')
    w.writerow(['symbol','start','end','stream','exchanges','selection_policy'])
    w.writerow([sel_sym,start,end,stream,','.join(exs),'win_count_desc,rows_total_asc,symbol_asc,window_asc'])

with (sel_dir/'object_keys_selected.tsv').open('w',encoding='utf-8',newline='') as f:
    w=csv.writer(f,delimiter='\t')
    w.writerow(['label','exchange','date','partition_key','data_key','meta_key','bucket'])
    for day,label in [(start,'day1'),(end,'day2')]:
        for ex in exs:
            pkey=f'{ex}/{stream}/{sel_sym}/{day}'
            data_key=f'exchange={ex}/stream={stream}/symbol={sel_sym}/date={day}/data.parquet'
            meta_key=f'exchange={ex}/stream={stream}/symbol={sel_sym}/date={day}/meta.json'
            w.writerow([label,ex,day,pkey,data_key,meta_key,'quantlab-compact'])

with (sel_dir/'state_excerpt.tsv').open('w',encoding='utf-8',newline='') as f:
    w=csv.writer(f,delimiter='\t')
    w.writerow(['exchange','stream','symbol','date','status','day_quality_post','rows','total_size_bytes','updated_at','partition_key'])
    for day in [start,end]:
        for ex in exs:
            meta=rows_map[(ex,sel_sym,day)]
            w.writerow([ex,stream,sel_sym,day,meta['status'],meta['day_quality_post'],meta['rows'],meta['total_size_bytes'],meta['updated_at'],f'{ex}/{stream}/{sel_sym}/{day}'])

spec={
  'family':'latency_leadlag_v1',
  'inputs':['bbo_top_of_book_parquet_replay'],
  'exchanges':exs,
  'symbol':sel_sym,
  'stream':stream,
  'window_id':f'{start}..{end}',
  'tolerance_ms':20,
  'delta_t_ms':[0,10,25,50,100,250,500],
  'h_ms':[50,100,250],
  'pair_order':['binance->bybit','binance->okx','bybit->okx'],
  'support_rule':'pair_aggregate>=200',
  'label_tie_rule':'abs_t_desc,event_count_desc,abs_mean_desc,pair_asc,delta_t_asc,h_asc',
}
(pack/'spec_v2.json').write_text(json.dumps(spec,indent=2,ensure_ascii=True)+'\n',encoding='utf-8')
PY"

run_step "run_primary" "python3 tools/hypotheses/latency_leadlag_v1.py --object-keys-tsv '$PACK_DIR/state_selection/object_keys_selected.tsv' --downloads-dir '$PACK_DIR/attempts/attempt_1/downloads_primary' --exchange-order '$EXCHANGES_CSV' --symbol 'ltcusdt' --start '20260127' --end '20260128' --tolerance-ms $TOL_MS --delta-ms-list '$DELTA_LIST' --h-ms-list '$H_LIST' --results-out '$PACK_DIR/attempts/attempt_1/run_primary/results_rollup.tsv' --pair-support-out '$PACK_DIR/attempts/attempt_1/run_primary/pair_support.tsv' --summary-out '$PACK_DIR/attempts/attempt_1/run_primary/summary.json'"

run_step "run_replay_on" "python3 tools/hypotheses/latency_leadlag_v1.py --object-keys-tsv '$PACK_DIR/state_selection/object_keys_selected.tsv' --downloads-dir '$PACK_DIR/attempts/attempt_1/downloads_replay' --exchange-order '$EXCHANGES_CSV' --symbol 'ltcusdt' --start '20260127' --end '20260128' --tolerance-ms $TOL_MS --delta-ms-list '$DELTA_LIST' --h-ms-list '$H_LIST' --results-out '$PACK_DIR/attempts/attempt_1/run_replay_on/results_rollup.tsv' --pair-support-out '$PACK_DIR/attempts/attempt_1/run_replay_on/pair_support.tsv' --summary-out '$PACK_DIR/attempts/attempt_1/run_replay_on/summary.json'"

run_step "determinism_and_label" "python3 - <<'PY'
import csv, json, hashlib
from pathlib import Path

pack=Path('$PACK_DIR')
window='20260127..20260128'
family='latency_leadlag_v1'

p_primary=pack/'attempts/attempt_1/run_primary/results_rollup.tsv'
p_replay=pack/'attempts/attempt_1/run_replay_on/results_rollup.tsv'
p_support=pack/'attempts/attempt_1/run_primary/pair_support.tsv'

def read_rows(path):
    with path.open('r',encoding='utf-8',newline='') as f:
        return list(csv.DictReader(f, delimiter='\t'))

pr=read_rows(p_primary)
rr=read_rows(p_replay)

key=lambda r:(r['pair'],int(r['delta_t_ms']),int(r['h_ms']))
prs=sorted(pr,key=key)
rrs=sorted(rr,key=key)

basis=['pair','delta_t_ms','h_ms','event_count','mean_forward_return_bps','t_stat']

def canon(rows):
    out=[]
    for r in rows:
        out.append({
            'pair':r['pair'],
            'delta_t_ms':int(r['delta_t_ms']),
            'h_ms':int(r['h_ms']),
            'event_count':int(r['event_count']),
            'mean_forward_return_bps':'{:.15f}'.format(float(r['mean_forward_return_bps'])),
            't_stat':'{:.15f}'.format(float(r['t_stat'])),
        })
    return out

cpr=canon(prs)
crr=canon(rrs)
primary_hash=hashlib.sha256(json.dumps(cpr,separators=(',',':'),ensure_ascii=True).encode()).hexdigest()
replay_hash=hashlib.sha256(json.dumps(crr,separators=(',',':'),ensure_ascii=True).encode()).hexdigest()
det='PASS' if cpr==crr else 'FAIL'

with (pack/'determinism_compare.tsv').open('w',encoding='utf-8',newline='') as f:
    w=csv.writer(f,delimiter='\t')
    w.writerow(['window','family_id','primary_hash','replay_hash','determinism_status','compare_basis'])
    w.writerow([window,family,primary_hash,replay_hash,det,','.join(basis)])

with (pack/'results_rollup.tsv').open('w',encoding='utf-8',newline='') as f:
    w=csv.writer(f,delimiter='\t')
    w.writerow(['window','pair','delta_t_ms','h_ms','event_count','mean_forward_return_bps','t_stat','determinism_status'])
    for r in prs:
        w.writerow([
            r['window'], r['pair'], r['delta_t_ms'], r['h_ms'], r['event_count'],
            '{:.15f}'.format(float(r['mean_forward_return_bps'])), '{:.15f}'.format(float(r['t_stat'])), det
        ])

supports=[]
with p_support.open('r',encoding='utf-8',newline='') as f:
    for r in csv.DictReader(f,delimiter='\t'):
        supports.append({'pair':r['pair'],'event_count_pair':int(r['event_count_pair'])})
max_support=max((x['event_count_pair'] for x in supports), default=0)

rows=[{
  'pair':r['pair'],
  'delta_t_ms':int(r['delta_t_ms']),
  'h_ms':int(r['h_ms']),
  'event_count':int(r['event_count']),
  'mean':float(r['mean_forward_return_bps']),
  't':float(r['t_stat']),
} for r in prs]

label=''
detail=''
selected=None
if det!='PASS':
    label='FAIL/DETERMINISM_FAIL'
    detail='primary_hash_mismatch'
elif max_support < 200:
    label='INSUFFICIENT_SUPPORT'
    top=max(rows,key=lambda x:abs(x['t'])) if rows else {'pair':'na','delta_t_ms':0,'h_ms':0,'event_count':0,'mean':0.0,'t':0.0}
    selected=top
else:
    sig=[r for r in rows if abs(r['t'])>=3.0]
    if not sig:
        label='NO_EDGE'
        top=max(rows,key=lambda x:abs(x['t'])) if rows else {'pair':'na','delta_t_ms':0,'h_ms':0,'event_count':0,'mean':0.0,'t':0.0}
        selected=top
    else:
        sig.sort(key=lambda x:(-abs(x['t']), -x['event_count'], -abs(x['mean']), x['pair'], x['delta_t_ms'], x['h_ms']))
        top=sig[0]
        selected=top
        label='DIRECTIONAL' if top['mean']>0 else 'ANTI_EDGE'

if selected is None:
    selected={'pair':'na','delta_t_ms':0,'h_ms':0,'event_count':0,'mean':0.0,'t':0.0}

if detail=='':
    detail='pair={pair},dt_ms={delta_t_ms},h_ms={h_ms},event_count={event_count},mean_bps={mean:.15f},t_stat={t:.15f}'.format(**selected)

result_line=(f'FAIL/DETERMINISM_FAIL:{detail}' if label.startswith('FAIL/') else f'PASS/{label}:{detail}')
(pack/'result.txt').write_text(result_line+'\n',encoding='utf-8')

manifest_rows=[
    ('spec_v2.json','spec_v2.json','spec_v2.json','OK'),
    ('results_rollup.tsv','results_rollup.tsv','results_rollup.tsv','OK'),
    ('determinism_compare.tsv','determinism_compare.tsv','determinism_compare.tsv','OK'),
    ('conditional_stats.tsv','conditional_stats.tsv','','N/A_NOT_PRODUCED'),
    ('label_report.txt','label_report.txt','label_report.txt','OK'),
]
with (pack/'artifact_manifest.tsv').open('w',encoding='utf-8',newline='') as f:
    w=csv.writer(f,delimiter='\t')
    w.writerow(['artifact','expected_relpath','resolved_relpath','status'])
    for row in manifest_rows:
        w.writerow(row)

code_ref=(pack/'analysis/code_version_ref.txt').read_text(encoding='utf-8').strip()
hash_inputs=hashlib.sha256(((pack/'spec_v2.json').read_text(encoding='utf-8') + (pack/'state_selection/object_keys_selected.tsv').read_text(encoding='utf-8') + (pack/'state_selection/selected_window.tsv').read_text(encoding='utf-8')).encode()).hexdigest()
hash_outputs=hashlib.sha256(((pack/'results_rollup.tsv').read_text(encoding='utf-8') + (pack/'determinism_compare.tsv').read_text(encoding='utf-8') + (pack/'result.txt').read_text(encoding='utf-8')).encode()).hexdigest()

lines=[
    f'label={label}',
    'artifact_manifest=spec_v2.json,results_rollup.tsv,determinism_compare.tsv,label_report',
    'tolerance_ms=20',
    'decision_inputs=top_hit:pair={pair},dt_ms={delta_t_ms},H_ms={h_ms},event_count={event_count},mean_bps={mean:.15f},t_stat={t:.15f}'.format(**selected),
    f'hash_inputs={hash_inputs}',
    f'hash_outputs={hash_outputs}',
    'window_id=20260127..20260128',
    f'run_id={pack.name}',
    f'code_version_ref={code_ref}',
    'aggregation_note=decision uses only pair-level aggregated stats; regime breakdown is report-only (if any)',
    'scope_guard=No new fields beyond listed',
    f'run_trace=source_run={pack.name};closure_pack={pack.name};no_new_runs=true',
]
(pack/'label_report.txt').write_text('\n'.join(lines)+'\n',encoding='utf-8')
PY"

run_step "integrity_check" "python3 - <<'PY'
from pathlib import Path
pack=Path('$PACK_DIR')
required=[
  'spec_v2.json','results_rollup.tsv','determinism_compare.tsv','label_report.txt','artifact_manifest.tsv','result.txt','command_index.tsv','time_v_summary.tsv'
]
missing=[x for x in required if not (pack/x).exists()]
(pack/'integrity_check.txt').write_text('missing_count='+str(len(missing))+'\n'+'\n'.join(missing)+'\n',encoding='utf-8')
if missing:
    raise SystemExit('missing_required_artifacts')
PY"

run_step "finalize_slim" "cd '$REPO' && tools/slim_finalize.sh '$PACK' '$PACK_DIR' '$ARCHIVE_ROOT'"

run_step "sha_verify_capture" "sha256sum -c '$REPO/evidence/$PACK.tar.gz.sha256' > '$REPO/evidence/$PACK.sha_verify_tmp.txt'"
MOVED_TO="$(cat "$REPO/evidence/$PACK.moved_to.txt")"
cp "$REPO/evidence/$PACK.sha_verify_tmp.txt" "$MOVED_TO/sha_verify.txt"
rm -f "$REPO/evidence/$PACK.sha_verify_tmp.txt"

run_step "post_guard" "test ! -d '$REPO/evidence/$PACK' && test -d '$MOVED_TO' && test -f '$REPO/evidence/$PACK.tar.gz' && test -f '$REPO/evidence/$PACK.tar.gz.sha256' && test -f '$REPO/evidence/$PACK.moved_to.txt' && grep -q 'OK' '$MOVED_TO/sha_verify.txt'"

echo "PACK=$PACK"
echo "MOVED_TO=$MOVED_TO"
