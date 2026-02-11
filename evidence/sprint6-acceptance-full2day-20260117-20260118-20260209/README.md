# Evidence Pack: sprint6-acceptance-full2day-20260117-20260118-20260209

Purpose: Sprint-6 FULL (interpreted as `day_quality==GOOD`) 2-day window discovery smoke sweep and (if possible) acceptance.

Status: **FAIL** (smoke did not reach `patterns_scanned>0` within 300s for any of 3 lowest-rows GOOD candidates; acceptance + determinism blocked per rules).

Verify bar (what to check):
- FULL definition applied: `day_quality==GOOD` (see `inventory/smoke_aday_listesi.txt` header + `sha256/meta_sha256_proof.txt`)
- Inventory: `inventory/adausdt_bbo_daily_inventory.tsv`
- Candidate list (max 3, lowest rows): `inventory/smoke_aday_listesi.txt`
- Real-day proof (sha256 differs):
  - Selected target window inputs: `sha256/meta_sha256_proof.txt`
  - Candidate-level sha_equal flags: `inventory/candidate_sha256_proof.txt`
- Smoke runs (timeout=300, heap=6144, perm ON default):
  - Try1 20260117-20260118: `inventory/smoke/try1_20260117_20260118/`
  - Try2 20260116-20260117: `inventory/smoke/try2_20260116_20260117/`
  - Try3 20260110-20260111: `inventory/smoke/try3_20260110_20260111/`

Acceptance + determinism:
- NOT RUN (blocked because smoke phase did not produce `patterns_scanned>0` within 300s for any of 3 candidates).

Inputs copied for target window (rank#1 candidate by lowest rows_total):
- `inputs/`
- `sha256/`

Key outputs:
- `summary.json`

Risk note: 300-second smoke timeout appears insufficient for GOOD full-day (multi-million row) parquets with current runner; cannot select an acceptance window without changing time budgets or runner behavior (out of scope).
