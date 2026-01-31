Amaç

ACTIVE (simulation) modunun ne zaman açılacağı, nasıl izleneceği, hangi durumda kapatılacağı ve kanıtların nerede olduğu net olsun.
Bu runbook deterministik ve audit-complete zincir üzerine kuruludur.

Tanımlar (Kısa)

OFF: ML kapalı.

SHADOW: ML çalışır, execution’a etki etmez.

ACTIVE: ML, bounded ve guard’lı şekilde qty’ye etki eder.

Derived artifact: Execution’a etki etmeyen, kanıt dosyası.

ACTIVE Ne Zaman Açılır?

ACTIVE ancak aşağıdaki şartlar birlikte sağlanırsa açılır:

Decision
decision_<strategy>_<seed>.json

decision = PROMOTE_ACTIVE

Active Config Var
active_config_<strategy>_<seed>.json mevcut

Runtime Read-Only Hook

Runtime, sadece active_config’tan okur

Env override yok sayılır

Bu üçü yoksa ACTIVE açılmaz → SHADOW davranışı.

ACTIVE Açıkken Ne İzlenir?
1) Health Check (tek bakış)

GET /health/active

Beklenen alanlar:

active_enabled

active_config_present

limits: { max_weight, daily_cap }

guards

provenance hashes

active_enabled=false ise ACTIVE kapalıdır (normal).

2) Active Audit (kanıt)

services/strategyd/runs/active_audit/<run_id>.json

Kontroller:

direction değişmemiş

applied_qty > 0

applied_qty <= base_qty * max_weight

violations_count == 0

Bu dosya append-only ve deterministiktir.

Hangi Durumda ACTIVE Kapanır?
A) Kill Switch

ML_ACTIVE_KILL=1 (env veya runtime flag)

Davranış:

ACTIVE anında kapatılır

SHADOW’a düşer

active_applied=false

active_reason="kill_switch"

B) Safety Violation

ActiveAudit invariant’larından biri bozulursa:

ACTIVE anında kapatılır

SHADOW’a fallback

active_reason="safety_violation:<rule>"

Run crash olmaz, devam eder.

C) Active Config Yok

active_config_<strategy>_<seed>.json yoksa

ACTIVE asla açılmaz

Hangi Dosyaya Bakılır? (Hızlı Rehber)
Soru	Dosya
ML açılmalı mı?	decision_<strategy>_<seed>.json
Hangi guard/limit?	active_config_<strategy>_<seed>.json
ML ne yaptı?	active_audit/<run_id>.json
Şu an durum ne?	/health/active
Kanıt arşivi	runs/archive/YYYYMMDD/
Archive (Zorunlu)

Her ACTIVE kararından sonra:

node archiveActive.js --seed <seed>


Yazılanlar:

triad report

decision

active_config

Overwrite yok.
Aynı gün tekrar çalıştırılırsa skip.

Yasaklar (Kesin)

Execution core’da değişiklik ❌

Replay ordering’e dokunmak ❌

active_config’u runtime’da override etmek ❌

Decision/Config olmadan ACTIVE açmak ❌

Operasyonel İlke

ACTIVE bir “feature” değil, kontrollü bir deneydir.
Kanıt yoksa → kapalıdır.
Şüphe varsa → SHADOW.

Kapanış

Bu runbook ile:

ACTIVE ne zaman, neden, nasıl açıldı kapandı bellidir.

Tüm kararlar dosya ile kanıtlıdır.

Sistem devredilebilir ve denetlenebilir.
