# QuantLab Run Lifecycle & Control

## Purpose
Standardize long-running runs and control endpoints so stop/kill/finalize semantics are safe and observable.

## When to use
- Any "run" concept (compaction runs, replay runs, strategy runs)
- /control endpoints (kill, stop, pause, resume)
- Any operation that writes manifests/final outputs

## Required semantics
- run_id is mandatory and stable for the lifetime of a run.
- A run has explicit states: CREATED -> RUNNING -> FINALIZING -> DONE (or FAILED/CANCELED).
- Control actions must be deterministic and safe:
  - "kill/stop" must wait for finalize (manifest/outputs) before returning success.
  - If immediate abort is necessary, response must clearly say what was finalized vs not.

## Persistence & idempotency
- Start is idempotent (retry-safe) or clearly guarded.
- Finalize is idempotent (can be called again without corruption).
- Manifests must reflect true start/end times and final status.

## Observability requirements
- Logs include: run_id, action, state transitions, duration_ms, counts.
- Metrics/counters: runs_started, runs_completed, runs_failed, runs_canceled.

## Output format
1) PLAN
2) FILE PATCH LIST
3) VERIFY STEPS
   - start a run => state RUNNING
   - stop/kill => waits for FINALIZING then DONE
   - repeated stop => safe no-op with clear response
