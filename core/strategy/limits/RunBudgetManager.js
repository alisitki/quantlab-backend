/**
 * Run Budget Manager
 */

function envBool(val) {
  return val === '1' || val === 'true' || val === 'yes';
}

function numEnv(val, fallback) {
  const n = Number(val);
  return Number.isFinite(n) ? n : fallback;
}

import { emitAudit } from '../../audit/AuditWriter.js';

function safeLog(level, payload) {
  try {
    const msg = JSON.stringify(payload);
    if (level === 'warn') console.warn(msg);
    else if (level === 'error') console.error(msg);
    else console.log(msg);
  } catch {
    // ignore log failures
  }
}

export class RunBudgetManager {
  #enabled;
  #maxDurationEnabled;
  #maxEventsEnabled;
  #maxDecisionRateEnabled;
  #maxRunSeconds;
  #maxEvents;
  #maxDecisionsPerMin;
  #startedAtMs;
  #eventCount = 0;
  #decisionTimestamps = [];
  #evaluatedCount = 0;
  #exceededCount = 0;

  constructor(config) {
    this.#enabled = config.enabled;
    this.#maxDurationEnabled = config.maxDurationEnabled;
    this.#maxEventsEnabled = config.maxEventsEnabled;
    this.#maxDecisionRateEnabled = config.maxDecisionRateEnabled;
    this.#maxRunSeconds = config.maxRunSeconds;
    this.#maxEvents = config.maxEvents;
    this.#maxDecisionsPerMin = config.maxDecisionsPerMin;
    this.#startedAtMs = Date.now();
  }

  static fromEnv(overrides = {}) {
    const enabled = envBool(process.env.RUN_BUDGETS_ENABLED ?? '1');
    const maxDurationEnabled = envBool(process.env.RUN_BUDGET_MAX_DURATION_ENABLED ?? '1');
    const maxEventsEnabled = envBool(process.env.RUN_BUDGET_MAX_EVENTS_ENABLED ?? '1');
    const maxDecisionRateEnabled = envBool(process.env.RUN_BUDGET_MAX_DECISION_RATE_ENABLED ?? '1');

    const maxRunSeconds = numEnv(process.env.RUN_BUDGET_MAX_RUN_SECONDS, 3600);
    const maxEvents = numEnv(process.env.RUN_BUDGET_MAX_EVENTS, 1_000_000);
    const maxDecisionsPerMin = numEnv(process.env.RUN_BUDGET_MAX_DECISIONS_PER_MIN, 600);

    return new RunBudgetManager({
      enabled,
      maxDurationEnabled,
      maxEventsEnabled,
      maxDecisionRateEnabled,
      maxRunSeconds,
      maxEvents,
      maxDecisionsPerMin,
      ...overrides
    });
  }

  get stats() {
    return {
      evaluated_count: this.#evaluatedCount,
      exceeded_count: this.#exceededCount
    };
  }

  getPressure(nowMs = Date.now()) {
    if (!this.#enabled) return 'LOW';
    let maxRatio = 0;
    if (this.#maxDurationEnabled) {
      const elapsed = (nowMs - this.#startedAtMs) / 1000;
      maxRatio = Math.max(maxRatio, elapsed / this.#maxRunSeconds);
    }
    if (this.#maxEventsEnabled && this.#maxEvents > 0) {
      maxRatio = Math.max(maxRatio, this.#eventCount / this.#maxEvents);
    }
    if (this.#maxDecisionRateEnabled && this.#maxDecisionsPerMin > 0) {
      const windowStart = nowMs - 60_000;
      while (this.#decisionTimestamps.length > 0 && this.#decisionTimestamps[0] < windowStart) {
        this.#decisionTimestamps.shift();
      }
      maxRatio = Math.max(maxRatio, this.#decisionTimestamps.length / this.#maxDecisionsPerMin);
    }
    if (maxRatio >= 0.95) return 'HIGH';
    if (maxRatio >= 0.8) return 'MED';
    return 'LOW';
  }

  recordDecision(nowMs = Date.now()) {
    if (!this.#enabled || !this.#maxDecisionRateEnabled) return;
    this.#decisionTimestamps.push(nowMs);
  }

  #checkDuration(nowMs, context) {
    if (!this.#maxDurationEnabled) return { ok: true };
    const elapsed = (nowMs - this.#startedAtMs) / 1000;
    if (elapsed > this.#maxRunSeconds) {
      this.#exceededCount += 1;
      safeLog('error', { event: 'budget_exceeded', budget: 'max_duration', elapsed_s: elapsed });
      emitAudit({
        actor: 'system',
        action: 'BUDGET_EXCEEDED',
        target_type: 'run',
        target_id: context?.live_run_id || 'unknown',
        reason: `max_duration ${elapsed}`,
        metadata: {
          budget: 'max_duration',
          value: elapsed,
          live_run_id: context?.live_run_id || null,
          strategy_id: context?.strategy_id || null
        }
      });
      return { ok: false, budget: 'max_duration', value: elapsed };
    }
    if (elapsed > this.#maxRunSeconds * 0.8) {
      safeLog('warn', { event: 'budget_warn', budget: 'max_duration', elapsed_s: elapsed });
    }
    return { ok: true };
  }

  #checkEvents(context) {
    if (!this.#maxEventsEnabled) return { ok: true };
    if (this.#eventCount > this.#maxEvents) {
      this.#exceededCount += 1;
      safeLog('error', { event: 'budget_exceeded', budget: 'max_events', events: this.#eventCount });
      emitAudit({
        actor: 'system',
        action: 'BUDGET_EXCEEDED',
        target_type: 'run',
        target_id: context?.live_run_id || 'unknown',
        reason: `max_events ${this.#eventCount}`,
        metadata: {
          budget: 'max_events',
          value: this.#eventCount,
          live_run_id: context?.live_run_id || null,
          strategy_id: context?.strategy_id || null
        }
      });
      return { ok: false, budget: 'max_events', value: this.#eventCount };
    }
    if (this.#eventCount > this.#maxEvents * 0.8) {
      safeLog('warn', { event: 'budget_warn', budget: 'max_events', events: this.#eventCount });
    }
    return { ok: true };
  }

  #checkDecisionRate(nowMs, context) {
    if (!this.#maxDecisionRateEnabled) return { ok: true };
    const windowStart = nowMs - 60_000;
    while (this.#decisionTimestamps.length > 0 && this.#decisionTimestamps[0] < windowStart) {
      this.#decisionTimestamps.shift();
    }
    const rate = this.#decisionTimestamps.length;
    if (rate > this.#maxDecisionsPerMin) {
      this.#exceededCount += 1;
      safeLog('error', { event: 'budget_exceeded', budget: 'max_decision_rate', decisions_per_min: rate });
      emitAudit({
        actor: 'system',
        action: 'BUDGET_EXCEEDED',
        target_type: 'run',
        target_id: context?.live_run_id || 'unknown',
        reason: `max_decision_rate ${rate}`,
        metadata: {
          budget: 'max_decision_rate',
          value: rate,
          live_run_id: context?.live_run_id || null,
          strategy_id: context?.strategy_id || null
        }
      });
      return { ok: false, budget: 'max_decision_rate', value: rate };
    }
    if (rate > this.#maxDecisionsPerMin * 0.8) {
      safeLog('warn', { event: 'budget_warn', budget: 'max_decision_rate', decisions_per_min: rate });
    }
    return { ok: true };
  }

  evaluate({ eventIndex, nowMs = Date.now(), live_run_id = null, strategy_id = null }) {
    if (!this.#enabled) return { ok: true };

    this.#evaluatedCount += 1;
    if (typeof eventIndex === 'number') this.#eventCount = eventIndex + 1;

    const context = { live_run_id, strategy_id };
    let res = this.#checkDuration(nowMs, context);
    if (!res.ok) return res;

    res = this.#checkEvents(context);
    if (!res.ok) return res;

    res = this.#checkDecisionRate(nowMs, context);
    if (!res.ok) return res;

    return { ok: true };
  }
}

export default RunBudgetManager;
