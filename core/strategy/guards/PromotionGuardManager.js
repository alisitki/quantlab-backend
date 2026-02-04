/**
 * Promotion Guard Manager
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
    else if (level === 'debug') console.debug(msg);
    else console.log(msg);
  } catch {
    // ignore logging failures
  }
}

export class PromotionGuardManager {
  #enabled;
  #replayParityEnabled;
  #minDecisionEnabled;
  #maxLossEnabled;
  #lossStreakEnabled;
  #minDecisions;
  #maxLoss;
  #lossStreak;
  #replayDecisionHash;
  #replayDecisionCount;
  #evaluatedCount = 0;
  #failedCount = 0;
  #lastRealizedPnl = null;
  #lossStreakCount = 0;

  constructor(config) {
    this.#enabled = config.enabled;
    this.#replayParityEnabled = config.replayParityEnabled;
    this.#minDecisionEnabled = config.minDecisionEnabled;
    this.#maxLossEnabled = config.maxLossEnabled;
    this.#lossStreakEnabled = config.lossStreakEnabled;
    this.#minDecisions = config.minDecisions;
    this.#maxLoss = config.maxLoss;
    this.#lossStreak = config.lossStreak;
    this.#replayDecisionHash = config.replayDecisionHash || null;
    this.#replayDecisionCount = config.replayDecisionCount ?? null;
  }

  static fromEnv(overrides = {}) {
    const enabled = envBool(process.env.PROMOTION_GUARDS_ENABLED ?? '1');
    const replayParityEnabled = envBool(process.env.PROMOTION_GUARD_REPLAY_PARITY_ENABLED ?? '1');
    const minDecisionEnabled = envBool(process.env.PROMOTION_GUARD_MIN_DECISIONS_ENABLED ?? '1');
    const maxLossEnabled = envBool(process.env.PROMOTION_GUARD_MAX_LOSS_ENABLED ?? '1');
    const lossStreakEnabled = envBool(process.env.PROMOTION_GUARD_LOSS_STREAK_ENABLED ?? '1');

    const minDecisions = numEnv(process.env.PROMOTION_GUARD_MIN_DECISIONS, 10);
    const maxLoss = numEnv(process.env.PROMOTION_GUARD_MAX_LOSS, 100);
    const lossStreak = numEnv(process.env.PROMOTION_GUARD_LOSS_STREAK, 3);

    const replayDecisionHash = process.env.PROMOTION_GUARD_REPLAY_HASH || null;
    const replayDecisionCount = process.env.PROMOTION_GUARD_REPLAY_DECISION_COUNT
      ? Number(process.env.PROMOTION_GUARD_REPLAY_DECISION_COUNT)
      : null;

    return new PromotionGuardManager({
      enabled,
      replayParityEnabled,
      minDecisionEnabled,
      maxLossEnabled,
      lossStreakEnabled,
      minDecisions,
      maxLoss,
      lossStreak,
      replayDecisionHash,
      replayDecisionCount,
      ...overrides
    });
  }

  get stats() {
    return {
      evaluated_count: this.#evaluatedCount,
      failed_count: this.#failedCount
    };
  }

  #fail(name, reason, context) {
    this.#failedCount += 1;
    safeLog('warn', { event: 'promotion_guard_fail', guard: name, reason });
    emitAudit({
      actor: 'system',
      action: 'GUARD_FAIL',
      target_type: 'run',
      target_id: context?.live_run_id || 'unknown',
      reason,
      metadata: {
        guard: name,
        live_run_id: context?.live_run_id || null,
        strategy_id: context?.strategy_id || null
      }
    });
    return { ok: false, guard: name, reason };
  }

  #pass(name) {
    return { ok: true };
  }

  evaluateEvent({ stateSnapshot, live_run_id = null, strategy_id = null }) {
    if (!this.#enabled) return { ok: true };

    if (this.#maxLossEnabled) {
      this.#evaluatedCount += 1;
      const exec = stateSnapshot.executionState;
      if (exec && typeof exec.totalRealizedPnl === 'number' && typeof exec.totalUnrealizedPnl === 'number') {
        const pnl = exec.totalRealizedPnl + exec.totalUnrealizedPnl;
        if (pnl < -this.#maxLoss) {
          return this.#fail('max_loss_guard', `pnl ${pnl} < -${this.#maxLoss}`, { live_run_id, strategy_id });
        }
      }
      this.#pass('max_loss_guard');
    }

    if (this.#lossStreakEnabled) {
      this.#evaluatedCount += 1;
      const exec = stateSnapshot.executionState;
      if (exec && typeof exec.totalRealizedPnl === 'number') {
        if (this.#lastRealizedPnl !== null) {
          if (exec.totalRealizedPnl < this.#lastRealizedPnl) {
            this.#lossStreakCount += 1;
          } else {
            this.#lossStreakCount = 0;
          }
          if (this.#lossStreakCount >= this.#lossStreak) {
            return this.#fail('loss_streak_guard', `loss_streak ${this.#lossStreakCount} >= ${this.#lossStreak}`, { live_run_id, strategy_id });
          }
        }
        this.#lastRealizedPnl = exec.totalRealizedPnl;
      }
      this.#pass('loss_streak_guard');
    }

    return { ok: true };
  }

  evaluateFinal({ decisionCount, decisionHash, live_run_id = null, strategy_id = null }) {
    if (!this.#enabled) return { ok: true };

    if (this.#replayParityEnabled) {
      this.#evaluatedCount += 1;
      if (!this.#replayDecisionHash || this.#replayDecisionCount === null) {
        this.#pass('replay_parity_guard');
      } else {
        if (decisionHash !== this.#replayDecisionHash || decisionCount !== this.#replayDecisionCount) {
          return this.#fail('replay_parity_guard', 'decision hash/count mismatch', { live_run_id, strategy_id });
        }
        this.#pass('replay_parity_guard');
      }
    }

    if (this.#minDecisionEnabled) {
      this.#evaluatedCount += 1;
      if (decisionCount < this.#minDecisions) {
        return this.#fail('min_decision_guard', `decision_count ${decisionCount} < ${this.#minDecisions}`, { live_run_id, strategy_id });
      }
      this.#pass('min_decision_guard');
    }

    return { ok: true };
  }
}

export default PromotionGuardManager;
