/**
 * Observer Registry (in-memory)
 */

import { getKillSwitchManager } from '../futures/KillSwitchManager.js';

const RUN_STATUS = {
  RUNNING: 'RUNNING',
  STOPPED: 'STOPPED',
  ERROR: 'ERROR'
};

class ObserverRegistry {
  #runs = new Map();

  addRun({ live_run_id, strategy_id, started_at, stopFn }) {
    this.#runs.set(live_run_id, {
      live_run_id,
      strategy_id,
      status: RUN_STATUS.RUNNING,
      started_at,
      last_event_ts: null,
      stop_reason: null,
      budget_pressure: 'LOW',
      stopFn
    });
  }

  updateRun(live_run_id, patch) {
    const run = this.#runs.get(live_run_id);
    if (!run) return;
    Object.assign(run, patch);
  }

  stopRun(live_run_id, reason = 'MANUAL_STOP') {
    const run = this.#runs.get(live_run_id);
    if (!run) return false;
    if (run.status !== RUN_STATUS.RUNNING) return true;
    run.stop_reason = reason;
    run.status = RUN_STATUS.STOPPED;
    if (typeof run.stopFn === 'function') {
      run.stopFn();
    }
    return true;
  }

  listRuns() {
    return Array.from(this.#runs.values()).map((r) => ({
      live_run_id: r.live_run_id,
      strategy_id: r.strategy_id,
      status: r.status,
      started_at: r.started_at,
      last_event_ts: r.last_event_ts,
      stop_reason: r.stop_reason,
      budget_pressure: r.budget_pressure
    }));
  }

  getHealth() {
    const runs = this.listRuns();
    const active = runs.filter(r => r.status === RUN_STATUS.RUNNING);
    let lastEventTs = null;
    for (const r of runs) {
      if (r.last_event_ts && (!lastEventTs || r.last_event_ts > lastEventTs)) {
        lastEventTs = r.last_event_ts;
      }
    }
    const lastEventAgeMs = lastEventTs ? Date.now() - lastEventTs : null;
    const budgetPressure = active.some(r => r.budget_pressure === 'HIGH')
      ? 'HIGH'
      : active.some(r => r.budget_pressure === 'MED')
        ? 'MED'
        : 'LOW';

    // Get kill switch status
    let killSwitchStatus = {
      active: false,
      reason: null,
      symbols: []
    };
    try {
      const killSwitchManager = getKillSwitchManager();
      const status = killSwitchManager.getStatus();
      killSwitchStatus = {
        active: status.is_active,
        reason: status.reason || null,
        symbols: status.symbol_kill || []
      };
    } catch {
      // KillSwitchManager not available, use defaults
    }

    return {
      active_runs: active.length,
      ws_connected: active.length > 0,
      last_event_age_ms: lastEventAgeMs,
      budget_pressure: budgetPressure,
      kill_switch_active: killSwitchStatus.active,
      kill_switch_reason: killSwitchStatus.reason,
      killed_symbols: killSwitchStatus.symbols
    };
  }
}

export const observerRegistry = new ObserverRegistry();
export { RUN_STATUS };
export default observerRegistry;
