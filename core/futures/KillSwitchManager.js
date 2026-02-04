/**
 * KillSwitchManager — Runtime kill switch state management.
 *
 * Provides:
 * - Runtime activation/deactivation (not just env vars)
 * - Global and per-symbol kill switches
 * - Audit trail for all state changes
 * - Singleton access for system-wide coordination
 *
 * Phase 4 Safety: Emergency stop mechanism for live trading.
 */

import { emitAudit } from '../audit/AuditWriter.js';

// Reason codes (matching futures_reason_code.ts)
const FuturesReasonCode = {
  GLOBAL_KILL_ACTIVE: 'GLOBAL_KILL_ACTIVE',
  SYMBOL_KILL_ACTIVE: 'SYMBOL_KILL_ACTIVE'
};

/**
 * Load kill-switch config from environment variables.
 * @returns {Object} Kill switch config
 */
function loadKillSwitchFromEnv() {
  const globalKill = process.env.FUTURES_GLOBAL_KILL === 'true';
  const symbolKillRaw = process.env.FUTURES_SYMBOL_KILL || '';
  const reason = process.env.FUTURES_KILL_REASON || '';

  // Parse symbol kill list (comma-separated: "BTCUSDT,ETHUSDT")
  const symbolKill = {};
  if (symbolKillRaw) {
    symbolKillRaw.split(',').forEach((sym) => {
      const trimmed = sym.trim().toUpperCase();
      if (trimmed) symbolKill[trimmed] = true;
    });
  }

  return Object.freeze({
    global_kill: globalKill,
    symbol_kill: Object.freeze(symbolKill),
    reason
  });
}

/**
 * Evaluate kill-switch state against an intent.
 * @param {Object} intent - Intent with symbol property
 * @param {Object} config - Kill switch config
 * @returns {Object} Kill switch result
 */
function evaluateKillSwitch(intent, config) {
  // RULE 1: Global kill-switch overrides everything
  if (config.global_kill) {
    return {
      killed: true,
      reason_code: FuturesReasonCode.GLOBAL_KILL_ACTIVE,
      reason: config.reason || 'Global kill-switch active'
    };
  }

  // RULE 2: Per-symbol kill-switch
  if (config.symbol_kill[intent.symbol]) {
    return {
      killed: true,
      reason_code: FuturesReasonCode.SYMBOL_KILL_ACTIVE,
      reason: config.reason || `Symbol ${intent.symbol} kill-switch active`
    };
  }

  // No kill-switch active
  return {
    killed: false,
    reason_code: null,
    reason: ''
  };
}

/**
 * @typedef {Object} KillSwitchState
 * @property {boolean} global_kill - Global kill switch active
 * @property {Set<string>} symbol_kill - Per-symbol kill switches
 * @property {string} reason - Reason for activation
 * @property {string|null} activated_by - Actor who activated
 * @property {number|null} activated_at - Timestamp of activation
 */

class KillSwitchManager {
  /** @type {KillSwitchState} */
  #state;

  /** @type {Set<function>} */
  #listeners = new Set();

  /** @type {Map<string, function>} */
  #runStopCallbacks = new Map();

  constructor() {
    // Initialize from environment
    const envConfig = loadKillSwitchFromEnv();
    this.#state = {
      global_kill: envConfig.global_kill,
      symbol_kill: new Set(Object.keys(envConfig.symbol_kill)),
      reason: envConfig.reason || '',
      activated_by: envConfig.global_kill ? 'env' : null,
      activated_at: envConfig.global_kill ? Date.now() : null
    };
  }

  /**
   * Get current kill switch status
   */
  getStatus() {
    return {
      global_kill: this.#state.global_kill,
      symbol_kill: Array.from(this.#state.symbol_kill),
      reason: this.#state.reason,
      activated_by: this.#state.activated_by,
      activated_at: this.#state.activated_at,
      is_active: this.#state.global_kill || this.#state.symbol_kill.size > 0
    };
  }

  /**
   * Check if a specific intent should be killed
   * @param {Object} intent - Intent with symbol property
   * @returns {{killed: boolean, reason_code: string|null, reason: string}}
   */
  evaluate(intent) {
    // Convert runtime state to config format for evaluateKillSwitch
    const config = {
      global_kill: this.#state.global_kill,
      symbol_kill: Object.fromEntries(
        Array.from(this.#state.symbol_kill).map(s => [s, true])
      ),
      reason: this.#state.reason
    };

    return evaluateKillSwitch(intent, config);
  }

  /**
   * Check if any kill switch is active
   */
  isActive() {
    return this.#state.global_kill || this.#state.symbol_kill.size > 0;
  }

  /**
   * Check if global kill switch is active
   */
  isGlobalActive() {
    return this.#state.global_kill;
  }

  /**
   * Check if a specific symbol is killed
   * @param {string} symbol
   */
  isSymbolKilled(symbol) {
    return this.#state.symbol_kill.has(symbol.toUpperCase());
  }

  /**
   * Activate global kill switch
   * @param {Object} options
   * @param {string} options.reason - Reason for activation
   * @param {string} [options.actor='system'] - Who activated
   * @param {boolean} [options.stopAllRuns=true] - Stop all running runs
   */
  activateGlobal({ reason, actor = 'system', stopAllRuns = true }) {
    const wasActive = this.#state.global_kill;

    this.#state.global_kill = true;
    this.#state.reason = reason;
    this.#state.activated_by = actor;
    this.#state.activated_at = Date.now();

    // Emit audit
    emitAudit({
      actor,
      action: 'KILL_SWITCH_ACTIVATE',
      target_type: 'system',
      target_id: 'global',
      reason,
      metadata: {
        kill_type: 'global',
        was_active: wasActive
      }
    });

    console.warn(JSON.stringify({
      event: 'kill_switch_activated',
      type: 'global',
      reason,
      actor,
      timestamp: new Date().toISOString()
    }));

    // Notify listeners
    this.#notifyListeners('activate', { type: 'global', reason });

    // Stop all runs if requested
    if (stopAllRuns) {
      this.#stopAllRuns('KILL_SWITCH');
    }

    return { success: true, was_active: wasActive };
  }

  /**
   * Activate kill switch for specific symbol(s)
   * @param {Object} options
   * @param {string[]} options.symbols - Symbols to kill
   * @param {string} options.reason - Reason for activation
   * @param {string} [options.actor='system'] - Who activated
   */
  activateSymbols({ symbols, reason, actor = 'system' }) {
    const added = [];

    for (const symbol of symbols) {
      const upper = symbol.toUpperCase();
      if (!this.#state.symbol_kill.has(upper)) {
        this.#state.symbol_kill.add(upper);
        added.push(upper);
      }
    }

    if (added.length === 0) {
      return { success: true, added: [], message: 'Symbols already killed' };
    }

    this.#state.reason = reason;
    this.#state.activated_by = actor;
    this.#state.activated_at = Date.now();

    // Emit audit
    emitAudit({
      actor,
      action: 'KILL_SWITCH_ACTIVATE',
      target_type: 'system',
      target_id: 'symbols',
      reason,
      metadata: {
        kill_type: 'symbol',
        symbols: added
      }
    });

    console.warn(JSON.stringify({
      event: 'kill_switch_activated',
      type: 'symbol',
      symbols: added,
      reason,
      actor,
      timestamp: new Date().toISOString()
    }));

    // Notify listeners
    this.#notifyListeners('activate', { type: 'symbol', symbols: added, reason });

    return { success: true, added };
  }

  /**
   * Deactivate global kill switch
   * @param {Object} options
   * @param {string} [options.actor='system'] - Who deactivated
   */
  deactivateGlobal({ actor = 'system' } = {}) {
    const wasActive = this.#state.global_kill;

    if (!wasActive) {
      return { success: true, was_active: false };
    }

    this.#state.global_kill = false;

    // Clear reason only if no symbol kills remain
    if (this.#state.symbol_kill.size === 0) {
      this.#state.reason = '';
      this.#state.activated_by = null;
      this.#state.activated_at = null;
    }

    // Emit audit
    emitAudit({
      actor,
      action: 'KILL_SWITCH_DEACTIVATE',
      target_type: 'system',
      target_id: 'global',
      reason: 'Manual deactivation',
      metadata: {
        kill_type: 'global'
      }
    });

    console.info(JSON.stringify({
      event: 'kill_switch_deactivated',
      type: 'global',
      actor,
      timestamp: new Date().toISOString()
    }));

    // Notify listeners
    this.#notifyListeners('deactivate', { type: 'global' });

    return { success: true, was_active: true };
  }

  /**
   * Deactivate kill switch for specific symbol(s)
   * @param {Object} options
   * @param {string[]} options.symbols - Symbols to unkill
   * @param {string} [options.actor='system'] - Who deactivated
   */
  deactivateSymbols({ symbols, actor = 'system' }) {
    const removed = [];

    for (const symbol of symbols) {
      const upper = symbol.toUpperCase();
      if (this.#state.symbol_kill.has(upper)) {
        this.#state.symbol_kill.delete(upper);
        removed.push(upper);
      }
    }

    if (removed.length === 0) {
      return { success: true, removed: [], message: 'Symbols not killed' };
    }

    // Clear activation info if nothing left
    if (!this.#state.global_kill && this.#state.symbol_kill.size === 0) {
      this.#state.reason = '';
      this.#state.activated_by = null;
      this.#state.activated_at = null;
    }

    // Emit audit
    emitAudit({
      actor,
      action: 'KILL_SWITCH_DEACTIVATE',
      target_type: 'system',
      target_id: 'symbols',
      reason: 'Manual deactivation',
      metadata: {
        kill_type: 'symbol',
        symbols: removed
      }
    });

    console.info(JSON.stringify({
      event: 'kill_switch_deactivated',
      type: 'symbol',
      symbols: removed,
      actor,
      timestamp: new Date().toISOString()
    }));

    // Notify listeners
    this.#notifyListeners('deactivate', { type: 'symbol', symbols: removed });

    return { success: true, removed };
  }

  /**
   * Deactivate all kill switches
   * @param {Object} options
   * @param {string} [options.actor='system'] - Who deactivated
   */
  deactivateAll({ actor = 'system' } = {}) {
    const wasGlobal = this.#state.global_kill;
    const wasSymbols = Array.from(this.#state.symbol_kill);

    this.#state.global_kill = false;
    this.#state.symbol_kill.clear();
    this.#state.reason = '';
    this.#state.activated_by = null;
    this.#state.activated_at = null;

    // Emit audit
    emitAudit({
      actor,
      action: 'KILL_SWITCH_DEACTIVATE',
      target_type: 'system',
      target_id: 'all',
      reason: 'Manual deactivation - all',
      metadata: {
        was_global: wasGlobal,
        was_symbols: wasSymbols
      }
    });

    console.info(JSON.stringify({
      event: 'kill_switch_deactivated',
      type: 'all',
      actor,
      timestamp: new Date().toISOString()
    }));

    // Notify listeners
    this.#notifyListeners('deactivate', { type: 'all' });

    return { success: true, was_global: wasGlobal, was_symbols: wasSymbols };
  }

  /**
   * Register a callback to stop a run when kill switch activates
   * @param {string} runId - Run ID
   * @param {function} stopFn - Function to call to stop the run
   */
  registerRun(runId, stopFn) {
    this.#runStopCallbacks.set(runId, stopFn);
  }

  /**
   * Unregister a run's stop callback
   * @param {string} runId - Run ID
   */
  unregisterRun(runId) {
    this.#runStopCallbacks.delete(runId);
  }

  /**
   * Add a listener for kill switch state changes
   * @param {function} listener - Called with (event, data)
   */
  addListener(listener) {
    this.#listeners.add(listener);
  }

  /**
   * Remove a listener
   * @param {function} listener
   */
  removeListener(listener) {
    this.#listeners.delete(listener);
  }

  /**
   * Stop all registered runs
   * @param {string} reason - Stop reason
   */
  #stopAllRuns(reason) {
    const count = this.#runStopCallbacks.size;

    for (const [runId, stopFn] of this.#runStopCallbacks) {
      try {
        stopFn();
        console.warn(JSON.stringify({
          event: 'kill_switch_stopped_run',
          run_id: runId,
          reason
        }));
      } catch (err) {
        console.error(JSON.stringify({
          event: 'kill_switch_stop_error',
          run_id: runId,
          error: err.message
        }));
      }
    }

    return count;
  }

  /**
   * Notify all listeners of state change
   */
  #notifyListeners(event, data) {
    for (const listener of this.#listeners) {
      try {
        listener(event, data);
      } catch (err) {
        console.error(JSON.stringify({
          event: 'kill_switch_listener_error',
          error: err.message
        }));
      }
    }
  }

  /**
   * Emergency stop - activates global kill and stops everything immediately
   * @param {Object} options
   * @param {string} options.reason - Reason for emergency stop
   * @param {string} [options.actor='system'] - Who triggered
   */
  emergencyStop({ reason, actor = 'system' }) {
    console.error(JSON.stringify({
      event: 'EMERGENCY_STOP_TRIGGERED',
      reason,
      actor,
      timestamp: new Date().toISOString()
    }));

    // Activate global kill
    this.activateGlobal({ reason: `EMERGENCY: ${reason}`, actor, stopAllRuns: true });

    // Emit special emergency audit
    emitAudit({
      actor,
      action: 'EMERGENCY_STOP',
      target_type: 'system',
      target_id: 'global',
      reason,
      metadata: {
        stopped_runs: this.#runStopCallbacks.size
      }
    });

    return {
      success: true,
      stopped_runs: this.#runStopCallbacks.size
    };
  }
}

// Singleton instance
let instance = null;

/**
 * Get the singleton KillSwitchManager instance
 * @returns {KillSwitchManager}
 */
export function getKillSwitchManager() {
  if (!instance) {
    instance = new KillSwitchManager();
  }
  return instance;
}

/**
 * Reset the singleton (for testing only)
 */
export function resetKillSwitchManager() {
  instance = null;
}

export { KillSwitchManager };
export default getKillSwitchManager;
