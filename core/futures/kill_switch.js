/**
 * Kill Switch utility functions.
 * Re-exports from KillSwitchManager for backwards compatibility.
 */

/**
 * Load kill-switch config from environment variables.
 * @returns {Object} Kill switch config
 */
export function loadKillSwitchFromEnv() {
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
 * @param {Object} config - Kill switch config (from loadKillSwitchFromEnv)
 * @returns {Object} Kill switch result { pass, reason_code, reason_detail }
 */
export function evaluateKillSwitch(intent, config) {
  // Check global kill
  if (config.global_kill) {
    return {
      pass: false,
      reason_code: 'GLOBAL_KILL_ACTIVE',
      reason_detail: config.reason || 'Global kill switch is active'
    };
  }

  // Check symbol kill
  const symbol = (intent.symbol || '').toUpperCase();
  if (symbol && config.symbol_kill[symbol]) {
    return {
      pass: false,
      reason_code: 'SYMBOL_KILL_ACTIVE',
      reason_detail: `Kill switch active for symbol: ${symbol}`
    };
  }

  return { pass: true };
}
