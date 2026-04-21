import { DEFAULT_FEE_RATE } from '../../execution/fill.js';

function normalizeSymbol(value) {
  return String(value || '').trim().toUpperCase();
}

function toFiniteNumber(value) {
  const n = Number(value);
  return Number.isFinite(n) ? n : null;
}

function toPositiveInt(value) {
  const n = Number(value);
  return Number.isInteger(n) && n > 0 ? n : null;
}

function toPositiveNumber(value) {
  const n = Number(value);
  return Number.isFinite(n) && n > 0 ? n : null;
}

function toBigIntOrNull(value) {
  if (value === undefined || value === null || value === '') return null;
  try {
    return BigInt(value);
  } catch {
    return null;
  }
}

function cloneJson(value) {
  return JSON.parse(JSON.stringify(value));
}

function extractTradeSample(event, expectedSymbol, expectedStream) {
  if (!event || typeof event !== 'object') return null;
  if (normalizeSymbol(event.symbol) !== expectedSymbol) return null;
  if (event.stream !== undefined && String(event.stream || '').trim().toLowerCase() !== expectedStream) return null;
  const price = toFiniteNumber(event.price);
  if (price === null || price <= 0) return null;
  const tsEventRaw = event.ts_event;
  if (tsEventRaw === undefined || tsEventRaw === null) return null;
  let tsEventNs;
  try {
    tsEventNs = BigInt(tsEventRaw);
  } catch {
    return null;
  }
  return {
    tsEventNs,
    price,
  };
}

function findAnchorSample(samples, targetTsNs) {
  for (let i = samples.length - 1; i >= 0; i -= 1) {
    const sample = samples[i];
    if (sample.tsEventNs <= targetTsNs) {
      return sample;
    }
  }
  return null;
}

function getPositionSnapshot(ctx, symbol) {
  if (!ctx || typeof ctx.getExecutionState !== 'function') return null;
  const state = ctx.getExecutionState();
  if (!state || typeof state !== 'object') return null;
  const positions = state.positions && typeof state.positions === 'object' ? state.positions : {};
  const position = positions[symbol];
  if (!position || typeof position !== 'object') return null;
  return position;
}

function getPositionSize(ctx, symbol) {
  const position = getPositionSnapshot(ctx, symbol);
  const size = toFiniteNumber(position?.size);
  return size === null ? 0 : size;
}

function buildOrderIntent(symbol, side, qty, metadata = {}) {
  return {
    symbol,
    side,
    qty,
    ...metadata,
  };
}

function validateConfig(rawConfig) {
  const config = rawConfig && typeof rawConfig === 'object' ? rawConfig : {};
  const familyId = String(config.family_id || '').trim();
  if (familyId !== 'family_b_simple_momentum') {
    throw new Error('FAMILY_B_SIMPLE_MOMENTUM_CONFIG_ERROR: family_id must be family_b_simple_momentum');
  }

  const bindingMode = String(config.binding_mode || '').trim();
  if (bindingMode !== 'PAPER_DIRECTIONAL_V1') {
    throw new Error('FAMILY_B_SIMPLE_MOMENTUM_CONFIG_ERROR: binding_mode must be PAPER_DIRECTIONAL_V1');
  }

  const stream = String(config.stream || '').trim().toLowerCase();
  if (stream !== 'trade') {
    throw new Error('FAMILY_B_SIMPLE_MOMENTUM_CONFIG_ERROR: stream must be trade');
  }
  const exchange = String(config.exchange || '').trim().toLowerCase();
  if (!exchange) {
    throw new Error('FAMILY_B_SIMPLE_MOMENTUM_CONFIG_ERROR: exchange required');
  }

  const symbols = Array.isArray(config.symbols)
    ? config.symbols.map((value) => normalizeSymbol(value)).filter(Boolean)
    : [];
  if (symbols.length !== 1) {
    throw new Error('FAMILY_B_SIMPLE_MOMENTUM_CONFIG_ERROR: exactly one symbol required');
  }

  const orderQty = toPositiveNumber(config.orderQty);
  if (orderQty === null) {
    throw new Error('FAMILY_B_SIMPLE_MOMENTUM_CONFIG_ERROR: positive orderQty required');
  }
  const executionConfig = config.execution_config && typeof config.execution_config === 'object'
    ? config.execution_config
    : null;
  const feeRate = toPositiveNumber(executionConfig?.feeRate) ?? DEFAULT_FEE_RATE;
  const minEdgeCostMultiple = toPositiveNumber(config.min_edge_cost_multiple) ?? 1.25;

  const params = config.params && typeof config.params === 'object' ? config.params : null;
  const selectedCell = config.selected_cell && typeof config.selected_cell === 'object' ? config.selected_cell : null;
  if (!params) {
    throw new Error('FAMILY_B_SIMPLE_MOMENTUM_CONFIG_ERROR: params object required');
  }
  if (!selectedCell) {
    throw new Error('FAMILY_B_SIMPLE_MOMENTUM_CONFIG_ERROR: selected_cell object required');
  }

  const lookbackMinutes = toPositiveInt(params.lookback_minutes ?? selectedCell.lookback_minutes);
  const forwardMinutes = toPositiveInt(params.forward_minutes ?? selectedCell.forward_minutes);
  const signalSupport = toPositiveInt(selectedCell.signal_support);
  const lookbackQuantileThreshold = toFiniteNumber(selectedCell.lookback_quantile_threshold);
  const meanForwardReturn = toFiniteNumber(selectedCell.mean_forward_return);
  const tStat = toFiniteNumber(selectedCell.t_stat);
  if (!lookbackMinutes || !forwardMinutes || signalSupport === null || lookbackQuantileThreshold === null || meanForwardReturn === null || tStat === null) {
    throw new Error('FAMILY_B_SIMPLE_MOMENTUM_CONFIG_ERROR: selected_cell lookback/forward/support/threshold/mean/t_stat required');
  }
  if (!(signalSupport >= 200) || !(meanForwardReturn > 0) || !(Math.abs(tStat) > 2)) {
    throw new Error('FAMILY_B_SIMPLE_MOMENTUM_CONFIG_ERROR: selected_cell must satisfy simple momentum pass bar');
  }

  const selectedSymbol = normalizeSymbol(selectedCell.symbol);
  if (selectedSymbol !== symbols[0]) {
    throw new Error('FAMILY_B_SIMPLE_MOMENTUM_CONFIG_ERROR: selected_cell symbol mismatch');
  }
  if (String(selectedCell.stream || '').trim().toLowerCase() !== stream) {
    throw new Error('FAMILY_B_SIMPLE_MOMENTUM_CONFIG_ERROR: selected_cell stream mismatch');
  }
  if (String(selectedCell.exchange || '').trim().toLowerCase() !== exchange) {
    throw new Error('FAMILY_B_SIMPLE_MOMENTUM_CONFIG_ERROR: selected_cell exchange mismatch');
  }

  const lookbackNs = BigInt(lookbackMinutes * 60 * 1000) * 1_000_000n;
  const holdNs = BigInt(forwardMinutes * 60 * 1000) * 1_000_000n;
  const feeFloorBps = feeRate * 10_000 * 2 * minEdgeCostMultiple;
  const quantileThresholdBps = lookbackQuantileThreshold * 10_000;

  return {
    familyId,
    bindingMode,
    exchange,
    stream,
    symbol: symbols[0],
    symbols,
    orderQty,
    closeOnZeroSignal: true,
    params: cloneJson(params),
    selectedCell: cloneJson(selectedCell),
    lookbackNs,
    holdNs,
    signalSupport,
    meanForwardReturn,
    tStat,
    feeFloorBps,
    requiredLookbackReturnBps: Math.max(quantileThresholdBps, feeFloorBps),
    historyRetentionNs: BigInt(Math.max(lookbackMinutes * 60 * 1000 * 4, 60_000)) * 1_000_000n,
  };
}

export class FamilyBSimpleMomentumStrategy {
  constructor(config = {}) {
    this.config = validateConfig(config);
    this.samples = [];
    this.state = {
      family_id: this.config.familyId,
      binding_mode: this.config.bindingMode,
      symbol: this.config.symbol,
      processed_events: 0,
      matched_trade_events: 0,
      signal_event_count: 0,
      order_event_count: 0,
      ignored_event_count: 0,
      fee_floor_bps: this.config.feeFloorBps,
      required_lookback_return_bps: this.config.requiredLookbackReturnBps,
      last_price: null,
      last_signal: null,
      last_action: null,
      commit_until_ts_event: null,
    };
  }

  async onInit(ctx) {
    ctx.logger.info(
      `[FamilyBSimpleMomentumStrategy] init symbol=${this.config.symbol} lookback_minutes=${this.config.params.lookback_minutes} forward_minutes=${this.config.params.forward_minutes} orderQty=${this.config.orderQty} requiredLookbackReturnBps=${this.config.requiredLookbackReturnBps}`
    );
  }

  async onEvent(event, ctx) {
    this.state.processed_events += 1;

    const sample = extractTradeSample(event, this.config.symbol, this.config.stream);
    if (!sample) {
      this.state.ignored_event_count += 1;
      return;
    }

    this.state.matched_trade_events += 1;
    this.state.last_price = sample.price;

    const anchor = findAnchorSample(this.samples, sample.tsEventNs - this.config.lookbackNs);
    this.samples.push(sample);
    const pruneBefore = sample.tsEventNs - this.config.historyRetentionNs;
    while (this.samples.length > 0 && this.samples[0].tsEventNs < pruneBefore) {
      this.samples.shift();
    }
    if (!anchor || anchor.price <= 0) {
      return;
    }

    const lookbackReturnBps = 10000 * (sample.price - anchor.price) / anchor.price;
    const signalDirection = lookbackReturnBps >= this.config.requiredLookbackReturnBps ? 'LONG' : 'FLAT';
    this.state.signal_event_count += 1;
    this.state.last_signal = {
      ts_event: sample.tsEventNs.toString(),
      signal_direction: signalDirection,
      lookback_return_bps: lookbackReturnBps,
      required_lookback_return_bps: this.config.requiredLookbackReturnBps,
      fee_floor_bps: this.config.feeFloorBps,
      signal_support: this.config.signalSupport,
      mean_forward_return: this.config.meanForwardReturn,
      t_stat: this.config.tStat,
      price: sample.price,
    };

    const currentSize = getPositionSize(ctx, this.config.symbol);
    if (currentSize === 0 && this.state.commit_until_ts_event !== null) {
      this.state.commit_until_ts_event = null;
    }
    const commitUntilTsNs = currentSize === 0 ? null : toBigIntOrNull(this.state.commit_until_ts_event);
    const commitActive = currentSize !== 0
      && commitUntilTsNs !== null
      && sample.tsEventNs < commitUntilTsNs;

    let action = currentSize === 0 ? 'STAY_FLAT' : (currentSize > 0 ? 'HOLD_LONG' : 'HOLD_SHORT');
    let orderIntent = null;
    if (commitActive) {
      action = currentSize > 0 ? 'HOLD_LONG' : 'HOLD_SHORT';
    } else if (signalDirection === 'LONG') {
      if (currentSize < 0) {
        action = 'SHORT_CLOSE';
        orderIntent = buildOrderIntent(this.config.symbol, 'BUY', Math.abs(currentSize), {
          action: 'EXIT_SHORT',
          position_effect: 'CLOSE',
          signal_action: action,
        });
      } else if (currentSize === 0) {
        action = 'LONG_OPEN';
        orderIntent = buildOrderIntent(this.config.symbol, 'BUY', this.config.orderQty, {
          action: 'LONG',
          position_effect: 'OPEN',
          signal_action: action,
        });
      } else {
        action = 'HOLD_LONG';
      }
    } else if (this.config.closeOnZeroSignal) {
      if (currentSize > 0) {
        action = 'LONG_CLOSE';
        orderIntent = buildOrderIntent(this.config.symbol, 'SELL', Math.abs(currentSize), {
          action: 'EXIT_LONG',
          position_effect: 'CLOSE',
          signal_action: action,
        });
      } else if (currentSize < 0) {
        action = 'SHORT_CLOSE';
        orderIntent = buildOrderIntent(this.config.symbol, 'BUY', Math.abs(currentSize), {
          action: 'EXIT_SHORT',
          position_effect: 'CLOSE',
          signal_action: action,
        });
      }
    }

    if (!orderIntent) {
      this.state.last_action = {
        ts_event: sample.tsEventNs.toString(),
        action,
        signal_direction: signalDirection,
        current_size: currentSize,
        required_lookback_return_bps: this.config.requiredLookbackReturnBps,
        commit_until_ts_event: this.state.commit_until_ts_event,
        commit_active: commitActive,
      };
      return;
    }

    ctx.placeOrder(orderIntent);
    this.state.order_event_count += 1;
    if (action === 'LONG_OPEN') {
      this.state.commit_until_ts_event = (sample.tsEventNs + this.config.holdNs).toString();
    } else if (action === 'LONG_CLOSE' || action === 'SHORT_CLOSE') {
      this.state.commit_until_ts_event = null;
    }
    this.state.last_action = {
      ts_event: sample.tsEventNs.toString(),
      action,
      signal_direction: signalDirection,
      current_size: currentSize,
      required_lookback_return_bps: this.config.requiredLookbackReturnBps,
      order_side: orderIntent.side,
      order_qty: orderIntent.qty,
      commit_until_ts_event: this.state.commit_until_ts_event,
      commit_active: commitActive,
    };
  }

  async onFinalize(ctx) {
    ctx.logger.info(`total_processed: ${ctx.stats.processed}`);
    ctx.logger.info(
      `[FamilyBSimpleMomentumStrategy] finalize matched_trade_events=${this.state.matched_trade_events} signal_event_count=${this.state.signal_event_count} order_event_count=${this.state.order_event_count}`
    );
  }

  getState() {
    return cloneJson(this.state);
  }

  setState(state) {
    if (!state || typeof state !== 'object') return;
    this.state = {
      ...this.state,
      ...cloneJson(state),
    };
  }
}

export default FamilyBSimpleMomentumStrategy;
