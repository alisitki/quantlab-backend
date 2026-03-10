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

function validateConfig(rawConfig) {
  const config = rawConfig && typeof rawConfig === 'object' ? rawConfig : {};
  const familyId = String(config.family_id || '').trim();
  if (familyId !== 'momentum_v1') {
    throw new Error('MOMENTUM_V1_CONFIG_ERROR: family_id must be momentum_v1');
  }

  const bindingMode = String(config.binding_mode || '').trim();
  if (bindingMode !== 'PAPER_DIRECTIONAL_V1') {
    throw new Error('MOMENTUM_V1_CONFIG_ERROR: binding_mode must be PAPER_DIRECTIONAL_V1');
  }

  const stream = String(config.stream || '').trim().toLowerCase();
  if (stream !== 'trade') {
    throw new Error('MOMENTUM_V1_CONFIG_ERROR: stream must be trade');
  }
  const exchange = String(config.exchange || '').trim().toLowerCase();
  if (!exchange) {
    throw new Error('MOMENTUM_V1_CONFIG_ERROR: exchange required');
  }

  const symbols = Array.isArray(config.symbols)
    ? config.symbols.map((value) => normalizeSymbol(value)).filter(Boolean)
    : [];
  if (symbols.length !== 1) {
    throw new Error('MOMENTUM_V1_CONFIG_ERROR: exactly one symbol required');
  }

  const orderQty = toPositiveNumber(config.orderQty);
  if (orderQty === null) {
    throw new Error('MOMENTUM_V1_CONFIG_ERROR: positive orderQty required');
  }

  const params = config.params && typeof config.params === 'object' ? config.params : null;
  const selectedCell = config.selected_cell && typeof config.selected_cell === 'object' ? config.selected_cell : null;
  if (!params) {
    throw new Error('MOMENTUM_V1_CONFIG_ERROR: params object required');
  }
  if (!selectedCell) {
    throw new Error('MOMENTUM_V1_CONFIG_ERROR: selected_cell object required');
  }

  const deltaMs = toPositiveInt(selectedCell.delta_ms);
  const hMs = toPositiveInt(selectedCell.h_ms);
  if (!deltaMs || !hMs) {
    throw new Error('MOMENTUM_V1_CONFIG_ERROR: selected_cell delta_ms and h_ms must be positive integers');
  }

  const selectedSymbol = normalizeSymbol(selectedCell.symbol);
  if (selectedSymbol !== symbols[0]) {
    throw new Error('MOMENTUM_V1_CONFIG_ERROR: selected_cell symbol mismatch');
  }
  if (String(selectedCell.stream || '').trim().toLowerCase() !== stream) {
    throw new Error('MOMENTUM_V1_CONFIG_ERROR: selected_cell stream mismatch');
  }
  if (String(selectedCell.exchange || '').trim().toLowerCase() !== exchange) {
    throw new Error('MOMENTUM_V1_CONFIG_ERROR: selected_cell exchange mismatch');
  }

  const eventCount = toPositiveInt(selectedCell.event_count);
  const meanProduct = toFiniteNumber(selectedCell.mean_product);
  const tStat = toFiniteNumber(selectedCell.t_stat);
  if (eventCount === null || meanProduct === null || tStat === null) {
    throw new Error('MOMENTUM_V1_CONFIG_ERROR: selected_cell event_count/mean_product/t_stat required');
  }
  if (!(meanProduct > 0) || !(tStat >= 2)) {
    throw new Error('MOMENTUM_V1_CONFIG_ERROR: selected_cell must satisfy directional momentum pass bar');
  }

  const toleranceMs = Number.isInteger(Number(params.tolerance_ms))
    ? Math.max(0, Number(params.tolerance_ms))
    : 0;
  const deltaMsList = Array.isArray(params.delta_ms_list)
    ? params.delta_ms_list.map((value) => toPositiveInt(value)).filter(Boolean)
    : [];
  const maxDeltaMs = Math.max(deltaMs, ...(deltaMsList.length > 0 ? deltaMsList : [deltaMs]));

  return {
    familyId,
    bindingMode,
    exchange,
    stream,
    symbol: symbols[0],
    symbols,
    orderQty,
    allowReversal: true,
    closeOnZeroSignal: true,
    window: String(config.window || '').trim() || null,
    params: cloneJson(params),
    selectedCell: cloneJson(selectedCell),
    deltaNs: BigInt(deltaMs) * 1_000_000n,
    hNs: BigInt(hMs) * 1_000_000n,
    toleranceNs: BigInt(toleranceMs) * 1_000_000n,
    historyRetentionNs: BigInt(Math.max(deltaMs * 4, maxDeltaMs * 4, 60_000)) * 1_000_000n,
    eventCount,
    meanProduct,
    tStat,
  };
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

function findAnchorSample(samples, targetTsNs, toleranceNs) {
  for (let i = samples.length - 1; i >= 0; i -= 1) {
    const sample = samples[i];
    if (sample.tsEventNs > targetTsNs) continue;
    if (toleranceNs > 0n && targetTsNs - sample.tsEventNs > toleranceNs) {
      return null;
    }
    return sample;
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

function signalFromPastReturn(pastReturnBps) {
  if (pastReturnBps > 0) return 'LONG';
  if (pastReturnBps < 0) return 'SHORT';
  return 'FLAT';
}

function buildOrderIntent(symbol, side, qty) {
  return {
    symbol,
    side,
    qty,
  };
}

export class MomentumV1Strategy {
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
      last_price: null,
      last_signal: null,
      last_action: null,
      commit_until_ts_event: null,
    };
  }

  async onInit(ctx) {
    ctx.logger.info(
      `[MomentumV1Strategy] init symbol=${this.config.symbol} delta_ms=${this.config.selectedCell.delta_ms} h_ms=${this.config.selectedCell.h_ms} orderQty=${this.config.orderQty}`
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

    const anchor = findAnchorSample(
      this.samples,
      sample.tsEventNs - this.config.deltaNs,
      this.config.toleranceNs,
    );

    this.samples.push(sample);
    const pruneBefore = sample.tsEventNs - this.config.historyRetentionNs;
    while (this.samples.length > 0 && this.samples[0].tsEventNs < pruneBefore) {
      this.samples.shift();
    }

    if (!anchor || anchor.price <= 0) {
      return;
    }

    const pastReturnBps = 10000 * (sample.price - anchor.price) / anchor.price;
    const signalDirection = signalFromPastReturn(pastReturnBps);
    this.state.signal_event_count += 1;
    this.state.last_signal = {
      ts_event: sample.tsEventNs.toString(),
      signal_direction: signalDirection,
      past_return_bps: pastReturnBps,
      price: sample.price,
      delta_ms: this.config.selectedCell.delta_ms,
      h_ms: this.config.selectedCell.h_ms,
      mean_product: this.config.meanProduct,
      t_stat: this.config.tStat,
      event_count: this.config.eventCount,
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
        action = 'SHORT_TO_LONG_REVERSAL';
        orderIntent = buildOrderIntent(this.config.symbol, 'BUY', Math.abs(currentSize) + this.config.orderQty);
      } else if (currentSize === 0) {
        action = 'LONG_OPEN';
        orderIntent = buildOrderIntent(this.config.symbol, 'BUY', this.config.orderQty);
      } else {
        action = 'HOLD_LONG';
      }
    } else if (signalDirection === 'SHORT') {
      if (currentSize > 0) {
        action = 'LONG_TO_SHORT_REVERSAL';
        orderIntent = buildOrderIntent(this.config.symbol, 'SELL', Math.abs(currentSize) + this.config.orderQty);
      } else if (currentSize === 0) {
        action = 'SHORT_OPEN';
        orderIntent = buildOrderIntent(this.config.symbol, 'SELL', this.config.orderQty);
      } else {
        action = 'HOLD_SHORT';
      }
    } else if (this.config.closeOnZeroSignal) {
      if (currentSize > 0) {
        action = 'LONG_CLOSE';
        orderIntent = buildOrderIntent(this.config.symbol, 'SELL', Math.abs(currentSize));
      } else if (currentSize < 0) {
        action = 'SHORT_CLOSE';
        orderIntent = buildOrderIntent(this.config.symbol, 'BUY', Math.abs(currentSize));
      } else {
        action = 'STAY_FLAT';
      }
    }

    if (!orderIntent) {
      this.state.last_action = {
        ts_event: sample.tsEventNs.toString(),
        action,
        signal_direction: signalDirection,
        current_size: currentSize,
        commit_until_ts_event: this.state.commit_until_ts_event,
        commit_active: commitActive,
      };
      return;
    }

    ctx.placeOrder(orderIntent);
    this.state.order_event_count += 1;
    if (action === 'LONG_OPEN' || action === 'SHORT_OPEN' || action === 'LONG_TO_SHORT_REVERSAL' || action === 'SHORT_TO_LONG_REVERSAL') {
      this.state.commit_until_ts_event = (sample.tsEventNs + this.config.hNs).toString();
    } else if (action === 'LONG_CLOSE' || action === 'SHORT_CLOSE') {
      this.state.commit_until_ts_event = null;
    }
    this.state.last_action = {
      ts_event: sample.tsEventNs.toString(),
      action,
      signal_direction: signalDirection,
      current_size: currentSize,
      order_side: orderIntent.side,
      order_qty: orderIntent.qty,
      commit_until_ts_event: this.state.commit_until_ts_event,
      commit_active: commitActive,
    };

    if (this.state.order_event_count === 1) {
      ctx.logger.info(
        `[MomentumV1Strategy] first_order action=${action} side=${orderIntent.side} qty=${orderIntent.qty} symbol=${this.config.symbol}`
      );
    }
  }

  async onFinalize(ctx) {
    ctx.logger.info(`total_processed: ${ctx.stats.processed}`);
    ctx.logger.info(
      `[MomentumV1Strategy] finalize matched_trade_events=${this.state.matched_trade_events} signal_event_count=${this.state.signal_event_count} order_event_count=${this.state.order_event_count}`
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

export default MomentumV1Strategy;
