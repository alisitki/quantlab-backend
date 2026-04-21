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
  if (familyId !== 'jump_reversion_v1') {
    throw new Error('JUMP_REVERSION_V1_CONFIG_ERROR: family_id must be jump_reversion_v1');
  }

  const bindingMode = String(config.binding_mode || '').trim();
  if (bindingMode !== 'PAPER_DIRECTIONAL_V1') {
    throw new Error('JUMP_REVERSION_V1_CONFIG_ERROR: binding_mode must be PAPER_DIRECTIONAL_V1');
  }

  const stream = String(config.stream || '').trim().toLowerCase();
  if (stream !== 'trade') {
    throw new Error('JUMP_REVERSION_V1_CONFIG_ERROR: stream must be trade');
  }
  const exchange = String(config.exchange || '').trim().toLowerCase();
  if (!exchange) {
    throw new Error('JUMP_REVERSION_V1_CONFIG_ERROR: exchange required');
  }

  const symbols = Array.isArray(config.symbols)
    ? config.symbols.map((value) => normalizeSymbol(value)).filter(Boolean)
    : [];
  if (symbols.length !== 1) {
    throw new Error('JUMP_REVERSION_V1_CONFIG_ERROR: exactly one symbol required');
  }

  const orderQty = toPositiveNumber(config.orderQty);
  if (orderQty === null) {
    throw new Error('JUMP_REVERSION_V1_CONFIG_ERROR: positive orderQty required');
  }

  const params = config.params && typeof config.params === 'object' ? config.params : null;
  const selectedCell = config.selected_cell && typeof config.selected_cell === 'object' ? config.selected_cell : null;
  if (!params) {
    throw new Error('JUMP_REVERSION_V1_CONFIG_ERROR: params object required');
  }
  if (!selectedCell) {
    throw new Error('JUMP_REVERSION_V1_CONFIG_ERROR: selected_cell object required');
  }

  const jumpThresholdBps = toPositiveNumber(selectedCell.jump_thresh_bps);
  const hMs = toPositiveInt(selectedCell.h_ms);
  const jumpCount = toPositiveInt(selectedCell.jump_count);
  const meanSignedReversal = toFiniteNumber(selectedCell.mean_signed_reversal);
  const tStat = toFiniteNumber(selectedCell.t_stat);
  if (jumpThresholdBps === null || !hMs || jumpCount === null || meanSignedReversal === null || tStat === null) {
    throw new Error('JUMP_REVERSION_V1_CONFIG_ERROR: selected_cell jump_thresh_bps/h_ms/jump_count/mean_signed_reversal/t_stat required');
  }
  if (!(jumpCount >= 200) || !(meanSignedReversal > 0) || !(tStat >= 2)) {
    throw new Error('JUMP_REVERSION_V1_CONFIG_ERROR: selected_cell must satisfy jump reversion pass bar');
  }

  const selectedSymbol = normalizeSymbol(selectedCell.symbol);
  if (selectedSymbol !== symbols[0]) {
    throw new Error('JUMP_REVERSION_V1_CONFIG_ERROR: selected_cell symbol mismatch');
  }
  if (String(selectedCell.stream || '').trim().toLowerCase() !== stream) {
    throw new Error('JUMP_REVERSION_V1_CONFIG_ERROR: selected_cell stream mismatch');
  }
  if (String(selectedCell.exchange || '').trim().toLowerCase() !== exchange) {
    throw new Error('JUMP_REVERSION_V1_CONFIG_ERROR: selected_cell exchange mismatch');
  }

  const cooldownMs = Number.isInteger(Number(params.cooldown_ms))
    ? Math.max(0, Number(params.cooldown_ms))
    : 0;

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
    jumpThresholdBps,
    hNs: BigInt(hMs) * 1_000_000n,
    cooldownNs: BigInt(cooldownMs) * 1_000_000n,
    jumpCount,
    meanSignedReversal,
    tStat,
    cooldownMs,
  };
}

export class JumpReversionV1Strategy {
  constructor(config = {}) {
    this.config = validateConfig(config);
    this.lastSample = null;
    this.lastQualifiedJumpTsNs = null;
    this.state = {
      family_id: this.config.familyId,
      binding_mode: this.config.bindingMode,
      symbol: this.config.symbol,
      processed_events: 0,
      matched_trade_events: 0,
      signal_event_count: 0,
      order_event_count: 0,
      ignored_event_count: 0,
      jump_threshold_bps: this.config.jumpThresholdBps,
      cooldown_ms: this.config.cooldownMs,
      last_price: null,
      last_signal: null,
      last_action: null,
      commit_until_ts_event: null,
      last_qualified_jump_ts_event: null,
    };
  }

  async onInit(ctx) {
    ctx.logger.info(
      `[JumpReversionV1Strategy] init symbol=${this.config.symbol} jump_thresh_bps=${this.config.jumpThresholdBps} h_ms=${this.config.selectedCell.h_ms} cooldown_ms=${this.config.cooldownMs} orderQty=${this.config.orderQty}`
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

    const previousSample = this.lastSample;
    this.lastSample = sample;
    if (!previousSample || previousSample.price <= 0) {
      return;
    }

    const jumpReturnBps = 10000 * (sample.price - previousSample.price) / previousSample.price;
    const absJumpReturnBps = Math.abs(jumpReturnBps);
    const cooldownPassed = this.lastQualifiedJumpTsNs === null
      || this.config.cooldownNs === 0n
      || (sample.tsEventNs - this.lastQualifiedJumpTsNs) >= this.config.cooldownNs;
    const jumpQualified = absJumpReturnBps >= this.config.jumpThresholdBps && cooldownPassed;
    let signalDirection = 'FLAT';
    if (jumpQualified) {
      signalDirection = jumpReturnBps > 0 ? 'SHORT' : (jumpReturnBps < 0 ? 'LONG' : 'FLAT');
      this.lastQualifiedJumpTsNs = sample.tsEventNs;
      this.state.last_qualified_jump_ts_event = sample.tsEventNs.toString();
    }

    this.state.signal_event_count += 1;
    this.state.last_signal = {
      ts_event: sample.tsEventNs.toString(),
      signal_direction: signalDirection,
      jump_return_bps: jumpReturnBps,
      abs_jump_return_bps: absJumpReturnBps,
      jump_threshold_bps: this.config.jumpThresholdBps,
      cooldown_ms: this.config.cooldownMs,
      cooldown_passed: cooldownPassed,
      jump_qualified: jumpQualified,
      price: sample.price,
      h_ms: this.config.selectedCell.h_ms,
      mean_signed_reversal: this.config.meanSignedReversal,
      t_stat: this.config.tStat,
      jump_count: this.config.jumpCount,
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
        orderIntent = buildOrderIntent(this.config.symbol, 'BUY', Math.abs(currentSize) + this.config.orderQty, {
          action: 'EXIT_SHORT',
          position_effect: 'REVERSE',
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
    } else if (signalDirection === 'SHORT') {
      if (currentSize > 0) {
        action = 'LONG_TO_SHORT_REVERSAL';
        orderIntent = buildOrderIntent(this.config.symbol, 'SELL', Math.abs(currentSize) + this.config.orderQty, {
          action: 'EXIT_LONG',
          position_effect: 'REVERSE',
          signal_action: action,
        });
      } else if (currentSize === 0) {
        action = 'SHORT_OPEN';
        orderIntent = buildOrderIntent(this.config.symbol, 'SELL', this.config.orderQty, {
          action: 'SHORT',
          position_effect: 'OPEN',
          signal_action: action,
        });
      } else {
        action = 'HOLD_SHORT';
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
        jump_threshold_bps: this.config.jumpThresholdBps,
        jump_qualified: jumpQualified,
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
      jump_threshold_bps: this.config.jumpThresholdBps,
      jump_qualified: jumpQualified,
      order_side: orderIntent.side,
      order_qty: orderIntent.qty,
      commit_until_ts_event: this.state.commit_until_ts_event,
      commit_active: commitActive,
    };
  }

  async onFinalize(ctx) {
    ctx.logger.info(`total_processed: ${ctx.stats.processed}`);
    ctx.logger.info(
      `[JumpReversionV1Strategy] finalize matched_trade_events=${this.state.matched_trade_events} signal_event_count=${this.state.signal_event_count} order_event_count=${this.state.order_event_count}`
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

export default JumpReversionV1Strategy;
