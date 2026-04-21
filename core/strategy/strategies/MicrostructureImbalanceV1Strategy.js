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
  if (familyId !== 'microstructure_imbalance_v1') {
    throw new Error('MICROSTRUCTURE_IMBALANCE_V1_CONFIG_ERROR: family_id must be microstructure_imbalance_v1');
  }

  const bindingMode = String(config.binding_mode || '').trim();
  if (bindingMode !== 'PAPER_DIRECTIONAL_V1') {
    throw new Error('MICROSTRUCTURE_IMBALANCE_V1_CONFIG_ERROR: binding_mode must be PAPER_DIRECTIONAL_V1');
  }

  const stream = String(config.stream || '').trim().toLowerCase();
  if (stream !== 'trade') {
    throw new Error('MICROSTRUCTURE_IMBALANCE_V1_CONFIG_ERROR: stream must be trade');
  }
  const exchange = String(config.exchange || '').trim().toLowerCase();
  if (!exchange) {
    throw new Error('MICROSTRUCTURE_IMBALANCE_V1_CONFIG_ERROR: exchange required');
  }

  const symbols = Array.isArray(config.symbols)
    ? config.symbols.map((value) => normalizeSymbol(value)).filter(Boolean)
    : [];
  if (symbols.length !== 1) {
    throw new Error('MICROSTRUCTURE_IMBALANCE_V1_CONFIG_ERROR: exactly one symbol required');
  }

  const orderQty = toPositiveNumber(config.orderQty);
  if (orderQty === null) {
    throw new Error('MICROSTRUCTURE_IMBALANCE_V1_CONFIG_ERROR: positive orderQty required');
  }

  const params = config.params && typeof config.params === 'object' ? config.params : {};
  const selectedCell = config.selected_cell && typeof config.selected_cell === 'object' ? config.selected_cell : null;
  if (!selectedCell) {
    throw new Error('MICROSTRUCTURE_IMBALANCE_V1_CONFIG_ERROR: selected_cell object required');
  }

  const deltaMs = toPositiveInt(selectedCell.delta_ms);
  const hMs = toPositiveInt(selectedCell.h_ms);
  const pressureThreshold = toPositiveNumber(selectedCell.pressure_threshold);
  if (!deltaMs || !hMs || pressureThreshold === null || pressureThreshold > 1) {
    throw new Error('MICROSTRUCTURE_IMBALANCE_V1_CONFIG_ERROR: selected_cell delta_ms/h_ms/pressure_threshold invalid');
  }

  const selectedSymbol = normalizeSymbol(selectedCell.symbol);
  if (selectedSymbol && selectedSymbol !== symbols[0]) {
    throw new Error('MICROSTRUCTURE_IMBALANCE_V1_CONFIG_ERROR: selected_cell symbol mismatch');
  }
  if (String(selectedCell.stream || '').trim().toLowerCase() !== stream) {
    throw new Error('MICROSTRUCTURE_IMBALANCE_V1_CONFIG_ERROR: selected_cell stream mismatch');
  }
  if (String(selectedCell.exchange || '').trim().toLowerCase() !== exchange) {
    throw new Error('MICROSTRUCTURE_IMBALANCE_V1_CONFIG_ERROR: selected_cell exchange mismatch');
  }

  const eventCount = toPositiveInt(selectedCell.event_count);
  const tStat = toFiniteNumber(selectedCell.t_stat);
  const meanSignedFwdReturnBps = toFiniteNumber(selectedCell.mean_signed_fwd_return_bps);
  if (eventCount === null || tStat === null || meanSignedFwdReturnBps === null) {
    throw new Error('MICROSTRUCTURE_IMBALANCE_V1_CONFIG_ERROR: selected_cell event_count/mean_signed_fwd_return_bps/t_stat required');
  }
  if (!(meanSignedFwdReturnBps > 0) || !(tStat >= 2)) {
    throw new Error('MICROSTRUCTURE_IMBALANCE_V1_CONFIG_ERROR: selected_cell must satisfy directional microstructure pass bar');
  }

  const toleranceMs = Number.isInteger(Number(params.tolerance_ms))
    ? Math.max(0, Number(params.tolerance_ms))
    : 0;
  const exitPressureThresholdRaw = toPositiveNumber(params.exit_pressure_threshold);
  const exitPressureThreshold = exitPressureThresholdRaw === null
    ? pressureThreshold
    : exitPressureThresholdRaw;
  if (!(exitPressureThreshold > 0) || exitPressureThreshold > pressureThreshold) {
    throw new Error('MICROSTRUCTURE_IMBALANCE_V1_CONFIG_ERROR: exit_pressure_threshold must be > 0 and <= selected_cell.pressure_threshold');
  }
  const minHoldMs = hMs;

  return {
    familyId,
    bindingMode,
    exchange,
    stream,
    symbol: symbols[0],
    symbols,
    orderQty,
    window: String(config.window || '').trim() || null,
    params: cloneJson(params),
    selectedCell: cloneJson(selectedCell),
    deltaNs: BigInt(deltaMs) * 1_000_000n,
    hNs: BigInt(minHoldMs) * 1_000_000n,
    toleranceNs: BigInt(toleranceMs) * 1_000_000n,
    historyRetentionNs: BigInt(Math.max(deltaMs * 4, 60_000)) * 1_000_000n,
    pressureThreshold,
    exitPressureThreshold,
    minHoldMs,
    eventCount,
    tStat,
    meanSignedFwdReturnBps,
  };
}

function parseTradeSide(value) {
  if (typeof value === 'number' && Number.isFinite(value)) {
    if (value > 0) return 1;
    if (value < 0) return -1;
  }
  const text = String(value || '').trim().toUpperCase();
  if (text === 'BUY' || text === 'BID' || text === '1') return 1;
  if (text === 'SELL' || text === 'ASK' || text === '-1') return -1;
  return null;
}

function extractTradeSample(event, expectedSymbol, expectedStream) {
  if (!event || typeof event !== 'object') return null;
  if (normalizeSymbol(event.symbol) !== expectedSymbol) return null;
  if (event.stream !== undefined && String(event.stream || '').trim().toLowerCase() !== expectedStream) return null;
  const qty = toPositiveNumber(event.qty ?? event.trade_qty ?? event.size);
  const side = parseTradeSide(event.side ?? event.aggressor_side ?? event.trade_side);
  const price = toPositiveNumber(event.price ?? event.close);
  const tsEventNs = toBigIntOrNull(event.ts_event);
  if (qty === null || side === null || price === null || tsEventNs === null) return null;
  return {
    tsEventNs,
    signedQty: side * qty,
    qty,
    price,
  };
}

function pressureSignal(pressure, threshold) {
  if (pressure >= threshold) return 'LONG';
  if (pressure <= -threshold) return 'SHORT';
  return 'FLAT';
}

function evaluateFeeAwareEntry({ pressure, price, orderQty, config }) {
  const scale = toPositiveNumber(config.params?.fee_aware_expected_edge_scale);
  if (scale === null) {
    return {
      enabled: false,
      expectedEdgeQuote: null,
      estimatedFeeQuote: null,
      allowEntry: true,
    };
  }

  const staticFeeQuote = toPositiveNumber(config.params?.estimated_fee_per_trade_quote);
  const dynamicFeeRate = toPositiveNumber(config.params?.fee_aware_fee_rate);
  const estimatedFeeQuote = staticFeeQuote !== null
    ? staticFeeQuote
    : (dynamicFeeRate === null ? null : price * orderQty * dynamicFeeRate);
  const expectedEdgeQuote = Math.abs(pressure) * scale;
  const allowEntry = estimatedFeeQuote === null
    ? true
    : expectedEdgeQuote > estimatedFeeQuote;
  return {
    enabled: true,
    expectedEdgeQuote,
    estimatedFeeQuote,
    allowEntry,
  };
}

function buildOrderIntent(symbol, side, qty, metadata = {}) {
  return {
    symbol,
    side,
    qty,
    ...metadata,
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

function positionSideFromSize(size) {
  if (size > 0) return 'LONG';
  if (size < 0) return 'SHORT';
  return 'FLAT';
}

function buildSelectedCellSnapshot(config) {
  return {
    delta_ms: config.selectedCell.delta_ms,
    h_ms: config.selectedCell.h_ms,
    pressure_threshold: config.selectedCell.pressure_threshold,
    symbol: config.symbol,
    exchange: config.exchange,
  };
}

function nextTradeSequenceId(state) {
  const raw = toPositiveInt(state?.next_trade_sequence_id);
  return raw === null ? 1 : raw;
}

function buildEntryTradeContext({
  config,
  tradeSequenceId,
  tsEventNs,
  pressure,
  entrySide,
  entrySignalReason,
  priorPositionSide,
  wasReversalTrade,
}) {
  const absPressure = Math.abs(pressure);
  return {
    schema_version: 'microstructure_trade_context_v0',
    trade_sequence_id: tradeSequenceId,
    entry_timestamp: tsEventNs.toString(),
    entry_pressure: pressure,
    entry_abs_pressure: absPressure,
    entry_threshold: config.pressureThreshold,
    exit_threshold: config.exitPressureThreshold,
    entry_side: entrySide,
    entry_signal_reason: entrySignalReason,
    entry_selected_cell: buildSelectedCellSnapshot(config),
    prior_position_side: priorPositionSide,
    was_reversal_trade: Boolean(wasReversalTrade),
    max_abs_pressure_seen_during_trade: absPressure,
    min_abs_pressure_seen_during_trade: absPressure,
    observation_count_during_trade: 1,
  };
}

function updateActiveTradeContext(activeTradeContext, pressure) {
  if (!activeTradeContext || typeof activeTradeContext !== 'object') {
    return activeTradeContext;
  }
  const absPressure = Math.abs(pressure);
  const previousMax = toFiniteNumber(activeTradeContext.max_abs_pressure_seen_during_trade);
  const previousMin = toFiniteNumber(activeTradeContext.min_abs_pressure_seen_during_trade);
  const previousCount = toPositiveInt(activeTradeContext.observation_count_during_trade) || 0;
  return {
    ...activeTradeContext,
    max_abs_pressure_seen_during_trade: previousMax === null ? absPressure : Math.max(previousMax, absPressure),
    min_abs_pressure_seen_during_trade: previousMin === null ? absPressure : Math.min(previousMin, absPressure),
    observation_count_during_trade: previousCount + 1,
  };
}

function buildExitTradeContext(activeTradeContext, { tsEventNs, pressure, exitReason, priorPositionSide }) {
  const context = activeTradeContext && typeof activeTradeContext === 'object' ? activeTradeContext : {};
  const entryTsEventNs = toBigIntOrNull(context.entry_timestamp);
  const holdDurationMs = entryTsEventNs === null
    ? null
    : Math.max(0, Number((tsEventNs - entryTsEventNs) / 1_000_000n));
  const entryAbsPressure = toFiniteNumber(context.entry_abs_pressure);
  const exitAbsPressure = Math.abs(pressure);
  return {
    schema_version: String(context.schema_version || 'microstructure_trade_context_v0'),
    trade_sequence_id: toPositiveInt(context.trade_sequence_id),
    entry_timestamp: context.entry_timestamp || null,
    entry_pressure: toFiniteNumber(context.entry_pressure),
    entry_abs_pressure: entryAbsPressure,
    entry_threshold: toFiniteNumber(context.entry_threshold),
    exit_threshold: toFiniteNumber(context.exit_threshold),
    entry_side: String(context.entry_side || '').trim().toUpperCase() || null,
    entry_signal_reason: String(context.entry_signal_reason || '').trim() || null,
    entry_selected_cell: context.entry_selected_cell && typeof context.entry_selected_cell === 'object'
      ? cloneJson(context.entry_selected_cell)
      : null,
    prior_position_side: priorPositionSide,
    was_reversal_trade: Boolean(context.was_reversal_trade),
    exit_timestamp: tsEventNs.toString(),
    exit_pressure: pressure,
    exit_abs_pressure: exitAbsPressure,
    exit_reason: exitReason,
    hold_duration_ms: holdDurationMs,
    max_abs_pressure_seen_during_trade: toFiniteNumber(context.max_abs_pressure_seen_during_trade),
    min_abs_pressure_seen_during_trade: toFiniteNumber(context.min_abs_pressure_seen_during_trade),
    pressure_decay_at_exit: entryAbsPressure === null ? null : entryAbsPressure - exitAbsPressure,
    observation_count_during_trade: toPositiveInt(context.observation_count_during_trade),
  };
}

function buildTradeContextPayload({ openingTradeContext = null, closingTradeContext = null }) {
  const payload = {
    schema_version: 'microstructure_trade_context_v0',
  };
  if (openingTradeContext && typeof openingTradeContext === 'object') {
    payload.opening_trade = cloneJson(openingTradeContext);
  }
  if (closingTradeContext && typeof closingTradeContext === 'object') {
    payload.closing_trade = cloneJson(closingTradeContext);
  }
  return payload.opening_trade || payload.closing_trade ? payload : null;
}

export class MicrostructureImbalanceV1Strategy {
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
      pressure_threshold: this.config.pressureThreshold,
      exit_pressure_threshold: this.config.exitPressureThreshold,
      fee_aware_entry_enabled: toPositiveNumber(this.config.params.fee_aware_expected_edge_scale) !== null,
      min_hold_ms: this.config.minHoldMs,
      last_pressure: null,
      last_signal: null,
      last_action: null,
      commit_until_ts_event: null,
      next_trade_sequence_id: 1,
      active_trade_context: null,
    };
  }

  async onInit(ctx) {
    ctx.logger.info(
      `[MicrostructureImbalanceV1Strategy] init symbol=${this.config.symbol} delta_ms=${this.config.selectedCell.delta_ms} h_ms=${this.config.selectedCell.h_ms} min_hold_ms=${this.config.minHoldMs} entry_threshold=${this.config.pressureThreshold} exit_threshold=${this.config.exitPressureThreshold} orderQty=${this.config.orderQty}`
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

    this.samples.push(sample);
    const windowStart = sample.tsEventNs - this.config.deltaNs;
    const pruneBefore = sample.tsEventNs - this.config.historyRetentionNs;
    while (this.samples.length > 0 && this.samples[0].tsEventNs < pruneBefore) {
      this.samples.shift();
    }

    let signedVolume = 0;
    let totalVolume = 0;
    for (let i = this.samples.length - 1; i >= 0; i -= 1) {
      const item = this.samples[i];
      if (item.tsEventNs < windowStart) break;
      signedVolume += item.signedQty;
      totalVolume += item.qty;
    }
    if (totalVolume <= 0) {
      return;
    }

    const pressure = signedVolume / totalVolume;
    const signalDirection = pressureSignal(pressure, this.config.pressureThreshold);
    this.state.signal_event_count += 1;
    this.state.last_pressure = pressure;
    this.state.last_signal = {
      ts_event: sample.tsEventNs.toString(),
      signal_direction: signalDirection,
      pressure,
      signed_volume: signedVolume,
      total_volume: totalVolume,
      pressure_threshold: this.config.pressureThreshold,
      exit_pressure_threshold: this.config.exitPressureThreshold,
      delta_ms: this.config.selectedCell.delta_ms,
      h_ms: this.config.selectedCell.h_ms,
      min_hold_ms: this.config.minHoldMs,
      mean_signed_fwd_return_bps: this.config.meanSignedFwdReturnBps,
      t_stat: this.config.tStat,
      event_count: this.config.eventCount,
    };
    const currentSize = getPositionSize(ctx, this.config.symbol);
    const currentPositionSide = positionSideFromSize(currentSize);
    if (currentSize !== 0 && this.state.active_trade_context) {
      this.state.active_trade_context = updateActiveTradeContext(this.state.active_trade_context, pressure);
    }
    const feeAwareEntry = evaluateFeeAwareEntry({
      pressure,
      price: sample.price,
      orderQty: this.config.orderQty,
      config: this.config,
    });
    this.state.last_signal.fee_aware_entry = feeAwareEntry;

    if (currentSize === 0 && this.state.commit_until_ts_event !== null) {
      this.state.commit_until_ts_event = null;
    }
    const commitUntilTsNs = currentSize === 0 ? null : toBigIntOrNull(this.state.commit_until_ts_event);
    const commitActive = currentSize !== 0
      && commitUntilTsNs !== null
      && sample.tsEventNs < commitUntilTsNs;

    let action = currentSize === 0 ? 'STAY_FLAT' : (currentSize > 0 ? 'HOLD_LONG' : 'HOLD_SHORT');
    let orderIntent = null;
    let nextActiveTradeContext = null;
    let shouldAdvanceTradeSequence = false;
    if (commitActive) {
      action = currentSize > 0 ? 'HOLD_LONG' : 'HOLD_SHORT';
    } else if (currentSize === 0) {
      if (signalDirection === 'LONG') {
        if (feeAwareEntry.allowEntry) {
          action = 'LONG_OPEN';
          nextActiveTradeContext = buildEntryTradeContext({
            config: this.config,
            tradeSequenceId: nextTradeSequenceId(this.state),
            tsEventNs: sample.tsEventNs,
            pressure,
            entrySide: 'LONG',
            entrySignalReason: 'LONG_ENTRY',
            priorPositionSide: currentPositionSide,
            wasReversalTrade: false,
          });
          shouldAdvanceTradeSequence = true;
          orderIntent = buildOrderIntent(this.config.symbol, 'BUY', this.config.orderQty, {
            action: 'LONG',
            position_effect: 'OPEN',
            signal_action: action,
            ts_event: sample.tsEventNs,
            trade_context: buildTradeContextPayload({ openingTradeContext: nextActiveTradeContext }),
          });
        } else {
          action = 'SKIP_LONG_LOW_EDGE';
        }
      } else if (signalDirection === 'SHORT') {
        if (feeAwareEntry.allowEntry) {
          action = 'SHORT_OPEN';
          nextActiveTradeContext = buildEntryTradeContext({
            config: this.config,
            tradeSequenceId: nextTradeSequenceId(this.state),
            tsEventNs: sample.tsEventNs,
            pressure,
            entrySide: 'SHORT',
            entrySignalReason: 'SHORT_ENTRY',
            priorPositionSide: currentPositionSide,
            wasReversalTrade: false,
          });
          shouldAdvanceTradeSequence = true;
          orderIntent = buildOrderIntent(this.config.symbol, 'SELL', this.config.orderQty, {
            action: 'SHORT',
            position_effect: 'OPEN',
            signal_action: action,
            ts_event: sample.tsEventNs,
            trade_context: buildTradeContextPayload({ openingTradeContext: nextActiveTradeContext }),
          });
        } else {
          action = 'SKIP_SHORT_LOW_EDGE';
        }
      }
    } else if (currentSize > 0) {
      // Hysteresis: only reverse on strong opposite pressure; close in the neutral band.
      if (signalDirection === 'SHORT') {
        if (feeAwareEntry.allowEntry) {
          action = 'LONG_TO_SHORT_REVERSAL';
          const closingTradeContext = buildExitTradeContext(this.state.active_trade_context, {
            tsEventNs: sample.tsEventNs,
            pressure,
            exitReason: 'REVERSAL_EXIT',
            priorPositionSide: currentPositionSide,
          });
          nextActiveTradeContext = buildEntryTradeContext({
            config: this.config,
            tradeSequenceId: nextTradeSequenceId(this.state),
            tsEventNs: sample.tsEventNs,
            pressure,
            entrySide: 'SHORT',
            entrySignalReason: 'REVERSAL_ENTRY',
            priorPositionSide: currentPositionSide,
            wasReversalTrade: true,
          });
          shouldAdvanceTradeSequence = true;
          orderIntent = buildOrderIntent(this.config.symbol, 'SELL', Math.abs(currentSize) + this.config.orderQty, {
            action: 'EXIT_LONG',
            position_effect: 'REVERSE',
            signal_action: action,
            ts_event: sample.tsEventNs,
            trade_context: buildTradeContextPayload({
              openingTradeContext: nextActiveTradeContext,
              closingTradeContext,
            }),
          });
        } else {
          action = 'LONG_CLOSE';
          orderIntent = buildOrderIntent(this.config.symbol, 'SELL', Math.abs(currentSize), {
            action: 'EXIT_LONG',
            position_effect: 'CLOSE',
            signal_action: 'LONG_CLOSE_LOW_EDGE_OPPOSITE',
            ts_event: sample.tsEventNs,
            trade_context: buildTradeContextPayload({
              closingTradeContext: buildExitTradeContext(this.state.active_trade_context, {
                tsEventNs: sample.tsEventNs,
                pressure,
                exitReason: 'LOW_EDGE_OPPOSITE_EXIT',
                priorPositionSide: currentPositionSide,
              }),
            }),
          });
        }
      } else if (Math.abs(pressure) < this.config.exitPressureThreshold) {
        action = 'LONG_CLOSE';
        orderIntent = buildOrderIntent(this.config.symbol, 'SELL', Math.abs(currentSize), {
          action: 'EXIT_LONG',
          position_effect: 'CLOSE',
          signal_action: action,
          ts_event: sample.tsEventNs,
          trade_context: buildTradeContextPayload({
            closingTradeContext: buildExitTradeContext(this.state.active_trade_context, {
              tsEventNs: sample.tsEventNs,
              pressure,
              exitReason: 'FLAT_EXIT',
              priorPositionSide: currentPositionSide,
            }),
          }),
        });
      } else {
        action = 'HOLD_LONG';
      }
    } else if (currentSize < 0) {
      if (signalDirection === 'LONG') {
        if (feeAwareEntry.allowEntry) {
          action = 'SHORT_TO_LONG_REVERSAL';
          const closingTradeContext = buildExitTradeContext(this.state.active_trade_context, {
            tsEventNs: sample.tsEventNs,
            pressure,
            exitReason: 'REVERSAL_EXIT',
            priorPositionSide: currentPositionSide,
          });
          nextActiveTradeContext = buildEntryTradeContext({
            config: this.config,
            tradeSequenceId: nextTradeSequenceId(this.state),
            tsEventNs: sample.tsEventNs,
            pressure,
            entrySide: 'LONG',
            entrySignalReason: 'REVERSAL_ENTRY',
            priorPositionSide: currentPositionSide,
            wasReversalTrade: true,
          });
          shouldAdvanceTradeSequence = true;
          orderIntent = buildOrderIntent(this.config.symbol, 'BUY', Math.abs(currentSize) + this.config.orderQty, {
            action: 'EXIT_SHORT',
            position_effect: 'REVERSE',
            signal_action: action,
            ts_event: sample.tsEventNs,
            trade_context: buildTradeContextPayload({
              openingTradeContext: nextActiveTradeContext,
              closingTradeContext,
            }),
          });
        } else {
          action = 'SHORT_CLOSE';
          orderIntent = buildOrderIntent(this.config.symbol, 'BUY', Math.abs(currentSize), {
            action: 'EXIT_SHORT',
            position_effect: 'CLOSE',
            signal_action: 'SHORT_CLOSE_LOW_EDGE_OPPOSITE',
            ts_event: sample.tsEventNs,
            trade_context: buildTradeContextPayload({
              closingTradeContext: buildExitTradeContext(this.state.active_trade_context, {
                tsEventNs: sample.tsEventNs,
                pressure,
                exitReason: 'LOW_EDGE_OPPOSITE_EXIT',
                priorPositionSide: currentPositionSide,
              }),
            }),
          });
        }
      } else if (Math.abs(pressure) < this.config.exitPressureThreshold) {
        action = 'SHORT_CLOSE';
        orderIntent = buildOrderIntent(this.config.symbol, 'BUY', Math.abs(currentSize), {
          action: 'EXIT_SHORT',
          position_effect: 'CLOSE',
          signal_action: action,
          ts_event: sample.tsEventNs,
          trade_context: buildTradeContextPayload({
            closingTradeContext: buildExitTradeContext(this.state.active_trade_context, {
              tsEventNs: sample.tsEventNs,
              pressure,
              exitReason: 'FLAT_EXIT',
              priorPositionSide: currentPositionSide,
            }),
          }),
        });
      } else {
        action = 'HOLD_SHORT';
      }
    }

    if (!orderIntent) {
      this.state.last_action = {
        ts_event: sample.tsEventNs.toString(),
        action,
        signal_direction: signalDirection,
        pressure,
        current_size: currentSize,
        commit_until_ts_event: this.state.commit_until_ts_event,
        commit_active: commitActive,
        fee_aware_entry: feeAwareEntry,
      };
      return;
    }

    ctx.placeOrder(orderIntent);
    this.state.order_event_count += 1;
    if (
      action === 'LONG_OPEN'
      || action === 'SHORT_OPEN'
      || action === 'LONG_TO_SHORT_REVERSAL'
      || action === 'SHORT_TO_LONG_REVERSAL'
    ) {
      this.state.commit_until_ts_event = (sample.tsEventNs + this.config.hNs).toString();
      this.state.active_trade_context = nextActiveTradeContext ? cloneJson(nextActiveTradeContext) : null;
      if (shouldAdvanceTradeSequence) {
        this.state.next_trade_sequence_id = nextTradeSequenceId(this.state) + 1;
      }
    } else if (action === 'LONG_CLOSE' || action === 'SHORT_CLOSE') {
      this.state.commit_until_ts_event = null;
      this.state.active_trade_context = null;
    }
    this.state.last_action = {
      ts_event: sample.tsEventNs.toString(),
      action,
      signal_direction: signalDirection,
      pressure,
      current_size: currentSize,
      order_side: orderIntent.side,
      order_qty: orderIntent.qty,
      commit_until_ts_event: this.state.commit_until_ts_event,
      commit_active: commitActive,
      fee_aware_entry: feeAwareEntry,
      trade_context: orderIntent.trade_context ? cloneJson(orderIntent.trade_context) : null,
    };

    if (this.state.order_event_count === 1) {
      ctx.logger.info(
        `[MicrostructureImbalanceV1Strategy] first_order action=${action} side=${orderIntent.side} qty=${orderIntent.qty} symbol=${this.config.symbol}`
      );
    }
  }

  async onFinalize(ctx) {
    ctx.logger.info(`total_processed: ${ctx.stats.processed}`);
    ctx.logger.info(
      `[MicrostructureImbalanceV1Strategy] finalize matched_trade_events=${this.state.matched_trade_events} signal_event_count=${this.state.signal_event_count} order_event_count=${this.state.order_event_count}`
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

export default MicrostructureImbalanceV1Strategy;
