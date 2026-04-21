function normalizeSymbol(value) {
  return String(value || '').trim().toUpperCase();
}

function normalizeExchange(value) {
  return String(value || '').trim().toLowerCase();
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

function uniqueNormalizedExchanges(values, exclude = '') {
  const seen = new Set();
  const out = [];
  for (const raw of Array.isArray(values) ? values : []) {
    const exchange = normalizeExchange(raw);
    if (!exchange || exchange === exclude || seen.has(exchange)) {
      continue;
    }
    seen.add(exchange);
    out.push(exchange);
  }
  return out;
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

function pressureSignal(pressure, threshold) {
  if (pressure >= threshold) return 'LONG';
  if (pressure <= -threshold) return 'SHORT';
  return 'FLAT';
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

function nextTradeSequenceId(state) {
  const raw = toPositiveInt(state?.next_trade_sequence_id);
  return raw === null ? 1 : raw;
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

function buildTradeContextPayload({ openingTradeContext = null, closingTradeContext = null }) {
  const payload = {
    schema_version: 'microstructure_trade_context_v1',
  };
  if (openingTradeContext && typeof openingTradeContext === 'object') {
    payload.opening_trade = cloneJson(openingTradeContext);
  }
  if (closingTradeContext && typeof closingTradeContext === 'object') {
    payload.closing_trade = cloneJson(closingTradeContext);
  }
  return payload.opening_trade || payload.closing_trade ? payload : null;
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
  confirmationSnapshot,
}) {
  const absPressure = Math.abs(pressure);
  return {
    schema_version: 'microstructure_trade_context_v1',
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
    venue_confirmation_window_ms: config.confirmationWindowMs,
    venue_alignment_count: confirmationSnapshot.venue_alignment_count,
    external_alignment_count: confirmationSnapshot.external_alignment_count,
    external_available_count: confirmationSnapshot.external_available_count,
    required_external_alignment_count: config.requiredExternalAlignmentCount,
    venue_divergence_score: confirmationSnapshot.venue_divergence_score,
    max_venue_divergence_score: config.maxVenueDivergenceScore,
    venue_divergence_pass: confirmationSnapshot.venue_divergence_pass,
    venue_pressure_snapshot: cloneJson(confirmationSnapshot.venue_pressure_snapshot),
    venue_confirmation_reason: confirmationSnapshot.reason,
    market_support_mode: confirmationSnapshot.market_support.mode,
    market_support_flag: confirmationSnapshot.market_support.support_flag,
    market_support_reason: confirmationSnapshot.market_support.reason,
    entry_decision_reason: confirmationSnapshot.entry_decision_reason,
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
    schema_version: String(context.schema_version || 'microstructure_trade_context_v1'),
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
    venue_confirmation_window_ms: toPositiveInt(context.venue_confirmation_window_ms),
    venue_alignment_count: toPositiveInt(context.venue_alignment_count),
    external_alignment_count: toPositiveInt(context.external_alignment_count),
    external_available_count: toPositiveInt(context.external_available_count),
    venue_divergence_score: toFiniteNumber(context.venue_divergence_score),
    venue_divergence_pass: Boolean(context.venue_divergence_pass),
    market_support_mode: String(context.market_support_mode || '').trim() || null,
    market_support_flag: context.market_support_flag === null || context.market_support_flag === undefined
      ? null
      : Boolean(context.market_support_flag),
    market_support_reason: String(context.market_support_reason || '').trim() || null,
    entry_decision_reason: String(context.entry_decision_reason || '').trim() || null,
  };
}

function validateConfig(rawConfig) {
  const config = rawConfig && typeof rawConfig === 'object' ? rawConfig : {};
  const familyId = String(config.family_id || '').trim();
  if (familyId !== 'microstructure_imbalance_v2') {
    throw new Error('MICROSTRUCTURE_IMBALANCE_V2_CONFIG_ERROR: family_id must be microstructure_imbalance_v2');
  }

  const bindingMode = String(config.binding_mode || '').trim();
  if (bindingMode !== 'PAPER_DIRECTIONAL_V1') {
    throw new Error('MICROSTRUCTURE_IMBALANCE_V2_CONFIG_ERROR: binding_mode must be PAPER_DIRECTIONAL_V1');
  }

  const stream = String(config.stream || '').trim().toLowerCase();
  if (stream !== 'trade') {
    throw new Error('MICROSTRUCTURE_IMBALANCE_V2_CONFIG_ERROR: stream must be trade');
  }
  const exchange = normalizeExchange(config.exchange);
  if (!exchange) {
    throw new Error('MICROSTRUCTURE_IMBALANCE_V2_CONFIG_ERROR: exchange required');
  }

  const symbols = Array.isArray(config.symbols)
    ? config.symbols.map((value) => normalizeSymbol(value)).filter(Boolean)
    : [];
  if (symbols.length !== 1) {
    throw new Error('MICROSTRUCTURE_IMBALANCE_V2_CONFIG_ERROR: exactly one symbol required');
  }

  const orderQty = toPositiveNumber(config.orderQty);
  if (orderQty === null) {
    throw new Error('MICROSTRUCTURE_IMBALANCE_V2_CONFIG_ERROR: positive orderQty required');
  }

  const params = config.params && typeof config.params === 'object' ? config.params : {};
  const selectedCell = config.selected_cell && typeof config.selected_cell === 'object' ? config.selected_cell : null;
  if (!selectedCell) {
    throw new Error('MICROSTRUCTURE_IMBALANCE_V2_CONFIG_ERROR: selected_cell object required');
  }

  const deltaMs = toPositiveInt(selectedCell.delta_ms);
  const hMs = toPositiveInt(selectedCell.h_ms);
  const pressureThreshold = toPositiveNumber(selectedCell.pressure_threshold);
  if (!deltaMs || !hMs || pressureThreshold === null || pressureThreshold > 1) {
    throw new Error('MICROSTRUCTURE_IMBALANCE_V2_CONFIG_ERROR: selected_cell delta_ms/h_ms/pressure_threshold invalid');
  }

  const selectedSymbol = normalizeSymbol(selectedCell.symbol);
  if (selectedSymbol && selectedSymbol !== symbols[0]) {
    throw new Error('MICROSTRUCTURE_IMBALANCE_V2_CONFIG_ERROR: selected_cell symbol mismatch');
  }
  if (String(selectedCell.stream || '').trim().toLowerCase() !== stream) {
    throw new Error('MICROSTRUCTURE_IMBALANCE_V2_CONFIG_ERROR: selected_cell stream mismatch');
  }
  if (normalizeExchange(selectedCell.exchange) !== exchange) {
    throw new Error('MICROSTRUCTURE_IMBALANCE_V2_CONFIG_ERROR: selected_cell exchange mismatch');
  }

  const eventCount = toPositiveInt(selectedCell.event_count);
  const tStat = toFiniteNumber(selectedCell.t_stat);
  const meanSignedFwdReturnBps = toFiniteNumber(selectedCell.mean_signed_fwd_return_bps);
  if (eventCount === null || tStat === null || meanSignedFwdReturnBps === null) {
    throw new Error('MICROSTRUCTURE_IMBALANCE_V2_CONFIG_ERROR: selected_cell event_count/mean_signed_fwd_return_bps/t_stat required');
  }
  if (!(meanSignedFwdReturnBps > 0) || !(tStat >= 2)) {
    throw new Error('MICROSTRUCTURE_IMBALANCE_V2_CONFIG_ERROR: selected_cell must satisfy directional microstructure pass bar');
  }

  const toleranceMs = Number.isInteger(Number(params.tolerance_ms))
    ? Math.max(0, Number(params.tolerance_ms))
    : 0;
  const exitPressureThresholdRaw = toPositiveNumber(params.exit_pressure_threshold);
  const exitPressureThreshold = exitPressureThresholdRaw === null
    ? pressureThreshold
    : exitPressureThresholdRaw;
  if (!(exitPressureThreshold > 0) || exitPressureThreshold > pressureThreshold) {
    throw new Error('MICROSTRUCTURE_IMBALANCE_V2_CONFIG_ERROR: exit_pressure_threshold must be > 0 and <= selected_cell.pressure_threshold');
  }

  const confirmation = params.confirmation && typeof params.confirmation === 'object' ? params.confirmation : {};
  const confirmationWindowMs = toPositiveInt(confirmation.window_ms ?? params.confirmation_window_ms) || deltaMs;
  const confirmationVenues = uniqueNormalizedExchanges(
    confirmation.venues ?? params.confirmation_venues ?? ['binance', 'okx'],
    exchange,
  );
  if (confirmationVenues.length === 0) {
    throw new Error('MICROSTRUCTURE_IMBALANCE_V2_CONFIG_ERROR: at least one external confirmation venue required');
  }
  const requiredExternalAlignmentCount = toPositiveInt(
    confirmation.required_alignment_count ?? params.required_external_alignment_count,
  ) || confirmationVenues.length;
  const minExternalAvailableCount = toPositiveInt(
    confirmation.min_available_count ?? params.min_external_available_count,
  ) || confirmationVenues.length;
  if (requiredExternalAlignmentCount > confirmationVenues.length) {
    throw new Error('MICROSTRUCTURE_IMBALANCE_V2_CONFIG_ERROR: required_external_alignment_count exceeds confirmation venues');
  }
  if (minExternalAvailableCount > confirmationVenues.length) {
    throw new Error('MICROSTRUCTURE_IMBALANCE_V2_CONFIG_ERROR: min_external_available_count exceeds confirmation venues');
  }
  const maxVenueDivergenceScore = toPositiveNumber(
    confirmation.max_divergence_score ?? params.max_venue_divergence_score ?? 0.35,
  );
  if (maxVenueDivergenceScore === null || maxVenueDivergenceScore > 2) {
    throw new Error('MICROSTRUCTURE_IMBALANCE_V2_CONFIG_ERROR: max_venue_divergence_score invalid');
  }

  const btcSupport = params.btc_support && typeof params.btc_support === 'object' ? params.btc_support : {};
  const btcSupportMode = String(
    btcSupport.mode ?? params.btc_support_mode ?? 'DISABLED',
  ).trim().toUpperCase();
  if (!['DISABLED', 'FLAG_ONLY', 'REQUIRE_SUPPORTIVE'].includes(btcSupportMode)) {
    throw new Error('MICROSTRUCTURE_IMBALANCE_V2_CONFIG_ERROR: unsupported btc_support_mode');
  }
  const btcSupportEnabled = btcSupportMode !== 'DISABLED';
  const btcSymbol = normalizeSymbol(btcSupport.symbol || 'BTCUSDT');
  const btcExchanges = uniqueNormalizedExchanges(
    btcSupport.exchanges ?? params.btc_support_exchanges ?? ['bybit', 'okx'],
  );
  const btcWindowMs = toPositiveInt(btcSupport.window_ms ?? params.btc_support_window_ms) || confirmationWindowMs;
  if (btcSupportEnabled && btcExchanges.length === 0) {
    throw new Error('MICROSTRUCTURE_IMBALANCE_V2_CONFIG_ERROR: btc_support exchanges required when btc support enabled');
  }

  const maxWindowMs = Math.max(deltaMs, hMs, confirmationWindowMs, btcWindowMs, 60_000);
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
    hNs: BigInt(hMs) * 1_000_000n,
    toleranceNs: BigInt(toleranceMs) * 1_000_000n,
    confirmationWindowMs,
    confirmationWindowNs: BigInt(confirmationWindowMs) * 1_000_000n,
    confirmationVenues,
    requiredExternalAlignmentCount,
    minExternalAvailableCount,
    maxVenueDivergenceScore,
    btcSupportMode,
    btcSupportEnabled,
    btcSymbol,
    btcExchanges,
    btcWindowMs,
    btcWindowNs: BigInt(btcWindowMs) * 1_000_000n,
    historyRetentionNs: BigInt(maxWindowMs * 4) * 1_000_000n,
    pressureThreshold,
    exitPressureThreshold,
    minHoldMs: hMs,
    eventCount,
    tStat,
    meanSignedFwdReturnBps,
  };
}

function extractTradeSample(event, expectedSymbol) {
  if (!event || typeof event !== 'object') return null;
  if (String(event.stream || '').trim().toLowerCase() !== 'trade') return null;
  if (normalizeSymbol(event.symbol) !== expectedSymbol) return null;
  const exchange = normalizeExchange(event.exchange);
  const qty = toPositiveNumber(event.qty ?? event.trade_qty ?? event.size);
  const side = parseTradeSide(event.side ?? event.aggressor_side ?? event.trade_side);
  const price = toPositiveNumber(event.price ?? event.close);
  const tsEventNs = toBigIntOrNull(event.ts_event);
  if (!exchange || qty === null || side === null || price === null || tsEventNs === null) return null;
  return {
    exchange,
    tsEventNs,
    signedQty: side * qty,
    qty,
    price,
  };
}

function extractBboSample(event, expectedSymbol) {
  if (!event || typeof event !== 'object') return null;
  if (String(event.stream || '').trim().toLowerCase() !== 'bbo') return null;
  if (normalizeSymbol(event.symbol) !== expectedSymbol) return null;
  const exchange = normalizeExchange(event.exchange);
  const bidPrice = toPositiveNumber(event.bid_price);
  const askPrice = toPositiveNumber(event.ask_price);
  const bidQty = toFiniteNumber(event.bid_qty);
  const askQty = toFiniteNumber(event.ask_qty);
  const tsEventNs = toBigIntOrNull(event.ts_event);
  if (!exchange || bidPrice === null || askPrice === null || bidQty === null || askQty === null || tsEventNs === null) {
    return null;
  }
  if (askPrice < bidPrice) return null;
  const midPrice = (bidPrice + askPrice) / 2;
  if (!(midPrice > 0)) return null;
  return {
    exchange,
    tsEventNs,
    midPrice,
  };
}

export class MicrostructureImbalanceV2Strategy {
  constructor(config = {}) {
    this.config = validateConfig(config);
    this.tradeSamplesByExchange = new Map();
    for (const exchange of [this.config.exchange, ...this.config.confirmationVenues]) {
      this.tradeSamplesByExchange.set(exchange, []);
    }
    this.bboSamplesByExchange = new Map();
    for (const exchange of this.config.btcExchanges) {
      this.bboSamplesByExchange.set(exchange, []);
    }
    this.state = {
      family_id: this.config.familyId,
      binding_mode: this.config.bindingMode,
      symbol: this.config.symbol,
      local_exchange: this.config.exchange,
      processed_events: 0,
      matched_local_trade_events: 0,
      matched_external_trade_events: 0,
      matched_btc_bbo_events: 0,
      signal_event_count: 0,
      order_event_count: 0,
      ignored_event_count: 0,
      pressure_threshold: this.config.pressureThreshold,
      exit_pressure_threshold: this.config.exitPressureThreshold,
      min_hold_ms: this.config.minHoldMs,
      confirmation_window_ms: this.config.confirmationWindowMs,
      confirmation_venues: cloneJson(this.config.confirmationVenues),
      required_external_alignment_count: this.config.requiredExternalAlignmentCount,
      min_external_available_count: this.config.minExternalAvailableCount,
      max_venue_divergence_score: this.config.maxVenueDivergenceScore,
      btc_support_mode: this.config.btcSupportMode,
      last_pressure: null,
      last_signal: null,
      last_confirmation: null,
      last_action: null,
      commit_until_ts_event: null,
      next_trade_sequence_id: 1,
      active_trade_context: null,
    };
  }

  async onInit(ctx) {
    ctx.logger.info(
      `[MicrostructureImbalanceV2Strategy] init symbol=${this.config.symbol} local_exchange=${this.config.exchange} entry_threshold=${this.config.pressureThreshold} exit_threshold=${this.config.exitPressureThreshold} confirmation_venues=${this.config.confirmationVenues.join(',')} confirmation_window_ms=${this.config.confirmationWindowMs} max_divergence=${this.config.maxVenueDivergenceScore} btc_support_mode=${this.config.btcSupportMode}`
    );
  }

  #appendSample(store, sample) {
    store.push(sample);
    const pruneBefore = sample.tsEventNs - this.config.historyRetentionNs;
    while (store.length > 0 && store[0].tsEventNs < pruneBefore) {
      store.shift();
    }
  }

  #computeTradePressure(exchange, tsEventNs, windowNs) {
    const samples = this.tradeSamplesByExchange.get(exchange) || [];
    const windowStart = tsEventNs - windowNs;
    let signedVolume = 0;
    let totalVolume = 0;
    for (let idx = samples.length - 1; idx >= 0; idx -= 1) {
      const item = samples[idx];
      if (item.tsEventNs < windowStart) break;
      signedVolume += item.signedQty;
      totalVolume += item.qty;
    }
    if (totalVolume <= 0) {
      return {
        pressure: null,
        totalVolume: 0,
        signedVolume: 0,
      };
    }
    return {
      pressure: signedVolume / totalVolume,
      totalVolume,
      signedVolume,
    };
  }

  #computeBtcSupportSnapshot(tsEventNs, tradeSideSign) {
    if (!this.config.btcSupportEnabled || tradeSideSign === 0) {
      return {
        enabled: this.config.btcSupportEnabled,
        mode: this.config.btcSupportMode,
        support_flag: null,
        reason: this.config.btcSupportEnabled ? 'NO_TRADE_SIDE' : 'DISABLED',
        average_return_bps: null,
        available_count: 0,
        detail_by_exchange: {},
      };
    }
    const windowStart = tsEventNs - this.config.btcWindowNs;
    const returns = [];
    const detailByExchange = {};
    let availableCount = 0;

    for (const exchange of this.config.btcExchanges) {
      const samples = this.bboSamplesByExchange.get(exchange) || [];
      let firstMid = null;
      let lastMid = null;
      let count = 0;
      for (let idx = samples.length - 1; idx >= 0; idx -= 1) {
        const item = samples[idx];
        if (item.tsEventNs < windowStart) break;
        firstMid = item.midPrice;
        if (lastMid === null) {
          lastMid = item.midPrice;
        }
        count += 1;
      }
      const returnBps = firstMid !== null && lastMid !== null && firstMid > 0
        ? (lastMid / firstMid - 1) * 10_000
        : null;
      detailByExchange[exchange] = {
        return_bps: returnBps,
        observation_count: count,
      };
      if (returnBps !== null) {
        availableCount += 1;
        returns.push(returnBps);
      }
    }

    const averageReturnBps = returns.length > 0
      ? returns.reduce((sum, value) => sum + value, 0) / returns.length
      : null;
    let supportFlag = null;
    if (averageReturnBps !== null) {
      supportFlag = averageReturnBps * tradeSideSign > 0;
    }
    return {
      enabled: true,
      mode: this.config.btcSupportMode,
      support_flag: supportFlag,
      reason: supportFlag === null
        ? 'NO_BTC_CONTEXT'
        : (supportFlag ? 'BTC_SUPPORTIVE' : 'BTC_UNSUPPORTIVE'),
      average_return_bps: averageReturnBps,
      available_count: availableCount,
      detail_by_exchange: detailByExchange,
    };
  }

  #buildConfirmationSnapshot(tsEventNs, signalDirection, localPressure) {
    const tradeSideSign = signalDirection === 'LONG' ? 1 : signalDirection === 'SHORT' ? -1 : 0;
    const venuePressureSnapshot = {
      [this.config.exchange]: localPressure,
    };
    const allPressures = [localPressure];
    let externalAvailableCount = 0;
    let externalAlignmentCount = 0;

    for (const exchange of this.config.confirmationVenues) {
      const metrics = this.#computeTradePressure(exchange, tsEventNs, this.config.confirmationWindowNs);
      venuePressureSnapshot[exchange] = metrics.pressure;
      if (metrics.pressure !== null) {
        externalAvailableCount += 1;
        allPressures.push(metrics.pressure);
        if (tradeSideSign !== 0 && metrics.pressure * tradeSideSign > 0) {
          externalAlignmentCount += 1;
        }
      }
    }

    const venueDivergenceScore = allPressures.length >= 2
      ? Math.max(...allPressures) - Math.min(...allPressures)
      : null;
    const venueDivergencePass = venueDivergenceScore !== null
      && venueDivergenceScore <= this.config.maxVenueDivergenceScore;
    const coveragePass = externalAvailableCount >= this.config.minExternalAvailableCount;
    const alignmentPass = externalAlignmentCount >= this.config.requiredExternalAlignmentCount;
    const marketSupport = this.#computeBtcSupportSnapshot(tsEventNs, tradeSideSign);
    const marketSupportPass = this.config.btcSupportMode !== 'REQUIRE_SUPPORTIVE'
      || marketSupport.support_flag === true;

    let reason = 'NO_LOCAL_SIGNAL';
    if (tradeSideSign !== 0) {
      if (!coveragePass) {
        reason = 'REJECT_NO_EXTERNAL_COVERAGE';
      } else if (!alignmentPass) {
        reason = 'REJECT_ALIGNMENT_WEAK';
      } else if (!venueDivergencePass) {
        reason = 'REJECT_HIGH_DIVERGENCE';
      } else if (!marketSupportPass) {
        reason = 'REJECT_MARKET_UNSUPPORTIVE';
      } else {
        reason = 'CONFIRMED';
      }
    }

    return {
      local_signal_present: tradeSideSign !== 0,
      signal_direction: signalDirection,
      confirmation_pass: reason === 'CONFIRMED',
      reason,
      venue_alignment_count: tradeSideSign === 0 ? 0 : 1 + externalAlignmentCount,
      external_alignment_count: externalAlignmentCount,
      external_available_count: externalAvailableCount,
      venue_divergence_score: venueDivergenceScore,
      venue_divergence_pass: venueDivergencePass,
      venue_pressure_snapshot: venuePressureSnapshot,
      market_support: marketSupport,
      entry_decision_reason: reason === 'CONFIRMED'
        ? `${signalDirection}_CONFIRMED`
        : `${signalDirection || 'FLAT'}_${reason}`,
    };
  }

  async onEvent(event, ctx) {
    this.state.processed_events += 1;

    const tradeSample = extractTradeSample(event, this.config.symbol);
    if (tradeSample && this.tradeSamplesByExchange.has(tradeSample.exchange)) {
      this.#appendSample(this.tradeSamplesByExchange.get(tradeSample.exchange), tradeSample);
      if (tradeSample.exchange === this.config.exchange) {
        this.state.matched_local_trade_events += 1;
      } else {
        this.state.matched_external_trade_events += 1;
      }
    } else {
      const btcBboSample = extractBboSample(event, this.config.btcSymbol);
      if (btcBboSample && this.bboSamplesByExchange.has(btcBboSample.exchange)) {
        this.#appendSample(this.bboSamplesByExchange.get(btcBboSample.exchange), btcBboSample);
        this.state.matched_btc_bbo_events += 1;
      } else {
        this.state.ignored_event_count += 1;
      }
    }

    if (!tradeSample || tradeSample.exchange !== this.config.exchange) {
      return;
    }

    const localMetrics = this.#computeTradePressure(this.config.exchange, tradeSample.tsEventNs, this.config.deltaNs);
    if (localMetrics.pressure === null) {
      return;
    }

    const pressure = localMetrics.pressure;
    const signalDirection = pressureSignal(pressure, this.config.pressureThreshold);
    const confirmation = this.#buildConfirmationSnapshot(tradeSample.tsEventNs, signalDirection, pressure);
    this.state.signal_event_count += 1;
    this.state.last_pressure = pressure;
    this.state.last_confirmation = cloneJson(confirmation);
    this.state.last_signal = {
      ts_event: tradeSample.tsEventNs.toString(),
      signal_direction: signalDirection,
      pressure,
      signed_volume: localMetrics.signedVolume,
      total_volume: localMetrics.totalVolume,
      pressure_threshold: this.config.pressureThreshold,
      exit_pressure_threshold: this.config.exitPressureThreshold,
      delta_ms: this.config.selectedCell.delta_ms,
      h_ms: this.config.selectedCell.h_ms,
      confirmation_window_ms: this.config.confirmationWindowMs,
      confirmation_venues: cloneJson(this.config.confirmationVenues),
      required_external_alignment_count: this.config.requiredExternalAlignmentCount,
      max_venue_divergence_score: this.config.maxVenueDivergenceScore,
      btc_support_mode: this.config.btcSupportMode,
      confirmation_snapshot: cloneJson(confirmation),
      mean_signed_fwd_return_bps: this.config.meanSignedFwdReturnBps,
      t_stat: this.config.tStat,
      event_count: this.config.eventCount,
    };

    const currentSize = getPositionSize(ctx, this.config.symbol);
    const currentPositionSide = positionSideFromSize(currentSize);
    if (currentSize !== 0 && this.state.active_trade_context) {
      this.state.active_trade_context = updateActiveTradeContext(this.state.active_trade_context, pressure);
    }

    if (currentSize === 0 && this.state.commit_until_ts_event !== null) {
      this.state.commit_until_ts_event = null;
    }
    const commitUntilTsNs = currentSize === 0 ? null : toBigIntOrNull(this.state.commit_until_ts_event);
    const commitActive = currentSize !== 0
      && commitUntilTsNs !== null
      && tradeSample.tsEventNs < commitUntilTsNs;

    let action = currentSize === 0 ? 'STAY_FLAT' : (currentSize > 0 ? 'HOLD_LONG' : 'HOLD_SHORT');
    let orderIntent = null;
    let nextActiveTradeContext = null;
    let shouldAdvanceTradeSequence = false;

    if (commitActive) {
      action = currentSize > 0 ? 'HOLD_LONG' : 'HOLD_SHORT';
    } else if (currentSize === 0) {
      if (signalDirection === 'LONG') {
        if (confirmation.confirmation_pass) {
          action = 'LONG_OPEN';
          nextActiveTradeContext = buildEntryTradeContext({
            config: this.config,
            tradeSequenceId: nextTradeSequenceId(this.state),
            tsEventNs: tradeSample.tsEventNs,
            pressure,
            entrySide: 'LONG',
            entrySignalReason: 'LONG_ENTRY',
            priorPositionSide: currentPositionSide,
            wasReversalTrade: false,
            confirmationSnapshot: confirmation,
          });
          shouldAdvanceTradeSequence = true;
          orderIntent = buildOrderIntent(this.config.symbol, 'BUY', this.config.orderQty, {
            action: 'LONG',
            position_effect: 'OPEN',
            signal_action: action,
            ts_event: tradeSample.tsEventNs,
            trade_context: buildTradeContextPayload({ openingTradeContext: nextActiveTradeContext }),
          });
        } else {
          action = `SKIP_LONG_${confirmation.reason}`;
        }
      } else if (signalDirection === 'SHORT') {
        if (confirmation.confirmation_pass) {
          action = 'SHORT_OPEN';
          nextActiveTradeContext = buildEntryTradeContext({
            config: this.config,
            tradeSequenceId: nextTradeSequenceId(this.state),
            tsEventNs: tradeSample.tsEventNs,
            pressure,
            entrySide: 'SHORT',
            entrySignalReason: 'SHORT_ENTRY',
            priorPositionSide: currentPositionSide,
            wasReversalTrade: false,
            confirmationSnapshot: confirmation,
          });
          shouldAdvanceTradeSequence = true;
          orderIntent = buildOrderIntent(this.config.symbol, 'SELL', this.config.orderQty, {
            action: 'SHORT',
            position_effect: 'OPEN',
            signal_action: action,
            ts_event: tradeSample.tsEventNs,
            trade_context: buildTradeContextPayload({ openingTradeContext: nextActiveTradeContext }),
          });
        } else {
          action = `SKIP_SHORT_${confirmation.reason}`;
        }
      }
    } else if (currentSize > 0) {
      if (signalDirection === 'SHORT') {
        if (confirmation.confirmation_pass) {
          action = 'LONG_TO_SHORT_REVERSAL';
          const closingTradeContext = buildExitTradeContext(this.state.active_trade_context, {
            tsEventNs: tradeSample.tsEventNs,
            pressure,
            exitReason: 'REVERSAL_EXIT',
            priorPositionSide: currentPositionSide,
          });
          nextActiveTradeContext = buildEntryTradeContext({
            config: this.config,
            tradeSequenceId: nextTradeSequenceId(this.state),
            tsEventNs: tradeSample.tsEventNs,
            pressure,
            entrySide: 'SHORT',
            entrySignalReason: 'REVERSAL_ENTRY',
            priorPositionSide: currentPositionSide,
            wasReversalTrade: true,
            confirmationSnapshot: confirmation,
          });
          shouldAdvanceTradeSequence = true;
          orderIntent = buildOrderIntent(this.config.symbol, 'SELL', Math.abs(currentSize) + this.config.orderQty, {
            action: 'EXIT_LONG',
            position_effect: 'REVERSE',
            signal_action: action,
            ts_event: tradeSample.tsEventNs,
            trade_context: buildTradeContextPayload({
              openingTradeContext: nextActiveTradeContext,
              closingTradeContext,
            }),
          });
        } else {
          action = `HOLD_LONG_UNCONFIRMED_${confirmation.reason}`;
        }
      } else if (Math.abs(pressure) < this.config.exitPressureThreshold) {
        action = 'LONG_CLOSE';
        orderIntent = buildOrderIntent(this.config.symbol, 'SELL', Math.abs(currentSize), {
          action: 'EXIT_LONG',
          position_effect: 'CLOSE',
          signal_action: action,
          ts_event: tradeSample.tsEventNs,
          trade_context: buildTradeContextPayload({
            closingTradeContext: buildExitTradeContext(this.state.active_trade_context, {
              tsEventNs: tradeSample.tsEventNs,
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
        if (confirmation.confirmation_pass) {
          action = 'SHORT_TO_LONG_REVERSAL';
          const closingTradeContext = buildExitTradeContext(this.state.active_trade_context, {
            tsEventNs: tradeSample.tsEventNs,
            pressure,
            exitReason: 'REVERSAL_EXIT',
            priorPositionSide: currentPositionSide,
          });
          nextActiveTradeContext = buildEntryTradeContext({
            config: this.config,
            tradeSequenceId: nextTradeSequenceId(this.state),
            tsEventNs: tradeSample.tsEventNs,
            pressure,
            entrySide: 'LONG',
            entrySignalReason: 'REVERSAL_ENTRY',
            priorPositionSide: currentPositionSide,
            wasReversalTrade: true,
            confirmationSnapshot: confirmation,
          });
          shouldAdvanceTradeSequence = true;
          orderIntent = buildOrderIntent(this.config.symbol, 'BUY', Math.abs(currentSize) + this.config.orderQty, {
            action: 'EXIT_SHORT',
            position_effect: 'REVERSE',
            signal_action: action,
            ts_event: tradeSample.tsEventNs,
            trade_context: buildTradeContextPayload({
              openingTradeContext: nextActiveTradeContext,
              closingTradeContext,
            }),
          });
        } else {
          action = `HOLD_SHORT_UNCONFIRMED_${confirmation.reason}`;
        }
      } else if (Math.abs(pressure) < this.config.exitPressureThreshold) {
        action = 'SHORT_CLOSE';
        orderIntent = buildOrderIntent(this.config.symbol, 'BUY', Math.abs(currentSize), {
          action: 'EXIT_SHORT',
          position_effect: 'CLOSE',
          signal_action: action,
          ts_event: tradeSample.tsEventNs,
          trade_context: buildTradeContextPayload({
            closingTradeContext: buildExitTradeContext(this.state.active_trade_context, {
              tsEventNs: tradeSample.tsEventNs,
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
        ts_event: tradeSample.tsEventNs.toString(),
        action,
        signal_direction: signalDirection,
        pressure,
        current_size: currentSize,
        commit_until_ts_event: this.state.commit_until_ts_event,
        commit_active: commitActive,
        confirmation_snapshot: cloneJson(confirmation),
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
      this.state.commit_until_ts_event = (tradeSample.tsEventNs + this.config.hNs).toString();
      this.state.active_trade_context = nextActiveTradeContext ? cloneJson(nextActiveTradeContext) : null;
      if (shouldAdvanceTradeSequence) {
        this.state.next_trade_sequence_id = nextTradeSequenceId(this.state) + 1;
      }
    } else if (action === 'LONG_CLOSE' || action === 'SHORT_CLOSE') {
      this.state.commit_until_ts_event = null;
      this.state.active_trade_context = null;
    }
    this.state.last_action = {
      ts_event: tradeSample.tsEventNs.toString(),
      action,
      signal_direction: signalDirection,
      pressure,
      current_size: currentSize,
      order_side: orderIntent.side,
      order_qty: orderIntent.qty,
      commit_until_ts_event: this.state.commit_until_ts_event,
      commit_active: commitActive,
      confirmation_snapshot: cloneJson(confirmation),
      trade_context: orderIntent.trade_context ? cloneJson(orderIntent.trade_context) : null,
    };

    if (this.state.order_event_count === 1) {
      ctx.logger.info(
        `[MicrostructureImbalanceV2Strategy] first_order action=${action} side=${orderIntent.side} qty=${orderIntent.qty} symbol=${this.config.symbol}`
      );
    }
  }

  async onFinalize(ctx) {
    ctx.logger.info(`total_processed: ${ctx.stats.processed}`);
    ctx.logger.info(
      `[MicrostructureImbalanceV2Strategy] finalize matched_local_trade_events=${this.state.matched_local_trade_events} matched_external_trade_events=${this.state.matched_external_trade_events} matched_btc_bbo_events=${this.state.matched_btc_bbo_events} signal_event_count=${this.state.signal_event_count} order_event_count=${this.state.order_event_count}`
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

export default MicrostructureImbalanceV2Strategy;
