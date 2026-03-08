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

function cloneJson(value) {
  return JSON.parse(JSON.stringify(value));
}

function validateConfig(rawConfig) {
  const config = rawConfig && typeof rawConfig === 'object' ? rawConfig : {};
  const familyId = String(config.family_id || '').trim();
  if (familyId !== 'spread_reversion_v1') {
    throw new Error('SPREAD_REVERSION_V1_CONFIG_ERROR: family_id must be spread_reversion_v1');
  }

  const stream = String(config.stream || '').trim().toLowerCase();
  if (stream !== 'bbo') {
    throw new Error('SPREAD_REVERSION_V1_CONFIG_ERROR: stream must be bbo');
  }
  const exchange = String(config.exchange || '').trim().toLowerCase();
  if (!exchange) {
    throw new Error('SPREAD_REVERSION_V1_CONFIG_ERROR: exchange required');
  }

  const symbols = Array.isArray(config.symbols)
    ? config.symbols.map((value) => normalizeSymbol(value)).filter(Boolean)
    : [];
  if (symbols.length !== 1) {
    throw new Error('SPREAD_REVERSION_V1_CONFIG_ERROR: exactly one symbol required');
  }

  const params = config.params && typeof config.params === 'object' ? config.params : null;
  const selectedCell = config.selected_cell && typeof config.selected_cell === 'object' ? config.selected_cell : null;
  if (!params) {
    throw new Error('SPREAD_REVERSION_V1_CONFIG_ERROR: params object required');
  }
  if (!selectedCell) {
    throw new Error('SPREAD_REVERSION_V1_CONFIG_ERROR: selected_cell object required');
  }

  const deltaMs = toPositiveInt(selectedCell.delta_ms);
  const hMs = toPositiveInt(selectedCell.h_ms);
  if (!deltaMs || !hMs) {
    throw new Error('SPREAD_REVERSION_V1_CONFIG_ERROR: selected_cell delta_ms and h_ms must be positive integers');
  }

  const selectedSymbol = normalizeSymbol(selectedCell.symbol);
  if (selectedSymbol !== symbols[0]) {
    throw new Error('SPREAD_REVERSION_V1_CONFIG_ERROR: selected_cell symbol mismatch');
  }
  if (String(selectedCell.stream || '').trim().toLowerCase() !== stream) {
    throw new Error('SPREAD_REVERSION_V1_CONFIG_ERROR: selected_cell stream mismatch');
  }
  if (String(selectedCell.exchange || '').trim().toLowerCase() !== exchange) {
    throw new Error('SPREAD_REVERSION_V1_CONFIG_ERROR: selected_cell exchange mismatch');
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
    exchange,
    stream,
    symbol: symbols[0],
    symbols,
    window: String(config.window || '').trim() || null,
    params: cloneJson(params),
    selectedCell: cloneJson(selectedCell),
    deltaNs: BigInt(deltaMs) * 1_000_000n,
    toleranceNs: BigInt(toleranceMs) * 1_000_000n,
    historyRetentionNs: BigInt(Math.max(deltaMs * 4, maxDeltaMs * 4, 60_000)) * 1_000_000n,
    meanProduct: toFiniteNumber(selectedCell.mean_product),
    tStat: toFiniteNumber(selectedCell.t_stat),
  };
}

function extractSpreadSample(event, expectedSymbol) {
  if (!event || typeof event !== 'object') return null;
  if (normalizeSymbol(event.symbol) !== expectedSymbol) return null;
  const bid = toFiniteNumber(event.bid_price);
  const ask = toFiniteNumber(event.ask_price);
  if (bid === null || ask === null || bid <= 0 || ask <= 0 || ask < bid) return null;
  const tsEventRaw = event.ts_event;
  if (tsEventRaw === undefined || tsEventRaw === null) return null;
  let tsEventNs;
  try {
    tsEventNs = BigInt(tsEventRaw);
  } catch {
    return null;
  }
  const midPrice = (bid + ask) / 2;
  if (!Number.isFinite(midPrice) || midPrice <= 0) return null;
  return {
    tsEventNs,
    spreadBps: 10000 * (ask - bid) / midPrice,
    midPrice,
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

export class SpreadReversionV1Strategy {
  constructor(config = {}) {
    this.config = validateConfig(config);
    this.samples = [];
    this.state = {
      family_id: this.config.familyId,
      symbol: this.config.symbol,
      processed_events: 0,
      matched_bbo_events: 0,
      signal_event_count: 0,
      ignored_event_count: 0,
      last_mid_price: null,
      last_spread_bps: null,
      last_signal: null,
    };
  }

  async onInit(ctx) {
    ctx.logger.info(
      `[SpreadReversionV1Strategy] init symbol=${this.config.symbol} delta_ms=${this.config.selectedCell.delta_ms} h_ms=${this.config.selectedCell.h_ms}`
    );
  }

  async onEvent(event, ctx) {
    this.state.processed_events += 1;

    const sample = extractSpreadSample(event, this.config.symbol);
    if (!sample) {
      this.state.ignored_event_count += 1;
      return;
    }

    this.state.matched_bbo_events += 1;
    this.state.last_mid_price = sample.midPrice;
    this.state.last_spread_bps = sample.spreadBps;

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

    if (!anchor) {
      return;
    }

    const pastChangeBps = sample.spreadBps - anchor.spreadBps;
    let signalType = 'SPREAD_FLAT_NO_SIGNAL';
    if (pastChangeBps > 0) {
      signalType = 'SPREAD_WIDENED_EXPECT_REVERSION';
    } else if (pastChangeBps < 0) {
      signalType = 'SPREAD_NARROWED_EXPECT_REBOUND';
    }

    this.state.signal_event_count += 1;
    this.state.last_signal = {
      ts_event: sample.tsEventNs.toString(),
      signal_type: signalType,
      past_change_bps: pastChangeBps,
      spread_bps: sample.spreadBps,
      mid_price: sample.midPrice,
      delta_ms: this.config.selectedCell.delta_ms,
      h_ms: this.config.selectedCell.h_ms,
      mean_product: this.config.meanProduct,
      t_stat: this.config.tStat,
    };

    if (this.state.signal_event_count === 1) {
      ctx.logger.info(
        `[SpreadReversionV1Strategy] first_signal type=${signalType} symbol=${this.config.symbol}`
      );
    }
  }

  async onFinalize(ctx) {
    ctx.logger.info(`total_processed: ${ctx.stats.processed}`);
    ctx.logger.info(
      `[SpreadReversionV1Strategy] finalize matched_bbo_events=${this.state.matched_bbo_events} signal_event_count=${this.state.signal_event_count}`
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

export default SpreadReversionV1Strategy;
