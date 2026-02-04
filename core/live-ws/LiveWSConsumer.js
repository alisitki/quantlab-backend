/**
 * Live WS Consumer (collector-parity)
 */

import { createRequire } from 'node:module';

const require = createRequire(new URL('../package.json', import.meta.url));
let wsLib = null;

function resolveWebSocketFactory() {
  if (globalThis.WebSocket) return globalThis.WebSocket;
  try {
    if (!wsLib) wsLib = require('ws');
    return wsLib.WebSocket || wsLib;
  } catch {
    return null;
  }
}

const DEFAULT_SYMBOLS = [
  'BTCUSDT',
  'ETHUSDT',
  'BNBUSDT',
  'SOLUSDT',
  'XRPUSDT',
  'LINKUSDT',
  'ADAUSDT',
  'AVAXUSDT',
  'LTCUSDT',
  'MATICUSDT'
];

const BINANCE_WS_URL = 'wss://fstream.binance.com/stream?streams=';
const BYBIT_WS_URL = 'wss://stream.bybit.com/v5/public/linear';
const OKX_WS_URL = 'wss://ws.okx.com:8443/ws/v5/public';

const RECONNECT_DELAY = 2_000;
const MAX_RECONNECT_DELAY = 60_000;
const RECONNECT_MAX_PER_WINDOW = 5;
const RECONNECT_WINDOW_MS = 60_000;
const RECONNECT_PAUSE_MS = 120_000;

const STREAM_VERSION = 1;

function envEnabled(val) {
  return val === '1' || val === 'true' || val === 'yes';
}

function nowMs() {
  return Date.now();
}

function normalizeSymbol(symbol) {
  return symbol.toUpperCase();
}

function toOkxSymbol(symbol) {
  let base = symbol.replace('USDT', '');
  if (base === 'MATIC') base = 'POL';
  return `${base}-USDT-SWAP`;
}

function fromOkxSymbol(instId) {
  const parts = instId.split('-');
  if (parts.length >= 2) {
    let base = parts[0];
    if (base === 'POL') base = 'MATIC';
    return `${base}${parts[1]}`;
  }
  return instId;
}

function safeJsonParse(raw) {
  try {
    return JSON.parse(raw);
  } catch {
    return null;
  }
}

function buildBinanceStreams(symbols) {
  const streams = [];
  for (const symbol of symbols) {
    const s = symbol.toLowerCase();
    streams.push(`${s}@bookTicker`);
    streams.push(`${s}@aggTrade`);
    streams.push(`${s}@markPrice@1s`);
  }
  return streams;
}

function makeBboEvent({ tsEvent, tsRecv, exchange, symbol, bidPrice, bidQty, askPrice, askQty }) {
  return {
    ts_event: tsEvent,
    ts_recv: tsRecv,
    exchange,
    symbol,
    stream: 'bbo',
    stream_version: STREAM_VERSION,
    bid_price: bidPrice,
    bid_qty: bidQty,
    ask_price: askPrice,
    ask_qty: askQty
  };
}

function makeTradeEvent({ tsEvent, tsRecv, exchange, symbol, price, qty, side, tradeId }) {
  return {
    ts_event: tsEvent,
    ts_recv: tsRecv,
    exchange,
    symbol,
    stream: 'trade',
    stream_version: STREAM_VERSION,
    price,
    qty,
    side,
    trade_id: tradeId
  };
}

function makeMarkPriceEvent({ tsEvent, tsRecv, exchange, symbol, markPrice, indexPrice }) {
  return {
    ts_event: tsEvent,
    ts_recv: tsRecv,
    exchange,
    symbol,
    stream: 'mark_price',
    stream_version: STREAM_VERSION,
    mark_price: markPrice,
    index_price: indexPrice
  };
}

function makeFundingEvent({ tsEvent, tsRecv, exchange, symbol, fundingRate, nextFundingTs }) {
  return {
    ts_event: tsEvent,
    ts_recv: tsRecv,
    exchange,
    symbol,
    stream: 'funding',
    stream_version: STREAM_VERSION,
    funding_rate: fundingRate,
    next_funding_ts: nextFundingTs
  };
}

function makeOpenInterestEvent({ tsEvent, tsRecv, exchange, symbol, openInterest }) {
  return {
    ts_event: tsEvent,
    ts_recv: tsRecv,
    exchange,
    symbol,
    stream: 'open_interest',
    stream_version: STREAM_VERSION,
    open_interest: openInterest
  };
}

export class LiveWSConsumer {
  #exchange;
  #symbols;
  #ws;
  #running = false;
  #queue = [];
  #waiters = [];
  #reconnectDelay = RECONNECT_DELAY;
  #reconnectTimestamps = [];
  #circuitBreakerUntil = 0;
  #lastEventKeyByStream = new Map();
  #wsFactory;
  #bybitPingTimer = null;

  constructor({ exchange, symbols = DEFAULT_SYMBOLS, wsFactory = null }) {
    this.#exchange = exchange;
    this.#symbols = symbols;
    this.#wsFactory = wsFactory;
  }

  async start() {
    if (!envEnabled(process.env.CORE_LIVE_WS_ENABLED || '0')) {
      throw new Error('CORE_LIVE_WS_DISABLED');
    }
    this.#running = true;
    while (this.#running) {
      try {
        await this.#connectOnce();
      } catch (err) {
        this.#logWarn('ws_error', { error: err.message });
        await this.#handleReconnect();
      }
    }
  }

  stop() {
    this.#running = false;
    if (this.#bybitPingTimer) {
      clearInterval(this.#bybitPingTimer);
      this.#bybitPingTimer = null;
    }
    if (this.#ws) {
      try { this.#ws.close(); } catch {}
    }
    while (this.#waiters.length > 0) {
      const resolve = this.#waiters.shift();
      resolve({ value: undefined, done: true });
    }
    this.#queue = [];
  }

  events() {
    const self = this;
    return {
      async next() {
        if (self.#queue.length > 0) {
          return { value: self.#queue.shift(), done: false };
        }
        if (!self.#running) {
          return { value: undefined, done: true };
        }
        return new Promise((resolve) => {
          self.#waiters.push(resolve);
        });
      },
      [Symbol.asyncIterator]() { return this; }
    };
  }

  #emit(event) {
    if (!event) return;
    const key = `${event.exchange}|${event.stream}|${event.symbol}|${event.ts_event}|${event.trade_id ?? ''}|${event.mark_price ?? ''}`;
    const lastKey = this.#lastEventKeyByStream.get(`${event.exchange}|${event.stream}|${event.symbol}`);
    // Duplicate events can happen on reconnect; keep silent to avoid log spam.
    this.#lastEventKeyByStream.set(`${event.exchange}|${event.stream}|${event.symbol}`, key);

    if (this.#waiters.length > 0) {
      const resolve = this.#waiters.shift();
      resolve({ value: event, done: false });
    } else {
      this.#queue.push(event);
    }
  }

  #logWarn(event, extra = {}) {
    try {
      console.warn(JSON.stringify({ event, ...extra }));
    } catch {}
  }

  async #connectOnce() {
    const exchange = this.#exchange;
    if (exchange === 'binance') return this.#connectBinance();
    if (exchange === 'bybit') return this.#connectBybit();
    if (exchange === 'okx') return this.#connectOkx();
    throw new Error(`UNKNOWN_EXCHANGE: ${exchange}`);
  }

  async #connectBinance() {
    const streams = buildBinanceStreams(this.#symbols);
    const url = BINANCE_WS_URL + streams.join('/');
    const ws = this.#openWs(url);
    this.#ws = ws;
    this.#reconnectDelay = RECONNECT_DELAY;

    ws.onmessage = (msg) => {
      if (!this.#running) return;
      const payload = safeJsonParse(msg.data);
      if (!payload || !payload.stream || !payload.data) return;
      const stream = payload.stream;
      const data = payload.data;
      const tsRecv = nowMs();
      const symbol = normalizeSymbol(stream.split('@')[0]);

      if (stream.toLowerCase().includes('bookticker')) {
        const event = parseBinanceBbo(data, symbol, tsRecv);
        this.#emit(event);
      } else if (stream.toLowerCase().includes('aggtrade')) {
        const event = parseBinanceTrade(data, symbol, tsRecv);
        this.#emit(event);
      } else if (stream.toLowerCase().includes('markprice')) {
        const events = parseBinanceMarkPrice(data, symbol, tsRecv);
        for (const ev of events) this.#emit(ev);
      }
    };

    await this.#awaitClose(ws);
  }

  async #connectBybit() {
    const ws = this.#openWs(BYBIT_WS_URL);
    this.#ws = ws;
    this.#reconnectDelay = RECONNECT_DELAY;

    ws.onopen = () => {
      const args = [];
      for (const symbol of this.#symbols) {
        args.push(`tickers.${symbol}`);
        args.push(`publicTrade.${symbol}`);
      }
      ws.send(JSON.stringify({ op: 'subscribe', args }));
      this.#startBybitPing(ws);
    };

    ws.onmessage = (msg) => {
      if (!this.#running) return;
      const payload = safeJsonParse(msg.data);
      if (!payload || !payload.topic || payload.data === undefined) return;
      const tsRecv = nowMs();
      const topic = payload.topic;
      if (topic.startsWith('tickers.')) {
        const symbol = normalizeSymbol(topic.split('.')[1]);
        const events = parseBybitTickers(payload.data, symbol, tsRecv);
        for (const ev of events) this.#emit(ev);
      } else if (topic.startsWith('publicTrade.')) {
        const symbol = normalizeSymbol(topic.split('.')[1]);
        const events = parseBybitTrades(payload.data, symbol, tsRecv);
        for (const ev of events) this.#emit(ev);
      }
    };

    await this.#awaitClose(ws);
    if (this.#bybitPingTimer) {
      clearInterval(this.#bybitPingTimer);
      this.#bybitPingTimer = null;
    }
  }

  async #connectOkx() {
    const ws = this.#openWs(OKX_WS_URL);
    this.#ws = ws;
    this.#reconnectDelay = RECONNECT_DELAY;

    ws.onopen = () => {
      const args = [];
      for (const symbol of this.#symbols) {
        const okxSymbol = toOkxSymbol(symbol);
        args.push({ channel: 'tickers', instId: okxSymbol });
        args.push({ channel: 'trades', instId: okxSymbol });
        args.push({ channel: 'open-interest', instId: okxSymbol });
        args.push({ channel: 'funding-rate', instId: okxSymbol });
        args.push({ channel: 'mark-price', instId: okxSymbol });
      }
      ws.send(JSON.stringify({ op: 'subscribe', args }));
    };

    ws.onmessage = (msg) => {
      if (!this.#running) return;
      const payload = safeJsonParse(msg.data);
      if (!payload || payload.event) return;
      if (!payload.arg || !payload.data) return;
      const channel = payload.arg.channel;
      const instId = payload.arg.instId;
      const symbol = fromOkxSymbol(instId);
      const tsRecv = nowMs();
      const dataList = payload.data;
      if (!Array.isArray(dataList)) return;
      for (const data of dataList) {
        const event = parseOkxEvent(channel, data, symbol, tsRecv);
        if (event) this.#emit(event);
      }
    };

    await this.#awaitClose(ws);
  }

  #openWs(url) {
    const factory = this.#wsFactory || resolveWebSocketFactory();
    if (!factory) throw new Error('WEBSOCKET_UNAVAILABLE');
    return new factory(url);
  }

  #startBybitPing(ws) {
    if (this.#bybitPingTimer) clearInterval(this.#bybitPingTimer);
    this.#bybitPingTimer = setInterval(() => {
      if (!this.#running) return;
      try {
        ws.send(JSON.stringify({ op: 'ping' }));
      } catch {
        // ignore ping failures
      }
    }, 20000);
  }

  async #awaitClose(ws) {
    await new Promise((resolve) => {
      ws.onclose = () => resolve();
      ws.onerror = () => resolve();
    });
  }

  async #handleReconnect() {
    const now = nowMs();
    if (now < this.#circuitBreakerUntil) {
      const remaining = this.#circuitBreakerUntil - now;
      await this.#sleep(remaining);
      return;
    }

    this.#reconnectTimestamps.push(now);
    const cutoff = now - RECONNECT_WINDOW_MS;
    while (this.#reconnectTimestamps.length > 0 && this.#reconnectTimestamps[0] < cutoff) {
      this.#reconnectTimestamps.shift();
    }

    if (this.#reconnectTimestamps.length >= RECONNECT_MAX_PER_WINDOW) {
      this.#circuitBreakerUntil = now + RECONNECT_PAUSE_MS;
      this.#reconnectTimestamps = [];
      await this.#sleep(RECONNECT_PAUSE_MS);
      this.#reconnectDelay = RECONNECT_DELAY;
      return;
    }

    const jittered = this.#reconnectDelay * (0.5 + Math.random());
    await this.#sleep(jittered);
    this.#reconnectDelay = Math.min(this.#reconnectDelay * 2, MAX_RECONNECT_DELAY);
  }

  async #sleep(ms) {
    return new Promise((resolve) => setTimeout(resolve, ms));
  }
}

export function parseBinanceBbo(data, symbol, tsRecv) {
  return makeBboEvent({
    tsEvent: data.T ?? tsRecv,
    tsRecv,
    exchange: 'binance',
    symbol,
    bidPrice: Number(data.b),
    bidQty: Number(data.B),
    askPrice: Number(data.a),
    askQty: Number(data.A)
  });
}

export function parseBinanceTrade(data, symbol, tsRecv) {
  const side = data.m ? -1 : 1;
  return makeTradeEvent({
    tsEvent: data.T ?? tsRecv,
    tsRecv,
    exchange: 'binance',
    symbol,
    price: Number(data.p),
    qty: Number(data.q),
    side,
    tradeId: String(data.a)
  });
}

export function parseBinanceMarkPrice(data, symbol, tsRecv) {
  const tsEvent = data.E ?? tsRecv;
  const events = [];
  events.push(makeMarkPriceEvent({
    tsEvent,
    tsRecv,
    exchange: 'binance',
    symbol,
    markPrice: Number(data.p),
    indexPrice: data.i ? Number(data.i) : null
  }));
  if (data.r) {
    events.push(makeFundingEvent({
      tsEvent,
      tsRecv,
      exchange: 'binance',
      symbol,
      fundingRate: Number(data.r),
      nextFundingTs: Number(data.T ?? 0)
    }));
  }
  return events;
}

export function parseBybitTickers(data, symbol, tsRecv) {
  const tsEvent = Number(data.ts ?? tsRecv);
  const events = [];

  if (data.bid1Price && data.ask1Price) {
    events.push(makeBboEvent({
      tsEvent,
      tsRecv,
      exchange: 'bybit',
      symbol,
      bidPrice: Number(data.bid1Price),
      bidQty: Number(data.bid1Size ?? 0),
      askPrice: Number(data.ask1Price),
      askQty: Number(data.ask1Size ?? 0)
    }));
  }

  if (data.markPrice) {
    events.push(makeMarkPriceEvent({
      tsEvent,
      tsRecv,
      exchange: 'bybit',
      symbol,
      markPrice: Number(data.markPrice),
      indexPrice: data.indexPrice ? Number(data.indexPrice) : null
    }));
  }

  if (data.fundingRate && data.nextFundingTime) {
    events.push(makeFundingEvent({
      tsEvent,
      tsRecv,
      exchange: 'bybit',
      symbol,
      fundingRate: Number(data.fundingRate),
      nextFundingTs: Number(data.nextFundingTime)
    }));
  }

  if (data.openInterest) {
    events.push(makeOpenInterestEvent({
      tsEvent,
      tsRecv,
      exchange: 'bybit',
      symbol,
      openInterest: Number(data.openInterest)
    }));
  }

  return events;
}

export function parseBybitTrades(data, symbol, tsRecv) {
  const events = [];
  if (!Array.isArray(data)) return events;
  for (const trade of data) {
    const side = trade.S === 'Buy' ? 1 : -1;
    events.push(makeTradeEvent({
      tsEvent: Number(trade.T ?? tsRecv),
      tsRecv,
      exchange: 'bybit',
      symbol,
      price: Number(trade.p),
      qty: Number(trade.v),
      side,
      tradeId: String(trade.i ?? '')
    }));
  }
  return events;
}

export function parseOkxEvent(channel, data, symbol, tsRecv) {
  if (channel === 'tickers') {
    return makeBboEvent({
      tsEvent: Number(data.ts ?? tsRecv),
      tsRecv,
      exchange: 'okx',
      symbol,
      bidPrice: Number(data.bidPx ?? 0),
      bidQty: Number(data.bidSz ?? 0),
      askPrice: Number(data.askPx ?? 0),
      askQty: Number(data.askSz ?? 0)
    });
  }
  if (channel === 'trades') {
    const side = data.side === 'buy' ? 1 : -1;
    return makeTradeEvent({
      tsEvent: Number(data.ts ?? tsRecv),
      tsRecv,
      exchange: 'okx',
      symbol,
      price: Number(data.px),
      qty: Number(data.sz),
      side,
      tradeId: String(data.tradeId ?? '')
    });
  }
  if (channel === 'open-interest') {
    return makeOpenInterestEvent({
      tsEvent: Number(data.ts ?? tsRecv),
      tsRecv,
      exchange: 'okx',
      symbol,
      openInterest: Number(data.oi ?? 0)
    });
  }
  if (channel === 'funding-rate') {
    return makeFundingEvent({
      tsEvent: Number(data.ts ?? tsRecv),
      tsRecv,
      exchange: 'okx',
      symbol,
      fundingRate: Number(data.fundingRate ?? 0),
      nextFundingTs: Number(data.nextFundingTime ?? 0)
    });
  }
  if (channel === 'mark-price') {
    return makeMarkPriceEvent({
      tsEvent: Number(data.ts ?? tsRecv),
      tsRecv,
      exchange: 'okx',
      symbol,
      markPrice: Number(data.markPx ?? 0),
      indexPrice: null
    });
  }
  return null;
}

export default LiveWSConsumer;
