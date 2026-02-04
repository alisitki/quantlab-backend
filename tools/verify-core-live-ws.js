#!/usr/bin/env node
/**
 * Verify core LiveWSConsumer normalization parity with collector
 */

import assert from 'node:assert';
import {
  parseBinanceBbo,
  parseBinanceTrade,
  parseBinanceMarkPrice,
  parseBybitTickers,
  parseBybitTrades,
  parseOkxEvent
} from '../core/live-ws/LiveWSConsumer.js';

function equal(a, b, label) {
  try {
    assert.deepStrictEqual(a, b);
  } catch (err) {
    console.error('FAIL:', label);
    console.error('Expected:', JSON.stringify(b));
    console.error('Actual:', JSON.stringify(a));
    process.exit(1);
  }
}

function main() {
  const tsRecv = 1700000000000;

  // Binance
  const binanceBboMsg = {
    T: 1700000000123,
    b: '30000.1',
    B: '0.5',
    a: '30001.2',
    A: '0.4'
  };
  const binanceBboExpected = {
    ts_event: 1700000000123,
    ts_recv: tsRecv,
    exchange: 'binance',
    symbol: 'BTCUSDT',
    stream: 'bbo',
    stream_version: 1,
    bid_price: 30000.1,
    bid_qty: 0.5,
    ask_price: 30001.2,
    ask_qty: 0.4
  };
  equal(parseBinanceBbo(binanceBboMsg, 'BTCUSDT', tsRecv), binanceBboExpected, 'binance_bbo');

  const binanceTradeMsg = { T: 1700000000456, p: '30000.5', q: '0.01', m: true, a: 1234 };
  const binanceTradeExpected = {
    ts_event: 1700000000456,
    ts_recv: tsRecv,
    exchange: 'binance',
    symbol: 'BTCUSDT',
    stream: 'trade',
    stream_version: 1,
    price: 30000.5,
    qty: 0.01,
    side: -1,
    trade_id: '1234'
  };
  equal(parseBinanceTrade(binanceTradeMsg, 'BTCUSDT', tsRecv), binanceTradeExpected, 'binance_trade');

  const binanceMarkMsg = { E: 1700000000789, p: '30002.0', i: '29990.0', r: '0.0001', T: 1700003600000 };
  const binanceMarkEvents = parseBinanceMarkPrice(binanceMarkMsg, 'BTCUSDT', tsRecv);
  const binanceMarkExpected = {
    ts_event: 1700000000789,
    ts_recv: tsRecv,
    exchange: 'binance',
    symbol: 'BTCUSDT',
    stream: 'mark_price',
    stream_version: 1,
    mark_price: 30002,
    index_price: 29990
  };
  const binanceFundingExpected = {
    ts_event: 1700000000789,
    ts_recv: tsRecv,
    exchange: 'binance',
    symbol: 'BTCUSDT',
    stream: 'funding',
    stream_version: 1,
    funding_rate: 0.0001,
    next_funding_ts: 1700003600000
  };
  equal(binanceMarkEvents[0], binanceMarkExpected, 'binance_mark_price');
  equal(binanceMarkEvents[1], binanceFundingExpected, 'binance_funding');

  // Bybit
  const bybitTickerMsg = {
    ts: '1700000000123',
    bid1Price: '2000.1',
    bid1Size: '1.2',
    ask1Price: '2000.2',
    ask1Size: '1.1',
    markPrice: '2000.3',
    indexPrice: '1999.9',
    fundingRate: '0.0002',
    nextFundingTime: '1700003600000',
    openInterest: '12345'
  };
  const bybitEvents = parseBybitTickers(bybitTickerMsg, 'ETHUSDT', tsRecv);
  equal(bybitEvents[0], {
    ts_event: 1700000000123,
    ts_recv: tsRecv,
    exchange: 'bybit',
    symbol: 'ETHUSDT',
    stream: 'bbo',
    stream_version: 1,
    bid_price: 2000.1,
    bid_qty: 1.2,
    ask_price: 2000.2,
    ask_qty: 1.1
  }, 'bybit_bbo');
  equal(bybitEvents[1], {
    ts_event: 1700000000123,
    ts_recv: tsRecv,
    exchange: 'bybit',
    symbol: 'ETHUSDT',
    stream: 'mark_price',
    stream_version: 1,
    mark_price: 2000.3,
    index_price: 1999.9
  }, 'bybit_mark');
  equal(bybitEvents[2], {
    ts_event: 1700000000123,
    ts_recv: tsRecv,
    exchange: 'bybit',
    symbol: 'ETHUSDT',
    stream: 'funding',
    stream_version: 1,
    funding_rate: 0.0002,
    next_funding_ts: 1700003600000
  }, 'bybit_funding');
  equal(bybitEvents[3], {
    ts_event: 1700000000123,
    ts_recv: tsRecv,
    exchange: 'bybit',
    symbol: 'ETHUSDT',
    stream: 'open_interest',
    stream_version: 1,
    open_interest: 12345
  }, 'bybit_oi');

  const bybitTradesMsg = [
    { T: 1700000000999, p: '2000.5', v: '0.2', S: 'Buy', i: 't1' }
  ];
  const bybitTradeEvents = parseBybitTrades(bybitTradesMsg, 'ETHUSDT', tsRecv);
  equal(bybitTradeEvents[0], {
    ts_event: 1700000000999,
    ts_recv: tsRecv,
    exchange: 'bybit',
    symbol: 'ETHUSDT',
    stream: 'trade',
    stream_version: 1,
    price: 2000.5,
    qty: 0.2,
    side: 1,
    trade_id: 't1'
  }, 'bybit_trade');

  // OKX
  const okxTicker = {
    ts: '1700000000555',
    bidPx: '100.1',
    bidSz: '2.0',
    askPx: '100.2',
    askSz: '2.1'
  };
  equal(parseOkxEvent('tickers', okxTicker, 'BTCUSDT', tsRecv), {
    ts_event: 1700000000555,
    ts_recv: tsRecv,
    exchange: 'okx',
    symbol: 'BTCUSDT',
    stream: 'bbo',
    stream_version: 1,
    bid_price: 100.1,
    bid_qty: 2.0,
    ask_price: 100.2,
    ask_qty: 2.1
  }, 'okx_bbo');

  const okxTrade = { ts: '1700000000666', px: '100.3', sz: '0.1', side: 'buy', tradeId: 'tx' };
  equal(parseOkxEvent('trades', okxTrade, 'BTCUSDT', tsRecv), {
    ts_event: 1700000000666,
    ts_recv: tsRecv,
    exchange: 'okx',
    symbol: 'BTCUSDT',
    stream: 'trade',
    stream_version: 1,
    price: 100.3,
    qty: 0.1,
    side: 1,
    trade_id: 'tx'
  }, 'okx_trade');

  const okxOi = { ts: '1700000000777', oi: '555' };
  equal(parseOkxEvent('open-interest', okxOi, 'BTCUSDT', tsRecv), {
    ts_event: 1700000000777,
    ts_recv: tsRecv,
    exchange: 'okx',
    symbol: 'BTCUSDT',
    stream: 'open_interest',
    stream_version: 1,
    open_interest: 555
  }, 'okx_oi');

  const okxFunding = { ts: '1700000000888', fundingRate: '0.0003', nextFundingTime: '1700003600000' };
  equal(parseOkxEvent('funding-rate', okxFunding, 'BTCUSDT', tsRecv), {
    ts_event: 1700000000888,
    ts_recv: tsRecv,
    exchange: 'okx',
    symbol: 'BTCUSDT',
    stream: 'funding',
    stream_version: 1,
    funding_rate: 0.0003,
    next_funding_ts: 1700003600000
  }, 'okx_funding');

  const okxMark = { ts: '1700000000999', markPx: '101.1' };
  equal(parseOkxEvent('mark-price', okxMark, 'BTCUSDT', tsRecv), {
    ts_event: 1700000000999,
    ts_recv: tsRecv,
    exchange: 'okx',
    symbol: 'BTCUSDT',
    stream: 'mark_price',
    stream_version: 1,
    mark_price: 101.1,
    index_price: null
  }, 'okx_mark');

  console.log('PASS');
}

main();
