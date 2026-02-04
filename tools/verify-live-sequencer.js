#!/usr/bin/env node
/**
 * Verify LiveEventSequencer ordering + cursor contract
 */

import assert from 'node:assert';
import { LiveEventSequencer } from '../core/live-ws/LiveEventSequencer.js';
import { decodeCursor } from '../core/replay/CursorCodec.js';

async function collect(iter) {
  const out = [];
  for await (const item of iter) out.push(item);
  return out;
}

async function* mockLiveEvents() {
  yield { ts_event: 1700000000000, ts_recv: 1700000000100, exchange: 'binance', symbol: 'BTCUSDT', stream: 'bbo', bid_price: 1, bid_qty: 1, ask_price: 2, ask_qty: 2, stream_version: 1 };
  yield { ts_event: 1700000000000, ts_recv: 1700000000101, exchange: 'binance', symbol: 'BTCUSDT', stream: 'bbo', bid_price: 1.1, bid_qty: 1, ask_price: 2.1, ask_qty: 2, stream_version: 1 };
  yield { ts_event: 1700000000001, ts_recv: 1700000000102, exchange: 'bybit', symbol: 'ETHUSDT', stream: 'trade', price: 10, qty: 1, side: 1, trade_id: 't1', stream_version: 1 };
}

async function* replayMock(events) {
  const seqByKey = new Map();
  for (const e of events) {
    const tsNs = BigInt(e.ts_event) * 1_000_000n;
    const key = `${e.exchange}|${e.stream}|${e.symbol}`;
    const last = seqByKey.get(key) ?? 0n;
    const seq = last + 1n;
    seqByKey.set(key, seq);
    const out = { ...e, ts_event: tsNs, seq };
    out.cursor = Buffer.from(JSON.stringify({ v: 1, ts_event: out.ts_event.toString(), seq: out.seq.toString() }), 'utf-8').toString('base64');
    yield out;
  }
}

async function main() {
  const sequencer = new LiveEventSequencer();
  const input = await collect(mockLiveEvents());
  const liveOut = await collect(sequencer.sequence(mockLiveEvents()));
  const replayOut = await collect(replayMock(input));

  assert.strictEqual(liveOut.length, replayOut.length);

  for (let i = 0; i < liveOut.length; i++) {
    const a = liveOut[i];
    const b = replayOut[i];
    assert.strictEqual(a.ts_event, b.ts_event, 'ts_event mismatch');
    assert.strictEqual(a.seq, b.seq, 'seq mismatch');

    const decoded = decodeCursor(a.cursor);
    assert.strictEqual(decoded.ts_event, a.ts_event);
    assert.strictEqual(decoded.seq, a.seq);
  }

  console.log('PASS');
}

main().catch((err) => {
  console.error('FAIL', err.message);
  process.exit(1);
});
