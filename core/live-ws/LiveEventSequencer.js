/**
 * Live Event Sequencer (Replay-compatible ordering)
 */

import { createCursor, encodeCursor } from '../replay/CursorCodec.js';

function toBigIntMs(ts) {
  if (typeof ts === 'bigint') return ts;
  if (typeof ts === 'number') return BigInt(Math.trunc(ts));
  if (typeof ts === 'string' && ts.trim() !== '') return BigInt(ts);
  throw new Error('INVALID_TS_EVENT');
}

export class LiveEventSequencer {
  #seqByKey = new Map();

  /**
   * @param {AsyncIterable<Object>} source
   */
  async *sequence(source) {
    for await (const event of source) {
      const tsMs = toBigIntMs(event.ts_event);
      const tsNs = tsMs * 1_000_000n;
      const key = `${event.exchange}|${event.stream}|${event.symbol}`;
      const last = this.#seqByKey.get(key) ?? 0n;
      const nextSeq = last + 1n;
      this.#seqByKey.set(key, nextSeq);

      const out = {
        ...event,
        ts_event: tsNs,
        seq: nextSeq
      };

      const cursor = encodeCursor(createCursor(out));
      out.cursor = cursor;
      yield out;
    }
  }
}

export default LiveEventSequencer;
