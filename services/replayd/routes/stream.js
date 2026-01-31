import { once } from 'node:events';
import { randomUUID } from 'node:crypto';
import { ReplayEngine, decodeCursor, encodeCursor } from '../../../core/replay/index.js';
import AsapClock from '../../../core/replay/clock/AsapClock.js';
import RealtimeClock from '../../../core/replay/clock/RealtimeClock.js';
import ScaledClock from '../../../core/replay/clock/ScaledClock.js';
import { buildDatasetPaths, DATASET_MAP } from '../config.js';
import { replayMetrics } from '../metrics.js';

const MAX_BUFFERED_EVENTS = 5000;

/**
 * Select clock based on speed parameter
 */
function selectClock(speed) {
  if (!speed || speed === 'asap') return new AsapClock();
  if (speed === 'realtime') return new RealtimeClock();
  const match = speed.match(/^(\d+)x$/i);
  if (match) return new ScaledClock({ speed: Number(match[1]) });
  return new AsapClock();
}

/**
 * Basic Auth Guard
 */
function validateAuth(request, reply) {
  const token = (request.headers.authorization?.startsWith('Bearer ') ? request.headers.authorization.substring(7) : null) || request.query.token;
  const secret = process.env.REPLAYD_TOKEN || 'test-secret';

  if (token === secret) return true;
  
  reply.code(401).send({ 
    error: 'UNAUTHORIZED', 
    message: 'Invalid token',
    debug: { got: token, expected: secret }
  });
  return false;
}

export default async function streamRoutes(fastify) {
  fastify.get('/stream', async (request, reply) => {
    // 1. Security check
    if (!validateAuth(request, reply)) return;

    const { dataset, symbol, date, cursor: cursorParam, startTs, endTs, speed, run_id, aggregate } = request.query;
    const req_id = request.id || randomUUID();
    const stream_id = randomUUID();

    // 2. Validation
    if (!dataset || !symbol || !date) {
      return reply.code(400).send({ error: 'MISSING_PARAMS', message: 'Required: dataset, symbol, date' });
    }

    if (!DATASET_MAP[dataset]) {
      return reply.code(400).send({ error: 'INVALID_DATASET', message: `Unknown: ${dataset}` });
    }

    // 3. Paths & Clock
    let paths;
    try {
      paths = buildDatasetPaths(dataset, symbol, date);
    } catch (err) {
      return reply.code(400).send({ error: 'PATH_BUILD_FAILED', message: err.message });
    }
    const clock = selectClock(speed);

    // Increment request metric
    replayMetrics.streamRequestsTotal++;
    if (cursorParam) {
      replayMetrics.replayStreamReconnectTotal++;
      replayMetrics.replayStreamRestartsTotal++;
    }
    console.log(`[Stream|${stream_id}] STREAM_REQUEST_RECEIVED { dataset: "${dataset}", symbol: "${symbol}", date: "${date}", cursor: "${cursorParam || 'BEGIN'}" }`);

    // 4. SSE Headers
    reply.raw.writeHead(200, {
      'Content-Type': 'text/event-stream',
      'Cache-Control': 'no-cache',
      'Connection': 'keep-alive',
      'X-Accel-Buffering': 'no',
      'X-Stream-Id': stream_id
    });

    replayMetrics.connectionsActive++;
    replayMetrics.replayActiveStreams = replayMetrics.connectionsActive;
    let aborted = false;
    let lastCursorEmitted = cursorParam || null;
    let bufferedCount = 0;
    let lastEventBytes = 0;
    let eventSentCount = 0;

    const logPrefix = `[Stream|${stream_id}|${run_id || 'no-run'}]`;

    request.raw.on('close', () => {
      aborted = true;
      replayMetrics.connectionsActive--;
      replayMetrics.replayActiveStreams = replayMetrics.connectionsActive;
      replayMetrics.replayQueueDepth = 0;
      console.log(`${logPrefix} Disconnected. events=${eventSentCount} lastCursor=${lastCursorEmitted}`);
    });

    const engine = new ReplayEngine(paths.parquet, paths.meta, { stream: dataset, symbol, date });

    try {
      await engine.validate();

      const replayOpts = {
        batchSize: 1000,
        cursor: cursorParam,
        startTs,
        endTs,
        clock,
        aggregate
      };

      for await (const row of engine.replay(replayOpts)) {
        if (aborted) break;

        const eventData = {
          cursor: encodeCursor(row),
          ts_event: row.ts_event,
          seq: row.seq,
          payload: row
        };

        const json = JSON.stringify(eventData, (_, v) => typeof v === 'bigint' ? v.toString() : v);
        const sseMessage = `data: ${json}\n\n`;

        // 5. Backpressure handling
        if (bufferedCount > MAX_BUFFERED_EVENTS) {
          console.error(`${logPrefix} Disconnected. reason=CLIENT_TOO_SLOW buffer=${bufferedCount} lastCursor=${lastCursorEmitted}`);
          reply.raw.write(`data: ${JSON.stringify({ error: 'CLIENT_TOO_SLOW', lastCursor: lastCursorEmitted })}\n\n`);
          aborted = true;
          replayMetrics.clientTooSlowDisconnectsTotal++;
          replayMetrics.replayQueueDepth = bufferedCount;
          reply.raw.end();
          break;
        }

        const ok = reply.raw.write(sseMessage);
        replayMetrics.eventsSentTotal++;
        replayMetrics.replayStreamEventsTotal++;
        replayMetrics.replayStreamLastEventTs = Date.now();
        replayMetrics.bytesSentTotal += sseMessage.length;
        lastCursorEmitted = eventData.cursor;
        eventSentCount++;
        lastEventBytes = sseMessage.length;
        if (reply.raw.writableLength && lastEventBytes > 0) {
          replayMetrics.replayQueueDepth = Math.ceil(reply.raw.writableLength / lastEventBytes);
        } else {
          replayMetrics.replayQueueDepth = 0;
        }
        if (replayMetrics.replayQueueDepth > 500) {
          replayMetrics.replayBackpressureTotal++;
        }

        if (!ok && !aborted) {
          replayMetrics.backpressureWaitsTotal++;
          bufferedCount += 1000;
          await once(reply.raw, 'drain');
          bufferedCount = 0;
          replayMetrics.replayQueueDepth = bufferedCount;
        }
      }

      if (!aborted && eventSentCount === 0) {
        console.log(`${logPrefix} EMPTY_STREAM`);
        replayMetrics.streamEmptyTotal++;
        reply.raw.write(`data: ${JSON.stringify({ error: 'EMPTY_STREAM' })}\n\n`);
      }

      if (!aborted) reply.raw.end();
    } catch (err) {
      replayMetrics.streamErrorsTotal++;
      replayMetrics.replayStreamErrorsTotal++;
      console.error(`${logPrefix} Error: ${err.message}`);
      if (!aborted) {
        reply.raw.write(`event: error\ndata: ${JSON.stringify({ error: err.message })}\n\n`);
        reply.raw.end();
      }
    } finally {
      await engine.close();
    }
  });
}
