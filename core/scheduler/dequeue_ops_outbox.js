#!/usr/bin/env node
/**
 * dequeue_ops_outbox.js: Dequeues messages from the outbox and delivers them via selected adapter.
 * Supports idempotency via sent.jsonl tracking and metrics via state.json.
 */
import fs from 'fs';
import path from 'path';
import 'dotenv/config';
import { deliver as stubDeliver } from './delivery/adapters/stub.js';
import { deliver as telegramDeliver } from './delivery/adapters/telegram.js';

const OUTBOX_DIR = 'ops/outbox';
const OUTBOX_FILE = path.join(OUTBOX_DIR, 'outbox.jsonl');
const SENT_FILE = path.join(OUTBOX_DIR, 'sent.jsonl');
const FAILURES_FILE = path.join(OUTBOX_DIR, 'failures.jsonl');
const STATE_FILE = path.join(OUTBOX_DIR, 'state.json');

const ADAPTERS = {
  stub: stubDeliver,
  telegram: telegramDeliver
};

/**
 * Parses CLI arguments
 */
function parseArgs() {
  const args = process.argv.slice(2);
  const result = { apply: false, max: 20, verbose: false, channel: 'ops', adapter: 'stub' };
  for (let i = 0; i < args.length; i++) {
    if (args[i] === '--apply') result.apply = true;
    else if (args[i] === '--max') result.max = parseInt(args[++i], 10);
    else if (args[i] === '--verbose') result.verbose = true;
    else if (args[i] === '--channel') result.channel = args[++i];
    else if (args[i] === '--adapter') result.adapter = args[++i];
  }
  return result;
}

/**
 * Safely parses a JSON line
 */
function safeJSON(line, context) {
  try {
    return JSON.parse(line);
  } catch (e) {
    console.error(`[Dequeue] JSON parse error (${context}): ${e.message}`);
    return null;
  }
}

/**
 * Loads sent IDs from sent.jsonl
 */
function loadSentIds() {
  const ids = new Set();
  if (!fs.existsSync(SENT_FILE)) return ids;
  
  try {
    const lines = fs.readFileSync(SENT_FILE, 'utf8').trim().split('\n');
    lines.forEach((line, i) => {
      if (!line.trim()) return;
      const entry = safeJSON(line, `sent.jsonl line ${i + 1}`);
      if (entry && entry.id) {
        // Multi-channel key: id:adapter (legacy defaults to stub)
        const adapter = entry.adapter || 'stub';
        ids.add(`${entry.id}:${adapter}`);
      }
    });
  } catch (e) {
    console.error(`[Dequeue] Failed to load sent ids: ${e.message}`);
  }
  return ids;
}

/**
 * Atomically updates state.json
 */
function updateState(metrics) {
  const statePath = path.resolve(STATE_FILE);
  const tmpPath = `${statePath}.tmp`;
  try {
    const data = {
      version: 1,
      last_run_at: new Date().toISOString(),
      ...metrics
    };
    fs.writeFileSync(tmpPath, JSON.stringify(data, null, 2));
    fs.renameSync(tmpPath, statePath);
  } catch (e) {
    console.error(`[Dequeue] State update failed: ${e.message}`);
  }
}

/**
 * Main
 */
async function main() {
  const opts = parseArgs();
  console.log(`[Dequeue] Mode: ${opts.apply ? 'APPLY' : 'DRY-RUN'} (adapter=${opts.adapter} max=${opts.max} channel=${opts.channel})`);

  if (!fs.existsSync(OUTBOX_DIR)) {
    fs.mkdirSync(OUTBOX_DIR, { recursive: true });
  }

  if (!fs.existsSync(OUTBOX_FILE)) {
    console.log(`[Dequeue] Outbox file missing: ${OUTBOX_FILE}`);
    process.exit(0);
  }

  const deliverFn = ADAPTERS[opts.adapter];
  if (!deliverFn) {
    console.error(`[Dequeue] Unknown adapter: ${opts.adapter}. Falling back to dry-run logic.`);
  }

  const sentIds = loadSentIds();
  
  let runProcessed = 0;
  let runSent = 0;
  let runSkipped = 0;
  let runFailed = 0;
  let lastId = null;

  try {
    const outboxContent = fs.readFileSync(OUTBOX_FILE, 'utf8').trim();
    if (!outboxContent) {
      console.log(`[Dequeue] Outbox is empty.`);
      process.exit(0);
    }
    
    const outboxLines = outboxContent.split('\n');
    const queue = [];

    for (const line of outboxLines) {
      if (!line.trim()) continue;
      const envelope = safeJSON(line, 'outbox.jsonl');
      if (!envelope || !envelope.id) {
        runSkipped++;
        continue;
      }
      
      if (envelope.channel !== opts.channel) continue;

      // Multi-channel check
      const sentKey = `${envelope.id}:${opts.adapter}`;
      if (sentIds.has(sentKey)) {
        runSkipped++;
        if (opts.verbose) console.log(`[Dequeue] Skip duplicate: ${sentKey}`);
        continue;
      }

      queue.push(envelope);
      if (queue.length >= opts.max) break;
    }

    if (queue.length === 0) {
      console.log(`[Dequeue] No new messages to process. (Skipped ${runSkipped})`);
    } else {
      for (const envelope of queue) {
        runProcessed++;
        lastId = envelope.id;

        if (!deliverFn) {
          console.log(`[Dequeue] would send id=${envelope.id} (UNKNOWN ADAPTER)`);
          continue;
        }

        const result = await deliverFn(envelope);

        if (result.success) {
          if (opts.apply) {
            console.log(`[Dequeue] [${envelope.date}] sent id=${envelope.id} adapter=${opts.adapter}`);
            const entry = {
              id: envelope.id,
              date: envelope.date,
              delivered_at: new Date().toISOString(),
              adapter: opts.adapter,
              body: result.body
            };
            fs.appendFileSync(SENT_FILE, JSON.stringify(entry) + '\n');
            sentIds.add(`${envelope.id}:${opts.adapter}`);
            runSent++;
          } else {
            console.log(`[Dequeue] would send id=${envelope.id} date=${envelope.date} adapter=${opts.adapter} OK`);
          }
        } else {
          if (opts.apply) {
            console.log(`[Dequeue] [${envelope.date}] failed id=${envelope.id} adapter=${opts.adapter} reason=${result.reason}`);
            const entry = {
              id: envelope.id,
              date: envelope.date,
              failed_at: new Date().toISOString(),
              adapter: opts.adapter,
              reason: result.reason,
              body: result.body
            };
            fs.appendFileSync(FAILURES_FILE, JSON.stringify(entry) + '\n');
            runFailed++;
          } else {
            console.log(`[Dequeue] would fail id=${envelope.id} date=${envelope.date} adapter=${opts.adapter} reason=${result.reason}`);
          }
        }
      }
    }

    // Finalize metrics
    if (opts.apply) {
      let currentState = { processed_count: 0, sent_count: 0, skipped_count: 0, failed_count: 0 };
      if (fs.existsSync(STATE_FILE)) {
        try { currentState = JSON.parse(fs.readFileSync(STATE_FILE, 'utf8')); } catch (e) {}
      }

      const finalMetrics = {
        processed_count: (currentState.processed_count || 0) + runProcessed,
        sent_count: (currentState.sent_count || 0) + runSent,
        skipped_count: (currentState.skipped_count || 0) + runSkipped,
        failed_count: (currentState.failed_count || 0) + runFailed,
        last_id: lastId || currentState.last_id
      };

      updateState(finalMetrics);
      console.log(`\nApply Summary: processed=${runProcessed}, sent=${runSent}, failed=${runFailed}, skipped=${runSkipped}`);
    } else {
      console.log(`\nDry-run Summary: would process=${runProcessed}, would skip=${runSkipped}`);
    }

  } catch (e) {
    console.error(`[Dequeue] Unexpected error: ${e.message}`);
  }
  process.exit(0);
}

main().catch(err => {
  console.error(`[Dequeue] Fatal: ${err.message}`);
  process.exit(0);
});
