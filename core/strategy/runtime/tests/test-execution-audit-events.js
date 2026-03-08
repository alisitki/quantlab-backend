import { test } from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';


const AUDIT_ROOT = fs.mkdtempSync(path.join(os.tmpdir(), 'quantlab-runtime-audit-'));
process.env.AUDIT_ENABLED = '1';
process.env.AUDIT_SPOOL_DIR = AUDIT_ROOT;

const { StrategyRuntime } = await import('../StrategyRuntime.js');


function resetAuditDir() {
  fs.rmSync(AUDIT_ROOT, { recursive: true, force: true });
  fs.mkdirSync(AUDIT_ROOT, { recursive: true });
}


async function collectAuditLines() {
  for (let attempt = 0; attempt < 20; attempt += 1) {
    const files = fs.existsSync(AUDIT_ROOT)
      ? fs.readdirSync(AUDIT_ROOT, { recursive: true })
          .filter((name) => typeof name === 'string' && name.endsWith('.jsonl'))
          .map((name) => path.join(AUDIT_ROOT, name))
      : [];
    if (files.length > 0) {
      const lines = [];
      for (const file of files.sort()) {
        const content = fs.readFileSync(file, 'utf-8').split('\n').filter(Boolean);
        for (const line of content) {
          lines.push(JSON.parse(line));
        }
      }
      return lines;
    }
    await new Promise((resolve) => setTimeout(resolve, 10));
  }
  return [];
}


async function runSingleEventRuntime({ liveRunId, strategy, executionEngine, riskManager = null }) {
  const runtime = new StrategyRuntime({
    dataset: {
      parquet: '/tmp/fake.parquet',
      meta: '/tmp/fake.meta'
    },
    strategy,
    strategyConfig: {},
    seed: 'test-seed',
    enableMetrics: false,
    enableCheckpoints: false
  });
  runtime.attachExecutionEngine(executionEngine);
  if (riskManager) {
    runtime.attachRiskManager(riskManager);
  }
  await runtime.init();
  runtime.setReplayRunId(liveRunId);
  await runtime.processStream(
    (async function* generate() {
      yield {
        ts_event: 1700000000000000000n,
        seq: 1,
        cursor: 'cursor_1'
      };
    })(),
  );
}


test('StrategyRuntime audit persists minimal decision and fill metadata', { concurrency: false }, async () => {
  resetAuditDir();
  const liveRunId = 'live_run_fill_test';
  await runSingleEventRuntime({
    liveRunId,
    strategy: {
      async onEvent(_event, context) {
        context.placeOrder({
          symbol: 'bnbusdt',
          side: 'buy',
          qty: 1
        });
      }
    },
    executionEngine: {
      onEvent() {},
      onOrder(intent) {
        return {
          fill_id: 'fill_1',
          symbol: intent.symbol,
          side: intent.side,
          qty: intent.qty,
          fillPrice: 612.5,
          ts_event: intent.ts_event
        };
      },
      snapshot() {
        return {
          positions: {},
          fills: [],
          totalRealizedPnl: 0,
          totalUnrealizedPnl: 0,
          equity: 10000,
          maxPositionValue: 100
        };
      }
    }
  });

  const lines = await collectAuditLines();
  const relevant = lines.filter((line) => line.metadata?.live_run_id === liveRunId);
  assert.equal(relevant.length, 2);
  assert.deepEqual(
    relevant.map((line) => line.action),
    ['DECISION', 'FILL'],
  );
  assert.equal(relevant[0].metadata.symbol, 'BNBUSDT');
  assert.equal(relevant[0].metadata.side, 'BUY');
  assert.equal(relevant[0].metadata.qty, 1);
  assert.equal(relevant[0].metadata.ts_event, '1700000000000000000');
  assert.equal(relevant[1].metadata.fill_price, 612.5);
});


test('StrategyRuntime audit persists minimal risk reject metadata without synthetic fill', { concurrency: false }, async () => {
  resetAuditDir();
  const liveRunId = 'live_run_reject_test';
  let onOrderCalled = false;
  await runSingleEventRuntime({
    liveRunId,
    strategy: {
      async onEvent(_event, context) {
        context.placeOrder({
          symbol: 'ethusdt',
          side: 'sell',
          qty: 0.5
        });
      }
    },
    executionEngine: {
      onEvent() {},
      onOrder() {
        onOrderCalled = true;
        return {};
      },
      snapshot() {
        return {
          positions: {},
          fills: [],
          totalRealizedPnl: 0,
          totalUnrealizedPnl: 0,
          equity: 10000,
          maxPositionValue: 0
        };
      }
    },
    riskManager: {
      onEvent() {},
      checkForExit() {
        return null;
      },
      allow() {
        return { allowed: false, reason: 'max_position_exceeded' };
      },
      getStats() {
        return {};
      }
    }
  });

  assert.equal(onOrderCalled, false);
  const lines = await collectAuditLines();
  const relevant = lines.filter((line) => line.metadata?.live_run_id === liveRunId);
  assert.equal(relevant.length, 1);
  assert.equal(relevant[0].action, 'RISK_REJECT');
  assert.equal(relevant[0].metadata.symbol, 'ETHUSDT');
  assert.equal(relevant[0].metadata.side, 'SELL');
  assert.equal(relevant[0].metadata.qty, 0.5);
  assert.equal(relevant[0].metadata.risk_reason, 'max_position_exceeded');
});
