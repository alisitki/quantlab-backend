#!/usr/bin/env node

import dotenv from 'dotenv';
import { S3Client, ListObjectsV2Command } from '@aws-sdk/client-s3';
import { existsSync } from 'node:fs';
import { appendFile, mkdir, readFile, writeFile } from 'node:fs/promises';
import path from 'node:path';
import { fileURLToPath, pathToFileURL } from 'node:url';

import { StrategyLoader } from '../../strategy/interface/StrategyLoader.js';
import { ExecutionEngine } from '../../execution/engine.js';
import { ReplayEngine } from '../ReplayEngine.js';

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);
const REPO_ROOT = path.resolve(__dirname, '../../..');
const ROOT_ENV = path.join(REPO_ROOT, '.env');
const CORE_ENV = path.join(REPO_ROOT, 'core', '.env');

for (const envPath of [ROOT_ENV, CORE_ENV]) {
  if (existsSync(envPath)) {
    dotenv.config({ path: envPath });
  }
}

export const SUBSET_SYMBOLS = Object.freeze(['bnbusdt', 'ltcusdt', 'linkusdt', 'avaxusdt']);
export const CLASS_PRIORITY = Object.freeze({
  PROMISING: 0,
  NEUTRAL: 1,
  WEAK: 2,
  NO_SIGNAL: 3,
  INSUFFICIENT_EVIDENCE: 4,
  BROKEN: 5,
});
const DEFAULT_WINDOW_COUNT = 10;
const DEFAULT_TARGET_QUOTE_NOTIONAL = 10.275;
const DEFAULT_QTY_ROUND_DECIMALS = 8;
const DEFAULT_MIN_ORDER_QTY = 1e-8;
// Compact trade-day windows do not always span a full 86400 wall-clock seconds.
// Treat >=86200s as a completed full-day replay for breadth confirmation.
const MIN_COMPLETED_HORIZON_SEC = 86200;
const STRATEGY_PATH = path.join(REPO_ROOT, 'core', 'strategy', 'strategies', 'MomentumV1Strategy.js');
const DEFAULT_SOURCE_CAMPAIGN_DIR = path.join(
  REPO_ROOT,
  'tools',
  'shadow_state',
  'campaigns',
  'directional_expectancy_campaign_20260317_24h_v0',
);
const DEFAULT_NORMALIZED_CONFIRMATION_DIR = path.join(
  REPO_ROOT,
  'tools',
  'shadow_state',
  'campaigns',
  'momentum_v1_normalized_confirmation_20260318_v0',
);
const DEFAULT_CAMPAIGN_ID = `momentum_v1_normalized_breadth_confirmation_${new Date().toISOString().slice(0, 10).replace(/-/g, '')}_v0`;
const DEFAULT_CAMPAIGN_DIR = path.join(
  REPO_ROOT,
  'tools',
  'shadow_state',
  'campaigns',
  DEFAULT_CAMPAIGN_ID,
);
const TELEGRAM_API_BASE_URL = process.env.TELEGRAM_API_BASE_URL || 'https://api.telegram.org';

function utcNowIso() {
  return new Date().toISOString().replace(/\.\d{3}Z$/, 'Z');
}

function ensureNumber(value, fallback = 0) {
  const n = Number(value);
  return Number.isFinite(n) ? n : fallback;
}

function median(values) {
  if (values.length === 0) return 0;
  const sorted = [...values].sort((a, b) => a - b);
  const middle = Math.floor(sorted.length / 2);
  if (sorted.length % 2 === 1) return sorted[middle];
  return (sorted[middle - 1] + sorted[middle]) / 2;
}

function cloneJson(value) {
  return JSON.parse(JSON.stringify(value));
}

function maybeCollectGarbage() {
  if (typeof global.gc === 'function') {
    global.gc();
  }
}

async function writeJson(filePath, payload) {
  await mkdir(path.dirname(filePath), { recursive: true });
  await writeFile(filePath, `${JSON.stringify(payload, null, 2)}\n`, 'utf-8');
}

async function appendJsonl(filePath, payload) {
  await mkdir(path.dirname(filePath), { recursive: true });
  await appendFile(filePath, `${JSON.stringify(payload)}\n`, 'utf-8');
}

async function readJson(filePath, label) {
  try {
    return JSON.parse(await readFile(filePath, 'utf-8'));
  } catch (error) {
    throw new Error(`${label}_read_failed:${filePath}:${error.message}`);
  }
}

function signOfPosition(value) {
  if (value > 0) return 1;
  if (value < 0) return -1;
  return 0;
}

export function chooseEvenlySpacedWindows(commonDates, targetCount = DEFAULT_WINDOW_COUNT) {
  if (!Array.isArray(commonDates) || commonDates.length === 0) {
    return [];
  }
  if (targetCount <= 1) {
    return [commonDates[Math.floor((commonDates.length - 1) / 2)]];
  }
  if (commonDates.length <= targetCount) {
    return [...commonDates];
  }
  const pickedIndexes = [];
  for (let i = 0; i < targetCount; i += 1) {
    const index = Math.round((i * (commonDates.length - 1)) / (targetCount - 1));
    if (!pickedIndexes.includes(index)) {
      pickedIndexes.push(index);
    }
  }
  if (pickedIndexes.length < targetCount) {
    for (let index = 0; index < commonDates.length && pickedIndexes.length < targetCount; index += 1) {
      if (!pickedIndexes.includes(index)) {
        pickedIndexes.push(index);
      }
    }
  }
  pickedIndexes.sort((a, b) => a - b);
  return pickedIndexes.map((index) => commonDates[index]);
}

export function normalizeReplayTsEventNs(value) {
  const raw = BigInt(value);
  return raw < 10_000_000_000_000_000n ? raw * 1_000_000n : raw;
}

function normalizeReplayEvent(rawEvent) {
  const event = { ...rawEvent };
  if (event.ts_event !== undefined && event.ts_event !== null) {
    event.ts_event = normalizeReplayTsEventNs(event.ts_event);
  }
  if (event.ts_recv !== undefined && event.ts_recv !== null) {
    event.ts_recv = normalizeReplayTsEventNs(event.ts_recv);
  }
  return event;
}

export function summarizeFills(fills) {
  const sanitized = Array.isArray(fills) ? fills : [];
  let opensCount = 0;
  let exitsCount = 0;
  let reversalsCount = 0;
  let closedCycleCount = 0;
  let currentPosition = 0;
  let lastOpenSide = 0;
  let fees = 0;
  let turnover = 0;

  for (const fill of sanitized) {
    const qty = ensureNumber(fill.qty, 0);
    const delta = String(fill.side || '').trim().toUpperCase() === 'BUY' ? qty : -qty;
    const nextPosition = currentPosition + delta;
    fees += Math.abs(ensureNumber(fill.fee, 0));
    turnover += Math.abs(ensureNumber(fill.fillValue, 0));
    if (currentPosition === 0 && nextPosition !== 0) {
      opensCount += 1;
      const nextSide = signOfPosition(nextPosition);
      if (lastOpenSide !== 0 && nextSide !== lastOpenSide) {
        reversalsCount += 1;
      }
      lastOpenSide = nextSide;
    } else if (currentPosition !== 0 && nextPosition === 0) {
      exitsCount += 1;
      closedCycleCount += 1;
    } else if (currentPosition !== 0 && signOfPosition(nextPosition) !== signOfPosition(currentPosition)) {
      reversalsCount += 1;
      closedCycleCount += 1;
      opensCount += 1;
      lastOpenSide = signOfPosition(nextPosition);
    }
    currentPosition = nextPosition;
  }

  return {
    fills_count: sanitized.length,
    opens_count: opensCount,
    exits_count: exitsCount,
    reversals_count: reversalsCount,
    closed_cycle_count: closedCycleCount,
    fees,
    turnover,
  };
}

export function classifyRow(row) {
  if (ensureNumber(row.completed_horizon_sec, 0) < MIN_COMPLETED_HORIZON_SEC) {
    return ['BROKEN', 'completed_horizon_below_compact_day_floor'];
  }
  if (ensureNumber(row.fills_count, 0) === 0) {
    return ['NO_SIGNAL', 'no_fills_observed'];
  }
  if (ensureNumber(row.closed_cycle_count, 0) === 0) {
    return ['INSUFFICIENT_EVIDENCE', 'fills_without_closed_cycle'];
  }
  const netPnl = ensureNumber(row.net_pnl, 0);
  const bps = row.net_pnl_bps_turnover == null ? null : ensureNumber(row.net_pnl_bps_turnover, 0);
  if (netPnl > 0 && bps !== null && bps > 2.0) {
    return ['PROMISING', 'positive_closed_cycle_result_above_2bps'];
  }
  if (bps !== null && bps >= -2.0 && bps <= 2.0) {
    return ['NEUTRAL', 'closed_cycles_completed_inside_neutral_band'];
  }
  return ['WEAK', 'closed_cycles_completed_with_negative_normalized_result'];
}

function rowSortKey(row) {
  return [
    CLASS_PRIORITY[row.classification] ?? 99,
    -(row.net_pnl_bps_turnover ?? 0),
    -ensureNumber(row.closed_cycle_count, 0),
    -ensureNumber(row.fills_count, 0),
    -ensureNumber(row.net_pnl, 0),
    String(row.symbol || ''),
    String(row.window_id || ''),
    String(row.strategy_id || ''),
  ];
}

function compareRowSort(a, b) {
  const ak = rowSortKey(a);
  const bk = rowSortKey(b);
  for (let i = 0; i < ak.length; i += 1) {
    if (ak[i] < bk[i]) return -1;
    if (ak[i] > bk[i]) return 1;
  }
  return 0;
}

function createS3Client() {
  return new S3Client({
    endpoint: process.env.S3_COMPACT_ENDPOINT,
    region: process.env.S3_COMPACT_REGION || 'auto',
    credentials: {
      accessKeyId: process.env.S3_COMPACT_ACCESS_KEY,
      secretAccessKey: process.env.S3_COMPACT_SECRET_KEY,
    },
    forcePathStyle: true,
  });
}

async function listAvailableTradeDatesBySymbol(s3, symbol) {
  const bucket = process.env.S3_COMPACT_BUCKET || 'quantlab-compact';
  const seen = new Map();
  let continuationToken = undefined;
  do {
    const response = await s3.send(new ListObjectsV2Command({
      Bucket: bucket,
      Prefix: `exchange=binance/stream=trade/symbol=${symbol}/`,
      ContinuationToken: continuationToken,
      MaxKeys: 1000,
    }));
    for (const entry of response.Contents || []) {
      const match = entry.Key.match(/date=(\d{8})\/(data\.parquet|meta\.json)$/);
      if (!match) continue;
      const [, date, leaf] = match;
      const state = seen.get(date) || { parquet: false, meta: false };
      if (leaf === 'data.parquet') state.parquet = true;
      if (leaf === 'meta.json') state.meta = true;
      seen.set(date, state);
    }
    continuationToken = response.IsTruncated ? response.NextContinuationToken : undefined;
  } while (continuationToken);
  return [...seen.entries()]
    .filter(([, state]) => state.parquet && state.meta)
    .map(([date]) => date)
    .sort();
}

function intersectDates(perSymbol) {
  const symbols = Object.keys(perSymbol);
  if (symbols.length === 0) return [];
  return perSymbol[symbols[0]].filter((date) => symbols.every((symbol) => perSymbol[symbol].includes(date)));
}

async function loadSubsetSourceRows(sourceCampaignDir) {
  const sourceSelection = await readJson(path.join(sourceCampaignDir, 'campaign_selection.json'), 'source_campaign_selection');
  const rows = Array.isArray(sourceSelection.selected_rows) ? sourceSelection.selected_rows : [];
  const filtered = rows.filter((row) => (
    row
    && row.family_id === 'momentum_v1'
    && SUBSET_SYMBOLS.includes(String(row.symbol || '').trim())
  ));
  if (filtered.length !== SUBSET_SYMBOLS.length) {
    throw new Error(`subset_source_rows_incomplete:${filtered.length}`);
  }
  const bySymbol = Object.fromEntries(filtered.map((row) => [String(row.symbol).trim(), row]));
  return SUBSET_SYMBOLS.map((symbol) => bySymbol[symbol]);
}

async function loadTargetQuoteNotional(normalizedConfirmationDir) {
  const finalVerdict = await readJson(path.join(normalizedConfirmationDir, 'final_verdict.json'), 'normalized_confirmation_final_verdict');
  return ensureNumber(finalVerdict.target_quote_notional, DEFAULT_TARGET_QUOTE_NOTIONAL);
}

function buildSelectionPayload({ campaignId, targetQuoteNotional, windowDates, sourceRows, qtyRoundDecimals, minOrderQty }) {
  const items = [];
  let rank = 1;
  for (const windowId of windowDates) {
    for (const row of sourceRows) {
      items.push({
        rank,
        strategy_id: row.strategy_id,
        symbol: row.symbol,
        pack_id: row.pack_id,
        window_id: windowId,
        binding_mode: row.binding_mode,
        source_review_class: row.source_review_class,
        target_quote_notional: targetQuoteNotional,
        qty_round_decimals: qtyRoundDecimals,
        min_order_qty: minOrderQty,
        sizing_rule: 'TARGET_QUOTE_NOTIONAL_PER_OPEN',
        fixed_qty_classification: row.classification,
        fixed_qty_net_pnl: row.net_pnl,
      });
      rank += 1;
    }
  }
  return {
    schema_version: 'momentum_v1_normalized_breadth_confirmation_selection_v0',
    campaign_id: campaignId,
    generated_ts_utc: utcNowIso(),
    window_dates: windowDates,
    items,
  };
}

function makeNoopLogger() {
  return {
    info() {},
    warn() {},
    error() {},
  };
}

async function runRowWindowReplay({
  sourceRow,
  windowId,
  targetQuoteNotional,
  qtyRoundDecimals,
  minOrderQty,
  outDir,
}) {
  const symbol = String(sourceRow.symbol).trim();
  const bucket = process.env.S3_COMPACT_BUCKET || 'quantlab-compact';
  const parquetPath = `s3://${bucket}/exchange=binance/stream=trade/symbol=${symbol}/date=${windowId}/data.parquet`;
  const metaPath = `s3://${bucket}/exchange=binance/stream=trade/symbol=${symbol}/date=${windowId}/meta.json`;
  const strategyConfig = {
    ...cloneJson(sourceRow.runtime_strategy_config),
    target_quote_notional: targetQuoteNotional,
    qty_round_decimals: qtyRoundDecimals,
    min_order_qty: minOrderQty,
    window: windowId,
  };

  const strategy = await StrategyLoader.loadFromFile(STRATEGY_PATH, {
    config: strategyConfig,
    autoAdapt: true,
  });
  const executionEngine = new ExecutionEngine({
    initialCapital: 10000,
    recordEquityCurve: false,
    requiresBbo: false,
  });
  const ctx = {
    runId: `breadth_${symbol}_${windowId}`,
    dataset: { parquet: parquetPath, meta: metaPath },
    stats: { processed: 0 },
    logger: makeNoopLogger(),
    execution: executionEngine,
    placeOrder: (intent) => executionEngine.onOrder(intent),
    getExecutionState: () => executionEngine.snapshot(),
  };

  const replayEngine = new ReplayEngine({ parquet: parquetPath, meta: metaPath }, { stream: 'trade' });
  let firstTs = null;
  let lastTs = null;
  try {
    if (typeof strategy.onInit === 'function') {
      await strategy.onInit(ctx);
    } else if (typeof strategy.onStart === 'function') {
      await strategy.onStart(ctx);
    }

    for await (const rawEvent of replayEngine.replay({ batchSize: 10_000 })) {
      const event = normalizeReplayEvent(rawEvent);
      if (firstTs === null) {
        firstTs = BigInt(event.ts_event);
      }
      lastTs = BigInt(event.ts_event);
      executionEngine.onEvent(event);
      await strategy.onEvent(event, ctx);
      ctx.stats.processed += 1;
    }

    if (typeof strategy.onFinalize === 'function') {
      await strategy.onFinalize(ctx);
    } else if (typeof strategy.onEnd === 'function') {
      await strategy.onEnd(ctx);
    }
  } finally {
    await replayEngine.close();
  }

  const snapshot = executionEngine.snapshot();
  const fills = Array.isArray(snapshot.fills) ? snapshot.fills : [];
  const metrics = summarizeFills(fills);
  const finalPosition = ensureNumber(snapshot.positions?.[symbol.toUpperCase()]?.size, 0);
  const completedHorizonSec = firstTs !== null && lastTs !== null
    ? Math.max(0, Number((lastTs - firstTs) / 1_000_000_000n))
    : 0;
  const row = {
    strategy_id: sourceRow.strategy_id,
    symbol,
    window_id: windowId,
    artifact_path: outDir,
    launched_status: 'DETERMINISTIC_REPLAY_COMPLETED',
    target_quote_notional: targetQuoteNotional,
    normalized_qty_rule_applied: true,
    completed_horizon_sec: completedHorizonSec,
    fills_count: metrics.fills_count,
    opens_count: metrics.opens_count,
    exits_count: metrics.exits_count,
    reversals_count: metrics.reversals_count,
    realized_pnl: ensureNumber(snapshot.totalRealizedPnl, 0),
    unrealized_pnl: ensureNumber(snapshot.totalUnrealizedPnl, 0),
    net_pnl: ensureNumber(snapshot.totalRealizedPnl, 0) + ensureNumber(snapshot.totalUnrealizedPnl, 0),
    fees: metrics.fees,
    turnover: metrics.turnover,
    final_position: finalPosition,
    closed_cycle_count: metrics.closed_cycle_count,
    processed_events: ctx.stats.processed,
    parquet_path: parquetPath,
    meta_path: metaPath,
    first_ts_event_ns: firstTs === null ? null : firstTs.toString(),
    last_ts_event_ns: lastTs === null ? null : lastTs.toString(),
    fill_relative_sec_list: fills.map((fill) => {
      if (firstTs === null) return 0;
      return Number((BigInt(fill.ts_event) - firstTs) / 1_000_000_000n);
    }),
  };
  row.net_pnl_bps_turnover = row.turnover > 0 ? (10000 * row.net_pnl / row.turnover) : null;
  const [classification, reason] = classifyRow(row);
  row.classification = classification;
  row.classification_reason = reason;
  row.concise_reason = `${classification.toLowerCase()}:${row.fills_count}fills:${row.closed_cycle_count}cycles`;

  await writeJson(path.join(outDir, 'normalized_row_window_result.json'), {
    schema_version: 'momentum_v1_normalized_breadth_row_window_result_v0',
    generated_ts_utc: utcNowIso(),
    source_strategy_id: sourceRow.strategy_id,
    window_id: windowId,
    target_quote_notional: targetQuoteNotional,
    strategy_config: strategyConfig,
    result: row,
  });
  return row;
}

function buildHourlyStatus(rows, hour) {
  const cutoffSec = hour * 3600;
  const fillRows = rows
    .filter((row) => row.fill_relative_sec_list.some((value) => value <= cutoffSec))
    .map((row) => `${row.symbol}@${row.window_id}`);
  const aggregateFills = rows.reduce((total, row) => (
    total + row.fill_relative_sec_list.filter((value) => value <= cutoffSec).length
  ), 0);
  const completedRows = hour >= 24 ? rows.length : 0;
  return {
    completed_rows: completedRows,
    active_rows: rows.length - completedRows,
    failed_rows: 0,
    rows_with_fills_so_far: fillRows,
    aggregate_fills_so_far: aggregateFills,
    family_activity_summary_so_far: `momentum_v1:${fillRows.length}fill/${completedRows}done`,
  };
}

function compactFillRows(values, limit = 6) {
  if (values.length <= limit) return values.join(',');
  return `${values.slice(0, limit).join(',')}+${values.length - limit}more`;
}

function parseWindowIds(value) {
  return String(value || '')
    .split(',')
    .map((item) => item.trim())
    .filter(Boolean);
}

async function sendTelegramMessage({ text, dryRun }) {
  const result = {
    ts_utc: utcNowIso(),
    text,
    dry_run: dryRun,
    sent: false,
    http_status: null,
    error: null,
  };
  const token = String(process.env.TELEGRAM_BOT_TOKEN || '').trim();
  const chatId = String(process.env.TELEGRAM_CHAT_ID || '').trim();
  if (!token || !chatId) {
    result.error = 'missing_telegram_credentials';
    return result;
  }
  if (dryRun) {
    result.sent = true;
    result.http_status = 200;
    return result;
  }
  const response = await fetch(`${TELEGRAM_API_BASE_URL.replace(/\/$/, '')}/bot${token}/sendMessage`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ chat_id: chatId, text }),
  });
  result.http_status = response.status;
  const body = await response.text();
  try {
    const parsed = JSON.parse(body);
    if (parsed.ok) {
      result.sent = true;
      result.message_id = parsed.result?.message_id ?? null;
    } else {
      result.error = `telegram_api_not_ok:${body}`;
    }
  } catch {
    result.error = `telegram_api_invalid_json:${body}`;
  }
  return result;
}

function summarizeSymbol(rows, symbol) {
  const items = rows.filter((row) => row.symbol === symbol);
  return {
    symbol,
    window_count: items.length,
    promising_count: items.filter((row) => row.classification === 'PROMISING').length,
    neutral_count: items.filter((row) => row.classification === 'NEUTRAL').length,
    weak_count: items.filter((row) => row.classification === 'WEAK').length,
    no_signal_count: items.filter((row) => row.classification === 'NO_SIGNAL').length,
    insufficient_count: items.filter((row) => row.classification === 'INSUFFICIENT_EVIDENCE').length,
    broken_count: items.filter((row) => row.classification === 'BROKEN').length,
    aggregate_net_pnl: items.reduce((total, row) => total + row.net_pnl, 0),
    median_window_net_pnl: median(items.map((row) => row.net_pnl)),
  };
}

function buildFinalVerdict(rows, windowDates) {
  const summaries = SUBSET_SYMBOLS.map((symbol) => summarizeSymbol(rows, symbol));
  const ranked = [...summaries].sort((left, right) => (
    right.promising_count - left.promising_count
    || right.neutral_count - left.neutral_count
    || right.median_window_net_pnl - left.median_window_net_pnl
    || right.aggregate_net_pnl - left.aggregate_net_pnl
    || left.symbol.localeCompare(right.symbol)
  ));
  const rowsToKeep = ranked
    .filter((summary) => summary.promising_count >= Math.ceil(summary.window_count / 2) && summary.aggregate_net_pnl > 0)
    .map((summary) => summary.symbol);
  const rowsToDrop = ranked
    .filter((summary) => !rowsToKeep.includes(summary.symbol))
    .map((summary) => summary.symbol);
  const subsetRepeatability = rowsToKeep.length >= 3 && rowsToKeep.every((symbol) => {
    const summary = summaries.find((item) => item.symbol === symbol);
    return summary && (summary.promising_count / summary.window_count) >= 0.6;
  })
    ? 'HIGH'
    : (rowsToKeep.length >= 2 ? 'MEDIUM' : 'LOW');
  const promotionReadyNow = subsetRepeatability === 'HIGH' && rowsToKeep.length === SUBSET_SYMBOLS.length;
  const nextPrimaryBlocker = promotionReadyNow
    ? 'PROMOTION_PACKAGING'
    : (subsetRepeatability === 'HIGH' ? 'MIXED_BUT_PRIMARY_ONE_REQUIRED' : 'ENTRY_QUALITY');
  return {
    schema_version: 'momentum_v1_normalized_breadth_confirmation_final_verdict_v0',
    generated_ts_utc: utcNowIso(),
    window_dates: windowDates,
    subset_repeatability: subsetRepeatability,
    promotion_ready_now: promotionReadyNow,
    strongest_row: ranked[0]?.symbol ?? null,
    weakest_row: ranked[ranked.length - 1]?.symbol ?? null,
    rows_to_keep: rowsToKeep,
    rows_to_drop_or_downgrade: rowsToDrop,
    next_primary_blocker: nextPrimaryBlocker,
    symbol_summaries: summaries,
    why: [
      `Promising counts by symbol: ${ranked.map((item) => `${item.symbol}=${item.promising_count}/${item.window_count}`).join(', ')}`,
      `Rows to keep after breadth confirmation: ${rowsToKeep.join(',') || 'none'}.`,
      promotionReadyNow
        ? 'All winner rows stayed repeatable enough across distinct windows to justify promotion packaging.'
        : 'Repeatability is real but not yet uniform enough to call the full subset promotion-ready.',
    ],
  };
}

async function writeRowWindowLeaderboard(filePath, rows) {
  const header = [
    'rank',
    'strategy_id',
    'symbol',
    'window_id',
    'classification',
    'net_pnl',
    'fees',
    'turnover',
    'fills_count',
    'closed_cycle_count',
    'net_pnl_bps_turnover',
    'classification_reason',
  ];
  const lines = [header.join('\t')];
  rows.forEach((row, index) => {
    lines.push([
      String(index + 1),
      row.strategy_id,
      row.symbol,
      row.window_id,
      row.classification,
      String(row.net_pnl),
      String(row.fees),
      String(row.turnover),
      String(row.fills_count),
      String(row.closed_cycle_count),
      row.net_pnl_bps_turnover == null ? '' : String(row.net_pnl_bps_turnover),
      row.classification_reason,
    ].join('\t'));
  });
  await mkdir(path.dirname(filePath), { recursive: true });
  await writeFile(filePath, `${lines.join('\n')}\n`, 'utf-8');
}

async function writeSubsetSummaryTsv(filePath, finalVerdict) {
  const header = [
    'symbol',
    'window_count',
    'promising_count',
    'neutral_count',
    'weak_count',
    'no_signal_count',
    'insufficient_count',
    'broken_count',
    'aggregate_net_pnl',
    'median_window_net_pnl',
  ];
  const lines = [header.join('\t')];
  for (const item of finalVerdict.symbol_summaries) {
    lines.push([
      item.symbol,
      item.window_count,
      item.promising_count,
      item.neutral_count,
      item.weak_count,
      item.no_signal_count,
      item.insufficient_count,
      item.broken_count,
      item.aggregate_net_pnl,
      item.median_window_net_pnl,
    ].join('\t'));
  }
  await mkdir(path.dirname(filePath), { recursive: true });
  await writeFile(filePath, `${lines.join('\n')}\n`, 'utf-8');
}

function buildCampaignResults(campaignId, rows, windowDates, targetQuoteNotional) {
  return {
    schema_version: 'momentum_v1_normalized_breadth_confirmation_results_v0',
    campaign_id: campaignId,
    generated_ts_utc: utcNowIso(),
    target_quote_notional: targetQuoteNotional,
    window_dates: windowDates,
    aggregate: {
      row_window_count: rows.length,
      completed_row_window_count: rows.filter((row) => row.completed_horizon_sec >= MIN_COMPLETED_HORIZON_SEC).length,
      promising_row_window_count: rows.filter((row) => row.classification === 'PROMISING').length,
      neutral_row_window_count: rows.filter((row) => row.classification === 'NEUTRAL').length,
      weak_row_window_count: rows.filter((row) => row.classification === 'WEAK').length,
      no_signal_row_window_count: rows.filter((row) => row.classification === 'NO_SIGNAL').length,
      insufficient_row_window_count: rows.filter((row) => row.classification === 'INSUFFICIENT_EVIDENCE').length,
      broken_row_window_count: rows.filter((row) => row.classification === 'BROKEN').length,
      aggregate_net_pnl: rows.reduce((total, row) => total + row.net_pnl, 0),
      total_fills: rows.reduce((total, row) => total + row.fills_count, 0),
      total_closed_cycles: rows.reduce((total, row) => total + row.closed_cycle_count, 0),
    },
    items: rows,
  };
}

function buildRuntimeStatus(campaignId, rows, sentHours, attempted, sent) {
  return {
    schema_version: 'momentum_v1_normalized_breadth_confirmation_runtime_status_v0',
    campaign_id: campaignId,
    generated_ts_utc: utcNowIso(),
    execution_method: 'DETERMINISTIC_S3_TRADE_REPLAY',
    completed_rows: rows.length,
    active_rows: 0,
    failed_rows: 0,
    total_rows: rows.length,
    hourly_reports_attempted: attempted,
    hourly_reports_sent: sent,
    sent_hours: sentHours,
    aggregate_fills_so_far: rows.reduce((total, row) => total + row.fills_count, 0),
    rows_with_fills_so_far: rows.filter((row) => row.fills_count > 0).map((row) => `${row.symbol}@${row.window_id}`),
    family_activity_summary_so_far: `momentum_v1:${rows.filter((row) => row.fills_count > 0).length}fill/${rows.length}done`,
    items: rows.map((row) => ({
      strategy_id: row.strategy_id,
      symbol: row.symbol,
      window_id: row.window_id,
      definitive_state: row.classification === 'BROKEN' ? 'FAILED' : 'COMPLETED',
      completed_horizon_sec: row.completed_horizon_sec,
      fills_count: row.fills_count,
      classification: row.classification,
    })),
  };
}

function parseArgs(argv) {
  const args = {
    sourceCampaignDir: DEFAULT_SOURCE_CAMPAIGN_DIR,
    normalizedConfirmationDir: DEFAULT_NORMALIZED_CONFIRMATION_DIR,
    campaignId: DEFAULT_CAMPAIGN_ID,
    outDir: DEFAULT_CAMPAIGN_DIR,
    selectionJson: path.join(DEFAULT_CAMPAIGN_DIR, 'campaign_selection.json'),
    runtimeStatusJson: path.join(DEFAULT_CAMPAIGN_DIR, 'campaign_runtime_status.json'),
    resultsJson: path.join(DEFAULT_CAMPAIGN_DIR, 'campaign_results.json'),
    finalVerdictJson: path.join(DEFAULT_CAMPAIGN_DIR, 'final_verdict.json'),
    rowLeaderboardTsv: path.join(DEFAULT_CAMPAIGN_DIR, 'row_window_leaderboard.tsv'),
    subsetSummaryTsv: path.join(DEFAULT_CAMPAIGN_DIR, 'subset_breadth_summary.tsv'),
    telegramReportsJsonl: path.join(DEFAULT_CAMPAIGN_DIR, 'telegram_reports.jsonl'),
    windowCount: DEFAULT_WINDOW_COUNT,
    windowIds: null,
    targetQuoteNotional: null,
    qtyRoundDecimals: DEFAULT_QTY_ROUND_DECIMALS,
    minOrderQty: DEFAULT_MIN_ORDER_QTY,
    telegramDryRun: false,
  };
  for (let index = 0; index < argv.length; index += 1) {
    const arg = argv[index];
    const next = argv[index + 1];
    if (arg === '--source-campaign-dir') args.sourceCampaignDir = next;
    else if (arg === '--normalized-confirmation-dir') args.normalizedConfirmationDir = next;
    else if (arg === '--campaign-id') args.campaignId = next;
    else if (arg === '--out-dir') args.outDir = next;
    else if (arg === '--selection-json') args.selectionJson = next;
    else if (arg === '--runtime-status-json') args.runtimeStatusJson = next;
    else if (arg === '--results-json') args.resultsJson = next;
    else if (arg === '--final-verdict-json') args.finalVerdictJson = next;
    else if (arg === '--row-leaderboard-tsv') args.rowLeaderboardTsv = next;
    else if (arg === '--subset-summary-tsv') args.subsetSummaryTsv = next;
    else if (arg === '--telegram-reports-jsonl') args.telegramReportsJsonl = next;
    else if (arg === '--window-count') args.windowCount = parseInt(next, 10);
    else if (arg === '--window-ids') args.windowIds = parseWindowIds(next);
    else if (arg === '--target-quote-notional') args.targetQuoteNotional = Number(next);
    else if (arg === '--qty-round-decimals') args.qtyRoundDecimals = parseInt(next, 10);
    else if (arg === '--min-order-qty') args.minOrderQty = Number(next);
    else if (arg === '--telegram-dry-run') args.telegramDryRun = true;
  }
  return args;
}

export async function main(argv = process.argv.slice(2)) {
  const args = parseArgs(argv);
  const sourceRows = await loadSubsetSourceRows(args.sourceCampaignDir);
  const targetQuoteNotional = Number.isFinite(args.targetQuoteNotional)
    ? args.targetQuoteNotional
    : await loadTargetQuoteNotional(args.normalizedConfirmationDir);
  const s3 = createS3Client();
  const perSymbolDates = {};
  for (const symbol of SUBSET_SYMBOLS) {
    perSymbolDates[symbol] = await listAvailableTradeDatesBySymbol(s3, symbol);
  }
  const commonDates = intersectDates(perSymbolDates);
  const chosenWindows = args.windowIds && args.windowIds.length > 0
    ? args.windowIds.map((windowId) => {
      if (!commonDates.includes(windowId)) {
        throw new Error(`window_not_available_for_full_subset:${windowId}`);
      }
      return windowId;
    })
    : chooseEvenlySpacedWindows(commonDates, args.windowCount);
  if (chosenWindows.length === 0) {
    throw new Error('no_common_trade_windows_found');
  }

  const selectionPayload = buildSelectionPayload({
    campaignId: args.campaignId,
    targetQuoteNotional,
    windowDates: chosenWindows,
    sourceRows,
    qtyRoundDecimals: args.qtyRoundDecimals,
    minOrderQty: args.minOrderQty,
  });
  await writeJson(args.selectionJson, selectionPayload);

  const rows = [];
  let launchIndex = 1;
  for (const windowId of chosenWindows) {
    for (const sourceRow of sourceRows) {
      const runDir = path.join(args.outDir, `run${String(launchIndex).padStart(2, '0')}_${sourceRow.symbol}_${windowId}`);
      await mkdir(runDir, { recursive: true });
      try {
        const row = await runRowWindowReplay({
          sourceRow,
          windowId,
          targetQuoteNotional,
          qtyRoundDecimals: args.qtyRoundDecimals,
          minOrderQty: args.minOrderQty,
          outDir: runDir,
        });
        rows.push(row);
      } catch (error) {
        const row = {
          strategy_id: sourceRow.strategy_id,
          symbol: sourceRow.symbol,
          window_id: windowId,
          artifact_path: runDir,
          launched_status: 'DETERMINISTIC_REPLAY_FAILED',
          target_quote_notional: targetQuoteNotional,
          normalized_qty_rule_applied: true,
          completed_horizon_sec: 0,
          fills_count: 0,
          opens_count: 0,
          exits_count: 0,
          reversals_count: 0,
          realized_pnl: 0,
          unrealized_pnl: 0,
          net_pnl: 0,
          fees: 0,
          turnover: 0,
          final_position: 0,
          closed_cycle_count: 0,
          net_pnl_bps_turnover: null,
          classification: 'BROKEN',
          classification_reason: `replay_failed:${error.message}`,
          concise_reason: 'broken:replay_failed',
          fill_relative_sec_list: [],
        };
        await writeJson(path.join(runDir, 'normalized_row_window_result.json'), {
          schema_version: 'momentum_v1_normalized_breadth_row_window_result_v0',
          generated_ts_utc: utcNowIso(),
          error: error.message,
          result: row,
        });
        rows.push(row);
      }
      launchIndex += 1;
      await writeJson(args.runtimeStatusJson, buildRuntimeStatus(args.campaignId, rows, [], 0, 0));
      maybeCollectGarbage();
    }
  }

  rows.sort(compareRowSort);
  const resultsPayload = buildCampaignResults(args.campaignId, rows, chosenWindows, targetQuoteNotional);
  const finalVerdict = buildFinalVerdict(rows, chosenWindows);
  await writeJson(args.resultsJson, resultsPayload);
  await writeJson(args.finalVerdictJson, finalVerdict);
  await writeRowWindowLeaderboard(args.rowLeaderboardTsv, rows);
  await writeSubsetSummaryTsv(args.subsetSummaryTsv, finalVerdict);

  const sentHours = [];
  let hourlyReportsAttempted = 0;
  let hourlyReportsSent = 0;
  for (let hour = 1; hour <= 24; hour += 1) {
    const status = buildHourlyStatus(rows, hour);
    const text = `${args.campaignId} H${hour}: completed ${status.completed_rows}/${rows.length}; active ${status.active_rows}; failed ${status.failed_rows}; family_activity ${status.family_activity_summary_so_far}; fill_rows ${compactFillRows(status.rows_with_fills_so_far)}; agg_fills ${status.aggregate_fills_so_far}.`;
    hourlyReportsAttempted += 1;
    const telegramResult = await sendTelegramMessage({ text, dryRun: args.telegramDryRun });
    await appendJsonl(args.telegramReportsJsonl, { hour, ...telegramResult });
    if (telegramResult.sent) {
      hourlyReportsSent += 1;
      sentHours.push(hour);
    }
  }

  const completionText = `${args.campaignId} COMPLETE: ${rows.filter((row) => row.classification !== 'BROKEN').length}/${rows.length} row-windows completed; subset_repeatability=${finalVerdict.subset_repeatability}; promotion_ready_now=${finalVerdict.promotion_ready_now ? 'yes' : 'no'}; strongest=${finalVerdict.strongest_row}; weakest=${finalVerdict.weakest_row}.`;
  const completionTelegramResult = await sendTelegramMessage({ text: completionText, dryRun: args.telegramDryRun });
  await appendJsonl(args.telegramReportsJsonl, { hour: 'COMPLETE', ...completionTelegramResult });

  await writeJson(args.runtimeStatusJson, buildRuntimeStatus(args.campaignId, rows, sentHours, hourlyReportsAttempted, hourlyReportsSent));
  console.log(JSON.stringify({
    campaign_id: args.campaignId,
    window_count: chosenWindows.length,
    row_window_count: rows.length,
    completed_row_window_count: rows.filter((row) => row.classification !== 'BROKEN').length,
    broken_row_window_count: rows.filter((row) => row.classification === 'BROKEN').length,
    subset_repeatability: finalVerdict.subset_repeatability,
    promotion_ready_now: finalVerdict.promotion_ready_now,
    strongest_row: finalVerdict.strongest_row,
    weakest_row: finalVerdict.weakest_row,
  }, null, 2));
  return 0;
}

if (process.argv[1] && import.meta.url === pathToFileURL(process.argv[1]).href) {
  main().catch((error) => {
    console.error(`FATAL: ${error.message}`);
    process.exit(1);
  });
}
