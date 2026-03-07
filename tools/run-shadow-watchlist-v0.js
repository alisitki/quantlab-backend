#!/usr/bin/env node

import { readFile } from 'node:fs/promises';
import { existsSync } from 'node:fs';
import { spawn } from 'node:child_process';
import path from 'node:path';
import process from 'node:process';
import { fileURLToPath } from 'node:url';

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);
const DEFAULT_WATCHLIST = path.resolve(__dirname, 'shadow_state', 'shadow_watchlist_v0.json');
const DEFAULT_RUNNER = path.resolve(__dirname, 'run-soft-live.js');

function usage() {
  return [
    'Usage: node tools/run-shadow-watchlist-v0.js [--watchlist PATH] [--rank N | --pack-id ID] [--dry-run]',
    '',
    'Minimal watchlist adapter for tools/run-soft-live.js.',
    '',
    'Options:',
    '  --watchlist PATH   Default: tools/shadow_state/shadow_watchlist_v0.json',
    '  --rank N           Select item by watchlist rank (default: 1)',
    '  --pack-id ID       Select item by exact pack_id',
    '  --dry-run          Validate selection and print env mapping without spawning run-soft-live.js',
    '  --help             Show this help',
  ].join('\n');
}

function fail(message) {
  console.error(`WATCHLIST_WRAPPER_ERROR: ${message}`);
  process.exit(1);
}

function parseArgs(argv) {
  const args = {
    watchlist: DEFAULT_WATCHLIST,
    rank: null,
    packId: '',
    dryRun: false,
    help: false,
  };
  for (let idx = 0; idx < argv.length; idx += 1) {
    const arg = argv[idx];
    if (arg === '--help' || arg === '-h') {
      args.help = true;
    } else if (arg === '--dry-run') {
      args.dryRun = true;
    } else if (arg === '--watchlist') {
      idx += 1;
      if (idx >= argv.length) fail('missing_value:--watchlist');
      args.watchlist = path.resolve(process.cwd(), argv[idx]);
    } else if (arg === '--rank') {
      idx += 1;
      if (idx >= argv.length) fail('missing_value:--rank');
      const value = Number(argv[idx]);
      if (!Number.isInteger(value) || value <= 0) fail(`invalid_rank:${argv[idx]}`);
      args.rank = value;
    } else if (arg === '--pack-id') {
      idx += 1;
      if (idx >= argv.length) fail('missing_value:--pack-id');
      args.packId = String(argv[idx] || '').trim();
      if (!args.packId) fail('empty_pack_id');
    } else {
      fail(`unknown_arg:${arg}`);
    }
  }
  return args;
}

async function loadWatchlist(watchlistPath) {
  if (!existsSync(watchlistPath)) {
    fail(`watchlist_missing:${watchlistPath}`);
  }
  let obj;
  try {
    obj = JSON.parse(await readFile(watchlistPath, 'utf-8'));
  } catch (err) {
    fail(`watchlist_invalid_json:${watchlistPath}:${err.message || String(err)}`);
  }
  if (!obj || typeof obj !== 'object') {
    fail(`watchlist_not_object:${watchlistPath}`);
  }
  if (!Array.isArray(obj.items)) {
    fail(`watchlist_missing_items:${watchlistPath}`);
  }
  if (obj.items.length === 0) {
    fail(`watchlist_empty:${watchlistPath}`);
  }
  return obj;
}

function normalizeSelectedItem(item) {
  const exchange = String(item?.exchange || '').trim();
  const symbols = Array.isArray(item?.symbols)
    ? item.symbols.map((value) => String(value || '').trim().toUpperCase()).filter(Boolean)
    : [];
  if (!exchange) {
    fail('selected_item_missing_exchange');
  }
  if (symbols.length === 0) {
    fail('selected_item_missing_symbols');
  }
  return {
    rank: Number(item.rank || 0),
    packId: String(item.pack_id || '').trim(),
    packPath: String(item.pack_path || '').trim(),
    decisionTier: String(item.decision_tier || '').trim(),
    selectionSlot: String(item.selection_slot || '').trim(),
    exchange,
    symbols,
  };
}

function resolveByRank(items, rank) {
  if (rank === null) return null;
  return items.find((item) => Number(item?.rank || 0) === rank) || null;
}

function resolveByPackId(items, packId) {
  if (!packId) return null;
  return items.find((item) => String(item?.pack_id || '').trim() === packId) || null;
}

function resolveSelectedItem(items, args) {
  const rankValue = args.rank === null && !args.packId ? 1 : args.rank;
  const byRank = resolveByRank(items, rankValue);
  const byPackId = resolveByPackId(items, args.packId);

  if (args.packId && !byPackId) {
    fail(`pack_id_not_found:${args.packId}`);
  }
  if (rankValue !== null && !byRank) {
    fail(`rank_not_found:${rankValue}`);
  }
  if (args.packId && rankValue !== null) {
    const rankPackId = String(byRank?.pack_id || '').trim();
    const packIdPackId = String(byPackId?.pack_id || '').trim();
    if (rankPackId !== packIdPackId) {
      fail(`selection_conflict:rank=${rankValue}:pack_id=${args.packId}`);
    }
    return byPackId;
  }
  if (args.packId) return byPackId;
  return byRank;
}

function validateRuntimeEnv() {
  const strategy = String(process.env.GO_LIVE_STRATEGY || '').trim();
  if (!strategy) {
    fail('missing_env:GO_LIVE_STRATEGY');
  }
}

function printSelection(selected, dryRun) {
  console.log(`selected_rank=${selected.rank}`);
  console.log(`selected_pack_id=${selected.packId}`);
  console.log(`selected_pack_path=${selected.packPath}`);
  console.log(`selected_exchange=${selected.exchange}`);
  console.log(`selected_symbols_csv=${selected.symbols.join(',')}`);
  console.log(`selected_decision_tier=${selected.decisionTier}`);
  console.log(`selected_selection_slot=${selected.selectionSlot}`);
  console.log(`runner_path=${DEFAULT_RUNNER}`);
  console.log(`dry_run=${dryRun ? '1' : '0'}`);
  console.log(`env_GO_LIVE_EXCHANGE=${selected.exchange}`);
  console.log(`env_GO_LIVE_SYMBOLS=${selected.symbols.join(',')}`);
  console.log(`env_GO_LIVE_STRATEGY=${String(process.env.GO_LIVE_STRATEGY || '').trim()}`);
  console.log(`env_SHADOW_WATCH_PACK_ID=${selected.packId}`);
  console.log(`env_SHADOW_WATCH_SELECTION_SLOT=${selected.selectionSlot}`);
  console.log(`env_SHADOW_WATCH_DECISION_TIER=${selected.decisionTier}`);
  console.log(`env_SHADOW_WATCH_RANK=${selected.rank}`);
}

async function main() {
  const args = parseArgs(process.argv.slice(2));
  if (args.help) {
    console.log(usage());
    return;
  }
  validateRuntimeEnv();
  if (!existsSync(DEFAULT_RUNNER)) {
    fail(`runner_missing:${DEFAULT_RUNNER}`);
  }
  const watchlist = await loadWatchlist(args.watchlist);
  const selected = normalizeSelectedItem(resolveSelectedItem(watchlist.items, args));
  printSelection(selected, args.dryRun);

  if (args.dryRun) {
    return;
  }

  const childEnv = {
    ...process.env,
    GO_LIVE_EXCHANGE: selected.exchange,
    GO_LIVE_SYMBOLS: selected.symbols.join(','),
    SHADOW_WATCH_PACK_ID: selected.packId,
    SHADOW_WATCH_SELECTION_SLOT: selected.selectionSlot,
    SHADOW_WATCH_DECISION_TIER: selected.decisionTier,
    SHADOW_WATCH_RANK: String(selected.rank),
  };
  const child = spawn(process.execPath, [DEFAULT_RUNNER], {
    cwd: process.cwd(),
    env: childEnv,
    stdio: 'inherit',
  });

  const exitCode = await new Promise((resolve, reject) => {
    child.on('error', reject);
    child.on('exit', (code, signal) => {
      if (signal) {
        reject(new Error(`runner_signal:${signal}`));
        return;
      }
      resolve(code ?? 1);
    });
  });
  process.exit(exitCode);
}

main().catch((err) => fail(err.message || String(err)));
