#!/usr/bin/env node
/**
 * archiveActive.js — archive derived ACTIVE artifacts.
 */

import { ArchiveExporter } from '../runtime/ArchiveExporter.js';

function parseArgs(argv) {
  const args = {};
  for (let i = 0; i < argv.length; i++) {
    const key = argv[i];
    if (!key.startsWith('--')) continue;
    const name = key.slice(2);
    const value = argv[i + 1] && !argv[i + 1].startsWith('--') ? argv[i + 1] : true;
    args[name] = value;
    if (value !== true) i++;
  }
  return args;
}

async function main() {
  const args = parseArgs(process.argv.slice(2));
  if (!args.seed && !args.strategy_id) {
    console.error('Usage: node archiveActive.js --seed <seed> | --strategy_id <id>');
    process.exit(1);
  }

  const exporter = new ArchiveExporter();
  const resolved = await exporter.resolveArtifacts({
    seed: args.seed || null,
    strategyId: args.strategy_id || null
  });

  const files = [resolved.triadPath, resolved.decisionPath, resolved.activeConfigPath].filter(Boolean);
  if (!files.length) {
    console.error('[ArchiveActive] action=error error=NO_ARTIFACTS');
    process.exit(1);
  }

  const archived = await exporter.exportFiles(files);
  if (archived.length === 0) {
    console.log('[ArchiveActive] action=skipped reason=already_archived');
    process.exit(0);
  }

  for (const filePath of archived) {
    console.log(`[ArchiveActive] action=archived path=${filePath}`);
  }
}

main().catch((err) => {
  console.error(`[ArchiveActive] action=error error=${err.message}`);
  process.exit(1);
});
