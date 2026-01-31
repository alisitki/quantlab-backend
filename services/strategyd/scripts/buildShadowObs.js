#!/usr/bin/env node
/**
 * buildShadowObs.js — CLI for ML shadow observability artifacts.
 */

import { ShadowObsBuilder } from '../runtime/ShadowObsBuilder.js';

function parseArgs(argv) {
  const args = { run: null, all: false };
  for (let i = 0; i < argv.length; i++) {
    const arg = argv[i];
    if (arg === '--run') args.run = argv[++i];
    else if (arg === '--all') args.all = true;
    else if (arg === '--feature_job_id') args.feature_job_id = argv[++i];
    else if (arg === '--label_job_id') args.label_job_id = argv[++i];
    else if (arg === '--model_path') args.model_path = argv[++i];
    else if (arg === '--decision_path') args.decision_path = argv[++i];
  }
  return args;
}

async function main() {
  const args = parseArgs(process.argv.slice(2));
  const builder = new ShadowObsBuilder();

  if (!args.run && !args.all) {
    console.error('Usage: node services/strategyd/scripts/buildShadowObs.js --run <run_id> [--feature_job_id ... --label_job_id ... --model_path ... --decision_path ...]');
    console.error('   or: node services/strategyd/scripts/buildShadowObs.js --all [--feature_job_id ... --label_job_id ... --model_path ... --decision_path ...]');
    process.exit(1);
  }

  if (args.all) {
    await builder.buildAll({
      featureJobId: args.feature_job_id,
      labelJobId: args.label_job_id,
      modelPath: args.model_path,
      decisionPath: args.decision_path
    });
    return;
  }

  await builder.buildForRun({
    runId: args.run,
    featureJobId: args.feature_job_id,
    labelJobId: args.label_job_id,
    modelPath: args.model_path,
    decisionPath: args.decision_path
  });
}

main().catch((err) => {
  console.error('[ShadowObs] FAILED', err.message);
  process.exit(1);
});
