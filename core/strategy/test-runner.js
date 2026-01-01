#!/usr/bin/env node
import { ReplayEngine } from '../replay/index.js';
import { runReplayWithStrategy } from './Runner.js';
import { PrintHeadTailStrategy } from './strategies/PrintHeadTailStrategy.js';

const [parquetPath, metaPath] = process.argv.slice(2);

if (!parquetPath || !metaPath) {
  console.error('Usage: node strategy/test-runner.js <s3_parquet_path> <s3_meta_path>');
  process.exit(1);
}

async function main() {
  console.log('=== STRATEGY RUNNER TEST ===');
  console.log('DATASET:');
  console.log(parquetPath);
  console.log(metaPath);
  console.log('');

  const replayEngine = new ReplayEngine(parquetPath, metaPath);
  const strategy = new PrintHeadTailStrategy();

  try {
    const result = await runReplayWithStrategy({
      replayEngine,
      strategy,
      options: {
        batchSize: 5000,
        parquetPath,
        metaPath
      }
    });

    console.log('\n--- STATUS ---');
    if (result.stats.processed > 0) {
      console.log('RESULT: PASS');
    } else {
      console.log('RESULT: FAIL (No events processed)');
      process.exit(1);
    }

  } catch (err) {
    console.error('\n--- STATUS ---');
    console.error(`RESULT: FAIL (${err.message})`);
    process.exit(1);
  } finally {
    await replayEngine.close();
  }
}

main();
