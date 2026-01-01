#!/usr/bin/env node
import { ReplayEngine } from '../replay/index.js';
import { runReplayWithStrategy } from './Runner.js';
import { FaultyStrategy } from './strategies/FaultyStrategy.js';

const [parquetPath, metaPath] = process.argv.slice(2);

if (!parquetPath || !metaPath) {
  console.error('Usage: node strategy/test-faulty.js <s3_parquet_path> <s3_meta_path>');
  process.exit(1);
}

async function main() {
  console.log('=== FAULTY STRATEGY TEST ===');
  
  const replayEngine = new ReplayEngine(parquetPath, metaPath);
  const strategy = new FaultyStrategy();

  try {
    await runReplayWithStrategy({
      replayEngine,
      strategy,
      options: {
        batchSize: 5000,
        parquetPath,
        metaPath
      }
    });

    console.log('RESULT: FAIL (Should have thrown error)');
    process.exit(1);

  } catch (err) {
    if (err.message === 'INTENTIONAL_STRATEGY_FAILURE') {
      // We expect the count to be around 10000 (actually exactly 10000 when the error is thrown)
      // but let's just log the error as requested.
      console.error(`ERROR: ${err.message}`);
      console.log('RESULT: PASS');
      process.exit(0);
    } else {
      console.error(`ERROR: Unexpected error: ${err.message}`);
      process.exit(1);
    }
  } finally {
    await replayEngine.close();
  }
}

main();
