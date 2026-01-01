#!/usr/bin/env node
import { ReplayEngine } from '../replay/index.js';
import { runReplayWithStrategy } from './Runner.js';
import { SlowStrategy } from './strategies/SlowStrategy.js';

const [parquetPath, metaPath] = process.argv.slice(2);

if (!parquetPath || !metaPath) {
  console.error('Usage: node strategy/test-slow.js <s3_parquet_path> <s3_meta_path>');
  process.exit(1);
}

async function main() {
  console.log('=== SLOW STRATEGY TEST ===');
  
  const replayEngine = new ReplayEngine(parquetPath, metaPath);
  const strategy = new SlowStrategy();

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

    console.log('RESULT: PASS');

  } catch (err) {
    if (err.message === 'STOP') {
      console.log('RESULT: PASS');
    } else {
      console.error(`RESULT: FAIL (${err.message})`);
      process.exit(1);
    }
  } finally {
    await replayEngine.close();
  }
}

main();
