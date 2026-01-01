#!/usr/bin/env node
import { ReplayEngine } from '../replay/index.js';
import { runReplayWithStrategy } from './Runner.js';
import AsapClock from '../replay/clock/AsapClock.js';
import RealtimeClock from '../replay/clock/RealtimeClock.js';
import ScaledClock from '../replay/clock/ScaledClock.js';

const [parquetPath, metaPath, mode, speed] = process.argv.slice(2);

if (!parquetPath || !metaPath || !mode) {
  console.error('Usage: node strategy/test-clock.js <s3_parquet_path> <s3_meta_path> asap|realtime|scaled [speed]');
  process.exit(1);
}

class LogStrategy {
  constructor() {
    this.count = 0;
  }
  async onEvent(event, ctx) {
    if (this.count >= 10) return;
    this.count++;
    
    // Manual wall clock formatting to HH:mm:ss.SSS
    const now = new Date();
    const pad = (n, m=2) => String(n).padStart(m, '0');
    const wallNow = `${pad(now.getHours())}:${pad(now.getMinutes())}:${pad(now.getSeconds())}.${pad(now.getMilliseconds(), 3)}`;
    
    const wallColor = '\x1b[36m';
    const reset = '\x1b[0m';
    
    console.log(`event ts_event=${event.ts_event} wall=${wallColor}${wallNow}${reset}`);
    
    if (this.count === 10) {
      console.log('\nRESULT: PASS');
      process.exit(0);
    }
  }
}

async function main() {
  console.log('=== CLOCK TEST ===');
  console.log(`MODE: ${mode}`);
  console.log('');

  const replayEngine = new ReplayEngine(parquetPath, metaPath);
  const strategy = new LogStrategy();
  
  let clock;
  if (mode === 'asap') {
    clock = AsapClock;
  } else if (mode === 'realtime') {
    clock = RealtimeClock;
  } else if (mode === 'scaled') {
    clock = new ScaledClock({ speed: parseFloat(speed || '2.0') });
  } else {
    console.error(`Unknown mode: ${mode}`);
    process.exit(1);
  }

  try {
    await runReplayWithStrategy({
      replayEngine,
      strategy,
      options: {
        batchSize: 50, 
        clock,
        parquetPath,
        metaPath
      }
    });

  } catch (err) {
    console.error('\n--- STATUS ---');
    console.error(`RESULT: FAIL (${err.message})`);
    process.exit(1);
  } finally {
    await replayEngine.close();
  }
}

main();
