import 'dotenv/config';
import {
  BACKTESTD_PORT,
  BACKTESTD_CONCURRENCY,
  BACKTESTD_STRATEGYD_PORT_BASE,
  BACKTESTD_RUN_POLL_INTERVAL_MS,
  BACKTESTD_RUN_TIMEOUT_MS,
  STRATEGYD_DIR,
  RUNS_DIR,
  BACKTESTS_DIR
} from './constants.js';

export function loadConfig() {
  return {
    port: BACKTESTD_PORT,
    authRequired: process.env.AUTH_REQUIRED !== 'false',
    token: process.env.BACKTESTD_TOKEN,
    replaydUrl: process.env.REPLAYD_URL || 'http://localhost:3030',
    replaydToken: process.env.REPLAYD_TOKEN || null,
    strategydToken: process.env.STRATEGYD_TOKEN || process.env.BACKTESTD_STRATEGYD_TOKEN || null,
    strategydDir: STRATEGYD_DIR,
    runsDir: RUNS_DIR,
    backtestsDir: BACKTESTS_DIR,
    concurrency: BACKTESTD_CONCURRENCY,
    strategydPortBase: BACKTESTD_STRATEGYD_PORT_BASE,
    runPollIntervalMs: BACKTESTD_RUN_POLL_INTERVAL_MS,
    runTimeoutMs: BACKTESTD_RUN_TIMEOUT_MS
  };
}
