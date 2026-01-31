import 'dotenv/config';
import {
  FEATUREXD_PORT,
  FEATUREXD_CONCURRENCY,
  FEATUREXD_BATCH_SIZE,
  DATASETS_DIR,
  JOBS_DIR
} from './constants.js';

export function loadConfig() {
  return {
    port: FEATUREXD_PORT,
    authRequired: process.env.AUTH_REQUIRED !== 'false',
    token: process.env.FEATUREXD_TOKEN,
    replaydUrl: process.env.REPLAYD_URL || 'http://localhost:3030',
    replaydToken: process.env.REPLAYD_TOKEN || null,
    datasetsDir: DATASETS_DIR,
    jobsDir: JOBS_DIR,
    concurrency: FEATUREXD_CONCURRENCY,
    batchSize: FEATUREXD_BATCH_SIZE
  };
}
