import 'dotenv/config';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import {
  LABELD_PORT,
  LABELD_CONCURRENCY,
  LABELD_BATCH_SIZE,
  DATASETS_DIR,
  JOBS_DIR
} from './constants.js';

export function loadConfig() {
  const __dirname = path.dirname(fileURLToPath(import.meta.url));
  return {
    port: LABELD_PORT,
    authRequired: process.env.AUTH_REQUIRED !== 'false',
    token: process.env.LABELD_TOKEN,
    datasetsDir: DATASETS_DIR,
    jobsDir: JOBS_DIR,
    concurrency: LABELD_CONCURRENCY,
    batchSize: LABELD_BATCH_SIZE,
    featureDatasetsDir: process.env.FEATUREXD_DATASETS_DIR || path.resolve(__dirname, '../featurexd/datasets')
  };
}
