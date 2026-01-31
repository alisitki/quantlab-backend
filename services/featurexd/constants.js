import path from 'node:path';
import { fileURLToPath } from 'node:url';

const __dirname = path.dirname(fileURLToPath(import.meta.url));

export const FEATUREXD_PORT = Number(process.env.FEATUREXD_PORT || 3051);
export const FEATUREXD_CONCURRENCY = Number(process.env.FEATUREXD_CONCURRENCY || 1);
export const FEATUREXD_BATCH_SIZE = Number(process.env.FEATUREXD_BATCH_SIZE || 10000);

export const DATASETS_DIR = path.resolve(__dirname, 'datasets');
export const JOBS_DIR = path.resolve(__dirname, 'datasets');

export const FEATURE_MANIFEST_SCHEMA_VERSION = 'v1';
