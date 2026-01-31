import path from 'node:path';
import { fileURLToPath } from 'node:url';

const __dirname = path.dirname(fileURLToPath(import.meta.url));

export const LABELD_PORT = Number(process.env.LABELD_PORT || 3061);
export const LABELD_CONCURRENCY = Number(process.env.LABELD_CONCURRENCY || 1);
export const LABELD_BATCH_SIZE = Number(process.env.LABELD_BATCH_SIZE || 50000);

export const DATASETS_DIR = path.resolve(__dirname, 'datasets');
export const JOBS_DIR = path.resolve(__dirname, 'datasets');

export const LABEL_MANIFEST_SCHEMA_VERSION = 'v1';
