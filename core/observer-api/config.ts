import path from 'path';
import { fileURLToPath } from 'url';
import dotenv from 'dotenv';
import fs from 'fs';

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

// Load environment variables from repo root
const envPath = path.resolve(__dirname, '../.env');
if (fs.existsSync(envPath)) {
    dotenv.config({ path: envPath });
    console.log(`[CONFIG] Loaded .env from: ${envPath}`);
} else {
    console.log(`[CONFIG] No .env found at: ${envPath}`);
}

// If this file is in observer-api/config.ts, repo root is 1 level up
export const REPO_ROOT = path.resolve(__dirname, '..');

export const DATA_PATHS = {
    health: path.join(REPO_ROOT, 'health'),
    logs: path.join(REPO_ROOT, 'logs'),
    reports: path.join(REPO_ROOT, 'reports/shadow/daily'),
    outbox: path.join(REPO_ROOT, 'ops/outbox'),
    messages: path.join(REPO_ROOT, 'ops/messages')
};

export const S3_CONFIG = {
    endpoint: process.env.S3_COMPACT_ENDPOINT,
    region: 'us-east-1',
    credentials: {
        accessKeyId: process.env.S3_COMPACT_ACCESS_KEY || '',
        secretAccessKey: process.env.S3_COMPACT_SECRET_KEY || ''
    },
    forcePathStyle: true,
    bucket: process.env.S3_COMPACT_BUCKET || 'quantlab-compact'
};

// Canonical S3 Key Builders
export const buildCompactKey = (date: string, exchange = 'binance', stream = 'bbo', symbol = 'btcusdt') => {
    return `exchange=${exchange}/stream=${stream}/symbol=${symbol}/date=${date}/data.parquet`;
};

export const buildFeaturesKey = (date: string, exchange = 'binance', stream = 'bbo', symbol = 'btcusdt', version = 'v1') => {
    return `features/featureset=${version}/exchange=${exchange}/stream=${stream}/symbol=${symbol}/date=${date}/data.parquet`;
};

export const CONFIG_METADATA = {
    env_path: envPath,
    observer_mode: process.env.OBSERVER_MODE,
    bucket: S3_CONFIG.bucket,
    endpoint: S3_CONFIG.endpoint ? new URL(S3_CONFIG.endpoint).hostname : 'N/A',
    key_scheme_version: 'v1'
};
