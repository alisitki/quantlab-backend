import path from 'node:path';
import { fileURLToPath } from 'node:url';

const __dirname = path.dirname(fileURLToPath(import.meta.url));

export const BACKTESTD_PORT = Number(process.env.BACKTESTD_PORT || 3041);
export const BACKTESTD_CONCURRENCY = Number(process.env.BACKTESTD_CONCURRENCY || 2);
export const BACKTESTD_STRATEGYD_PORT_BASE = Number(process.env.BACKTESTD_STRATEGYD_PORT_BASE || 3400);
export const BACKTESTD_RUN_POLL_INTERVAL_MS = Number(process.env.BACKTESTD_RUN_POLL_INTERVAL_MS || 1000);
export const BACKTESTD_RUN_TIMEOUT_MS = Number(process.env.BACKTESTD_RUN_TIMEOUT_MS || 60 * 60 * 1000);

export const STRATEGYD_DIR = path.resolve(__dirname, '../strategyd');
export const RUNS_DIR = path.resolve(__dirname, '../strategyd/runs');
export const BACKTESTS_DIR = path.resolve(__dirname, '../strategyd/backtests');
