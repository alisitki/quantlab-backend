const parsedRetention = Number(process.env.RUN_ARCHIVE_RETENTION_DAYS || 14);
export const RUN_ARCHIVE_RETENTION_DAYS = Number.isFinite(parsedRetention) ? parsedRetention : 14;
export const CLEANER_INTERVAL_MS = 6 * 60 * 60 * 1000;

export const ML_ACTIVE_ENABLED = process.env.ML_ACTIVE_ENABLED === '1';
export const ML_ACTIVE_KILL = process.env.ML_ACTIVE_KILL === '1';
export const ML_ACTIVE_C_MIN = Number(process.env.ML_ACTIVE_C_MIN || 0.55);
export const ML_ACTIVE_C_MAX = Number(process.env.ML_ACTIVE_C_MAX || 0.75);
export const ML_ACTIVE_MIN_WEIGHT = Number(process.env.ML_ACTIVE_MIN_WEIGHT || 0.9);
export const ML_ACTIVE_MAX_WEIGHT = Number(process.env.ML_ACTIVE_MAX_WEIGHT || 1.1);
export const ML_ACTIVE_MAX_DAILY_IMPACT_PCT = Number(process.env.ML_ACTIVE_MAX_DAILY_IMPACT_PCT || 0.2);
