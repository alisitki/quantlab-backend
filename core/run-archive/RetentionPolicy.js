/**
 * Run Archive Retention Policy
 */

function envBool(val) {
  return val === '1' || val === 'true' || val === 'yes';
}

function parseRetentionDays(val, fallback) {
  const n = parseInt(val, 10);
  if (!Number.isFinite(n) || n <= 0) return fallback;
  return n;
}

export class RetentionPolicy {
  /** @type {boolean} */
  #enabled;
  /** @type {number} */
  #retentionDays;

  constructor({ enabled, retentionDays }) {
    this.#enabled = enabled;
    this.#retentionDays = retentionDays;
  }

  static fromEnv() {
    const enabled = envBool(process.env.RUN_ARCHIVE_RETENTION_ENABLED || '0');
    const retentionDays = parseRetentionDays(process.env.RUN_ARCHIVE_RETENTION_DAYS, 30);
    return new RetentionPolicy({ enabled, retentionDays });
  }

  get enabled() { return this.#enabled; }
  get retentionDays() { return this.#retentionDays; }

  /**
   * Determine if a run is expired based on finished_at.
   * @param {string|null} finishedAtIso
   * @param {Date} now
   */
  isExpired(finishedAtIso, now) {
    if (!finishedAtIso) return false;
    const finishedAt = Date.parse(finishedAtIso);
    if (!Number.isFinite(finishedAt)) return false;

    const retentionMs = this.#retentionDays * 24 * 60 * 60 * 1000;
    return finishedAt + retentionMs < now.getTime();
  }
}

export default RetentionPolicy;
