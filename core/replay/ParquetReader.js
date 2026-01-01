/**
 * QuantLab Replay Engine v1.1 — Parquet Reader
 * DuckDB-based streaming reader with cursor-based pagination.
 * Uses ts_event + seq for deterministic ordering.
 */

import duckdb from 'duckdb';
import dotenv from 'dotenv';

dotenv.config();

/**
 * @typedef {Object} Cursor
 * @property {bigint|number} ts_event - Last seen ts_event
 * @property {bigint|number} seq - Last seen seq
 */

/**
 * Streaming parquet reader using DuckDB with cursor-based pagination
 */
export class ParquetReader {
  /** @type {duckdb.Database|null} */
  #db = null;
  /** @type {duckdb.Connection|null} */
  #conn = null;
  /** @type {string} */
  #parquetPath;
  /** @type {boolean} */
  #initialized = false;

  /**
   * @param {string} parquetPath - Absolute path to data.parquet or s3:// URI
   */
  constructor(parquetPath) {
    this.#parquetPath = parquetPath;
  }

  /**
   * Initialize DuckDB connection
   * @returns {Promise<void>}
   */
  async init() {
    if (this.#initialized) return;

    return new Promise((resolve, reject) => {
      // In-memory DB for read-only parquet queries
      this.#db = new duckdb.Database(':memory:', (err) => {
        if (err) return reject(new Error(`DUCKDB_INIT_FAILED: ${err.message}`));
        this.#conn = this.#db.connect();

        // If S3 path, configure DuckDB
        if (this.#parquetPath.startsWith('s3://')) {
          const endpoint = (process.env.S3_COMPACT_ENDPOINT || process.env.S3_ENDPOINT || '').replace('https://', '');
          const accessKey = process.env.S3_COMPACT_ACCESS_KEY || process.env.S3_ACCESS_KEY;
          const secretKey = process.env.S3_COMPACT_SECRET_KEY || process.env.S3_SECRET_KEY;

          const setupQueries = [
            "INSTALL httpfs",
            "LOAD httpfs",
            `SET s3_endpoint='${endpoint}'`,
            `SET s3_access_key_id='${accessKey}'`,
            `SET s3_secret_access_key='${secretKey}'`,
            `SET s3_region='${process.env.S3_COMPACT_REGION || 'us-east-1'}'`,
            "SET s3_url_style='path'",
            "SET s3_use_ssl=true"
          ];

          let completed = 0;
          for (const query of setupQueries) {
            this.#conn.run(query, (err) => {
              if (err) {
                console.warn(`[DuckDB] Setup query failed: ${query} - ${err.message}`);
              }
              completed++;
              if (completed === setupQueries.length) {
                this.#initialized = true;
                resolve();
              }
            });
          }
        } else {
          this.#initialized = true;
          resolve();
        }
      });
    });
  }

  /**
   * Get total row count from parquet file
   * @returns {Promise<number>}
   */
  async getRowCount() {
    await this.init();
    const sql = `SELECT COUNT(*) as cnt FROM '${this.#parquetPath}'`;
    return new Promise((resolve, reject) => {
      this.#conn.all(sql, (err, rows) => {
        if (err) return reject(new Error(`PARQUET_COUNT_FAILED: ${err.message}`));
        resolve(Number(rows[0].cnt));
      });
    });
  }

  /**
   * Query a batch of rows using cursor-based pagination
   * @param {number} limit - Max rows to return
   * @param {Cursor|null} cursor - Last seen cursor (null for first batch)
   * @param {number} [startTs] - Optional ts_event filter (>=)
   * @param {number} [endTs] - Optional ts_event filter (<=)
   * @returns {Promise<import('./types.js').Row[]>}
   */
  async queryBatchCursor(limit, cursor, startTs, endTs) {
    await this.init();

    const conditions = [];

    // Cursor condition: (ts_event > last) OR (ts_event = last AND seq > last_seq)
    if (cursor !== null) {
      const ts = cursor.ts_event.toString();
      const sq = cursor.seq.toString();
      conditions.push(`((ts_event > ${ts}) OR (ts_event = ${ts} AND seq > ${sq}))`);
    } else if (startTs !== undefined) {
      // Time filter: startTs (only applies if no cursor)
      conditions.push(`ts_event >= ${startTs}`);
    }

    // Time filter: endTs always applies
    if (endTs !== undefined) {
      conditions.push(`ts_event <= ${endTs}`);
    }

    const whereClause = conditions.length > 0 ? `WHERE ${conditions.join(' AND ')}` : '';

    // FINAL Production Query Template
    // ORDER BY ts_event, seq ASC guarantees deterministic replay
    const sql = `
      SELECT * FROM read_parquet('${this.#parquetPath}')
      ${whereClause}
      ORDER BY ts_event ASC, seq ASC
      LIMIT ${limit}
    `;

    return new Promise((resolve, reject) => {
      this.#conn.all(sql, (err, rows) => {
        if (err) return reject(new Error(`PARQUET_QUERY_FAILED: ${err.message}`));
        resolve(rows);
      });
    });
  }

  /**
   * Get filtered row count (for validation when using time filters)
   * @param {number} [startTs]
   * @param {number} [endTs]
   * @returns {Promise<number>}
   */
  async getFilteredRowCount(startTs, endTs) {
    await this.init();

    let whereClause = '';
    const conditions = [];
    if (startTs !== undefined) conditions.push(`ts_event >= ${startTs}`);
    if (endTs !== undefined) conditions.push(`ts_event <= ${endTs}`);
    if (conditions.length > 0) whereClause = `WHERE ${conditions.join(' AND ')}`;

    const sql = `SELECT COUNT(*) as cnt FROM read_parquet('${this.#parquetPath}') ${whereClause}`;
    return new Promise((resolve, reject) => {
      this.#conn.all(sql, (err, rows) => {
        if (err) return reject(new Error(`PARQUET_COUNT_FAILED: ${err.message}`));
        resolve(Number(rows[0].cnt));
      });
    });
  }

  /**
   * Close DuckDB connection and release resources
   */
  async close() {
    if (this.#conn) {
      this.#conn.close();
      this.#conn = null;
    }
    if (this.#db) {
      return new Promise((resolve) => {
        this.#db.close(() => {
          this.#db = null;
          this.#initialized = false;
          resolve();
        });
      });
    }
  }
}
