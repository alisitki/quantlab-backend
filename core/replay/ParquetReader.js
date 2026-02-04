/**
 * QuantLab Replay Engine v1.1 — Parquet Reader
 * DuckDB-based streaming reader with cursor-based pagination.
 * Uses ts_event + seq for deterministic ordering.
 * 
 * See ORDERING_CONTRACT.js for ordering rules.
 */

import duckdb from 'duckdb';
import dotenv from 'dotenv';
import { SQL_ORDER_CLAUSE, buildCursorWhereClause } from './ORDERING_CONTRACT.js';

dotenv.config();

/**
 * @typedef {Object} Cursor
 * @description Cursor containing all fields defined in ORDERING_CONTRACT
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
   * Classify DuckDB/parquet errors
   * @param {string} message
   * @returns {{category: string, code: string}}
   */
  #classifyError(message) {
    const msg = message.toLowerCase();
    if (msg.includes('snappy')) return { category: 'snappy', code: 'PARQUET_SNAPPY_CORRUPTION' };
    if (msg.includes('corrupt') || msg.includes('parquet') || msg.includes('footer') || msg.includes('magic') || msg.includes('metadata')) {
      return { category: 'corruption', code: 'PARQUET_CORRUPTION' };
    }
    if (msg.includes('schema') || msg.includes('column') || msg.includes('type') || msg.includes('cast')) {
      return { category: 'schema', code: 'PARQUET_SCHEMA' };
    }
    if (msg.includes('no such file') || msg.includes('not found') || msg.includes('permission') || msg.includes('i/o') || msg.includes('io error') || msg.includes('read error')) {
      return { category: 'io', code: 'PARQUET_IO' };
    }
    return { category: 'unknown', code: 'PARQUET_ERROR' };
  }

  /**
   * Wrap DuckDB error into classified error
   * @param {Error} err
   * @param {string} operation
   */
  #wrapDuckdbError(err, operation) {
    const { category, code } = this.#classifyError(err.message || String(err));
    const parquetPath = this.#parquetPath;
    if (category === 'corruption' || category === 'snappy') {
      console.error(JSON.stringify({
        event: 'parquet_quarantine',
        code: 'QUARANTINED_FILE',
        reason: code,
        error_type: category,
        action: 'quarantine',
        stream: 'unknown',
        date: 'unknown',
        symbol: 'unknown',
        parquet_path: parquetPath,
        operation,
        detail: err.message || String(err)
      }));
      const e = new Error(`QUARANTINED_FILE: ${code}: ${err.message || String(err)}`);
      e.code = 'QUARANTINED_FILE';
      e.reason = code;
      e.parquet_path = parquetPath;
      return e;
    }

    const e = new Error(`${code}: ${err.message || String(err)}`);
    e.code = code;
    e.parquet_path = parquetPath;
    return e;
  }

  /**
   * @param {string|string[]} parquetPath - Absolute path to data.parquet, s3:// URI, or array of paths
   */
  constructor(parquetPath) {
    this.#parquetPath = parquetPath;
  }

  /**
   * Get formatted parquet source for DuckDB SQL
   * @returns {string}
   */
  #getParquetSource() {
    if (Array.isArray(this.#parquetPath)) {
      return "[" + this.#parquetPath.map(p => `'${p}'`).join(', ') + "]";
    }
    return `'${this.#parquetPath}'`;
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

        // Check if any path is S3
        const paths = Array.isArray(this.#parquetPath) ? this.#parquetPath : [this.#parquetPath];
        const hasS3 = paths.some(p => p.startsWith('s3://'));

        // If S3 path, configure DuckDB
        if (hasS3) {
          const endpointRaw = process.env.S3_COMPACT_ENDPOINT;
          const accessKey = process.env.S3_COMPACT_ACCESS_KEY;
          const secretKey = process.env.S3_COMPACT_SECRET_KEY;

          if (!endpointRaw || !accessKey || !secretKey) {
            return reject(new Error(
              `CREDENTIAL_ERROR: Missing required S3_COMPACT_* variables for DuckDB S3 access. ` +
              `Required: [S3_COMPACT_ENDPOINT, S3_COMPACT_ACCESS_KEY, S3_COMPACT_SECRET_KEY]`
            ));
          }

          const endpoint = endpointRaw.replace('https://', '');

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
   * Get total row count from parquet file(s)
   * @returns {Promise<number>}
   */
  async getRowCount() {
    await this.init();
    const source = this.#getParquetSource();
    const sql = `SELECT COUNT(*) as cnt FROM read_parquet(${source})`;
    return new Promise((resolve, reject) => {
      this.#conn.all(sql, (err, rows) => {
        if (err) return reject(this.#wrapDuckdbError(err, 'count'));
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

    // Cursor condition: EXCLUSIVE - events strictly AFTER cursor position
    // Uses buildCursorWhereClause from ORDERING_CONTRACT for consistency
    if (cursor !== null) {
      conditions.push(buildCursorWhereClause(cursor));
    }

    if (startTs !== undefined) {
      conditions.push(`ts_event >= CAST('${startTs}' AS UBIGINT)`);
    }

    // Time filter: endTs always applies
    if (endTs !== undefined) {
      conditions.push(`ts_event <= CAST('${endTs}' AS UBIGINT)`);
    }


    const whereClause = conditions.length > 0 ? `WHERE ${conditions.join(' AND ')}` : '';
    const source = this.#getParquetSource();

    // Use SQL_ORDER_CLAUSE from ORDERING_CONTRACT for deterministic replay
    const sql = `
      SELECT * FROM read_parquet(${source})
      ${whereClause}
      ${SQL_ORDER_CLAUSE}
      LIMIT ${limit}
    `;

    return new Promise((resolve, reject) => {
      this.#conn.all(sql, (err, rows) => {
        if (err) return reject(this.#wrapDuckdbError(err, 'query'));
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
    if (startTs !== undefined) conditions.push(`ts_event >= CAST('${startTs}' AS UBIGINT)`);
    if (endTs !== undefined) conditions.push(`ts_event <= CAST('${endTs}' AS UBIGINT)`);
    if (conditions.length > 0) whereClause = `WHERE ${conditions.join(' AND ')}`;


    const source = this.#getParquetSource();
    const sql = `SELECT COUNT(*) as cnt FROM read_parquet(${source}) ${whereClause}`;
    return new Promise((resolve, reject) => {
      this.#conn.all(sql, (err, rows) => {
        if (err) return reject(this.#wrapDuckdbError(err, 'filtered_count'));
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
