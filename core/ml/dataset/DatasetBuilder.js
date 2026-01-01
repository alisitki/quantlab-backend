import { ParquetReader } from '../../replay/ParquetReader.js';

/**
 * DatasetBuilder: Orchestrates Replay + FeatureBuilder + LabelBuilder
 */
export class DatasetBuilder {
  /**
   * Run Replay + FeatureBuilder, collect features, and align with labels.
   * 
   * @param {Object} params
   * @param {ReplayEngine} params.replay
   * @param {FeatureBuilder} params.featureBuilder
   * @param {LabelBuilder} [params.labelBuilder]
   * @param {number} [params.startTs]
   * @param {number} [params.endTs]
   * @returns {Promise<{X: Array<Array<number>>, y: Array<number>, meta: Object}>}
   */
  async buildDataset({ replay, featureBuilder, startTs, endTs }) {
    const vectors = [];
    const rawEvents = [];

    // 1. Run Replay
    for await (const event of replay.replay({ startTs, endTs })) {
      const vector = featureBuilder.onEvent(event);
      if (vector) {
        vectors.push(vector);
      }
    }

    if (vectors.length < 2) {
      throw new Error(`Insufficient data for dataset building: ${vectors.length} vectors collected.`);
    }

    // 2. Generate Labels (Next-Return Sign)
    // Note: We use mid_price from the vector to build labels
    // Label for row T is sign(mid_price[T+1] - mid_price[T])
    const labels = [];
    const X = [];

    for (let i = 0; i < vectors.length - 1; i++) {
        const currentVector = vectors[i];
        const nextVector = vectors[i + 1];

        // Label: sign(next_mid - current_mid)
        let label = 0;
        if (nextVector.mid_price > currentVector.mid_price) label = 1;
        else if (nextVector.mid_price < currentVector.mid_price) label = -1;

        // Convert named vector to array for ML
        const xRow = Object.values(currentVector);
        X.push(xRow);
        labels.push(label);
    }

    const featureNames = Object.keys(vectors[0]);
    const meta = await replay.getMeta();

    return {
      X,
      y: labels,
      meta: {
        featureNames,
        symbol: meta.symbol,
        dateRange: { startTs, endTs }
      }
    };
  }

  /**
   * Load features and labels directly from a pre-calculated parquet file.
   * Loads in batches to avoid full-memory load.
   * 
   * @param {string} parquetPath - Path to feature parquet
   * @param {number} [batchSize=10000] - Batch size for loading
   * @returns {Promise<{X: Array<Array<number>>, y: Array<number>, meta: Object}>}
   */
  async loadFromParquet(parquetPath, batchSize = 10000) {
    // Use DuckDB directly for feature parquet (no seq column)
    const duckdb = (await import('duckdb')).default;
    const dotenv = await import('dotenv');
    dotenv.config();
    
    return new Promise((resolve, reject) => {
      const db = new duckdb.Database(':memory:', (err) => {
        if (err) return reject(new Error(`DUCKDB_INIT_FAILED: ${err.message}`));
        
        const conn = db.connect();
        
        // S3 setup if needed
        if (parquetPath.startsWith('s3://')) {
          const endpoint = (process.env.S3_COMPACT_ENDPOINT || process.env.S3_ENDPOINT || '').replace('https://', '');
          const accessKey = process.env.S3_COMPACT_ACCESS_KEY || process.env.S3_ACCESS_KEY;
          const secretKey = process.env.S3_COMPACT_SECRET_KEY || process.env.S3_SECRET_KEY;
          const region = process.env.S3_COMPACT_REGION || process.env.S3_REGION || 'us-east-1';
          
          const setupQueries = [
            "INSTALL httpfs", "LOAD httpfs",
            `SET s3_endpoint='${endpoint}'`,
            `SET s3_access_key_id='${accessKey}'`,
            `SET s3_secret_access_key='${secretKey}'`,
            `SET s3_region='${region}'`,
            "SET s3_url_style='path'",
            "SET s3_use_ssl=true"
          ];
          
          let completed = 0;
          for (const q of setupQueries) {
            conn.run(q, () => {
              completed++;
              if (completed === setupQueries.length) {
                runQuery();
              }
            });
          }
        } else {
          runQuery();
        }
        
        function runQuery() {
          // Simple query without seq column - ORDER BY ts_event only
          const sql = `SELECT * FROM read_parquet('${parquetPath}') ORDER BY ts_event ASC`;
          
          conn.all(sql, (err, rows) => {
            if (err) {
              db.close();
              return reject(new Error(`PARQUET_QUERY_FAILED: ${err.message}`));
            }
            
            if (rows.length === 0) {
              db.close();
              return reject(new Error(`No data found in parquet: ${parquetPath}`));
            }
            
            console.log(`[DatasetBuilder] Loaded ${rows.length} rows from parquet`);
            
            // Extract features and labels
            const featureNames = Object.keys(rows[0]).filter(k => k.startsWith('f_'));
            console.log(`[DatasetBuilder] Detected features: ${featureNames.join(', ')}`);
            
            const X = [];
            const y = [];
            
            for (const row of rows) {
              const xRow = featureNames.map(name => Number(row[name]));
              X.push(xRow);
              y.push(Number(row['label_dir_10s']));
            }
            
            db.close();
            
            resolve({
              X,
              y,
              meta: {
                featureNames,
                path: parquetPath
              }
            });
          });
        }
      });
    });
  }
}
