/**
 * Feature Extraction Worker
 * Calculates technical indicators from OHLCV data
 */

import duckdb from "duckdb";
import { S3Client, GetObjectCommand, PutObjectCommand, ListObjectsV2Command } from "@aws-sdk/client-s3";
import fs from "fs";
import path from "path";
import os from "os";

/**
 * Execute feature extraction job using DuckDB
 */
export async function executeFeatureWithDuckDB(job, s3Client, bucket, signal) {
  const { input_dataset, params, start_date, end_date, output_dataset } = job;
  const { symbols = [], feature_preset = "volatility_v1", window_param = 14 } = params;

  // Parse input dataset ID (must be curated OHLCV data)
  if (!input_dataset.endsWith("-curated")) {
    throw new Error(`Feature extraction requires a curated dataset. Got: ${input_dataset}. Expected format: exchange-datasetname-curated`);
  }

  const parts = input_dataset.split("-");
  if (parts.length < 3) {
    throw new Error(`Invalid input_dataset format: ${input_dataset}`);
  }

  const exchange = parts[0];
  const datasetName = parts.slice(1, -1).join("-"); // Everything between exchange and "curated"
  
  // Find curated dataset prefix
  const prefix = `curated/exchange=${exchange}/dataset=${datasetName}/`;
  job.log.push(`Scanning curated prefix: ${prefix}`);

  // List all keys
  const keys = await listAllParquetKeys(s3Client, bucket, prefix);
  if (keys.length === 0) {
    throw new Error(`No curated data found for: ${input_dataset}`);
  }

  job.log.push(`Found ${keys.length} curated files`);

  // Filter by symbols and dates
  let filteredKeys = filterKeys(keys, symbols, start_date, end_date);
  job.log.push(`Filtered to ${filteredKeys.length} files`);

  if (filteredKeys.length === 0) {
    throw new Error(`No data found for specified filters`);
  }

  if (signal?.aborted) throw new Error("AbortError");

  // Create temp directory
  const tempDir = fs.mkdtempSync(path.join(os.tmpdir(), "feature-"));
  job.log.push(`Using temp directory: ${tempDir}`);

  try {
    // Group by symbol and date
    const groupedKeys = groupKeysBySymbolAndDate(filteredKeys);

    for (const [groupKey, groupFiles] of Object.entries(groupedKeys)) {
      if (signal?.aborted) throw new Error("AbortError");

      const [symbol, date] = groupKey.split("|");
      job.log.push(`Processing features for ${symbol} on ${date}`);

      // Download files
      const localFiles = [];
      for (const s3Key of groupFiles) {
        const localPath = path.join(tempDir, path.basename(s3Key));
        await downloadFromS3(s3Client, bucket, s3Key, localPath);
        localFiles.push(localPath);
      }

      // Run feature extraction
      const outputPath = path.join(tempDir, `features_${symbol}_${date}.parquet`);
      await runFeatureExtraction(localFiles, outputPath, feature_preset, window_param);

      // Upload result
      const outputName = output_dataset || `features_${feature_preset}`;
      const s3OutputKey = `curated/exchange=${exchange}/dataset=${outputName}/symbol=${symbol}/date=${date}/part-0000.parquet`;
      await uploadToS3(s3Client, bucket, s3OutputKey, outputPath);

      job.log.push(`Wrote: ${s3OutputKey}`);

      // Cleanup
      localFiles.forEach((f) => fs.existsSync(f) && fs.unlinkSync(f));
      fs.existsSync(outputPath) && fs.unlinkSync(outputPath);
    }

    const outputName = output_dataset || `features_${feature_preset}`;
    job.output_dataset = `${exchange}-${outputName}-curated`;
    job.log.push(`Completed: ${job.output_dataset}`);

  } finally {
    fs.rmSync(tempDir, { recursive: true, force: true });
  }
}

/**
 * Run DuckDB feature extraction based on preset
 */
async function runFeatureExtraction(inputFiles, outputPath, preset, windowParam) {
  return new Promise((resolve, reject) => {
    const db = new duckdb.Database(":memory:");
    const conn = db.connect();

    const fileList = inputFiles.map((f) => `'${f}'`).join(", ");
    const w = windowParam;

    let featureSql;
    
    switch (preset) {
      case "volatility_v1":
        // ATR, Bollinger Bands, True Range
        featureSql = `
          SELECT 
            ts,
            open, high, low, close, volume,
            -- True Range
            GREATEST(high - low, ABS(high - LAG(close) OVER w), ABS(low - LAG(close) OVER w)) as true_range,
            -- ATR (Average True Range)
            AVG(GREATEST(high - low, ABS(high - LAG(close) OVER w), ABS(low - LAG(close) OVER w))) OVER (ORDER BY ts ROWS BETWEEN ${w-1} PRECEDING AND CURRENT ROW) as atr_${w},
            -- Bollinger Bands
            AVG(close) OVER (ORDER BY ts ROWS BETWEEN ${w-1} PRECEDING AND CURRENT ROW) as bb_middle_${w},
            AVG(close) OVER (ORDER BY ts ROWS BETWEEN ${w-1} PRECEDING AND CURRENT ROW) + 2 * STDDEV(close) OVER (ORDER BY ts ROWS BETWEEN ${w-1} PRECEDING AND CURRENT ROW) as bb_upper_${w},
            AVG(close) OVER (ORDER BY ts ROWS BETWEEN ${w-1} PRECEDING AND CURRENT ROW) - 2 * STDDEV(close) OVER (ORDER BY ts ROWS BETWEEN ${w-1} PRECEDING AND CURRENT ROW) as bb_lower_${w}
          FROM read_parquet([${fileList}])
          WINDOW w AS (ORDER BY ts)
          ORDER BY ts
        `;
        break;

      case "momentum_v1":
        // RSI, MACD components, simple momentum
        featureSql = `
          WITH price_changes AS (
            SELECT 
              ts, open, high, low, close, volume,
              close - LAG(close) OVER (ORDER BY ts) as price_change
            FROM read_parquet([${fileList}])
          ),
          gains_losses AS (
            SELECT *,
              CASE WHEN price_change > 0 THEN price_change ELSE 0 END as gain,
              CASE WHEN price_change < 0 THEN ABS(price_change) ELSE 0 END as loss
            FROM price_changes
          )
          SELECT 
            ts, open, high, low, close, volume,
            -- RSI
            100 - (100 / (1 + (AVG(gain) OVER (ORDER BY ts ROWS BETWEEN ${w-1} PRECEDING AND CURRENT ROW) / 
                   NULLIF(AVG(loss) OVER (ORDER BY ts ROWS BETWEEN ${w-1} PRECEDING AND CURRENT ROW), 0)))) as rsi_${w},
            -- MACD components (12, 26 EMA approximation using SMA)
            AVG(close) OVER (ORDER BY ts ROWS BETWEEN 11 PRECEDING AND CURRENT ROW) as ema_12,
            AVG(close) OVER (ORDER BY ts ROWS BETWEEN 25 PRECEDING AND CURRENT ROW) as ema_26,
            AVG(close) OVER (ORDER BY ts ROWS BETWEEN 11 PRECEDING AND CURRENT ROW) - AVG(close) OVER (ORDER BY ts ROWS BETWEEN 25 PRECEDING AND CURRENT ROW) as macd_line,
            -- Momentum
            close - LAG(close, ${w}) OVER (ORDER BY ts) as momentum_${w},
            -- Rate of Change
            (close - LAG(close, ${w}) OVER (ORDER BY ts)) / NULLIF(LAG(close, ${w}) OVER (ORDER BY ts), 0) * 100 as roc_${w}
          FROM gains_losses
          ORDER BY ts
        `;
        break;

      case "microstructure":
        // Spread, VWAP, trade metrics (simplified for tick data)
        featureSql = `
          SELECT 
            ts, open, high, low, close, volume,
            -- High-Low Spread proxy
            (high - low) / NULLIF((high + low) / 2, 0) as hl_spread,
            -- VWAP
            SUM(close * volume) OVER (ORDER BY ts) / NULLIF(SUM(volume) OVER (ORDER BY ts), 0) as vwap,
            -- Volume-weighted price
            close * volume as dollar_volume,
            -- Rolling VWAP
            SUM(close * volume) OVER (ORDER BY ts ROWS BETWEEN ${w-1} PRECEDING AND CURRENT ROW) / 
              NULLIF(SUM(volume) OVER (ORDER BY ts ROWS BETWEEN ${w-1} PRECEDING AND CURRENT ROW), 0) as vwap_${w},
            -- Volume imbalance (requires consecutive comparison)
            volume - LAG(volume) OVER (ORDER BY ts) as volume_delta
          FROM read_parquet([${fileList}])
          ORDER BY ts
        `;
        break;

      default:
        throw new Error(`Unknown feature preset: ${preset}`);
    }

    const sql = `COPY (${featureSql}) TO '${outputPath}' (FORMAT PARQUET)`;

    conn.run(sql, (err) => {
      conn.close();
      db.close();
      if (err) reject(err);
      else resolve();
    });
  });
}

// Helper functions
async function listAllParquetKeys(s3Client, bucket, prefix) {
  const keys = [];
  let token;
  while (true) {
    const cmd = new ListObjectsV2Command({ Bucket: bucket, Prefix: prefix, ContinuationToken: token });
    const res = await s3Client.send(cmd);
    if (res.Contents) {
      res.Contents.forEach((c) => {
        if (c.Key?.endsWith(".parquet") && !c.Key.includes("/._")) keys.push(c.Key);
      });
    }
    if (!res.IsTruncated) break;
    token = res.NextContinuationToken;
  }
  return keys;
}

function filterKeys(keys, symbols, startDate, endDate) {
  let filtered = keys;
  if (symbols.length > 0) {
    const symbolSet = new Set(symbols.map((s) => s.toLowerCase()));
    filtered = filtered.filter((key) => {
      const match = key.match(/symbol=([^/]+)/);
      return match ? symbolSet.has(match[1].toLowerCase()) : false;
    });
  }
  if (startDate || endDate) {
    filtered = filtered.filter((key) => {
      const dateMatch = key.match(/date=(\d{8})/);
      if (!dateMatch) return true;
      const keyDate = dateMatch[1];
      if (startDate && keyDate < startDate) return false;
      if (endDate && keyDate > endDate) return false;
      return true;
    });
  }
  return filtered;
}

function groupKeysBySymbolAndDate(keys) {
  const groups = {};
  for (const key of keys) {
    const symbolMatch = key.match(/symbol=([^/]+)/);
    const dateMatch = key.match(/date=(\d{8})/);
    if (symbolMatch && dateMatch) {
      const groupKey = `${symbolMatch[1]}|${dateMatch[1]}`;
      if (!groups[groupKey]) groups[groupKey] = [];
      groups[groupKey].push(key);
    }
  }
  return groups;
}

async function downloadFromS3(s3Client, bucket, key, localPath) {
  const cmd = new GetObjectCommand({ Bucket: bucket, Key: key });
  const res = await s3Client.send(cmd);
  const writeStream = fs.createWriteStream(localPath);
  await new Promise((resolve, reject) => {
    res.Body.pipe(writeStream);
    res.Body.on("error", reject);
    writeStream.on("finish", resolve);
  });
}

async function uploadToS3(s3Client, bucket, key, localPath) {
  const fileContent = fs.readFileSync(localPath);
  await s3Client.send(new PutObjectCommand({
    Bucket: bucket, Key: key, Body: fileContent, ContentType: "application/octet-stream"
  }));
}
