/**
 * DuckDB-based Resample Worker - Download-Then-Process with Staged Upload
 * Downloads files from S3 first, then processes locally with DuckDB
 * Uses staging prefix to prevent partial uploads on failed jobs
 */

import duckdb from "duckdb";
import { S3Client, PutObjectCommand, ListObjectsV2Command, GetObjectCommand, CopyObjectCommand, DeleteObjectCommand } from "@aws-sdk/client-s3";
import fs from "fs";
import path from "path";
import os from "os";

const MAX_PARALLEL_DOWNLOADS = 100;

/**
 * Execute OHLCV resample job using download-then-process approach with staged upload
 */
export async function executeResampleWithDuckDB(job, s3Client, bucket, signal) {
  const { input_dataset, params, start_date, end_date, output_dataset } = job;
  const { symbols = [], timeframe = "1m", aggregation_type = "OHLCV" } = params;

  // Timeframe to milliseconds mapping for validation
  const TIMEFRAME_MS = {
    "1s": 1000, "5s": 5000, "10s": 10000, "30s": 30000,
    "1m": 60000, "5m": 300000, "15m": 900000, "30m": 1800000,
    "1h": 3600000, "4h": 14400000, "1d": 86400000,
  };

  // Parse input dataset ID: exchange-stream-version
  // Special handling for BINANCE MASTER STORE
  let exchange, stream, version;
  
  if (input_dataset === "BINANCE MASTER STORE") {
    exchange = "binance";
    stream = "tick";
    version = "v3-ready";
  } else if (input_dataset.endsWith("-curated")) {
    const parts = input_dataset.split("-");
    exchange = parts[0];
    version = "curated";
    stream = parts.slice(1, parts.length - 1).join("-");
  } else {
    const parts = input_dataset.split("-");
    if (parts.length < 3) {
      throw new Error(`Invalid input_dataset format: ${input_dataset}`);
    }
    exchange = parts[0];
    stream = parts[1];
    version = parts.slice(2).join("-");
  }
  const outputName = output_dataset || `${stream}_${timeframe}`;
  
  // TIMEFRAME VALIDATION FOR CURATED SOURCES
  // You can only downsample (not upsample) - e.g., raw→1m OK, 1m→5m OK, but 1m→1s NOT OK
  if (version === "curated") {
    job.log.push(`Source is curated dataset, checking timeframe compatibility...`);
    
    // Try to read source dataset metadata
    const sourceMetadataKey = `curated/exchange=${exchange}/dataset=${stream}/_metadata.json`;
    try {
      const sourceMetadata = await fetchSourceMetadata(s3Client, bucket, sourceMetadataKey);
      if (sourceMetadata && sourceMetadata.timeframe) {
        const sourceMs = TIMEFRAME_MS[sourceMetadata.timeframe];
        const targetMs = TIMEFRAME_MS[timeframe];
        
        if (sourceMs && targetMs) {
          if (targetMs < sourceMs) {
            throw new Error(
              `Cannot upsample: source timeframe is ${sourceMetadata.timeframe}, ` +
              `requested ${timeframe}. You can only downsample (e.g., 1m→5m) not upsample (e.g., 1m→1s).`
            );
          }

          // REDUNDANCY CHECK: Block 1m -> 1m unless it's a subset
          if (targetMs === sourceMs) {
            const sourceSymbols = sourceMetadata.symbols || [];
            const targetSymbolSet = new Set(symbols);

            // Check if target is a subset of source symbols (i.e., we are filtering)
            // If there is AT LEAST ONE source symbol that is NOT in target, it is a subset.
            const isSymbolSubset = sourceSymbols.length > 0 && sourceSymbols.some(s => !targetSymbolSet.has(s));

            // Check if target is a subset of date range
            const sStart = sourceMetadata.date_range?.start;
            const sEnd = sourceMetadata.date_range?.end;
            
            // It is a date subset if target start is later than source start OR target end is earlier than source end
            const isDateSubset = (start_date && sStart && start_date > sStart) || 
                                 (end_date && sEnd && end_date < sEnd);

            // If neither symbol subset nor date subset, it is redundant (identical or swallower)
            // UNLESS the aggregation type is different (e.g. OHLCV -> Tick Resample)
            const sourceAgg = sourceMetadata.aggregation || "OHLCV";
            const isDifferentAggregation = aggregation_type !== sourceAgg;

            if (!isSymbolSubset && !isDateSubset && !isDifferentAggregation) {
              throw new Error(
                `Redundant operation: Target dataset would be identical to source (${timeframe} → ${timeframe}). ` +
                `To create a subset, specify fewer symbols or a narrower date range, or change the aggregation type.`
              );
            }
            
            job.log.push(`Redundancy check passed: Subset=${isSymbolSubset || isDateSubset}, DifferentAgg=${isDifferentAggregation}`);
          }
        }
        job.log.push(`Timeframe OK: ${sourceMetadata.timeframe} → ${timeframe}`);
      }
    } catch (err) {
      if (err.message.includes("Cannot upsample") || err.message.includes("Redundant operation")) {
        throw err; // Re-throw validation error
      }
      job.log.push(`Note: Could not read source metadata for timeframe validation. Error: ${err.message}`);
      console.error("Metadata read error:", err);
    }
  }
  
  // Staging prefix for atomic uploads
  const stagingPrefix = `staging/${job.job_id}/`;
  const uploadedStagingKeys = []; // Track uploaded files for cleanup/move

  job.log.push(`Using staged upload (atomic)`);

  // Create temp directory
  const tempDir = fs.mkdtempSync(path.join(os.tmpdir(), "resample-"));
  job.log.push(`Temp directory: ${tempDir}`);

  try {
    const symbolList = symbols.length > 0 ? symbols : [];
    
    if (symbolList.length === 0) {
      throw new Error("No symbols provided for processing");
    }

    job.log.push(`Processing ${symbolList.length} symbols: ${symbolList.join(", ")}`);

    for (const symbol of symbolList) {
      if (signal?.aborted) throw new Error("AbortError");

      // Build S3 prefix based on version
      let s3Prefix;
      if (version === "v3") {
        s3Prefix = `v3/exchange=${exchange}/stream=${stream}/symbol=${symbol}/`;
        if (start_date && end_date && start_date === end_date) {
          s3Prefix += `date=${start_date}/`;
        }
      } else if (version === "v2") {
        s3Prefix = `v2/exchange=${exchange}/stream=${stream}/symbol=${symbol}/`;
        if (start_date && end_date && start_date === end_date) {
          s3Prefix += `date=${start_date}/`;
        }
      } else if (version === "legacy" || version === "v3-ready" || version === "v2-nodepth") {
        s3Prefix = `${version}/exchange=${exchange}/stream=${stream}/${symbol}/`;
        if (start_date && end_date && start_date === end_date) {
          s3Prefix += `${start_date}/`;
        }
      } else if (version === "curated") {
        // Curated datasets have structure: curated/exchange=X/dataset=Y/symbol=Z/
        s3Prefix = `curated/exchange=${exchange}/dataset=${stream}/symbol=${symbol}/`;
        // No date filtering for curated - files are already aggregated
      } else {
        throw new Error(`Unknown version: ${version}`);
      }

      // List and filter files
      job.log.push(`Listing files from: ${s3Prefix}`);
      const filteredFiles = await listAndFilterParquetFiles(s3Client, bucket, s3Prefix, start_date, end_date);
      
      if (filteredFiles.length === 0) {
        job.log.push(`No valid files found for ${symbol}`);
        continue;
      }

      job.log.push(`Found ${filteredFiles.length} files for ${symbol}`);

      // Download files in parallel
      const downloadDir = path.join(tempDir, symbol);
      fs.mkdirSync(downloadDir, { recursive: true });
      
      const downloadStart = Date.now();
      const localFiles = await downloadFilesParallel(s3Client, bucket, filteredFiles, downloadDir, signal);
      job.log.push(`Downloaded ${localFiles.length} files in ${Date.now() - downloadStart}ms`);

      if (localFiles.length === 0) {
        job.log.push(`No files downloaded for ${symbol}`);
        continue;
      }

      // Run DuckDB aggregation on local files
      const outputPath = path.join(tempDir, `${symbol}_${timeframe}.parquet`);
      const aggStart = Date.now();
      
      await runDuckDBAggregation({
        localFiles,
        outputPath,
        timeframe,
        aggregationType: aggregation_type,
        isCurated: version === "curated",
      });

      job.log.push(`Aggregation completed in ${Date.now() - aggStart}ms`);

      // Check output
      if (!fs.existsSync(outputPath)) {
        job.log.push(`Warning: No output for ${symbol}`);
        continue;
      }

      // Upload to STAGING first (not curated)
      const stagingKey = `${stagingPrefix}exchange=${exchange}/dataset=${outputName}/symbol=${symbol}/part-0000.parquet`;
      await uploadToS3(s3Client, bucket, stagingKey, outputPath);
      uploadedStagingKeys.push(stagingKey);
      job.log.push(`Staged: ${stagingKey}`);

      // Cleanup downloaded files
      fs.rmSync(downloadDir, { recursive: true, force: true });
      fs.unlinkSync(outputPath);
    }

    // ALL SYMBOLS SUCCESSFUL - Move from staging to curated
    job.log.push(`Moving ${uploadedStagingKeys.length} files from staging to curated...`);
    
    for (const stagingKey of uploadedStagingKeys) {
      const curatedKey = stagingKey.replace(stagingPrefix, "curated/");
      await moveS3Object(s3Client, bucket, stagingKey, curatedKey);
    }
    
    job.log.push(`Moved all files to curated`);

    // Write metadata file with config info
    // Determine source type from input dataset
    const sourceType = input_dataset.endsWith("-curated") ? "curated" : "raw";
    
    // Create config hash for deduplication
    const configForHash = { input_dataset, timeframe, aggregation_type, symbols: symbolList.sort() };
    const configHash = Buffer.from(JSON.stringify(configForHash)).toString("base64").slice(0, 16);
    
    const metadata = {
      // Required fields
      source_type: sourceType,
      parent_dataset_ids: [input_dataset],
      job_type: "resample",
      aggregation: aggregation_type,
      timeframe: timeframe,
      feature_presets: null, // Not applicable for resample jobs
      date_range: { start: start_date, end: end_date },
      symbols: symbolList,
      exchange: exchange,
      created_at: new Date().toISOString(),
      config_hash: configHash,
      
      // Additional useful fields
      job_id: job.job_id,
      input_dataset: input_dataset, // Keep for backward compat
    };
    
    const metadataKey = `curated/exchange=${exchange}/dataset=${outputName}/_metadata.json`;
    await uploadMetadataToS3(s3Client, bucket, metadataKey, metadata);
    job.log.push(`Wrote metadata: ${metadataKey}`);

    // Set output dataset name
    job.output_dataset = `${exchange}-${outputName}-curated`;
    job.log.push(`Completed: ${job.output_dataset}`);

  } catch (err) {
    // CLEANUP STAGING on error
    if (uploadedStagingKeys.length > 0) {
      job.log.push(`Cleaning up ${uploadedStagingKeys.length} staged files due to error...`);
      for (const key of uploadedStagingKeys) {
        try {
          await deleteS3Object(s3Client, bucket, key);
        } catch (e) {
          // Ignore cleanup errors
        }
      }
      job.log.push(`Staging cleaned up`);
    }
    throw err; // Re-throw to mark job as failed
    
  } finally {
    fs.rmSync(tempDir, { recursive: true, force: true });
  }
}

/**
 * List and filter parquet files using S3 API
 */
async function listAndFilterParquetFiles(s3Client, bucket, prefix, startDate, endDate) {
  const validFiles = [];
  let token;

  while (true) {
    const cmd = new ListObjectsV2Command({
      Bucket: bucket,
      Prefix: prefix,
      ContinuationToken: token,
    });

    const res = await s3Client.send(cmd);

    if (res.Contents) {
      for (const obj of res.Contents) {
        const key = obj.Key;
        
        if (!key.endsWith(".parquet")) continue;
        
        // Skip hidden ._ files (MacOS metadata)
        if (key.includes("/._")) continue;
        const filename = key.split("/").pop();
        if (filename.startsWith("._")) continue;
        
        if (startDate || endDate) {
          // Extract date from path - supports both date=YYYYMMDD and /YYYYMMDD/ formats
          const dateMatch = key.match(/(?:date=|\/)(\d{8})(?:\/|$)/);
          if (dateMatch) {
            const fileDate = dateMatch[1];
            if (startDate && fileDate < startDate) continue;
            if (endDate && fileDate > endDate) continue;
          }
        }
        
        validFiles.push(key);
      }
    }

    if (!res.IsTruncated) break;
    token = res.NextContinuationToken;
  }

  return validFiles;
}

/**
 * Download files from S3 in parallel
 */
async function downloadFilesParallel(s3Client, bucket, keys, downloadDir, signal) {
  const localFiles = [];
  
  for (let i = 0; i < keys.length; i += MAX_PARALLEL_DOWNLOADS) {
    if (signal?.aborted) throw new Error("AbortError");
    
    const batch = keys.slice(i, i + MAX_PARALLEL_DOWNLOADS);
    const downloads = batch.map(async (key, idx) => {
      const filename = `${i + idx}_${path.basename(key)}`;
      const localPath = path.join(downloadDir, filename);
      
      try {
        const cmd = new GetObjectCommand({ Bucket: bucket, Key: key });
        const res = await s3Client.send(cmd);
        
        const chunks = [];
        for await (const chunk of res.Body) {
          chunks.push(chunk);
        }
        
        fs.writeFileSync(localPath, Buffer.concat(chunks));
        return localPath;
      } catch (err) {
        console.error(`Failed to download ${key}:`, err.message);
        return null;
      }
    });
    
    const results = await Promise.all(downloads);
    localFiles.push(...results.filter(Boolean));
  }
  
  return localFiles;
}

/**
 * Run DuckDB aggregation on local files
 */
async function runDuckDBAggregation(options) {
  const { localFiles, outputPath, timeframe, aggregationType, isCurated } = options;

  return new Promise((resolve, reject) => {
    const db = new duckdb.Database(":memory:");
    const conn = db.connect();

    const sql = buildAggregationSql({
      localFiles,
      outputPath,
      timeframe,
      aggregationType,
      isCurated,
    });

    conn.run(sql, (err) => {
      conn.close();
      db.close();
      if (err) {
        reject(err);
      } else {
        resolve();
      }
    });
  });
}

/**
 * Build DuckDB SQL for aggregation - OPTIMIZED with arg_min/arg_max
 */
function buildAggregationSql(options) {
  const { localFiles, outputPath, timeframe, aggregationType, isCurated } = options;

  const truncMap = {
    "1s": "second", "5s": "second", "10s": "second",
    "1m": "minute", "5m": "minute", "15m": "minute",
    "1h": "hour", "4h": "hour", "1d": "day",
  };
  const truncPrecision = truncMap[timeframe] || "minute";

  const useIntegerBucket = ["5s", "10s", "5m", "15m", "4h"].includes(timeframe);
  const bucketMs = { "5s": 5000, "10s": 10000, "5m": 300000, "15m": 900000, "4h": 14400000 }[timeframe];

  let bucketExpr;
  if (isCurated) {
    // For curated sources, the time column is already 'bucket' (timestamp)
    // We need to re-bucket based on the existing 'bucket' column
    if (useIntegerBucket) {
      bucketExpr = `to_timestamp((epoch_ms(bucket) / ${bucketMs})::BIGINT * ${bucketMs} / 1000)`;
    } else {
      bucketExpr = `date_trunc('${truncPrecision}', bucket)`;
    }
  } else {
    // For raw sources, use 'ts' (milliseconds epoch)
    if (useIntegerBucket) {
      bucketExpr = `to_timestamp((ts / ${bucketMs})::BIGINT * ${bucketMs} / 1000)`;
    } else {
      bucketExpr = `date_trunc('${truncPrecision}', epoch_ms(ts))`;
    }
  }

  let selectClause;
  
  if (isCurated && aggregationType === "OHLCV") {
    // Merging logic for already aggregated OHLCV data
    selectClause = `
      ${bucketExpr} as bucket,
      arg_min(open, bucket) as open,
      max(high) as high,
      min(low) as low,
      arg_max(close, bucket) as close,
      sum(volume) as volume,
      sum(trade_count) as trade_count
    `;
  } else {
    // Standard aggregation from raw data
    switch (aggregationType) {
      case "OHLCV":
      default:
        selectClause = `
          ${bucketExpr} as bucket,
          arg_min(mid, ts) as open,
          max(mid) as high,
          min(mid) as low,
          arg_max(mid, ts) as close,
          sum(COALESCE(bid_qty, 0) + COALESCE(ask_qty, 0)) as volume,
          count(*) as trade_count
        `;
        break;

      case "tick-resample":
        selectClause = `
          ${bucketExpr} as bucket,
          arg_max(mid, ts) as price,
          avg(mid) as avg_price,
          stddev(mid) as price_stddev,
          count(*) as tick_count
        `;
        break;

      case "orderbook-spread":
        selectClause = `
          ${bucketExpr} as bucket,
          avg(spread) as avg_spread,
          min(spread) as min_spread,
          max(spread) as max_spread,
          avg(spread_pct) * 100 as spread_bps
        `;
        break;

      case "trade-imbalance":
        selectClause = `
          ${bucketExpr} as bucket,
          sum(COALESCE(bid_qty, 0)) as bid_volume,
          sum(COALESCE(ask_qty, 0)) as ask_volume,
          sum(COALESCE(bid_qty, 0)) - sum(COALESCE(ask_qty, 0)) as imbalance,
          count(*) as tick_count
        `;
        break;
    }
  }

  const fileList = localFiles.map(f => `'${f}'`).join(", ");

  return `
    COPY (
      SELECT ${selectClause}
      FROM read_parquet([${fileList}])
      GROUP BY 1
      ORDER BY 1
    ) TO '${outputPath}' (FORMAT PARQUET)
  `;
}

/**
 * Upload file to S3
 */
async function uploadToS3(s3Client, bucket, key, localPath) {
  const fileContent = fs.readFileSync(localPath);
  const cmd = new PutObjectCommand({
    Bucket: bucket,
    Key: key,
    Body: fileContent,
    ContentType: "application/octet-stream",
  });
  await s3Client.send(cmd);
}

/**
 * Move S3 object from source to destination (copy + delete)
 */
async function moveS3Object(s3Client, bucket, sourceKey, destKey) {
  // Copy
  await s3Client.send(new CopyObjectCommand({
    Bucket: bucket,
    CopySource: `${bucket}/${sourceKey}`,
    Key: destKey,
  }));
  
  // Delete source
  await s3Client.send(new DeleteObjectCommand({
    Bucket: bucket,
    Key: sourceKey,
  }));
}

/**
 * Delete S3 object
 */
async function deleteS3Object(s3Client, bucket, key) {
  await s3Client.send(new DeleteObjectCommand({
    Bucket: bucket,
    Key: key,
  }));
}

/**
 * Upload metadata JSON to S3
 */
async function uploadMetadataToS3(s3Client, bucket, key, metadata) {
  const cmd = new PutObjectCommand({
    Bucket: bucket,
    Key: key,
    Body: JSON.stringify(metadata, null, 2),
    ContentType: "application/json",
  });
  await s3Client.send(cmd);
}

/**
 * Fetch source dataset metadata for validation
 */
async function fetchSourceMetadata(s3Client, bucket, key) {
  try {
    const cmd = new GetObjectCommand({ Bucket: bucket, Key: key });
    const res = await s3Client.send(cmd);
    
    const chunks = [];
    for await (const chunk of res.Body) {
      chunks.push(chunk);
    }
    
    const body = Buffer.concat(chunks).toString("utf-8");
    return JSON.parse(body);
  } catch (err) {
    return null;
  }
}
