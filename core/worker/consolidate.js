/**
 * Consolidate Worker
 * Converts V2 partitioned parquet files to legacy format (single daily parquet)
 * Called via API job system
 */

import duckdb from "duckdb";
import { ListObjectsV2Command, GetObjectCommand, PutObjectCommand, DeleteObjectsCommand } from "@aws-sdk/client-s3";
import fs from "fs";
import path from "path";
import os from "os";

const MAX_PARALLEL_DOWNLOADS = 50;

/**
 * Execute consolidate job - V2 to Legacy migration
 */
export async function executeConsolidateJob(job, s3Client, bucket, signal) {
  const { input_dataset, params, start_date, end_date } = job;
  const { 
    symbols = [], 
    delete_source = false,
    overwrite = false,
    target_dataset = "v3-ready" // Default target
  } = params;

  // Parse input dataset: exchange-stream-v3
  const parts = input_dataset.split("-");
  if (parts.length < 3 || !input_dataset.endsWith("-v3")) {
    throw new Error(`Invalid input_dataset format. Expected: exchange-stream-v3, got: ${input_dataset}`);
  }

  const exchange = parts[0];
  const stream = parts.slice(1, -1).join("-"); // Everything between exchange and v2

  job.log.push(`Consolidating V3 → ${target_dataset} for ${exchange}/${stream}`);
  job.log.push(`Symbols filter: ${symbols.length > 0 ? symbols.join(", ") : "all"}`);
  job.log.push(`Date range: ${start_date || "start"} - ${end_date || "end"}`);
  job.log.push(`Delete source: ${delete_source}`);
  job.log.push(`Overwrite: ${overwrite}`);

  // Get today's date - skip it (collector still writing)
  const today = new Date();
  const todayStr = `${today.getFullYear()}${String(today.getMonth() + 1).padStart(2, '0')}${String(today.getDate()).padStart(2, '0')}`;
  job.log.push(`Skipping today (${todayStr}) - collector active`);

  // Step 1: List V3 files
  job.log.push("Step 1: Scanning V3 files...");
  const files = await listV3Files(s3Client, bucket, exchange, stream, symbols, start_date, end_date, todayStr, signal);
  
  if (files.length === 0) {
    job.log.push("No V3 files found matching criteria");
    job.output_dataset = input_dataset;
    return;
  }

  job.log.push(`Found ${files.length} parquet files`);

  // Step 2: Group by symbol/date
  const groups = groupFilesBySymbolDate(files);
  job.log.push(`Found ${groups.length} symbol/date combinations`);

  // Step 3: Process each group
  const stats = {
    processed: 0,
    skipped: 0,
    failed: 0,
    deletedFiles: 0,
  };

  const tempDir = fs.mkdtempSync(path.join(os.tmpdir(), "consolidate-"));

  try {
    for (const group of groups) {
      if (signal?.aborted) throw new Error("AbortError");

      const { symbol, date, files: sourceKeys, totalSize } = group;
      
      // Determine target key based on dataset type
      let targetKey;
      if (target_dataset === "v2-nodepth" || target_dataset === "v3-ready" || target_dataset === "legacy") {
        targetKey = `${target_dataset}/exchange=${exchange}/stream=${stream}/${symbol}/${date}/data.parquet`;
      } else {
        throw new Error(`Unknown target_dataset: ${target_dataset}`);
      }

      // Check if legacy exists
      if (!overwrite) {
        const exists = await targetFileExists(s3Client, bucket, targetKey);
        if (exists) {
          job.log.push(`SKIP ${symbol}/${date} (exists)`);
          stats.skipped++;
          continue;
        }
      }

      try {
        job.log.push(`Processing ${symbol}/${date}: ${sourceKeys.length} files (${formatBytes(totalSize)})`);

        // Create temp dir for this group
        const groupDir = path.join(tempDir, `${symbol}_${date}`);
        fs.mkdirSync(groupDir, { recursive: true });

        // Download files
        const localFiles = await downloadFiles(s3Client, bucket, sourceKeys, groupDir, signal);
        if (localFiles.length === 0) {
          job.log.push(`FAILED ${symbol}/${date} (download)`);
          stats.failed++;
          continue;
        }

        // Merge with DuckDB
        const outputPath = path.join(tempDir, `${symbol}_${date}.parquet`);
        await mergeParquetFiles(localFiles, outputPath);

        // Get output size
        const outputSize = fs.statSync(outputPath).size;

        // Upload to target
        await uploadToS3(s3Client, bucket, outputPath, targetKey);

        job.log.push(`OK ${symbol}/${date} → ${formatBytes(outputSize)}`);
        stats.processed++;

        // Delete source files if requested
        if (delete_source) {
          await deleteV3Files(s3Client, bucket, sourceKeys);
          stats.deletedFiles += sourceKeys.length;
          job.log.push(`Deleted ${sourceKeys.length} V3 files`);
        }

        // Cleanup temp files
        fs.rmSync(groupDir, { recursive: true, force: true });
        fs.unlinkSync(outputPath);

      } catch (err) {
        job.log.push(`FAILED ${symbol}/${date}: ${err.message}`);
        stats.failed++;
      }
    }
  } finally {
    fs.rmSync(tempDir, { recursive: true, force: true });
  }

  // Summary
  job.log.push(`--- Summary ---`);
  job.log.push(`Processed: ${stats.processed}`);
  job.log.push(`Skipped: ${stats.skipped}`);
  job.log.push(`Failed: ${stats.failed}`);
  if (delete_source) {
    job.log.push(`Deleted V3 files: ${stats.deletedFiles}`);
  }

  job.output_dataset = `${exchange}-${stream}-${target_dataset}`;
}

// Helper: List V3 files
async function listV3Files(s3Client, bucket, exchange, stream, symbols, startDate, endDate, todayStr, signal) {
  const prefix = `v3/exchange=${exchange}/stream=${stream}/`;
  const files = [];
  let token;

  const symbolSet = new Set(symbols.map(s => s.toLowerCase()));

  while (true) {
    if (signal?.aborted) throw new Error("AbortError");

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
        if (key.includes("/._") || path.basename(key).startsWith("._")) continue;

        const symbolMatch = key.match(/symbol=([^/]+)/);
        const dateMatch = key.match(/date=(\d{8})/);

        if (!symbolMatch || !dateMatch) continue;

        const symbol = symbolMatch[1];
        const date = dateMatch[1];

        // Skip today
        if (date === todayStr) continue;

        // Apply filters
        if (symbolSet.size > 0 && !symbolSet.has(symbol.toLowerCase())) continue;
        if (startDate && date < startDate) continue;
        if (endDate && date > endDate) continue;

        files.push({ key, symbol, date, size: obj.Size });
      }
    }

    if (!res.IsTruncated) break;
    token = res.NextContinuationToken;
  }

  return files;
}

// Helper: Group files by symbol/date
function groupFilesBySymbolDate(files) {
  const groups = {};

  for (const file of files) {
    const key = `${file.symbol}/${file.date}`;
    if (!groups[key]) {
      groups[key] = {
        symbol: file.symbol,
        date: file.date,
        files: [],
        totalSize: 0,
      };
    }
    groups[key].files.push(file.key);
    groups[key].totalSize += file.size;
  }

  return Object.values(groups);
}

// Helper: Check if target file exists
async function targetFileExists(s3Client, bucket, key) {
  const cmd = new ListObjectsV2Command({
    Bucket: bucket,
    Prefix: key,
    MaxKeys: 1,
  });

  const res = await s3Client.send(cmd);
  return res.Contents && res.Contents.length > 0;
}

// Helper: Download files
async function downloadFiles(s3Client, bucket, keys, downloadDir, signal) {
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
        return null;
      }
    });

    const results = await Promise.all(downloads);
    localFiles.push(...results.filter(Boolean));
  }

  return localFiles;
}

// Helper: Merge parquet files with DuckDB
async function mergeParquetFiles(localFiles, outputPath) {
  return new Promise((resolve, reject) => {
    const db = new duckdb.Database(":memory:");
    const conn = db.connect();

    const fileList = localFiles.map(f => `'${f}'`).join(", ");

    const sql = `
      COPY (
        SELECT *
        FROM read_parquet([${fileList}])
        ORDER BY ts
      ) TO '${outputPath}' (FORMAT PARQUET, COMPRESSION 'ZSTD')
    `;

    conn.run(sql, (err) => {
      conn.close();
      db.close();
      if (err) reject(err);
      else resolve();
    });
  });
}

// Helper: Upload to S3
async function uploadToS3(s3Client, bucket, localPath, s3Key) {
  const fileContent = fs.readFileSync(localPath);
  const cmd = new PutObjectCommand({
    Bucket: bucket,
    Key: s3Key,
    Body: fileContent,
    ContentType: "application/octet-stream",
  });
  await s3Client.send(cmd);
}

// Helper: Delete V3 files
async function deleteV3Files(s3Client, bucket, keys) {
  for (let i = 0; i < keys.length; i += 1000) {
    const batch = keys.slice(i, i + 1000);
    const cmd = new DeleteObjectsCommand({
      Bucket: bucket,
      Delete: { Objects: batch.map(key => ({ Key: key })) },
    });
    await s3Client.send(cmd);
  }
}

// Helper: Format bytes
function formatBytes(bytes) {
  if (bytes < 1024) return bytes + " B";
  if (bytes < 1024 * 1024) return (bytes / 1024).toFixed(1) + " KB";
  if (bytes < 1024 * 1024 * 1024) return (bytes / (1024 * 1024)).toFixed(1) + " MB";
  return (bytes / (1024 * 1024 * 1024)).toFixed(2) + " GB";
}
