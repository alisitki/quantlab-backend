/**
 * V3 Migration Script
 * 
 * Converts V3 partitioned parquet files to consolidated format
 * Supports multiple targets: v3-ready (default), v2-nodepth, legacy
 * 
 * Usage:
 *   node worker/migrate_v3.js --exchange binance --stream trades [options]
 * 
 * Options:
 *   --exchange      Required. Exchange name (e.g., binance)
 *   --stream        Required. Stream name (e.g., trades)
 *   --target        Optional. Target dataset (v3-ready, v2-nodepth, legacy). Default: v3-ready
 *   --symbol        Optional. Filter by symbol (e.g., BTCUSDT)
 *   --start-date    Optional. Start date YYYYMMDD
 *   --end-date      Optional. End date YYYYMMDD
 *   --dry-run       Optional. Show what would be done without doing it
 *   --delete-source Optional. Delete V3 files after successful migration
 *   --overwrite     Optional. Overwrite existing target files (default: skip)
 *   --source-prefix Optional. Source prefix (default: v3/)
 */

import duckdb from "duckdb";
import { S3Client, ListObjectsV2Command, GetObjectCommand, PutObjectCommand, DeleteObjectsCommand } from "@aws-sdk/client-s3";
import fs from "fs";
import path from "path";
import os from "os";
import dotenv from "dotenv";

dotenv.config();

// Configuration
const BUCKET = process.env.S3_BUCKET;
const MAX_PARALLEL_DOWNLOADS = 50;

// S3 Client (matches index.js config)
const s3 = new S3Client({
  endpoint: process.env.S3_ENDPOINT,
  region: "us-east-1",
  credentials: {
    accessKeyId: process.env.S3_ACCESS_KEY,
    secretAccessKey: process.env.S3_SECRET_KEY,
  },
  forcePathStyle: true,
});

// Parse command line arguments
function parseArgs() {
  const args = process.argv.slice(2);
  const config = {
    exchange: null,
    stream: null,
    target: "v3-ready",
    symbol: null,
    startDate: null,
    endDate: null,
    dryRun: false,
    deleteSource: false,
    overwrite: false,
    sourcePrefix: "v3/"
  };

  for (let i = 0; i < args.length; i++) {
    switch (args[i]) {
      case "--exchange":
        config.exchange = args[++i];
        break;
      case "--stream":
        config.stream = args[++i];
        break;
      case "--target":
        config.target = args[++i];
        break;
      case "--symbol":
        config.symbol = args[++i];
        break;
      case "--start-date":
        config.startDate = args[++i];
        break;
      case "--end-date":
        config.endDate = args[++i];
        break;
      case "--dry-run":
        config.dryRun = true;
        break;
      case "--delete-source":
        config.deleteSource = true;
        break;
      case "--overwrite":
        config.overwrite = true;
        break;
      case "--source-prefix":
        config.sourcePrefix = args[++i];
        break;
    }
  }

  return config;
}

// List all files for a given exchange/stream
async function listFiles(config) {
  const { exchange, stream, symbol, startDate, endDate, sourcePrefix } = config;
  const prefix = `${sourcePrefix}exchange=${exchange}/stream=${stream}/`;
  const files = [];
  let token;

  // Get today's date in YYYYMMDD format - SKIP today (collector still writing)
  const today = new Date();
  const todayStr = `${today.getFullYear()}${String(today.getMonth() + 1).padStart(2, '0')}${String(today.getDate()).padStart(2, '0')}`;
  console.log(`📂 Scanning: ${prefix}`);
  console.log(`⚠️  Skipping today (${todayStr}) - collector still active`);

  while (true) {
    const cmd = new ListObjectsV2Command({
      Bucket: BUCKET,
      Prefix: prefix,
      ContinuationToken: token,
    });

    const res = await s3.send(cmd);

    if (res.Contents) {
      for (const obj of res.Contents) {
        const key = obj.Key;

        // Skip non-parquet files
        if (!key.endsWith(".parquet")) continue;

        // Skip hidden files
        if (key.includes("/._") || path.basename(key).startsWith("._")) continue;

        // Parse key structure: prefix/exchange=X/stream=Y/symbol=Z/date=YYYYMMDD/file.parquet
        const symbolMatch = key.match(/symbol=([^/]+)/);
        const dateMatch = key.match(/date=(\d{8})/);

        if (!symbolMatch || !dateMatch) continue;

        const symbol = symbolMatch[1];
        const date = dateMatch[1];

        // SKIP TODAY - collector is still writing
        // if (date === todayStr) continue; // Disabled for manual run

        // Apply filters
        if (symbol && symbol !== symbolMatch[1]) continue;
        if (startDate && date < startDate) continue;
        if (endDate && date > endDate) continue;

        files.push({
          key,
          symbol,
          date,
          size: obj.Size,
        });
      }
    }

    if (!res.IsTruncated) break;
    token = res.NextContinuationToken;
  }

  return files;
}

// Group files by symbol and date
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

// Check if target file exists
async function targetFileExists(targetKey) {
  const cmd = new ListObjectsV2Command({
    Bucket: BUCKET,
    Prefix: targetKey,
    MaxKeys: 1,
  });

  const res = await s3.send(cmd);
  return res.Contents && res.Contents.length > 0;
}

// Download files from S3 in parallel
async function downloadFiles(keys, downloadDir) {
  const localFiles = [];

  for (let i = 0; i < keys.length; i += MAX_PARALLEL_DOWNLOADS) {
    const batch = keys.slice(i, i + MAX_PARALLEL_DOWNLOADS);
    const downloads = batch.map(async (key, idx) => {
      const filename = `${i + idx}_${path.basename(key)}`;
      const localPath = path.join(downloadDir, filename);

      try {
        const cmd = new GetObjectCommand({ Bucket: BUCKET, Key: key });
        const res = await s3.send(cmd);

        const chunks = [];
        for await (const chunk of res.Body) {
          chunks.push(chunk);
        }

        fs.writeFileSync(localPath, Buffer.concat(chunks));
        return localPath;
      } catch (err) {
        console.error(`  ❌ Failed to download ${key}: ${err.message}`);
        return null;
      }
    });

    const results = await Promise.all(downloads);
    localFiles.push(...results.filter(Boolean));
  }

  return localFiles;
}

// Merge parquet files using DuckDB with ORDER BY for determinism
async function mergeParquetFiles(localFiles, outputPath) {
  return new Promise((resolve, reject) => {
    const db = new duckdb.Database(":memory:");
    const conn = db.connect();

    const fileList = localFiles.map(f => `'${f}'`).join(", ");

    // Deterministic merge: ORDER BY ts (timestamp column)
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
      if (err) {
        reject(err);
      } else {
        resolve();
      }
    });
  });
}

// Upload file to S3
async function uploadToS3(localPath, s3Key) {
  const fileContent = fs.readFileSync(localPath);
  const cmd = new PutObjectCommand({
    Bucket: BUCKET,
    Key: s3Key,
    Body: fileContent,
    ContentType: "application/octet-stream",
  });
  await s3.send(cmd);
}

// Delete source files
async function deleteSourceFiles(keys) {
  // Delete in batches of 1000 (S3 limit)
  for (let i = 0; i < keys.length; i += 1000) {
    const batch = keys.slice(i, i + 1000);
    const cmd = new DeleteObjectsCommand({
      Bucket: BUCKET,
      Delete: {
        Objects: batch.map(key => ({ Key: key })),
      },
    });
    await s3.send(cmd);
  }
}

// Format bytes to human readable
function formatBytes(bytes) {
  if (bytes < 1024) return bytes + " B";
  if (bytes < 1024 * 1024) return (bytes / 1024).toFixed(1) + " KB";
  if (bytes < 1024 * 1024 * 1024) return (bytes / (1024 * 1024)).toFixed(1) + " MB";
  return (bytes / (1024 * 1024 * 1024)).toFixed(2) + " GB";
}

// Main migration function
async function migrate() {
  const config = parseArgs();

  // Validate required args
  if (!config.exchange || !config.stream) {
    console.error("❌ Error: --exchange and --stream are required");
    console.error("\nUsage: node worker/migrate_v3.js --exchange binance --stream trades [options]");
    console.error("\nOptions:");
    console.error("  --target        Target dataset (v3-ready, v2-nodepth, legacy). Default: v3-ready");
    console.error("  --symbol        Filter by symbol");
    console.error("  --start-date    Start date YYYYMMDD");
    console.error("  --end-date      End date YYYYMMDD");
    console.error("  --dry-run       Show what would be done");
    console.error("  --delete-source Delete V3 files after migration");
    console.error("  --overwrite     Overwrite existing target files");
    process.exit(1);
  }

  // Validate target
  const validTargets = ["v3-ready", "v2-nodepth", "legacy"];
  if (!validTargets.includes(config.target)) {
    console.error(`❌ Error: Invalid target '${config.target}'. Must be one of: ${validTargets.join(", ")}`);
    process.exit(1);
  }

  console.log("\n🚀 V3 Migration");
  console.log("========================");
  console.log(`Exchange: ${config.exchange}`);
  console.log(`Stream:   ${config.stream}`);
  console.log(`Target:   ${config.target}`);
  if (config.symbol) console.log(`Symbol: ${config.symbol}`);
  if (config.startDate) console.log(`Start Date: ${config.startDate}`);
  if (config.endDate) console.log(`End Date: ${config.endDate}`);
  console.log(`Dry Run: ${config.dryRun}`);
  console.log(`Delete Source: ${config.deleteSource}`);
  console.log(`Overwrite: ${config.overwrite}`);
  console.log("");

  // Step 1: List all files
  console.log(`📋 Step 1: Scanning files from ${config.sourcePrefix}...`);
  const files = await listFiles(config);

  if (files.length === 0) {
    console.log("❌ No files found matching criteria");
    return;
  }

  console.log(`   Found ${files.length} parquet files\n`);

  // Step 2: Group by symbol/date
  console.log("📋 Step 2: Grouping by symbol/date...");
  const groups = groupFilesBySymbolDate(files);
  console.log(`   Found ${groups.length} symbol/date combinations\n`);

  // Step 3: Process each group
  console.log("📋 Step 3: Processing migrations...\n");

  const stats = {
    processed: 0,
    skipped: 0,
    failed: 0,
    totalSourceFiles: 0,
    totalSourceBytes: 0,
    totalOutputBytes: 0,
    deletedSourceFiles: 0,
  };

  // Create temp directory
  const tempDir = fs.mkdtempSync(path.join(os.tmpdir(), "v3-mig-"));

  try {
    for (const group of groups) {
      const { symbol, date, files: sourceKeys, totalSize } = group;
      
      const targetKey = `${config.target}/exchange=${config.exchange}/stream=${config.stream}/${symbol}/${date}/data.parquet`;

      process.stdout.write(`  ${symbol}/${date}: ${sourceKeys.length} files (${formatBytes(totalSize)}) → `);

      // Check if target exists
      if (!config.overwrite) {
        const exists = await targetFileExists(targetKey);
        if (exists) {
          console.log("⏭️  SKIP (exists)");
          stats.skipped++;
          continue;
        }
      }

      if (config.dryRun) {
        console.log("🔍 DRY-RUN");
        stats.processed++;
        stats.totalSourceFiles += sourceKeys.length;
        stats.totalSourceBytes += totalSize;
        continue;
      }

      try {
        // Create temp dir for this group
        const groupDir = path.join(tempDir, `${symbol}_${date}`);
        fs.mkdirSync(groupDir, { recursive: true });

        // Download files
        const localFiles = await downloadFiles(sourceKeys, groupDir);
        if (localFiles.length === 0) {
          console.log("❌ FAILED (download)");
          stats.failed++;
          continue;
        }

        // Merge with DuckDB
        const outputPath = path.join(tempDir, `${symbol}_${date}.parquet`);
        await mergeParquetFiles(localFiles, outputPath);

        // Get output size
        const outputSize = fs.statSync(outputPath).size;

        // Upload to target
        await uploadToS3(outputPath, targetKey);

        console.log(`✅ ${formatBytes(outputSize)}`);

        stats.processed++;
        stats.totalSourceFiles += sourceKeys.length;
        stats.totalSourceBytes += totalSize;
        stats.totalOutputBytes += outputSize;

        // Delete source files if requested
        if (config.deleteSource) {
          await deleteSourceFiles(sourceKeys);
          stats.deletedSourceFiles += sourceKeys.length;
          console.log(`     🗑️  Deleted ${sourceKeys.length} source files`);
        }

        // Cleanup temp files
        fs.rmSync(groupDir, { recursive: true, force: true });
        fs.unlinkSync(outputPath);

      } catch (err) {
        console.log(`❌ FAILED (${err.message})`);
        stats.failed++;
      }
    }
  } finally {
    // Cleanup temp directory
    fs.rmSync(tempDir, { recursive: true, force: true });
  }

  // Summary
  console.log("\n═══════════════════════════════════════");
  console.log("📊 Migration Summary");
  console.log("═══════════════════════════════════════");
  console.log(`Processed: ${stats.processed}`);
  console.log(`Skipped:   ${stats.skipped}`);
  console.log(`Failed:    ${stats.failed}`);
  console.log(`─────────────────────────────────────`);
  console.log(`Source Files:  ${stats.totalSourceFiles}`);
  console.log(`Source Size:   ${formatBytes(stats.totalSourceBytes)}`);
  if (!config.dryRun) {
    console.log(`Output Size:   ${formatBytes(stats.totalOutputBytes)}`);
    console.log(`Compression:   ${((1 - stats.totalOutputBytes / stats.totalSourceBytes) * 100).toFixed(1)}%`);
  }
  if (config.deleteSource) {
    console.log(`Deleted Source: ${stats.deletedSourceFiles} files`);
  }
  console.log("═══════════════════════════════════════\n");
}

// Run
migrate().catch(err => {
  console.error("❌ Fatal error:", err);
  process.exit(1);
});
