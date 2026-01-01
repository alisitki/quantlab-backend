import express from "express";
import cors from "cors";
import dotenv from "dotenv";
import { S3Client, ListObjectsV2Command, GetObjectCommand, PutObjectCommand } from "@aws-sdk/client-s3";
import { v4 as uuidv4 } from "uuid";
import { executeResampleWithDuckDB } from "./worker/resample.js";
import { executeFeatureWithDuckDB } from "./worker/feature.js";
import { executeBackfillJob } from "./worker/backfill.js";
import { executeConsolidateJob } from "./worker/consolidate.js";

dotenv.config();

const app = express();
app.use(cors());
app.use(express.json());

/* ================================================================
   S3 CLIENT
   ================================================================ */

const s3 = new S3Client({
  endpoint: process.env.S3_ENDPOINT,
  region: "us-east-1",
  credentials: {
    accessKeyId: process.env.S3_ACCESS_KEY,
    secretAccessKey: process.env.S3_SECRET_KEY,
  },
  forcePathStyle: true,
});

const BUCKET = process.env.S3_BUCKET;

/* ================================================================
   JOBS - In-Memory Store (MVP)
   ================================================================ */

const jobs = new Map();

// Track running job abort controllers for cancellation
const jobAbortControllers = new Map();

// S3 listing cache for performance (5 minute TTL)
const s3Cache = {
  data: {},
  timestamp: {},
  TTL_MS: 5 * 60 * 1000, // 5 minutes
};

/* ================================================================
   HELPERS
   ================================================================ */

/**
 * List subdirectories at a given prefix level using S3 Delimiter
 * Much faster than listing all files
 */
async function listSubdirectories(bucket, prefix, delimiter = "/") {
  const prefixes = [];
  let token;

  while (true) {
    const cmd = new ListObjectsV2Command({
      Bucket: bucket,
      Prefix: prefix,
      Delimiter: delimiter,
      ContinuationToken: token,
    });

    const res = await s3.send(cmd);

    if (res.CommonPrefixes) {
      prefixes.push(...res.CommonPrefixes.map(p => p.Prefix));
    }

    if (!res.IsTruncated) break;
    token = res.NextContinuationToken;
  }

  return prefixes;
}

/**
 * Fast dataset discovery - only scans directory structure, not files
 * Returns dataset metadata by walking exchange/stream directories
 */
async function discoverDatasetsFast(bucket) {
  const cacheKey = `datasets:${bucket}`;
  const now = Date.now();
  
  // Check cache
  if (s3Cache.data[cacheKey] && (now - s3Cache.timestamp[cacheKey] < s3Cache.TTL_MS)) {
    return s3Cache.data[cacheKey];
  }

  const datasets = [];

//   // ========= V3 RAW (formerly V2) =========
//   // Structure: v3/exchange=X/stream=Y/symbol=Z/date=D/
//   const v3Exchanges = await listSubdirectories(bucket, "v3/");
//   for (const exchPrefix of v3Exchanges) {
//     const exchange = exchPrefix.match(/exchange=([^/]+)/)?.[1];
//     if (!exchange) continue;
//     
//     const streams = await listSubdirectories(bucket, exchPrefix);
//     for (const streamPrefix of streams) {
//       const stream = streamPrefix.match(/stream=([^/]+)/)?.[1];
//       if (!stream) continue;
//       
//       // Get symbol names from directory structure
//       const symbolDirs = await listSubdirectories(bucket, streamPrefix);
//       const symbols = symbolDirs
//         .map(s => s.match(/symbol=([^/]+)/)?.[1])
//         .filter(Boolean);
//       
//       // Get date range from first symbol's dates
//       let dateRange = { min: null, max: null };
//       if (symbolDirs.length > 0) {
//         const dateDirs = await listSubdirectories(bucket, symbolDirs[0]);
//         const dates = dateDirs
//           .map(d => d.match(/date=(\d{8})/)?.[1])
//           .filter(Boolean)
//           .sort();
//         if (dates.length > 0) {
//           dateRange = { min: dates[0], max: dates[dates.length - 1] };
//         }
//       }
//       
//       datasets.push({
//         id: `${exchange}-${stream}-v3`,
//         type: "raw",
//         version: "v3",
//         exchange,
//         stream,
//         symbols: symbols,
//         date_range: dateRange,
//         produced_by: null,
//       });
//     }
//   }

  // ========= CONSOLIDATED DATASETS (Legacy, V3-Tekpart, V2-NoDepth) =========
  // Structure: prefix/exchange=X/stream=Y/symbol/date/
  const consolidatedPrefixes = ["v3-ready"];
  
  for (const rootPrefix of consolidatedPrefixes) {
    const exchPrefixes = await listSubdirectories(bucket, `${rootPrefix}/`);
    
    for (const exchPrefix of exchPrefixes) {
      const exchange = exchPrefix.match(/exchange=([^/]+)/)?.[1];
      if (!exchange) continue;
      
      const streams = await listSubdirectories(bucket, exchPrefix);
      for (const streamPrefix of streams) {
        const stream = streamPrefix.match(/stream=([^/]+)/)?.[1];
        if (!stream) continue;
        
        // Get symbol names (can be plain folder or symbol=X)
        const symbolDirs = await listSubdirectories(bucket, streamPrefix);
        const symbolsRaw = symbolDirs.map(s => {
          // Try symbol= format first
          const match = s.match(/symbol=([^/]+)/);
          if (match) return match[1];
          // Otherwise extract folder name
          const parts = s.replace(streamPrefix, "").split("/").filter(Boolean);
          return parts[0];
        }).filter(s => s && !s.includes("="));
        
        // Deduplicate
        const symbols = [...new Set(symbolsRaw)];
        
        // Get date range from first symbol's dates  
        let dateRange = { min: null, max: null };
        if (symbolDirs.length > 0) {
          const dateDirs = await listSubdirectories(bucket, symbolDirs[0]);
          const dates = dateDirs
            .map(d => d.match(/(\d{8})/)?.[1])
            .filter(Boolean)
            .sort();
          if (dates.length > 0) {
            dateRange = { min: dates[0], max: dates[dates.length - 1] };
          }
        }
        
        // Determine dataset ID and version based on root prefix
        const datasetId = `${exchange}-${stream}-${rootPrefix}`;
        const displayName = rootPrefix === "v3-ready" ? "BINANCE MASTER STORE" : null;
        
        datasets.push({
          id: datasetId,
          display_name: displayName,
          type: "raw",
          version: rootPrefix,
          exchange,
          stream,
          symbols: symbols,
          date_range: dateRange,
          produced_by: null,
          allowed_aggregations: ["OHLCV", "TickResample"],  // Raw data supports both
        });
      }
    }
  }

  // ========= CURATED =========
  // Structure: curated/exchange=X/dataset=Y/symbol=Z/date=D/
  const curatedExchanges = await listSubdirectories(bucket, "curated/");
  for (const exchPrefix of curatedExchanges) {
    const exchange = exchPrefix.match(/exchange=([^/]+)/)?.[1];
    if (!exchange) continue;
    
    const datasetDirs = await listSubdirectories(bucket, exchPrefix);
    for (const datasetPrefix of datasetDirs) {
      const datasetName = datasetPrefix.match(/dataset=([^/]+)/)?.[1];
      if (!datasetName) continue;
      
      const symbolDirs = await listSubdirectories(bucket, datasetPrefix);
      const symbols = symbolDirs
        .map(s => s.match(/symbol=([^/]+)/)?.[1])
        .filter(Boolean);
      
      // Get date range from first symbol's dates
      let dateRange = { min: null, max: null };
      if (symbolDirs.length > 0) {
        const dateDirs = await listSubdirectories(bucket, symbolDirs[0]);
        const dates = dateDirs
          .map(d => d.match(/date=(\d{8})/)?.[1])
          .filter(Boolean)
          .sort();
        if (dates.length > 0) {
          dateRange = { min: dates[0], max: dates[dates.length - 1] };
        }
      }
      
      // Try to read metadata file for config
      let config = null;
      try {
        const metadataKey = `${datasetPrefix}_metadata.json`;
        config = await fetchMetadataFromS3(bucket, metadataKey);
      } catch (e) {
        // Metadata file doesn't exist, that's OK
      }
      
      // Determine allowed aggregations based on current aggregation type
      let allowedAggregations = [];
      if (config?.aggregation === "OHLCV") {
        allowedAggregations = ["OHLCV"];  // Can resample to larger timeframe
      } else if (config?.aggregation === "TickResample") {
        allowedAggregations = [];  // Cannot be further resampled
      } else {
        allowedAggregations = ["OHLCV"];  // Default for unknown curated
      }
      
      datasets.push({
        id: `${exchange}-${datasetName}-curated`,
        type: "curated",
        version: "curated",
        exchange,
        stream: datasetName,
        symbols: symbols,
        date_range: dateRange,
        produced_by: null,
        config: config,
        allowed_aggregations: allowedAggregations,
      });
    }
  }

  // Update cache
  s3Cache.data[cacheKey] = datasets;
  s3Cache.timestamp[cacheKey] = now;

  return datasets;
}

/**
 * List all keys with a given prefix (for job execution, not listing)
 */
async function listAllKeys(bucket, prefix) {
  const keys = [];
  let token;

  while (true) {
    const cmd = new ListObjectsV2Command({
      Bucket: bucket,
      Prefix: prefix,
      ContinuationToken: token,
    });

    const res = await s3.send(cmd);

    if (res.Contents) {
      res.Contents.forEach((c) => {
        if (c.Key && c.Key.endsWith(".parquet") && !c.Key.includes("/._")) {
          keys.push(c.Key);
        }
      });
    }

    if (!res.IsTruncated) break;
    token = res.NextContinuationToken;
  }

  return keys;
}

/**
 * Invalidate cache for a prefix (call after writes)
 */
function invalidateCache(prefix) {
  // Clear all cache entries that might be affected
  Object.keys(s3Cache.data).forEach(k => {
    if (k.includes(prefix) || k.startsWith("datasets:")) {
      delete s3Cache.data[k];
      delete s3Cache.timestamp[k];
    }
  });
}

/**
 * Fetch metadata JSON from S3
 */
async function fetchMetadataFromS3(bucket, key) {
  try {
    const cmd = new GetObjectCommand({ Bucket: bucket, Key: key });
    const res = await s3.send(cmd);
    
    const chunks = [];
    for await (const chunk of res.Body) {
      chunks.push(chunk);
    }
    
    const body = Buffer.concat(chunks).toString("utf-8");
    return JSON.parse(body);
  } catch (err) {
    // File doesn't exist or other error
    return null;
  }
}

/**
 * Parse v3 keys: v3/exchange=X/stream=Y/symbol=Z/date=YYYYMMDD/file.parquet
 */
function parseV3Key(key) {
  const clean = key.replace("v3/", "");
  const parts = clean.split("/");
  if (parts.length < 5) return null;

  const [exchangePart, streamPart, symbolPart, datePart] = parts;

  if (
    !exchangePart.startsWith("exchange=") ||
    !streamPart.startsWith("stream=") ||
    !symbolPart.startsWith("symbol=") ||
    !datePart.startsWith("date=")
  ) return null;

  return {
    exchange: exchangePart.split("=")[1],
    stream: streamPart.split("=")[1],
    symbol: symbolPart.split("=")[1],
    date: datePart.split("=")[1],
    version: "v3",
    type: "raw",
  };
}

/**
 * Parse legacy keys: legacy/exchange=X/stream=Y/symbol/YYYYMMDD/file.parquet
 */
// Parse consolidated keys (legacy, v3-ready, v2-nodepth)
function parseConsolidatedKey(key) {
  const prefixes = ["v3-ready"];
  const prefix = prefixes.find(p => key.startsWith(p + "/"));
  
  if (!prefix) return null;

  const clean = key.replace(prefix + "/", "");
  const parts = clean.split("/");
  if (parts.length < 5) return null;

  const [exchangePart, streamPart, symbol, date] = parts;

  if (
    !exchangePart.startsWith("exchange=") ||
    !streamPart.startsWith("stream=")
  ) return null;

  // Skip malformed entries
  if (symbol.includes("=")) return null;
  if (!/^\d{8}$/.test(date)) return null;

  return {
    exchange: exchangePart.split("=")[1],
    stream: streamPart.split("=")[1],
    symbol,
    date,
    version: prefix,
    type: "raw",
  };
}

/**
 * Parse curated keys: curated/exchange=X/dataset=Y/symbol=Z/date=YYYYMMDD/file.parquet
 */
function parseCuratedKey(key) {
  const clean = key.replace("curated/", "");
  const parts = clean.split("/");
  if (parts.length < 5) return null;

  const [exchangePart, datasetPart, symbolPart, datePart] = parts;

  if (
    !exchangePart.startsWith("exchange=") ||
    !datasetPart.startsWith("dataset=") ||
    !symbolPart.startsWith("symbol=") ||
    !datePart.startsWith("date=")
  ) return null;

  return {
    exchange: exchangePart.split("=")[1],
    stream: datasetPart.split("=")[1], // dataset name as stream
    symbol: symbolPart.split("=")[1],
    date: datePart.split("=")[1],
    version: "curated",
    type: "curated",
  };
}

/* ================================================================
   GET /api/datasets
   Query params: type, version, search
   OPTIMIZED: Uses directory-level listing instead of scanning all files
   ================================================================ */

app.get("/api/datasets", async (req, res) => {
  try {
    const { type, version, search } = req.query;
    
    // Fast discovery using S3 CommonPrefixes (directories only)
    let datasets = await discoverDatasetsFast(BUCKET);

    // Filter by type
    if (type) {
      datasets = datasets.filter((d) => d.type === type);
    }

    // Filter by version
    if (version) {
      datasets = datasets.filter((d) => d.version === version);
    }

    // Filter by search (substring match on id)
    if (search) {
      const searchLower = search.toLowerCase();
      datasets = datasets.filter((d) => d.id.toLowerCase().includes(searchLower));
    }

    res.json(datasets);
  } catch (err) {
    console.error("DATASETS ERROR:", err);
    res.status(500).json({ error: "Internal Server Error" });
  }
});

/* ================================================================
   DELETE /api/datasets/:id - Delete a curated dataset
   Only curated datasets can be deleted (raw data is protected)
   ================================================================ */

app.delete("/api/datasets/:id", async (req, res) => {
  try {
    const datasetId = req.params.id;
    
    // Parse dataset ID: exchange-name-version
    // Example: binance-denemeeeee-curated
    if (!datasetId.endsWith('-curated')) {
      return res.status(400).json({ 
        error: "Only curated datasets can be deleted. Raw data is protected." 
      });
    }

    const parts = datasetId.split("-");
    if (parts.length < 3) {
      return res.status(400).json({ error: "Invalid dataset ID format" });
    }

    const exchange = parts[0];
    const datasetName = parts.slice(1, -1).join("-"); // Everything between exchange and 'curated'

    // Build S3 prefix for this dataset
    const prefix = `curated/exchange=${exchange}/dataset=${datasetName}/`;
    
    // List all objects with this prefix
    let token;
    const keysToDelete = [];
    
    while (true) {
      const cmd = new ListObjectsV2Command({
        Bucket: BUCKET,
        Prefix: prefix,
        ContinuationToken: token,
      });
      const response = await s3.send(cmd);
      
      if (response.Contents) {
        keysToDelete.push(...response.Contents.map(c => ({ Key: c.Key })));
      }
      
      if (!response.IsTruncated) break;
      token = response.NextContinuationToken;
    }

    if (keysToDelete.length === 0) {
      return res.status(404).json({ error: "Dataset not found or already deleted" });
    }

    // Delete in batches of 1000
    const { DeleteObjectsCommand } = await import("@aws-sdk/client-s3");
    
    for (let i = 0; i < keysToDelete.length; i += 1000) {
      const batch = keysToDelete.slice(i, i + 1000);
      await s3.send(new DeleteObjectsCommand({
        Bucket: BUCKET,
        Delete: { Objects: batch }
      }));
    }
    // Invalidate cache so changes reflect immediately
    invalidateCache("curated/");

    res.json({ 
      message: "Dataset deleted successfully",
      dataset_id: datasetId,
          display_name: displayName,
      files_deleted: keysToDelete.length
    });
  } catch (err) {
    console.error("DELETE DATASET ERROR:", err);
    res.status(500).json({ error: "Internal Server Error" });
  }
});

// ─────────────────────────────────────────────────────────────────────────────
// BULK DELETE CURATED DATASETS
// ─────────────────────────────────────────────────────────────────────────────
app.post("/api/datasets/bulk-delete", async (req, res) => {
  try {
    const { dataset_ids } = req.body;
    
    if (!Array.isArray(dataset_ids) || dataset_ids.length === 0) {
      return res.status(400).json({ error: "dataset_ids array is required" });
    }

    const results = { deleted_count: 0, errors: [] };
    const { DeleteObjectsCommand } = await import("@aws-sdk/client-s3");

    for (const datasetId of dataset_ids) {
      try {
        // Only allow curated datasets
        if (!datasetId.endsWith('-curated')) {
          results.errors.push({ id: datasetId,
          display_name: displayName, error: "Only curated datasets can be deleted" });
          continue;
        }

        const parts = datasetId.split("-");
        if (parts.length < 3) {
          results.errors.push({ id: datasetId,
          display_name: displayName, error: "Invalid dataset ID format" });
          continue;
        }

        const exchange = parts[0];
        const datasetName = parts.slice(1, -1).join("-");
        const prefix = `curated/exchange=${exchange}/dataset=${datasetName}/`;

        // List all objects with this prefix
        let token;
        const keysToDelete = [];
        
        while (true) {
          const cmd = new ListObjectsV2Command({
            Bucket: BUCKET,
            Prefix: prefix,
            ContinuationToken: token,
          });
          const response = await s3.send(cmd);
          
          if (response.Contents) {
            keysToDelete.push(...response.Contents.map(c => ({ Key: c.Key })));
          }
          
          if (!response.IsTruncated) break;
          token = response.NextContinuationToken;
        }

        if (keysToDelete.length === 0) {
          results.errors.push({ id: datasetId,
          display_name: displayName, error: "Dataset not found" });
          continue;
        }

        // Delete in batches of 1000
        for (let i = 0; i < keysToDelete.length; i += 1000) {
          const batch = keysToDelete.slice(i, i + 1000);
          await s3.send(new DeleteObjectsCommand({
            Bucket: BUCKET,
            Delete: { Objects: batch }
          }));
        }

        results.deleted_count++;
      } catch (err) {
        results.errors.push({ id: datasetId,
          display_name: displayName, error: err.message });
      }
    }

    // Invalidate cache
    invalidateCache("curated/");

    res.json(results);
  } catch (err) {
    console.error("BULK DELETE ERROR:", err);
    res.status(500).json({ error: "Internal Server Error" });
  }
});

/* ================================================================
   POST /api/jobs - Create a new job
   Extended payload: start_date, end_date, output_dataset_name
   ================================================================ */

app.post("/api/jobs", async (req, res) => {
  try {
    const { 
      type, 
      input_dataset, 
      params,
      // Extended fields
      symbols,
      start_date,
      end_date,
      output_dataset_name,
      config
    } = req.body;

    // Validate required fields
    if (!type || !input_dataset) {
      return res.status(400).json({ error: "Missing required fields: type, input_dataset" });
    }

    // Validate job type
    const validTypes = ["resample", "feature", "backfill", "consolidate"];
    if (!validTypes.includes(type)) {
      return res.status(400).json({ error: `Invalid job type. Must be one of: ${validTypes.join(", ")}` });
    }

    // CONSOLIDATE: Only allow one at a time
    if (type === "consolidate") {
      const runningConsolidate = Array.from(jobs.values()).find(
        j => j.type === "consolidate" && (j.status === "queued" || j.status === "running")
      );
      if (runningConsolidate) {
        return res.status(409).json({ 
          error: "A consolidate job is already running. Please wait for it to complete or stop it first.",
          running_job_id: runningConsolidate.job_id,
          running_job_status: runningConsolidate.status
        });
      }
    }

    const jobId = uuidv4();
    
    // Merge params with top-level fields for flexibility
    const mergedParams = {
      ...params,
      symbols: symbols || params?.symbols || [],
      timeframe: config?.timeframe || params?.timeframe || "1s",
      aggregation_type: config?.aggregation_type || params?.aggregation_type,
    };

    const job = {
      job_id: jobId,
      type,
      status: "queued",
      input_dataset,
      params: mergedParams,
      start_date: start_date || null,   // YYYYMMDD format
      end_date: end_date || null,       // YYYYMMDD format
      output_dataset: output_dataset_name || null,
      created_at: new Date().toISOString(),
      started_at: null,
      completed_at: null,
      error: null,
      log: [],
    };

    jobs.set(jobId, job);

    // Trigger async worker (fire-and-forget for MVP)
    setImmediate(() => runWorker(jobId));

    res.status(201).json({
      job_id: jobId,
      status: "queued",
    });
  } catch (err) {
    console.error("POST /api/jobs ERROR:", err);
    res.status(500).json({ error: "Internal Server Error" });
  }
});

/* ================================================================
   GET /api/jobs - List all jobs
   ================================================================ */

app.get("/api/jobs", (req, res) => {
  const { status, type } = req.query;
  
  let jobList = Array.from(jobs.values())
    .sort((a, b) => new Date(b.created_at) - new Date(a.created_at));

  // Filter by status
  if (status) {
    jobList = jobList.filter((j) => j.status === status);
  }

  // Filter by type
  if (type) {
    jobList = jobList.filter((j) => j.type === type);
  }

  const result = jobList.map((j) => ({
    job_id: j.job_id,
    type: j.type,
    status: j.status,
    input_dataset: j.input_dataset,
    output_dataset: j.output_dataset,
    created_at: j.created_at,
    started_at: j.started_at,
    completed_at: j.completed_at,
  }));

  res.json(result);
});

/* ================================================================
   GET /api/jobs/:id - Job details
   ================================================================ */

app.get("/api/jobs/:id", (req, res) => {
  const job = jobs.get(req.params.id);

  if (!job) {
    return res.status(404).json({ error: "Job not found" });
  }

  res.json(job);
});

/* ================================================================
   POST /api/jobs/:id/stop - Stop a running job
   ================================================================ */

app.post("/api/jobs/:id/stop", (req, res) => {
  const job = jobs.get(req.params.id);

  if (!job) {
    return res.status(404).json({ error: "Job not found" });
  }

  // Can only stop queued or running jobs
  if (job.status !== "queued" && job.status !== "running") {
    return res.status(400).json({ 
      error: `Cannot stop job with status: ${job.status}. Only 'queued' or 'running' jobs can be stopped.` 
    });
  }

  // If job has an abort controller, trigger it
  const abortController = jobAbortControllers.get(req.params.id);
  if (abortController) {
    abortController.abort();
    jobAbortControllers.delete(req.params.id);
  }

  job.status = "stopped";
  job.completed_at = new Date().toISOString();
  job.log.push(`Job stopped by user at ${job.completed_at}`);

  res.json({
    job_id: job.job_id,
    status: "stopped",
    message: "Job has been stopped",
  });
});

/* ================================================================
   DELETE /api/jobs/:id - Delete a job
   ================================================================ */

app.delete("/api/jobs/:id", (req, res) => {
  const job = jobs.get(req.params.id);

  if (!job) {
    return res.status(404).json({ error: "Job not found" });
  }

  // Cannot delete running jobs
  if (job.status === "running") {
    return res.status(400).json({ 
      error: "Cannot delete a running job. Stop it first using POST /api/jobs/:id/stop" 
    });
  }

  // Clean up abort controller if exists
  jobAbortControllers.delete(req.params.id);
  
  // Remove from store
  jobs.delete(req.params.id);

  res.json({
    message: "Job deleted successfully",
    job_id: req.params.id,
  });
});

/* ================================================================
   WORKER - Async job execution (MVP, in-process)
   ================================================================ */

async function runWorker(jobId) {
  const job = jobs.get(jobId);
  if (!job) return;

  // Check if job was stopped before starting
  if (job.status === "stopped") return;

  job.status = "running";
  job.started_at = new Date().toISOString();
  job.log.push(`Job started at ${job.started_at}`);

  // Create abort controller for this job
  const abortController = new AbortController();
  jobAbortControllers.set(jobId, abortController);

  try {
    if (job.type === "resample") {
      await executeResampleWithDuckDB(job, s3, BUCKET, abortController.signal);
    } else if (job.type === "feature") {
      await executeFeatureWithDuckDB(job, s3, BUCKET, abortController.signal);
    } else if (job.type === "backfill") {
      await executeBackfillJob(job, s3, BUCKET, abortController.signal);
    } else if (job.type === "consolidate") {
      await executeConsolidateJob(job, s3, BUCKET, abortController.signal);
    }

    // Check if aborted
    if (abortController.signal.aborted) {
      job.status = "stopped";
      job.log.push("Job was stopped during execution");
    } else {
      job.status = "done";
      job.log.push("Job completed successfully");
      
      // Invalidate cache so new datasets appear immediately
      invalidateCache("curated/");
    }
    
    job.completed_at = new Date().toISOString();
    console.log(`Job ${jobId} completed with status: ${job.status}`);
  } catch (err) {
    if (err.name === "AbortError") {
      job.status = "stopped";
      job.log.push("Job was aborted");
    } else {
      job.status = "failed";
      job.error = err.message;
      job.log.push(`Error: ${err.message}`);
    }
    job.completed_at = new Date().toISOString();
    console.error(`Job ${jobId} ended with status ${job.status}:`, err.message);
  } finally {
    jobAbortControllers.delete(jobId);
  }
}

/**
 * Execute resample job: tick → OHLCV aggregation
 * MVP: Uses simple in-memory aggregation (DuckDB optional for larger datasets)
 */
async function executeResampleJob(job, signal) {
  const { input_dataset, params, start_date, end_date } = job;
  const { symbols = [], timeframe = "1s" } = params;

  // Check for abort
  if (signal?.aborted) throw new Error("AbortError");

  // Parse input dataset ID: exchange-stream-version
  const parts = input_dataset.split("-");
  if (parts.length < 3) {
    throw new Error(`Invalid input_dataset format: ${input_dataset}`);
  }

  const exchange = parts[0];
  const stream = parts[1];
  const version = parts.slice(2).join("-"); // Handle v2, legacy, etc.

  // Determine prefix based on version
  let prefix;
  if (version === "v2") {
    prefix = `v2/exchange=${exchange}/stream=${stream}/`;
  } else if (version === "legacy") {
    prefix = `legacy/exchange=${exchange}/stream=${stream}/`;
  } else {
    throw new Error(`Unknown version: ${version}`);
  }

  job.log.push(`Scanning prefix: ${prefix}`);

  // List all keys for this dataset
  const keys = await listAllKeys(BUCKET, prefix);
  if (keys.length === 0) {
    throw new Error(`No data found for dataset: ${input_dataset}`);
  }

  job.log.push(`Found ${keys.length} files`);

  // Filter by symbols if specified
  let filteredKeys = keys;
  if (symbols.length > 0) {
    const symbolSet = new Set(symbols.map((s) => s.toLowerCase()));
    filteredKeys = keys.filter((key) => {
      // Extract symbol from key
      const match = key.match(/symbol=([^/]+)/);
      if (match) {
        return symbolSet.has(match[1].toLowerCase());
      }
      // Legacy format: check if symbol is in path
      const legacyParts = key.split("/");
      if (legacyParts.length >= 4) {
        return symbolSet.has(legacyParts[3].toLowerCase());
      }
      return false;
    });
    job.log.push(`Filtered to ${filteredKeys.length} files for symbols: ${symbols.join(", ")}`);
  }

  // Filter by date range if specified
  if (start_date || end_date) {
    filteredKeys = filteredKeys.filter((key) => {
      const dateMatch = key.match(/date=(\d{8})/);
      if (!dateMatch) return true; // Keep if no date in path
      
      const keyDate = dateMatch[1];
      if (start_date && keyDate < start_date) return false;
      if (end_date && keyDate > end_date) return false;
      return true;
    });
    job.log.push(`Filtered to ${filteredKeys.length} files for date range: ${start_date || '*'} to ${end_date || '*'}`);
  }

  if (filteredKeys.length === 0) {
    throw new Error(`No data found for specified filters`);
  }

  // Check for abort before processing
  if (signal?.aborted) throw new Error("AbortError");

  // Output dataset name
  const outputDataset = job.output_dataset || `${exchange}-ohlcv_${timeframe}-curated`;
  job.output_dataset = outputDataset;

  // MVP: Create a marker file to indicate job completion
  // Real implementation would process parquet files through DuckDB
  const markerKey = `curated/exchange=${exchange}/dataset=ohlcv_${timeframe}/_job_${job.job_id}.marker`;
  
  const markerContent = JSON.stringify({
    job_id: job.job_id,
    input_dataset,
    output_dataset: outputDataset,
    symbols: symbols.length > 0 ? symbols : "all",
    timeframe,
    start_date,
    end_date,
    processed_files: filteredKeys.length,
    created_at: new Date().toISOString(),
  }, null, 2);

  await s3.send(new PutObjectCommand({
    Bucket: BUCKET,
    Key: markerKey,
    Body: markerContent,
    ContentType: "application/json",
  }));

  job.log.push(`Created marker file: ${markerKey}`);
  job.log.push(`Processed ${filteredKeys.length} files -> ${outputDataset}`);
  
  console.log(`Resample job ${job.job_id}: Processed ${filteredKeys.length} files -> ${outputDataset}`);
}

/* ================================================================
   HEALTH CHECK
   ================================================================ */

app.get("/health", (req, res) => {
  res.json({ ok: true });
});

/* ================================================================
   START SERVER
   ================================================================ */

app.listen(3001, () => {
  console.log("API running on :3001");
});
