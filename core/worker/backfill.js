/**
 * Backfill Worker
 * Fills missing data gaps in datasets
 */

import { S3Client, ListObjectsV2Command, CopyObjectCommand } from "@aws-sdk/client-s3";

/**
 * Execute backfill job - identifies and fills gaps in date ranges
 */
export async function executeBackfillJob(job, s3Client, bucket, signal) {
  const { input_dataset, params, start_date, end_date } = job;
  const { symbols = [], fill_method = "forward" } = params;

  // Parse input dataset
  // Special handling for BINANCE MASTER STORE
  let exchange, stream, version;
  
  if (input_dataset === "BINANCE MASTER STORE") {
    exchange = "binance";
    stream = "tick";
    version = "v3-ready";
  } else {
    const parts = input_dataset.split("-");
    if (parts.length < 3) {
      throw new Error(`Invalid input_dataset format: ${input_dataset}`);
    }
    exchange = parts[0];
    stream = parts[1];
    version = parts.slice(2).join("-");
  }

  // Determine prefix
  let prefix;
  if (version === "v3") {
    prefix = `v3/exchange=${exchange}/stream=${stream}/`;
  } else if (version === "v2") {
    prefix = `v3/exchange=${exchange}/stream=${stream}/`;
  } else if (version === "v2") {
    prefix = `v2/exchange=${exchange}/stream=${stream}/`;
  } else if (version === "legacy" || version === "v3-ready" || version === "v2-nodepth") {
    prefix = `${version}/exchange=${exchange}/stream=${stream}/`;
  } else if (version === "curated" || version.includes("curated")) {
    prefix = `curated/exchange=${exchange}/`;
  } else {
    throw new Error(`Unknown version: ${version}`);
  }

  job.log.push(`Scanning prefix: ${prefix}`);

  // List all keys to find existing dates
  const keys = await listAllKeys(s3Client, bucket, prefix, symbols, start_date, end_date);
  job.log.push(`Found ${keys.length} existing files`);

  if (keys.length === 0) {
    throw new Error(`No data found for dataset: ${input_dataset}`);
  }

  if (signal?.aborted) throw new Error("AbortError");

  // Extract existing dates per symbol
  const symbolDates = extractSymbolDates(keys, version);
  job.log.push(`Analyzing date gaps for ${Object.keys(symbolDates).length} symbols`);

  // Find gaps
  const gaps = findDateGaps(symbolDates, start_date, end_date);
  const totalGaps = Object.values(gaps).reduce((sum, dates) => sum + dates.length, 0);
  
  if (totalGaps === 0) {
    job.log.push("No gaps found - dataset is complete");
    job.output_dataset = input_dataset;
    return;
  }

  job.log.push(`Found ${totalGaps} date gaps across ${Object.keys(gaps).length} symbols`);

  // Fill gaps based on method
  let filled = 0;
  for (const [symbol, missingDates] of Object.entries(gaps)) {
    if (signal?.aborted) throw new Error("AbortError");

    for (const missingDate of missingDates) {
      const sourceDate = findSourceDate(symbolDates[symbol], missingDate, fill_method);
      
      if (sourceDate) {
        // Copy from source date to missing date
        const sourceKey = findKeyForSymbolDate(keys, symbol, sourceDate, version);
        if (sourceKey) {
          const targetKey = sourceKey.replace(`date=${sourceDate}`, `date=${missingDate}`);
          
          await s3Client.send(new CopyObjectCommand({
            Bucket: bucket,
            CopySource: `${bucket}/${sourceKey}`,
            Key: targetKey,
          }));
          
          filled++;
          job.log.push(`Filled gap: ${symbol}/${missingDate} from ${sourceDate}`);
        }
      }
    }
  }

  job.log.push(`Backfill complete: filled ${filled} gaps`);
  job.output_dataset = input_dataset;
}

// Helper functions
async function listAllKeys(s3Client, bucket, prefix, symbols, startDate, endDate) {
  const keys = [];
  let token;
  
  while (true) {
    const cmd = new ListObjectsV2Command({ Bucket: bucket, Prefix: prefix, ContinuationToken: token });
    const res = await s3Client.send(cmd);
    
    if (res.Contents) {
      res.Contents.forEach((c) => {
        if (!c.Key?.endsWith(".parquet")) return;
        if (c.Key.includes("/._")) return;
        
        // Filter by symbols if specified
        if (symbols.length > 0) {
          const symbolMatch = c.Key.match(/symbol=([^/]+)/);
          if (symbolMatch && !symbols.map(s => s.toLowerCase()).includes(symbolMatch[1].toLowerCase())) {
            return;
          }
        }
        
        // Filter by date range
        if (startDate || endDate) {
          const dateMatch = c.Key.match(/date=(\d{8})/);
          if (dateMatch) {
            const keyDate = dateMatch[1];
            if (startDate && keyDate < startDate) return;
            if (endDate && keyDate > endDate) return;
          }
        }
        
        keys.push(c.Key);
      });
    }
    
    if (!res.IsTruncated) break;
    token = res.NextContinuationToken;
  }
  
  return keys;
}

function extractSymbolDates(keys, version) {
  const result = {};
  
  for (const key of keys) {
    let symbol, date;
    
    const symbolMatch = key.match(/symbol=([^/]+)/);
    const dateMatch = key.match(/date=(\d{8})/);
    
    if (symbolMatch && dateMatch) {
      symbol = symbolMatch[1];
      date = dateMatch[1];
    } else if (version === "legacy" || version === "v3-ready" || version === "v2-nodepth") {
      const parts = key.split("/");
      if (parts.length >= 5) {
        symbol = parts[3];
        date = parts[4];
      }
    }
    
    if (symbol && date) {
      if (!result[symbol]) result[symbol] = new Set();
      result[symbol].add(date);
    }
  }
  
  // Convert sets to sorted arrays
  for (const symbol of Object.keys(result)) {
    result[symbol] = Array.from(result[symbol]).sort();
  }
  
  return result;
}

function findDateGaps(symbolDates, startDate, endDate) {
  const gaps = {};
  
  for (const [symbol, dates] of Object.entries(symbolDates)) {
    if (dates.length < 2) continue;
    
    const start = startDate || dates[0];
    const end = endDate || dates[dates.length - 1];
    const existingSet = new Set(dates);
    
    // Generate all dates in range
    const allDates = generateDateRange(start, end);
    const missing = allDates.filter(d => !existingSet.has(d));
    
    if (missing.length > 0) {
      gaps[symbol] = missing;
    }
  }
  
  return gaps;
}

function generateDateRange(start, end) {
  const dates = [];
  let current = new Date(
    parseInt(start.slice(0, 4)),
    parseInt(start.slice(4, 6)) - 1,
    parseInt(start.slice(6, 8))
  );
  const endDate = new Date(
    parseInt(end.slice(0, 4)),
    parseInt(end.slice(4, 6)) - 1,
    parseInt(end.slice(6, 8))
  );
  
  while (current <= endDate) {
    const y = current.getFullYear();
    const m = String(current.getMonth() + 1).padStart(2, "0");
    const d = String(current.getDate()).padStart(2, "0");
    dates.push(`${y}${m}${d}`);
    current.setDate(current.getDate() + 1);
  }
  
  return dates;
}

function findSourceDate(existingDates, targetDate, method) {
  if (!existingDates || existingDates.length === 0) return null;
  
  if (method === "forward") {
    // Find the closest date before targetDate
    for (let i = existingDates.length - 1; i >= 0; i--) {
      if (existingDates[i] < targetDate) return existingDates[i];
    }
  } else if (method === "backward") {
    // Find the closest date after targetDate
    for (const date of existingDates) {
      if (date > targetDate) return date;
    }
  } else if (method === "nearest") {
    // Find the nearest date
    let nearest = null;
    let minDiff = Infinity;
    for (const date of existingDates) {
      const diff = Math.abs(parseInt(date) - parseInt(targetDate));
      if (diff < minDiff) {
        minDiff = diff;
        nearest = date;
      }
    }
    return nearest;
  }
  
  return null;
}

function findKeyForSymbolDate(keys, symbol, date, version) {
  const symbolLower = symbol.toLowerCase();
  
  for (const key of keys) {
    const hasSymbol = key.includes(`symbol=${symbol}`) || 
                      key.includes(`symbol=${symbolLower}`) ||
                      (["legacy", "v3-ready", "v2-nodepth"].includes(version) && key.includes(`/${symbol}/`));
    const hasDate = key.includes(`date=${date}`) || 
                    (["legacy", "v3-ready", "v2-nodepth"].includes(version) && key.includes(`/${date}/`));
    
    if (hasSymbol && hasDate) return key;
  }
  
  return null;
}
