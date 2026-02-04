#!/usr/bin/env node
/**
 * QuantLab Replay Engine — Resume Determinism Test
 * 
 * Verifies that chunked resume produces identical results to full replay.
 * Uses deterministic 20% slice strategy.
 * 
 * Usage: node verify-resume.js <parquet_path> <meta_path>
 */

import { createHash } from 'node:crypto';
import { ReplayEngine, encodeCursor } from '../index.js';

const BATCH_SIZE = 5000;
const CHUNK_COUNT = 5; // 20% each

// Canonical JSON for hashing (sorted keys, BigInt handled)
function canonicalJson(obj) {
  return JSON.stringify(obj, (_, v) => {
    if (typeof v === 'bigint') return v.toString();
    if (v && typeof v === 'object' && !Array.isArray(v)) {
      return Object.keys(v).sort().reduce((acc, k) => { acc[k] = v[k]; return acc; }, {});
    }
    return v;
  });
}

function sha256(data) {
  return createHash('sha256').update(data).digest('hex');
}

/**
 * Full replay - collect all events and hash
 */
async function fullReplay(parquetPath, metaPath) {
  const engine = new ReplayEngine(parquetPath, metaPath);
  const events = [];
  
  try {
    await engine.validate();
    
    for await (const row of engine.replay({ batchSize: BATCH_SIZE })) {
      events.push(canonicalJson(row));
    }
  } finally {
    await engine.close();
  }
  
  return {
    eventCount: events.length,
    hash: sha256(events.join('\n'))
  };
}

/**
 * Chunked replay - replay in 20% chunks using cursor resume
 */
async function chunkedReplay(parquetPath, metaPath, targetChunkCount) {
  const engine = new ReplayEngine(parquetPath, metaPath);
  const allEvents = [];
  const chunkResults = [];
  
  try {
    const meta = await engine.validate();
    const totalRows = meta.rows;
    const chunkSize = Math.ceil(totalRows / targetChunkCount);
    
    console.log(`\nTotal rows: ${totalRows}`);
    console.log(`Target chunk size: ${chunkSize} (20% of dataset)`);
    console.log('');
    
    let currentCursor = null;
    let chunkIndex = 0;
    let processedTotal = 0;
    
    while (processedTotal < totalRows && chunkIndex < targetChunkCount) {
      const chunkEvents = [];
      let chunkProcessed = 0;
      
      // Create new engine for each chunk to simulate independent resume
      const chunkEngine = new ReplayEngine(parquetPath, metaPath);
      await chunkEngine.validate();
      
      const replayOpts = {
        batchSize: BATCH_SIZE,
        cursor: currentCursor
      };
      
      for await (const row of chunkEngine.replay(replayOpts)) {
        chunkEvents.push(canonicalJson(row));
        allEvents.push(canonicalJson(row));
        chunkProcessed++;
        processedTotal++;
        
        // Stop at chunk boundary
        if (chunkProcessed >= chunkSize && chunkIndex < targetChunkCount - 1) {
          // Create cursor for next chunk
          currentCursor = encodeCursor(row);
          break;
        }
      }
      
      await chunkEngine.close();
      
      const chunkPercent = ((chunkIndex + 1) * 20);
      const chunkResult = {
        index: chunkIndex,
        range: `${chunkIndex * 20}-${chunkPercent}%`,
        rows: chunkProcessed,
        cursor: currentCursor ? currentCursor.slice(0, 20) + '...' : null
      };
      chunkResults.push(chunkResult);
      
      console.log(`CHUNK_${chunkIndex + 1} (${chunkResult.range}): rows=${chunkProcessed}, cursor=${chunkResult.cursor || 'END'}`);
      
      chunkIndex++;
      
      // Safety: if no events processed, dataset is exhausted
      if (chunkProcessed === 0) break;
    }
    
  } finally {
    await engine.close();
  }
  
  return {
    eventCount: allEvents.length,
    hash: sha256(allEvents.join('\n')),
    chunks: chunkResults
  };
}

async function main() {
  const [,, parquetPath, metaPath] = process.argv;

  if (!parquetPath || !metaPath) {
    console.error('Usage: node verify-resume.js <parquet_path> <meta_path>');
    console.error('');
    console.error('Examples:');
    console.error('  node verify-resume.js /tmp/test/data.parquet /tmp/test/meta.json');
    console.error('  node verify-resume.js s3://bucket/path/data.parquet s3://bucket/path/meta.json');
    process.exit(1);
  }

  console.log('='.repeat(60));
  console.log('RESUME DETERMINISM TEST');
  console.log('='.repeat(60));
  console.log(`PARQUET: ${parquetPath}`);
  console.log(`META:    ${metaPath}`);
  console.log(`BATCH_SIZE: ${BATCH_SIZE}`);
  console.log(`CHUNK_COUNT: ${CHUNK_COUNT} (20% slices)`);

  // Phase 1: Full replay
  console.log('\n--- PHASE 1: FULL REPLAY ---');
  const fullResult = await fullReplay(parquetPath, metaPath);
  console.log(`FULL_REPLAY_ROWS: ${fullResult.eventCount}`);
  console.log(`FULL_REPLAY_HASH: ${fullResult.hash}`);

  // Phase 2: Chunked replay with cursor resume
  console.log('\n--- PHASE 2: CHUNKED REPLAY (5 x 20%) ---');
  const chunkedResult = await chunkedReplay(parquetPath, metaPath, CHUNK_COUNT);
  console.log('');
  console.log(`CHUNKED_TOTAL_ROWS: ${chunkedResult.eventCount}`);
  console.log(`CHUNKED_COMBINED_HASH: ${chunkedResult.hash}`);

  // Phase 3: Comparison
  console.log('\n--- COMPARISON ---');
  console.log(`FULL_HASH:    ${fullResult.hash.slice(0, 32)}...`);
  console.log(`CHUNKED_HASH: ${chunkedResult.hash.slice(0, 32)}...`);
  
  const rowsMatch = fullResult.eventCount === chunkedResult.eventCount;
  const hashMatch = fullResult.hash === chunkedResult.hash;
  
  console.log('');
  console.log(`ROWS_MATCH: ${rowsMatch ? 'true ✓' : 'false ✗'}`);
  console.log(`HASH_MATCH: ${hashMatch ? 'true ✓' : 'false ✗'}`);
  
  if (rowsMatch && hashMatch) {
    console.log('\n=== RESULT: PASS ✓ ===');
    console.log('Chunked resume produces identical results to full replay.');
    console.log('Cursor-based resume is DETERMINISTIC.');
  } else {
    console.log('\n=== RESULT: FAIL ✗ ===');
    if (!rowsMatch) {
      console.log(`Row count mismatch: full=${fullResult.eventCount}, chunked=${chunkedResult.eventCount}`);
    }
    if (!hashMatch) {
      console.log('Hash mismatch: event sequence differs between full and chunked replay.');
    }
    process.exit(1);
  }
}

main().catch(err => {
  console.error('FATAL:', err);
  process.exit(1);
});
