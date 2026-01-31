/**
 * QuantLab Replay Engine — S3 Credential Smoke Test
 * 
 * Verifies that MetaLoader and ParquetReader correctly throw errors
 * when required S3_COMPACT_* environment variables are missing.
 */

import { loadMeta } from './MetaLoader.js';
import { ParquetReader } from './ParquetReader.js';

async function testMetaLoader() {
  console.log('Testing MetaLoader credential guardrail...');
  
  // Backup and clear env
  const backup = {
    endpoint: process.env.S3_COMPACT_ENDPOINT,
    access: process.env.S3_COMPACT_ACCESS_KEY,
    secret: process.env.S3_COMPACT_SECRET_KEY
  };
  
  delete process.env.S3_COMPACT_ENDPOINT;
  delete process.env.S3_COMPACT_ACCESS_KEY;
  delete process.env.S3_COMPACT_SECRET_KEY;
  
  try {
    await loadMeta('s3://quantlab-compact/test/meta.json');
    console.error('FAIL: MetaLoader should have thrown an error');
    process.exit(1);
  } catch (err) {
    if (err.message.includes('CREDENTIAL_ERROR')) {
      console.log('PASS: MetaLoader threw correct error:', err.message);
    } else {
      console.error('FAIL: MetaLoader threw unexpected error:', err.message);
      process.exit(1);
    }
  } finally {
    // Restore env
    process.env.S3_COMPACT_ENDPOINT = backup.endpoint;
    process.env.S3_COMPACT_ACCESS_KEY = backup.access;
    process.env.S3_COMPACT_SECRET_KEY = backup.secret;
  }
}

async function testParquetReader() {
  console.log('\nTesting ParquetReader credential guardrail...');
  
  // Backup and clear env
  const backup = {
    endpoint: process.env.S3_COMPACT_ENDPOINT,
    access: process.env.S3_COMPACT_ACCESS_KEY,
    secret: process.env.S3_COMPACT_SECRET_KEY
  };
  
  delete process.env.S3_COMPACT_ENDPOINT;
  delete process.env.S3_COMPACT_ACCESS_KEY;
  delete process.env.S3_COMPACT_SECRET_KEY;
  
  const reader = new ParquetReader('s3://quantlab-compact/test/data.parquet');
  
  try {
    await reader.init();
    console.error('FAIL: ParquetReader should have thrown an error');
    process.exit(1);
  } catch (err) {
    if (err.message.includes('CREDENTIAL_ERROR')) {
      console.log('PASS: ParquetReader threw correct error:', err.message);
    } else {
      console.error('FAIL: ParquetReader threw unexpected error:', err.message);
      process.exit(1);
    }
  } finally {
    // Restore env
    process.env.S3_COMPACT_ENDPOINT = backup.endpoint;
    process.env.S3_COMPACT_ACCESS_KEY = backup.access;
    process.env.S3_COMPACT_SECRET_KEY = backup.secret;
  }
}

async function runTests() {
  try {
    await testMetaLoader();
    await testParquetReader();
    console.log('\nALL CREDENTIAL TESTS PASSED ✅');
    process.exit(0);
  } catch (err) {
    console.error('UNEXPECTED ERROR:', err);
    process.exit(1);
  }
}

runTests();
