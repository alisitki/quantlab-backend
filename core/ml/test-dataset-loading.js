import { DatasetBuilder } from './dataset/DatasetBuilder.js';
import pkg from 'parquetjs-lite';
const { ParquetWriter, ParquetSchema } = pkg;
import { rm, mkdir } from 'node:fs/promises';
import path from 'node:path';
import assert from 'node:assert';

const TMP_DIR = './tmp-test-dataset';
const TEST_PARQUET = path.join(TMP_DIR, 'test.parquet');

const SCHEMA = new ParquetSchema({
  ts_event: { type: 'INT64' },
  seq: { type: 'INT64' },
  f_mid: { type: 'DOUBLE' },
  f_spread: { type: 'DOUBLE' },
  label_dir_10s: { type: 'INT32' },
  other_col: { type: 'UTF8' }
});

async function runTest() {
  console.log('--- DatasetBuilder.loadFromParquet Verification ---');
  
  try {
    await mkdir(TMP_DIR, { recursive: true });

    // 1. Create a dummy parquet file
    console.log(`[INIT] Creating test parquet: ${TEST_PARQUET}`);
    const writer = await ParquetWriter.openFile(SCHEMA, TEST_PARQUET);
    
    const rows = [];
    for (let i = 0; i < 25; i++) {
      const row = {
        ts_event: BigInt(1700000000000 + i * 1000),
        seq: BigInt(i),
        f_mid: 100.0 + i,
        f_spread: 0.1,
        label_dir_10s: i % 2,
        other_col: 'ignore_me'
      };
      rows.push(row);
      await writer.appendRow(row);
    }
    await writer.close();
    console.log('[INIT] Test parquet created.');

    // 2. Load using DatasetBuilder (Batch size 10)
    console.log('[TEST] Loading in batches of 10...');
    const builder = new DatasetBuilder();
    const ds = await builder.loadFromParquet(TEST_PARQUET, 10);

    // 3. Verify
    console.log(`[TEST] Loaded ${ds.X.length} rows.`);
    assert.strictEqual(ds.X.length, 25, 'Total rows mismatch');
    assert.strictEqual(ds.y.length, 25, 'Labels count mismatch');
    
    // Check features: only f_mid and f_spread should be there
    assert.deepStrictEqual(ds.meta.featureNames, ['f_mid', 'f_spread'], 'Feature selection mismatch');
    
    // Check first row values
    // f_mid = 100.0, f_spread = 0.1
    assert.strictEqual(ds.X[0][0], 100.0, 'First feature value mismatch');
    assert.strictEqual(ds.X[0][1], 0.1, 'Second feature value mismatch');
    assert.strictEqual(ds.y[0], 0, 'First label mismatch');
    
    // Check last row
    assert.strictEqual(ds.X[24][0], 124.0, 'Last feature value mismatch');
    assert.strictEqual(ds.y[24], 0, 'Last label mismatch');

    console.log('✅ DatasetBuilder.loadFromParquet: PASS');

  } catch (err) {
    console.error('❌ Test FAILED');
    console.error(err);
    process.exit(1);
  } finally {
    await rm(TMP_DIR, { recursive: true, force: true });
  }
}

runTest();
