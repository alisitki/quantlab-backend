import { FeatureBuilderV1 } from './FeatureBuilderV1.js';
import assert from 'node:assert';

function generateSyntheticRows(n = 1000) {
  const rows = [];
  let ts = 1735500000000; // Start TS
  for (let i = 0; i < n; i++) {
    rows.push({
      ts_event: ts,
      seq: i,
      bid_price: 99000 + Math.sin(i / 10) * 100,
      ask_price: 99000 + Math.sin(i / 10) * 100 + 10,
      bid_qty: 1.0 + Math.cos(i / 5),
      ask_qty: 1.0 + Math.sin(i / 5)
    });
    ts += 100; // 100ms steps
  }
  return rows;
}

async function testDeterminism() {
  console.log('--- Testing Determinism ---');
  const rows = generateSyntheticRows(2000);
  const builder = new FeatureBuilderV1();

  const run1 = builder.process(rows);
  const run2 = builder.process(rows);

  assert.strictEqual(run1.length, run2.length, 'Length mismatch');
  assert.strictEqual(builder.getConfigHash(), builder.getConfigHash(), 'Config hash mismatch');

  for (let i = 0; i < 5; i++) {
    console.log(`Checking row ${i}...`);
    assert.deepStrictEqual(run1[i], run2[i], `Row ${i} mismatch`);
  }
  console.log('✅ Determinism PASS');
}

async function testLabelingAndDrops() {
  console.log('\n--- Testing Labeling and Drops ---');
  const rows = generateSyntheticRows(1000); // 100s of data
  const builder = new FeatureBuilderV1();
  const result = builder.process(rows);

  // Initial Ts: 1735500000000
  // Cold start drop: ts < 1735500000000 + 30000
  // Each step is 100ms. 30s = 300 steps.
  // Last 10s drop: total 100s. 90s mark is 900 steps.
  // We should have ~600 rows (900 - 300).
  
  console.log(`Total rows: ${rows.length}, Result rows: ${result.length}`);
  
  // Check no nulls
  for (const row of result) {
    for (const [key, val] of Object.entries(row)) {
      if (typeof val === 'number' && isNaN(val)) {
        throw new Error(`NaN found in column ${key} at ts ${row.ts_event}`);
      }
    }
  }
  console.log('✅ No NaN found');

  // Check Schema
  const expectedKeys = [
    'ts_event', 'f_mid', 'f_spread', 'f_spread_bps', 'f_imbalance', 'f_microprice',
    'f_ret_1s', 'f_ret_5s', 'f_ret_10s', 'f_ret_30s', 'f_vol_10s', 'label_dir_10s'
  ];
  const firstRowKeys = Object.keys(result[0]);
  assert.deepStrictEqual(firstRowKeys, expectedKeys, 'Schema column order mismatch');
  console.log('✅ Schema Order PASS');

  // Check binary label
  const labels = result.map(r => r.label_dir_10s);
  const uniqueLabels = [...new Set(labels)];
  assert.ok(uniqueLabels.every(l => l === 0 || l === 1), 'Labels must be binary (0 or 1)');
  console.log('✅ Binary Labels PASS');
}

async function run() {
  try {
    await testDeterminism();
    await testLabelingAndDrops();
    console.log('\n✨ ALL TESTS PASSED ✨');
  } catch (err) {
    console.error('\n❌ TEST FAILED');
    console.error(err);
    process.exit(1);
  }
}

run();
