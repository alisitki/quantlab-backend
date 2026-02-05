/**
 * Unit tests for FillsStream
 * Validates buffered JSONL write/read with BigInt serialization
 */

import { FillsStream } from '../FillsStream.js';
import fs from 'fs';
import crypto from 'crypto';

// Test utilities
function assert(condition, message) {
  if (!condition) {
    throw new Error(`Assertion failed: ${message}`);
  }
}

function createMockFill(id, symbol = 'BTCUSDT', side = 'BUY') {
  return {
    id: `fill_${id}`,
    orderId: `ord_${id}`,
    symbol,
    side,
    qty: 1.5,
    fillPrice: 50000 + id,
    fillValue: (50000 + id) * 1.5,
    fee: 10 + id * 0.1,
    ts_event: BigInt(1000000 + id * 1000)
  };
}

function getTempPath() {
  const random = crypto.randomBytes(8).toString('hex');
  return `/tmp/test-fills-${random}.jsonl`;
}

// Test 1: Basic write and read
async function testBasicWriteRead() {
  console.log('\n[TEST 1] Basic write and read');

  const filePath = getTempPath();
  const stream = new FillsStream(filePath, 10);  // Small buffer for testing

  // Write 5 fills
  for (let i = 0; i < 5; i++) {
    stream.writeFill(createMockFill(i));
  }

  await stream.close();

  // Read back
  const fills = FillsStream.readFills(filePath);

  console.log(`  Wrote: 5 fills`);
  console.log(`  Read: ${fills.length} fills`);

  assert(fills.length === 5, 'Should read 5 fills');
  assert(fills[0].id === 'fill_0', 'First fill ID should match');
  assert(fills[4].id === 'fill_4', 'Last fill ID should match');

  // Verify BigInt restoration
  assert(typeof fills[0].ts_event === 'bigint', 'ts_event should be BigInt');
  assert(fills[0].ts_event === BigInt(1000000), 'ts_event value should match');

  // Cleanup
  fs.unlinkSync(filePath);

  console.log('  ✅ PASS');
}

// Test 2: Buffered flushing
async function testBufferedFlushing() {
  console.log('\n[TEST 2] Buffered flushing');

  const filePath = getTempPath();
  const bufferSize = 100;
  const stream = new FillsStream(filePath, bufferSize);

  // Write exactly buffer size fills
  for (let i = 0; i < bufferSize; i++) {
    stream.writeFill(createMockFill(i));
  }

  // Buffer should have auto-flushed (but async write may not be complete yet)
  // We'll verify after close()

  // Write one more (triggers another flush)
  stream.writeFill(createMockFill(bufferSize));

  await stream.close();

  const fills = FillsStream.readFills(filePath);
  console.log(`  Total fills read: ${fills.length}`);

  assert(fills.length === bufferSize + 1, `Should read ${bufferSize + 1} fills`);

  // Cleanup
  fs.unlinkSync(filePath);

  console.log('  ✅ PASS');
}

// Test 3: BigInt serialization round-trip
async function testBigIntSerialization() {
  console.log('\n[TEST 3] BigInt serialization round-trip');

  const filePath = getTempPath();
  const stream = new FillsStream(filePath);

  // Create fills with various BigInt values
  const testValues = [
    BigInt(0),
    BigInt(1000000),
    BigInt(Number.MAX_SAFE_INTEGER),
    BigInt('9007199254740992'),  // MAX_SAFE_INTEGER + 1
    BigInt('1234567890123456789')
  ];

  for (let i = 0; i < testValues.length; i++) {
    const fill = createMockFill(i);
    fill.ts_event = testValues[i];
    stream.writeFill(fill);
  }

  await stream.close();

  const fills = FillsStream.readFills(filePath);

  console.log(`  Test values: ${testValues.length}`);
  console.log(`  Fills read: ${fills.length}`);

  for (let i = 0; i < testValues.length; i++) {
    console.log(`    [${i}] Original: ${testValues[i]}, Read: ${fills[i].ts_event}`);
    assert(fills[i].ts_event === testValues[i], `BigInt round-trip failed for value ${testValues[i]}`);
  }

  // Cleanup
  fs.unlinkSync(filePath);

  console.log('  ✅ PASS');
}

// Test 4: Empty file handling
async function testEmptyFile() {
  console.log('\n[TEST 4] Empty file handling');

  const filePath = getTempPath();
  const stream = new FillsStream(filePath);
  await stream.close();  // Close without writing

  const fills = FillsStream.readFills(filePath);

  console.log(`  Fills read from empty file: ${fills.length}`);
  assert(fills.length === 0, 'Should read 0 fills from empty file');

  // Cleanup
  fs.unlinkSync(filePath);

  console.log('  ✅ PASS');
}

// Test 5: Non-existent file handling
async function testNonExistentFile() {
  console.log('\n[TEST 5] Non-existent file handling');

  const filePath = '/tmp/non-existent-file.jsonl';

  const fills = FillsStream.readFills(filePath);

  console.log(`  Fills read from non-existent file: ${fills.length}`);
  assert(fills.length === 0, 'Should read 0 fills from non-existent file');

  console.log('  ✅ PASS');
}

// Test 6: Large volume (memory efficiency)
async function testLargeVolume() {
  console.log('\n[TEST 6] Large volume (memory efficiency)');

  const filePath = getTempPath();
  const stream = new FillsStream(filePath, 100);

  const numFills = 10000;

  console.log(`  Writing ${numFills} fills...`);

  for (let i = 0; i < numFills; i++) {
    stream.writeFill(createMockFill(i));
  }

  await stream.close();

  const stats = fs.statSync(filePath);
  console.log(`  File size: ${(stats.size / 1024).toFixed(2)} KB`);
  console.log(`  Avg bytes per fill: ${(stats.size / numFills).toFixed(2)}`);

  console.log(`  Reading back...`);
  const fills = FillsStream.readFills(filePath);

  console.log(`  Fills read: ${fills.length}`);

  assert(fills.length === numFills, `Should read ${numFills} fills`);
  assert(fills[0].id === 'fill_0', 'First fill should match');
  assert(fills[numFills - 1].id === `fill_${numFills - 1}`, 'Last fill should match');

  // Cleanup
  fs.unlinkSync(filePath);

  console.log('  ✅ PASS');
}

// Test 7: Error handling - write after close
async function testWriteAfterClose() {
  console.log('\n[TEST 7] Error handling - write after close');

  const filePath = getTempPath();
  const stream = new FillsStream(filePath);

  await stream.close();

  try {
    stream.writeFill(createMockFill(0));
    assert(false, 'Should throw error when writing after close');
  } catch (error) {
    console.log(`  Error caught: ${error.message}`);
    assert(error.message.includes('closed'), 'Error should mention closed stream');
  }

  // Cleanup
  fs.unlinkSync(filePath);

  console.log('  ✅ PASS');
}

// Run all tests
async function runTests() {
  console.log('=== FillsStream Unit Tests ===');

  try {
    await testBasicWriteRead();
    await testBufferedFlushing();
    await testBigIntSerialization();
    await testEmptyFile();
    await testNonExistentFile();
    await testLargeVolume();
    await testWriteAfterClose();

    console.log('\n✅ All FillsStream tests passed!');
    process.exit(0);
  } catch (error) {
    console.error('\n❌ Test failed:', error.message);
    console.error(error.stack);
    process.exit(1);
  }
}

runTests();
