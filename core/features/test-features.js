import { FeatureRegistry } from './FeatureRegistry.js';
import assert from 'assert';

async function testFeatureBuilder() {
  console.log('🧪 Testing FeatureBuilder...');

  const symbol = 'btcusdt';
  const builder = FeatureRegistry.createFeatureBuilder(symbol, {
    enabledFeatures: ['mid_price', 'spread', 'return_1', 'volatility'],
    volatility: { windowSize: 3 } // Small window for testing
  });

  const events = [
    { bid_price: 100, ask_price: 102, ts_event: 1000 }, // mid=101, spread=2, ret=null, vol=null
    { bid_price: 102, ask_price: 104, ts_event: 2000 }, // mid=103, spread=2, ret=0.0198, vol=null
    { bid_price: 104, ask_price: 106, ts_event: 3000 }, // mid=105, spread=2, ret=0.0194, vol=null
    { bid_price: 106, ask_price: 108, ts_event: 4000 }, // mid=107, spread=2, ret=0.0190, vol=0.0003...
  ];

  console.log('1. Testing Warm-up Behavior...');
  let res1 = builder.onEvent(events[0]);
  assert.strictEqual(res1, null, 'Should return null on 1st event');

  let res2 = builder.onEvent(events[1]);
  assert.strictEqual(res2, null, 'Should return null on 2nd event (volatility needs 3 returns)');

  let res3 = builder.onEvent(events[2]);
  assert.strictEqual(res3, null, 'Should return null on 3rd event (volatility needs 3 returns)');

  let res4 = builder.onEvent(events[3]);
  assert.notStrictEqual(res4, null, 'Should be warm on 4th event');
  assert.strictEqual(res4.mid_price, 107);
  assert.strictEqual(res4.spread, 2);
  assert.ok(res4.return_1 > 0);
  assert.ok(res4.volatility > 0);
  console.log('✅ Warm-up Behavior OK');

  console.log('2. Testing Reset Correctness...');
  builder.reset();
  let res5 = builder.onEvent(events[0]);
  assert.strictEqual(res5, null, 'Should return null after reset');
  console.log('✅ Reset Correctness OK');

  console.log('3. Testing Determinism...');
  builder.reset();
  const results1 = events.map(e => builder.onEvent(e));
  
  builder.reset();
  const results2 = events.map(e => builder.onEvent(e));

  assert.deepStrictEqual(results1, results2, 'Results should be identical for same input');
  console.log('✅ Determinism OK');

  console.log('DONE.');
}

testFeatureBuilder().catch(err => {
  console.error('❌ Test Failed:', err);
  process.exit(1);
});
