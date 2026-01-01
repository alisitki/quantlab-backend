#!/usr/bin/env node
/**
 * test-promoter.js: Verification for Promoter decision logic.
 */

// Test the Promoter logic in isolation (no S3 dependency)
// Test the Promoter logic in isolation (no S3 dependency)
class MockPromoter {
  async evaluate(symbol, newMetrics, jobId, options = {}) {
    let { mode = 'off', canary = false } = options;
    
    // Canary Guard
    if (canary && mode === 'auto') {
      console.log(`[PROMOTE] canary run: auto->dry (blocked) for ${jobId}`);
      mode = 'dry';
    }
    
    if (mode === 'off') {
      return { decision: 'off', mode };
    }
    
    // Mocking current metrics as null for simplicity in some tests or fixed value
    const currentMetrics = options.currentMetrics || null;
    const decision = this.compare(newMetrics, currentMetrics);
    
    let promotionStatus = decision.promote ? 'passed' : 'rejected';
    
    if (decision.promote) {
      if (mode === 'auto') {
        promotionStatus = 'promoted';
      } else {
        promotionStatus = 'dry_pass';
      }
    }
    
    return {
      symbol,
      jobId,
      mode,
      decision: promotionStatus,
      reason: decision.reason
    };
  }

  compare(newMetrics, currentMetrics) {
    if (!currentMetrics) {
      return { promote: true, reason: 'First model' };
    }
    const newHitRate = newMetrics.directionalHitRate ?? 0;
    const curHitRate = currentMetrics.directionalHitRate ?? 0;
    
    if (newHitRate > curHitRate) {
      return { promote: true, reason: 'Better hit rate' };
    }
    return { promote: false, reason: 'Not better' };
  }
}

async function runTest() {
  console.log('--- Promotion Guard v2 Verification ---\n');
  let passed = 0;
  let failed = 0;
  
  const promoter = new MockPromoter();
  
  // Helper to assert
  const assert = (name, condition) => {
    if (condition) {
      console.log(`✅ ${name}: SUCCESS`);
      passed++;
    } else {
      console.log(`❌ ${name}: FAILED`);
      failed++;
    }
  };

  // 1. Default (off) mode
  console.log('1. Testing Default (off) mode...');
  const res1 = await promoter.evaluate('BTC', { directionalHitRate: 0.6 }, 'job1', { mode: 'off' });
  assert('Off mode never promotes', res1.decision === 'off');

  // 2. Dry mode (pass)
  console.log('\n2. Testing Dry mode (pass)...');
  const res2 = await promoter.evaluate('BTC', { directionalHitRate: 0.6 }, 'job2', { mode: 'dry' });
  assert('Dry mode returns dry_pass on success', res2.decision === 'dry_pass');

  // 3. Auto mode (pass)
  console.log('\n3. Testing Auto mode (pass)...');
  const res3 = await promoter.evaluate('BTC', { directionalHitRate: 0.6 }, 'job3', { mode: 'auto' });
  assert('Auto mode promotes on success', res3.decision === 'promoted');

  // 4. Canary downgrade (auto -> dry)
  console.log('\n4. Testing Canary downgrade...');
  const res4 = await promoter.evaluate('BTC', { directionalHitRate: 0.6 }, 'job4', { mode: 'auto', canary: true });
  assert('Canary downgrades auto to dry', res4.mode === 'dry' && res4.decision === 'dry_pass');

  // 5. Canary with dry mode
  console.log('\n5. Testing Canary with dry mode...');
  const res5 = await promoter.evaluate('BTC', { directionalHitRate: 0.6 }, 'job5', { mode: 'dry', canary: true });
  assert('Canary stays dry if already dry', res5.mode === 'dry' && res5.decision === 'dry_pass');

  // 6. Auto mode (reject)
  console.log('\n6. Testing Auto mode (reject)...');
  const res6 = await promoter.evaluate('BTC', { directionalHitRate: 0.4 }, 'job6', { 
    mode: 'auto', 
    currentMetrics: { directionalHitRate: 0.5 } 
  });
  assert('Auto mode rejects if not better', res6.decision === 'rejected');

  // Summary
  console.log('\n--- Summary ---');
  console.log(`Passed: ${passed}`);
  console.log(`Failed: ${failed}`);
  
  if (failed > 0) {
    process.exit(1);
  }
}

runTest().catch(err => {
  console.error('Test failed:', err);
  process.exit(1);
});
