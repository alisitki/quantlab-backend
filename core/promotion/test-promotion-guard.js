/**
 * test-promotion-guard.js: Unit tests for PromotionGuard v1
 */
import { evaluate, logDecision, PROMOTION_RULES } from './PromotionGuard.js';

async function runTests() {
  console.log('=== PromotionGuard v1 Unit Tests ===\n');
  
  let passed = 0;
  let failed = 0;
  
  function assert(condition, testName) {
    if (condition) {
      console.log(`✅ ${testName}`);
      passed++;
    } else {
      console.log(`❌ ${testName}`);
      failed++;
    }
  }
  
  // Test 1: All rules pass
  console.log('\n--- Test 1: All Pass ---');
  const result1 = evaluate(
    { return_pct: 5.0, max_drawdown_pct: 10.0, trades: 50 },
    { return_pct: 2.0 }
  );
  assert(result1.safety_pass === true, 'safety_pass should be true');
  assert(result1.reasons.length === 0, 'reasons should be empty');
  assert(result1.metrics_snapshot.return_pct === 5.0, 'metrics_snapshot.return_pct correct');
  logDecision(result1, 'test-1');
  
  // Test 2: Negative return
  console.log('\n--- Test 2: Negative Return ---');
  const result2 = evaluate(
    { return_pct: -2.5, max_drawdown_pct: 10.0, trades: 50 },
    { return_pct: 2.0 }
  );
  assert(result2.safety_pass === false, 'safety_pass should be false');
  assert(result2.reasons.some(r => r.includes('Negative return')), 'reason should mention negative return');
  logDecision(result2, 'test-2');
  
  // Test 3: High drawdown (>= 25%)
  console.log('\n--- Test 3: High Drawdown ---');
  const result3 = evaluate(
    { return_pct: 10.0, max_drawdown_pct: 30.0, trades: 50 },
    { return_pct: 2.0 }
  );
  assert(result3.safety_pass === false, 'safety_pass should be false');
  assert(result3.reasons.some(r => r.includes('Drawdown exceeds')), 'reason should mention drawdown');
  logDecision(result3, 'test-3');
  
  // Test 4: Underperforms baseline
  console.log('\n--- Test 4: Underperforms Baseline ---');
  const result4 = evaluate(
    { return_pct: 3.0, max_drawdown_pct: 10.0, trades: 50 },
    { return_pct: 5.0 }
  );
  assert(result4.safety_pass === false, 'safety_pass should be false');
  assert(result4.reasons.some(r => r.includes('Underperforms baseline')), 'reason should mention baseline');
  logDecision(result4, 'test-4');
  
  // Test 5: Missing backtest summary
  console.log('\n--- Test 5: Missing Backtest Summary ---');
  const result5 = evaluate(null, { return_pct: 2.0 });
  assert(result5.safety_pass === false, 'safety_pass should be false');
  assert(result5.reasons.some(r => r.includes('Missing')), 'reason should mention missing');
  logDecision(result5, 'test-5');
  
  // Test 6: Missing baseline
  console.log('\n--- Test 6: Missing Baseline ---');
  const result6 = evaluate(
    { return_pct: 5.0, max_drawdown_pct: 10.0, trades: 50 },
    null
  );
  assert(result6.safety_pass === false, 'safety_pass should be false');
  assert(result6.reasons.some(r => r.includes('Missing')), 'reason should mention missing');
  logDecision(result6, 'test-6');
  
  // Test 7: Multiple failures
  console.log('\n--- Test 7: Multiple Failures ---');
  const result7 = evaluate(
    { return_pct: -5.0, max_drawdown_pct: 40.0, trades: 50 },
    { return_pct: 2.0 }
  );
  assert(result7.safety_pass === false, 'safety_pass should be false');
  assert(result7.reasons.length >= 2, 'should have multiple failure reasons');
  logDecision(result7, 'test-7');
  
  // Test 8: Edge case - exactly 0% return (should fail)
  console.log('\n--- Test 8: Zero Return (Edge Case) ---');
  const result8 = evaluate(
    { return_pct: 0.0, max_drawdown_pct: 10.0, trades: 50 },
    { return_pct: -1.0 }
  );
  assert(result8.safety_pass === false, 'safety_pass should be false (0% is not > 0%)');
  logDecision(result8, 'test-8');
  
  // Test 9: Edge case - exactly 25% drawdown (should fail)
  console.log('\n--- Test 9: Exactly 25% Drawdown (Edge Case) ---');
  const result9 = evaluate(
    { return_pct: 10.0, max_drawdown_pct: 25.0, trades: 50 },
    { return_pct: 2.0 }
  );
  assert(result9.safety_pass === false, 'safety_pass should be false (25% is not < 25%)');
  logDecision(result9, 'test-9');
  
  // Test 10: Verify rule constants
  console.log('\n--- Test 10: Rule Constants ---');
  assert(PROMOTION_RULES.MIN_RETURN_PCT === 0, 'MIN_RETURN_PCT should be 0');
  assert(PROMOTION_RULES.MAX_DRAWDOWN_PCT === 25, 'MAX_DRAWDOWN_PCT should be 25');
  
  // Summary
  console.log('\n=== Summary ===');
  console.log(`Passed: ${passed}/${passed + failed}`);
  console.log(`Failed: ${failed}/${passed + failed}`);
  
  if (failed > 0) {
    console.log('\n❌ TESTS FAILED');
    process.exit(1);
  } else {
    console.log('\n✅ ALL PROMOTION GUARD TESTS PASSED');
  }
}

runTests().catch(err => {
  console.error('Test runner error:', err);
  process.exit(1);
});
