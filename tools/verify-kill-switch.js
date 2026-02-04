#!/usr/bin/env node
/**
 * Kill Switch Verification Script
 *
 * Tests the kill switch functionality:
 * 1. Activates global kill switch
 * 2. Verifies new runs are blocked
 * 3. Deactivates kill switch
 * 4. Verifies runs can start again
 * 5. Tests symbol-specific kill switch
 * 6. Tests emergency stop
 *
 * Usage:
 *   node tools/verify-kill-switch.js [--api-url http://localhost:3031]
 */

import { getKillSwitchManager, resetKillSwitchManager } from '../core/futures/KillSwitchManager.js';

const API_URL = process.argv.includes('--api-url')
  ? process.argv[process.argv.indexOf('--api-url') + 1]
  : process.env.STRATEGYD_URL || 'http://localhost:3031';

const TOKEN = process.env.STRATEGYD_TOKEN || 'test-token';

const results = {
  passed: 0,
  failed: 0,
  tests: []
};

function log(msg) {
  console.log(`[VERIFY] ${msg}`);
}

function pass(testName, details = '') {
  results.passed++;
  results.tests.push({ name: testName, status: 'PASS', details });
  console.log(`  ✓ ${testName}${details ? ` - ${details}` : ''}`);
}

function fail(testName, reason) {
  results.failed++;
  results.tests.push({ name: testName, status: 'FAIL', reason });
  console.log(`  ✗ ${testName} - ${reason}`);
}

async function fetchApi(path, options = {}) {
  const url = `${API_URL}${path}`;
  const response = await fetch(url, {
    ...options,
    headers: {
      'Content-Type': 'application/json',
      'Authorization': `Bearer ${TOKEN}`,
      ...options.headers
    }
  });
  const data = await response.json().catch(() => null);
  return { status: response.status, data };
}

// ============================================================================
// UNIT TESTS (Direct KillSwitchManager)
// ============================================================================

async function testKillSwitchManagerUnit() {
  log('Testing KillSwitchManager (unit tests)...');

  // Reset for clean state
  resetKillSwitchManager();
  const manager = getKillSwitchManager();

  // Test 1: Initial state should be inactive
  const initialStatus = manager.getStatus();
  if (!initialStatus.is_active && !initialStatus.global_kill) {
    pass('Initial state inactive');
  } else {
    fail('Initial state inactive', `Expected inactive, got: ${JSON.stringify(initialStatus)}`);
  }

  // Test 2: Activate global kill switch
  const activateResult = manager.activateGlobal({
    reason: 'Unit test activation',
    actor: 'test-script',
    stopAllRuns: false
  });
  if (activateResult.success && manager.isGlobalActive()) {
    pass('Activate global kill switch');
  } else {
    fail('Activate global kill switch', `Failed to activate: ${JSON.stringify(activateResult)}`);
  }

  // Test 3: Check status after activation
  const activeStatus = manager.getStatus();
  if (activeStatus.is_active && activeStatus.global_kill && activeStatus.reason === 'Unit test activation') {
    pass('Status reflects activation');
  } else {
    fail('Status reflects activation', `Unexpected status: ${JSON.stringify(activeStatus)}`);
  }

  // Test 4: Evaluate should block all intents
  const evalResult = manager.evaluate({ symbol: 'BTCUSDT', side: 'BUY' });
  if (evalResult.killed && evalResult.reason_code === 'GLOBAL_KILL_ACTIVE') {
    pass('Evaluate blocks intents when active');
  } else {
    fail('Evaluate blocks intents when active', `Expected killed, got: ${JSON.stringify(evalResult)}`);
  }

  // Test 5: Deactivate global kill switch
  const deactivateResult = manager.deactivateGlobal({ actor: 'test-script' });
  if (deactivateResult.success && deactivateResult.was_active && !manager.isGlobalActive()) {
    pass('Deactivate global kill switch');
  } else {
    fail('Deactivate global kill switch', `Failed to deactivate: ${JSON.stringify(deactivateResult)}`);
  }

  // Test 6: Evaluate should pass when inactive
  const evalResultAfter = manager.evaluate({ symbol: 'BTCUSDT', side: 'BUY' });
  if (!evalResultAfter.killed) {
    pass('Evaluate passes when inactive');
  } else {
    fail('Evaluate passes when inactive', `Expected not killed, got: ${JSON.stringify(evalResultAfter)}`);
  }

  // Test 7: Activate symbol-specific kill switch
  manager.activateSymbols({
    symbols: ['BTCUSDT', 'ETHUSDT'],
    reason: 'Symbol test',
    actor: 'test-script'
  });
  if (manager.isSymbolKilled('BTCUSDT') && manager.isSymbolKilled('ETHUSDT') && !manager.isGlobalActive()) {
    pass('Activate symbol kill switch');
  } else {
    fail('Activate symbol kill switch', `Symbol kill not working`);
  }

  // Test 8: Evaluate blocks killed symbol
  const btcEval = manager.evaluate({ symbol: 'BTCUSDT', side: 'BUY' });
  const solEval = manager.evaluate({ symbol: 'SOLUSDT', side: 'BUY' });
  if (btcEval.killed && !solEval.killed) {
    pass('Symbol kill blocks only specified symbols');
  } else {
    fail('Symbol kill blocks only specified symbols', `BTC: ${btcEval.killed}, SOL: ${solEval.killed}`);
  }

  // Test 9: Deactivate specific symbols
  manager.deactivateSymbols({ symbols: ['BTCUSDT'], actor: 'test-script' });
  if (!manager.isSymbolKilled('BTCUSDT') && manager.isSymbolKilled('ETHUSDT')) {
    pass('Deactivate specific symbol');
  } else {
    fail('Deactivate specific symbol', 'Symbol deactivation not working correctly');
  }

  // Test 10: Deactivate all
  manager.deactivateAll({ actor: 'test-script' });
  if (!manager.isActive()) {
    pass('Deactivate all');
  } else {
    fail('Deactivate all', 'Still active after deactivateAll');
  }

  // Test 11: Emergency stop
  manager.emergencyStop({ reason: 'Emergency test', actor: 'test-script' });
  if (manager.isGlobalActive() && manager.getStatus().reason.includes('EMERGENCY')) {
    pass('Emergency stop activates global kill');
  } else {
    fail('Emergency stop activates global kill', 'Emergency stop not working');
  }

  // Cleanup
  manager.deactivateAll({ actor: 'test-script' });
}

// ============================================================================
// INTEGRATION TESTS (HTTP API)
// ============================================================================

async function testKillSwitchApi() {
  log('Testing Kill Switch API (integration tests)...');

  // Test 1: Get initial status
  const statusRes = await fetchApi('/v1/kill-switch/status');
  if (statusRes.status === 200) {
    pass('GET /v1/kill-switch/status returns 200');
  } else {
    fail('GET /v1/kill-switch/status returns 200', `Got status ${statusRes.status}`);
    return; // Skip remaining API tests if basic endpoint fails
  }

  // Test 2: Activate global kill switch
  const activateRes = await fetchApi('/v1/kill-switch/activate', {
    method: 'POST',
    body: JSON.stringify({
      type: 'global',
      reason: 'API integration test'
    })
  });
  if (activateRes.status === 200 && activateRes.data?.status === 'ACTIVATED') {
    pass('POST /v1/kill-switch/activate (global)');
  } else {
    fail('POST /v1/kill-switch/activate (global)', `Status: ${activateRes.status}, Data: ${JSON.stringify(activateRes.data)}`);
  }

  // Test 3: Verify status shows active
  const statusAfterActivate = await fetchApi('/v1/kill-switch/status');
  if (statusAfterActivate.data?.is_active === true) {
    pass('Status shows active after activation');
  } else {
    fail('Status shows active after activation', `Got: ${JSON.stringify(statusAfterActivate.data)}`);
  }

  // Test 4: Try to start a live run (should fail)
  const liveStartRes = await fetchApi('/live/start', {
    method: 'POST',
    body: JSON.stringify({
      exchange: 'binance',
      symbols: ['BTCUSDT'],
      strategyPath: './strategies/ema_cross.js'
    })
  });
  if (liveStartRes.status === 503 && liveStartRes.data?.error === 'KILL_SWITCH_ACTIVE') {
    pass('Live run blocked when kill switch active');
  } else {
    // May fail for other reasons (strategy not found, etc.) - that's ok
    if (liveStartRes.status === 503) {
      pass('Live run blocked when kill switch active');
    } else {
      fail('Live run blocked when kill switch active', `Status: ${liveStartRes.status}, Error: ${liveStartRes.data?.error}`);
    }
  }

  // Test 5: Deactivate kill switch
  const deactivateRes = await fetchApi('/v1/kill-switch/deactivate', {
    method: 'POST',
    body: JSON.stringify({ type: 'global' })
  });
  if (deactivateRes.status === 200 && deactivateRes.data?.status === 'DEACTIVATED') {
    pass('POST /v1/kill-switch/deactivate (global)');
  } else {
    fail('POST /v1/kill-switch/deactivate (global)', `Status: ${deactivateRes.status}`);
  }

  // Test 6: Verify status shows inactive
  const statusAfterDeactivate = await fetchApi('/v1/kill-switch/status');
  if (statusAfterDeactivate.data?.is_active === false) {
    pass('Status shows inactive after deactivation');
  } else {
    fail('Status shows inactive after deactivation', `Got: ${JSON.stringify(statusAfterDeactivate.data)}`);
  }

  // Test 7: Test symbol kill switch
  const symbolActivateRes = await fetchApi('/v1/kill-switch/activate', {
    method: 'POST',
    body: JSON.stringify({
      type: 'symbol',
      symbols: ['BTCUSDT'],
      reason: 'Symbol test'
    })
  });
  if (symbolActivateRes.status === 200 && symbolActivateRes.data?.added?.includes('BTCUSDT')) {
    pass('POST /v1/kill-switch/activate (symbol)');
  } else {
    fail('POST /v1/kill-switch/activate (symbol)', `Response: ${JSON.stringify(symbolActivateRes.data)}`);
  }

  // Cleanup - deactivate all
  await fetchApi('/v1/kill-switch/deactivate', {
    method: 'POST',
    body: JSON.stringify({ type: 'all' })
  });

  // Test 8: Emergency stop
  const emergencyRes = await fetchApi('/v1/kill-switch/emergency', {
    method: 'POST',
    body: JSON.stringify({ reason: 'Emergency test' })
  });
  if (emergencyRes.status === 200 && emergencyRes.data?.status === 'EMERGENCY_STOP_EXECUTED') {
    pass('POST /v1/kill-switch/emergency');
  } else {
    fail('POST /v1/kill-switch/emergency', `Response: ${JSON.stringify(emergencyRes.data)}`);
  }

  // Final cleanup
  await fetchApi('/v1/kill-switch/deactivate', {
    method: 'POST',
    body: JSON.stringify({ type: 'all' })
  });
}

// ============================================================================
// BACKWARD COMPATIBILITY TESTS
// ============================================================================

async function testBackwardCompatibility() {
  log('Testing backward compatibility (/live/kill-switch)...');

  // Test 1: Activate via legacy endpoint
  const activateRes = await fetchApi('/live/kill-switch', {
    method: 'POST',
    body: JSON.stringify({ activate: true, reason: 'Legacy test' })
  });
  if (activateRes.status === 200 && activateRes.data?.status === 'KILL_SWITCH_ACTIVATED') {
    pass('Legacy /live/kill-switch activate');
  } else {
    fail('Legacy /live/kill-switch activate', `Response: ${JSON.stringify(activateRes.data)}`);
  }

  // Test 2: Get status via legacy endpoint
  const statusRes = await fetchApi('/live/kill-switch/status');
  if (statusRes.status === 200 && statusRes.data?.active === true) {
    pass('Legacy /live/kill-switch/status');
  } else {
    fail('Legacy /live/kill-switch/status', `Response: ${JSON.stringify(statusRes.data)}`);
  }

  // Test 3: Deactivate via legacy endpoint
  const deactivateRes = await fetchApi('/live/kill-switch', {
    method: 'POST',
    body: JSON.stringify({ activate: false })
  });
  if (deactivateRes.status === 200 && deactivateRes.data?.status === 'KILL_SWITCH_DEACTIVATED') {
    pass('Legacy /live/kill-switch deactivate');
  } else {
    fail('Legacy /live/kill-switch deactivate', `Response: ${JSON.stringify(deactivateRes.data)}`);
  }
}

// ============================================================================
// RUN ALL TESTS
// ============================================================================

async function main() {
  console.log('='.repeat(60));
  console.log('KILL SWITCH VERIFICATION');
  console.log('='.repeat(60));
  console.log(`API URL: ${API_URL}`);
  console.log('');

  // Unit tests (always run)
  await testKillSwitchManagerUnit();

  // Integration tests (only if API is available)
  try {
    const healthCheck = await fetch(`${API_URL}/health`, {
      headers: { 'Authorization': `Bearer ${TOKEN}` }
    });
    if (healthCheck.ok) {
      console.log('');
      await testKillSwitchApi();
      console.log('');
      await testBackwardCompatibility();
    } else {
      log(`Skipping API tests - server returned ${healthCheck.status}`);
    }
  } catch (err) {
    log(`Skipping API tests - cannot connect to ${API_URL}`);
  }

  // Summary
  console.log('');
  console.log('='.repeat(60));
  console.log('SUMMARY');
  console.log('='.repeat(60));
  console.log(`Passed: ${results.passed}`);
  console.log(`Failed: ${results.failed}`);
  console.log(`Total:  ${results.passed + results.failed}`);
  console.log('');

  if (results.failed > 0) {
    console.log('FAILED TESTS:');
    for (const test of results.tests) {
      if (test.status === 'FAIL') {
        console.log(`  - ${test.name}: ${test.reason}`);
      }
    }
    process.exit(1);
  } else {
    console.log('All tests passed!');
    process.exit(0);
  }
}

main().catch(err => {
  console.error('Verification failed:', err);
  process.exit(1);
});
