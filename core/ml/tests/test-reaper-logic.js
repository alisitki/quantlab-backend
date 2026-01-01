import 'dotenv/config';
import { SCHEDULER_CONFIG } from '../../scheduler/config.js';

async function runTests() {
  console.log('--- Orphan Reaper v1 Unit Tests ---');

  // Logic to test (extracted from reaper script)
  function evaluateReap(inst, lease, now) {
    const createdAt = new Date(inst.start_date * 1000);
    const ageMin = (now - createdAt.getTime()) / (60 * 1000);

    if (ageMin > 60) {
      return `Max age exceeded (${ageMin.toFixed(1)}m > 60m)`;
    }

    if (!lease) {
      return 'No S3 lease file found';
    }

    const lastHeartbeat = new Date(lease.lastHeartbeatAt);
    const idleMin = (now - lastHeartbeat.getTime()) / (60 * 1000);

    if (idleMin > 15) {
      return `Stale lease (last heartbeat ${idleMin.toFixed(1)}m ago > 15m)`;
    }

    return null;
  }

  const now = Date.now();

  // Test Case 1: Healthy Instance
  console.log('\n1. Testing healthy instance...');
  const inst1 = { id: 101, start_date: (now - 10 * 60 * 1000) / 1000 };
  const lease1 = { lastHeartbeatAt: new Date(now - 1 * 60 * 1000).toISOString() };
  const r1 = evaluateReap(inst1, lease1, now);
  console.log(`   Result: ${r1 || 'Active (Correct)'}`);
  if (r1 !== null) throw new Error('Healthy instance marked for reap');

  // Test Case 2: No Lease
  console.log('\n2. Testing missing lease...');
  const r2 = evaluateReap(inst1, null, now);
  console.log(`   Result: ${r2}`);
  if (r2 !== 'No S3 lease file found') throw new Error('Missing lease not detected');

  // Test Case 3: Stale Heartbeat (20m)
  console.log('\n3. Testing stale heartbeat (20m)...');
  const lease3 = { lastHeartbeatAt: new Date(now - 20 * 60 * 1000).toISOString() };
  const r3 = evaluateReap(inst1, lease3, now);
  console.log(`   Result: ${r3}`);
  if (!r3?.includes('Stale lease')) throw new Error('Stale heartbeat not detected');

  // Test Case 4: Max Age (70m)
  console.log('\n4. Testing max age (70m)...');
  const inst4 = { id: 104, start_date: (now - 70 * 60 * 1000) / 1000 };
  const lease4 = { lastHeartbeatAt: new Date(now - 1 * 60 * 1000).toISOString() };
  const r4 = evaluateReap(inst4, lease4, now);
  console.log(`   Result: ${r4}`);
  if (!r4?.includes('Max age exceeded')) throw new Error('Max age not detected');

  console.log('\n✅ ALL REAPER LOGIC TESTS PASSED');
}

runTests().catch(err => {
  console.error('\n❌ REAPER TESTS FAILED');
  console.error(err);
  process.exit(1);
});
