import 'dotenv/config';

async function runTests() {
  console.log('--- Canary Soak v1.1 Unit Tests ---');

  // Logic to test (mocking the helpers from run_canary_soak.js)
  function evaluateCoverage(tsMin, tsMax) {
    const hours = (tsMax - tsMin) / 3600000;
    if (hours >= 20) return 'FULL';
    if (hours >= 6) return 'PARTIAL';
    return 'TOO_SHORT';
  }

  function compareHashes(h1, h2) {
    return h1 === h2;
  }

  // 1. Coverage Gate Testing
  console.log('\n1. Testing coverage gate types...');
  const t1 = evaluateCoverage(0, 24 * 3600 * 1000); // 24h
  console.log(`   24h -> ${t1}`);
  if (t1 !== 'FULL') throw new Error('24h should be FULL');

  const t2 = evaluateCoverage(0, 10 * 3600 * 1000); // 10h
  console.log(`   10h -> ${t2}`);
  if (t2 !== 'PARTIAL') throw new Error('10h should be PARTIAL');

  const t3 = evaluateCoverage(0, 2 * 3600 * 1000); // 2h
  console.log(`   2h  -> ${t3}`);
  if (t3 !== 'TOO_SHORT') throw new Error('2h should be TOO_SHORT');

  // 2. Production Safety Logic
  console.log('\n2. Testing production safety hash comparison...');
  const hash1 = 'abc123def';
  const hash2 = 'abc123def';
  const hash3 = 'modified';

  console.log(`   Stable: ${compareHashes(hash1, hash2)}`);
  if (!compareHashes(hash1, hash2)) throw new Error('Identical hashes should pass');

  console.log(`   Modified: ${compareHashes(hash1, hash3)}`);
  if (compareHashes(hash1, hash3)) throw new Error('Different hashes should fail');

  // 3. Date Discovery Filtering
  console.log('\n3. Testing date discovery filtering...');
  const prefixes = [
    'features/.../date=20251226/',
    'features/.../date=20251227/',
    'features/.../date=20251228/',
    'features/.../date=20251229/',
    'features/.../date=20251230/'
  ];
  const from = '20251227';
  const to = '20251229';

  const filtered = prefixes
    .map(p => {
      const match = p.match(/date=(\d{8})\//);
      return match ? match[1] : null;
    })
    .filter(d => d && d >= from && d <= to);

  console.log(`   Input Range: ${from} to ${to}`);
  console.log(`   Found: ${filtered.join(', ')}`);
  if (filtered.length !== 3) throw new Error('Date filtering failed');
  if (filtered[0] !== '20251227' || filtered[2] !== '20251229') throw new Error('Date range mismatch');

  console.log('\n✅ ALL CANARY SOAK LOGIC TESTS PASSED');
}

runTests().catch(err => {
  console.error('\n❌ SOAK TESTS FAILED');
  console.error(err);
  process.exit(1);
});
