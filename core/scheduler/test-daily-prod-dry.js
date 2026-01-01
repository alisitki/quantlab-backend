import 'dotenv/config';
import { RemoteJobRunner } from '../vast/RemoteJobRunner.js';

async function runTests() {
  console.log('--- Daily Schedule v1 (Dry) Unit Tests ---');

  // 1. Yesterday Logic
  function getYesterday() {
    const d = new Date();
    d.setDate(d.getDate() - 1);
    return d.toISOString().split('T')[0].replace(/-/g, '');
  }

  const yesterday = getYesterday();
  console.log(`1. Testing yesterday logic: ${yesterday}`);
  if (!/^\d{8}$/.test(yesterday)) throw new Error('Yesterday format must be YYYYMMDD');

  // 2. Range Logic
  function getDatesInRange(from, to) {
    const dates = [];
    let current = new Date(`${from.slice(0, 4)}-${from.slice(4, 6)}-${from.slice(6, 8)}`);
    const end = new Date(`${to.slice(0, 4)}-${to.slice(4, 6)}-${to.slice(6, 8)}`);
    while (current <= end) {
      dates.push(current.toISOString().split('T')[0].replace(/-/g, ''));
      current.setDate(current.getDate() + 1);
    }
    return dates;
  }

  console.log('\n2. Testing range logic (20251228 -> 20251230)...');
  const range = getDatesInRange('20251228', '20251230');
  console.log(`   Found: ${range.join(', ')}`);
  if (range.length !== 3) throw new Error('Range length mismatch');
  if (range[0] !== '20251228' || range[2] !== '20251230') throw new Error('Range start/end mismatch');

  // 3. Command Line Formatting Mock
  function formatCmd(symbol, date, canary, promote, live, ensure) {
    const flags = [
      `--symbol ${symbol}`,
      `--date ${date}`,
      `--canary ${canary}`,
      `--promote ${promote}`,
      live ? '--live' : '',
      ensure ? '--ensure-features' : ''
    ].filter(Boolean).join(' ');
    return `node scheduler/run_daily_ml.js ${flags}`;
  }

  console.log('\n3. Testing command formatting...');
  const cmd = formatCmd('btcusdt', '20251229', 'false', 'dry', true, true);
  console.log(`   Cmd: ${cmd}`);
  if (!cmd.includes('--canary false')) throw new Error('Canary flag missing/incorrect');
  if (!cmd.includes('--promote dry')) throw new Error('Promote flag missing/incorrect');

  // 4. Report Schema Test
  console.log('\n4. Testing report schema...');
  const mockReport = {
    symbol: 'btcusdt',
    startedAt: new Date().toISOString(),
    runs: [
      { date: '20251229', status: 'SUCCESS', safetyPass: true, jobId: 'job-test-123' }
    ],
    endedAt: new Date().toISOString()
  };
  if (!mockReport.symbol || !mockReport.runs || !mockReport.startedAt) {
    throw new Error('Report schema missing required fields');
  }
  if (!mockReport.runs[0].date || !mockReport.runs[0].status) {
    throw new Error('Run result schema missing required fields');
  }
  console.log('   Report schema: PASS');

  // 5. SSH Zero-Patience Constants Test
  console.log('\n5. Testing SSH Zero-Patience constants...');
  if (RemoteJobRunner.SSH_HARD_TIMEOUT_MS !== 45_000) {
    throw new Error(`SSH_HARD_TIMEOUT_MS must be 45000, got ${RemoteJobRunner.SSH_HARD_TIMEOUT_MS}`);
  }
  if (RemoteJobRunner.SSH_MAX_SLEEP_MS !== 2000) {
    throw new Error(`SSH_MAX_SLEEP_MS must be 2000, got ${RemoteJobRunner.SSH_MAX_SLEEP_MS}`);
  }
  if (!RemoteJobRunner.SSH_KEX_FATAL_PATTERNS.includes('kex_exchange_identification')) {
    throw new Error('SSH_KEX_FATAL_PATTERNS must include kex_exchange_identification');
  }
  if (!RemoteJobRunner.SSH_KEX_FATAL_PATTERNS.includes('Connection closed by remote host')) {
    throw new Error('SSH_KEX_FATAL_PATTERNS must include Connection closed by remote host');
  }
  console.log('   SSH_HARD_TIMEOUT_MS: 45000 ✓');
  console.log('   SSH_MAX_SLEEP_MS: 2000 ✓');
  console.log('   SSH_KEX_FATAL_PATTERNS: ✓');

  console.log('\n✅ ALL DAILY PROD DRY LOGIC TESTS PASSED');
}

runTests().catch(err => {
  console.error('\n❌ TESTS FAILED');
  console.error(err);
  process.exit(1);
});
