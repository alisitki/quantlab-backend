/**
 * test-no-hotpatch.js: Smoke test for Deploy Discipline v1.2
 * Verifies that REMOTE_HOTPATCH=1 triggers a hard failure.
 */
import { RemoteJobRunner } from './RemoteJobRunner.js';

async function runTest() {
  console.log('--- Smoke Test: No Hotpatch (v1.2) ---');
  
  // Set the forbidden env var
  process.env.REMOTE_HOTPATCH = '1';
  
  const runner = new RemoteJobRunner({ host: 'localhost', port: 22, username: 'test' });
  const dummyJobSpec = { jobId: 'test-job' };
  
  try {
    console.log('Execution with REMOTE_HOTPATCH=1...');
    await runner.executeJob(dummyJobSpec);
    
    console.error('❌ FAILED: Hotpatch was NOT blocked!');
    process.exit(1);
  } catch (err) {
    if (err.message.includes('REMOTE_HOTPATCH is disabled')) {
      console.log('✅ PASSED: Hotpatch correctly blocked with error:');
      console.log(`   "${err.message}"`);
    } else {
      // If it failed because of SSH connection (which we expect since it's dummy), 
      // it means the check passed (as it should be before connection ideally).
      // Wait, in my implementation it's AFTER connection but BEFORE clone.
      // Let's check the logic again.
      
      // Actually, my implementation is:
      // 1. connect
      // 2. hotpatch guard  <-- it should throw here
      
      console.error('❌ FAILED: Unexpected error:', err.message);
      process.exit(1);
    }
  }
}

runTest().catch(console.error);
