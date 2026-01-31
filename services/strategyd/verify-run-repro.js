/**
 * verify-run-repro.js
 * 
 * Verifies that two runs with identical inputs produce the same fills_hash.
 */

const STRATEGYD_URL = 'http://localhost:3031';

async function main() {
  console.log('--- STARTING REPRO TEST ---');
  
  const res = await fetch(`${STRATEGYD_URL}/runs`);
  const { runs } = await res.json();
  
  if (runs.length < 2) {
    console.error('Error: Need at least 2 runs in history to compare.');
    process.exit(1);
  }

  const run1Id = runs[1];
  const run2Id = runs[0];
  
  console.log(`Comparing Run 1: ${run1Id}`);
  console.log(`Comparing Run 2: ${run2Id}`);
  
  const m1 = await (await fetch(`${STRATEGYD_URL}/run/${run1Id}`)).json();
  const m2 = await (await fetch(`${STRATEGYD_URL}/run/${run2Id}`)).json();
  
  console.log('Run 1 FillsHash:', m1.output.fills_hash);
  console.log('Run 2 FillsHash:', m2.output.fills_hash);
  
  if (m1.output.fills_hash === m2.output.fills_hash) {
    console.log('✅ SUCCESS: Determinism verified!');
  } else {
    console.error('❌ FAILURE: Fills hashes do not match!');
    process.exit(1);
  }
}

main().catch(console.error);
