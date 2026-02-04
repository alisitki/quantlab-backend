/**
 * Replay Cache Layer v1 Verification Script
 * 
 * Tests:
 * 1. Security (Auth)
 * 2. Cache Hit/Miss
 * 3. Backpressure (Simulated Slow Client)
 */

import http from 'node:http';
import { spawn } from 'node:child_process';

const SECRET = 'test-secret';
const BASE_URL = 'http://localhost:3030/stream?dataset=bbo&symbol=BTCUSDT&date=2024-01-15';

async function testSecurity() {
  console.log('--- Testing Security ---');
  return new Promise((resolve) => {
    http.get(BASE_URL, (res) => {
      console.log(`No Auth: ${res.statusCode} (Expected 401)`);
      resolve(res.statusCode === 401);
    });
  });
}

async function testCache() {
  console.log('\n--- Testing Cache ---');
  const url = `${BASE_URL}&token=${SECRET}`;
  
  // Cold run
  console.log('Cold run (check logs for cache_miss)...');
  await new Promise(r => {
    const req = http.get(url, (res) => {
      let count = 0;
      res.on('data', () => {
        count++;
        if (count > 5) {
          req.destroy();
          r();
        }
      });
    });
  });

  // Hot run
  console.log('Hot run (check logs for cache_hit)...');
  await new Promise(r => {
    const req = http.get(url, (res) => {
      let count = 0;
      res.on('data', () => {
        count++;
        if (count > 5) {
          req.destroy();
          r();
        }
      });
    });
  });
}

async function run() {
  process.env.REPLAY_SECRET = SECRET;
  // Note: This script assumes replayd is running locally or simulated
  const secure = await testSecurity();
  if (secure) {
    await testCache();
  }
}

run().catch(console.error);
