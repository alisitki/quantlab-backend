import { ReplayStreamClient } from './runtime/ReplayStreamClient.js';

const REPLAYD_URL = process.env.REPLAYD_URL || 'http://localhost:3000';
const TOKEN = process.env.REPLAYD_TOKEN || 'test-secret';

async function testSuccess() {
  console.log('--- TEST: Success Case ---');
  let eventCount = 0;
  const client = new ReplayStreamClient({
    url: `${REPLAYD_URL}/stream?dataset=compact&symbol=BTCUSDT&date=2024-01-15`,
    token: TOKEN,
    onEvent: (event) => {
      eventCount++;
      if (eventCount === 5) {
        console.log('Received 5 events, stopping client...');
        client.stop();
      }
    },
    onError: (err) => {
      if (err.message === 'SSE_CONNECTION_FAILED') {
        // Might be still starting up or transient
      }
      console.error('Client Error:', err.message);
    },
    onEnd: () => console.log('Stream ended.')
  });

  console.log('Starting client...');
  await client.start();
  console.log('Connects:', client.metrics.connectsTotal);
  console.log('Latency:', client.metrics.firstEventLatencyMs);
  if (eventCount > 0) console.log('SUCCESS: Events received.');
  else console.error('FAILED: No events received.');
}

async function testEmpty() {
  console.log('\n--- TEST: Empty Stream Case ---');
  const client = new ReplayStreamClient({
    url: `${REPLAYD_URL}/stream?dataset=compact&symbol=BTCUSDT&date=2025-01-01`, // Should be empty
    token: TOKEN,
    onEvent: (event) => console.log('Event:', event),
    onError: (err) => console.log('Client Error (Expected):', err.message),
    onEnd: () => console.log('Stream ended.')
  });

  await client.start();
  console.log('Connects:', client.metrics.connectsTotal);
  if (!client.running) console.log('SUCCESS: Client stopped on EMPTY_STREAM.');
  else console.error('FAILED: Client still running.');
}

async function run() {
  await testSuccess();
  await testEmpty();
}

run().catch(console.error);
