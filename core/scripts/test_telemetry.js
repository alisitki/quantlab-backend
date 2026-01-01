import fs from 'fs';
import path from 'path';
import { execSync } from 'child_process';
import { emitExecutionEvent } from '../ops/emit_execution_event.js';

const NOW = 1735657500000;
const TEST_DATE = '20251231';
const EVENTS_LOG = 'logs/events.jsonl';
const ALERTS_LOG = 'logs/alerts.jsonl';
const OPS_DIR = 'ops/messages';

async function test() {
    console.log('--- Telemetry Verification ---');

    // 1. GAP-1: OPS_EVENT Persistence
    console.log('Testing GAP-1: OPS_EVENT Persistence...');
    const executionResult = {
        decision_id: 'test-decision-telemetry',
        symbol: 'BTCUSDT',
        outcome: 'PASSED',
        reason_code: 'PASSED',
        evaluated_at: NOW,
        policy_version: 'v1',
        policy_snapshot: { mode: 'CANARY' }
    };

    const event = emitExecutionEvent(executionResult);
    
    // Check events.jsonl
    if (!fs.existsSync(EVENTS_LOG)) throw new Error('events.jsonl not created');
    const eventLines = fs.readFileSync(EVENTS_LOG, 'utf8').trim().split('\n');
    const lastEvent = JSON.parse(eventLines[eventLines.length - 1]);
    
    if (lastEvent.event_id !== event.event_id) throw new Error('Event ID mismatch in events.jsonl');
    console.log('✅ GAP-1: OPS_EVENT persisted correctly');

    // 2. GAP-2: Alert Correlation
    console.log('Testing GAP-2: Alert Correlation...');
    
    // Trigger alert
    const alertMsg = 'Test alert for telemetry correlation';
    execSync(`node scheduler/alert_hook.js --type CRON_FAILURE --date ${TEST_DATE} --message "${alertMsg}"`);
    
    // Check alerts.jsonl
    const alertLines = fs.readFileSync(ALERTS_LOG, 'utf8').trim().split('\n');
    const lastAlert = JSON.parse(alertLines[alertLines.length - 1]);
    if (!lastAlert.alert_id) throw new Error('alert_id missing in alerts.jsonl');
    console.log(`✅ alert_id generated: ${lastAlert.alert_id}`);

    // Generate ops message
    execSync(`node scheduler/generate_ops_message.js --date ${TEST_DATE}`);
    
    // Check ops/messages/message_{date}.json
    const msgPath = path.join(OPS_DIR, `message_${TEST_DATE}.json`);
    const message = JSON.parse(fs.readFileSync(msgPath, 'utf8'));
    
    if (!message.alerts.alert_ids.includes(lastAlert.alert_id)) {
        throw new Error('alert_id not propagated to ops message');
    }
    console.log('✅ GAP-2: alert_id propagated correctly');

    console.log('\n--- ALL TELEMETRY TESTS PASSED ---');
}

test().catch(err => {
    console.error('❌ Verification FAILED:', err.message);
    process.exit(1);
});
