/**
 * test-ssh-ready.js: Mock test for SSH Readiness Gate v1
 * Verifies the 3-stage probe (TCP, SSH, Node) with simulated failures.
 */
import { test, mock } from 'node:test';
import assert from 'node:assert';
import net from 'node:net';
import cp from 'node:child_process';
import EventEmitter from 'node:events';
import { RemoteJobRunner } from './RemoteJobRunner.js';

test('SSH Readiness Gate - Success after retries', async (t) => {
  let tcpAttempts = 0;
  let sshAttempts = 0;
  let nodeAttempts = 0;

  // Mock Stage 1: TCP
  const mockConnect = mock.method(net, 'connect', () => {
    tcpAttempts++;
    const emitter = new EventEmitter();
    if (tcpAttempts <= 2) {
      // Fail first 2 attempts
      setTimeout(() => emitter.emit('error', new Error('ECONNREFUSED')), 10);
    } else {
      // Success on 3rd attempt
      setTimeout(() => emitter.emit('connect'), 10);
    }
    emitter.destroy = () => {};
    return emitter;
  });

  // Mock Stage 2 & 3: SSH (cp.exec)
  const mockExec = mock.method(cp, 'exec', (cmd, options, callback) => {
    if (typeof options === 'function') {
      callback = options;
    }

    if (cmd.includes('echo ok')) {
      sshAttempts++;
      if (sshAttempts <= 1) {
        // Fail first SSH attempt
        callback(new Error('Connection timeout'), '', 'timeout');
      } else {
        // Success
        callback(null, { stdout: 'ok' }, '');
      }
    } else if (cmd.includes('node -v')) {
      nodeAttempts++;
      if (nodeAttempts <= 1) {
        // Fail first Node attempt (not cloud-init ready)
        callback(null, { stdout: 'sh: node: command not found' }, '');
      } else {
        // Success
        callback(null, { stdout: 'v20.0.0' }, '');
      }
    } else {
      callback(new Error(`Unexpected command: ${cmd}`));
    }
  });

  // Dummy SSH info
  const sshInfo = { host: '1.2.3.4', port: 1234, username: 'root' };
  
  // Create runner with NO vastClient to skip status updates for this test
  const runner = new RemoteJobRunner(sshInfo);

  console.log('--- Starting Readiness Gate Test (Mocked) ---');
  
  const startTime = Date.now();
  await runner.waitForReady();
  const duration = Date.now() - startTime;

  console.log(`Test completed. Duration: ${duration}ms`);
  
  // Assertions
  assert.strictEqual(tcpAttempts, 3, 'Should have 3 TCP attempts (2 fails, 1 ok)');
  assert.strictEqual(sshAttempts, 2, 'Should have 2 SSH attempts (1 fail, 1 ok)');
  assert.strictEqual(nodeAttempts, 2, 'Should have 2 Node attempts (1 fail, 1 ok)');
  
  console.log('✅ Mock test passed!');
  
  // Cleanup mocks
  mockConnect.restore();
  mockExec.restore();
});
