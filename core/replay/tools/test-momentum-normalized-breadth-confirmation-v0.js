import test from 'node:test';
import assert from 'node:assert/strict';

import {
  chooseEvenlySpacedWindows,
  classifyRow,
  normalizeReplayTsEventNs,
  summarizeFills,
} from './run-momentum-normalized-breadth-confirmation-v0.js';

test('chooseEvenlySpacedWindows picks deterministic common-window sample', () => {
  const commonDates = [
    '20260125', '20260126', '20260127', '20260128', '20260129',
    '20260203', '20260208', '20260209', '20260210', '20260211',
    '20260212', '20260213', '20260214', '20260215', '20260216',
    '20260217', '20260218', '20260219', '20260220', '20260221',
    '20260222', '20260223', '20260224', '20260225', '20260226',
    '20260227', '20260228', '20260302', '20260303', '20260304',
    '20260305', '20260306', '20260307', '20260309', '20260310',
    '20260311', '20260312', '20260313', '20260314', '20260315',
    '20260316', '20260317',
  ];
  assert.deepEqual(
    chooseEvenlySpacedWindows(commonDates, 10),
    ['20260125', '20260203', '20260211', '20260216', '20260220', '20260225', '20260302', '20260307', '20260312', '20260317'],
  );
});

test('normalizeReplayTsEventNs converts millisecond replay timestamps to nanoseconds', () => {
  assert.equal(normalizeReplayTsEventNs(1769385421076n), 1769385421076000000n);
  assert.equal(normalizeReplayTsEventNs(1769385421076000000n), 1769385421076000000n);
});

test('summarizeFills derives opens, exits, reversals, closed cycles, fees, turnover', () => {
  const summary = summarizeFills([
    { side: 'BUY', qty: 1, fee: 0.1, fillValue: 100 },
    { side: 'SELL', qty: 2, fee: 0.2, fillValue: 198 },
    { side: 'BUY', qty: 1, fee: 0.1, fillValue: 97 },
  ]);
  assert.deepEqual(summary, {
    fills_count: 3,
    opens_count: 2,
    exits_count: 1,
    reversals_count: 1,
    closed_cycle_count: 2,
    fees: 0.4,
    turnover: 395,
  });
});

test('classifyRow keeps common 24h contract thresholds', () => {
  const [promisingClass] = classifyRow({
    completed_horizon_sec: 86400,
    fills_count: 4,
    closed_cycle_count: 2,
    net_pnl: 1,
    net_pnl_bps_turnover: 3.5,
  });
  const [weakClass] = classifyRow({
    completed_horizon_sec: 86400,
    fills_count: 10,
    closed_cycle_count: 5,
    net_pnl: -1,
    net_pnl_bps_turnover: -4.2,
  });
  assert.equal(promisingClass, 'PROMISING');
  assert.equal(weakClass, 'WEAK');
});

test('classifyRow treats compact-day coverage above floor as completed', () => {
  const [classification] = classifyRow({
    completed_horizon_sec: 86386,
    fills_count: 6,
    closed_cycle_count: 3,
    net_pnl: 0.2,
    net_pnl_bps_turnover: 4.1,
  });
  assert.equal(classification, 'PROMISING');
});

test('classifyRow still rejects rows below compact-day coverage floor', () => {
  const [classification, reason] = classifyRow({
    completed_horizon_sec: 86199,
    fills_count: 6,
    closed_cycle_count: 3,
    net_pnl: 0.2,
    net_pnl_bps_turnover: 4.1,
  });
  assert.equal(classification, 'BROKEN');
  assert.equal(reason, 'completed_horizon_below_compact_day_floor');
});
