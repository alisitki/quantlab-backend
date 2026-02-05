# SignalGate - Decision Gating Layer

## Overview

SignalGate is a **noise reduction layer** that sits between strategy signal generation and trade execution. It prevents noise trading by applying structural filters to every trade decision.

**Important:** SignalGate does NOT modify strategy logic. It operates transparently as a gating mechanism.

## Architecture

```
Strategy → Features → Signals → Decision → [SignalGate] → Execution
                                              ↓
                                         Block/Allow
```

## Gate Rules

A trade is executed ONLY if ALL gate rules pass:

### 1. Regime Gate
- **Trend Threshold:** Trade allowed if `trend_score >= regimeTrendMin`
- **Volatility Threshold:** Trade allowed if `volatility_score >= regimeVolMin`
- **Spread Threshold:** Trade allowed if `spread_score <= regimeSpreadMax`

### 2. Signal Strength Gate
- **Min Confidence:** Trade allowed if `signalScore >= minSignalScore`
- Note: This is typically STRICTER than strategy's `execution.minConfidence`

### 3. Cooldown Gate
- **Time-based filter:** Trade allowed if `time_since_last_trade >= cooldownMs`
- Prevents rapid-fire trading and over-trading

### 4. Spread Penalty Gate
- **Cost filter:** Trade allowed if `spread/mid_price <= maxSpreadNormalized`
- Prevents trading in expensive spread conditions

## Configuration

### Default Config (Balanced)
```javascript
gate: {
  enabled: true,
  regimeTrendMin: -0.5,         // Allow trend >= -0.5
  regimeVolMin: 0,              // Allow all volatility regimes
  regimeSpreadMax: 2,           // Block only VERY_WIDE spread
  minSignalScore: 0.6,          // 60% confidence minimum
  cooldownMs: 5000,             // 5 seconds between trades
  maxSpreadNormalized: 0.001,   // 0.1% max spread/mid ratio
  logBlockedTrades: true
}
```

### Preset Configurations

| Preset | minSignalScore | cooldownMs | Spread Threshold | Use Case |
|--------|---------------|------------|------------------|----------|
| **DEFAULT** | 0.6 | 5000ms | 0.001 | Balanced noise reduction |
| **HIGH_FREQUENCY** | 0.5 | 3000ms | 0.0005 | More trades, tighter spread |
| **QUALITY** | 0.75 | 10000ms | 0.0005 | Fewer, high-conviction trades |
| **AGGRESSIVE** | 0.4 | 2000ms | 0.002 | High risk tolerance |
| **CONSERVATIVE** | 0.85 | 15000ms | 0.0003 | Minimal risk, very selective |

## Integration with StrategyV1

### Automatic Integration
StrategyV1 automatically uses SignalGate if `gate.enabled = true` in config.

```javascript
const strategy = new StrategyV1({
  gate: {
    enabled: true,
    minSignalScore: 0.7,
    cooldownMs: 8000
  }
});
```

### Gate Flow in Strategy

1. Strategy generates decision with confidence score
2. If `decision.action !== HOLD` and `confidence >= execution.minConfidence`:
   - **Gate evaluation happens HERE**
   - If gate blocks: log and return (no trade)
   - If gate allows: proceed to execution
3. Track `lastTradeTime` for cooldown

### Statistics Tracking

Gate tracks all evaluations and block reasons:

```javascript
const state = strategy.getState();
console.log(state.gateStats);
// {
//   passed: 45,
//   blocked: 123,
//   total: 168,
//   passRate: '0.268',
//   blockReasons: {
//     'cooldown: 1234ms < 5000ms': 67,
//     'signal_strength: 0.542 < 0.6': 34,
//     'spread_penalty: 0.0012 > 0.001': 22
//   }
// }
```

## Usage Examples

### Example 1: Enable Gate with Custom Thresholds
```javascript
import { StrategyV1, DEFAULT_CONFIG } from './core/strategy/v1/index.js';

const strategy = new StrategyV1({
  ...DEFAULT_CONFIG,
  gate: {
    enabled: true,
    minSignalScore: 0.65,     // Custom threshold
    cooldownMs: 7000,         // 7 second cooldown
    maxSpreadNormalized: 0.0008
  }
});
```

### Example 2: Disable Gate (Not Recommended)
```javascript
const strategy = new StrategyV1({
  ...DEFAULT_CONFIG,
  gate: { enabled: false }  // High trade frequency expected!
});
```

### Example 3: Runtime Config Update
```javascript
const strategy = new StrategyV1(config);

// During execution, adjust gate
const state = strategy.getState();
if (state.gateStats.passRate < 0.1) {
  // Too strict, relax threshold
  strategy.#signalGate.updateConfig({ minSignalScore: 0.5 });
}
```

## Testing

### Unit Tests
```bash
node core/decision/test-signal-gate.js
```

Tests:
- Regime gate rules
- Signal strength filtering
- Cooldown enforcement
- Spread penalty
- Statistics tracking
- Runtime config updates

### Integration Tests
```bash
node core/strategy/v1/tests/test-gate-integration.js
```

Tests gate integration with StrategyV1 in mock backtest scenarios.

## Performance Impact

### Trade Frequency Reduction
Expected reduction with DEFAULT config: **40-70%**

### Latency
Gate adds **< 1μs** per evaluation (negligible)

### Memory
Gate state: **< 1KB** (statistics map)

## Logging

### Blocked Trades
When a trade is blocked, gate logs at DEBUG level:
```
Gate blocked: cooldown: 2341ms < 5000ms
```

### End-of-Run Summary
```
=== Decision Gate Statistics ===
Total evaluations: 1250
Passed: 312 (25.0%)
Blocked: 938
Block reasons: {
  'cooldown: ...': 456,
  'signal_strength: ...': 289,
  'spread_penalty: ...': 193
}
```

## Trade-offs

### Benefits
- **Reduces noise trading** significantly
- **Structure-based filtering** (not ML-based)
- **Transparent to strategy logic**
- **Zero modification to feature generation**
- **Configurable per trading style**

### Limitations
- **May miss some profitable trades** (trade-off for noise reduction)
- **Fixed rules** (not adaptive)
- **Requires tuning** for optimal performance

## Best Practices

1. **Start with DEFAULT config** and observe passRate
2. **Target 20-40% pass rate** for balanced filtering
3. **Use QUALITY preset** for conservative trading
4. **Use HIGH_FREQUENCY preset** for active trading
5. **Monitor gate statistics** to tune thresholds
6. **Backtest with and without gate** to measure impact

## Related Components

- **StrategyV1:** Main strategy orchestrator
- **RegimeModeSelector:** Regime-based mode switching
- **SignalGenerator:** Feature-based signal generation
- **Combiner:** Signal aggregation and decision making

## Version History

- **v1.0.0** (2026-02-05): Initial implementation with 4 gate rules
