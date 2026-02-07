# Integration Tests

## Factory-Backtest Integration Test
- **File:** tests/integration/test-factory-backtest-integration.js
- **Date:** 2026-02-06
- **Purpose:** Validate end-to-end factory pipeline with real parquet data
- **Coverage:** 
  - Edge → Template selection
  - Parameter mapping
  - Strategy assembly
  - AutoBacktester execution
  - ExecutionEngine integration
- **Result:** PASS (3 strategies, 234K events, 0 trades due to synthetic patterns)
- **Duration:** ~66 seconds total

