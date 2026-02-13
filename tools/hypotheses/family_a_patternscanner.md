# Family-A PatternScanner (Existing)

- family_id: `family_a_patternscanner`
- engine: existing `tools/run-multi-day-discovery.js` acceptance flow
- semantics: EXACT / unchanged
- expected metrics in report:
  - `result.patternsScanned`
  - `result.edgeCandidatesGenerated`
  - `result.edgeCandidatesRegistered`
  - `result.edges` (count)
- runner extraction:
  - parse `[Run] report_saved=...`
  - parse `[Run] edges_saved=...`
  - parse `patterns_scanned: ...` from stdout
