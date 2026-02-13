# Multi-Hypothesis Contract v0

- families:
  - `family_a_patternscanner`
  - `family_b_simple_momentum`
- input:
  - `exchange`, `stream`, `symbol`, `start`, `end` (2-day window)
- output per family report:
  - JSON report file
  - key metrics written to `rollup.tsv`
- pass bar:
  - Family-A: unchanged current acceptance signal metrics (`patternsScanned`, edges fields)
  - Family-B: `mean_forward_return > 0` and `|t_stat| > 2` with `support >= 200`
