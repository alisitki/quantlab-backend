# Family-B SimpleMomentum (New)

- family_id: `family_b_simple_momentum`
- objective: short-term continuation check on trade stream
- input:
  - same 2-day trade window as Family-A
- definition:
  - lookback return: `R_lb = P(t) / P(t-5m) - 1`
  - forward return: `R_fw = P(t+5m) / P(t) - 1`
  - signal set: `R_lb >= q90(R_lb)`
- support:
  - minimum `N >= 200`
- metrics:
  - `mean_forward_return`
  - `t_stat` (one-sample, sample std)
  - `signal_support`
- pass bar:
  - `mean_forward_return > 0`
  - `|t_stat| > 2`
  - `signal_support >= 200`
