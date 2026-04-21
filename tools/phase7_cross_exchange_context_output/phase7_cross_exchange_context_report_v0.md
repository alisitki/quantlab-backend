# Phase7 Cross-Exchange Context v0

This analysis compares GOOD vs BAD `linkusdt` microstructure trades using only ex-ante context from same-symbol cross-exchange and BTC market surfaces.

- GOOD subset: minimum subset by descending gross_pnl producing >50% of total gross pnl (`206` trades)
- BAD subset: all trades with gross_pnl < 0 (`658` trades)
- Join / coverage quality: full attribution join already at `100%`; context build covered `2723` trades with same-symbol context and `2723` trades with BTC context

## Strongest Patterns
- Same-symbol best rule: `link_trade_alignment_count_100ms == 3 AND link_trade_divergence_bucket_100ms == low`
- BTC / market best rule: `btc_bbo_return_relation_100ms == supportive`
- Combined best rule: `link_trade_alignment_count_250ms == 3 AND link_trade_divergence_bucket_250ms == low AND btc_bbo_return_relation_250ms == supportive`

## Weakest / Useless Pattern
- Weakest candidate observed: `btc_bbo_return_relation_100ms == disagreement`

Final context verdict: `NARROW_CONTEXT_SIGNAL`
Next step: `RESHAPE_REQUIRED`
Reason: external context improves trade quality, but coverage and/or net profile remain too weak for a clean confirmation layer

Best simple rule candidate: `link_trade_alignment_count_100ms == 3 AND link_trade_divergence_bucket_100ms == low` (trade_count=168, avg_gross=0.002571428571, avg_net=-0.0044971, good/bad lift=10.292340884574)

FINAL DECISION: `RESHAPE_REQUIRED`
