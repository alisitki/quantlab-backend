# Candidate Repro Pack

## Candidate
- candidate_id: 9bd0eb5615e13b1111b521cf77acc92fb6b3d1211338d4655b1c07b0c7788510
- exp_id: demo_sweep
- strategy_id: ema_cross
- params_hash: 811b1dfaef6a2749283071e83493dde8509830831226f4d3f4922e7b10b25140
- params_short: ema_fast=1;ema_slow=3;fastPeriod=1;slowPeriod=3
- dataset: bbo/BTCUSDT/2024-01-19

## Expected hashes
- snapshot.state_hash: e65ef8bd5b48d2eea3a000de94dcd2a31b0fbc3950c33e592da8d236929081c6
- snapshot.fills_hash: 39dc2270d998ad34ea7f0ae44b66751a2e6966cb1b68fcb4357e41471061d6be
- tick.state_hash: e65ef8bd5b48d2eea3a000de94dcd2a31b0fbc3950c33e592da8d236929081c6
- tick.fills_hash: 39dc2270d998ad34ea7f0ae44b66751a2e6966cb1b68fcb4357e41471061d6be
- leaderboard_hash: 870fcc36682d800008ce006c0dbaee964de7ee22add89ca0df8e54dd8ef9e835

## Run
1) Ensure REPLAYD_URL and REPLAYD_TOKEN are set in env (if required).
2) Execute:
   ./repro.sh
