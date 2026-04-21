# External Access Pack

This folder is a minimal handoff pack for another repository that needs:

- Vast.ai API access
- Read-only access to compacted data in `quantlab-compact`
- Access to `compacted/_state.json` and derived parquet/meta object paths

## Files

- `compact-vast.env.example`: secret-free environment template
- `read-compacted-state.js`: Node helper using `@aws-sdk/client-s3`
- `read_compacted_state.py`: Python helper using `boto3`

## Required Environment Variables

```bash
VAST_API_KEY=...
S3_COMPACT_ENDPOINT=...
S3_COMPACT_BUCKET=quantlab-compact
S3_COMPACT_ACCESS_KEY=...
S3_COMPACT_SECRET_KEY=...
S3_COMPACT_REGION=us-east-1
S3_COMPACT_STATE_KEY=compacted/_state.json
```

`VAST_API_KEY` is only needed for Vast.ai calls. The S3 variables are enough for compacted-state and parquet/meta reads.

## State Contract

The helpers expect:

- top-level `partitions` object
- partition key format: `exchange/stream/symbol/date`
- partition metadata fields such as `status`, `day_quality_post`, `rows`, `total_size_bytes`, `updated_at`

The helpers derive these object keys from each partition entry:

- `exchange={exchange}/stream={stream}/symbol={symbol}/date={date}/data.parquet`
- `exchange={exchange}/stream={stream}/symbol={symbol}/date={date}/meta.json`

## Usage

Node:

```bash
npm install @aws-sdk/client-s3
node docs/external-access/read-compacted-state.js \
  --exchange binance \
  --stream bbo \
  --symbol btcusdt \
  --status success \
  --day-quality GOOD \
  --limit 5
```

Python:

```bash
pip install boto3
python3 docs/external-access/read_compacted_state.py \
  --exchange binance \
  --stream bbo \
  --symbol btcusdt \
  --status success \
  --day-quality GOOD \
  --limit 5
```

Both scripts print JSON to stdout.

## Secret Handling

Do not commit real values into the target repository. Keep the template file as-is and inject credentials through CI/CD secrets, a local `.env`, or the deployment environment.
