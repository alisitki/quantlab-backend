#!/usr/bin/env node

const { S3Client, GetObjectCommand } = require("@aws-sdk/client-s3");

function parseArgs(argv) {
  const out = { limit: 50 };
  for (let i = 0; i < argv.length; i += 1) {
    const key = argv[i];
    const value = argv[i + 1];
    if (key === "--exchange") out.exchange = String(value || "").toLowerCase();
    if (key === "--stream") out.stream = String(value || "").toLowerCase();
    if (key === "--symbol") out.symbol = String(value || "").toLowerCase();
    if (key === "--date") out.date = String(value || "");
    if (key === "--status") out.status = String(value || "").toLowerCase();
    if (key === "--day-quality") out.dayQuality = String(value || "").toUpperCase();
    if (key === "--limit") out.limit = Number.parseInt(String(value || "50"), 10);
  }
  return out;
}

function envRequired(name) {
  const value = process.env[name];
  if (!value) throw new Error(`Missing required env: ${name}`);
  return value;
}

function parsePartitionKey(partitionKey) {
  const parts = String(partitionKey || "").split("/");
  if (parts.length !== 4) return null;
  const [exchange, stream, symbol, date] = parts;
  if (!/^\d{8}$/.test(date)) return null;
  return { exchange, stream, symbol, date };
}

async function readStateFromS3() {
  const endpoint = envRequired("S3_COMPACT_ENDPOINT");
  const accessKeyId = envRequired("S3_COMPACT_ACCESS_KEY");
  const secretAccessKey = envRequired("S3_COMPACT_SECRET_KEY");
  const bucket = process.env.S3_COMPACT_BUCKET || "quantlab-compact";
  const key = process.env.S3_COMPACT_STATE_KEY || "compacted/_state.json";
  const region = process.env.S3_COMPACT_REGION || "us-east-1";

  const s3 = new S3Client({
    endpoint,
    region,
    credentials: { accessKeyId, secretAccessKey },
    forcePathStyle: true
  });

  const res = await s3.send(new GetObjectCommand({ Bucket: bucket, Key: key }));
  const text = await res.Body.transformToString();
  return { bucket, key, state: JSON.parse(text) };
}

function matchesFilters(row, args) {
  if (args.exchange && row.exchange !== args.exchange) return false;
  if (args.stream && row.stream !== args.stream) return false;
  if (args.symbol && row.symbol !== args.symbol) return false;
  if (args.date && row.date !== args.date) return false;
  if (args.status && row.status !== args.status) return false;
  if (args.dayQuality && row.day_quality_post !== args.dayQuality) return false;
  return true;
}

async function main() {
  const args = parseArgs(process.argv.slice(2));
  const { bucket, key, state } = await readStateFromS3();
  if (!state || typeof state !== "object" || Array.isArray(state.partitions)) {
    throw new Error("Invalid state JSON");
  }

  const partitions = state.partitions;
  if (!partitions || typeof partitions !== "object") {
    throw new Error("state json missing partitions object");
  }

  const rows = [];
  for (const [partitionKey, metaRaw] of Object.entries(partitions)) {
    const parsed = parsePartitionKey(partitionKey);
    if (!parsed) continue;
    const meta = metaRaw && typeof metaRaw === "object" ? metaRaw : {};
    const row = {
      ...parsed,
      partition_key: partitionKey,
      status: String(meta.status || "").toLowerCase(),
      day_quality_post: meta.day_quality_post == null ? null : String(meta.day_quality_post).toUpperCase(),
      rows: meta.rows ?? null,
      total_size_bytes: meta.total_size_bytes ?? null,
      updated_at: meta.updated_at ?? null,
      data_key: `exchange=${parsed.exchange}/stream=${parsed.stream}/symbol=${parsed.symbol}/date=${parsed.date}/data.parquet`,
      meta_key: `exchange=${parsed.exchange}/stream=${parsed.stream}/symbol=${parsed.symbol}/date=${parsed.date}/meta.json`
    };
    row.data_s3_uri = `s3://${bucket}/${row.data_key}`;
    row.meta_s3_uri = `s3://${bucket}/${row.meta_key}`;
    if (matchesFilters(row, args)) rows.push(row);
  }

  rows.sort((a, b) => {
    return a.date.localeCompare(b.date) || a.symbol.localeCompare(b.symbol) || a.stream.localeCompare(b.stream);
  });

  const limit = Number.isFinite(args.limit) && args.limit > 0 ? args.limit : 50;
  console.log(JSON.stringify({
    state_bucket: bucket,
    state_key: key,
    state_uri: `s3://${bucket}/${key}`,
    last_compacted_date: state.last_compacted_date || null,
    updated_at: state.updated_at || null,
    match_count: rows.length,
    results: rows.slice(0, limit)
  }, null, 2));
}

main().catch((err) => {
  console.error(err.message || String(err));
  process.exit(1);
});
