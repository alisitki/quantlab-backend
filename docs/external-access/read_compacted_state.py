#!/usr/bin/env python3

from __future__ import annotations

import argparse
import json
import os
import sys

import boto3


def env_required(name: str) -> str:
    value = os.getenv(name, "").strip()
    if not value:
        raise RuntimeError(f"Missing required env: {name}")
    return value


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Read QuantLab compacted state from S3.")
    p.add_argument("--exchange", default="")
    p.add_argument("--stream", default="")
    p.add_argument("--symbol", default="")
    p.add_argument("--date", default="")
    p.add_argument("--status", default="")
    p.add_argument("--day-quality", default="")
    p.add_argument("--limit", type=int, default=50)
    return p.parse_args()


def parse_partition_key(partition_key: str) -> dict[str, str] | None:
    parts = str(partition_key).split("/")
    if len(parts) != 4:
      return None
    exchange, stream, symbol, date = parts
    if len(date) != 8 or not date.isdigit():
        return None
    return {
        "exchange": exchange.lower(),
        "stream": stream.lower(),
        "symbol": symbol.lower(),
        "date": date,
    }


def read_state_from_s3() -> tuple[str, str, dict]:
    endpoint = env_required("S3_COMPACT_ENDPOINT")
    access_key = env_required("S3_COMPACT_ACCESS_KEY")
    secret_key = env_required("S3_COMPACT_SECRET_KEY")
    bucket = os.getenv("S3_COMPACT_BUCKET", "quantlab-compact")
    key = os.getenv("S3_COMPACT_STATE_KEY", "compacted/_state.json")
    region = os.getenv("S3_COMPACT_REGION", "us-east-1")

    s3 = boto3.client(
        "s3",
        endpoint_url=endpoint,
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
        region_name=region,
    )
    res = s3.get_object(Bucket=bucket, Key=key)
    return bucket, key, json.loads(res["Body"].read().decode("utf-8"))


def matches_filters(row: dict, args: argparse.Namespace) -> bool:
    if args.exchange and row["exchange"] != args.exchange.lower():
        return False
    if args.stream and row["stream"] != args.stream.lower():
        return False
    if args.symbol and row["symbol"] != args.symbol.lower():
        return False
    if args.date and row["date"] != args.date:
        return False
    if args.status and row["status"] != args.status.lower():
        return False
    if args.day_quality and row["day_quality_post"] != args.day_quality.upper():
        return False
    return True


def main() -> int:
    args = parse_args()
    bucket, key, state = read_state_from_s3()
    partitions = state.get("partitions")
    if not isinstance(partitions, dict):
        raise RuntimeError("state json missing partitions object")

    rows: list[dict] = []
    for partition_key, meta_raw in partitions.items():
        parsed = parse_partition_key(partition_key)
        if not parsed:
            continue
        meta = meta_raw if isinstance(meta_raw, dict) else {}
        row = {
            **parsed,
            "partition_key": partition_key,
            "status": str(meta.get("status", "")).strip().lower(),
            "day_quality_post": None if meta.get("day_quality_post") is None else str(meta.get("day_quality_post")).strip().upper(),
            "rows": meta.get("rows"),
            "total_size_bytes": meta.get("total_size_bytes"),
            "updated_at": meta.get("updated_at"),
        }
        row["data_key"] = f"exchange={row['exchange']}/stream={row['stream']}/symbol={row['symbol']}/date={row['date']}/data.parquet"
        row["meta_key"] = f"exchange={row['exchange']}/stream={row['stream']}/symbol={row['symbol']}/date={row['date']}/meta.json"
        row["data_s3_uri"] = f"s3://{bucket}/{row['data_key']}"
        row["meta_s3_uri"] = f"s3://{bucket}/{row['meta_key']}"
        if matches_filters(row, args):
            rows.append(row)

    rows.sort(key=lambda item: (item["date"], item["symbol"], item["stream"]))
    limit = args.limit if args.limit > 0 else 50
    payload = {
        "state_bucket": bucket,
        "state_key": key,
        "state_uri": f"s3://{bucket}/{key}",
        "last_compacted_date": state.get("last_compacted_date"),
        "updated_at": state.get("updated_at"),
        "match_count": len(rows),
        "results": rows[:limit],
    }
    json.dump(payload, sys.stdout, indent=2)
    sys.stdout.write("\n")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
