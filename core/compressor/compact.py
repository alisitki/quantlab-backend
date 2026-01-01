"""
QuantLab Parquet Compaction Worker
Consolidates small parquet files into single daily files with seq column for deterministic replay.
Uses state-based catch-up and fast discovery.
"""

import os
import tempfile
import json
from pathlib import Path
from typing import List, Dict, Optional, Set
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed
import logging

import boto3
from botocore.config import Config
from botocore.exceptions import ClientError
import pyarrow as pa
import pyarrow.parquet as pq
import pyarrow.dataset as ds

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Suppress boto3/urllib3 warnings
logging.getLogger('urllib3').setLevel(logging.ERROR)
logging.getLogger('botocore').setLevel(logging.ERROR)

MAX_PARALLEL_DOWNLOADS = 50
STATE_FILE_KEY = "compacted/_state.json"


class StateManager:
    """Manages compaction state (_state.json) in S3"""
    
    def __init__(self, s3_client, bucket: str):
        self.s3_client = s3_client
        self.bucket = bucket
        
    def get_last_compacted_date(self) -> Optional[str]:
        """Read last_compacted_date from S3"""
        try:
            resp = self.s3_client.get_object(Bucket=self.bucket, Key=STATE_FILE_KEY)
            state = json.loads(resp['Body'].read().decode('utf-8'))
            return state.get('last_compacted_date')
        except ClientError as e:
            if e.response['Error']['Code'] == 'NoSuchKey':
                return None
            raise
        except Exception as e:
            logger.error(f"Error reading state file: {e}")
            return None
            
    def update_last_compacted_date(self, date_str: str):
        """Update last_compacted_date and current timestamp in S3 for audit"""
        state = {
            "last_compacted_date": date_str,
            "updated_at": datetime.utcnow().isoformat() + "Z"
        }
        self.s3_client.put_object(
            Bucket=self.bucket,
            Key=STATE_FILE_KEY,
            Body=json.dumps(state, indent=2).encode('utf-8'),
            ContentType='application/json'
        )
        logger.info(f"Updated state: last_compacted_date={date_str}")


class CompactionJob:
    """Handles compaction of parquet files from raw to compact bucket"""
    
    def __init__(
        self,
        s3_endpoint: str,
        s3_access_key: str,
        s3_secret_key: str,
        raw_bucket: str,
        compact_bucket: str
    ):
        config = Config(max_pool_connections=100)
        self.s3_client = boto3.client(
            's3',
            endpoint_url=s3_endpoint,
            aws_access_key_id=s3_access_key,
            aws_secret_access_key=s3_secret_key,
            config=config
        )
        self.raw_bucket = raw_bucket
        self.compact_bucket = compact_bucket
        self.state_manager = StateManager(self.s3_client, compact_bucket)
        
    def compact_date_partition(
        self,
        exchange: str,
        stream: str,
        symbol: str,
        date: str,
        overwrite: bool = False
    ) -> Dict:
        """Compact a single partition for a specific date"""
        result = {
            'exchange': exchange, 'stream': stream, 'symbol': symbol, 'date': date,
            'status': 'unknown', 'files_processed': 0, 'total_size_bytes': 0,
            'output_size_bytes': 0, 'rows': 0, 'error': None
        }
        
        try:
            raw_prefix = f"exchange={exchange}/stream={stream}/symbol={symbol}/date={date}/"
            compact_key = f"exchange={exchange}/stream={stream}/symbol={symbol}/date={date}/data.parquet"
            meta_key = f"exchange={exchange}/stream={stream}/symbol={symbol}/date={date}/meta.json"
            
            if not overwrite and self._compact_exists(compact_key):
                result['status'] = 'skipped'
                return result
            
            raw_files = self._list_raw_files(raw_prefix)
            if not raw_files:
                result['status'] = 'no_files'
                return result
            
            result['files_processed'] = len(raw_files)
            result['total_size_bytes'] = sum(f['size'] for f in raw_files)
            
            with tempfile.TemporaryDirectory() as temp_dir:
                local_files = self._download_files(raw_files, temp_dir, symbol, date)
                if not local_files:
                    result['status'] = 'download_failed'
                    result['error'] = 'No files downloaded'
                    return result
                
                output_path = Path(temp_dir) / 'data.parquet'
                metadata = self._merge_parquet_files(local_files, output_path)
                
                result['rows'] = metadata['rows']
                result['output_size_bytes'] = output_path.stat().st_size
                
                self._upload_to_s3(output_path, compact_key)
                
                meta_content = {
                    "rows": metadata['rows'],
                    "ts_event_min": metadata['ts_event_min'],
                    "ts_event_max": metadata['ts_event_max'],
                    "source_files": len(raw_files),
                    "schema_version": 1
                }
                self._upload_json_to_s3(meta_content, meta_key)
                
            result['status'] = 'success'
            logger.info(f"[COMPACT] {symbol} {stream} {date} | {len(raw_files)} -> 1 | rows={result['rows']}")
            
        except Exception as e:
            result['status'] = 'failed'
            result['error'] = str(e)
            logger.error(f"FAILED {symbol}/{date}: {e}")
            
        return result
    
    def _compact_exists(self, key: str) -> bool:
        try:
            self.s3_client.head_object(Bucket=self.compact_bucket, Key=key)
            return True
        except ClientError as e:
            if e.response['Error']['Code'] == '404':
                return False
            raise
    
    def _list_raw_files(self, prefix: str) -> List[Dict]:
        """List files for a specific partition (date-bounded)"""
        files = []
        paginator = self.s3_client.get_paginator('list_objects_v2')
        for page in paginator.paginate(Bucket=self.raw_bucket, Prefix=prefix):
            if 'Contents' not in page:
                continue
            for obj in page['Contents']:
                key = obj['Key']
                if not key.endswith('.parquet') or '/._' in key or key.split('/')[-1].startswith('._'):
                    continue
                files.append({'key': key, 'size': obj['Size']})
        return files
    
    def _download_files(self, files: List[Dict], download_dir: str, symbol: str, date: str) -> List[Path]:
        local_files = []
        download_path = Path(download_dir)
        
        def download_file(idx: int, key: str) -> Optional[Path]:
            try:
                filename = f"{idx:04d}_{Path(key).name}"
                local_path = download_path / filename
                self.s3_client.download_file(Bucket=self.raw_bucket, Key=key, Filename=str(local_path))
                return local_path
            except Exception:
                return None
        
        with ThreadPoolExecutor(max_workers=MAX_PARALLEL_DOWNLOADS) as executor:
            futures = {executor.submit(download_file, idx, f['key']): idx for idx, f in enumerate(files)}
            for future in as_completed(futures):
                result = future.result()
                if result:
                    local_files.append(result)
        
        return sorted(local_files)
    
    def _merge_parquet_files(self, input_files: List[Path], output_path: Path) -> dict:
        """
        Merge parquet files, sort by ts_event, and add seq column for deterministic ordering.
        
        seq provides a stable, monotonic intra-day ordering key that guarantees deterministic replay even when multiple events share the same ts_event value.
        
        Schema output: ts_event, seq, ts_recv, exchange, symbol, ...
        seq: 0 to (row_count - 1), monotonically increasing
        """
        dataset = ds.dataset([str(f) for f in input_files], format='parquet')
        table = dataset.to_table()
        
        # Sort by ts_event
        sorted_indices = pa.compute.sort_indices(table, sort_keys=[('ts_event', 'ascending')])
        sorted_table = pa.compute.take(table, sorted_indices)
        
        # Add seq column
        row_count = len(sorted_table)
        seq_array = pa.array(range(row_count), type=pa.int64())
        
        # Schema reconstruction (seq after ts_event)
        old_schema = sorted_table.schema
        new_columns = []
        new_fields = []
        seq_added = False
        
        for i, field in enumerate(old_schema):
            new_columns.append(sorted_table.column(i))
            new_fields.append(field)
            if field.name == 'ts_event':
                new_fields.append(pa.field('seq', pa.int64()))
                new_columns.append(seq_array)
                seq_added = True
        
        if not seq_added:
            new_fields.insert(0, pa.field('seq', pa.int64()))
            new_columns.insert(0, seq_array)
            
        final_table = pa.table(new_columns, schema=pa.schema(new_fields))
        
        metadata = {
            'rows': row_count,
            'ts_event_min': int(pa.compute.min(final_table['ts_event']).as_py()),
            'ts_event_max': int(pa.compute.max(final_table['ts_event']).as_py())
        }
        
        pq.write_table(final_table, output_path, compression='zstd', row_group_size=100000)
        return metadata
    
    def _upload_to_s3(self, local_path: Path, s3_key: str):
        self.s3_client.upload_file(Filename=str(local_path), Bucket=self.compact_bucket, Key=s3_key)
    
    def _upload_json_to_s3(self, content: dict, s3_key: str):
        self.s3_client.put_object(
            Bucket=self.compact_bucket,
            Key=s3_key,
            Body=json.dumps(content, indent=2).encode('utf-8'),
            ContentType='application/json'
        )

    def discover_dates(self) -> Set[str]:
        """
        Fast O(1) discovery of processed dates in raw bucket using delimiters.
        Walks: exchange=/ -> stream=/ -> symbol=/ -> date=/
        """
        logger.info("Discovering available dates in raw bucket...")
        dates = set()
        
        def list_prefixes(bucket: str, prefix: str) -> List[str]:
            paginator = self.s3_client.get_paginator('list_objects_v2')
            prefixes = []
            for page in paginator.paginate(Bucket=bucket, Prefix=prefix, Delimiter='/'):
                for cp in page.get('CommonPrefixes', []):
                    prefixes.append(cp['Prefix'])
            return prefixes

        # Level 1: Exchange
        exchanges = list_prefixes(self.raw_bucket, "exchange=")
        for ex_prefix in exchanges:
            # Level 2: Stream
            streams = list_prefixes(self.raw_bucket, ex_prefix + "stream=")
            for st_prefix in streams:
                # Level 3: Symbol
                symbols = list_prefixes(self.raw_bucket, st_prefix + "symbol=")
                for sy_prefix in symbols:
                    # Level 4: Date
                    date_prefixes = list_prefixes(self.raw_bucket, sy_prefix + "date=")
                    for d_prefix in date_prefixes:
                        # Extract date from "exchange=.../date=YYYYMMDD/"
                        date_str = d_prefix.rstrip('/').split('=')[-1]
                        if len(date_str) == 8 and date_str.isdigit():
                            dates.add(date_str)
        
        return dates

    def discover_partitions_for_date(self, target_date: str) -> List[Dict]:
        """Find all partitions (ex/st/sy) for a specific date using delimiters"""
        partitions = []
        
        def list_prefixes(bucket: str, prefix: str) -> List[str]:
            paginator = self.s3_client.get_paginator('list_objects_v2')
            prefixes = []
            for page in paginator.paginate(Bucket=bucket, Prefix=prefix, Delimiter='/'):
                for cp in page.get('CommonPrefixes', []):
                    prefixes.append(cp['Prefix'])
            return prefixes

        exchanges = list_prefixes(self.raw_bucket, "exchange=")
        for ex_prefix in exchanges:
            streams = list_prefixes(self.raw_bucket, ex_prefix + "stream=")
            for st_prefix in streams:
                symbols = list_prefixes(self.raw_bucket, st_prefix + "symbol=")
                for sy_prefix in symbols:
                    target_prefix = f"{sy_prefix}date={target_date}/"
                    # Check if this date exists for this symbol
                    paginator = self.s3_client.get_paginator('list_objects_v2')
                    for page in paginator.paginate(Bucket=self.raw_bucket, Prefix=target_prefix, MaxKeys=1):
                        if 'Contents' in page:
                            # Partition found
                            parts = sy_prefix.rstrip('/').split('/')
                            partition = {
                                'exchange': parts[0].split('=')[1],
                                'stream': parts[1].split('=')[1],
                                'symbol': parts[2].split('=')[1],
                                'date': target_date
                            }
                            partitions.append(partition)
                            break
        
        return partitions


def get_yesterday_date() -> str:
    return (datetime.now() - timedelta(days=1)).strftime('%Y%m%d')


def get_today_date() -> str:
    return datetime.now().strftime('%Y%m%d')


def format_bytes(bytes_size: int) -> str:
    for unit in ['B', 'KB', 'MB', 'GB']:
        if bytes_size < 1024:
            return f"{bytes_size:.1f} {unit}"
        bytes_size /= 1024
    return f"{bytes_size:.1f} TB"
