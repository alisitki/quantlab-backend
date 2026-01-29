"""
QuantLab Streaming K-Way Merge Writer (Production Grade)

Algorithm:
1. Entry point: merge()
2. If num_files > MAX_OPEN_FILES: perform hierarchical chunked merge.
3. Check for Fast Path: if files are strictly non-overlapping, skip k-way and just concatenate batches.
4. Otherwise: perform k-way merge using min-heap for deterministic ordering.
5. Optimized loop: uses tuples and columnar buffering to minimize Python overhead.
"""

import heapq
import time
import logging
import shutil
import tempfile
import hashlib
from pathlib import Path
from datetime import datetime
from typing import List, Dict, Optional, Tuple, Any, Callable

import pyarrow as pa
import pyarrow.parquet as pq

logger = logging.getLogger(__name__)

# Configuration constants
MERGE_BATCH_SIZE = 100_000          # Rows per input batch
MERGE_OUTPUT_BUFFER_SIZE = 200_000  # Rows before flush to writer
MERGE_LOG_INTERVAL = 5_000_000      # Log progress every N rows
MAX_OPEN_FILES = 1200               # Max files to open simultaneously (safe for ulimit)


class FileStream:
    """
    Manages streaming reads from a single parquet file.
    Holds only one batch in memory at a time.
    """
    
    __slots__ = ['file_idx', 'path', 'pf', 'batch_iter', 'current_batch', 
                 'batch_row_idx', 'global_row_idx', 'exhausted', 'schema', 'col_names', 'decode_dicts']
    
    def __init__(self, file_idx: int, path: Path, batch_size: int, decode_dicts: bool = False):
        self.file_idx = file_idx
        self.path = path
        self.decode_dicts = decode_dicts
        try:
            self.pf = pq.ParquetFile(path)
        except Exception as e:
            raise ValueError(f"Failed to open {path}: {e}") from e
            
        if self.decode_dicts:
            self.schema = self._get_decoded_schema(self.pf.schema_arrow)
        else:
            self.schema = self.pf.schema_arrow
            
        self.col_names = self.schema.names
        self.batch_iter = self.pf.iter_batches(batch_size=batch_size)
        self.current_batch: Optional[pa.RecordBatch] = None
        self.batch_row_idx = 0
        self.global_row_idx = 0
        self.exhausted = False
        
        # Load first batch
        self._load_next_batch()
    
    def _load_next_batch(self):
        """Load next batch from iterator."""
        try:
            self.current_batch = next(self.batch_iter)
            self.batch_row_idx = 0
        except StopIteration:
            self.current_batch = None
            self.exhausted = True
        except Exception as e:
            raise ValueError(f"Failed to read batch from {self.path}: {e}") from e
            
        if self.current_batch and self.decode_dicts:
            self.current_batch = self._decode_batch(self.current_batch)

    def _get_decoded_schema(self, schema: pa.Schema) -> pa.Schema:
        new_fields = []
        for field in schema:
            if pa.types.is_dictionary(field.type):
                new_fields.append(pa.field(field.name, field.type.value_type, nullable=field.nullable))
            else:
                new_fields.append(field)
        return pa.schema(new_fields)

    def _decode_batch(self, batch: pa.RecordBatch) -> pa.RecordBatch:
        new_arrays = []
        for col in batch.columns:
            if pa.types.is_dictionary(col.type):
                new_arrays.append(pa.compute.dictionary_decode(col))
            else:
                new_arrays.append(col)
        return pa.RecordBatch.from_arrays(new_arrays, schema=self.schema)
    
    def has_rows(self) -> bool:
        """Check if stream has more rows available."""
        return not self.exhausted
    
    def peek_sort_key(self) -> Tuple[int, int, int]:
        """
        Get sort key for current row: (ts_event, file_idx, global_row_idx)
        """
        ts_event = self.current_batch['ts_event'][self.batch_row_idx].as_py()
        return (ts_event, self.file_idx, self.global_row_idx)
    
    def get_current_row_tuple(self) -> Tuple:
        """Get current row as tuple of python objects (optimized)."""
        return tuple(self.current_batch[i][self.batch_row_idx].as_py() for i in range(len(self.col_names)))
    
    def advance(self):
        """Move to next row. Load new batch if needed."""
        self.batch_row_idx += 1
        self.global_row_idx += 1
        
        if self.batch_row_idx >= len(self.current_batch):
            self._load_next_batch()
    
    def close(self):
        """Close the parquet file."""
        try:
            self.pf.close()
        except:
            pass


class HeapEntry:
    """Wrapper for heap entries to enable proper comparison."""
    __slots__ = ['key', 'stream']
    def __init__(self, key: Tuple[int, int, int], stream: FileStream):
        self.key = key
        self.stream = stream
    def __lt__(self, other: 'HeapEntry') -> bool:
        return self.key < other.key


class StreamingMergeWriter:
    """
    Streaming external k-way merge for parquet files (Production Grade).
    """
    
    def __init__(
        self,
        input_files: List[Path],
        output_path: Path,
        batch_size: int = MERGE_BATCH_SIZE,
        output_buffer_size: int = MERGE_OUTPUT_BUFFER_SIZE,
        log_interval: int = MERGE_LOG_INTERVAL,
        max_open_files: int = MAX_OPEN_FILES,
        add_seq_column: bool = True,
        check_shutdown: Optional[Callable[[], bool]] = None,
        decode_dictionaries: bool = False
    ):
        self.input_files = sorted(input_files)
        self.output_path = output_path
        self.batch_size = batch_size
        self.output_buffer_size = output_buffer_size
        self.log_interval = log_interval
        self.max_open_files = max_open_files
        self.add_seq_column = add_seq_column
        self.check_shutdown = check_shutdown or (lambda: False)
        self.decode_dictionaries = decode_dictionaries
        
        # State
        self.streams: List[FileStream] = []
        self.heap: List[HeapEntry] = []
        self.schema: Optional[pa.Schema] = None
        self.writer: Optional[pq.ParquetWriter] = None
        self.output_buffer: List[Tuple] = []
        self.seq_idx = -1
        
        # Stats
        self.rows_written = 0
        self.ts_event_min: Optional[int] = None
        self.ts_event_max: Optional[int] = None
        self.start_time: Optional[datetime] = None
        
        # Detailed timings
        self.t_init = 0.0
        self.t_loop = 0.0
        self.t_flush = 0.0
    
    def merge(self) -> Dict:
        """Entry point for merging."""
        self.start_time = datetime.utcnow()
        num_files = len(self.input_files)
        
        if num_files > self.max_open_files:
            return self._hierarchical_merge()
        else:
            is_ordered, reason = self._check_ordering()

            if is_ordered:
                logger.info("FASTPATH=ON: Files are strictly ordered. Skipping k-way merge.")
                try:
                    return self._fast_concat()
                except Exception as e:
                    if "more than one dictionary" in str(e).lower() and not self.decode_dictionaries:
                        logger.warning(f"FASTPATH=FALLBACK: dictionary_conflict in fast-path. Retrying with decoding.")
                        self.decode_dictionaries = True
                        return self.merge()
                    raise
            else:
                logger.warning(f"FASTPATH=FALLBACK: {reason}. Switching to k-way merge.")
                try:
                    return self._direct_merge()
                except Exception as e:
                    if "more than one dictionary" in str(e).lower() and not self.decode_dictionaries:
                        logger.warning(f"FASTPATH=FALLBACK: dictionary_conflict in direct merge. Retrying with decoding.")
                        self.decode_dictionaries = True
                        return self.merge()
                    raise
    
    def _hierarchical_merge(self) -> Dict:
        """Perform chunked merge for large file counts."""
        num_files = len(self.input_files)
        temp_dir = tempfile.mkdtemp(prefix='merge_intermediate_')
        intermediate_files = []
        
        try:
            chunk_idx = 0
            for i in range(0, num_files, self.max_open_files):
                if self.check_shutdown(): raise InterruptedError()
                chunk_files = self.input_files[i:i + self.max_open_files]
                chunk_output = Path(temp_dir) / f"chunk_{chunk_idx:04d}.parquet"
                
                logger.info(f"Merging chunk {chunk_idx}: files {i} to {i + len(chunk_files) - 1}")
                chunk_merger = StreamingMergeWriter(
                    input_files=chunk_files,
                    output_path=chunk_output,
                    batch_size=self.batch_size,
                    output_buffer_size=self.output_buffer_size,
                    max_open_files=self.max_open_files,
                    add_seq_column=False,
                    check_shutdown=self.check_shutdown
                )
                chunk_merger.merge()
                intermediate_files.append(chunk_output)
                chunk_idx += 1
            
            logger.info(f"All chunks merged. Now merging {len(intermediate_files)} intermediate files.")
            final_merger = StreamingMergeWriter(
                input_files=intermediate_files,
                output_path=self.output_path,
                batch_size=self.batch_size,
                output_buffer_size=self.output_buffer_size,
                max_open_files=self.max_open_files,
                add_seq_column=self.add_seq_column,
                check_shutdown=self.check_shutdown
            )
            return final_merger.merge()
        finally:
            shutil.rmtree(temp_dir, ignore_errors=True)

    def _check_ordering(self) -> Tuple[bool, str]:
        """Check if files are strictly non-overlapping and sorted by ts_event."""
        if len(self.input_files) <= 1: 
            return True, "single_file"
        try:
            prev_max = -1
            for path in self.input_files:
                pf = pq.ParquetFile(path)
                ts_idx = -1
                for i, name in enumerate(pf.schema_arrow.names):
                    if name == 'ts_event': ts_idx = i; break
                if ts_idx == -1: return False, "missing_ts_event"
                
                # Check row group statistics
                stats = pf.metadata.row_group(0).column(ts_idx).statistics
                if not stats or not stats.has_min_max: 
                    return False, f"missing_stats:{path.name}"
                
                f_min = stats.min
                f_max = stats.max
                
                if f_min < prev_max:
                    return False, f"overlap:current_min({f_min}) < prev_max({prev_max}) at {path.name}"
                
                prev_max = f_max
            return True, "strictly_ordered"
        except Exception as e:
            return False, f"error:{str(e)}"

    def _fast_concat(self) -> Dict:
        """Fast path for non-overlapping files."""
        t0 = time.perf_counter()
        first_pf = pq.ParquetFile(self.input_files[0])
        base_schema = first_pf.schema_arrow
        ts_pos = -1
        
        if self.add_seq_column:
            new_fields = []
            for i, field in enumerate(base_schema):
                new_fields.append(field)
                if field.name == 'ts_event':
                    new_fields.append(pa.field('seq', pa.int64()))
                    ts_pos = i
            self.schema = pa.schema(new_fields)
        else:
            self.schema = base_schema
            
        self.writer = pq.ParquetWriter(self.output_path, self.schema, compression='zstd')
        seq = 0
        
        for path in self.input_files:
            if self.check_shutdown(): raise InterruptedError()
            pf = pq.ParquetFile(path)
            ts_idx = [j for j, n in enumerate(pf.schema_arrow.names) if n == 'ts_event'][0]
            stats = pf.metadata.row_group(0).column(ts_idx).statistics
            if stats:
                if self.ts_event_min is None or stats.min < self.ts_event_min: self.ts_event_min = stats.min
                if self.ts_event_max is None or stats.max > self.ts_event_max: self.ts_event_max = stats.max
                
            for batch in pf.iter_batches(batch_size=self.batch_size):
                if self.add_seq_column:
                    seq_arr = pa.array(range(seq, seq + batch.num_rows), type=pa.int64())
                    arrays = [batch.column(j) for j in range(batch.num_columns)]
                    arrays.insert(ts_pos + 1, seq_arr)
                    batch = pa.RecordBatch.from_arrays(arrays, schema=self.schema)
                    seq += batch.num_rows
                self.writer.write_batch(batch)
                self.rows_written += batch.num_rows
            
        self.writer.close()
        self.t_loop = time.perf_counter() - t0
        return self._build_metadata()

    def _direct_merge(self) -> Dict:
        """Standard k-way merge with columnar optimizations."""
        try:
            t0 = time.perf_counter()
            self._init_streams()
            self._init_heap()
            self.t_init = time.perf_counter() - t0
            if not self.heap: return self._build_metadata()
            self._init_schema_and_writer()
            
            if self.add_seq_column:
                self.seq_idx = [i for i, n in enumerate(self.schema.names) if n == 'seq'][0]
            
            t0 = time.perf_counter()
            self._merge_loop()
            self.t_loop = time.perf_counter() - t0
            
            if self.output_buffer:
                tf0 = time.perf_counter()
                self._flush_buffer()
                self.t_flush += (time.perf_counter() - tf0)
            
            if self.writer: self.writer.close(); self.writer = None
            return self._build_metadata()
        finally:
            self._cleanup()

    def _init_streams(self):
        for idx, path in enumerate(self.input_files):
            self.streams.append(FileStream(idx, path, self.batch_size, self.decode_dictionaries))

    def _init_heap(self):
        for s in self.streams:
            if s.has_rows(): heapq.heappush(self.heap, HeapEntry(s.peek_sort_key(), s))

    def _init_schema_and_writer(self):
        base_schema = self.streams[0].schema
        if self.add_seq_column:
            fields = []
            for f in base_schema:
                fields.append(f)
                if f.name == 'ts_event': fields.append(pa.field('seq', pa.int64()))
            self.schema = pa.schema(fields)
        else:
            self.schema = base_schema
        self.writer = pq.ParquetWriter(self.output_path, self.schema, compression='zstd', write_statistics=True)

    def _merge_loop(self):
        last_log = 0
        while self.heap:
            entry = heapq.heappop(self.heap)
            s = entry.stream
            ts = entry.key[0]
            if self.ts_event_min is None or ts < self.ts_event_min: self.ts_event_min = ts
            if self.ts_event_max is None or ts > self.ts_event_max: self.ts_event_max = ts
            
            self.output_buffer.append(s.get_current_row_tuple())
            s.advance()
            if s.has_rows(): heapq.heappush(self.heap, HeapEntry(s.peek_sort_key(), s))
            
            if len(self.output_buffer) >= self.output_buffer_size:
                tf0 = time.perf_counter()
                self._flush_buffer()
                self.t_flush += (time.perf_counter() - tf0)
            
            if self.rows_written - last_log >= self.log_interval:
                logger.info(f"Progress: {self.rows_written:,} rows written")
                last_log = self.rows_written

    def _flush_buffer(self):
        if not self.output_buffer: return
        n_rows = len(self.output_buffer)
        n_cols = len(self.schema)
        cols = [[] for _ in range(n_cols)]
        
        if self.add_seq_column:
            seq_start = self.rows_written
            for r, row in enumerate(self.output_buffer):
                ci = 0
                for oi in range(n_cols):
                    if oi == self.seq_idx: cols[oi].append(seq_start + r)
                    else: cols[oi].append(row[ci]); ci += 1
        else:
            for row in self.output_buffer:
                for i in range(n_cols): cols[i].append(row[i])
        
        batch = pa.RecordBatch.from_arrays([pa.array(cols[i], type=self.schema.field(i).type) for i in range(n_cols)], schema=self.schema)
        self.writer.write_batch(batch)
        self.rows_written += n_rows
        self.output_buffer.clear()

    def _build_metadata(self) -> Dict:
        dur = None
        if self.start_time: dur = int((datetime.utcnow() - self.start_time).total_seconds() * 1000)
        
        # Calculate SHA256 of the output file
        sha256 = "N/A"
        try:
            if self.output_path.exists():
                h = hashlib.sha256()
                with open(self.output_path, "rb") as f:
                    for chunk in iter(lambda: f.read(4096), b""):
                        h.update(chunk)
                sha256 = h.hexdigest()
        except: pass

        return {
            'rows': self.rows_written, 
            'ts_event_min': self.ts_event_min, 
            'ts_event_max': self.ts_event_max,
            'sha256': sha256,
            'input_parts': len(self.input_files), 
            'created_at': datetime.utcnow().isoformat() + 'Z',
            'compaction_version': 'kway-merge-v1-opt', 
            'duration_ms': dur,
            'timings': {'init': self.t_init, 'loop': self.t_loop, 'flush': self.t_flush}
        }

    def _cleanup(self):
        if self.writer:
            try: self.writer.close()
            except: pass
        for s in self.streams: s.close()
        self.streams.clear(); self.heap.clear(); self.output_buffer.clear()
