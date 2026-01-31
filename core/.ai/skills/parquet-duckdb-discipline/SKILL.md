# QuantLab Parquet + DuckDB Discipline

## Purpose
Make Parquet reads predictable, efficient, and safe; avoid accidental full scans and schema drift.

## When to use
- Anything reading/writing parquet
- DuckDB queries, scans, filtering, pagination
- Any schema evolution

## Rules
- Always pin/validate schema_version before processing.
- Prefer predicate pushdown: filter by partition keys first (stream/date/symbol).
- Avoid full scans by default: page/window reads (cursor range or LIMIT + ORDER).
- Keep query plans deterministic: explicit ORDER BY with stable tie-breakers.
- Log: files read, rows read, rows emitted, duration_ms.

## Schema evolution
- If schema changes: bump schema_version and document compatibility.
- Ensure old partitions remain readable or have migration plan.

## Output format
1) PLAN
2) FILE PATCH LIST
3) VERIFY STEPS
   - query reads only expected partitions
   - row counts match expectations
