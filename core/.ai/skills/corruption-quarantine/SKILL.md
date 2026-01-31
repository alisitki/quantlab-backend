# QuantLab Corruption & Quarantine Handling

## Purpose
Handle corrupt/partial data safely without breaking pipelines or silently losing information.

## When to use
- Collector upload validation ideas
- Compact reads encountering snappy/parquet corruption
- Replay reads encountering unreadable files/rowgroups

## Rules
- Never crash the entire pipeline for a small number of corrupt files unless policy says so.
- Never silently skip: log with context and mark quarantine signal.
- Quarantine means: isolate path/list + record reason + keep reproducibility.
- Continue processing remaining healthy data if policy allows.

## Required logging fields
- stream, date, symbol
- file path/key
- error type (snappy/parquet/io)
- action taken (skip/quarantine/abort)
- counts of corrupt vs ok

## Output format
1) PLAN
2) FILE PATCH LIST
3) VERIFY STEPS
   - simulate one corrupt file => pipeline continues + quarantine recorded
