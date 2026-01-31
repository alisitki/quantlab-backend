# QuantLab Interface & Contract Discipline

## Purpose
Prevent breaking changes across services by enforcing versioning and stable schemas.

## When to use
- Adding/changing endpoints
- Changing cursor format, event shape, schema_version
- Any change that affects UI/strategyd/replayd interaction

## Rules
- Stable JSON fields: do not change types (string->number etc).
- If a contract must change:
  - introduce versioning (api_version or schema_version)
  - provide migration notes and backward-compat period
- Cursor must remain backwards compatible or versioned explicitly.
- Event shape changes must be versioned; include a compatibility plan.

## Required outputs in PR
- Contract diff (old vs new behavior)
- Upgrade path (what clients must do)

## Output format
1) PLAN
2) CONTRACT DIFF (bulleted)
3) FILE PATCH LIST
4) VERIFY STEPS
