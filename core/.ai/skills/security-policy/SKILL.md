# QuantLab Security Policy

## Purpose
Enforce consistent auth, rate limits, and permission boundaries across services.

## When to use
- Any endpoint addition/change
- Any /control, /stream, /metrics, /runs endpoints
- Anything that touches tokens, secrets, or external integrations

## Rules
- Auth is required by default for non-public endpoints.
- Support both Authorization: Bearer and (if needed for SSE) token query param.
- Rate limit is required by default; tighter limits for /control and connection endpoints.
- Sensitive actions (kill, cancel, delete, purge) require stronger checks and clear audit logs.
- No secrets in logs.

## Scope/role guidance (simple)
- READ: state, health, metrics (if allowed)
- STREAM: SSE /stream
- CONTROL: kill/stop/pause/resume
If scopes are not implemented, at minimum document intended boundary and keep endpoints grouped.

## Audit logging
- For CONTROL actions: log who/what/when + run_id + result.

## Output format
1) PLAN
2) FILE PATCH LIST
3) VERIFY STEPS
   - unauthorized request rejected
   - rate limit triggers predictably
   - control action logged with context
