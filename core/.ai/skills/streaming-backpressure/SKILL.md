# QuantLab Streaming & Backpressure

## Purpose
Make streaming endpoints reliable under disconnects, slow clients, and multiple consumers.

## When to use
- SSE endpoints (/stream)
- Fanout (one producer, many consumers)
- Reconnect logic, buffering, batching, heartbeat

## Rules
- Resume semantics: client provides lastCursor, server resumes from it.
- Reconnect: exponential backoff on client; server must be stateless or replayable.
- Heartbeats allowed, must be clearly separated from data events.
- Bounded buffering required:
  - define max buffer size or max in-flight batches per client
  - if client is too slow: apply backpressure strategy (pause read / smaller batches)
- Never drop data silently.
- If forced to disconnect slow clients: log reason + lastCursor sent.

## Multi-consumer fanout
- Prefer single read path + per-client cursors (if implemented).
- Ensure deterministic per-client ordering.

## Output format
1) PLAN
2) FILE PATCH LIST
3) VERIFY STEPS
   - slow client test (buffer stays bounded)
   - disconnect/reconnect continues from lastCursor
   - 2 clients receive ordered streams
