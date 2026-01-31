# QuantLab Service Scaffold

## When to use
Creating or modifying backend services.

## Required
- Config loader
- Structured logging
- /health endpoint
- Auth middleware
- Rate limit
- Graceful shutdown

## Standard layout
service/
  index.js
  routes/
  middleware/
  lib/
  config.js

## Output
- File tree
- Required env vars
- Health check command
