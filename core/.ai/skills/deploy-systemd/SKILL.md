# QuantLab Deploy & Systemd Discipline

## Purpose
Keep deployments repeatable and services resilient on VPS (systemd-first).

## When to use
- Any production service deployment (replayd, strategyd, collector, compact workers)
- Changes that require new env vars, ports, units, or restart logic

## Rules
- systemd is the source of truth (avoid ad-hoc nohup/pm2 drift unless explicitly required).
- Unit files must define:
  - WorkingDirectory
  - EnvironmentFile (or explicit Environment)
  - Restart=on-failure with sane backoff
  - User/group permissions
  - StandardOutput/StandardError to journal (or explicit log path)
- Health check:
  - /health endpoint required
  - post-deploy verify uses curl + journalctl
- Secrets only via env files with correct permissions.

## Deploy checklist (minimum)
- git pull / build step if needed
- restart service
- verify: curl /health, check logs, confirm version hash (if available)

## Output format
1) PLAN
2) FILE PATCH LIST
3) DEPLOY STEPS (commands)
4) VERIFY STEPS (curl + journalctl patterns)
