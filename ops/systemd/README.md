# QuantLab BigHunt Scheduler (systemd)

Timer runs every 30 minutes (`OnUnitActiveSec=30min`); scheduler itself enforces
the active window (`21:00..08:00 Europe/Istanbul`).

Install units:

```bash
sudo cp ops/systemd/quantlab-bighunt-scheduler.service /etc/systemd/system/
sudo cp ops/systemd/quantlab-bighunt-scheduler.timer /etc/systemd/system/
sudo systemctl daemon-reload
sudo systemctl enable --now quantlab-bighunt-scheduler.timer
```

Inspect timer and logs:

```bash
systemctl list-timers | rg quantlab
journalctl -u quantlab-bighunt-scheduler.service -n 200 --no-pager
```
