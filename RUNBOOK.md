# Cronlog Production Runbook

This runbook covers pulling Cronlog from GitHub, building it, and using it as a small local replacement for simple cron jobs that need durable history, logs, status, timeout handling, and overlap protection.

Cronlog is not a workflow orchestrator. Keep pipeline recovery, retries across business steps, artifact checkpoints, and publish idempotency inside the pipeline code.

## 1. Fit Check

Use Cronlog when the job schedule fits one of these forms:

- `every N seconds`
- `every N minutes`
- `every N hours`
- `daily at HH:MM`

Keep existing cron or use a mature scheduler when you need:

- complex cron expressions such as weekdays, month rules, or calendars
- distributed workers
- dependency graphs
- web UI
- queue semantics
- built-in alert routing

## 2. Install Prerequisites

Install Rust stable.

Linux:

```bash
curl https://sh.rustup.rs -sSf | sh
. "$HOME/.cargo/env"
rustc --version
cargo --version
```

Windows PowerShell:

```powershell
winget install Rustlang.Rustup
rustc --version
cargo --version
```

## 3. Pull From GitHub

```bash
git clone https://github.com/Plicerin/cronlog.git
cd cronlog
```

For updates later:

```bash
git pull --ff-only
```

## 4. Build

Debug build:

```bash
cargo build
./target/debug/cronlog --help
```

Release build:

```bash
cargo build --release
./target/release/cronlog --help
```

Recommended production install path:

```bash
sudo install -m 0755 target/release/cronlog /usr/local/bin/cronlog
cronlog --help
```

Windows PowerShell:

```powershell
cargo build --release
.\target\release\cronlog.exe --help
```

## 5. Choose Runtime Paths

Use explicit paths in production.

Recommended Linux layout:

```bash
sudo mkdir -p /var/lib/cronlog /var/log/cronlog
sudo chown "$USER":"$USER" /var/lib/cronlog /var/log/cronlog
export CRONLOG_DB=/var/lib/cronlog/cronlog.db
```

Cronlog stores job definitions, run history, and captured stdout/stderr in SQLite.

Back up this file:

```bash
/var/lib/cronlog/cronlog.db
```

## 6. Register Jobs

Cronlog runs commands directly, not through a shell. If you need shell features such as `&&`, redirects, globs, or environment setup, call the shell explicitly.

Simple command:

```bash
cronlog --db "$CRONLOG_DB" add \
  --name heartbeat \
  --schedule "every 1 minutes" \
  --timeout 30s \
  -- echo alive
```

Shell command:

```bash
cronlog --db "$CRONLOG_DB" add \
  --name nightly-import \
  --schedule "daily at 02:00" \
  --timeout 2h \
  -- bash -lc 'cd /srv/myapp && ./scripts/import.sh >> /var/log/myapp/import.log 2>&1'
```

Python pipeline:

```bash
cronlog --db "$CRONLOG_DB" add \
  --name content-pipeline \
  --schedule "every 1 hours" \
  --timeout 45m \
  -- bash -lc 'cd /srv/content-agent && . .venv/bin/activate && python pipeline.py'
```

## 7. Replace Existing Cron Entries

List current cron jobs:

```bash
crontab -l
```

For each cron entry, convert only if Cronlog supports the schedule.

Examples:

```cron
*/15 * * * * /srv/app/sync.sh
```

Becomes:

```bash
cronlog --db "$CRONLOG_DB" add \
  --name sync \
  --schedule "every 15 minutes" \
  --timeout 10m \
  -- /srv/app/sync.sh
```

```cron
0 2 * * * /srv/app/backup.sh
```

Becomes:

```bash
cronlog --db "$CRONLOG_DB" add \
  --name backup \
  --schedule "daily at 02:00" \
  --timeout 2h \
  -- /srv/app/backup.sh
```

Do a side-by-side pilot first:

1. Add the Cronlog job.
2. Run it manually with `cronlog run <name> --now`.
3. Check `status`, `history`, and `logs`.
4. Disable the old crontab line only after Cronlog output matches expectations.

Disable a crontab line by editing:

```bash
crontab -e
```

Comment the old entry with `#` instead of deleting it during the pilot.

## 8. Run The Daemon

Foreground test:

```bash
cronlog --db "$CRONLOG_DB" daemon
```

Create a production systemd service:

```bash
sudo tee /etc/systemd/system/cronlog.service >/dev/null <<'EOF'
[Unit]
Description=Cronlog local scheduler
After=network-online.target
Wants=network-online.target

[Service]
Type=simple
Environment=CRONLOG_DB=/var/lib/cronlog/cronlog.db
ExecStart=/usr/local/bin/cronlog --db /var/lib/cronlog/cronlog.db daemon
Restart=always
RestartSec=5
WorkingDirectory=/var/lib/cronlog

[Install]
WantedBy=multi-user.target
EOF
```

Enable it:

```bash
sudo systemctl daemon-reload
sudo systemctl enable --now cronlog
sudo systemctl status cronlog
```

## 9. Operate

List jobs:

```bash
cronlog --db "$CRONLOG_DB" list
```

Summary status for all jobs:

```bash
cronlog --db "$CRONLOG_DB" status
```

Summary status for one job:

```bash
cronlog --db "$CRONLOG_DB" status content-pipeline
```

Run manually:

```bash
cronlog --db "$CRONLOG_DB" run content-pipeline --now
```

History:

```bash
cronlog --db "$CRONLOG_DB" history content-pipeline --limit 20
```

Logs for the last run:

```bash
cronlog --db "$CRONLOG_DB" logs content-pipeline --last
```

JSON for scripts:

```bash
cronlog --db "$CRONLOG_DB" --json status content-pipeline
cronlog --db "$CRONLOG_DB" --json history content-pipeline --limit 5
cronlog --db "$CRONLOG_DB" --json logs content-pipeline --last
```

Disable or enable a job:

```bash
cronlog --db "$CRONLOG_DB" disable content-pipeline
cronlog --db "$CRONLOG_DB" enable content-pipeline
```

Remove future scheduling while keeping run history:

```bash
cronlog --db "$CRONLOG_DB" remove content-pipeline
```

## 10. Monitoring Checks

Basic systemd health:

```bash
systemctl is-active cronlog
journalctl -u cronlog -n 100 --no-pager
```

Basic job health:

```bash
cronlog --db "$CRONLOG_DB" --json status
```

Look for:

- `last_status` not equal to `success`
- `running_runs` greater than `0` for longer than expected
- `last_finished_at` older than expected
- repeated `timed_out`, `failed`, `skipped`, or `interrupted` runs

Cronlog marks stale `running` runs as `interrupted` when the daemon restarts. Treat that as a signal to inspect whether the job left partial output.

## 11. Backup And Upgrade

Back up the DB before upgrading:

```bash
systemctl stop cronlog
cp /var/lib/cronlog/cronlog.db "/var/lib/cronlog/cronlog.db.$(date +%Y%m%d%H%M%S).bak"
systemctl start cronlog
```

Upgrade:

```bash
cd /opt/cronlog
git pull --ff-only
cargo build --release
sudo systemctl stop cronlog
sudo install -m 0755 target/release/cronlog /usr/local/bin/cronlog
sudo systemctl start cronlog
cronlog --db /var/lib/cronlog/cronlog.db status
```

## 12. Rollback

If Cronlog is not behaving correctly:

1. Stop Cronlog.
2. Re-enable the old cron entries.
3. Start cron.
4. Inspect Cronlog history and logs.

Linux:

```bash
sudo systemctl stop cronlog
crontab -e
sudo systemctl restart cron
```

If the system uses `crond`:

```bash
sudo systemctl restart crond
```

Rollback the binary:

```bash
sudo install -m 0755 /path/to/previous/cronlog /usr/local/bin/cronlog
sudo systemctl restart cronlog
```

## 13. Production Pilot Checklist

- [ ] Build release binary.
- [ ] Create `/var/lib/cronlog`.
- [ ] Register one low-risk job.
- [ ] Run it manually.
- [ ] Verify `status`, `history`, and `logs`.
- [ ] Run daemon in foreground for one interval.
- [ ] Install systemd service.
- [ ] Let Cronlog and cron run side by side for one cycle if safe.
- [ ] Comment out the old cron line.
- [ ] Monitor for at least one full schedule interval.
- [ ] Document rollback command for the job owner.
