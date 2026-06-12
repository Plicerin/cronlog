# Cron2 MVP

Cron2 is a tiny SQLite-backed scheduler: cron's simple idea, with durable run history, captured logs, timeouts, and overlap protection.

## MVP commands

```bash
cargo run -- add --name heartbeat --schedule "every 10 seconds" -- echo alive
cargo run -- list
cargo run -- daemon
cargo run -- history heartbeat
cargo run -- logs heartbeat --last
cargo run -- run heartbeat --now
cargo run -- disable heartbeat
cargo run -- enable heartbeat
cargo run -- remove heartbeat
```

## Supported schedules

- `every N second(s)`
- `every N minute(s)`
- `every N hour(s)`
- `daily at HH:MM`

Examples:

```bash
cron2 add --name sync --schedule "every 15 minutes" -- ./sync.sh
cron2 add --name backup --schedule "daily at 02:00" -- ./backup.sh
```

## Defaults

- Database: `./cron2.db`
- Daemon poll interval: 1 second
- Timeout: 1 hour
- Stdout limit: 256 KB
- Stderr limit: 256 KB
- Overlap behavior: forbid overlapping runs; skipped runs are recorded
- Missed runs: run once, then compute the next future run

## Build

```bash
cargo build --release
./target/release/cron2 --help
```

## Local smoke test

On Windows PowerShell:

```powershell
.\scripts\smoke.ps1
```

The smoke script builds, runs unit tests, creates a temporary database, adds a heartbeat job, runs it, checks history/logs, toggles enablement, and removes the job.

## Real-world bakeoff

The `examples/content_pipeline.ps1` script simulates a content generation and social posting pipeline without touching live accounts. It writes drafts, metadata, JSONL event logs, and dry-run post records under `bakeoff-runs`.

Register the same workload under Cron2:

```powershell
.\scripts\bakeoff-cron2.ps1 -Mode flaky
.\scripts\bakeoff-start-cron2-daemon.ps1
```

Register the same workload under Windows Task Scheduler:

```powershell
.\scripts\bakeoff-task-scheduler.ps1 -Mode flaky
```

Inspect both sides:

```powershell
.\scripts\bakeoff-report.ps1
.\target\debug\cron2.exe --db .\bakeoff-cron2.db history content-pipeline-cron2
.\target\debug\cron2.exe --db .\bakeoff-cron2.db logs content-pipeline-cron2 --last
```

Useful modes:

- `normal`: successful content generation and dry-run posting.
- `flaky`: randomly simulates upstream API failures.
- `fail`: always fails before content generation.
- `hang`: sleeps long enough to test timeout and overlap behavior.
- `large-output`: emits enough stdout to exercise log capture.

Compare schedulers on launch reliability, missed runs, timeout handling, overlap behavior, failure visibility, log inspection, and how quickly you can answer what happened overnight.

Clean up the bakeoff artifacts and scheduled task:

```powershell
.\scripts\bakeoff-clean.ps1
```

## Notes

This MVP defaults to direct process execution, not shell execution. Commands are stored as argv arrays. For shell features, call the shell explicitly:

```bash
cron2 add --name shell-example --schedule "every 1 minute" -- sh -c "echo hello && date"
```
