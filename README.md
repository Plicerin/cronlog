# Cronlog MVP

[![CI](https://github.com/Plicerin/cronlog/actions/workflows/ci.yml/badge.svg)](https://github.com/Plicerin/cronlog/actions/workflows/ci.yml)

Cronlog is a tiny SQLite-backed scheduler: cron's simple idea, with durable run history, captured logs, timeouts, and overlap protection.

## Scope

Cronlog is intentionally not a workflow orchestrator. It does not try to replace Temporal, Airflow, Prefect, Dagster, Jenkins, or a production queue.

The MVP thesis is narrower: cron-style local scheduling with a durable audit trail. Cronlog should make it easy to answer:

- What ran?
- When was it supposed to run?
- When did it actually start and finish?
- Did it succeed, fail, time out, get skipped, or get interrupted?
- What stdout/stderr did it produce?

Pipeline recovery still belongs in the pipeline. If a video workflow needs checkpoints, cached artifacts, source fallback, retry policy, or publish idempotency, those should live in the workflow code. Cronlog provides scheduler-owned status and logs that the workflow can inspect.

## MVP commands

```bash
cargo run -- add --name heartbeat --schedule "every 10 seconds" -- echo alive
cargo run -- add --name pilot --schedule "every 1 hours" --max-runs 12 -- ./pipeline.sh
cargo run -- list
cargo run -- status
cargo run -- daemon
cargo run -- history heartbeat
cargo run -- logs heartbeat --last
cargo run -- run heartbeat --now
cargo run -- disable heartbeat
cargo run -- enable heartbeat
cargo run -- remove heartbeat
cargo run -- prune --older-than 30d --dry-run
```

Inspection commands also support `--json` for scripts:

```bash
cronlog --json list
cronlog --json status heartbeat
cronlog --json history heartbeat --limit 5
cronlog --json logs heartbeat --last
```

## Supported schedules

- `every N second(s)`
- `every N minute(s)`
- `every N hour(s)`
- `daily at HH:MM`, in the machine's local time zone

Examples:

```bash
cronlog add --name sync --schedule "every 15 minutes" -- ./sync.sh
cronlog add --name backup --schedule "daily at 02:00" -- ./backup.sh
cronlog add --name pilot --schedule "every 1 hours" --max-runs 12 -- ./pipeline.sh
```

## Defaults

- Database: `./cronlog.db`
- Daemon poll interval: 1 second
- Timeout: 1 hour
- Stdout limit: 256 KB
- Stderr limit: 256 KB
- Concurrency: due jobs run in parallel, each on its own thread, so a long job never delays another job
- Overlap behavior: forbid overlapping runs of the same job; skipped runs are recorded
- Output: stdout/stderr are read while the job runs, so large output never blocks it. If a background process the job started keeps them open, Cronlog captures 5 more seconds after the job exits, then closes its end: that process's next write to stdout/stderr fails (on Unix it gets SIGPIPE). Redirect a long-lived background process's output to a file
- Enabling a job whose next run passed while it was disabled schedules the next future slot instead of running it at once. Interval jobs keep their phase (an `every 10 minutes` job due at 12:00, enabled at 12:35, next runs at 12:40), and enabling and rescheduling happen in one transaction, so the daemon never runs the stale slot
- Timeouts: kill the job's whole process tree (its process group on Unix, `taskkill /T` on Windows), not only the direct child. Because each job leads its own process group, Ctrl+C on a foreground daemon does not stop running jobs; under systemd, stopping the service stops them
- Missed runs: run once, then compute the next future run
- Daemon restart recovery: stale `running` runs are marked `interrupted` with a stderr log, then due jobs can run again normally
- Worker failure recovery: if a run's worker cannot record its result (a database error or a panic), the daemon marks that run `interrupted` on the next tick, retrying until the database accepts it, so the job's later slots are not skipped
- Bounded runs: optional `--max-runs N`; scheduled runs that actually start count, manual runs and overlap skips do not
- History retention: once a day the daemon deletes runs (and their logs) scheduled more than 30 days ago. It works one bounded step per tick (500 runs, then about 4 MB of freed space at a time), so jobs never wait long to record results. It always keeps each job's newest 20 runs, every `running` run, and, for a job with `--max-runs`, the runs that count toward its limit. Change it with `daemon --retain 90d --keep-last 50`, or keep everything with `--retain forever`
- Database size: new databases use SQLite's incremental auto-vacuum, so pruning shrinks the file. A database created by an older Cronlog reuses the freed space but does not shrink until you run `cronlog prune --vacuum` once (it rewrites the file and locks it while it runs; stop the daemon first if the file is large)

## Times and daylight saving

Cronlog stores every time in UTC and uses local time only to show times and to read a `daily at` schedule:

- Interval jobs run at an even real-time pace through clock changes: an `every 5 minutes` job runs every 5 minutes through the night the clocks go back, not pausing for an hour
- A `daily at` job keeps its wall-clock time, so the day the clocks change it runs 23 or 25 hours after the day before
- A `daily at` time that happens twice when the clocks go back (01:30 in the US) runs once, the first time
- A `daily at` time the clocks skip when they go forward (02:30 in the US) runs when the clock jumps, at 03:00
- Tables show local time with its UTC offset (`2026-11-01 01:30:00 -04:00`), so the two 01:30s of a fall-back night can be told apart. JSON times are RFC 3339 local time with offset (`2026-11-17T12:48:00.5-05:00`)
- Jobs get `CRONLOG_SCHEDULED_FOR` as local wall-clock time without an offset, as before

A database written by an older Cronlog, which stored local times, is converted to UTC the first time a new Cronlog opens it, after a backup copy is made. See RUNBOOK.md, Backup And Upgrade.

## Pruning by hand

```bash
cronlog prune --older-than 30d --dry-run   # count what would go
cronlog prune --older-than 30d             # delete it
cronlog prune --older-than 30d --vacuum    # delete, then compact the file
cronlog --json prune --older-than 7d --keep-last 5
```

## Build

```bash
cargo build --release
./target/release/cronlog --help
```

For production installation and migration from cron, see [RUNBOOK.md](RUNBOOK.md).

## Local smoke test

On Windows PowerShell:

```powershell
.\scripts\smoke.ps1
```

The smoke script builds, runs unit tests, creates a temporary database, adds a heartbeat job, runs it, checks history/logs, toggles enablement, and removes the job.

## Real-world bakeoff

The `examples/content_pipeline.ps1` script simulates a content generation and social posting pipeline without touching live accounts. It writes drafts, metadata, JSONL event logs, and dry-run post records under `bakeoff-runs`.

Register the same workload under Cronlog:

```powershell
.\scripts\bakeoff-cronlog.ps1 -Mode flaky
.\scripts\bakeoff-start-cronlog-daemon.ps1
```

Register the same workload under Windows Task Scheduler:

```powershell
.\scripts\bakeoff-task-scheduler.ps1 -Mode flaky
```

Inspect both sides:

```powershell
.\scripts\bakeoff-report.ps1
.\target\debug\cronlog.exe --db .\bakeoff-cronlog.db history content-pipeline-cronlog
.\target\debug\cronlog.exe --db .\bakeoff-cronlog.db logs content-pipeline-cronlog --last
```

Useful modes:

- `normal`: successful content generation and dry-run posting.
- `flaky`: randomly simulates upstream API failures.
- `fail`: always fails before content generation.
- `hang`: sleeps long enough to test timeout and overlap behavior.
- `large-output`: emits enough stdout to exercise log capture.

For a more demanding workload without creating any Windows Scheduled Tasks, use the bounded local bakeoff:

```powershell
.\scripts\bakeoff-local.ps1 -Mode mixed -DurationSeconds 180 -IntervalSeconds 60 -TimeoutSeconds 90
```

The complex workload adds step metrics, retries, fan-out draft generation, quality gates, CPU work, memory pressure, dry-run posting for many artifacts, and overlap detection. Extra complex modes include `cpu`, `memory`, and `mixed`.

To compare Cronlog against a bounded baseline loop using the copied historical video pipeline's real Wikimedia media-pack stage:

```powershell
.\scripts\bakeoff-production-local.ps1 -PipelineRoot C:\Users\admin\Documents\historical-video-agent -DurationSeconds 240 -IntervalSeconds 120
```

This runs the real `scripts\wikimedia_media_pack.py` stage into isolated `cronlog` and `baseline-loop` output directories. It does not create Windows Scheduled Tasks.

## Jet Fighter Montage Pipeline

Create a vertical MP4 montage from Wikimedia Commons jet fighter photos:

```powershell
.\scripts\jet-fighter-montage.ps1 -OutDir .\jet-fighter-montage-output -Photos 8 -SecondsPerPhoto 3
```

The pipeline downloads source images, renders `jet-fighter-montage.mp4`, and writes `montage-manifest.json` with source URLs, license metadata, warnings, and output details.

The job wrapper also keeps a stable per-scheduler recovery state under `<out>\<scheduler>\state` with:

- stage checkpoints for source search, download, render, manifest, and pipeline completion
- cached source metadata and downloaded media
- failure classification for connectivity, missing source, crash, and generic pipeline errors
- Cronlog context when run by Cronlog: run id, job name, scheduled time, trigger type, and previous run status

Fault-injection switches for smoke tests:

```powershell
.\scripts\jet-fighter-montage-job.ps1 -Scheduler cronlog -OutDir .\jet-montage-hardening-smoke -SimulateOffline
.\scripts\jet-fighter-montage-job.ps1 -Scheduler cronlog -OutDir .\jet-montage-hardening-smoke -SimulateMissingSources
.\scripts\jet-fighter-montage-job.ps1 -Scheduler cronlog -OutDir .\jet-montage-hardening-smoke -SimulateCrashAt after-render
```

After an interrupted or failed run, the next run can reuse cached sources/media/render output and finish from the latest valid checkpoint instead of starting from zero.

Run a bounded three-run comparison, one montage per side immediately and then hourly until each side has three outputs:

```powershell
.\scripts\bakeoff-jet-montage-3h.ps1
```

This uses Cronlog for the `cronlog` side and a local cron-style baseline loop for the baseline side. It creates no Windows Scheduled Tasks.

Compare schedulers on launch reliability, missed runs, timeout handling, overlap behavior, failure visibility, log inspection, and how quickly you can answer what happened overnight.

Clean up bakeoff artifacts and any older scheduled-task bakeoff entries:

```powershell
.\scripts\bakeoff-clean.ps1
```

## Notes

This MVP defaults to direct process execution, not shell execution. Commands are stored as argv arrays. For shell features, call the shell explicitly:

```bash
cronlog add --name shell-example --schedule "every 1 minute" -- sh -c "echo hello && date"
```
