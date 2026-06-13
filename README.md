# Cron2 MVP

Cron2 is a tiny SQLite-backed scheduler: cron's simple idea, with durable run history, captured logs, timeouts, and overlap protection.

## Scope

Cron2 is intentionally not a workflow orchestrator. It does not try to replace Temporal, Airflow, Prefect, Dagster, Jenkins, or a production queue.

The MVP thesis is narrower: cron-style local scheduling with a durable audit trail. Cron2 should make it easy to answer:

- What ran?
- When was it supposed to run?
- When did it actually start and finish?
- Did it succeed, fail, time out, get skipped, or get interrupted?
- What stdout/stderr did it produce?

Pipeline recovery still belongs in the pipeline. If a video workflow needs checkpoints, cached artifacts, source fallback, retry policy, or publish idempotency, those should live in the workflow code. Cron2 provides scheduler-owned status and logs that the workflow can inspect.

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
- Daemon restart recovery: stale `running` runs are marked `interrupted` with a stderr log, then due jobs can run again normally

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

For a more demanding workload without creating any Windows Scheduled Tasks, use the bounded local bakeoff:

```powershell
.\scripts\bakeoff-local.ps1 -Mode mixed -DurationSeconds 180 -IntervalSeconds 60 -TimeoutSeconds 90
```

The complex workload adds step metrics, retries, fan-out draft generation, quality gates, CPU work, memory pressure, dry-run posting for many artifacts, and overlap detection. Extra complex modes include `cpu`, `memory`, and `mixed`.

To compare Cron2 against a bounded baseline loop using the copied historical video pipeline's real Wikimedia media-pack stage:

```powershell
.\scripts\bakeoff-production-local.ps1 -PipelineRoot C:\Users\admin\Documents\historical-video-agent -DurationSeconds 240 -IntervalSeconds 120
```

This runs the real `scripts\wikimedia_media_pack.py` stage into isolated `cron2` and `baseline-loop` output directories. It does not create Windows Scheduled Tasks.

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
- Cron2 context when run by Cron2: run id, job name, scheduled time, trigger type, and previous run status

Fault-injection switches for smoke tests:

```powershell
.\scripts\jet-fighter-montage-job.ps1 -Scheduler cron2 -OutDir .\jet-montage-hardening-smoke -SimulateOffline
.\scripts\jet-fighter-montage-job.ps1 -Scheduler cron2 -OutDir .\jet-montage-hardening-smoke -SimulateMissingSources
.\scripts\jet-fighter-montage-job.ps1 -Scheduler cron2 -OutDir .\jet-montage-hardening-smoke -SimulateCrashAt after-render
```

After an interrupted or failed run, the next run can reuse cached sources/media/render output and finish from the latest valid checkpoint instead of starting from zero.

Run a bounded three-run comparison, one montage per side immediately and then hourly until each side has three outputs:

```powershell
.\scripts\bakeoff-jet-montage-3h.ps1
```

This uses Cron2 for the `cron2` side and a local cron-style baseline loop for the baseline side. It creates no Windows Scheduled Tasks.

Compare schedulers on launch reliability, missed runs, timeout handling, overlap behavior, failure visibility, log inspection, and how quickly you can answer what happened overnight.

Clean up bakeoff artifacts and any older scheduled-task bakeoff entries:

```powershell
.\scripts\bakeoff-clean.ps1
```

## Notes

This MVP defaults to direct process execution, not shell execution. Commands are stored as argv arrays. For shell features, call the shell explicitly:

```bash
cron2 add --name shell-example --schedule "every 1 minute" -- sh -c "echo hello && date"
```
