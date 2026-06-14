# Cronlog MVP Scope

## Product promise

Cronlog v0.1 is cron with memory: a tiny local scheduler that records every run, captures logs, prevents overlaps, and lets users inspect what happened.

## In scope

- Register jobs
- List jobs
- Foreground daemon
- Manual run-now
- Durable run history
- Captured stdout/stderr
- Basic timeout
- Basic schedule parser
- Enable/disable/remove jobs
- SQLite storage

## Out of scope for v0.1

- Web UI
- Distributed workers
- Cloud sync
- Secrets manager
- Full natural-language schedule parser
- DAGs/dependencies
- Agent runtime
- Notifications
- Service installation

## Demo script

Terminal 1:

```bash
cargo run -- add --name heartbeat --schedule "every 10 seconds" -- echo alive
cargo run -- daemon
```

Terminal 2:

```bash
cargo run -- list
cargo run -- history heartbeat
cargo run -- logs heartbeat --last
cargo run -- run heartbeat --now
```

Failing job demo:

```bash
cargo run -- add --name broken --schedule "every 30 seconds" -- sh -c "echo bad >&2; exit 1"
cargo run -- history broken
cargo run -- logs broken --last
```
