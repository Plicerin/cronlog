mod cli;
mod clock;
mod color;
mod db;
mod error;
mod health;
mod hooks;
mod runner;
mod schedule;
mod scheduler;
mod ui;

use clap::Parser;
use cli::{Cli, Commands, HookCommand};
use db::Database;
use error::Result;
use hooks::HookEvent;

fn main() {
    if let Err(e) = run(Cli::parse()) {
        eprintln!("error: {e}");
        std::process::exit(1);
    }
}

fn run(cli: Cli) -> Result<()> {
    let db = Database::open(&cli.db)?;
    db.init()?;
    let json = cli.json;
    let color = color::for_stdout(cli.color);

    match cli.command {
        Commands::Add {
            name,
            schedule,
            timeout,
            max_runs,
            late_limit,
            command,
        } => {
            if command.is_empty() {
                return Err(error::CronlogError::InvalidCommand(
                    "command cannot be empty".into(),
                ));
            }
            let parsed = schedule::parse_schedule(&schedule)?;
            let next_run_at = parsed.next_after(clock::now())?;
            let timeout_seconds = timeout.map(|d| d.as_secs() as i64).unwrap_or(3600);
            db.add_job_with_late_limit(
                &name,
                &command,
                &schedule,
                timeout_seconds,
                max_runs,
                next_run_at,
                late_limit.and_then(cli::LateLimit::seconds),
            )?;
            match max_runs {
                Some(limit) => println!(
                    "Added job '{name}' scheduled for {} (max {limit} scheduled runs)",
                    clock::display(next_run_at)
                ),
                None => println!(
                    "Added job '{name}' scheduled for {}",
                    clock::display(next_run_at)
                ),
            }
        }
        Commands::Edit {
            name,
            schedule,
            timeout,
            late_limit,
        } => {
            if schedule.is_none() && timeout.is_none() && late_limit.is_none() {
                return Err(error::CronlogError::InvalidCommand(
                    "nothing to change: give --schedule, --timeout or --late-limit".into(),
                ));
            }
            if let Some(schedule) = &schedule {
                schedule::parse_schedule(schedule)?;
            }
            let change = db::JobEdit {
                schedule: schedule.clone(),
                timeout_seconds: timeout.map(|d| d.as_secs().max(1) as i64),
                late_limit_seconds: late_limit.map(cli::LateLimit::seconds),
            };
            let next = db.edit_job(&name, &change, |schedule| {
                schedule::parse_schedule(schedule)?.next_after(clock::now())
            })?;
            let mut changed = Vec::new();
            if let Some(schedule) = schedule {
                changed.push(format!("schedule '{schedule}'"));
            }
            if let Some(timeout) = timeout {
                changed.push(format!("timeout {}", humantime::format_duration(timeout)));
            }
            match late_limit {
                Some(cli::LateLimit::After(limit)) => {
                    changed.push(format!("late limit {}", humantime::format_duration(limit)))
                }
                Some(cli::LateLimit::None) => changed.push("no late limit".into()),
                None => {}
            }
            let next = next
                .map(|t| format!(", next run {}", clock::display(t)))
                .unwrap_or_default();
            println!("Updated job '{name}': {}{next}", changed.join(", "));
        }
        Commands::Backup { path, force } => {
            db.backup_to(&path, force)?;
            let size = std::fs::metadata(&path).map(|m| m.len()).unwrap_or(0);
            println!(
                "Backed up {} to {} ({} KB)",
                cli.db,
                path.display(),
                size.div_ceil(1024)
            );
        }
        Commands::List => {
            let jobs = db.list_jobs()?;
            if json {
                ui::print_json(&jobs)?;
            } else {
                ui::print_jobs(&jobs, color);
            }
        }
        Commands::Status { name } => {
            let rows = db.status(name.as_deref())?;
            if json {
                ui::print_json(&rows)?;
            } else {
                ui::print_status(&rows, color);
            }
        }
        Commands::Health {
            job,
            grace,
            failures,
            daemon_stale,
        } => {
            let options = health::HealthOptions {
                job,
                grace: to_chrono(grace),
                failures,
                daemon_stale: to_chrono(daemon_stale),
            };
            let report = health::check(&db, clock::now(), &options)?;
            if json {
                ui::print_json(&report)?;
            } else {
                health::print(&report, color);
            }
            if !report.healthy {
                std::process::exit(1);
            }
        }
        Commands::Daemon {
            poll_seconds,
            retain,
            keep_last,
        } => {
            let retention = match retain {
                cli::Retain::Forever => None,
                cli::Retain::For(age) => Some(scheduler::Retention {
                    max_age: to_chrono(age),
                    keep_last,
                }),
            };
            let kept = match retain {
                cli::Retain::Forever => "keeping all history".to_string(),
                cli::Retain::For(age) => format!(
                    "keeping {} of history (at least {keep_last} runs per job)",
                    humantime::format_duration(age)
                ),
            };
            println!(
                "Cronlog daemon started. polling every {poll_seconds}s, {kept}. press Ctrl+C to stop."
            );
            scheduler::run_daemon(&db, poll_seconds, retention)?;
        }
        Commands::Prune {
            older_than,
            keep_last,
            dry_run,
            vacuum,
        } => {
            let retention = scheduler::Retention {
                max_age: to_chrono(older_than),
                keep_last,
            };
            let now = clock::now();
            let age = humantime::format_duration(older_than);
            if dry_run {
                let count = db.count_prunable(retention.cutoff(now), keep_last)?;
                if json {
                    ui::print_json(&serde_json::json!({
                        "dry_run": true,
                        "runs": count,
                    }))?;
                } else {
                    println!(
                        "Would delete {count} runs scheduled more than {age} ago (keeping each job's newest {keep_last})"
                    );
                }
            } else {
                let before = db.size_bytes()?;
                let report = scheduler::prune_history(&db, retention, now)?;
                db.reclaim_space(vacuum)?;
                let after = db.size_bytes()?;
                if json {
                    ui::print_json(&serde_json::json!({
                        "runs": report.runs,
                        "logs": report.logs,
                        "size_before": before,
                        "size_after": after,
                    }))?;
                } else {
                    println!(
                        "Deleted {} runs and {} log rows scheduled more than {age} ago (kept each job's newest {keep_last}). Database {} -> {} KB",
                        report.runs,
                        report.logs,
                        before / 1024,
                        after / 1024
                    );
                }
            }
        }
        Commands::History { name, limit } => {
            let rows = db.history(&name, limit)?;
            if json {
                ui::print_json(&rows)?;
            } else {
                ui::print_history(&rows, color);
            }
        }
        Commands::Logs { name, last, run_id } => {
            let rid = match (last, run_id) {
                (_, Some(id)) => id,
                (true, None) | (false, None) => db.last_run_id(&name)?.ok_or_else(|| {
                    error::CronlogError::NotFound(format!("no runs found for job '{name}'"))
                })?,
            };
            let logs = db.logs_for_run(rid)?;
            if json {
                ui::print_json(&serde_json::json!({
                    "run_id": rid,
                    "logs": logs,
                }))?;
            } else {
                ui::print_logs(rid, &logs);
            }
        }
        Commands::Run {
            name,
            now: _,
            force,
        } => {
            let job = db.get_job_by_name(&name)?;
            scheduler::run_manual(&db, &job, clock::now(), force)?;
        }
        Commands::Enable { name } => {
            let next = scheduler::enable_job(&db, &name, clock::now())?;
            println!("Enabled job '{name}', next run {}", clock::display(next));
        }
        Commands::Disable { name } => {
            db.set_enabled(&name, false)?;
            println!("Disabled job '{name}'");
        }
        Commands::Remove { name } => {
            db.remove_job(&name)?;
            println!("Removed job '{name}'");
        }
        Commands::Hook { action } => hook_command(&db, action, json, color)?,
    }

    Ok(())
}

fn hook_command(db: &Database, action: HookCommand, json: bool, color: bool) -> Result<()> {
    match action {
        HookCommand::Set { event, command } => {
            let command = hooks::store_command(&command)?;
            db.set_hook_command(event.as_str(), Some(&command))?;
            println!("Set the {} hook: {command}", event.as_str());
        }
        HookCommand::Show => {
            let failure = db.hook_command(HookEvent::Failure.as_str())?;
            let recovery = db.hook_command(HookEvent::Recovery.as_str())?;
            if json {
                ui::print_json(&serde_json::json!({
                    "on_failure": failure,
                    "on_recovery": recovery,
                }))?;
            } else {
                let shown = |c: &Option<String>| c.clone().unwrap_or_else(|| "(not set)".into());
                println!("on failure:  {}", shown(&failure));
                println!("on recovery: {}", shown(&recovery));
            }
        }
        HookCommand::Clear { event } => {
            for each in [HookEvent::Failure, HookEvent::Recovery] {
                if event.is_none_or(|named| named == each) {
                    db.set_hook_command(each.as_str(), None)?;
                    println!("Cleared the {} hook", each.as_str());
                }
            }
        }
        HookCommand::Test { event } => {
            let outcome = hooks::test(db, event)?;
            if json {
                ui::print_json(&serde_json::json!({
                    "event": event.as_str(),
                    "status": outcome.status,
                    "exit_code": outcome.exit_code,
                    "duration_ms": outcome.duration_ms,
                    "error": outcome.error,
                }))?;
            } else {
                let status = color::paint(
                    outcome.status,
                    if outcome.status == "success" {
                        color::GREEN
                    } else {
                        color::RED
                    },
                    color,
                );
                let detail = outcome
                    .error
                    .as_deref()
                    .map(|e| format!(": {e}"))
                    .unwrap_or_default();
                println!(
                    "Tested the {} hook: {status} in {}ms{detail}",
                    event.as_str(),
                    outcome.duration_ms
                );
            }
            if outcome.status != "success" {
                std::process::exit(1);
            }
        }
        HookCommand::History { limit } => {
            let rows = db.hook_history(limit)?;
            if json {
                ui::print_json(&rows)?;
            } else {
                ui::print_hook_history(&rows, color);
            }
        }
    }
    Ok(())
}

/// A std duration as chrono's; one too long for chrono keeps everything.
fn to_chrono(duration: std::time::Duration) -> chrono::Duration {
    chrono::Duration::from_std(duration).unwrap_or(chrono::Duration::MAX)
}
