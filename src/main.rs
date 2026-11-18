mod cli;
mod clock;
mod db;
mod error;
mod runner;
mod schedule;
mod scheduler;
mod ui;

use clap::Parser;
use cli::{Cli, Commands};
use db::Database;
use error::Result;

fn main() -> Result<()> {
    let cli = Cli::parse();
    let db = Database::open(&cli.db)?;
    db.init()?;
    let json = cli.json;

    match cli.command {
        Commands::Add {
            name,
            schedule,
            timeout,
            max_runs,
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
            db.add_job(
                &name,
                &command,
                &schedule,
                timeout_seconds,
                max_runs,
                next_run_at,
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
        Commands::List => {
            let jobs = db.list_jobs()?;
            if json {
                ui::print_json(&jobs)?;
            } else {
                ui::print_jobs(&jobs);
            }
        }
        Commands::Status { name } => {
            let rows = db.status(name.as_deref())?;
            if json {
                ui::print_json(&rows)?;
            } else {
                ui::print_status(&rows);
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
                ui::print_history(&rows);
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
        Commands::Run { name, now: _ } => {
            let job = db.get_job_by_name(&name)?;
            scheduler::run_job_once(&db, &job, clock::now(), "manual")?;
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
    }

    Ok(())
}

/// A std duration as chrono's; one too long for chrono keeps everything.
fn to_chrono(duration: std::time::Duration) -> chrono::Duration {
    chrono::Duration::from_std(duration).unwrap_or(chrono::Duration::MAX)
}
