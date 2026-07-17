mod cli;
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
            let next_run_at = parsed.next_after(chrono::Local::now().naive_local())?;
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
                    "Added job '{name}' scheduled for {next_run_at} (max {limit} scheduled runs)"
                ),
                None => println!("Added job '{name}' scheduled for {next_run_at}"),
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
        Commands::Daemon { poll_seconds } => {
            println!(
                "Cronlog daemon started. polling every {poll_seconds}s. press Ctrl+C to stop."
            );
            scheduler::run_daemon(&db, poll_seconds)?;
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
            scheduler::run_job_once(&db, &job, chrono::Local::now().naive_local(), "manual")?;
        }
        Commands::Enable { name } => {
            db.set_enabled(&name, true)?;
            println!("Enabled job '{name}'");
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
