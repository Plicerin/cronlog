use clap::{Parser, Subcommand};
use std::time::Duration;

fn parse_duration(input: &str) -> std::result::Result<Duration, String> {
    humantime::parse_duration(input).map_err(|e| e.to_string())
}

fn parse_positive_i64(input: &str) -> std::result::Result<i64, String> {
    let value = input
        .parse::<i64>()
        .map_err(|_| format!("expected positive integer, got '{input}'"))?;
    if value <= 0 {
        return Err("value must be greater than zero".into());
    }
    Ok(value)
}

#[derive(Parser, Debug)]
#[command(author, version, about = "Cronlog: cron with durable history and logs")]
pub struct Cli {
    /// SQLite database path
    #[arg(long, global = true, default_value = "cronlog.db")]
    pub db: String,

    /// Emit machine-readable JSON for inspection commands
    #[arg(long, global = true)]
    pub json: bool,

    #[command(subcommand)]
    pub command: Commands,
}

#[derive(Subcommand, Debug)]
pub enum Commands {
    /// Add a scheduled job
    Add {
        /// Unique job name
        #[arg(short, long)]
        name: String,

        /// Schedule: "every N seconds", "every N minutes", "every N hours", or "daily at HH:MM"
        #[arg(short, long)]
        schedule: String,

        /// Job timeout, for example 30s, 5m, 1h
        #[arg(long, value_parser = parse_duration)]
        timeout: Option<Duration>,

        /// Disable the job after N scheduled runs that actually start
        #[arg(long, value_parser = parse_positive_i64)]
        max_runs: Option<i64>,

        /// Command and arguments to run. Put this after --
        #[arg(last = true, required = true)]
        command: Vec<String>,
    },

    /// List registered jobs
    List,

    /// Show scheduler status for all jobs or one job
    Status {
        /// Optional job name
        name: Option<String>,
    },

    /// Run the scheduler loop in the foreground
    Daemon {
        /// Poll interval in seconds
        #[arg(long, default_value_t = 1)]
        poll_seconds: u64,
    },

    /// Show run history for a job
    History {
        /// Job name
        name: String,

        /// Number of runs to show
        #[arg(short, long, default_value_t = 20)]
        limit: i64,
    },

    /// Show captured logs for a job run
    Logs {
        /// Job name
        name: String,

        /// Show last run logs
        #[arg(long)]
        last: bool,

        /// Specific run ID
        #[arg(long)]
        run_id: Option<i64>,
    },

    /// Run a registered job immediately
    Run {
        /// Job name
        name: String,

        /// Explicitly run now
        #[arg(long)]
        now: bool,
    },

    /// Enable a job
    Enable { name: String },

    /// Disable a job
    Disable { name: String },

    /// Remove a job and its future schedule. Run history is kept.
    Remove { name: String },
}
