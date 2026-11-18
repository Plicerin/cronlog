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

/// How long run history is kept: a duration such as 30d or 12h, or `forever`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Retain {
    Forever,
    For(Duration),
}

fn parse_retain(input: &str) -> std::result::Result<Retain, String> {
    let trimmed = input.trim();
    // Only the keywords ignore case: humantime reads `M` as months and `m` as minutes.
    match trimmed.to_lowercase().as_str() {
        "forever" | "off" | "never" => Ok(Retain::Forever),
        _ => {
            let duration = humantime::parse_duration(trimmed).map_err(|e| e.to_string())?;
            if duration.is_zero() {
                return Err("retention must be longer than zero, or `forever`".into());
            }
            Ok(Retain::For(duration))
        }
    }
}

fn parse_age(input: &str) -> std::result::Result<Duration, String> {
    match parse_retain(input)? {
        Retain::For(duration) => Ok(duration),
        Retain::Forever => Err("expected a duration such as 30d".into()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn retain_takes_a_duration_or_forever() {
        assert_eq!(
            parse_retain("30d"),
            Ok(Retain::For(Duration::from_secs(30 * 86_400)))
        );
        assert_eq!(parse_retain("Forever"), Ok(Retain::Forever));
        // Months, not minutes.
        assert_eq!(
            parse_retain("1M"),
            Ok(Retain::For(humantime::parse_duration("1month").unwrap()))
        );
        assert_eq!(parse_retain("1m"), Ok(Retain::For(Duration::from_secs(60))));
        assert!(parse_retain("0s").is_err());
        assert!(parse_retain("soon").is_err());
        assert!(parse_age("forever").is_err());
    }
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

    /// Check that the daemon is alive and every job is on time and succeeding. Exits 1
    /// and lists the problems if not; with --json, prints the report either way
    Health {
        /// Check only this job (and the daemon); a disabled job named here is a problem
        #[arg(long)]
        job: Option<String>,

        /// A job whose run is this late and not started is overdue
        #[arg(long, default_value = "5m", value_parser = parse_duration)]
        grace: Duration,

        /// A job whose last N finished runs all failed, timed out or were interrupted is
        /// failing (skipped runs are not counted)
        #[arg(long, default_value_t = 3, value_parser = parse_positive_i64)]
        failures: i64,

        /// The daemon is down when not seen for this long (it checks in every 30s), plus
        /// its poll interval
        #[arg(long, default_value = "2m", value_parser = parse_duration)]
        daemon_stale: Duration,
    },

    /// Run the scheduler loop in the foreground
    Daemon {
        /// Poll interval in seconds
        #[arg(long, default_value_t = 1)]
        poll_seconds: u64,

        /// Delete runs scheduled longer ago than this, checked daily: 30d, 12h, 1M, or
        /// `forever` to keep everything
        #[arg(long, default_value = "30d", value_parser = parse_retain)]
        retain: Retain,

        /// Always keep each job's newest N runs, however old
        #[arg(long, default_value_t = 20, value_parser = parse_positive_i64)]
        keep_last: i64,
    },

    /// Delete old run history now (the daemon also does this daily)
    Prune {
        /// Delete runs scheduled longer ago than this, for example 30d, 12h or 1M (month)
        #[arg(long, default_value = "30d", value_parser = parse_age)]
        older_than: Duration,

        /// Always keep each job's newest N runs, however old
        #[arg(long, default_value_t = 20, value_parser = parse_positive_i64)]
        keep_last: i64,

        /// Only count what would be deleted
        #[arg(long)]
        dry_run: bool,

        /// Afterwards, rewrite the file to its smallest size and switch it to incremental
        /// auto-vacuum. Locks the database while it runs; stop the daemon first for a
        /// large file
        #[arg(long)]
        vacuum: bool,
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
