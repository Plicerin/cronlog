use crate::db::{HistoryRow, JobListRow, LogRow, StatusRow};
use comfy_table::{presets::UTF8_FULL, Cell, Table};
use serde::Serialize;

pub fn print_json<T: Serialize>(value: &T) -> crate::error::Result<()> {
    println!("{}", serde_json::to_string_pretty(value)?);
    Ok(())
}

pub fn print_jobs(jobs: &[JobListRow]) {
    let mut table = Table::new();
    table.load_preset(UTF8_FULL);
    table.set_header(vec!["Name", "Enabled", "Schedule", "Next Run", "Command"]);

    for job in jobs {
        table.add_row(vec![
            Cell::new(&job.name),
            Cell::new(if job.enabled { "yes" } else { "no" }),
            Cell::new(&job.schedule),
            Cell::new(
                job.next_run_at
                    .map(|d| d.to_string())
                    .unwrap_or_else(|| "-".into()),
            ),
            Cell::new(&job.command),
        ]);
    }

    println!("{table}");
}

pub fn print_history(rows: &[HistoryRow]) {
    let mut table = Table::new();
    table.load_preset(UTF8_FULL);
    table.set_header(vec![
        "Run",
        "Status",
        "Trigger",
        "Scheduled",
        "Started",
        "Finished",
        "Duration",
        "Exit",
        "Error",
    ]);

    for row in rows {
        table.add_row(vec![
            Cell::new(row.run_id),
            Cell::new(&row.status),
            Cell::new(&row.trigger_type),
            Cell::new(
                row.scheduled_for
                    .map(|d| d.to_string())
                    .unwrap_or_else(|| "-".into()),
            ),
            Cell::new(
                row.started_at
                    .map(|d| d.to_string())
                    .unwrap_or_else(|| "-".into()),
            ),
            Cell::new(
                row.finished_at
                    .map(|d| d.to_string())
                    .unwrap_or_else(|| "-".into()),
            ),
            Cell::new(
                row.duration_ms
                    .map(|d| format!("{d}ms"))
                    .unwrap_or_else(|| "-".into()),
            ),
            Cell::new(
                row.exit_code
                    .map(|c| c.to_string())
                    .unwrap_or_else(|| "-".into()),
            ),
            Cell::new(row.error.clone().unwrap_or_else(|| "".into())),
        ]);
    }

    println!("{table}");
}

pub fn print_status(rows: &[StatusRow]) {
    let mut table = Table::new();
    table.load_preset(UTF8_FULL);
    table.set_header(vec![
        "Name",
        "Enabled",
        "Schedule",
        "Next Run",
        "Running",
        "Last Run",
        "Last Status",
        "Last Finished",
        "Last Duration",
        "Exit",
    ]);

    for row in rows {
        table.add_row(vec![
            Cell::new(&row.name),
            Cell::new(if row.enabled { "yes" } else { "no" }),
            Cell::new(&row.schedule),
            Cell::new(
                row.next_run_at
                    .map(|d| d.to_string())
                    .unwrap_or_else(|| "-".into()),
            ),
            Cell::new(row.running_runs),
            Cell::new(
                row.last_run_id
                    .map(|id| id.to_string())
                    .unwrap_or_else(|| "-".into()),
            ),
            Cell::new(row.last_status.clone().unwrap_or_else(|| "-".into())),
            Cell::new(
                row.last_finished_at
                    .map(|d| d.to_string())
                    .unwrap_or_else(|| "-".into()),
            ),
            Cell::new(
                row.last_duration_ms
                    .map(|d| format!("{d}ms"))
                    .unwrap_or_else(|| "-".into()),
            ),
            Cell::new(
                row.last_exit_code
                    .map(|c| c.to_string())
                    .unwrap_or_else(|| "-".into()),
            ),
        ]);
    }

    println!("{table}");
}

pub fn print_logs(run_id: i64, logs: &LogRow) {
    println!("Logs for run #{run_id}");
    println!(
        "\n--- STDOUT{} ---",
        if logs.stdout_truncated {
            " (truncated)"
        } else {
            ""
        }
    );
    match &logs.stdout {
        Some(s) if !s.is_empty() => println!("{s}"),
        _ => println!("<empty>"),
    }
    println!(
        "\n--- STDERR{} ---",
        if logs.stderr_truncated {
            " (truncated)"
        } else {
            ""
        }
    );
    match &logs.stderr {
        Some(s) if !s.is_empty() => println!("{s}"),
        _ => println!("<empty>"),
    }
}
