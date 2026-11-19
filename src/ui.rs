use crate::clock;
use crate::color;
use crate::db::{HistoryRow, HookRow, JobListRow, LogRow, StatusRow};
use comfy_table::{presets::UTF8_FULL, Cell, Color, Table};
use serde::Serialize;

pub fn print_json<T: Serialize>(value: &T) -> crate::error::Result<()> {
    println!("{}", serde_json::to_string_pretty(value)?);
    Ok(())
}

/// A table that is styled only when colour is on.
fn new_table(color: bool) -> Table {
    let mut table = Table::new();
    table.load_preset(UTF8_FULL);
    if color {
        table.enforce_styling();
    }
    table
}

/// Colours a cell when colour is on and there is a colour for it.
fn paint(cell: Cell, fg: Option<Color>, color: bool) -> Cell {
    match fg {
        Some(fg) if color => cell.fg(fg),
        _ => cell,
    }
}

fn enabled_cell(enabled: bool, color: bool) -> Cell {
    let text = if enabled { "yes" } else { "no" };
    paint(
        Cell::new(text),
        (!enabled).then_some(Color::DarkYellow),
        color,
    )
}

fn exit_cell(code: Option<i64>, color: bool) -> Cell {
    let text = code.map(|c| c.to_string()).unwrap_or_else(|| "-".into());
    let fg = code.filter(|c| *c != 0).map(|_| Color::DarkRed);
    paint(Cell::new(text), fg, color)
}

pub fn print_jobs(jobs: &[JobListRow], color: bool) {
    let mut table = new_table(color);
    table.set_header(vec![
        "Name", "Enabled", "Schedule", "Next Run", "Runs", "Command",
    ]);

    for job in jobs {
        table.add_row(vec![
            Cell::new(&job.name),
            enabled_cell(job.enabled, color),
            Cell::new(match job.late_limit_seconds {
                Some(limit) => format!(
                    "{} (late limit {})",
                    job.schedule,
                    clock::format_seconds(limit)
                ),
                None => job.schedule.clone(),
            }),
            Cell::new(
                job.next_run_at
                    .map(clock::display)
                    .unwrap_or_else(|| "-".into()),
            ),
            Cell::new(format_runs(job.scheduled_runs, job.max_runs)),
            Cell::new(&job.command),
        ]);
    }

    println!("{table}");
}

fn format_runs(scheduled_runs: i64, max_runs: Option<i64>) -> String {
    match max_runs {
        Some(max) => format!("{scheduled_runs}/{max}"),
        None => scheduled_runs.to_string(),
    }
}

pub fn print_history(rows: &[HistoryRow], color: bool) {
    let mut table = new_table(color);
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
            paint(
                Cell::new(if row.skip_count > 1 {
                    format!("{} x{}", row.status, row.skip_count)
                } else {
                    row.status.clone()
                }),
                color::for_status(&row.status),
                color,
            ),
            Cell::new(&row.trigger_type),
            Cell::new(
                row.scheduled_for
                    .map(clock::display)
                    .unwrap_or_else(|| "-".into()),
            ),
            Cell::new(
                row.started_at
                    .map(clock::display)
                    .unwrap_or_else(|| "-".into()),
            ),
            Cell::new(
                row.finished_at
                    .map(clock::display)
                    .unwrap_or_else(|| "-".into()),
            ),
            Cell::new(
                row.duration_ms
                    .map(|d| format!("{d}ms"))
                    .unwrap_or_else(|| "-".into()),
            ),
            exit_cell(row.exit_code, color),
            paint(
                Cell::new(row.error.clone().unwrap_or_default()),
                Some(Color::DarkRed),
                color,
            ),
        ]);
    }

    println!("{table}");
}

pub fn print_status(rows: &[StatusRow], color: bool) {
    let mut table = new_table(color);
    table.set_header(vec![
        "Name",
        "Enabled",
        "Schedule",
        "Next Run",
        "Running",
        "Runs",
        "Last Run",
        "Last Status",
        "Last Finished",
        "Last Duration",
        "Exit",
    ]);

    for row in rows {
        table.add_row(vec![
            Cell::new(&row.name),
            enabled_cell(row.enabled, color),
            Cell::new(&row.schedule),
            Cell::new(
                row.next_run_at
                    .map(clock::display)
                    .unwrap_or_else(|| "-".into()),
            ),
            paint(
                Cell::new(row.running_runs),
                (row.running_runs > 0).then_some(Color::DarkCyan),
                color,
            ),
            Cell::new(format_runs(row.scheduled_runs, row.max_runs)),
            Cell::new(
                row.last_run_id
                    .map(|id| id.to_string())
                    .unwrap_or_else(|| "-".into()),
            ),
            paint(
                Cell::new(row.last_status.as_deref().unwrap_or("-")),
                row.last_status.as_deref().and_then(color::for_status),
                color,
            ),
            Cell::new(
                row.last_finished_at
                    .map(clock::display)
                    .unwrap_or_else(|| "-".into()),
            ),
            Cell::new(
                row.last_duration_ms
                    .map(|d| format!("{d}ms"))
                    .unwrap_or_else(|| "-".into()),
            ),
            exit_cell(row.last_exit_code, color),
        ]);
    }

    println!("{table}");
}

pub fn print_hook_history(rows: &[HookRow], color: bool) {
    let mut table = new_table(color);
    table.set_header(vec![
        "Hook", "Event", "Job", "Run", "Status", "Started", "Duration", "Exit", "Error",
    ]);
    let time =
        |t: Option<chrono::NaiveDateTime>| t.map(clock::display).unwrap_or_else(|| "-".into());
    for row in rows {
        table.add_row(vec![
            Cell::new(row.hook_id),
            Cell::new(&row.event),
            Cell::new(&row.job),
            Cell::new(row.run_id),
            paint(
                Cell::new(&row.status),
                color::for_status(&row.status),
                color,
            ),
            Cell::new(time(row.started_at)),
            Cell::new(
                row.duration_ms
                    .map(|d| format!("{d}ms"))
                    .unwrap_or_else(|| "-".into()),
            ),
            exit_cell(row.exit_code, color),
            paint(
                Cell::new(row.error.clone().unwrap_or_default()),
                Some(Color::DarkRed),
                color,
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
