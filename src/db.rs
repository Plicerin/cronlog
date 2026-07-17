use crate::error::{CronlogError, Result};
use chrono::NaiveDateTime;
use rusqlite::{params, Connection, OptionalExtension, TransactionBehavior};
use serde::Serialize;
use std::path::Path;

#[derive(Debug, Clone, Serialize)]
pub struct Job {
    pub id: i64,
    pub name: String,
    pub command: Vec<String>,
    pub schedule: String,
    pub timeout_seconds: i64,
    pub max_runs: Option<i64>,
    pub next_run_at: Option<NaiveDateTime>,
}

#[derive(Debug, Serialize)]
pub struct JobListRow {
    pub name: String,
    pub command: String,
    pub schedule: String,
    pub enabled: bool,
    pub next_run_at: Option<NaiveDateTime>,
    pub max_runs: Option<i64>,
    pub scheduled_runs: i64,
    pub remaining_runs: Option<i64>,
}

#[derive(Debug, Serialize)]
pub struct HistoryRow {
    pub run_id: i64,
    pub status: String,
    pub trigger_type: String,
    pub scheduled_for: Option<NaiveDateTime>,
    pub started_at: Option<NaiveDateTime>,
    pub finished_at: Option<NaiveDateTime>,
    pub duration_ms: Option<i64>,
    pub exit_code: Option<i64>,
    pub error: Option<String>,
}

#[derive(Debug)]
pub struct InterruptedRun {
    pub run_id: i64,
    pub job_name: String,
}

#[derive(Debug, PartialEq, Eq)]
pub enum RunClaim {
    Claimed(i64),
    Skipped(i64),
    NotDue,
}

#[derive(Debug, Serialize)]
pub struct LogRow {
    pub stdout: Option<String>,
    pub stderr: Option<String>,
    pub stdout_truncated: bool,
    pub stderr_truncated: bool,
}

#[derive(Debug, Serialize)]
pub struct StatusRow {
    pub name: String,
    pub enabled: bool,
    pub schedule: String,
    pub next_run_at: Option<NaiveDateTime>,
    pub max_runs: Option<i64>,
    pub scheduled_runs: i64,
    pub remaining_runs: Option<i64>,
    pub running_runs: i64,
    pub last_run_id: Option<i64>,
    pub last_status: Option<String>,
    pub last_trigger_type: Option<String>,
    pub last_scheduled_for: Option<NaiveDateTime>,
    pub last_started_at: Option<NaiveDateTime>,
    pub last_finished_at: Option<NaiveDateTime>,
    pub last_duration_ms: Option<i64>,
    pub last_exit_code: Option<i64>,
    pub last_error: Option<String>,
}

pub struct Database {
    path: String,
}

impl Database {
    pub fn open(path: impl AsRef<Path>) -> Result<Self> {
        Ok(Self {
            path: path.as_ref().to_string_lossy().to_string(),
        })
    }

    fn conn(&self) -> Result<Connection> {
        let conn = Connection::open(&self.path)?;
        conn.pragma_update(None, "foreign_keys", "ON")?;
        Ok(conn)
    }

    pub fn init(&self) -> Result<()> {
        let conn = self.conn()?;
        conn.execute_batch(
            r#"
            CREATE TABLE IF NOT EXISTS jobs (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT NOT NULL UNIQUE,
                command TEXT NOT NULL,
                schedule TEXT NOT NULL,
                enabled INTEGER NOT NULL DEFAULT 1,
                timeout_seconds INTEGER NOT NULL DEFAULT 3600,
                max_runs INTEGER,
                next_run_at TEXT,
                deleted_at TEXT,
                created_at TEXT DEFAULT CURRENT_TIMESTAMP,
                updated_at TEXT DEFAULT CURRENT_TIMESTAMP
            );

            CREATE TABLE IF NOT EXISTS runs (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                job_id INTEGER NOT NULL,
                status TEXT NOT NULL,
                scheduled_for TEXT,
                started_at TEXT,
                finished_at TEXT,
                duration_ms INTEGER,
                exit_code INTEGER,
                trigger_type TEXT NOT NULL,
                error TEXT,
                FOREIGN KEY(job_id) REFERENCES jobs(id)
            );

            CREATE TABLE IF NOT EXISTS logs (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                run_id INTEGER NOT NULL,
                stdout TEXT,
                stderr TEXT,
                stdout_truncated INTEGER NOT NULL DEFAULT 0,
                stderr_truncated INTEGER NOT NULL DEFAULT 0,
                FOREIGN KEY(run_id) REFERENCES runs(id)
            );

            CREATE INDEX IF NOT EXISTS idx_jobs_due ON jobs(enabled, next_run_at, deleted_at);
            CREATE INDEX IF NOT EXISTS idx_runs_job_started ON runs(job_id, started_at);
            "#,
        )?;
        if !jobs_has_column(&conn, "max_runs")? {
            conn.execute("ALTER TABLE jobs ADD COLUMN max_runs INTEGER", [])?;
        }
        Ok(())
    }

    pub fn add_job(
        &self,
        name: &str,
        command: &[String],
        schedule: &str,
        timeout_seconds: i64,
        max_runs: Option<i64>,
        next_run_at: NaiveDateTime,
    ) -> Result<()> {
        let conn = self.conn()?;
        let command_joined = shell_words::join(command.iter().map(String::as_str));
        conn.execute(
            "INSERT INTO jobs (name, command, schedule, timeout_seconds, max_runs, next_run_at) VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
            params![name, command_joined, schedule, timeout_seconds, max_runs, next_run_at.to_string()],
        )?;
        Ok(())
    }

    pub fn list_jobs(&self) -> Result<Vec<JobListRow>> {
        let conn = self.conn()?;
        let mut stmt = conn.prepare(
            "SELECT j.name, j.command, j.schedule, j.enabled, j.next_run_at, j.max_runs,
                    (SELECT COUNT(*) FROM runs rr WHERE rr.job_id = j.id AND rr.trigger_type = 'schedule' AND rr.status NOT IN ('running', 'skipped')) AS scheduled_runs
             FROM jobs j
             WHERE j.deleted_at IS NULL
             ORDER BY j.name"
        )?;
        let rows = stmt.query_map([], |row| {
            let next: Option<String> = row.get(4)?;
            Ok(JobListRow {
                name: row.get(0)?,
                command: row.get(1)?,
                schedule: row.get(2)?,
                enabled: row.get::<_, i64>(3)? == 1,
                next_run_at: parse_dt_opt(next),
                max_runs: row.get(5)?,
                scheduled_runs: row.get(6)?,
                remaining_runs: remaining_runs(row.get(5)?, row.get(6)?),
            })
        })?;
        rows.collect::<std::result::Result<Vec<_>, _>>()
            .map_err(Into::into)
    }

    pub fn status(&self, name: Option<&str>) -> Result<Vec<StatusRow>> {
        let conn = self.conn()?;
        let (sql, bind_name) = if name.is_some() {
            (
                "SELECT j.name, j.enabled, j.schedule, j.next_run_at, j.max_runs,
                        (SELECT COUNT(*) FROM runs rr WHERE rr.job_id = j.id AND rr.trigger_type = 'schedule' AND rr.status NOT IN ('running', 'skipped')) AS scheduled_runs,
                        (SELECT COUNT(*) FROM runs rr WHERE rr.job_id = j.id AND rr.status = 'running') AS running_runs,
                        r.id, r.status, r.trigger_type, r.scheduled_for, r.started_at, r.finished_at, r.duration_ms, r.exit_code, r.error
                 FROM jobs j
                 LEFT JOIN runs r ON r.id = (SELECT MAX(id) FROM runs WHERE job_id = j.id)
                 WHERE j.deleted_at IS NULL AND j.name = ?1
                 ORDER BY j.name",
                true,
            )
        } else {
            (
                "SELECT j.name, j.enabled, j.schedule, j.next_run_at, j.max_runs,
                        (SELECT COUNT(*) FROM runs rr WHERE rr.job_id = j.id AND rr.trigger_type = 'schedule' AND rr.status NOT IN ('running', 'skipped')) AS scheduled_runs,
                        (SELECT COUNT(*) FROM runs rr WHERE rr.job_id = j.id AND rr.status = 'running') AS running_runs,
                        r.id, r.status, r.trigger_type, r.scheduled_for, r.started_at, r.finished_at, r.duration_ms, r.exit_code, r.error
                 FROM jobs j
                 LEFT JOIN runs r ON r.id = (SELECT MAX(id) FROM runs WHERE job_id = j.id)
                 WHERE j.deleted_at IS NULL
                 ORDER BY j.name",
                false,
            )
        };

        let mut stmt = conn.prepare(sql)?;
        let mapper = |row: &rusqlite::Row<'_>| {
            Ok(StatusRow {
                name: row.get(0)?,
                enabled: row.get::<_, i64>(1)? == 1,
                schedule: row.get(2)?,
                next_run_at: parse_dt_opt(row.get::<_, Option<String>>(3)?),
                max_runs: row.get(4)?,
                scheduled_runs: row.get(5)?,
                remaining_runs: remaining_runs(row.get(4)?, row.get(5)?),
                running_runs: row.get(6)?,
                last_run_id: row.get(7)?,
                last_status: row.get(8)?,
                last_trigger_type: row.get(9)?,
                last_scheduled_for: parse_dt_opt(row.get::<_, Option<String>>(10)?),
                last_started_at: parse_dt_opt(row.get::<_, Option<String>>(11)?),
                last_finished_at: parse_dt_opt(row.get::<_, Option<String>>(12)?),
                last_duration_ms: row.get(13)?,
                last_exit_code: row.get(14)?,
                last_error: row.get(15)?,
            })
        };

        let rows = if bind_name {
            stmt.query_map(params![name], mapper)?
                .collect::<std::result::Result<Vec<_>, _>>()?
        } else {
            stmt.query_map([], mapper)?
                .collect::<std::result::Result<Vec<_>, _>>()?
        };

        if name.is_some() && rows.is_empty() {
            return Err(CronlogError::NotFound(format!(
                "job '{}'",
                name.unwrap_or_default()
            )));
        }

        Ok(rows)
    }

    pub fn get_due_jobs(&self, now: NaiveDateTime) -> Result<Vec<Job>> {
        let conn = self.conn()?;
        let mut stmt = conn.prepare(
            "SELECT id, name, command, schedule, enabled, timeout_seconds, max_runs, next_run_at
             FROM jobs
             WHERE enabled = 1 AND deleted_at IS NULL AND next_run_at IS NOT NULL AND next_run_at <= ?1
             ORDER BY next_run_at ASC"
        )?;
        let rows = stmt.query_map(params![now.to_string()], row_to_job)?;
        rows.collect::<std::result::Result<Vec<_>, _>>()
            .map_err(Into::into)
    }

    pub fn get_job_by_name(&self, name: &str) -> Result<Job> {
        let conn = self.conn()?;
        conn.query_row(
            "SELECT id, name, command, schedule, enabled, timeout_seconds, max_runs, next_run_at FROM jobs WHERE name = ?1 AND deleted_at IS NULL",
            params![name],
            row_to_job,
        ).optional()?.ok_or_else(|| CronlogError::NotFound(format!("job '{name}'")))
    }

    pub fn claim_scheduled_run(
        &self,
        job_id: i64,
        scheduled_for: NaiveDateTime,
        next_run_at: NaiveDateTime,
        started_at: NaiveDateTime,
    ) -> Result<RunClaim> {
        let mut conn = self.conn()?;
        let tx = conn.transaction_with_behavior(TransactionBehavior::Immediate)?;

        let job_state = tx
            .query_row(
                "SELECT j.next_run_at, j.max_runs,
                        (SELECT COUNT(*) FROM runs rr WHERE rr.job_id = j.id AND rr.trigger_type = 'schedule' AND rr.status NOT IN ('running', 'skipped')) AS scheduled_runs
                 FROM jobs j
                 WHERE j.id = ?1 AND j.enabled = 1 AND j.deleted_at IS NULL",
                params![job_id],
                |row| {
                    Ok((
                        row.get::<_, Option<String>>(0)?,
                        row.get::<_, Option<i64>>(1)?,
                        row.get::<_, i64>(2)?,
                    ))
                },
            )
            .optional()?;

        let Some((current_next, max_runs, scheduled_runs)) = job_state else {
            tx.commit()?;
            return Ok(RunClaim::NotDue);
        };

        if max_runs.is_some_and(|max| scheduled_runs >= max) {
            tx.execute(
                "UPDATE jobs SET enabled = 0, next_run_at = NULL, updated_at = CURRENT_TIMESTAMP WHERE id = ?1",
                params![job_id],
            )?;
            tx.commit()?;
            return Ok(RunClaim::NotDue);
        }

        if parse_dt_opt(current_next) != Some(scheduled_for) {
            tx.commit()?;
            return Ok(RunClaim::NotDue);
        }

        let running_count: i64 = tx.query_row(
            "SELECT COUNT(*) FROM runs WHERE job_id = ?1 AND status = 'running'",
            params![job_id],
            |row| row.get(0),
        )?;

        let claim = if running_count > 0 {
            tx.execute(
                "INSERT INTO runs (job_id, status, scheduled_for, finished_at, duration_ms, trigger_type, error)
                 VALUES (?1, 'skipped', ?2, ?3, 0, 'schedule', 'previous run still active')",
                params![job_id, scheduled_for.to_string(), started_at.to_string()],
            )?;
            RunClaim::Skipped(tx.last_insert_rowid())
        } else {
            tx.execute(
                "INSERT INTO runs (job_id, status, scheduled_for, started_at, trigger_type)
                 VALUES (?1, 'running', ?2, ?3, 'schedule')",
                params![job_id, scheduled_for.to_string(), started_at.to_string()],
            )?;
            RunClaim::Claimed(tx.last_insert_rowid())
        };

        tx.execute(
            "UPDATE jobs SET next_run_at = ?1, updated_at = CURRENT_TIMESTAMP WHERE id = ?2",
            params![next_run_at.to_string(), job_id],
        )?;

        tx.commit()?;
        Ok(claim)
    }

    pub fn create_run(
        &self,
        job_id: i64,
        status: &str,
        scheduled_for: NaiveDateTime,
        started_at: Option<NaiveDateTime>,
        trigger_type: &str,
    ) -> Result<i64> {
        let conn = self.conn()?;
        conn.execute(
            "INSERT INTO runs (job_id, status, scheduled_for, started_at, trigger_type) VALUES (?1, ?2, ?3, ?4, ?5)",
            params![job_id, status, scheduled_for.to_string(), started_at.map(|d| d.to_string()), trigger_type],
        )?;
        Ok(conn.last_insert_rowid())
    }

    pub fn finish_run(
        &self,
        run_id: i64,
        status: &str,
        finished_at: NaiveDateTime,
        duration_ms: i64,
        exit_code: Option<i64>,
        error: Option<&str>,
    ) -> Result<()> {
        let conn = self.conn()?;
        conn.execute(
            "UPDATE runs SET status = ?1, finished_at = ?2, duration_ms = ?3, exit_code = ?4, error = ?5 WHERE id = ?6",
            params![status, finished_at.to_string(), duration_ms, exit_code, error, run_id],
        )?;
        Ok(())
    }

    pub fn interrupt_stale_running_runs(
        &self,
        finished_at: NaiveDateTime,
        message: &str,
    ) -> Result<Vec<InterruptedRun>> {
        let mut conn = self.conn()?;
        let tx = conn.transaction()?;
        let stale_runs = {
            let mut stmt = tx.prepare(
                "SELECT r.id, j.name, r.started_at
                 FROM runs r JOIN jobs j ON j.id = r.job_id
                 WHERE r.status = 'running'
                 ORDER BY r.id",
            )?;
            let rows = stmt.query_map([], |row| {
                let started_at = parse_dt_opt(row.get::<_, Option<String>>(2)?);
                Ok((row.get::<_, i64>(0)?, row.get::<_, String>(1)?, started_at))
            })?;
            rows.collect::<std::result::Result<Vec<_>, _>>()?
        };

        let mut interrupted = Vec::with_capacity(stale_runs.len());
        for (run_id, job_name, started_at) in stale_runs {
            let duration_ms = started_at
                .map(|started| {
                    finished_at
                        .signed_duration_since(started)
                        .num_milliseconds()
                })
                .unwrap_or(0)
                .max(0);
            tx.execute(
                "UPDATE runs SET status = 'interrupted', finished_at = ?1, duration_ms = ?2, error = ?3 WHERE id = ?4",
                params![finished_at.to_string(), duration_ms, message, run_id],
            )?;
            tx.execute(
                "INSERT INTO logs (run_id, stdout, stderr, stdout_truncated, stderr_truncated) VALUES (?1, '', ?2, 0, 0)",
                params![run_id, message],
            )?;
            interrupted.push(InterruptedRun { run_id, job_name });
        }

        tx.commit()?;
        Ok(interrupted)
    }

    pub fn disable_if_max_runs_reached(&self, job_id: i64) -> Result<Option<(String, i64)>> {
        let conn = self.conn()?;
        let state = conn
            .query_row(
                "SELECT j.name, j.enabled, j.max_runs,
                        (SELECT COUNT(*) FROM runs rr WHERE rr.job_id = j.id AND rr.trigger_type = 'schedule' AND rr.status NOT IN ('running', 'skipped')) AS scheduled_runs
                 FROM jobs j
                 WHERE j.id = ?1 AND j.deleted_at IS NULL",
                params![job_id],
                |row| {
                    Ok((
                        row.get::<_, String>(0)?,
                        row.get::<_, i64>(1)? == 1,
                        row.get::<_, Option<i64>>(2)?,
                        row.get::<_, i64>(3)?,
                    ))
                },
            )
            .optional()?;

        let Some((name, enabled, Some(max_runs), scheduled_runs)) = state else {
            return Ok(None);
        };
        if !enabled || scheduled_runs < max_runs {
            return Ok(None);
        }

        conn.execute(
            "UPDATE jobs SET enabled = 0, next_run_at = NULL, updated_at = CURRENT_TIMESTAMP WHERE id = ?1",
            params![job_id],
        )?;
        Ok(Some((name, max_runs)))
    }

    pub fn insert_logs(
        &self,
        run_id: i64,
        stdout: &str,
        stderr: &str,
        stdout_truncated: bool,
        stderr_truncated: bool,
    ) -> Result<()> {
        let conn = self.conn()?;
        conn.execute(
            "INSERT INTO logs (run_id, stdout, stderr, stdout_truncated, stderr_truncated) VALUES (?1, ?2, ?3, ?4, ?5)",
            params![run_id, stdout, stderr, stdout_truncated as i64, stderr_truncated as i64],
        )?;
        Ok(())
    }

    pub fn set_enabled(&self, name: &str, enabled: bool) -> Result<()> {
        let conn = self.conn()?;
        let affected = conn.execute(
            "UPDATE jobs SET enabled = ?1, updated_at = CURRENT_TIMESTAMP WHERE name = ?2 AND deleted_at IS NULL",
            params![enabled as i64, name],
        )?;
        if affected == 0 {
            return Err(CronlogError::NotFound(format!("job '{name}'")));
        }
        Ok(())
    }

    pub fn remove_job(&self, name: &str) -> Result<()> {
        let conn = self.conn()?;
        let affected = conn.execute(
            "UPDATE jobs SET deleted_at = CURRENT_TIMESTAMP, enabled = 0, updated_at = CURRENT_TIMESTAMP WHERE name = ?1 AND deleted_at IS NULL",
            params![name],
        )?;
        if affected == 0 {
            return Err(CronlogError::NotFound(format!("job '{name}'")));
        }
        Ok(())
    }

    pub fn history(&self, name: &str, limit: i64) -> Result<Vec<HistoryRow>> {
        let conn = self.conn()?;
        let mut stmt = conn.prepare(
            "SELECT r.id, r.status, r.trigger_type, r.scheduled_for, r.started_at, r.finished_at, r.duration_ms, r.exit_code, r.error
             FROM runs r JOIN jobs j ON j.id = r.job_id
             WHERE j.name = ?1
             ORDER BY r.id DESC LIMIT ?2"
        )?;
        let rows = stmt.query_map(params![name, limit], |row| {
            Ok(HistoryRow {
                run_id: row.get(0)?,
                status: row.get(1)?,
                trigger_type: row.get(2)?,
                scheduled_for: parse_dt_opt(row.get::<_, Option<String>>(3)?),
                started_at: parse_dt_opt(row.get::<_, Option<String>>(4)?),
                finished_at: parse_dt_opt(row.get::<_, Option<String>>(5)?),
                duration_ms: row.get(6)?,
                exit_code: row.get(7)?,
                error: row.get(8)?,
            })
        })?;
        rows.collect::<std::result::Result<Vec<_>, _>>()
            .map_err(Into::into)
    }

    pub fn last_run_id(&self, name: &str) -> Result<Option<i64>> {
        let conn = self.conn()?;
        conn.query_row(
            "SELECT r.id FROM runs r JOIN jobs j ON j.id = r.job_id WHERE j.name = ?1 ORDER BY r.id DESC LIMIT 1",
            params![name],
            |row| row.get(0),
        ).optional().map_err(Into::into)
    }

    pub fn logs_for_run(&self, run_id: i64) -> Result<LogRow> {
        let conn = self.conn()?;
        let logs = conn.query_row(
            "SELECT stdout, stderr, stdout_truncated, stderr_truncated FROM logs WHERE run_id = ?1 ORDER BY id DESC LIMIT 1",
            params![run_id],
            |row| Ok(LogRow {
                stdout: row.get(0)?,
                stderr: row.get(1)?,
                stdout_truncated: row.get::<_, i64>(2)? == 1,
                stderr_truncated: row.get::<_, i64>(3)? == 1,
            }),
        ).optional()?;

        if let Some(logs) = logs {
            return Ok(logs);
        }

        let run_exists = conn
            .query_row("SELECT 1 FROM runs WHERE id = ?1", params![run_id], |_| {
                Ok(())
            })
            .optional()?
            .is_some();

        if !run_exists {
            return Err(CronlogError::NotFound(format!("run {run_id}")));
        }

        Ok(LogRow {
            stdout: None,
            stderr: None,
            stdout_truncated: false,
            stderr_truncated: false,
        })
    }
}

fn row_to_job(row: &rusqlite::Row<'_>) -> rusqlite::Result<Job> {
    let command_str: String = row.get(2)?;
    let command = shell_words::split(&command_str).unwrap_or_else(|_| vec![command_str]);
    let next: Option<String> = row.get(7)?;
    Ok(Job {
        id: row.get(0)?,
        name: row.get(1)?,
        command,
        schedule: row.get(3)?,
        timeout_seconds: row.get(5)?,
        max_runs: row.get(6)?,
        next_run_at: parse_dt_opt(next),
    })
}

fn remaining_runs(max_runs: Option<i64>, scheduled_runs: i64) -> Option<i64> {
    max_runs.map(|max| (max - scheduled_runs).max(0))
}

fn jobs_has_column(conn: &Connection, column: &str) -> Result<bool> {
    let mut stmt = conn.prepare("PRAGMA table_info(jobs)")?;
    let columns = stmt.query_map([], |row| row.get::<_, String>(1))?;
    for existing in columns {
        if existing? == column {
            return Ok(true);
        }
    }
    Ok(false)
}

fn parse_dt_opt(input: Option<String>) -> Option<NaiveDateTime> {
    input.and_then(|s| {
        NaiveDateTime::parse_from_str(&s, "%Y-%m-%d %H:%M:%S%.f")
            .ok()
            .or_else(|| NaiveDateTime::parse_from_str(&s, "%Y-%m-%d %H:%M:%S").ok())
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::NaiveDate;
    use std::fs;
    use std::time::{SystemTime, UNIX_EPOCH};

    fn test_db() -> Database {
        let unique = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("system time should be after epoch")
            .as_nanos();
        let path = std::env::temp_dir().join(format!("Cronlog-test-{unique}.db"));
        let db = Database::open(&path).expect("open test db");
        db.init().expect("init test db");
        db
    }

    fn fixed_time() -> NaiveDateTime {
        NaiveDate::from_ymd_opt(2026, 6, 11)
            .expect("valid date")
            .and_hms_opt(12, 0, 0)
            .expect("valid time")
    }

    #[test]
    fn logs_for_run_returns_empty_logs_when_run_has_no_logs_row() {
        let db = test_db();
        let when = fixed_time();
        db.add_job(
            "heartbeat",
            &["echo".into(), "alive".into()],
            "every 10 seconds",
            3600,
            None,
            when,
        )
        .expect("add job");
        let job = db.get_job_by_name("heartbeat").expect("get job");
        let run_id = db
            .create_run(job.id, "skipped", when, None, "schedule")
            .expect("create run");

        let logs = db
            .logs_for_run(run_id)
            .expect("missing logs row should be empty logs");

        assert!(logs.stdout.is_none());
        assert!(logs.stderr.is_none());
        assert!(!logs.stdout_truncated);
        assert!(!logs.stderr_truncated);

        let _ = fs::remove_file(db.path);
    }

    #[test]
    fn logs_for_run_still_errors_for_unknown_run() {
        let db = test_db();

        let err = db
            .logs_for_run(999_999)
            .expect_err("unknown run should error");

        assert!(matches!(err, CronlogError::NotFound(message) if message == "run 999999"));

        let _ = fs::remove_file(db.path);
    }

    #[test]
    fn interrupt_stale_running_runs_marks_running_runs_and_writes_logs() {
        let db = test_db();
        let when = fixed_time();
        let finished_at = when + chrono::Duration::seconds(5);
        db.add_job(
            "heartbeat",
            &["echo".into(), "alive".into()],
            "every 10 seconds",
            3600,
            None,
            when,
        )
        .expect("add job");
        let job = db.get_job_by_name("heartbeat").expect("get job");
        let run_id = db
            .create_run(job.id, "running", when, Some(when), "schedule")
            .expect("create running run");

        let interrupted = db
            .interrupt_stale_running_runs(finished_at, "daemon restarted while run was active")
            .expect("interrupt stale runs");

        assert_eq!(interrupted.len(), 1);
        assert_eq!(interrupted[0].run_id, run_id);
        assert_eq!(interrupted[0].job_name, "heartbeat");

        let history = db.history("heartbeat", 1).expect("history");
        assert_eq!(history[0].status, "interrupted");
        assert_eq!(history[0].duration_ms, Some(5000));
        assert_eq!(
            history[0].error.as_deref(),
            Some("daemon restarted while run was active")
        );

        let logs = db.logs_for_run(run_id).expect("logs");
        assert_eq!(
            logs.stderr.as_deref(),
            Some("daemon restarted while run was active")
        );

        let _ = fs::remove_file(db.path);
    }

    #[test]
    fn status_includes_latest_run_and_running_count() {
        let db = test_db();
        let when = fixed_time();
        db.add_job(
            "heartbeat",
            &["echo".into(), "alive".into()],
            "every 10 seconds",
            3600,
            None,
            when,
        )
        .expect("add job");
        let job = db.get_job_by_name("heartbeat").expect("get job");
        db.create_run(job.id, "running", when, Some(when), "manual")
            .expect("create running run");
        let latest = db
            .create_run(job.id, "success", when, Some(when), "schedule")
            .expect("create latest run");
        db.finish_run(latest, "success", when, 123, Some(0), None)
            .expect("finish latest run");

        let status = db.status(Some("heartbeat")).expect("status");

        assert_eq!(status.len(), 1);
        assert_eq!(status[0].name, "heartbeat");
        assert_eq!(status[0].running_runs, 1);
        assert_eq!(status[0].last_run_id, Some(latest));
        assert_eq!(status[0].last_status.as_deref(), Some("success"));
        assert_eq!(status[0].last_exit_code, Some(0));

        let _ = fs::remove_file(db.path);
    }

    #[test]
    fn claim_scheduled_run_claims_once_and_updates_next_run() {
        let db = test_db();
        let when = fixed_time();
        let next = when + chrono::Duration::seconds(10);
        db.add_job(
            "heartbeat",
            &["echo".into(), "alive".into()],
            "every 10 seconds",
            3600,
            None,
            when,
        )
        .expect("add job");
        let job = db.get_job_by_name("heartbeat").expect("get job");

        let claim = db
            .claim_scheduled_run(job.id, when, next, when)
            .expect("claim run");

        assert!(matches!(claim, RunClaim::Claimed(_)));
        assert_eq!(
            db.get_job_by_name("heartbeat")
                .expect("get updated job")
                .next_run_at,
            Some(next)
        );
        assert_eq!(
            db.claim_scheduled_run(job.id, when, next, when)
                .expect("stale claim"),
            RunClaim::NotDue
        );

        let _ = fs::remove_file(db.path);
    }

    #[test]
    fn claim_scheduled_run_skips_when_previous_run_is_active() {
        let db = test_db();
        let when = fixed_time();
        let next = when + chrono::Duration::seconds(10);
        db.add_job(
            "heartbeat",
            &["echo".into(), "alive".into()],
            "every 10 seconds",
            3600,
            None,
            when,
        )
        .expect("add job");
        let job = db.get_job_by_name("heartbeat").expect("get job");
        db.create_run(job.id, "running", when, Some(when), "manual")
            .expect("create active run");

        let claim = db
            .claim_scheduled_run(job.id, when, next, when)
            .expect("claim skipped run");

        let RunClaim::Skipped(run_id) = claim else {
            panic!("expected skipped claim");
        };
        let history = db.history("heartbeat", 2).expect("history");
        let skipped = history
            .iter()
            .find(|row| row.run_id == run_id)
            .expect("skipped run in history");
        assert_eq!(skipped.status, "skipped");
        assert_eq!(skipped.error.as_deref(), Some("previous run still active"));

        let _ = fs::remove_file(db.path);
    }
    #[test]
    fn max_runs_report_count_and_remaining_runs() {
        let db = test_db();
        let when = fixed_time();
        db.add_job(
            "bounded",
            &["echo".into(), "alive".into()],
            "every 10 seconds",
            3600,
            Some(2),
            when,
        )
        .expect("add job");
        let job = db.get_job_by_name("bounded").expect("get job");

        let manual = db
            .create_run(job.id, "success", when, Some(when), "manual")
            .expect("create manual run");
        db.finish_run(manual, "success", when, 10, Some(0), None)
            .expect("finish manual run");
        db.create_run(job.id, "skipped", when, None, "schedule")
            .expect("create skipped run");
        let scheduled = db
            .create_run(job.id, "running", when, Some(when), "schedule")
            .expect("create scheduled run");
        db.finish_run(scheduled, "success", when, 10, Some(0), None)
            .expect("finish scheduled run");

        let jobs = db.list_jobs().expect("list jobs");
        assert_eq!(jobs[0].max_runs, Some(2));
        assert_eq!(jobs[0].scheduled_runs, 1);
        assert_eq!(jobs[0].remaining_runs, Some(1));

        let status = db.status(Some("bounded")).expect("status");
        assert_eq!(status[0].max_runs, Some(2));
        assert_eq!(status[0].scheduled_runs, 1);
        assert_eq!(status[0].remaining_runs, Some(1));

        let _ = fs::remove_file(db.path);
    }

    #[test]
    fn disable_if_max_runs_reached_disables_bounded_job() {
        let db = test_db();
        let when = fixed_time();
        db.add_job(
            "bounded",
            &["echo".into(), "alive".into()],
            "every 10 seconds",
            3600,
            Some(1),
            when,
        )
        .expect("add job");
        let job = db.get_job_by_name("bounded").expect("get job");
        let run = db
            .create_run(job.id, "running", when, Some(when), "schedule")
            .expect("create scheduled run");
        db.finish_run(run, "failed", when, 10, Some(1), Some("boom"))
            .expect("finish scheduled run");

        let disabled = db
            .disable_if_max_runs_reached(job.id)
            .expect("disable bounded job");

        assert_eq!(disabled, Some(("bounded".into(), 1)));
        let status = db.status(Some("bounded")).expect("status");
        assert!(!status[0].enabled);
        assert_eq!(status[0].next_run_at, None);
        assert_eq!(status[0].remaining_runs, Some(0));

        let _ = fs::remove_file(db.path);
    }

    #[test]
    fn claim_scheduled_run_disables_job_after_limit_is_already_reached() {
        let db = test_db();
        let when = fixed_time();
        let next = when + chrono::Duration::seconds(10);
        db.add_job(
            "bounded",
            &["echo".into(), "alive".into()],
            "every 10 seconds",
            3600,
            Some(1),
            when,
        )
        .expect("add job");
        let job = db.get_job_by_name("bounded").expect("get job");
        let run = db
            .create_run(job.id, "running", when, Some(when), "schedule")
            .expect("create scheduled run");
        db.finish_run(run, "success", when, 10, Some(0), None)
            .expect("finish scheduled run");

        let claim = db
            .claim_scheduled_run(job.id, when, next, when)
            .expect("claim after limit");

        assert_eq!(claim, RunClaim::NotDue);
        let status = db.status(Some("bounded")).expect("status");
        assert!(!status[0].enabled);
        assert_eq!(status[0].next_run_at, None);

        let _ = fs::remove_file(db.path);
    }
}
