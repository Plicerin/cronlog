use crate::error::{Cron2Error, Result};
use chrono::NaiveDateTime;
use rusqlite::{params, Connection, OptionalExtension};
use std::path::Path;

#[derive(Debug, Clone)]
pub struct Job {
    pub id: i64,
    pub name: String,
    pub command: Vec<String>,
    pub schedule: String,
    pub timeout_seconds: i64,
    pub next_run_at: Option<NaiveDateTime>,
}

#[derive(Debug)]
pub struct JobListRow {
    pub name: String,
    pub command: String,
    pub schedule: String,
    pub enabled: bool,
    pub next_run_at: Option<NaiveDateTime>,
}

#[derive(Debug)]
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

#[derive(Debug)]
pub struct LogRow {
    pub stdout: Option<String>,
    pub stderr: Option<String>,
    pub stdout_truncated: bool,
    pub stderr_truncated: bool,
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
        Ok(())
    }

    pub fn add_job(
        &self,
        name: &str,
        command: &[String],
        schedule: &str,
        timeout_seconds: i64,
        next_run_at: NaiveDateTime,
    ) -> Result<()> {
        let conn = self.conn()?;
        let command_joined = shell_words::join(command.iter().map(String::as_str));
        conn.execute(
            "INSERT INTO jobs (name, command, schedule, timeout_seconds, next_run_at) VALUES (?1, ?2, ?3, ?4, ?5)",
            params![name, command_joined, schedule, timeout_seconds, next_run_at.to_string()],
        )?;
        Ok(())
    }

    pub fn list_jobs(&self) -> Result<Vec<JobListRow>> {
        let conn = self.conn()?;
        let mut stmt = conn.prepare(
            "SELECT name, command, schedule, enabled, next_run_at FROM jobs WHERE deleted_at IS NULL ORDER BY name"
        )?;
        let rows = stmt.query_map([], |row| {
            let next: Option<String> = row.get(4)?;
            Ok(JobListRow {
                name: row.get(0)?,
                command: row.get(1)?,
                schedule: row.get(2)?,
                enabled: row.get::<_, i64>(3)? == 1,
                next_run_at: parse_dt_opt(next),
            })
        })?;
        rows.collect::<std::result::Result<Vec<_>, _>>()
            .map_err(Into::into)
    }

    pub fn get_due_jobs(&self, now: NaiveDateTime) -> Result<Vec<Job>> {
        let conn = self.conn()?;
        let mut stmt = conn.prepare(
            "SELECT id, name, command, schedule, enabled, timeout_seconds, next_run_at
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
            "SELECT id, name, command, schedule, enabled, timeout_seconds, next_run_at FROM jobs WHERE name = ?1 AND deleted_at IS NULL",
            params![name],
            row_to_job,
        ).optional()?.ok_or_else(|| Cron2Error::NotFound(format!("job '{name}'")))
    }

    pub fn has_running_run(&self, job_id: i64) -> Result<bool> {
        let conn = self.conn()?;
        let count: i64 = conn.query_row(
            "SELECT COUNT(*) FROM runs WHERE job_id = ?1 AND status = 'running'",
            params![job_id],
            |row| row.get(0),
        )?;
        Ok(count > 0)
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

    pub fn update_next_run(&self, job_id: i64, next_run_at: NaiveDateTime) -> Result<()> {
        let conn = self.conn()?;
        conn.execute(
            "UPDATE jobs SET next_run_at = ?1, updated_at = CURRENT_TIMESTAMP WHERE id = ?2",
            params![next_run_at.to_string(), job_id],
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
            return Err(Cron2Error::NotFound(format!("job '{name}'")));
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
            return Err(Cron2Error::NotFound(format!("job '{name}'")));
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
            return Err(Cron2Error::NotFound(format!("run {run_id}")));
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
    let next: Option<String> = row.get(6)?;
    Ok(Job {
        id: row.get(0)?,
        name: row.get(1)?,
        command,
        schedule: row.get(3)?,
        timeout_seconds: row.get(5)?,
        next_run_at: parse_dt_opt(next),
    })
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
        let path = std::env::temp_dir().join(format!("cron2-test-{unique}.db"));
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

        assert!(matches!(err, Cron2Error::NotFound(message) if message == "run 999999"));

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
}
