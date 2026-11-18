use crate::clock;
use crate::error::{CronlogError, Result};
use chrono::{Local, NaiveDateTime, TimeZone};
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
    #[serde(serialize_with = "clock::serialize_local")]
    pub next_run_at: Option<NaiveDateTime>,
}

#[derive(Debug, Serialize)]
pub struct JobListRow {
    pub name: String,
    pub command: String,
    pub schedule: String,
    pub enabled: bool,
    #[serde(serialize_with = "clock::serialize_local")]
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
    #[serde(serialize_with = "clock::serialize_local")]
    pub scheduled_for: Option<NaiveDateTime>,
    #[serde(serialize_with = "clock::serialize_local")]
    pub started_at: Option<NaiveDateTime>,
    #[serde(serialize_with = "clock::serialize_local")]
    pub finished_at: Option<NaiveDateTime>,
    pub duration_ms: Option<i64>,
    pub exit_code: Option<i64>,
    pub error: Option<String>,
}

/// Hook events, as stored in `hook_runs.event` and in the settings keys.
pub const HOOK_FAILURE: &str = "failure";
pub const HOOK_RECOVERY: &str = "recovery";

fn hook_key(event: &str) -> String {
    format!("hook.on_{event}")
}

/// A queued hook, claimed to run, with what it needs to know about the run.
#[derive(Debug)]
pub struct PendingHook {
    pub id: i64,
    pub event: String,
    pub failure_streak: i64,
    pub run_id: i64,
    pub job_name: String,
    pub run_status: Option<String>,
    pub exit_code: Option<i64>,
    pub error: Option<String>,
    pub finished_at: Option<NaiveDateTime>,
    pub stderr: Option<String>,
}

#[derive(Debug, Serialize)]
pub struct HookRow {
    pub hook_id: i64,
    pub event: String,
    pub job: String,
    pub run_id: i64,
    pub status: String,
    pub failure_streak: i64,
    #[serde(serialize_with = "clock::serialize_local")]
    pub queued_at: Option<NaiveDateTime>,
    #[serde(serialize_with = "clock::serialize_local")]
    pub started_at: Option<NaiveDateTime>,
    #[serde(serialize_with = "clock::serialize_local")]
    pub finished_at: Option<NaiveDateTime>,
    pub duration_ms: Option<i64>,
    pub exit_code: Option<i64>,
    pub error: Option<String>,
}

const HOOK_ROW_SELECT: &str = "SELECT h.id, h.event, j.name, h.run_id, h.status, h.failure_streak,
        h.queued_at, h.started_at, h.finished_at, h.duration_ms, h.exit_code, h.error
     FROM hook_runs h JOIN jobs j ON j.id = h.job_id";

fn row_to_hook(row: &rusqlite::Row<'_>) -> rusqlite::Result<HookRow> {
    Ok(HookRow {
        hook_id: row.get(0)?,
        event: row.get(1)?,
        job: row.get(2)?,
        run_id: row.get(3)?,
        status: row.get(4)?,
        failure_streak: row.get(5)?,
        queued_at: parse_dt_opt(row.get(6)?),
        started_at: parse_dt_opt(row.get(7)?),
        finished_at: parse_dt_opt(row.get(8)?),
        duration_ms: row.get(9)?,
        exit_code: row.get(10)?,
        error: row.get(11)?,
    })
}

#[derive(Debug)]
pub struct InterruptedRun {
    pub run_id: i64,
    pub job_name: String,
}

/// The result of asking to start a manual run.
#[derive(Debug, PartialEq, Eq)]
pub enum ManualClaim {
    Started(i64),
    /// The job already has a run in progress: its id and start time.
    Busy(i64, Option<NaiveDateTime>),
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
    #[serde(serialize_with = "clock::serialize_local")]
    pub next_run_at: Option<NaiveDateTime>,
    pub max_runs: Option<i64>,
    pub scheduled_runs: i64,
    pub remaining_runs: Option<i64>,
    pub running_runs: i64,
    pub last_run_id: Option<i64>,
    pub last_status: Option<String>,
    pub last_trigger_type: Option<String>,
    #[serde(serialize_with = "clock::serialize_local")]
    pub last_scheduled_for: Option<NaiveDateTime>,
    #[serde(serialize_with = "clock::serialize_local")]
    pub last_started_at: Option<NaiveDateTime>,
    #[serde(serialize_with = "clock::serialize_local")]
    pub last_finished_at: Option<NaiveDateTime>,
    pub last_duration_ms: Option<i64>,
    pub last_exit_code: Option<i64>,
    pub last_error: Option<String>,
}

/// A finished run's outcome and captured output, recorded together.
#[derive(Debug)]
pub struct RunResult<'a> {
    pub status: &'a str,
    pub finished_at: NaiveDateTime,
    pub duration_ms: i64,
    pub exit_code: Option<i64>,
    pub error: Option<&'a str>,
    pub stdout: &'a str,
    pub stderr: &'a str,
    pub stdout_truncated: bool,
    pub stderr_truncated: bool,
    /// The end of stderr when `stderr` is a truncated start, for a failure hook.
    pub stderr_tail: Option<&'a str>,
}

/// The daemon's last sign of life, written every HEARTBEAT_EVERY.
#[derive(Debug, Clone)]
pub struct Heartbeat {
    pub pid: i64,
    pub started_at: NaiveDateTime,
    pub last_tick: NaiveDateTime,
    pub poll_seconds: i64,
}

/// A job as `health` checks it.
#[derive(Debug, Clone)]
pub struct HealthJob {
    pub job: Job,
    pub enabled: bool,
    /// Finished runs in a row, in completion order, that failed, timed out or were
    /// interrupted. A success resets it; a skip leaves it. Kept on the job, so pruning
    /// old runs never shortens it.
    pub failure_streak: i64,
}

/// A finished run, as `health` describes it.
#[derive(Debug, Clone)]
pub struct Outcome {
    pub run_id: i64,
    pub status: String,
    pub finished_at: Option<NaiveDateTime>,
    pub exit_code: Option<i64>,
    pub error: Option<String>,
}

/// What one prune pass removed.
#[derive(Debug, Default, Clone, Copy, Serialize, PartialEq, Eq)]
pub struct PruneReport {
    pub runs: usize,
    pub logs: usize,
}

/// One job's runs old enough to delete: job ?1, scheduled before the cutoff ?2, and older
/// than the job's keep-last boundary run (?3 scheduled_for, ?4 id). Never a `running` run.
/// Never a run whose hook is queued or running (its alert must still be sent), nor the
/// run of the job's latest finished hook, which `cronlog health` reads: pruning must not
/// clear `hook_failing` when no hook has succeeded since.
///
/// Age is `scheduled_for`, the one timestamp every run has (an overlap skip has no
/// `started_at`). Each query is a range scan on an index whose rows are nearly all
/// deletable, so the rows examined stay close to the rows deleted: at most keep-last plus
/// any `running` runs are scanned and kept.
const PRUNABLE_FOR_JOB: &str = "FROM runs INDEXED BY idx_runs_job_scheduled
    WHERE job_id = ?1 AND scheduled_for < ?2 AND (scheduled_for, id) < (?3, ?4)
      AND status <> 'running'
      AND NOT EXISTS (SELECT 1 FROM hook_runs h WHERE h.run_id = runs.id
                      AND (h.status IN ('pending', 'running')
                           OR h.id = (SELECT l.id FROM hook_runs l
                                      WHERE l.job_id = h.job_id
                                        AND l.status NOT IN ('pending', 'running', 'skipped')
                                      ORDER BY l.finished_at DESC, l.id DESC LIMIT 1)))";

/// For a live job with `--max-runs`: its limit is enforced by counting its scheduled,
/// started runs, so only its overlap skips and its manual runs may go. Separate indexed
/// queries for each, so the kept counted runs are never scanned. INDEXED BY pins the
/// index: left to itself SQLite picks idx_runs_job_scheduled and walks those runs.
const PRUNABLE_SKIPS_FOR_BOUNDED_JOB: &str = "FROM runs INDEXED BY idx_runs_job_trigger_status
    WHERE job_id = ?1 AND trigger_type = 'schedule' AND status = 'skipped'
      AND scheduled_for < ?2 AND (scheduled_for, id) < (?3, ?4)
      AND NOT EXISTS (SELECT 1 FROM hook_runs h WHERE h.run_id = runs.id
                      AND (h.status IN ('pending', 'running')
                           OR h.id = (SELECT l.id FROM hook_runs l
                                      WHERE l.job_id = h.job_id
                                        AND l.status NOT IN ('pending', 'running', 'skipped')
                                      ORDER BY l.finished_at DESC, l.id DESC LIMIT 1)))";
const PRUNABLE_MANUAL_FOR_BOUNDED_JOB: &str = "FROM runs INDEXED BY idx_runs_job_trigger_status
    WHERE job_id = ?1 AND trigger_type = 'manual'
      AND status IN ('success', 'failed', 'timed_out', 'interrupted', 'skipped')
      AND scheduled_for < ?2 AND (scheduled_for, id) < (?3, ?4)
      AND NOT EXISTS (SELECT 1 FROM hook_runs h WHERE h.run_id = runs.id
                      AND (h.status IN ('pending', 'running')
                           OR h.id = (SELECT l.id FROM hook_runs l
                                      WHERE l.job_id = h.job_id
                                        AND l.status NOT IN ('pending', 'running', 'skipped')
                                      ORDER BY l.finished_at DESC, l.id DESC LIMIT 1)))";

/// The prunable-run filters for one job.
fn prunable_filters(protect_counted: bool) -> &'static [&'static str] {
    if protect_counted {
        &[
            PRUNABLE_SKIPS_FOR_BOUNDED_JOB,
            PRUNABLE_MANUAL_FOR_BOUNDED_JOB,
        ]
    } else {
        &[PRUNABLE_FOR_JOB]
    }
}

/// Pages handed back to the filesystem per step (about 4 MB at SQLite's default 4 KB page
/// size). Each step is its own short write transaction.
pub const RECLAIM_CHUNK_PAGES: i64 = 1000;

/// A job with more than keep-last runs, and the newest run that pruning may not pass.
struct PruneTarget {
    job_id: i64,
    boundary_scheduled_for: String,
    boundary_id: i64,
    protect_counted: bool,
}

/// A path, not a connection: every operation opens its own connection, so a clone can be
/// handed to each job's thread.
#[derive(Debug, Clone)]
pub struct Database {
    path: String,
}

/// Jobs now finish concurrently and each records its result; wait for a lock instead of
/// failing with SQLITE_BUSY.
const BUSY_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(10);

impl Database {
    pub fn open(path: impl AsRef<Path>) -> Result<Self> {
        Ok(Self {
            path: path.as_ref().to_string_lossy().to_string(),
        })
    }

    fn conn(&self) -> Result<Connection> {
        let conn = Connection::open(&self.path)?;
        conn.busy_timeout(BUSY_TIMEOUT)?;
        conn.pragma_update(None, "foreign_keys", "ON")?;
        Ok(conn)
    }

    pub fn init(&self) -> Result<()> {
        let mut conn = self.conn()?;
        let tables: i64 = conn.query_row(
            "SELECT COUNT(*) FROM sqlite_master WHERE type = 'table'",
            [],
            |r| r.get(0),
        )?;
        if tables == 0 {
            // Only takes effect before the first table exists. Lets pruning hand freed
            // pages back to the filesystem; older databases convert with `prune --vacuum`.
            conn.execute_batch("PRAGMA auto_vacuum = INCREMENTAL;")?;
        }
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

            -- One row: the daemon's last sign of life, for `cronlog health`.
            CREATE TABLE IF NOT EXISTS daemon_heartbeat (
                id INTEGER PRIMARY KEY CHECK (id = 1),
                pid INTEGER NOT NULL,
                started_at TEXT NOT NULL,
                last_tick TEXT NOT NULL,
                poll_seconds INTEGER NOT NULL
            );

            -- Settings kept in the database, such as the failure and recovery hooks.
            CREATE TABLE IF NOT EXISTS settings (
                key TEXT PRIMARY KEY,
                value TEXT NOT NULL
            );

            -- Hooks queued in the transaction that records a run, and how each went.
            CREATE TABLE IF NOT EXISTS hook_runs (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                run_id INTEGER NOT NULL,
                job_id INTEGER NOT NULL,
                event TEXT NOT NULL,
                status TEXT NOT NULL,
                failure_streak INTEGER NOT NULL,
                queued_at TEXT,
                started_at TEXT,
                finished_at TEXT,
                duration_ms INTEGER,
                exit_code INTEGER,
                error TEXT,
                stderr_tail TEXT
            );

            CREATE INDEX IF NOT EXISTS idx_jobs_due ON jobs(enabled, next_run_at, deleted_at);
            CREATE INDEX IF NOT EXISTS idx_runs_job_started ON runs(job_id, started_at);
            CREATE INDEX IF NOT EXISTS idx_logs_run ON logs(run_id);
            CREATE INDEX IF NOT EXISTS idx_runs_job_scheduled ON runs(job_id, scheduled_for);
            -- A job's running runs and its newest failure, without walking its history.
            CREATE INDEX IF NOT EXISTS idx_runs_job_status ON runs(job_id, status);
            CREATE INDEX IF NOT EXISTS idx_runs_job_trigger_status
                ON runs(job_id, trigger_type, status, scheduled_for);
            CREATE INDEX IF NOT EXISTS idx_hook_runs_status ON hook_runs(status, id);
            CREATE INDEX IF NOT EXISTS idx_hook_runs_run ON hook_runs(run_id);
            -- Each job's latest finished hook (health, and what pruning must keep).
            CREATE INDEX IF NOT EXISTS idx_hook_runs_job_finished
                ON hook_runs(job_id, finished_at DESC, id DESC)
                WHERE status NOT IN ('pending', 'running', 'skipped');
            "#,
        )?;
        if !jobs_has_column(&conn, "max_runs")? {
            conn.execute("ALTER TABLE jobs ADD COLUMN max_runs INTEGER", [])?;
        }
        if !jobs_has_column(&conn, "failure_streak")? {
            // Counting starts now; runs recorded before this column are not counted.
            conn.execute(
                "ALTER TABLE jobs ADD COLUMN failure_streak INTEGER NOT NULL DEFAULT 0",
                [],
            )?;
        }
        self.convert_times_to_utc(&mut conn, &Local)?;
        Ok(())
    }

    /// Cronlog used to store local wall-clock times, read as `tz`; rewrite them as UTC,
    /// once. A database with anything in it is first copied next to itself, because the
    /// change is one-way: an older Cronlog would read the UTC times as local ones.
    ///
    /// The copy and the conversion happen while this connection holds the write lock, so
    /// when several Cronlog processes open an old database at once, one backs it up and
    /// converts it and the others wait, then find it converted.
    fn convert_times_to_utc<Tz: TimeZone>(&self, conn: &mut Connection, tz: &Tz) -> Result<()> {
        if user_version(conn)? >= UTC_TIMES {
            return Ok(());
        }
        let tx = conn.transaction_with_behavior(TransactionBehavior::Immediate)?;
        if user_version(&tx)? >= UTC_TIMES {
            return Ok(()); // another cronlog process converted it first
        }
        let has_data: bool = tx.query_row(
            "SELECT EXISTS (SELECT 1 FROM jobs) OR EXISTS (SELECT 1 FROM runs)",
            [],
            |r| r.get(0),
        )?;
        // Readers may share the lock this transaction holds, and it has written nothing
        // yet, so a second connection copies the database exactly as it is.
        let backup = if has_data {
            Some(self.backup_before_utc()?)
        } else {
            None
        };

        let mut converted = 0;
        for (table, column) in LOCAL_TIME_COLUMNS {
            let rows: Vec<(i64, String)> = tx
                .prepare(&format!(
                    "SELECT id, {column} FROM {table} WHERE {column} IS NOT NULL"
                ))?
                .query_map([], |r| Ok((r.get(0)?, r.get(1)?)))?
                .collect::<rusqlite::Result<_>>()?;
            let mut update =
                tx.prepare(&format!("UPDATE {table} SET {column} = ?1 WHERE id = ?2"))?;
            for (id, value) in rows {
                // Anything unreadable is left as it is; nothing reads it now either.
                if let Some(local) = parse_dt_opt(Some(value)) {
                    update.execute(params![clock::local_to_utc(tz, local).to_string(), id])?;
                    converted += 1;
                }
            }
        }
        tx.pragma_update(None, "user_version", UTC_TIMES)?;
        tx.commit()?;
        if let Some(backup) = backup {
            eprintln!(
                "Converted {converted} stored times in {} from local time to UTC. The database as it was is at {backup}",
                self.path
            );
        }
        Ok(())
    }

    /// A consistent copy of the database at `<path>.before-utc.bak`, or a name with a
    /// timestamp if that is taken. Returns its path.
    ///
    /// The file is created first, empty and locked down (see `create_backup_file`), so
    /// job commands and captured output are never readable by more users than the
    /// database is, not even while the copy is written. Creating it also claims the
    /// name, so an existing backup is never overwritten.
    fn backup_before_utc(&self) -> Result<String> {
        let stamp = clock::now().format("%Y%m%d%H%M%S");
        let candidates = std::iter::once(format!("{}.before-utc.bak", self.path)).chain(
            (0..100).map(|n| match n {
                0 => format!("{}.before-utc-{stamp}.bak", self.path),
                n => format!("{}.before-utc-{stamp}-{n}.bak", self.path),
            }),
        );
        for backup in candidates {
            match create_backup_file(&backup, &self.path) {
                Ok(()) => {}
                Err(err) if err.kind() == std::io::ErrorKind::AlreadyExists => continue,
                Err(err) => return Err(err.into()),
            }
            // A second connection: VACUUM cannot run inside the caller's transaction.
            let copied = self
                .conn()
                .and_then(|c| Ok(c.execute("VACUUM INTO ?1", params![backup])?));
            return match copied {
                Ok(_) => Ok(backup),
                Err(err) => {
                    let _ = std::fs::remove_file(&backup);
                    Err(err)
                }
            };
        }
        Err(CronlogError::InvalidCommand(format!(
            "could not choose a backup name next to {}",
            self.path
        )))
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

    pub fn record_heartbeat(&self, heartbeat: &Heartbeat) -> Result<()> {
        self.conn()?.execute(
            "INSERT INTO daemon_heartbeat (id, pid, started_at, last_tick, poll_seconds)
             VALUES (1, ?1, ?2, ?3, ?4)
             ON CONFLICT(id) DO UPDATE SET pid = ?1, started_at = ?2, last_tick = ?3, poll_seconds = ?4",
            params![
                heartbeat.pid,
                heartbeat.started_at.to_string(),
                heartbeat.last_tick.to_string(),
                heartbeat.poll_seconds
            ],
        )?;
        Ok(())
    }

    pub fn heartbeat(&self) -> Result<Option<Heartbeat>> {
        let row = self
            .conn()?
            .query_row(
                "SELECT pid, started_at, last_tick, poll_seconds FROM daemon_heartbeat WHERE id = 1",
                [],
                |r| {
                    Ok((
                        r.get::<_, i64>(0)?,
                        r.get::<_, String>(1)?,
                        r.get::<_, String>(2)?,
                        r.get::<_, i64>(3)?,
                    ))
                },
            )
            .optional()?;
        Ok(row.and_then(|(pid, started_at, last_tick, poll_seconds)| {
            Some(Heartbeat {
                pid,
                started_at: parse_dt_opt(Some(started_at))?,
                last_tick: parse_dt_opt(Some(last_tick))?,
                poll_seconds,
            })
        }))
    }

    /// The enabled jobs, or the one named (enabled or not).
    pub fn health_jobs(&self, name: Option<&str>) -> Result<Vec<HealthJob>> {
        let conn = self.conn()?;
        let select =
            "SELECT id, name, command, schedule, enabled, timeout_seconds, max_runs, next_run_at,
                    failure_streak
             FROM jobs WHERE deleted_at IS NULL";
        let to_health = |row: &rusqlite::Row<'_>| -> rusqlite::Result<HealthJob> {
            Ok(HealthJob {
                job: row_to_job(row)?,
                enabled: row.get::<_, i64>(4)? != 0,
                failure_streak: row.get(8)?,
            })
        };
        match name {
            Some(name) => conn
                .query_row(&format!("{select} AND name = ?1"), params![name], to_health)
                .optional()?
                .map(|job| vec![job])
                .ok_or_else(|| CronlogError::NotFound(format!("job '{name}'"))),
            None => Ok(conn
                .prepare(&format!("{select} AND enabled = 1 ORDER BY name"))?
                .query_map([], to_health)?
                .collect::<rusqlite::Result<_>>()?),
        }
    }

    /// A job's most recently finished failed, timed-out or interrupted run.
    pub fn last_failure(&self, job_id: i64) -> Result<Option<Outcome>> {
        Ok(self
            .conn()?
            .query_row(
                "SELECT id, status, finished_at, exit_code, error FROM runs
                 WHERE job_id = ?1 AND status IN ('failed', 'timed_out', 'interrupted')
                 ORDER BY finished_at DESC, id DESC LIMIT 1",
                params![job_id],
                |r| {
                    Ok(Outcome {
                        run_id: r.get(0)?,
                        status: r.get(1)?,
                        finished_at: parse_dt_opt(r.get(2)?),
                        exit_code: r.get(3)?,
                        error: r.get(4)?,
                    })
                },
            )
            .optional()?)
    }

    /// A job's `running` runs: (run id, started at).
    pub fn running_runs(&self, job_id: i64) -> Result<Vec<(i64, Option<NaiveDateTime>)>> {
        let conn = self.conn()?;
        let mut stmt = conn.prepare(
            "SELECT id, started_at FROM runs WHERE job_id = ?1 AND status = 'running' ORDER BY id",
        )?;
        let rows = stmt.query_map(params![job_id], |r| {
            Ok((r.get(0)?, parse_dt_opt(r.get(1)?)))
        })?;
        Ok(rows.collect::<rusqlite::Result<_>>()?)
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

    /// Start a manual run, unless the job already has one in progress. The check and
    /// the insert share one transaction, so the daemon can't start a run in between.
    /// `force` starts it regardless.
    pub fn claim_manual_run(
        &self,
        job_id: i64,
        scheduled_for: NaiveDateTime,
        started_at: NaiveDateTime,
        force: bool,
    ) -> Result<ManualClaim> {
        let mut conn = self.conn()?;
        let tx = conn.transaction_with_behavior(TransactionBehavior::Immediate)?;
        if !force {
            let active = tx
                .query_row(
                    "SELECT id, started_at FROM runs WHERE job_id = ?1 AND status = 'running' ORDER BY id LIMIT 1",
                    params![job_id],
                    |r| Ok((r.get::<_, i64>(0)?, parse_dt_opt(r.get(1)?))),
                )
                .optional()?;
            if let Some((run_id, started)) = active {
                tx.commit()?;
                return Ok(ManualClaim::Busy(run_id, started));
            }
        }
        tx.execute(
            "INSERT INTO runs (job_id, status, scheduled_for, started_at, trigger_type)
             VALUES (?1, 'running', ?2, ?3, 'manual')",
            params![job_id, scheduled_for.to_string(), started_at.to_string()],
        )?;
        let run_id = tx.last_insert_rowid();
        tx.commit()?;
        Ok(ManualClaim::Started(run_id))
    }

    /// The command line set for a hook event, if any.
    pub fn hook_command(&self, event: &str) -> Result<Option<String>> {
        let conn = self.conn()?;
        Ok(conn
            .query_row(
                "SELECT value FROM settings WHERE key = ?1",
                params![hook_key(event)],
                |r| r.get(0),
            )
            .optional()?)
    }

    /// Set (Some) or remove (None) the command line for a hook event.
    pub fn set_hook_command(&self, event: &str, command: Option<&str>) -> Result<()> {
        let conn = self.conn()?;
        match command {
            Some(command) => conn.execute(
                "INSERT INTO settings (key, value) VALUES (?1, ?2)
                 ON CONFLICT(key) DO UPDATE SET value = excluded.value",
                params![hook_key(event), command],
            )?,
            None => conn.execute(
                "DELETE FROM settings WHERE key = ?1",
                params![hook_key(event)],
            )?,
        };
        Ok(())
    }

    pub fn has_pending_hooks(&self) -> Result<bool> {
        let conn = self.conn()?;
        Ok(conn.query_row(
            "SELECT EXISTS(SELECT 1 FROM hook_runs WHERE status = 'pending')",
            [],
            |r| r.get(0),
        )?)
    }

    /// Take the oldest queued hook and mark it `running`, in one transaction, so two
    /// processes (the daemon and a manual run) never run the same hook.
    /// Take the oldest queued hook (of one run, if given) and mark it running.
    pub fn claim_pending_hook(
        &self,
        now: NaiveDateTime,
        run_id: Option<i64>,
    ) -> Result<Option<PendingHook>> {
        let mut conn = self.conn()?;
        let tx = conn.transaction_with_behavior(TransactionBehavior::Immediate)?;
        let pending = tx
            .query_row(
                "SELECT h.id, h.event, h.failure_streak, h.run_id, j.name,
                        r.status, r.exit_code, r.error, r.finished_at,
                        COALESCE(h.stderr_tail,
                            (SELECT l.stderr FROM logs l WHERE l.run_id = h.run_id ORDER BY l.id DESC LIMIT 1))
                 FROM hook_runs h
                 JOIN jobs j ON j.id = h.job_id
                 LEFT JOIN runs r ON r.id = h.run_id
                 WHERE h.status = 'pending' AND (?1 IS NULL OR h.run_id = ?1)
                 ORDER BY h.id
                 LIMIT 1",
                params![run_id],
                |r| {
                    Ok(PendingHook {
                        id: r.get(0)?,
                        event: r.get(1)?,
                        failure_streak: r.get(2)?,
                        run_id: r.get(3)?,
                        job_name: r.get(4)?,
                        run_status: r.get(5)?,
                        exit_code: r.get(6)?,
                        error: r.get(7)?,
                        finished_at: parse_dt_opt(r.get(8)?),
                        stderr: r.get(9)?,
                    })
                },
            )
            .optional()?;
        if let Some(hook) = &pending {
            tx.execute(
                "UPDATE hook_runs SET status = 'running', started_at = ?1 WHERE id = ?2",
                params![now.to_string(), hook.id],
            )?;
        }
        tx.commit()?;
        Ok(pending)
    }

    pub fn finish_hook(
        &self,
        id: i64,
        status: &str,
        finished_at: NaiveDateTime,
        duration_ms: i64,
        exit_code: Option<i64>,
        error: Option<&str>,
    ) -> Result<()> {
        let conn = self.conn()?;
        conn.execute(
            "UPDATE hook_runs SET status = ?1, finished_at = ?2, duration_ms = ?3, exit_code = ?4, error = ?5
             WHERE id = ?6",
            params![status, finished_at.to_string(), duration_ms, exit_code, error, id],
        )?;
        Ok(())
    }

    /// Hooks still `running` that started before `started_before` were left by a
    /// process that stopped: mark them interrupted. Returns how many.
    pub fn interrupt_stale_hooks(
        &self,
        started_before: NaiveDateTime,
        finished_at: NaiveDateTime,
    ) -> Result<usize> {
        let conn = self.conn()?;
        Ok(conn.execute(
            "UPDATE hook_runs SET status = 'interrupted', finished_at = ?1,
                    error = 'cronlog stopped while the hook was running'
             WHERE status = 'running' AND started_at < ?2",
            params![finished_at.to_string(), started_before.to_string()],
        )?)
    }

    /// The newest hook runs, newest first.
    pub fn hook_history(&self, limit: i64) -> Result<Vec<HookRow>> {
        let conn = self.conn()?;
        let mut stmt = conn.prepare(&format!("{HOOK_ROW_SELECT} ORDER BY h.id DESC LIMIT ?1"))?;
        let rows = stmt.query_map(params![limit], row_to_hook)?;
        Ok(rows.collect::<rusqlite::Result<_>>()?)
    }

    /// The hook that most recently ran to an end (not queued, running or skipped), for
    /// one job or any. Ordered by when it finished: the daemon and a manual run can run
    /// hooks at the same time, so a later id can finish first.
    pub fn last_finished_hook(&self, job: Option<&str>) -> Result<Option<HookRow>> {
        let conn = self.conn()?;
        Ok(conn
            .query_row(
                &format!(
                    "{HOOK_ROW_SELECT} WHERE h.status NOT IN ('pending', 'running', 'skipped')
                       AND (?1 IS NULL OR j.name = ?1)
                     ORDER BY h.finished_at DESC, h.id DESC LIMIT 1"
                ),
                params![job],
                row_to_hook,
            )
            .optional()?)
    }

    /// Insert a run directly, for setting up test state.
    #[cfg(test)]
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

    /// Record a run's outcome and its captured output in one transaction. The run stays
    /// `running` until this commits, and pruning never touches a `running` run, so it can
    /// never delete a run between its status and its logs.
    pub fn record_result(&self, run_id: i64, result: &RunResult<'_>) -> Result<()> {
        let mut conn = self.conn()?;
        let tx = conn.transaction_with_behavior(TransactionBehavior::Immediate)?;
        let updated = tx.execute(
            "UPDATE runs SET status = ?1, finished_at = ?2, duration_ms = ?3, exit_code = ?4, error = ?5 WHERE id = ?6",
            params![
                result.status,
                result.finished_at.to_string(),
                result.duration_ms,
                result.exit_code,
                result.error,
                run_id
            ],
        )?;
        if updated == 0 {
            return Err(CronlogError::NotFound(format!("run {run_id}")));
        }
        update_failure_streak(&tx, run_id, result.status)?;
        tx.execute(
            "INSERT INTO logs (run_id, stdout, stderr, stdout_truncated, stderr_truncated) VALUES (?1, ?2, ?3, ?4, ?5)",
            params![
                run_id,
                result.stdout,
                result.stderr,
                result.stdout_truncated as i64,
                result.stderr_truncated as i64
            ],
        )?;
        if let Some(tail) = result.stderr_tail {
            tx.execute(
                "UPDATE hook_runs SET stderr_tail = ?1 WHERE run_id = ?2 AND status = 'pending'",
                params![tail, run_id],
            )?;
        }
        tx.commit()?;
        Ok(())
    }

    #[cfg(test)]
    pub fn finish_run(
        &self,
        run_id: i64,
        status: &str,
        finished_at: NaiveDateTime,
        duration_ms: i64,
        exit_code: Option<i64>,
        error: Option<&str>,
    ) -> Result<()> {
        let mut conn = self.conn()?;
        let tx = conn.transaction()?;
        tx.execute(
            "UPDATE runs SET status = ?1, finished_at = ?2, duration_ms = ?3, exit_code = ?4, error = ?5 WHERE id = ?6",
            params![status, finished_at.to_string(), duration_ms, exit_code, error, run_id],
        )?;
        update_failure_streak(&tx, run_id, status)?;
        tx.commit()?;
        Ok(())
    }

    /// Mark one run `interrupted` if it is still `running`. Returns false when it had
    /// already finished (nothing to do).
    pub fn interrupt_run(
        &self,
        run_id: i64,
        finished_at: NaiveDateTime,
        message: &str,
    ) -> Result<bool> {
        let mut conn = self.conn()?;
        let tx = conn.transaction()?;
        let updated = tx.execute(
            "UPDATE runs SET status = 'interrupted', finished_at = ?1, error = ?2,
                    duration_ms = MAX(0, CAST((julianday(?1) - julianday(started_at)) * 86400000 AS INTEGER))
             WHERE id = ?3 AND status = 'running'",
            params![finished_at.to_string(), message, run_id],
        )?;
        if updated > 0 {
            update_failure_streak(&tx, run_id, "interrupted")?;
            tx.execute(
                "INSERT INTO logs (run_id, stdout, stderr, stdout_truncated, stderr_truncated) VALUES (?1, '', ?2, 0, 0)",
                params![run_id, message],
            )?;
        }
        tx.commit()?;
        Ok(updated > 0)
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
            update_failure_streak(&tx, run_id, "interrupted")?;
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

    #[cfg(test)]
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

    /// Delete up to `batch` prunable runs (see PRUNABLE_FOR_JOB) and their logs, in one
    /// short transaction so the daemon's own writes wait at most one batch. The work is a
    /// few index lookups per job plus the rows deleted, however long the history. Returns
    /// what was removed; fewer than `batch` runs means nothing prunable is left.
    pub fn prune_batch(
        &self,
        cutoff: NaiveDateTime,
        keep_last: i64,
        batch: i64,
    ) -> Result<PruneReport> {
        let mut conn = self.conn()?;
        let tx = conn.transaction_with_behavior(TransactionBehavior::Immediate)?;
        let cutoff = cutoff.to_string();
        let mut report = PruneReport::default();
        {
            let mut delete_logs = tx.prepare("DELETE FROM logs WHERE run_id = ?1")?;
            let mut delete_hooks = tx.prepare("DELETE FROM hook_runs WHERE run_id = ?1")?;
            let mut delete_run = tx.prepare("DELETE FROM runs WHERE id = ?1")?;
            'jobs: for target in prune_targets(&tx, keep_last)? {
                for filter in prunable_filters(target.protect_counted) {
                    let room = batch - report.runs as i64;
                    if room <= 0 {
                        break 'jobs;
                    }
                    let ids = tx
                        .prepare_cached(&format!("SELECT id {filter} LIMIT ?5"))?
                        .query_map(
                            params![
                                target.job_id,
                                cutoff,
                                target.boundary_scheduled_for,
                                target.boundary_id,
                                room
                            ],
                            |row| row.get::<_, i64>(0),
                        )?
                        .collect::<rusqlite::Result<Vec<_>>>()?;
                    for id in ids {
                        report.logs += delete_logs.execute([id])?;
                        delete_hooks.execute([id])?;
                        report.runs += delete_run.execute([id])?;
                    }
                }
            }
        }
        tx.commit()?;
        Ok(report)
    }

    /// How many runs `prune_batch` would delete in total, without deleting any.
    pub fn count_prunable(&self, cutoff: NaiveDateTime, keep_last: i64) -> Result<i64> {
        let conn = self.conn()?;
        let cutoff = cutoff.to_string();
        let mut total = 0;
        for target in prune_targets(&conn, keep_last)? {
            for filter in prunable_filters(target.protect_counted) {
                total += conn
                    .prepare_cached(&format!("SELECT COUNT(*) {filter}"))?
                    .query_row(
                        params![
                            target.job_id,
                            cutoff,
                            target.boundary_scheduled_for,
                            target.boundary_id
                        ],
                        |row| row.get::<_, i64>(0),
                    )?;
            }
        }
        Ok(total)
    }

    /// Hand up to `max_pages` freed pages back to the filesystem, in one short statement.
    /// Returns how many free pages remain. A no-op (returning 0) unless the database uses
    /// incremental auto-vacuum.
    pub fn reclaim_some(&self, max_pages: i64) -> Result<i64> {
        let conn = self.conn()?;
        if auto_vacuum_mode(&conn)? != 2 {
            return Ok(0);
        }
        {
            // Frees one page per step, so step it to the end; a single step (as
            // execute_batch does) hands back one page.
            let mut stmt = conn.prepare(&format!("PRAGMA incremental_vacuum({max_pages})"))?;
            let mut rows = stmt.query([])?;
            while rows.next()?.is_some() {}
        }
        Ok(conn.query_row("PRAGMA freelist_count", [], |r| r.get(0))?)
    }

    /// Give all pages freed by pruning back to the filesystem, chunk by chunk so other
    /// writers get the lock in between. `full` instead rewrites the whole file (VACUUM)
    /// and switches it to incremental auto-vacuum, holding an exclusive lock for the whole
    /// rewrite. Without either, SQLite still reuses freed pages, so the file stops growing.
    pub fn reclaim_space(&self, full: bool) -> Result<()> {
        if full {
            self.conn()?
                .execute_batch("PRAGMA auto_vacuum = INCREMENTAL; VACUUM;")?;
            return Ok(());
        }
        while self.reclaim_some(RECLAIM_CHUNK_PAGES)? > 0 {}
        Ok(())
    }

    /// The database file's size in bytes, as SQLite counts its pages.
    pub fn size_bytes(&self) -> Result<i64> {
        let conn = self.conn()?;
        let pages: i64 = conn.query_row("PRAGMA page_count", [], |r| r.get(0))?;
        let page_size: i64 = conn.query_row("PRAGMA page_size", [], |r| r.get(0))?;
        Ok(pages * page_size)
    }

    #[cfg(test)]
    pub fn auto_vacuum(&self) -> Result<i64> {
        auto_vacuum_mode(&self.conn()?)
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

    /// Enable a job and set its next run in one transaction, so the daemon never sees
    /// it enabled with a stale timestamp. `next_for` gets the job's schedule, whether it
    /// was already enabled, and its current next run, and returns the next run to store.
    pub fn enable_job<F>(&self, name: &str, next_for: F) -> Result<NaiveDateTime>
    where
        F: FnOnce(&str, bool, Option<NaiveDateTime>) -> Result<NaiveDateTime>,
    {
        let mut conn = self.conn()?;
        let tx = conn.transaction_with_behavior(TransactionBehavior::Immediate)?;
        let (schedule, enabled, current_next) = tx
            .query_row(
                "SELECT schedule, enabled, next_run_at FROM jobs WHERE name = ?1 AND deleted_at IS NULL",
                params![name],
                |row| {
                    Ok((
                        row.get::<_, String>(0)?,
                        row.get::<_, i64>(1)? != 0,
                        row.get::<_, Option<String>>(2)?,
                    ))
                },
            )
            .optional()?
            .ok_or_else(|| CronlogError::NotFound(format!("job '{name}'")))?;
        let next = next_for(&schedule, enabled, parse_dt_opt(current_next))?;
        tx.execute(
            "UPDATE jobs SET enabled = 1, next_run_at = ?1, updated_at = CURRENT_TIMESTAMP WHERE name = ?2 AND deleted_at IS NULL",
            params![next.to_string(), name],
        )?;
        tx.commit()?;
        Ok(next)
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

/// Every job with more than `keep_last` runs, with the newest run pruning may not pass
/// (its `keep_last`-th newest). Reads each boundary from idx_runs_job_scheduled.
fn prune_targets(conn: &Connection, keep_last: i64) -> Result<Vec<PruneTarget>> {
    let jobs = conn
        .prepare("SELECT id, max_runs IS NOT NULL AND deleted_at IS NULL FROM jobs ORDER BY id")?
        .query_map([], |row| {
            Ok((row.get::<_, i64>(0)?, row.get::<_, bool>(1)?))
        })?
        .collect::<rusqlite::Result<Vec<_>>>()?;
    let mut boundary = conn.prepare(
        "SELECT scheduled_for, id FROM runs
         WHERE job_id = ?1 AND scheduled_for IS NOT NULL
         ORDER BY scheduled_for DESC, id DESC LIMIT 1 OFFSET ?2",
    )?;
    let mut targets = Vec::new();
    for (job_id, protect_counted) in jobs {
        let found = boundary
            .query_row(params![job_id, keep_last - 1], |row| {
                Ok((row.get::<_, String>(0)?, row.get::<_, i64>(1)?))
            })
            .optional()?;
        if let Some((boundary_scheduled_for, boundary_id)) = found {
            targets.push(PruneTarget {
                job_id,
                boundary_scheduled_for,
                boundary_id,
                protect_counted,
            });
        }
    }
    Ok(targets)
}

/// Create `path`, empty and new, for a copy of the database at `db`.
///
/// - Unix: owner-only (0600). Copying the database's mode is not enough: the new file
///   takes its folder's group, which may not be the database's.
/// - Windows: the database's own access list, protected from the folder's inherited
///   rules, so a database locked down inside a shared folder stays locked down.
fn create_backup_file(path: &str, db: &str) -> std::io::Result<()> {
    let mut options = std::fs::OpenOptions::new();
    options.write(true).create_new(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.mode(0o600);
    }
    drop(options.open(path)?);
    #[cfg(windows)]
    if let Err(err) = copy_access_list(db, path) {
        let _ = std::fs::remove_file(path);
        return Err(err);
    }
    #[cfg(not(windows))]
    let _ = db;
    Ok(())
}

/// Give `to` the access list (DACL) of `from`, and stop it inheriting its folder's.
#[cfg(windows)]
fn copy_access_list(from: &str, to: &str) -> std::io::Result<()> {
    use std::ffi::{c_void, OsStr};
    use std::os::windows::ffi::OsStrExt;
    use std::ptr::null_mut;

    const SE_FILE_OBJECT: i32 = 1;
    const DACL_SECURITY_INFORMATION: u32 = 0x0000_0004;
    const PROTECTED_DACL_SECURITY_INFORMATION: u32 = 0x8000_0000;

    #[link(name = "advapi32")]
    extern "system" {
        fn GetNamedSecurityInfoW(
            object_name: *const u16,
            object_type: i32,
            security_info: u32,
            owner: *mut *mut c_void,
            group: *mut *mut c_void,
            dacl: *mut *mut c_void,
            sacl: *mut *mut c_void,
            security_descriptor: *mut *mut c_void,
        ) -> u32;
        fn SetNamedSecurityInfoW(
            object_name: *mut u16,
            object_type: i32,
            security_info: u32,
            owner: *mut c_void,
            group: *mut c_void,
            dacl: *mut c_void,
            sacl: *mut c_void,
        ) -> u32;
    }
    #[link(name = "kernel32")]
    extern "system" {
        fn LocalFree(memory: *mut c_void) -> *mut c_void;
    }

    let wide = |s: &str| -> Vec<u16> { OsStr::new(s).encode_wide().chain(Some(0)).collect() };
    let from = wide(from);
    let mut to = wide(to);
    let mut dacl = null_mut();
    let mut descriptor = null_mut();
    // SAFETY: the names are NUL-terminated UTF-16; the out-pointers are valid, and
    // `dacl` points into `descriptor`, which is freed only after it is used.
    unsafe {
        let err = GetNamedSecurityInfoW(
            from.as_ptr(),
            SE_FILE_OBJECT,
            DACL_SECURITY_INFORMATION,
            null_mut(),
            null_mut(),
            &mut dacl,
            null_mut(),
            &mut descriptor,
        );
        if err != 0 {
            return Err(std::io::Error::from_raw_os_error(err as i32));
        }
        let err = SetNamedSecurityInfoW(
            to.as_mut_ptr(),
            SE_FILE_OBJECT,
            DACL_SECURITY_INFORMATION | PROTECTED_DACL_SECURITY_INFORMATION,
            null_mut(),
            null_mut(),
            dacl,
            null_mut(),
        );
        LocalFree(descriptor);
        if err != 0 {
            return Err(std::io::Error::from_raw_os_error(err as i32));
        }
    }
    Ok(())
}

/// Count a finished run toward its job's failure streak, in the transaction that
/// records it, so the streak follows completion order.
///
/// When the job starts failing (its streak goes from 0 to 1) or recovers (a success
/// after failures), and a hook is set for that, the hook is queued here too: the run's
/// record and its alert commit together, whichever path recorded the run.
fn update_failure_streak(conn: &Connection, run_id: i64, status: &str) -> Result<()> {
    let before: Option<(i64, i64)> = conn
        .query_row(
            "SELECT j.id, j.failure_streak FROM jobs j JOIN runs r ON r.job_id = j.id WHERE r.id = ?1",
            params![run_id],
            |r| Ok((r.get(0)?, r.get(1)?)),
        )
        .optional()?;
    conn.execute(
        "UPDATE jobs SET failure_streak = CASE
             WHEN ?2 IN ('failed', 'timed_out', 'interrupted') THEN failure_streak + 1
             WHEN ?2 = 'success' THEN 0
             ELSE failure_streak END
         WHERE id = (SELECT job_id FROM runs WHERE id = ?1)",
        params![run_id, status],
    )?;

    let Some((job_id, before)) = before else {
        return Ok(());
    };
    let failed = matches!(status, "failed" | "timed_out" | "interrupted");
    let event = if failed && before == 0 {
        Some((HOOK_FAILURE, 1))
    } else if status == "success" && before > 0 {
        Some((HOOK_RECOVERY, before))
    } else {
        None
    };
    if let Some((event, streak)) = event {
        let hook_set: bool = conn.query_row(
            "SELECT EXISTS(SELECT 1 FROM settings WHERE key = ?1)",
            params![hook_key(event)],
            |r| r.get(0),
        )?;
        if hook_set {
            conn.execute(
                "INSERT INTO hook_runs (run_id, job_id, event, status, failure_streak, queued_at)
                 SELECT ?1, ?2, ?3, 'pending', ?4, finished_at FROM runs WHERE id = ?1",
                params![run_id, job_id, event, streak],
            )?;
        }
    }
    Ok(())
}

/// `PRAGMA user_version` from which job and run times are stored in UTC.
const UTC_TIMES: i64 = 1;

/// The times Cronlog itself writes, which were local before UTC_TIMES. `created_at`,
/// `updated_at` and `deleted_at` come from SQLite's CURRENT_TIMESTAMP, always UTC.
const LOCAL_TIME_COLUMNS: [(&str, &str); 4] = [
    ("jobs", "next_run_at"),
    ("runs", "scheduled_for"),
    ("runs", "started_at"),
    ("runs", "finished_at"),
];

fn user_version(conn: &Connection) -> Result<i64> {
    Ok(conn.query_row("PRAGMA user_version", [], |r| r.get(0))?)
}

/// 0 = none, 1 = full, 2 = incremental.
fn auto_vacuum_mode(conn: &Connection) -> Result<i64> {
    Ok(conn.query_row("PRAGMA auto_vacuum", [], |r| r.get(0))?)
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

/// Helpers other modules' tests use to set up a database.
#[cfg(test)]
pub mod tests_support {
    use super::*;

    pub fn test_db() -> Database {
        let unique = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("system time should be after epoch")
            .as_nanos();
        let path = std::env::temp_dir().join(format!("Cronlog-test-{unique}.db"));
        let db = Database::open(&path).expect("open test db");
        db.init().expect("init test db");
        db
    }

    pub fn remove(db: Database) {
        let _ = std::fs::remove_file(db.path);
    }

    /// Record a failed run (exit code 3, this error output) for `job`, adding the job if
    /// it is new. Returns the run's id.
    pub fn failed_run(db: &Database, job: &str, stderr: &str) -> i64 {
        finished_run(db, job, "failed", Some(3), stderr)
    }

    pub fn finished_run(
        db: &Database,
        job: &str,
        status: &str,
        exit_code: Option<i64>,
        stderr: &str,
    ) -> i64 {
        let when = clock::now();
        if db.get_job_by_name(job).is_err() {
            db.add_job(job, &["echo".into()], "every 1 hours", 3600, None, when)
                .expect("add job");
        }
        let job_id = db.get_job_by_name(job).expect("job").id;
        let run_id = db
            .create_run(job_id, "running", when, Some(when), "schedule")
            .expect("create run");
        db.record_result(
            run_id,
            &RunResult {
                status,
                finished_at: when,
                duration_ms: 10,
                exit_code,
                error: None,
                stdout: "",
                stderr,
                stdout_truncated: false,
                stderr_truncated: false,
                stderr_tail: None,
            },
        )
        .expect("record result");
        run_id
    }
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

    /// SQLite's plan for `sql`, one line per step.
    fn query_plan(db: &Database, sql: &str, params: impl rusqlite::Params) -> String {
        let conn = db.conn().expect("conn");
        let mut stmt = conn
            .prepare(&format!("EXPLAIN QUERY PLAN {sql}"))
            .expect("explain");
        let rows = stmt
            .query_map(params, |row| row.get::<_, String>(3))
            .expect("plan");
        rows.map(|r| r.expect("row")).collect::<Vec<_>>().join("; ")
    }

    #[test]
    fn pruning_reads_only_each_jobs_old_rows_through_an_index() {
        let db = test_db();
        let when = "2026-01-01 00:00:00";
        for (filter, index) in [
            (PRUNABLE_FOR_JOB, "idx_runs_job_scheduled"),
            // A bounded job's kept counted runs must not be in the scanned range.
            (
                PRUNABLE_SKIPS_FOR_BOUNDED_JOB,
                "idx_runs_job_trigger_status",
            ),
            (
                PRUNABLE_MANUAL_FOR_BOUNDED_JOB,
                "idx_runs_job_trigger_status",
            ),
        ] {
            let select = query_plan(
                &db,
                &format!("SELECT id {filter} LIMIT ?5"),
                params![1, when, when, 1, 500],
            );
            assert!(
                select.contains(index) && select.contains("scheduled_for<?"),
                "candidate scan is not a range on {index}: {select}"
            );
        }
        let boundary = query_plan(
            &db,
            "SELECT scheduled_for, id FROM runs WHERE job_id = ?1 AND scheduled_for IS NOT NULL
             ORDER BY scheduled_for DESC, id DESC LIMIT 1 OFFSET ?2",
            params![1, 19],
        );
        assert!(
            boundary.contains("idx_runs_job_scheduled") && !boundary.contains("TEMP B-TREE"),
            "boundary lookup sorts instead of reading the index: {boundary}"
        );
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
    fn claim_manual_run_refuses_while_a_run_is_active() {
        let db = test_db();
        let when = fixed_time();
        db.add_job(
            "dispatcher",
            &["echo".into(), "hi".into()],
            "every 1 minutes",
            3600,
            None,
            when,
        )
        .expect("add job");
        let job = db.get_job_by_name("dispatcher").expect("get job");

        // Nothing running: it starts.
        let ManualClaim::Started(first) = db
            .claim_manual_run(job.id, when, when, false)
            .expect("first claim")
        else {
            panic!("expected the first manual run to start");
        };

        // That run is still going: a second is refused and nothing is recorded.
        assert_eq!(
            db.claim_manual_run(job.id, when, when, false)
                .expect("second claim"),
            ManualClaim::Busy(first, Some(when))
        );
        assert_eq!(db.history("dispatcher", 10).expect("history").len(), 1);

        // --force starts another copy anyway.
        let forced = db
            .claim_manual_run(job.id, when, when, true)
            .expect("forced claim");
        assert!(matches!(forced, ManualClaim::Started(id) if id != first));

        let _ = fs::remove_file(db.path);
    }

    #[test]
    fn claim_manual_run_waits_for_a_scheduled_run_too() {
        let db = test_db();
        let when = fixed_time();
        db.add_job(
            "sync",
            &["echo".into(), "hi".into()],
            "every 15 minutes",
            3600,
            None,
            when,
        )
        .expect("add job");
        let job = db.get_job_by_name("sync").expect("get job");
        let scheduled = db
            .create_run(job.id, "running", when, Some(when), "schedule")
            .expect("scheduled run");

        assert_eq!(
            db.claim_manual_run(job.id, when, when, false)
                .expect("claim"),
            ManualClaim::Busy(scheduled, Some(when))
        );

        // Once it finishes, a manual run starts.
        db.create_run(job.id, "success", when, Some(when), "schedule")
            .expect("unrelated finished run");
        let conn = db.conn().expect("conn");
        conn.execute(
            "UPDATE runs SET status = 'success' WHERE id = ?1",
            params![scheduled],
        )
        .expect("finish scheduled run");
        drop(conn);
        assert!(matches!(
            db.claim_manual_run(job.id, when, when, false)
                .expect("claim after"),
            ManualClaim::Started(_)
        ));

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

    #[test]
    fn an_old_databases_local_times_are_converted_to_utc_once_with_a_backup() {
        use chrono_tz::America::New_York;
        let db = test_db();
        let local = |s: &str| NaiveDateTime::parse_from_str(s, "%Y-%m-%d %H:%M:%S%.f").unwrap();
        // What an older Cronlog in New York left: local times, user_version 0.
        db.add_job(
            "a",
            &["true".into()],
            "daily at 07:00",
            60,
            None,
            local("2026-11-17 07:00:00"),
        )
        .expect("add");
        let job = db.get_job_by_name("a").expect("job");
        let ran = db
            .create_run(
                job.id,
                "running",
                local("2026-06-14 07:00:00.5"),
                Some(local("2026-06-14 07:00:01")),
                "schedule",
            )
            .expect("run");
        db.finish_run(
            ran,
            "success",
            local("2026-06-14 07:00:03"),
            2000,
            Some(0),
            None,
        )
        .expect("finish");
        let skipped = db
            .create_run(
                job.id,
                "skipped",
                local("2026-11-01 01:30:00"),
                None,
                "schedule",
            )
            .expect("skip");
        let mut conn = db.conn().expect("conn");
        conn.pragma_update(None, "user_version", 0)
            .expect("old version");

        db.convert_times_to_utc(&mut conn, &New_York)
            .expect("convert");
        db.convert_times_to_utc(&mut conn, &New_York)
            .expect("second time is a no-op");

        assert_eq!(user_version(&conn).expect("version"), UTC_TIMES);
        assert_eq!(
            db.get_job_by_name("a").expect("job").next_run_at,
            Some(local("2026-11-17 12:00:00")) // EST, +5h
        );
        let history = db.history("a", 10).expect("history");
        let row = |id| history.iter().find(|r| r.run_id == id).expect("run");
        assert_eq!(row(ran).scheduled_for, Some(local("2026-06-14 11:00:00.5"))); // EDT, +4h
        assert_eq!(row(ran).started_at, Some(local("2026-06-14 11:00:01")));
        assert_eq!(row(ran).finished_at, Some(local("2026-06-14 11:00:03")));
        // The repeated hour is read as its first occurrence; a skip has no start time.
        assert_eq!(
            row(skipped).scheduled_for,
            Some(local("2026-11-01 05:30:00"))
        );
        assert_eq!(row(skipped).started_at, None);

        // The backup holds the times as they were, and was made only once.
        let backup = format!("{}.before-utc.bak", db.path);
        let old: String = Connection::open(&backup)
            .expect("backup")
            .query_row("SELECT next_run_at FROM jobs", [], |r| r.get(0))
            .expect("old time");
        assert_eq!(old, "2026-11-17 07:00:00");
        let copies = fs::read_dir(std::env::temp_dir())
            .expect("dir")
            .filter_map(|e| e.ok())
            .filter(|e| {
                let name = e.file_name().to_string_lossy().to_string();
                name.starts_with(&format!(
                    "{}.before-utc",
                    Path::new(&db.path).file_name().unwrap().to_string_lossy()
                ))
            })
            .count();
        assert_eq!(copies, 1);
    }

    #[test]
    fn a_new_database_starts_in_utc_without_a_backup() {
        let db = test_db();
        assert_eq!(
            user_version(&db.conn().expect("conn")).expect("version"),
            UTC_TIMES
        );
        assert!(!Path::new(&format!("{}.before-utc.bak", db.path)).exists());
    }

    #[test]
    fn a_backup_never_replaces_an_earlier_one_and_is_as_private_as_the_database() {
        let db = test_db();
        db.add_job(
            "a",
            &["true".into()],
            "every 1 hours",
            60,
            None,
            fixed_time(),
        )
        .expect("add");
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            // Group-readable: the backup must still be owner-only.
            fs::set_permissions(&db.path, fs::Permissions::from_mode(0o640)).expect("chmod");
        }
        #[cfg(windows)]
        restrict_to_current_user(&db.path);
        let first = format!("{}.before-utc.bak", db.path);
        fs::write(&first, b"an earlier backup").expect("earlier backup");

        let mut names = Vec::new();
        for _ in 0..3 {
            names.push(db.backup_before_utc().expect("backup"));
        }
        assert_eq!(fs::read(&first).expect("kept"), b"an earlier backup");
        names.sort();
        names.dedup();
        assert_eq!(names.len(), 3, "each backup gets its own name: {names:?}");
        for name in &names {
            let jobs: i64 = Connection::open(name)
                .expect("open backup")
                .query_row("SELECT COUNT(*) FROM jobs", [], |r| r.get(0))
                .expect("count");
            assert_eq!(jobs, 1, "{name}");
            #[cfg(unix)]
            {
                use std::os::unix::fs::PermissionsExt;
                let mode = fs::metadata(name).expect("meta").permissions().mode() & 0o777;
                assert_eq!(mode, 0o600, "{name}");
            }
            #[cfg(windows)]
            assert_eq!(access_list(name), access_list(&db.path), "{name}");
        }
    }

    /// The file's access rules as icacls lists them, without the path.
    #[cfg(windows)]
    fn access_list(path: &str) -> Vec<String> {
        let out = std::process::Command::new("icacls")
            .arg(path)
            .output()
            .expect("icacls");
        String::from_utf8_lossy(&out.stdout)
            .lines()
            .map(|line| line.replacen(path, "", 1).trim().to_string())
            .filter(|line| line.contains(':') && !line.starts_with("Successfully"))
            .collect()
    }

    /// Drop the file's inherited rules and allow only the current user, so it is
    /// stricter than a new file in the same folder.
    #[cfg(windows)]
    fn restrict_to_current_user(path: &str) {
        let user = std::env::var("USERNAME").expect("USERNAME");
        let icacls = |args: &[&str]| {
            let out = std::process::Command::new("icacls")
                .arg(path)
                .args(args)
                .output()
                .expect("icacls");
            assert!(out.status.success(), "icacls {args:?}: {out:?}");
        };
        icacls(&["/inheritance:r", "/grant:r", &format!("{user}:F")]);
        // Some machines (CI runners) also give new files explicit SYSTEM and
        // Administrators entries; remove them by SID, which no locale renames.
        icacls(&["/remove:g", "*S-1-5-18", "*S-1-5-32-544"]);
        let rules = access_list(path);
        assert_eq!(rules.len(), 1, "{rules:?}");

        let fresh = format!("{path}.fresh");
        fs::write(&fresh, b"").expect("fresh file");
        let folder_rules = access_list(&fresh);
        let _ = fs::remove_file(&fresh);
        assert_ne!(
            rules, folder_rules,
            "the test needs rules the folder would not give"
        );
    }

    #[test]
    fn a_process_opening_an_old_database_while_another_converts_it_waits() {
        use chrono_tz::America::New_York;
        let db = test_db();
        db.add_job(
            "a",
            &["true".into()],
            "every 1 hours",
            60,
            None,
            fixed_time(),
        )
        .expect("add");
        db.conn()
            .expect("conn")
            .pragma_update(None, "user_version", 0)
            .expect("old version");
        let workers: Vec<_> = (0..4)
            .map(|_| {
                let db = db.clone();
                std::thread::spawn(move || {
                    let mut conn = db.conn().expect("conn");
                    db.convert_times_to_utc(&mut conn, &New_York)
                })
            })
            .collect();
        for worker in workers {
            worker.join().expect("thread").expect("convert");
        }
        // Converted once (06-11 12:00 EDT is 16:00 UTC), with one backup.
        assert_eq!(
            db.get_job_by_name("a").expect("job").next_run_at,
            Some(fixed_time() + chrono::Duration::hours(4))
        );
        let prefix = format!(
            "{}.before-utc",
            Path::new(&db.path).file_name().unwrap().to_string_lossy()
        );
        let backups = fs::read_dir(std::env::temp_dir())
            .expect("dir")
            .filter_map(|e| e.ok())
            .filter(|e| e.file_name().to_string_lossy().starts_with(&prefix))
            .count();
        assert_eq!(backups, 1);
    }
}
