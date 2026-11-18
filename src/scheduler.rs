use crate::db::{
    Database, Heartbeat, HistoryRow, Job, ManualClaim, PruneReport, RunClaim, RunResult,
    RECLAIM_CHUNK_PAGES,
};
use crate::error::{CronlogError, Result};
use crate::{clock, hooks, runner, schedule};
use std::panic::{self, AssertUnwindSafe};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::{Duration, Instant};

/// Claimed runs whose worker failed before recording a result, with the reason. The daemon
/// marks them `interrupted`, retrying each tick until the database accepts it.
///
/// Without this, a run left `running` by a failed worker made every later slot of that job
/// `skipped` until the daemon restarted (stale-run recovery only runs at startup).
pub type Orphans = Arc<Mutex<Vec<(i64, String)>>>;

/// How much run history to keep: runs scheduled more than `max_age` ago are deleted,
/// except each job's newest `keep_last` runs.
#[derive(Debug, Clone, Copy)]
pub struct Retention {
    pub max_age: chrono::Duration,
    pub keep_last: i64,
}

impl Retention {
    pub fn cutoff(&self, now: chrono::NaiveDateTime) -> chrono::NaiveDateTime {
        // A retention longer than the calendar allows keeps everything.
        now.checked_sub_signed(self.max_age)
            .unwrap_or(chrono::NaiveDateTime::MIN)
    }
}

/// Runs deleted per transaction. Small enough that a job recording its result never
/// waits long for the write lock.
pub const PRUNE_BATCH: i64 = 500;
/// How often the daemon prunes once it has caught up.
const PRUNE_EVERY: Duration = Duration::from_secs(24 * 60 * 60);
/// After a failed prune, try again this much later rather than every tick.
const PRUNE_RETRY: Duration = Duration::from_secs(60 * 60);

/// Delete all history `retention` allows, batch by batch. Returns the totals.
pub fn prune_history(
    db: &Database,
    retention: Retention,
    now: chrono::NaiveDateTime,
) -> Result<PruneReport> {
    let cutoff = retention.cutoff(now);
    let mut total = PruneReport::default();
    loop {
        let batch = db.prune_batch(cutoff, retention.keep_last, PRUNE_BATCH)?;
        total.runs += batch.runs;
        total.logs += batch.logs;
        if (batch.runs as i64) < PRUNE_BATCH {
            return Ok(total);
        }
    }
}

/// The daemon's pruning. Every step is bounded, one per tick, so a large backlog never
/// stalls dispatch or holds the write lock long: delete one batch per tick until caught
/// up, then hand back one chunk of freed pages per tick, then wait PRUNE_EVERY.
struct Pruner {
    retention: Retention,
    next: Instant,
    pass: PruneReport,
    reclaiming: bool,
}

impl Pruner {
    fn new(retention: Retention) -> Self {
        Self {
            retention,
            next: Instant::now(),
            pass: PruneReport::default(),
            reclaiming: false,
        }
    }

    fn tick(&mut self, db: &Database, now: chrono::NaiveDateTime) {
        if Instant::now() < self.next {
            return;
        }
        if self.reclaiming {
            match db.reclaim_some(RECLAIM_CHUNK_PAGES) {
                Ok(0) => self.finish_pass(),
                Ok(_) => {} // more pages to hand back; next tick
                Err(err) => {
                    eprintln!("Could not reclaim space after pruning: {err}");
                    self.finish_pass();
                }
            }
            return;
        }
        let cutoff = self.retention.cutoff(now);
        match db.prune_batch(cutoff, self.retention.keep_last, PRUNE_BATCH) {
            Ok(batch) => {
                self.pass.runs += batch.runs;
                self.pass.logs += batch.logs;
                if (batch.runs as i64) == PRUNE_BATCH {
                    return; // more to do; next tick
                }
                if self.pass.runs > 0 {
                    println!(
                        "Pruned {} runs scheduled before {} (kept each job's newest {})",
                        self.pass.runs,
                        clock::display(cutoff),
                        self.retention.keep_last
                    );
                }
                // Reclaim even when nothing was deleted this pass: pages freed before a
                // restart or a failed reclaim would otherwise never be handed back. With
                // no free pages this is one quick step.
                self.reclaiming = true;
            }
            Err(err) => {
                eprintln!("Could not prune run history: {err}; retrying in 1h");
                self.pass = PruneReport::default();
                self.next = Instant::now() + PRUNE_RETRY;
            }
        }
    }

    fn finish_pass(&mut self) {
        self.pass = PruneReport::default();
        self.reclaiming = false;
        self.next = Instant::now() + PRUNE_EVERY;
    }
}

/// Runs queued hooks on a thread of its own, one batch at a time, so a slow hook
/// never holds up the schedule.
struct HookRunner {
    busy: Arc<AtomicBool>,
}

impl HookRunner {
    fn new(db: &Database) -> Self {
        // A hook still `running` from before this daemon started was cut off.
        let now = clock::now();
        let cutoff = now
            - chrono::Duration::from_std(hooks::HOOK_TIMEOUT * 2)
                .unwrap_or(chrono::Duration::zero());
        match db.interrupt_stale_hooks(cutoff, now) {
            Ok(0) => {}
            Ok(n) => println!("Marked {n} hook run(s) left running as interrupted"),
            Err(err) => eprintln!("Could not check for interrupted hooks: {err}"),
        }
        Self {
            busy: Arc::new(AtomicBool::new(false)),
        }
    }

    fn tick(&self, db: &Database) {
        if self.busy.load(Ordering::SeqCst) {
            return;
        }
        match db.has_pending_hooks() {
            Ok(true) => {}
            Ok(false) => return,
            Err(err) => {
                eprintln!("Could not check for queued hooks: {err}");
                return;
            }
        }
        self.busy.store(true, Ordering::SeqCst);
        let busy = Arc::clone(&self.busy);
        let db = db.clone();
        thread::spawn(move || {
            let outcome = panic::catch_unwind(AssertUnwindSafe(|| hooks::dispatch_pending(&db)));
            match outcome {
                Ok(Ok(_)) => {}
                Ok(Err(err)) => eprintln!("Could not run hooks: {err}"),
                Err(_) => eprintln!("Hook thread panicked"),
            }
            busy.store(false, Ordering::SeqCst);
        });
    }
}

/// How often the daemon records that it is alive, for `cronlog health`.
pub const HEARTBEAT_EVERY: Duration = Duration::from_secs(30);

/// Writes the daemon's heartbeat at most every HEARTBEAT_EVERY. A failed write is
/// reported and retried next tick; it never stops the daemon.
struct HeartbeatWriter {
    heartbeat: Heartbeat,
    last_written: Option<Instant>,
}

impl HeartbeatWriter {
    fn new(started_at: chrono::NaiveDateTime, poll_seconds: u64) -> Self {
        Self {
            heartbeat: Heartbeat {
                pid: i64::from(std::process::id()),
                started_at,
                last_tick: started_at,
                poll_seconds: i64::try_from(poll_seconds).unwrap_or(i64::MAX),
            },
            last_written: None,
        }
    }

    fn tick(&mut self, db: &Database, now: chrono::NaiveDateTime) {
        if self
            .last_written
            .is_some_and(|last| last.elapsed() < HEARTBEAT_EVERY)
        {
            return;
        }
        self.heartbeat.last_tick = now;
        match db.record_heartbeat(&self.heartbeat) {
            Ok(()) => self.last_written = Some(Instant::now()),
            Err(err) => eprintln!("Could not record the daemon heartbeat: {err}"),
        }
    }
}

pub fn run_daemon(db: &Database, poll_seconds: u64, retention: Option<Retention>) -> Result<()> {
    recover_interrupted_runs(db)?;
    let orphans = Orphans::default();
    let mut pruner = retention.map(Pruner::new);
    let mut heartbeat = HeartbeatWriter::new(clock::now(), poll_seconds.max(1));
    let hook_runner = HookRunner::new(db);

    loop {
        recover_orphaned_runs(db, &orphans);
        let now = clock::now();
        heartbeat.tick(db, now);
        // Handles are dropped: runs finish on their own threads and record their own results.
        dispatch_due_jobs(db, now, &orphans)?;
        hook_runner.tick(db);
        if let Some(pruner) = pruner.as_mut() {
            pruner.tick(db, now);
        }
        thread::sleep(Duration::from_secs(poll_seconds.max(1)));
    }
}

/// Claim every due job and start each claimed run on its own thread.
///
/// Runs used to execute inline, one at a time: a 20-minute job held every other job until
/// it finished, and their slots passed unrecorded. Overlap protection is unchanged: the
/// claim transaction records a `skipped` run while the same job is still running.
pub fn dispatch_due_jobs(
    db: &Database,
    now: chrono::NaiveDateTime,
    orphans: &Orphans,
) -> Result<Vec<thread::JoinHandle<()>>> {
    let mut handles = Vec::new();
    for job in db.get_due_jobs(now)? {
        let scheduled_for = job.next_run_at.unwrap_or(now);
        let next_run_at = next_run_after_scheduled_time(&job, scheduled_for, now)?;

        match db.claim_scheduled_run(job.id, scheduled_for, next_run_at, clock::now())? {
            RunClaim::Claimed(run_id) => {
                let db = db.clone();
                let orphans = Arc::clone(orphans);
                handles.push(thread::spawn(move || {
                    let outcome = panic::catch_unwind(AssertUnwindSafe(|| {
                        run_claimed_job(&db, &job, run_id, scheduled_for, "schedule")
                    }));
                    let reason = match outcome {
                        Ok(Ok(())) => return,
                        Ok(Err(err)) => err.to_string(),
                        Err(_) => "worker thread panicked".to_string(),
                    };
                    eprintln!(
                        "Could not record job '{}' run #{}: {reason}",
                        job.name, run_id
                    );
                    orphans
                        .lock()
                        .unwrap_or_else(|e| e.into_inner())
                        .push((run_id, reason));
                }));
            }
            RunClaim::Skipped(_) => {
                println!(
                    "Skipped '{}' because a previous run is still active",
                    job.name
                );
            }
            RunClaim::NotDue => {}
        }
    }
    Ok(handles)
}

/// Enable a job. If its next run passed while it was disabled, schedule the next future
/// slot instead, so a `daily at 07:00` job enabled in the afternoon waits for tomorrow's
/// 07:00 rather than firing at once. Interval jobs keep their phase: an `every 10
/// minutes` job whose 12:00 slot passed, enabled at 12:35, next runs at 12:40.
/// Enabling and the new timestamp are written in one transaction. Returns the next run.
pub fn enable_job(
    db: &Database,
    name: &str,
    now: chrono::NaiveDateTime,
) -> Result<chrono::NaiveDateTime> {
    db.enable_job(name, |schedule, enabled, current_next| match current_next {
        // An already-enabled job is left as it is: its due slot still runs.
        Some(next) if enabled || next > now => Ok(next),
        Some(previous) => first_slot_after(schedule, previous, now),
        None => schedule::parse_schedule(schedule)?.next_after(now),
    })
}

/// Mark runs abandoned by failed workers as `interrupted`. Any that the database still
/// refuses stay queued for the next tick.
pub fn recover_orphaned_runs(db: &Database, orphans: &Orphans) {
    let pending = std::mem::take(&mut *orphans.lock().unwrap_or_else(|e| e.into_inner()));
    let mut still_pending = Vec::new();
    for (run_id, reason) in pending {
        let message = format!("cronlog could not record this run: {reason}");
        match db.interrupt_run(run_id, clock::now(), &message) {
            Ok(true) => println!("Marked run #{run_id} as interrupted: {message}"),
            Ok(false) => {} // it was recorded after all
            Err(err) => {
                eprintln!("Still cannot mark run #{run_id} as interrupted: {err}");
                still_pending.push((run_id, reason));
            }
        }
    }
    orphans
        .lock()
        .unwrap_or_else(|e| e.into_inner())
        .extend(still_pending);
}

fn recover_interrupted_runs(db: &Database) -> Result<()> {
    let message = "daemon restarted while run was active";
    let interrupted = db.interrupt_stale_running_runs(clock::now(), message)?;
    for run in interrupted {
        println!(
            "Marked job '{}' run #{} as interrupted: {}",
            run.job_name, run.run_id, message
        );
    }
    Ok(())
}

/// Run a job now, by hand. Refuses while the job already has a run in progress,
/// scheduled or manual, unless `force` is set.
pub fn run_manual(
    db: &Database,
    job: &Job,
    scheduled_for: chrono::NaiveDateTime,
    force: bool,
) -> Result<()> {
    let trigger_type = "manual";
    let previous = db.history(&job.name, 1)?.into_iter().next();
    let started_at = clock::now();
    let run_id = match db.claim_manual_run(job.id, scheduled_for, started_at, force)? {
        ManualClaim::Started(run_id) => run_id,
        ManualClaim::Busy(active, started) => {
            let since = started
                .map(|t| format!(", started {}", clock::display(t)))
                .unwrap_or_default();
            return Err(CronlogError::AlreadyRunning(format!(
                "job '{}' is already running (run #{active}{since}); \
                 use --force to start another copy",
                job.name
            )));
        }
    };
    println!(
        "Starting job '{}' run #{}: {}",
        job.name,
        run_id,
        job.command.join(" ")
    );

    let result = execute_claimed_run(db, job, run_id, scheduled_for, trigger_type, previous);
    // Run the hook this run queued (if any) before exiting; a daemon would also
    // pick it up, and the claim keeps the two from both running it.
    if let Err(err) = hooks::dispatch_pending(db) {
        eprintln!("Could not run hooks: {err}");
    }
    result
}

fn run_claimed_job(
    db: &Database,
    job: &Job,
    run_id: i64,
    scheduled_for: chrono::NaiveDateTime,
    trigger_type: &str,
) -> Result<()> {
    let previous = db
        .history(&job.name, 2)?
        .into_iter()
        .find(|row| row.run_id != run_id);
    println!(
        "Starting job '{}' run #{}: {}",
        job.name,
        run_id,
        job.command.join(" ")
    );

    execute_claimed_run(db, job, run_id, scheduled_for, trigger_type, previous)
}

fn execute_claimed_run(
    db: &Database,
    job: &Job,
    run_id: i64,
    scheduled_for: chrono::NaiveDateTime,
    trigger_type: &str,
    previous: Option<HistoryRow>,
) -> Result<()> {
    let context = runner::ExecuteContext {
        run_id,
        job_name: job.name.clone(),
        // Local wall-clock time, as before times were stored in UTC.
        scheduled_for: clock::to_local(scheduled_for).naive_local().to_string(),
        trigger_type: trigger_type.to_string(),
        previous_run_id: previous.as_ref().map(|row| row.run_id),
        previous_status: previous.as_ref().map(|row| row.status.clone()),
    };

    match runner::execute(job, &context) {
        Ok(out) => {
            db.record_result(
                run_id,
                &RunResult {
                    status: &out.status,
                    finished_at: clock::now(),
                    duration_ms: out.duration_ms,
                    exit_code: out.exit_code,
                    error: out.error.as_deref(),
                    stdout: &out.stdout,
                    stderr: &out.stderr,
                    stdout_truncated: out.stdout_truncated,
                    stderr_truncated: out.stderr_truncated,
                },
            )?;
            println!(
                "Finished job '{}' run #{}: {}",
                job.name, run_id, out.status
            );
            disable_if_max_runs_reached(db, job.id)?;
        }
        Err(err) => {
            let message = err.to_string();
            db.record_result(
                run_id,
                &RunResult {
                    status: "failed",
                    finished_at: clock::now(),
                    duration_ms: 0,
                    exit_code: None,
                    error: Some(&message),
                    stdout: "",
                    stderr: &message,
                    stdout_truncated: false,
                    stderr_truncated: false,
                },
            )?;
            println!("Failed job '{}' run #{}: {}", job.name, run_id, message);
            disable_if_max_runs_reached(db, job.id)?;
        }
    }

    Ok(())
}

fn disable_if_max_runs_reached(db: &Database, job_id: i64) -> Result<()> {
    if let Some((name, max_runs)) = db.disable_if_max_runs_reached(job_id)? {
        println!("Disabled '{name}' after {max_runs} scheduled runs");
    }
    Ok(())
}

fn next_run_after_scheduled_time(
    job: &Job,
    scheduled_for: chrono::NaiveDateTime,
    now: chrono::NaiveDateTime,
) -> Result<chrono::NaiveDateTime> {
    first_slot_after(&job.schedule, scheduled_for, now)
}

/// The first slot of `schedule` after `now`, stepping from `previous` so intervals
/// keep their phase.
fn first_slot_after(
    schedule: &str,
    previous: chrono::NaiveDateTime,
    now: chrono::NaiveDateTime,
) -> Result<chrono::NaiveDateTime> {
    schedule::parse_schedule(schedule)?.first_after(previous, now)
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::{Local, NaiveDate};

    fn job(schedule: &str) -> Job {
        Job {
            id: 1,
            name: "heartbeat".into(),
            command: vec!["echo".into(), "alive".into()],
            schedule: schedule.into(),
            timeout_seconds: 3600,
            max_runs: None,
            next_run_at: None,
        }
    }

    fn at(hour: u32, minute: u32, second: u32) -> chrono::NaiveDateTime {
        NaiveDate::from_ymd_opt(2026, 6, 14)
            .expect("valid date")
            .and_hms_opt(hour, minute, second)
            .expect("valid time")
    }

    #[test]
    fn next_run_is_anchored_to_scheduled_time_not_finish_time() {
        let scheduled_for = at(12, 0, 0);
        let now_after_long_run = at(12, 5, 0);

        let next = next_run_after_scheduled_time(
            &job("every 10 minutes"),
            scheduled_for,
            now_after_long_run,
        )
        .expect("next run");

        assert_eq!(next, at(12, 10, 0));
    }

    #[test]
    fn next_run_skips_missed_intervals_without_drifting() {
        let scheduled_for = at(12, 0, 0);
        let now_after_outage = at(12, 35, 0);

        let next = next_run_after_scheduled_time(
            &job("every 10 minutes"),
            scheduled_for,
            now_after_outage,
        )
        .expect("next run");

        assert_eq!(next, at(12, 40, 0));
    }

    fn test_db() -> Database {
        let unique = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("system time should be after epoch")
            .as_nanos();
        let path = std::env::temp_dir().join(format!("Cronlog-sched-test-{unique}.db"));
        let db = Database::open(&path).expect("open test db");
        db.init().expect("init test db");
        db
    }

    #[cfg(unix)]
    fn slow_command() -> Vec<String> {
        vec!["sleep".into(), "3".into()]
    }

    #[cfg(windows)]
    fn slow_command() -> Vec<String> {
        vec!["ping".into(), "-n".into(), "4".into(), "127.0.0.1".into()]
    }

    #[cfg(unix)]
    fn quick_command() -> Vec<String> {
        vec!["true".into()]
    }

    #[cfg(windows)]
    fn quick_command() -> Vec<String> {
        vec!["cmd".into(), "/C".into(), "exit 0".into()]
    }

    fn last_status(db: &Database, name: &str) -> String {
        db.history(name, 1).expect("history")[0].status.clone()
    }

    /// A finished run with a logs row, as the daemon records one.
    fn finished_run(
        db: &Database,
        job_id: i64,
        status: &str,
        trigger: &str,
        when: chrono::NaiveDateTime,
    ) -> i64 {
        let id = db
            .create_run(job_id, "running", when, Some(when), trigger)
            .expect("create run");
        db.finish_run(id, status, when, 0, Some(0), None)
            .expect("finish run");
        db.insert_logs(id, "out", "", false, false).expect("logs");
        id
    }

    fn days_before(now: chrono::NaiveDateTime, days: i64) -> chrono::NaiveDateTime {
        now - chrono::Duration::days(days)
    }

    #[test]
    fn pruning_keeps_recent_runs_each_jobs_newest_and_running_runs() {
        let db = test_db();
        let now = at(12, 0, 0);
        db.add_job("a", &quick_command(), "every 1 hours", 60, None, now)
            .expect("add");
        let job = db.get_job_by_name("a").expect("job");
        for _ in 0..30 {
            finished_run(&db, job.id, "success", "schedule", days_before(now, 10));
        }
        let old_running = db
            .create_run(
                job.id,
                "running",
                days_before(now, 10),
                Some(days_before(now, 10)),
                "schedule",
            )
            .expect("running run");
        let recent = finished_run(&db, job.id, "success", "schedule", now);
        finished_run(&db, job.id, "failed", "schedule", now);

        // Keep 1 day and the newest 5: the 2 recent runs, the running one and 2 old ones.
        let retention = Retention {
            max_age: chrono::Duration::days(1),
            keep_last: 5,
        };
        assert_eq!(
            db.count_prunable(retention.cutoff(now), 5).expect("count"),
            28
        );

        let pruned = prune_history(&db, retention, now).expect("prune");
        assert_eq!(pruned, PruneReport { runs: 28, logs: 28 });

        let left = db.history("a", 100).expect("history");
        assert_eq!(left.len(), 5);
        assert!(left
            .iter()
            .any(|r| r.run_id == old_running && r.status == "running"));
        assert!(left.iter().any(|r| r.run_id == recent));
        assert_eq!(
            db.count_prunable(retention.cutoff(now), 5).expect("count"),
            0
        );
    }

    #[test]
    fn pruning_goes_in_batches() {
        let db = test_db();
        let now = at(12, 0, 0);
        db.add_job("a", &quick_command(), "every 1 hours", 60, None, now)
            .expect("add");
        let job = db.get_job_by_name("a").expect("job");
        for _ in 0..25 {
            finished_run(&db, job.id, "success", "schedule", days_before(now, 10));
        }
        let cutoff = days_before(now, 1);
        let sizes: Vec<usize> = (0..4)
            .map(|_| db.prune_batch(cutoff, 1, 10).expect("batch").runs)
            .collect();
        assert_eq!(sizes, vec![10, 10, 4, 0]);
    }

    #[test]
    fn pruning_keeps_runs_counted_toward_max_runs() {
        let db = test_db();
        let now = at(12, 0, 0);
        db.add_job(
            "bounded",
            &quick_command(),
            "every 1 hours",
            60,
            Some(3),
            now,
        )
        .expect("add");
        let job = db.get_job_by_name("bounded").expect("job");
        let old = days_before(now, 10);
        let counted: Vec<i64> = (0..3)
            .map(|_| finished_run(&db, job.id, "success", "schedule", old))
            .collect();
        for _ in 0..4 {
            finished_run(&db, job.id, "skipped", "schedule", old);
        }
        for _ in 0..2 {
            finished_run(&db, job.id, "success", "manual", old);
        }
        let remaining = |db: &Database| db.list_jobs().expect("list")[0].remaining_runs;
        assert_eq!(remaining(&db), Some(0));

        // The skipped and manual runs go (bar the newest); the 3 counted runs stay, so
        // the job still has no runs left instead of 3 more.
        let retention = Retention {
            max_age: chrono::Duration::days(1),
            keep_last: 1,
        };
        let pruned = prune_history(&db, retention, now).expect("prune");
        assert_eq!(pruned.runs, 5);
        let left: Vec<i64> = db
            .history("bounded", 100)
            .expect("history")
            .iter()
            .map(|r| r.run_id)
            .collect();
        assert!(counted.iter().all(|id| left.contains(id)));
        assert_eq!(remaining(&db), Some(0));
    }

    #[test]
    fn a_retention_longer_than_the_calendar_keeps_everything() {
        let db = test_db();
        let now = at(12, 0, 0);
        db.add_job("a", &quick_command(), "every 1 hours", 60, None, now)
            .expect("add");
        let job = db.get_job_by_name("a").expect("job");
        finished_run(&db, job.id, "success", "schedule", days_before(now, 10));
        finished_run(&db, job.id, "success", "schedule", days_before(now, 10));
        let retention = Retention {
            max_age: chrono::Duration::MAX,
            keep_last: 1,
        };
        assert_eq!(prune_history(&db, retention, now).expect("prune").runs, 0);
    }

    #[test]
    fn pruning_shrinks_a_new_database() {
        let db = test_db();
        let now = at(12, 0, 0);
        db.add_job("big", &quick_command(), "every 1 hours", 60, None, now)
            .expect("add");
        let job = db.get_job_by_name("big").expect("job");
        let output = "x".repeat(20_000);
        for _ in 0..50 {
            let id = db
                .create_run(job.id, "running", days_before(now, 10), None, "schedule")
                .expect("run");
            db.finish_run(id, "success", days_before(now, 10), 0, Some(0), None)
                .expect("finish");
            db.insert_logs(id, &output, "", false, false).expect("logs");
        }
        let before = db.size_bytes().expect("size");
        let retention = Retention {
            max_age: chrono::Duration::days(1),
            keep_last: 1,
        };
        assert_eq!(prune_history(&db, retention, now).expect("prune").runs, 49);
        // A bounded step hands back only part of it...
        let left = db.reclaim_some(10).expect("reclaim chunk");
        assert!(left > 0, "a 10-page step should leave free pages");
        // ...and the rest comes back chunk by chunk.
        db.reclaim_space(false).expect("reclaim");
        assert_eq!(db.reclaim_some(10).expect("reclaim chunk"), 0);
        let after = db.size_bytes().expect("size");
        assert!(
            after < before / 4,
            "expected most of {before} bytes back, still {after}"
        );
    }

    /// The bug this guards against: times were local wall-clock time, so when New York
    /// fell back from 02:00 to 01:00 on 2026-11-01 an every-5-minutes job due at "02:00"
    /// sat idle for 65 minutes. Replay that night one real minute at a time.
    #[test]
    fn an_interval_job_keeps_running_through_the_fall_back_hour() {
        let db = test_db();
        let start = NaiveDate::from_ymd_opt(2026, 11, 1)
            .unwrap()
            .and_hms_opt(5, 30, 0)
            .unwrap(); // 01:30 EDT
        db.add_job("tick", &quick_command(), "every 5 minutes", 60, None, start)
            .expect("add");
        let orphans = Orphans::default();
        let mut ran = Vec::new();
        for minute in 0..=120 {
            let now = start + chrono::Duration::minutes(minute);
            let handles = dispatch_due_jobs(&db, now, &orphans).expect("dispatch");
            if !handles.is_empty() {
                ran.push(now);
            }
            for handle in handles {
                handle.join().expect("worker");
            }
        }
        let gaps: Vec<i64> = ran
            .windows(2)
            .map(|w| (w[1] - w[0]).num_minutes())
            .collect();
        assert_eq!(ran.len(), 25, "gaps (min): {gaps:?}");
        assert!(gaps.iter().all(|gap| *gap == 5), "gaps (min): {gaps:?}");
    }

    #[test]
    fn the_daemon_reclaims_pages_left_from_an_earlier_pass() {
        let db = test_db();
        let now = at(12, 0, 0);
        db.add_job("big", &quick_command(), "every 1 hours", 60, None, now)
            .expect("add");
        let job = db.get_job_by_name("big").expect("job");
        let output = "x".repeat(20_000);
        for _ in 0..30 {
            let id = db
                .create_run(job.id, "running", days_before(now, 10), None, "schedule")
                .expect("run");
            db.finish_run(id, "success", days_before(now, 10), 0, Some(0), None)
                .expect("finish");
            db.insert_logs(id, &output, "", false, false).expect("logs");
        }
        let retention = Retention {
            max_age: chrono::Duration::days(1),
            keep_last: 1,
        };
        // Deleted, but the daemon stopped before handing the pages back.
        prune_history(&db, retention, now).expect("prune");
        let before = db.size_bytes().expect("size");

        // A fresh daemon finds nothing to delete, and still reclaims the leftover pages.
        let mut pruner = Pruner::new(retention);
        for _ in 0..10 {
            pruner.tick(&db, now);
        }
        assert!(!pruner.reclaiming, "pass should have finished");
        assert!(
            db.size_bytes().expect("size") < before / 4,
            "leftover pages were not reclaimed"
        );
    }

    #[test]
    fn a_result_is_recorded_whole_or_not_at_all() {
        let db = test_db();
        let now = at(12, 0, 0);
        db.add_job("a", &quick_command(), "every 1 hours", 60, None, now)
            .expect("add");
        let job = db.get_job_by_name("a").expect("job");
        let id = db
            .create_run(job.id, "running", now, Some(now), "schedule")
            .expect("run");
        let result = RunResult {
            status: "success",
            finished_at: now,
            duration_ms: 5,
            exit_code: Some(0),
            error: None,
            stdout: "hello",
            stderr: "",
            stdout_truncated: false,
            stderr_truncated: false,
        };
        db.record_result(id, &result).expect("record");
        assert_eq!(last_status(&db, "a"), "success");
        assert_eq!(
            db.logs_for_run(id).expect("logs").stdout.as_deref(),
            Some("hello")
        );

        // A run that is gone (as if pruned) is an error, and no orphan log row is left.
        assert!(db.record_result(id + 1000, &result).is_err());
        assert!(db.logs_for_run(id + 1000).is_err());
    }

    #[test]
    fn new_databases_trim_freed_space_and_vacuum_converts_old_ones() {
        let db = test_db();
        assert_eq!(
            db.auto_vacuum().expect("mode"),
            2,
            "new database is incremental"
        );
        db.reclaim_space(false).expect("incremental vacuum");

        // A database whose tables predate this release stays as it was until --vacuum.
        let unique = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("time")
            .as_nanos();
        let path = std::env::temp_dir().join(format!("Cronlog-legacy-{unique}.db"));
        rusqlite::Connection::open(&path)
            .expect("open")
            .execute_batch("CREATE TABLE legacy (x INTEGER);")
            .expect("legacy table");
        let legacy = Database::open(&path).expect("open");
        legacy.init().expect("init");
        assert_eq!(legacy.auto_vacuum().expect("mode"), 0);
        legacy.reclaim_space(false).expect("no-op");
        legacy.reclaim_space(true).expect("vacuum");
        assert_eq!(legacy.auto_vacuum().expect("mode"), 2);
    }

    #[test]
    fn a_slow_job_does_not_delay_other_jobs() {
        let db = test_db();
        let now = clock::now();
        db.add_job("slow", &slow_command(), "every 1 hours", 60, None, now)
            .expect("add slow");
        db.add_job("quick", &quick_command(), "every 1 hours", 60, None, now)
            .expect("add quick");

        let handles = dispatch_due_jobs(&db, now, &Orphans::default()).expect("dispatch");
        assert_eq!(handles.len(), 2);
        thread::sleep(Duration::from_millis(1500));
        assert_eq!(last_status(&db, "quick"), "success");
        assert_eq!(last_status(&db, "slow"), "running");

        for handle in handles {
            handle.join().expect("run thread");
        }
        assert_eq!(last_status(&db, "slow"), "success");
    }

    #[test]
    fn a_job_still_running_at_its_next_slot_is_recorded_as_skipped() {
        let db = test_db();
        let now = clock::now();
        db.add_job("slow", &slow_command(), "every 1 seconds", 60, None, now)
            .expect("add slow");

        let orphans = Orphans::default();
        let first = dispatch_due_jobs(&db, now, &orphans).expect("first dispatch");
        thread::sleep(Duration::from_millis(300));
        let second = dispatch_due_jobs(&db, now + chrono::Duration::seconds(1), &orphans)
            .expect("second dispatch");
        assert!(
            second.is_empty(),
            "the overlapping slot must not start a run"
        );
        assert_eq!(last_status(&db, "slow"), "skipped");

        for handle in first {
            handle.join().expect("run thread");
        }
    }

    #[test]
    fn a_run_its_worker_could_not_record_is_interrupted_and_the_job_unblocked() {
        let db = test_db();
        let now = clock::now();
        db.add_job("quick", &quick_command(), "every 1 seconds", 60, None, now)
            .expect("add quick");
        let job = db.get_job_by_name("quick").expect("job");
        // A claimed run whose worker failed before finish_run: still `running`.
        let run_id = db
            .create_run(job.id, "running", now, Some(now), "schedule")
            .expect("run");
        let orphans = Orphans::default();
        orphans
            .lock()
            .unwrap()
            .push((run_id, "database is locked".into()));

        // While it is `running`, the next slot is skipped.
        assert!(dispatch_due_jobs(&db, now, &Orphans::default())
            .expect("dispatch")
            .is_empty());
        assert_eq!(last_status(&db, "quick"), "skipped");

        recover_orphaned_runs(&db, &orphans);
        assert!(orphans.lock().unwrap().is_empty());
        let interrupted = db
            .history("quick", 10)
            .expect("history")
            .into_iter()
            .find(|row| row.run_id == run_id)
            .expect("orphaned run");
        assert_eq!(interrupted.status, "interrupted");

        // The next slot runs again.
        let handles =
            dispatch_due_jobs(&db, now + chrono::Duration::seconds(2), &orphans).expect("dispatch");
        assert_eq!(handles.len(), 1);
        for handle in handles {
            handle.join().expect("run thread");
        }
        assert_eq!(last_status(&db, "quick"), "success");
    }

    /// `at` on this machine's clock, as the UTC time Cronlog stores. A daily schedule
    /// reads its time of day in the local zone.
    fn local_at(day: u32, hour: u32, minute: u32) -> chrono::NaiveDateTime {
        let local = NaiveDate::from_ymd_opt(2026, 6, day)
            .expect("valid date")
            .and_hms_opt(hour, minute, 0)
            .expect("valid time");
        clock::local_to_utc(&Local, local)
    }

    #[test]
    fn enabling_a_job_whose_slot_passed_waits_for_the_next_slot() {
        let db = test_db();
        let seven = local_at(14, 7, 0);
        db.add_job(
            "digest",
            &quick_command(),
            "daily at 07:00",
            60,
            None,
            seven,
        )
        .expect("add");
        db.set_enabled("digest", false).expect("disable");

        // Enabled at 13:00, after today's 07:00 passed: tomorrow 07:00, not now.
        let afternoon = local_at(14, 13, 0);
        let next = enable_job(&db, "digest", afternoon).expect("enable");
        assert_eq!(next, local_at(15, 7, 0));
        assert!(db.get_due_jobs(afternoon).expect("due").is_empty());

        // Enabled before its slot: the slot is kept.
        db.add_job("early", &quick_command(), "daily at 07:00", 60, None, seven)
            .expect("add");
        db.set_enabled("early", false).expect("disable");
        assert_eq!(
            enable_job(&db, "early", local_at(14, 6, 0)).expect("enable"),
            seven
        );
    }

    #[test]
    fn enabling_an_interval_job_keeps_its_phase() {
        let db = test_db();
        db.add_job(
            "tick",
            &quick_command(),
            "every 10 minutes",
            60,
            None,
            at(12, 0, 0),
        )
        .expect("add");
        db.set_enabled("tick", false).expect("disable");

        // The 12:00 slot passed; enabled at 12:35 the next slot is 12:40, not 12:45.
        let next = enable_job(&db, "tick", at(12, 35, 0)).expect("enable");
        assert_eq!(next, at(12, 40, 0));
        assert!(db.get_due_jobs(at(12, 35, 0)).expect("due").is_empty());
        assert_eq!(db.get_due_jobs(at(12, 40, 0)).expect("due").len(), 1);
    }

    #[test]
    fn enabling_an_already_enabled_job_keeps_its_due_slot() {
        let db = test_db();
        db.add_job(
            "digest",
            &quick_command(),
            "daily at 07:00",
            60,
            None,
            at(7, 0, 0),
        )
        .expect("add");

        // Due at 07:00, not yet picked up; `enable` at 07:01 must not push it to tomorrow.
        let next = enable_job(&db, "digest", at(7, 1, 0)).expect("enable");
        assert_eq!(next, at(7, 0, 0));
        assert_eq!(db.get_due_jobs(at(7, 1, 0)).expect("due").len(), 1);
    }

    #[test]
    fn enabling_a_missing_job_is_not_found() {
        let db = test_db();
        assert!(enable_job(&db, "nope", at(12, 0, 0)).is_err());
    }

    #[test]
    fn a_run_recorded_after_all_is_left_alone() {
        let db = test_db();
        let now = clock::now();
        db.add_job("quick", &quick_command(), "every 1 hours", 60, None, now)
            .expect("add quick");
        let job = db.get_job_by_name("quick").expect("job");
        let run_id = db
            .create_run(job.id, "running", now, Some(now), "schedule")
            .expect("run");
        db.finish_run(run_id, "success", now, 5, Some(0), None)
            .expect("finish");
        let orphans = Orphans::default();
        orphans
            .lock()
            .unwrap()
            .push((run_id, "logs insert failed".into()));

        recover_orphaned_runs(&db, &orphans);
        assert!(orphans.lock().unwrap().is_empty());
        assert_eq!(last_status(&db, "quick"), "success");
    }

    #[test]
    fn the_daemon_heartbeat_is_written_at_start_then_every_30_seconds() {
        let db = test_db();
        assert!(db.heartbeat().expect("read").is_none());
        let mut writer = HeartbeatWriter::new(at(12, 0, 0), 1);
        writer.tick(&db, at(12, 0, 0));
        let seen = db.heartbeat().expect("read").expect("written");
        assert_eq!(seen.pid, i64::from(std::process::id()));
        assert_eq!(seen.last_tick, at(12, 0, 0));
        assert_eq!(seen.poll_seconds, 1);

        // The next tick, within 30 seconds, writes nothing.
        writer.tick(&db, at(12, 0, 1));
        assert_eq!(
            db.heartbeat().expect("read").unwrap().last_tick,
            at(12, 0, 0)
        );

        // Once 30 seconds have passed it writes again.
        writer.last_written = Some(Instant::now() - HEARTBEAT_EVERY);
        writer.tick(&db, at(12, 0, 30));
        let seen = db.heartbeat().expect("read").unwrap();
        assert_eq!(seen.last_tick, at(12, 0, 30));
        assert_eq!(seen.started_at, at(12, 0, 0));
    }
}
