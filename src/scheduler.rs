use crate::db::{Database, HistoryRow, Job, RunClaim};
use crate::error::Result;
use crate::{runner, schedule};
use chrono::Local;
use std::panic::{self, AssertUnwindSafe};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::Duration;

/// Claimed runs whose worker failed before recording a result, with the reason. The daemon
/// marks them `interrupted`, retrying each tick until the database accepts it.
///
/// Without this, a run left `running` by a failed worker made every later slot of that job
/// `skipped` until the daemon restarted (stale-run recovery only runs at startup).
pub type Orphans = Arc<Mutex<Vec<(i64, String)>>>;

pub fn run_daemon(db: &Database, poll_seconds: u64) -> Result<()> {
    recover_interrupted_runs(db)?;
    let orphans = Orphans::default();

    loop {
        recover_orphaned_runs(db, &orphans);
        // Handles are dropped: runs finish on their own threads and record their own results.
        dispatch_due_jobs(db, Local::now().naive_local(), &orphans)?;
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

        match db.claim_scheduled_run(job.id, scheduled_for, next_run_at, runner::now())? {
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
    db.enable_job(name, |schedule, current_next| match current_next {
        Some(next) if next > now => Ok(next),
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
        match db.interrupt_run(run_id, runner::now(), &message) {
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
    let interrupted = db.interrupt_stale_running_runs(runner::now(), message)?;
    for run in interrupted {
        println!(
            "Marked job '{}' run #{} as interrupted: {}",
            run.job_name, run.run_id, message
        );
    }
    Ok(())
}

pub fn run_job_once(
    db: &Database,
    job: &Job,
    scheduled_for: chrono::NaiveDateTime,
    trigger_type: &str,
) -> Result<()> {
    let previous = db.history(&job.name, 1)?.into_iter().next();
    let started_at = runner::now();
    let run_id = db.create_run(
        job.id,
        "running",
        scheduled_for,
        Some(started_at),
        trigger_type,
    )?;
    println!(
        "Starting job '{}' run #{}: {}",
        job.name,
        run_id,
        job.command.join(" ")
    );

    execute_claimed_run(db, job, run_id, scheduled_for, trigger_type, previous)
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
        scheduled_for: scheduled_for.to_string(),
        trigger_type: trigger_type.to_string(),
        previous_run_id: previous.as_ref().map(|row| row.run_id),
        previous_status: previous.as_ref().map(|row| row.status.clone()),
    };

    match runner::execute(job, &context) {
        Ok(out) => {
            db.finish_run(
                run_id,
                &out.status,
                runner::now(),
                out.duration_ms,
                out.exit_code,
                out.error.as_deref(),
            )?;
            db.insert_logs(
                run_id,
                &out.stdout,
                &out.stderr,
                out.stdout_truncated,
                out.stderr_truncated,
            )?;
            println!(
                "Finished job '{}' run #{}: {}",
                job.name, run_id, out.status
            );
            disable_if_max_runs_reached(db, job.id)?;
        }
        Err(err) => {
            let message = err.to_string();
            db.finish_run(run_id, "failed", runner::now(), 0, None, Some(&message))?;
            db.insert_logs(run_id, "", &message, false, false)?;
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
    let parsed = schedule::parse_schedule(schedule)?;
    let mut next = parsed.next_after(previous)?;
    while next <= now {
        next = parsed.next_after(next)?;
    }
    Ok(next)
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::NaiveDate;

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

    #[test]
    fn a_slow_job_does_not_delay_other_jobs() {
        let db = test_db();
        let now = Local::now().naive_local();
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
        let now = Local::now().naive_local();
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
        let now = Local::now().naive_local();
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

    #[test]
    fn enabling_a_job_whose_slot_passed_waits_for_the_next_slot() {
        let db = test_db();
        let added = at(6, 0, 0);
        db.add_job(
            "digest",
            &quick_command(),
            "daily at 07:00",
            60,
            None,
            at(7, 0, 0),
        )
        .expect("add");
        db.set_enabled("digest", false).expect("disable");

        // Enabled at 13:00, after today's 07:00 passed: tomorrow 07:00, not now.
        let afternoon = at(13, 0, 0);
        let next = enable_job(&db, "digest", afternoon).expect("enable");
        assert_eq!(next, at(7, 0, 0) + chrono::Duration::days(1));
        assert!(db.get_due_jobs(afternoon).expect("due").is_empty());

        // Enabled before its slot: the slot is kept.
        db.set_enabled("digest", false).expect("disable");
        db.set_next_run_at("digest", at(7, 0, 0)).expect("reset");
        assert_eq!(
            enable_job(&db, "digest", added).expect("enable"),
            at(7, 0, 0)
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
    fn enabling_a_missing_job_is_not_found() {
        let db = test_db();
        assert!(enable_job(&db, "nope", at(12, 0, 0)).is_err());
    }

    #[test]
    fn a_run_recorded_after_all_is_left_alone() {
        let db = test_db();
        let now = Local::now().naive_local();
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
}
