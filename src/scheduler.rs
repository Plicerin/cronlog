use crate::db::{Database, HistoryRow, Job, RunClaim};
use crate::error::Result;
use crate::{runner, schedule};
use chrono::Local;
use std::thread;
use std::time::Duration;

pub fn run_daemon(db: &Database, poll_seconds: u64) -> Result<()> {
    recover_interrupted_runs(db)?;

    loop {
        let now = Local::now().naive_local();
        let due_jobs = db.get_due_jobs(now)?;

        for job in due_jobs {
            let scheduled_for = job.next_run_at.unwrap_or(now);
            let next_run_at = next_run_after_scheduled_time(&job, scheduled_for, now)?;

            match db.claim_scheduled_run(job.id, scheduled_for, next_run_at, runner::now())? {
                RunClaim::Claimed(run_id) => {
                    run_claimed_job(db, &job, run_id, scheduled_for, "schedule")?;
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

        thread::sleep(Duration::from_secs(poll_seconds.max(1)));
    }
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
        }
        Err(err) => {
            let message = err.to_string();
            db.finish_run(run_id, "failed", runner::now(), 0, None, Some(&message))?;
            db.insert_logs(run_id, "", &message, false, false)?;
            println!("Failed job '{}' run #{}: {}", job.name, run_id, message);
        }
    }

    Ok(())
}

fn next_run_after_scheduled_time(
    job: &Job,
    scheduled_for: chrono::NaiveDateTime,
    now: chrono::NaiveDateTime,
) -> Result<chrono::NaiveDateTime> {
    let parsed = schedule::parse_schedule(&job.schedule)?;
    let mut next = parsed.next_after(scheduled_for)?;
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
}
