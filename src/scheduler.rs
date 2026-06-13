use crate::db::{Database, Job};
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

            if db.has_running_run(job.id)? {
                let run_id = db.create_run(job.id, "skipped", scheduled_for, None, "schedule")?;
                db.finish_run(
                    run_id,
                    "skipped",
                    runner::now(),
                    0,
                    None,
                    Some("previous run still active"),
                )?;
                update_next_run_from_now(db, &job)?;
                println!(
                    "Skipped '{}' because a previous run is still active",
                    job.name
                );
                continue;
            }

            run_job_once(db, &job, scheduled_for, "schedule")?;
            update_next_run_from_now(db, &job)?;
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

fn update_next_run_from_now(db: &Database, job: &Job) -> Result<()> {
    let parsed = schedule::parse_schedule(&job.schedule)?;
    let next = parsed.next_future_after(Local::now().naive_local())?;
    db.update_next_run(job.id, next)
}
