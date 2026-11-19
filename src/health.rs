//! `cronlog health`: is the daemon alive, and is every job running and succeeding?
//! Exits non-zero when not, so a watchdog or monitor can act on the exit code alone.

use crate::clock;
use crate::db::{Database, Heartbeat};
use crate::error::Result;
use crate::scheduler::HEARTBEAT_EVERY;
use chrono::{Duration, NaiveDateTime};
use serde::Serialize;

/// A run still `running` this long after its timeout is stuck: Cronlog kills a timed-out
/// job and records it within seconds.
const STUCK_AFTER_TIMEOUT: Duration = Duration::seconds(60);

#[derive(Debug, Clone)]
pub struct HealthOptions {
    /// Check only this job (and the daemon). A disabled job named here is a problem, and
    /// is still checked for failures and stuck runs.
    pub job: Option<String>,
    /// A job whose slot passed this long ago without being claimed is overdue.
    pub grace: Duration,
    /// This many failed runs in a row (in completion order; skips not counted) is a
    /// problem.
    pub failures: i64,
    /// The daemon is down when its check-in, due every HEARTBEAT_EVERY (plus its poll
    /// interval), is this late.
    pub daemon_stale: Duration,
}

#[derive(Debug, Serialize, PartialEq, Eq)]
pub struct Problem {
    /// daemon_missing, daemon_stale, disabled, overdue, failing, stuck or hook_failing
    pub kind: &'static str,
    pub job: Option<String>,
    pub message: String,
}

#[derive(Debug, Serialize)]
pub struct DaemonSeen {
    pub pid: i64,
    #[serde(serialize_with = "clock::serialize_local")]
    pub started_at: Option<NaiveDateTime>,
    #[serde(serialize_with = "clock::serialize_local")]
    pub last_seen: Option<NaiveDateTime>,
    pub seconds_ago: i64,
}

#[derive(Debug, Serialize)]
pub struct HealthReport {
    pub healthy: bool,
    #[serde(serialize_with = "clock::serialize_local")]
    pub checked_at: Option<NaiveDateTime>,
    pub daemon: Option<DaemonSeen>,
    pub jobs_checked: usize,
    pub problems: Vec<Problem>,
}

pub fn check(db: &Database, now: NaiveDateTime, options: &HealthOptions) -> Result<HealthReport> {
    let mut problems = Vec::new();
    let mut problem = |kind, job: Option<&str>, message: String| {
        problems.push(Problem {
            kind,
            job: job.map(str::to_string),
            message,
        })
    };

    let heartbeat = db.heartbeat()?;
    match &heartbeat {
        None => problem(
            "daemon_missing",
            None,
            "no daemon has run against this database (or only one older than `health`)".into(),
        ),
        Some(seen) => {
            let age = now - seen.last_tick;
            // Late, not merely quiet: a check-in is only due HEARTBEAT_EVERY after the
            // last one, so a threshold shorter than that cannot make a live daemon flap.
            let due = Duration::from_std(HEARTBEAT_EVERY).unwrap_or(Duration::zero())
                + Duration::seconds(seen.poll_seconds.max(1));
            let limit = due + options.daemon_stale;
            if age > limit {
                problem(
                    "daemon_stale",
                    None,
                    format!(
                        "daemon (pid {}) last seen {} ago, at {}",
                        seen.pid,
                        ago(age),
                        clock::display(seen.last_tick)
                    ),
                );
            }
        }
    }

    let jobs = db.health_jobs(options.job.as_deref())?;
    for checked in &jobs {
        let job = &checked.job;
        let name = Some(job.name.as_str());
        if !checked.enabled {
            // Disabling a job stops its schedule, not a run already going, and manual runs
            // still happen: check those, but there is no slot to be overdue for.
            problem("disabled", name, "job is disabled".into());
        }

        if let Some(due) = job.next_run_at.filter(|_| checked.enabled) {
            if now - due > options.grace {
                problem(
                    "overdue",
                    name,
                    format!(
                        "due {} ago, at {}, and not started",
                        ago(now - due),
                        clock::display(due)
                    ),
                );
            }
        }

        if checked.failure_streak >= options.failures {
            let newest = match db.last_failure(job.id)? {
                Some(newest) => {
                    let detail = match (&newest.error, newest.exit_code) {
                        (Some(error), _) if !error.is_empty() => format!(": {error}"),
                        (_, Some(code)) => format!(", exit code {code}"),
                        _ => String::new(),
                    };
                    let when = newest
                        .finished_at
                        .map(|t| format!(" at {}", clock::display(t)))
                        .unwrap_or_default();
                    format!(
                        "; newest, run #{}, {}{when}{detail}",
                        newest.run_id, newest.status
                    )
                }
                None => String::new(), // pruned
            };
            problem(
                "failing",
                name,
                format!("{} runs in a row failed{newest}", checked.failure_streak),
            );
        }

        for (run_id, started_at, run_timeout) in db.running_runs(job.id)? {
            // The timeout this run was started with: `cronlog edit` may have changed the
            // job's since, but the worker still uses the one it began with.
            let timeout = Duration::seconds(run_timeout.unwrap_or(job.timeout_seconds));
            if let Some(started_at) = started_at {
                if now - started_at > timeout + STUCK_AFTER_TIMEOUT {
                    problem(
                        "stuck",
                        name,
                        format!(
                            "run #{run_id} still running after {}, past its {} timeout",
                            ago(now - started_at),
                            ago(timeout)
                        ),
                    );
                }
            }
        }
    }

    // A broken alert shouldn't stay silent: report it if the newest hook to run didn't
    // succeed. Hooks are set for every job, so a `--job` check looks at its own.
    if let Some(hook) = db.last_finished_hook(options.job.as_deref())? {
        if hook.status != "success" {
            let when = hook
                .finished_at
                .map(|t| format!(" at {}", clock::display(t)))
                .unwrap_or_default();
            let detail = hook
                .error
                .as_deref()
                .map(|e| format!(": {e}"))
                .unwrap_or_default();
            problem(
                "hook_failing",
                Some(hook.job.as_str()),
                format!(
                    "the {} hook for run #{} {}{when}{detail}",
                    hook.event, hook.run_id, hook.status
                ),
            );
        }
    }

    Ok(HealthReport {
        healthy: problems.is_empty(),
        checked_at: Some(now),
        daemon: heartbeat.map(|seen: Heartbeat| DaemonSeen {
            pid: seen.pid,
            started_at: Some(seen.started_at),
            last_seen: Some(seen.last_tick),
            seconds_ago: (now - seen.last_tick).num_seconds(),
        }),
        jobs_checked: jobs.len(),
        problems,
    })
}

/// One line when healthy; otherwise a line per problem.
pub fn print(report: &HealthReport, color: bool) {
    if report.healthy {
        let daemon = report
            .daemon
            .as_ref()
            .map(|d| format!("daemon pid {} seen {}s ago", d.pid, d.seconds_ago))
            .unwrap_or_default();
        let jobs = report.jobs_checked;
        let plural = if jobs == 1 { "" } else { "s" };
        let healthy = crate::color::paint("healthy", crate::color::GREEN, color);
        println!("{healthy}: {daemon}; {jobs} job{plural} checked");
        return;
    }
    println!(
        "{}: {} problem{}",
        crate::color::paint("UNHEALTHY", crate::color::RED, color),
        report.problems.len(),
        if report.problems.len() == 1 { "" } else { "s" }
    );
    for p in &report.problems {
        match &p.job {
            Some(job) => println!("- {job}: {} ({})", p.message, p.kind),
            None => println!("- {} ({})", p.message, p.kind),
        }
    }
}

/// A duration rounded to whole seconds, as `1h 2m 3s`.
fn ago(duration: Duration) -> String {
    let seconds = duration.num_seconds().max(0) as u64;
    humantime::format_duration(std::time::Duration::from_secs(seconds)).to_string()
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::NaiveDate;

    fn at(hour: u32, minute: u32) -> NaiveDateTime {
        NaiveDate::from_ymd_opt(2026, 11, 17)
            .unwrap()
            .and_hms_opt(hour, minute, 0)
            .unwrap()
    }

    fn test_db() -> Database {
        let unique = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("time")
            .as_nanos();
        let path = std::env::temp_dir().join(format!("Cronlog-health-test-{unique}.db"));
        let db = Database::open(&path).expect("open");
        db.init().expect("init");
        db
    }

    fn options() -> HealthOptions {
        HealthOptions {
            job: None,
            grace: Duration::minutes(5),
            failures: 3,
            daemon_stale: Duration::seconds(90),
        }
    }

    fn beat(db: &Database, when: NaiveDateTime) {
        db.record_heartbeat(&Heartbeat {
            pid: 42,
            started_at: at(9, 0),
            last_tick: when,
            poll_seconds: 1,
        })
        .expect("heartbeat");
    }

    fn kinds(report: &HealthReport) -> Vec<(&'static str, Option<&str>)> {
        report
            .problems
            .iter()
            .map(|p| (p.kind, p.job.as_deref()))
            .collect()
    }

    fn finished(db: &Database, job: &str, status: &str, when: NaiveDateTime) -> i64 {
        let job = db.get_job_by_name(job).expect("job");
        let id = db
            .create_run(job.id, "running", when, Some(when), "schedule")
            .expect("run");
        db.finish_run(id, status, when, 10, Some(1), None)
            .expect("finish");
        id
    }

    #[test]
    fn a_live_daemon_with_jobs_on_time_is_healthy() {
        let db = test_db();
        db.add_job(
            "a",
            &["true".into()],
            "every 5 minutes",
            60,
            None,
            at(12, 5),
        )
        .expect("add");
        beat(&db, at(12, 0));
        let report = check(&db, at(12, 1), &options()).expect("check");
        assert!(report.healthy, "{:?}", report.problems);
        assert_eq!(report.jobs_checked, 1);
        assert_eq!(report.daemon.as_ref().map(|d| d.seconds_ago), Some(60));
    }

    #[test]
    fn a_daemon_never_seen_or_gone_quiet_is_a_problem() {
        let db = test_db();
        let report = check(&db, at(12, 0), &options()).expect("check");
        assert_eq!(kinds(&report), vec![("daemon_missing", None)]);

        beat(&db, at(12, 0));
        // The 30-second check-in, the 1-second poll and 90 seconds late is still fine...
        assert!(check(&db, at(12, 2), &options()).expect("check").healthy);
        // ...a little more is not.
        let report = check(&db, at(12, 3), &options()).expect("check");
        assert_eq!(kinds(&report), vec![("daemon_stale", None)]);
        assert!(report.problems[0].message.contains("pid 42"));
    }

    #[test]
    fn a_slot_not_claimed_within_the_grace_period_is_overdue() {
        let db = test_db();
        db.add_job(
            "a",
            &["true".into()],
            "every 5 minutes",
            60,
            None,
            at(12, 0),
        )
        .expect("add");
        beat(&db, at(12, 10));
        assert!(check(&db, at(12, 5), &options()).expect("check").healthy);
        let report = check(&db, at(12, 10), &options()).expect("check");
        assert_eq!(kinds(&report), vec![("overdue", Some("a"))]);
        assert!(report.problems[0].message.starts_with("due 10m ago"));
    }

    #[test]
    fn failures_in_a_row_are_a_problem_but_a_success_or_skip_between_is_not() {
        let db = test_db();
        db.add_job(
            "a",
            &["false".into()],
            "every 5 minutes",
            60,
            None,
            at(13, 0),
        )
        .expect("add");
        beat(&db, at(12, 30));
        finished(&db, "a", "failed", at(12, 0));
        finished(&db, "a", "timed_out", at(12, 5));
        finished(&db, "a", "skipped", at(12, 10)); // not a failure, and not a success
        assert!(check(&db, at(12, 30), &options()).expect("check").healthy);

        let newest = finished(&db, "a", "failed", at(12, 15));
        let report = check(&db, at(12, 30), &options()).expect("check");
        assert_eq!(kinds(&report), vec![("failing", Some("a"))]);
        assert!(report.problems[0]
            .message
            .starts_with("3 runs in a row failed"));
        assert!(
            report.problems[0]
                .message
                .contains(&format!("run #{newest}, failed")),
            "{}",
            report.problems[0].message
        );
        assert!(report.problems[0].message.ends_with("exit code 1"));

        finished(&db, "a", "success", at(12, 20));
        assert!(check(&db, at(12, 30), &options()).expect("check").healthy);
    }

    #[test]
    fn a_short_stale_limit_does_not_flap_between_check_ins() {
        let db = test_db();
        beat(&db, at(12, 0));
        let short = HealthOptions {
            daemon_stale: Duration::seconds(5),
            ..options()
        };
        // 30 seconds after a check-in the next is only just due.
        let between = at(12, 0) + Duration::seconds(30);
        assert!(check(&db, between, &short).expect("check").healthy);
        let late = at(12, 0) + Duration::seconds(37);
        assert_eq!(
            kinds(&check(&db, late, &short).expect("check")),
            vec![("daemon_stale", None)]
        );
    }

    #[test]
    fn failures_count_in_the_order_runs_finish() {
        let db = test_db();
        db.add_job(
            "a",
            &["false".into()],
            "every 5 minutes",
            60,
            None,
            at(13, 0),
        )
        .expect("add");
        beat(&db, at(12, 30));
        let job = db.get_job_by_name("a").expect("job");
        let once = HealthOptions {
            failures: 1,
            ..options()
        };
        // A manual run succeeds at 12:05; the 12:00 slot, picked up late, fails at 12:10.
        let manual = db
            .create_run(job.id, "running", at(12, 5), Some(at(12, 5)), "manual")
            .expect("manual");
        let late = db
            .create_run(job.id, "running", at(12, 0), Some(at(12, 9)), "schedule")
            .expect("late");
        db.finish_run(manual, "success", at(12, 5), 1, Some(0), None)
            .expect("manual done");
        db.finish_run(late, "failed", at(12, 10), 1, Some(2), None)
            .expect("late done");
        let report = check(&db, at(12, 30), &once).expect("check");
        assert_eq!(kinds(&report), vec![("failing", Some("a"))]);
        assert!(report.problems[0]
            .message
            .contains(&format!("run #{late}, failed")));
    }

    #[test]
    fn pruning_old_failures_does_not_end_the_streak() {
        let db = test_db();
        db.add_job(
            "a",
            &["false".into()],
            "daily at 07:00",
            60,
            None,
            at(23, 0),
        )
        .expect("add");
        beat(&db, at(12, 30));
        for hour in 9..12 {
            finished(&db, "a", "failed", at(hour, 0));
        }
        // Keep only the newest run: two of the three failures are gone.
        let pruned = db.prune_batch(at(12, 30), 1, 100).expect("prune");
        assert_eq!(pruned.runs, 2);
        let report = check(&db, at(12, 30), &options()).expect("check");
        assert_eq!(kinds(&report), vec![("failing", Some("a"))]);
    }

    #[test]
    fn interrupted_runs_count_as_failures() {
        let db = test_db();
        db.add_job(
            "a",
            &["true".into()],
            "every 5 minutes",
            60,
            None,
            at(13, 0),
        )
        .expect("add");
        beat(&db, at(12, 30));
        let job = db.get_job_by_name("a").expect("job");
        for minute in [0, 5] {
            let id = db
                .create_run(
                    job.id,
                    "running",
                    at(12, minute),
                    Some(at(12, minute)),
                    "schedule",
                )
                .expect("run");
            assert!(db
                .interrupt_run(id, at(12, minute + 1), "lost")
                .expect("interrupt"));
        }
        db.create_run(job.id, "running", at(12, 10), Some(at(12, 10)), "schedule")
            .expect("run");
        db.interrupt_stale_running_runs(at(12, 11), "daemon restarted")
            .expect("restart");
        let report = check(&db, at(12, 30), &options()).expect("check");
        assert_eq!(kinds(&report), vec![("failing", Some("a"))]);
        assert!(report.problems[0].message.contains("daemon restarted"));
    }

    #[test]
    fn a_run_well_past_its_timeout_is_stuck() {
        let db = test_db();
        // 60-second timeout; Cronlog would have killed it by 12:01.
        db.add_job("a", &["sleep".into()], "every 1 hours", 60, None, at(13, 0))
            .expect("add");
        let job = db.get_job_by_name("a").expect("job");
        db.create_run(job.id, "running", at(12, 0), Some(at(12, 0)), "schedule")
            .expect("run");
        beat(&db, at(12, 5));
        assert!(check(&db, at(12, 2), &options()).expect("check").healthy);
        let report = check(&db, at(12, 5), &options()).expect("check");
        assert_eq!(kinds(&report), vec![("stuck", Some("a"))]);
    }

    #[test]
    fn checking_one_job_skips_the_others_and_flags_it_disabled() {
        let db = test_db();
        db.add_job(
            "a",
            &["true".into()],
            "every 5 minutes",
            60,
            None,
            at(11, 0),
        )
        .expect("add");
        db.add_job(
            "b",
            &["true".into()],
            "every 5 minutes",
            60,
            None,
            at(12, 5),
        )
        .expect("add");
        beat(&db, at(12, 0));
        let only = |job: &str| HealthOptions {
            job: Some(job.into()),
            ..options()
        };
        // `a` is overdue, but only `b` is checked.
        let report = check(&db, at(12, 0), &only("b")).expect("check");
        assert!(report.healthy);
        assert_eq!(report.jobs_checked, 1);

        db.set_enabled("b", false).expect("disable");
        let report = check(&db, at(12, 0), &only("b")).expect("check");
        assert_eq!(kinds(&report), vec![("disabled", Some("b"))]);

        // Disabled, but a run it started before is wedged: both are reported.
        let job = db.get_job_by_name("b").expect("job");
        db.create_run(job.id, "running", at(11, 0), Some(at(11, 0)), "schedule")
            .expect("run");
        let report = check(&db, at(12, 0), &only("b")).expect("check");
        assert_eq!(
            kinds(&report),
            vec![("disabled", Some("b")), ("stuck", Some("b"))]
        );
        // Unnamed, a disabled job is simply not checked.
        let report = check(&db, at(12, 0), &options()).expect("check");
        assert_eq!(kinds(&report), vec![("overdue", Some("a"))]);

        assert!(check(&db, at(12, 0), &only("nope")).is_err());
    }

    #[test]
    fn json_has_local_times_and_problem_kinds() {
        let db = test_db();
        let report = check(&db, at(12, 0), &options()).expect("check");
        let json = serde_json::to_value(&report).expect("json");
        assert_eq!(json["healthy"], false);
        assert_eq!(json["problems"][0]["kind"], "daemon_missing");
        assert_eq!(json["daemon"], serde_json::Value::Null);
        let checked = json["checked_at"].as_str().expect("time");
        let local = clock::to_local(at(12, 0))
            .naive_local()
            .format("%Y-%m-%dT%H:%M:%S")
            .to_string();
        assert_eq!(&checked[..19], local);
    }
}
