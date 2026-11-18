//! Failure and recovery hooks: commands Cronlog runs when a job starts failing or
//! recovers, for example to send an alert.
//!
//! One hook per event is set for every job (`cronlog hook set`). A hook is queued in
//! the transaction that records the run (`update_failure_streak` in db.rs), only when
//! the job's failure streak goes from 0 to 1 (failure) or back to 0 after failures
//! (recovery), so a job failing every minute alerts once. Queued hooks run here, oldest
//! first, from the daemon or right after a manual run; a hook still queued when a
//! process stops runs the next time either looks.

use crate::clock;
use crate::db::{Database, PendingHook, HOOK_FAILURE, HOOK_RECOVERY};
use crate::error::{CronlogError, Result};
use crate::runner::{self, Keep};
use clap::ValueEnum;
use std::process::{Command, Stdio};
use std::time::{Duration, Instant};
use wait_timeout::ChildExt;

/// A hook still running after this is killed and recorded as timed out.
pub const HOOK_TIMEOUT: Duration = Duration::from_secs(30);

/// How much of the run's error output a hook is given.
const STDERR_TAIL_LINES: usize = 20;
const STDERR_TAIL_BYTES: usize = 2000;
/// How much of a failed hook's own error output is kept.
const ERROR_BYTES: usize = 500;

#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
pub enum HookEvent {
    /// The job's first failure after a success
    Failure,
    /// The job's first success after failures
    Recovery,
}

impl HookEvent {
    pub fn as_str(self) -> &'static str {
        match self {
            HookEvent::Failure => HOOK_FAILURE,
            HookEvent::Recovery => HOOK_RECOVERY,
        }
    }
}

/// A hook's command and arguments, word by word as given after `--`, in the form job
/// commands are stored (quoted, so a Windows path keeps its backslashes).
pub fn store_command(words: &[String]) -> Result<String> {
    if words
        .first()
        .is_none_or(|program| program.trim().is_empty())
    {
        return Err(CronlogError::InvalidCommand("hook command is empty".into()));
    }
    Ok(shell_words::join(words.iter().map(String::as_str)))
}

fn split(line: &str) -> Vec<String> {
    shell_words::split(line).unwrap_or_else(|_| vec![line.to_string()])
}

/// How a hook ran.
#[derive(Debug, PartialEq, Eq)]
pub struct Outcome {
    pub status: &'static str,
    pub exit_code: Option<i64>,
    pub duration_ms: i64,
    pub error: Option<String>,
}

/// Run a hook: no shell, no stdin, stdout discarded, the end of stderr kept for the
/// error message. It runs in its own process group, like a job, so a timeout kills
/// everything it started, and returns within `timeout` plus the runner's drain grace
/// even when a background process it started holds stderr open.
pub fn execute(command: &[String], env: &[(&str, String)], timeout: Duration) -> Outcome {
    let started = Instant::now();
    let elapsed = || i64::try_from(started.elapsed().as_millis()).unwrap_or(i64::MAX);
    let failed = |error: String, exit_code: Option<i64>, duration_ms: i64| Outcome {
        status: "failed",
        exit_code,
        duration_ms,
        error: Some(error),
    };

    let Some((program, args)) = command.split_first() else {
        return failed("hook command is empty".into(), None, 0);
    };
    let mut cmd = Command::new(program);
    cmd.args(args)
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::piped());
    for (key, value) in env {
        cmd.env(key, value);
    }
    #[cfg(unix)]
    {
        use std::os::unix::process::CommandExt;
        cmd.process_group(0);
    }
    let mut child = match cmd.spawn() {
        Ok(child) => child,
        Err(e) => return failed(format!("could not start hook: {e}"), None, elapsed()),
    };

    // Drained while the hook runs, keeping only the end, so a chatty hook neither blocks
    // on a full pipe nor grows the daemon's memory.
    let reader = child
        .stderr
        .take()
        .map(|stream| runner::spawn_reader_keeping(stream, ERROR_BYTES * 4, "stderr", Keep::Last));
    // Stops the reader when the hook ends, at the latest after the drain grace.
    let stderr = |reader| {
        let captured = runner::collect(
            reader,
            Instant::now() + runner::DRAIN_GRACE,
            &mut Vec::new(),
        );
        String::from_utf8_lossy(&captured.bytes).into_owned()
    };

    match child.wait_timeout(timeout) {
        Ok(Some(status)) => {
            let stderr = stderr(reader);
            let exit_code = status.code().map(i64::from);
            if status.success() {
                return Outcome {
                    status: "success",
                    exit_code,
                    duration_ms: elapsed(),
                    error: None,
                };
            }
            let detail = last_bytes(stderr.trim(), ERROR_BYTES);
            let error = if !detail.is_empty() {
                detail
            } else if let Some(code) = exit_code {
                format!("exit code {code}")
            } else {
                "ended by a signal".into()
            };
            failed(error, exit_code, elapsed())
        }
        Ok(None) => {
            runner::kill_process_tree(&mut child);
            let _ = child.wait();
            stderr(reader);
            Outcome {
                status: "timed_out",
                exit_code: None,
                duration_ms: elapsed(),
                error: Some(format!(
                    "hook still running after {}s; killed",
                    timeout.as_secs()
                )),
            }
        }
        Err(e) => {
            runner::kill_process_tree(&mut child);
            let _ = child.wait();
            stderr(reader);
            failed(format!("could not wait for hook: {e}"), None, elapsed())
        }
    }
}

/// The end of `text`, at most `max` bytes, cut on a character boundary.
fn last_bytes(text: &str, max: usize) -> String {
    if text.len() <= max {
        return text.to_string();
    }
    let mut start = text.len() - max;
    while !text.is_char_boundary(start) {
        start += 1;
    }
    text[start..].to_string()
}

/// The last lines of a run's error output, for the alert.
pub fn stderr_tail(stderr: &str) -> String {
    let lines: Vec<&str> = stderr.lines().collect();
    let tail = lines[lines.len().saturating_sub(STDERR_TAIL_LINES)..].join("\n");
    // An environment variable can't hold a NUL byte.
    last_bytes(tail.trim(), STDERR_TAIL_BYTES).replace('\0', "")
}

/// The environment a hook runs with.
fn env_for(hook: &PendingHook) -> Vec<(&'static str, String)> {
    let mut env = vec![
        ("CRONLOG_EVENT", hook.event.clone()),
        ("CRONLOG_JOB_NAME", hook.job_name.clone()),
        ("CRONLOG_RUN_ID", hook.run_id.to_string()),
        ("CRONLOG_FAILURE_STREAK", hook.failure_streak.to_string()),
    ];
    if let Some(status) = &hook.run_status {
        env.push(("CRONLOG_STATUS", status.clone()));
    }
    if let Some(code) = hook.exit_code {
        env.push(("CRONLOG_EXIT_CODE", code.to_string()));
    }
    if let Some(error) = hook.error.as_ref().filter(|e| !e.is_empty()) {
        env.push(("CRONLOG_ERROR", error.replace('\0', "")));
    }
    if let Some(finished) = hook.finished_at {
        env.push(("CRONLOG_FINISHED_AT", clock::display(finished)));
    }
    let tail = stderr_tail(hook.stderr.as_deref().unwrap_or(""));
    if !tail.is_empty() {
        env.push(("CRONLOG_STDERR_TAIL", tail));
    }
    env
}

/// Run every queued hook, oldest first, and record how each went. Returns how many
/// were taken from the queue.
pub fn dispatch_pending(db: &Database) -> Result<usize> {
    let mut taken = 0;
    while let Some(hook) = db.claim_pending_hook(clock::now())? {
        taken += 1;
        let outcome = match db.hook_command(&hook.event)? {
            // Cleared after this hook was queued.
            None => Outcome {
                status: "skipped",
                exit_code: None,
                duration_ms: 0,
                error: Some("no hook set when it came to run".into()),
            },
            Some(line) => execute(&split(&line), &env_for(&hook), HOOK_TIMEOUT),
        };
        db.finish_hook(
            hook.id,
            outcome.status,
            clock::now(),
            outcome.duration_ms,
            outcome.exit_code,
            outcome.error.as_deref(),
        )?;
        let detail = outcome
            .error
            .as_deref()
            .map(|e| format!(": {e}"))
            .unwrap_or_default();
        let line = format!(
            "Ran the {} hook for job '{}' run #{}: {}{detail}",
            hook.event, hook.job_name, hook.run_id, outcome.status
        );
        if matches!(outcome.status, "success" | "skipped") {
            println!("{line}");
        } else {
            eprintln!("{line}");
        }
    }
    Ok(taken)
}

/// Run the hook set for `event` now, with sample values and CRONLOG_TEST=1, to check
/// that it works. Nothing is recorded.
pub fn test(db: &Database, event: HookEvent) -> Result<Outcome> {
    let line = db.hook_command(event.as_str())?.ok_or_else(|| {
        CronlogError::NotFound(format!(
            "no {} hook is set; use `cronlog hook set {} -- <command>`",
            event.as_str(),
            event.as_str()
        ))
    })?;
    let mut env = vec![
        ("CRONLOG_TEST", "1".to_string()),
        ("CRONLOG_EVENT", event.as_str().to_string()),
        ("CRONLOG_JOB_NAME", "cronlog-hook-test".to_string()),
        ("CRONLOG_RUN_ID", "0".to_string()),
        ("CRONLOG_FINISHED_AT", clock::display(clock::now())),
    ];
    match event {
        HookEvent::Failure => env.extend([
            ("CRONLOG_STATUS", "failed".to_string()),
            ("CRONLOG_EXIT_CODE", "1".to_string()),
            ("CRONLOG_FAILURE_STREAK", "1".to_string()),
            (
                "CRONLOG_ERROR",
                "a test from `cronlog hook test`".to_string(),
            ),
            (
                "CRONLOG_STDERR_TAIL",
                "sample error output from `cronlog hook test`".to_string(),
            ),
        ]),
        HookEvent::Recovery => env.extend([
            ("CRONLOG_STATUS", "success".to_string()),
            ("CRONLOG_EXIT_CODE", "0".to_string()),
            ("CRONLOG_FAILURE_STREAK", "3".to_string()),
        ]),
    }
    Ok(execute(&split(&line), &env, HOOK_TIMEOUT))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn words(parts: &[&str]) -> Vec<String> {
        parts.iter().map(|s| s.to_string()).collect()
    }

    #[cfg(unix)]
    fn exit_with(code: i32) -> Vec<String> {
        words(&["sh", "-c", &format!("exit {code}")])
    }

    #[cfg(windows)]
    fn exit_with(code: i32) -> Vec<String> {
        words(&["cmd", "/c", "exit", &code.to_string()])
    }

    #[cfg(unix)]
    fn complain_and_fail() -> Vec<String> {
        words(&["sh", "-c", "echo boom >&2; exit 2"])
    }

    #[cfg(windows)]
    fn complain_and_fail() -> Vec<String> {
        words(&[
            "powershell",
            "-NoProfile",
            "-Command",
            "[Console]::Error.WriteLine('boom'); exit 2",
        ])
    }

    #[cfg(unix)]
    fn sleep_long() -> Vec<String> {
        words(&["sleep", "5"])
    }

    #[cfg(windows)]
    fn sleep_long() -> Vec<String> {
        words(&["ping", "-n", "6", "127.0.0.1"])
    }

    #[cfg(unix)]
    fn dump_env(path: &std::path::Path) -> Vec<String> {
        words(&["sh", "-c", &format!("env > '{}'", path.display())])
    }

    #[cfg(windows)]
    fn dump_env(path: &std::path::Path) -> Vec<String> {
        words(&[
            "cmd",
            "/c",
            "set",
            "CRONLOG_",
            ">",
            &path.display().to_string(),
        ])
    }

    #[test]
    fn execute_reports_success_failure_and_missing_programs() {
        let ok = execute(&exit_with(0), &[], HOOK_TIMEOUT);
        assert_eq!(ok.status, "success", "{ok:?}");

        let bad = execute(&exit_with(3), &[], HOOK_TIMEOUT);
        assert_eq!(bad.status, "failed");
        assert_eq!(bad.exit_code, Some(3));
        assert_eq!(bad.error.as_deref(), Some("exit code 3"));

        let missing = execute(&words(&["cronlog-no-such-program"]), &[], HOOK_TIMEOUT);
        assert_eq!(missing.status, "failed");
        assert!(missing
            .error
            .as_deref()
            .unwrap()
            .starts_with("could not start hook"));
    }

    #[test]
    fn a_failed_hook_keeps_its_error_output() {
        let out = execute(&complain_and_fail(), &[], HOOK_TIMEOUT);
        assert_eq!(out.status, "failed");
        assert_eq!(out.exit_code, Some(2));
        assert!(out.error.as_deref().unwrap().contains("boom"), "{out:?}");
    }

    #[test]
    fn a_hook_past_its_timeout_is_killed() {
        let out = execute(&sleep_long(), &[], Duration::from_secs(1));
        assert_eq!(out.status, "timed_out");
        assert!(out.duration_ms < 4_000, "took {} ms", out.duration_ms);
    }

    #[cfg(unix)]
    #[test]
    fn a_background_process_holding_stderr_does_not_hang_the_hook() {
        let out = execute(
            &words(&["sh", "-c", "sleep 30 & exit 0"]),
            &[],
            HOOK_TIMEOUT,
        );
        assert_eq!(out.status, "success", "{out:?}");
        let bound = crate::runner::DRAIN_GRACE.as_millis() as i64 + 3_000;
        assert!(out.duration_ms < bound, "took {} ms", out.duration_ms);
    }

    #[test]
    fn stderr_tail_keeps_the_last_lines() {
        let many: String = (1..=50).map(|i| format!("line {i}\n")).collect();
        let tail = stderr_tail(&many);
        assert!(tail.starts_with("line 31"), "{tail}");
        assert!(tail.ends_with("line 50"));
        let long = "é".repeat(5000);
        assert!(stderr_tail(&long).len() <= STDERR_TAIL_BYTES);
        assert_eq!(stderr_tail(""), "");
    }

    #[test]
    fn stored_commands_keep_spaces_and_backslashes() {
        let given = words(&[r"C:\Hermes Scripts\notify.cmd", "--topic", "sys ops"]);
        let stored = store_command(&given).unwrap();
        assert_eq!(split(&stored), given);
        assert!(store_command(&[]).is_err());
        assert!(store_command(&words(&["  "])).is_err());
    }

    #[test]
    fn dispatch_runs_queued_hooks_with_the_run_details() {
        let db = crate::db::tests_support::test_db();
        let env_file = std::env::temp_dir().join(format!(
            "cronlog-hook-env-{}.txt",
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        let command = shell_words::join(dump_env(&env_file).iter().map(String::as_str));
        db.set_hook_command(HOOK_FAILURE, Some(&command))
            .expect("set hook");
        let run_id =
            crate::db::tests_support::failed_run(&db, "report", "source 2/3 returned HTTP 503");

        assert_eq!(dispatch_pending(&db).expect("dispatch"), 1);
        assert_eq!(dispatch_pending(&db).expect("dispatch again"), 0);

        let env = std::fs::read_to_string(&env_file).expect("hook wrote its environment");
        assert!(env.contains("CRONLOG_EVENT=failure"), "{env}");
        assert!(env.contains("CRONLOG_JOB_NAME=report"));
        assert!(env.contains(&format!("CRONLOG_RUN_ID={run_id}")));
        assert!(env.contains("CRONLOG_STATUS=failed"));
        assert!(env.contains("CRONLOG_EXIT_CODE=3"));
        assert!(env.contains("CRONLOG_FAILURE_STREAK=1"));
        assert!(env.contains("CRONLOG_STDERR_TAIL=source 2/3 returned HTTP 503"));

        let history = db.hook_history(10).expect("hook history");
        assert_eq!(history.len(), 1);
        assert_eq!(history[0].status, "success");
        assert_eq!(history[0].event, "failure");

        let _ = std::fs::remove_file(env_file);
        crate::db::tests_support::remove(db);
    }

    #[test]
    fn a_hook_cleared_before_it_runs_is_skipped() {
        let db = crate::db::tests_support::test_db();
        db.set_hook_command(HOOK_FAILURE, Some("cronlog-no-such-program"))
            .expect("set hook");
        crate::db::tests_support::failed_run(&db, "report", "");
        db.set_hook_command(HOOK_FAILURE, None).expect("clear hook");

        dispatch_pending(&db).expect("dispatch");
        let history = db.hook_history(10).expect("hook history");
        assert_eq!(history[0].status, "skipped");
        // A skipped hook is not a failing one.
        assert!(db.last_finished_hook(None).expect("last").is_none());

        crate::db::tests_support::remove(db);
    }

    use crate::db::tests_support::{finished_run, remove, test_db};

    fn queued(db: &Database) -> Vec<(String, i64)> {
        let mut rows = db.hook_history(100).expect("hook history");
        rows.reverse();
        rows.into_iter()
            .map(|r| (r.event, r.failure_streak))
            .collect()
    }

    #[test]
    fn hooks_queue_only_when_a_job_starts_failing_or_recovers() {
        let db = test_db();
        db.set_hook_command(HOOK_FAILURE, Some("notify")).unwrap();
        db.set_hook_command(HOOK_RECOVERY, Some("notify")).unwrap();

        finished_run(&db, "sync", "success", Some(0), ""); // healthy: nothing
        finished_run(&db, "sync", "failed", Some(1), ""); // starts failing
        finished_run(&db, "sync", "timed_out", None, ""); // still failing: nothing
        finished_run(&db, "sync", "skipped", None, ""); // skips don't count
        finished_run(&db, "sync", "failed", Some(1), ""); // still failing
        finished_run(&db, "sync", "success", Some(0), ""); // recovers after 3
        finished_run(&db, "sync", "success", Some(0), ""); // healthy again: nothing
        finished_run(&db, "other", "failed", Some(1), ""); // another job's own streak

        assert_eq!(
            queued(&db),
            vec![
                ("failure".to_string(), 1),
                ("recovery".to_string(), 3),
                ("failure".to_string(), 1),
            ]
        );
        remove(db);
    }

    #[test]
    fn nothing_is_queued_for_an_event_with_no_hook() {
        let db = test_db();
        db.set_hook_command(HOOK_RECOVERY, Some("notify")).unwrap();
        finished_run(&db, "sync", "failed", Some(1), "");
        finished_run(&db, "sync", "success", Some(0), "");
        assert_eq!(queued(&db), vec![("recovery".to_string(), 1)]);
        remove(db);
    }

    #[test]
    fn an_interrupted_run_queues_the_failure_hook() {
        let db = test_db();
        db.set_hook_command(HOOK_FAILURE, Some("notify")).unwrap();
        db.add_job(
            "sync",
            &["echo".into()],
            "every 1 hours",
            3600,
            None,
            clock::now(),
        )
        .unwrap();
        let job = db.get_job_by_name("sync").unwrap();
        let when = clock::now();
        db.create_run(job.id, "running", when, Some(when), "schedule")
            .unwrap();
        db.interrupt_stale_running_runs(when, "daemon restarted while run was active")
            .unwrap();
        assert_eq!(queued(&db), vec![("failure".to_string(), 1)]);
        remove(db);
    }

    #[test]
    fn a_queued_hook_is_claimed_once() {
        let db = test_db();
        db.set_hook_command(HOOK_FAILURE, Some("notify")).unwrap();
        finished_run(&db, "sync", "failed", Some(1), "boom");
        let first = db.claim_pending_hook(clock::now()).unwrap();
        assert!(first.is_some());
        assert!(db.claim_pending_hook(clock::now()).unwrap().is_none());
        assert_eq!(first.unwrap().stderr.as_deref(), Some("boom"));
        remove(db);
    }

    #[test]
    fn a_hook_left_running_by_a_stopped_process_is_marked_interrupted() {
        let db = test_db();
        db.set_hook_command(HOOK_FAILURE, Some("notify")).unwrap();
        finished_run(&db, "sync", "failed", Some(1), "");
        let then = clock::now() - chrono::Duration::minutes(5);
        db.claim_pending_hook(then).unwrap().expect("claimed");
        assert_eq!(
            db.interrupt_stale_hooks(clock::now() - chrono::Duration::minutes(1), clock::now())
                .unwrap(),
            1
        );
        assert_eq!(db.hook_history(1).unwrap()[0].status, "interrupted");
        remove(db);
    }

    #[test]
    fn health_reports_a_failing_hook_until_one_succeeds() {
        let db = test_db();
        let options = crate::health::HealthOptions {
            job: None,
            grace: chrono::Duration::minutes(5),
            failures: 100,
            daemon_stale: chrono::Duration::minutes(5),
        };
        let kinds = |db: &Database| -> Vec<&'static str> {
            crate::health::check(db, clock::now(), &options)
                .unwrap()
                .problems
                .iter()
                .map(|p| p.kind)
                .collect()
        };

        let failing_hook = shell_words::join(exit_with(4).iter().map(String::as_str));
        db.set_hook_command(HOOK_FAILURE, Some(&failing_hook))
            .unwrap();
        finished_run(&db, "sync", "failed", Some(1), "");
        dispatch_pending(&db).unwrap();
        assert!(kinds(&db).contains(&"hook_failing"));

        let working_hook = shell_words::join(exit_with(0).iter().map(String::as_str));
        db.set_hook_command(HOOK_RECOVERY, Some(&working_hook))
            .unwrap();
        finished_run(&db, "sync", "success", Some(0), "");
        dispatch_pending(&db).unwrap();
        assert!(!kinds(&db).contains(&"hook_failing"));
        remove(db);
    }

    #[test]
    fn the_latest_hook_is_the_last_to_finish_and_can_be_scoped_to_a_job() {
        let db = test_db();
        db.set_hook_command(HOOK_FAILURE, Some("notify")).unwrap();
        finished_run(&db, "alpha", "failed", Some(1), "");
        finished_run(&db, "beta", "failed", Some(1), "");
        let now = clock::now();
        let first = db.claim_pending_hook(now).unwrap().expect("alpha's hook");
        let second = db.claim_pending_hook(now).unwrap().expect("beta's hook");
        // Run side by side: the newer hook fails first, the older one then succeeds.
        db.finish_hook(
            second.id,
            "failed",
            now + chrono::Duration::seconds(1),
            5,
            Some(1),
            Some("down"),
        )
        .unwrap();
        db.finish_hook(
            first.id,
            "success",
            now + chrono::Duration::seconds(2),
            5,
            Some(0),
            None,
        )
        .unwrap();

        let latest = db.last_finished_hook(None).unwrap().expect("latest");
        assert_eq!(
            (latest.hook_id, latest.status.as_str()),
            (first.id, "success")
        );
        let beta = db.last_finished_hook(Some("beta")).unwrap().expect("beta");
        assert_eq!((beta.hook_id, beta.status.as_str()), (second.id, "failed"));
        assert!(db.last_finished_hook(Some("gamma")).unwrap().is_none());
        remove(db);
    }

    #[test]
    fn pruning_a_run_removes_its_hook_rows() {
        let db = test_db();
        db.set_hook_command(HOOK_FAILURE, Some("notify")).unwrap();
        finished_run(&db, "sync", "failed", Some(1), "");
        finished_run(&db, "sync", "success", Some(0), "");
        db.prune_batch(clock::now() + chrono::Duration::days(1), 1, 100)
            .unwrap();
        assert!(db.hook_history(10).unwrap().is_empty());
        remove(db);
    }
}
