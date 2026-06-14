use crate::db::Job;
use crate::error::{CronlogError, Result};
use chrono::Local;
use std::io::Read;
use std::process::{Command, Stdio};
use std::time::{Duration, Instant};
use wait_timeout::ChildExt;

const MAX_STDOUT_BYTES: usize = 256 * 1024;
const MAX_STDERR_BYTES: usize = 256 * 1024;

#[derive(Debug)]
pub struct RunOutput {
    pub status: String,
    pub exit_code: Option<i64>,
    pub stdout: String,
    pub stderr: String,
    pub stdout_truncated: bool,
    pub stderr_truncated: bool,
    pub duration_ms: i64,
    pub error: Option<String>,
}

#[derive(Debug, Default)]
pub struct ExecuteContext {
    pub run_id: i64,
    pub job_name: String,
    pub scheduled_for: String,
    pub trigger_type: String,
    pub previous_run_id: Option<i64>,
    pub previous_status: Option<String>,
}

pub fn execute(job: &Job, context: &ExecuteContext) -> Result<RunOutput> {
    if job.command.is_empty() {
        return Err(CronlogError::InvalidCommand(format!(
            "job '{}' command is empty",
            job.name
        )));
    }

    let start = Instant::now();
    let mut command = Command::new(&job.command[0]);
    command
        .args(&job.command[1..])
        .env("CRONLOG_RUN_ID", context.run_id.to_string())
        .env("CRONLOG_JOB_NAME", &context.job_name)
        .env("CRONLOG_SCHEDULED_FOR", &context.scheduled_for)
        .env("CRONLOG_TRIGGER_TYPE", &context.trigger_type)
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());

    if let Some(previous_run_id) = context.previous_run_id {
        command.env("CRONLOG_PREVIOUS_RUN_ID", previous_run_id.to_string());
    }
    if let Some(previous_status) = &context.previous_status {
        command.env("CRONLOG_PREVIOUS_STATUS", previous_status);
    }

    let mut child = command.spawn().map_err(|e| {
        CronlogError::InvalidCommand(format!("failed to spawn '{}': {e}", job.command[0]))
    })?;

    let timeout = Duration::from_secs(job.timeout_seconds.max(1) as u64);
    let wait_result = child.wait_timeout(timeout)?;

    let timed_out = wait_result.is_none();
    if timed_out {
        let _ = child.kill();
        let _ = child.wait();
    }

    let mut stdout_bytes = Vec::new();
    let mut stderr_bytes = Vec::new();

    let mut log_read_errors = Vec::new();
    if let Some(mut stdout) = child.stdout.take() {
        if let Err(err) = stdout.read_to_end(&mut stdout_bytes) {
            log_read_errors.push(format!("failed to read stdout: {err}"));
        }
    }
    if let Some(mut stderr) = child.stderr.take() {
        if let Err(err) = stderr.read_to_end(&mut stderr_bytes) {
            log_read_errors.push(format!("failed to read stderr: {err}"));
        }
    }

    if !log_read_errors.is_empty() {
        stderr_bytes.extend_from_slice(b"\n[cronlog log capture warning]\n");
        stderr_bytes.extend_from_slice(log_read_errors.join("\n").as_bytes());
        stderr_bytes.push(b'\n');
    }

    let (stdout, stdout_truncated) = bytes_to_limited_string(
        stdout_bytes,
        max_log_bytes("CRONLOG_MAX_STDOUT_BYTES", MAX_STDOUT_BYTES),
    );
    let (stderr, stderr_truncated) = bytes_to_limited_string(
        stderr_bytes,
        max_log_bytes("CRONLOG_MAX_STDERR_BYTES", MAX_STDERR_BYTES),
    );
    let duration_ms = start.elapsed().as_millis() as i64;
    let log_capture_error = if log_read_errors.is_empty() {
        None
    } else {
        Some(log_read_errors.join("; "))
    };

    if timed_out {
        return Ok(RunOutput {
            status: "timed_out".into(),
            exit_code: None,
            stdout,
            stderr,
            stdout_truncated,
            stderr_truncated,
            duration_ms,
            error: Some(
                [
                    Some(format!(
                        "job timed out after {} seconds",
                        job.timeout_seconds
                    )),
                    log_capture_error,
                ]
                .into_iter()
                .flatten()
                .collect::<Vec<_>>()
                .join("; "),
            ),
        });
    }

    let status = wait_result.ok_or_else(|| {
        CronlogError::Io(std::io::Error::new(
            std::io::ErrorKind::Other,
            "process status unavailable",
        ))
    })?;
    let exit_code = status.code().map(|c| c as i64);
    let run_status = if exit_code == Some(0) {
        "success"
    } else {
        "failed"
    };

    Ok(RunOutput {
        status: run_status.into(),
        exit_code,
        stdout,
        stderr,
        stdout_truncated,
        stderr_truncated,
        duration_ms,
        error: log_capture_error,
    })
}

fn bytes_to_limited_string(mut bytes: Vec<u8>, max_bytes: usize) -> (String, bool) {
    let truncated = bytes.len() > max_bytes;
    if truncated {
        bytes.truncate(max_bytes);
    }
    let mut s = String::from_utf8_lossy(&bytes).to_string();
    if truncated {
        s.push_str(&format!(
            "\n...[truncated by cronlog at {max_bytes} bytes; full output was larger]"
        ));
    }
    (s, truncated)
}

fn max_log_bytes(env_key: &str, default: usize) -> usize {
    std::env::var(env_key)
        .ok()
        .and_then(|value| value.parse::<usize>().ok())
        .filter(|value| *value > 0)
        .unwrap_or(default)
}

pub fn now() -> chrono::NaiveDateTime {
    Local::now().naive_local()
}
