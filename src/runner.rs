use crate::db::Job;
use crate::error::{Cron2Error, Result};
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

pub fn execute(job: &Job) -> Result<RunOutput> {
    if job.command.is_empty() {
        return Err(Cron2Error::InvalidCommand(format!(
            "job '{}' command is empty",
            job.name
        )));
    }

    let start = Instant::now();
    let mut child = Command::new(&job.command[0])
        .args(&job.command[1..])
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .map_err(|e| {
            Cron2Error::InvalidCommand(format!("failed to spawn '{}': {e}", job.command[0]))
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

    if let Some(mut stdout) = child.stdout.take() {
        let _ = stdout.read_to_end(&mut stdout_bytes);
    }
    if let Some(mut stderr) = child.stderr.take() {
        let _ = stderr.read_to_end(&mut stderr_bytes);
    }

    let (stdout, stdout_truncated) = bytes_to_limited_string(stdout_bytes, MAX_STDOUT_BYTES);
    let (stderr, stderr_truncated) = bytes_to_limited_string(stderr_bytes, MAX_STDERR_BYTES);
    let duration_ms = start.elapsed().as_millis() as i64;

    if timed_out {
        return Ok(RunOutput {
            status: "timed_out".into(),
            exit_code: None,
            stdout,
            stderr,
            stdout_truncated,
            stderr_truncated,
            duration_ms,
            error: Some(format!(
                "job timed out after {} seconds",
                job.timeout_seconds
            )),
        });
    }

    let status = wait_result.ok_or_else(|| {
        Cron2Error::Io(std::io::Error::new(
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
        error: None,
    })
}

fn bytes_to_limited_string(mut bytes: Vec<u8>, max_bytes: usize) -> (String, bool) {
    let truncated = bytes.len() > max_bytes;
    if truncated {
        bytes.truncate(max_bytes);
    }
    let mut s = String::from_utf8_lossy(&bytes).to_string();
    if truncated {
        s.push_str("\n...[truncated]");
    }
    (s, truncated)
}

pub fn now() -> chrono::NaiveDateTime {
    Local::now().naive_local()
}
