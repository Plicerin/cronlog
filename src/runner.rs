use crate::db::Job;
use crate::error::{CronlogError, Result};
use chrono::Local;
use std::io::Read;
use std::process::{Child, Command, Stdio};
use std::sync::{mpsc, Arc, Mutex};
use std::thread;
use std::time::{Duration, Instant};
use wait_timeout::ChildExt;

const MAX_STDOUT_BYTES: usize = 256 * 1024;
const MAX_STDERR_BYTES: usize = 256 * 1024;

/// How long to keep reading output after the job exits (or is killed). A process the job
/// started in the background can inherit stdout/stderr and keep them open for hours; the
/// run must still finish.
const DRAIN_GRACE: Duration = Duration::from_secs(5);

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

/// Output read from one stream: the first `limit` bytes, and whether there was more.
#[derive(Debug, Default)]
struct Captured {
    bytes: Vec<u8>,
    truncated: bool,
    error: Option<String>,
}

/// A stream being read on its own thread: what it has read so far, and a signal at EOF.
struct Reader {
    name: &'static str,
    captured: Arc<Mutex<Captured>>,
    done: mpsc::Receiver<()>,
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
        // A job that reads stdin would otherwise wait on the daemon's terminal.
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());
    // Own process group, so a timeout can kill everything the job started.
    #[cfg(unix)]
    {
        use std::os::unix::process::CommandExt;
        command.process_group(0);
    }

    if let Some(previous_run_id) = context.previous_run_id {
        command.env("CRONLOG_PREVIOUS_RUN_ID", previous_run_id.to_string());
    }
    if let Some(previous_status) = &context.previous_status {
        command.env("CRONLOG_PREVIOUS_STATUS", previous_status);
    }

    let mut child = command.spawn().map_err(|e| {
        CronlogError::InvalidCommand(format!("failed to spawn '{}': {e}", job.command[0]))
    })?;

    // Read both streams while the job runs. Reading only after it exits deadlocks as soon as
    // the job writes more than the OS pipe buffer (64 KiB on Linux): the job blocks on
    // write, never exits, and is killed as timed_out.
    let stdout_reader = child.stdout.take().map(|stream| {
        spawn_reader(
            stream,
            max_log_bytes("CRONLOG_MAX_STDOUT_BYTES", MAX_STDOUT_BYTES),
            "stdout",
        )
    });
    let stderr_reader = child.stderr.take().map(|stream| {
        spawn_reader(
            stream,
            max_log_bytes("CRONLOG_MAX_STDERR_BYTES", MAX_STDERR_BYTES),
            "stderr",
        )
    });

    let timeout = Duration::from_secs(job.timeout_seconds.max(1) as u64);
    let wait_result = child.wait_timeout(timeout)?;

    let timed_out = wait_result.is_none();
    if timed_out {
        kill_process_tree(&mut child);
        let _ = child.wait();
    }

    let mut log_errors = Vec::new();
    // One deadline for both streams, so a held-open stdout and stderr cost DRAIN_GRACE once.
    let drain_deadline = Instant::now() + DRAIN_GRACE;
    let stdout = collect(stdout_reader, drain_deadline, &mut log_errors);
    let mut stderr = collect(stderr_reader, drain_deadline, &mut log_errors);
    if !log_errors.is_empty() {
        stderr
            .bytes
            .extend_from_slice(b"\n[cronlog log capture warning]\n");
        stderr
            .bytes
            .extend_from_slice(log_errors.join("\n").as_bytes());
        stderr.bytes.push(b'\n');
    }

    let (stdout, stdout_truncated) = to_logged_string(
        stdout,
        max_log_bytes("CRONLOG_MAX_STDOUT_BYTES", MAX_STDOUT_BYTES),
    );
    let (stderr, stderr_truncated) = to_logged_string(
        stderr,
        max_log_bytes("CRONLOG_MAX_STDERR_BYTES", MAX_STDERR_BYTES),
    );
    let duration_ms = start.elapsed().as_millis() as i64;
    let log_capture_error = if log_errors.is_empty() {
        None
    } else {
        Some(log_errors.join("; "))
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

/// Drain a stream on its own thread: keep the first `limit` bytes, discard the rest, and
/// keep reading so the job never blocks on a full pipe.
fn spawn_reader<R: Read + Send + 'static>(
    mut source: R,
    limit: usize,
    name: &'static str,
) -> Reader {
    let captured = Arc::new(Mutex::new(Captured::default()));
    let (done_tx, done) = mpsc::channel();
    let shared = Arc::clone(&captured);
    thread::spawn(move || {
        let mut buf = [0u8; 8192];
        loop {
            match source.read(&mut buf) {
                Ok(0) => break,
                Ok(n) => {
                    let mut captured = shared.lock().unwrap_or_else(|e| e.into_inner());
                    let room = limit.saturating_sub(captured.bytes.len());
                    if n > room {
                        captured.truncated = true;
                    }
                    captured.bytes.extend_from_slice(&buf[..n.min(room)]);
                }
                Err(err) if err.kind() == std::io::ErrorKind::Interrupted => continue,
                Err(err) => {
                    let mut captured = shared.lock().unwrap_or_else(|e| e.into_inner());
                    captured.error = Some(format!("failed to read {name}: {err}"));
                    break;
                }
            }
        }
        let _ = done_tx.send(());
    });
    Reader {
        name,
        captured,
        done,
    }
}

/// What a reader captured. Waits until `deadline` for EOF; past that, keeps what was read
/// so far and notes that the stream was still open.
fn collect(reader: Option<Reader>, deadline: Instant, errors: &mut Vec<String>) -> Captured {
    let Some(reader) = reader else {
        return Captured::default();
    };
    let wait = deadline.saturating_duration_since(Instant::now());
    if reader.done.recv_timeout(wait).is_err() {
        errors.push(format!(
            "{} was still open {}s after the job ended (a background process kept it); \
             output after that point was not captured",
            reader.name,
            DRAIN_GRACE.as_secs()
        ));
    }
    let mut captured = reader.captured.lock().unwrap_or_else(|e| e.into_inner());
    let taken = std::mem::take(&mut *captured);
    if let Some(err) = &taken.error {
        errors.push(err.clone());
    }
    taken
}

fn to_logged_string(captured: Captured, max_bytes: usize) -> (String, bool) {
    let mut s = String::from_utf8_lossy(&captured.bytes).to_string();
    if captured.truncated {
        s.push_str(&format!(
            "\n...[truncated by cronlog at {max_bytes} bytes; full output was larger]"
        ));
    }
    (s, captured.truncated)
}

/// Kill the job and everything it started. Killing only the direct child leaves the work
/// running when the command is a wrapper (`bash -lc 'python ...'`), and the orphans keep
/// stdout/stderr open, so the timeout did not end the run.
#[cfg(unix)]
fn kill_process_tree(child: &mut Child) {
    // The child leads its own process group (process_group(0) above): its pid is the pgid.
    let pgid = child.id() as libc::pid_t;
    // SAFETY: a plain kill(2); a negative pid means "every process in group pgid".
    unsafe {
        libc::kill(-pgid, libc::SIGKILL);
    }
    let _ = child.kill();
}

#[cfg(windows)]
fn kill_process_tree(child: &mut Child) {
    // taskkill /T walks the tree from the parent, so it must run before the parent dies.
    let _ = Command::new("taskkill")
        .args(["/T", "/F", "/PID", &child.id().to_string()])
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status();
    let _ = child.kill();
}

#[cfg(not(any(unix, windows)))]
fn kill_process_tree(child: &mut Child) {
    let _ = child.kill();
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

#[cfg(test)]
mod tests {
    use super::*;

    fn job(command: &[&str], timeout_seconds: i64) -> Job {
        Job {
            id: 1,
            name: "test".into(),
            command: command.iter().map(|s| s.to_string()).collect(),
            schedule: "every 1 hours".into(),
            timeout_seconds,
            max_runs: None,
            next_run_at: None,
        }
    }

    #[cfg(unix)]
    fn big_output() -> Vec<&'static str> {
        vec![
            "sh",
            "-c",
            "head -c 300000 /dev/zero | tr '\\0' x; echo done >&2",
        ]
    }

    #[cfg(windows)]
    fn big_output() -> Vec<&'static str> {
        vec![
            "powershell",
            "-NoProfile",
            "-Command",
            "[Console]::Out.Write('x' * 300000); [Console]::Error.WriteLine('done')",
        ]
    }

    #[test]
    fn large_output_does_not_block_the_job() {
        let out = execute(&job(&big_output(), 30), &ExecuteContext::default()).expect("run");
        assert_eq!(out.status, "success", "error: {:?}", out.error);
        assert!(out.stdout_truncated);
        assert!(out.stdout.len() >= MAX_STDOUT_BYTES);
        assert!(out.stderr.contains("done"));
        assert!(out.duration_ms < 25_000, "took {} ms", out.duration_ms);
    }

    #[cfg(unix)]
    #[test]
    fn timeout_kills_the_whole_process_tree() {
        // sh runs sleep as a child (more commands follow), like `bash -lc 'python ...'`.
        let out = execute(
            &job(&["sh", "-c", "sleep 30; echo finished"], 1),
            &ExecuteContext::default(),
        )
        .expect("run");
        assert_eq!(out.status, "timed_out");
        assert!(out.duration_ms < 4_000, "took {} ms", out.duration_ms);
        assert!(!out.stdout.contains("finished"));
    }

    #[cfg(windows)]
    #[test]
    fn timeout_kills_the_whole_process_tree() {
        let out = execute(
            &job(
                &["cmd", "/C", "ping -n 30 127.0.0.1 >NUL & echo finished"],
                1,
            ),
            &ExecuteContext::default(),
        )
        .expect("run");
        assert_eq!(out.status, "timed_out");
        assert!(out.duration_ms < 6_000, "took {} ms", out.duration_ms);
        assert!(!out.stdout.contains("finished"));
    }

    #[cfg(unix)]
    #[test]
    fn a_background_process_holding_output_does_not_hang_the_run() {
        let out = execute(
            &job(&["sh", "-c", "sleep 30 & echo started"], 60),
            &ExecuteContext::default(),
        )
        .expect("run");
        assert_eq!(out.status, "success");
        assert!(
            out.stdout.contains("started"),
            "kept the output read so far"
        );
        assert!(out.duration_ms < DRAIN_GRACE.as_millis() as i64 + 3_000);
        assert!(out
            .error
            .as_deref()
            .unwrap_or_default()
            .contains("still open"));
    }
}
