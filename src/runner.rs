use crate::db::Job;
use crate::error::{CronlogError, Result};
use chrono::Local;
use std::io::Read;
use std::process::{Child, Command, Stdio};
use std::sync::atomic::{AtomicBool, Ordering};
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

/// A stream being read on its own thread: what it has read so far, a signal at EOF, and a
/// flag that stops it.
struct Reader {
    name: &'static str,
    captured: Arc<Mutex<Captured>>,
    cancel: Arc<AtomicBool>,
    done: mpsc::Receiver<()>,
    /// Taken by `collect`; still here if the run errored or unwound before collecting.
    handle: Option<thread::JoinHandle<()>>,
}

impl Drop for Reader {
    /// A reader dropped without `collect` (an error or a panic mid-run) is stopped and
    /// joined too, so its thread, pipe and buffer never outlive the run.
    fn drop(&mut self) {
        self.cancel.store(true, Ordering::Relaxed);
        if let Some(handle) = self.handle.take() {
            let _ = handle.join();
        }
    }
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
    let wait_result = match child.wait_timeout(timeout) {
        Ok(result) => result,
        Err(err) => {
            // Don't leave the job running unwatched; the readers stop when dropped.
            kill_process_tree(&mut child);
            let _ = child.wait();
            return Err(err.into());
        }
    };

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

/// How often a reader wakes to check for cancellation while its stream is quiet.
const POLL_INTERVAL: Duration = Duration::from_millis(100);

/// Drain a stream on its own thread: keep the first `limit` bytes, discard the rest, and
/// keep reading so the job never blocks on a full pipe.
///
/// Reads only after the stream reports data (or EOF), so the thread can notice `cancel`
/// and exit. A blocking read could not be interrupted: when a background process held the
/// pipe open past DRAIN_GRACE, the thread, the pipe and a fresh capture buffer outlived
/// the run, one set per run of a recurring job.
fn spawn_reader<R: PollRead + Send + 'static>(
    mut source: R,
    limit: usize,
    name: &'static str,
) -> Reader {
    let captured = Arc::new(Mutex::new(Captured::default()));
    let cancel = Arc::new(AtomicBool::new(false));
    let (done_tx, done) = mpsc::channel();
    let shared = Arc::clone(&captured);
    let cancelled = Arc::clone(&cancel);
    let handle = thread::spawn(move || {
        let mut buf = [0u8; 8192];
        while !cancelled.load(Ordering::Relaxed) {
            match source.wait_readable(POLL_INTERVAL) {
                Ok(false) => continue,
                Ok(true) => {}
                Err(err) => {
                    let mut captured = shared.lock().unwrap_or_else(|e| e.into_inner());
                    captured.error = Some(format!("failed to read {name}: {err}"));
                    break;
                }
            }
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
        // `source` drops here, closing our end of the pipe.
        let _ = done_tx.send(());
    });
    Reader {
        name,
        captured,
        cancel,
        done,
        handle: Some(handle),
    }
}

/// What a reader captured. Waits until `deadline` for EOF; past that, stops the reader,
/// closes the stream and keeps what was read so far. The reader thread is always joined.
fn collect(reader: Option<Reader>, deadline: Instant, errors: &mut Vec<String>) -> Captured {
    let Some(mut reader) = reader else {
        return Captured::default();
    };
    let wait = deadline.saturating_duration_since(Instant::now());
    if reader.done.recv_timeout(wait).is_err() {
        reader.cancel.store(true, Ordering::Relaxed);
        errors.push(format!(
            "{} was still open {}s after the job ended (a background process kept it); \
             cronlog closed it, so output after that point was not captured",
            reader.name,
            DRAIN_GRACE.as_secs()
        ));
    }
    // Returns within one POLL_INTERVAL of cancel.
    if let Some(handle) = reader.handle.take() {
        let _ = handle.join();
    }
    let mut captured = reader.captured.lock().unwrap_or_else(|e| e.into_inner());
    let taken = std::mem::take(&mut *captured);
    if let Some(err) = &taken.error {
        errors.push(err.clone());
    }
    taken
}

/// A pipe that can wait for data with a timeout, so its reader can be cancelled.
trait PollRead: Read {
    /// Ok(true) when a read will not block (data or EOF), Ok(false) on timeout.
    fn wait_readable(&self, timeout: Duration) -> std::io::Result<bool>;
}

#[cfg(unix)]
impl<T: Read + std::os::fd::AsRawFd> PollRead for T {
    fn wait_readable(&self, timeout: Duration) -> std::io::Result<bool> {
        let mut fds = libc::pollfd {
            fd: self.as_raw_fd(),
            events: libc::POLLIN,
            revents: 0,
        };
        // SAFETY: one valid pollfd for an fd this value owns, for the duration of the call.
        let ready = unsafe { libc::poll(&mut fds, 1, timeout.as_millis() as libc::c_int) };
        if ready < 0 {
            let err = std::io::Error::last_os_error();
            return if err.kind() == std::io::ErrorKind::Interrupted {
                Ok(false)
            } else {
                Err(err)
            };
        }
        // POLLHUP/POLLERR also mean a read returns at once (EOF or the error).
        Ok(ready > 0)
    }
}

#[cfg(windows)]
impl<T: Read + std::os::windows::io::AsRawHandle> PollRead for T {
    fn wait_readable(&self, timeout: Duration) -> std::io::Result<bool> {
        #[link(name = "kernel32")]
        extern "system" {
            fn PeekNamedPipe(
                pipe: *mut std::ffi::c_void,
                buffer: *mut std::ffi::c_void,
                buffer_size: u32,
                bytes_read: *mut u32,
                total_available: *mut u32,
                bytes_left: *mut u32,
            ) -> i32;
        }
        let deadline = Instant::now() + timeout;
        loop {
            let mut available = 0u32;
            // SAFETY: peeks a pipe handle this value owns; only the count is written.
            let ok = unsafe {
                PeekNamedPipe(
                    self.as_raw_handle() as *mut std::ffi::c_void,
                    std::ptr::null_mut(),
                    0,
                    std::ptr::null_mut(),
                    &mut available,
                    std::ptr::null_mut(),
                )
            };
            // A failed peek is a closed pipe (EOF) or an error; the read reports which.
            if ok == 0 || available > 0 {
                return Ok(true);
            }
            if Instant::now() >= deadline {
                return Ok(false);
            }
            thread::sleep(Duration::from_millis(20));
        }
    }
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

    #[test]
    fn a_reader_past_its_deadline_is_stopped_and_its_pipe_closed() {
        use std::io::Write;
        let (pipe_reader, mut writer) = std::io::pipe().expect("pipe");
        let reader = spawn_reader(pipe_reader, 1024, "stdout");
        writer.write_all(b"before the deadline").expect("write");
        writer.flush().expect("flush");
        thread::sleep(Duration::from_millis(300));

        let started = Instant::now();
        let mut errors = Vec::new();
        // The writer stays open, as a background process would keep it.
        let captured = collect(Some(reader), Instant::now(), &mut errors);

        assert!(
            started.elapsed() < Duration::from_secs(2),
            "reader was joined promptly"
        );
        assert_eq!(captured.bytes, b"before the deadline");
        assert!(errors[0].contains("still open"));
        // Our end is closed, so the holder's next write fails instead of filling a pipe
        // nobody reads.
        assert!(pipe_closes(&mut writer));
    }

    #[test]
    fn a_reader_dropped_without_collect_is_stopped_and_its_pipe_closed() {
        use std::io::Write;
        let (pipe_reader, mut writer) = std::io::pipe().expect("pipe");
        let reader = spawn_reader(pipe_reader, 1024, "stdout");
        writer.write_all(b"partial").expect("write");

        // As when the run errors or unwinds before collecting.
        let started = Instant::now();
        drop(reader);

        assert!(
            started.elapsed() < Duration::from_secs(2),
            "reader was joined promptly"
        );
        assert!(pipe_closes(&mut writer));
    }

    /// Whether writes to `writer` start failing, i.e. nothing holds the read end any more.
    /// Allows a moment: a child another test is spawning in parallel holds a copy of every
    /// descriptor between its fork and exec.
    fn pipe_closes(writer: &mut std::io::PipeWriter) -> bool {
        use std::io::Write;
        (0..50).any(|_| {
            if writer.write_all(b"after").is_err() {
                return true;
            }
            thread::sleep(Duration::from_millis(20));
            false
        })
    }
}
