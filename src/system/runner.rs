use std::{
    fmt,
    io::{self, Read},
    os::{fd::AsRawFd, unix::process::CommandExt},
    process::{Command, Stdio},
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
    thread,
    time::{Duration, Instant},
};

const MAX_OUTPUT_BYTES: usize = 64 * 1024;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CommandSpec {
    pub program: String,
    pub args: Vec<String>,
    pub env: Vec<(String, String)>,
    pub timeout: Duration,
    pub max_output_bytes: usize,
}

impl CommandSpec {
    pub fn new<I, S>(program: impl Into<String>, args: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        Self {
            program: program.into(),
            args: args.into_iter().map(Into::into).collect(),
            env: vec![("LC_ALL".to_owned(), "C".to_owned())],
            timeout: Duration::from_millis(900),
            max_output_bytes: MAX_OUTPUT_BYTES,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CommandOutput {
    pub status: i32,
    pub stdout: Vec<u8>,
    pub stderr: Vec<u8>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RunnerErrorKind {
    Timeout,
    Cancelled,
    Spawn,
    Io,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RunnerError {
    kind: RunnerErrorKind,
    message: String,
}

impl RunnerError {
    pub fn timeout(message: impl Into<String>) -> Self {
        Self::new(RunnerErrorKind::Timeout, message)
    }

    pub fn spawn(message: impl Into<String>) -> Self {
        Self::new(RunnerErrorKind::Spawn, message)
    }

    pub(crate) fn cancelled() -> Self {
        Self::new(RunnerErrorKind::Cancelled, "stale request was cancelled")
    }

    fn new(kind: RunnerErrorKind, message: impl Into<String>) -> Self {
        Self {
            kind,
            message: message.into(),
        }
    }

    pub fn kind(&self) -> RunnerErrorKind {
        self.kind
    }

    pub fn message(&self) -> &str {
        &self.message
    }
}

impl fmt::Display for RunnerError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.message)
    }
}

impl std::error::Error for RunnerError {}

pub trait CommandRunner: Clone + Send + Sync + 'static {
    fn run(&self, command: &CommandSpec) -> Result<CommandOutput, RunnerError>;

    fn run_controlled(
        &self,
        command: &CommandSpec,
        control: &RunControl,
    ) -> Result<CommandOutput, RunnerError> {
        if control.is_cancelled() {
            return Err(RunnerError::cancelled());
        }
        self.run(command)
    }

    fn run_controlled_started(
        &self,
        command: &CommandSpec,
        control: &RunControl,
    ) -> Result<(CommandOutput, Instant), RunnerError> {
        let started_at = Instant::now();
        self.run_controlled(command, control)
            .map(|output| (output, started_at))
    }
}

#[derive(Clone)]
pub struct RunControl {
    deadline: Instant,
    generation: Option<u64>,
    latest_generation: Option<Arc<AtomicU64>>,
    cancelled: Option<Arc<std::sync::atomic::AtomicBool>>,
}

impl RunControl {
    pub fn for_generation(
        deadline: Instant,
        generation: u64,
        latest_generation: Arc<AtomicU64>,
    ) -> Self {
        Self {
            deadline,
            generation: Some(generation),
            latest_generation: Some(latest_generation),
            cancelled: None,
        }
    }

    pub fn for_timeout(timeout: Duration) -> Self {
        Self {
            deadline: Instant::now() + timeout,
            generation: None,
            latest_generation: None,
            cancelled: None,
        }
    }

    pub fn for_request(deadline: Instant, cancelled: Arc<std::sync::atomic::AtomicBool>) -> Self {
        Self {
            deadline,
            generation: None,
            latest_generation: None,
            cancelled: Some(cancelled),
        }
    }

    pub fn is_cancelled(&self) -> bool {
        self.cancelled
            .as_ref()
            .is_some_and(|cancelled| cancelled.load(Ordering::SeqCst))
            || self
                .generation
                .zip(self.latest_generation.as_ref())
                .is_some_and(|(generation, latest)| generation < latest.load(Ordering::SeqCst))
    }

    pub fn remaining(&self) -> Duration {
        self.deadline.saturating_duration_since(Instant::now())
    }
}

#[derive(Debug, Clone, Copy, Default)]
pub struct ProcessCommandRunner;

impl CommandRunner for ProcessCommandRunner {
    fn run(&self, spec: &CommandSpec) -> Result<CommandOutput, RunnerError> {
        self.execute(spec, &RunControl::for_timeout(spec.timeout))
            .map(|(output, _)| output)
    }

    fn run_controlled(
        &self,
        spec: &CommandSpec,
        control: &RunControl,
    ) -> Result<CommandOutput, RunnerError> {
        self.execute(spec, control).map(|(output, _)| output)
    }

    fn run_controlled_started(
        &self,
        spec: &CommandSpec,
        control: &RunControl,
    ) -> Result<(CommandOutput, Instant), RunnerError> {
        self.execute(spec, control)
    }
}

impl ProcessCommandRunner {
    fn execute(
        &self,
        spec: &CommandSpec,
        control: &RunControl,
    ) -> Result<(CommandOutput, Instant), RunnerError> {
        if control.is_cancelled() {
            return Err(RunnerError::cancelled());
        }
        let mut command = Command::new(&spec.program);
        command
            .args(&spec.args)
            .stdin(Stdio::null())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .process_group(0);
        for (key, value) in &spec.env {
            command.env(key, value);
        }
        ensure_process_subreaper()?;
        let started = Instant::now();
        let mut child = command.spawn().map_err(|error| {
            RunnerError::new(
                RunnerErrorKind::Spawn,
                format!("adapter executable is unavailable: {error}"),
            )
        })?;
        let process_group = child.id() as libc::pid_t;
        let mut stdout = child.stdout.take().expect("piped stdout");
        let mut stderr = child.stderr.take().expect("piped stderr");
        if let Err(error) = set_nonblocking(stdout.as_raw_fd()) {
            terminate_process_boundary(&mut child, process_group);
            return Err(output_error(error));
        }
        if let Err(error) = set_nonblocking(stderr.as_raw_fd()) {
            terminate_process_boundary(&mut child, process_group);
            return Err(output_error(error));
        }
        let mut stdout_capture = OutputCapture::new(spec.max_output_bytes);
        let mut stderr_capture = OutputCapture::new(spec.max_output_bytes);
        let status = loop {
            if let Err(error) = stdout_capture.read_available(&mut stdout) {
                terminate_process_boundary(&mut child, process_group);
                return Err(error);
            }
            if let Err(error) = stderr_capture.read_available(&mut stderr) {
                terminate_process_boundary(&mut child, process_group);
                return Err(error);
            }
            if control.is_cancelled() {
                terminate_process_boundary(&mut child, process_group);
                drain_closed_boundary(
                    &mut stdout,
                    &mut stderr,
                    &mut stdout_capture,
                    &mut stderr_capture,
                );
                return Err(RunnerError::cancelled());
            }
            match child.try_wait() {
                Ok(Some(status)) => {
                    kill_process_group(process_group);
                    reap_process_group(process_group);
                    drain_closed_boundary(
                        &mut stdout,
                        &mut stderr,
                        &mut stdout_capture,
                        &mut stderr_capture,
                    );
                    break status;
                }
                Ok(None) if started.elapsed() < spec.timeout && !control.remaining().is_zero() => {
                    thread::sleep(Duration::from_millis(5));
                }
                Ok(None) => {
                    terminate_process_boundary(&mut child, process_group);
                    drain_closed_boundary(
                        &mut stdout,
                        &mut stderr,
                        &mut stdout_capture,
                        &mut stderr_capture,
                    );
                    return Err(RunnerError::timeout(
                        "adapter command exceeded its deadline",
                    ));
                }
                Err(error) => {
                    terminate_process_boundary(&mut child, process_group);
                    drain_closed_boundary(
                        &mut stdout,
                        &mut stderr,
                        &mut stdout_capture,
                        &mut stderr_capture,
                    );
                    return Err(RunnerError::new(
                        RunnerErrorKind::Io,
                        format!("could not wait for adapter command: {error}"),
                    ));
                }
            }
        };
        let stdout = stdout_capture.finish()?;
        let stderr = stderr_capture.finish()?;
        Ok((
            CommandOutput {
                status: status.code().unwrap_or(128),
                stdout,
                stderr,
            },
            started,
        ))
    }
}

fn terminate_process_boundary(child: &mut std::process::Child, process_group: libc::pid_t) {
    kill_process_group(process_group);
    let _ = child.kill();
    let _ = child.wait();
    reap_process_group(process_group);
}

fn kill_process_group(process_group: libc::pid_t) {
    if process_group > 0 {
        unsafe {
            libc::kill(-process_group, libc::SIGKILL);
        }
    }
}

fn ensure_process_subreaper() -> Result<(), RunnerError> {
    if unsafe { libc::prctl(libc::PR_SET_CHILD_SUBREAPER, 1, 0, 0, 0) } == 0 {
        Ok(())
    } else {
        Err(RunnerError::new(
            RunnerErrorKind::Io,
            format!(
                "could not establish the adapter process boundary: {}",
                io::Error::last_os_error()
            ),
        ))
    }
}

fn reap_process_group(process_group: libc::pid_t) {
    let deadline = Instant::now() + Duration::from_millis(50);
    loop {
        let mut status = 0;
        let reaped = unsafe { libc::waitpid(-process_group, &mut status, libc::WNOHANG) };
        if reaped > 0 {
            continue;
        }
        let group_exists = unsafe { libc::kill(-process_group, 0) } == 0;
        if !group_exists || Instant::now() >= deadline {
            return;
        }
        thread::sleep(Duration::from_millis(1));
    }
}

fn drain_closed_boundary(
    stdout: &mut impl Read,
    stderr: &mut impl Read,
    stdout_capture: &mut OutputCapture,
    stderr_capture: &mut OutputCapture,
) {
    let deadline = Instant::now() + Duration::from_millis(50);
    loop {
        let stdout_closed = stdout_capture.read_available(stdout).unwrap_or(true);
        let stderr_closed = stderr_capture.read_available(stderr).unwrap_or(true);
        if stdout_closed && stderr_closed || Instant::now() >= deadline {
            return;
        }
        thread::sleep(Duration::from_millis(1));
    }
}

struct OutputCapture {
    bytes: Vec<u8>,
    limit: usize,
    exceeded: bool,
}

impl OutputCapture {
    fn new(limit: usize) -> Self {
        Self {
            bytes: Vec::new(),
            limit,
            exceeded: false,
        }
    }

    fn read_available(&mut self, reader: &mut impl Read) -> Result<bool, RunnerError> {
        let mut chunk = [0_u8; 8192];
        for _ in 0..16 {
            match reader.read(&mut chunk) {
                Ok(0) => return Ok(true),
                Ok(count) => {
                    let remaining = self.limit.saturating_sub(self.bytes.len());
                    self.bytes.extend_from_slice(&chunk[..count.min(remaining)]);
                    self.exceeded |= count > remaining;
                }
                Err(error) if error.kind() == io::ErrorKind::WouldBlock => return Ok(false),
                Err(error) => return Err(output_error(error)),
            }
        }
        Ok(false)
    }

    fn finish(self) -> Result<Vec<u8>, RunnerError> {
        if self.exceeded {
            Err(RunnerError::new(
                RunnerErrorKind::Io,
                "adapter output exceeded the bounded capture limit",
            ))
        } else {
            Ok(self.bytes)
        }
    }
}

fn set_nonblocking(fd: std::os::fd::RawFd) -> io::Result<()> {
    let flags = unsafe { libc::fcntl(fd, libc::F_GETFL) };
    if flags < 0 {
        return Err(io::Error::last_os_error());
    }
    if unsafe { libc::fcntl(fd, libc::F_SETFL, flags | libc::O_NONBLOCK) } < 0 {
        return Err(io::Error::last_os_error());
    }
    Ok(())
}

fn output_error(error: io::Error) -> RunnerError {
    RunnerError::new(
        RunnerErrorKind::Io,
        format!("could not read adapter output: {error}"),
    )
}
