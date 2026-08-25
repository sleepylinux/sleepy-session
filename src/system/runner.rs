use std::{
    fmt,
    io::Read,
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

    fn for_timeout(timeout: Duration) -> Self {
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
    }

    fn run_controlled(
        &self,
        spec: &CommandSpec,
        control: &RunControl,
    ) -> Result<CommandOutput, RunnerError> {
        self.execute(spec, control)
    }
}

impl ProcessCommandRunner {
    fn execute(
        &self,
        spec: &CommandSpec,
        control: &RunControl,
    ) -> Result<CommandOutput, RunnerError> {
        if control.is_cancelled() {
            return Err(RunnerError::cancelled());
        }
        let mut command = Command::new(&spec.program);
        command
            .args(&spec.args)
            .stdin(Stdio::null())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped());
        for (key, value) in &spec.env {
            command.env(key, value);
        }
        let mut child = command.spawn().map_err(|error| {
            RunnerError::new(
                RunnerErrorKind::Spawn,
                format!("adapter executable is unavailable: {error}"),
            )
        })?;
        let stdout = child.stdout.take().expect("piped stdout");
        let stderr = child.stderr.take().expect("piped stderr");
        let capture_limit = spec.max_output_bytes;
        let stdout_reader = thread::spawn(move || read_capped(stdout, capture_limit));
        let stderr_reader = thread::spawn(move || read_capped(stderr, capture_limit));
        let started = Instant::now();
        let status = loop {
            if control.is_cancelled() {
                terminate_and_reap(&mut child, stdout_reader, stderr_reader);
                return Err(RunnerError::cancelled());
            }
            match child.try_wait() {
                Ok(Some(status)) => break status,
                Ok(None) if started.elapsed() < spec.timeout && !control.remaining().is_zero() => {
                    thread::sleep(Duration::from_millis(5));
                }
                Ok(None) => {
                    terminate_and_reap(&mut child, stdout_reader, stderr_reader);
                    return Err(RunnerError::timeout(
                        "adapter command exceeded its deadline",
                    ));
                }
                Err(error) => {
                    terminate_and_reap(&mut child, stdout_reader, stderr_reader);
                    return Err(RunnerError::new(
                        RunnerErrorKind::Io,
                        format!("could not wait for adapter command: {error}"),
                    ));
                }
            }
        };
        let stdout = stdout_reader
            .join()
            .map_err(|_| RunnerError::new(RunnerErrorKind::Io, "stdout reader failed"))??;
        let stderr = stderr_reader
            .join()
            .map_err(|_| RunnerError::new(RunnerErrorKind::Io, "stderr reader failed"))??;
        Ok(CommandOutput {
            status: status.code().unwrap_or(128),
            stdout,
            stderr,
        })
    }
}

fn terminate_and_reap(
    child: &mut std::process::Child,
    stdout_reader: thread::JoinHandle<Result<Vec<u8>, RunnerError>>,
    stderr_reader: thread::JoinHandle<Result<Vec<u8>, RunnerError>>,
) {
    let _ = child.kill();
    let _ = child.wait();
    let _ = stdout_reader.join();
    let _ = stderr_reader.join();
}

fn read_capped(mut reader: impl Read, limit: usize) -> Result<Vec<u8>, RunnerError> {
    let mut bytes = Vec::new();
    let mut exceeded = false;
    let mut chunk = [0_u8; 8192];
    loop {
        let count = reader.read(&mut chunk).map_err(|error| {
            RunnerError::new(
                RunnerErrorKind::Io,
                format!("could not read adapter output: {error}"),
            )
        })?;
        if count == 0 {
            break;
        }
        let remaining = limit.saturating_sub(bytes.len());
        bytes.extend_from_slice(&chunk[..count.min(remaining)]);
        exceeded |= count > remaining;
    }
    if exceeded {
        return Err(RunnerError::new(
            RunnerErrorKind::Io,
            "adapter output exceeded the bounded capture limit",
        ));
    }
    Ok(bytes)
}
