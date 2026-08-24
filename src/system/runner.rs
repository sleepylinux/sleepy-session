use std::{
    fmt,
    io::Read,
    process::{Command, Stdio},
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
}

#[derive(Debug, Clone, Copy, Default)]
pub struct ProcessCommandRunner;

impl CommandRunner for ProcessCommandRunner {
    fn run(&self, spec: &CommandSpec) -> Result<CommandOutput, RunnerError> {
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
        let stdout_reader = thread::spawn(move || read_capped(stdout));
        let stderr_reader = thread::spawn(move || read_capped(stderr));
        let started = Instant::now();
        let status = loop {
            match child.try_wait() {
                Ok(Some(status)) => break status,
                Ok(None) if started.elapsed() < spec.timeout => {
                    thread::sleep(Duration::from_millis(5));
                }
                Ok(None) => {
                    let _ = child.kill();
                    let _ = child.wait();
                    let _ = stdout_reader.join();
                    let _ = stderr_reader.join();
                    return Err(RunnerError::timeout(
                        "adapter command exceeded its deadline",
                    ));
                }
                Err(error) => {
                    let _ = child.kill();
                    let _ = child.wait();
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

fn read_capped(mut reader: impl Read) -> Result<Vec<u8>, RunnerError> {
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
        let remaining = MAX_OUTPUT_BYTES.saturating_sub(bytes.len());
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
