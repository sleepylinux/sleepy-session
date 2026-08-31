use std::{
    collections::BTreeSet,
    fmt, fs,
    io::{self, Read, Write},
    os::{fd::AsRawFd, unix::process::CommandExt},
    path::PathBuf,
    process::{ChildStdin, ChildStdout, Command, Stdio},
    sync::{
        atomic::{AtomicBool, AtomicU64, Ordering},
        Arc, Condvar, Mutex as StdMutex,
    },
    thread,
    time::{Duration, Instant},
};

use serde::{Deserialize, Serialize};

const MAX_OUTPUT_BYTES: usize = 64 * 1024;
const SUPERVISOR_ARG: &str = "--sleepy-internal-command-supervisor";
const SUPERVISOR_ENV: &str = "SLEEPY_INTERNAL_COMMAND_SUPERVISOR";
const SUPERVISOR_MAGIC: &[u8; 8] = b"SLPCMD1\0";
const SUPERVISOR_REQUEST_LIMIT: usize = 1024 * 1024;
const SUPERVISOR_RESULT_HEADER: usize = 8 + 1 + 4 + 4 + 4;

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
    cancelled: Option<Arc<AtomicBool>>,
    cancellation: Option<RunCancellation>,
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
            cancellation: None,
        }
    }

    pub fn for_timeout(timeout: Duration) -> Self {
        Self {
            deadline: Instant::now() + timeout,
            generation: None,
            latest_generation: None,
            cancelled: None,
            cancellation: None,
        }
    }

    pub fn for_request(deadline: Instant, cancelled: Arc<AtomicBool>) -> Self {
        Self {
            deadline,
            generation: None,
            latest_generation: None,
            cancelled: Some(cancelled),
            cancellation: None,
        }
    }

    pub(crate) fn for_cancellation(deadline: Instant, cancellation: RunCancellation) -> Self {
        Self {
            deadline,
            generation: None,
            latest_generation: None,
            cancelled: None,
            cancellation: Some(cancellation),
        }
    }

    pub fn is_cancelled(&self) -> bool {
        self.cancelled
            .as_ref()
            .is_some_and(|cancelled| cancelled.load(Ordering::SeqCst))
            || self
                .cancellation
                .as_ref()
                .is_some_and(RunCancellation::is_cancelled)
            || self
                .generation
                .zip(self.latest_generation.as_ref())
                .is_some_and(|(generation, latest)| generation < latest.load(Ordering::SeqCst))
    }

    pub fn remaining(&self) -> Duration {
        self.deadline.saturating_duration_since(Instant::now())
    }

    pub(crate) fn begin_commit(&self) -> io::Result<Option<RunCommitGuard>> {
        if self.remaining().is_zero() {
            return Err(io::Error::new(
                io::ErrorKind::TimedOut,
                "operation exceeded its deadline before commit",
            ));
        }
        match &self.cancellation {
            Some(cancellation) => cancellation.begin_commit().map(Some),
            None if self.is_cancelled() => Err(io::Error::new(
                io::ErrorKind::Interrupted,
                "operation was cancelled before commit",
            )),
            None => Ok(None),
        }
    }
}

#[derive(Clone)]
pub(crate) struct RunCancellation {
    inner: Arc<RunCancellationInner>,
}

struct RunCancellationInner {
    observable: AtomicBool,
    state: StdMutex<RunCancellationState>,
    changed: Condvar,
}

#[derive(Default)]
struct RunCancellationState {
    requested: bool,
    cancelled: bool,
    commits: usize,
}

pub(crate) struct RunCommitGuard {
    cancellation: RunCancellation,
}

impl Default for RunCancellation {
    fn default() -> Self {
        Self::new()
    }
}

impl RunCancellation {
    pub(crate) fn new() -> Self {
        Self {
            inner: Arc::new(RunCancellationInner {
                observable: AtomicBool::new(false),
                state: StdMutex::new(RunCancellationState::default()),
                changed: Condvar::new(),
            }),
        }
    }

    pub(crate) fn cancel(&self) {
        let mut state = self
            .inner
            .state
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        state.requested = true;
        while state.commits != 0 {
            state = self
                .inner
                .changed
                .wait(state)
                .unwrap_or_else(|error| error.into_inner());
        }
        state.cancelled = true;
        self.inner.observable.store(true, Ordering::SeqCst);
        self.inner.changed.notify_all();
    }

    pub(crate) fn is_cancelled(&self) -> bool {
        self.inner.observable.load(Ordering::SeqCst)
    }

    fn begin_commit(&self) -> io::Result<RunCommitGuard> {
        let mut state = self
            .inner
            .state
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        if state.requested || state.cancelled {
            return Err(io::Error::new(
                io::ErrorKind::Interrupted,
                "operation was cancelled before commit",
            ));
        }
        state.commits = state
            .commits
            .checked_add(1)
            .expect("commit guard count overflow");
        Ok(RunCommitGuard {
            cancellation: self.clone(),
        })
    }
}

impl Drop for RunCommitGuard {
    fn drop(&mut self) {
        let mut state = self
            .cancellation
            .inner
            .state
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        state.commits = state.commits.saturating_sub(1);
        if state.commits == 0 {
            self.cancellation.inner.changed.notify_all();
        }
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
        let effective_timeout = spec.timeout.min(control.remaining());
        if effective_timeout.is_zero() {
            return Err(RunnerError::timeout(
                "adapter command exceeded its deadline",
            ));
        }
        let started = Instant::now();
        let request = SupervisorRequest::new(spec, effective_timeout);
        let supervisor_path = command_supervisor_path()?;
        let mut supervisor = Command::new(supervisor_path);
        supervisor
            .arg(SUPERVISOR_ARG)
            .env(SUPERVISOR_ENV, "1")
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::null())
            .process_group(0);
        let mut child = supervisor.spawn().map_err(|error| {
            RunnerError::new(
                RunnerErrorKind::Spawn,
                format!("adapter command supervisor is unavailable: {error}"),
            )
        })?;
        let mut supervisor_stdin = Some(child.stdin.take().expect("piped supervisor stdin"));
        if let Err(error) = write_supervisor_request(
            supervisor_stdin
                .as_mut()
                .expect("supervisor stdin is present before request"),
            &request,
        ) {
            supervisor_stdin.take();
            let _ = child.kill();
            let _ = child.wait();
            return Err(error);
        }
        let mut supervisor_stdout = child.stdout.take().expect("piped supervisor stdout");
        if let Err(error) = set_nonblocking(supervisor_stdout.as_raw_fd()).map_err(output_error) {
            supervisor_stdin.take();
            let _ = child.kill();
            let _ = child.wait();
            return Err(error);
        }
        let response = wait_for_supervisor(
            &mut child,
            &mut supervisor_stdin,
            &mut supervisor_stdout,
            control,
            spec.max_output_bytes,
        )?;
        Ok((response.into_output()?, started))
    }
}

#[derive(Debug, Serialize, Deserialize)]
struct SupervisorRequest {
    program: String,
    args: Vec<String>,
    env: Vec<(String, String)>,
    timeout_millis: u64,
    max_output_bytes: usize,
}

impl SupervisorRequest {
    fn new(spec: &CommandSpec, timeout: Duration) -> Self {
        Self {
            program: spec.program.clone(),
            args: spec.args.clone(),
            env: spec.env.clone(),
            timeout_millis: u64::try_from(timeout.as_millis())
                .unwrap_or(u64::MAX)
                .max(1),
            max_output_bytes: spec.max_output_bytes,
        }
    }

    fn timeout(&self) -> Duration {
        Duration::from_millis(self.timeout_millis)
    }
}

struct SupervisorResponse {
    tag: u8,
    status: i32,
    stdout: Vec<u8>,
    stderr: Vec<u8>,
}

impl SupervisorResponse {
    fn exited(status: i32, stdout: Vec<u8>, stderr: Vec<u8>) -> Self {
        Self {
            tag: 0,
            status,
            stdout,
            stderr,
        }
    }

    fn timeout() -> Self {
        Self::error(1)
    }

    fn cancelled() -> Self {
        Self::error(2)
    }

    fn spawn() -> Self {
        Self::error(3)
    }

    fn io() -> Self {
        Self::error(4)
    }

    fn error(tag: u8) -> Self {
        Self {
            tag,
            status: 0,
            stdout: Vec::new(),
            stderr: Vec::new(),
        }
    }

    fn into_output(self) -> Result<CommandOutput, RunnerError> {
        match self.tag {
            0 => Ok(CommandOutput {
                status: self.status,
                stdout: self.stdout,
                stderr: self.stderr,
            }),
            1 => Err(RunnerError::timeout(
                "adapter command exceeded its deadline",
            )),
            2 => Err(RunnerError::cancelled()),
            3 => Err(RunnerError::spawn("adapter executable is unavailable")),
            _ => Err(RunnerError::new(
                RunnerErrorKind::Io,
                "adapter command supervisor failed",
            )),
        }
    }
}

fn command_supervisor_path() -> Result<PathBuf, RunnerError> {
    let current = std::env::current_exe().map_err(|error| {
        RunnerError::new(
            RunnerErrorKind::Io,
            format!("could not locate current executable: {error}"),
        )
    })?;
    if current
        .file_name()
        .and_then(|name| name.to_str())
        .is_some_and(|name| name == "sleepy-sessiond")
    {
        return Ok(current);
    }
    if let Some(directory) = current.parent() {
        if directory
            .file_name()
            .and_then(|name| name.to_str())
            .is_some_and(|name| name == "deps")
        {
            if let Some(debug_dir) = directory.parent() {
                let candidate = debug_dir.join("sleepy-sessiond");
                if candidate.exists() {
                    return Ok(candidate);
                }
            }
        }
        let candidate = directory.join("sleepy-sessiond");
        if candidate.exists() {
            return Ok(candidate);
        }
    }
    Err(RunnerError::spawn(
        "adapter command supervisor executable is unavailable",
    ))
}

fn write_supervisor_request(
    stdin: &mut ChildStdin,
    request: &SupervisorRequest,
) -> Result<(), RunnerError> {
    let bytes = serde_json::to_vec(request).map_err(|_| {
        RunnerError::new(
            RunnerErrorKind::Io,
            "could not encode adapter command supervisor request",
        )
    })?;
    let length = u32::try_from(bytes.len()).map_err(|_| {
        RunnerError::new(
            RunnerErrorKind::Io,
            "adapter command supervisor request exceeded its bounded limit",
        )
    })?;
    stdin
        .write_all(&length.to_le_bytes())
        .and_then(|()| stdin.write_all(&bytes))
        .and_then(|()| stdin.flush())
        .map_err(|error| {
            RunnerError::new(
                RunnerErrorKind::Io,
                format!("could not send adapter command supervisor request: {error}"),
            )
        })
}

fn supervisor_response_limit(max_output_bytes: usize) -> usize {
    SUPERVISOR_RESULT_HEADER.saturating_add(max_output_bytes.saturating_mul(2))
}

fn read_supervisor_response_bytes(
    stdout: &mut ChildStdout,
    response: &mut Vec<u8>,
    limit: usize,
) -> Result<bool, RunnerError> {
    let mut chunk = [0_u8; 8192];
    loop {
        match stdout.read(&mut chunk) {
            Ok(0) => return Ok(true),
            Ok(count) => {
                if response.len().saturating_add(count) > limit {
                    return Err(RunnerError::new(
                        RunnerErrorKind::Io,
                        "adapter command supervisor response exceeded its bounded limit",
                    ));
                }
                response.extend_from_slice(&chunk[..count]);
            }
            Err(error) if error.kind() == io::ErrorKind::WouldBlock => return Ok(false),
            Err(error) => return Err(output_error(error)),
        }
    }
}

fn drain_supervisor_response(
    stdout: &mut ChildStdout,
    response: &mut Vec<u8>,
    limit: usize,
) -> Result<(), RunnerError> {
    while !read_supervisor_response_bytes(stdout, response, limit)? {
        thread::sleep(Duration::from_millis(1));
    }
    Ok(())
}

fn parse_supervisor_response(bytes: &[u8]) -> Result<SupervisorResponse, RunnerError> {
    if bytes.len() < SUPERVISOR_RESULT_HEADER || &bytes[..8] != SUPERVISOR_MAGIC {
        return Err(RunnerError::new(
            RunnerErrorKind::Io,
            "adapter command supervisor response was malformed",
        ));
    }
    let tag = bytes[8];
    let status = i32::from_le_bytes(bytes[9..13].try_into().expect("status bytes"));
    let stdout_len =
        u32::from_le_bytes(bytes[13..17].try_into().expect("stdout length bytes")) as usize;
    let stderr_len =
        u32::from_le_bytes(bytes[17..21].try_into().expect("stderr length bytes")) as usize;
    let expected = SUPERVISOR_RESULT_HEADER
        .saturating_add(stdout_len)
        .saturating_add(stderr_len);
    if bytes.len() != expected {
        return Err(RunnerError::new(
            RunnerErrorKind::Io,
            "adapter command supervisor response size was invalid",
        ));
    }
    let stdout_start = SUPERVISOR_RESULT_HEADER;
    let stderr_start = stdout_start + stdout_len;
    Ok(SupervisorResponse {
        tag,
        status,
        stdout: bytes[stdout_start..stderr_start].to_vec(),
        stderr: bytes[stderr_start..].to_vec(),
    })
}

fn wait_for_supervisor(
    child: &mut std::process::Child,
    supervisor_stdin: &mut Option<ChildStdin>,
    supervisor_stdout: &mut ChildStdout,
    control: &RunControl,
    max_output_bytes: usize,
) -> Result<SupervisorResponse, RunnerError> {
    let mut response = Vec::new();
    let response_limit = supervisor_response_limit(max_output_bytes);
    loop {
        read_supervisor_response_bytes(supervisor_stdout, &mut response, response_limit)?;
        if control.is_cancelled() {
            supervisor_stdin.take();
        }
        match child.try_wait() {
            Ok(Some(status)) => {
                supervisor_stdin.take();
                drain_supervisor_response(supervisor_stdout, &mut response, response_limit)?;
                if !status.success() {
                    return Err(RunnerError::new(
                        RunnerErrorKind::Io,
                        "adapter command supervisor exited before reporting",
                    ));
                }
                return parse_supervisor_response(&response);
            }
            Ok(None) => thread::sleep(Duration::from_millis(1)),
            Err(error) => {
                supervisor_stdin.take();
                let _ = child.kill();
                let _ = child.wait();
                return Err(RunnerError::new(
                    RunnerErrorKind::Io,
                    format!("could not wait for adapter command supervisor: {error}"),
                ));
            }
        }
    }
}

fn process_children(pid: libc::pid_t) -> io::Result<Vec<libc::pid_t>> {
    let tasks = match fs::read_dir(format!("/proc/{pid}/task")) {
        Ok(tasks) => tasks,
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(Vec::new()),
        Err(error) => return Err(error),
    };
    let mut children = BTreeSet::new();
    for task in tasks {
        let task = task?;
        let thread = task
            .file_name()
            .to_string_lossy()
            .parse::<libc::pid_t>()
            .map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "invalid thread PID"))?;
        let contents = match fs::read_to_string(format!("/proc/{pid}/task/{thread}/children")) {
            Ok(contents) => contents,
            Err(error) if error.kind() == io::ErrorKind::NotFound => continue,
            Err(error) => return Err(error),
        };
        for value in contents.split_ascii_whitespace() {
            children.insert(
                value
                    .parse::<libc::pid_t>()
                    .map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "invalid child PID"))?,
            );
        }
    }
    Ok(children.into_iter().collect())
}

fn kill_process_group(process_group: libc::pid_t) {
    if process_group > 0 {
        unsafe {
            libc::kill(-process_group, libc::SIGKILL);
        }
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

pub fn run_command_supervisor() -> io::Result<()> {
    let mut stdin = io::stdin();
    let request = read_supervisor_request(&mut stdin)?;
    set_nonblocking(stdin.as_raw_fd())?;
    let response = supervise_command(request, &mut stdin);
    let mut stdout = io::stdout();
    write_supervisor_response(&mut stdout, &response)?;
    stdout.flush()
}

fn read_supervisor_request(stdin: &mut impl Read) -> io::Result<SupervisorRequest> {
    let mut length = [0_u8; 4];
    stdin.read_exact(&mut length)?;
    let length = u32::from_le_bytes(length) as usize;
    if length > SUPERVISOR_REQUEST_LIMIT {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "command supervisor request exceeded its bounded limit",
        ));
    }
    let mut bytes = vec![0_u8; length];
    stdin.read_exact(&mut bytes)?;
    serde_json::from_slice(&bytes)
        .map_err(|error| io::Error::new(io::ErrorKind::InvalidData, error))
}

fn supervise_command(request: SupervisorRequest, parent: &mut impl Read) -> SupervisorResponse {
    if ensure_supervisor_subreaper().is_err() {
        return SupervisorResponse::io();
    }
    let mut command = Command::new(&request.program);
    command
        .args(&request.args)
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .env_remove(SUPERVISOR_ENV)
        .process_group(0);
    for (key, value) in &request.env {
        command.env(key, value);
    }
    let started = Instant::now();
    let mut child = match command.spawn() {
        Ok(child) => child,
        Err(_) => return SupervisorResponse::spawn(),
    };
    let process_group = child.id() as libc::pid_t;
    let mut stdout = child.stdout.take().expect("piped stdout");
    let mut stderr = child.stderr.take().expect("piped stderr");
    if set_nonblocking(stdout.as_raw_fd()).is_err() || set_nonblocking(stderr.as_raw_fd()).is_err()
    {
        let _ = terminate_supervised_boundary(process_group);
        return SupervisorResponse::io();
    }
    let mut stdout_capture = OutputCapture::new(request.max_output_bytes);
    let mut stderr_capture = OutputCapture::new(request.max_output_bytes);
    let status = loop {
        if stdout_capture.read_available(&mut stdout).is_err()
            || stderr_capture.read_available(&mut stderr).is_err()
        {
            let _ = terminate_supervised_boundary(process_group);
            drain_closed_boundary(
                &mut stdout,
                &mut stderr,
                &mut stdout_capture,
                &mut stderr_capture,
            );
            return SupervisorResponse::io();
        }
        match parent_control_cancelled(parent) {
            Ok(true) => {
                let _ = terminate_supervised_boundary(process_group);
                drain_closed_boundary(
                    &mut stdout,
                    &mut stderr,
                    &mut stdout_capture,
                    &mut stderr_capture,
                );
                return SupervisorResponse::cancelled();
            }
            Ok(false) => {}
            Err(_) => {
                let _ = terminate_supervised_boundary(process_group);
                return SupervisorResponse::io();
            }
        }
        match child.try_wait() {
            Ok(Some(status)) => {
                if terminate_supervised_boundary(process_group).is_err() {
                    return SupervisorResponse::io();
                }
                drain_closed_boundary(
                    &mut stdout,
                    &mut stderr,
                    &mut stdout_capture,
                    &mut stderr_capture,
                );
                break status;
            }
            Ok(None) if started.elapsed() < request.timeout() => {
                thread::sleep(Duration::from_millis(1));
            }
            Ok(None) => {
                let _ = terminate_supervised_boundary(process_group);
                drain_closed_boundary(
                    &mut stdout,
                    &mut stderr,
                    &mut stdout_capture,
                    &mut stderr_capture,
                );
                return SupervisorResponse::timeout();
            }
            Err(_) => {
                let _ = terminate_supervised_boundary(process_group);
                return SupervisorResponse::io();
            }
        }
    };
    match (stdout_capture.finish(), stderr_capture.finish()) {
        (Ok(stdout), Ok(stderr)) => {
            SupervisorResponse::exited(status.code().unwrap_or(128), stdout, stderr)
        }
        _ => SupervisorResponse::io(),
    }
}

fn parent_control_cancelled(parent: &mut impl Read) -> io::Result<bool> {
    let mut byte = [0_u8; 1];
    match parent.read(&mut byte) {
        Ok(0) => Ok(true),
        Ok(_) => Ok(true),
        Err(error) if error.kind() == io::ErrorKind::WouldBlock => Ok(false),
        Err(error) if error.kind() == io::ErrorKind::Interrupted => Ok(false),
        Err(error) => Err(error),
    }
}

fn write_supervisor_response(
    stdout: &mut impl Write,
    response: &SupervisorResponse,
) -> io::Result<()> {
    let stdout_len = u32::try_from(response.stdout.len()).map_err(|_| {
        io::Error::new(
            io::ErrorKind::InvalidData,
            "supervisor stdout response exceeded its bounded limit",
        )
    })?;
    let stderr_len = u32::try_from(response.stderr.len()).map_err(|_| {
        io::Error::new(
            io::ErrorKind::InvalidData,
            "supervisor stderr response exceeded its bounded limit",
        )
    })?;
    stdout.write_all(SUPERVISOR_MAGIC)?;
    stdout.write_all(&[response.tag])?;
    stdout.write_all(&response.status.to_le_bytes())?;
    stdout.write_all(&stdout_len.to_le_bytes())?;
    stdout.write_all(&stderr_len.to_le_bytes())?;
    stdout.write_all(&response.stdout)?;
    stdout.write_all(&response.stderr)
}

fn ensure_supervisor_subreaper() -> io::Result<()> {
    if unsafe { libc::prctl(libc::PR_SET_CHILD_SUBREAPER, 1, 0, 0, 0) } == 0 {
        Ok(())
    } else {
        Err(io::Error::last_os_error())
    }
}

fn terminate_supervised_boundary(process_group: libc::pid_t) -> io::Result<()> {
    loop {
        kill_process_group(process_group);
        kill_supervisor_children()?;
        if !reap_supervisor_children()? {
            return Ok(());
        }
        thread::sleep(Duration::from_millis(1));
    }
}

fn kill_supervisor_children() -> io::Result<()> {
    let mut descendants = BTreeSet::new();
    collect_descendants(unsafe { libc::getpid() }, &mut descendants)?;
    for pid in descendants {
        if pid > 0 {
            unsafe {
                libc::kill(pid, libc::SIGKILL);
            }
        }
    }
    Ok(())
}

fn collect_descendants(
    parent: libc::pid_t,
    descendants: &mut BTreeSet<libc::pid_t>,
) -> io::Result<()> {
    for child in process_children(parent)? {
        if descendants.insert(child) {
            collect_descendants(child, descendants)?;
        }
    }
    Ok(())
}

fn reap_supervisor_children() -> io::Result<bool> {
    loop {
        let mut status = 0;
        let reaped = unsafe { libc::waitpid(-1, &mut status, libc::WNOHANG) };
        if reaped > 0 {
            continue;
        }
        if reaped == 0 {
            return Ok(true);
        }
        let error = io::Error::last_os_error();
        match error.raw_os_error() {
            Some(libc::ECHILD) => return Ok(false),
            Some(libc::EINTR) => continue,
            _ => return Err(error),
        }
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
