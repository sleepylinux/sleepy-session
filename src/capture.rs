//! Opt-in capture v1. Human consent runs outside the desktop mutation queue.
//! No legacy desktop-v3 capability or helper contract is changed here.
use crate::{
    sessiond::{
        private_socket::read_bounded_line,
        supervisor::{
            ConnectionContext, ConnectionLimits, EndpointKind, RequiredStartupTask,
            SocketSupervisor,
        },
    },
    store::{NoReplacePublication, SecureDir},
};
use serde::Deserialize;
use sleepy_sdk::{
    validate_capture_reply, validate_capture_request, CaptureCommand, CaptureDiagnostic,
    CaptureErrorCode, CaptureJob, CapturePayload, CapturePng, CaptureReply, CaptureState,
    CAPTURE_SCHEMA_VERSION,
};
use std::{
    collections::VecDeque,
    ffi::OsStr,
    fs::File,
    io::{self, Read, Seek, SeekFrom},
    os::{
        fd::{AsRawFd, FromRawFd},
        unix::fs::{MetadataExt, PermissionsExt},
    },
    path::{Path, PathBuf},
    process::Stdio,
    sync::{Arc, Mutex},
    time::Duration,
};
use tokio::{
    io::{AsyncBufReadExt, AsyncReadExt, BufReader},
    process::Command,
    task::JoinHandle,
};
use tokio_util::sync::CancellationToken;
const HISTORY: usize = 16;
const MAX_PNG: usize = 32 * 1024 * 1024;
const MAX_DECODED: usize = 128 * 1024 * 1024;
/// Whole helper lifetime, including consent and image delivery. Not a mutation timeout.
pub const CONSENT_DEADLINE: Duration = Duration::from_secs(120);
struct Entry {
    identity: Option<(u64, u64)>,
    job: CaptureJob,
    cancel: CancellationToken,
    task: Option<JoinHandle<()>>,
}
struct State {
    jobs: VecDeque<Entry>,
    stopping: bool,
}
struct Inner {
    directory: SecureDir,
    path: PathBuf,
    helper: Option<PathBuf>,
    deadline: Duration,
    state: Mutex<State>,
}
#[derive(Clone)]
pub struct CaptureService(Arc<Inner>);
fn diagnostic(code: CaptureErrorCode, message: &str) -> CaptureDiagnostic {
    CaptureDiagnostic {
        code,
        message: message.into(),
    }
}
fn failure(code: CaptureErrorCode, message: &str) -> CapturePayload {
    CapturePayload::Error {
        diagnostic: diagnostic(code, message),
    }
}
fn active(state: CaptureState) -> bool {
    matches!(
        state,
        CaptureState::AwaitingConsent | CaptureState::Capturing
    )
}
fn store_error(e: crate::StoreError) -> io::Error {
    io::Error::other(e.to_string())
}
impl CaptureService {
    pub fn open(path: PathBuf, helper: Option<PathBuf>, deadline: Duration) -> io::Result<Self> {
        let directory = SecureDir::open_writable(&path, true).map_err(store_error)?;
        directory.enforce_private_directory().map_err(store_error)?;
        Ok(Self(Arc::new(Inner {
            directory,
            path,
            helper,
            deadline,
            state: Mutex::new(State {
                jobs: VecDeque::new(),
                stopping: false,
            }),
        })))
    }
    pub async fn handle_json(&self, input: &str) -> io::Result<String> {
        let payload = match validate_capture_request(input) {
            Ok(request) => self.handle(request.command),
            Err(_) => failure(CaptureErrorCode::InvalidRequest, "Invalid capture request"),
        };
        serde_json::to_string(&CaptureReply {
            schema_version: CAPTURE_SCHEMA_VERSION,
            payload,
        })
        .map_err(io::Error::other)
    }
    fn handle(&self, command: CaptureCommand) -> CapturePayload {
        let mut state = self.0.state.lock().unwrap();
        if state.stopping {
            return failure(CaptureErrorCode::Unavailable, "Capture service is stopping");
        }
        let cancel_request = matches!(&command, CaptureCommand::Cancel { .. });
        match command {
            CaptureCommand::Capabilities => CapturePayload::Capabilities {
                screenshot: self.0.helper.is_some(),
                color_picker: false,
            },
            CaptureCommand::Status { job_id } | CaptureCommand::Cancel { job_id } => {
                // Cancellation signals the worker; terminal state is published only after reaping.
                let cancel = cancel_request;
                match state.jobs.iter().find(|entry| entry.job.job_id == job_id) {
                    Some(entry) => {
                        if cancel && active(entry.job.state) {
                            entry.cancel.cancel()
                        }
                        CapturePayload::Job {
                            job: entry.job.clone(),
                        }
                    }
                    None => failure(CaptureErrorCode::NotFound, "Capture job was not found"),
                }
            }
            CaptureCommand::Begin { job_id, output_id } => {
                if let Some(entry) = state.jobs.iter().find(|entry| entry.job.job_id == job_id) {
                    return if entry.job.output_id == output_id {
                        CapturePayload::Job {
                            job: entry.job.clone(),
                        }
                    } else {
                        failure(
                            CaptureErrorCode::InvalidRequest,
                            "Job ID already has a different output",
                        )
                    };
                }
                if state.jobs.iter().any(|entry| active(entry.job.state)) {
                    return failure(CaptureErrorCode::Busy, "Another capture is active");
                }
                let Some(helper) = self.0.helper.clone() else {
                    return failure(
                        CaptureErrorCode::Unavailable,
                        "Capture helper is unavailable",
                    );
                };
                let name = format!("screenshot-{job_id}.png");
                if !matches!(self.0.directory.entry_metadata(OsStr::new(&name)), Ok(None)) {
                    return failure(
                        CaptureErrorCode::InvalidRequest,
                        "Capture result path already exists or is unsafe",
                    );
                }
                while state.jobs.len() >= HISTORY {
                    if let Some(old) = state.jobs.pop_front() {
                        if let Some(identity) = old.identity {
                            self.remove_known(
                                &format!("screenshot-{}.png", old.job.job_id),
                                identity,
                            );
                        }
                    }
                }
                let memfd = match capture_memfd() {
                    Ok(file) => file,
                    Err(_) => {
                        return failure(
                            CaptureErrorCode::Unavailable,
                            "Capture memory file could not be created",
                        )
                    }
                };
                let job = CaptureJob {
                    job_id: job_id.clone(),
                    output_id: output_id.clone(),
                    state: CaptureState::AwaitingConsent,
                    result: None,
                    diagnostic: None,
                };
                let cancel = CancellationToken::new();
                let worker = self.clone();
                let token = cancel.clone();
                let task = tokio::spawn(async move {
                    worker.run(job_id, output_id, helper, token, memfd).await;
                });
                state.jobs.push_back(Entry {
                    identity: None,
                    job: job.clone(),
                    cancel,
                    task: Some(task),
                });
                CapturePayload::Job { job }
            }
        }
    }
    async fn run(
        &self,
        id: String,
        output: String,
        helper: PathBuf,
        cancel: CancellationToken,
        mut memfd: File,
    ) {
        let name = format!("screenshot-{id}.png");
        let path = self.0.path.join(&name);
        let mut command = Command::new(helper);
        command
            .args(["--job-id", &id, "--output-id", &output, "--output"])
            .arg(&path)
            .args(["--output-fd", "4"])
            .env_remove("NOTIFY_SOCKET")
            .stdin(Stdio::null())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .kill_on_drop(true)
            .process_group(0);
        let parent_pid = unsafe { libc::getpid() };
        let output_fd = memfd.as_raw_fd();
        unsafe {
            command.pre_exec(move || {
                if output_fd != 4 && libc::dup2(output_fd, 4) < 0 {
                    return Err(io::Error::last_os_error());
                }
                if libc::fcntl(4, libc::F_SETFD, 0) < 0 {
                    return Err(io::Error::last_os_error());
                }
                let limit = libc::rlimit {
                    rlim_cur: MAX_PNG as libc::rlim_t,
                    rlim_max: MAX_PNG as libc::rlim_t,
                };
                if libc::setrlimit(libc::RLIMIT_FSIZE, &limit) != 0 {
                    return Err(io::Error::last_os_error());
                }
                if libc::prctl(libc::PR_SET_PDEATHSIG, libc::SIGKILL) != 0 {
                    return Err(io::Error::last_os_error());
                }
                if libc::getppid() != parent_pid {
                    return Err(io::Error::other("Capture daemon exited"));
                }
                Ok(())
            });
        }
        let outcome = match command.spawn() {
            Err(_) => Err(diagnostic(
                CaptureErrorCode::Unavailable,
                "Capture helper could not start",
            )),
            Ok(mut child) => {
                let pid = child.id().expect("new child PID") as i32;
                let mut leader_exited = false;
                let mut stdout = BufReader::new(child.stdout.take().unwrap());
                let mut stderr = child.stderr.take().unwrap();
                let (sender, mut messages) = tokio::sync::mpsc::channel(16);
                let reader_task = tokio::spawn(async move {
                    loop {
                        let mut bytes = Vec::new();
                        let read = (&mut stdout).take(4097).read_until(b'\n', &mut bytes).await;
                        if matches!(read, Ok(0)) {
                            break;
                        }
                        let valid =
                            read.is_ok() && bytes.len() <= 4096 && bytes.last() == Some(&b'\n');
                        let message = if valid {
                            bytes.pop();
                            Ok(bytes)
                        } else {
                            Err(io::Error::other("Invalid helper frame"))
                        };
                        if sender.send(message).await.is_err() || !valid {
                            break;
                        }
                    }
                });
                let mut stderr_bytes = 0usize;
                let mut chunk = [0u8; 1024];
                let mut frames = 0;
                let mut last = None;
                let mut eof = false;
                let mut stderr_eof = false;
                let deadline = tokio::time::sleep(self.0.deadline);
                tokio::pin!(deadline);
                let result = loop {
                    tokio::select! {
                        biased;
                        _ = cancel.cancelled() => break Ok(CaptureState::Cancelled),
                        _ = &mut deadline => break Err(diagnostic(
                            CaptureErrorCode::ConsentTimedOut,
                            "Capture consent or delivery timed out",
                        )),
                        line = messages.recv(), if !eof => {
                            match line {
                                None => eof = true,
                                Some(Ok(bytes)) => {
                                    frames += 1;
                                    if frames > 16 {
                                        break Err(diagnostic(CaptureErrorCode::CaptureFailed,
                                            "Too many capture helper messages"));
                                    }
                                    match serde_json::from_slice::<HelperStatus>(&bytes) {
                                        Ok(value) => {
                                            if last.as_ref().is_some_and(|v: &HelperStatus| !active(v.state)) {
                                                break Err(diagnostic(CaptureErrorCode::CaptureFailed,
                                                    "Capture helper sent messages after completion"));
                                            }
                                            if value.state == CaptureState::Capturing {
                                                self.update_state(&id, CaptureState::Capturing);
                                            }
                                            last = Some(value);
                                        }
                                        Err(_) => break Err(diagnostic(CaptureErrorCode::CaptureFailed,
                                            "Invalid capture helper message")),
                                    }
                                }
                                Some(Err(_)) => break Err(diagnostic(CaptureErrorCode::CaptureFailed,
                                    "Invalid capture helper framing")),
                            }
                        }
                        read = stderr.read(&mut chunk), if !stderr_eof => {
                            match read {
                                Ok(0) => stderr_eof = true,
                                Ok(n) => {
                                    stderr_bytes += n;
                                    if stderr_bytes > 65536 {
                                        break Err(diagnostic(CaptureErrorCode::CaptureFailed,
                                            "Capture helper diagnostics exceeded limit"));
                                    }
                                }
                                Err(_) => stderr_eof = true,
                            }
                        }
                        exit = wait_for_owned_exit(pid), if !leader_exited => {
                            if exit.is_err() {
                                break Err(diagnostic(CaptureErrorCode::CaptureFailed,
                                    "Capture helper wait failed"));
                            }
                            // Keep the exited leader unreaped until its process group is dead:
                            // this pins the group ID and prevents signalling a reused PID.
                            unsafe { libc::kill(-pid, libc::SIGKILL); }
                            leader_exited = true;
                        }
                        status = child.wait(), if eof && leader_exited => {
                            break match (status, last) {
                                (Ok(status), Some(message))
                                    if status.success() && message.state == CaptureState::Completed =>
                                    Ok(CaptureState::Completed),
                                (Ok(status), Some(message))
                                    if status.success() && message.state == CaptureState::Cancelled =>
                                    Ok(CaptureState::Cancelled),
                                (_, Some(message)) if message.state == CaptureState::Failed =>
                                    Err(safe_diagnostic(message.diagnostic)),
                                _ => Err(diagnostic(CaptureErrorCode::CaptureFailed,
                                    "Capture helper exited without a successful result")),
                            }
                        }
                    }
                };
                reader_task.abort();
                let _ = reader_task.await;
                // On cancellation/error keep the leader unreaped while terminating its
                // entire owned group, including TERM-ignoring descendants.
                if child.id().is_some() {
                    unsafe {
                        libc::kill(-pid, libc::SIGTERM);
                    }
                    let _ =
                        tokio::time::timeout(Duration::from_millis(300), wait_for_owned_exit(pid))
                            .await;
                    unsafe {
                        libc::kill(-pid, libc::SIGKILL);
                    }
                    let _ = child.wait().await;
                }
                result
            }
        };
        let mut final_result = match outcome {
            Ok(CaptureState::Completed) => self.png(&name, &mut memfd).map(Some).map_err(|_| {
                diagnostic(
                    CaptureErrorCode::CaptureFailed,
                    "Capture result is not a private valid PNG",
                )
            }),
            Ok(_) => Ok(None),
            Err(e) => Err(e),
        };
        let mut state = self.0.state.lock().unwrap();
        if cancel.is_cancelled() {
            if let Ok(Some((_, identity))) = &final_result {
                self.remove_known(&name, *identity);
            }
            final_result = Ok(None);
        }
        if let Some(entry) = state.jobs.iter_mut().find(|entry| entry.job.job_id == id) {
            match final_result {
                Ok(Some((png, identity))) => {
                    entry.identity = Some(identity);
                    entry.job.state = CaptureState::Completed;
                    entry.job.result = Some(png)
                }
                Ok(None) => entry.job.state = CaptureState::Cancelled,
                Err(error) => {
                    entry.job.state = CaptureState::Failed;
                    entry.job.diagnostic = Some(error)
                }
            }
        }
    }
    fn update_state(&self, id: &str, value: CaptureState) {
        if let Some(entry) = self
            .0
            .state
            .lock()
            .unwrap()
            .jobs
            .iter_mut()
            .find(|e| e.job.job_id == id)
        {
            entry.job.state = value;
        }
    }
    fn remove_known(&self, name: &str, identity: (u64, u64)) {
        if let Ok(Some(metadata)) = self.0.directory.entry_metadata(OsStr::new(name)) {
            if metadata.uid == unsafe { libc::geteuid() }
                && metadata.mode & libc::S_IFMT == libc::S_IFREG
                && (metadata.device, metadata.inode) == identity
            {
                let _ = self.0.directory.remove_file(OsStr::new(name));
            }
        }
    }
    fn png(&self, name: &str, file: &mut File) -> io::Result<(CapturePng, (u64, u64))> {
        let seals =
            libc::F_SEAL_WRITE | libc::F_SEAL_GROW | libc::F_SEAL_SHRINK | libc::F_SEAL_SEAL;
        if unsafe { libc::fcntl(file.as_raw_fd(), libc::F_ADD_SEALS, seals) } < 0 {
            return Err(io::Error::last_os_error());
        }
        file.seek(SeekFrom::Start(0))?;
        let metadata = file.metadata()?;
        if !metadata.is_file()
            || metadata.uid() != unsafe { libc::geteuid() }
            || metadata.permissions().mode() & 0o077 != 0
            || metadata.len() > MAX_PNG as u64
        {
            return Err(io::Error::other("unsafe PNG"));
        }
        let mut bytes = Vec::new();
        file.take(MAX_PNG as u64 + 1).read_to_end(&mut bytes)?;
        if bytes.len() > MAX_PNG {
            return Err(io::Error::other("PNG exceeds budget"));
        }
        let decoder = png::Decoder::new(std::io::Cursor::new(&bytes));
        let mut reader = decoder.read_info().map_err(io::Error::other)?;
        let info = reader.info();
        let (width, height) = (info.width, info.height);
        if !(1..=32768).contains(&width)
            || !(1..=32768).contains(&height)
            || reader.output_buffer_size() > MAX_DECODED
        {
            return Err(io::Error::other("PNG dimensions exceed budget"));
        }
        let mut buffer = vec![0; reader.output_buffer_size()];
        reader.next_frame(&mut buffer).map_err(io::Error::other)?;
        reader.finish().map_err(io::Error::other)?;
        drop(buffer);
        drop(reader);
        let temporary = format!(".capture-{}.tmp", uuid::Uuid::new_v4());
        let identity = match self.0.directory.publish_new_no_replace(
            OsStr::new(&temporary),
            OsStr::new(name),
            &bytes,
            |_| Ok(()),
        ) {
            NoReplacePublication::Published(snapshot) => snapshot.identity(),
            NoReplacePublication::NotPublished(error) => return Err(store_error(error)),
            NoReplacePublication::PublishedWithError { snapshot, error } => {
                if let Some(snapshot) = snapshot {
                    self.remove_known(name, snapshot.identity());
                }
                return Err(store_error(error));
            }
        };
        Ok((
            CapturePng {
                path: self.0.path.join(name).to_string_lossy().into_owned(),
                mime_type: "image/png".into(),
                width,
                height,
            },
            identity,
        ))
    }
    pub async fn shutdown(&self) {
        let tasks = {
            let mut state = self.0.state.lock().unwrap();
            state.stopping = true;
            state
                .jobs
                .iter_mut()
                .filter_map(|e| {
                    e.cancel.cancel();
                    e.task.take()
                })
                .collect::<Vec<_>>()
        };
        for task in tasks {
            let _ = task.await;
        }
    }
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct HelperStatus {
    state: CaptureState,
    #[serde(default)]
    diagnostic: Option<CaptureDiagnostic>,
}
fn safe_diagnostic(value: Option<CaptureDiagnostic>) -> CaptureDiagnostic {
    match value {
        Some(v)
            if !v.message.is_empty()
                && v.message.len() <= 256
                && !v.message.chars().any(char::is_control) =>
        {
            v
        }
        _ => diagnostic(CaptureErrorCode::CaptureFailed, "Capture helper failed"),
    }
}

pub struct CaptureSocket {
    supervisor: SocketSupervisor,
    service: CaptureService,
}
impl CaptureSocket {
    pub async fn bind(path: &Path, service: CaptureService) -> io::Result<Self> {
        let limits = ConnectionLimits {
            max_clients: 16,
            max_frame_bytes: 4096,
            read_timeout: Duration::from_secs(2),
            write_timeout: Duration::from_secs(1),
            drain_timeout: Duration::from_secs(2),
        };
        Ok(Self {
            supervisor: SocketSupervisor::bind(
                path,
                unsafe { libc::geteuid() },
                EndpointKind::Request,
                limits,
            )
            .await?,
            service,
        })
    }
    pub async fn serve(&self) -> io::Result<()> {
        let service = self.service.clone();
        self.supervisor
            .serve(move |stream, context| serve_stream(stream, context, service.clone()))
            .await
            .map(|_| ())
    }
    pub async fn serve_with_startup(&self, startup: RequiredStartupTask) -> io::Result<()> {
        let service = self.service.clone();
        self.supervisor
            .serve_with_startup(startup, move |stream, context| {
                serve_stream(stream, context, service.clone())
            })
            .await
            .map(|_| ())
    }
    pub async fn shutdown(&self) -> io::Result<()> {
        let result = self.supervisor.shutdown_and_drain().await;
        self.service.shutdown().await;
        result.map(|_| ())
    }
}
async fn serve_stream(
    stream: tokio::net::UnixStream,
    context: ConnectionContext,
    service: CaptureService,
) -> io::Result<()> {
    let (read, mut write) = stream.into_split();
    let mut read = BufReader::new(read);
    let data = context.read_frame(&mut read).await?;
    let input = std::str::from_utf8(&data).map_err(io::Error::other)?;
    let reply = service.handle_json(input).await?;
    context.write_frame(&mut write, reply.as_bytes()).await
}
/// One bounded wire transaction; consent is never awaited by this CLI.
pub async fn client_request(path: &Path, input: &str) -> io::Result<String> {
    validate_capture_request(input).map_err(io::Error::other)?;
    tokio::time::timeout(Duration::from_secs(2), async {
        use tokio::io::AsyncWriteExt;
        let mut stream = tokio::net::UnixStream::connect(path).await?;
        if stream.peer_cred()?.uid() != unsafe { libc::geteuid() } {
            return Err(io::Error::new(
                io::ErrorKind::PermissionDenied,
                "Capture socket peer UID differs",
            ));
        }
        stream.write_all(input.as_bytes()).await?;
        stream.write_all(b"\n").await?;
        let data = read_bounded_line(
            &mut BufReader::new(stream),
            4096,
            Duration::from_secs(2),
            "Invalid capture reply",
        )
        .await?;
        let reply = String::from_utf8(data).map_err(io::Error::other)?;
        validate_capture_reply(&reply).map_err(io::Error::other)?;
        Ok(reply)
    })
    .await
    .map_err(|_| io::Error::new(io::ErrorKind::TimedOut, "Capture reply timed out"))?
}

async fn wait_for_owned_exit(pid: i32) -> io::Result<()> {
    loop {
        let mut info = std::mem::MaybeUninit::<libc::siginfo_t>::zeroed();
        let result = unsafe {
            libc::waitid(
                libc::P_PID,
                pid as libc::id_t,
                info.as_mut_ptr(),
                libc::WEXITED | libc::WNOWAIT | libc::WNOHANG,
            )
        };
        if result != 0 {
            return Err(io::Error::last_os_error());
        }
        if unsafe { info.assume_init().si_pid() } == pid {
            return Ok(());
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}

fn capture_memfd() -> io::Result<File> {
    let fd = unsafe {
        libc::memfd_create(
            c"sleepy-capture".as_ptr(),
            libc::MFD_CLOEXEC | libc::MFD_ALLOW_SEALING,
        )
    };
    if fd < 0 {
        return Err(io::Error::last_os_error());
    }
    let file = unsafe { File::from_raw_fd(fd) };
    if unsafe { libc::fchmod(fd, 0o600) } < 0 {
        return Err(io::Error::last_os_error());
    }
    // Reserve fd4 in the parent if it was free, so Command's exec-error pipe
    // cannot occupy our fixed child output descriptor during pre_exec.
    if fd < 4 {
        let duplicate = unsafe { libc::fcntl(fd, libc::F_DUPFD_CLOEXEC, 4) };
        if duplicate < 0 {
            return Err(io::Error::last_os_error());
        }
        Ok(unsafe { File::from_raw_fd(duplicate) })
    } else {
        Ok(file)
    }
}
