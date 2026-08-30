// SPDX-License-Identifier: GPL-3.0-only

use std::{
    fmt,
    future::Future,
    io,
    os::unix::fs::{FileTypeExt, MetadataExt, PermissionsExt},
    path::Path,
    sync::{
        atomic::{AtomicBool, AtomicU8, AtomicUsize, Ordering},
        Arc, Condvar, Mutex,
    },
    time::Duration,
};

use tokio::{
    io::{AsyncBufReadExt, AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt, BufReader},
    net::UnixStream,
    sync::mpsc,
    task::{JoinError, JoinSet},
};
use tokio_util::sync::CancellationToken;

use super::private_socket::{peer_credentials, PrivateSocketBindObserver, PrivateSocketEndpoint};

const ONE_MIB: usize = 1024 * 1024;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EndpointKind {
    Stream,
    Request,
    DesktopStream,
    DesktopRequest,
}

impl fmt::Display for EndpointKind {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::Stream => "stream",
            Self::Request => "request",
            Self::DesktopStream => "desktop-stream",
            Self::DesktopRequest => "desktop-request",
        })
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ConnectionLimits {
    pub max_clients: usize,
    pub max_frame_bytes: usize,
    pub read_timeout: Duration,
    pub write_timeout: Duration,
    pub drain_timeout: Duration,
}

impl ConnectionLimits {
    pub const fn stream() -> Self {
        Self {
            max_clients: 32,
            max_frame_bytes: ONE_MIB,
            read_timeout: Duration::from_secs(5),
            write_timeout: Duration::from_secs(5),
            drain_timeout: Duration::from_secs(10),
        }
    }

    pub const fn request() -> Self {
        Self {
            max_clients: 16,
            ..Self::stream()
        }
    }

    fn validate(self) -> io::Result<Self> {
        if self.max_clients == 0
            || self.max_frame_bytes == 0
            || self.max_frame_bytes > ONE_MIB
            || self.read_timeout.is_zero()
            || self.write_timeout.is_zero()
            || self.drain_timeout.is_zero()
        {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "socket connection limits must be nonzero and representable",
            ));
        }
        Ok(self)
    }
}

#[derive(Clone)]
pub struct ConnectionContext {
    pub peer_pid: u32,
    pub peer_uid: u32,
    pub cancellation: CancellationToken,
    limits: ConnectionLimits,
}

impl ConnectionContext {
    pub async fn read_frame<R>(&self, reader: &mut BufReader<R>) -> io::Result<Vec<u8>>
    where
        R: AsyncRead + Unpin,
    {
        let read = async {
            let maximum_read = self.limits.max_frame_bytes + 1;
            let mut limited = reader.take(maximum_read as u64);
            let mut bytes = Vec::with_capacity(maximum_read);
            let count = limited.read_until(b'\n', &mut bytes).await?;
            if count == 0 {
                return Err(io::Error::new(
                    io::ErrorKind::UnexpectedEof,
                    "frame ended before its delimiter",
                ));
            }
            if bytes.last() != Some(&b'\n') || bytes.len() - 1 > self.limits.max_frame_bytes {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "frame exceeds the configured limit",
                ));
            }
            bytes.pop();
            Ok(bytes)
        };
        tokio::time::timeout(self.limits.read_timeout, async {
            tokio::select! {
                biased;
                _ = self.cancellation.cancelled() => Err(io::Error::new(io::ErrorKind::Interrupted, "connection cancelled")),
                result = read => result,
            }
        })
        .await
        .map_err(|_| io::Error::new(io::ErrorKind::TimedOut, "frame read timed out"))?
    }

    pub async fn write_frame<W>(&self, writer: &mut W, frame: &[u8]) -> io::Result<()>
    where
        W: AsyncWrite + Unpin,
    {
        if frame.len() > self.limits.max_frame_bytes {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "frame exceeds the configured limit",
            ));
        }
        self.write_legacy_frame(writer, frame).await
    }

    pub(crate) async fn write_legacy_frame<W>(&self, writer: &mut W, frame: &[u8]) -> io::Result<()>
    where
        W: AsyncWrite + Unpin,
    {
        if frame.len() > ONE_MIB {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "legacy frame exceeds the global frame limit",
            ));
        }
        tokio::time::timeout(self.limits.write_timeout, async {
            writer.write_all(frame).await?;
            writer.write_all(b"\n").await
        })
        .await
        .map_err(|_| io::Error::new(io::ErrorKind::TimedOut, "frame write timed out"))?
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PeerCredentials {
    pub pid: u32,
    pub uid: u32,
}

pub trait PeerCredentialProvider: Send + Sync + 'static {
    fn peer_credentials(&self, stream: &UnixStream) -> io::Result<PeerCredentials>;
}

#[derive(Debug, Default)]
pub struct LinuxPeerCredentialProvider;

impl PeerCredentialProvider for LinuxPeerCredentialProvider {
    fn peer_credentials(&self, stream: &UnixStream) -> io::Result<PeerCredentials> {
        let credentials = peer_credentials(stream)?;
        let pid = u32::try_from(credentials.pid).map_err(|_| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                "peer PID is outside the valid range",
            )
        })?;
        Ok(PeerCredentials {
            pid,
            uid: credentials.uid,
        })
    }
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct SupervisorMetrics {
    pub active: usize,
    pub tracked: usize,
    pub completed: usize,
    pub rejected: usize,
    pub available_permits: usize,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct SocketDrainReport {
    pub completed: usize,
    pub aborted: usize,
}

#[derive(Default)]
struct Metrics {
    active: AtomicUsize,
    tracked: AtomicUsize,
    completed: AtomicUsize,
    rejected: AtomicUsize,
}

pub struct SocketSupervisor {
    endpoint: Arc<PrivateSocketEndpoint>,
    endpoint_kind: EndpointKind,
    limits: ConnectionLimits,
    credentials: Arc<dyn PeerCredentialProvider>,
    permits: Arc<tokio::sync::Semaphore>,
    cancellation: CancellationToken,
    serving: Arc<AtomicBool>,
    stopped: Arc<tokio::sync::Notify>,
    drain_report: Arc<Mutex<Option<SocketDrainReport>>>,
    drain_timeout: Arc<Mutex<Duration>>,
    metrics: Arc<Metrics>,
    actor: Mutex<Option<tokio::task::JoinHandle<()>>>,
    serve_error: Arc<Mutex<Option<StoredError>>>,
}

impl SocketSupervisor {
    pub async fn bind(
        path: impl AsRef<Path>,
        expected_uid: libc::uid_t,
        endpoint_kind: EndpointKind,
        limits: ConnectionLimits,
    ) -> io::Result<Self> {
        Self::bind_with_peer_credentials(
            path,
            expected_uid,
            endpoint_kind,
            limits,
            Arc::new(LinuxPeerCredentialProvider),
        )
        .await
    }

    pub async fn bind_with_peer_credentials(
        path: impl AsRef<Path>,
        expected_uid: libc::uid_t,
        endpoint_kind: EndpointKind,
        limits: ConnectionLimits,
        credentials: Arc<dyn PeerCredentialProvider>,
    ) -> io::Result<Self> {
        Self::bind_with_parts(
            path,
            expected_uid,
            endpoint_kind,
            limits,
            credentials,
            Arc::new(super::private_socket::NoopBindObserver),
        )
        .await
    }

    pub(crate) async fn bind_with_observer(
        path: impl AsRef<Path>,
        expected_uid: libc::uid_t,
        endpoint_kind: EndpointKind,
        limits: ConnectionLimits,
        observer: Arc<dyn PrivateSocketBindObserver>,
    ) -> io::Result<Self> {
        Self::bind_with_parts(
            path,
            expected_uid,
            endpoint_kind,
            limits,
            Arc::new(LinuxPeerCredentialProvider),
            observer,
        )
        .await
    }

    async fn bind_with_parts(
        path: impl AsRef<Path>,
        expected_uid: libc::uid_t,
        endpoint_kind: EndpointKind,
        limits: ConnectionLimits,
        credentials: Arc<dyn PeerCredentialProvider>,
        observer: Arc<dyn PrivateSocketBindObserver>,
    ) -> io::Result<Self> {
        let limits = limits.validate()?;
        Ok(Self {
            endpoint: Arc::new(
                PrivateSocketEndpoint::bind_with_observer(path, expected_uid, observer).await?,
            ),
            endpoint_kind,
            limits,
            credentials,
            permits: Arc::new(tokio::sync::Semaphore::new(limits.max_clients)),
            cancellation: CancellationToken::new(),
            serving: Arc::new(AtomicBool::new(false)),
            stopped: Arc::new(tokio::sync::Notify::new()),
            drain_report: Arc::new(Mutex::new(None)),
            drain_timeout: Arc::new(Mutex::new(limits.drain_timeout)),
            metrics: Arc::new(Metrics::default()),
            actor: Mutex::new(None),
            serve_error: Arc::new(Mutex::new(None)),
        })
    }

    pub async fn serve<H, F>(&self, handler: H) -> io::Result<SocketDrainReport>
    where
        H: Fn(UnixStream, ConnectionContext) -> F + Clone + Send + Sync + 'static,
        F: Future<Output = io::Result<()>> + Send + 'static,
    {
        self.start_actor(handler, None, false, None)?;
        self.wait_for_actor().await
    }

    pub async fn serve_with_startup<H, F>(
        &self,
        startup: RequiredStartupTask,
        handler: H,
    ) -> io::Result<SocketDrainReport>
    where
        H: Fn(UnixStream, ConnectionContext) -> F + Clone + Send + Sync + 'static,
        F: Future<Output = io::Result<()>> + Send + 'static,
    {
        self.start_actor(handler, None, false, Some(startup))?;
        self.wait_for_actor().await
    }

    pub async fn serve_one<H, F>(&self, handler: H) -> io::Result<()>
    where
        H: Fn(UnixStream, ConnectionContext) -> F + Clone + Send + Sync + 'static,
        F: Future<Output = io::Result<()>> + Send + 'static,
    {
        self.start_actor(handler, Some(1), true, None)?;
        self.wait_for_actor().await.map(|_| ())
    }

    pub async fn shutdown_and_drain(&self) -> io::Result<SocketDrainReport> {
        self.shutdown_and_drain_with_timeout(self.limits.drain_timeout)
            .await
    }

    pub async fn shutdown_and_drain_with_timeout(
        &self,
        timeout: Duration,
    ) -> io::Result<SocketDrainReport> {
        if timeout.is_zero() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "socket drain timeout must be nonzero",
            ));
        }
        {
            let mut drain_timeout = self.drain_timeout.lock().unwrap();
            *drain_timeout = (*drain_timeout).min(timeout);
        }
        self.cancellation.cancel();
        if self.serving.load(Ordering::Acquire) {
            self.wait_until_stopped().await;
        }
        self.reap_actor().await?;
        Ok(self.drain_report.lock().unwrap().unwrap_or_default())
    }

    pub fn path(&self) -> &Path {
        self.endpoint.path()
    }

    pub fn metrics(&self) -> SupervisorMetrics {
        SupervisorMetrics {
            active: self.metrics.active.load(Ordering::Acquire),
            tracked: self.metrics.tracked.load(Ordering::Acquire),
            completed: self.metrics.completed.load(Ordering::Acquire),
            rejected: self.metrics.rejected.load(Ordering::Acquire),
            available_permits: self.permits.available_permits(),
        }
    }

    fn start_actor<H, F>(
        &self,
        handler: H,
        accept_limit: Option<usize>,
        propagate_handler_error: bool,
        startup: Option<RequiredStartupTask>,
    ) -> io::Result<()>
    where
        H: Fn(UnixStream, ConnectionContext) -> F + Clone + Send + Sync + 'static,
        F: Future<Output = io::Result<()>> + Send + 'static,
    {
        let mut actor_slot = self.actor.lock().unwrap();
        if self.serving.swap(true, Ordering::AcqRel) {
            return Err(io::Error::new(
                io::ErrorKind::AlreadyExists,
                "socket supervisor is already serving",
            ));
        }
        let parts = ActorParts {
            endpoint: Arc::clone(&self.endpoint),
            endpoint_kind: self.endpoint_kind,
            limits: self.limits,
            credentials: Arc::clone(&self.credentials),
            permits: Arc::clone(&self.permits),
            cancellation: self.cancellation.clone(),
            serving: Arc::clone(&self.serving),
            stopped: Arc::clone(&self.stopped),
            drain_report: Arc::clone(&self.drain_report),
            drain_timeout: Arc::clone(&self.drain_timeout),
            metrics: Arc::clone(&self.metrics),
            serve_error: Arc::clone(&self.serve_error),
        };
        let actor = tokio::spawn(run_actor(
            parts,
            handler,
            accept_limit,
            propagate_handler_error,
            startup,
        ));
        *actor_slot = Some(actor);
        Ok(())
    }

    async fn wait_for_actor(&self) -> io::Result<SocketDrainReport> {
        let mut guard = ServeWaiterGuard {
            cancellation: self.cancellation.clone(),
            actor: &self.actor,
            armed: true,
        };
        self.wait_until_stopped().await;
        guard.armed = false;
        self.reap_actor().await?;
        if let Some(error) = self.serve_error.lock().unwrap().clone() {
            return Err(error.into_io_error());
        }
        Ok(self.drain_report.lock().unwrap().unwrap_or_default())
    }

    async fn wait_until_stopped(&self) {
        loop {
            let stopped = self.stopped.notified();
            if !self.serving.load(Ordering::Acquire) {
                break;
            }
            stopped.await;
        }
    }

    async fn reap_actor(&self) -> io::Result<()> {
        let actor = self.actor.lock().unwrap().take();
        if let Some(actor) = actor {
            actor.await.map_err(|error| {
                io::Error::other(format!("socket supervisor task failed: {error}"))
            })?;
        }
        Ok(())
    }
}

struct ActiveConnectionGuard {
    metrics: Arc<Metrics>,
}

impl Drop for ActiveConnectionGuard {
    fn drop(&mut self) {
        self.metrics.active.fetch_sub(1, Ordering::AcqRel);
    }
}

impl Drop for SocketSupervisor {
    fn drop(&mut self) {
        self.cancellation.cancel();
    }
}

#[derive(Clone)]
struct StoredError {
    kind: io::ErrorKind,
    message: String,
}

impl StoredError {
    fn from_io_error(error: &io::Error) -> Self {
        Self {
            kind: error.kind(),
            message: error.to_string(),
        }
    }

    fn into_io_error(self) -> io::Error {
        io::Error::new(self.kind, self.message)
    }
}

struct ActorParts {
    endpoint: Arc<PrivateSocketEndpoint>,
    endpoint_kind: EndpointKind,
    limits: ConnectionLimits,
    credentials: Arc<dyn PeerCredentialProvider>,
    permits: Arc<tokio::sync::Semaphore>,
    cancellation: CancellationToken,
    serving: Arc<AtomicBool>,
    stopped: Arc<tokio::sync::Notify>,
    drain_report: Arc<Mutex<Option<SocketDrainReport>>>,
    drain_timeout: Arc<Mutex<Duration>>,
    metrics: Arc<Metrics>,
    serve_error: Arc<Mutex<Option<StoredError>>>,
}

async fn run_actor<H, F>(
    parts: ActorParts,
    handler: H,
    accept_limit: Option<usize>,
    propagate_handler_error: bool,
    startup: Option<RequiredStartupTask>,
) where
    H: Fn(UnixStream, ConnectionContext) -> F + Clone + Send + Sync + 'static,
    F: Future<Output = io::Result<()>> + Send + 'static,
{
    let _guard = ActorGuard {
        serving: Arc::clone(&parts.serving),
        stopped: Arc::clone(&parts.stopped),
        metrics: Arc::clone(&parts.metrics),
    };
    let mut tasks = JoinSet::new();
    let mut accepted_count = 0;
    let mut accept_error = None;
    let mut first_handler_error = None;

    if let Some(startup) = startup {
        if let Err(error) = startup.ready_and_wait().await {
            parts.cancellation.cancel();
            *parts.drain_report.lock().unwrap() = Some(SocketDrainReport::default());
            *parts.serve_error.lock().unwrap() = Some(StoredError::from_io_error(&error));
            return;
        }
    }

    loop {
        tokio::select! {
            biased;
            _ = parts.cancellation.cancelled() => break,
            joined = tasks.join_next(), if !tasks.is_empty() => {
                if let Some(joined) = joined {
                    let (_, handler_error) = record_completion(&parts, joined, &tasks);
                    if propagate_handler_error && first_handler_error.is_none() {
                        first_handler_error = handler_error;
                    }
                }
            }
            accepted = parts.endpoint.accept() => {
                let stream = match accepted {
                    Ok(stream) => stream,
                    Err(error) => {
                        accept_error = Some(StoredError::from_io_error(&error));
                        break;
                    }
                };
                let peer = match parts.credentials.peer_credentials(&stream) {
                    Ok(peer) => peer,
                    Err(_) => {
                        let rejected = parts.metrics.rejected.fetch_add(1, Ordering::AcqRel) + 1;
                        log_rejection(parts.endpoint_kind, None, "peer-credentials", rejected);
                        continue;
                    }
                };
                if peer.uid != parts.endpoint.expected_uid() {
                    let rejected = parts.metrics.rejected.fetch_add(1, Ordering::AcqRel) + 1;
                    log_rejection(parts.endpoint_kind, Some(peer), "uid-mismatch", rejected);
                    continue;
                }
                let permit = match Arc::clone(&parts.permits).try_acquire_owned() {
                    Ok(permit) => permit,
                    Err(_) => {
                        let rejected = parts.metrics.rejected.fetch_add(1, Ordering::AcqRel) + 1;
                        log_rejection(parts.endpoint_kind, Some(peer), "client-limit", rejected);
                        continue;
                    }
                };
                let context = ConnectionContext {
                    peer_pid: peer.pid,
                    peer_uid: peer.uid,
                    cancellation: parts.cancellation.child_token(),
                    limits: parts.limits,
                };
                let handler = handler.clone();
                let metrics = Arc::clone(&parts.metrics);
                metrics.active.fetch_add(1, Ordering::AcqRel);
                tasks.spawn(async move {
                    let _permit = permit;
                    let _active = ActiveConnectionGuard { metrics };
                    handler(stream, context).await
                });
                parts.metrics.tracked.store(tasks.len(), Ordering::Release);
                accepted_count += 1;
                if accept_limit.is_some_and(|limit| accepted_count >= limit) {
                    break;
                }
            }
        }
    }

    if accept_limit.is_none() || accept_error.is_some() {
        parts.cancellation.cancel();
    }
    let (report, drain_error) = drain_actor_tasks(&parts, &mut tasks).await;
    if propagate_handler_error && first_handler_error.is_none() {
        first_handler_error = drain_error;
    }
    parts.cancellation.cancel();
    *parts.drain_report.lock().unwrap() = Some(report);
    *parts.serve_error.lock().unwrap() = accept_error.or(first_handler_error);
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum JoinClassification {
    Completed,
    Aborted,
}

async fn drain_actor_tasks(
    parts: &ActorParts,
    tasks: &mut JoinSet<io::Result<()>>,
) -> (SocketDrainReport, Option<StoredError>) {
    let drain_timeout = *parts.drain_timeout.lock().unwrap();
    let deadline = tokio::time::Instant::now() + drain_timeout;
    let mut report = SocketDrainReport::default();
    let mut first_handler_error = None;
    let mut deadline_elapsed = false;
    while !tasks.is_empty() {
        let joined = if deadline_elapsed {
            tasks.join_next().await
        } else {
            match tokio::time::timeout_at(deadline, tasks.join_next()).await {
                Ok(joined) => joined,
                Err(_) => {
                    deadline_elapsed = true;
                    tasks.abort_all();
                    continue;
                }
            }
        };
        let Some(joined) = joined else {
            break;
        };
        let (classification, handler_error) = record_completion(parts, joined, tasks);
        match classification {
            JoinClassification::Completed => report.completed += 1,
            JoinClassification::Aborted => report.aborted += 1,
        }
        if first_handler_error.is_none() {
            first_handler_error = handler_error;
        }
    }
    parts.metrics.tracked.store(0, Ordering::Release);
    (report, first_handler_error)
}

fn record_completion(
    parts: &ActorParts,
    joined: Result<io::Result<()>, JoinError>,
    tasks: &JoinSet<io::Result<()>>,
) -> (JoinClassification, Option<StoredError>) {
    parts.metrics.completed.fetch_add(1, Ordering::AcqRel);
    parts.metrics.tracked.store(tasks.len(), Ordering::Release);
    let (classification, handler_error, reason) = match joined {
        Ok(Ok(())) => return (JoinClassification::Completed, None),
        Ok(Err(error)) => (
            JoinClassification::Completed,
            Some(StoredError::from_io_error(&error)),
            "handler-error",
        ),
        Err(error) if error.is_cancelled() => {
            (JoinClassification::Aborted, None, "handler-aborted")
        }
        Err(error) => (
            JoinClassification::Completed,
            Some(StoredError {
                kind: io::ErrorKind::Other,
                message: format!("connection handler panicked: {error}"),
            }),
            "handler-panic",
        ),
    };
    log_line(format_args!(
        "sleepy-sessiond endpoint={} reason={reason} active={} tracked={}",
        parts.endpoint_kind,
        parts.metrics.active.load(Ordering::Acquire),
        tasks.len()
    ));
    (classification, handler_error)
}

struct ActorGuard {
    serving: Arc<AtomicBool>,
    stopped: Arc<tokio::sync::Notify>,
    metrics: Arc<Metrics>,
}

impl Drop for ActorGuard {
    fn drop(&mut self) {
        self.metrics.tracked.store(0, Ordering::Release);
        self.serving.store(false, Ordering::Release);
        self.stopped.notify_waiters();
    }
}

struct ServeWaiterGuard<'a> {
    cancellation: CancellationToken,
    actor: &'a Mutex<Option<tokio::task::JoinHandle<()>>>,
    armed: bool,
}

impl Drop for ServeWaiterGuard<'_> {
    fn drop(&mut self) {
        if self.armed {
            self.cancellation.cancel();
            let mut actor_slot = self.actor.lock().unwrap();
            if let Some(actor) = actor_slot.take() {
                match tokio::runtime::Handle::try_current() {
                    Ok(runtime) => drop(runtime.spawn(async move {
                        let _ = actor.await;
                    })),
                    Err(_) => *actor_slot = Some(actor),
                }
            }
        }
    }
}

/// Bound v3 endpoints whose protocol handlers are injected by the desktop
/// domain implementation. Until Task 5 supplies those handlers, callers may
/// accept and close connections without inventing a placeholder document.
pub struct PreparedDesktopSockets {
    events: Arc<SocketSupervisor>,
    requests: Arc<SocketSupervisor>,
}

impl PreparedDesktopSockets {
    pub async fn bind(directory: impl AsRef<Path>, expected_uid: libc::uid_t) -> io::Result<Self> {
        let directory = directory.as_ref();
        let events = SocketSupervisor::bind(
            directory.join("desktop.sock"),
            expected_uid,
            EndpointKind::DesktopStream,
            ConnectionLimits::stream(),
        )
        .await?;
        let requests = SocketSupervisor::bind(
            directory.join("desktop-control.sock"),
            expected_uid,
            EndpointKind::DesktopRequest,
            ConnectionLimits::request(),
        )
        .await?;
        Ok(Self {
            events: Arc::new(events),
            requests: Arc::new(requests),
        })
    }

    pub fn events(&self) -> Arc<SocketSupervisor> {
        Arc::clone(&self.events)
    }

    pub fn requests(&self) -> Arc<SocketSupervisor> {
        Arc::clone(&self.requests)
    }

    pub fn listener_paths(&self) -> [&Path; 2] {
        [self.events.path(), self.requests.path()]
    }
}

fn log_rejection(
    endpoint_kind: EndpointKind,
    peer: Option<PeerCredentials>,
    reason: &'static str,
    rejected: usize,
) {
    if let Some(peer) = peer {
        log_line(format_args!(
            "sleepy-sessiond endpoint={endpoint_kind} peer_pid={} peer_uid={} reason={reason} rejected={rejected}",
            peer.pid, peer.uid
        ));
    } else {
        log_line(format_args!(
            "sleepy-sessiond endpoint={endpoint_kind} reason={reason} rejected={rejected}"
        ));
    }
}

fn log_line(message: fmt::Arguments<'_>) {
    write_log(&mut io::stderr().lock(), message);
}

fn write_log(writer: &mut impl io::Write, message: fmt::Arguments<'_>) {
    let _ = writeln!(writer, "{message}");
}

const STARTUP_PENDING: u8 = 0;
const STARTUP_RELEASED: u8 = 1;
const STARTUP_CANCELLED: u8 = 2;

struct StartupRelease {
    state: AtomicU8,
    async_changed: tokio::sync::Notify,
    blocking_mutex: Mutex<StartupControl>,
    blocking_changed: Condvar,
}

#[derive(Default)]
struct StartupControl {
    failure: Option<StoredError>,
}

impl StartupRelease {
    fn new() -> Self {
        Self {
            state: AtomicU8::new(STARTUP_PENDING),
            async_changed: tokio::sync::Notify::new(),
            blocking_mutex: Mutex::new(StartupControl::default()),
            blocking_changed: Condvar::new(),
        }
    }

    fn transition(&self, state: u8) {
        let _guard = self.blocking_mutex.lock().unwrap();
        self.transition_locked(state);
    }

    fn transition_locked(&self, state: u8) {
        if self
            .state
            .compare_exchange(STARTUP_PENDING, state, Ordering::AcqRel, Ordering::Acquire)
            .is_ok()
        {
            self.async_changed.notify_waiters();
            self.blocking_changed.notify_all();
        }
    }

    fn fail(&self, error: StoredError) -> bool {
        let mut control = self.blocking_mutex.lock().unwrap();
        if self.state.load(Ordering::Acquire) != STARTUP_PENDING {
            return false;
        }
        control.failure.get_or_insert(error);
        self.transition_locked(STARTUP_CANCELLED);
        true
    }

    fn notify_and_release(&self, notify: impl FnOnce() -> io::Result<()>) -> io::Result<()> {
        let control = self.blocking_mutex.lock().unwrap();
        match self.state.load(Ordering::Acquire) {
            STARTUP_CANCELLED => {
                return Err(control
                    .failure
                    .clone()
                    .map_or_else(startup_cancelled, StoredError::into_io_error));
            }
            STARTUP_RELEASED => return Ok(()),
            _ => {}
        }
        if let Err(error) = notify() {
            self.transition_locked(STARTUP_CANCELLED);
            return Err(error);
        }
        self.transition_locked(STARTUP_RELEASED);
        Ok(())
    }

    async fn wait_async(&self) -> io::Result<()> {
        loop {
            let changed = self.async_changed.notified();
            match self.state.load(Ordering::Acquire) {
                STARTUP_RELEASED => return Ok(()),
                STARTUP_CANCELLED => return Err(startup_cancelled()),
                _ => changed.await,
            }
        }
    }

    fn wait_blocking(&self) -> io::Result<()> {
        let mut guard = self.blocking_mutex.lock().unwrap();
        loop {
            match self.state.load(Ordering::Acquire) {
                STARTUP_RELEASED => return Ok(()),
                STARTUP_CANCELLED => return Err(startup_cancelled()),
                _ => guard = self.blocking_changed.wait(guard).unwrap(),
            }
        }
    }
}

fn startup_cancelled() -> io::Error {
    io::Error::new(io::ErrorKind::Interrupted, "daemon startup was cancelled")
}

enum StartupAcknowledgement {
    Ready,
    Failed(StoredError),
}

#[derive(Clone)]
pub(crate) struct StartupTaskCancellation {
    name: &'static str,
    acknowledgements: mpsc::UnboundedSender<StartupAcknowledgement>,
    release: Arc<StartupRelease>,
}

impl StartupTaskCancellation {
    pub(crate) fn fail(&self, error: io::Error) {
        let error = StoredError::from_io_error(&error);
        if self.release.fail(error.clone()) {
            let _ = self
                .acknowledgements
                .send(StartupAcknowledgement::Failed(error));
        }
    }

    fn disappeared(&self) {
        self.fail(io::Error::new(
            io::ErrorKind::BrokenPipe,
            format!(
                "required startup task {} stopped before release handoff",
                self.name
            ),
        ));
    }
}

pub struct RequiredStartupTask {
    cancellation: StartupTaskCancellation,
    handed_off: bool,
}

impl RequiredStartupTask {
    fn acknowledge_ready(&self) {
        let _ = self
            .cancellation
            .acknowledgements
            .send(StartupAcknowledgement::Ready);
    }

    pub(crate) fn cancellation(&self) -> StartupTaskCancellation {
        self.cancellation.clone()
    }

    pub fn fail(self, error: io::Error) {
        self.cancellation.fail(error);
    }

    pub async fn ready_and_wait(mut self) -> io::Result<()> {
        self.acknowledge_ready();
        let result = self.cancellation.release.wait_async().await;
        if result.is_ok() {
            self.handed_off = true;
        }
        result
    }

    pub fn ready_and_wait_blocking(mut self) -> io::Result<()> {
        self.acknowledge_ready();
        let result = self.cancellation.release.wait_blocking();
        if result.is_ok() {
            self.handed_off = true;
        }
        result
    }
}

impl Drop for RequiredStartupTask {
    fn drop(&mut self) {
        if !self.handed_off {
            self.cancellation.disappeared();
        }
    }
}

pub struct StartupBarrier {
    acknowledgements: mpsc::UnboundedReceiver<StartupAcknowledgement>,
    sender: mpsc::UnboundedSender<StartupAcknowledgement>,
    release: Arc<StartupRelease>,
    required: usize,
    waiting: bool,
}

impl Default for StartupBarrier {
    fn default() -> Self {
        Self::new()
    }
}

impl StartupBarrier {
    pub fn new() -> Self {
        let (sender, acknowledgements) = mpsc::unbounded_channel();
        Self {
            acknowledgements,
            sender,
            release: Arc::new(StartupRelease::new()),
            required: 0,
            waiting: false,
        }
    }

    pub fn required_task(&mut self, name: &'static str) -> RequiredStartupTask {
        assert!(!self.waiting, "required startup tasks are already frozen");
        self.required += 1;
        RequiredStartupTask {
            cancellation: StartupTaskCancellation {
                name,
                acknowledgements: self.sender.clone(),
                release: Arc::clone(&self.release),
            },
            handed_off: false,
        }
    }

    async fn wait_until_started(&mut self) -> io::Result<()> {
        self.waiting = true;
        for _ in 0..self.required {
            match self.acknowledgements.recv().await {
                Some(StartupAcknowledgement::Ready) => {}
                Some(StartupAcknowledgement::Failed(error)) => {
                    self.cancel();
                    return Err(error.into_io_error());
                }
                None => {
                    self.cancel();
                    return Err(io::Error::new(
                        io::ErrorKind::BrokenPipe,
                        "required startup acknowledgement channel closed",
                    ));
                }
            }
        }
        Ok(())
    }

    fn notify_and_release(&self, notify: impl FnOnce() -> io::Result<()>) -> io::Result<()> {
        self.release.notify_and_release(notify)
    }

    fn cancel(&self) {
        self.release.transition(STARTUP_CANCELLED);
    }
}

impl Drop for StartupBarrier {
    fn drop(&mut self) {
        self.cancel();
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DaemonNotification {
    Ready,
    Stopping,
}

pub trait DaemonNotifier: Send + Sync + 'static {
    fn notify(&self, state: DaemonNotification) -> io::Result<()>;
}

#[derive(Debug, Default)]
pub struct SystemdNotifier;

impl DaemonNotifier for SystemdNotifier {
    fn notify(&self, state: DaemonNotification) -> io::Result<()> {
        match state {
            DaemonNotification::Ready => sd_notify::notify(false, &[sd_notify::NotifyState::Ready]),
            DaemonNotification::Stopping => {
                sd_notify::notify(false, &[sd_notify::NotifyState::Stopping])
            }
        }
    }
}

pub struct ReadinessGate {
    notifier: Arc<dyn DaemonNotifier>,
}

pub struct DaemonLifecycle {
    notifier: Arc<dyn DaemonNotifier>,
}

impl DaemonLifecycle {
    pub fn new<N>(notifier: Arc<N>) -> Self
    where
        N: DaemonNotifier,
    {
        Self { notifier }
    }

    pub async fn complete_startup<P, S, Fut>(
        &self,
        required_listeners: &[&Path],
        startup: &mut StartupBarrier,
        start_producers: S,
    ) -> io::Result<P>
    where
        S: FnOnce() -> Fut,
        Fut: Future<Output = io::Result<P>>,
    {
        if let Err(error) = validate_required_listeners(required_listeners) {
            startup.cancel();
            return Err(error);
        }
        if let Err(error) = startup.wait_until_started().await {
            startup.cancel();
            return Err(error);
        }
        startup.notify_and_release(|| self.notifier.notify(DaemonNotification::Ready))?;
        start_producers().await
    }

    pub async fn stop_and_drain<T, D, Fut>(&self, drain: D) -> io::Result<T>
    where
        D: FnOnce() -> Fut,
        Fut: Future<Output = io::Result<T>>,
    {
        let notification_error = self.notifier.notify(DaemonNotification::Stopping).err();
        let drained = drain().await;
        match notification_error {
            Some(error) => Err(error),
            None => drained,
        }
    }
}

impl ReadinessGate {
    pub fn new<N>(notifier: Arc<N>) -> Self
    where
        N: DaemonNotifier,
    {
        Self { notifier }
    }

    pub fn ready(&self, required_listeners: &[&Path]) -> io::Result<()> {
        validate_required_listeners(required_listeners)?;
        self.notifier.notify(DaemonNotification::Ready)
    }

    pub fn stopping(&self) -> io::Result<()> {
        self.notifier.notify(DaemonNotification::Stopping)
    }
}

fn validate_required_listeners(required_listeners: &[&Path]) -> io::Result<()> {
    let expected_uid = unsafe { libc::geteuid() };
    for path in required_listeners {
        let metadata = std::fs::symlink_metadata(path)?;
        if !metadata.file_type().is_socket() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "required listener path is not a Unix socket",
            ));
        }
        if metadata.uid() != expected_uid || metadata.permissions().mode() & 0o777 != 0o600 {
            return Err(io::Error::new(
                io::ErrorKind::PermissionDenied,
                "required listener is not owned mode-0600",
            ));
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    struct BrokenLogSink;

    impl io::Write for BrokenLogSink {
        fn write(&mut self, _buffer: &[u8]) -> io::Result<usize> {
            Err(io::Error::new(io::ErrorKind::BrokenPipe, "closed log sink"))
        }

        fn flush(&mut self) -> io::Result<()> {
            Err(io::Error::new(io::ErrorKind::BrokenPipe, "closed log sink"))
        }
    }

    #[test]
    fn supervisor_logging_ignores_sink_failures_without_panicking() {
        write_log(
            &mut BrokenLogSink,
            format_args!("sleepy-sessiond endpoint=stream reason=handler-error active=0 tracked=0"),
        );
    }

    #[test]
    fn blocking_startup_transition_is_serialized_with_waiters() {
        let release = Arc::new(StartupRelease::new());
        let guard = release.blocking_mutex.lock().unwrap();
        let (finished_sender, finished) = std::sync::mpsc::channel();
        let transition_release = Arc::clone(&release);
        let transition = std::thread::spawn(move || {
            transition_release.transition(STARTUP_RELEASED);
            finished_sender.send(()).unwrap();
        });

        assert_eq!(
            finished.recv_timeout(Duration::from_millis(100)),
            Err(std::sync::mpsc::RecvTimeoutError::Timeout),
            "a transition must not notify between a blocking waiter's state check and wait"
        );
        assert_eq!(release.state.load(Ordering::Acquire), STARTUP_PENDING);

        drop(guard);
        finished.recv_timeout(Duration::from_secs(1)).unwrap();
        transition.join().unwrap();
        assert_eq!(release.state.load(Ordering::Acquire), STARTUP_RELEASED);
    }

    #[tokio::test]
    async fn oversized_legacy_output_is_invalid_data_not_a_timeout() {
        let context = ConnectionContext {
            peer_pid: 1,
            peer_uid: 1,
            cancellation: CancellationToken::new(),
            limits: ConnectionLimits::stream(),
        };
        let mut writer = tokio::io::sink();

        let error = context
            .write_legacy_frame(&mut writer, &vec![b'x'; ONE_MIB + 1])
            .await
            .unwrap_err();

        assert_eq!(error.kind(), io::ErrorKind::InvalidData);
    }

    #[tokio::test]
    async fn dropping_the_outer_serve_waiter_transfers_the_actor_to_a_reaper() {
        use std::os::unix::fs::PermissionsExt;

        let temp = tempfile::tempdir().unwrap();
        let parent = temp.path().join("sleepy");
        std::fs::create_dir(&parent).unwrap();
        std::fs::set_permissions(&parent, std::fs::Permissions::from_mode(0o700)).unwrap();
        let supervisor = Arc::new(
            SocketSupervisor::bind(
                parent.join("session.sock"),
                unsafe { libc::geteuid() },
                EndpointKind::Stream,
                ConnectionLimits::stream(),
            )
            .await
            .unwrap(),
        );
        let serving = Arc::clone(&supervisor);
        let task = tokio::spawn(async move {
            serving
                .serve(|_stream, _context| async move { Ok(()) })
                .await
        });
        while !supervisor.serving.load(Ordering::Acquire) {
            tokio::task::yield_now().await;
        }
        assert!(supervisor.actor.lock().unwrap().is_some());

        task.abort();
        assert!(task.await.unwrap_err().is_cancelled());

        assert!(
            supervisor.actor.lock().unwrap().is_none(),
            "the dropped public serve future must not leave its actor handle detached"
        );
        supervisor.shutdown_and_drain().await.unwrap();
    }
}
