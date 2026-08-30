// SPDX-License-Identifier: GPL-3.0-only

use std::{
    fmt,
    future::Future,
    io,
    os::unix::fs::{FileTypeExt, MetadataExt, PermissionsExt},
    path::Path,
    sync::{
        atomic::{AtomicBool, AtomicUsize, Ordering},
        Arc, Mutex,
    },
    time::Duration,
};

use tokio::{
    io::{AsyncBufReadExt, AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt, BufReader},
    net::UnixStream,
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
    pub async fn read_frame<R>(&self, reader: &mut R) -> io::Result<Vec<u8>>
    where
        R: AsyncRead + Unpin,
    {
        let read = async {
            let maximum_read = self.limits.max_frame_bytes + 1;
            let mut limited = BufReader::new(reader).take(maximum_read as u64);
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
                io::ErrorKind::TimedOut,
                "legacy frame cannot complete within the bounded write contract",
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
    endpoint: PrivateSocketEndpoint,
    endpoint_kind: EndpointKind,
    limits: ConnectionLimits,
    credentials: Arc<dyn PeerCredentialProvider>,
    permits: Arc<tokio::sync::Semaphore>,
    cancellation: CancellationToken,
    serving: AtomicBool,
    stopped: tokio::sync::Notify,
    drain_report: Mutex<Option<SocketDrainReport>>,
    drain_timeout: Mutex<Duration>,
    metrics: Arc<Metrics>,
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
            endpoint: PrivateSocketEndpoint::bind_with_observer(path, expected_uid, observer)
                .await?,
            endpoint_kind,
            limits,
            credentials,
            permits: Arc::new(tokio::sync::Semaphore::new(limits.max_clients)),
            cancellation: CancellationToken::new(),
            serving: AtomicBool::new(false),
            stopped: tokio::sync::Notify::new(),
            drain_report: Mutex::new(None),
            drain_timeout: Mutex::new(limits.drain_timeout),
            metrics: Arc::new(Metrics::default()),
        })
    }

    pub async fn serve<H, F>(&self, handler: H) -> io::Result<SocketDrainReport>
    where
        H: Fn(UnixStream, ConnectionContext) -> F + Clone + Send + Sync + 'static,
        F: Future<Output = io::Result<()>> + Send + 'static,
    {
        if self.serving.swap(true, Ordering::AcqRel) {
            return Err(io::Error::new(
                io::ErrorKind::AlreadyExists,
                "socket supervisor is already serving",
            ));
        }
        let _guard = ServeGuard {
            serving: &self.serving,
            stopped: &self.stopped,
        };
        let mut tasks = JoinSet::new();
        let mut accept_error = None;

        loop {
            tokio::select! {
                biased;
                _ = self.cancellation.cancelled() => break,
                joined = tasks.join_next(), if !tasks.is_empty() => {
                    if let Some(joined) = joined {
                        self.record_completion(joined, &tasks);
                    }
                }
                accepted = self.endpoint.accept() => {
                    let stream = match accepted {
                        Ok(stream) => stream,
                        Err(error) => {
                            accept_error = Some(error);
                            break;
                        }
                    };
                    let peer = match self.credentials.peer_credentials(&stream) {
                        Ok(peer) => peer,
                        Err(_) => {
                            let rejected = self.metrics.rejected.fetch_add(1, Ordering::AcqRel) + 1;
                            log_rejection(self.endpoint_kind, None, "peer-credentials", rejected);
                            continue;
                        }
                    };
                    if peer.uid != self.endpoint.expected_uid() {
                        let rejected = self.metrics.rejected.fetch_add(1, Ordering::AcqRel) + 1;
                        log_rejection(self.endpoint_kind, Some(peer), "uid-mismatch", rejected);
                        continue;
                    }
                    let permit = match Arc::clone(&self.permits).try_acquire_owned() {
                        Ok(permit) => permit,
                        Err(_) => {
                            let rejected = self.metrics.rejected.fetch_add(1, Ordering::AcqRel) + 1;
                            log_rejection(self.endpoint_kind, Some(peer), "client-limit", rejected);
                            continue;
                        }
                    };
                    let context = ConnectionContext {
                        peer_pid: peer.pid,
                        peer_uid: peer.uid,
                        cancellation: self.cancellation.child_token(),
                        limits: self.limits,
                    };
                    let handler = handler.clone();
                    let metrics = Arc::clone(&self.metrics);
                    metrics.active.fetch_add(1, Ordering::AcqRel);
                    tasks.spawn(async move {
                        let _permit = permit;
                        let _active = ActiveConnectionGuard { metrics };
                        handler(stream, context).await
                    });
                    self.metrics.tracked.store(tasks.len(), Ordering::Release);
                }
            }
        }

        self.cancellation.cancel();
        let report = self.drain_tasks(&mut tasks).await;
        *self.drain_report.lock().unwrap() = Some(report);
        if let Some(error) = accept_error {
            Err(error)
        } else {
            Ok(report)
        }
    }

    pub async fn serve_one<H, F>(&self, handler: H) -> io::Result<()>
    where
        H: FnOnce(UnixStream, ConnectionContext) -> F,
        F: Future<Output = io::Result<()>>,
    {
        let stream = self.endpoint.accept().await?;
        let peer = self.credentials.peer_credentials(&stream)?;
        if peer.uid != self.endpoint.expected_uid() {
            return Err(io::Error::new(
                io::ErrorKind::PermissionDenied,
                "socket peer UID mismatch",
            ));
        }
        handler(
            stream,
            ConnectionContext {
                peer_pid: peer.pid,
                peer_uid: peer.uid,
                cancellation: self.cancellation.child_token(),
                limits: self.limits,
            },
        )
        .await
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
        if !self.serving.load(Ordering::Acquire) {
            return Ok(self.drain_report.lock().unwrap().unwrap_or_default());
        }
        loop {
            let stopped = self.stopped.notified();
            if !self.serving.load(Ordering::Acquire) {
                break;
            }
            stopped.await;
        }
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
        }
    }

    async fn drain_tasks(&self, tasks: &mut JoinSet<io::Result<()>>) -> SocketDrainReport {
        let drain_timeout = *self.drain_timeout.lock().unwrap();
        let deadline = tokio::time::Instant::now() + drain_timeout;
        let mut report = SocketDrainReport::default();
        while !tasks.is_empty() {
            match tokio::time::timeout_at(deadline, tasks.join_next()).await {
                Ok(Some(joined)) => {
                    report.completed += 1;
                    self.record_completion(joined, tasks);
                }
                Ok(None) => break,
                Err(_) => {
                    let remaining = tasks.len();
                    tasks.abort_all();
                    while let Some(joined) = tasks.join_next().await {
                        self.record_completion(joined, tasks);
                    }
                    report.aborted += remaining;
                    break;
                }
            }
        }
        self.metrics.tracked.store(0, Ordering::Release);
        report
    }

    fn record_completion(
        &self,
        joined: Result<io::Result<()>, JoinError>,
        tasks: &JoinSet<io::Result<()>>,
    ) {
        self.metrics.completed.fetch_add(1, Ordering::AcqRel);
        self.metrics.tracked.store(tasks.len(), Ordering::Release);
        let reason = match joined {
            Ok(Ok(())) => return,
            Ok(Err(_)) => "handler-error",
            Err(error) if error.is_cancelled() => "handler-aborted",
            Err(_) => "handler-panic",
        };
        eprintln!(
            "sleepy-sessiond endpoint={} reason={reason} active={} tracked={}",
            self.endpoint_kind,
            self.metrics.active.load(Ordering::Acquire),
            tasks.len()
        );
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

struct ServeGuard<'a> {
    serving: &'a AtomicBool,
    stopped: &'a tokio::sync::Notify,
}

impl Drop for ServeGuard<'_> {
    fn drop(&mut self) {
        self.serving.store(false, Ordering::Release);
        self.stopped.notify_waiters();
    }
}

fn log_rejection(
    endpoint_kind: EndpointKind,
    peer: Option<PeerCredentials>,
    reason: &'static str,
    rejected: usize,
) {
    if let Some(peer) = peer {
        eprintln!(
            "sleepy-sessiond endpoint={endpoint_kind} peer_pid={} peer_uid={} reason={reason} rejected={rejected}",
            peer.pid, peer.uid
        );
    } else {
        eprintln!("sleepy-sessiond endpoint={endpoint_kind} reason={reason} rejected={rejected}");
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

impl ReadinessGate {
    pub fn new<N>(notifier: Arc<N>) -> Self
    where
        N: DaemonNotifier,
    {
        Self { notifier }
    }

    pub fn ready(&self, required_listeners: &[&Path]) -> io::Result<()> {
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
        self.notifier.notify(DaemonNotification::Ready)
    }

    pub fn stopping(&self) -> io::Result<()> {
        self.notifier.notify(DaemonNotification::Stopping)
    }
}
