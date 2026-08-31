// SPDX-License-Identifier: GPL-3.0-only

use std::{
    collections::HashMap,
    ffi::CString,
    fmt, io,
    os::unix::ffi::OsStrExt,
    path::Path,
    sync::{
        atomic::{AtomicBool, Ordering},
        mpsc as std_mpsc, Arc, Mutex as StdMutex,
    },
    thread,
    time::Duration,
};

use async_trait::async_trait;
use dbus::{
    arg::{ArgType, Variant},
    blocking::SyncConnection,
    channel::{MatchingReceiver, Sender},
    message::MatchRule,
    strings::ErrorName,
    Message,
};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::UnixStream,
    sync::Mutex,
    time::Instant,
};
use zeroize::Zeroize;

use crate::sessiond::supervisor::{
    ConnectionContext, ConnectionLimits, EndpointKind, RequiredStartupTask, SocketDrainReport,
    SocketSupervisor,
};

pub const MAX_SECRET_FRAME: usize = 64 * 1024;
const SECRET_DEADLINE: Duration = Duration::from_secs(30);

pub trait SecretZeroizeObserver: Send + Sync + 'static {
    fn after_zeroize(&self, bytes: &[u8]);
}

struct NoopZeroizeObserver;

impl SecretZeroizeObserver for NoopZeroizeObserver {
    fn after_zeroize(&self, _bytes: &[u8]) {}
}

pub struct LockedSecret {
    bytes: Vec<u8>,
    secret_offset: usize,
    locked: bool,
    observer: Arc<dyn SecretZeroizeObserver>,
}

impl fmt::Debug for LockedSecret {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("LockedSecret")
            .field("length", &self.expose().len())
            .finish_non_exhaustive()
    }
}

impl LockedSecret {
    fn from_frame(
        bytes: Vec<u8>,
        secret_offset: usize,
        observer: Arc<dyn SecretZeroizeObserver>,
    ) -> Self {
        let locked = if bytes.is_empty() {
            false
        } else {
            unsafe { libc::mlock(bytes.as_ptr().cast(), bytes.len()) == 0 }
        };
        Self {
            bytes,
            secret_offset,
            locked,
            observer,
        }
    }

    pub fn expose(&self) -> &[u8] {
        &self.bytes[self.secret_offset..]
    }
}

impl Drop for LockedSecret {
    fn drop(&mut self) {
        self.bytes.zeroize();
        std::sync::atomic::compiler_fence(std::sync::atomic::Ordering::SeqCst);
        self.observer.after_zeroize(&self.bytes);
        if self.locked && !self.bytes.is_empty() {
            unsafe {
                libc::munlock(self.bytes.as_ptr().cast(), self.bytes.len());
            }
        }
    }
}

#[derive(Clone)]
pub struct SecretBroker {
    pending: Arc<Mutex<Option<PendingChallenge>>>,
    observer: Arc<dyn SecretZeroizeObserver>,
}

struct PendingChallenge {
    id: [u8; 16],
    deadline: Instant,
}

impl Default for SecretBroker {
    fn default() -> Self {
        Self::with_observer(Arc::new(NoopZeroizeObserver))
    }
}

impl SecretBroker {
    pub fn with_observer(observer: Arc<dyn SecretZeroizeObserver>) -> Self {
        Self {
            pending: Arc::new(Mutex::new(None)),
            observer,
        }
    }

    pub async fn issue(&self) -> io::Result<[u8; 16]> {
        let mut pending = self.pending.lock().await;
        if pending.is_some() {
            return Err(io::Error::new(
                io::ErrorKind::WouldBlock,
                "a network secret challenge is already pending",
            ));
        }
        let id = *uuid::Uuid::new_v4().as_bytes();
        *pending = Some(PendingChallenge {
            id,
            deadline: Instant::now() + SECRET_DEADLINE,
        });
        Ok(id)
    }

    pub async fn accept_response(&self, response: Vec<u8>) -> io::Result<LockedSecret> {
        let locked = LockedSecret::from_frame(response, 0, Arc::clone(&self.observer));
        self.accept_locked_response(locked).await
    }

    async fn accept_locked_response(&self, mut locked: LockedSecret) -> io::Result<LockedSecret> {
        if locked.bytes.len() > MAX_SECRET_FRAME || locked.bytes.len() <= 16 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "network secret frame violates its bounded size",
            ));
        }
        let mut pending = self.pending.lock().await;
        let challenge = pending.take().ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::PermissionDenied,
                "network secret challenge is absent or was already consumed",
            )
        })?;
        if Instant::now() >= challenge.deadline {
            return Err(io::Error::new(
                io::ErrorKind::TimedOut,
                "network secret challenge exceeded its total deadline",
            ));
        }
        if !constant_time_equal(&locked.bytes[..16], &challenge.id) {
            return Err(io::Error::new(
                io::ErrorKind::PermissionDenied,
                "network secret challenge did not match",
            ));
        }
        locked.secret_offset = 16;
        Ok(locked)
    }

    async fn cancel_pending(&self) {
        self.pending.lock().await.take();
    }
}

#[async_trait]
pub trait NetworkSecretExchange: Send + Sync + 'static {
    fn has_pending_request(&self) -> bool;
    async fn submit(&self, secret: LockedSecret) -> io::Result<()>;
}

#[derive(Debug, Default)]
pub struct UnavailableNetworkManagerExchange;

#[async_trait]
impl NetworkSecretExchange for UnavailableNetworkManagerExchange {
    fn has_pending_request(&self) -> bool {
        false
    }

    async fn submit(&self, _secret: LockedSecret) -> io::Result<()> {
        Err(io::Error::new(
            io::ErrorKind::Unsupported,
            "NetworkManager secret-agent exchange is unavailable",
        ))
    }
}

pub struct NetworkManagerSecretExchange {
    pending: StdMutex<Option<std_mpsc::SyncSender<LockedSecret>>>,
}

impl NetworkManagerSecretExchange {
    fn new() -> Arc<Self> {
        Arc::new(Self {
            pending: StdMutex::new(None),
        })
    }

    fn begin(&self) -> io::Result<std_mpsc::Receiver<LockedSecret>> {
        let mut pending = self
            .pending
            .lock()
            .map_err(|_| io::Error::other("NetworkManager secret exchange lock poisoned"))?;
        if pending.is_some() {
            return Err(io::Error::new(
                io::ErrorKind::WouldBlock,
                "NetworkManager already has a pending secret request",
            ));
        }
        let (sender, receiver) = std_mpsc::sync_channel(1);
        *pending = Some(sender);
        Ok(receiver)
    }

    fn cancel(&self) {
        if let Ok(mut pending) = self.pending.lock() {
            pending.take();
        }
    }
}

#[async_trait]
impl NetworkSecretExchange for NetworkManagerSecretExchange {
    fn has_pending_request(&self) -> bool {
        self.pending.lock().is_ok_and(|pending| pending.is_some())
    }

    async fn submit(&self, secret: LockedSecret) -> io::Result<()> {
        let sender = self
            .pending
            .lock()
            .map_err(|_| io::Error::other("NetworkManager secret exchange lock poisoned"))?
            .take()
            .ok_or_else(|| {
                io::Error::new(
                    io::ErrorKind::NotConnected,
                    "NetworkManager did not request a secret",
                )
            })?;
        sender.send(secret).map_err(|error| {
            drop(error.0);
            io::Error::new(
                io::ErrorKind::BrokenPipe,
                "NetworkManager secret request was cancelled",
            )
        })
    }
}

pub struct NetworkManagerSecretAgent {
    stop: Arc<AtomicBool>,
    exchange: Arc<NetworkManagerSecretExchange>,
    thread: Option<thread::JoinHandle<()>>,
    workers: Arc<StdMutex<Vec<thread::JoinHandle<()>>>>,
}

impl NetworkManagerSecretAgent {
    pub fn start_if_available() -> io::Result<(Option<Self>, Arc<dyn NetworkSecretExchange>)> {
        let connection = match SyncConnection::new_system() {
            Ok(connection) => connection,
            Err(_) => {
                return Ok((None, Arc::new(UnavailableNetworkManagerExchange)));
            }
        };
        let connection = Arc::new(connection);
        let exchange = NetworkManagerSecretExchange::new();
        let workers = Arc::new(StdMutex::new(Vec::new()));
        register_network_manager_methods(&connection, Arc::clone(&exchange), Arc::clone(&workers));
        let manager = connection.with_proxy(
            "org.freedesktop.NetworkManager",
            "/org/freedesktop/NetworkManager/AgentManager",
            Duration::from_secs(5),
        );
        let registration: Result<(), dbus::Error> = manager.method_call(
            "org.freedesktop.NetworkManager.AgentManager",
            "RegisterWithCapabilities",
            ("org.sleepylinux.SleepySession", 0_u32),
        );
        if registration.is_err() {
            return Ok((None, Arc::new(UnavailableNetworkManagerExchange)));
        }
        let stop = Arc::new(AtomicBool::new(false));
        let thread_stop = Arc::clone(&stop);
        let thread = thread::Builder::new()
            .name("sleepy-network-secret-agent".into())
            .spawn(move || {
                while !thread_stop.load(Ordering::Acquire) {
                    if connection.process(Duration::from_millis(25)).is_err() {
                        return;
                    }
                }
            })?;
        Ok((
            Some(Self {
                stop,
                exchange: Arc::clone(&exchange),
                thread: Some(thread),
                workers,
            }),
            exchange,
        ))
    }
}

impl Drop for NetworkManagerSecretAgent {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::Release);
        self.exchange.cancel();
        if let Some(thread) = self.thread.take() {
            let _ = thread.join();
        }
        let workers = self
            .workers
            .lock()
            .map(|mut workers| workers.drain(..).collect::<Vec<_>>())
            .unwrap_or_default();
        for worker in workers {
            let _ = worker.join();
        }
    }
}

const NM_AGENT_PATH: &str = "/org/freedesktop/NetworkManager/SecretAgent";
const NM_AGENT_INTERFACE: &str = "org.freedesktop.NetworkManager.SecretAgent";

fn register_network_manager_methods(
    connection: &Arc<SyncConnection>,
    exchange: Arc<NetworkManagerSecretExchange>,
    workers: Arc<StdMutex<Vec<thread::JoinHandle<()>>>>,
) {
    let mut rule = MatchRule::new_method_call();
    rule.path = Some(NM_AGENT_PATH.into());
    rule.interface = Some(NM_AGENT_INTERFACE.into());
    let callback_connection = Arc::clone(connection);
    connection.start_receive(
        rule,
        Box::new(move |message, channel| {
            if message.member().as_deref() == Some("GetSecrets") {
                match begin_get_secrets(&message, &exchange) {
                    Ok(receiver) => {
                        let connection = Arc::clone(&callback_connection);
                        let exchange = Arc::clone(&exchange);
                        let worker = thread::spawn(move || {
                            let (reply, retained) =
                                finish_get_secrets(&message, &exchange, receiver);
                            let _ = connection.send(reply);
                            // Keep the locked owner alive through libdbus serialization/send.
                            // libdbus necessarily owns its transport copy after append; the
                            // daemon creates no Rust String/serde copy of the secret.
                            drop(retained);
                        });
                        if let Ok(mut handles) = workers.lock() {
                            let mut index = 0;
                            while index < handles.len() {
                                if handles[index].is_finished() {
                                    let finished = handles.swap_remove(index);
                                    let _ = finished.join();
                                } else {
                                    index += 1;
                                }
                            }
                            handles.push(worker);
                        }
                    }
                    Err(error) => {
                        let _ = channel.send(secret_error_reply(&message, &error));
                    }
                }
                return true;
            }
            let (reply, retained) = handle_network_manager_method(&message, &exchange);
            let _ = channel.send(reply);
            drop(retained);
            true
        }),
    );
}

fn handle_network_manager_method(
    message: &Message,
    exchange: &NetworkManagerSecretExchange,
) -> (Message, Option<LockedSecret>) {
    let result = match message.member().as_deref() {
        Some("GetSecrets") => Err(io::Error::new(
            io::ErrorKind::WouldBlock,
            "NetworkManager secret request was already dispatched",
        )),
        Some("CancelGetSecrets") => read_cancel_args(message).map(|_| {
            exchange.cancel();
            (message.method_return(), None)
        }),
        Some("SaveSecrets") | Some("DeleteSecrets") => {
            read_connection_and_path(message).map(|_| (message.method_return(), None))
        }
        _ => Err(io::Error::new(
            io::ErrorKind::Unsupported,
            "unknown NetworkManager secret-agent method",
        )),
    };
    result.unwrap_or_else(|error| (secret_error_reply(message, &error), None))
}

fn begin_get_secrets(
    message: &Message,
    exchange: &NetworkManagerSecretExchange,
) -> io::Result<std_mpsc::Receiver<LockedSecret>> {
    let mut arguments = message.iter_init();
    require_argument(&mut arguments, "a{sa{sv}}", "connection settings")?;
    let _path: dbus::Path<'_> = arguments.read().map_err(invalid_dbus_args)?;
    let setting_name: &str = arguments.read().map_err(invalid_dbus_args)?;
    require_argument(&mut arguments, "as", "secret hints")?;
    let _flags: u32 = arguments.read().map_err(invalid_dbus_args)?;
    require_end(&mut arguments)?;
    if setting_name != "802-11-wireless-security" {
        return Err(io::Error::new(
            io::ErrorKind::Unsupported,
            "only Wi-Fi security secrets are supported",
        ));
    }
    exchange.begin()
}

fn finish_get_secrets(
    message: &Message,
    exchange: &NetworkManagerSecretExchange,
    receiver: std_mpsc::Receiver<LockedSecret>,
) -> (Message, Option<LockedSecret>) {
    match finish_get_secrets_result(message, exchange, receiver) {
        Ok(result) => result,
        Err(error) => (secret_error_reply(message, &error), None),
    }
}

fn finish_get_secrets_result(
    message: &Message,
    exchange: &NetworkManagerSecretExchange,
    receiver: std_mpsc::Receiver<LockedSecret>,
) -> io::Result<(Message, Option<LockedSecret>)> {
    let secret = receiver.recv_timeout(SECRET_DEADLINE).map_err(|error| {
        exchange.cancel();
        match error {
            std_mpsc::RecvTimeoutError::Timeout => io::Error::new(
                io::ErrorKind::TimedOut,
                "network secret response exceeded its total deadline",
            ),
            std_mpsc::RecvTimeoutError::Disconnected => io::Error::new(
                io::ErrorKind::BrokenPipe,
                "network secret response channel closed",
            ),
        }
    })?;
    let value = std::str::from_utf8(secret.expose()).map_err(|_| {
        io::Error::new(
            io::ErrorKind::InvalidData,
            "NetworkManager Wi-Fi secret must be UTF-8",
        )
    })?;
    let setting = HashMap::from([("psk", Variant(value))]);
    let reply = HashMap::from([("802-11-wireless-security", setting)]);
    let message = message.return_with_args((reply,));
    Ok((message, Some(secret)))
}

fn read_cancel_args(message: &Message) -> io::Result<()> {
    let mut arguments = message.iter_init();
    let _path: dbus::Path<'_> = arguments.read().map_err(invalid_dbus_args)?;
    let _setting: &str = arguments.read().map_err(invalid_dbus_args)?;
    require_end(&mut arguments)
}

fn read_connection_and_path(message: &Message) -> io::Result<()> {
    let mut arguments = message.iter_init();
    require_argument(&mut arguments, "a{sa{sv}}", "connection settings")?;
    let _path: dbus::Path<'_> = arguments.read().map_err(invalid_dbus_args)?;
    require_end(&mut arguments)
}

fn require_argument(
    arguments: &mut dbus::arg::Iter<'_>,
    signature: &str,
    description: &str,
) -> io::Result<()> {
    if arguments.signature() != signature || !arguments.next() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            format!("NetworkManager {description} has an invalid D-Bus type"),
        ));
    }
    Ok(())
}

fn require_end(arguments: &mut dbus::arg::Iter<'_>) -> io::Result<()> {
    if arguments.arg_type() != ArgType::Invalid {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "NetworkManager secret request has trailing arguments",
        ));
    }
    Ok(())
}

fn secret_error_reply(message: &Message, error: &io::Error) -> Message {
    let name = ErrorName::new("org.freedesktop.NetworkManager.SecretAgent.Error.Failed")
        .expect("static NetworkManager error name");
    let sanitized = error.to_string().replace('\0', " ");
    let text = CString::new(sanitized).expect("NUL bytes were removed");
    message.error(&name, &text)
}

fn invalid_dbus_args(error: impl std::fmt::Display) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidInput, error.to_string())
}

pub struct SecretSocket<X: NetworkSecretExchange + ?Sized> {
    supervisor: SocketSupervisor,
    broker: SecretBroker,
    exchange: Arc<X>,
}

impl<X: NetworkSecretExchange + ?Sized> SecretSocket<X> {
    pub async fn bind(
        path: impl AsRef<Path>,
        expected_uid: libc::uid_t,
        broker: SecretBroker,
        exchange: Arc<X>,
    ) -> io::Result<Arc<Self>> {
        let limits = ConnectionLimits {
            max_clients: 1,
            max_frame_bytes: MAX_SECRET_FRAME,
            read_timeout: SECRET_DEADLINE,
            write_timeout: SECRET_DEADLINE,
            drain_timeout: Duration::from_secs(10),
        };
        let supervisor =
            SocketSupervisor::bind(path, expected_uid, EndpointKind::Secret, limits).await?;
        Ok(Arc::new(Self {
            supervisor,
            broker,
            exchange,
        }))
    }

    pub async fn serve_with_startup(&self, startup: RequiredStartupTask) -> io::Result<()> {
        let broker = self.broker.clone();
        let exchange = Arc::clone(&self.exchange);
        self.supervisor
            .serve_with_startup(startup, move |stream, context| {
                serve_secret(stream, context, broker.clone(), Arc::clone(&exchange))
            })
            .await
            .map(|_| ())
    }

    pub async fn serve_one(&self) -> io::Result<()> {
        let broker = self.broker.clone();
        let exchange = Arc::clone(&self.exchange);
        self.supervisor
            .serve_one(move |stream, context| {
                serve_secret(stream, context, broker.clone(), Arc::clone(&exchange))
            })
            .await
    }

    pub async fn shutdown_and_drain(&self) -> io::Result<SocketDrainReport> {
        self.supervisor.shutdown_and_drain().await
    }

    pub fn path(&self) -> &Path {
        self.supervisor.path()
    }
}

async fn serve_secret<X: NetworkSecretExchange + ?Sized>(
    mut stream: UnixStream,
    context: ConnectionContext,
    broker: SecretBroker,
    exchange: Arc<X>,
) -> io::Result<()> {
    let operation = async {
        if !exchange.has_pending_request() {
            return Err(io::Error::new(
                io::ErrorKind::NotConnected,
                "NetworkManager did not request a secret",
            ));
        }
        let challenge = broker.issue().await?;
        write_binary_frame(&mut stream, &challenge).await?;
        let response = read_binary_frame(&mut stream, Arc::clone(&broker.observer)).await?;
        let secret = broker.accept_locked_response(response).await?;
        exchange.submit(secret).await
    };
    let result = tokio::select! {
        biased;
        _ = context.cancellation.cancelled() => Err(io::Error::new(io::ErrorKind::Interrupted, "secret connection cancelled")),
        result = tokio::time::timeout(SECRET_DEADLINE, operation) => result.map_err(|_| io::Error::new(io::ErrorKind::TimedOut, "secret exchange timed out"))?,
    };
    broker.cancel_pending().await;
    result
}

async fn read_binary_frame(
    stream: &mut UnixStream,
    observer: Arc<dyn SecretZeroizeObserver>,
) -> io::Result<LockedSecret> {
    let length = stream.read_u32().await? as usize;
    if length == 0 || length > MAX_SECRET_FRAME {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "network secret frame violates its bounded size",
        ));
    }
    let mut bytes = LockedSecret::from_frame(vec![0_u8; length], 0, observer);
    stream.read_exact(&mut bytes.bytes).await?;
    Ok(bytes)
}

async fn write_binary_frame(stream: &mut UnixStream, bytes: &[u8]) -> io::Result<()> {
    let length = u32::try_from(bytes.len())
        .map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "secret frame is too large"))?;
    stream.write_u32(length).await?;
    stream.write_all(bytes).await
}

fn constant_time_equal(left: &[u8], right: &[u8]) -> bool {
    if left.len() != right.len() {
        return false;
    }
    left.iter()
        .zip(right)
        .fold(0_u8, |difference, (left, right)| {
            difference | (left ^ right)
        })
        == 0
}

pub fn validate_secret_socket_path(path: &Path) -> io::Result<()> {
    if !path.is_absolute() || path.as_os_str().as_bytes().contains(&0) {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "secret socket path must be an absolute non-NUL path",
        ));
    }
    Ok(())
}
