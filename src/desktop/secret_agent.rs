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
    time::{Duration, Instant as StdInstant},
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
        self.issue_until(Instant::now() + SECRET_DEADLINE).await
    }

    async fn issue_until(&self, deadline: Instant) -> io::Result<[u8; 16]> {
        let mut pending = self.pending.lock().await;
        if pending.is_some() {
            return Err(io::Error::new(
                io::ErrorKind::WouldBlock,
                "a network secret challenge is already pending",
            ));
        }
        let id = *uuid::Uuid::new_v4().as_bytes();
        *pending = Some(PendingChallenge { id, deadline });
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
    fn acquire_lease(&self) -> io::Result<SecretRequestLease>;
    async fn submit(&self, lease: &SecretRequestLease, secret: LockedSecret) -> io::Result<()>;
}

#[derive(Clone)]
pub struct SecretRequestLease {
    request_id: [u8; 16],
    key: SecretRequestKey,
    deadline: StdInstant,
    cancelled: Arc<AtomicBool>,
}

impl SecretRequestLease {
    pub fn new(
        request_id: [u8; 16],
        connection_path: impl Into<String>,
        setting_name: impl Into<String>,
        deadline: StdInstant,
    ) -> Self {
        Self {
            request_id,
            key: SecretRequestKey {
                connection_path: connection_path.into(),
                setting_name: setting_name.into(),
            },
            deadline,
            cancelled: Arc::new(AtomicBool::new(false)),
        }
    }

    fn tokio_deadline(&self) -> Instant {
        Instant::from_std(self.deadline)
    }
}

#[derive(Debug, Default)]
pub struct UnavailableNetworkManagerExchange;

#[async_trait]
impl NetworkSecretExchange for UnavailableNetworkManagerExchange {
    fn acquire_lease(&self) -> io::Result<SecretRequestLease> {
        Err(io::Error::new(
            io::ErrorKind::NotConnected,
            "NetworkManager did not request a secret",
        ))
    }

    async fn submit(&self, _lease: &SecretRequestLease, _secret: LockedSecret) -> io::Result<()> {
        Err(io::Error::new(
            io::ErrorKind::Unsupported,
            "NetworkManager secret-agent exchange is unavailable",
        ))
    }
}

pub struct NetworkManagerSecretExchange {
    pending: StdMutex<Option<PendingSecretRequest>>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct SecretRequestKey {
    connection_path: String,
    setting_name: String,
}

struct PendingSecretRequest {
    id: [u8; 16],
    key: SecretRequestKey,
    sender: std_mpsc::SyncSender<LockedSecret>,
    cancelled: Arc<AtomicBool>,
    deadline: StdInstant,
    reply_started: bool,
}

struct PendingSecretReceiver {
    id: [u8; 16],
    key: SecretRequestKey,
    receiver: std_mpsc::Receiver<LockedSecret>,
    cancelled: Arc<AtomicBool>,
    deadline: StdInstant,
}

impl NetworkManagerSecretExchange {
    fn new() -> Arc<Self> {
        Arc::new(Self {
            pending: StdMutex::new(None),
        })
    }

    fn begin(&self, key: SecretRequestKey) -> io::Result<PendingSecretReceiver> {
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
        let id = *uuid::Uuid::new_v4().as_bytes();
        let cancelled = Arc::new(AtomicBool::new(false));
        let deadline = StdInstant::now() + SECRET_DEADLINE;
        *pending = Some(PendingSecretRequest {
            id,
            key: key.clone(),
            sender,
            cancelled: Arc::clone(&cancelled),
            deadline,
            reply_started: false,
        });
        Ok(PendingSecretReceiver {
            id,
            key,
            receiver,
            cancelled,
            deadline,
        })
    }

    fn cancel(&self) {
        if let Ok(mut pending) = self.pending.lock() {
            if let Some(request) = pending.take() {
                request.cancelled.store(true, Ordering::Release);
            }
        }
    }

    fn cancel_matching(&self, key: &SecretRequestKey) {
        if let Ok(mut pending) = self.pending.lock() {
            if pending
                .as_ref()
                .is_some_and(|request| request.key == *key && !request.reply_started)
            {
                if let Some(request) = pending.take() {
                    request.cancelled.store(true, Ordering::Release);
                }
            }
        }
    }

    fn complete(&self, id: &[u8; 16]) {
        if let Ok(mut pending) = self.pending.lock() {
            if pending.as_ref().is_some_and(|request| request.id == *id) {
                pending.take();
            }
        }
    }

    fn acquire_lease(&self) -> io::Result<SecretRequestLease> {
        let pending = self
            .pending
            .lock()
            .map_err(|_| io::Error::other("NetworkManager secret exchange lock poisoned"))?;
        let request = pending.as_ref().ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::NotConnected,
                "NetworkManager did not request a secret",
            )
        })?;
        if request.cancelled.load(Ordering::Acquire) || StdInstant::now() >= request.deadline {
            return Err(io::Error::new(
                io::ErrorKind::NotConnected,
                "NetworkManager secret request is no longer live",
            ));
        }
        Ok(SecretRequestLease {
            request_id: request.id,
            key: request.key.clone(),
            deadline: request.deadline,
            cancelled: Arc::clone(&request.cancelled),
        })
    }

    fn send_reply_if_live(&self, id: &[u8; 16], send: impl FnOnce() -> bool) -> bool {
        let Ok(mut pending) = self.pending.lock() else {
            return false;
        };
        let Some(request) = pending.as_mut().filter(|request| request.id == *id) else {
            return false;
        };
        if request.cancelled.load(Ordering::Acquire) {
            return false;
        }
        request.reply_started = true;
        send()
    }
}

#[async_trait]
impl NetworkSecretExchange for NetworkManagerSecretExchange {
    fn acquire_lease(&self) -> io::Result<SecretRequestLease> {
        NetworkManagerSecretExchange::acquire_lease(self)
    }

    async fn submit(&self, lease: &SecretRequestLease, secret: LockedSecret) -> io::Result<()> {
        let pending = self
            .pending
            .lock()
            .map_err(|_| io::Error::other("NetworkManager secret exchange lock poisoned"))?;
        let request = pending.as_ref().ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::NotConnected,
                "NetworkManager did not request a secret",
            )
        })?;
        let matching = request.id == lease.request_id
            && request.key == lease.key
            && request.deadline == lease.deadline
            && Arc::ptr_eq(&request.cancelled, &lease.cancelled);
        if !matching
            || request.cancelled.load(Ordering::Acquire)
            || lease.cancelled.load(Ordering::Acquire)
            || StdInstant::now() >= request.deadline
        {
            return Err(io::Error::new(
                io::ErrorKind::NotConnected,
                "NetworkManager secret request lease is stale",
            ));
        }
        request.sender.try_send(secret).map_err(|error| {
            match error {
                std_mpsc::TrySendError::Full(secret)
                | std_mpsc::TrySendError::Disconnected(secret) => drop(secret),
            }
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

#[derive(Clone)]
struct NetworkManagerPeerAuthority {
    unique_owner: Arc<StdMutex<Option<String>>>,
}

impl NetworkManagerPeerAuthority {
    fn new(unique_owner: String) -> Self {
        Self {
            unique_owner: Arc::new(StdMutex::new(Some(unique_owner))),
        }
    }

    fn authorize(&self, message: &Message) -> io::Result<()> {
        let sender = message
            .sender()
            .map(|sender| sender.to_string())
            .ok_or_else(|| {
                io::Error::new(
                    io::ErrorKind::PermissionDenied,
                    "NetworkManager request has no authenticated D-Bus sender",
                )
            })?;
        let owner = self
            .unique_owner
            .lock()
            .map_err(|_| io::Error::other("NetworkManager owner lock poisoned"))?;
        if owner.as_deref() != Some(sender.as_str()) {
            return Err(io::Error::new(
                io::ErrorKind::PermissionDenied,
                "NetworkManager request sender is not the pinned unique owner",
            ));
        }
        Ok(())
    }

    fn replace(&self, owner: Option<String>) {
        if let Ok(mut current) = self.unique_owner.lock() {
            *current = owner;
        }
    }
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
        let unique_owner = match resolve_network_manager_owner(&connection) {
            Ok(owner) => owner,
            Err(_) => return Ok((None, Arc::new(UnavailableNetworkManagerExchange))),
        };
        let authority = NetworkManagerPeerAuthority::new(unique_owner);
        let exchange = NetworkManagerSecretExchange::new();
        let workers = Arc::new(StdMutex::new(Vec::new()));
        register_network_manager_methods(
            &connection,
            Arc::clone(&exchange),
            Arc::clone(&workers),
            authority.clone(),
        );
        if register_network_manager_owner_watch(
            &connection,
            Arc::clone(&exchange),
            authority,
            Arc::clone(&workers),
        )
        .is_err()
        {
            return Ok((None, Arc::new(UnavailableNetworkManagerExchange)));
        }
        if register_with_network_manager(&connection).is_err() {
            return Ok((None, Arc::new(UnavailableNetworkManagerExchange)));
        }
        let stop = Arc::new(AtomicBool::new(false));
        let thread_stop = Arc::clone(&stop);
        let thread_exchange = Arc::clone(&exchange);
        let thread = thread::Builder::new()
            .name("sleepy-network-secret-agent".into())
            .spawn(move || {
                while !thread_stop.load(Ordering::Acquire) {
                    if connection.process(Duration::from_millis(25)).is_err() {
                        thread_exchange.cancel();
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
const NM_DESTINATION: &str = "org.freedesktop.NetworkManager";
const DBUS_DESTINATION: &str = "org.freedesktop.DBus";
const DBUS_PATH: &str = "/org/freedesktop/DBus";
const DBUS_INTERFACE: &str = "org.freedesktop.DBus";

fn resolve_network_manager_owner(connection: &SyncConnection) -> io::Result<String> {
    let proxy = connection.with_proxy(DBUS_DESTINATION, DBUS_PATH, Duration::from_secs(2));
    let (owner,): (String,) = proxy
        .method_call(DBUS_INTERFACE, "GetNameOwner", (NM_DESTINATION,))
        .map_err(dbus_io_error)?;
    if !owner.starts_with(':') {
        return Err(io::Error::new(
            io::ErrorKind::PermissionDenied,
            "NetworkManager did not resolve to a unique D-Bus owner",
        ));
    }
    if let Ok((uid,)) = proxy.method_call::<(u32,), _, _, _>(
        DBUS_INTERFACE,
        "GetConnectionUnixUser",
        (owner.as_str(),),
    ) {
        if uid != 0 {
            return Err(io::Error::new(
                io::ErrorKind::PermissionDenied,
                "NetworkManager D-Bus owner is not privileged",
            ));
        }
    }
    Ok(owner)
}

fn register_with_network_manager(connection: &SyncConnection) -> io::Result<()> {
    let manager = connection.with_proxy(
        NM_DESTINATION,
        "/org/freedesktop/NetworkManager/AgentManager",
        Duration::from_secs(5),
    );
    manager
        .method_call(
            "org.freedesktop.NetworkManager.AgentManager",
            "RegisterWithCapabilities",
            ("org.sleepylinux.SleepySession", 0_u32),
        )
        .map_err(dbus_io_error)
}

fn register_network_manager_methods(
    connection: &Arc<SyncConnection>,
    exchange: Arc<NetworkManagerSecretExchange>,
    workers: Arc<StdMutex<Vec<thread::JoinHandle<()>>>>,
    authority: NetworkManagerPeerAuthority,
) {
    let mut rule = MatchRule::new_method_call();
    rule.path = Some(NM_AGENT_PATH.into());
    rule.interface = Some(NM_AGENT_INTERFACE.into());
    let callback_connection = Arc::clone(connection);
    connection.start_receive(
        rule,
        Box::new(move |message, channel| {
            if let Err(error) = authority.authorize(&message) {
                let _ = channel.send(secret_error_reply(&message, &error));
                return true;
            }
            if message.member().as_deref() == Some("GetSecrets") {
                match begin_get_secrets(&message, &exchange) {
                    Ok(receiver) => {
                        let connection = Arc::clone(&callback_connection);
                        let exchange = Arc::clone(&exchange);
                        let request_id = receiver.id;
                        let worker = thread::spawn(move || {
                            let (reply, retained) =
                                finish_get_secrets(&message, &exchange, receiver);
                            if retained.is_some() {
                                let mut reply = Some(reply);
                                if !exchange.send_reply_if_live(&request_id, || {
                                    connection
                                        .send(reply.take().expect("reply is sent once"))
                                        .is_ok()
                                }) {
                                    let _ = connection.send(secret_error_reply(
                                        &message,
                                        &io::Error::new(
                                            io::ErrorKind::Interrupted,
                                            "NetworkManager secret request was cancelled",
                                        ),
                                    ));
                                }
                            } else {
                                let _ = connection.send(reply);
                            }
                            exchange.complete(&request_id);
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

fn register_network_manager_owner_watch(
    connection: &Arc<SyncConnection>,
    exchange: Arc<NetworkManagerSecretExchange>,
    authority: NetworkManagerPeerAuthority,
    workers: Arc<StdMutex<Vec<thread::JoinHandle<()>>>>,
) -> io::Result<()> {
    let rule = MatchRule::new_signal(DBUS_INTERFACE, "NameOwnerChanged");
    let callback_connection = Arc::clone(connection);
    connection
        .add_match(
            rule,
            move |(name, _old_owner, new_owner): (String, String, String), _, _| {
                if name != NM_DESTINATION {
                    return true;
                }
                exchange.cancel();
                authority.replace(None);
                if new_owner.is_empty() {
                    return true;
                }
                let connection = Arc::clone(&callback_connection);
                let authority = authority.clone();
                let worker = thread::spawn(move || {
                    let Ok(resolved) = resolve_network_manager_owner(&connection) else {
                        return;
                    };
                    if resolved != new_owner {
                        return;
                    }
                    authority.replace(Some(resolved));
                    if register_with_network_manager(&connection).is_err() {
                        authority.replace(None);
                    }
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
                true
            },
        )
        .map(|_| ())
        .map_err(dbus_io_error)
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
        Some("CancelGetSecrets") => read_cancel_args(message).map(|key| {
            exchange.cancel_matching(&key);
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
) -> io::Result<PendingSecretReceiver> {
    let mut arguments = message.iter_init();
    require_argument(&mut arguments, "a{sa{sv}}", "connection settings")?;
    let path: dbus::Path<'_> = arguments.read().map_err(invalid_dbus_args)?;
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
    exchange.begin(SecretRequestKey {
        connection_path: path.to_string(),
        setting_name: setting_name.to_owned(),
    })
}

fn finish_get_secrets(
    message: &Message,
    exchange: &NetworkManagerSecretExchange,
    receiver: PendingSecretReceiver,
) -> (Message, Option<LockedSecret>) {
    match finish_get_secrets_result(message, exchange, receiver) {
        Ok(result) => result,
        Err(error) => (secret_error_reply(message, &error), None),
    }
}

fn finish_get_secrets_result(
    message: &Message,
    exchange: &NetworkManagerSecretExchange,
    receiver: PendingSecretReceiver,
) -> io::Result<(Message, Option<LockedSecret>)> {
    let remaining = receiver
        .deadline
        .saturating_duration_since(StdInstant::now());
    if remaining.is_zero() {
        exchange.cancel_matching(&receiver.key);
        return Err(io::Error::new(
            io::ErrorKind::TimedOut,
            "network secret response exceeded its total deadline",
        ));
    }
    let secret = receiver.receiver.recv_timeout(remaining).map_err(|error| {
        exchange.cancel_matching(&receiver.key);
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
    if receiver.cancelled.load(Ordering::Acquire) || StdInstant::now() >= receiver.deadline {
        return Err(io::Error::new(
            io::ErrorKind::Interrupted,
            "NetworkManager secret request was cancelled",
        ));
    }
    let value = std::str::from_utf8(secret.expose()).map_err(|_| {
        io::Error::new(
            io::ErrorKind::InvalidData,
            "NetworkManager Wi-Fi secret must be UTF-8",
        )
    })?;
    let setting = HashMap::from([("psk", Variant(value))]);
    let reply = HashMap::from([("802-11-wireless-security", setting)]);
    let message = message.return_with_args((reply,));
    if receiver.cancelled.load(Ordering::Acquire) {
        return Err(io::Error::new(
            io::ErrorKind::Interrupted,
            "NetworkManager secret request was cancelled",
        ));
    }
    Ok((message, Some(secret)))
}

fn read_cancel_args(message: &Message) -> io::Result<SecretRequestKey> {
    let mut arguments = message.iter_init();
    let path: dbus::Path<'_> = arguments.read().map_err(invalid_dbus_args)?;
    let setting: &str = arguments.read().map_err(invalid_dbus_args)?;
    require_end(&mut arguments)?;
    Ok(SecretRequestKey {
        connection_path: path.to_string(),
        setting_name: setting.to_owned(),
    })
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

fn dbus_io_error(error: dbus::Error) -> io::Error {
    let kind = match error.name() {
        Some("org.freedesktop.DBus.Error.AccessDenied") => io::ErrorKind::PermissionDenied,
        Some("org.freedesktop.DBus.Error.NoReply") => io::ErrorKind::TimedOut,
        Some("org.freedesktop.DBus.Error.NameHasNoOwner")
        | Some("org.freedesktop.DBus.Error.ServiceUnknown") => io::ErrorKind::NotFound,
        _ => io::ErrorKind::Other,
    };
    io::Error::new(kind, error.to_string())
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
    let lease = exchange.acquire_lease()?;
    let deadline = lease.tokio_deadline();
    if deadline <= Instant::now() {
        return Err(io::Error::new(
            io::ErrorKind::TimedOut,
            "network secret exchange exceeded its total deadline",
        ));
    }
    let operation = async {
        let challenge = broker.issue_until(deadline).await?;
        write_binary_frame(&mut stream, &challenge).await?;
        let response = read_binary_frame(&mut stream, Arc::clone(&broker.observer)).await?;
        let secret = broker.accept_locked_response(response).await?;
        exchange.submit(&lease, secret).await
    };
    let result = tokio::select! {
        biased;
        _ = context.cancellation.cancelled() => Err(io::Error::new(io::ErrorKind::Interrupted, "secret connection cancelled")),
        result = tokio::time::timeout_at(deadline, operation) => result.map_err(|_| io::Error::new(io::ErrorKind::TimedOut, "secret exchange timed out"))?,
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

#[cfg(test)]
mod tests {
    use std::sync::atomic::AtomicUsize;

    use dbus::strings::BusName;

    use super::*;

    #[derive(Default)]
    struct ZeroizeAudit {
        calls: AtomicUsize,
        nonzero_bytes: AtomicUsize,
    }

    impl SecretZeroizeObserver for ZeroizeAudit {
        fn after_zeroize(&self, bytes: &[u8]) {
            self.calls.fetch_add(1, Ordering::SeqCst);
            self.nonzero_bytes.fetch_add(
                bytes.iter().filter(|byte| **byte != 0).count(),
                Ordering::SeqCst,
            );
        }
    }

    fn message_from(sender: &str) -> Message {
        let mut message = Message::new_method_call(
            NM_DESTINATION,
            NM_AGENT_PATH,
            NM_AGENT_INTERFACE,
            "GetSecrets",
        )
        .unwrap();
        message.set_sender(Some(BusName::new(sender).unwrap()));
        message
    }

    #[test]
    fn network_manager_peer_authority_pins_unique_sender_across_restart() {
        let authority = NetworkManagerPeerAuthority::new(":1.42".into());
        assert!(authority.authorize(&message_from(":1.42")).is_ok());
        assert_eq!(
            authority
                .authorize(&message_from(":1.43"))
                .unwrap_err()
                .kind(),
            io::ErrorKind::PermissionDenied
        );

        authority.replace(None);
        assert!(authority.authorize(&message_from(":1.42")).is_err());
        authority.replace(Some(":1.43".into()));
        assert!(authority.authorize(&message_from(":1.43")).is_ok());
    }

    #[tokio::test]
    async fn cancellation_token_survives_submit_until_dbus_reply_completion() {
        let exchange = NetworkManagerSecretExchange::new();
        let key = SecretRequestKey {
            connection_path: "/org/freedesktop/NetworkManager/Settings/1".into(),
            setting_name: "802-11-wireless-security".into(),
        };
        let pending = exchange.begin(key.clone()).unwrap();
        let lease = exchange.acquire_lease().unwrap();
        let secret =
            LockedSecret::from_frame(b"super-secret".to_vec(), 0, Arc::new(NoopZeroizeObserver));
        exchange.submit(&lease, secret).await.unwrap();

        assert!(exchange.pending.lock().unwrap().is_some());
        exchange.cancel_matching(&key);
        assert!(pending.cancelled.load(Ordering::Acquire));
        assert!(pending.receiver.recv().is_ok());
        assert!(exchange.pending.lock().unwrap().is_none());
    }

    #[tokio::test]
    async fn stale_dbus_reply_completion_cannot_remove_restarted_request() {
        let exchange = NetworkManagerSecretExchange::new();
        let key = SecretRequestKey {
            connection_path: "/org/freedesktop/NetworkManager/Settings/1".into(),
            setting_name: "802-11-wireless-security".into(),
        };
        let old = exchange.begin(key.clone()).unwrap();
        exchange.cancel_matching(&key);
        let restarted = exchange.begin(key).unwrap();

        exchange.complete(&old.id);
        assert!(exchange.pending.lock().unwrap().is_some());
        assert_eq!(
            exchange.pending.lock().unwrap().as_ref().unwrap().id,
            restarted.id
        );
    }

    #[tokio::test]
    async fn stale_socket_lease_cannot_submit_request_a_secret_into_request_b() {
        let audit = Arc::new(ZeroizeAudit::default());
        let exchange = NetworkManagerSecretExchange::new();
        let key_a = SecretRequestKey {
            connection_path: "/org/freedesktop/NetworkManager/Settings/1".into(),
            setting_name: "802-11-wireless-security".into(),
        };
        let key_b = SecretRequestKey {
            connection_path: "/org/freedesktop/NetworkManager/Settings/2".into(),
            setting_name: "802-11-wireless-security".into(),
        };
        let request_a = exchange.begin(key_a.clone()).unwrap();
        let lease_a = exchange.acquire_lease().unwrap();
        exchange.cancel_matching(&key_a);
        let request_b = exchange.begin(key_b).unwrap();
        let secret = LockedSecret::from_frame(b"request-a-sentinel".to_vec(), 0, audit.clone());

        assert_eq!(
            exchange.submit(&lease_a, secret).await.unwrap_err().kind(),
            io::ErrorKind::NotConnected
        );
        assert!(matches!(
            request_b.receiver.try_recv(),
            Err(std_mpsc::TryRecvError::Empty)
        ));
        assert!(request_a.cancelled.load(Ordering::Acquire));
        assert_eq!(audit.calls.load(Ordering::SeqCst), 1);
        assert_eq!(audit.nonzero_bytes.load(Ordering::SeqCst), 0);
    }

    #[test]
    fn owner_revocation_that_wins_prevents_secret_bearing_send() {
        let exchange = NetworkManagerSecretExchange::new();
        let request = exchange
            .begin(SecretRequestKey {
                connection_path: "/org/freedesktop/NetworkManager/Settings/3".into(),
                setting_name: "802-11-wireless-security".into(),
            })
            .unwrap();
        exchange.cancel();
        let sent = AtomicBool::new(false);

        assert!(!exchange.send_reply_if_live(&request.id, || {
            sent.store(true, Ordering::Release);
            true
        }));
        assert!(!sent.load(Ordering::Acquire));
    }

    #[tokio::test]
    async fn correlated_secret_is_retained_through_typed_dbus_reply() {
        let exchange = NetworkManagerSecretExchange::new();
        let key = SecretRequestKey {
            connection_path: "/org/freedesktop/NetworkManager/Settings/7".into(),
            setting_name: "802-11-wireless-security".into(),
        };
        let pending = exchange.begin(key).unwrap();
        let lease = exchange.acquire_lease().unwrap();
        let secret = LockedSecret::from_frame(
            b"correct-horse-battery-staple".to_vec(),
            0,
            Arc::new(NoopZeroizeObserver),
        );
        exchange.submit(&lease, secret).await.unwrap();
        let mut request = message_from(":1.42");
        request.set_serial(7);

        let request_id = pending.id;
        let (reply, retained) = finish_get_secrets_result(&request, &exchange, pending).unwrap();
        assert!(exchange.send_reply_if_live(&request_id, || true));
        let values: HashMap<String, HashMap<String, Variant<String>>> = reply.read1().unwrap();
        assert_eq!(
            values["802-11-wireless-security"]["psk"].0,
            "correct-horse-battery-staple"
        );
        assert_eq!(
            retained.as_ref().unwrap().expose(),
            b"correct-horse-battery-staple"
        );
        exchange.complete(&request_id);
        drop(retained);
        assert!(exchange.pending.lock().unwrap().is_none());
    }
}
