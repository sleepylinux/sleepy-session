use std::{
    ffi::OsString,
    io,
    os::fd::AsRawFd,
    path::{Path, PathBuf},
    sync::atomic::{AtomicBool, Ordering},
    time::Duration,
};

use tokio::{
    io::AsyncWriteExt,
    net::{UnixListener, UnixStream},
};

use super::EventHub;
use crate::store::{SecureDir, StoreError};

pub struct SessionSocket {
    path: PathBuf,
    directory: SecureDir,
    socket_name: OsString,
    expected_uid: libc::uid_t,
    listener: UnixListener,
    hub: EventHub,
    socket_identity: (u64, u64),
    shutdown: tokio::sync::broadcast::Sender<()>,
    connections: tokio::sync::Mutex<Vec<tokio::task::JoinHandle<io::Result<()>>>>,
    serving: AtomicBool,
    serve_stopped: tokio::sync::Notify,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct SocketDrainReport {
    pub completed: usize,
    pub aborted: usize,
}

impl SessionSocket {
    pub async fn bind(
        path: impl AsRef<Path>,
        expected_uid: libc::uid_t,
        hub: EventHub,
    ) -> io::Result<Self> {
        if expected_uid != unsafe { libc::geteuid() } {
            return Err(io::Error::new(
                io::ErrorKind::PermissionDenied,
                "session socket owner UID must match the daemon effective UID",
            ));
        }
        let path = path.as_ref().to_path_buf();
        let parent = path
            .parent()
            .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "socket has no parent"))?;
        let socket_name = path
            .file_name()
            .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "socket has no name"))?
            .to_owned();
        let directory = SecureDir::open_writable(parent, true).map_err(store_error)?;
        directory.enforce_private_directory().map_err(store_error)?;
        let descriptor_path = directory
            .descriptor_path(&socket_name)
            .map_err(store_error)?;
        if let Some(metadata) = directory
            .entry_metadata(&socket_name)
            .map_err(store_error)?
        {
            if metadata.mode & libc::S_IFMT != libc::S_IFSOCK
                || metadata.uid != expected_uid
                || metadata.mode & 0o777 != 0o600
            {
                return Err(io::Error::new(
                    io::ErrorKind::AlreadyExists,
                    "refusing to replace a socket path that is not owned mode-0600",
                ));
            }
            match UnixStream::connect(&descriptor_path).await {
                Ok(_) => {
                    return Err(io::Error::new(
                        io::ErrorKind::AlreadyExists,
                        "a live session daemon already owns the socket",
                    ));
                }
                Err(error) if error.kind() == io::ErrorKind::ConnectionRefused => {
                    directory.remove_file(&socket_name).map_err(store_error)?;
                }
                Err(error) if error.kind() == io::ErrorKind::NotFound => {}
                Err(error) => return Err(error),
            }
        }
        let listener = UnixListener::bind(&descriptor_path)?;
        directory
            .chmod_entry(&socket_name, 0o600)
            .map_err(store_error)?;
        let metadata = directory
            .entry_metadata(&socket_name)
            .map_err(store_error)?
            .ok_or_else(|| io::Error::new(io::ErrorKind::NotFound, "socket disappeared"))?;
        if metadata.mode & libc::S_IFMT != libc::S_IFSOCK
            || metadata.uid != expected_uid
            || metadata.mode & 0o777 != 0o600
        {
            let _ = directory.remove_file(&socket_name);
            return Err(io::Error::new(
                io::ErrorKind::PermissionDenied,
                "private session socket ownership could not be established",
            ));
        }
        let (shutdown, _) = tokio::sync::broadcast::channel(1);
        Ok(Self {
            path,
            directory,
            socket_name,
            expected_uid,
            listener,
            hub,
            socket_identity: (metadata.device, metadata.inode),
            shutdown,
            connections: tokio::sync::Mutex::new(Vec::new()),
            serving: AtomicBool::new(false),
            serve_stopped: tokio::sync::Notify::new(),
        })
    }

    pub async fn serve_one(&self) -> io::Result<()> {
        let (stream, _) = self.listener.accept().await?;
        serve_stream(
            stream,
            self.expected_uid,
            self.hub.clone(),
            self.shutdown.subscribe(),
        )
        .await
    }

    pub async fn serve(&self) -> io::Result<()> {
        if self.serving.swap(true, Ordering::AcqRel) {
            return Err(io::Error::new(
                io::ErrorKind::AlreadyExists,
                "session socket is already being served",
            ));
        }
        let _guard = ServeGuard {
            serving: &self.serving,
            stopped: &self.serve_stopped,
        };
        let mut listener_shutdown = self.shutdown.subscribe();
        loop {
            let accepted = tokio::select! {
                accepted = self.listener.accept() => accepted,
                _ = listener_shutdown.recv() => return Ok(()),
            };
            let (stream, _) = accepted?;
            let expected_uid = self.expected_uid;
            let hub = self.hub.clone();
            let shutdown = self.shutdown.subscribe();
            self.connections.lock().await.push(tokio::spawn(async move {
                serve_stream(stream, expected_uid, hub, shutdown).await
            }));
        }
    }

    pub async fn shutdown_and_drain(&self, timeout: Duration) -> io::Result<SocketDrainReport> {
        let deadline = tokio::time::Instant::now() + timeout;
        let _ = self.shutdown.send(());
        if self.serving.load(Ordering::Acquire) {
            tokio::time::timeout_at(deadline, async {
                while self.serving.load(Ordering::Acquire) {
                    let stopped = self.serve_stopped.notified();
                    if !self.serving.load(Ordering::Acquire) {
                        break;
                    }
                    stopped.await;
                }
            })
            .await
            .map_err(|_| {
                io::Error::new(io::ErrorKind::TimedOut, "socket accept loop did not stop")
            })?;
        }

        let mut handles = std::mem::take(&mut *self.connections.lock().await);
        let mut report = SocketDrainReport::default();
        while !handles.is_empty() {
            let mut handle = handles.remove(0);
            match tokio::time::timeout_at(deadline, &mut handle).await {
                Ok(_) => report.completed += 1,
                Err(_) => {
                    handle.abort();
                    let _ = handle.await;
                    report.aborted += 1;
                    for handle in handles {
                        handle.abort();
                        let _ = handle.await;
                        report.aborted += 1;
                    }
                    break;
                }
            }
        }
        Ok(report)
    }

    pub fn path(&self) -> &Path {
        &self.path
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

impl Drop for SessionSocket {
    fn drop(&mut self) {
        let _ = self.shutdown.send(());
        let Ok(Some(metadata)) = self.directory.entry_metadata(&self.socket_name) else {
            return;
        };
        if metadata.mode & libc::S_IFMT == libc::S_IFSOCK
            && metadata.uid == self.expected_uid
            && (metadata.device, metadata.inode) == self.socket_identity
        {
            let _ = self.directory.remove_file(&self.socket_name);
        }
    }
}

async fn serve_stream(
    mut stream: UnixStream,
    expected_uid: libc::uid_t,
    hub: EventHub,
    mut shutdown: tokio::sync::broadcast::Receiver<()>,
) -> io::Result<()> {
    let peer_uid = peer_uid(&stream)?;
    if peer_uid != expected_uid {
        return Err(io::Error::new(
            io::ErrorKind::PermissionDenied,
            "session socket peer UID mismatch",
        ));
    }

    let mut subscriber = hub.subscribe().await;
    loop {
        let event = tokio::select! {
            biased;
            event = subscriber.recv() => event.map_err(|error| match error {
                tokio::sync::broadcast::error::RecvError::Closed => {
                    io::Error::new(io::ErrorKind::BrokenPipe, "event hub closed")
                }
                tokio::sync::broadcast::error::RecvError::Lagged(count) => io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!("event subscriber lagged by {count}"),
                ),
            })?,
            _ = shutdown.recv() => return Ok(()),
        };
        let mut line = serde_json::to_vec(&event)
            .map_err(|error| io::Error::new(io::ErrorKind::InvalidData, error))?;
        line.push(b'\n');
        stream.write_all(&line).await?;
    }
}

fn peer_uid(stream: &UnixStream) -> io::Result<libc::uid_t> {
    let mut credentials = libc::ucred {
        pid: 0,
        uid: 0,
        gid: 0,
    };
    let mut length = std::mem::size_of::<libc::ucred>() as libc::socklen_t;
    let result = unsafe {
        libc::getsockopt(
            stream.as_raw_fd(),
            libc::SOL_SOCKET,
            libc::SO_PEERCRED,
            (&mut credentials as *mut libc::ucred).cast(),
            &mut length,
        )
    };
    if result == -1 {
        return Err(io::Error::last_os_error());
    }
    if length as usize != std::mem::size_of::<libc::ucred>() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "invalid peer credentials length",
        ));
    }
    Ok(credentials.uid)
}

fn store_error(error: StoreError) -> io::Error {
    let kind = if error.code() == "unsafe_path" {
        io::ErrorKind::PermissionDenied
    } else {
        io::ErrorKind::Other
    };
    io::Error::new(kind, error)
}
