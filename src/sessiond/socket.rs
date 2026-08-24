use std::{
    fs, io,
    os::{
        fd::AsRawFd,
        unix::fs::{FileTypeExt, MetadataExt, PermissionsExt},
    },
    path::{Path, PathBuf},
};

use tokio::{
    io::AsyncWriteExt,
    net::{UnixListener, UnixStream},
};

use super::EventHub;

pub struct SessionSocket {
    path: PathBuf,
    expected_uid: libc::uid_t,
    listener: UnixListener,
    hub: EventHub,
    socket_identity: (u64, u64),
    shutdown: tokio::sync::broadcast::Sender<()>,
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
        fs::create_dir_all(parent)?;
        let parent_metadata = fs::symlink_metadata(parent)?;
        if !parent_metadata.file_type().is_dir() || parent_metadata.uid() != expected_uid {
            return Err(io::Error::new(
                io::ErrorKind::PermissionDenied,
                "session socket parent must be an owned real directory",
            ));
        }
        fs::set_permissions(parent, fs::Permissions::from_mode(0o700))?;
        if let Ok(metadata) = fs::symlink_metadata(&path) {
            if !std::os::unix::fs::FileTypeExt::is_socket(&metadata.file_type()) {
                return Err(io::Error::new(
                    io::ErrorKind::AlreadyExists,
                    "refusing to replace non-socket path",
                ));
            }
            fs::remove_file(&path)?;
        }
        let listener = UnixListener::bind(&path)?;
        fs::set_permissions(&path, fs::Permissions::from_mode(0o600))?;
        let metadata = fs::symlink_metadata(&path)?;
        if !metadata.file_type().is_socket() || metadata.uid() != expected_uid {
            let _ = fs::remove_file(&path);
            return Err(io::Error::new(
                io::ErrorKind::PermissionDenied,
                "session socket ownership could not be established",
            ));
        }
        let (shutdown, _) = tokio::sync::broadcast::channel(1);
        Ok(Self {
            path,
            expected_uid,
            listener,
            hub,
            socket_identity: (metadata.dev(), metadata.ino()),
            shutdown,
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
        loop {
            let (stream, _) = self.listener.accept().await?;
            let expected_uid = self.expected_uid;
            let hub = self.hub.clone();
            let shutdown = self.shutdown.subscribe();
            tokio::spawn(async move {
                let _ = serve_stream(stream, expected_uid, hub, shutdown).await;
            });
        }
    }

    pub fn path(&self) -> &Path {
        &self.path
    }
}

impl Drop for SessionSocket {
    fn drop(&mut self) {
        let _ = self.shutdown.send(());
        let Ok(metadata) = fs::symlink_metadata(&self.path) else {
            return;
        };
        if metadata.file_type().is_socket()
            && metadata.uid() == self.expected_uid
            && (metadata.dev(), metadata.ino()) == self.socket_identity
        {
            let _ = fs::remove_file(&self.path);
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
