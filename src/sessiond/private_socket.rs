use std::{
    ffi::OsString,
    io,
    path::{Path, PathBuf},
    sync::Arc,
};

use tokio::io::{AsyncBufReadExt, AsyncRead, AsyncReadExt, BufReader};
use tokio::net::{UnixListener, UnixStream};

use crate::store::{SecureDir, StoreError};

pub trait PrivateSocketBindObserver: Send + Sync + 'static {
    fn stale_socket_probed(&self, socket_path: &Path) -> io::Result<()>;
}

pub(crate) struct NoopBindObserver;

impl PrivateSocketBindObserver for NoopBindObserver {
    fn stale_socket_probed(&self, _socket_path: &Path) -> io::Result<()> {
        Ok(())
    }
}

pub(crate) struct PrivateSocketEndpoint {
    path: PathBuf,
    directory: SecureDir,
    socket_name: OsString,
    expected_uid: libc::uid_t,
    listener: UnixListener,
    socket_identity: (u64, u64),
}

impl PrivateSocketEndpoint {
    pub(crate) async fn bind(
        path: impl AsRef<Path>,
        expected_uid: libc::uid_t,
    ) -> io::Result<Self> {
        Self::bind_with_observer(path, expected_uid, Arc::new(NoopBindObserver)).await
    }

    pub(crate) async fn bind_with_observer(
        path: impl AsRef<Path>,
        expected_uid: libc::uid_t,
        observer: Arc<dyn PrivateSocketBindObserver>,
    ) -> io::Result<Self> {
        if expected_uid != unsafe { libc::geteuid() } {
            return Err(io::Error::new(
                io::ErrorKind::PermissionDenied,
                "private socket owner UID must match the daemon effective UID",
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
                        "a live daemon already owns the socket",
                    ));
                }
                Err(error) if error.kind() == io::ErrorKind::ConnectionRefused => {
                    observer.stale_socket_probed(&path)?;
                    match directory
                        .entry_metadata(&socket_name)
                        .map_err(store_error)?
                    {
                        Some(current)
                            if (current.device, current.inode)
                                == (metadata.device, metadata.inode) =>
                        {
                            directory.remove_file(&socket_name).map_err(store_error)?;
                        }
                        None => {}
                        Some(_) => {
                            return Err(io::Error::new(
                                io::ErrorKind::AlreadyExists,
                                "socket changed while stale ownership was checked",
                            ));
                        }
                    }
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
                "private socket ownership could not be established",
            ));
        }
        Ok(Self {
            path,
            directory,
            socket_name,
            expected_uid,
            listener,
            socket_identity: (metadata.device, metadata.inode),
        })
    }

    pub(crate) async fn accept(&self) -> io::Result<UnixStream> {
        self.listener.accept().await.map(|(stream, _)| stream)
    }

    pub(crate) fn expected_uid(&self) -> libc::uid_t {
        self.expected_uid
    }

    pub(crate) fn path(&self) -> &Path {
        &self.path
    }
}

impl Drop for PrivateSocketEndpoint {
    fn drop(&mut self) {
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

pub(crate) fn peer_uid(stream: &UnixStream) -> io::Result<libc::uid_t> {
    peer_credentials(stream).map(|credentials| credentials.uid)
}

pub(crate) fn peer_credentials(stream: &UnixStream) -> io::Result<libc::ucred> {
    use std::os::fd::AsRawFd;

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
    Ok(credentials)
}

pub(crate) async fn read_bounded_line<R: AsyncRead + Unpin>(
    reader: R,
    max_line: usize,
    deadline: std::time::Duration,
    description: &'static str,
) -> io::Result<Vec<u8>> {
    let read = async {
        let mut limited = BufReader::new(reader).take((max_line + 1) as u64);
        let mut bytes = Vec::with_capacity(max_line + 1);
        let count = limited.read_until(b'\n', &mut bytes).await?;
        if count == 0 || count > max_line || bytes.last() != Some(&b'\n') {
            return Err(io::Error::new(io::ErrorKind::InvalidData, description));
        }
        bytes.pop();
        Ok(bytes)
    };
    tokio::time::timeout(deadline, read)
        .await
        .map_err(|_| io::Error::new(io::ErrorKind::TimedOut, description))?
}

fn store_error(error: StoreError) -> io::Error {
    io::Error::new(io::ErrorKind::PermissionDenied, error)
}
