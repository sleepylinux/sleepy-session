use std::{
    io,
    path::Path,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
    time::Duration,
};

use tokio::{io::AsyncWriteExt, net::UnixStream};

pub use crate::socket_supervisor::ConnectionDrainReport as SocketDrainReport;
use crate::socket_supervisor::ConnectionSupervisor;

use super::{
    private_socket::{peer_uid, NoopBindObserver, PrivateSocketEndpoint},
    EventHub, SessionSocketBindObserver,
};

pub struct SessionSocket {
    endpoint: PrivateSocketEndpoint,
    hub: EventHub,
    shutdown: tokio::sync::broadcast::Sender<()>,
    connections: ConnectionSupervisor,
    serving: AtomicBool,
    serve_stopped: tokio::sync::Notify,
}

impl SessionSocket {
    pub async fn bind(
        path: impl AsRef<Path>,
        expected_uid: libc::uid_t,
        hub: EventHub,
    ) -> io::Result<Self> {
        Self::bind_with_observer(path, expected_uid, hub, Arc::new(NoopBindObserver)).await
    }

    pub async fn bind_with_observer(
        path: impl AsRef<Path>,
        expected_uid: libc::uid_t,
        hub: EventHub,
        observer: Arc<dyn SessionSocketBindObserver>,
    ) -> io::Result<Self> {
        let endpoint =
            PrivateSocketEndpoint::bind_with_observer(path, expected_uid, observer).await?;
        let (shutdown, _) = tokio::sync::broadcast::channel(1);
        Ok(Self {
            endpoint,
            hub,
            shutdown,
            connections: ConnectionSupervisor::new(32),
            serving: AtomicBool::new(false),
            serve_stopped: tokio::sync::Notify::new(),
        })
    }

    pub async fn serve_one(&self) -> io::Result<()> {
        let stream = self.endpoint.accept().await?;
        serve_stream(
            stream,
            self.endpoint.expected_uid(),
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
                accepted = self.endpoint.accept() => accepted,
                _ = listener_shutdown.recv() => return Ok(()),
            };
            let stream = accepted?;
            let expected_uid = self.endpoint.expected_uid();
            let hub = self.hub.clone();
            let shutdown = self.shutdown.subscribe();
            let Some(permit) = self.connections.try_admit() else {
                eprintln!("event=rejected_connection endpoint=session reason=limit");
                drop(stream);
                continue;
            };
            self.connections
                .spawn(permit, async move {
                    serve_stream(stream, expected_uid, hub, shutdown).await
                })
                .await;
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

        Ok(self.connections.drain(deadline).await)
    }

    pub fn path(&self) -> &Path {
        self.endpoint.path()
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
        tokio::time::timeout(Duration::from_secs(1), stream.write_all(&line))
            .await
            .map_err(|_| {
                io::Error::new(io::ErrorKind::TimedOut, "session event write timed out")
            })??;
    }
}
