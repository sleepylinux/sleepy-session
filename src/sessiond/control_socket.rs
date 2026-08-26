// SPDX-License-Identifier: GPL-3.0-only

use super::{
    private_socket::{peer_uid, read_bounded_line, PrivateSocketEndpoint},
    MutationBackend, MutationPipeline,
};
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

use crate::socket_supervisor::ConnectionSupervisor;

const MAX_LINE: usize = 256 * 1024;
const MAX_CONNECTIONS: usize = 16;
const WRITE_TIMEOUT: Duration = Duration::from_secs(1);

pub struct ControlSocket<B: MutationBackend> {
    endpoint: PrivateSocketEndpoint,
    pipeline: Arc<MutationPipeline<B>>,
    shutdown: tokio::sync::broadcast::Sender<()>,
    connections: ConnectionSupervisor,
    serving: AtomicBool,
    stopped: tokio::sync::Notify,
}

impl<B: MutationBackend> ControlSocket<B> {
    pub async fn bind(
        path: impl AsRef<Path>,
        expected_uid: libc::uid_t,
        pipeline: Arc<MutationPipeline<B>>,
    ) -> io::Result<Self> {
        let (shutdown, _) = tokio::sync::broadcast::channel(1);
        Ok(Self {
            endpoint: PrivateSocketEndpoint::bind(path, expected_uid).await?,
            pipeline,
            shutdown,
            connections: ConnectionSupervisor::new(MAX_CONNECTIONS),
            serving: AtomicBool::new(false),
            stopped: tokio::sync::Notify::new(),
        })
    }
    pub async fn serve_one(&self) -> io::Result<()> {
        let stream = self.endpoint.accept().await?;
        serve_stream(
            stream,
            self.endpoint.expected_uid(),
            Arc::clone(&self.pipeline),
        )
        .await
    }
    pub async fn serve(&self) -> io::Result<()> {
        if self.serving.swap(true, Ordering::AcqRel) {
            return Err(io::Error::new(
                io::ErrorKind::AlreadyExists,
                "control socket already serving",
            ));
        }
        let _guard = Serving {
            flag: &self.serving,
            stopped: &self.stopped,
        };
        let mut shutdown = self.shutdown.subscribe();
        loop {
            let stream = tokio::select! {
                stream = self.endpoint.accept() => stream?,
                _ = shutdown.recv() => return Ok(()),
            };
            let expected_uid = self.endpoint.expected_uid();
            let pipeline = Arc::clone(&self.pipeline);
            let Some(permit) = self.connections.try_admit() else {
                eprintln!("event=rejected_connection endpoint=control reason=limit");
                drop(stream);
                continue;
            };
            self.connections
                .spawn(permit, async move {
                    serve_stream(stream, expected_uid, pipeline).await
                })
                .await;
        }
    }
    pub async fn shutdown_and_drain(&self, timeout: Duration) -> io::Result<usize> {
        let deadline = tokio::time::Instant::now() + timeout;
        let _ = self.shutdown.send(());
        if self.serving.load(Ordering::Acquire) {
            tokio::time::timeout_at(deadline, async {
                while self.serving.load(Ordering::Acquire) {
                    let notified = self.stopped.notified();
                    if !self.serving.load(Ordering::Acquire) {
                        break;
                    }
                    notified.await;
                }
            })
            .await
            .map_err(|_| {
                io::Error::new(io::ErrorKind::TimedOut, "control accept loop did not stop")
            })?;
        }
        let report = self.connections.drain(deadline).await;
        Ok(report.completed + report.aborted)
    }
    pub fn path(&self) -> &Path {
        self.endpoint.path()
    }
}

struct Serving<'a> {
    flag: &'a AtomicBool,
    stopped: &'a tokio::sync::Notify,
}
impl Drop for Serving<'_> {
    fn drop(&mut self) {
        self.flag.store(false, Ordering::Release);
        self.stopped.notify_waiters();
    }
}

async fn serve_stream<B: MutationBackend>(
    stream: UnixStream,
    expected_uid: libc::uid_t,
    pipeline: Arc<MutationPipeline<B>>,
) -> io::Result<()> {
    if peer_uid(&stream)? != expected_uid {
        return Err(io::Error::new(
            io::ErrorKind::PermissionDenied,
            "control socket peer UID mismatch",
        ));
    }
    let (read, mut write) = stream.into_split();
    let bytes = read_bounded_line(
        read,
        MAX_LINE,
        Duration::from_secs(3),
        "invalid bounded control request",
    )
    .await?;
    let input = std::str::from_utf8(&bytes)
        .map_err(|error| io::Error::new(io::ErrorKind::InvalidData, error))?;
    let result = pipeline
        .handle_json(input)
        .await
        .map_err(|error| io::Error::new(io::ErrorKind::InvalidData, error))?;
    let mut response = serde_json::to_vec(&result)
        .map_err(|error| io::Error::new(io::ErrorKind::InvalidData, error))?;
    response.push(b'\n');
    tokio::time::timeout(WRITE_TIMEOUT, async {
        write.write_all(&response).await?;
        write.shutdown().await
    })
    .await
    .map_err(|_| io::Error::new(io::ErrorKind::TimedOut, "control response write timed out"))?
}
