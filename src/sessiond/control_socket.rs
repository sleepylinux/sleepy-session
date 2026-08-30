// SPDX-License-Identifier: GPL-3.0-only

use std::{io, path::Path, sync::Arc, time::Duration};

use tokio::net::UnixStream;

use super::{
    supervisor::{ConnectionContext, ConnectionLimits, EndpointKind, SocketSupervisor},
    MutationBackend, MutationPipeline,
};

const MAX_LINE: usize = 256 * 1024;
const MAX_CONNECTIONS: usize = 16;
const READ_TIMEOUT: Duration = Duration::from_secs(3);
const WRITE_TIMEOUT: Duration = Duration::from_secs(1);
const DRAIN_TIMEOUT: Duration = Duration::from_secs(10);

pub struct ControlSocket<B: MutationBackend> {
    supervisor: SocketSupervisor,
    pipeline: Arc<MutationPipeline<B>>,
}

impl<B: MutationBackend> ControlSocket<B> {
    pub async fn bind(
        path: impl AsRef<Path>,
        expected_uid: libc::uid_t,
        pipeline: Arc<MutationPipeline<B>>,
    ) -> io::Result<Self> {
        let limits = ConnectionLimits {
            max_clients: MAX_CONNECTIONS,
            max_frame_bytes: MAX_LINE,
            read_timeout: READ_TIMEOUT,
            write_timeout: WRITE_TIMEOUT,
            drain_timeout: DRAIN_TIMEOUT,
        };
        Ok(Self {
            supervisor: SocketSupervisor::bind(path, expected_uid, EndpointKind::Request, limits)
                .await?,
            pipeline,
        })
    }

    pub async fn serve_one(&self) -> io::Result<()> {
        let pipeline = Arc::clone(&self.pipeline);
        self.supervisor
            .serve_one(move |stream, context| serve_stream(stream, pipeline, context))
            .await
    }

    pub async fn serve(&self) -> io::Result<()> {
        let pipeline = Arc::clone(&self.pipeline);
        self.supervisor
            .serve(move |stream, context| serve_stream(stream, Arc::clone(&pipeline), context))
            .await
            .map(|_| ())
    }

    pub async fn shutdown_and_drain(&self, timeout: Duration) -> io::Result<usize> {
        let report = self
            .supervisor
            .shutdown_and_drain_with_timeout(timeout)
            .await?;
        let drained = report.completed + report.aborted;
        // Preserve the v2 wrapper's observable count: a serve loop that handled
        // and already reaped work still reports one completed handler.
        Ok(if drained == 0 && self.supervisor.metrics().completed > 0 {
            1
        } else {
            drained
        })
    }

    pub fn path(&self) -> &Path {
        self.supervisor.path()
    }
}

async fn serve_stream<B: MutationBackend>(
    stream: UnixStream,
    pipeline: Arc<MutationPipeline<B>>,
    context: ConnectionContext,
) -> io::Result<()> {
    let (mut read, mut write) = stream.into_split();
    let bytes = context.read_frame(&mut read).await?;
    let input = std::str::from_utf8(&bytes)
        .map_err(|error| io::Error::new(io::ErrorKind::InvalidData, error))?;
    let result = pipeline
        .handle_json(input)
        .await
        .map_err(|error| io::Error::new(io::ErrorKind::InvalidData, error))?;
    let response = serde_json::to_vec(&result)
        .map_err(|error| io::Error::new(io::ErrorKind::InvalidData, error))?;
    context.write_legacy_frame(&mut write, &response).await
}
