// SPDX-License-Identifier: GPL-3.0-only

use std::{io, path::Path, sync::Arc, time::Duration};

use tokio::net::UnixStream;

use super::{
    supervisor::{
        ConnectionContext, ConnectionLimits, EndpointKind, RequiredStartupTask, SocketSupervisor,
    },
    EventHub, SessionSocketBindObserver, SocketDrainReport,
};

pub struct SessionSocket {
    supervisor: SocketSupervisor,
    hub: EventHub,
}

impl SessionSocket {
    pub async fn bind(
        path: impl AsRef<Path>,
        expected_uid: libc::uid_t,
        hub: EventHub,
    ) -> io::Result<Self> {
        Ok(Self {
            supervisor: SocketSupervisor::bind(
                path,
                expected_uid,
                EndpointKind::Stream,
                ConnectionLimits::stream(),
            )
            .await?,
            hub,
        })
    }

    pub async fn bind_with_observer(
        path: impl AsRef<Path>,
        expected_uid: libc::uid_t,
        hub: EventHub,
        observer: Arc<dyn SessionSocketBindObserver>,
    ) -> io::Result<Self> {
        Ok(Self {
            supervisor: SocketSupervisor::bind_with_observer(
                path,
                expected_uid,
                EndpointKind::Stream,
                ConnectionLimits::stream(),
                observer,
            )
            .await?,
            hub,
        })
    }

    pub async fn serve_one(&self) -> io::Result<()> {
        let hub = self.hub.clone();
        self.supervisor
            .serve_one(move |stream, context| serve_stream(stream, hub.clone(), context))
            .await
    }

    pub async fn serve(&self) -> io::Result<()> {
        let hub = self.hub.clone();
        self.supervisor
            .serve(move |stream, context| serve_stream(stream, hub.clone(), context))
            .await
            .map(|_| ())
    }

    pub async fn serve_with_startup(&self, startup: RequiredStartupTask) -> io::Result<()> {
        let hub = self.hub.clone();
        self.supervisor
            .serve_with_startup(startup, move |stream, context| {
                serve_stream(stream, hub.clone(), context)
            })
            .await
            .map(|_| ())
    }

    pub async fn shutdown_and_drain(&self, timeout: Duration) -> io::Result<SocketDrainReport> {
        self.supervisor
            .shutdown_and_drain_with_timeout(timeout)
            .await
    }

    pub fn path(&self) -> &Path {
        self.supervisor.path()
    }
}

async fn serve_stream(
    mut stream: UnixStream,
    hub: EventHub,
    context: ConnectionContext,
) -> io::Result<()> {
    let mut subscriber = hub.subscribe().await;
    loop {
        let event = tokio::select! {
            biased;
            event = subscriber.recv() => event.map_err(|error| match error {
                tokio::sync::broadcast::error::RecvError::Closed => {
                    io::Error::new(io::ErrorKind::BrokenPipe, "event hub closed")
                }
                tokio::sync::broadcast::error::RecvError::Lagged(_) => {
                    io::Error::new(io::ErrorKind::InvalidData, "event subscriber lagged")
                }
            })?,
            _ = context.cancellation.cancelled() => return Ok(()),
        };
        let line = serde_json::to_vec(&event)
            .map_err(|error| io::Error::new(io::ErrorKind::InvalidData, error))?;
        context.write_legacy_frame(&mut stream, &line).await?;
    }
}
