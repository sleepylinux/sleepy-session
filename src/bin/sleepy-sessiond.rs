use std::{env, io, path::PathBuf, process::ExitCode, sync::Arc};

use sleepy_sdk::{EventCause, EventCauseKind, ProviderEvent, SessionEvent};
use sleepy_session::notifications::{
    FreedesktopNotificationProvider, NotificationDbusServer, NotificationEventService,
    NotificationStore,
};
use sleepy_session::osd::{spawn_osd_runtime, OsdPublicationHub, OsdSocket};
use sleepy_session::sessiond::{
    full_snapshot_event, EventHub, GenerationAllocator, GenerationAuthority, ProductionSources,
    SessionSocket, ShutdownCoordinator,
};

#[tokio::main]
async fn main() -> ExitCode {
    match run().await {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) => {
            eprintln!("sleepy-sessiond: {error}");
            ExitCode::from(1)
        }
    }
}

async fn run() -> io::Result<()> {
    let runtime_dir = required_path("XDG_RUNTIME_DIR")?;
    let state_dir = state_home()?;
    let socket_path = runtime_dir.join("sleepy/session.sock");
    let osd_socket_path = runtime_dir.join("sleepy/osd.sock");
    let generation_path = state_dir.join("sleepy/session-generation");

    let mut allocator = GenerationAllocator::open(generation_path, 1024)?;
    let generation = allocator.next_generation()?;
    let hub = EventHub::new(full_snapshot_event(generation)?, 256);
    let authority = GenerationAuthority::new(allocator, generation, hub.clone());
    let osd_events = hub.subscribe().await;
    let (osd_runtime, mut osd_task) = spawn_osd_runtime(osd_events, 16);
    let mut osd_publications = osd_runtime.subscribe();
    let osd_hub = OsdPublicationHub::new(64);
    let publication_hub = osd_hub.clone();
    let mut osd_bridge = tokio::spawn(async move {
        loop {
            let publication = osd_publications.recv().await.map_err(|error| {
                io::Error::new(
                    io::ErrorKind::BrokenPipe,
                    format!("OSD runtime stopped: {error}"),
                )
            })?;
            publication_hub.publish(publication)?;
        }
    });
    let notification_store =
        NotificationStore::open_default(state_dir.join("sleepy/notifications"))?;
    let notification_provider = FreedesktopNotificationProvider::new(notification_store)?;
    let notification_service = Arc::new(tokio::sync::Mutex::new(NotificationEventService::new(
        notification_provider,
        authority.clone(),
    )));
    let mut notification_bus = NotificationDbusServer::start_session(
        Arc::clone(&notification_service),
        tokio::runtime::Handle::current(),
    )?;
    let expected_uid = unsafe { libc::geteuid() };
    let socket = SessionSocket::bind(&socket_path, expected_uid, hub.clone()).await?;
    let osd_socket = OsdSocket::bind(&osd_socket_path, expected_uid, osd_hub).await?;
    let sources = ProductionSources::start(authority.clone());
    let shutdown = ShutdownCoordinator::new(authority.clone(), std::time::Duration::from_secs(2));
    let result = {
        tokio::select! {
            result = socket.serve() => result,
            result = osd_socket.serve() => result,
            bridge = &mut osd_bridge => match bridge {
                Ok(Ok(())) => Err(io::Error::new(io::ErrorKind::BrokenPipe, "OSD publication bridge stopped")),
                Ok(Err(error)) => Err(error),
                Err(error) => Err(io::Error::other(format!("OSD publication bridge failed: {error}"))),
            },
            runtime = &mut osd_task => match runtime {
                Ok(()) => Err(io::Error::new(io::ErrorKind::BrokenPipe, "OSD runtime stopped")),
                Err(error) => Err(io::Error::other(format!("OSD runtime failed: {error}"))),
            },
            error = notification_bus.wait_for_failure() => {
                let _ = authority
                    .lock()
                    .await
                    .publish(
                        EventCause {
                            kind: EventCauseKind::External,
                            request_id: None,
                        },
                        SessionEvent::Provider(ProviderEvent {
                            provider_id: "org.freedesktop.Notifications".into(),
                            online: false,
                        }),
                    )
                    .await;
                Err(error)
            }
            signal = tokio::signal::ctrl_c() => signal,
        }
    };
    let mut cleanup_error = None;
    if let Err(error) = shutdown.reconcile(&[]).await {
        cleanup_error = Some(error);
    }
    if let Err(error) = socket
        .shutdown_and_drain(std::time::Duration::from_secs(2))
        .await
    {
        cleanup_error.get_or_insert(error);
    }
    if let Err(error) = osd_socket
        .shutdown_and_drain(std::time::Duration::from_secs(2))
        .await
    {
        cleanup_error.get_or_insert(error);
    }
    if let Err(error) = sources
        .shutdown_and_join(std::time::Duration::from_secs(2))
        .await
    {
        cleanup_error.get_or_insert(error);
    }
    osd_bridge.abort();
    let _ = osd_bridge.await;
    osd_task.abort();
    let _ = osd_task.await;
    result.and_then(|_| cleanup_error.map_or(Ok(()), Err))
}

fn required_path(name: &str) -> io::Result<PathBuf> {
    env::var_os(name).map(PathBuf::from).ok_or_else(|| {
        io::Error::new(
            io::ErrorKind::NotFound,
            format!("required environment variable {name} is not set"),
        )
    })
}

fn state_home() -> io::Result<PathBuf> {
    if let Some(path) = env::var_os("XDG_STATE_HOME") {
        return Ok(PathBuf::from(path));
    }
    Ok(required_path("HOME")?.join(".local/state"))
}
