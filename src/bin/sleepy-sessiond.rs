use std::{env, io, path::PathBuf, process::ExitCode, sync::Arc};

use sleepy_sdk::{EventCause, EventCauseKind, ProviderEvent, SessionEvent};
use sleepy_session::daily::{DailySocket, ProductionDailyBackend};
use sleepy_session::notifications::{
    FreedesktopNotificationProvider, NotificationDbusServer, NotificationEventService,
    NotificationSocket, NotificationStore,
};
use sleepy_session::osd::{spawn_osd_runtime, OsdPublicationHub, OsdSocket};
use sleepy_session::overview::overview_event_channel;
use sleepy_session::sessiond::supervisor::{
    ConnectionContext, DaemonLifecycle, PreparedDesktopSockets, StartupBarrier, SystemdNotifier,
};
use sleepy_session::sessiond::{
    full_snapshot_event, ControlSocket, EventHub, GenerationAllocator, GenerationAuthority,
    MutationPipeline, ProductionMutationBackend, ProductionSources, SessionSocket,
    ShutdownCoordinator,
};
use sleepy_session::{theme::ThemeManager, theme_socket::ThemeSocket};

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
    let config_dir = config_home()?;
    let cache_dir = cache_home()?;
    let socket_dir = runtime_dir.join("sleepy");
    let socket_path = socket_dir.join("session.sock");
    let control_socket_path = socket_dir.join("control.sock");
    let osd_socket_path = socket_dir.join("osd.sock");
    let daily_socket_path = socket_dir.join("daily.sock");
    let theme_socket_path = socket_dir.join("theme.sock");
    let notification_socket_path = socket_dir.join("notification.sock");
    let generation_path = state_dir.join("sleepy/session-generation");
    let (overview_sender, overview_events) = overview_event_channel(256);
    let daily_backend = Arc::new(ProductionDailyBackend::open_with_overview(
        &state_dir,
        &cache_dir,
        overview_events,
    )?);

    let mut allocator = GenerationAllocator::open(generation_path, 1024)?;
    let generation = allocator.next_generation()?;
    let hub = EventHub::new(full_snapshot_event(generation)?, 256);
    let authority = GenerationAuthority::new(allocator, generation, hub.clone());
    let osd_hub = OsdPublicationHub::new(64);
    let notification_store =
        NotificationStore::open_default(state_dir.join("sleepy/notifications"))?;
    let notification_provider = FreedesktopNotificationProvider::new(notification_store)?;
    let notification_service = Arc::new(tokio::sync::Mutex::new(NotificationEventService::new(
        notification_provider,
        authority.clone(),
    )));
    let notification_socket = NotificationSocket::bind(
        &notification_socket_path,
        unsafe { libc::geteuid() },
        Arc::clone(&notification_service),
    )
    .await?;
    let expected_uid = unsafe { libc::geteuid() };
    let socket = Arc::new(SessionSocket::bind(&socket_path, expected_uid, hub.clone()).await?);
    let mutation_backend = Arc::new(ProductionMutationBackend::new(hub.clone()));
    let mutation_pipeline = Arc::new(MutationPipeline::new(authority.clone(), mutation_backend));
    let control_socket =
        Arc::new(ControlSocket::bind(&control_socket_path, expected_uid, mutation_pipeline).await?);
    let osd_socket =
        Arc::new(OsdSocket::bind(&osd_socket_path, expected_uid, osd_hub.clone()).await?);
    let daily_socket =
        Arc::new(DailySocket::bind(&daily_socket_path, expected_uid, daily_backend).await?);
    let theme_manager = ThemeManager::open(&config_dir, &state_dir)
        .map_err(|error| io::Error::other(format!("theme provider: {error}")))?;
    let theme_socket = Arc::new(
        ThemeSocket::bind(
            &theme_socket_path,
            expected_uid,
            theme_manager,
            authority.clone(),
        )
        .await?,
    );
    let desktop_sockets = PreparedDesktopSockets::bind(&socket_dir, expected_uid).await?;

    let mut startup = StartupBarrier::new();
    let session_startup = startup.required_task("session");
    let control_startup = startup.required_task("control");
    let osd_startup = startup.required_task("osd");
    let daily_startup = startup.required_task("daily");
    let theme_startup = startup.required_task("theme");
    let notification_startup = startup.required_task("notification");
    let desktop_events_startup = startup.required_task("desktop-events");
    let desktop_requests_startup = startup.required_task("desktop-requests");
    let notification_dbus_startup = startup.required_task("notification-dbus");

    // D-Bus ownership is acquired after all Unix listeners bind. Its worker
    // thread acknowledges startup, then remains gated before process()/timer
    // activity so it cannot mutate or publish before READY.
    let mut notification_bus = NotificationDbusServer::start_session_gated(
        Arc::clone(&notification_service),
        tokio::runtime::Handle::current(),
        notification_dbus_startup,
    )?;
    let notification_socket =
        Arc::new(notification_socket.with_action_dispatcher(notification_bus.action_dispatcher()));
    let lifecycle = DaemonLifecycle::new(Arc::new(SystemdNotifier));
    let session_serving = Arc::clone(&socket);
    let mut session_task =
        tokio::spawn(async move { session_serving.serve_with_startup(session_startup).await });
    let control_serving = Arc::clone(&control_socket);
    let mut control_task =
        tokio::spawn(async move { control_serving.serve_with_startup(control_startup).await });
    let osd_serving = Arc::clone(&osd_socket);
    let mut osd_socket_task =
        tokio::spawn(async move { osd_serving.serve_with_startup(osd_startup).await });
    let daily_serving = Arc::clone(&daily_socket);
    let mut daily_task =
        tokio::spawn(async move { daily_serving.serve_with_startup(daily_startup).await });
    let theme_serving = Arc::clone(&theme_socket);
    let mut theme_task =
        tokio::spawn(async move { theme_serving.serve_with_startup(theme_startup).await });
    let notification_serving = Arc::clone(&notification_socket);
    let mut notification_task = tokio::spawn(async move {
        notification_serving
            .serve_with_startup(notification_startup)
            .await
    });
    let desktop_events = desktop_sockets.events();
    let desktop_events_serving = Arc::clone(&desktop_events);
    let mut desktop_events_task = tokio::spawn(async move {
        desktop_events_serving
            .serve_with_startup(desktop_events_startup, reject_prepared_desktop)
            .await
            .map(|_| ())
    });
    let desktop_requests = desktop_sockets.requests();
    let desktop_requests_serving = Arc::clone(&desktop_requests);
    let mut desktop_requests_task = tokio::spawn(async move {
        desktop_requests_serving
            .serve_with_startup(desktop_requests_startup, reject_prepared_desktop)
            .await
            .map(|_| ())
    });

    let desktop_paths = desktop_sockets.listener_paths();
    let producers = lifecycle
        .complete_startup(
            &[
                &socket_path,
                &control_socket_path,
                &osd_socket_path,
                &daily_socket_path,
                &theme_socket_path,
                &notification_socket_path,
                desktop_paths[0],
                desktop_paths[1],
            ],
            &mut startup,
            || async {
                let osd_events = hub.subscribe().await;
                let (osd_runtime, osd_task) = spawn_osd_runtime(osd_events, 16);
                let mut osd_publications = osd_runtime.subscribe();
                let publication_hub = osd_hub.clone();
                let osd_bridge = tokio::spawn(async move {
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
                let sources =
                    ProductionSources::start_with_overview(authority.clone(), overview_sender);
                let shutdown =
                    ShutdownCoordinator::new(authority.clone(), std::time::Duration::from_secs(2));
                Ok((osd_runtime, sources, shutdown, osd_task, osd_bridge))
            },
        )
        .await;
    let (osd_runtime, sources, shutdown, osd_task, osd_bridge) = match producers {
        Ok(producers) => producers,
        Err(error) => {
            let _ = tokio::join!(
                &mut session_task,
                &mut control_task,
                &mut osd_socket_task,
                &mut daily_task,
                &mut theme_task,
                &mut notification_task,
                &mut desktop_events_task,
                &mut desktop_requests_task,
            );
            return Err(error);
        }
    };
    let mut osd_task = Some(osd_task);
    let mut osd_bridge = Some(osd_bridge);
    let result = {
        tokio::select! {
            result = &mut session_task => socket_task_result(result, "session socket"),
            result = &mut control_task => socket_task_result(result, "control socket"),
            result = &mut osd_socket_task => socket_task_result(result, "OSD socket"),
            result = &mut daily_task => socket_task_result(result, "daily socket"),
            result = &mut theme_task => socket_task_result(result, "theme socket"),
            result = &mut notification_task => socket_task_result(result, "notification socket"),
            result = &mut desktop_events_task => socket_task_result(result, "desktop stream socket"),
            result = &mut desktop_requests_task => socket_task_result(result, "desktop request socket"),
            bridge = osd_bridge.as_mut().expect("OSD bridge handle is present") => {
                osd_bridge.take();
                match bridge {
                    Ok(Ok(())) => Err(io::Error::new(io::ErrorKind::BrokenPipe, "OSD publication bridge stopped")),
                    Ok(Err(error)) => Err(error),
                    Err(error) => Err(io::Error::other(format!("OSD publication bridge failed: {error}"))),
                }
            },
            runtime = osd_task.as_mut().expect("OSD runtime handle is present") => {
                osd_task.take();
                match runtime {
                    Ok(()) => Err(io::Error::new(io::ErrorKind::BrokenPipe, "OSD runtime stopped")),
                    Err(error) => Err(io::Error::other(format!("OSD runtime failed: {error}"))),
                }
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
    // The lifecycle seam emits STOPPING before entering this closure, so no
    // listener cancellation, D-Bus stop, producer stop, or drain can overtake
    // the systemd notification attempt.
    let cleanup = lifecycle
        .stop_and_drain(|| async move {
            let mut cleanup_error = None;
            drop(notification_bus);
            let (desktop_events, desktop_requests) = tokio::join!(
                desktop_events.shutdown_and_drain(),
                desktop_requests.shutdown_and_drain(),
            );
            if let Err(error) = desktop_events {
                cleanup_error.get_or_insert(error);
            }
            if let Err(error) = desktop_requests {
                cleanup_error.get_or_insert(error);
            }
            if let Err(error) = control_socket
                .shutdown_and_drain(std::time::Duration::from_secs(2))
                .await
            {
                cleanup_error = Some(error);
            }
            if let Err(error) = notification_socket
                .shutdown_and_drain(std::time::Duration::from_secs(2))
                .await
            {
                cleanup_error.get_or_insert(error);
            }
            if let Err(error) = theme_socket
                // Theme mutations can publish through the shared generation authority.
                // Cancel and join them before the terminal lifecycle barrier.
                .shutdown_and_drain(std::time::Duration::from_secs(2))
                .await
            {
                cleanup_error = Some(error);
            }
            if let Err(error) = sources
                // Stop producers before the lifecycle reconciliation barrier so no
                // capability event can overtake Stopping/Reconciled on shutdown.
                .shutdown_and_join(std::time::Duration::from_secs(4))
                .await
            {
                cleanup_error = Some(error);
            }
            if let Err(error) = shutdown.reconcile(&[]).await {
                cleanup_error.get_or_insert(error);
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
            if let Err(error) = daily_socket
                .shutdown_and_drain(std::time::Duration::from_secs(2))
                .await
            {
                cleanup_error.get_or_insert(error);
            }
            drop(osd_runtime);
            if let Some(osd_bridge) = osd_bridge {
                osd_bridge.abort();
                let _ = osd_bridge.await;
            }
            if let Some(osd_task) = osd_task {
                osd_task.abort();
                let _ = osd_task.await;
            }
            cleanup_error.map_or(Ok(()), Err)
        })
        .await;
    result.and(cleanup)
}

async fn reject_prepared_desktop(
    stream: tokio::net::UnixStream,
    _context: ConnectionContext,
) -> io::Result<()> {
    // Task 5 injects strict v3 handlers here. Closing is fail-closed and avoids
    // publishing a placeholder document that could violate the v3 schema.
    drop(stream);
    Ok(())
}

fn socket_task_result(
    result: Result<io::Result<()>, tokio::task::JoinError>,
    description: &'static str,
) -> io::Result<()> {
    result.unwrap_or_else(|error| {
        Err(io::Error::other(format!(
            "{description} task failed: {error}"
        )))
    })
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

fn cache_home() -> io::Result<PathBuf> {
    if let Some(path) = env::var_os("XDG_CACHE_HOME") {
        return Ok(PathBuf::from(path));
    }
    Ok(required_path("HOME")?.join(".cache"))
}

fn config_home() -> io::Result<PathBuf> {
    if let Some(path) = env::var_os("XDG_CONFIG_HOME") {
        return Ok(PathBuf::from(path));
    }
    Ok(required_path("HOME")?.join(".config"))
}
