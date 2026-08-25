use std::{
    collections::{BTreeMap, HashMap},
    ffi::CString,
    future::Future,
    io,
    os::unix::ffi::OsStrExt,
    path::{Path, PathBuf},
    pin::Pin,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc, RwLock,
    },
    time::Duration,
};

use dbus::{blocking::Connection, message::MatchRule};
use serde_json::Value;
use sleepy_sdk::{
    CapabilityAvailability, CapabilityFailure, CapabilityRecord, CapabilityValue, EventCause,
    EventCauseKind, NiriEvent, NiriRuntimeState, RuntimeCapabilityId, SessionEvent,
};
use tokio::{
    io::{AsyncBufReadExt, BufReader},
    process::{Child, Command},
    sync::{mpsc, watch},
    task::JoinHandle,
};

use crate::{
    sessiond::{AdapterActor, AdapterFailure, CapabilityAdapter, GenerationAuthority},
    system::{ProcessCommandRunner, SystemFacade},
};

// Audio performs three fixed-argv reads and power performs two. Each command
// owns a 900 ms kill/wait/reader-join deadline in ProcessCommandRunner, so the
// actor deadline exceeds the proven worst case without leaving detached work.
const READBACK_TIMEOUT: Duration = Duration::from_secs(4);
const RESTART_DELAY: Duration = Duration::from_millis(250);
const MAX_EVENT_LINE: usize = 64 * 1024;

#[derive(Clone, Copy)]
enum Trigger {
    Read,
}

struct RuntimeAdapter {
    id: RuntimeCapabilityId,
    facade: SystemFacade<ProcessCommandRunner>,
}

pub struct ProductionSources {
    shutdown: watch::Sender<bool>,
    tasks: Vec<JoinHandle<io::Result<()>>>,
}

impl CapabilityAdapter for RuntimeAdapter {
    fn id(&self) -> RuntimeCapabilityId {
        self.id
    }

    fn observe(
        &self,
    ) -> Pin<Box<dyn Future<Output = Result<CapabilityRecord, AdapterFailure>> + Send + '_>> {
        let facade = self.facade.clone();
        let id = self.id;
        Box::pin(async move {
            tokio::task::spawn_blocking(move || facade.runtime_capability(id))
                .await
                .map_err(|error| {
                    AdapterFailure::new(
                        CapabilityAvailability::Error,
                        format!("{id:?} readback task failed: {error}"),
                    )
                })
        })
    }
}

impl ProductionSources {
    pub fn start(authority: GenerationAuthority) -> Self {
        let (shutdown, shutdown_receiver) = watch::channel(false);
        let facade = SystemFacade::new(ProcessCommandRunner);
        let focused_output = Arc::new(RwLock::new(None::<String>));
        let mut tasks = Vec::new();
        let mut triggers = BTreeMap::new();

        for id in [
            RuntimeCapabilityId::Audio,
            RuntimeCapabilityId::Brightness,
            RuntimeCapabilityId::PowerProfile,
            RuntimeCapabilityId::Media,
        ] {
            let (sender, receiver) = mpsc::channel(1);
            triggers.insert(id, sender.clone());
            tasks.push(tokio::spawn(run_capability_actor(
                id,
                facade.clone(),
                authority.clone(),
                Arc::clone(&focused_output),
                receiver,
                shutdown_receiver.clone(),
            )));
            let _ = sender.try_send(Trigger::Read);
        }

        tasks.push(tokio::spawn(run_process_event_source(
            "pw-mon",
            &[],
            triggers[&RuntimeCapabilityId::Audio].clone(),
            authority.clone(),
            RuntimeCapabilityId::Audio,
            shutdown_receiver.clone(),
        )));
        tasks.push(tokio::spawn(run_niri_source(
            authority.clone(),
            focused_output,
            shutdown_receiver.clone(),
        )));

        let brightness_path = std::env::var_os("SLEEPY_BACKLIGHT_PATH")
            .map(PathBuf::from)
            .unwrap_or_else(|| PathBuf::from("/sys/class/backlight"));
        tasks.push(tokio::spawn(run_brightness_source(
            brightness_path,
            triggers[&RuntimeCapabilityId::Brightness].clone(),
            authority.clone(),
            shutdown_receiver.clone(),
        )));
        tasks.push(tokio::spawn(run_dbus_sources(
            triggers[&RuntimeCapabilityId::PowerProfile].clone(),
            triggers[&RuntimeCapabilityId::Media].clone(),
            authority,
            shutdown_receiver,
        )));

        Self { shutdown, tasks }
    }

    pub async fn shutdown_and_join(mut self, timeout: Duration) -> io::Result<()> {
        let deadline = tokio::time::Instant::now() + timeout;
        let _ = self.shutdown.send(true);
        let mut first_error = None;
        for mut task in self.tasks.drain(..) {
            match tokio::time::timeout_at(deadline, &mut task).await {
                Ok(Ok(Ok(()))) => {}
                Ok(Ok(Err(error))) => {
                    first_error.get_or_insert(error);
                }
                Ok(Err(error)) => {
                    first_error.get_or_insert_with(|| {
                        io::Error::other(format!("source task failed: {error}"))
                    });
                }
                Err(_) => {
                    task.abort();
                    let _ = task.await;
                    first_error.get_or_insert_with(|| {
                        io::Error::new(
                            io::ErrorKind::TimedOut,
                            "production source task did not stop",
                        )
                    });
                }
            }
        }
        first_error.map_or(Ok(()), Err)
    }
}

async fn run_capability_actor(
    id: RuntimeCapabilityId,
    facade: SystemFacade<ProcessCommandRunner>,
    authority: GenerationAuthority,
    focused_output: Arc<RwLock<Option<String>>>,
    mut triggers: mpsc::Receiver<Trigger>,
    mut shutdown: watch::Receiver<bool>,
) -> io::Result<()> {
    let actor = AdapterActor::new(
        Arc::new(RuntimeAdapter { id, facade }),
        READBACK_TIMEOUT,
        RESTART_DELAY,
    );
    loop {
        tokio::select! {
            biased;
            changed = shutdown.changed() => {
                if changed.is_err() || *shutdown.borrow() { return Ok(()); }
            }
            trigger = triggers.recv() => {
                if trigger.is_none() { return Ok(()); }
                let record = actor.observe_once().await;
                let focus = focused_output
                    .read()
                    .map_err(|_| io::Error::other("Niri focus cache was poisoned"))?
                    .clone();
                if let Some(output_id) = focus {
                    publish(
                        &authority,
                        SessionEvent::Niri(NiriEvent {
                            focused_output_id: Some(output_id),
                        }),
                    )
                    .await?;
                }
                publish(&authority, SessionEvent::CapabilityUpdate(record)).await?;
            }
        }
    }
}

async fn run_process_event_source(
    program: &'static str,
    args: &'static [&'static str],
    trigger: mpsc::Sender<Trigger>,
    authority: GenerationAuthority,
    capability: RuntimeCapabilityId,
    mut shutdown: watch::Receiver<bool>,
) -> io::Result<()> {
    loop {
        if *shutdown.borrow() {
            return Ok(());
        }
        let mut child = match spawn_monitor(program, args) {
            Ok(child) => child,
            Err(error) => {
                publish_degraded(&authority, capability, error.kind(), &error.to_string()).await?;
                if wait_backoff(&mut shutdown).await {
                    return Ok(());
                }
                continue;
            }
        };
        let stdout = child
            .stdout
            .take()
            .ok_or_else(|| io::Error::other("monitor stdout missing"))?;
        let mut lines = BufReader::new(stdout);
        let mut buffer = Vec::new();
        loop {
            buffer.clear();
            let ended = tokio::select! {
                biased;
                changed = shutdown.changed() => {
                    if changed.is_err() || *shutdown.borrow() {
                        stop_child(&mut child).await;
                        return Ok(());
                    }
                    false
                }
                read = lines.read_until(b'\n', &mut buffer) => {
                    let count = read?;
                    if count == 0 { true } else {
                        if buffer.len() > MAX_EVENT_LINE {
                            stop_child(&mut child).await;
                            publish_degraded(&authority, capability, io::ErrorKind::InvalidData, "monitor event exceeded 64 KiB").await?;
                            true
                        } else {
                            let _ = trigger.try_send(Trigger::Read);
                            false
                        }
                    }
                }
            };
            if ended {
                break;
            }
        }
        let status = child.wait().await?;
        publish_degraded(
            &authority,
            capability,
            io::ErrorKind::BrokenPipe,
            &format!("{program} event stream exited with {status}"),
        )
        .await?;
        if wait_backoff(&mut shutdown).await {
            return Ok(());
        }
    }
}

async fn run_niri_source(
    authority: GenerationAuthority,
    focused_output: Arc<RwLock<Option<String>>>,
    mut shutdown: watch::Receiver<bool>,
) -> io::Result<()> {
    loop {
        if *shutdown.borrow() {
            return Ok(());
        }
        let mut child = match spawn_monitor("niri", &["msg", "--json", "event-stream"]) {
            Ok(child) => child,
            Err(error) => {
                clear_focus(&focused_output)?;
                publish_degraded(
                    &authority,
                    RuntimeCapabilityId::Niri,
                    error.kind(),
                    &error.to_string(),
                )
                .await?;
                if wait_backoff(&mut shutdown).await {
                    return Ok(());
                }
                continue;
            }
        };
        let stdout = child
            .stdout
            .take()
            .ok_or_else(|| io::Error::other("Niri stdout missing"))?;
        let mut lines = BufReader::new(stdout);
        let mut buffer = Vec::new();
        let mut workspaces = HashMap::<u64, String>::new();
        loop {
            buffer.clear();
            tokio::select! {
                biased;
                changed = shutdown.changed() => {
                    if changed.is_err() || *shutdown.borrow() {
                        stop_child(&mut child).await;
                        return Ok(());
                    }
                }
                read = lines.read_until(b'\n', &mut buffer) => {
                    if read? == 0 { break; }
                    if buffer.len() > MAX_EVENT_LINE {
                        stop_child(&mut child).await;
                        publish_degraded(&authority, RuntimeCapabilityId::Niri, io::ErrorKind::InvalidData, "Niri event exceeded 64 KiB").await?;
                        break;
                    }
                    match parse_niri_event(&buffer, &mut workspaces) {
                        Ok(Some(output)) => {
                            *focused_output
                                .write()
                                .map_err(|_| io::Error::other("Niri focus cache was poisoned"))? =
                                Some(output.clone());
                            publish(&authority, SessionEvent::Niri(NiriEvent { focused_output_id: Some(output) })).await?;
                            publish(&authority, SessionEvent::CapabilityUpdate(niri_record(&workspaces))).await?;
                        }
                        Ok(None) => {}
                        Err(error) => {
                            clear_focus(&focused_output)?;
                            publish(&authority, SessionEvent::Niri(NiriEvent { focused_output_id: None })).await?;
                            publish_degraded(&authority, RuntimeCapabilityId::Niri, io::ErrorKind::InvalidData, &error.to_string()).await?;
                        }
                    }
                }
            }
        }
        let status = child.wait().await?;
        clear_focus(&focused_output)?;
        publish(
            &authority,
            SessionEvent::Niri(NiriEvent {
                focused_output_id: None,
            }),
        )
        .await?;
        publish_degraded(
            &authority,
            RuntimeCapabilityId::Niri,
            io::ErrorKind::BrokenPipe,
            &format!("Niri event stream exited with {status}"),
        )
        .await?;
        if wait_backoff(&mut shutdown).await {
            return Ok(());
        }
    }
}

fn clear_focus(focused_output: &RwLock<Option<String>>) -> io::Result<()> {
    *focused_output
        .write()
        .map_err(|_| io::Error::other("Niri focus cache was poisoned"))? = None;
    Ok(())
}

fn parse_niri_event(
    bytes: &[u8],
    workspaces: &mut HashMap<u64, String>,
) -> io::Result<Option<String>> {
    let value: Value = serde_json::from_slice(bytes)
        .map_err(|error| io::Error::new(io::ErrorKind::InvalidData, error))?;
    if let Some(items) = value
        .pointer("/WorkspacesChanged/workspaces")
        .and_then(Value::as_array)
    {
        workspaces.clear();
        for workspace in items {
            if let (Some(id), Some(output)) = (
                workspace.get("id").and_then(Value::as_u64),
                workspace.get("output").and_then(Value::as_str),
            ) {
                workspaces.insert(id, output.to_owned());
                if workspace.get("is_focused").and_then(Value::as_bool) == Some(true) {
                    return Ok(Some(output.to_owned()));
                }
            }
        }
        return Ok(None);
    }
    let Some(activated) = value.get("WorkspaceActivated") else {
        return Ok(None);
    };
    if activated.get("focused").and_then(Value::as_bool) != Some(true) {
        return Ok(None);
    }
    let id = activated.get("id").and_then(Value::as_u64).ok_or_else(|| {
        io::Error::new(
            io::ErrorKind::InvalidData,
            "focused Niri workspace omitted id",
        )
    })?;
    Ok(workspaces.get(&id).cloned())
}

fn niri_record(workspaces: &HashMap<u64, String>) -> CapabilityRecord {
    let mut output_ids = workspaces.values().cloned().collect::<Vec<_>>();
    output_ids.sort();
    output_ids.dedup();
    let mut workspace_ids = workspaces.keys().copied().collect::<Vec<_>>();
    workspace_ids.sort_unstable();
    CapabilityRecord {
        id: RuntimeCapabilityId::Niri,
        status: CapabilityAvailability::Available,
        value: Some(CapabilityValue::Niri(NiriRuntimeState {
            output_ids,
            workspace_ids,
            window_ids: Vec::new(),
        })),
        diagnostic: None,
    }
}

async fn run_brightness_source(
    path: PathBuf,
    trigger: mpsc::Sender<Trigger>,
    authority: GenerationAuthority,
    mut shutdown: watch::Receiver<bool>,
) -> io::Result<()> {
    loop {
        let cancelled = Arc::new(AtomicBool::new(false));
        let thread_cancelled = Arc::clone(&cancelled);
        let watch_path = path.clone();
        let watch_trigger = trigger.clone();
        let mut task = tokio::task::spawn_blocking(move || {
            watch_inotify(&watch_path, &watch_trigger, &thread_cancelled)
        });
        let result = tokio::select! {
            result = &mut task => result.map_err(|error| io::Error::other(format!("brightness watcher task failed: {error}")))?,
            _ = shutdown.changed() => {
                cancelled.store(true, Ordering::Release);
                return task.await
                    .map_err(|error| io::Error::other(format!("brightness watcher task failed: {error}")))?;
            }
        };
        if let Err(error) = result {
            publish_degraded(
                &authority,
                RuntimeCapabilityId::Brightness,
                error.kind(),
                &error.to_string(),
            )
            .await?;
        }
        if wait_backoff(&mut shutdown).await {
            return Ok(());
        }
    }
}

fn watch_inotify(
    path: &Path,
    trigger: &mpsc::Sender<Trigger>,
    cancelled: &AtomicBool,
) -> io::Result<()> {
    let fd = unsafe { libc::inotify_init1(libc::IN_CLOEXEC | libc::IN_NONBLOCK) };
    if fd < 0 {
        return Err(io::Error::last_os_error());
    }
    if let Err(error) = add_backlight_watches(fd, path) {
        unsafe { libc::close(fd) };
        return Err(error);
    }
    let mut buffer = [0_u8; 4096];
    while !cancelled.load(Ordering::Acquire) {
        let mut poll = libc::pollfd {
            fd,
            events: libc::POLLIN,
            revents: 0,
        };
        let ready = unsafe { libc::poll(&mut poll, 1, 100) };
        if ready < 0 {
            let error = io::Error::last_os_error();
            unsafe { libc::close(fd) };
            return Err(error);
        }
        if ready > 0 {
            let read = unsafe { libc::read(fd, buffer.as_mut_ptr().cast(), buffer.len()) };
            if read > 0 {
                // A newly created backlight device needs its value file added
                // to the same kernel event set before subsequent changes.
                let _ = add_backlight_watches(fd, path);
                let _ = trigger.blocking_send(Trigger::Read);
            }
        }
    }
    unsafe { libc::close(fd) };
    Ok(())
}

fn add_backlight_watches(fd: libc::c_int, path: &Path) -> io::Result<()> {
    const VALUE_MASK: u32 = libc::IN_CLOSE_WRITE | libc::IN_MODIFY | libc::IN_ATTRIB;
    const ROOT_MASK: u32 = VALUE_MASK | libc::IN_CREATE | libc::IN_MOVED_TO;
    add_inotify_watch(fd, path, ROOT_MASK)?;
    if path.is_dir() {
        for entry in std::fs::read_dir(path)? {
            let brightness = entry?.path().join("brightness");
            if brightness.is_file() {
                add_inotify_watch(fd, &brightness, VALUE_MASK)?;
            }
        }
    }
    Ok(())
}

fn add_inotify_watch(fd: libc::c_int, path: &Path, mask: u32) -> io::Result<()> {
    let path = CString::new(path.as_os_str().as_bytes())
        .map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "backlight path contains NUL"))?;
    if unsafe { libc::inotify_add_watch(fd, path.as_ptr(), mask) } < 0 {
        return Err(io::Error::last_os_error());
    }
    Ok(())
}

async fn run_dbus_sources(
    power: mpsc::Sender<Trigger>,
    media: mpsc::Sender<Trigger>,
    authority: GenerationAuthority,
    mut shutdown: watch::Receiver<bool>,
) -> io::Result<()> {
    loop {
        let cancelled = Arc::new(AtomicBool::new(false));
        let thread_cancelled = Arc::clone(&cancelled);
        let power = power.clone();
        let media = media.clone();
        let mut task =
            tokio::task::spawn_blocking(move || watch_dbus(&power, &media, &thread_cancelled));
        let result = tokio::select! {
            result = &mut task => result.map_err(|error| io::Error::other(format!("D-Bus watcher task failed: {error}")))?,
            _ = shutdown.changed() => {
                cancelled.store(true, Ordering::Release);
                return task.await
                    .map_err(|error| io::Error::other(format!("D-Bus watcher task failed: {error}")))?;
            }
        };
        if let Err(error) = result {
            publish_degraded(
                &authority,
                RuntimeCapabilityId::PowerProfile,
                error.kind(),
                &error.to_string(),
            )
            .await?;
            publish_degraded(
                &authority,
                RuntimeCapabilityId::Media,
                error.kind(),
                &error.to_string(),
            )
            .await?;
        }
        if wait_backoff(&mut shutdown).await {
            return Ok(());
        }
    }
}

fn watch_dbus(
    power: &mpsc::Sender<Trigger>,
    media: &mpsc::Sender<Trigger>,
    cancelled: &AtomicBool,
) -> io::Result<()> {
    let connection = Connection::new_session().map_err(dbus_error)?;
    let power_sender = power.clone();
    let media_sender = media.clone();
    let rule = MatchRule::new_signal("org.freedesktop.DBus.Properties", "PropertiesChanged");
    connection
        .add_match(
            rule,
            move |_: (String, dbus::arg::PropMap, Vec<String>), _, message| {
                let path = message
                    .path()
                    .map(|path| path.to_string())
                    .unwrap_or_default();
                let sender = message
                    .sender()
                    .map(|sender| sender.to_string())
                    .unwrap_or_default();
                if path.contains("PowerProfiles") || sender.contains("PowerProfiles") {
                    let _ = power_sender.try_send(Trigger::Read);
                }
                if path.contains("MediaPlayer2") || sender.starts_with("org.mpris.MediaPlayer2") {
                    let _ = media_sender.try_send(Trigger::Read);
                }
                true
            },
        )
        .map_err(dbus_error)?;
    while !cancelled.load(Ordering::Acquire) {
        connection
            .process(Duration::from_millis(100))
            .map_err(dbus_error)?;
    }
    Ok(())
}

fn spawn_monitor(program: &str, args: &[&str]) -> io::Result<Child> {
    let mut command = Command::new(program);
    command
        .args(args)
        .kill_on_drop(true)
        .env("LC_ALL", "C")
        .stdin(std::process::Stdio::null())
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::null());
    command.spawn()
}

async fn stop_child(child: &mut Child) {
    let _ = child.kill().await;
    let _ = child.wait().await;
}

async fn wait_backoff(shutdown: &mut watch::Receiver<bool>) -> bool {
    tokio::select! {
        _ = tokio::time::sleep(RESTART_DELAY) => false,
        _ = shutdown.changed() => true,
    }
}

async fn publish(authority: &GenerationAuthority, event: SessionEvent) -> io::Result<()> {
    authority
        .lock()
        .await
        .publish(
            EventCause {
                kind: EventCauseKind::External,
                request_id: None,
            },
            event,
        )
        .await
        .map(|_| ())
}

async fn publish_degraded(
    authority: &GenerationAuthority,
    capability: RuntimeCapabilityId,
    kind: io::ErrorKind,
    message: &str,
) -> io::Result<()> {
    let status = match kind {
        io::ErrorKind::NotFound => CapabilityAvailability::Unsupported,
        io::ErrorKind::PermissionDenied => CapabilityAvailability::PermissionDenied,
        io::ErrorKind::TimedOut => CapabilityAvailability::Timeout,
        io::ErrorKind::InvalidData => CapabilityAvailability::Parse,
        _ => CapabilityAvailability::Error,
    };
    publish(
        authority,
        SessionEvent::CapabilityUpdate(CapabilityRecord {
            id: capability,
            status,
            value: None,
            diagnostic: Some(CapabilityFailure {
                message: message.to_owned(),
            }),
        }),
    )
    .await
}

fn dbus_error(error: dbus::Error) -> io::Error {
    io::Error::new(io::ErrorKind::ConnectionAborted, error)
}
