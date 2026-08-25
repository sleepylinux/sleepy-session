use std::{
    collections::{BTreeMap, HashMap},
    ffi::CString,
    io,
    os::unix::ffi::OsStrExt,
    path::{Path, PathBuf},
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
    io::{AsyncBufRead, AsyncBufReadExt, BufReader},
    process::{Child, Command},
    sync::{mpsc, watch},
    task::JoinHandle,
};

use crate::{
    sessiond::GenerationAuthority,
    system::{ProcessCommandRunner, SystemFacade},
};

const RESTART_DELAY: Duration = Duration::from_millis(250);
const MAX_EVENT_LINE: usize = 64 * 1024;

#[derive(Clone, Copy)]
enum Trigger {
    Read,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum DbusBus {
    System,
    Session,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct DbusSourceSpec {
    capability: RuntimeCapabilityId,
    bus: DbusBus,
    path_fragment: &'static str,
}

impl DbusSourceSpec {
    const fn power_profile() -> Self {
        Self {
            capability: RuntimeCapabilityId::PowerProfile,
            bus: DbusBus::System,
            path_fragment: "PowerProfiles",
        }
    }

    const fn mpris() -> Self {
        Self {
            capability: RuntimeCapabilityId::Media,
            bus: DbusBus::Session,
            path_fragment: "MediaPlayer2",
        }
    }
}

pub struct ProductionSources {
    shutdown: watch::Sender<bool>,
    tasks: Vec<JoinHandle<io::Result<()>>>,
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
        tasks.push(tokio::spawn(run_dbus_source(
            DbusSourceSpec::power_profile(),
            triggers[&RuntimeCapabilityId::PowerProfile].clone(),
            authority.clone(),
            shutdown_receiver.clone(),
        )));
        tasks.push(tokio::spawn(run_dbus_source(
            DbusSourceSpec::mpris(),
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
                    // Never abort an actor that may currently own a
                    // spawn_blocking readback: aborting the async wrapper would
                    // detach the blocking task and let commands outlive daemon
                    // shutdown. Record the deadline miss, then still join.
                    first_error.get_or_insert_with(|| {
                        io::Error::new(
                            io::ErrorKind::TimedOut,
                            "production source task did not stop",
                        )
                    });
                    match task.await {
                        Ok(Ok(())) => {}
                        Ok(Err(error)) => {
                            first_error.get_or_insert(error);
                        }
                        Err(error) => {
                            first_error.get_or_insert_with(|| {
                                io::Error::other(format!("source task failed: {error}"))
                            });
                        }
                    }
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
    let mut retry_not_before = None;
    loop {
        tokio::select! {
            biased;
            changed = shutdown.changed() => {
                if changed.is_err() || *shutdown.borrow() { return Ok(()); }
            }
            trigger = triggers.recv() => {
                if trigger.is_none() { return Ok(()); }
                if let Some(deadline) = retry_not_before.take() {
                    tokio::select! {
                        _ = tokio::time::sleep_until(deadline) => {}
                        _ = shutdown.changed() => return Ok(()),
                    }
                }
                let (record, stop_after_join) = joined_readback(
                    facade.clone(),
                    id,
                    &mut shutdown,
                ).await?;
                if stop_after_join {
                    return Ok(());
                }
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
                retry_not_before = (record.status != CapabilityAvailability::Available)
                    .then(|| tokio::time::Instant::now() + RESTART_DELAY);
                publish(&authority, SessionEvent::CapabilityUpdate(record)).await?;
            }
        }
    }
}

async fn joined_readback(
    facade: SystemFacade<ProcessCommandRunner>,
    id: RuntimeCapabilityId,
    shutdown: &mut watch::Receiver<bool>,
) -> io::Result<(CapabilityRecord, bool)> {
    let task = tokio::task::spawn_blocking(move || facade.runtime_capability(id));
    await_joined_readback(task, id, shutdown).await
}

async fn await_joined_readback(
    mut task: JoinHandle<CapabilityRecord>,
    id: RuntimeCapabilityId,
    shutdown: &mut watch::Receiver<bool>,
) -> io::Result<(CapabilityRecord, bool)> {
    tokio::select! {
        biased;
        changed = shutdown.changed() => {
            let stop = changed.is_err() || *shutdown.borrow();
            let record = task.await.map_err(|error| {
                io::Error::other(format!("{id:?} readback task failed: {error}"))
            })?;
            Ok((record, stop))
        }
        result = &mut task => {
            let record = result.map_err(|error| {
                io::Error::other(format!("{id:?} readback task failed: {error}"))
            })?;
            Ok((record, false))
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
    'supervisor: loop {
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
        let mut lines = BufReader::with_capacity(4096, stdout);
        loop {
            let read = tokio::select! {
                biased;
                changed = shutdown.changed() => {
                    if changed.is_err() || *shutdown.borrow() {
                        stop_child(&mut child).await;
                        return Ok(());
                    }
                    continue;
                }
                read = read_bounded_line(&mut lines) => read,
            };
            match read {
                Ok(Some(_)) => {
                    let _ = trigger.try_send(Trigger::Read);
                }
                Ok(None) => {
                    stop_child(&mut child).await;
                    publish_degraded(
                        &authority,
                        capability,
                        io::ErrorKind::BrokenPipe,
                        &format!("{program} event stream reached EOF"),
                    )
                    .await?;
                    if wait_backoff(&mut shutdown).await {
                        return Ok(());
                    }
                    continue 'supervisor;
                }
                Err(error) => {
                    stop_child(&mut child).await;
                    publish_degraded(
                        &authority,
                        capability,
                        error.kind(),
                        &format!("{program} event stream read failed: {error}"),
                    )
                    .await?;
                    if wait_backoff(&mut shutdown).await {
                        return Ok(());
                    }
                    continue 'supervisor;
                }
            }
        }
    }
}

async fn run_niri_source(
    authority: GenerationAuthority,
    focused_output: Arc<RwLock<Option<String>>>,
    mut shutdown: watch::Receiver<bool>,
) -> io::Result<()> {
    'supervisor: loop {
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
        let mut lines = BufReader::with_capacity(4096, stdout);
        let mut workspaces = HashMap::<u64, String>::new();
        loop {
            let read = tokio::select! {
                biased;
                changed = shutdown.changed() => {
                    if changed.is_err() || *shutdown.borrow() {
                        stop_child(&mut child).await;
                        return Ok(());
                    }
                    continue;
                }
                read = read_bounded_line(&mut lines) => read,
            };
            match read {
                Ok(Some(line)) => match parse_niri_event(&line, &mut workspaces) {
                    Ok(Some(output)) => {
                        *focused_output
                            .write()
                            .map_err(|_| io::Error::other("Niri focus cache was poisoned"))? =
                            Some(output.clone());
                        publish(
                            &authority,
                            SessionEvent::Niri(NiriEvent {
                                focused_output_id: Some(output),
                            }),
                        )
                        .await?;
                        publish(
                            &authority,
                            SessionEvent::CapabilityUpdate(niri_record(&workspaces)),
                        )
                        .await?;
                    }
                    Ok(None) => {}
                    Err(error) => {
                        stop_child(&mut child).await;
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
                            io::ErrorKind::InvalidData,
                            &error.to_string(),
                        )
                        .await?;
                        if wait_backoff(&mut shutdown).await {
                            return Ok(());
                        }
                        continue 'supervisor;
                    }
                },
                Ok(None) => {
                    stop_child(&mut child).await;
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
                        "Niri event stream reached EOF",
                    )
                    .await?;
                    if wait_backoff(&mut shutdown).await {
                        return Ok(());
                    }
                    continue 'supervisor;
                }
                Err(error) => {
                    stop_child(&mut child).await;
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
                        error.kind(),
                        &format!("Niri event stream read failed: {error}"),
                    )
                    .await?;
                    if wait_backoff(&mut shutdown).await {
                        return Ok(());
                    }
                    continue 'supervisor;
                }
            }
        }
    }
}

async fn read_bounded_line<R: AsyncBufRead + Unpin>(reader: &mut R) -> io::Result<Option<Vec<u8>>> {
    // Reserve the exact hard cap once. Extends are checked before copying, so
    // Vec never asks the allocator to grow beyond 64 KiB.
    let mut line = Vec::with_capacity(MAX_EVENT_LINE);
    loop {
        let available = reader.fill_buf().await?;
        if available.is_empty() {
            return Ok((!line.is_empty()).then_some(line));
        }
        let take = available
            .iter()
            .position(|byte| *byte == b'\n')
            .map_or(available.len(), |position| position + 1);
        if line.len().saturating_add(take) > MAX_EVENT_LINE {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "event line exceeded 64 KiB",
            ));
        }
        line.extend_from_slice(&available[..take]);
        reader.consume(take);
        if line.last() == Some(&b'\n') {
            return Ok(Some(line));
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

async fn run_dbus_source(
    spec: DbusSourceSpec,
    trigger: mpsc::Sender<Trigger>,
    authority: GenerationAuthority,
    mut shutdown: watch::Receiver<bool>,
) -> io::Result<()> {
    loop {
        let cancelled = Arc::new(AtomicBool::new(false));
        let thread_cancelled = Arc::clone(&cancelled);
        let trigger = trigger.clone();
        let mut task =
            tokio::task::spawn_blocking(move || watch_dbus(spec, &trigger, &thread_cancelled));
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
                spec.capability,
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
    spec: DbusSourceSpec,
    trigger: &mpsc::Sender<Trigger>,
    cancelled: &AtomicBool,
) -> io::Result<()> {
    let connection = match spec.bus {
        DbusBus::System => Connection::new_system(),
        DbusBus::Session => Connection::new_session(),
    }
    .map_err(dbus_error)?;
    let sender = trigger.clone();
    let rule = MatchRule::new_signal("org.freedesktop.DBus.Properties", "PropertiesChanged");
    connection
        .add_match(
            rule,
            move |_: (String, dbus::arg::PropMap, Vec<String>), _, message| {
                let path = message
                    .path()
                    .map(|path| path.to_string())
                    .unwrap_or_default();
                if path.contains(spec.path_fragment) {
                    let _ = sender.try_send(Trigger::Read);
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

#[cfg(test)]
mod tests {
    use std::sync::{
        atomic::{AtomicBool, AtomicUsize, Ordering},
        Arc,
    };

    use super::*;
    use tokio::io::{AsyncWriteExt, BufReader};

    #[test]
    fn dbus_source_specs_keep_system_power_and_session_mpris_isolated() {
        assert_eq!(DbusSourceSpec::power_profile().bus, DbusBus::System);
        assert_eq!(
            DbusSourceSpec::power_profile().capability,
            RuntimeCapabilityId::PowerProfile
        );
        assert_eq!(DbusSourceSpec::mpris().bus, DbusBus::Session);
        assert_eq!(
            DbusSourceSpec::mpris().capability,
            RuntimeCapabilityId::Media
        );
    }

    #[tokio::test]
    async fn unterminated_hostile_line_is_rejected_before_growth_past_hard_cap() {
        let (mut writer, reader) = tokio::io::duplex(MAX_EVENT_LINE + 4096);
        let payload = vec![b'x'; MAX_EVENT_LINE + 1];
        let write = tokio::spawn(async move {
            writer.write_all(&payload).await.unwrap();
        });
        let mut reader = BufReader::with_capacity(4096, reader);

        let error = read_bounded_line(&mut reader).await.unwrap_err();
        assert_eq!(error.kind(), io::ErrorKind::InvalidData);
        assert_eq!(error.to_string(), "event line exceeded 64 KiB");
        drop(reader);
        write.await.unwrap();
    }

    #[tokio::test]
    async fn shutdown_waits_for_owned_readback_task_and_never_detaches_it() {
        let finished = Arc::new(AtomicBool::new(false));
        let launches = Arc::new(AtomicUsize::new(0));
        let task_finished = Arc::clone(&finished);
        let task_launches = Arc::clone(&launches);
        let task = tokio::task::spawn_blocking(move || {
            task_launches.fetch_add(1, Ordering::SeqCst);
            std::thread::sleep(Duration::from_millis(60));
            task_finished.store(true, Ordering::Release);
            CapabilityRecord {
                id: RuntimeCapabilityId::Audio,
                status: CapabilityAvailability::Unsupported,
                value: None,
                diagnostic: Some(CapabilityFailure {
                    message: "fixture".into(),
                }),
            }
        });
        let (shutdown, mut receiver) = watch::channel(false);
        shutdown.send(true).unwrap();

        let (_, stop) = await_joined_readback(task, RuntimeCapabilityId::Audio, &mut receiver)
            .await
            .unwrap();

        assert!(stop);
        assert!(finished.load(Ordering::Acquire));
        assert_eq!(launches.load(Ordering::SeqCst), 1);
    }
}
