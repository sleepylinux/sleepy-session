// SPDX-License-Identifier: GPL-3.0-only

use std::{
    fs,
    io::{self, Write},
    os::fd::AsRawFd,
    os::unix::fs::PermissionsExt,
    path::{Path, PathBuf},
    process::{Child, Command, Stdio},
    sync::{Arc, Mutex as StdMutex},
    thread,
    time::{Duration, Instant},
};

use async_trait::async_trait;
use dbus::blocking::stdintf::org_freedesktop_dbus::Properties;
use sleepy_sdk::{
    CapabilityAvailability, ClipboardEntry, DesktopSessionCommand, LockState, RecordingState,
    RecordingStatus, UtilityCommand,
};
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;
use zeroize::Zeroize;

use super::{
    DesktopDomainId, DesktopDomainState, DesktopDomainUpdate, DesktopDomainValue, DesktopProducer,
    ProducerError,
};
use crate::system::{CommandSpec, ProcessCommandRunner};

const LOGIN1_DESTINATION: &str = "org.freedesktop.login1";
const LOGIN1_MANAGER_PATH: &str = "/org/freedesktop/login1";
const LOGIN1_MANAGER: &str = "org.freedesktop.login1.Manager";
const LOGIN1_SESSION: &str = "org.freedesktop.login1.Session";
const LOGIND_ACTION_TIMEOUT: Duration = Duration::from_secs(30);
const LOGIND_STATE_TIMEOUT: Duration = Duration::from_millis(1_750);
const UTILITY_REFRESH: Duration = Duration::from_secs(2);
const GAME_MODE_DESTINATION: &str = "com.feralinteractive.GameMode";
const GAME_MODE_PATH: &str = "/com/feralinteractive/GameMode";
const GAME_MODE_INTERFACE: &str = "com.feralinteractive.GameMode";

pub fn action_spec(
    command: &UtilityCommand,
    output_path: &str,
    gesture_token: &str,
) -> io::Result<Option<CommandSpec>> {
    let spec = match command {
        UtilityCommand::Screenshot { output_id } => {
            validate_output_path(output_path)?;
            validate_gesture_token(gesture_token)?;
            CommandSpec::new(
                "sleepy-capture-helper",
                [
                    "screenshot",
                    "--gesture-token",
                    gesture_token,
                    "--output-id",
                    output_name(output_id)?,
                    "--output-path",
                    output_path,
                ],
            )
        }
        UtilityCommand::PickColor => {
            validate_gesture_token(gesture_token)?;
            CommandSpec::new(
                "sleepy-capture-helper",
                [
                    "pick-color",
                    "--gesture-token",
                    gesture_token,
                    "--result-fd",
                    "1",
                ],
            )
        }
        UtilityCommand::StartRecording { output_id } => {
            validate_output_path(output_path)?;
            validate_gesture_token(gesture_token)?;
            CommandSpec::new(
                "sleepy-capture-helper",
                [
                    "record",
                    "--gesture-token",
                    gesture_token,
                    "--output-id",
                    output_name(output_id)?,
                    "--output-path",
                    output_path,
                ],
            )
        }
        UtilityCommand::InvokeTrayMenu { .. }
        | UtilityCommand::PasteClipboard { .. }
        | UtilityCommand::ClearClipboard
        | UtilityCommand::SetIdleInhibited { .. }
        | UtilityCommand::PauseRecording
        | UtilityCommand::StopRecording
        | UtilityCommand::SetGameMode { .. } => return Ok(None),
    };
    Ok(Some(spec))
}

fn execute_capture_with<R: crate::system::CommandRunner>(
    runner: &R,
    command: &UtilityCommand,
    output_path: &str,
    gesture_token: &str,
) -> io::Result<()> {
    let spec = action_spec(command, output_path, gesture_token)?
        .ok_or_else(|| io::Error::new(io::ErrorKind::Unsupported, "capture command required"))?;
    super::network::run(runner, spec).map(|_| ())
}

fn validate_gesture_token(token: &str) -> io::Result<()> {
    let parsed = uuid::Uuid::parse_str(token)
        .map_err(|_| io::Error::new(io::ErrorKind::PermissionDenied, "capture.gesture-required"))?;
    if parsed.to_string() != token {
        return Err(io::Error::new(
            io::ErrorKind::PermissionDenied,
            "capture.gesture-required",
        ));
    }
    Ok(())
}

fn output_name(output_id: &sleepy_sdk::StableId) -> io::Result<&str> {
    let value = output_id
        .as_str()
        .strip_prefix("output:")
        .unwrap_or_else(|| output_id.as_str());
    Some(value)
        .filter(|value| {
            !value.is_empty()
                && value.len() <= 128
                && value
                    .bytes()
                    .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.'))
        })
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "invalid output ID"))
}

fn validate_output_path(path: &str) -> io::Result<()> {
    let path = std::path::Path::new(path);
    if !path.is_absolute() || path.as_os_str().as_encoded_bytes().contains(&0) {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "utility output path is invalid",
        ));
    }
    Ok(())
}

struct RecordingRuntime {
    state: RecordingState,
    child: Option<Child>,
}

impl Default for RecordingRuntime {
    fn default() -> Self {
        Self {
            state: RecordingState {
                status: RecordingStatus::Inactive,
                recording_id: None,
                output_id: None,
            },
            child: None,
        }
    }
}

pub struct ProductionUtilityService {
    runner: ProcessCommandRunner,
    capture_root: PathBuf,
    _capture_directory: crate::store::SecureDir,
    tray: super::tray::TrayService,
    recording: StdMutex<RecordingRuntime>,
    idle_inhibitor: StdMutex<Option<dbus::arg::OwnedFd>>,
    game_mode: StdMutex<bool>,
}

impl ProductionUtilityService {
    pub fn open(capture_root: impl Into<PathBuf>) -> io::Result<Self> {
        let capture_root = capture_root.into();
        let capture_directory = crate::store::SecureDir::open_writable(&capture_root, true)
            .map_err(|error| io::Error::other(error.to_string()))?;
        Ok(Self {
            runner: ProcessCommandRunner,
            capture_root,
            _capture_directory: capture_directory,
            tray: super::tray::TrayService::default(),
            recording: StdMutex::new(RecordingRuntime::default()),
            idle_inhibitor: StdMutex::new(None),
            game_mode: StdMutex::new(false),
        })
    }

    pub fn state(&self, domain: DesktopDomainId) -> DesktopDomainState {
        if matches!(
            domain,
            DesktopDomainId::Screenshot | DesktopDomainId::ColorPicker
        ) {
            return match ensure_executable("sleepy-capture-helper") {
                Ok(()) => DesktopDomainState::available(
                    domain,
                    if domain == DesktopDomainId::Screenshot {
                        DesktopDomainValue::Screenshot
                    } else {
                        DesktopDomainValue::ColorPicker
                    },
                )
                .expect("matching stateless capture domain"),
                Err(error) => terminal(domain, availability_for_io(&error), error.to_string()),
            };
        }
        let result = match domain {
            DesktopDomainId::Tray => self.tray.probe().map(DesktopDomainValue::Tray),
            DesktopDomainId::Clipboard => {
                self.clipboard_snapshot().map(DesktopDomainValue::Clipboard)
            }
            DesktopDomainId::Recording => {
                self.recording_snapshot().map(DesktopDomainValue::Recording)
            }
            DesktopDomainId::IdleInhibit => {
                self.idle_snapshot().map(DesktopDomainValue::IdleInhibit)
            }
            DesktopDomainId::GameMode => {
                self.game_mode_snapshot().map(DesktopDomainValue::GameMode)
            }
            _ => Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "utility service was assigned a non-utility domain",
            )),
        };
        match result {
            Ok(value) => DesktopDomainState::available(domain, value).unwrap_or_else(|error| {
                terminal(domain, CapabilityAvailability::Parse, error.to_string())
            }),
            Err(error) => terminal(domain, availability_for_io(&error), error.to_string()),
        }
    }

    pub fn execute(
        &self,
        command: &UtilityCommand,
        gesture_token: &str,
    ) -> io::Result<DesktopDomainState> {
        match command {
            UtilityCommand::InvokeTrayMenu { item_id, menu_id } => {
                self.tray.invoke(item_id, menu_id)?;
                Ok(self.state(DesktopDomainId::Tray))
            }
            UtilityCommand::PasteClipboard { entry_id } => {
                self.paste_clipboard(entry_id)?;
                Ok(self.state(DesktopDomainId::Clipboard))
            }
            UtilityCommand::ClearClipboard => {
                super::network::run(&self.runner, super::clipboard::clear_spec())?;
                Ok(self.state(DesktopDomainId::Clipboard))
            }
            UtilityCommand::SetIdleInhibited { enabled } => {
                self.set_idle_inhibited(*enabled)?;
                Ok(self.state(DesktopDomainId::IdleInhibit))
            }
            UtilityCommand::StartRecording { output_id } => {
                self.start_recording(output_id, gesture_token)?;
                Ok(self.state(DesktopDomainId::Recording))
            }
            UtilityCommand::PauseRecording => {
                self.pause_recording()?;
                Ok(self.state(DesktopDomainId::Recording))
            }
            UtilityCommand::StopRecording => {
                self.stop_recording()?;
                Ok(self.state(DesktopDomainId::Recording))
            }
            UtilityCommand::Screenshot { .. } => {
                let path = self.output_path("screenshot", "png");
                let path = path_to_string(&path)?;
                execute_capture_with(&self.runner, command, path, gesture_token)?;
                Ok(self.state(DesktopDomainId::Screenshot))
            }
            UtilityCommand::PickColor => {
                execute_capture_with(&self.runner, command, "unused", gesture_token)?;
                Ok(self.state(DesktopDomainId::ColorPicker))
            }
            UtilityCommand::SetGameMode { enabled } => {
                self.set_game_mode(*enabled)?;
                Ok(self.state(DesktopDomainId::GameMode))
            }
        }
    }

    fn clipboard_snapshot(&self) -> io::Result<Vec<ClipboardEntry>> {
        self.clipboard_snapshot_with(&self.runner)
    }

    fn clipboard_snapshot_with<R: crate::system::CommandRunner>(
        &self,
        runner: &R,
    ) -> io::Result<Vec<ClipboardEntry>> {
        let list = super::network::run(runner, super::clipboard::list_spec())?;
        let rows = super::clipboard::parse_list(&list)?;
        let mut entries = Vec::with_capacity(rows.len());
        for (id, preview) in rows {
            let stable_id = sleepy_sdk::StableId(format!("clipboard:{id}"));
            let binary = preview.starts_with("[[ binary data");
            entries.push(ClipboardEntry {
                id: stable_id.0,
                byte_length: u64::try_from(preview.len())
                    .map_err(|_| io::Error::other("clipboard length overflow"))?,
                preview,
                mime_type: if binary {
                    "application/octet-stream"
                } else {
                    "text/plain;charset=utf-8"
                }
                .into(),
            });
        }
        Ok(entries)
    }

    fn paste_clipboard(&self, entry_id: &sleepy_sdk::StableId) -> io::Result<()> {
        let timeout = Duration::from_secs(10);
        let deadline = Instant::now() + timeout;
        let runner = super::core::DeadlineRunner::new(self.runner, timeout);
        let current = self.clipboard_snapshot_with(&runner)?;
        if !current.iter().any(|entry| entry.id == entry_id.as_str()) {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "unknown clipboard entry ID",
            ));
        }
        let mut contents = super::network::run(&runner, super::clipboard::decode_spec(entry_id)?)?;
        let result = write_wayland_clipboard(&contents, deadline);
        contents.zeroize();
        result
    }

    fn recording_snapshot(&self) -> io::Result<RecordingState> {
        ensure_executable("sleepy-capture-helper")?;
        let mut runtime = self
            .recording
            .lock()
            .map_err(|_| io::Error::other("recording state lock poisoned"))?;
        if let Some(child) = runtime.child.as_mut() {
            if child.try_wait()?.is_some() {
                *runtime = RecordingRuntime::default();
            }
        }
        Ok(runtime.state.clone())
    }

    fn start_recording(
        &self,
        output_id: &sleepy_sdk::StableId,
        gesture_token: &str,
    ) -> io::Result<()> {
        let mut runtime = self
            .recording
            .lock()
            .map_err(|_| io::Error::other("recording state lock poisoned"))?;
        if runtime.child.is_some() {
            return Err(io::Error::new(
                io::ErrorKind::AlreadyExists,
                "a recording is already active",
            ));
        }
        let recording_id = uuid::Uuid::new_v4().hyphenated().to_string();
        let path = self
            .capture_root
            .join(format!("recording-{recording_id}.mkv"));
        let path_text = path_to_string(&path)?;
        let spec = action_spec(
            &UtilityCommand::StartRecording {
                output_id: output_id.clone(),
            },
            path_text,
            gesture_token,
        )?
        .ok_or_else(|| io::Error::other("recording command contract missing"))?;
        let mut child = ChildGuard::new(fixed_child(&spec)?.spawn()?);
        thread::sleep(Duration::from_millis(40));
        if child.child_mut()?.try_wait()?.is_some() {
            return Err(io::Error::other("recording process exited before readback"));
        }
        runtime.state = RecordingState {
            status: RecordingStatus::Recording,
            recording_id: Some(recording_id),
            output_id: Some(output_id.as_str().to_owned()),
        };
        runtime.child = Some(child.disarm()?);
        Ok(())
    }

    fn pause_recording(&self) -> io::Result<()> {
        let mut runtime = self
            .recording
            .lock()
            .map_err(|_| io::Error::other("recording state lock poisoned"))?;
        let child = runtime
            .child
            .as_mut()
            .ok_or_else(|| io::Error::new(io::ErrorKind::NotFound, "no recording is active"))?;
        if child.try_wait()?.is_some() {
            *runtime = RecordingRuntime::default();
            return Err(io::Error::new(
                io::ErrorKind::BrokenPipe,
                "recording process already exited",
            ));
        }
        let result = unsafe { libc::kill(child.id() as libc::pid_t, libc::SIGUSR1) };
        if result != 0 {
            return Err(io::Error::last_os_error());
        }
        runtime.state.status = match runtime.state.status {
            RecordingStatus::Recording => RecordingStatus::Paused,
            RecordingStatus::Paused => RecordingStatus::Recording,
            RecordingStatus::Inactive => return Err(io::Error::other("recording state mismatch")),
        };
        Ok(())
    }

    fn stop_recording(&self) -> io::Result<()> {
        let mut runtime = self
            .recording
            .lock()
            .map_err(|_| io::Error::other("recording state lock poisoned"))?;
        let Some(mut child) = runtime.child.take() else {
            return Err(io::Error::new(
                io::ErrorKind::NotFound,
                "no recording is active",
            ));
        };
        let result = terminate_child(&mut child, libc::SIGINT, Duration::from_secs(5));
        if result.is_err() {
            let _ = child.kill();
            let _ = child.wait();
        }
        runtime.state = RecordingRuntime::default().state;
        result
    }

    fn idle_snapshot(&self) -> io::Result<bool> {
        require_bus_name(true, LOGIN1_DESTINATION)?;
        Ok(self
            .idle_inhibitor
            .lock()
            .map_err(|_| io::Error::other("idle inhibitor lock poisoned"))?
            .is_some())
    }

    fn set_idle_inhibited(&self, enabled: bool) -> io::Result<()> {
        let mut inhibitor = self
            .idle_inhibitor
            .lock()
            .map_err(|_| io::Error::other("idle inhibitor lock poisoned"))?;
        if enabled == inhibitor.is_some() {
            return Ok(());
        }
        if enabled {
            let connection = dbus::blocking::Connection::new_system().map_err(dbus_error)?;
            let proxy = connection.with_proxy(
                LOGIN1_DESTINATION,
                LOGIN1_MANAGER_PATH,
                LOGIND_ACTION_TIMEOUT,
            );
            let (fd,): (dbus::arg::OwnedFd,) = proxy
                .method_call(
                    LOGIN1_MANAGER,
                    "Inhibit",
                    ("idle", "Sleepy", "desktop idle inhibitor", "block"),
                )
                .map_err(dbus_error)?;
            *inhibitor = Some(fd);
        } else {
            inhibitor.take();
        }
        Ok(())
    }

    fn game_mode_snapshot(&self) -> io::Result<bool> {
        require_bus_name(false, GAME_MODE_DESTINATION)?;
        self.game_mode
            .lock()
            .map(|state| *state)
            .map_err(|_| io::Error::other("game mode lock poisoned"))
    }

    fn set_game_mode(&self, enabled: bool) -> io::Result<()> {
        let connection = dbus::blocking::Connection::new_session().map_err(dbus_error)?;
        let proxy =
            connection.with_proxy(GAME_MODE_DESTINATION, GAME_MODE_PATH, LOGIND_ACTION_TIMEOUT);
        execute_game_mode_with(enabled, |method| {
            let (status,): (i32,) = proxy
                .method_call(GAME_MODE_INTERFACE, method, (std::process::id() as i32,))
                .map_err(dbus_error)?;
            Ok(status)
        })?;
        *self
            .game_mode
            .lock()
            .map_err(|_| io::Error::other("game mode lock poisoned"))? = enabled;
        Ok(())
    }

    fn output_path(&self, prefix: &str, extension: &str) -> PathBuf {
        self.capture_root.join(format!(
            "{prefix}-{}.{}",
            uuid::Uuid::new_v4().hyphenated(),
            extension
        ))
    }
}

fn execute_game_mode_with(
    enabled: bool,
    transport: impl FnOnce(&str) -> io::Result<i32>,
) -> io::Result<()> {
    let method = if enabled {
        "RegisterGame"
    } else {
        "UnregisterGame"
    };
    if transport(method)? < 0 {
        return Err(io::Error::other("GameMode rejected the request"));
    }
    Ok(())
}

impl Drop for ProductionUtilityService {
    fn drop(&mut self) {
        if let Ok(runtime) = self.recording.get_mut() {
            if let Some(child) = runtime.child.as_mut() {
                let _ = child.kill();
                let _ = child.wait();
            }
        }
    }
}

pub struct UtilityProducer {
    domain: DesktopDomainId,
    service: Arc<ProductionUtilityService>,
}

impl UtilityProducer {
    pub fn new(
        domain: DesktopDomainId,
        service: Arc<ProductionUtilityService>,
    ) -> io::Result<Self> {
        if !matches!(
            domain,
            DesktopDomainId::Tray
                | DesktopDomainId::Clipboard
                | DesktopDomainId::Recording
                | DesktopDomainId::IdleInhibit
                | DesktopDomainId::GameMode
                | DesktopDomainId::Screenshot
                | DesktopDomainId::ColorPicker
        ) {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "utility producer was assigned a non-utility domain",
            ));
        }
        Ok(Self { domain, service })
    }

    async fn probe(&self) -> DesktopDomainState {
        let service = Arc::clone(&self.service);
        let domain = self.domain;
        tokio::task::spawn_blocking(move || service.state(domain))
            .await
            .unwrap_or_else(|_| {
                terminal(
                    domain,
                    CapabilityAvailability::Error,
                    "utility probe worker failed",
                )
            })
    }
}

#[async_trait]
impl DesktopProducer for UtilityProducer {
    fn domain(&self) -> DesktopDomainId {
        self.domain
    }

    async fn initial(&self) -> DesktopDomainState {
        self.probe().await
    }

    async fn run(
        &self,
        sender: mpsc::Sender<DesktopDomainUpdate>,
        cancellation: CancellationToken,
    ) -> Result<(), ProducerError> {
        let mut previous = None;
        let mut interval = tokio::time::interval(UTILITY_REFRESH);
        interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        interval.tick().await;
        loop {
            tokio::select! {
                biased;
                _ = cancellation.cancelled() => return Ok(()),
                _ = interval.tick() => {
                    let current = self.probe().await;
                    if previous.as_ref() != Some(&current) {
                        sender.send(DesktopDomainUpdate { state: current.clone() })
                            .await
                            .map_err(|_| ProducerError::new("desktop state authority stopped"))?;
                        previous = Some(current);
                    }
                }
            }
        }
    }
}

fn write_wayland_clipboard(contents: &[u8], deadline: Instant) -> io::Result<()> {
    if Instant::now() >= deadline {
        return Err(io::Error::new(
            io::ErrorKind::TimedOut,
            "clipboard write exceeded its total deadline",
        ));
    }
    let child = Command::new("wl-copy")
        .stdin(Stdio::piped())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()?;
    let mut child = ChildGuard::new(child);
    let mut stdin = child
        .child_mut()?
        .stdin
        .take()
        .ok_or_else(|| io::Error::other("wl-copy stdin missing"))?;
    set_nonblocking(stdin.as_raw_fd())?;
    let mut offset = 0_usize;
    while offset < contents.len() {
        if Instant::now() >= deadline {
            return Err(io::Error::new(
                io::ErrorKind::TimedOut,
                "clipboard write exceeded its total deadline",
            ));
        }
        match stdin.write(&contents[offset..]) {
            Ok(0) => {
                return Err(io::Error::new(
                    io::ErrorKind::WriteZero,
                    "clipboard child closed its input",
                ));
            }
            Ok(written) => offset += written,
            Err(error) if error.kind() == io::ErrorKind::WouldBlock => {
                if child.child_mut()?.try_wait()?.is_some() {
                    return Err(io::Error::new(
                        io::ErrorKind::BrokenPipe,
                        "clipboard child exited before accepting input",
                    ));
                }
                thread::sleep(Duration::from_millis(5));
            }
            Err(error) => return Err(error),
        }
    }
    drop(stdin);
    child.wait_until(deadline)
}

struct ChildGuard {
    child: Option<Child>,
}

impl ChildGuard {
    fn new(child: Child) -> Self {
        Self { child: Some(child) }
    }

    fn child_mut(&mut self) -> io::Result<&mut Child> {
        self.child
            .as_mut()
            .ok_or_else(|| io::Error::other("utility child was already reaped"))
    }

    fn disarm(mut self) -> io::Result<Child> {
        self.child
            .take()
            .ok_or_else(|| io::Error::other("utility child was already reaped"))
    }

    fn wait_until(mut self, deadline: Instant) -> io::Result<()> {
        loop {
            let child = self.child_mut()?;
            if let Some(status) = child.try_wait()? {
                self.child.take();
                return if status.success() {
                    Ok(())
                } else {
                    Err(io::Error::other("utility process failed"))
                };
            }
            if Instant::now() >= deadline {
                return Err(io::Error::new(
                    io::ErrorKind::TimedOut,
                    "utility process exceeded its total deadline",
                ));
            }
            thread::sleep(Duration::from_millis(5));
        }
    }
}

impl Drop for ChildGuard {
    fn drop(&mut self) {
        if let Some(mut child) = self.child.take() {
            child.stdin.take();
            let _ = child.kill();
            let _ = child.wait();
        }
    }
}

fn set_nonblocking(fd: std::os::fd::RawFd) -> io::Result<()> {
    let flags = unsafe { libc::fcntl(fd, libc::F_GETFL) };
    if flags < 0 {
        return Err(io::Error::last_os_error());
    }
    if unsafe { libc::fcntl(fd, libc::F_SETFL, flags | libc::O_NONBLOCK) } < 0 {
        return Err(io::Error::last_os_error());
    }
    Ok(())
}

fn fixed_child(spec: &CommandSpec) -> io::Result<Command> {
    let mut command = Command::new(&spec.program);
    command
        .args(&spec.args)
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null());
    for (key, value) in &spec.env {
        command.env(key, value);
    }
    Ok(command)
}

fn wait_child(child: &mut Child, timeout: Duration) -> io::Result<()> {
    let deadline = Instant::now() + timeout;
    loop {
        if let Some(status) = child.try_wait()? {
            return if status.success() {
                Ok(())
            } else {
                Err(io::Error::other("utility process failed"))
            };
        }
        if Instant::now() >= deadline {
            let _ = child.kill();
            let _ = child.wait();
            return Err(io::Error::new(
                io::ErrorKind::TimedOut,
                "utility process exceeded its deadline",
            ));
        }
        thread::sleep(Duration::from_millis(5));
    }
}

fn terminate_child(child: &mut Child, signal: i32, timeout: Duration) -> io::Result<()> {
    if unsafe { libc::kill(child.id() as libc::pid_t, signal) } != 0 {
        return Err(io::Error::last_os_error());
    }
    wait_child(child, timeout)
}

fn ensure_executable(program: &str) -> io::Result<()> {
    let path = std::env::var_os("PATH")
        .ok_or_else(|| io::Error::new(io::ErrorKind::NotFound, "PATH is unavailable"))?;
    for directory in std::env::split_paths(&path) {
        let candidate = directory.join(program);
        if let Ok(metadata) = fs::metadata(candidate) {
            if metadata.is_file() && metadata.permissions().mode() & 0o111 != 0 {
                return Ok(());
            }
        }
    }
    Err(io::Error::new(
        io::ErrorKind::NotFound,
        "utility executable is unavailable",
    ))
}

fn require_bus_name(system: bool, name: &str) -> io::Result<()> {
    let connection = if system {
        dbus::blocking::Connection::new_system()
    } else {
        dbus::blocking::Connection::new_session()
    }
    .map_err(dbus_error)?;
    let proxy = connection.with_proxy(
        "org.freedesktop.DBus",
        "/org/freedesktop/DBus",
        Duration::from_secs(2),
    );
    let (owned,): (bool,) = proxy
        .method_call("org.freedesktop.DBus", "NameHasOwner", (name,))
        .map_err(dbus_error)?;
    if owned {
        Ok(())
    } else {
        Err(io::Error::new(
            io::ErrorKind::NotFound,
            "required D-Bus service is unavailable",
        ))
    }
}

fn path_to_string(path: &Path) -> io::Result<&str> {
    path.to_str()
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "output path is not UTF-8"))
}

fn terminal(
    domain: DesktopDomainId,
    status: CapabilityAvailability,
    diagnostic: impl Into<String>,
) -> DesktopDomainState {
    DesktopDomainState::terminal(domain, status, diagnostic).unwrap_or_else(|_| {
        DesktopDomainState::terminal(domain, status, "utility provider failed")
            .expect("static terminal state")
    })
}

fn availability_for_io(error: &io::Error) -> CapabilityAvailability {
    match error.kind() {
        io::ErrorKind::NotFound | io::ErrorKind::NotConnected => {
            CapabilityAvailability::Unavailable
        }
        io::ErrorKind::PermissionDenied => CapabilityAvailability::PermissionDenied,
        io::ErrorKind::TimedOut => CapabilityAvailability::Timeout,
        io::ErrorKind::InvalidData | io::ErrorKind::InvalidInput => CapabilityAvailability::Parse,
        io::ErrorKind::Unsupported => CapabilityAvailability::Unsupported,
        _ => CapabilityAvailability::Error,
    }
}

#[derive(Debug, Clone, Copy, Default)]
pub struct ProductionLogind;

impl ProductionLogind {
    pub fn state(self) -> io::Result<DesktopDomainState> {
        let deadline = Instant::now() + LOGIND_STATE_TIMEOUT;
        let connection = dbus::blocking::Connection::new_system().map_err(dbus_error)?;
        let manager = connection.with_proxy(
            LOGIN1_DESTINATION,
            LOGIN1_MANAGER_PATH,
            remaining(deadline)?,
        );
        let session_id = required_session_id()?;
        let (session_path,): (dbus::Path<'static>,) = manager
            .method_call(LOGIN1_MANAGER, "GetSession", (session_id,))
            .map_err(dbus_error)?;
        let session = connection.with_proxy(LOGIN1_DESTINATION, session_path, remaining(deadline)?);
        let locked: bool = session
            .get(LOGIN1_SESSION, "LockedHint")
            .map_err(dbus_error)?;
        DesktopDomainState::available(
            DesktopDomainId::Lock,
            DesktopDomainValue::Lock(LockState { secure: locked }),
        )
    }

    pub fn execute(self, command: DesktopSessionCommand) -> io::Result<Option<DesktopDomainState>> {
        let deadline = Instant::now() + LOGIND_ACTION_TIMEOUT;
        let connection = dbus::blocking::Connection::new_system().map_err(dbus_error)?;
        execute_logind_with(
            command,
            || session_locked_hint(&connection, deadline),
            |command| invoke_logind_action(&connection, command, deadline),
        )
    }
}

fn execute_logind_with(
    command: DesktopSessionCommand,
    mut locked_hint: impl FnMut() -> io::Result<bool>,
    mut invoke: impl FnMut(DesktopSessionCommand) -> io::Result<()>,
) -> io::Result<Option<DesktopDomainState>> {
    match command {
        DesktopSessionCommand::Lock => {
            invoke(command)?;
            if !locked_hint()? {
                return Err(io::Error::other(
                    "logind readback did not confirm the session lock",
                ));
            }
            DesktopDomainState::available(
                DesktopDomainId::Lock,
                DesktopDomainValue::Lock(LockState { secure: true }),
            )
            .map(Some)
        }
        DesktopSessionCommand::Suspend => {
            validate_session_precondition(command, locked_hint()?)?;
            invoke(command)?;
            Ok(None)
        }
        DesktopSessionCommand::Logout
        | DesktopSessionCommand::Reboot
        | DesktopSessionCommand::PowerOff => {
            invoke(command)?;
            Ok(None)
        }
    }
}

fn invoke_logind_action(
    connection: &dbus::blocking::Connection,
    command: DesktopSessionCommand,
    deadline: Instant,
) -> io::Result<()> {
    let manager = connection.with_proxy(
        LOGIN1_DESTINATION,
        LOGIN1_MANAGER_PATH,
        remaining(deadline)?,
    );
    match command {
        DesktopSessionCommand::Lock => {
            let _: () = manager
                .method_call(LOGIN1_MANAGER, "LockSession", (required_session_id()?,))
                .map_err(dbus_error)?;
        }
        DesktopSessionCommand::Logout => {
            let _: () = manager
                .method_call(
                    LOGIN1_MANAGER,
                    "TerminateSession",
                    (required_session_id()?,),
                )
                .map_err(dbus_error)?;
        }
        DesktopSessionCommand::Suspend => {
            let _: () = manager
                .method_call(LOGIN1_MANAGER, "Suspend", (true,))
                .map_err(dbus_error)?;
        }
        DesktopSessionCommand::Reboot => {
            let _: () = manager
                .method_call(LOGIN1_MANAGER, "Reboot", (true,))
                .map_err(dbus_error)?;
        }
        DesktopSessionCommand::PowerOff => {
            let _: () = manager
                .method_call(LOGIN1_MANAGER, "PowerOff", (true,))
                .map_err(dbus_error)?;
        }
    }
    Ok(())
}

pub struct LogindProducer;

impl LogindProducer {
    async fn probe(&self) -> DesktopDomainState {
        match tokio::task::spawn_blocking(|| ProductionLogind.state()).await {
            Ok(Ok(state)) => state,
            Ok(Err(error)) => terminal(
                DesktopDomainId::Lock,
                availability_for_io(&error),
                error.to_string(),
            ),
            Err(_) => terminal(
                DesktopDomainId::Lock,
                CapabilityAvailability::Error,
                "logind state worker failed",
            ),
        }
    }
}

#[async_trait]
impl DesktopProducer for LogindProducer {
    fn domain(&self) -> DesktopDomainId {
        DesktopDomainId::Lock
    }

    async fn initial(&self) -> DesktopDomainState {
        self.probe().await
    }

    async fn run(
        &self,
        sender: mpsc::Sender<DesktopDomainUpdate>,
        cancellation: CancellationToken,
    ) -> Result<(), ProducerError> {
        let mut previous = None;
        let mut interval = tokio::time::interval(Duration::from_secs(2));
        interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        interval.tick().await;
        loop {
            tokio::select! {
                biased;
                _ = cancellation.cancelled() => return Ok(()),
                _ = interval.tick() => {
                    let current = self.probe().await;
                    if previous.as_ref() != Some(&current) {
                        sender.send(DesktopDomainUpdate { state: current.clone() })
                            .await
                            .map_err(|_| ProducerError::new("desktop state authority stopped"))?;
                        previous = Some(current);
                    }
                }
            }
        }
    }
}

fn session_locked_hint(
    connection: &dbus::blocking::Connection,
    deadline: Instant,
) -> io::Result<bool> {
    let manager = connection.with_proxy(
        LOGIN1_DESTINATION,
        LOGIN1_MANAGER_PATH,
        remaining(deadline)?,
    );
    let session_id = required_session_id()?;
    let (session_path,): (dbus::Path<'static>,) = manager
        .method_call(LOGIN1_MANAGER, "GetSession", (session_id,))
        .map_err(dbus_error)?;
    let session = connection.with_proxy(LOGIN1_DESTINATION, session_path, remaining(deadline)?);
    let locked: bool = session
        .get(LOGIN1_SESSION, "LockedHint")
        .map_err(dbus_error)?;
    Ok(locked)
}

pub fn validate_session_precondition(
    command: DesktopSessionCommand,
    secure_lock: bool,
) -> io::Result<()> {
    if command == DesktopSessionCommand::Suspend && !secure_lock {
        return Err(io::Error::new(
            io::ErrorKind::PermissionDenied,
            "session.suspend-requires-secure-lock",
        ));
    }
    Ok(())
}

fn required_session_id() -> io::Result<String> {
    std::env::var("XDG_SESSION_ID")
        .ok()
        .filter(|value| !value.is_empty() && !value.contains('\0'))
        .ok_or_else(|| io::Error::new(io::ErrorKind::NotFound, "XDG session ID is unavailable"))
}

fn remaining(deadline: Instant) -> io::Result<Duration> {
    let remaining = deadline.saturating_duration_since(Instant::now());
    if remaining.is_zero() {
        Err(io::Error::new(
            io::ErrorKind::TimedOut,
            "logind operation exceeded its total deadline",
        ))
    } else {
        Ok(remaining)
    }
}

fn dbus_error(error: dbus::Error) -> io::Error {
    let kind = match error.name() {
        Some("org.freedesktop.DBus.Error.AccessDenied") => io::ErrorKind::PermissionDenied,
        Some("org.freedesktop.DBus.Error.NoReply") => io::ErrorKind::TimedOut,
        _ => io::ErrorKind::Other,
    };
    io::Error::new(kind, error.to_string())
}

#[cfg(test)]
mod tests {
    use std::{cell::RefCell, sync::Mutex};

    use sleepy_sdk::StableId;

    use super::*;
    use crate::system::{CommandOutput, CommandRunner, RunnerError};

    #[derive(Clone, Default)]
    struct FixtureRunner {
        seen: Arc<Mutex<Vec<CommandSpec>>>,
    }

    impl CommandRunner for FixtureRunner {
        fn run(&self, command: &CommandSpec) -> Result<CommandOutput, RunnerError> {
            self.seen.lock().unwrap().push(command.clone());
            Ok(CommandOutput {
                status: 0,
                stdout: Vec::new(),
                stderr: Vec::new(),
            })
        }
    }

    #[test]
    fn injected_capture_helper_executes_only_the_fixed_authorized_contract() {
        let runner = FixtureRunner::default();
        execute_capture_with(
            &runner,
            &UtilityCommand::Screenshot {
                output_id: StableId("output:DP-1".into()),
            },
            "/run/user/1000/sleepy/captures/screenshot.png",
            "00000000-0000-4000-8000-000000000071",
        )
        .unwrap();
        let seen = runner.seen.lock().unwrap();
        assert_eq!(seen.len(), 1);
        assert_eq!(seen[0].program, "sleepy-capture-helper");
        assert_eq!(seen[0].args[0], "screenshot");
        assert!(seen[0]
            .args
            .windows(2)
            .any(|pair| { pair == ["--gesture-token", "00000000-0000-4000-8000-000000000071",] }));
    }

    #[test]
    fn injected_game_mode_transport_checks_method_and_backend_status() {
        let mut invoked = None;
        execute_game_mode_with(true, |method| {
            invoked = Some(method.to_owned());
            Ok(0)
        })
        .unwrap();
        assert_eq!(invoked.as_deref(), Some("RegisterGame"));

        let error = execute_game_mode_with(false, |method| {
            assert_eq!(method, "UnregisterGame");
            Ok(-1)
        })
        .unwrap_err();
        assert_eq!(error.kind(), io::ErrorKind::Other);
    }

    #[test]
    fn injected_logind_policy_confirms_lock_and_gates_suspend_before_action() {
        let steps = RefCell::new(Vec::new());
        let locked = execute_logind_with(
            DesktopSessionCommand::Lock,
            || {
                steps.borrow_mut().push("locked-hint");
                Ok(true)
            },
            |command| {
                steps.borrow_mut().push(match command {
                    DesktopSessionCommand::Lock => "lock",
                    _ => "unexpected",
                });
                Ok(())
            },
        )
        .unwrap()
        .unwrap();
        assert_eq!(steps.into_inner(), ["lock", "locked-hint"]);
        assert_eq!(locked.status(), CapabilityAvailability::Available);

        let actions = RefCell::new(Vec::new());
        let error = execute_logind_with(
            DesktopSessionCommand::Suspend,
            || Ok(false),
            |command| {
                actions.borrow_mut().push(command);
                Ok(())
            },
        )
        .unwrap_err();
        assert_eq!(error.kind(), io::ErrorKind::PermissionDenied);
        assert!(actions.into_inner().is_empty());

        let actions = RefCell::new(Vec::new());
        assert!(execute_logind_with(
            DesktopSessionCommand::Reboot,
            || panic!("reboot does not query lock state"),
            |command| {
                actions.borrow_mut().push(command);
                Ok(())
            },
        )
        .unwrap()
        .is_none());
        assert_eq!(actions.into_inner(), [DesktopSessionCommand::Reboot]);
    }
}
