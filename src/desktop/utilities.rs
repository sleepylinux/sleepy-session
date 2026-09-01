// SPDX-License-Identifier: GPL-3.0-only

use std::{
    fs,
    io::{self, BufRead, BufReader, Write},
    os::fd::AsRawFd,
    os::unix::{
        ffi::OsStrExt,
        fs::{FileTypeExt, MetadataExt, PermissionsExt},
        net::UnixStream as StdUnixStream,
    },
    path::{Path, PathBuf},
    process::{Child, ChildStdout, Command, Stdio},
    sync::{mpsc as std_mpsc, Arc, Mutex as StdMutex},
    thread,
    time::{Duration, Instant},
};

use async_trait::async_trait;
use dbus::message::MatchRule;
use sleepy_sdk::{
    CapabilityAvailability, ClipboardEntry, DesktopSessionCommand, LockState, RecordingState,
    RecordingStatus, UtilityCommand,
};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    sync::mpsc,
};
use zeroize::Zeroize;

use super::{
    DesktopDomainId, DesktopDomainState, DesktopDomainUpdate, DesktopDomainValue, DesktopProducer,
    DesktopProducerContext, ProducerError,
};
use crate::system::{CommandSpec, ProcessCommandRunner, RunControl};

const LOGIN1_DESTINATION: &str = "org.freedesktop.login1";
const LOGIN1_MANAGER_PATH: &str = "/org/freedesktop/login1";
const LOGIN1_MANAGER: &str = "org.freedesktop.login1.Manager";
const LOGIND_ACTION_TIMEOUT: Duration = Duration::from_secs(30);
const LOGIND_STATE_TIMEOUT: Duration = Duration::from_millis(1_750);
const LOCKER_ACK_TIMEOUT: Duration = Duration::from_secs(5);
const UTILITY_REFRESH: Duration = Duration::from_secs(2);
const RECORDING_ACK_TIMEOUT: Duration = Duration::from_secs(5);
const GAME_MODE_DESTINATION: &str = "com.feralinteractive.GameMode";
const GAME_MODE_PATH: &str = "/com/feralinteractive/GameMode";
const GAME_MODE_INTERFACE: &str = "com.feralinteractive.GameMode";

pub fn action_spec(command: &UtilityCommand, output_path: &str) -> io::Result<Option<CommandSpec>> {
    let spec = match command {
        UtilityCommand::Screenshot { output_id } => {
            validate_output_path(output_path)?;
            CommandSpec::new(
                "sleepy-capture-helper",
                [
                    "screenshot",
                    "--interactive-consent",
                    "--output-id",
                    output_name(output_id)?,
                    "--output-path",
                    output_path,
                ],
            )
        }
        UtilityCommand::PickColor => CommandSpec::new(
            "sleepy-capture-helper",
            ["pick-color", "--interactive-consent", "--clipboard"],
        ),
        UtilityCommand::StartRecording { output_id, audio } => {
            validate_output_path(output_path)?;
            let mut args = vec![
                "record".to_owned(),
                "--interactive-consent".to_owned(),
                "--output-id".to_owned(),
                output_name(output_id)?.to_owned(),
                "--output-path".to_owned(),
                output_path.to_owned(),
            ];
            if *audio {
                args.push("--audio".to_owned());
            }
            args.extend(["--status-fd".to_owned(), "1".to_owned()]);
            CommandSpec::new("sleepy-capture-helper", args)
        }
        UtilityCommand::InvokeTrayMenu { .. }
        | UtilityCommand::PasteClipboard { .. }
        | UtilityCommand::ClearClipboard
        | UtilityCommand::SetIdleInhibited { .. }
        | UtilityCommand::PauseRecording
        | UtilityCommand::StopRecording
        | UtilityCommand::DeleteRecording { .. }
        | UtilityCommand::SetGameMode { .. } => return Ok(None),
    };
    Ok(Some(spec))
}

fn execute_capture_with<R: crate::system::CommandRunner>(
    runner: &R,
    command: &UtilityCommand,
    output_path: &str,
) -> io::Result<()> {
    let spec = action_spec(command, output_path)?
        .ok_or_else(|| io::Error::new(io::ErrorKind::Unsupported, "capture command required"))?;
    super::network::run(runner, spec).map(|_| ())
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
    status: Option<BufReader<ChildStdout>>,
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
            status: None,
        }
    }
}

pub struct ProductionUtilityService {
    runner: ProcessCommandRunner,
    capture_root: PathBuf,
    capture_directory: crate::store::SecureDir,
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
            capture_directory,
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

    fn state_for_poll(
        &self,
        domain: DesktopDomainId,
        control: &RunControl,
    ) -> io::Result<DesktopDomainState> {
        let result = match domain {
            DesktopDomainId::Tray => self
                .tray
                .probe_controlled(control)
                .map(DesktopDomainValue::Tray),
            DesktopDomainId::Clipboard => self
                .clipboard_snapshot_controlled(control)
                .map(DesktopDomainValue::Clipboard),
            DesktopDomainId::Recording => self
                .recording_snapshot_for_poll()
                .map(DesktopDomainValue::Recording),
            DesktopDomainId::IdleInhibit => self
                .idle_snapshot_for_poll(control)
                .map(DesktopDomainValue::IdleInhibit),
            DesktopDomainId::GameMode => self
                .game_mode_snapshot_controlled(control)
                .map(DesktopDomainValue::GameMode),
            DesktopDomainId::Screenshot | DesktopDomainId::ColorPicker => {
                ensure_run_active(control, "utility polling")?;
                let available = ensure_executable("sleepy-capture-helper");
                ensure_run_active(control, "utility polling")?;
                available.map(|()| {
                    if domain == DesktopDomainId::Screenshot {
                        DesktopDomainValue::Screenshot
                    } else {
                        DesktopDomainValue::ColorPicker
                    }
                })
            }
            _ => {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "utility service was assigned a non-utility domain",
                ))
            }
        };
        match result {
            Ok(value) => Ok(
                DesktopDomainState::available(domain, value).unwrap_or_else(|error| {
                    terminal(domain, CapabilityAvailability::Parse, error.to_string())
                }),
            ),
            Err(error) if error.kind() == io::ErrorKind::WouldBlock => Err(error),
            Err(error) => Ok(terminal(
                domain,
                availability_for_io(&error),
                error.to_string(),
            )),
        }
    }

    pub fn execute(&self, command: &UtilityCommand) -> io::Result<DesktopDomainState> {
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
            UtilityCommand::StartRecording { output_id, audio } => {
                self.start_recording(output_id, *audio)?;
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
            UtilityCommand::DeleteRecording { recording_id } => {
                self.delete_recording(recording_id)?;
                Ok(self.state(DesktopDomainId::Recording))
            }
            UtilityCommand::Screenshot { .. } => {
                let path = self.output_path("screenshot", "png");
                let path = path_to_string(&path)?;
                execute_capture_with(&self.runner, command, path)?;
                Ok(self.state(DesktopDomainId::Screenshot))
            }
            UtilityCommand::PickColor => {
                execute_capture_with(&self.runner, command, "unused")?;
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

    fn clipboard_snapshot_controlled(
        &self,
        control: &RunControl,
    ) -> io::Result<Vec<ClipboardEntry>> {
        let list =
            super::network::run_controlled(&self.runner, super::clipboard::list_spec(), control)?;
        self.clipboard_entries(&list)
    }

    fn clipboard_snapshot_with<R: crate::system::CommandRunner>(
        &self,
        runner: &R,
    ) -> io::Result<Vec<ClipboardEntry>> {
        let list = super::network::run(runner, super::clipboard::list_spec())?;
        self.clipboard_entries(&list)
    }

    fn clipboard_entries(&self, list: &[u8]) -> io::Result<Vec<ClipboardEntry>> {
        let rows = super::clipboard::parse_list(list)?;
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

    fn recording_snapshot_for_poll(&self) -> io::Result<RecordingState> {
        let mut runtime = self.recording.try_lock().map_err(|error| match error {
            std::sync::TryLockError::Poisoned(_) => {
                io::Error::other("recording state lock poisoned")
            }
            std::sync::TryLockError::WouldBlock => {
                io::Error::new(io::ErrorKind::WouldBlock, "recording state is busy")
            }
        })?;
        ensure_executable("sleepy-capture-helper")?;
        if let Some(child) = runtime.child.as_mut() {
            if child.try_wait()?.is_some() {
                *runtime = RecordingRuntime::default();
            }
        }
        Ok(runtime.state.clone())
    }

    fn start_recording(&self, output_id: &sleepy_sdk::StableId, audio: bool) -> io::Result<()> {
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
        let now: chrono::DateTime<chrono::Utc> = std::time::SystemTime::now().into();
        let recording_id = format!(
            "recording_{}_{}.mp4",
            now.format("%Y%m%d_%H-%M-%S"),
            uuid::Uuid::new_v4().simple()
        );
        let path = self.capture_root.join(&recording_id);
        let path_text = path_to_string(&path)?;
        let spec = action_spec(
            &UtilityCommand::StartRecording {
                output_id: output_id.clone(),
                audio,
            },
            path_text,
        )?
        .ok_or_else(|| io::Error::other("recording command contract missing"))?;
        let mut command = fixed_child(&spec)?;
        command.stdout(Stdio::piped());
        let mut child = ChildGuard::new(command.spawn()?);
        let stdout = child
            .child_mut()?
            .stdout
            .take()
            .ok_or_else(|| io::Error::other("recording helper status stream missing"))?;
        set_nonblocking(stdout.as_raw_fd())?;
        let mut status = BufReader::new(stdout);
        wait_for_recording_ack(
            &mut status,
            child.child_mut()?,
            RecordingStatus::Recording,
            Instant::now() + RECORDING_ACK_TIMEOUT,
        )?;
        runtime.state = RecordingState {
            status: RecordingStatus::Recording,
            recording_id: Some(recording_id),
            output_id: Some(output_id.as_str().to_owned()),
        };
        runtime.child = Some(child.disarm()?);
        runtime.status = Some(status);
        Ok(())
    }

    fn delete_recording(&self, recording_id: &sleepy_sdk::StableId) -> io::Result<()> {
        let name = recording_id.as_str();
        if !name.starts_with("recording_")
            || !name.ends_with(".mp4")
            || name.len() > 96
            || !name
                .bytes()
                .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'_' | b'-' | b'.'))
        {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "invalid recording basename",
            ));
        }
        let metadata = self
            .capture_directory
            .entry_metadata(std::ffi::OsStr::new(name))
            .map_err(|error| io::Error::other(error.to_string()))?
            .ok_or_else(|| io::Error::new(io::ErrorKind::NotFound, "recording not found"))?;
        if metadata.mode & libc::S_IFMT != libc::S_IFREG
            || metadata.uid != unsafe { libc::geteuid() }
        {
            return Err(io::Error::new(
                io::ErrorKind::PermissionDenied,
                "recording must be a regular file owned by the session user",
            ));
        }
        self.capture_directory
            .remove_file(std::ffi::OsStr::new(name))
            .map_err(|error| io::Error::other(error.to_string()))
    }

    fn pause_recording(&self) -> io::Result<()> {
        let mut runtime = self
            .recording
            .lock()
            .map_err(|_| io::Error::other("recording state lock poisoned"))?;
        let desired = match runtime.state.status {
            RecordingStatus::Recording => RecordingStatus::Paused,
            RecordingStatus::Paused => RecordingStatus::Recording,
            RecordingStatus::Inactive => return Err(io::Error::other("recording state mismatch")),
        };
        let RecordingRuntime { child, status, .. } = &mut *runtime;
        let child = child
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
        let status = status
            .as_mut()
            .ok_or_else(|| io::Error::other("recording helper status stream missing"))?;
        let acknowledgement = wait_for_recording_ack(
            status,
            child,
            desired,
            Instant::now() + RECORDING_ACK_TIMEOUT,
        );
        if let Err(error) = acknowledgement {
            invalidate_recording_runtime(&mut runtime);
            return Err(error);
        }
        runtime.state.status = desired;
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
        runtime.status.take();
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

    fn idle_snapshot_for_poll(&self, control: &RunControl) -> io::Result<bool> {
        let inhibited = self
            .idle_inhibitor
            .try_lock()
            .map_err(|error| match error {
                std::sync::TryLockError::Poisoned(_) => {
                    io::Error::other("idle inhibitor lock poisoned")
                }
                std::sync::TryLockError::WouldBlock => {
                    io::Error::new(io::ErrorKind::WouldBlock, "idle inhibitor state is busy")
                }
            })?
            .is_some();
        require_bus_name_controlled(true, LOGIN1_DESTINATION, control)?;
        Ok(inhibited)
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

    fn game_mode_snapshot_controlled(&self, control: &RunControl) -> io::Result<bool> {
        require_bus_name_controlled(false, GAME_MODE_DESTINATION, control)?;
        ensure_run_active(control, "game-mode polling")?;
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

fn wait_for_recording_ack(
    status: &mut BufReader<ChildStdout>,
    child: &mut Child,
    expected: RecordingStatus,
    deadline: Instant,
) -> io::Result<()> {
    let expected = match expected {
        RecordingStatus::Recording => "STATE recording",
        RecordingStatus::Paused => "STATE paused",
        RecordingStatus::Inactive => "STATE inactive",
    };
    let mut line = String::new();
    loop {
        if Instant::now() >= deadline {
            return Err(io::Error::new(
                io::ErrorKind::TimedOut,
                "recording helper acknowledgement timed out",
            ));
        }
        line.clear();
        match status.read_line(&mut line) {
            Ok(0) => {
                if child.try_wait()?.is_some() {
                    return Err(io::Error::new(
                        io::ErrorKind::BrokenPipe,
                        "recording helper exited before acknowledgement",
                    ));
                }
            }
            Ok(_) => {
                if line.len() > 64 || line.trim_end() != expected {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        "recording helper returned an invalid acknowledgement",
                    ));
                }
                return Ok(());
            }
            Err(error) if error.kind() == io::ErrorKind::WouldBlock => {}
            Err(error) => return Err(error),
        }
        thread::sleep(Duration::from_millis(5));
    }
}

fn invalidate_recording_runtime(runtime: &mut RecordingRuntime) {
    runtime.status.take();
    if let Some(mut child) = runtime.child.take() {
        let _ = child.kill();
        let _ = child.wait();
    }
    runtime.state = RecordingRuntime::default().state;
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

    async fn initial_probe(&self) -> DesktopDomainState {
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

    async fn polling_probe(&self, context: &DesktopProducerContext) -> Option<DesktopDomainState> {
        let service = Arc::clone(&self.service);
        let domain = self.domain;
        let result = context
            .spawn_blocking(Instant::now() + Duration::from_secs(2), move |control| {
                if control.is_cancelled() {
                    return Err(io::Error::new(
                        io::ErrorKind::Interrupted,
                        "utility polling was cancelled",
                    ));
                }
                let state = service.state_for_poll(domain, &control)?;
                if control.is_cancelled() {
                    return Err(io::Error::new(
                        io::ErrorKind::Interrupted,
                        "utility polling was cancelled",
                    ));
                }
                Ok(state)
            })
            .await;
        match result {
            Ok(Ok(state)) => Some(state),
            Ok(Err(error))
                if error.kind() == io::ErrorKind::WouldBlock || context.is_cancelled() =>
            {
                None
            }
            Ok(Err(error)) => Some(terminal(
                domain,
                availability_for_io(&error),
                error.to_string(),
            )),
            Err(_) if context.is_cancelled() => None,
            Err(_) => Some(terminal(
                domain,
                CapabilityAvailability::Error,
                "utility probe worker failed",
            )),
        }
    }
}

#[async_trait]
impl DesktopProducer for UtilityProducer {
    fn domain(&self) -> DesktopDomainId {
        self.domain
    }

    async fn initial(&self) -> DesktopDomainState {
        self.initial_probe().await
    }

    async fn run(
        &self,
        sender: mpsc::Sender<DesktopDomainUpdate>,
        context: DesktopProducerContext,
    ) -> Result<(), ProducerError> {
        let mut previous = None;
        let mut interval = tokio::time::interval(UTILITY_REFRESH);
        interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        interval.tick().await;
        loop {
            tokio::select! {
                biased;
                _ = context.cancelled() => return Ok(()),
                _ = interval.tick() => {
                    let observation = context.begin_observation();
                    let Some(current) = self.polling_probe(&context).await else {
                        continue;
                    };
                    if previous.as_ref() != Some(&current) {
                        let update = observation
                            .finish(current.clone())
                            .map_err(|error| ProducerError::new(error.to_string()))?;
                        sender.send(update)
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
    require_bus_name_until(system, name, Duration::from_secs(2))
}

fn require_bus_name_controlled(system: bool, name: &str, control: &RunControl) -> io::Result<()> {
    ensure_run_active(control, "D-Bus availability probe")?;
    let timeout = control.remaining().min(Duration::from_secs(2));
    if timeout.is_zero() {
        return Err(io::Error::new(
            io::ErrorKind::TimedOut,
            "D-Bus availability probe exceeded its deadline",
        ));
    }
    require_bus_name_until(system, name, timeout)?;
    ensure_run_active(control, "D-Bus availability probe")
}

fn require_bus_name_until(system: bool, name: &str, timeout: Duration) -> io::Result<()> {
    let connection = if system {
        dbus::blocking::Connection::new_system()
    } else {
        dbus::blocking::Connection::new_session()
    }
    .map_err(dbus_error)?;
    let proxy = connection.with_proxy("org.freedesktop.DBus", "/org/freedesktop/DBus", timeout);
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

fn ensure_run_active(control: &RunControl, operation: &str) -> io::Result<()> {
    if control.is_cancelled() {
        Err(io::Error::new(
            io::ErrorKind::Interrupted,
            format!("{operation} was cancelled"),
        ))
    } else if control.remaining().is_zero() {
        Err(io::Error::new(
            io::ErrorKind::TimedOut,
            format!("{operation} exceeded its deadline"),
        ))
    } else {
        Ok(())
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

fn request_secure_lock() -> io::Result<()> {
    let (runtime, socket) = locker_paths()?;
    request_secure_lock_at(&socket, &runtime, LOCKER_ACK_TIMEOUT)
}

fn locker_paths() -> io::Result<(PathBuf, PathBuf)> {
    let runtime = std::env::var_os("XDG_RUNTIME_DIR")
        .filter(|value| !value.is_empty())
        .map(PathBuf::from)
        .ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::NotFound,
                "XDG runtime directory is unavailable",
            )
        })?;
    let socket = std::env::var_os("SLEEPY_LOCKER_SOCKET")
        .filter(|value| !value.is_empty())
        .map(PathBuf::from)
        .unwrap_or_else(|| runtime.join("sleepy/locker.sock"));
    Ok((runtime, socket))
}

fn request_secure_lock_at(socket: &Path, runtime: &Path, timeout: Duration) -> io::Result<()> {
    let reply = request_locker_reply_at(socket, runtime, b"lock\n", 8, timeout)?;
    if reply != b"locked\n" {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "locker returned an invalid secure-lock acknowledgement",
        ));
    }
    Ok(())
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum LockerStatus {
    Locked,
    Unlocked,
}

fn request_locker_status(timeout: Duration) -> io::Result<LockerStatus> {
    let (runtime, socket) = locker_paths()?;
    request_locker_status_at(&socket, &runtime, timeout)
}

fn request_locker_status_at(
    socket: &Path,
    runtime: &Path,
    timeout: Duration,
) -> io::Result<LockerStatus> {
    match request_locker_reply_at(socket, runtime, b"status\n", 10, timeout)?.as_slice() {
        b"locked\n" => Ok(LockerStatus::Locked),
        b"unlocked\n" => Ok(LockerStatus::Unlocked),
        _ => Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "locker returned an invalid status acknowledgement",
        )),
    }
}

async fn connect_verified_locker(socket: &Path) -> io::Result<tokio::net::UnixStream> {
    let stream = tokio::net::UnixStream::connect(socket).await?;
    let peer_uid = crate::sessiond::private_socket::peer_uid(&stream)?;
    if peer_uid != unsafe { libc::getuid() } {
        return Err(io::Error::new(
            io::ErrorKind::PermissionDenied,
            "locker socket peer UID mismatch",
        ));
    }
    Ok(stream)
}

fn request_locker_reply_at(
    socket: &Path,
    runtime: &Path,
    request: &'static [u8],
    reply_limit: u64,
    timeout: Duration,
) -> io::Result<Vec<u8>> {
    validate_locker_path(socket, runtime)?;
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_io()
        .enable_time()
        .build()
        .map_err(io::Error::other)?;
    runtime.block_on(async move {
        tokio::time::timeout(timeout, async {
            let mut stream = connect_verified_locker(socket).await?;
            stream.write_all(request).await?;
            stream.flush().await?;
            let mut reply = Vec::with_capacity(reply_limit as usize);
            stream.take(reply_limit).read_to_end(&mut reply).await?;
            Ok(reply)
        })
        .await
        .map_err(|_| {
            io::Error::new(
                io::ErrorKind::TimedOut,
                "locker did not reply within its deadline",
            )
        })?
    })
}

struct LockerSuspendHold {
    stream: Option<StdUnixStream>,
}

impl LockerSuspendHold {
    fn release(mut self) {
        self.stream.take();
    }

    fn fail_closed(mut self) {
        if let Some(stream) = self.stream.take() {
            // Closing this connection would release the locker's suspend hold while the sleep
            // state is unknown. Retain it until process teardown, where the kernel closes it.
            std::mem::forget(stream);
        }
    }
}

fn acquire_suspend_hold() -> io::Result<LockerSuspendHold> {
    let (runtime, socket) = locker_paths()?;
    acquire_suspend_hold_at(&socket, &runtime, LOCKER_ACK_TIMEOUT)
}

fn acquire_suspend_hold_at(
    socket: &Path,
    runtime: &Path,
    timeout: Duration,
) -> io::Result<LockerSuspendHold> {
    validate_locker_path(socket, runtime)?;
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_io()
        .enable_time()
        .build()
        .map_err(io::Error::other)?;
    runtime.block_on(async move {
        tokio::time::timeout(timeout, async {
            let mut stream = connect_verified_locker(socket).await?;
            stream.write_all(b"suspend\n").await?;
            stream.flush().await?;
            let mut reply = Vec::with_capacity(8);
            (&mut stream).take(8).read_to_end(&mut reply).await?;
            if reply != b"locked\n" {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "locker returned an invalid suspend acknowledgement",
                ));
            }
            let mut descriptor = libc::pollfd {
                fd: stream.as_raw_fd(),
                events: 0,
                revents: 0,
            };
            if unsafe { libc::poll(&mut descriptor, 1, 0) } == -1 {
                return Err(io::Error::last_os_error());
            }
            if descriptor.revents & libc::POLLHUP != 0 {
                return Err(io::Error::new(
                    io::ErrorKind::UnexpectedEof,
                    "locker closed the suspend hold after acknowledgement",
                ));
            }
            Ok(LockerSuspendHold {
                stream: Some(stream.into_std()?),
            })
        })
        .await
        .map_err(|_| {
            io::Error::new(
                io::ErrorKind::TimedOut,
                "locker did not establish a suspend hold within its deadline",
            )
        })?
    })
}

fn validate_locker_path(socket: &Path, runtime: &Path) -> io::Result<()> {
    fn clean_absolute(path: &Path) -> bool {
        path.is_absolute()
            && path.components().all(|component| {
                matches!(
                    component,
                    std::path::Component::RootDir | std::path::Component::Normal(_)
                )
            })
            && !path.as_os_str().as_bytes().contains(&0)
    }

    fn owned_directory(path: &Path, required_mode: Option<u32>) -> io::Result<()> {
        let metadata = fs::symlink_metadata(path)?;
        let mode = metadata.permissions().mode() & 0o777;
        if !metadata.file_type().is_dir()
            || metadata.file_type().is_symlink()
            || metadata.uid() != unsafe { libc::getuid() }
            || required_mode.is_some_and(|required| mode != required)
        {
            return Err(io::Error::new(
                io::ErrorKind::PermissionDenied,
                "locker runtime path is not a protected owned directory",
            ));
        }
        Ok(())
    }

    if !clean_absolute(runtime) || !clean_absolute(socket) {
        return Err(io::Error::new(
            io::ErrorKind::PermissionDenied,
            "locker endpoint path is not clean and absolute",
        ));
    }
    owned_directory(runtime, None)?;
    let directory = runtime.join("sleepy");
    owned_directory(&directory, Some(0o700))?;
    if socket.parent() != Some(directory.as_path()) || socket.file_name().is_none() {
        return Err(io::Error::new(
            io::ErrorKind::PermissionDenied,
            "locker endpoint is outside the protected runtime directory",
        ));
    }
    let metadata = fs::symlink_metadata(socket)?;
    if !metadata.file_type().is_socket()
        || metadata.file_type().is_symlink()
        || metadata.uid() != unsafe { libc::getuid() }
        || metadata.permissions().mode() & 0o777 != 0o600
    {
        return Err(io::Error::new(
            io::ErrorKind::PermissionDenied,
            "locker endpoint is not an owned mode-0600 socket",
        ));
    }
    Ok(())
}

#[derive(Debug, Clone, Copy, Default)]
pub struct ProductionLogind;

impl ProductionLogind {
    pub fn state(self) -> io::Result<DesktopDomainState> {
        self.state_until(Instant::now() + LOGIND_STATE_TIMEOUT, None)
    }

    fn state_controlled(self, control: &RunControl) -> io::Result<DesktopDomainState> {
        ensure_run_active(control, "locker status polling")?;
        let remaining = control.remaining().min(LOGIND_STATE_TIMEOUT);
        if remaining.is_zero() {
            return Err(io::Error::new(
                io::ErrorKind::TimedOut,
                "locker status polling exceeded its deadline",
            ));
        }
        self.state_until(Instant::now() + remaining, Some(control))
    }

    fn state_until(
        self,
        deadline: Instant,
        control: Option<&RunControl>,
    ) -> io::Result<DesktopDomainState> {
        if let Some(control) = control {
            ensure_run_active(control, "locker status polling")?;
        }
        let locked = request_locker_status(remaining(deadline)?)? == LockerStatus::Locked;
        if let Some(control) = control {
            ensure_run_active(control, "locker status polling")?;
        }
        DesktopDomainState::available(
            DesktopDomainId::Lock,
            DesktopDomainValue::Lock(LockState { secure: locked }),
        )
    }

    pub fn execute(self, command: DesktopSessionCommand) -> io::Result<Option<DesktopDomainState>> {
        let deadline = Instant::now() + LOGIND_ACTION_TIMEOUT;
        execute_session_with(
            command,
            request_secure_lock,
            execute_suspend_via_logind,
            |command| {
                let connection = dbus::blocking::Connection::new_system().map_err(dbus_error)?;
                invoke_logind_action(&connection, command, deadline)
            },
        )
    }
}

fn execute_session_with(
    command: DesktopSessionCommand,
    mut request_lock: impl FnMut() -> io::Result<()>,
    mut suspend: impl FnMut() -> io::Result<()>,
    mut invoke: impl FnMut(DesktopSessionCommand) -> io::Result<()>,
) -> io::Result<Option<DesktopDomainState>> {
    match command {
        DesktopSessionCommand::Lock => {
            request_lock()?;
            DesktopDomainState::available(
                DesktopDomainId::Lock,
                DesktopDomainValue::Lock(LockState { secure: true }),
            )
            .map(Some)
        }
        DesktopSessionCommand::Suspend => {
            suspend()?;
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

struct SuspendTransitionWait<F> {
    resume_timeout: Duration,
    wait: F,
}

impl<F> SuspendTransitionWait<F> {
    fn new(resume_timeout: Duration, wait: F) -> Self {
        Self {
            resume_timeout,
            wait,
        }
    }
}

fn execute_suspend_lifecycle<I, H, F>(
    acquire_inhibitor: impl FnOnce() -> io::Result<I>,
    acquire_hold: impl FnOnce() -> io::Result<H>,
    invoke_suspend: impl FnOnce() -> io::Result<()>,
    mut transitions: SuspendTransitionWait<F>,
    release_inhibitor: impl FnOnce(I),
    release_hold: impl FnOnce(H),
    fail_closed_hold: impl FnOnce(H),
) -> io::Result<()>
where
    F: FnMut(bool, Option<Instant>) -> io::Result<()>,
{
    let inhibitor = acquire_inhibitor()?;
    let hold = match acquire_hold() {
        Ok(hold) => hold,
        Err(error) => {
            release_inhibitor(inhibitor);
            return Err(error);
        }
    };
    if let Err(error) = invoke_suspend() {
        fail_closed_hold(hold);
        release_inhibitor(inhibitor);
        return Err(error);
    }
    if let Err(error) = (transitions.wait)(true, None) {
        fail_closed_hold(hold);
        release_inhibitor(inhibitor);
        return Err(error);
    }
    let resume_deadline = Instant::now() + transitions.resume_timeout;
    release_inhibitor(inhibitor);
    if let Err(error) = (transitions.wait)(false, Some(resume_deadline)) {
        fail_closed_hold(hold);
        return Err(error);
    }
    release_hold(hold);
    Ok(())
}

fn execute_suspend_via_logind() -> io::Result<()> {
    let connection = dbus::blocking::Connection::new_system().map_err(dbus_error)?;
    let (sender, receiver) = std_mpsc::sync_channel(4);
    let rule = MatchRule::new_signal(LOGIN1_MANAGER, "PrepareForSleep")
        .with_sender(LOGIN1_DESTINATION)
        .with_path(LOGIN1_MANAGER_PATH);
    connection
        .add_match(rule, move |(preparing,): (bool,), _, _| {
            let _ = sender.try_send(preparing);
            true
        })
        .map_err(dbus_error)?;
    let prepare_deadline = Instant::now() + LOGIND_ACTION_TIMEOUT;
    execute_suspend_lifecycle(
        || acquire_sleep_delay_inhibitor(&connection, prepare_deadline),
        acquire_suspend_hold,
        || {
            invoke_logind_action(
                &connection,
                DesktopSessionCommand::Suspend,
                prepare_deadline,
            )
        },
        SuspendTransitionWait::new(LOGIND_ACTION_TIMEOUT, |expected, resume_deadline| {
            wait_for_sleep_transition(
                &connection,
                &receiver,
                expected,
                if expected {
                    Some(prepare_deadline)
                } else {
                    resume_deadline
                },
            )
        }),
        drop,
        LockerSuspendHold::release,
        LockerSuspendHold::fail_closed,
    )
}

fn acquire_sleep_delay_inhibitor(
    connection: &dbus::blocking::Connection,
    deadline: Instant,
) -> io::Result<dbus::arg::OwnedFd> {
    let manager = connection.with_proxy(
        LOGIN1_DESTINATION,
        LOGIN1_MANAGER_PATH,
        remaining(deadline)?,
    );
    let (inhibitor,): (dbus::arg::OwnedFd,) = manager
        .method_call(
            LOGIN1_MANAGER,
            "Inhibit",
            (
                "sleep",
                "Sleepy",
                "hold secure session lock across suspend",
                "delay",
            ),
        )
        .map_err(dbus_error)?;
    Ok(inhibitor)
}

fn wait_for_sleep_transition(
    connection: &dbus::blocking::Connection,
    receiver: &std_mpsc::Receiver<bool>,
    expected: bool,
    deadline: Option<Instant>,
) -> io::Result<()> {
    loop {
        match receiver.try_recv() {
            Ok(observed) if observed == expected => return Ok(()),
            Ok(_) => {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "logind reported an invalid sleep transition order",
                ))
            }
            Err(std_mpsc::TryRecvError::Disconnected) => {
                return Err(io::Error::new(
                    io::ErrorKind::BrokenPipe,
                    "logind sleep transition receiver disconnected",
                ))
            }
            Err(std_mpsc::TryRecvError::Empty) => {}
        }
        let process_for = match deadline {
            Some(deadline) => remaining(deadline)?.min(Duration::from_millis(100)),
            None => Duration::from_millis(100),
        };
        connection.process(process_for).map_err(dbus_error)?;
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
            return Err(io::Error::new(
                io::ErrorKind::Unsupported,
                "session lock is owned exclusively by the private locker endpoint",
            ))
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
    async fn probe(&self, context: Option<&DesktopProducerContext>) -> DesktopDomainState {
        let result = match context {
            Some(context) => {
                context
                    .spawn_blocking(Instant::now() + LOGIND_STATE_TIMEOUT, |control| {
                        ProductionLogind.state_controlled(&control)
                    })
                    .await
            }
            None => tokio::task::spawn_blocking(|| ProductionLogind.state()).await,
        };
        match result {
            Ok(Ok(state)) => state,
            Ok(Err(error)) => terminal(
                DesktopDomainId::Lock,
                availability_for_io(&error),
                error.to_string(),
            ),
            Err(_) => terminal(
                DesktopDomainId::Lock,
                CapabilityAvailability::Error,
                "locker status worker failed",
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
        self.probe(None).await
    }

    async fn run(
        &self,
        sender: mpsc::Sender<DesktopDomainUpdate>,
        context: DesktopProducerContext,
    ) -> Result<(), ProducerError> {
        let mut previous = None;
        let mut interval = tokio::time::interval(Duration::from_secs(2));
        interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        interval.tick().await;
        loop {
            tokio::select! {
                biased;
                _ = context.cancelled() => return Ok(()),
                _ = interval.tick() => {
                    let observation = context.begin_observation();
                    let current = self.probe(Some(&context)).await;
                    if previous.as_ref() != Some(&current) {
                        let update = observation
                            .finish(current.clone())
                            .map_err(|error| ProducerError::new(error.to_string()))?;
                        sender.send(update)
                            .await
                            .map_err(|_| ProducerError::new("desktop state authority stopped"))?;
                        previous = Some(current);
                    }
                }
            }
        }
    }
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
    use std::{
        cell::RefCell,
        io::Read,
        net::Shutdown,
        os::unix::{fs::symlink, net::UnixListener},
        rc::Rc,
        sync::Mutex,
    };

    use sleepy_sdk::StableId;

    use super::*;
    use crate::system::{CommandOutput, CommandRunner, RunnerError};

    fn locker_fixture(
        reply: Option<&'static [u8]>,
    ) -> (tempfile::TempDir, PathBuf, std::thread::JoinHandle<Vec<u8>>) {
        let runtime = tempfile::tempdir().unwrap();
        fs::set_permissions(runtime.path(), fs::Permissions::from_mode(0o700)).unwrap();
        let directory = runtime.path().join("sleepy");
        fs::create_dir(&directory).unwrap();
        fs::set_permissions(&directory, fs::Permissions::from_mode(0o700)).unwrap();
        let socket = directory.join("locker.sock");
        let listener = UnixListener::bind(&socket).unwrap();
        fs::set_permissions(&socket, fs::Permissions::from_mode(0o600)).unwrap();
        let server = std::thread::spawn(move || {
            let (mut stream, _) = listener.accept().unwrap();
            let mut request = [0_u8; 5];
            stream.read_exact(&mut request).unwrap();
            if let Some(reply) = reply {
                stream.write_all(reply).unwrap();
            } else {
                std::thread::sleep(Duration::from_millis(250));
            }
            request.to_vec()
        });
        (runtime, socket, server)
    }

    #[test]
    fn locker_client_sends_only_lock_and_requires_exact_secure_acknowledgement() {
        let (runtime, socket, server) = locker_fixture(Some(b"locked\n"));

        request_secure_lock_at(&socket, runtime.path(), Duration::from_secs(1)).unwrap();

        assert_eq!(server.join().unwrap(), b"lock\n");
    }

    #[test]
    fn locker_client_rejects_any_reply_other_than_exact_locked_line() {
        for reply in [&b"error\n"[..], &b"locked\nextra"[..], &b"locked"[..]] {
            let (runtime, socket, server) = locker_fixture(Some(reply));
            let error = request_secure_lock_at(&socket, runtime.path(), Duration::from_secs(1))
                .unwrap_err();
            assert_eq!(error.kind(), io::ErrorKind::InvalidData);
            assert_eq!(server.join().unwrap(), b"lock\n");
        }
    }

    #[test]
    fn locker_client_rejects_unprotected_or_out_of_runtime_socket_paths() {
        let runtime = tempfile::tempdir().unwrap();
        fs::set_permissions(runtime.path(), fs::Permissions::from_mode(0o700)).unwrap();
        let directory = runtime.path().join("sleepy");
        fs::create_dir(&directory).unwrap();
        fs::set_permissions(&directory, fs::Permissions::from_mode(0o700)).unwrap();
        let socket = directory.join("locker.sock");
        let listener = UnixListener::bind(&socket).unwrap();
        fs::set_permissions(&socket, fs::Permissions::from_mode(0o666)).unwrap();
        assert_eq!(
            request_secure_lock_at(&socket, runtime.path(), Duration::from_secs(1))
                .unwrap_err()
                .kind(),
            io::ErrorKind::PermissionDenied
        );
        drop(listener);

        let outside = tempfile::tempdir().unwrap();
        let outside_path = outside.path().join("locker.sock");
        assert_eq!(
            validate_locker_path(&outside_path, runtime.path())
                .unwrap_err()
                .kind(),
            io::ErrorKind::PermissionDenied
        );
    }

    #[test]
    fn locker_client_bounds_unconfirmed_lock_wait() {
        let (runtime, socket, server) = locker_fixture(None);

        let started = Instant::now();
        let error =
            request_secure_lock_at(&socket, runtime.path(), Duration::from_millis(40)).unwrap_err();

        assert_eq!(error.kind(), io::ErrorKind::TimedOut);
        assert!(started.elapsed() < Duration::from_millis(200));
        assert_eq!(server.join().unwrap(), b"lock\n");
    }

    #[test]
    fn locker_status_uses_authoritative_exact_protocol_values() {
        for (reply, expected) in [
            (&b"locked\n"[..], LockerStatus::Locked),
            (&b"unlocked\n"[..], LockerStatus::Unlocked),
        ] {
            let (runtime, socket, server) = locker_fixture_with_request(reply, 7, false);
            assert_eq!(
                request_locker_status_at(&socket, runtime.path(), Duration::from_secs(1)).unwrap(),
                expected
            );
            assert_eq!(server.join().unwrap(), b"status\n");
        }

        let (runtime, socket, server) = locker_fixture_with_request(b"unknown\n", 7, false);
        assert_eq!(
            request_locker_status_at(&socket, runtime.path(), Duration::from_secs(1))
                .unwrap_err()
                .kind(),
            io::ErrorKind::InvalidData
        );
        assert_eq!(server.join().unwrap(), b"status\n");
    }

    #[test]
    fn suspend_locker_transaction_keeps_connection_held_after_secure_ack() {
        let (runtime, socket, server) = locker_fixture_with_request(b"locked\n", 8, true);

        let hold =
            acquire_suspend_hold_at(&socket, runtime.path(), Duration::from_secs(1)).unwrap();
        assert!(!server.is_finished(), "locker hold closed before resume");

        drop(hold);
        assert_eq!(server.join().unwrap(), b"suspend\n");
    }

    #[test]
    fn suspend_locker_transaction_rejects_bad_ack() {
        let (runtime, socket, server) = locker_fixture_with_request(b"error!\n", 8, true);
        let error = match acquire_suspend_hold_at(&socket, runtime.path(), Duration::from_secs(1)) {
            Ok(hold) => {
                hold.release();
                panic!("invalid suspend acknowledgement was accepted")
            }
            Err(error) => error,
        };
        assert_eq!(error.kind(), io::ErrorKind::InvalidData);
        assert_eq!(server.join().unwrap(), b"suspend\n");
    }

    #[test]
    fn suspend_locker_transaction_rejects_delayed_extra_byte_and_post_ack_close() {
        for action in [
            HeldFixtureAction::DelayedExtra,
            HeldFixtureAction::DelayedClose,
        ] {
            let (runtime, socket, server) = held_locker_fixture(action);
            let error =
                match acquire_suspend_hold_at(&socket, runtime.path(), Duration::from_secs(1)) {
                    Ok(hold) => {
                        hold.release();
                        panic!("unstable suspend acknowledgement was accepted")
                    }
                    Err(error) => error,
                };
            assert!(matches!(
                error.kind(),
                io::ErrorKind::InvalidData | io::ErrorKind::UnexpectedEof
            ));
            assert_eq!(server.join().unwrap(), b"suspend\n");
        }
    }

    #[derive(Clone, Copy)]
    enum HeldFixtureAction {
        DelayedExtra,
        DelayedClose,
    }

    fn held_locker_fixture(
        action: HeldFixtureAction,
    ) -> (tempfile::TempDir, PathBuf, std::thread::JoinHandle<Vec<u8>>) {
        let runtime = tempfile::tempdir().unwrap();
        fs::set_permissions(runtime.path(), fs::Permissions::from_mode(0o700)).unwrap();
        let directory = runtime.path().join("sleepy");
        fs::create_dir(&directory).unwrap();
        fs::set_permissions(&directory, fs::Permissions::from_mode(0o700)).unwrap();
        let socket = directory.join("locker.sock");
        let listener = UnixListener::bind(&socket).unwrap();
        fs::set_permissions(&socket, fs::Permissions::from_mode(0o600)).unwrap();
        let server = std::thread::spawn(move || {
            let (mut stream, _) = listener.accept().unwrap();
            let mut request = vec![0_u8; 8];
            stream.read_exact(&mut request).unwrap();
            stream.write_all(b"locked\n").unwrap();
            std::thread::sleep(Duration::from_millis(15));
            if matches!(action, HeldFixtureAction::DelayedExtra) {
                stream.write_all(b"x").unwrap();
                stream.shutdown(Shutdown::Write).unwrap();
            }
            request
        });
        (runtime, socket, server)
    }

    #[test]
    fn locker_path_rejects_symlink_non_socket_and_dirty_components() {
        let runtime = tempfile::tempdir().unwrap();
        fs::set_permissions(runtime.path(), fs::Permissions::from_mode(0o700)).unwrap();
        let directory = runtime.path().join("sleepy");
        fs::create_dir(&directory).unwrap();
        fs::set_permissions(&directory, fs::Permissions::from_mode(0o700)).unwrap();

        let regular = directory.join("regular");
        fs::write(&regular, b"not a socket").unwrap();
        fs::set_permissions(&regular, fs::Permissions::from_mode(0o600)).unwrap();
        assert_eq!(
            validate_locker_path(&regular, runtime.path())
                .unwrap_err()
                .kind(),
            io::ErrorKind::PermissionDenied
        );

        let target = directory.join("target.sock");
        let listener = UnixListener::bind(&target).unwrap();
        fs::set_permissions(&target, fs::Permissions::from_mode(0o600)).unwrap();
        let linked = directory.join("linked.sock");
        symlink(&target, &linked).unwrap();
        assert_eq!(
            validate_locker_path(&linked, runtime.path())
                .unwrap_err()
                .kind(),
            io::ErrorKind::PermissionDenied
        );

        let dirty = directory.join("..").join("sleepy").join("target.sock");
        assert_eq!(
            validate_locker_path(&dirty, runtime.path())
                .unwrap_err()
                .kind(),
            io::ErrorKind::PermissionDenied
        );

        let link_root = tempfile::tempdir().unwrap();
        let runtime_link = link_root.path().join("runtime-link");
        symlink(runtime.path(), &runtime_link).unwrap();
        let linked_runtime_socket = runtime_link.join("sleepy/target.sock");
        assert_eq!(
            validate_locker_path(&linked_runtime_socket, &runtime_link)
                .unwrap_err()
                .kind(),
            io::ErrorKind::PermissionDenied
        );
        drop(listener);
    }

    fn locker_fixture_with_request(
        reply: &'static [u8],
        request_len: usize,
        wait_for_release: bool,
    ) -> (tempfile::TempDir, PathBuf, std::thread::JoinHandle<Vec<u8>>) {
        let runtime = tempfile::tempdir().unwrap();
        fs::set_permissions(runtime.path(), fs::Permissions::from_mode(0o700)).unwrap();
        let directory = runtime.path().join("sleepy");
        fs::create_dir(&directory).unwrap();
        fs::set_permissions(&directory, fs::Permissions::from_mode(0o700)).unwrap();
        let socket = directory.join("locker.sock");
        let listener = UnixListener::bind(&socket).unwrap();
        fs::set_permissions(&socket, fs::Permissions::from_mode(0o600)).unwrap();
        let server = std::thread::spawn(move || {
            let (mut stream, _) = listener.accept().unwrap();
            let mut request = vec![0_u8; request_len];
            stream.read_exact(&mut request).unwrap();
            stream.write_all(reply).unwrap();
            if wait_for_release {
                stream.shutdown(Shutdown::Write).unwrap();
                let mut trailing = Vec::new();
                stream.read_to_end(&mut trailing).unwrap();
                assert!(trailing.is_empty());
            }
            request
        });
        (runtime, socket, server)
    }

    #[test]
    fn suspend_lifecycle_releases_delay_only_for_prepare_and_hold_only_for_resume() {
        let steps = Rc::new(RefCell::new(Vec::new()));
        let inhibitor_released_at = Rc::new(RefCell::new(None));
        let resume_timeout = Duration::from_millis(40);
        let record = |step: &'static str, steps: &Rc<RefCell<Vec<&'static str>>>| {
            steps.borrow_mut().push(step);
        };

        execute_suspend_lifecycle(
            {
                let steps = Rc::clone(&steps);
                move || {
                    record("inhibit", &steps);
                    Ok("inhibitor")
                }
            },
            {
                let steps = Rc::clone(&steps);
                move || {
                    record("hold", &steps);
                    Ok("hold")
                }
            },
            {
                let steps = Rc::clone(&steps);
                move || {
                    record("suspend", &steps);
                    Ok(())
                }
            },
            SuspendTransitionWait::new(resume_timeout, {
                let steps = Rc::clone(&steps);
                let inhibitor_released_at = Rc::clone(&inhibitor_released_at);
                move |expected, deadline: Option<Instant>| {
                    record(if expected { "prepare" } else { "resume" }, &steps);
                    if expected {
                        assert!(deadline.is_none());
                        std::thread::sleep(resume_timeout + Duration::from_millis(20));
                    } else {
                        let deadline = deadline.expect("resume must be bounded");
                        let released_at = inhibitor_released_at
                            .borrow()
                            .expect("inhibitor must be released before resume wait");
                        assert!(deadline > released_at);
                        assert!(deadline - resume_timeout <= released_at);
                    }
                    Ok(())
                }
            }),
            {
                let steps = Rc::clone(&steps);
                let inhibitor_released_at = Rc::clone(&inhibitor_released_at);
                move |_| {
                    *inhibitor_released_at.borrow_mut() = Some(Instant::now());
                    record("release-inhibitor", &steps)
                }
            },
            {
                let steps = Rc::clone(&steps);
                move |_| record("release-hold", &steps)
            },
            {
                let steps = Rc::clone(&steps);
                move |_| record("fail-closed-hold", &steps)
            },
        )
        .unwrap();

        assert_eq!(
            steps.borrow().as_slice(),
            [
                "inhibit",
                "hold",
                "suspend",
                "prepare",
                "release-inhibitor",
                "resume",
                "release-hold"
            ]
        );
    }

    #[test]
    fn suspend_lifecycle_keeps_locker_hold_on_transition_error() {
        let steps = Rc::new(RefCell::new(Vec::new()));
        let resume_timeout = Duration::from_millis(20);
        let error = execute_suspend_lifecycle(
            || Ok("inhibitor"),
            || Ok("hold"),
            || Ok(()),
            SuspendTransitionWait::new(resume_timeout, {
                let steps = Rc::clone(&steps);
                move |expected, deadline: Option<Instant>| {
                    if expected {
                        steps.borrow_mut().push("prepare");
                        return Ok(());
                    }
                    let deadline = deadline.expect("resume must be bounded");
                    std::thread::sleep(resume_timeout + Duration::from_millis(10));
                    assert!(Instant::now() >= deadline);
                    steps.borrow_mut().push("resume-timeout");
                    Err(io::Error::new(io::ErrorKind::TimedOut, "fixture"))
                }
            }),
            {
                let steps = Rc::clone(&steps);
                move |_| steps.borrow_mut().push("release-inhibitor")
            },
            {
                let steps = Rc::clone(&steps);
                move |_| steps.borrow_mut().push("unexpected-release-hold")
            },
            {
                let steps = Rc::clone(&steps);
                move |_| steps.borrow_mut().push("fail-closed-hold")
            },
        )
        .unwrap_err();
        assert_eq!(error.kind(), io::ErrorKind::TimedOut);
        assert_eq!(
            steps.borrow().as_slice(),
            [
                "prepare",
                "release-inhibitor",
                "resume-timeout",
                "fail-closed-hold"
            ]
        );
    }

    fn scripted_status_child(
        script: &str,
    ) -> (Child, std::io::BufReader<std::process::ChildStdout>) {
        let mut child = Command::new("sh")
            .args(["-c", script])
            .stdin(Stdio::null())
            .stdout(Stdio::piped())
            .stderr(Stdio::null())
            .spawn()
            .unwrap();
        let stdout = child.stdout.take().unwrap();
        set_nonblocking(stdout.as_raw_fd()).unwrap();
        (child, std::io::BufReader::new(stdout))
    }

    #[test]
    fn recording_confirmation_requires_bounded_helper_state_acknowledgements() {
        let (mut child, mut status) = scripted_status_child(
            "printf 'STATE recording\\n'; trap \"printf 'STATE paused\\n'\" USR1; while :; do sleep 0.02; done",
        );
        wait_for_recording_ack(
            &mut status,
            &mut child,
            RecordingStatus::Recording,
            Instant::now() + Duration::from_secs(1),
        )
        .unwrap();
        assert_eq!(
            unsafe { libc::kill(child.id() as libc::pid_t, libc::SIGUSR1) },
            0
        );
        wait_for_recording_ack(
            &mut status,
            &mut child,
            RecordingStatus::Paused,
            Instant::now() + Duration::from_secs(1),
        )
        .unwrap();
        child.kill().unwrap();
        child.wait().unwrap();
    }

    #[test]
    fn polling_recording_state_never_waits_for_a_mutation_owned_lock() {
        let temp = tempfile::tempdir().unwrap();
        let service = ProductionUtilityService::open(temp.path().join("captures")).unwrap();
        let _held_mutation = service.recording.lock().unwrap();

        assert_eq!(
            service.recording_snapshot_for_poll().unwrap_err().kind(),
            io::ErrorKind::WouldBlock
        );
    }

    #[test]
    fn polling_idle_state_never_waits_for_a_mutation_owned_lock() {
        let temp = tempfile::tempdir().unwrap();
        let service = ProductionUtilityService::open(temp.path().join("captures")).unwrap();
        let _held_mutation = service.idle_inhibitor.lock().unwrap();

        assert_eq!(
            service
                .idle_snapshot_for_poll(&RunControl::for_timeout(Duration::from_secs(1)))
                .unwrap_err()
                .kind(),
            io::ErrorKind::WouldBlock
        );
    }

    #[tokio::test]
    async fn recording_poll_contention_emits_no_producer_update() {
        let temp = tempfile::tempdir().unwrap();
        let service =
            Arc::new(ProductionUtilityService::open(temp.path().join("captures")).unwrap());
        let producer =
            UtilityProducer::new(DesktopDomainId::Recording, Arc::clone(&service)).unwrap();
        let held_service = Arc::clone(&service);
        let (held, held_observed) = std::sync::mpsc::channel();
        let (release, released) = std::sync::mpsc::channel();
        let holder = std::thread::spawn(move || {
            let _held_mutation = held_service.recording.lock().unwrap();
            held.send(()).unwrap();
            released.recv().unwrap();
        });
        held_observed.recv_timeout(Duration::from_secs(1)).unwrap();
        let (sender, mut receiver) = mpsc::channel(1);
        let cancellation = tokio_util::sync::CancellationToken::new();
        let context = DesktopProducerContext::for_domain(
            cancellation.clone(),
            Arc::new(super::super::BlockingTaskTracker::default()),
            DesktopDomainId::Recording,
            Arc::new(std::sync::atomic::AtomicU64::new(0)),
        );
        let task = tokio::spawn(async move { producer.run(sender, context).await });

        tokio::time::sleep(UTILITY_REFRESH + Duration::from_millis(50)).await;
        let contention_update = receiver.try_recv();
        release.send(()).unwrap();
        holder.join().unwrap();
        cancellation.cancel();
        task.await.unwrap().unwrap();

        assert!(matches!(
            contention_update,
            Err(tokio::sync::mpsc::error::TryRecvError::Empty)
        ));
    }

    #[test]
    fn live_helper_without_ready_ack_cannot_confirm_recording_start() {
        let (mut child, mut status) = scripted_status_child("sleep 5");
        assert_eq!(
            wait_for_recording_ack(
                &mut status,
                &mut child,
                RecordingStatus::Recording,
                Instant::now() + Duration::from_millis(50),
            )
            .unwrap_err()
            .kind(),
            io::ErrorKind::TimedOut
        );
        child.kill().unwrap();
        child.wait().unwrap();
    }

    #[test]
    fn helper_that_ignores_pause_cannot_acknowledge_a_fabricated_state() {
        let (mut child, mut status) = scripted_status_child(
            "printf 'STATE recording\\n'; trap ':' USR1; while :; do sleep 0.02; done",
        );
        wait_for_recording_ack(
            &mut status,
            &mut child,
            RecordingStatus::Recording,
            Instant::now() + Duration::from_secs(1),
        )
        .unwrap();
        assert_eq!(
            unsafe { libc::kill(child.id() as libc::pid_t, libc::SIGUSR1) },
            0
        );
        let error = wait_for_recording_ack(
            &mut status,
            &mut child,
            RecordingStatus::Paused,
            Instant::now() + Duration::from_millis(50),
        )
        .unwrap_err();
        assert_eq!(error.kind(), io::ErrorKind::TimedOut);
        let mut runtime = RecordingRuntime {
            state: RecordingState {
                status: RecordingStatus::Recording,
                recording_id: Some("recording-under-test".into()),
                output_id: Some("output-under-test".into()),
            },
            child: Some(child),
            status: Some(status),
        };
        invalidate_recording_runtime(&mut runtime);
        assert_eq!(runtime.state.status, RecordingStatus::Inactive);
        assert!(runtime.child.is_none());
        assert!(runtime.status.is_none());
    }

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
        )
        .unwrap();
        let seen = runner.seen.lock().unwrap();
        assert_eq!(seen.len(), 1);
        assert_eq!(seen[0].program, "sleepy-capture-helper");
        assert_eq!(
            seen[0].args,
            [
                "screenshot",
                "--interactive-consent",
                "--output-id",
                "DP-1",
                "--output-path",
                "/run/user/1000/sleepy/captures/screenshot.png"
            ]
        );
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
    fn injected_session_policy_uses_private_locker_and_gates_suspend_before_action() {
        let steps = RefCell::new(Vec::new());
        let locked = execute_session_with(
            DesktopSessionCommand::Lock,
            || {
                steps.borrow_mut().push("locker-lock");
                Ok(())
            },
            || panic!("lock does not enter suspend lifecycle"),
            |command| {
                steps.borrow_mut().push(match command {
                    DesktopSessionCommand::Lock => "unexpected-logind-lock",
                    _ => "unexpected",
                });
                Ok(())
            },
        )
        .unwrap()
        .unwrap();
        assert_eq!(steps.into_inner(), ["locker-lock"]);
        assert_eq!(locked.status(), CapabilityAvailability::Available);

        let steps = RefCell::new(Vec::new());
        let error = execute_session_with(
            DesktopSessionCommand::Suspend,
            || panic!("suspend does not use the one-shot lock request"),
            || {
                steps.borrow_mut().push("suspend-lifecycle");
                Err(io::Error::new(io::ErrorKind::TimedOut, "fixture timeout"))
            },
            |command| {
                steps.borrow_mut().push(match command {
                    DesktopSessionCommand::Suspend => "suspend",
                    _ => "unexpected",
                });
                Ok(())
            },
        )
        .unwrap_err();
        assert_eq!(error.kind(), io::ErrorKind::TimedOut);
        assert_eq!(steps.into_inner(), ["suspend-lifecycle"]);

        let steps = RefCell::new(Vec::new());
        assert!(execute_session_with(
            DesktopSessionCommand::Suspend,
            || panic!("suspend does not use the one-shot lock request"),
            || {
                steps.borrow_mut().push("suspend-lifecycle");
                Ok(())
            },
            |command| {
                steps.borrow_mut().push(match command {
                    DesktopSessionCommand::Suspend => "suspend",
                    _ => "unexpected",
                });
                Ok(())
            },
        )
        .unwrap()
        .is_none());
        assert_eq!(steps.into_inner(), ["suspend-lifecycle"]);

        let actions = RefCell::new(Vec::new());
        assert!(execute_session_with(
            DesktopSessionCommand::Reboot,
            || panic!("reboot does not request the locker"),
            || panic!("reboot does not enter suspend lifecycle"),
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
