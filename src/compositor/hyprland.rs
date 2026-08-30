// SPDX-License-Identifier: GPL-3.0-only

use std::{
    ffi::CString,
    fs::{File, OpenOptions},
    os::{
        fd::{AsRawFd, FromRawFd},
        unix::fs::{MetadataExt, OpenOptionsExt},
    },
    path::{Component, Path, PathBuf},
    sync::Arc,
    time::Duration,
};

use sleepy_sdk::{HyprlandCommand, HyprlandSnapshot};
use tokio::{
    io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader},
    net::UnixStream,
    sync::mpsc,
    time::Instant,
};
use tokio_util::sync::CancellationToken;

use super::{
    bounds_error,
    protocol::{parse_full_snapshot_with_metadata, ParsedHyprlandSnapshot},
    CompositorError, CompositorErrorKind,
};

pub const MAX_INSTANCE_SIGNATURE_BYTES: usize = 128;
pub const MAX_COMMAND_RESPONSE_BYTES: usize = 1024 * 1024;
pub const MAX_EVENT_LINE_BYTES: usize = 64 * 1024;

const MAX_EVENT_FIELD_BYTES: usize = 4 * 1024;
const MAX_EVENT_GROUP_MEMBERS: usize = 16_384;
const MAX_BATCH_SUBRESPONSE_BYTES: usize = 4 * 1024;

const MONITORS_REQUEST: &[u8] = b"j/monitors";
const WORKSPACES_REQUEST: &[u8] = b"j/workspaces";
const CLIENTS_REQUEST: &[u8] = b"j/clients";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AdapterTiming {
    pub operation_timeout: Duration,
    pub confirmation_poll: Duration,
    pub reconnect_delay: Duration,
    pub fallback_reconcile: Duration,
}

impl Default for AdapterTiming {
    fn default() -> Self {
        Self {
            operation_timeout: Duration::from_secs(2),
            confirmation_poll: Duration::from_millis(25),
            reconnect_delay: Duration::from_millis(250),
            fallback_reconcile: Duration::from_secs(30),
        }
    }
}

#[derive(Debug, Clone)]
pub struct HyprlandPaths {
    _instance_dir: Arc<File>,
    command_socket: PathBuf,
    event_socket: PathBuf,
}

#[derive(Clone)]
pub struct HyprlandAdapter {
    paths: HyprlandPaths,
    cancellation: CancellationToken,
    timing: AdapterTiming,
}

impl HyprlandAdapter {
    pub fn new(paths: HyprlandPaths, cancellation: CancellationToken) -> Self {
        Self::with_timing(paths, cancellation, AdapterTiming::default())
    }

    pub fn discover(cancellation: CancellationToken) -> Result<Self, CompositorError> {
        Ok(Self::new(HyprlandPaths::discover()?, cancellation))
    }

    pub fn with_timing(
        paths: HyprlandPaths,
        cancellation: CancellationToken,
        timing: AdapterTiming,
    ) -> Self {
        Self {
            paths,
            cancellation,
            timing,
        }
    }

    pub fn paths(&self) -> &HyprlandPaths {
        &self.paths
    }

    pub(crate) fn cancellation_token(&self) -> CancellationToken {
        self.cancellation.clone()
    }

    pub async fn snapshot(&self) -> Result<HyprlandSnapshot, CompositorError> {
        let deadline = Instant::now() + self.timing.operation_timeout;
        self.snapshot_at(deadline).await
    }

    pub async fn execute(
        &self,
        command: HyprlandCommand,
    ) -> Result<CompositorExecution, CompositorError> {
        let deadline = Instant::now() + self.timing.operation_timeout;
        if command == HyprlandCommand::Exit {
            self.dispatch_at(b"dispatch exit", deadline).await?;
            return self.confirm_exit(deadline).await;
        }

        let pre = self.snapshot_with_metadata_at(deadline).await?;
        let plan = ActionPlan::from_command(command, &pre)?;
        for request in &plan.requests {
            self.dispatch_at(request, deadline).await?;
        }

        loop {
            if Instant::now() >= deadline {
                return Err(CompositorError::new(
                    CompositorErrorKind::Unconfirmed,
                    "Hyprland readback did not confirm the requested postcondition",
                ));
            }
            let post = self.snapshot_with_metadata_at(deadline).await?;
            if plan.expected.confirms(&post) {
                return Ok(CompositorExecution::Snapshot(post.snapshot));
            }
            tokio::select! {
                biased;
                _ = self.cancellation.cancelled() => return Err(CompositorError::new(
                    CompositorErrorKind::Cancelled,
                    "Hyprland confirmation was cancelled",
                )),
                _ = tokio::time::sleep_until(
                    std::cmp::min(deadline, Instant::now() + self.timing.confirmation_poll)
                ) => {}
            }
        }
    }

    async fn dispatch_at(&self, request: &[u8], deadline: Instant) -> Result<(), CompositorError> {
        let response = self.request_at(request, deadline).await?;
        if request.starts_with(b"[[BATCH]]") {
            return validate_batch_response(request, &response);
        }
        if response != b"ok" {
            return Err(CompositorError::new(
                CompositorErrorKind::Rejected,
                "Hyprland rejected a fixed compositor dispatcher",
            ));
        }
        Ok(())
    }

    async fn confirm_exit(
        &self,
        deadline: Instant,
    ) -> Result<CompositorExecution, CompositorError> {
        loop {
            if Instant::now() >= deadline {
                return Err(CompositorError::new(
                    CompositorErrorKind::Unconfirmed,
                    "Hyprland exit was not confirmed by both IPC sockets disappearing",
                ));
            }
            let command_gone = self
                .socket_unavailable(self.paths.command_socket(), deadline)
                .await?;
            let events_gone = self
                .socket_unavailable(self.paths.event_socket(), deadline)
                .await?;
            if command_gone && events_gone {
                return Ok(CompositorExecution::Exited);
            }
            tokio::select! {
                biased;
                _ = self.cancellation.cancelled() => return Err(CompositorError::new(
                    CompositorErrorKind::Cancelled,
                    "Hyprland exit confirmation was cancelled",
                )),
                _ = tokio::time::sleep_until(
                    std::cmp::min(deadline, Instant::now() + self.timing.confirmation_poll)
                ) => {}
            }
        }
    }

    async fn socket_unavailable(
        &self,
        path: &Path,
        deadline: Instant,
    ) -> Result<bool, CompositorError> {
        tokio::time::timeout_at(deadline, async {
            tokio::select! {
                biased;
                _ = self.cancellation.cancelled() => Err(CompositorError::new(
                    CompositorErrorKind::Cancelled,
                    "Hyprland exit confirmation was cancelled",
                )),
                result = UnixStream::connect(path) => match result {
                    Ok(stream) => {
                        drop(stream);
                        Ok(false)
                    }
                    Err(error) if matches!(
                        error.kind(),
                        std::io::ErrorKind::NotFound | std::io::ErrorKind::ConnectionRefused
                    ) => Ok(true),
                    Err(error) => Err(CompositorError::from(error)),
                }
            }
        })
        .await
        .map_err(|_| {
            CompositorError::new(
                CompositorErrorKind::Unconfirmed,
                "Hyprland exit was not confirmed before its total deadline",
            )
        })?
    }

    pub async fn run_events(
        &self,
        sender: mpsc::Sender<HyprlandEvent>,
    ) -> Result<(), CompositorError> {
        loop {
            if self.cancellation.is_cancelled() || sender.is_closed() {
                return Ok(());
            }

            if let Err(error) = self.run_event_connection(&sender).await {
                if error.kind() == CompositorErrorKind::Cancelled {
                    return Ok(());
                }
                let degraded = HyprlandEvent::Degraded { kind: error.kind() };
                match sender.try_send(degraded) {
                    Ok(()) | Err(mpsc::error::TrySendError::Full(_)) => {}
                    Err(mpsc::error::TrySendError::Closed(_)) => return Ok(()),
                }
            }

            tokio::select! {
                _ = self.cancellation.cancelled() => return Ok(()),
                _ = sender.closed() => return Ok(()),
                _ = tokio::time::sleep(self.timing.reconnect_delay) => {}
            }
        }
    }

    async fn run_event_connection(
        &self,
        sender: &mpsc::Sender<HyprlandEvent>,
    ) -> Result<(), CompositorError> {
        let deadline = Instant::now() + self.timing.operation_timeout;
        let stream = tokio::time::timeout_at(deadline, async {
            tokio::select! {
                biased;
                _ = self.cancellation.cancelled() => Err(CompositorError::new(
                    CompositorErrorKind::Cancelled,
                    "Hyprland event connection was cancelled",
                )),
                result = UnixStream::connect(self.paths.event_socket()) => {
                    result.map_err(CompositorError::from)
                }
            }
        })
        .await
        .map_err(|_| {
            CompositorError::new(
                CompositorErrorKind::Timeout,
                "Hyprland event connection exceeded its total two-second deadline",
            )
        })??;
        let mut reader = BufReader::new(stream);

        self.publish_snapshot(sender).await?;
        let fallback = tokio::time::sleep(self.timing.fallback_reconcile);
        tokio::pin!(fallback);

        loop {
            tokio::select! {
                biased;
                _ = self.cancellation.cancelled() => return Err(CompositorError::new(
                    CompositorErrorKind::Cancelled,
                    "Hyprland event loop was cancelled",
                )),
                _ = sender.closed() => return Ok(()),
                _ = &mut fallback => {
                    self.publish_snapshot(sender).await?;
                    fallback.as_mut().reset(Instant::now() + self.timing.fallback_reconcile);
                }
                line = read_bounded_event_line(&mut reader) => {
                    let line = line?;
                    if parse_event_line(&line)? == EventDisposition::Reconcile {
                        self.publish_snapshot(sender).await?;
                        fallback.as_mut().reset(Instant::now() + self.timing.fallback_reconcile);
                    }
                }
            }
        }
    }

    async fn publish_snapshot(
        &self,
        sender: &mpsc::Sender<HyprlandEvent>,
    ) -> Result<(), CompositorError> {
        let snapshot = self.snapshot().await?;
        sender
            .try_send(HyprlandEvent::Snapshot(snapshot))
            .map_err(|error| match error {
                mpsc::error::TrySendError::Full(_) => CompositorError::new(
                    CompositorErrorKind::Lagged,
                    "Hyprland event receiver lagged; forcing authoritative resynchronization",
                ),
                mpsc::error::TrySendError::Closed(_) => CompositorError::new(
                    CompositorErrorKind::Cancelled,
                    "Hyprland event receiver was closed",
                ),
            })
    }

    pub(crate) async fn snapshot_at(
        &self,
        deadline: Instant,
    ) -> Result<HyprlandSnapshot, CompositorError> {
        Ok(self.snapshot_with_metadata_at(deadline).await?.snapshot)
    }

    async fn snapshot_with_metadata_at(
        &self,
        deadline: Instant,
    ) -> Result<ParsedHyprlandSnapshot, CompositorError> {
        loop {
            let monitors = self.request_at(MONITORS_REQUEST, deadline).await?;
            let workspaces = self.request_at(WORKSPACES_REQUEST, deadline).await?;
            let clients = self.request_at(CLIENTS_REQUEST, deadline).await?;
            match parse_full_snapshot_with_metadata(&monitors, &workspaces, &clients) {
                Ok(snapshot) => return Ok(snapshot),
                Err(error) if error.kind() == CompositorErrorKind::Inconsistent => {
                    if Instant::now() >= deadline {
                        return Err(CompositorError::new(
                            CompositorErrorKind::Timeout,
                            "Hyprland queries did not become mutually consistent before the total deadline",
                        ));
                    }
                    tokio::select! {
                        biased;
                        _ = self.cancellation.cancelled() => return Err(CompositorError::new(
                            CompositorErrorKind::Cancelled,
                            "Hyprland snapshot reconciliation was cancelled",
                        )),
                        _ = tokio::time::sleep_until(
                            std::cmp::min(deadline, Instant::now() + self.timing.confirmation_poll)
                        ) => {}
                    }
                }
                Err(error) => return Err(error),
            }
        }
    }

    async fn request_at(
        &self,
        request: &[u8],
        deadline: Instant,
    ) -> Result<Vec<u8>, CompositorError> {
        let operation = async {
            let mut stream = UnixStream::connect(self.paths.command_socket())
                .await
                .map_err(CompositorError::from)?;
            stream
                .write_all(request)
                .await
                .map_err(CompositorError::from)?;
            stream.shutdown().await.map_err(CompositorError::from)?;
            read_bounded_response(&mut stream).await
        };
        tokio::time::timeout_at(deadline, async {
            tokio::select! {
                biased;
                _ = self.cancellation.cancelled() => Err(CompositorError::new(
                    CompositorErrorKind::Cancelled,
                    "Hyprland operation was cancelled",
                )),
                result = operation => result,
            }
        })
        .await
        .map_err(|_| {
            CompositorError::new(
                CompositorErrorKind::Timeout,
                "Hyprland operation exceeded its total two-second deadline",
            )
        })?
    }
}

impl super::CompositorAdapter for HyprlandAdapter {
    fn snapshot(
        &self,
    ) -> impl std::future::Future<Output = Result<HyprlandSnapshot, CompositorError>> + Send {
        HyprlandAdapter::snapshot(self)
    }

    fn execute(
        &self,
        command: HyprlandCommand,
    ) -> impl std::future::Future<Output = Result<CompositorExecution, CompositorError>> + Send
    {
        HyprlandAdapter::execute(self, command)
    }

    fn run_events(
        &self,
        sender: mpsc::Sender<HyprlandEvent>,
    ) -> impl std::future::Future<Output = Result<(), CompositorError>> + Send {
        HyprlandAdapter::run_events(self, sender)
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum CompositorExecution {
    Snapshot(HyprlandSnapshot),
    Exited,
}

struct ActionPlan {
    requests: Vec<Vec<u8>>,
    expected: ExpectedPostcondition,
}

enum ExpectedPostcondition {
    WindowFocused {
        window_id: String,
    },
    WindowWorkspace {
        window_id: String,
        workspace_id: String,
    },
    WindowClosed {
        window_id: String,
    },
    WorkspaceFocused {
        workspace_id: String,
    },
    WorkspaceMonitor {
        workspace_id: String,
        monitor_id: String,
    },
    WindowFullscreen {
        window_id: String,
        expected_mode: u8,
    },
    WindowFloating {
        window_id: String,
        expected: bool,
    },
    WindowPinned {
        window_id: String,
        expected: bool,
    },
    WindowGrouped {
        window_id: String,
        expected: bool,
    },
}

impl ActionPlan {
    fn from_command(
        command: HyprlandCommand,
        parsed: &ParsedHyprlandSnapshot,
    ) -> Result<Self, CompositorError> {
        let snapshot = &parsed.snapshot;
        let (requests, expected) = match command {
            HyprlandCommand::FocusWindow { window_id } => {
                let window = find_window(snapshot, window_id.as_str())?;
                (
                    vec![dispatch(format!("focuswindow address:{}", window.id))?],
                    ExpectedPostcondition::WindowFocused {
                        window_id: window.id.clone(),
                    },
                )
            }
            HyprlandCommand::MoveWindowToWorkspace {
                window_id,
                workspace_id,
            } => {
                let window = find_window(snapshot, window_id.as_str())?;
                let workspace = find_workspace(snapshot, workspace_id.as_str())?;
                let target = workspace_dispatch_target(workspace, true)?;
                (
                    vec![dispatch(format!(
                        "movetoworkspacesilent {target},address:{}",
                        window.id
                    ))?],
                    ExpectedPostcondition::WindowWorkspace {
                        window_id: window.id.clone(),
                        workspace_id: workspace.id.clone(),
                    },
                )
            }
            HyprlandCommand::CloseWindow { window_id } => {
                let window = find_window(snapshot, window_id.as_str())?;
                (
                    vec![dispatch(format!("closewindow address:{}", window.id))?],
                    ExpectedPostcondition::WindowClosed {
                        window_id: window.id.clone(),
                    },
                )
            }
            HyprlandCommand::FocusWorkspace { workspace_id } => {
                let workspace = find_workspace(snapshot, workspace_id.as_str())?;
                let target = workspace_dispatch_target(workspace, false)?;
                (
                    vec![dispatch(format!("workspace {target}"))?],
                    ExpectedPostcondition::WorkspaceFocused {
                        workspace_id: workspace.id.clone(),
                    },
                )
            }
            HyprlandCommand::MoveWorkspaceToMonitor {
                workspace_id,
                monitor_id,
            } => {
                let workspace = find_workspace(snapshot, workspace_id.as_str())?;
                let monitor = snapshot
                    .monitors
                    .iter()
                    .find(|monitor| monitor.id == monitor_id.as_str())
                    .ok_or_else(|| rejected_target("monitor"))?;
                validate_dispatch_token(&monitor.id, "monitor")?;
                let target = workspace_dispatch_target(workspace, false)?;
                (
                    vec![dispatch(format!(
                        "moveworkspacetomonitor {target} {}",
                        monitor.id
                    ))?],
                    ExpectedPostcondition::WorkspaceMonitor {
                        workspace_id: workspace.id.clone(),
                        monitor_id: monitor.id.clone(),
                    },
                )
            }
            HyprlandCommand::ToggleFullscreen { window_id } => {
                let window = find_window(snapshot, window_id.as_str())?;
                let mode = parsed
                    .fullscreen_modes
                    .get(&window.id)
                    .copied()
                    .ok_or_else(|| rejected_target("window fullscreen state"))?;
                let (operation, expected_mode) = match mode {
                    0 => ("fullscreen 0 set", 2),
                    1 => ("fullscreen 1 unset", 0),
                    2 => ("fullscreen 0 unset", 0),
                    _ => {
                        return Err(CompositorError::new(
                            CompositorErrorKind::Rejected,
                            "Hyprland window had an unsupported fullscreen mode",
                        ))
                    }
                };
                (
                    vec![batch_dispatches(
                        format!("focuswindow address:{}", window.id),
                        operation.into(),
                    )?],
                    ExpectedPostcondition::WindowFullscreen {
                        window_id: window.id.clone(),
                        expected_mode,
                    },
                )
            }
            HyprlandCommand::ToggleFloating { window_id } => {
                let window = find_window(snapshot, window_id.as_str())?;
                (
                    vec![dispatch(format!("togglefloating address:{}", window.id))?],
                    ExpectedPostcondition::WindowFloating {
                        window_id: window.id.clone(),
                        expected: !window.floating,
                    },
                )
            }
            HyprlandCommand::TogglePinned { window_id } => {
                let window = find_window(snapshot, window_id.as_str())?;
                (
                    vec![dispatch(format!("pin address:{}", window.id))?],
                    ExpectedPostcondition::WindowPinned {
                        window_id: window.id.clone(),
                        expected: !window.pinned,
                    },
                )
            }
            HyprlandCommand::ToggleGroup { window_id } => {
                let window = find_window(snapshot, window_id.as_str())?;
                (
                    vec![batch_dispatches(
                        format!("focuswindow address:{}", window.id),
                        "togglegroup".into(),
                    )?],
                    ExpectedPostcondition::WindowGrouped {
                        window_id: window.id.clone(),
                        expected: !window.grouped,
                    },
                )
            }
            HyprlandCommand::Exit => unreachable!("exit is handled without snapshot readback"),
        };
        Ok(Self { requests, expected })
    }
}

impl ExpectedPostcondition {
    fn confirms(&self, parsed: &ParsedHyprlandSnapshot) -> bool {
        let snapshot = &parsed.snapshot;
        match self {
            Self::WindowFocused { window_id } => snapshot
                .windows
                .iter()
                .any(|window| window.id == *window_id && window.focused),
            Self::WindowWorkspace {
                window_id,
                workspace_id,
            } => snapshot
                .windows
                .iter()
                .any(|window| window.id == *window_id && window.workspace_id == *workspace_id),
            Self::WindowClosed { window_id } => snapshot
                .windows
                .iter()
                .all(|window| window.id != *window_id),
            Self::WorkspaceFocused { workspace_id } => snapshot
                .workspaces
                .iter()
                .any(|workspace| workspace.id == *workspace_id && workspace.focused),
            Self::WorkspaceMonitor {
                workspace_id,
                monitor_id,
            } => snapshot.workspaces.iter().any(|workspace| {
                workspace.id == *workspace_id && workspace.monitor_id == *monitor_id
            }),
            Self::WindowFullscreen {
                window_id,
                expected_mode,
            } => parsed.fullscreen_modes.get(window_id) == Some(expected_mode),
            Self::WindowFloating {
                window_id,
                expected,
            } => snapshot
                .windows
                .iter()
                .any(|window| window.id == *window_id && window.floating == *expected),
            Self::WindowPinned {
                window_id,
                expected,
            } => snapshot
                .windows
                .iter()
                .any(|window| window.id == *window_id && window.pinned == *expected),
            Self::WindowGrouped {
                window_id,
                expected,
            } => snapshot
                .windows
                .iter()
                .any(|window| window.id == *window_id && window.grouped == *expected),
        }
    }
}

fn find_window<'a>(
    snapshot: &'a HyprlandSnapshot,
    id: &str,
) -> Result<&'a sleepy_sdk::Window, CompositorError> {
    snapshot
        .windows
        .iter()
        .find(|window| window.id == id)
        .ok_or_else(|| rejected_target("window"))
}

fn find_workspace<'a>(
    snapshot: &'a HyprlandSnapshot,
    id: &str,
) -> Result<&'a sleepy_sdk::Workspace, CompositorError> {
    snapshot
        .workspaces
        .iter()
        .find(|workspace| workspace.id == id)
        .ok_or_else(|| rejected_target("workspace"))
}

fn rejected_target(description: &str) -> CompositorError {
    CompositorError::new(
        CompositorErrorKind::Rejected,
        format!("Hyprland command referenced an unknown {description}"),
    )
}

fn workspace_dispatch_target(
    workspace: &sleepy_sdk::Workspace,
    allow_special: bool,
) -> Result<String, CompositorError> {
    if workspace.name.starts_with("special:") {
        if !allow_special {
            return Err(CompositorError::new(
                CompositorErrorKind::Unsupported,
                "Hyprland special workspace operation has no deterministic legacy dispatcher",
            ));
        }
        validate_dispatch_token(&workspace.name, "special workspace")?;
        return Ok(workspace.name.clone());
    }

    if workspace.name == workspace.id && workspace.id.parse::<u64>().ok().is_some_and(|id| id > 0) {
        return Ok(workspace.id.clone());
    }

    validate_dispatch_token(&workspace.name, "named workspace")?;
    Ok(format!("name:{}", workspace.name))
}

fn validate_dispatch_token(value: &str, description: &str) -> Result<(), CompositorError> {
    if value.is_empty()
        || value.len() > 256
        || !value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'.' | b'_' | b':' | b'-'))
    {
        return Err(CompositorError::new(
            CompositorErrorKind::Rejected,
            format!("Hyprland {description} cannot be encoded as a fixed dispatcher argument"),
        ));
    }
    Ok(())
}

fn dispatch(argument: String) -> Result<Vec<u8>, CompositorError> {
    if argument.len() > 1024 || argument.chars().any(char::is_control) {
        return Err(CompositorError::new(
            CompositorErrorKind::Rejected,
            "Hyprland dispatcher argument was outside the fixed encoding bounds",
        ));
    }
    Ok(format!("dispatch {argument}").into_bytes())
}

fn batch_dispatches(first: String, second: String) -> Result<Vec<u8>, CompositorError> {
    let first = dispatch(first)?;
    let second = dispatch(second)?;
    if first.contains(&b';') || second.contains(&b';') {
        return Err(CompositorError::new(
            CompositorErrorKind::Rejected,
            "Hyprland batch dispatcher contained an unsafe separator",
        ));
    }
    let mut request = Vec::with_capacity(9 + first.len() + 1 + second.len());
    request.extend_from_slice(b"[[BATCH]]");
    request.extend_from_slice(&first);
    request.push(b';');
    request.extend_from_slice(&second);
    Ok(request)
}

fn validate_batch_response(request: &[u8], response: &[u8]) -> Result<(), CompositorError> {
    let expected = request[9..].split(|byte| *byte == b';').count();
    let mut replies = Vec::new();
    let mut start = 0;
    while let Some(offset) = response[start..]
        .windows(3)
        .position(|window| window == b"\n\n\n")
    {
        let end = start + offset;
        replies.push(&response[start..end]);
        start = end + 3;
    }
    replies.push(&response[start..]);
    if replies.len() != expected {
        return Err(CompositorError::new(
            CompositorErrorKind::Rejected,
            "Hyprland batch reply count did not match the fixed request",
        ));
    }
    for reply in replies {
        if reply.len() > MAX_BATCH_SUBRESPONSE_BYTES {
            return Err(bounds_error(format!(
                "Hyprland batch subresponse exceeded {MAX_BATCH_SUBRESPONSE_BYTES} bytes"
            )));
        }
        if reply != b"ok" {
            return Err(CompositorError::new(
                CompositorErrorKind::Rejected,
                "Hyprland rejected a fixed compositor batch subcommand",
            ));
        }
    }
    Ok(())
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EventDisposition {
    Ignore,
    Reconcile,
}

#[derive(Debug, Clone, PartialEq)]
pub enum HyprlandEvent {
    Snapshot(HyprlandSnapshot),
    Degraded { kind: CompositorErrorKind },
}

pub fn parse_event_line(line: &[u8]) -> Result<EventDisposition, CompositorError> {
    if line.len() > MAX_EVENT_LINE_BYTES {
        return Err(bounds_error(format!(
            "Hyprland event line exceeded {MAX_EVENT_LINE_BYTES} bytes"
        )));
    }
    let line = std::str::from_utf8(line)
        .map_err(|_| super::parse_error("Hyprland event line was not valid UTF-8"))?;
    let (name, payload) = line
        .split_once(">>")
        .ok_or_else(|| super::parse_error("Hyprland event line omitted the >> separator"))?;
    if name.is_empty()
        || name.len() > 64
        || !name
            .bytes()
            .all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit())
    {
        return Err(super::parse_error("Hyprland event name was malformed"));
    }

    match name {
        "workspace" | "createworkspace" | "destroyworkspace" => {
            validate_event_text(payload, false)?;
        }
        "workspacev2" | "createworkspacev2" | "destroyworkspacev2" => {
            let fields = exact_fields(payload, 2)?;
            validate_event_workspace_id(fields[0], false)?;
            validate_event_text(fields[1], false)?;
        }
        "focusedmon" => {
            let fields = exact_fields(payload, 2)?;
            validate_event_text(fields[0], false)?;
            validate_event_text(fields[1], false)?;
        }
        "focusedmonv2" => {
            let fields = exact_fields(payload, 2)?;
            validate_event_text(fields[0], false)?;
            validate_event_workspace_id(fields[1], false)?;
        }
        "activewindow" => {
            let (class, title) = payload
                .split_once(',')
                .ok_or_else(|| super::parse_error("activewindow event omitted its title"))?;
            validate_event_text(class, true)?;
            validate_event_text(title, true)?;
        }
        "activewindowv2" => {
            if !payload.is_empty() {
                validate_event_address(payload)?;
            }
        }
        "closewindow" => validate_event_address(payload)?,
        "openwindow" => {
            let fields = minimum_fields(payload, 4)?;
            validate_event_address(fields[0])?;
            validate_event_text(fields[1], false)?;
            validate_event_text(fields[2], false)?;
            validate_event_text(fields[3], true)?;
        }
        "movewindow" => {
            let fields = exact_fields(payload, 2)?;
            validate_event_address(fields[0])?;
            validate_event_text(fields[1], false)?;
        }
        "movewindowv2" => {
            let fields = exact_fields(payload, 3)?;
            validate_event_address(fields[0])?;
            validate_event_workspace_id(fields[1], false)?;
            validate_event_text(fields[2], false)?;
        }
        "fullscreen" => validate_event_bool(payload)?,
        "changefloatingmode" | "pin" => {
            let fields = exact_fields(payload, 2)?;
            validate_event_address(fields[0])?;
            validate_event_bool(fields[1])?;
        }
        "togglegroup" => {
            let fields = payload.split(',').collect::<Vec<_>>();
            if fields.len() < 2 {
                return Err(super::parse_error(
                    "Hyprland togglegroup event omitted its member addresses",
                ));
            }
            if fields.len() - 1 > MAX_EVENT_GROUP_MEMBERS {
                return Err(bounds_error(format!(
                    "Hyprland togglegroup event exceeded {MAX_EVENT_GROUP_MEMBERS} members"
                )));
            }
            validate_event_bool(fields[0])?;
            for address in &fields[1..] {
                validate_event_address(address)?;
            }
        }
        "moveintogroup" | "moveoutofgroup" | "windowtitle" => {
            validate_event_address(payload)?;
        }
        "windowtitlev2" => {
            let (address, title) = payload.split_once(',').ok_or_else(|| {
                super::parse_error("Hyprland windowtitlev2 event omitted its title")
            })?;
            validate_event_address(address)?;
            validate_event_text(title, true)?;
        }
        "monitoradded" | "monitorremoved" => validate_event_text(payload, false)?,
        "monitoraddedv2" | "monitorremovedv2" => {
            let fields = minimum_fields(payload, 3)?;
            validate_event_monitor_id(fields[0])?;
            validate_event_text(fields[1], false)?;
            validate_event_text(fields[2], true)?;
        }
        "moveworkspace" => {
            let fields = exact_fields(payload, 2)?;
            validate_event_text(fields[0], false)?;
            validate_event_text(fields[1], false)?;
        }
        "moveworkspacev2" => {
            let fields = exact_fields(payload, 3)?;
            validate_event_workspace_id(fields[0], false)?;
            validate_event_text(fields[1], false)?;
            validate_event_text(fields[2], false)?;
        }
        "renameworkspace" => {
            let fields = exact_fields(payload, 2)?;
            validate_event_workspace_id(fields[0], false)?;
            validate_event_text(fields[1], false)?;
        }
        "activespecial" => {
            let fields = exact_fields(payload, 2)?;
            validate_event_text(fields[0], true)?;
            validate_event_text(fields[1], false)?;
        }
        "activespecialv2" => {
            let fields = exact_fields(payload, 3)?;
            if fields[0].is_empty() {
                if !fields[1].is_empty() {
                    return Err(super::parse_error(
                        "closed Hyprland special workspace retained a name",
                    ));
                }
            } else {
                validate_event_workspace_id(fields[0], false)?;
                validate_event_text(fields[1], false)?;
            }
            validate_event_text(fields[2], false)?;
        }
        _ => return Ok(EventDisposition::Ignore),
    }
    Ok(EventDisposition::Reconcile)
}

fn exact_fields(payload: &str, count: usize) -> Result<Vec<&str>, CompositorError> {
    let fields = payload.split(',').collect::<Vec<_>>();
    if fields.len() != count {
        return Err(super::parse_error(format!(
            "Hyprland event expected exactly {count} fields"
        )));
    }
    Ok(fields)
}

fn minimum_fields(payload: &str, count: usize) -> Result<Vec<&str>, CompositorError> {
    let fields = payload.splitn(count, ',').collect::<Vec<_>>();
    if fields.len() != count {
        return Err(super::parse_error(format!(
            "Hyprland event expected at least {count} fields"
        )));
    }
    Ok(fields)
}

fn validate_event_text(value: &str, allow_empty: bool) -> Result<(), CompositorError> {
    if (!allow_empty && value.is_empty())
        || value.len() > MAX_EVENT_FIELD_BYTES
        || value.chars().any(char::is_control)
    {
        return Err(super::parse_error(
            "Hyprland event field was empty, overlong, or contained controls",
        ));
    }
    Ok(())
}

fn validate_event_workspace_id(value: &str, allow_zero: bool) -> Result<(), CompositorError> {
    let id = value
        .parse::<i64>()
        .map_err(|_| super::parse_error("Hyprland event workspace ID was not an integer"))?;
    if !allow_zero && id == 0 {
        return Err(super::parse_error(
            "Hyprland event workspace ID was outside the accepted domain",
        ));
    }
    Ok(())
}

fn validate_event_monitor_id(value: &str) -> Result<(), CompositorError> {
    value
        .parse::<u64>()
        .map_err(|_| super::parse_error("Hyprland event monitor ID was not nonnegative"))?;
    Ok(())
}

fn validate_event_address(value: &str) -> Result<(), CompositorError> {
    let digits = value.strip_prefix("0x").unwrap_or(value);
    if digits.is_empty()
        || digits.len() > 16
        || digits == "0"
        || (digits.len() > 1 && digits.starts_with('0'))
        || !digits
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
    {
        return Err(super::parse_error(
            "Hyprland event address was not 1..=16 hexadecimal digits",
        ));
    }
    Ok(())
}

fn validate_event_bool(value: &str) -> Result<(), CompositorError> {
    if !matches!(value, "0" | "1") {
        return Err(super::parse_error(
            "Hyprland event boolean was neither 0 nor 1",
        ));
    }
    Ok(())
}

async fn read_bounded_event_line(
    reader: &mut BufReader<UnixStream>,
) -> Result<Vec<u8>, CompositorError> {
    let mut line = Vec::with_capacity(1024);
    loop {
        let available = reader.fill_buf().await.map_err(CompositorError::from)?;
        if available.is_empty() {
            return Err(CompositorError::new(
                CompositorErrorKind::Unavailable,
                "Hyprland event socket disconnected",
            ));
        }
        if let Some(newline) = available.iter().position(|byte| *byte == b'\n') {
            if line.len().saturating_add(newline) > MAX_EVENT_LINE_BYTES {
                return Err(bounds_error(format!(
                    "Hyprland event line exceeded {MAX_EVENT_LINE_BYTES} bytes"
                )));
            }
            line.extend_from_slice(&available[..newline]);
            reader.consume(newline + 1);
            return Ok(line);
        }
        if line.len().saturating_add(available.len()) > MAX_EVENT_LINE_BYTES {
            return Err(bounds_error(format!(
                "Hyprland event line exceeded {MAX_EVENT_LINE_BYTES} bytes"
            )));
        }
        let consumed = available.len();
        line.extend_from_slice(available);
        reader.consume(consumed);
    }
}

async fn read_bounded_response(stream: &mut UnixStream) -> Result<Vec<u8>, CompositorError> {
    let mut response = Vec::with_capacity(8 * 1024);
    let mut chunk = [0_u8; 8 * 1024];
    loop {
        let read = stream
            .read(&mut chunk)
            .await
            .map_err(CompositorError::from)?;
        if read == 0 {
            return Ok(response);
        }
        if response.len().saturating_add(read) > MAX_COMMAND_RESPONSE_BYTES {
            return Err(bounds_error(format!(
                "Hyprland command response exceeded {MAX_COMMAND_RESPONSE_BYTES} bytes"
            )));
        }
        response.extend_from_slice(&chunk[..read]);
    }
}

impl HyprlandPaths {
    pub fn discover() -> Result<Self, CompositorError> {
        let runtime = std::env::var_os("XDG_RUNTIME_DIR").ok_or_else(|| {
            CompositorError::new(
                CompositorErrorKind::Unavailable,
                "XDG_RUNTIME_DIR is not set for the current session",
            )
        })?;
        let signature = std::env::var("HYPRLAND_INSTANCE_SIGNATURE").map_err(|_| {
            CompositorError::new(
                CompositorErrorKind::Unavailable,
                "HYPRLAND_INSTANCE_SIGNATURE is not set for the current session",
            )
        })?;
        Self::from_runtime_dir_and_signature(Path::new(&runtime), &signature)
    }

    pub fn from_runtime_dir_and_signature(
        runtime_dir: &Path,
        signature: &str,
    ) -> Result<Self, CompositorError> {
        validate_runtime_root(runtime_dir)?;
        validate_signature(signature)?;
        let runtime = open_private_directory(runtime_dir, "XDG_RUNTIME_DIR")?;
        let hypr = open_private_child(&runtime, "hypr", "Hyprland runtime directory")?;
        let instance = open_private_child(&hypr, signature, "Hyprland instance directory")?;
        let instance = Arc::new(instance);
        let anchored = PathBuf::from(format!("/proc/self/fd/{}", instance.as_raw_fd()));
        Ok(Self {
            _instance_dir: instance,
            command_socket: anchored.join(".socket.sock"),
            event_socket: anchored.join(".socket2.sock"),
        })
    }

    pub fn command_socket(&self) -> &Path {
        &self.command_socket
    }

    pub fn event_socket(&self) -> &Path {
        &self.event_socket
    }
}

fn open_private_directory(path: &Path, description: &str) -> Result<File, CompositorError> {
    let file = OpenOptions::new()
        .read(true)
        .custom_flags(libc::O_DIRECTORY | libc::O_NOFOLLOW | libc::O_CLOEXEC)
        .open(path)
        .map_err(|_| unsafe_instance_directory(description))?;
    validate_private_directory(&file, description)?;
    Ok(file)
}

fn open_private_child(
    parent: &File,
    name: &str,
    description: &str,
) -> Result<File, CompositorError> {
    let name = CString::new(name).map_err(|_| unsafe_instance_directory(description))?;
    // SAFETY: parent is a live directory fd, name is NUL-terminated and contains no slash,
    // and a successful descriptor is immediately owned by File.
    let fd = unsafe {
        libc::openat(
            parent.as_raw_fd(),
            name.as_ptr(),
            libc::O_RDONLY | libc::O_DIRECTORY | libc::O_NOFOLLOW | libc::O_CLOEXEC,
        )
    };
    if fd < 0 {
        return Err(unsafe_instance_directory(description));
    }
    // SAFETY: openat returned a new owned descriptor on success.
    let file = unsafe { File::from_raw_fd(fd) };
    validate_private_directory(&file, description)?;
    Ok(file)
}

fn validate_private_directory(file: &File, description: &str) -> Result<(), CompositorError> {
    let metadata = file
        .metadata()
        .map_err(|_| unsafe_instance_directory(description))?;
    if !metadata.is_dir()
        || metadata.uid() != unsafe { libc::geteuid() }
        || metadata.mode() & 0o777 != 0o700
    {
        return Err(unsafe_instance_directory(description));
    }
    Ok(())
}

fn unsafe_instance_directory(description: &str) -> CompositorError {
    CompositorError::new(
        CompositorErrorKind::UnsafeInstance,
        format!("{description} was not a private, owned, non-symlink directory"),
    )
}

fn validate_runtime_root(runtime_dir: &Path) -> Result<(), CompositorError> {
    let mut normal_components = 0_usize;
    let normalized_absolute = runtime_dir.is_absolute()
        && runtime_dir.components().all(|component| match component {
            Component::RootDir => true,
            Component::Normal(_) => {
                normal_components += 1;
                true
            }
            Component::CurDir | Component::ParentDir | Component::Prefix(_) => false,
        })
        && normal_components > 0;
    if !normalized_absolute {
        return Err(CompositorError::new(
            CompositorErrorKind::UnsafeInstance,
            "XDG_RUNTIME_DIR must be an absolute normalized runtime root",
        ));
    }
    Ok(())
}

fn validate_signature(signature: &str) -> Result<(), CompositorError> {
    let unsafe_value = signature.is_empty()
        || signature == "."
        || signature == ".."
        || signature.len() > MAX_INSTANCE_SIGNATURE_BYTES
        || signature
            .chars()
            .any(|character| character.is_control() || matches!(character, '/' | '\\'));
    if unsafe_value {
        return Err(CompositorError::new(
            CompositorErrorKind::UnsafeInstance,
            "HYPRLAND_INSTANCE_SIGNATURE is not a safe path component",
        ));
    }
    Ok(())
}
