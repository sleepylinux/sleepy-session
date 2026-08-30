// SPDX-License-Identifier: GPL-3.0-only

mod hyprland;
mod protocol;

use std::{error::Error, fmt, future::Future, io};

use sleepy_sdk::{HyprlandCommand, HyprlandSnapshot};
use tokio::sync::mpsc;

pub use hyprland::{
    parse_event_line, AdapterTiming, CompositorExecution, EventDisposition, HyprlandAdapter,
    HyprlandEvent, HyprlandPaths, MAX_COMMAND_RESPONSE_BYTES, MAX_EVENT_LINE_BYTES,
    MAX_INSTANCE_SIGNATURE_BYTES,
};
pub use protocol::parse_full_snapshot;

pub trait CompositorAdapter: Send + Sync {
    fn snapshot(&self) -> impl Future<Output = Result<HyprlandSnapshot, CompositorError>> + Send;

    fn execute(
        &self,
        command: HyprlandCommand,
    ) -> impl Future<Output = Result<CompositorExecution, CompositorError>> + Send;

    fn run_events(
        &self,
        sender: mpsc::Sender<HyprlandEvent>,
    ) -> impl Future<Output = Result<(), CompositorError>> + Send;
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CompositorErrorKind {
    Unavailable,
    UnsafeInstance,
    Io,
    Timeout,
    Parse,
    Inconsistent,
    Bounds,
    Rejected,
    Unsupported,
    Unconfirmed,
    Lagged,
    Cancelled,
}

#[derive(Debug)]
pub struct CompositorError {
    kind: CompositorErrorKind,
    message: String,
}

impl CompositorError {
    pub fn new(kind: CompositorErrorKind, message: impl Into<String>) -> Self {
        let message = message.into();
        Self {
            kind,
            message: if message.trim().is_empty() {
                "Hyprland adapter failed".into()
            } else {
                message
            },
        }
    }

    pub fn kind(&self) -> CompositorErrorKind {
        self.kind
    }
}

impl fmt::Display for CompositorError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.message)
    }
}

impl Error for CompositorError {}

impl From<io::Error> for CompositorError {
    fn from(error: io::Error) -> Self {
        let kind = match error.kind() {
            io::ErrorKind::NotFound | io::ErrorKind::ConnectionRefused => {
                CompositorErrorKind::Unavailable
            }
            io::ErrorKind::PermissionDenied => CompositorErrorKind::Unavailable,
            io::ErrorKind::TimedOut => CompositorErrorKind::Timeout,
            io::ErrorKind::InvalidData | io::ErrorKind::InvalidInput => CompositorErrorKind::Parse,
            io::ErrorKind::Interrupted => CompositorErrorKind::Cancelled,
            _ => CompositorErrorKind::Io,
        };
        Self::new(kind, error.to_string())
    }
}

pub(crate) fn parse_error(message: impl Into<String>) -> CompositorError {
    CompositorError::new(CompositorErrorKind::Parse, message)
}

pub(crate) fn inconsistent_error(message: impl Into<String>) -> CompositorError {
    CompositorError::new(CompositorErrorKind::Inconsistent, message)
}

pub(crate) fn bounds_error(message: impl Into<String>) -> CompositorError {
    CompositorError::new(CompositorErrorKind::Bounds, message)
}
