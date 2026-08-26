//! Durable XDG-backed settings and preset state for the Sleepy desktop.

pub mod bindings;
pub mod calendar;
pub mod cli;
pub mod daily;
pub mod launcher;
pub mod notifications;
pub mod osd;
pub mod overview;
pub mod sessiond;
mod socket_supervisor;
mod store;
pub mod system;
mod systemd_notify;
pub mod theme;
pub mod theme_socket;
pub mod weather;

pub use systemd_notify::ready as notify_ready;

pub use store::{
    Defaults, ImportMode, InspectionDocumentReport, InspectionIssue, InspectionReport,
    PresetMutationObserver, PresetMutationStage, ReplacementObserver, ReplacementStage,
    StateInspector, StateStore, StoreError, StorePaths,
};
