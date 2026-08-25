//! Durable XDG-backed settings and preset state for the Sleepy desktop.

pub mod bindings;
pub mod cli;
pub mod notifications;
pub mod osd;
pub mod sessiond;
mod store;
pub mod system;

pub use store::{
    Defaults, ImportMode, InspectionDocumentReport, InspectionIssue, InspectionReport,
    PresetMutationObserver, PresetMutationStage, ReplacementObserver, ReplacementStage,
    StateInspector, StateStore, StoreError, StorePaths,
};
