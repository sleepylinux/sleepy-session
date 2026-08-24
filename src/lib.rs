//! Durable XDG-backed settings and preset state for the Sleepy desktop.

pub mod cli;
mod store;

pub use store::{
    Defaults, ImportMode, InspectionDocumentReport, InspectionIssue, InspectionReport,
    ReplacementObserver, ReplacementStage, StateInspector, StateStore, StoreError, StorePaths,
};
