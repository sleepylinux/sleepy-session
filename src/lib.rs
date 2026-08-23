//! Durable XDG-backed settings and preset state for the Sleepy desktop.

mod store;

pub use store::{
    Defaults, ReplacementObserver, ReplacementStage, StateStore, StoreError, StorePaths,
};
