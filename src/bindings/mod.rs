mod actions;
mod apply;
mod compiler;
mod journal;
mod secure_fs;

use std::{error::Error, fmt};

use serde_json::{json, Value};
use sleepy_sdk::KeybindingConflict;

pub use apply::{
    activate_and_apply, apply_active_bindings, import_replace_active_and_apply,
    initialize_bindings, mutate_keybinding_and_apply, reconcile_bindings,
    reconcile_bindings_online_required, repair_state, update_active_bindings_and_apply,
    ApplyObserver, ApplyReport, ApplyStage, ApplyStatus, BindingPaths, BindingReloader,
    BindingValidator, ConfigEventStream, ConfigLoaded, NiriReloader, NiriValidator, RepairBundle,
};
pub use compiler::compile_bindings;

/// A closed, structured failure returned by binding compilation and application.
#[derive(Debug)]
pub struct BindingError {
    code: &'static str,
    message: String,
    details: Option<Value>,
}

impl BindingError {
    pub(crate) fn new(code: &'static str, message: impl Into<String>) -> Self {
        Self {
            code,
            message: message.into(),
            details: None,
        }
    }

    fn from_store(error: crate::StoreError) -> Self {
        Self {
            code: error.code(),
            message: error.message().to_owned(),
            details: error.details().cloned(),
        }
    }

    fn keybinding_conflict(conflict: KeybindingConflict) -> Self {
        Self {
            code: "keybinding_conflict",
            message: conflict.to_string(),
            details: Some(json!({
                "kind": conflict.kind,
                "accelerator": conflict.accelerator,
                "actions": conflict.actions,
            })),
        }
    }

    pub fn code(&self) -> &'static str {
        self.code
    }

    pub fn message(&self) -> &str {
        &self.message
    }

    pub fn details(&self) -> Option<&Value> {
        self.details.as_ref()
    }
}

impl fmt::Display for BindingError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "{}: {}", self.code, self.message)
    }
}

impl Error for BindingError {}
