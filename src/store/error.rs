use std::{error::Error, fmt};

use serde_json::{json, Value};
use sleepy_sdk::KeybindingConflict;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StoreError {
    code: &'static str,
    message: String,
    details: Option<Value>,
}

impl StoreError {
    pub(crate) fn invalid(message: impl Into<String>) -> Self {
        Self {
            code: "invalid_document",
            message: message.into(),
            details: None,
        }
    }

    pub(crate) fn not_found(id: &str) -> Self {
        Self {
            code: "preset_not_found",
            message: format!("preset {id:?} was not found"),
            details: None,
        }
    }

    pub(crate) fn immutable(id: &str) -> Self {
        Self {
            code: "immutable_preset",
            message: format!("preset {id:?} is immutable"),
            details: None,
        }
    }

    pub(crate) fn active(id: &str) -> Self {
        Self {
            code: "active_preset",
            message: format!("preset {id:?} is active and cannot be deleted"),
            details: None,
        }
    }

    pub(crate) fn apply_required(id: &str) -> Self {
        Self {
            code: "apply_required",
            message: format!("preset {id:?} requires the journaled apply path"),
            details: None,
        }
    }

    pub(crate) fn conflict(message: impl Into<String>) -> Self {
        Self {
            code: "preset_conflict",
            message: message.into(),
            details: None,
        }
    }

    pub(crate) fn keybinding_conflict(conflict: KeybindingConflict) -> Self {
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

    pub(crate) fn io(error: impl fmt::Display) -> Self {
        Self {
            code: "io_error",
            message: error.to_string(),
            details: None,
        }
    }

    pub(crate) fn unsafe_path(path: impl fmt::Display) -> Self {
        Self {
            code: "unsafe_path",
            message: format!("refusing symlinked store path: {path}"),
            details: None,
        }
    }

    #[cfg(not(target_os = "linux"))]
    pub(crate) fn unsupported(message: impl Into<String>) -> Self {
        Self {
            code: "unsupported_platform",
            message: message.into(),
            details: None,
        }
    }

    pub(crate) fn commit_state_unknown(error: impl fmt::Display) -> Self {
        Self {
            code: "commit_state_unknown",
            message: format!(
                "the replacement may already be visible and durable state is unknown: {error}"
            ),
            details: None,
        }
    }

    pub(crate) fn binding_with_details(
        code: &'static str,
        message: String,
        details: Option<Value>,
    ) -> Self {
        Self {
            code,
            message,
            details,
        }
    }

    pub fn invalid_command() -> Self {
        Self {
            code: "invalid_command",
            message: "expected a settings, presets, keybindings, bindings, or state command with the documented arguments".to_owned(),
            details: None,
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

impl fmt::Display for StoreError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "{}: {}", self.code, self.message)
    }
}

impl Error for StoreError {}
