use std::{error::Error, fmt};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StoreError {
    code: &'static str,
    message: String,
}

impl StoreError {
    pub(crate) fn invalid(message: impl Into<String>) -> Self {
        Self {
            code: "invalid_document",
            message: message.into(),
        }
    }

    pub(crate) fn not_found(id: &str) -> Self {
        Self {
            code: "preset_not_found",
            message: format!("preset {id:?} was not found"),
        }
    }

    pub(crate) fn immutable(id: &str) -> Self {
        Self {
            code: "immutable_preset",
            message: format!("preset {id:?} is immutable"),
        }
    }

    pub(crate) fn io(error: impl fmt::Display) -> Self {
        Self {
            code: "io_error",
            message: error.to_string(),
        }
    }

    pub(crate) fn unsafe_path(path: impl fmt::Display) -> Self {
        Self {
            code: "unsafe_path",
            message: format!("refusing symlinked store path: {path}"),
        }
    }

    pub(crate) fn commit_state_unknown(error: impl fmt::Display) -> Self {
        Self {
            code: "commit_state_unknown",
            message: format!(
                "the replacement may already be visible and durable state is unknown: {error}"
            ),
        }
    }

    pub fn invalid_command() -> Self {
        Self {
            code: "invalid_command",
            message: "expected: settings show | presets list | presets duplicate <id> <name> | presets rename <id> <name> | presets activate <id>".to_owned(),
        }
    }

    pub fn code(&self) -> &'static str {
        self.code
    }

    pub fn message(&self) -> &str {
        &self.message
    }
}

impl fmt::Display for StoreError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "{}: {}", self.code, self.message)
    }
}

impl Error for StoreError {}
