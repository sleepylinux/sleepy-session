use serde_json::Value;
use sleepy_sdk::{
    validate_preset, validate_settings, PresetDocument, PresetOrigin, SettingsDocument,
    BUILTIN_PRESET_ID,
};

use super::StoreError;

#[derive(Debug, Clone)]
pub struct Defaults {
    pub(crate) settings: SettingsDocument,
    pub(crate) builtins: Vec<PresetDocument>,
}

impl Defaults {
    pub fn from_json(settings: Value, presets: Vec<Value>) -> Result<Self, StoreError> {
        let settings = validate_settings(&settings.to_string())
            .map_err(|error| StoreError::invalid(error.to_string()))?;
        let builtins = presets
            .into_iter()
            .map(|preset| {
                validate_preset(&preset.to_string())
                    .map_err(|error| StoreError::invalid(error.to_string()))
            })
            .collect::<Result<Vec<_>, _>>()?;
        if builtins.is_empty()
            || builtins
                .iter()
                .any(|preset| preset.origin != PresetOrigin::Builtin)
            || builtins.iter().any(|preset| preset.id != BUILTIN_PRESET_ID)
        {
            return Err(StoreError::invalid(
                "defaults must contain only builtin.sleepy",
            ));
        }
        Ok(Self { settings, builtins })
    }
}
