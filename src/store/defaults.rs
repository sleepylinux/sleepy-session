use serde_json::Value;
use sleepy_sdk::{
    validate_preset, validate_settings, PresetDocument, PresetOrigin, SettingsDocument,
    BUILTIN_PRESET_ID,
};
use std::collections::BTreeSet;

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
        let builtin_ids = builtins
            .iter()
            .map(|preset| preset.id.as_str())
            .collect::<BTreeSet<_>>();
        if builtins.is_empty()
            || builtins
                .iter()
                .any(|preset| preset.origin != PresetOrigin::Builtin)
            || builtins.iter().any(|preset| preset.id != BUILTIN_PRESET_ID)
            || builtin_ids.len() != builtins.len()
            || !builtin_ids.contains(settings.active_preset_id.as_str())
        {
            return Err(StoreError::invalid(
                "defaults must contain only builtin.sleepy",
            ));
        }
        Ok(Self { settings, builtins })
    }
}
