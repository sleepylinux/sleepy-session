use serde_json::{json, Value};
use sleepy_sdk::{PresetOrigin, BUILTIN_PRESET_ID};

use super::{state::parse_preset, StateStore, StoreError};

impl StateStore {
    pub fn preset_json(&self, id: &str) -> Result<Value, StoreError> {
        self.with_transaction(|store| {
            let preset = store
                .find_preset(id)?
                .ok_or_else(|| StoreError::not_found(id))?;
            serde_json::to_value(preset).map_err(StoreError::io)
        })
    }

    pub fn active_preset_json(&self) -> Result<Value, StoreError> {
        self.with_transaction(|store| {
            let active_id = store.load_settings()?.active_preset_id;
            let preset = store
                .find_preset(&active_id)?
                .ok_or_else(|| StoreError::not_found(&active_id))?;
            serde_json::to_value(preset).map_err(StoreError::io)
        })
    }

    pub fn update_user_preset(&self, id: &str, candidate: Value) -> Result<Value, StoreError> {
        self.with_transaction(|store| {
            let existing = store
                .find_preset(id)?
                .ok_or_else(|| StoreError::not_found(id))?;
            if existing.origin == PresetOrigin::Builtin || id == BUILTIN_PRESET_ID {
                return Err(StoreError::immutable(id));
            }

            let candidate = parse_preset(candidate)?;
            if candidate.origin != PresetOrigin::User {
                return Err(StoreError::immutable(id));
            }
            if candidate.id != id {
                return Err(StoreError::conflict(format!(
                    "candidate id {:?} does not match target {id:?}",
                    candidate.id
                )));
            }
            if store.load_settings()?.active_preset_id == id {
                return Err(StoreError::apply_required(id));
            }

            let mut users = store.load_user_presets()?;
            let position = users
                .iter()
                .position(|preset| preset.id == id)
                .ok_or_else(|| StoreError::not_found(id))?;
            users[position] = candidate.clone();
            store.write_user_presets(&users)?;
            Ok(json!({ "preset": candidate }))
        })
    }

    pub fn delete_user_preset(&self, id: &str) -> Result<Value, StoreError> {
        self.with_transaction(|store| {
            let existing = store
                .find_preset(id)?
                .ok_or_else(|| StoreError::not_found(id))?;
            if existing.origin == PresetOrigin::Builtin || id == BUILTIN_PRESET_ID {
                return Err(StoreError::immutable(id));
            }
            if store.load_settings()?.active_preset_id == id {
                return Err(StoreError::active(id));
            }

            let mut users = store.load_user_presets()?;
            let position = users
                .iter()
                .position(|preset| preset.id == id)
                .ok_or_else(|| StoreError::not_found(id))?;
            let removed = users.remove(position);
            store.write_user_presets(&users)?;
            Ok(json!({ "preset": removed }))
        })
    }
}
