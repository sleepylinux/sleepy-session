use serde_json::{json, Value};
use sleepy_sdk::{PresetDocument, PresetOrigin};
use uuid::Uuid;

use super::{state::parse_preset, StateStore, StoreError};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ImportMode {
    Reject,
    Copy,
    Replace,
}

impl StateStore {
    pub fn export_preset(&self, id: &str) -> Result<Value, StoreError> {
        self.preset_json(id)
    }

    pub fn import_preset(&self, candidate: Value, mode: ImportMode) -> Result<Value, StoreError> {
        self.with_transaction(|store| {
            let mut candidate = parse_preset(candidate)?;

            if candidate.origin == PresetOrigin::Builtin || mode == ImportMode::Copy {
                candidate = imported_copy(candidate);
                let mut users = store.load_user_presets()?;
                users.push(candidate.clone());
                store.write_user_presets(&users)?;
                return Ok(json!({ "preset": candidate }));
            }

            let existing = store.find_preset(&candidate.id)?;
            match (existing, mode) {
                (Some(_), ImportMode::Reject) => Err(StoreError::conflict(format!(
                    "preset id {:?} already exists",
                    candidate.id
                ))),
                (Some(existing), ImportMode::Replace) => {
                    if existing.origin == PresetOrigin::Builtin {
                        return Err(StoreError::immutable(&candidate.id));
                    }
                    if store.load_settings()?.active_preset_id == candidate.id {
                        return Err(StoreError::apply_required(&candidate.id));
                    }
                    let mut users = store.load_user_presets()?;
                    let position = users
                        .iter()
                        .position(|preset| preset.id == candidate.id)
                        .ok_or_else(|| StoreError::not_found(&candidate.id))?;
                    users[position] = candidate.clone();
                    store.write_user_presets(&users)?;
                    Ok(json!({ "preset": candidate }))
                }
                (None, ImportMode::Reject | ImportMode::Replace) => {
                    let mut users = store.load_user_presets()?;
                    users.push(candidate.clone());
                    store.write_user_presets(&users)?;
                    Ok(json!({ "preset": candidate }))
                }
                (_, ImportMode::Copy) => unreachable!("copy mode handled above"),
            }
        })
    }

    pub fn validate_preset_candidate(&self, candidate: Value) -> Result<Value, StoreError> {
        let candidate = parse_preset(candidate)?;
        serde_json::to_value(candidate).map_err(StoreError::io)
    }
}

fn imported_copy(mut candidate: PresetDocument) -> PresetDocument {
    let source_id = candidate.id.clone();
    candidate.id = Uuid::new_v4().hyphenated().to_string();
    candidate.origin = PresetOrigin::User;
    candidate.base_preset_id = Some(source_id);
    candidate
}
