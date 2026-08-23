use std::{
    fs::{self, File, OpenOptions},
    io::Write,
    path::Path,
};

use serde_json::{json, Value};
use sleepy_sdk::{
    validate_preset, validate_settings, PresetDocument, PresetOrigin, SettingsDocument,
    BUILTIN_PRESET_ID,
};
use uuid::Uuid;

use super::{Defaults, StoreError, StorePaths};

#[derive(Debug, Clone)]
pub struct StateStore {
    paths: StorePaths,
    defaults: Defaults,
}

impl StateStore {
    pub fn open(paths: StorePaths, defaults: Defaults) -> Result<Self, StoreError> {
        let store = Self { paths, defaults };
        store.initialize()?;
        let settings = store.load_settings()?;
        store.load_user_presets()?;
        if store.find_preset(&settings.active_preset_id)?.is_none() {
            return Err(StoreError::invalid(
                "settings activePresetId does not exist",
            ));
        }
        Ok(store)
    }

    pub fn settings_json(&self) -> Result<Value, StoreError> {
        serde_json::to_value(self.load_settings()?)
            .map_err(|error| StoreError::invalid(error.to_string()))
    }

    pub fn presets_json(&self) -> Result<Value, StoreError> {
        let mut presets = self.defaults.builtins.clone();
        let mut users = self.load_user_presets()?;
        users.sort_by(|left, right| left.id.cmp(&right.id));
        presets.extend(users);
        Ok(json!({ "presets": presets }))
    }

    pub fn create_user_preset(&self, preset: Value) -> Result<Value, StoreError> {
        let preset = parse_preset(preset)?;
        if preset.origin != PresetOrigin::User {
            return Err(StoreError::invalid("created presets must have origin user"));
        }
        if self.find_preset(&preset.id)?.is_some() {
            return Err(StoreError::invalid("preset id already exists"));
        }
        let mut users = self.load_user_presets()?;
        users.push(preset.clone());
        self.write_user_presets(&users)?;
        Ok(json!({ "preset": preset }))
    }

    pub fn duplicate_preset(&self, source_id: &str, name: &str) -> Result<Value, StoreError> {
        let mut preset = self
            .find_preset(source_id)?
            .ok_or_else(|| StoreError::not_found(source_id))?;
        preset.id = Uuid::new_v4().hyphenated().to_string();
        preset.name = checked_name(name)?;
        preset.origin = PresetOrigin::User;
        preset.base_preset_id = Some(source_id.to_owned());
        let mut users = self.load_user_presets()?;
        users.push(preset.clone());
        self.write_user_presets(&users)?;
        Ok(json!({ "preset": preset }))
    }

    pub fn rename_preset(&self, id: &str, name: &str) -> Result<Value, StoreError> {
        if id == BUILTIN_PRESET_ID {
            return Err(StoreError::immutable(id));
        }
        let mut users = self.load_user_presets()?;
        let preset = users
            .iter_mut()
            .find(|preset| preset.id == id)
            .ok_or_else(|| StoreError::not_found(id))?;
        preset.name = checked_name(name)?;
        let result = preset.clone();
        self.write_user_presets(&users)?;
        Ok(json!({ "preset": result }))
    }

    pub fn activate_preset(&self, id: &str) -> Result<Value, StoreError> {
        if self.find_preset(id)?.is_none() {
            return Err(StoreError::not_found(id));
        }
        let mut settings = self.load_settings()?;
        settings.active_preset_id = id.to_owned();
        self.write_settings(&settings)?;
        self.settings_json()
    }

    pub fn replace_settings_json(&self, settings: Value) -> Result<Value, StoreError> {
        let settings = parse_settings(settings)?;
        if self.find_preset(&settings.active_preset_id)?.is_none() {
            return Err(StoreError::invalid(
                "settings activePresetId does not exist",
            ));
        }
        self.write_settings(&settings)?;
        self.settings_json()
    }

    fn initialize(&self) -> Result<(), StoreError> {
        if !self.paths.settings_path().exists() {
            self.write_settings(&self.defaults.settings)?;
        }
        if !self.paths.presets_path().exists() {
            self.write_user_presets(&[])?;
        }
        Ok(())
    }

    fn load_settings(&self) -> Result<SettingsDocument, StoreError> {
        let input = fs::read_to_string(self.paths.settings_path()).map_err(StoreError::io)?;
        validate_settings(&input).map_err(|error| StoreError::invalid(error.to_string()))
    }

    fn load_user_presets(&self) -> Result<Vec<PresetDocument>, StoreError> {
        let input = fs::read_to_string(self.paths.presets_path()).map_err(StoreError::io)?;
        let presets: Vec<Value> =
            serde_json::from_str(&input).map_err(|error| StoreError::invalid(error.to_string()))?;
        presets
            .into_iter()
            .map(parse_preset)
            .map(|result| {
                result.and_then(|preset| {
                    if preset.origin == PresetOrigin::User {
                        Ok(preset)
                    } else {
                        Err(StoreError::invalid(
                            "user preset store contains a builtin preset",
                        ))
                    }
                })
            })
            .collect()
    }

    fn find_preset(&self, id: &str) -> Result<Option<PresetDocument>, StoreError> {
        if let Some(preset) = self.defaults.builtins.iter().find(|preset| preset.id == id) {
            return Ok(Some(preset.clone()));
        }
        Ok(self
            .load_user_presets()?
            .into_iter()
            .find(|preset| preset.id == id))
    }

    fn write_settings(&self, settings: &SettingsDocument) -> Result<(), StoreError> {
        let value = serde_json::to_value(settings)
            .map_err(|error| StoreError::invalid(error.to_string()))?;
        let validated = parse_settings(value)?;
        atomic_replace(
            &self.paths.settings_dir(),
            &self.paths.settings_path(),
            &serde_json::to_vec(&validated).map_err(StoreError::io)?,
        )
    }

    fn write_user_presets(&self, presets: &[PresetDocument]) -> Result<(), StoreError> {
        for preset in presets {
            if preset.origin != PresetOrigin::User {
                return Err(StoreError::invalid(
                    "user preset store contains a builtin preset",
                ));
            }
            parse_preset(serde_json::to_value(preset).map_err(StoreError::io)?)?;
        }
        let mut sorted = presets.to_vec();
        sorted.sort_by(|left, right| left.id.cmp(&right.id));
        atomic_replace(
            &self.paths.presets_dir(),
            &self.paths.presets_path(),
            &serde_json::to_vec(&sorted).map_err(StoreError::io)?,
        )
    }
}

fn checked_name(name: &str) -> Result<String, StoreError> {
    if name.trim().is_empty() {
        Err(StoreError::invalid("preset name must not be empty"))
    } else {
        Ok(name.to_owned())
    }
}

fn parse_settings(value: Value) -> Result<SettingsDocument, StoreError> {
    validate_settings(&value.to_string()).map_err(|error| StoreError::invalid(error.to_string()))
}

fn parse_preset(value: Value) -> Result<PresetDocument, StoreError> {
    validate_preset(&value.to_string()).map_err(|error| StoreError::invalid(error.to_string()))
}

fn atomic_replace(directory: &Path, destination: &Path, bytes: &[u8]) -> Result<(), StoreError> {
    fs::create_dir_all(directory).map_err(StoreError::io)?;
    let temporary = directory.join(format!(
        ".{}.{}.tmp",
        destination
            .file_name()
            .and_then(|name| name.to_str())
            .unwrap_or("state"),
        Uuid::new_v4()
    ));
    let replacement = OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(&temporary)
        .map_err(StoreError::io);
    let mut replacement = replacement?;
    let result = replacement
        .write_all(bytes)
        .and_then(|()| replacement.sync_all());
    if let Err(error) = result {
        let _ = fs::remove_file(&temporary);
        return Err(StoreError::io(error));
    }
    drop(replacement);
    if let Err(error) = fs::rename(&temporary, destination) {
        let _ = fs::remove_file(&temporary);
        return Err(StoreError::io(error));
    }
    File::open(directory)
        .and_then(|file| file.sync_all())
        .map_err(StoreError::io)
}
