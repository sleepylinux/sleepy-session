use std::{
    collections::BTreeSet,
    fmt,
    fs::{self, File, OpenOptions},
    io::{self, Write},
    path::Path,
    sync::Arc,
};

use fs2::FileExt;
use serde::Serialize;
use serde_json::{json, Value};
use sleepy_sdk::{
    packaged_reserved_keybindings, validate_keybindings_with_reserved, validate_preset,
    validate_settings, PresetDocument, PresetOrigin, SettingsDocument, BUILTIN_PRESET_ID,
};
use uuid::Uuid;

use super::{Defaults, StoreError, StorePaths};

/// Observable replacement boundary used by callers that need explicit fault injection.
pub trait ReplacementObserver: Send + Sync {
    fn reached(&self, stage: ReplacementStage) -> io::Result<()>;
}

/// The durable-replacement points at which an observer can stop or fail a write.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReplacementStage {
    TemporaryFileSynced,
    RenamedBeforeParentSync,
}

/// Observable preset-mutation boundary used for deterministic concurrency tests.
pub trait PresetMutationObserver: Send + Sync {
    fn reached(&self, stage: PresetMutationStage) -> io::Result<()>;
}

/// A preset-mutation point at which an observer may pause or fail an operation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PresetMutationStage {
    KeybindingTargetEligible,
}

#[derive(Clone)]
pub struct StateStore {
    paths: StorePaths,
    defaults: Defaults,
    replacement_observer: Option<Arc<dyn ReplacementObserver>>,
    mutation_observer: Option<Arc<dyn PresetMutationObserver>>,
}

pub struct StateInspector;

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct InspectionReport {
    pub clean: bool,
    pub settings: InspectionDocumentReport,
    pub presets: InspectionDocumentReport,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct InspectionDocumentReport {
    pub exists: bool,
    pub byte_length: usize,
    pub valid: bool,
    pub issues: Vec<InspectionIssue>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct InspectionIssue {
    pub record_index: Option<usize>,
    pub record_id: Option<String>,
    pub actions: Vec<String>,
    pub code: String,
    pub message: String,
}

impl StateInspector {
    pub fn inspect(paths: StorePaths) -> InspectionReport {
        if let Err(error) = paths.reject_symlinks() {
            let issue = inspection_issue(None, None, Vec::new(), &error);
            let document = InspectionDocumentReport {
                exists: false,
                byte_length: 0,
                valid: false,
                issues: vec![issue],
            };
            return InspectionReport {
                clean: false,
                settings: document.clone(),
                presets: document,
            };
        }

        let (mut settings, active_id) = inspect_settings(&paths);
        let (presets, preset_ids) = inspect_presets(&paths);
        if settings.valid {
            if let Some(active_id) = active_id {
                if active_id != BUILTIN_PRESET_ID && !preset_ids.contains(&active_id) {
                    let error = StoreError::invalid("settings activePresetId does not exist");
                    settings.valid = false;
                    settings
                        .issues
                        .push(inspection_issue(None, None, Vec::new(), &error));
                }
            }
        }
        InspectionReport {
            clean: settings.valid && presets.valid,
            settings,
            presets,
        }
    }
}

fn inspect_settings(paths: &StorePaths) -> (InspectionDocumentReport, Option<String>) {
    let bytes = match fs::read(paths.settings_path()) {
        Ok(bytes) => bytes,
        Err(error) => return (unreadable_document(error), None),
    };
    let byte_length = bytes.len();
    let input = match std::str::from_utf8(&bytes) {
        Ok(input) => input,
        Err(error) => {
            return (
                invalid_document_report(
                    byte_length,
                    StoreError::invalid(format!("settings are not UTF-8: {error}")),
                ),
                None,
            )
        }
    };
    match validate_settings(input) {
        Ok(settings) => (
            InspectionDocumentReport {
                exists: true,
                byte_length,
                valid: true,
                issues: Vec::new(),
            },
            Some(settings.active_preset_id),
        ),
        Err(error) => (
            invalid_document_report(byte_length, StoreError::invalid(error.to_string())),
            None,
        ),
    }
}

fn inspect_presets(paths: &StorePaths) -> (InspectionDocumentReport, BTreeSet<String>) {
    let bytes = match fs::read(paths.presets_path()) {
        Ok(bytes) => bytes,
        Err(error) => return (unreadable_document(error), BTreeSet::new()),
    };
    let byte_length = bytes.len();
    let input = match std::str::from_utf8(&bytes) {
        Ok(input) => input,
        Err(error) => {
            return (
                invalid_document_report(
                    byte_length,
                    StoreError::invalid(format!("presets are not UTF-8: {error}")),
                ),
                BTreeSet::new(),
            )
        }
    };
    let records: Vec<Value> = match serde_json::from_str(input) {
        Ok(records) => records,
        Err(error) => {
            return (
                invalid_document_report(byte_length, StoreError::invalid(error.to_string())),
                BTreeSet::new(),
            )
        }
    };

    let mut issues = Vec::new();
    let mut ids = BTreeSet::new();
    for (index, record) in records.into_iter().enumerate() {
        let record_id = record.get("id").and_then(Value::as_str).map(str::to_owned);
        match parse_preset(record) {
            Ok(preset) if preset.origin != PresetOrigin::User => {
                let error = StoreError::invalid("user preset store contains a builtin preset");
                issues.push(inspection_issue(Some(index), record_id, Vec::new(), &error));
            }
            Ok(preset) if !ids.insert(preset.id.clone()) => {
                let error = StoreError::invalid("user preset store contains duplicate ids");
                issues.push(inspection_issue(
                    Some(index),
                    Some(preset.id),
                    Vec::new(),
                    &error,
                ));
            }
            Ok(_) => {}
            Err(error) => {
                let actions = error
                    .details()
                    .and_then(|details| details.get("actions"))
                    .and_then(Value::as_array)
                    .map(|actions| {
                        actions
                            .iter()
                            .filter_map(Value::as_str)
                            .map(str::to_owned)
                            .collect()
                    })
                    .unwrap_or_default();
                issues.push(inspection_issue(Some(index), record_id, actions, &error));
            }
        }
    }

    (
        InspectionDocumentReport {
            exists: true,
            byte_length,
            valid: issues.is_empty(),
            issues,
        },
        ids,
    )
}

fn unreadable_document(error: io::Error) -> InspectionDocumentReport {
    let exists = error.kind() != io::ErrorKind::NotFound;
    let error = StoreError::io(error);
    InspectionDocumentReport {
        exists,
        byte_length: 0,
        valid: false,
        issues: vec![inspection_issue(None, None, Vec::new(), &error)],
    }
}

fn invalid_document_report(byte_length: usize, error: StoreError) -> InspectionDocumentReport {
    InspectionDocumentReport {
        exists: true,
        byte_length,
        valid: false,
        issues: vec![inspection_issue(None, None, Vec::new(), &error)],
    }
}

fn inspection_issue(
    record_index: Option<usize>,
    record_id: Option<String>,
    actions: Vec<String>,
    error: &StoreError,
) -> InspectionIssue {
    InspectionIssue {
        record_index,
        record_id,
        actions,
        code: error.code().to_owned(),
        message: error.message().to_owned(),
    }
}

impl fmt::Debug for StateStore {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("StateStore")
            .field("paths", &self.paths)
            .field("defaults", &self.defaults)
            .finish_non_exhaustive()
    }
}

impl StateStore {
    pub fn open(paths: StorePaths, defaults: Defaults) -> Result<Self, StoreError> {
        let store = Self {
            paths,
            defaults,
            replacement_observer: None,
            mutation_observer: None,
        };
        store.with_transaction(|store| {
            store.initialize()?;
            let settings = store.load_settings()?;
            store.load_user_presets()?;
            if store.find_preset(&settings.active_preset_id)?.is_none() {
                return Err(StoreError::invalid(
                    "settings activePresetId does not exist",
                ));
            }
            Ok(())
        })?;
        Ok(store)
    }

    /// Adds a replacement-stage observer to a cloned store handle.
    pub fn with_replacement_observer(mut self, observer: Arc<dyn ReplacementObserver>) -> Self {
        self.replacement_observer = Some(observer);
        self
    }

    /// Adds a preset-mutation observer to a cloned store handle.
    pub fn with_mutation_observer(mut self, observer: Arc<dyn PresetMutationObserver>) -> Self {
        self.mutation_observer = Some(observer);
        self
    }

    pub fn settings_json(&self) -> Result<Value, StoreError> {
        self.with_transaction(|store| {
            serde_json::to_value(store.load_settings()?)
                .map_err(|error| StoreError::invalid(error.to_string()))
        })
    }

    pub fn presets_json(&self) -> Result<Value, StoreError> {
        self.with_transaction(|store| {
            let mut presets = store.defaults.builtins.clone();
            let mut users = store.load_user_presets()?;
            users.sort_by(|left, right| left.id.cmp(&right.id));
            presets.extend(users);
            Ok(json!({ "presets": presets }))
        })
    }

    pub fn create_user_preset(&self, preset: Value) -> Result<Value, StoreError> {
        self.with_transaction(|store| {
            let preset = parse_preset(preset)?;
            if preset.origin != PresetOrigin::User {
                return Err(StoreError::invalid("created presets must have origin user"));
            }
            if store.find_preset(&preset.id)?.is_some() {
                return Err(StoreError::invalid("preset id already exists"));
            }
            let mut users = store.load_user_presets()?;
            users.push(preset.clone());
            store.write_user_presets(&users)?;
            Ok(json!({ "preset": preset }))
        })
    }

    pub fn duplicate_preset(&self, source_id: &str, name: &str) -> Result<Value, StoreError> {
        self.with_transaction(|store| {
            let mut preset = store
                .find_preset(source_id)?
                .ok_or_else(|| StoreError::not_found(source_id))?;
            preset.id = Uuid::new_v4().hyphenated().to_string();
            preset.name = checked_name(name)?;
            preset.origin = PresetOrigin::User;
            preset.base_preset_id = Some(source_id.to_owned());
            let mut users = store.load_user_presets()?;
            users.push(preset.clone());
            store.write_user_presets(&users)?;
            Ok(json!({ "preset": preset }))
        })
    }

    pub fn rename_preset(&self, id: &str, name: &str) -> Result<Value, StoreError> {
        self.with_transaction(|store| {
            if id == BUILTIN_PRESET_ID {
                return Err(StoreError::immutable(id));
            }
            let mut users = store.load_user_presets()?;
            let preset = users
                .iter_mut()
                .find(|preset| preset.id == id)
                .ok_or_else(|| StoreError::not_found(id))?;
            preset.name = checked_name(name)?;
            let result = preset.clone();
            store.write_user_presets(&users)?;
            Ok(json!({ "preset": result }))
        })
    }

    pub fn activate_preset(&self, id: &str) -> Result<Value, StoreError> {
        self.with_transaction(|store| {
            if store.find_preset(id)?.is_none() {
                return Err(StoreError::not_found(id));
            }
            let mut settings = store.load_settings()?;
            settings.active_preset_id = id.to_owned();
            store.write_settings(&settings)?;
            serde_json::to_value(settings).map_err(|error| StoreError::invalid(error.to_string()))
        })
    }

    pub fn replace_settings_json(&self, settings: Value) -> Result<Value, StoreError> {
        self.with_transaction(|store| {
            let settings = parse_settings(settings)?;
            if store.find_preset(&settings.active_preset_id)?.is_none() {
                return Err(StoreError::invalid(
                    "settings activePresetId does not exist",
                ));
            }
            store.write_settings(&settings)?;
            serde_json::to_value(settings).map_err(|error| StoreError::invalid(error.to_string()))
        })
    }

    pub(super) fn with_transaction<T>(
        &self,
        operation: impl FnOnce(&Self) -> Result<T, StoreError>,
    ) -> Result<T, StoreError> {
        // A single configuration-root lock establishes the order for all settings/preset operations.
        self.paths.reject_symlinks()?;
        fs::create_dir_all(self.paths.settings_dir()).map_err(StoreError::io)?;
        self.paths.reject_symlinks()?;
        let lock = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(self.paths.lock_path())
            .map_err(StoreError::io)?;
        lock.lock_exclusive().map_err(StoreError::io)?;
        let result = operation(self);
        FileExt::unlock(&lock).map_err(StoreError::io)?;
        result
    }

    pub(super) fn observe_mutation(&self, stage: PresetMutationStage) -> Result<(), StoreError> {
        if let Some(observer) = self.mutation_observer.as_deref() {
            observer.reached(stage).map_err(StoreError::io)?;
        }
        Ok(())
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

    pub(super) fn load_settings(&self) -> Result<SettingsDocument, StoreError> {
        let input = fs::read_to_string(self.paths.settings_path()).map_err(StoreError::io)?;
        validate_settings(&input).map_err(|error| StoreError::invalid(error.to_string()))
    }

    pub(super) fn load_user_presets(&self) -> Result<Vec<PresetDocument>, StoreError> {
        let input = fs::read_to_string(self.paths.presets_path()).map_err(StoreError::io)?;
        let presets: Vec<Value> =
            serde_json::from_str(&input).map_err(|error| StoreError::invalid(error.to_string()))?;
        let presets = presets
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
            .collect::<Result<Vec<_>, _>>()?;
        let ids = presets
            .iter()
            .map(|preset| preset.id.as_str())
            .collect::<BTreeSet<_>>();
        if ids.len() != presets.len() {
            return Err(StoreError::invalid(
                "user preset store contains duplicate ids",
            ));
        }
        Ok(presets)
    }

    pub(super) fn find_preset(&self, id: &str) -> Result<Option<PresetDocument>, StoreError> {
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
            self.replacement_observer.as_deref(),
        )
    }

    pub(super) fn write_user_presets(&self, presets: &[PresetDocument]) -> Result<(), StoreError> {
        let ids = presets
            .iter()
            .map(|preset| preset.id.as_str())
            .collect::<BTreeSet<_>>();
        if ids.len() != presets.len() {
            return Err(StoreError::invalid(
                "user preset store contains duplicate ids",
            ));
        }
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
            self.replacement_observer.as_deref(),
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

pub(super) fn parse_preset(value: Value) -> Result<PresetDocument, StoreError> {
    if let Ok(document) = serde_json::from_value::<PresetDocument>(value.clone()) {
        validate_keybindings_with_reserved(&document.keybindings, &packaged_reserved_keybindings())
            .map_err(StoreError::keybinding_conflict)?;
    }
    validate_preset(&value.to_string()).map_err(|error| StoreError::invalid(error.to_string()))
}

fn atomic_replace(
    directory: &Path,
    destination: &Path,
    bytes: &[u8],
    observer: Option<&dyn ReplacementObserver>,
) -> Result<(), StoreError> {
    fs::create_dir_all(directory).map_err(StoreError::io)?;
    let temporary = directory.join(format!(
        ".{}.{}.tmp",
        destination
            .file_name()
            .and_then(|name| name.to_str())
            .unwrap_or("state"),
        Uuid::new_v4()
    ));
    let mut replacement = OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(&temporary)
        .map_err(StoreError::io)?;
    let result = replacement
        .write_all(bytes)
        .and_then(|()| replacement.sync_all());
    if let Err(error) = result {
        let _ = fs::remove_file(&temporary);
        return Err(StoreError::io(error));
    }
    if let Some(observer) = observer {
        if let Err(error) = observer.reached(ReplacementStage::TemporaryFileSynced) {
            let _ = fs::remove_file(&temporary);
            return Err(StoreError::io(error));
        }
    }
    drop(replacement);
    if let Err(error) = fs::rename(&temporary, destination) {
        let _ = fs::remove_file(&temporary);
        return Err(StoreError::io(error));
    }
    if let Some(observer) = observer {
        if let Err(error) = observer.reached(ReplacementStage::RenamedBeforeParentSync) {
            return Err(StoreError::commit_state_unknown(error));
        }
    }
    File::open(directory)
        .and_then(|file| file.sync_all())
        .map_err(StoreError::commit_state_unknown)
}
