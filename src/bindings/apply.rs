use std::{
    collections::BTreeSet,
    fs,
    io::{BufRead, BufReader},
    os::unix::fs::{FileTypeExt, MetadataExt},
    path::{Path, PathBuf},
    process::{Child, Command, Stdio},
    sync::mpsc::{self, Receiver, RecvTimeoutError},
    sync::Arc,
    thread,
    time::{Duration, Instant},
};

use serde::{Deserialize, Serialize};
use serde_json::Value;
use sleepy_sdk::{
    canonicalize_accelerator, validate_settings, PresetDocument, PresetOrigin, SettingsDocument,
    BUILTIN_PRESET_ID,
};
use uuid::Uuid;

use crate::{store::StateCandidate, Defaults, StateStore, StorePaths};

use super::{
    compile_bindings,
    journal::{io_error, ArtifactKind, JournalPhase, RecoveryTarget, TransactionJournal},
    secure_fs::BindingFileSystem,
    BindingError,
};

const RELOAD_TIMEOUT: Duration = Duration::from_secs(5);

pub trait BindingValidator {
    fn validate(&self, staged_root: &Path, staged_config: &Path) -> Result<(), String>;
}

pub trait ConfigEventStream {
    fn await_initial_snapshot(&mut self, timeout: Duration) -> Result<ConfigLoaded, String>;
    fn next_config_loaded(&mut self, timeout: Duration) -> Result<Option<ConfigLoaded>, String>;
}

pub trait BindingReloader {
    fn subscribe(&self) -> Result<Option<Box<dyn ConfigEventStream>>, String>;
    fn subscribe_required(&self, _timeout: Duration) -> Result<Box<dyn ConfigEventStream>, String> {
        self.subscribe()?
            .ok_or_else(|| "Niri socket or event stream is unavailable".to_owned())
    }
    fn request_reload(&self, trusted_config: &Path) -> Result<(), String>;
}

#[derive(Debug, Clone)]
pub struct NiriValidator {
    executable: PathBuf,
}

impl NiriValidator {
    pub fn new(executable: impl Into<PathBuf>) -> Self {
        Self {
            executable: executable.into(),
        }
    }
}

impl BindingValidator for NiriValidator {
    fn validate(&self, staged_root: &Path, staged_config: &Path) -> Result<(), String> {
        let output = Command::new(&self.executable)
            .args(["validate", "--config"])
            .arg(staged_config)
            .current_dir(staged_root)
            .output()
            .map_err(|error| format!("failed to execute pinned Niri validator: {error}"))?;
        if output.status.success() {
            Ok(())
        } else {
            Err(format!(
                "Niri validation failed: {}",
                String::from_utf8_lossy(&output.stderr).trim()
            ))
        }
    }
}

#[derive(Debug, Clone)]
pub struct NiriReloader {
    executable: PathBuf,
}

impl NiriReloader {
    pub fn new(executable: impl Into<PathBuf>) -> Self {
        Self {
            executable: executable.into(),
        }
    }
}

impl BindingReloader for NiriReloader {
    fn subscribe(&self) -> Result<Option<Box<dyn ConfigEventStream>>, String> {
        if std::env::var_os("NIRI_SOCKET").is_none() {
            return Ok(None);
        }
        ensure_supported_niri(&self.executable)?;
        let mut child = Command::new(&self.executable)
            .args(["msg", "--json", "event-stream"])
            .stdout(Stdio::piped())
            .stderr(Stdio::null())
            .spawn()
            .map_err(|error| format!("failed to subscribe to Niri event stream: {error}"))?;
        let stdout = child
            .stdout
            .take()
            .ok_or_else(|| "Niri event stream did not expose stdout".to_owned())?;
        let (sender, receiver) = mpsc::channel();
        thread::spawn(move || {
            for line in BufReader::new(stdout).lines().map_while(Result::ok) {
                if let Some(message) = parse_stream_message(&line) {
                    if sender.send(message).is_err() {
                        break;
                    }
                }
            }
        });
        Ok(Some(Box::new(NiriEventStream { child, receiver })))
    }

    fn request_reload(&self, trusted_config: &Path) -> Result<(), String> {
        let status = Command::new(&self.executable)
            .args(["msg", "action", "load-config-file", "--path"])
            .arg(trusted_config)
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .status()
            .map_err(|error| format!("failed to request Niri config reload: {error}"))?;
        if status.success() {
            Ok(())
        } else {
            Err(format!("Niri reload request exited with {status}"))
        }
    }

    fn subscribe_required(&self, timeout: Duration) -> Result<Box<dyn ConfigEventStream>, String> {
        let deadline = Instant::now() + timeout;
        loop {
            if let Some(socket) = std::env::var_os("NIRI_SOCKET") {
                if std::fs::symlink_metadata(socket)
                    .map(|metadata| metadata.file_type().is_socket())
                    .unwrap_or(false)
                {
                    if let Some(stream) = self.subscribe()? {
                        return Ok(stream);
                    }
                }
            }
            if Instant::now() >= deadline {
                return Err("timed out waiting for Niri socket and event stream".to_owned());
            }
            thread::sleep(Duration::from_millis(10));
        }
    }
}

struct NiriEventStream {
    child: Child,
    receiver: Receiver<NiriStreamMessage>,
}

impl ConfigEventStream for NiriEventStream {
    fn await_initial_snapshot(&mut self, timeout: Duration) -> Result<ConfigLoaded, String> {
        let deadline = Instant::now() + timeout;
        let mut config = None;
        loop {
            let remaining = deadline.saturating_duration_since(Instant::now());
            match self.receiver.recv_timeout(remaining) {
                Ok(NiriStreamMessage::ConfigLoaded(event)) if config.is_none() => {
                    config = Some(event);
                }
                Ok(NiriStreamMessage::ConfigLoaded(_)) => {
                    return Err(
                        "Niri initial snapshot contained multiple ConfigLoaded events".to_owned(),
                    );
                }
                Ok(NiriStreamMessage::InitialSnapshotComplete) => {
                    return config.ok_or_else(|| {
                        "Niri initial snapshot ended before ConfigLoaded (requires Niri >= 26.04)"
                            .to_owned()
                    });
                }
                Err(RecvTimeoutError::Timeout) => {
                    return Err("timed out awaiting complete Niri initial snapshot".to_owned());
                }
                Err(RecvTimeoutError::Disconnected) => {
                    return Err(
                        "Niri event stream closed before initial snapshot completed".to_owned()
                    );
                }
            }
        }
    }

    fn next_config_loaded(&mut self, timeout: Duration) -> Result<Option<ConfigLoaded>, String> {
        match self.receiver.recv_timeout(timeout) {
            Ok(NiriStreamMessage::ConfigLoaded(event)) => Ok(Some(event)),
            Ok(NiriStreamMessage::InitialSnapshotComplete) => {
                Err("Niri emitted an unexpected second initial snapshot marker".to_owned())
            }
            Err(RecvTimeoutError::Timeout) => Ok(None),
            Err(RecvTimeoutError::Disconnected) => {
                Err("Niri event stream closed before ConfigLoaded".to_owned())
            }
        }
    }
}

impl Drop for NiriEventStream {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum NiriStreamMessage {
    ConfigLoaded(ConfigLoaded),
    InitialSnapshotComplete,
}

fn parse_stream_message(line: &str) -> Option<NiriStreamMessage> {
    let value: Value = serde_json::from_str(line).ok()?;
    if value.get("CastsChanged").is_some() {
        return Some(NiriStreamMessage::InitialSnapshotComplete);
    }
    let payload = value.get("ConfigLoaded")?;
    let failed = payload
        .get("failed")
        .and_then(Value::as_bool)
        .or_else(|| payload.as_bool())?;
    Some(NiriStreamMessage::ConfigLoaded(ConfigLoaded { failed }))
}

fn ensure_supported_niri(executable: &Path) -> Result<(), String> {
    let output = Command::new(executable)
        .arg("--version")
        .output()
        .map_err(|error| format!("failed to query pinned Niri version: {error}"))?;
    if !output.status.success() {
        return Err("pinned Niri version query failed".to_owned());
    }
    let version = String::from_utf8_lossy(&output.stdout);
    let supported = version.split_whitespace().find_map(|word| {
        let (major, minor) = word.split_once('.')?;
        let major = major.parse::<u32>().ok()?;
        let minor = minor
            .trim_end_matches(|character: char| !character.is_ascii_digit())
            .parse::<u32>()
            .ok()?;
        Some(major > 26 || (major == 26 && minor >= 4))
    });
    match supported {
        Some(true) => Ok(()),
        _ => Err(format!(
            "unsupported Niri event protocol {version:?}; requires Niri >= 26.04"
        )),
    }
}

pub trait ApplyObserver: Send + Sync {
    fn reached(&self, stage: ApplyStage) -> Result<(), String>;
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ApplyStage {
    WritableDirectoriesOpened,
    PreparedSynced,
    PresetRenamed,
    PresetDirectorySynced,
    PresetCommittedSynced,
    SettingsRenamed,
    SettingsDirectorySynced,
    SettingsCommittedSynced,
    BindingsRenamed,
    BindingsDirectorySynced,
    BindingsCommittedSynced,
    ReloadPendingSynced,
    ReloadRequested,
    ReloadConfirmedSynced,
    ArtifactsRemoved,
    ArtifactDirectoriesSynced,
    JournalRemoved,
    JournalDirectorySynced,
    RollbackPresetRenamed,
    RollbackPresetDirectorySynced,
    RollbackSettingsRenamed,
    RollbackSettingsDirectorySynced,
    RollbackBindingsRenamed,
    RollbackBindingsDirectorySynced,
    PresetOldSidecarSynced,
    PresetNewSidecarSynced,
    SettingsOldSidecarSynced,
    SettingsNewSidecarSynced,
    BindingsOldSidecarSynced,
    BindingsNewSidecarSynced,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ConfigLoaded {
    pub failed: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub enum ApplyStatus {
    Committed,
    RolledBackConfirmed,
    CommitStateUnknown,
    ReloadPending,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ApplyReport {
    pub status: ApplyStatus,
    pub active_preset_id: String,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct RepairBundle {
    pub settings: Value,
    pub presets: Vec<Value>,
}

#[derive(Clone)]
pub struct BindingPaths {
    store: StorePaths,
    niri_root: PathBuf,
    trusted_config: PathBuf,
    generated_include: PathBuf,
    journal: PathBuf,
    recovery_root: PathBuf,
    observer: Option<Arc<dyn ApplyObserver>>,
}

impl std::fmt::Debug for BindingPaths {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("BindingPaths")
            .field("store", &self.store)
            .field("niri_root", &self.niri_root)
            .field("trusted_config", &self.trusted_config)
            .field("generated_include", &self.generated_include)
            .field("journal", &self.journal)
            .field("recovery_root", &self.recovery_root)
            .finish_non_exhaustive()
    }
}

impl BindingPaths {
    pub fn from_xdg_roots(config_root: impl Into<PathBuf>, state_root: impl Into<PathBuf>) -> Self {
        let store = StorePaths::from_xdg_roots(config_root, state_root);
        let niri_root = store.config_root().join("niri");
        Self {
            trusted_config: niri_root.join("config.kdl"),
            generated_include: niri_root.join("sleepy-user-bindings.kdl"),
            journal: store.state_root().join("sleepy/bindings-transaction.json"),
            recovery_root: store.state_root().join("sleepy/recovery"),
            store,
            niri_root,
            observer: None,
        }
    }

    pub fn from_environment() -> Self {
        let store = StorePaths::from_environment();
        Self::from_xdg_roots(store.config_root(), store.state_root())
    }

    pub fn store(&self) -> &StorePaths {
        &self.store
    }

    pub fn niri_root(&self) -> &Path {
        &self.niri_root
    }

    pub fn trusted_config(&self) -> &Path {
        &self.trusted_config
    }

    pub fn generated_include(&self) -> &Path {
        &self.generated_include
    }

    pub fn journal(&self) -> &Path {
        &self.journal
    }

    pub fn recovery_root(&self) -> &Path {
        &self.recovery_root
    }

    pub fn with_observer(mut self, observer: Arc<dyn ApplyObserver>) -> Self {
        self.observer = Some(observer);
        self
    }

    pub(crate) fn observe(&self, stage: ApplyStage) -> Result<(), BindingError> {
        if let Some(observer) = self.observer.as_deref() {
            observer
                .reached(stage)
                .map_err(|message| BindingError::new("fault_injected", message))?;
        }
        Ok(())
    }

    pub fn validate(&self) -> Result<(), BindingError> {
        let expected = Self::from_xdg_roots(self.store.config_root(), self.store.state_root());
        if !self.store.config_root().is_absolute()
            || !self.store.state_root().is_absolute()
            || self.niri_root != expected.niri_root
            || self.journal != expected.journal
            || self.generated_include != expected.generated_include
            || self.trusted_config != expected.trusted_config
            || self.recovery_root != expected.recovery_root
        {
            return Err(BindingError::new(
                "unsafe_path",
                "binding paths escape their XDG roots",
            ));
        }
        Ok(())
    }
}

pub fn apply_active_bindings(
    paths: &BindingPaths,
    validator: &dyn BindingValidator,
    reloader: &dyn BindingReloader,
) -> Result<ApplyReport, BindingError> {
    prepare_new_transaction(paths)?;
    let defaults = Defaults::packaged();
    let store = StateStore::for_repair(paths.store.clone(), defaults.clone())
        .map_err(BindingError::from_store)?;
    store
        .with_repair_candidate_transaction(|store| {
            let (settings_exists, presets_exists) = store.document_presence()?;
            if !settings_exists && !presets_exists {
                let settings = defaults.settings();
                let active = defaults
                    .builtin(&settings.active_preset_id)
                    .expect("packaged active preset exists");
                apply_store_candidate(
                    store,
                    paths,
                    validator,
                    reloader,
                    StateCandidate {
                        settings: settings.clone(),
                        user_presets: Vec::new(),
                    },
                    active.clone(),
                )
            } else {
                let settings = store.load_settings()?;
                let preset = store
                    .find_preset(&settings.active_preset_id)?
                    .ok_or_else(|| crate::StoreError::invalid("active preset does not exist"))?;
                let users = store.load_user_presets()?;
                let bindings = compile_bindings(&preset).map_err(store_binding_error)?;
                let preset_bytes = serde_json::to_vec(&users).map_err(crate::StoreError::io)?;
                let settings_bytes =
                    serde_json::to_vec(&settings).map_err(crate::StoreError::io)?;
                apply_candidate_locked(
                    &BindingFileSystem::open(paths, store.secure_handles())
                        .map_err(store_binding_error)?,
                    paths,
                    validator,
                    reloader,
                    &settings.active_preset_id,
                    CandidateArtifacts {
                        preset: &preset_bytes,
                        settings: &settings_bytes,
                        bindings: bindings.as_bytes(),
                    },
                )
                .map_err(store_binding_error)
            }
        })
        .map_err(BindingError::from_store)
}

pub fn activate_and_apply(
    id: &str,
    paths: &BindingPaths,
    validator: &dyn BindingValidator,
    reloader: &dyn BindingReloader,
) -> Result<ApplyReport, BindingError> {
    prepare_new_transaction(paths)?;
    let store = StateStore::open(paths.store.clone(), Defaults::packaged())
        .map_err(BindingError::from_store)?;
    store
        .with_candidate_transaction(|store, mut candidate| {
            let preset = store
                .find_preset(id)?
                .ok_or_else(|| crate::StoreError::not_found(id))?;
            candidate.settings.active_preset_id = id.to_owned();
            apply_store_candidate(store, paths, validator, reloader, candidate, preset)
        })
        .map_err(BindingError::from_store)
}

pub fn update_active_bindings_and_apply(
    id: &str,
    document: Value,
    paths: &BindingPaths,
    validator: &dyn BindingValidator,
    reloader: &dyn BindingReloader,
) -> Result<ApplyReport, BindingError> {
    prepare_new_transaction(paths)?;
    let replacement = crate::store::parse_preset(document).map_err(BindingError::from_store)?;
    let store = StateStore::open(paths.store.clone(), Defaults::packaged())
        .map_err(BindingError::from_store)?;
    store
        .with_candidate_transaction(|store, mut candidate| {
            require_active_target(&candidate, id)?;
            if replacement.id != id {
                return Err(crate::StoreError::conflict(format!(
                    "candidate id {:?} does not match target {id:?}",
                    replacement.id
                )));
            }
            if id == BUILTIN_PRESET_ID {
                if replacement.origin != PresetOrigin::Builtin {
                    return Err(crate::StoreError::immutable(id));
                }
                let mut copy = replacement.clone();
                copy.id = Uuid::new_v4().hyphenated().to_string();
                copy.origin = PresetOrigin::User;
                copy.base_preset_id = Some(BUILTIN_PRESET_ID.to_owned());
                candidate.settings.active_preset_id = copy.id.clone();
                candidate.user_presets.push(copy.clone());
                return apply_store_candidate(store, paths, validator, reloader, candidate, copy);
            }
            if replacement.origin != PresetOrigin::User {
                return Err(crate::StoreError::immutable(id));
            }
            let position = candidate
                .user_presets
                .iter()
                .position(|preset| preset.id == id)
                .ok_or_else(|| crate::StoreError::not_found(id))?;
            candidate.user_presets[position] = replacement.clone();
            apply_store_candidate(
                store,
                paths,
                validator,
                reloader,
                candidate,
                replacement.clone(),
            )
        })
        .map_err(BindingError::from_store)
}

pub fn mutate_keybinding_and_apply(
    id: &str,
    action: &str,
    accelerator: Option<&str>,
    paths: &BindingPaths,
    validator: &dyn BindingValidator,
    reloader: &dyn BindingReloader,
) -> Result<ApplyReport, BindingError> {
    prepare_new_transaction(paths)?;
    sleepy_sdk::SemanticAction::try_from(action)
        .map_err(|error| BindingError::new("unknown_semantic_action", error.to_string()))?;
    let accelerator = accelerator
        .map(canonicalize_accelerator)
        .transpose()
        .map_err(|error| BindingError::new("invalid_document", error.to_string()))?;
    let store = StateStore::open(paths.store.clone(), Defaults::packaged())
        .map_err(BindingError::from_store)?;
    store
        .with_candidate_transaction(|store, mut state| {
            require_active_target(&state, id)?;
            let mut preset = store
                .find_preset(id)?
                .ok_or_else(|| crate::StoreError::not_found(id))?;
            if preset.origin == PresetOrigin::Builtin {
                let source_id = preset.id.clone();
                preset.id = Uuid::new_v4().hyphenated().to_string();
                preset.name = format!("{} copy", preset.name);
                preset.origin = PresetOrigin::User;
                preset.base_preset_id = Some(source_id);
                state.settings.active_preset_id = preset.id.clone();
                state.user_presets.push(preset.clone());
            }
            match &accelerator {
                Some(accelerator) => {
                    preset
                        .keybindings
                        .insert(action.to_owned(), accelerator.clone());
                }
                None => {
                    preset.keybindings.remove(action);
                }
            }
            preset = crate::store::parse_preset(
                serde_json::to_value(&preset).map_err(crate::StoreError::io)?,
            )?;
            let position = state
                .user_presets
                .iter()
                .position(|candidate| candidate.id == preset.id)
                .ok_or_else(|| crate::StoreError::not_found(&preset.id))?;
            state.user_presets[position] = preset.clone();
            apply_store_candidate(store, paths, validator, reloader, state, preset)
        })
        .map_err(BindingError::from_store)
}

pub fn import_replace_active_and_apply(
    document: Value,
    paths: &BindingPaths,
    validator: &dyn BindingValidator,
    reloader: &dyn BindingReloader,
) -> Result<ApplyReport, BindingError> {
    let replacement = crate::store::parse_preset(document).map_err(BindingError::from_store)?;
    if replacement.origin != PresetOrigin::User || replacement.id == BUILTIN_PRESET_ID {
        return Err(BindingError::from_store(crate::StoreError::immutable(
            &replacement.id,
        )));
    }
    let replacement_id = replacement.id.clone();
    update_active_bindings_and_apply(
        &replacement_id,
        serde_json::to_value(replacement)
            .map_err(|error| BindingError::new("invalid_document", error.to_string()))?,
        paths,
        validator,
        reloader,
    )
}

pub fn repair_state(
    bundle: RepairBundle,
    paths: &BindingPaths,
    validator: &dyn BindingValidator,
    reloader: &dyn BindingReloader,
) -> Result<ApplyReport, BindingError> {
    prepare_new_transaction(paths)?;
    let settings = validate_settings(&bundle.settings.to_string())
        .map_err(|error| BindingError::new("invalid_document", error.to_string()))?;
    let user_presets = bundle
        .presets
        .into_iter()
        .map(crate::store::parse_preset)
        .collect::<Result<Vec<_>, _>>()
        .map_err(BindingError::from_store)?;
    validate_repair_presets(&settings, &user_presets)?;
    let defaults = Defaults::packaged();
    let active = if settings.active_preset_id == BUILTIN_PRESET_ID {
        defaults
            .builtin(BUILTIN_PRESET_ID)
            .expect("packaged builtin exists")
    } else {
        user_presets
            .iter()
            .find(|preset| preset.id == settings.active_preset_id)
            .cloned()
            .ok_or_else(|| {
                BindingError::new("invalid_document", "repair active preset does not exist")
            })?
    };
    compile_bindings(&active)?;
    let store =
        StateStore::for_repair(paths.store.clone(), defaults).map_err(BindingError::from_store)?;
    store
        .with_repair_candidate_transaction(|store| {
            let fs = BindingFileSystem::open(paths, store.secure_handles())
                .map_err(store_binding_error)?;
            backup_original_state(paths, &fs).map_err(store_binding_error)?;
            apply_store_candidate(
                store,
                paths,
                validator,
                reloader,
                StateCandidate {
                    settings: settings.clone(),
                    user_presets: user_presets.clone(),
                },
                active.clone(),
            )
        })
        .map_err(BindingError::from_store)
}

fn apply_store_candidate(
    store: &StateStore,
    paths: &BindingPaths,
    validator: &dyn BindingValidator,
    reloader: &dyn BindingReloader,
    mut candidate: StateCandidate,
    active: PresetDocument,
) -> Result<ApplyReport, crate::StoreError> {
    candidate
        .user_presets
        .sort_by(|left, right| left.id.cmp(&right.id));
    let bindings = compile_bindings(&active).map_err(store_binding_error)?;
    let preset_bytes =
        serde_json::to_vec(&candidate.user_presets).map_err(crate::StoreError::io)?;
    let settings_bytes = serde_json::to_vec(&candidate.settings).map_err(crate::StoreError::io)?;
    apply_candidate_locked(
        &BindingFileSystem::open(paths, store.secure_handles()).map_err(store_binding_error)?,
        paths,
        validator,
        reloader,
        &candidate.settings.active_preset_id,
        CandidateArtifacts {
            preset: &preset_bytes,
            settings: &settings_bytes,
            bindings: bindings.as_bytes(),
        },
    )
    .map_err(store_binding_error)
}

fn require_active_target(candidate: &StateCandidate, id: &str) -> Result<(), crate::StoreError> {
    if candidate.settings.active_preset_id == id {
        Ok(())
    } else {
        Err(crate::StoreError::invalid(format!(
            "preset {id:?} is not active"
        )))
    }
}

fn prepare_new_transaction(paths: &BindingPaths) -> Result<(), BindingError> {
    paths.validate()?;
    Ok(())
}

fn validate_repair_presets(
    settings: &SettingsDocument,
    presets: &[PresetDocument],
) -> Result<(), BindingError> {
    let ids = presets
        .iter()
        .map(|preset| preset.id.as_str())
        .collect::<BTreeSet<_>>();
    if ids.len() != presets.len()
        || presets
            .iter()
            .any(|preset| preset.origin != PresetOrigin::User)
        || (settings.active_preset_id != BUILTIN_PRESET_ID
            && !ids.contains(settings.active_preset_id.as_str()))
    {
        return Err(BindingError::new(
            "invalid_document",
            "repair bundle presets are invalid or do not contain the active preset",
        ));
    }
    Ok(())
}

fn backup_original_state(paths: &BindingPaths, fs: &BindingFileSystem) -> Result<(), BindingError> {
    let expected = paths.store().state_root().join("sleepy/recovery");
    if paths.recovery_root() != expected {
        return Err(BindingError::new(
            "unsafe_path",
            "recovery path escapes state root",
        ));
    }
    let recovery_root = fs
        .handles
        .presets
        .child_writable("recovery".as_ref(), true)
        .map_err(BindingError::from_store)?;
    let name = Uuid::new_v4().hyphenated().to_string();
    let recovery = recovery_root
        .child_writable(name.as_ref(), true)
        .map_err(BindingError::from_store)?;
    for (source, destination) in [
        (&fs.handles.settings, "settings.json"),
        (&fs.handles.presets, "presets.json"),
    ] {
        if let Some(bytes) = source
            .read_optional(destination.as_ref())
            .map_err(BindingError::from_store)?
        {
            recovery
                .write_new(destination.as_ref(), &bytes)
                .map_err(BindingError::from_store)?;
        }
    }
    recovery.sync().map_err(BindingError::from_store)?;
    recovery_root.sync().map_err(BindingError::from_store)
}

pub fn reconcile_bindings(
    paths: &BindingPaths,
    reloader: &dyn BindingReloader,
) -> Result<Option<ApplyReport>, BindingError> {
    reconcile_bindings_mode(paths, reloader, false)
}

pub fn reconcile_bindings_online_required(
    paths: &BindingPaths,
    reloader: &dyn BindingReloader,
) -> Result<Option<ApplyReport>, BindingError> {
    reconcile_bindings_mode(paths, reloader, true)
}

pub fn initialize_bindings(
    paths: &BindingPaths,
    validator: &dyn BindingValidator,
    reloader: &dyn BindingReloader,
) -> Result<ApplyReport, BindingError> {
    if let Some(report) = reconcile_bindings(paths, reloader)? {
        return Ok(report);
    }
    let store = StateStore::for_repair(paths.store.clone(), Defaults::packaged())
        .map_err(BindingError::from_store)?;
    if let Some(active_preset_id) = store
        .with_repair_candidate_transaction(|store| {
            let (settings_exists, presets_exists) = store.document_presence()?;
            if !settings_exists || !presets_exists {
                return Ok(None);
            }
            let settings = store.load_settings()?;
            let preset = store
                .find_preset(&settings.active_preset_id)?
                .ok_or_else(|| crate::StoreError::invalid("active preset does not exist"))?;
            let compiled = compile_bindings(&preset).map_err(store_binding_error)?;
            let fs = BindingFileSystem::open(paths, store.secure_handles())
                .map_err(store_binding_error)?;
            let include = fs
                .niri
                .read_optional(BindingFileSystem::artifact_name(ArtifactKind::Bindings))?;
            Ok((include.as_deref() == Some(compiled.as_bytes()))
                .then_some(settings.active_preset_id))
        })
        .map_err(BindingError::from_store)?
    {
        return Ok(ApplyReport {
            status: ApplyStatus::Committed,
            active_preset_id,
        });
    }
    apply_active_bindings(paths, validator, reloader)
}

fn reconcile_bindings_mode(
    paths: &BindingPaths,
    reloader: &dyn BindingReloader,
    online_required: bool,
) -> Result<Option<ApplyReport>, BindingError> {
    paths.validate()?;
    let store = StateStore::for_repair(paths.store.clone(), Defaults::packaged())
        .map_err(BindingError::from_store)?;
    store
        .with_repair_candidate_transaction(|store| {
            let fs = BindingFileSystem::open(paths, store.secure_handles())
                .map_err(store_binding_error)?;
            paths
                .observe(ApplyStage::WritableDirectoriesOpened)
                .map_err(store_binding_error)?;
            reconcile_bindings_locked(&fs, paths, reloader, online_required)
                .map_err(store_binding_error)
        })
        .map_err(BindingError::from_store)
}

fn reconcile_bindings_locked(
    fs: &BindingFileSystem,
    paths: &BindingPaths,
    reloader: &dyn BindingReloader,
    online_required: bool,
) -> Result<Option<ApplyReport>, BindingError> {
    let Some(mut journal) = TransactionJournal::load(fs, paths)? else {
        return Ok(None);
    };
    if !journal.sidecars_complete {
        journal.cleanup(fs, paths)?;
        return Ok(None);
    }
    if journal.phase == JournalPhase::ReloadConfirmed {
        let status = status_for(journal.recovery_target);
        let active_preset_id = journal.active_preset_id_for(journal.recovery_target)?;
        journal.cleanup(fs, paths)?;
        return Ok(Some(ApplyReport {
            status,
            active_preset_id,
        }));
    }

    let target = match journal.phase {
        JournalPhase::Prepared
        | JournalPhase::PresetCommitted
        | JournalPhase::SettingsCommitted => RecoveryTarget::Previous,
        JournalPhase::BindingsCommitted | JournalPhase::ReloadPending => journal.recovery_target,
        JournalPhase::ReloadConfirmed => unreachable!("handled above"),
    };
    let mut stream = if online_required {
        Some(
            reloader
                .subscribe_required(RELOAD_TIMEOUT)
                .map_err(|message| BindingError::new("niri_unavailable", message))?,
        )
    } else {
        reloader
            .subscribe()
            .map_err(|message| BindingError::new("reload_failed", message))?
    };
    if let Some(stream) = stream.as_mut() {
        stream
            .await_initial_snapshot(RELOAD_TIMEOUT)
            .map_err(|message| {
                BindingError::new(
                    if online_required {
                        "niri_unavailable"
                    } else {
                        "reload_failed"
                    },
                    message,
                )
            })?;
    }
    if target == RecoveryTarget::Previous {
        journal.set_recovery_target(fs, target)?;
    }
    journal.install_target(fs, paths, target)?;
    if target == RecoveryTarget::Candidate {
        journal.set_recovery_target(fs, target)?;
    }
    journal.set_phase(fs, paths, JournalPhase::ReloadPending)?;
    let active_preset_id = journal.active_preset_id_for(target)?;

    let Some(mut stream) = stream else {
        return Ok(Some(ApplyReport {
            status: ApplyStatus::ReloadPending,
            active_preset_id,
        }));
    };
    if reload_confirmed(paths, reloader, stream.as_mut())? {
        journal.set_phase(fs, paths, JournalPhase::ReloadConfirmed)?;
        journal.cleanup(fs, paths)?;
        return Ok(Some(ApplyReport {
            status: status_for(target),
            active_preset_id,
        }));
    }
    if target == RecoveryTarget::Previous {
        return Ok(Some(ApplyReport {
            status: ApplyStatus::CommitStateUnknown,
            active_preset_id,
        }));
    }

    let rollback_stream = reloader.subscribe();
    let mut rollback_stream = match rollback_stream {
        Ok(Some(mut stream)) => {
            if stream.await_initial_snapshot(RELOAD_TIMEOUT).is_ok() {
                Some(stream)
            } else {
                None
            }
        }
        Ok(None) | Err(_) => None,
    };
    journal.set_recovery_target(fs, RecoveryTarget::Previous)?;
    journal.restore_all(fs, paths)?;
    journal.set_phase(fs, paths, JournalPhase::ReloadPending)?;
    let previous_active_preset_id = journal.active_preset_id_for(RecoveryTarget::Previous)?;
    let confirmed = match rollback_stream.as_mut() {
        Some(stream) => reload_confirmed(paths, reloader, stream.as_mut())?,
        None => false,
    };
    if confirmed {
        journal.set_phase(fs, paths, JournalPhase::ReloadConfirmed)?;
        journal.cleanup(fs, paths)?;
        Ok(Some(ApplyReport {
            status: ApplyStatus::RolledBackConfirmed,
            active_preset_id: previous_active_preset_id,
        }))
    } else {
        Ok(Some(ApplyReport {
            status: ApplyStatus::CommitStateUnknown,
            active_preset_id: previous_active_preset_id,
        }))
    }
}

fn status_for(target: RecoveryTarget) -> ApplyStatus {
    match target {
        RecoveryTarget::Candidate => ApplyStatus::Committed,
        RecoveryTarget::Previous => ApplyStatus::RolledBackConfirmed,
    }
}

fn store_binding_error(error: BindingError) -> crate::StoreError {
    crate::StoreError::binding_with_details(
        error.code(),
        error.message().to_owned(),
        error.details().cloned(),
    )
}

struct CandidateArtifacts<'a> {
    preset: &'a [u8],
    settings: &'a [u8],
    bindings: &'a [u8],
}

fn apply_candidate_locked(
    fs: &BindingFileSystem,
    paths: &BindingPaths,
    validator: &dyn BindingValidator,
    reloader: &dyn BindingReloader,
    active_preset_id: &str,
    artifacts: CandidateArtifacts<'_>,
) -> Result<ApplyReport, BindingError> {
    paths.observe(ApplyStage::WritableDirectoriesOpened)?;
    if fs
        .handles
        .presets
        .exists(BindingFileSystem::journal_name())
        .map_err(BindingError::from_store)?
    {
        return Err(BindingError::new(
            "transaction_in_progress",
            "reconcile the existing binding transaction before starting another",
        ));
    }
    validate_candidate_tree(fs, paths, artifacts.bindings, validator)?;
    let mut candidate_stream = reloader
        .subscribe()
        .map_err(|message| BindingError::new("reload_failed", message))?;
    if let Some(stream) = candidate_stream.as_mut() {
        stream
            .await_initial_snapshot(RELOAD_TIMEOUT)
            .map_err(|message| BindingError::new("reload_failed", message))?;
    }

    let mut journal = TransactionJournal::prepare(
        fs,
        paths,
        active_preset_id,
        artifacts.preset,
        artifacts.settings,
        artifacts.bindings,
    )?;
    journal.install_new(fs, paths, ArtifactKind::Preset)?;
    journal.set_phase(fs, paths, JournalPhase::PresetCommitted)?;
    journal.install_new(fs, paths, ArtifactKind::Settings)?;
    journal.set_phase(fs, paths, JournalPhase::SettingsCommitted)?;
    journal.install_new(fs, paths, ArtifactKind::Bindings)?;
    journal.set_phase(fs, paths, JournalPhase::BindingsCommitted)?;
    journal.set_phase(fs, paths, JournalPhase::ReloadPending)?;

    let Some(mut candidate_stream) = candidate_stream else {
        return Ok(ApplyReport {
            status: ApplyStatus::ReloadPending,
            active_preset_id: active_preset_id.to_owned(),
        });
    };
    if reload_confirmed(paths, reloader, candidate_stream.as_mut())? {
        journal.set_phase(fs, paths, JournalPhase::ReloadConfirmed)?;
        journal.cleanup(fs, paths)?;
        return Ok(ApplyReport {
            status: ApplyStatus::Committed,
            active_preset_id: active_preset_id.to_owned(),
        });
    }

    let rollback_stream = reloader.subscribe();
    let mut rollback_stream = match rollback_stream {
        Ok(Some(mut stream)) => {
            if stream.await_initial_snapshot(RELOAD_TIMEOUT).is_err() {
                None
            } else {
                Some(stream)
            }
        }
        Ok(None) | Err(_) => None,
    };
    journal.set_recovery_target(fs, RecoveryTarget::Previous)?;
    journal.restore_all(fs, paths)?;
    journal.set_phase(fs, paths, JournalPhase::ReloadPending)?;
    let previous_active_preset_id = journal.active_preset_id_for(RecoveryTarget::Previous)?;
    let rollback_confirmed = match rollback_stream.as_mut() {
        Some(stream) => reload_confirmed(paths, reloader, stream.as_mut())?,
        None => false,
    };
    if rollback_confirmed {
        journal.set_phase(fs, paths, JournalPhase::ReloadConfirmed)?;
        journal.cleanup(fs, paths)?;
        Ok(ApplyReport {
            status: ApplyStatus::RolledBackConfirmed,
            active_preset_id: previous_active_preset_id,
        })
    } else {
        Ok(ApplyReport {
            status: ApplyStatus::CommitStateUnknown,
            active_preset_id: previous_active_preset_id,
        })
    }
}

fn reload_confirmed(
    paths: &BindingPaths,
    reloader: &dyn BindingReloader,
    stream: &mut dyn ConfigEventStream,
) -> Result<bool, BindingError> {
    if reloader.request_reload(paths.trusted_config()).is_err() {
        return Ok(false);
    }
    paths.observe(ApplyStage::ReloadRequested)?;
    Ok(matches!(
        stream.next_config_loaded(RELOAD_TIMEOUT),
        Ok(Some(ConfigLoaded { failed: false }))
    ))
}

fn validate_candidate_tree(
    fs: &BindingFileSystem,
    _paths: &BindingPaths,
    binding_bytes: &[u8],
    validator: &dyn BindingValidator,
) -> Result<(), BindingError> {
    let staging_name = format!(".niri-validation-{}", Uuid::new_v4());
    let staging_root = fs
        .handles
        .presets
        .child_writable(staging_name.as_ref(), true)
        .map_err(BindingError::from_store)?;
    let result = (|| {
        copy_config_tree(&fs.niri.proc_path(), &staging_root, true)?;
        staging_root
            .write_new(
                BindingFileSystem::artifact_name(ArtifactKind::Bindings),
                binding_bytes,
            )
            .map_err(BindingError::from_store)?;
        staging_root.sync().map_err(BindingError::from_store)?;
        let staged_path = staging_root.proc_path();
        let staged_config = staged_path.join("config.kdl");
        validator
            .validate(&staged_path, &staged_config)
            .map_err(|message| BindingError::new("validation_failed", message))
    })();
    let cleanup = remove_tree(&staging_root).and_then(|()| {
        fs.handles
            .presets
            .remove_dir(staging_name.as_ref())
            .map_err(BindingError::from_store)
    });
    match (result, cleanup) {
        (Err(error), _) => Err(error),
        (Ok(()), Err(error)) => Err(error),
        (Ok(()), Ok(())) => Ok(()),
    }
}

fn copy_config_tree(
    source: &Path,
    destination: &crate::store::SecureDir,
    root: bool,
) -> Result<(), BindingError> {
    for entry in fs::read_dir(source).map_err(|error| io_error("read Niri config tree", error))? {
        let entry = entry.map_err(|error| io_error("read Niri config entry", error))?;
        let source_path = entry.path();
        if root && entry.file_name() == BindingFileSystem::artifact_name(ArtifactKind::Bindings) {
            continue;
        }
        let metadata = fs::symlink_metadata(&source_path)
            .map_err(|error| io_error("inspect Niri config entry", error))?;
        if metadata.file_type().is_symlink() {
            let resolved = fs::canonicalize(&source_path)
                .map_err(|error| io_error("resolve static Niri symlink", error))?;
            let target = fs::metadata(&resolved)
                .map_err(|error| io_error("inspect static Niri symlink target", error))?;
            if !resolved.starts_with("/nix/store") || !target.is_file() || target.uid() != 0 {
                return Err(BindingError::new(
                    "unsafe_path",
                    format!("untrusted static Niri symlink: {}", source_path.display()),
                ));
            }
            let bytes =
                fs::read(&resolved).map_err(|error| io_error("read static Niri file", error))?;
            destination
                .write_new(&entry.file_name(), &bytes)
                .map_err(BindingError::from_store)?;
        } else if metadata.is_dir() {
            let child = destination
                .child_writable(&entry.file_name(), true)
                .map_err(BindingError::from_store)?;
            copy_config_tree(&source_path, &child, false)?;
            child.sync().map_err(BindingError::from_store)?;
        } else if metadata.is_file() {
            let bytes =
                fs::read(&source_path).map_err(|error| io_error("read static Niri file", error))?;
            destination
                .write_new(&entry.file_name(), &bytes)
                .map_err(BindingError::from_store)?;
        } else {
            return Err(BindingError::new(
                "unsafe_path",
                format!(
                    "Niri config entry is not a regular file: {}",
                    source_path.display()
                ),
            ));
        }
    }
    Ok(())
}

fn remove_tree(directory: &crate::store::SecureDir) -> Result<(), BindingError> {
    for name in directory.entries().map_err(BindingError::from_store)? {
        let path = directory.proc_path().join(&name);
        let metadata =
            fs::symlink_metadata(&path).map_err(|error| io_error("inspect staged entry", error))?;
        if metadata.is_dir() && !metadata.file_type().is_symlink() {
            let child = directory
                .child_writable(&name, false)
                .map_err(BindingError::from_store)?;
            remove_tree(&child)?;
            directory
                .remove_dir(&name)
                .map_err(BindingError::from_store)?;
        } else {
            directory
                .remove_file(&name)
                .map_err(BindingError::from_store)?;
        }
    }
    directory.sync().map_err(BindingError::from_store)
}
