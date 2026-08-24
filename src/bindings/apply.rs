use std::{
    collections::BTreeSet,
    fs,
    fs::File,
    io::{BufRead, BufReader},
    os::unix::fs::{MetadataExt, PermissionsExt},
    path::{Path, PathBuf},
    process::{Child, Command, Stdio},
    sync::mpsc::{self, Receiver, RecvTimeoutError},
    sync::Arc,
    thread,
    time::Duration,
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
    BindingError,
};

const RELOAD_TIMEOUT: Duration = Duration::from_secs(5);

pub trait BindingValidator {
    fn validate(&self, staged_root: &Path, staged_config: &Path) -> Result<(), String>;
}

pub trait ConfigEventStream {
    fn drain_initial(&mut self) -> Result<(), String>;
    fn next_config_loaded(&mut self, timeout: Duration) -> Result<Option<ConfigLoaded>, String>;
}

pub trait BindingReloader {
    fn subscribe(&self) -> Result<Option<Box<dyn ConfigEventStream>>, String>;
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
                if let Some(event) = parse_config_loaded_event(&line) {
                    if sender.send(event).is_err() {
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
}

struct NiriEventStream {
    child: Child,
    receiver: Receiver<ConfigLoaded>,
}

impl ConfigEventStream for NiriEventStream {
    fn drain_initial(&mut self) -> Result<(), String> {
        loop {
            match self.receiver.recv_timeout(Duration::from_millis(25)) {
                Ok(_) => {}
                Err(RecvTimeoutError::Timeout) => return Ok(()),
                Err(RecvTimeoutError::Disconnected) => {
                    return Err("Niri event stream closed during initial drain".to_owned());
                }
            }
        }
    }

    fn next_config_loaded(&mut self, timeout: Duration) -> Result<Option<ConfigLoaded>, String> {
        match self.receiver.recv_timeout(timeout) {
            Ok(event) => Ok(Some(event)),
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

fn parse_config_loaded_event(line: &str) -> Option<ConfigLoaded> {
    let value: Value = serde_json::from_str(line).ok()?;
    let payload = value
        .get("ConfigLoaded")
        .or_else(|| value.get("configLoaded"))?;
    let failed = payload
        .get("failed")
        .and_then(Value::as_bool)
        .or_else(|| payload.as_bool())?;
    Some(ConfigLoaded { failed })
}

pub trait ApplyObserver: Send + Sync {
    fn reached(&self, stage: ApplyStage) -> Result<(), String>;
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ApplyStage {
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
        self.store
            .reject_symlinks()
            .map_err(BindingError::from_store)?;
        for path in [
            self.store.config_root(),
            self.store.state_root(),
            self.niri_root(),
        ] {
            validate_writable_directory(path)?;
        }
        if self.recovery_root.exists() {
            validate_writable_directory(&self.recovery_root)?;
        }
        for path in [self.store.settings_dir(), self.store.presets_dir()] {
            if path.exists() {
                validate_writable_directory(&path)?;
            }
        }
        for path in [
            self.store.settings_path(),
            self.store.presets_path(),
            self.generated_include.clone(),
            self.journal.clone(),
        ] {
            validate_writable_file_if_present(&path)?;
        }
        if !self.niri_root.starts_with(self.store.config_root())
            || !self.journal.starts_with(self.store.state_root())
            || !self.generated_include.starts_with(&self.niri_root)
            || !self.trusted_config.starts_with(&self.niri_root)
            || !self.recovery_root.starts_with(self.store.state_root())
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
    let settings_missing = !paths.store().settings_path().exists();
    let presets_missing = !paths.store().presets_path().exists();
    if settings_missing && presets_missing {
        let defaults = Defaults::packaged();
        let settings = defaults.settings();
        let active = defaults
            .builtin(&settings.active_preset_id)
            .expect("packaged active preset exists");
        let store = StateStore::for_repair(paths.store.clone(), defaults);
        return store
            .with_repair_candidate_transaction(|_| {
                apply_store_candidate(
                    paths,
                    validator,
                    reloader,
                    StateCandidate {
                        settings: settings.clone(),
                        user_presets: Vec::new(),
                    },
                    active.clone(),
                )
            })
            .map_err(BindingError::from_store);
    }
    let store = StateStore::open(paths.store.clone(), Defaults::packaged())
        .map_err(BindingError::from_store)?;
    store
        .with_transaction(|store| {
            let settings = store.load_settings()?;
            let preset = store
                .find_preset(&settings.active_preset_id)?
                .ok_or_else(|| crate::StoreError::invalid("active preset does not exist"))?;
            let users = store.load_user_presets()?;
            let bindings = compile_bindings(&preset).map_err(store_binding_error)?;
            let preset_bytes = serde_json::to_vec(&users).map_err(crate::StoreError::io)?;
            let settings_bytes = serde_json::to_vec(&settings).map_err(crate::StoreError::io)?;
            apply_candidate_locked(
                paths,
                validator,
                reloader,
                &settings.active_preset_id,
                &preset_bytes,
                &settings_bytes,
                bindings.as_bytes(),
            )
            .map_err(store_binding_error)
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
            apply_store_candidate(paths, validator, reloader, candidate, preset)
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
        .with_candidate_transaction(|_store, mut candidate| {
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
                return apply_store_candidate(paths, validator, reloader, candidate, copy);
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
            apply_store_candidate(paths, validator, reloader, candidate, replacement.clone())
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
            apply_store_candidate(paths, validator, reloader, state, preset)
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
    let store = StateStore::for_repair(paths.store.clone(), defaults);
    store
        .with_repair_candidate_transaction(|_| {
            backup_original_state(paths).map_err(store_binding_error)?;
            apply_store_candidate(
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
        paths,
        validator,
        reloader,
        &candidate.settings.active_preset_id,
        &preset_bytes,
        &settings_bytes,
        bindings.as_bytes(),
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
    if paths.journal().exists() {
        return Err(BindingError::new(
            "transaction_in_progress",
            "reconcile the existing binding transaction before starting another",
        ));
    }
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

fn backup_original_state(paths: &BindingPaths) -> Result<(), BindingError> {
    use std::os::unix::fs::DirBuilderExt;

    if !paths.recovery_root().exists() {
        let mut builder = fs::DirBuilder::new();
        builder.mode(0o700);
        builder
            .create(paths.recovery_root())
            .map_err(|error| io_error("create recovery root", error))?;
    }
    validate_writable_directory(paths.recovery_root())?;
    let recovery = paths
        .recovery_root()
        .join(Uuid::new_v4().hyphenated().to_string());
    let mut builder = fs::DirBuilder::new();
    builder.mode(0o700);
    builder
        .create(&recovery)
        .map_err(|error| io_error("create non-overwriting recovery directory", error))?;
    for (source, name) in [
        (paths.store().settings_path(), "settings.json"),
        (paths.store().presets_path(), "presets.json"),
    ] {
        match fs::read(&source) {
            Ok(bytes) => write_private_file(&recovery.join(name), &bytes)?,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => return Err(io_error("read malformed original state", error)),
        }
    }
    File::open(&recovery)
        .and_then(|directory| directory.sync_all())
        .map_err(|error| io_error("sync recovery directory", error))?;
    File::open(paths.recovery_root())
        .and_then(|directory| directory.sync_all())
        .map_err(|error| io_error("sync recovery root", error))
}

pub fn reconcile_bindings(
    paths: &BindingPaths,
    reloader: &dyn BindingReloader,
) -> Result<Option<ApplyReport>, BindingError> {
    paths.validate()?;
    let store = StateStore::for_repair(paths.store.clone(), Defaults::packaged());
    store
        .with_repair_candidate_transaction(|_| {
            reconcile_bindings_locked(paths, reloader).map_err(store_binding_error)
        })
        .map_err(BindingError::from_store)
}

fn reconcile_bindings_locked(
    paths: &BindingPaths,
    reloader: &dyn BindingReloader,
) -> Result<Option<ApplyReport>, BindingError> {
    let Some(mut journal) = TransactionJournal::load(paths)? else {
        return Ok(None);
    };
    if journal.phase == JournalPhase::ReloadConfirmed {
        let status = status_for(journal.recovery_target);
        let active_preset_id = journal.active_preset_id_for(journal.recovery_target)?;
        journal.cleanup(paths)?;
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
    let mut stream = reloader
        .subscribe()
        .map_err(|message| BindingError::new("reload_failed", message))?;
    if let Some(stream) = stream.as_mut() {
        stream
            .drain_initial()
            .map_err(|message| BindingError::new("reload_failed", message))?;
    }
    if target == RecoveryTarget::Previous {
        journal.set_recovery_target(paths, target)?;
    }
    journal.install_target(paths, target)?;
    if target == RecoveryTarget::Candidate {
        journal.set_recovery_target(paths, target)?;
    }
    journal.set_phase(paths, JournalPhase::ReloadPending)?;
    let active_preset_id = journal.active_preset_id_for(target)?;

    let Some(mut stream) = stream else {
        return Ok(Some(ApplyReport {
            status: ApplyStatus::ReloadPending,
            active_preset_id,
        }));
    };
    if reload_confirmed(paths, reloader, stream.as_mut())? {
        journal.set_phase(paths, JournalPhase::ReloadConfirmed)?;
        journal.cleanup(paths)?;
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
            if stream.drain_initial().is_ok() {
                Some(stream)
            } else {
                None
            }
        }
        Ok(None) | Err(_) => None,
    };
    journal.set_recovery_target(paths, RecoveryTarget::Previous)?;
    journal.restore_all(paths)?;
    journal.set_phase(paths, JournalPhase::ReloadPending)?;
    let previous_active_preset_id = journal.active_preset_id_for(RecoveryTarget::Previous)?;
    let confirmed = match rollback_stream.as_mut() {
        Some(stream) => reload_confirmed(paths, reloader, stream.as_mut())?,
        None => false,
    };
    if confirmed {
        journal.set_phase(paths, JournalPhase::ReloadConfirmed)?;
        journal.cleanup(paths)?;
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

fn apply_candidate_locked(
    paths: &BindingPaths,
    validator: &dyn BindingValidator,
    reloader: &dyn BindingReloader,
    active_preset_id: &str,
    preset_bytes: &[u8],
    settings_bytes: &[u8],
    binding_bytes: &[u8],
) -> Result<ApplyReport, BindingError> {
    if paths.journal().exists() {
        return Err(BindingError::new(
            "transaction_in_progress",
            "reconcile the existing binding transaction before starting another",
        ));
    }
    validate_candidate_tree(paths, binding_bytes, validator)?;
    let mut candidate_stream = reloader
        .subscribe()
        .map_err(|message| BindingError::new("reload_failed", message))?;
    if let Some(stream) = candidate_stream.as_mut() {
        stream
            .drain_initial()
            .map_err(|message| BindingError::new("reload_failed", message))?;
    }

    let mut journal = TransactionJournal::prepare(
        paths,
        active_preset_id,
        preset_bytes,
        settings_bytes,
        binding_bytes,
    )?;
    journal.install_new(paths, ArtifactKind::Preset)?;
    journal.set_phase(paths, JournalPhase::PresetCommitted)?;
    journal.install_new(paths, ArtifactKind::Settings)?;
    journal.set_phase(paths, JournalPhase::SettingsCommitted)?;
    journal.install_new(paths, ArtifactKind::Bindings)?;
    journal.set_phase(paths, JournalPhase::BindingsCommitted)?;
    journal.set_phase(paths, JournalPhase::ReloadPending)?;

    let Some(mut candidate_stream) = candidate_stream else {
        return Ok(ApplyReport {
            status: ApplyStatus::ReloadPending,
            active_preset_id: active_preset_id.to_owned(),
        });
    };
    if reload_confirmed(paths, reloader, candidate_stream.as_mut())? {
        journal.set_phase(paths, JournalPhase::ReloadConfirmed)?;
        journal.cleanup(paths)?;
        return Ok(ApplyReport {
            status: ApplyStatus::Committed,
            active_preset_id: active_preset_id.to_owned(),
        });
    }

    let rollback_stream = reloader.subscribe();
    let mut rollback_stream = match rollback_stream {
        Ok(Some(mut stream)) => {
            if stream.drain_initial().is_err() {
                None
            } else {
                Some(stream)
            }
        }
        Ok(None) | Err(_) => None,
    };
    journal.set_recovery_target(paths, RecoveryTarget::Previous)?;
    journal.restore_all(paths)?;
    journal.set_phase(paths, JournalPhase::ReloadPending)?;
    let previous_active_preset_id = journal.active_preset_id_for(RecoveryTarget::Previous)?;
    let rollback_confirmed = match rollback_stream.as_mut() {
        Some(stream) => reload_confirmed(paths, reloader, stream.as_mut())?,
        None => false,
    };
    if rollback_confirmed {
        journal.set_phase(paths, JournalPhase::ReloadConfirmed)?;
        journal.cleanup(paths)?;
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
    paths: &BindingPaths,
    binding_bytes: &[u8],
    validator: &dyn BindingValidator,
) -> Result<(), BindingError> {
    let staging_root = paths
        .store
        .state_root()
        .join("sleepy")
        .join(format!(".niri-validation-{}", Uuid::new_v4()));
    fs::create_dir(&staging_root)
        .map_err(|error| io_error("create Niri validation tree", error))?;
    let result = (|| {
        copy_config_tree(paths.niri_root(), &staging_root, paths.generated_include())?;
        let staged_include = staging_root.join(
            paths
                .generated_include()
                .file_name()
                .expect("generated include has file name"),
        );
        write_private_file(&staged_include, binding_bytes)?;
        let staged_config = staging_root.join(
            paths
                .trusted_config()
                .file_name()
                .expect("trusted config has file name"),
        );
        validator
            .validate(&staging_root, &staged_config)
            .map_err(|message| BindingError::new("validation_failed", message))
    })();
    let cleanup = fs::remove_dir_all(&staging_root);
    match (result, cleanup) {
        (Err(error), _) => Err(error),
        (Ok(()), Err(error)) => Err(io_error("remove Niri validation tree", error)),
        (Ok(()), Ok(())) => Ok(()),
    }
}

fn copy_config_tree(
    source: &Path,
    destination: &Path,
    generated: &Path,
) -> Result<(), BindingError> {
    for entry in fs::read_dir(source).map_err(|error| io_error("read Niri config tree", error))? {
        let entry = entry.map_err(|error| io_error("read Niri config entry", error))?;
        let source_path = entry.path();
        if source_path == generated {
            continue;
        }
        let destination_path = destination.join(entry.file_name());
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
            fs::copy(&resolved, &destination_path)
                .map_err(|error| io_error("copy static Niri file", error))?;
        } else if metadata.is_dir() {
            fs::create_dir(&destination_path)
                .map_err(|error| io_error("create staged Niri directory", error))?;
            copy_config_tree(&source_path, &destination_path, generated)?;
        } else if metadata.is_file() {
            fs::copy(&source_path, &destination_path)
                .map_err(|error| io_error("copy static Niri file", error))?;
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

fn write_private_file(path: &Path, bytes: &[u8]) -> Result<(), BindingError> {
    use std::io::Write;
    use std::os::unix::fs::OpenOptionsExt;
    let mut file = fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .mode(0o600)
        .open(path)
        .map_err(|error| io_error("create staged generated include", error))?;
    file.write_all(bytes)
        .and_then(|()| file.sync_all())
        .map_err(|error| io_error("sync staged generated include", error))
}

fn validate_writable_directory(path: &Path) -> Result<(), BindingError> {
    let metadata = fs::symlink_metadata(path)
        .map_err(|error| io_error("inspect writable binding directory", error))?;
    if metadata.file_type().is_symlink()
        || !metadata.is_dir()
        || metadata.uid() != unsafe { libc::geteuid() }
        || metadata.permissions().mode() & 0o022 != 0
    {
        return Err(BindingError::new(
            "unsafe_path",
            format!("unsafe writable binding directory: {}", path.display()),
        ));
    }
    Ok(())
}

pub(crate) fn validate_writable_file_if_present(path: &Path) -> Result<(), BindingError> {
    let metadata = match fs::symlink_metadata(path) {
        Ok(metadata) => metadata,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(()),
        Err(error) => return Err(io_error("inspect writable binding file", error)),
    };
    if metadata.file_type().is_symlink()
        || !metadata.is_file()
        || metadata.uid() != unsafe { libc::geteuid() }
        || metadata.permissions().mode() & 0o133 != 0
    {
        return Err(BindingError::new(
            "unsafe_path",
            format!("unsafe writable binding file: {}", path.display()),
        ));
    }
    Ok(())
}
