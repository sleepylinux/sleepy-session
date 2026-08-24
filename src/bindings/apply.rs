use std::{
    collections::BTreeSet,
    io::Read,
    os::fd::AsRawFd,
    os::unix::fs::FileTypeExt,
    path::{Path, PathBuf},
    process::{Child, Command, Output, Stdio},
    sync::{
        atomic::{AtomicBool, Ordering},
        mpsc::{self, Receiver, RecvTimeoutError},
        Arc,
    },
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

use crate::{
    store::{SecureDir, SecureEntry, StateCandidate},
    Defaults, StateStore, StorePaths,
};

use super::{
    compile_bindings,
    journal::{ArtifactKind, JournalPhase, RecoveryTarget, TransactionJournal},
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
    fn request_reload_with_timeout(
        &self,
        trusted_config: &Path,
        _timeout: Duration,
    ) -> Result<(), String> {
        self.request_reload(trusted_config)
    }
}

#[derive(Debug, Clone)]
pub struct NiriValidator {
    executable: PathBuf,
    timeout: Duration,
}

impl NiriValidator {
    pub fn new(executable: impl Into<PathBuf>) -> Self {
        Self {
            executable: executable.into(),
            timeout: RELOAD_TIMEOUT,
        }
    }

    pub fn with_timeout(executable: impl Into<PathBuf>, timeout: Duration) -> Self {
        Self {
            executable: executable.into(),
            timeout,
        }
    }
}

impl BindingValidator for NiriValidator {
    fn validate(&self, staged_root: &Path, staged_config: &Path) -> Result<(), String> {
        let mut command = Command::new(&self.executable);
        command
            .args(["validate", "--config"])
            .arg(staged_config)
            .current_dir(staged_root);
        let output = run_command_bounded(&mut command, self.timeout, "pinned Niri validator")?;
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
    socket: Option<PathBuf>,
    timeout: Duration,
}

impl NiriReloader {
    pub fn new(executable: impl Into<PathBuf>) -> Self {
        Self {
            executable: executable.into(),
            socket: std::env::var_os("NIRI_SOCKET").map(PathBuf::from),
            timeout: RELOAD_TIMEOUT,
        }
    }

    pub fn with_runtime(
        executable: impl Into<PathBuf>,
        socket: Option<PathBuf>,
        timeout: Duration,
    ) -> Self {
        Self {
            executable: executable.into(),
            socket,
            timeout,
        }
    }

    fn subscribe_until(
        &self,
        deadline: Instant,
    ) -> Result<Option<Box<dyn ConfigEventStream>>, String> {
        if self.socket.is_none() {
            return Ok(None);
        }
        ensure_supported_niri(&self.executable, remaining(deadline)?)?;
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
        set_nonblocking(stdout.as_raw_fd())?;
        let (sender, receiver) = mpsc::channel();
        let cancelled = Arc::new(AtomicBool::new(false));
        let reader_cancelled = Arc::clone(&cancelled);
        let reader = thread::spawn(move || {
            let mut stdout = stdout;
            let mut pending = Vec::new();
            let mut chunk = [0_u8; 4096];
            while !reader_cancelled.load(Ordering::Acquire) {
                match stdout.read(&mut chunk) {
                    Ok(0) => break,
                    Ok(length) => {
                        pending.extend_from_slice(&chunk[..length]);
                        while let Some(newline) = pending.iter().position(|byte| *byte == b'\n') {
                            let line = pending.drain(..=newline).collect::<Vec<_>>();
                            if let Ok(line) = std::str::from_utf8(&line) {
                                if let Some(message) = parse_stream_message(line) {
                                    if sender.send(message).is_err() {
                                        return;
                                    }
                                }
                            }
                        }
                    }
                    Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                        thread::sleep(Duration::from_millis(5));
                    }
                    Err(_) => break,
                }
            }
        });
        Ok(Some(Box::new(NiriEventStream {
            child: Some(child),
            reader: Some(reader),
            cancelled,
            receiver,
            deadline,
        })))
    }
}

impl BindingReloader for NiriReloader {
    fn subscribe(&self) -> Result<Option<Box<dyn ConfigEventStream>>, String> {
        self.subscribe_until(Instant::now() + self.timeout)
    }

    fn request_reload(&self, trusted_config: &Path) -> Result<(), String> {
        self.request_reload_with_timeout(trusted_config, self.timeout)
    }

    fn request_reload_with_timeout(
        &self,
        trusted_config: &Path,
        timeout: Duration,
    ) -> Result<(), String> {
        let mut command = Command::new(&self.executable);
        command
            .args(["msg", "action", "load-config-file", "--path"])
            .arg(trusted_config);
        let output = run_command_bounded(
            &mut command,
            timeout.min(self.timeout),
            "Niri reload request",
        )?;
        if output.status.success() {
            Ok(())
        } else {
            Err(format!("Niri reload request exited with {}", output.status))
        }
    }

    fn subscribe_required(&self, timeout: Duration) -> Result<Box<dyn ConfigEventStream>, String> {
        let deadline = Instant::now() + timeout.min(self.timeout);
        loop {
            if let Some(socket) = self.socket.as_deref() {
                if std::fs::symlink_metadata(socket)
                    .map(|metadata| metadata.file_type().is_socket())
                    .unwrap_or(false)
                {
                    if let Some(stream) = self.subscribe_until(deadline)? {
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
    child: Option<Child>,
    reader: Option<thread::JoinHandle<()>>,
    cancelled: Arc<AtomicBool>,
    receiver: Receiver<NiriStreamMessage>,
    deadline: Instant,
}

impl ConfigEventStream for NiriEventStream {
    fn await_initial_snapshot(&mut self, timeout: Duration) -> Result<ConfigLoaded, String> {
        let deadline = (Instant::now() + timeout).min(self.deadline);
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
                    self.terminate();
                    return Err("timed out awaiting complete Niri initial snapshot".to_owned());
                }
                Err(RecvTimeoutError::Disconnected) => {
                    self.terminate();
                    return Err(
                        "Niri event stream closed before initial snapshot completed".to_owned()
                    );
                }
            }
        }
    }

    fn next_config_loaded(&mut self, timeout: Duration) -> Result<Option<ConfigLoaded>, String> {
        let timeout = timeout.min(self.deadline.saturating_duration_since(Instant::now()));
        match self.receiver.recv_timeout(timeout) {
            Ok(NiriStreamMessage::ConfigLoaded(event)) => Ok(Some(event)),
            Ok(NiriStreamMessage::InitialSnapshotComplete) => {
                Err("Niri emitted an unexpected second initial snapshot marker".to_owned())
            }
            Err(RecvTimeoutError::Timeout) => {
                self.terminate();
                Ok(None)
            }
            Err(RecvTimeoutError::Disconnected) => {
                self.terminate();
                Err("Niri event stream closed before ConfigLoaded".to_owned())
            }
        }
    }
}

impl Drop for NiriEventStream {
    fn drop(&mut self) {
        self.terminate();
    }
}

impl NiriEventStream {
    fn terminate(&mut self) {
        self.cancelled.store(true, Ordering::Release);
        if let Some(mut child) = self.child.take() {
            let _ = child.kill();
            let _ = child.wait();
        }
        if let Some(reader) = self.reader.take() {
            let _ = reader.join();
        }
    }
}

fn set_nonblocking(descriptor: i32) -> Result<(), String> {
    let flags = unsafe { libc::fcntl(descriptor, libc::F_GETFL) };
    if flags < 0 {
        return Err(format!(
            "failed to inspect Niri event stream flags: {}",
            std::io::Error::last_os_error()
        ));
    }
    if unsafe { libc::fcntl(descriptor, libc::F_SETFL, flags | libc::O_NONBLOCK) } < 0 {
        return Err(format!(
            "failed to make Niri event stream nonblocking: {}",
            std::io::Error::last_os_error()
        ));
    }
    Ok(())
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

fn ensure_supported_niri(executable: &Path, timeout: Duration) -> Result<(), String> {
    let mut command = Command::new(executable);
    command.arg("--version");
    let output = run_command_bounded(&mut command, timeout, "pinned Niri version query")?;
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

fn remaining(deadline: Instant) -> Result<Duration, String> {
    let remaining = deadline.saturating_duration_since(Instant::now());
    if remaining.is_zero() {
        Err("timed out before starting Niri subprocess".to_owned())
    } else {
        Ok(remaining)
    }
}

fn run_command_bounded(
    command: &mut Command,
    timeout: Duration,
    description: &str,
) -> Result<Output, String> {
    command.stdout(Stdio::piped()).stderr(Stdio::piped());
    let mut child = command
        .spawn()
        .map_err(|error| format!("failed to execute {description}: {error}"))?;
    let stdout = child
        .stdout
        .take()
        .ok_or_else(|| format!("{description} did not expose stdout"))?;
    let stderr = child
        .stderr
        .take()
        .ok_or_else(|| format!("{description} did not expose stderr"))?;
    let stdout_reader = thread::spawn(move || {
        let mut bytes = Vec::new();
        let mut stdout = stdout;
        stdout.read_to_end(&mut bytes).map(|_| bytes)
    });
    let stderr_reader = thread::spawn(move || {
        let mut bytes = Vec::new();
        let mut stderr = stderr;
        stderr.read_to_end(&mut bytes).map(|_| bytes)
    });
    let deadline = Instant::now() + timeout;
    let status = loop {
        match child.try_wait() {
            Ok(Some(status)) => break status,
            Ok(None) if Instant::now() < deadline => {
                thread::sleep(
                    Duration::from_millis(5)
                        .min(deadline.saturating_duration_since(Instant::now())),
                );
            }
            Ok(None) => {
                let _ = child.kill();
                let _ = child.wait();
                let _ = stdout_reader.join();
                let _ = stderr_reader.join();
                return Err(format!("timed out waiting for {description}"));
            }
            Err(error) => {
                let _ = child.kill();
                let _ = child.wait();
                let _ = stdout_reader.join();
                let _ = stderr_reader.join();
                return Err(format!("failed waiting for {description}: {error}"));
            }
        }
    };
    let stdout = stdout_reader
        .join()
        .map_err(|_| format!("{description} stdout reader panicked"))?
        .map_err(|error| format!("failed reading {description} stdout: {error}"))?;
    let stderr = stderr_reader
        .join()
        .map_err(|_| format!("{description} stderr reader panicked"))?
        .map_err(|error| format!("failed reading {description} stderr: {error}"))?;
    Ok(Output {
        status,
        stdout,
        stderr,
    })
}

pub trait ApplyObserver: Send + Sync {
    fn reached(&self, stage: ApplyStage) -> Result<(), String>;
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ApplyStage {
    WritableDirectoriesOpened,
    PublicationPartialWritten,
    PublicationFileSyncStarted,
    PublicationFileSynced,
    PublicationRenamed,
    PublicationDirectorySyncStarted,
    PublicationDirectorySynced,
    BindingStateSnapshotsCaptured,
    NiriSourceEntryEnumerated,
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
    if let Some(report) = store
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
            if include.is_none() || include.as_deref() == Some(compiled.as_bytes()) {
                return initialize_binding_only_locked(
                    &fs,
                    paths,
                    validator,
                    reloader,
                    &settings.active_preset_id,
                    compiled.as_bytes(),
                    include.is_none(),
                )
                .map(Some)
                .map_err(store_binding_error);
            }
            Ok(None)
        })
        .map_err(BindingError::from_store)?
    {
        return Ok(report);
    }
    apply_active_bindings(paths, validator, reloader)
}

fn initialize_binding_only_locked(
    fs: &BindingFileSystem,
    paths: &BindingPaths,
    validator: &dyn BindingValidator,
    reloader: &dyn BindingReloader,
    active_preset_id: &str,
    bindings: &[u8],
    include_missing: bool,
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
            "reconcile the existing binding transaction before initializing bindings",
        ));
    }
    if include_missing {
        validate_candidate_tree(fs, paths, bindings, validator)?;
    }

    let settings = fs
        .handles
        .settings
        .snapshot_regular(BindingFileSystem::artifact_name(ArtifactKind::Settings))
        .map_err(BindingError::from_store)?
        .ok_or_else(|| BindingError::new("concurrent_state_change", "settings disappeared"))?;
    let presets = fs
        .handles
        .presets
        .snapshot_regular(BindingFileSystem::artifact_name(ArtifactKind::Preset))
        .map_err(BindingError::from_store)?
        .ok_or_else(|| BindingError::new("concurrent_state_change", "presets disappeared"))?;
    paths.observe(ApplyStage::BindingStateSnapshotsCaptured)?;
    verify_binding_state_snapshots(fs, &settings, &presets)?;

    let mut stream = reloader
        .subscribe()
        .map_err(|message| BindingError::new("reload_failed", message))?;
    if let Some(stream) = stream.as_mut() {
        stream
            .await_initial_snapshot(RELOAD_TIMEOUT)
            .map_err(|message| BindingError::new("reload_failed", message))?;
    }

    let published = if include_missing {
        verify_binding_state_snapshots(fs, &settings, &presets)?;
        let temporary = format!(".sleepy-user-bindings.{}.tmp", Uuid::new_v4());
        let mut renamed = false;
        let publication = fs.niri.publish_new_no_replace(
            temporary.as_ref(),
            BindingFileSystem::artifact_name(ArtifactKind::Bindings),
            bindings,
            |boundary| {
                if matches!(boundary, crate::store::PublicationBoundary::Renamed) {
                    renamed = true;
                }
                paths
                    .observe(binding_publication_stage(boundary))
                    .map_err(store_binding_error)
            },
        );
        if let Err(error) = publication {
            if !renamed {
                return Err(BindingError::from_store(error));
            }
            let published = fs
                .niri
                .snapshot_regular(BindingFileSystem::artifact_name(ArtifactKind::Bindings))
                .map_err(BindingError::from_store)?;
            return match published {
                Some(published) => rollback_new_binding_only_include(
                    fs,
                    paths,
                    reloader,
                    active_preset_id,
                    &settings,
                    &presets,
                    &published,
                ),
                None => Ok(ApplyReport {
                    status: ApplyStatus::CommitStateUnknown,
                    active_preset_id: active_preset_id.to_owned(),
                }),
            };
        }
        Some(
            fs.niri
                .snapshot_regular(BindingFileSystem::artifact_name(ArtifactKind::Bindings))
                .map_err(BindingError::from_store)?
                .ok_or_else(|| {
                    BindingError::new("commit_state_unknown", "published bindings disappeared")
                })?,
        )
    } else {
        Some(
            fs.niri
                .snapshot_regular(BindingFileSystem::artifact_name(ArtifactKind::Bindings))
                .map_err(BindingError::from_store)?
                .ok_or_else(|| {
                    BindingError::new("concurrent_state_change", "matching bindings disappeared")
                })?,
        )
    };
    if !include_missing {
        let matching = published.expect("matching include has a snapshot");
        verify_binding_state_snapshots(fs, &settings, &presets)?;
        verify_binding_snapshot(fs, &matching)?;
        let Some(mut stream) = stream else {
            return Ok(ApplyReport {
                status: ApplyStatus::ReloadPending,
                active_preset_id: active_preset_id.to_owned(),
            });
        };
        if reload_confirmed(paths, reloader, stream.as_mut(), RELOAD_TIMEOUT)? {
            verify_binding_state_snapshots(fs, &settings, &presets)?;
            verify_binding_snapshot(fs, &matching)?;
            return Ok(ApplyReport {
                status: ApplyStatus::Committed,
                active_preset_id: active_preset_id.to_owned(),
            });
        }
        return Ok(ApplyReport {
            status: ApplyStatus::CommitStateUnknown,
            active_preset_id: active_preset_id.to_owned(),
        });
    }

    let published = published.expect("new include has a snapshot");
    let candidate_result = (|| -> Result<ApplyReport, BindingError> {
        verify_binding_state_snapshots(fs, &settings, &presets)?;
        verify_binding_snapshot(fs, &published)?;
        let Some(mut stream) = stream else {
            return Ok(ApplyReport {
                status: ApplyStatus::ReloadPending,
                active_preset_id: active_preset_id.to_owned(),
            });
        };
        if reload_confirmed(paths, reloader, stream.as_mut(), RELOAD_TIMEOUT)? {
            verify_binding_state_snapshots(fs, &settings, &presets)?;
            verify_binding_snapshot(fs, &published)?;
            return Ok(ApplyReport {
                status: ApplyStatus::Committed,
                active_preset_id: active_preset_id.to_owned(),
            });
        }
        Err(BindingError::new(
            "reload_rejected",
            "Niri did not confirm the newly published bindings",
        ))
    })();
    match candidate_result {
        Ok(report) => Ok(report),
        Err(_) => rollback_new_binding_only_include(
            fs,
            paths,
            reloader,
            active_preset_id,
            &settings,
            &presets,
            &published,
        ),
    }
}

fn verify_binding_snapshot(
    fs: &BindingFileSystem,
    expected: &crate::store::SecureFileSnapshot,
) -> Result<(), BindingError> {
    if fs
        .niri
        .snapshot_matches(
            BindingFileSystem::artifact_name(ArtifactKind::Bindings),
            expected,
        )
        .map_err(BindingError::from_store)?
    {
        Ok(())
    } else {
        Err(BindingError::new(
            "concurrent_state_change",
            "generated bindings changed during initialization",
        ))
    }
}

fn rollback_new_binding_only_include(
    fs: &BindingFileSystem,
    paths: &BindingPaths,
    reloader: &dyn BindingReloader,
    active_preset_id: &str,
    settings: &crate::store::SecureFileSnapshot,
    presets: &crate::store::SecureFileSnapshot,
    published: &crate::store::SecureFileSnapshot,
) -> Result<ApplyReport, BindingError> {
    let unknown = || ApplyReport {
        status: ApplyStatus::CommitStateUnknown,
        active_preset_id: active_preset_id.to_owned(),
    };
    if !fs
        .niri
        .snapshot_matches(
            BindingFileSystem::artifact_name(ArtifactKind::Bindings),
            published,
        )
        .map_err(BindingError::from_store)?
    {
        return Ok(unknown());
    }
    if fs
        .niri
        .remove_file(BindingFileSystem::artifact_name(ArtifactKind::Bindings))
        .and_then(|()| fs.niri.sync())
        .is_err()
    {
        return Ok(unknown());
    }
    let mut stream = match reloader.subscribe() {
        Ok(stream) => stream,
        Err(_) => return Ok(unknown()),
    };
    let Some(stream) = stream.as_mut() else {
        return Ok(unknown());
    };
    if stream.await_initial_snapshot(RELOAD_TIMEOUT).is_err()
        || !reload_confirmed(paths, reloader, stream.as_mut(), RELOAD_TIMEOUT).unwrap_or(false)
    {
        return Ok(unknown());
    }
    if verify_binding_state_snapshots(fs, settings, presets).is_err() {
        return Ok(unknown());
    }
    Ok(ApplyReport {
        status: ApplyStatus::RolledBackConfirmed,
        active_preset_id: active_preset_id.to_owned(),
    })
}

fn verify_binding_state_snapshots(
    fs: &BindingFileSystem,
    settings: &crate::store::SecureFileSnapshot,
    presets: &crate::store::SecureFileSnapshot,
) -> Result<(), BindingError> {
    for (directory, name, snapshot, document) in [
        (
            &fs.handles.settings,
            BindingFileSystem::artifact_name(ArtifactKind::Settings),
            settings,
            "settings",
        ),
        (
            &fs.handles.presets,
            BindingFileSystem::artifact_name(ArtifactKind::Preset),
            presets,
            "presets",
        ),
    ] {
        if !directory
            .snapshot_matches(name, snapshot)
            .map_err(BindingError::from_store)?
        {
            return Err(BindingError::new(
                "concurrent_state_change",
                format!("{document} changed during binding initialization"),
            ));
        }
    }
    Ok(())
}

fn binding_publication_stage(boundary: crate::store::PublicationBoundary) -> ApplyStage {
    match boundary {
        crate::store::PublicationBoundary::PartialWritten => ApplyStage::PublicationPartialWritten,
        crate::store::PublicationBoundary::FileSyncStarted => {
            ApplyStage::PublicationFileSyncStarted
        }
        crate::store::PublicationBoundary::FileSynced => ApplyStage::PublicationFileSynced,
        crate::store::PublicationBoundary::Renamed => ApplyStage::PublicationRenamed,
        crate::store::PublicationBoundary::DirectorySyncStarted => {
            ApplyStage::PublicationDirectorySyncStarted
        }
        crate::store::PublicationBoundary::DirectorySynced => {
            ApplyStage::PublicationDirectorySynced
        }
    }
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
    let online_deadline = online_required.then(|| Instant::now() + RELOAD_TIMEOUT);
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
                .subscribe_required(
                    remaining(online_deadline.expect("required deadline"))
                        .map_err(|message| BindingError::new("niri_unavailable", message))?,
                )
                .map_err(|message| BindingError::new("niri_unavailable", message))?,
        )
    } else {
        reloader
            .subscribe()
            .map_err(|message| BindingError::new("reload_failed", message))?
    };
    if let Some(stream) = stream.as_mut() {
        stream
            .await_initial_snapshot(operation_timeout(online_deadline))
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
    journal.verify_noop_artifacts(fs)?;
    if reload_confirmed(
        paths,
        reloader,
        stream.as_mut(),
        operation_timeout(online_deadline),
    )? {
        journal.verify_noop_artifacts(fs)?;
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

    let rollback_stream = if online_required {
        remaining(online_deadline.expect("required deadline"))
            .and_then(|timeout| reloader.subscribe_required(timeout).map(Some))
    } else {
        reloader.subscribe()
    };
    let mut rollback_stream = match rollback_stream {
        Ok(Some(mut stream)) => {
            if stream
                .await_initial_snapshot(operation_timeout(online_deadline))
                .is_ok()
            {
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
    journal.verify_noop_artifacts(fs)?;
    let confirmed = match rollback_stream.as_mut() {
        Some(stream) => reload_confirmed(
            paths,
            reloader,
            stream.as_mut(),
            operation_timeout(online_deadline),
        )?,
        None => false,
    };
    if confirmed {
        journal.verify_noop_artifacts(fs)?;
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
    journal.verify_noop_artifacts(fs)?;
    if reload_confirmed(paths, reloader, candidate_stream.as_mut(), RELOAD_TIMEOUT)? {
        journal.verify_noop_artifacts(fs)?;
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
    journal.verify_noop_artifacts(fs)?;
    let rollback_confirmed = match rollback_stream.as_mut() {
        Some(stream) => reload_confirmed(paths, reloader, stream.as_mut(), RELOAD_TIMEOUT)?,
        None => false,
    };
    if rollback_confirmed {
        journal.verify_noop_artifacts(fs)?;
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
    timeout: Duration,
) -> Result<bool, BindingError> {
    if reloader
        .request_reload_with_timeout(paths.trusted_config(), timeout)
        .is_err()
    {
        return Ok(false);
    }
    paths.observe(ApplyStage::ReloadRequested)?;
    Ok(matches!(
        stream.next_config_loaded(timeout),
        Ok(Some(ConfigLoaded { failed: false }))
    ))
}

fn operation_timeout(deadline: Option<Instant>) -> Duration {
    deadline
        .map(|deadline| deadline.saturating_duration_since(Instant::now()))
        .unwrap_or(RELOAD_TIMEOUT)
}

fn validate_candidate_tree(
    fs: &BindingFileSystem,
    paths: &BindingPaths,
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
        copy_config_tree(&fs.niri, &staging_root, true, paths)?;
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
    source: &SecureDir,
    destination: &SecureDir,
    root: bool,
    paths: &BindingPaths,
) -> Result<(), BindingError> {
    for name in source.entries().map_err(BindingError::from_store)? {
        if root && name == BindingFileSystem::artifact_name(ArtifactKind::Bindings) {
            continue;
        }
        paths.observe(ApplyStage::NiriSourceEntryEnumerated)?;
        match source.open_entry(&name).map_err(BindingError::from_store)? {
            SecureEntry::Symlink(target) => {
                let bytes = SecureDir::read_root_store_regular(&target)
                    .map_err(BindingError::from_store)?;
                destination
                    .write_new(&name, &bytes)
                    .map_err(BindingError::from_store)?;
            }
            SecureEntry::Directory(child) => {
                let staged_child = destination
                    .child_writable(&name, true)
                    .map_err(BindingError::from_store)?;
                copy_config_tree(&child, &staged_child, false, paths)?;
                staged_child.sync().map_err(BindingError::from_store)?;
            }
            SecureEntry::Regular(bytes) => {
                destination
                    .write_new(&name, &bytes)
                    .map_err(BindingError::from_store)?;
            }
        }
    }
    Ok(())
}

fn remove_tree(directory: &SecureDir) -> Result<(), BindingError> {
    for name in directory.entries().map_err(BindingError::from_store)? {
        match directory
            .open_entry(&name)
            .map_err(BindingError::from_store)?
        {
            SecureEntry::Directory(child) => {
                remove_tree(&child)?;
                directory
                    .remove_dir(&name)
                    .map_err(BindingError::from_store)?;
            }
            SecureEntry::Regular(_) | SecureEntry::Symlink(_) => {
                directory
                    .remove_file(&name)
                    .map_err(BindingError::from_store)?;
            }
        }
    }
    directory.sync().map_err(BindingError::from_store)
}

#[cfg(test)]
mod process_lifecycle_tests {
    use std::sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    };

    use super::*;

    #[test]
    fn event_stream_termination_joins_reader_before_returning() {
        let child = Command::new(std::env::current_exe().unwrap())
            .args(["--exact", "__sleepy_event_stream_child__"])
            .spawn()
            .unwrap();
        let (_sender, receiver) = mpsc::channel();
        let completed = Arc::new(AtomicBool::new(false));
        let completed_in_reader = Arc::clone(&completed);
        let reader = thread::spawn(move || {
            thread::sleep(Duration::from_millis(25));
            completed_in_reader.store(true, Ordering::SeqCst);
        });
        let mut stream = NiriEventStream {
            child: Some(child),
            reader: Some(reader),
            cancelled: Arc::new(AtomicBool::new(false)),
            receiver,
            deadline: Instant::now() + Duration::from_secs(1),
        };

        stream.terminate();

        assert!(completed.load(Ordering::SeqCst));
    }
}
