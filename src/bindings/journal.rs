use std::{
    collections::BTreeSet,
    fs::{self, File, OpenOptions},
    io::Write,
    os::unix::fs::OpenOptionsExt,
    path::{Path, PathBuf},
};

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use uuid::Uuid;

use super::{ApplyStage, BindingError, BindingPaths};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) enum JournalPhase {
    Prepared,
    PresetCommitted,
    SettingsCommitted,
    BindingsCommitted,
    ReloadPending,
    ReloadConfirmed,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) enum RecoveryTarget {
    Candidate,
    Previous,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) enum ArtifactKind {
    Preset,
    Settings,
    Bindings,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(crate) struct JournalArtifact {
    pub kind: ArtifactKind,
    pub destination: PathBuf,
    pub old_artifact: PathBuf,
    pub new_artifact: PathBuf,
    pub old_existed: bool,
    pub old_hash: String,
    pub new_hash: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(crate) struct TransactionJournal {
    pub schema_version: u32,
    pub transaction_id: String,
    pub phase: JournalPhase,
    pub recovery_target: RecoveryTarget,
    #[serde(default = "sidecars_complete_for_legacy_journal")]
    pub sidecars_complete: bool,
    pub active_preset_id: String,
    pub previous_active_preset_id: Option<String>,
    pub artifacts: Vec<JournalArtifact>,
}

impl TransactionJournal {
    pub fn load(paths: &BindingPaths) -> Result<Option<Self>, BindingError> {
        let bytes = match fs::read(paths.journal()) {
            Ok(bytes) => bytes,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
            Err(error) => return Err(io_error("read binding transaction journal", error)),
        };
        let journal: Self = serde_json::from_slice(&bytes)
            .map_err(|error| BindingError::new("invalid_journal", error.to_string()))?;
        journal.validate(paths)?;
        Ok(Some(journal))
    }

    pub fn prepare(
        paths: &BindingPaths,
        active_preset_id: &str,
        preset_bytes: &[u8],
        settings_bytes: &[u8],
        binding_bytes: &[u8],
    ) -> Result<Self, BindingError> {
        let transaction_id = Uuid::new_v4().hyphenated().to_string();
        let specifications = [
            (
                ArtifactKind::Preset,
                paths.store().presets_path(),
                preset_bytes,
            ),
            (
                ArtifactKind::Settings,
                paths.store().settings_path(),
                settings_bytes,
            ),
            (
                ArtifactKind::Bindings,
                paths.generated_include().to_owned(),
                binding_bytes,
            ),
        ];
        let mut artifacts = Vec::new();
        let mut sidecar_bytes = Vec::new();
        let mut previous_active_preset_id = None;
        for (kind, destination, new_bytes) in specifications {
            let old = match fs::read(&destination) {
                Ok(bytes) => (true, bytes),
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => (false, Vec::new()),
                Err(error) => return Err(io_error("read previous transaction artifact", error)),
            };
            let directory = destination.parent().ok_or_else(|| {
                BindingError::new("unsafe_path", "transaction destination has no parent")
            })?;
            let name = destination
                .file_name()
                .and_then(|name| name.to_str())
                .ok_or_else(|| BindingError::new("unsafe_path", "non-UTF-8 artifact name"))?;
            let old_artifact = directory.join(format!(".{name}.{transaction_id}.old"));
            let new_artifact = directory.join(format!(".{name}.{transaction_id}.new"));
            if kind == ArtifactKind::Settings && old.0 {
                previous_active_preset_id = String::from_utf8(old.1.clone())
                    .ok()
                    .and_then(|input| sleepy_sdk::validate_settings(&input).ok())
                    .map(|settings| settings.active_preset_id);
            }
            artifacts.push(JournalArtifact {
                kind,
                destination,
                old_artifact,
                new_artifact,
                old_existed: old.0,
                old_hash: hash(&old.1),
                new_hash: hash(new_bytes),
            });
            sidecar_bytes.push((old.1, new_bytes.to_vec()));
        }
        let mut journal = Self {
            schema_version: 1,
            transaction_id,
            phase: JournalPhase::Prepared,
            recovery_target: RecoveryTarget::Candidate,
            sidecars_complete: false,
            active_preset_id: active_preset_id.to_owned(),
            previous_active_preset_id,
            artifacts,
        };
        journal.persist_initial(paths)?;
        for (artifact, (old_bytes, new_bytes)) in journal.artifacts.iter().zip(sidecar_bytes.iter())
        {
            write_new_file(&artifact.old_artifact, old_bytes)?;
            paths.observe(sidecar_stage(artifact.kind, false))?;
            write_new_file(&artifact.new_artifact, new_bytes)?;
            paths.observe(sidecar_stage(artifact.kind, true))?;
            sync_directory(
                artifact
                    .destination
                    .parent()
                    .expect("closed artifact has parent"),
            )?;
        }
        journal.sidecars_complete = true;
        journal.persist(paths)?;
        paths.observe(ApplyStage::PreparedSynced)?;
        Ok(journal)
    }

    pub fn artifact(&self, kind: ArtifactKind) -> &JournalArtifact {
        self.artifacts
            .iter()
            .find(|artifact| artifact.kind == kind)
            .expect("journal contains all closed artifacts")
    }

    pub fn active_preset_id_for(&self, target: RecoveryTarget) -> Result<String, BindingError> {
        if target == RecoveryTarget::Candidate {
            return Ok(self.active_preset_id.clone());
        }
        Ok(self
            .previous_active_preset_id
            .clone()
            .unwrap_or_else(|| "unknown".to_owned()))
    }

    pub fn install_new(
        &self,
        paths: &BindingPaths,
        kind: ArtifactKind,
    ) -> Result<(), BindingError> {
        install(Some(paths), self.artifact(kind), RecoveryTarget::Candidate)
    }

    pub fn restore_all(&self, paths: &BindingPaths) -> Result<(), BindingError> {
        for artifact in &self.artifacts {
            install(Some(paths), artifact, RecoveryTarget::Previous)?;
        }
        Ok(())
    }

    pub fn install_target(
        &self,
        paths: &BindingPaths,
        target: RecoveryTarget,
    ) -> Result<(), BindingError> {
        for artifact in &self.artifacts {
            install(Some(paths), artifact, target)?;
        }
        Ok(())
    }

    pub fn set_phase(
        &mut self,
        paths: &BindingPaths,
        phase: JournalPhase,
    ) -> Result<(), BindingError> {
        self.phase = phase;
        self.persist(paths)?;
        let stage = match phase {
            JournalPhase::Prepared => ApplyStage::PreparedSynced,
            JournalPhase::PresetCommitted => ApplyStage::PresetCommittedSynced,
            JournalPhase::SettingsCommitted => ApplyStage::SettingsCommittedSynced,
            JournalPhase::BindingsCommitted => ApplyStage::BindingsCommittedSynced,
            JournalPhase::ReloadPending => ApplyStage::ReloadPendingSynced,
            JournalPhase::ReloadConfirmed => ApplyStage::ReloadConfirmedSynced,
        };
        paths.observe(stage)
    }

    pub fn set_recovery_target(
        &mut self,
        paths: &BindingPaths,
        target: RecoveryTarget,
    ) -> Result<(), BindingError> {
        self.recovery_target = target;
        self.persist(paths)
    }

    pub fn persist(&self, paths: &BindingPaths) -> Result<(), BindingError> {
        let bytes = serde_json::to_vec(self)
            .map_err(|error| BindingError::new("invalid_journal", error.to_string()))?;
        atomic_replace(paths.journal(), &bytes)
    }

    fn persist_initial(&self, paths: &BindingPaths) -> Result<(), BindingError> {
        let bytes = serde_json::to_vec(self)
            .map_err(|error| BindingError::new("invalid_journal", error.to_string()))?;
        write_new_file(paths.journal(), &bytes)?;
        sync_directory(paths.journal().parent().expect("journal has parent"))
    }

    pub fn cleanup(&self, paths: &BindingPaths) -> Result<(), BindingError> {
        let mut directories = BTreeSet::new();
        for artifact in &self.artifacts {
            remove_if_exists(&artifact.old_artifact)?;
            remove_if_exists(&artifact.new_artifact)?;
            if let Some(parent) = artifact.destination.parent() {
                directories.insert(parent.to_owned());
            }
        }
        paths.observe(ApplyStage::ArtifactsRemoved)?;
        for directory in directories {
            sync_directory(&directory)?;
        }
        paths.observe(ApplyStage::ArtifactDirectoriesSynced)?;
        remove_if_exists(paths.journal())?;
        paths.observe(ApplyStage::JournalRemoved)?;
        sync_directory(paths.journal().parent().expect("journal has parent"))?;
        paths.observe(ApplyStage::JournalDirectorySynced)
    }

    fn validate(&self, paths: &BindingPaths) -> Result<(), BindingError> {
        if self.schema_version != 1
            || Uuid::parse_str(&self.transaction_id)
                .map(|id| id.hyphenated().to_string() != self.transaction_id)
                .unwrap_or(true)
            || self.active_preset_id.trim().is_empty()
            || self.artifacts.len() != 3
        {
            return Err(BindingError::new(
                "invalid_journal",
                "binding transaction journal header is invalid",
            ));
        }
        let expected = [
            (ArtifactKind::Preset, paths.store().presets_path()),
            (ArtifactKind::Settings, paths.store().settings_path()),
            (ArtifactKind::Bindings, paths.generated_include().to_owned()),
        ];
        for (kind, destination) in expected {
            let matches = self
                .artifacts
                .iter()
                .filter(|artifact| artifact.kind == kind)
                .collect::<Vec<_>>();
            if matches.len() != 1 || matches[0].destination != destination {
                return Err(BindingError::new(
                    "invalid_journal",
                    "binding transaction artifact destination is invalid",
                ));
            }
            let artifact = matches[0];
            let directory = destination.parent().expect("closed destination has parent");
            let name = destination
                .file_name()
                .and_then(|name| name.to_str())
                .expect("closed destination is UTF-8");
            if artifact.old_artifact
                != directory.join(format!(".{name}.{}.old", self.transaction_id))
                || artifact.new_artifact
                    != directory.join(format!(".{name}.{}.new", self.transaction_id))
            {
                return Err(BindingError::new(
                    "invalid_journal",
                    "binding transaction sidecar path is invalid",
                ));
            }
            for (path, expected_hash) in [
                (&artifact.old_artifact, &artifact.old_hash),
                (&artifact.new_artifact, &artifact.new_hash),
            ] {
                super::apply::validate_writable_file_if_present(path)?;
                let bytes = match fs::read(path) {
                    Ok(bytes) => bytes,
                    Err(error)
                        if error.kind() == std::io::ErrorKind::NotFound
                            && (!self.sidecars_complete
                                || self.phase == JournalPhase::ReloadConfirmed) =>
                    {
                        continue;
                    }
                    Err(error) => {
                        return Err(io_error("read binding transaction sidecar", error));
                    }
                };
                if hash(&bytes) != *expected_hash {
                    return Err(BindingError::new(
                        "invalid_journal",
                        "binding transaction sidecar hash mismatch",
                    ));
                }
            }
        }
        Ok(())
    }
}

fn sidecars_complete_for_legacy_journal() -> bool {
    true
}

fn sidecar_stage(kind: ArtifactKind, new: bool) -> ApplyStage {
    match (kind, new) {
        (ArtifactKind::Preset, false) => ApplyStage::PresetOldSidecarSynced,
        (ArtifactKind::Preset, true) => ApplyStage::PresetNewSidecarSynced,
        (ArtifactKind::Settings, false) => ApplyStage::SettingsOldSidecarSynced,
        (ArtifactKind::Settings, true) => ApplyStage::SettingsNewSidecarSynced,
        (ArtifactKind::Bindings, false) => ApplyStage::BindingsOldSidecarSynced,
        (ArtifactKind::Bindings, true) => ApplyStage::BindingsNewSidecarSynced,
    }
}

fn install(
    paths: Option<&BindingPaths>,
    artifact: &JournalArtifact,
    target: RecoveryTarget,
) -> Result<(), BindingError> {
    if target == RecoveryTarget::Previous && !artifact.old_existed {
        remove_if_exists(&artifact.destination)?;
        return sync_directory(artifact.destination.parent().expect("artifact has parent"));
    }
    let source = match target {
        RecoveryTarget::Candidate => &artifact.new_artifact,
        RecoveryTarget::Previous => &artifact.old_artifact,
    };
    let bytes = fs::read(source).map_err(|error| io_error("read durable artifact", error))?;
    let expected = match target {
        RecoveryTarget::Candidate => &artifact.new_hash,
        RecoveryTarget::Previous => &artifact.old_hash,
    };
    if &hash(&bytes) != expected {
        return Err(BindingError::new(
            "invalid_journal",
            "durable transaction artifact hash mismatch",
        ));
    }
    atomic_replace_observed(paths, artifact.kind, target, &artifact.destination, &bytes)
}

fn atomic_replace(destination: &Path, bytes: &[u8]) -> Result<(), BindingError> {
    atomic_replace_observed(
        None,
        ArtifactKind::Settings,
        RecoveryTarget::Candidate,
        destination,
        bytes,
    )
}

fn atomic_replace_observed(
    paths: Option<&BindingPaths>,
    kind: ArtifactKind,
    target: RecoveryTarget,
    destination: &Path,
    bytes: &[u8],
) -> Result<(), BindingError> {
    let parent = destination
        .parent()
        .ok_or_else(|| BindingError::new("unsafe_path", "artifact has no parent"))?;
    fs::create_dir_all(parent).map_err(|error| io_error("create artifact directory", error))?;
    let name = destination
        .file_name()
        .and_then(|name| name.to_str())
        .ok_or_else(|| BindingError::new("unsafe_path", "non-UTF-8 artifact name"))?;
    let temporary = parent.join(format!(".{name}.{}.tmp", Uuid::new_v4()));
    write_new_file(&temporary, bytes)?;
    fs::rename(&temporary, destination)
        .map_err(|error| io_error("rename transaction artifact", error))?;
    if let Some(paths) = paths {
        paths.observe(replacement_stage(kind, target, false))?;
    }
    sync_directory(parent)?;
    if let Some(paths) = paths {
        paths.observe(replacement_stage(kind, target, true))?;
    }
    Ok(())
}

fn replacement_stage(kind: ArtifactKind, target: RecoveryTarget, synced: bool) -> ApplyStage {
    match (target, kind, synced) {
        (RecoveryTarget::Candidate, ArtifactKind::Preset, false) => ApplyStage::PresetRenamed,
        (RecoveryTarget::Candidate, ArtifactKind::Preset, true) => {
            ApplyStage::PresetDirectorySynced
        }
        (RecoveryTarget::Candidate, ArtifactKind::Settings, false) => ApplyStage::SettingsRenamed,
        (RecoveryTarget::Candidate, ArtifactKind::Settings, true) => {
            ApplyStage::SettingsDirectorySynced
        }
        (RecoveryTarget::Candidate, ArtifactKind::Bindings, false) => ApplyStage::BindingsRenamed,
        (RecoveryTarget::Candidate, ArtifactKind::Bindings, true) => {
            ApplyStage::BindingsDirectorySynced
        }
        (RecoveryTarget::Previous, ArtifactKind::Preset, false) => {
            ApplyStage::RollbackPresetRenamed
        }
        (RecoveryTarget::Previous, ArtifactKind::Preset, true) => {
            ApplyStage::RollbackPresetDirectorySynced
        }
        (RecoveryTarget::Previous, ArtifactKind::Settings, false) => {
            ApplyStage::RollbackSettingsRenamed
        }
        (RecoveryTarget::Previous, ArtifactKind::Settings, true) => {
            ApplyStage::RollbackSettingsDirectorySynced
        }
        (RecoveryTarget::Previous, ArtifactKind::Bindings, false) => {
            ApplyStage::RollbackBindingsRenamed
        }
        (RecoveryTarget::Previous, ArtifactKind::Bindings, true) => {
            ApplyStage::RollbackBindingsDirectorySynced
        }
    }
}

fn write_new_file(path: &Path, bytes: &[u8]) -> Result<(), BindingError> {
    let mut file = OpenOptions::new()
        .write(true)
        .create_new(true)
        .mode(0o600)
        .open(path)
        .map_err(|error| io_error("create durable transaction artifact", error))?;
    file.write_all(bytes)
        .and_then(|()| file.sync_all())
        .map_err(|error| io_error("sync durable transaction artifact", error))
}

fn sync_directory(path: &Path) -> Result<(), BindingError> {
    File::open(path)
        .and_then(|directory| directory.sync_all())
        .map_err(|error| io_error("sync transaction directory", error))
}

fn remove_if_exists(path: &Path) -> Result<(), BindingError> {
    match fs::remove_file(path) {
        Ok(()) => Ok(()),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(error) => Err(io_error("remove transaction artifact", error)),
    }
}

fn hash(bytes: &[u8]) -> String {
    Sha256::digest(bytes)
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect()
}

pub(crate) fn io_error(context: &str, error: std::io::Error) -> BindingError {
    BindingError::new("io_error", format!("{context}: {error}"))
}
