use std::{collections::BTreeSet, path::PathBuf};

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use uuid::Uuid;

use crate::store::PublicationBoundary;

use super::{secure_fs::BindingFileSystem, ApplyStage, BindingError, BindingPaths};

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
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

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
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
    pub fn load(
        fs: &BindingFileSystem,
        paths: &BindingPaths,
    ) -> Result<Option<Self>, BindingError> {
        let Some(bytes) = fs
            .handles
            .presets
            .read_optional(BindingFileSystem::journal_name())
            .map_err(BindingError::from_store)?
        else {
            cleanup_orphan_journal_temps(fs)?;
            return Ok(None);
        };
        let journal: Self = serde_json::from_slice(&bytes)
            .map_err(|e| BindingError::new("invalid_journal", e.to_string()))?;
        journal.validate(fs, paths)?;
        Ok(Some(journal))
    }

    pub fn prepare(
        fs: &BindingFileSystem,
        paths: &BindingPaths,
        active_preset_id: &str,
        preset_bytes: &[u8],
        settings_bytes: &[u8],
        binding_bytes: &[u8],
    ) -> Result<Self, BindingError> {
        let transaction_id = Uuid::new_v4().hyphenated().to_string();
        let specs = [
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
        let mut sidecars = Vec::new();
        let mut previous_active_preset_id = None;
        for (kind, destination, new_bytes) in specs {
            let old = fs
                .artifact_dir(kind)
                .read_optional(BindingFileSystem::artifact_name(kind))
                .map_err(BindingError::from_store)?;
            let old_existed = old.is_some();
            let old = old.unwrap_or_default();
            let name = destination
                .file_name()
                .and_then(|n| n.to_str())
                .ok_or_else(|| BindingError::new("unsafe_path", "non-UTF-8 artifact name"))?
                .to_owned();
            let directory = destination
                .parent()
                .ok_or_else(|| BindingError::new("unsafe_path", "artifact has no parent"))?
                .to_owned();
            if kind == ArtifactKind::Settings && old_existed {
                previous_active_preset_id = String::from_utf8(old.clone())
                    .ok()
                    .and_then(|s| sleepy_sdk::validate_settings(&s).ok())
                    .map(|s| s.active_preset_id);
            }
            artifacts.push(JournalArtifact {
                kind,
                destination,
                old_artifact: directory.join(format!(".{name}.{transaction_id}.old")),
                new_artifact: directory.join(format!(".{name}.{transaction_id}.new")),
                old_existed,
                old_hash: hash(&old),
                new_hash: hash(new_bytes),
            });
            sidecars.push((old, new_bytes.to_vec()));
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
        journal.persist_initial(fs, paths)?;
        for (artifact, (old, new)) in journal.artifacts.iter().zip(sidecars.iter()) {
            let dir = fs.artifact_dir(artifact.kind);
            publish_sidecar(fs, paths, artifact.kind, &artifact.old_artifact, old)?;
            paths.observe(sidecar_stage(artifact.kind, false))?;
            publish_sidecar(fs, paths, artifact.kind, &artifact.new_artifact, new)?;
            paths.observe(sidecar_stage(artifact.kind, true))?;
            dir.sync().map_err(BindingError::from_store)?;
        }
        journal.sidecars_complete = true;
        journal.persist(fs)?;
        paths.observe(ApplyStage::PreparedSynced)?;
        Ok(journal)
    }

    fn artifact(&self, kind: ArtifactKind) -> &JournalArtifact {
        self.artifacts
            .iter()
            .find(|a| a.kind == kind)
            .expect("closed journal artifacts")
    }
    pub fn active_preset_id_for(&self, target: RecoveryTarget) -> Result<String, BindingError> {
        Ok(if target == RecoveryTarget::Candidate {
            self.active_preset_id.clone()
        } else {
            self.previous_active_preset_id
                .clone()
                .unwrap_or_else(|| "unknown".to_owned())
        })
    }
    pub fn install_new(
        &self,
        fs: &BindingFileSystem,
        paths: &BindingPaths,
        kind: ArtifactKind,
    ) -> Result<(), BindingError> {
        install(
            fs,
            Some(paths),
            self.artifact(kind),
            RecoveryTarget::Candidate,
        )
    }
    pub fn restore_all(
        &self,
        fs: &BindingFileSystem,
        paths: &BindingPaths,
    ) -> Result<(), BindingError> {
        for a in &self.artifacts {
            install(fs, Some(paths), a, RecoveryTarget::Previous)?;
        }
        Ok(())
    }
    pub fn install_target(
        &self,
        fs: &BindingFileSystem,
        paths: &BindingPaths,
        target: RecoveryTarget,
    ) -> Result<(), BindingError> {
        for a in &self.artifacts {
            install(fs, Some(paths), a, target)?;
        }
        Ok(())
    }

    pub fn set_phase(
        &mut self,
        fs: &BindingFileSystem,
        paths: &BindingPaths,
        phase: JournalPhase,
    ) -> Result<(), BindingError> {
        self.phase = phase;
        self.persist(fs)?;
        paths.observe(match phase {
            JournalPhase::Prepared => ApplyStage::PreparedSynced,
            JournalPhase::PresetCommitted => ApplyStage::PresetCommittedSynced,
            JournalPhase::SettingsCommitted => ApplyStage::SettingsCommittedSynced,
            JournalPhase::BindingsCommitted => ApplyStage::BindingsCommittedSynced,
            JournalPhase::ReloadPending => ApplyStage::ReloadPendingSynced,
            JournalPhase::ReloadConfirmed => ApplyStage::ReloadConfirmedSynced,
        })
    }
    pub fn set_recovery_target(
        &mut self,
        fs: &BindingFileSystem,
        target: RecoveryTarget,
    ) -> Result<(), BindingError> {
        self.recovery_target = target;
        self.persist(fs)
    }
    fn persist(&self, fs: &BindingFileSystem) -> Result<(), BindingError> {
        let bytes = serde_json::to_vec(self)
            .map_err(|e| BindingError::new("invalid_journal", e.to_string()))?;
        fs.handles
            .presets
            .atomic_replace(
                BindingFileSystem::journal_name(),
                &bytes,
                || Ok(()),
                || Ok(()),
                || Ok(()),
            )
            .map_err(BindingError::from_store)
    }
    fn persist_initial(
        &self,
        fs: &BindingFileSystem,
        paths: &BindingPaths,
    ) -> Result<(), BindingError> {
        let bytes = serde_json::to_vec(self)
            .map_err(|e| BindingError::new("invalid_journal", e.to_string()))?;
        let temporary = format!(
            ".bindings-transaction.json.{}.prepare.tmp",
            self.transaction_id
        );
        fs.handles
            .presets
            .publish_new(
                temporary.as_ref(),
                BindingFileSystem::journal_name(),
                &bytes,
                |boundary| observe_publication(paths, boundary),
            )
            .map_err(BindingError::from_store)
    }

    pub fn cleanup(
        &self,
        fs: &BindingFileSystem,
        paths: &BindingPaths,
    ) -> Result<(), BindingError> {
        let mut dirs = BTreeSet::new();
        for a in &self.artifacts {
            let dir = fs.artifact_dir(a.kind);
            dir.remove_file(BindingFileSystem::sidecar_name(&a.old_artifact)?)
                .map_err(BindingError::from_store)?;
            dir.remove_file(BindingFileSystem::sidecar_name(&a.new_artifact)?)
                .map_err(BindingError::from_store)?;
            for sidecar in [&a.old_artifact, &a.new_artifact] {
                let temporary = publication_temp(BindingFileSystem::sidecar_name(sidecar)?);
                dir.remove_file(temporary.as_ref())
                    .map_err(BindingError::from_store)?;
            }
            dirs.insert(a.kind);
        }
        paths.observe(ApplyStage::ArtifactsRemoved)?;
        for kind in dirs {
            fs.artifact_dir(kind)
                .sync()
                .map_err(BindingError::from_store)?;
        }
        paths.observe(ApplyStage::ArtifactDirectoriesSynced)?;
        fs.handles
            .presets
            .remove_file(BindingFileSystem::journal_name())
            .map_err(BindingError::from_store)?;
        paths.observe(ApplyStage::JournalRemoved)?;
        fs.handles
            .presets
            .sync()
            .map_err(BindingError::from_store)?;
        paths.observe(ApplyStage::JournalDirectorySynced)
    }

    fn validate(&self, fs: &BindingFileSystem, paths: &BindingPaths) -> Result<(), BindingError> {
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
                .filter(|a| a.kind == kind)
                .collect::<Vec<_>>();
            if matches.len() != 1 || matches[0].destination != destination {
                return Err(BindingError::new(
                    "invalid_journal",
                    "binding transaction artifact destination is invalid",
                ));
            }
            let a = matches[0];
            let dir = destination.parent().expect("closed parent");
            let name = destination
                .file_name()
                .and_then(|n| n.to_str())
                .expect("closed utf8");
            if a.old_artifact != dir.join(format!(".{name}.{}.old", self.transaction_id))
                || a.new_artifact != dir.join(format!(".{name}.{}.new", self.transaction_id))
            {
                return Err(BindingError::new(
                    "invalid_journal",
                    "binding transaction sidecar path is invalid",
                ));
            }
            if !self.sidecars_complete {
                continue;
            }
            for (path, expected_hash) in [
                (&a.old_artifact, &a.old_hash),
                (&a.new_artifact, &a.new_hash),
            ] {
                let bytes = fs
                    .artifact_dir(kind)
                    .read_optional(BindingFileSystem::sidecar_name(path)?)
                    .map_err(BindingError::from_store)?;
                let Some(bytes) = bytes else {
                    if !self.sidecars_complete || self.phase == JournalPhase::ReloadConfirmed {
                        continue;
                    }
                    return Err(BindingError::new(
                        "invalid_journal",
                        "binding transaction sidecar is missing",
                    ));
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

fn publish_sidecar(
    fs: &BindingFileSystem,
    paths: &BindingPaths,
    kind: ArtifactKind,
    path: &std::path::Path,
    bytes: &[u8],
) -> Result<(), BindingError> {
    let name = BindingFileSystem::sidecar_name(path)?;
    let temporary = publication_temp(name);
    fs.artifact_dir(kind)
        .publish_new(temporary.as_ref(), name, bytes, |boundary| {
            observe_publication(paths, boundary)
        })
        .map_err(BindingError::from_store)
}

fn publication_temp(name: &std::ffi::OsStr) -> String {
    format!("{}.publish.tmp", name.to_string_lossy())
}

fn cleanup_orphan_journal_temps(fs: &BindingFileSystem) -> Result<(), BindingError> {
    let prefix = ".bindings-transaction.json.";
    let suffix = ".prepare.tmp";
    let mut removed = false;
    for name in fs
        .handles
        .presets
        .entries()
        .map_err(BindingError::from_store)?
    {
        let Some(name_utf8) = name.to_str() else {
            continue;
        };
        let Some(id) = name_utf8
            .strip_prefix(prefix)
            .and_then(|value| value.strip_suffix(suffix))
        else {
            continue;
        };
        if Uuid::parse_str(id)
            .map(|uuid| uuid.hyphenated().to_string() == id)
            .unwrap_or(false)
        {
            fs.handles
                .presets
                .remove_file(&name)
                .map_err(BindingError::from_store)?;
            removed = true;
        }
    }
    if removed {
        fs.handles
            .presets
            .sync()
            .map_err(BindingError::from_store)?;
    }
    Ok(())
}

fn observe_publication(
    paths: &BindingPaths,
    boundary: PublicationBoundary,
) -> Result<(), crate::StoreError> {
    let stage = match boundary {
        PublicationBoundary::PartialWritten => ApplyStage::PublicationPartialWritten,
        PublicationBoundary::FileSyncStarted => ApplyStage::PublicationFileSyncStarted,
        PublicationBoundary::FileSynced => ApplyStage::PublicationFileSynced,
        PublicationBoundary::Renamed => ApplyStage::PublicationRenamed,
        PublicationBoundary::DirectorySyncStarted => ApplyStage::PublicationDirectorySyncStarted,
        PublicationBoundary::DirectorySynced => ApplyStage::PublicationDirectorySynced,
    };
    paths.observe(stage).map_err(|error| {
        crate::StoreError::binding_with_details(
            error.code(),
            error.message().to_owned(),
            error.details().cloned(),
        )
    })
}

fn install(
    fs: &BindingFileSystem,
    paths: Option<&BindingPaths>,
    artifact: &JournalArtifact,
    target: RecoveryTarget,
) -> Result<(), BindingError> {
    let dir = fs.artifact_dir(artifact.kind);
    let destination = BindingFileSystem::artifact_name(artifact.kind);
    if target == RecoveryTarget::Previous && !artifact.old_existed {
        dir.remove_file(destination)
            .map_err(BindingError::from_store)?;
        observe(paths, renamed_stage(artifact.kind, target))?;
        dir.sync().map_err(BindingError::from_store)?;
        observe(paths, directory_stage(artifact.kind, target))?;
        return Ok(());
    }
    let source = if target == RecoveryTarget::Candidate {
        &artifact.new_artifact
    } else {
        &artifact.old_artifact
    };
    let bytes = dir
        .read(BindingFileSystem::sidecar_name(source)?)
        .map_err(BindingError::from_store)?;
    let expected = if target == RecoveryTarget::Candidate {
        &artifact.new_hash
    } else {
        &artifact.old_hash
    };
    if hash(&bytes) != *expected {
        return Err(BindingError::new(
            "invalid_journal",
            "durable transaction artifact hash mismatch",
        ));
    }
    dir.atomic_replace(
        destination,
        &bytes,
        || Ok(()),
        || observe_store(paths, renamed_stage(artifact.kind, target)),
        || observe_store(paths, directory_stage(artifact.kind, target)),
    )
    .map_err(BindingError::from_store)
}

fn observe(paths: Option<&BindingPaths>, stage: ApplyStage) -> Result<(), BindingError> {
    if let Some(p) = paths {
        p.observe(stage)
    } else {
        Ok(())
    }
}
fn observe_store(paths: Option<&BindingPaths>, stage: ApplyStage) -> Result<(), crate::StoreError> {
    observe(paths, stage).map_err(|e| {
        crate::StoreError::binding_with_details(
            e.code(),
            e.message().to_owned(),
            e.details().cloned(),
        )
    })
}
fn renamed_stage(k: ArtifactKind, t: RecoveryTarget) -> ApplyStage {
    match (k, t) {
        (ArtifactKind::Preset, RecoveryTarget::Candidate) => ApplyStage::PresetRenamed,
        (ArtifactKind::Settings, RecoveryTarget::Candidate) => ApplyStage::SettingsRenamed,
        (ArtifactKind::Bindings, RecoveryTarget::Candidate) => ApplyStage::BindingsRenamed,
        (ArtifactKind::Preset, RecoveryTarget::Previous) => ApplyStage::RollbackPresetRenamed,
        (ArtifactKind::Settings, RecoveryTarget::Previous) => ApplyStage::RollbackSettingsRenamed,
        (ArtifactKind::Bindings, RecoveryTarget::Previous) => ApplyStage::RollbackBindingsRenamed,
    }
}
fn directory_stage(k: ArtifactKind, t: RecoveryTarget) -> ApplyStage {
    match (k, t) {
        (ArtifactKind::Preset, RecoveryTarget::Candidate) => ApplyStage::PresetDirectorySynced,
        (ArtifactKind::Settings, RecoveryTarget::Candidate) => ApplyStage::SettingsDirectorySynced,
        (ArtifactKind::Bindings, RecoveryTarget::Candidate) => ApplyStage::BindingsDirectorySynced,
        (ArtifactKind::Preset, RecoveryTarget::Previous) => {
            ApplyStage::RollbackPresetDirectorySynced
        }
        (ArtifactKind::Settings, RecoveryTarget::Previous) => {
            ApplyStage::RollbackSettingsDirectorySynced
        }
        (ArtifactKind::Bindings, RecoveryTarget::Previous) => {
            ApplyStage::RollbackBindingsDirectorySynced
        }
    }
}
fn sidecars_complete_for_legacy_journal() -> bool {
    true
}
fn sidecar_stage(k: ArtifactKind, new: bool) -> ApplyStage {
    match (k, new) {
        (ArtifactKind::Preset, false) => ApplyStage::PresetOldSidecarSynced,
        (ArtifactKind::Preset, true) => ApplyStage::PresetNewSidecarSynced,
        (ArtifactKind::Settings, false) => ApplyStage::SettingsOldSidecarSynced,
        (ArtifactKind::Settings, true) => ApplyStage::SettingsNewSidecarSynced,
        (ArtifactKind::Bindings, false) => ApplyStage::BindingsOldSidecarSynced,
        (ArtifactKind::Bindings, true) => ApplyStage::BindingsNewSidecarSynced,
    }
}
fn hash(bytes: &[u8]) -> String {
    format!("{:x}", Sha256::digest(bytes))
}
pub(crate) fn io_error(context: &str, error: std::io::Error) -> BindingError {
    BindingError::new("io_error", format!("{context}: {error}"))
}
