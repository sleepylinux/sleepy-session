use std::{ffi::OsStr, path::Path, sync::Arc};

use crate::store::{SecureDir, StoreHandles};

use super::{journal::ArtifactKind, BindingError, BindingPaths};

#[derive(Clone)]
pub(crate) struct BindingFileSystem {
    pub handles: Arc<StoreHandles>,
    pub niri: SecureDir,
}

impl BindingFileSystem {
    pub fn open(paths: &BindingPaths, handles: Arc<StoreHandles>) -> Result<Self, BindingError> {
        let niri = handles
            .config_root
            .child_writable(OsStr::new("niri"), false)
            .map_err(BindingError::from_store)?;
        let fs = Self { handles, niri };
        fs.validate_closed_paths(paths)?;
        Ok(fs)
    }

    pub fn artifact_dir(&self, kind: ArtifactKind) -> &SecureDir {
        match kind {
            ArtifactKind::Preset => &self.handles.presets,
            ArtifactKind::Settings => &self.handles.settings,
            ArtifactKind::Bindings => &self.niri,
        }
    }

    pub fn artifact_name(kind: ArtifactKind) -> &'static OsStr {
        match kind {
            ArtifactKind::Preset => OsStr::new("presets.json"),
            ArtifactKind::Settings => OsStr::new("settings.json"),
            ArtifactKind::Bindings => OsStr::new("sleepy-user-bindings.kdl"),
        }
    }

    pub fn journal_name() -> &'static OsStr {
        OsStr::new("bindings-transaction.json")
    }

    fn validate_closed_paths(&self, paths: &BindingPaths) -> Result<(), BindingError> {
        let expected =
            BindingPaths::from_xdg_roots(paths.store().config_root(), paths.store().state_root());
        if paths.niri_root() != expected.niri_root()
            || paths.trusted_config() != expected.trusted_config()
            || paths.generated_include() != expected.generated_include()
            || paths.journal() != expected.journal()
            || paths.recovery_root() != expected.recovery_root()
        {
            return Err(BindingError::new(
                "unsafe_path",
                "binding paths escape their XDG roots",
            ));
        }
        for (dir, name) in [
            (&self.handles.settings, OsStr::new("settings.json")),
            (&self.handles.presets, OsStr::new("presets.json")),
            (&self.handles.presets, Self::journal_name()),
            (&self.niri, OsStr::new("sleepy-user-bindings.kdl")),
        ] {
            dir.validate_file_if_present(name)
                .map_err(BindingError::from_store)?;
        }
        Ok(())
    }

    pub fn sidecar_name(path: &Path) -> Result<&OsStr, BindingError> {
        path.file_name()
            .ok_or_else(|| BindingError::new("invalid_journal", "sidecar has no file name"))
    }
}
