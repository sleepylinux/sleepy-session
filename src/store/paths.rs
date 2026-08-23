use std::path::PathBuf;

#[derive(Debug, Clone)]
pub struct StorePaths {
    config_root: PathBuf,
    state_root: PathBuf,
}

impl StorePaths {
    pub fn from_xdg_roots(config_root: impl Into<PathBuf>, state_root: impl Into<PathBuf>) -> Self {
        Self {
            config_root: config_root.into(),
            state_root: state_root.into(),
        }
    }

    pub fn from_environment() -> Self {
        let home = std::env::var_os("HOME").unwrap_or_else(|| PathBuf::from(".").into_os_string());
        let config_root = std::env::var_os("XDG_CONFIG_HOME")
            .map(PathBuf::from)
            .unwrap_or_else(|| PathBuf::from(&home).join(".config"));
        let state_root = std::env::var_os("XDG_STATE_HOME")
            .map(PathBuf::from)
            .unwrap_or_else(|| PathBuf::from(&home).join(".local/state"));
        Self::from_xdg_roots(config_root, state_root)
    }

    pub fn settings_path(&self) -> PathBuf {
        self.config_root.join("sleepy").join("settings.json")
    }

    pub fn presets_path(&self) -> PathBuf {
        self.state_root.join("sleepy").join("presets.json")
    }

    pub(crate) fn settings_dir(&self) -> PathBuf {
        self.config_root.join("sleepy")
    }

    pub(crate) fn presets_dir(&self) -> PathBuf {
        self.state_root.join("sleepy")
    }
}
