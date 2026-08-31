// SPDX-License-Identifier: GPL-3.0-only

use std::{ffi::OsStr, io, path::Path, sync::Arc};

use serde::{Deserialize, Serialize};
use sleepy_sdk::{AppearanceCommand, DesktopAppearanceSnapshot, StableId};
use tokio::sync::Mutex;

use crate::{store::SecureDir, theme::ThemeManager};

const APPEARANCE_FILE: &str = "desktop-appearance.json";
const DEFAULT_WALLPAPER: &str = "builtin.sleepy-default";

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct PersistentAppearance {
    schema_version: u32,
    wallpaper_id: String,
}

pub struct AppearanceService {
    manager: Arc<Mutex<ThemeManager>>,
    state: SecureDir,
}

impl AppearanceService {
    pub fn open(manager: Arc<Mutex<ThemeManager>>, state_root: &Path) -> io::Result<Self> {
        let state = SecureDir::open_writable(state_root, true)
            .and_then(|root| root.child_writable(OsStr::new("sleepy"), true))
            .map_err(store_error)?;
        let service = Self { manager, state };
        service.wallpaper_id()?;
        Ok(service)
    }

    pub async fn snapshot(&self) -> io::Result<DesktopAppearanceSnapshot> {
        let manager = Arc::clone(&self.manager);
        let state = self.state.clone();
        tokio::task::spawn_blocking(move || {
            let theme = manager
                .blocking_lock()
                .current()
                .map_err(|error| io::Error::other(error.to_string()))?;
            let wallpaper_id = read_wallpaper(&state)?;
            Ok(DesktopAppearanceSnapshot {
                availability: super::available_producer(),
                theme,
                wallpaper_id,
            })
        })
        .await
        .map_err(|error| io::Error::other(format!("appearance worker failed: {error}")))?
    }

    pub(crate) async fn polling_snapshot(&self) -> io::Result<DesktopAppearanceSnapshot> {
        let manager = Arc::clone(&self.manager);
        let state = self.state.clone();
        tokio::task::spawn_blocking(move || {
            let theme = manager
                .try_lock()
                .map_err(|_| io::Error::new(io::ErrorKind::WouldBlock, "theme manager is busy"))?
                .current()
                .map_err(|error| io::Error::other(error.to_string()))?;
            let wallpaper_id = read_wallpaper(&state)?;
            Ok(DesktopAppearanceSnapshot {
                availability: super::available_producer(),
                theme,
                wallpaper_id,
            })
        })
        .await
        .map_err(|error| io::Error::other(format!("appearance worker failed: {error}")))?
    }

    pub async fn apply(
        &self,
        command: &AppearanceCommand,
    ) -> io::Result<DesktopAppearanceSnapshot> {
        match command {
            AppearanceCommand::ApplyTheme { theme_id } => {
                validate_stable(theme_id, "theme")?;
                let manager = Arc::clone(&self.manager);
                let id = theme_id.as_str().to_owned();
                tokio::task::spawn_blocking(move || {
                    manager
                        .blocking_lock()
                        .activate_for_desktop(&id)
                        .map_err(|error| io::Error::other(error.to_string()))
                })
                .await
                .map_err(|error| {
                    io::Error::other(format!("appearance worker failed: {error}"))
                })??;
            }
            AppearanceCommand::SetWallpaper { wallpaper_id } => {
                validate_stable(wallpaper_id, "wallpaper")?;
                let state = self.state.clone();
                let wallpaper_id = wallpaper_id.as_str().to_owned();
                tokio::task::spawn_blocking(move || write_wallpaper(&state, &wallpaper_id))
                    .await
                    .map_err(|error| {
                        io::Error::other(format!("appearance worker failed: {error}"))
                    })??;
            }
            AppearanceCommand::SetReducedMotion { enabled } => {
                let manager = Arc::clone(&self.manager);
                let enabled = *enabled;
                tokio::task::spawn_blocking(move || {
                    manager
                        .blocking_lock()
                        .set_desktop_effect_preferences(Some(enabled), None)
                        .map_err(|error| io::Error::other(error.to_string()))
                })
                .await
                .map_err(|error| {
                    io::Error::other(format!("appearance worker failed: {error}"))
                })??;
            }
            AppearanceCommand::SetOpaque { enabled } => {
                let manager = Arc::clone(&self.manager);
                let enabled = *enabled;
                tokio::task::spawn_blocking(move || {
                    manager
                        .blocking_lock()
                        .set_desktop_effect_preferences(None, Some(enabled))
                        .map_err(|error| io::Error::other(error.to_string()))
                })
                .await
                .map_err(|error| {
                    io::Error::other(format!("appearance worker failed: {error}"))
                })??;
            }
        }
        self.snapshot().await
    }

    fn wallpaper_id(&self) -> io::Result<String> {
        read_wallpaper(&self.state)
    }
}

fn read_wallpaper(state: &SecureDir) -> io::Result<String> {
    let Some(bytes) = state
        .read_optional(OsStr::new(APPEARANCE_FILE))
        .map_err(store_error)?
    else {
        write_wallpaper(state, DEFAULT_WALLPAPER)?;
        return Ok(DEFAULT_WALLPAPER.into());
    };
    let document: PersistentAppearance = serde_json::from_slice(&bytes)
        .map_err(|error| io::Error::new(io::ErrorKind::InvalidData, error))?;
    if document.schema_version != 1 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "appearance state schema version is unknown",
        ));
    }
    validate_text(&document.wallpaper_id, "wallpaper")?;
    Ok(document.wallpaper_id)
}

fn write_wallpaper(state: &SecureDir, wallpaper_id: &str) -> io::Result<()> {
    validate_text(wallpaper_id, "wallpaper")?;
    let mut bytes = serde_json::to_vec_pretty(&PersistentAppearance {
        schema_version: 1,
        wallpaper_id: wallpaper_id.to_owned(),
    })
    .map_err(io::Error::other)?;
    bytes.push(b'\n');
    state
        .atomic_replace(
            OsStr::new(APPEARANCE_FILE),
            &bytes,
            || Ok(()),
            || Ok(()),
            || Ok(()),
        )
        .map_err(store_error)
}

fn validate_stable(id: &StableId, description: &str) -> io::Result<()> {
    validate_text(id.as_str(), description)
}

fn validate_text(value: &str, description: &str) -> io::Result<()> {
    if value.is_empty()
        || value.trim() != value
        || value.len() > 256
        || value.chars().any(char::is_control)
    {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            format!("invalid {description} ID"),
        ));
    }
    Ok(())
}

fn store_error(error: crate::store::StoreError) -> io::Error {
    io::Error::other(error.to_string())
}
