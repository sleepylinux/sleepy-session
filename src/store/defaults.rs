use serde_json::Value;
use sleepy_sdk::{
    validate_preset, validate_settings, PresetDocument, PresetOrigin, SettingsDocument,
    BUILTIN_PRESET_ID,
};
use std::collections::BTreeSet;

use super::StoreError;

#[derive(Debug, Clone)]
pub struct Defaults {
    pub(crate) settings: SettingsDocument,
    pub(crate) builtins: Vec<PresetDocument>,
}

impl Defaults {
    /// Returns the packaged Sleepy preset used for first initialization.
    pub fn packaged() -> Self {
        Self::from_json(
            serde_json::json!({
                "schemaVersion": 1,
                "activePresetId": "builtin.sleepy",
                "appearanceMode": "dark",
                "paletteSource": "sleepy",
                "reducedMotion": false,
                "effectsProfile": "full",
                "panelVisibility": "always",
                "webSearchEnabled": true
            }),
            vec![serde_json::json!({
                "schemaVersion": 1,
                "id": "builtin.sleepy",
                "name": "Sleepy",
                "origin": "builtin",
                "basePresetId": null,
                "layouts": {},
                "drawers": { "leftQuickSettings": {} },
                "keybindings": {
                    "app.terminal.open": "Mod+Return",
                    "launcher.open": "Mod+D",
                    "window.close": "Mod+Q",
                    "window.focus.left": "Mod+Left",
                    "window.focus.right": "Mod+Right",
                    "window.focus.up": "Mod+Up",
                    "window.focus.down": "Mod+Down",
                    "workspace.previous": "Mod+Page_Up",
                    "workspace.next": "Mod+Page_Down",
                    "surface.controlCenter.toggle": "Mod+C",
                    "session.lock": "Mod+L",
                    "session.logout": "Mod+Shift+E",
                    "session.reboot": "Mod+Ctrl+R",
                    "session.powerOff": "Mod+Ctrl+P",
                    "session.power": "Mod+P",
                    "media.playPause": "XF86AudioPlay",
                    "media.next": "XF86AudioNext",
                    "media.previous": "XF86AudioPrev",
                    "audio.volume.up": "XF86AudioRaiseVolume",
                    "audio.volume.down": "XF86AudioLowerVolume",
                    "audio.volume.toggleMute": "XF86AudioMute",
                    "audio.microphone.toggleMute": "XF86AudioMicMute",
                    "display.brightness.up": "XF86MonBrightnessUp",
                    "display.brightness.down": "XF86MonBrightnessDown"
                },
                "pluginRequirements": []
            })],
        )
        .expect("packaged defaults must satisfy the reviewed SDK contract")
    }

    pub(crate) fn builtin(&self, id: &str) -> Option<PresetDocument> {
        self.builtins.iter().find(|preset| preset.id == id).cloned()
    }

    pub(crate) fn settings(&self) -> SettingsDocument {
        self.settings.clone()
    }

    pub fn from_json(settings: Value, presets: Vec<Value>) -> Result<Self, StoreError> {
        let settings = validate_settings(&settings.to_string())
            .map_err(|error| StoreError::invalid(error.to_string()))?;
        let builtins = presets
            .into_iter()
            .map(|preset| {
                validate_preset(&preset.to_string())
                    .map_err(|error| StoreError::invalid(error.to_string()))
            })
            .collect::<Result<Vec<_>, _>>()?;
        let builtin_ids = builtins
            .iter()
            .map(|preset| preset.id.as_str())
            .collect::<BTreeSet<_>>();
        if builtins.is_empty()
            || builtins
                .iter()
                .any(|preset| preset.origin != PresetOrigin::Builtin)
            || builtins.iter().any(|preset| preset.id != BUILTIN_PRESET_ID)
            || builtin_ids.len() != builtins.len()
            || !builtin_ids.contains(settings.active_preset_id.as_str())
        {
            return Err(StoreError::invalid(
                "defaults must contain only builtin.sleepy",
            ));
        }
        Ok(Self { settings, builtins })
    }
}
