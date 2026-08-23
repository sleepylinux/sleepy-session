use std::process::ExitCode;

use serde_json::{json, Value};
use sleepy_session::{Defaults, StateStore, StoreError, StorePaths};

fn main() -> ExitCode {
    match run(std::env::args().skip(1).collect()) {
        Ok(output) => {
            println!(
                "{}",
                serde_json::to_string(&output).expect("JSON values serialize")
            );
            ExitCode::SUCCESS
        }
        Err(error) => {
            eprintln!(
                "{}",
                serde_json::to_string(&json!({
                    "error": { "code": error.code(), "message": error.message() }
                }))
                .expect("JSON errors serialize")
            );
            ExitCode::from(1)
        }
    }
}

fn run(arguments: Vec<String>) -> Result<Value, StoreError> {
    let store = StateStore::open(StorePaths::from_environment(), default_state()?)?;
    match arguments.as_slice() {
        [command, action] if command == "settings" && action == "show" => store.settings_json(),
        [command, action] if command == "presets" && action == "list" => store.presets_json(),
        [command, action, source, name] if command == "presets" && action == "duplicate" => {
            store.duplicate_preset(source, name)
        }
        [command, action, id, name] if command == "presets" && action == "rename" => {
            store.rename_preset(id, name)
        }
        [command, action, id] if command == "presets" && action == "activate" => {
            store.activate_preset(id)
        }
        _ => Err(invalid_command()),
    }
}

fn default_state() -> Result<Defaults, StoreError> {
    Defaults::from_json(
        json!({
            "schemaVersion": 1,
            "activePresetId": "builtin.sleepy",
            "appearanceMode": "dark",
            "paletteSource": "sleepy",
            "reducedMotion": false,
            "effectsProfile": "full",
            "panelVisibility": "always",
            "webSearchEnabled": true
        }),
        vec![json!({
            "schemaVersion": 1,
            "id": "builtin.sleepy",
            "name": "Sleepy",
            "origin": "builtin",
            "basePresetId": null,
            "layouts": {},
            "drawers": { "leftQuickSettings": {} },
            "keybindings": {},
            "pluginRequirements": []
        })],
    )
}

fn invalid_command() -> StoreError {
    StoreError::invalid_command()
}
