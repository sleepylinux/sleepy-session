use std::fs;

use serde_json::{json, Value};
use sleepy_session::{Defaults, StateStore, StorePaths};
use tempfile::TempDir;

fn defaults() -> Defaults {
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
            "drawers": {"leftQuickSettings": {}},
            "keybindings": {},
            "pluginRequirements": []
        })],
    )
    .unwrap()
}

fn paths(temp: &TempDir) -> StorePaths {
    StorePaths::from_xdg_roots(temp.path().join("config"), temp.path().join("state"))
}

fn user_preset(id: &str, name: &str) -> Value {
    json!({
        "schemaVersion": 1,
        "id": id,
        "name": name,
        "origin": "user",
        "basePresetId": "builtin.sleepy",
        "layouts": {},
        "drawers": {"leftQuickSettings": {}},
        "keybindings": {},
        "pluginRequirements": []
    })
}

#[test]
fn open_initializes_valid_defaults_only_once() {
    let temp = TempDir::new().unwrap();
    let paths = paths(&temp);
    let store = StateStore::open(paths.clone(), defaults()).unwrap();
    let first = store.settings_json().unwrap();

    fs::write(
        paths.settings_path(),
        r#"{"schemaVersion":1,"activePresetId":"builtin.sleepy","appearanceMode":"light","paletteSource":"custom","reducedMotion":true,"effectsProfile":"reduced","panelVisibility":"hidden","webSearchEnabled":false}"#,
    )
    .unwrap();
    let second = StateStore::open(paths, defaults()).unwrap();

    assert_eq!(first["appearanceMode"], "dark");
    assert_eq!(second.settings_json().unwrap()["appearanceMode"], "light");
}

#[test]
fn repeat_open_never_overwrites_existing_user_presets() {
    let temp = TempDir::new().unwrap();
    let paths = paths(&temp);
    let store = StateStore::open(paths.clone(), defaults()).unwrap();
    store
        .create_user_preset(user_preset("5268c988-5c83-4921-a592-2c3342e59d61", "Mine"))
        .unwrap();

    let reopened = StateStore::open(paths, defaults()).unwrap();
    assert_eq!(
        reopened.presets_json().unwrap()["presets"]
            .as_array()
            .unwrap()
            .len(),
        2
    );
    assert_eq!(
        reopened.presets_json().unwrap()["presets"][1]["name"],
        "Mine"
    );
}

#[test]
fn builtin_preset_cannot_be_renamed_or_activated_as_a_mutable_user_record() {
    let temp = TempDir::new().unwrap();
    let store = StateStore::open(paths(&temp), defaults()).unwrap();

    let error = store
        .rename_preset("builtin.sleepy", "Changed")
        .unwrap_err();
    assert_eq!(error.code(), "immutable_preset");
    assert_eq!(
        store.presets_json().unwrap()["presets"][0]["name"],
        "Sleepy"
    );
}

#[test]
fn duplicate_creates_a_new_uuid_user_preset_deterministically() {
    let temp = TempDir::new().unwrap();
    let store = StateStore::open(paths(&temp), defaults()).unwrap();

    let output = store.duplicate_preset("builtin.sleepy", "My copy").unwrap();
    let id = output["preset"]["id"].as_str().unwrap();
    assert!(uuid::Uuid::parse_str(id).is_ok());
    assert_eq!(output["preset"]["origin"], "user");
    assert_eq!(output["preset"]["basePresetId"], "builtin.sleepy");
    assert_eq!(
        serde_json::to_string(&store.presets_json().unwrap()).unwrap(),
        format!(
            r#"{{"presets":[{{"drawers":{{"leftQuickSettings":{{}}}},"id":"builtin.sleepy","keybindings":{{}},"layouts":{{}},"name":"Sleepy","origin":"builtin","pluginRequirements":[],"schemaVersion":1}},{{"basePresetId":"builtin.sleepy","drawers":{{"leftQuickSettings":{{}}}},"id":"{id}","keybindings":{{}},"layouts":{{}},"name":"My copy","origin":"user","pluginRequirements":[],"schemaVersion":1}}]}}"#
        )
    );
}

#[test]
fn rename_updates_only_a_user_preset() {
    let temp = TempDir::new().unwrap();
    let store = StateStore::open(paths(&temp), defaults()).unwrap();
    let id = store.duplicate_preset("builtin.sleepy", "Before").unwrap()["preset"]["id"]
        .as_str()
        .unwrap()
        .to_owned();

    let output = store.rename_preset(&id, "After").unwrap();
    assert_eq!(output["preset"]["name"], "After");
}

#[test]
fn activate_changes_settings_atomically_to_an_existing_preset() {
    let temp = TempDir::new().unwrap();
    let store = StateStore::open(paths(&temp), defaults()).unwrap();
    let id = store.duplicate_preset("builtin.sleepy", "Active").unwrap()["preset"]["id"]
        .as_str()
        .unwrap()
        .to_owned();

    assert_eq!(store.activate_preset(&id).unwrap()["activePresetId"], id);
    assert_eq!(store.settings_json().unwrap()["activePresetId"], id);
}

#[test]
fn malformed_settings_are_rejected_without_becoming_visible() {
    let temp = TempDir::new().unwrap();
    let paths = paths(&temp);
    let store = StateStore::open(paths.clone(), defaults()).unwrap();
    let original = fs::read_to_string(paths.settings_path()).unwrap();

    let error = store
        .replace_settings_json(json!({"schemaVersion": 1}))
        .unwrap_err();
    assert_eq!(error.code(), "invalid_document");
    assert_eq!(fs::read_to_string(paths.settings_path()).unwrap(), original);
}

#[test]
fn open_rejects_settings_that_reference_a_missing_preset() {
    let temp = TempDir::new().unwrap();
    let paths = paths(&temp);
    StateStore::open(paths.clone(), defaults()).unwrap();
    fs::write(
        paths.settings_path(),
        r#"{"schemaVersion":1,"activePresetId":"missing","appearanceMode":"dark","paletteSource":"sleepy","reducedMotion":false,"effectsProfile":"full","panelVisibility":"always","webSearchEnabled":true}"#,
    )
    .unwrap();

    let error = StateStore::open(paths, defaults()).unwrap_err();
    assert_eq!(error.code(), "invalid_document");
}

#[test]
fn failed_replacement_preserves_last_valid_document() {
    let temp = TempDir::new().unwrap();
    let paths = paths(&temp);
    let store = StateStore::open(paths.clone(), defaults()).unwrap();
    let original = fs::read_to_string(paths.settings_path()).unwrap();

    let error = store
        .replace_settings_json(json!({"schemaVersion": 99}))
        .unwrap_err();
    assert_eq!(error.code(), "invalid_document");
    assert_eq!(fs::read_to_string(paths.settings_path()).unwrap(), original);
    assert_eq!(
        StateStore::open(paths, defaults())
            .unwrap()
            .settings_json()
            .unwrap()["schemaVersion"],
        1
    );
}
