use std::process::Command;

use serde_json::Value;
use tempfile::TempDir;

fn command(root: &TempDir) -> Command {
    let mut command = Command::new(env!("CARGO_BIN_EXE_sleepyctl"));
    command
        .env("XDG_CONFIG_HOME", root.path().join("config"))
        .env("XDG_STATE_HOME", root.path().join("state"));
    command
}

#[test]
fn cli_uses_xdg_roots_for_json_settings_and_preset_operations() {
    let root = TempDir::new().unwrap();
    let show = command(&root).args(["settings", "show"]).output().unwrap();
    assert!(show.status.success());
    assert_eq!(
        serde_json::from_slice::<Value>(&show.stdout).unwrap()["activePresetId"],
        "builtin.sleepy"
    );

    let duplicate = command(&root)
        .args(["presets", "duplicate", "builtin.sleepy", "CLI copy"])
        .output()
        .unwrap();
    assert!(duplicate.status.success());
    let id = serde_json::from_slice::<Value>(&duplicate.stdout).unwrap()["preset"]["id"]
        .as_str()
        .unwrap()
        .to_owned();

    let rename = command(&root)
        .args(["presets", "rename", &id, "Renamed"])
        .output()
        .unwrap();
    assert!(rename.status.success());
    assert_eq!(
        serde_json::from_slice::<Value>(&rename.stdout).unwrap()["preset"]["name"],
        "Renamed"
    );

    let activate = command(&root)
        .args(["presets", "activate", &id])
        .output()
        .unwrap();
    assert!(activate.status.success());
    assert_eq!(
        serde_json::from_slice::<Value>(&activate.stdout).unwrap()["activePresetId"],
        id
    );
}

#[test]
fn cli_writes_structured_json_errors_to_stderr() {
    let root = TempDir::new().unwrap();
    let output = command(&root)
        .args(["presets", "rename", "builtin.sleepy", "Nope"])
        .output()
        .unwrap();

    assert!(!output.status.success());
    let error: Value = serde_json::from_slice(&output.stderr).unwrap();
    assert_eq!(error["error"]["code"], "immutable_preset");
    assert!(error["error"]["message"]
        .as_str()
        .unwrap()
        .contains("immutable"));
}

#[test]
fn invalid_cli_commands_do_not_initialize_xdg_state() {
    let root = TempDir::new().unwrap();
    let output = command(&root).args(["not-a-command"]).output().unwrap();

    assert!(!output.status.success());
    assert_eq!(
        serde_json::from_slice::<Value>(&output.stderr).unwrap()["error"]["code"],
        "invalid_command"
    );
    assert!(!root.path().join("config/sleepy/settings.json").exists());
    assert!(!root.path().join("state/sleepy/presets.json").exists());
}
