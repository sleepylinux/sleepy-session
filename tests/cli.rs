use std::{
    fs,
    io::Write,
    os::unix::fs::PermissionsExt,
    process::{Command, Stdio},
};

use serde_json::{json, Value};
use tempfile::TempDir;

const USER_ID: &str = "5268c988-5c83-4921-a592-2c3342e59d61";

fn command(root: &TempDir) -> Command {
    let mut command = Command::new(env!("CARGO_BIN_EXE_sleepyctl"));
    command
        .env("XDG_CONFIG_HOME", root.path().join("config"))
        .env("XDG_STATE_HOME", root.path().join("state"));
    command
}

fn install_fake_system_tools(root: &TempDir) -> std::path::PathBuf {
    let bin = root.path().join("system-bin");
    fs::create_dir(&bin).unwrap();
    let script = r#"#!/bin/sh
tool=${0##*/}
case "$tool" in
  nmcli)
    if [ "$3" = "WIFI" ]; then printf 'enabled\n'; else printf '*:Sleepy WiFi:73\n'; fi ;;
  bluetoothctl)
    if [ "$1" = "show" ]; then printf 'Powered: yes\n'; else printf 'Device AA:BB Moonbuds\n'; fi ;;
  wpctl)
    if [ "$1" = "get-volume" ] && [ "$2" = "@DEFAULT_AUDIO_SINK@" ]; then printf 'Volume: 0.42\n'
    elif [ "$1" = "get-volume" ]; then printf 'Volume: 0.31 [MUTED]\n'
    else printf 'Audio\n ├─ Sinks:\n │  * 52. Built-in Audio [vol: 0.42]\n ├─ Sources:\n'; fi ;;
  brightnessctl) printf 'backlight,backlight,500,50%%,1000\n' ;;
  powerprofilesctl)
    if [ "$1" = "get" ]; then printf 'balanced\n'
    elif [ "${SLEEPY_TEST_INVALID_POWER:-}" = "1" ]; then printf '* balanced:\n  balanced:\n'
    else printf '* balanced:\n  performance:\n  power-saver:\n'; fi ;;
  upower) printf 'state: charging\npercentage: 81%%\n' ;;
  playerctl) printf 'Playing\tNight Drive\tSleepy Artist\n' ;;
  systemctl)
    if [ "$1" = "--user" ]; then printf 'active\n'; else printf 'systemd 260\n'; fi ;;
  swaylock) printf 'swaylock 1.8\n' ;;
  niri) printf 'niri 26.04\n' ;;
  *) exit 2 ;;
esac
"#;
    for tool in [
        "nmcli",
        "bluetoothctl",
        "wpctl",
        "brightnessctl",
        "powerprofilesctl",
        "upower",
        "playerctl",
        "systemctl",
        "swaylock",
        "niri",
    ] {
        let path = bin.join(tool);
        fs::write(&path, script).unwrap();
        fs::set_permissions(&path, fs::Permissions::from_mode(0o755)).unwrap();
    }
    bin
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

fn write_json(root: &TempDir, name: &str, value: &Value) -> String {
    let path = root.path().join(name);
    fs::write(&path, value.to_string()).unwrap();
    path.to_str().unwrap().to_owned()
}

fn run_with_stdin(root: &TempDir, arguments: &[&str], input: &[u8]) -> std::process::Output {
    let mut child = command(root)
        .args(arguments)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();
    child.stdin.take().unwrap().write_all(input).unwrap();
    child.wait_with_output().unwrap()
}

#[test]
fn cli_system_commands_require_and_echo_a_positive_client_generation() {
    let root = TempDir::new().unwrap();
    for arguments in [
        vec!["system", "show"],
        vec!["system", "show", "--generation", "0"],
        vec![
            "system",
            "set",
            "network.enabled",
            "true",
            "--generation",
            "0",
        ],
        vec![
            "session",
            "perform",
            "lock",
            "confirmed",
            "--generation",
            "0",
        ],
    ] {
        let output = command(&root).args(arguments).output().unwrap();
        assert!(!output.status.success());
        assert_eq!(
            serde_json::from_slice::<Value>(&output.stderr).unwrap()["error"]["code"],
            "invalid_generation"
        );
    }
}

#[test]
fn cli_system_show_returns_only_an_sdk_validated_snapshot() {
    let root = TempDir::new().unwrap();
    let bin = install_fake_system_tools(&root);
    let output = command(&root)
        .env("PATH", bin)
        .args(["system", "show", "--generation", "72"])
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let snapshot =
        sleepy_sdk::validate_system_snapshot(std::str::from_utf8(&output.stdout).unwrap()).unwrap();
    assert_eq!(snapshot.generation, 72);
}

#[test]
fn cli_system_show_localizes_duplicate_power_profiles_before_assembly() {
    let root = TempDir::new().unwrap();
    let bin = install_fake_system_tools(&root);
    let output = command(&root)
        .env("PATH", bin)
        .env("SLEEPY_TEST_INVALID_POWER", "1")
        .args(["system", "show", "--generation", "73"])
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let snapshot =
        sleepy_sdk::validate_system_snapshot(std::str::from_utf8(&output.stdout).unwrap()).unwrap();
    assert_eq!(
        snapshot.capabilities[&sleepy_sdk::CapabilityId::PowerProfile],
        sleepy_sdk::CapabilityState::Error
    );
    assert_eq!(
        snapshot.capabilities[&sleepy_sdk::CapabilityId::BatteryStatus],
        sleepy_sdk::CapabilityState::Available
    );
}

#[test]
fn cli_system_set_rejects_mismatched_and_read_only_values_before_execution() {
    let root = TempDir::new().unwrap();
    for arguments in [
        vec![
            "system",
            "set",
            "network.enabled",
            "0.5",
            "--generation",
            "1",
        ],
        vec![
            "system",
            "set",
            "battery.status",
            "true",
            "--generation",
            "1",
        ],
        vec![
            "system",
            "set",
            "power.profile",
            "turbo",
            "--generation",
            "1",
        ],
    ] {
        let output = command(&root).args(arguments).output().unwrap();
        assert!(!output.status.success());
        assert_eq!(
            serde_json::from_slice::<Value>(&output.stderr).unwrap()["error"]["code"],
            "invalid_request"
        );
    }
}

#[test]
fn cli_session_perform_requires_literal_confirmation() {
    let root = TempDir::new().unwrap();
    let output = command(&root)
        .args(["session", "perform", "powerOff", "yes", "--generation", "4"])
        .output()
        .unwrap();
    assert!(!output.status.success());
    assert_eq!(
        serde_json::from_slice::<Value>(&output.stderr).unwrap()["error"]["code"],
        "confirmation_required"
    );
}

#[test]
fn cli_session_perform_echoes_generation_in_typed_result() {
    let root = TempDir::new().unwrap();
    let bin = root.path().join("bin");
    fs::create_dir(&bin).unwrap();
    let swaylock = bin.join("swaylock");
    fs::write(&swaylock, "#!/bin/sh\nexit 0\n").unwrap();
    fs::set_permissions(&swaylock, fs::Permissions::from_mode(0o755)).unwrap();
    let output = command(&root)
        .env("PATH", &bin)
        .args([
            "session",
            "perform",
            "lock",
            "confirmed",
            "--generation",
            "184",
        ])
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let result =
        sleepy_sdk::validate_session_action_result(std::str::from_utf8(&output.stdout).unwrap())
            .unwrap();
    assert_eq!(result.generation, 184);
    assert_eq!(result.status, sleepy_sdk::SessionActionStatus::Initiated);
}

#[test]
fn cli_preset_crud_validate_import_and_export_round_trip() {
    let root = TempDir::new().unwrap();
    let create_input = write_json(&root, "create.json", &user_preset(USER_ID, "Created"));
    let create = command(&root)
        .args(["presets", "create", "--input", &create_input])
        .output()
        .unwrap();
    assert!(
        create.status.success(),
        "{}",
        String::from_utf8_lossy(&create.stderr)
    );

    let show = command(&root)
        .args(["presets", "show", USER_ID])
        .output()
        .unwrap();
    assert!(show.status.success());
    assert_eq!(
        serde_json::from_slice::<Value>(&show.stdout).unwrap()["name"],
        "Created"
    );

    let updated = user_preset(USER_ID, "Updated");
    let update = run_with_stdin(
        &root,
        &["presets", "update", USER_ID, "--input", "-"],
        updated.to_string().as_bytes(),
    );
    assert!(
        update.status.success(),
        "{}",
        String::from_utf8_lossy(&update.stderr)
    );
    assert_eq!(
        serde_json::from_slice::<Value>(&update.stdout).unwrap()["preset"]["name"],
        "Updated"
    );

    let validate_input = write_json(&root, "validate.json", &updated);
    let validate = command(&root)
        .args(["presets", "validate", "--input", &validate_input])
        .output()
        .unwrap();
    assert!(validate.status.success());
    assert_eq!(
        serde_json::from_slice::<Value>(&validate.stdout).unwrap(),
        updated
    );

    let export = command(&root)
        .args(["presets", "export", USER_ID])
        .output()
        .unwrap();
    assert!(export.status.success());
    let exported: Value = serde_json::from_slice(&export.stdout).unwrap();
    assert_eq!(exported, updated);

    let delete = command(&root)
        .args(["presets", "delete", USER_ID])
        .output()
        .unwrap();
    assert!(delete.status.success());

    let import = run_with_stdin(
        &root,
        &["presets", "import", "--input", "-", "--mode", "reject"],
        &export.stdout,
    );
    assert!(
        import.status.success(),
        "{}",
        String::from_utf8_lossy(&import.stderr)
    );
    assert_eq!(
        serde_json::from_slice::<Value>(&import.stdout).unwrap()["preset"],
        exported
    );
}

#[test]
fn cli_keybinding_set_and_unset_require_an_inactive_user_preset() {
    let root = TempDir::new().unwrap();
    let input = write_json(&root, "preset.json", &user_preset(USER_ID, "Bindings"));
    assert!(command(&root)
        .args(["presets", "create", "--input", &input])
        .output()
        .unwrap()
        .status
        .success());

    let missing_target = command(&root)
        .args(["keybindings", "set", "app.terminal.open", "Mod+Return"])
        .output()
        .unwrap();
    assert!(!missing_target.status.success());
    assert_eq!(
        serde_json::from_slice::<Value>(&missing_target.stderr).unwrap()["error"]["code"],
        "invalid_command"
    );
    let missing_unset_target = command(&root)
        .args(["keybindings", "unset", "app.terminal.open"])
        .output()
        .unwrap();
    assert!(!missing_unset_target.status.success());
    assert_eq!(
        serde_json::from_slice::<Value>(&missing_unset_target.stderr).unwrap()["error"]["code"],
        "invalid_command"
    );

    let set = command(&root)
        .args([
            "keybindings",
            "set",
            "--preset",
            USER_ID,
            "app.terminal.open",
            "mod+return",
        ])
        .output()
        .unwrap();
    assert!(
        set.status.success(),
        "{}",
        String::from_utf8_lossy(&set.stderr)
    );
    assert_eq!(
        serde_json::from_slice::<Value>(&set.stdout).unwrap()["preset"]["keybindings"]
            ["app.terminal.open"],
        "Mod+Return"
    );

    let list = command(&root)
        .args(["keybindings", "list", "--preset", USER_ID])
        .output()
        .unwrap();
    assert!(list.status.success());
    assert_eq!(
        serde_json::from_slice::<Value>(&list.stdout).unwrap()["keybindings"]["app.terminal.open"],
        "Mod+Return"
    );

    let unset = command(&root)
        .args([
            "keybindings",
            "unset",
            "--preset",
            USER_ID,
            "app.terminal.open",
        ])
        .output()
        .unwrap();
    assert!(unset.status.success());
    assert!(
        serde_json::from_slice::<Value>(&unset.stdout).unwrap()["preset"]["keybindings"]
            .as_object()
            .unwrap()
            .is_empty()
    );
}

#[test]
fn cli_keybindings_validate_accepts_stdin_and_returns_canonical_bindings() {
    let root = TempDir::new().unwrap();
    let bindings = json!({
        "app.terminal.open": "mod+return",
        "surface.controlCenter.toggle": "mod+shift+c"
    });

    let validate = run_with_stdin(
        &root,
        &["keybindings", "validate", "--input", "-"],
        bindings.to_string().as_bytes(),
    );

    assert!(
        validate.status.success(),
        "{}",
        String::from_utf8_lossy(&validate.stderr)
    );
    assert_eq!(
        serde_json::from_slice::<Value>(&validate.stdout).unwrap(),
        json!({"keybindings": {
            "app.terminal.open": "Mod+Return",
            "surface.controlCenter.toggle": "Mod+Shift+C"
        }})
    );
}

#[test]
fn cli_keybinding_mutations_return_immutable_apply_required_and_structured_conflicts() {
    let root = TempDir::new().unwrap();
    let builtin = command(&root)
        .args([
            "keybindings",
            "set",
            "--preset",
            "builtin.sleepy",
            "app.terminal.open",
            "Mod+Return",
        ])
        .output()
        .unwrap();
    assert!(!builtin.status.success());
    assert_eq!(
        serde_json::from_slice::<Value>(&builtin.stderr).unwrap()["error"]["code"],
        "immutable_preset"
    );

    let mut preset = user_preset(USER_ID, "Bindings");
    preset["keybindings"]["app.terminal.open"] = json!("Mod+Return");
    let input = write_json(&root, "preset.json", &preset);
    assert!(command(&root)
        .args(["presets", "create", "--input", &input])
        .output()
        .unwrap()
        .status
        .success());

    let conflict = command(&root)
        .args([
            "keybindings",
            "set",
            "--preset",
            USER_ID,
            "launcher.open",
            "Mod+Return",
        ])
        .output()
        .unwrap();
    assert!(!conflict.status.success());
    let conflict: Value = serde_json::from_slice(&conflict.stderr).unwrap();
    assert_eq!(conflict["error"]["code"], "keybinding_conflict");
    assert_eq!(conflict["error"]["details"]["kind"], "duplicate");
    assert_eq!(conflict["error"]["details"]["accelerator"], "Mod+Return");
    assert_eq!(
        conflict["error"]["details"]["actions"],
        json!(["app.terminal.open", "launcher.open"])
    );

    assert!(command(&root)
        .args(["presets", "activate", USER_ID])
        .output()
        .unwrap()
        .status
        .success());
    let active = command(&root)
        .args([
            "keybindings",
            "unset",
            "--preset",
            USER_ID,
            "app.terminal.open",
        ])
        .output()
        .unwrap();
    assert!(!active.status.success());
    assert_eq!(
        serde_json::from_slice::<Value>(&active.stderr).unwrap()["error"]["code"],
        "apply_required"
    );
}

#[test]
fn cli_keybinding_error_precedence_checks_target_before_requested_binding() {
    let root = TempDir::new().unwrap();
    assert!(command(&root)
        .args(["presets", "list"])
        .output()
        .unwrap()
        .status
        .success());
    let presets_path = root.path().join("state/sleepy/presets.json");

    let builtin = command(&root)
        .args([
            "keybindings",
            "set",
            "--preset",
            "builtin.sleepy",
            "app.terminal.open",
            "not a chord",
        ])
        .output()
        .unwrap();
    assert!(!builtin.status.success());
    assert_eq!(
        serde_json::from_slice::<Value>(&builtin.stderr).unwrap()["error"]["code"],
        "immutable_preset"
    );
    assert_eq!(fs::read(&presets_path).unwrap(), b"[]");

    let mut preset = user_preset(USER_ID, "Active bindings");
    preset["keybindings"]["app.terminal.open"] = json!("Mod+Return");
    let input = write_json(&root, "active.json", &preset);
    assert!(command(&root)
        .args(["presets", "create", "--input", &input])
        .output()
        .unwrap()
        .status
        .success());
    assert!(command(&root)
        .args(["presets", "activate", USER_ID])
        .output()
        .unwrap()
        .status
        .success());
    let before = fs::read(&presets_path).unwrap();

    for accelerator in ["Mod+Return", "not a chord"] {
        let active = command(&root)
            .args([
                "keybindings",
                "set",
                "--preset",
                USER_ID,
                "launcher.open",
                accelerator,
            ])
            .output()
            .unwrap();
        assert!(!active.status.success());
        assert_eq!(
            serde_json::from_slice::<Value>(&active.stderr).unwrap()["error"]["code"],
            "apply_required"
        );
        assert_eq!(fs::read(&presets_path).unwrap(), before);
    }
}

#[test]
fn cli_rejects_symlinked_oversized_and_non_utf8_inputs_without_changing_store_bytes() {
    let root = TempDir::new().unwrap();
    assert!(command(&root)
        .args(["presets", "list"])
        .output()
        .unwrap()
        .status
        .success());
    let presets_path = root.path().join("state/sleepy/presets.json");
    let original = b"[\n]\n";
    fs::write(&presets_path, original).unwrap();

    let non_utf8_path = root.path().join("non-utf8.json");
    fs::write(&non_utf8_path, [0xff, 0xfe]).unwrap();
    let non_utf8 = command(&root)
        .args([
            "presets",
            "import",
            "--input",
            non_utf8_path.to_str().unwrap(),
        ])
        .output()
        .unwrap();
    assert!(!non_utf8.status.success());
    assert_eq!(fs::read(&presets_path).unwrap(), original);

    let oversized_path = root.path().join("oversized.json");
    fs::write(&oversized_path, vec![b' '; 1024 * 1024 + 1]).unwrap();
    let oversized = command(&root)
        .args([
            "presets",
            "import",
            "--input",
            oversized_path.to_str().unwrap(),
        ])
        .output()
        .unwrap();
    assert!(!oversized.status.success());
    assert_eq!(fs::read(&presets_path).unwrap(), original);

    #[cfg(unix)]
    {
        use std::os::unix::fs::symlink;
        let valid_path = root.path().join("valid.json");
        let symlink_path = root.path().join("symlink.json");
        fs::write(&valid_path, user_preset(USER_ID, "Unsafe").to_string()).unwrap();
        symlink(&valid_path, &symlink_path).unwrap();
        let symlinked = command(&root)
            .args([
                "presets",
                "import",
                "--input",
                symlink_path.to_str().unwrap(),
            ])
            .output()
            .unwrap();
        assert!(!symlinked.status.success());
        assert_eq!(
            serde_json::from_slice::<Value>(&symlinked.stderr).unwrap()["error"]["code"],
            "unsafe_path"
        );
        assert_eq!(fs::read(&presets_path).unwrap(), original);
    }
}

#[cfg(unix)]
#[test]
fn cli_descriptor_input_rejects_symlinked_ancestor_and_final_components() {
    use std::os::unix::fs::symlink;

    let root = TempDir::new().unwrap();
    assert!(command(&root)
        .args(["presets", "list"])
        .output()
        .unwrap()
        .status
        .success());
    let presets_path = root.path().join("state/sleepy/presets.json");
    let before = fs::read(&presets_path).unwrap();
    let real_directory = root.path().join("real-inputs");
    fs::create_dir(&real_directory).unwrap();
    let real_file = real_directory.join("preset.json");
    fs::write(&real_file, user_preset(USER_ID, "Unsafe").to_string()).unwrap();

    let symlinked_directory = root.path().join("linked-inputs");
    symlink(&real_directory, &symlinked_directory).unwrap();
    let ancestor = command(&root)
        .args([
            "presets",
            "import",
            "--input",
            symlinked_directory.join("preset.json").to_str().unwrap(),
        ])
        .output()
        .unwrap();
    assert!(!ancestor.status.success());
    assert_eq!(
        serde_json::from_slice::<Value>(&ancestor.stderr).unwrap()["error"]["code"],
        "unsafe_path"
    );

    let symlinked_file = root.path().join("linked-preset.json");
    symlink(&real_file, &symlinked_file).unwrap();
    let final_component = command(&root)
        .args([
            "presets",
            "import",
            "--input",
            symlinked_file.to_str().unwrap(),
        ])
        .output()
        .unwrap();
    assert!(!final_component.status.success());
    assert_eq!(
        serde_json::from_slice::<Value>(&final_component.stderr).unwrap()["error"]["code"],
        "unsafe_path"
    );
    assert_eq!(fs::read(&presets_path).unwrap(), before);
}

#[test]
fn cli_state_inspect_reports_record_and_action_errors_without_rewriting_bytes() {
    let root = TempDir::new().unwrap();
    assert!(command(&root)
        .args(["settings", "show"])
        .output()
        .unwrap()
        .status
        .success());
    let settings_path = root.path().join("config/sleepy/settings.json");
    let presets_path = root.path().join("state/sleepy/presets.json");
    let malformed_settings = b"{\"schemaVersion\":1,\"activePresetId\":\"missing\"}\n";
    let conflicting_preset = json!([{
        "schemaVersion": 1,
        "id": USER_ID,
        "name": "Broken bindings",
        "origin": "user",
        "basePresetId": "builtin.sleepy",
        "layouts": {},
        "drawers": {},
        "keybindings": {
            "app.terminal.open": "Mod+Return",
            "launcher.open": "Mod+Return"
        },
        "pluginRequirements": []
    }]);
    let malformed_presets = format!("{}\n", conflicting_preset);
    fs::write(&settings_path, malformed_settings).unwrap();
    fs::write(&presets_path, malformed_presets.as_bytes()).unwrap();

    let inspect = command(&root).args(["state", "inspect"]).output().unwrap();

    assert!(
        inspect.status.success(),
        "{}",
        String::from_utf8_lossy(&inspect.stderr)
    );
    let report: Value = serde_json::from_slice(&inspect.stdout).unwrap();
    assert_eq!(report["clean"], false);
    assert_eq!(report["settings"]["issues"][0]["recordIndex"], Value::Null);
    assert_eq!(report["presets"]["issues"][0]["recordIndex"], 0);
    assert_eq!(report["presets"]["issues"][0]["recordId"], USER_ID);
    assert_eq!(
        report["presets"]["issues"][0]["actions"],
        json!(["app.terminal.open", "launcher.open"])
    );
    assert_eq!(fs::read(&settings_path).unwrap(), malformed_settings);
    assert_eq!(
        fs::read(&presets_path).unwrap(),
        malformed_presets.as_bytes()
    );
}

#[test]
fn cli_state_inspect_does_not_initialize_a_missing_store() {
    let root = TempDir::new().unwrap();

    let inspect = command(&root).args(["state", "inspect"]).output().unwrap();

    assert!(inspect.status.success());
    assert_eq!(
        serde_json::from_slice::<Value>(&inspect.stdout).unwrap()["clean"],
        false
    );
    assert!(!root.path().join("config/sleepy/settings.json").exists());
    assert!(!root.path().join("state/sleepy/presets.json").exists());
    assert!(!root
        .path()
        .join("config/sleepy/.sleepy-session.lock")
        .exists());
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
