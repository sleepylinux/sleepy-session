use std::{
    fs,
    io::Write,
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

fn seed_active_preset(root: &TempDir, id: &str) {
    let path = root.path().join("config/sleepy/settings.json");
    let mut settings: Value = serde_json::from_slice(&fs::read(&path).unwrap()).unwrap();
    settings["activePresetId"] = json!(id);
    fs::write(path, serde_json::to_vec(&settings).unwrap()).unwrap();
}

fn prepare_niri_tree(root: &TempDir) {
    let niri = root.path().join("config/niri");
    fs::create_dir_all(&niri).unwrap();
    fs::write(
        niri.join("config.kdl"),
        "include optional=true \"sleepy-user-bindings.kdl\"\n",
    )
    .unwrap();
    fs::write(niri.join("sleepy-user-bindings.kdl"), "binds {}\n").unwrap();
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

    seed_active_preset(&root, USER_ID);
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
    seed_active_preset(&root, USER_ID);
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
    assert!(!activate.status.success());
    assert_eq!(
        serde_json::from_slice::<Value>(&activate.stderr).unwrap()["error"]["code"],
        "apply_required"
    );
}

#[test]
fn cli_bindings_render_uses_packaged_defaults_and_exact_typed_ipc_argv() {
    let root = TempDir::new().unwrap();

    let output = command(&root)
        .args(["bindings", "render"])
        .output()
        .unwrap();

    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let rendered = serde_json::from_slice::<Value>(&output.stdout).unwrap();
    assert_eq!(rendered["activePresetId"], "builtin.sleepy");
    let kdl = rendered["kdl"].as_str().unwrap();
    assert!(kdl.contains("Mod+Return { spawn \"ghostty\"; }"));
    assert!(kdl.contains("Mod+D { spawn \"fuzzel\"; }"));
    assert!(kdl.contains("focus-column-left"));
    assert!(kdl.contains(
        "spawn \"quickshell\" \"ipc\" \"--config\" \"sleepy\" \"call\" \"sleepy\" \"toggleControlCenter\""
    ));
}

#[test]
fn cli_activation_apply_and_builtin_edit_use_the_offline_journal_path() {
    let root = TempDir::new().unwrap();
    prepare_niri_tree(&root);
    let create_input = write_json(&root, "active.json", &user_preset(USER_ID, "Active"));
    assert!(command(&root)
        .args(["presets", "create", "--input", &create_input])
        .output()
        .unwrap()
        .status
        .success());

    let activate = command(&root)
        .env("SLEEPY_NIRI_VALIDATOR", "/bin/true")
        .args(["presets", "activate", USER_ID, "--apply"])
        .output()
        .unwrap();

    assert!(
        activate.status.success(),
        "{}",
        String::from_utf8_lossy(&activate.stderr)
    );
    let report = serde_json::from_slice::<Value>(&activate.stdout).unwrap();
    assert_eq!(report["status"], "reloadPending");
    assert_eq!(report["activePresetId"], USER_ID);
    assert!(root
        .path()
        .join("state/sleepy/bindings-transaction.json")
        .exists());

    let second = TempDir::new().unwrap();
    prepare_niri_tree(&second);
    assert!(command(&second)
        .args(["settings", "show"])
        .output()
        .unwrap()
        .status
        .success());
    let edit = command(&second)
        .env("SLEEPY_NIRI_VALIDATOR", "/bin/true")
        .args([
            "keybindings",
            "set",
            "--preset",
            "builtin.sleepy",
            "app.terminal.open",
            "Mod+T",
            "--apply",
        ])
        .output()
        .unwrap();
    assert!(
        edit.status.success(),
        "{}",
        String::from_utf8_lossy(&edit.stderr)
    );
    let report = serde_json::from_slice::<Value>(&edit.stdout).unwrap();
    assert_ne!(report["activePresetId"], "builtin.sleepy");
    assert_eq!(report["status"], "reloadPending");
}

#[test]
fn cli_online_reload_waits_through_delayed_stale_snapshot_and_keeps_json_clean() {
    use std::os::unix::fs::PermissionsExt;

    let root = TempDir::new().unwrap();
    prepare_niri_tree(&root);
    fs::create_dir_all(root.path().join("state")).unwrap();
    let fake_niri = root.path().join("fake-niri");
    let reload_marker = root.path().join("reload-requested");
    fs::write(
        &fake_niri,
        "#!/bin/sh\nif [ \"$1\" = \"--version\" ]; then\n  printf '%s\\n' 'niri 26.04'\nelif [ \"$2\" = \"--json\" ]; then\n  sleep 0.08\n  printf '%s\\n' '{\"ConfigLoaded\":{\"failed\":true}}'\n  printf '%s\\n' '{\"CastsChanged\":{\"casts\":[]}}'\n  while [ ! -f \"$SLEEPY_TEST_RELOAD_MARKER\" ]; do sleep 0.01; done\n  printf '%s\\n' '{\"ConfigLoaded\":{\"failed\":false}}'\n  sleep 1\nelse\n  : > \"$SLEEPY_TEST_RELOAD_MARKER\"\n  printf '%s\\n' 'niri-child-noise'\nfi\n",
    )
    .unwrap();
    fs::set_permissions(&fake_niri, fs::Permissions::from_mode(0o700)).unwrap();

    let output = command(&root)
        .env("SLEEPY_NIRI_VALIDATOR", "/bin/true")
        .env("SLEEPY_NIRI", &fake_niri)
        .env("SLEEPY_TEST_RELOAD_MARKER", &reload_marker)
        .env("NIRI_SOCKET", root.path().join("niri.sock"))
        .args(["bindings", "initialize"])
        .output()
        .unwrap();

    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let report = serde_json::from_slice::<Value>(&output.stdout).unwrap();
    assert_eq!(report["status"], "committed");
}

#[test]
fn cli_active_apply_conflict_keeps_structured_action_details_without_writing() {
    let root = TempDir::new().unwrap();
    prepare_niri_tree(&root);
    assert!(command(&root)
        .args(["settings", "show"])
        .output()
        .unwrap()
        .status
        .success());
    let settings_before = fs::read(root.path().join("config/sleepy/settings.json")).unwrap();
    let presets_before = fs::read(root.path().join("state/sleepy/presets.json")).unwrap();
    let bindings_before =
        fs::read(root.path().join("config/niri/sleepy-user-bindings.kdl")).unwrap();

    let output = command(&root)
        .env("SLEEPY_NIRI_VALIDATOR", "/bin/true")
        .args([
            "keybindings",
            "set",
            "--preset",
            "builtin.sleepy",
            "launcher.open",
            "Mod+Return",
            "--apply",
        ])
        .output()
        .unwrap();

    assert!(!output.status.success());
    let error = serde_json::from_slice::<Value>(&output.stderr).unwrap();
    assert_eq!(error["error"]["code"], "keybinding_conflict");
    assert_eq!(error["error"]["details"]["accelerator"], "Mod+Return");
    assert_eq!(
        error["error"]["details"]["actions"],
        json!(["app.terminal.open", "launcher.open"])
    );
    assert_eq!(
        fs::read(root.path().join("config/sleepy/settings.json")).unwrap(),
        settings_before
    );
    assert_eq!(
        fs::read(root.path().join("state/sleepy/presets.json")).unwrap(),
        presets_before
    );
    assert_eq!(
        fs::read(root.path().join("config/niri/sleepy-user-bindings.kdl")).unwrap(),
        bindings_before
    );
}

#[test]
fn cli_state_repair_accepts_only_complete_bundle_apply_and_preserves_originals() {
    let root = TempDir::new().unwrap();
    prepare_niri_tree(&root);
    assert!(command(&root)
        .args(["settings", "show"])
        .output()
        .unwrap()
        .status
        .success());
    let settings_path = root.path().join("config/sleepy/settings.json");
    let presets_path = root.path().join("state/sleepy/presets.json");
    fs::write(&settings_path, b"broken settings").unwrap();
    fs::write(&presets_path, b"broken presets").unwrap();
    let bundle = json!({
        "settings": {
            "schemaVersion": 1,
            "activePresetId": "builtin.sleepy",
            "appearanceMode": "dark",
            "paletteSource": "sleepy",
            "reducedMotion": false,
            "effectsProfile": "full",
            "panelVisibility": "always",
            "webSearchEnabled": true
        },
        "presets": []
    });

    let repaired = run_with_stdin_validator(
        &root,
        &["state", "repair", "--bundle", "-", "--apply"],
        bundle.to_string().as_bytes(),
    );

    assert!(
        repaired.status.success(),
        "{}",
        String::from_utf8_lossy(&repaired.stderr)
    );
    assert_eq!(
        serde_json::from_slice::<Value>(&repaired.stdout).unwrap()["status"],
        "reloadPending"
    );
    let backups = fs::read_dir(root.path().join("state/sleepy/recovery"))
        .unwrap()
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    assert_eq!(backups.len(), 1);
    assert_eq!(
        fs::read(backups[0].path().join("settings.json")).unwrap(),
        b"broken settings"
    );
    assert_eq!(
        fs::read(backups[0].path().join("presets.json")).unwrap(),
        b"broken presets"
    );
}

fn run_with_stdin_validator(
    root: &TempDir,
    arguments: &[&str],
    input: &[u8],
) -> std::process::Output {
    let mut child = command(root)
        .env("SLEEPY_NIRI_VALIDATOR", "/bin/true")
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
