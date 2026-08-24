use std::{
    fs, io,
    sync::{mpsc, Arc, Mutex},
    thread,
    time::Duration,
};

use fs2::FileExt;
use serde_json::{json, Value};
use sleepy_session::{
    Defaults, ImportMode, PresetMutationObserver, PresetMutationStage, ReplacementObserver,
    ReplacementStage, StateStore, StorePaths,
};
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
        vec![builtin_preset()],
    )
    .unwrap()
}

fn builtin_preset() -> Value {
    json!({
        "schemaVersion": 1,
        "id": "builtin.sleepy",
        "name": "Sleepy",
        "origin": "builtin",
        "basePresetId": null,
        "layouts": {},
        "drawers": {"leftQuickSettings": {}},
        "keybindings": {},
        "pluginRequirements": []
    })
}

fn paths(temp: &TempDir) -> StorePaths {
    StorePaths::from_xdg_roots(temp.path().join("config"), temp.path().join("state"))
}

fn seed_active_preset(paths: &StorePaths, id: &str) {
    let mut settings: Value =
        serde_json::from_slice(&fs::read(paths.settings_path()).unwrap()).unwrap();
    settings["activePresetId"] = json!(id);
    fs::write(
        paths.settings_path(),
        serde_json::to_vec(&settings).unwrap(),
    )
    .unwrap();
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

fn updated_user_preset(id: &str, name: &str) -> Value {
    json!({
        "schemaVersion": 1,
        "id": id,
        "name": name,
        "origin": "user",
        "basePresetId": "f34956a5-5d02-4a7f-9728-cf784088b97a",
        "layouts": {"DP-1": {"main": "terminal"}},
        "drawers": {"rightNotifications": {"edge": "right"}},
        "keybindings": {
            "app.terminal.open": "Mod+Return",
            "surface.controlCenter.toggle": "Mod+C"
        },
        "pluginRequirements": ["org.sleepy.clock"]
    })
}

#[test]
fn preset_mutation_update_replaces_every_field_of_an_inactive_user_preset() {
    let temp = TempDir::new().unwrap();
    let paths = paths(&temp);
    let store = StateStore::open(paths, defaults()).unwrap();
    let id = "5268c988-5c83-4921-a592-2c3342e59d61";
    store.create_user_preset(user_preset(id, "Before")).unwrap();
    let candidate = updated_user_preset(id, "After");

    let output = store.update_user_preset(id, candidate.clone()).unwrap();

    assert_eq!(output["preset"], candidate);
    assert_eq!(store.preset_json(id).unwrap(), candidate);
    assert_eq!(store.export_preset(id).unwrap(), candidate);
}

#[test]
fn preset_mutation_rejects_every_direct_builtin_mutation() {
    let temp = TempDir::new().unwrap();
    let store = StateStore::open(paths(&temp), defaults()).unwrap();

    let update = store
        .update_user_preset("builtin.sleepy", builtin_preset())
        .unwrap_err();
    let delete = store.delete_user_preset("builtin.sleepy").unwrap_err();

    assert_eq!(update.code(), "immutable_preset");
    assert_eq!(delete.code(), "immutable_preset");
    assert_eq!(
        store.preset_json("builtin.sleepy").unwrap()["name"],
        "Sleepy"
    );
}

#[test]
fn preset_mutation_rejects_delete_of_the_active_user_preset() {
    let temp = TempDir::new().unwrap();
    let paths = paths(&temp);
    let store = StateStore::open(paths.clone(), defaults()).unwrap();
    let id = "5268c988-5c83-4921-a592-2c3342e59d61";
    store.create_user_preset(user_preset(id, "Active")).unwrap();
    seed_active_preset(&paths, id);

    let error = store.delete_user_preset(id).unwrap_err();

    assert_eq!(error.code(), "active_preset");
    assert_eq!(store.preset_json(id).unwrap()["name"], "Active");
}

#[test]
fn preset_mutation_rejects_candidate_identity_and_import_conflicts() {
    let temp = TempDir::new().unwrap();
    let store = StateStore::open(paths(&temp), defaults()).unwrap();
    let first = "5268c988-5c83-4921-a592-2c3342e59d61";
    let second = "f34956a5-5d02-4a7f-9728-cf784088b97a";
    store
        .create_user_preset(user_preset(first, "Existing"))
        .unwrap();

    let update = store
        .update_user_preset(first, user_preset(second, "Wrong identity"))
        .unwrap_err();
    let import = store
        .import_preset(user_preset(first, "Conflict"), ImportMode::Reject)
        .unwrap_err();

    assert_eq!(update.code(), "preset_conflict");
    assert_eq!(import.code(), "preset_conflict");
    assert_eq!(store.preset_json(first).unwrap()["name"], "Existing");
}

#[test]
fn preset_mutation_imports_a_builtin_as_a_new_user_copy() {
    let temp = TempDir::new().unwrap();
    let store = StateStore::open(paths(&temp), defaults()).unwrap();

    let output = store
        .import_preset(builtin_preset(), ImportMode::Reject)
        .unwrap();
    let imported = &output["preset"];
    let id = imported["id"].as_str().unwrap();

    assert!(uuid::Uuid::parse_str(id).is_ok());
    assert_eq!(imported["origin"], "user");
    assert_eq!(imported["basePresetId"], "builtin.sleepy");
    assert_eq!(
        store.preset_json("builtin.sleepy").unwrap()["origin"],
        "builtin"
    );
}

#[test]
fn preset_mutation_explicit_replace_updates_an_existing_inactive_user() {
    let temp = TempDir::new().unwrap();
    let store = StateStore::open(paths(&temp), defaults()).unwrap();
    let id = "5268c988-5c83-4921-a592-2c3342e59d61";
    store
        .create_user_preset(user_preset(id, "Existing"))
        .unwrap();
    let replacement = updated_user_preset(id, "Replacement");

    let output = store
        .import_preset(replacement.clone(), ImportMode::Replace)
        .unwrap();

    assert_eq!(output["preset"], replacement);
    assert_eq!(store.preset_json(id).unwrap(), replacement);
}

#[test]
fn preset_mutation_invalid_import_preserves_the_existing_bytes_exactly() {
    let temp = TempDir::new().unwrap();
    let paths = paths(&temp);
    let store = StateStore::open(paths.clone(), defaults()).unwrap();
    let original = b"[\n  ]\n";
    fs::write(paths.presets_path(), original).unwrap();

    let error = store
        .import_preset(json!({"schemaVersion": 1}), ImportMode::Reject)
        .unwrap_err();

    assert_eq!(error.code(), "invalid_document");
    assert_eq!(fs::read(paths.presets_path()).unwrap(), original);
}

#[test]
fn preset_mutation_allows_duplicate_display_names() {
    let temp = TempDir::new().unwrap();
    let store = StateStore::open(paths(&temp), defaults()).unwrap();
    let first = "5268c988-5c83-4921-a592-2c3342e59d61";
    let second = "f34956a5-5d02-4a7f-9728-cf784088b97a";
    store
        .create_user_preset(user_preset(first, "Same name"))
        .unwrap();

    store
        .import_preset(user_preset(second, "Same name"), ImportMode::Reject)
        .unwrap();

    assert_eq!(store.preset_json(first).unwrap()["name"], "Same name");
    assert_eq!(store.preset_json(second).unwrap()["name"], "Same name");
}

#[test]
fn preset_mutation_rejects_active_update_and_replace_until_apply_is_available() {
    let temp = TempDir::new().unwrap();
    let paths = paths(&temp);
    let store = StateStore::open(paths.clone(), defaults()).unwrap();
    let id = "5268c988-5c83-4921-a592-2c3342e59d61";
    store
        .create_user_preset(user_preset(id, "Original"))
        .unwrap();
    seed_active_preset(&paths, id);

    let update = store
        .update_user_preset(id, updated_user_preset(id, "Update"))
        .unwrap_err();
    let replace = store
        .import_preset(updated_user_preset(id, "Replacement"), ImportMode::Replace)
        .unwrap_err();

    assert_eq!(update.code(), "apply_required");
    assert_eq!(replace.code(), "apply_required");
    assert_eq!(store.active_preset_json().unwrap()["name"], "Original");
}

#[test]
fn preset_mutation_validation_returns_the_complete_validated_document() {
    let temp = TempDir::new().unwrap();
    let store = StateStore::open(paths(&temp), defaults()).unwrap();
    let candidate = updated_user_preset("5268c988-5c83-4921-a592-2c3342e59d61", "Validated");

    assert_eq!(
        store.validate_preset_candidate(candidate.clone()).unwrap(),
        candidate
    );
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
fn activate_requires_the_journaled_apply_path_without_changing_settings() {
    let temp = TempDir::new().unwrap();
    let store = StateStore::open(paths(&temp), defaults()).unwrap();
    let id = store.duplicate_preset("builtin.sleepy", "Active").unwrap()["preset"]["id"]
        .as_str()
        .unwrap()
        .to_owned();

    let error = store.activate_preset(&id).unwrap_err();
    assert_eq!(error.code(), "apply_required");
    assert_eq!(
        store.settings_json().unwrap()["activePresetId"],
        "builtin.sleepy"
    );
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

#[test]
fn duplicate_defaults_are_rejected_before_initialization_writes_state() {
    let temp = TempDir::new().unwrap();
    let paths = paths(&temp);
    let settings = json!({
        "schemaVersion": 1,
        "activePresetId": "builtin.sleepy",
        "appearanceMode": "dark",
        "paletteSource": "sleepy",
        "reducedMotion": false,
        "effectsProfile": "full",
        "panelVisibility": "always",
        "webSearchEnabled": true
    });

    let error =
        Defaults::from_json(settings, vec![builtin_preset(), builtin_preset()]).unwrap_err();
    assert_eq!(error.code(), "invalid_document");
    assert!(!paths.settings_path().exists());
    assert!(!paths.presets_path().exists());
}

#[test]
fn defaults_with_an_unknown_active_preset_are_rejected_before_initialization() {
    let temp = TempDir::new().unwrap();
    let paths = paths(&temp);
    let error = Defaults::from_json(
        json!({
            "schemaVersion": 1,
            "activePresetId": "missing",
            "appearanceMode": "dark",
            "paletteSource": "sleepy",
            "reducedMotion": false,
            "effectsProfile": "full",
            "panelVisibility": "always",
            "webSearchEnabled": true
        }),
        vec![builtin_preset()],
    )
    .unwrap_err();
    assert_eq!(error.code(), "invalid_document");
    assert!(!paths.settings_path().exists());
    assert!(!paths.presets_path().exists());
}

#[test]
fn persisted_duplicate_user_ids_are_rejected_on_read() {
    let temp = TempDir::new().unwrap();
    let paths = paths(&temp);
    StateStore::open(paths.clone(), defaults()).unwrap();
    let duplicate = user_preset("5268c988-5c83-4921-a592-2c3342e59d61", "Mine");
    fs::write(
        paths.presets_path(),
        json!([duplicate.clone(), duplicate]).to_string(),
    )
    .unwrap();

    let error = StateStore::open(paths, defaults()).unwrap_err();
    assert_eq!(error.code(), "invalid_document");
}

#[cfg(unix)]
#[test]
fn symlinked_application_directories_are_rejected_without_redirecting_state() {
    use std::os::unix::fs::symlink;

    let temp = TempDir::new().unwrap();
    let paths = paths(&temp);
    let redirected = temp.path().join("redirected");
    fs::create_dir_all(temp.path().join("config")).unwrap();
    fs::create_dir_all(temp.path().join("state")).unwrap();
    fs::create_dir_all(&redirected).unwrap();
    symlink(&redirected, temp.path().join("config/sleepy")).unwrap();
    symlink(&redirected, temp.path().join("state/sleepy")).unwrap();

    let error = StateStore::open(paths, defaults()).unwrap_err();
    assert_eq!(error.code(), "unsafe_path");
    assert!(fs::read_dir(redirected).unwrap().next().is_none());
}

#[cfg(unix)]
#[test]
fn symlinked_final_state_files_are_rejected() {
    use std::os::unix::fs::symlink;

    let temp = TempDir::new().unwrap();
    let paths = paths(&temp);
    StateStore::open(paths.clone(), defaults()).unwrap();
    let redirected = temp.path().join("redirected.json");
    fs::write(&redirected, "[]").unwrap();
    fs::remove_file(paths.presets_path()).unwrap();
    symlink(&redirected, paths.presets_path()).unwrap();

    let error = StateStore::open(paths, defaults()).unwrap_err();
    assert_eq!(error.code(), "unsafe_path");
    assert_eq!(fs::read_to_string(redirected).unwrap(), "[]");
}

#[derive(Clone)]
struct FailingObserver {
    fail_at: ReplacementStage,
    seen: Arc<Mutex<Vec<ReplacementStage>>>,
}

impl ReplacementObserver for FailingObserver {
    fn reached(&self, stage: ReplacementStage) -> io::Result<()> {
        self.seen.lock().unwrap().push(stage);
        if stage == self.fail_at {
            Err(io::Error::other("injected replacement failure"))
        } else {
            Ok(())
        }
    }
}

#[test]
fn replacement_failure_before_rename_preserves_the_previous_document() {
    let temp = TempDir::new().unwrap();
    let paths = paths(&temp);
    let store = StateStore::open(paths.clone(), defaults())
        .unwrap()
        .with_replacement_observer(Arc::new(FailingObserver {
            fail_at: ReplacementStage::TemporaryFileSynced,
            seen: Arc::new(Mutex::new(Vec::new())),
        }));
    let previous = fs::read_to_string(paths.settings_path()).unwrap();

    let error = store
        .replace_settings_json(json!({
            "schemaVersion": 1, "activePresetId": "builtin.sleepy", "appearanceMode": "light",
            "paletteSource": "sleepy", "reducedMotion": false, "effectsProfile": "full",
            "panelVisibility": "always", "webSearchEnabled": true
        }))
        .unwrap_err();
    assert_eq!(error.code(), "io_error");
    assert_eq!(fs::read_to_string(paths.settings_path()).unwrap(), previous);
}

#[test]
fn replacement_failure_after_rename_reports_an_ambiguous_commit_outcome() {
    let temp = TempDir::new().unwrap();
    let store = StateStore::open(paths(&temp), defaults())
        .unwrap()
        .with_replacement_observer(Arc::new(FailingObserver {
            fail_at: ReplacementStage::RenamedBeforeParentSync,
            seen: Arc::new(Mutex::new(Vec::new())),
        }));

    let error = store
        .duplicate_preset("builtin.sleepy", "Do not retry blindly")
        .unwrap_err();
    assert_eq!(error.code(), "commit_state_unknown");
    assert_eq!(
        store.presets_json().unwrap()["presets"]
            .as_array()
            .unwrap()
            .len(),
        2
    );
}

struct PauseFirstReplacement {
    started: mpsc::Sender<()>,
    release: Mutex<mpsc::Receiver<()>>,
    seen: Mutex<usize>,
}

impl ReplacementObserver for PauseFirstReplacement {
    fn reached(&self, stage: ReplacementStage) -> io::Result<()> {
        if stage == ReplacementStage::TemporaryFileSynced {
            let mut seen = self.seen.lock().unwrap();
            *seen += 1;
            if *seen == 1 {
                self.started.send(()).unwrap();
                self.release.lock().unwrap().recv().unwrap();
            }
        }
        Ok(())
    }
}

struct PauseEligibleKeybindingMutation {
    paused: mpsc::Sender<()>,
    release: Mutex<mpsc::Receiver<()>>,
}

impl PresetMutationObserver for PauseEligibleKeybindingMutation {
    fn reached(&self, stage: PresetMutationStage) -> io::Result<()> {
        if stage == PresetMutationStage::KeybindingTargetEligible {
            self.paused.send(()).unwrap();
            self.release.lock().unwrap().recv().unwrap();
        }
        Ok(())
    }
}

#[test]
fn keybinding_transaction_holds_lock_from_eligible_snapshot_through_write() {
    let temp = TempDir::new().unwrap();
    let id = "5268c988-5c83-4921-a592-2c3342e59d61";
    let initial = StateStore::open(paths(&temp), defaults()).unwrap();
    initial
        .create_user_preset(user_preset(id, "Before"))
        .unwrap();
    let (paused_sender, paused_receiver) = mpsc::channel();
    let (release_sender, release_receiver) = mpsc::channel();
    let store = initial.with_mutation_observer(Arc::new(PauseEligibleKeybindingMutation {
        paused: paused_sender,
        release: Mutex::new(release_receiver),
    }));
    let key_store = store.clone();
    let key_thread = thread::spawn(move || {
        key_store.mutate_user_keybinding(id, "launcher.open", Some("Mod+Space"))
    });
    paused_receiver.recv().unwrap();

    let lock_path = temp.path().join("config/sleepy/.sleepy-session.lock");
    let lock_probe = fs::OpenOptions::new()
        .read(true)
        .write(true)
        .open(lock_path)
        .unwrap();
    match lock_probe.try_lock_exclusive() {
        Ok(()) => {
            FileExt::unlock(&lock_probe).unwrap();
            panic!("configuration lock was free after the eligible target snapshot");
        }
        Err(error) => assert_eq!(error.kind(), io::ErrorKind::WouldBlock),
    }

    let whole_store = store.clone();
    let (attempted_sender, attempted_receiver) = mpsc::channel();
    let (completed_sender, completed_receiver) = mpsc::channel();
    let whole_thread = thread::spawn(move || {
        attempted_sender.send(()).unwrap();
        let result =
            whole_store.update_user_preset(id, updated_user_preset(id, "Serialized whole update"));
        completed_sender.send(()).unwrap();
        result
    });
    attempted_receiver.recv().unwrap();
    assert!(completed_receiver
        .recv_timeout(Duration::from_millis(100))
        .is_err());

    release_sender.send(()).unwrap();
    key_thread.join().unwrap().unwrap();
    whole_thread.join().unwrap().unwrap();
    completed_receiver.recv().unwrap();

    let final_preset = store.preset_json(id).unwrap();
    assert_eq!(final_preset["name"], "Serialized whole update");
    assert_eq!(final_preset["layouts"]["DP-1"]["main"], "terminal");
    assert_eq!(
        final_preset["keybindings"],
        json!({
            "app.terminal.open": "Mod+Return",
            "surface.controlCenter.toggle": "Mod+C"
        })
    );
}

#[test]
fn preset_mutation_concurrent_updates_serialize_the_full_transaction() {
    let temp = TempDir::new().unwrap();
    let id = "5268c988-5c83-4921-a592-2c3342e59d61";
    let initial = StateStore::open(paths(&temp), defaults()).unwrap();
    initial
        .create_user_preset(user_preset(id, "Original"))
        .unwrap();
    let (started_sender, started_receiver) = mpsc::channel();
    let (release_sender, release_receiver) = mpsc::channel();
    let store = initial.with_replacement_observer(Arc::new(PauseFirstReplacement {
        started: started_sender,
        release: Mutex::new(release_receiver),
        seen: Mutex::new(0),
    }));
    let first = store.clone();
    let first_thread =
        thread::spawn(move || first.update_user_preset(id, updated_user_preset(id, "First")));
    started_receiver.recv().unwrap();
    let second = store.clone();
    let (done_sender, done_receiver) = mpsc::channel();
    let second_thread = thread::spawn(move || {
        let result = second.update_user_preset(id, updated_user_preset(id, "Second"));
        done_sender.send(()).unwrap();
        result
    });

    assert!(done_receiver
        .recv_timeout(Duration::from_millis(100))
        .is_err());
    release_sender.send(()).unwrap();
    first_thread.join().unwrap().unwrap();
    second_thread.join().unwrap().unwrap();
    done_receiver.recv().unwrap();

    assert_eq!(store.preset_json(id).unwrap()["name"], "Second");
}

#[test]
fn keybinding_transaction_set_and_unset_validate_and_replace_atomically() {
    let temp = TempDir::new().unwrap();
    let id = "5268c988-5c83-4921-a592-2c3342e59d61";
    let store = StateStore::open(paths(&temp), defaults()).unwrap();
    store
        .create_user_preset(user_preset(id, "Bindings"))
        .unwrap();

    let set = store
        .mutate_user_keybinding(id, "app.terminal.open", Some("mod+return"))
        .unwrap();
    assert_eq!(
        set["preset"]["keybindings"]["app.terminal.open"],
        "Mod+Return"
    );

    let unset = store
        .mutate_user_keybinding(id, "app.terminal.open", None)
        .unwrap();
    assert!(unset["preset"]["keybindings"]
        .as_object()
        .unwrap()
        .is_empty());
}

#[test]
fn keybinding_transaction_concurrent_edits_preserve_both_actions() {
    let temp = TempDir::new().unwrap();
    let id = "5268c988-5c83-4921-a592-2c3342e59d61";
    let initial = StateStore::open(paths(&temp), defaults()).unwrap();
    initial
        .create_user_preset(user_preset(id, "Bindings"))
        .unwrap();
    let (started_sender, started_receiver) = mpsc::channel();
    let (release_sender, release_receiver) = mpsc::channel();
    let store = initial.with_replacement_observer(Arc::new(PauseFirstReplacement {
        started: started_sender,
        release: Mutex::new(release_receiver),
        seen: Mutex::new(0),
    }));
    let first = store.clone();
    let first_thread = thread::spawn(move || {
        first.mutate_user_keybinding(id, "app.terminal.open", Some("Mod+Return"))
    });
    started_receiver.recv().unwrap();
    let second = store.clone();
    let (done_sender, done_receiver) = mpsc::channel();
    let second_thread = thread::spawn(move || {
        let result = second.mutate_user_keybinding(id, "launcher.open", Some("Mod+Space"));
        done_sender.send(()).unwrap();
        result
    });

    assert!(done_receiver
        .recv_timeout(Duration::from_millis(100))
        .is_err());
    release_sender.send(()).unwrap();
    first_thread.join().unwrap().unwrap();
    second_thread.join().unwrap().unwrap();
    done_receiver.recv().unwrap();

    assert_eq!(
        store.preset_json(id).unwrap()["keybindings"],
        json!({
            "app.terminal.open": "Mod+Return",
            "launcher.open": "Mod+Space"
        })
    );
}

#[test]
fn keybinding_transaction_after_concurrent_whole_update_preserves_new_fields() {
    let temp = TempDir::new().unwrap();
    let id = "5268c988-5c83-4921-a592-2c3342e59d61";
    let initial = StateStore::open(paths(&temp), defaults()).unwrap();
    initial
        .create_user_preset(user_preset(id, "Before"))
        .unwrap();
    let (started_sender, started_receiver) = mpsc::channel();
    let (release_sender, release_receiver) = mpsc::channel();
    let store = initial.with_replacement_observer(Arc::new(PauseFirstReplacement {
        started: started_sender,
        release: Mutex::new(release_receiver),
        seen: Mutex::new(0),
    }));
    let whole = store.clone();
    let whole_thread = thread::spawn(move || {
        whole.update_user_preset(id, updated_user_preset(id, "Whole update"))
    });
    started_receiver.recv().unwrap();
    let key = store.clone();
    let (done_sender, done_receiver) = mpsc::channel();
    let key_thread = thread::spawn(move || {
        let result = key.mutate_user_keybinding(id, "launcher.open", Some("Mod+Space"));
        done_sender.send(()).unwrap();
        result
    });

    assert!(done_receiver
        .recv_timeout(Duration::from_millis(100))
        .is_err());
    release_sender.send(()).unwrap();
    whole_thread.join().unwrap().unwrap();
    key_thread.join().unwrap().unwrap();
    done_receiver.recv().unwrap();

    let final_preset = store.preset_json(id).unwrap();
    assert_eq!(final_preset["name"], "Whole update");
    assert_eq!(final_preset["layouts"]["DP-1"]["main"], "terminal");
    assert_eq!(
        final_preset["keybindings"],
        json!({
            "app.terminal.open": "Mod+Return",
            "launcher.open": "Mod+Space",
            "surface.controlCenter.toggle": "Mod+C"
        })
    );
}

#[test]
fn concurrent_duplicates_serialize_the_full_read_modify_write_transaction() {
    let temp = TempDir::new().unwrap();
    let (started_sender, started_receiver) = mpsc::channel();
    let (release_sender, release_receiver) = mpsc::channel();
    let store = StateStore::open(paths(&temp), defaults())
        .unwrap()
        .with_replacement_observer(Arc::new(PauseFirstReplacement {
            started: started_sender,
            release: Mutex::new(release_receiver),
            seen: Mutex::new(0),
        }));
    let first = store.clone();
    let first_thread = thread::spawn(move || first.duplicate_preset("builtin.sleepy", "First"));
    started_receiver.recv().unwrap();
    let second = store.clone();
    let (done_sender, done_receiver) = mpsc::channel();
    let second_thread = thread::spawn(move || {
        let result = second.duplicate_preset("builtin.sleepy", "Second");
        done_sender.send(()).unwrap();
        result
    });
    assert!(done_receiver
        .recv_timeout(Duration::from_millis(100))
        .is_err());
    release_sender.send(()).unwrap();
    first_thread.join().unwrap().unwrap();
    second_thread.join().unwrap().unwrap();
    done_receiver.recv().unwrap();

    assert_eq!(
        store.presets_json().unwrap()["presets"]
            .as_array()
            .unwrap()
            .len(),
        3
    );
}
