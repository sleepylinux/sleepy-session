use std::{
    fs, io,
    sync::{mpsc, Arc, Mutex},
    thread,
    time::Duration,
};

use serde_json::{json, Value};
use sleepy_session::{Defaults, ReplacementObserver, ReplacementStage, StateStore, StorePaths};
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
