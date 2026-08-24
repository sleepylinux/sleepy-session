use std::{
    collections::{BTreeMap, VecDeque},
    fs,
    path::Path,
    process::Command,
    sync::{Arc, Mutex},
    time::Duration,
};

use sleepy_sdk::{PresetDocument, PresetOrigin};
use sleepy_session::{
    bindings::{
        activate_and_apply, apply_active_bindings, compile_bindings,
        import_replace_active_and_apply, initialize_bindings, mutate_keybinding_and_apply,
        reconcile_bindings, reconcile_bindings_online_required, repair_state,
        update_active_bindings_and_apply, ApplyObserver, ApplyStage, ApplyStatus, BindingPaths,
        BindingReloader, BindingValidator, ConfigEventStream, ConfigLoaded, NiriReloader,
        NiriValidator, RepairBundle,
    },
    Defaults, StateStore, StorePaths,
};
use tempfile::TempDir;

fn preset(bindings: &[(&str, &str)]) -> PresetDocument {
    PresetDocument {
        schema_version: 1,
        id: "builtin.sleepy".to_owned(),
        name: "Sleepy".to_owned(),
        origin: PresetOrigin::Builtin,
        base_preset_id: None,
        layouts: BTreeMap::new(),
        drawers: BTreeMap::new(),
        keybindings: bindings
            .iter()
            .map(|(action, accelerator)| ((*action).to_owned(), (*accelerator).to_owned()))
            .collect(),
        plugin_requirements: Vec::new(),
    }
}

fn all_actions_preset() -> PresetDocument {
    preset(&[
        ("workspace.next", "Mod+Page_Down"),
        ("app.terminal.open", "Mod+Return"),
        ("session.powerOff", "Mod+Shift+P"),
        ("audio.volume.up", "XF86AudioRaiseVolume"),
        ("window.focus.left", "Mod+Left"),
        ("surface.controlCenter.toggle", "Mod+C"),
        ("media.previous", "XF86AudioPrev"),
        ("session.power", "Mod+P"),
        ("display.brightness.down", "XF86MonBrightnessDown"),
        ("window.focus.right", "Mod+Right"),
        ("session.logout", "Mod+Shift+E"),
        ("audio.volume.toggleMute", "XF86AudioMute"),
        ("launcher.open", "Mod+D"),
        ("media.next", "XF86AudioNext"),
        ("window.focus.down", "Mod+Down"),
        ("workspace.previous", "Mod+Page_Up"),
        ("session.lock", "Mod+L"),
        ("audio.microphone.toggleMute", "XF86AudioMicMute"),
        ("window.close", "Mod+Q"),
        ("media.playPause", "XF86AudioPlay"),
        ("window.focus.up", "Mod+Up"),
        ("session.reboot", "Mod+Shift+R"),
        ("audio.volume.down", "XF86AudioLowerVolume"),
        ("display.brightness.up", "XF86MonBrightnessUp"),
    ])
}

fn compile_rollback_hanging_niri(root: &TempDir) -> (std::path::PathBuf, std::path::PathBuf) {
    let source = root.path().join("rollback-hang.rs");
    let executable = root.path().join("rollback-hang");
    let count = root.path().join("load-count");
    let pid = root.path().join("rollback-load.pid");
    let program = r#"use std::{env, fs, io::{self, Write}, path::Path, thread, time::Duration};
const COUNT: &str = __COUNT__;
const PID: &str = __PID__;
fn main() {
    let args = env::args().skip(1).collect::<Vec<_>>();
    if args == ["--version"] { println!("niri 26.04"); return; }
    if args.iter().any(|arg| arg == "event-stream") {
        let initial = fs::read_to_string(COUNT).unwrap_or_else(|_| "0".to_owned());
        println!("{{\"ConfigLoaded\":{{\"failed\":{}}}}}", initial != "0");
        println!("{{\"CastsChanged\":{{\"casts\":[]}}}}");
        io::stdout().flush().unwrap();
        loop {
            let current = fs::read_to_string(COUNT).unwrap_or_else(|_| "0".to_owned());
            if current != initial {
                println!("{{\"ConfigLoaded\":{{\"failed\":true}}}}");
                io::stdout().flush().unwrap();
                loop { thread::sleep(Duration::from_secs(30)); }
            }
            thread::sleep(Duration::from_millis(1));
        }
    }
    if args.iter().any(|arg| arg == "load-config-file") {
        if Path::new(COUNT).exists() {
            fs::write(COUNT, "2").unwrap();
            fs::write(PID, std::process::id().to_string()).unwrap();
            loop { thread::sleep(Duration::from_secs(30)); }
        }
        fs::write(COUNT, "1").unwrap();
    }
}
"#
    .replace("__COUNT__", &format!("{:?}", count.to_string_lossy()))
    .replace("__PID__", &format!("{:?}", pid.to_string_lossy()));
    fs::write(&source, program).unwrap();
    let output = Command::new("rustc")
        .args(["--edition=2021", source.to_str().unwrap(), "-o"])
        .arg(&executable)
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    (executable, pid)
}

#[test]
fn compiler_golden_maps_every_closed_action_to_typed_niri_kdl() {
    let candidate = all_actions_preset();

    let rendered = compile_bindings(&candidate).unwrap();

    assert_eq!(rendered, include_str!("fixtures/bindings/expected.kdl"));
    assert!(!rendered.contains("recovery.shell"));
    assert!(!rendered.contains("Mod+Shift+Escape"));
    assert!(!rendered.contains(" sh "));
    assert!(!rendered.contains("bash"));
    assert!(!rendered.contains("\"-c\""));
    for required in [
        "spawn \"ghostty\"",
        "spawn \"fuzzel\"",
        "focus-column-left",
        "focus-workspace-down",
        "toggleControlCenter",
    ] {
        assert!(
            rendered.contains(required),
            "missing {required} from {rendered}"
        );
    }
}

#[test]
#[ignore = "run by the mandatory checks.niri-bindings Nix contract"]
fn compiler_registry_validates_with_niri_26_04() {
    let niri = std::env::var_os("SLEEPY_NIRI_CONTRACT")
        .expect("checks.niri-bindings must provide SLEEPY_NIRI_CONTRACT");
    let version = Command::new(&niri).arg("--version").output().unwrap();
    assert!(version.status.success());
    assert!(
        String::from_utf8_lossy(&version.stdout).starts_with("niri 26.04 "),
        "contract must run against Niri 26.04, got {}",
        String::from_utf8_lossy(&version.stdout).trim()
    );

    let (temp, paths) = niri_contract_fixture();
    let events = Arc::new(Mutex::new(Vec::new()));
    let report = apply_active_bindings(
        &paths,
        &NiriValidator::new(niri),
        &ScriptedReloader::offline(events),
    )
    .unwrap();

    assert!(temp.path().is_dir());
    assert_eq!(report.status, ApplyStatus::ReloadPending);
    let generated = fs::read_to_string(paths.generated_include()).unwrap();
    assert!(generated.starts_with("binds {\n"));
    assert!(generated.contains("focus-workspace-down"));
    assert!(generated.contains("focus-workspace-up"));
}

#[test]
fn compiler_rejects_unknown_semantic_actions_without_rendering_them() {
    let candidate = preset(&[("plugin.untrusted.shell", "Mod+X")]);

    let error = compile_bindings(&candidate).unwrap_err();

    assert_eq!(error.code(), "unknown_semantic_action");
    assert!(error.message().contains("plugin.untrusted.shell"));
}

#[test]
fn compiler_output_is_deterministic_for_equivalent_binding_maps() {
    let first = preset(&[
        ("window.close", "Mod+Q"),
        ("app.terminal.open", "Mod+Return"),
    ]);
    let second = preset(&[
        ("app.terminal.open", "Mod+Return"),
        ("window.close", "Mod+Q"),
    ]);

    assert_eq!(
        compile_bindings(&first).unwrap(),
        compile_bindings(&second).unwrap()
    );
}

#[derive(Clone)]
struct RecordingValidator {
    live_include: std::path::PathBuf,
    events: Arc<Mutex<Vec<String>>>,
    failure: Option<String>,
}

struct InitializingValidator;

struct ForbiddenValidator;

impl BindingValidator for ForbiddenValidator {
    fn validate(&self, _staged_root: &Path, _staged_config: &Path) -> Result<(), String> {
        panic!("coherent initializer must not validate or rewrite")
    }
}

impl BindingValidator for InitializingValidator {
    fn validate(&self, staged_root: &Path, staged_config: &Path) -> Result<(), String> {
        assert!(staged_config.starts_with(staged_root));
        assert!(
            fs::read_to_string(staged_root.join("sleepy-user-bindings.kdl"))
                .unwrap()
                .contains("spawn \"ghostty\"")
        );
        Ok(())
    }
}

impl BindingValidator for RecordingValidator {
    fn validate(&self, staged_root: &Path, staged_config: &Path) -> Result<(), String> {
        self.events.lock().unwrap().push("validate".to_owned());
        assert!(self.live_include.is_file());
        assert!(staged_config.starts_with(staged_root));
        let staged_include = staged_root.join("sleepy-user-bindings.kdl");
        let candidate = fs::read_to_string(staged_include).unwrap();
        assert!(candidate.starts_with("binds {\n"));
        self.failure.clone().map_or(Ok(()), Err)
    }
}

struct ScriptedStream {
    event: Option<ConfigLoaded>,
    events: Arc<Mutex<Vec<String>>>,
}

impl ConfigEventStream for ScriptedStream {
    fn await_initial_snapshot(&mut self, _timeout: Duration) -> Result<ConfigLoaded, String> {
        self.events.lock().unwrap().push("snapshot".to_owned());
        Ok(ConfigLoaded { failed: true })
    }

    fn next_config_loaded(&mut self, _timeout: Duration) -> Result<Option<ConfigLoaded>, String> {
        self.events.lock().unwrap().push("event".to_owned());
        Ok(self.event.take())
    }
}

#[derive(Clone)]
struct ScriptedReloader {
    scripts: Arc<Mutex<VecDeque<Option<ConfigLoaded>>>>,
    events: Arc<Mutex<Vec<String>>>,
}

impl ScriptedReloader {
    fn online(events: Arc<Mutex<Vec<String>>>, scripts: Vec<Option<ConfigLoaded>>) -> Self {
        Self {
            scripts: Arc::new(Mutex::new(scripts.into())),
            events,
        }
    }

    fn offline(events: Arc<Mutex<Vec<String>>>) -> Self {
        Self::online(events, Vec::new())
    }
}

impl BindingReloader for ScriptedReloader {
    fn subscribe(&self) -> Result<Option<Box<dyn ConfigEventStream>>, String> {
        self.events.lock().unwrap().push("subscribe".to_owned());
        let Some(event) = self.scripts.lock().unwrap().pop_front() else {
            return Ok(None);
        };
        Ok(Some(Box::new(ScriptedStream {
            event,
            events: Arc::clone(&self.events),
        })))
    }

    fn request_reload(&self, trusted_config: &Path) -> Result<(), String> {
        self.events
            .lock()
            .unwrap()
            .push(format!("reload:{}", trusted_config.display()));
        Ok(())
    }
}

fn apply_fixture() -> (TempDir, BindingPaths) {
    let temp = TempDir::new().unwrap();
    let config_root = temp.path().join("config");
    let state_root = temp.path().join("state");
    let niri_root = config_root.join("niri");
    fs::create_dir_all(&niri_root).unwrap();
    fs::create_dir_all(&state_root).unwrap();
    fs::write(
        niri_root.join("config.kdl"),
        "include optional=true \"sleepy-user-bindings.kdl\"\n",
    )
    .unwrap();
    fs::write(niri_root.join("input.kdl"), "input {}\n").unwrap();
    fs::write(niri_root.join("sleepy-user-bindings.kdl"), "old include\n").unwrap();
    let store_paths = StorePaths::from_xdg_roots(&config_root, &state_root);
    StateStore::open(store_paths, Defaults::packaged()).unwrap();
    let paths = BindingPaths::from_xdg_roots(config_root, state_root);
    (temp, paths)
}

fn niri_contract_fixture() -> (TempDir, BindingPaths) {
    let temp = TempDir::new().unwrap();
    let config_root = temp.path().join("config");
    let state_root = temp.path().join("state");
    let niri_root = config_root.join("niri");
    fs::create_dir_all(&niri_root).unwrap();
    fs::create_dir_all(&state_root).unwrap();
    for name in [
        "config.kdl",
        "input.kdl",
        "appearance.kdl",
        "bindings-core.kdl",
        "rules.kdl",
        "startup.kdl",
    ] {
        fs::copy(
            Path::new(env!("CARGO_MANIFEST_DIR"))
                .join("tests/fixtures/niri-contract")
                .join(name),
            niri_root.join(name),
        )
        .unwrap();
    }
    fs::write(niri_root.join("sleepy-user-bindings.kdl"), "binds {}\n").unwrap();
    let store_paths = StorePaths::from_xdg_roots(&config_root, &state_root);
    StateStore::open(store_paths, Defaults::packaged()).unwrap();
    let paths = BindingPaths::from_xdg_roots(config_root, state_root);
    (temp, paths)
}

#[test]
fn apply_validates_and_awaits_snapshot_before_replacement_then_confirms_next_loaded_event() {
    let (_temp, paths) = apply_fixture();
    let events = Arc::new(Mutex::new(Vec::new()));
    let validator = RecordingValidator {
        live_include: paths.generated_include().to_owned(),
        events: Arc::clone(&events),
        failure: None,
    };
    let reloader = ScriptedReloader::online(
        Arc::clone(&events),
        vec![Some(ConfigLoaded { failed: false })],
    );

    let report = apply_active_bindings(&paths, &validator, &reloader).unwrap();

    assert_eq!(report.status, ApplyStatus::Committed);
    assert!(fs::read_to_string(paths.generated_include())
        .unwrap()
        .contains("spawn \"ghostty\""));
    assert!(!paths.journal().exists());
    let events = events.lock().unwrap();
    assert_eq!(events[0..3], ["validate", "subscribe", "snapshot"]);
    assert!(events[3].starts_with("reload:"));
    assert_eq!(events[4], "event");
}

#[test]
fn apply_validation_failure_preserves_the_last_valid_include_without_reload() {
    let (_temp, paths) = apply_fixture();
    let events = Arc::new(Mutex::new(Vec::new()));
    let validator = RecordingValidator {
        live_include: paths.generated_include().to_owned(),
        events: Arc::clone(&events),
        failure: Some("candidate rejected".to_owned()),
    };
    let reloader = ScriptedReloader::online(
        Arc::clone(&events),
        vec![Some(ConfigLoaded { failed: false })],
    );

    let error = apply_active_bindings(&paths, &validator, &reloader).unwrap_err();

    assert_eq!(error.code(), "validation_failed");
    assert_eq!(
        fs::read_to_string(paths.generated_include()).unwrap(),
        "old include\n"
    );
    assert_eq!(*events.lock().unwrap(), vec!["validate"]);
}

#[test]
fn apply_failed_candidate_reload_rolls_back_and_requires_its_own_confirmation() {
    let (_temp, paths) = apply_fixture();
    let events = Arc::new(Mutex::new(Vec::new()));
    let validator = RecordingValidator {
        live_include: paths.generated_include().to_owned(),
        events: Arc::clone(&events),
        failure: None,
    };
    let reloader = ScriptedReloader::online(
        Arc::clone(&events),
        vec![
            Some(ConfigLoaded { failed: true }),
            Some(ConfigLoaded { failed: false }),
        ],
    );

    let report = apply_active_bindings(&paths, &validator, &reloader).unwrap();

    assert_eq!(report.status, ApplyStatus::RolledBackConfirmed);
    assert_eq!(
        fs::read_to_string(paths.generated_include()).unwrap(),
        "old include\n"
    );
    let events = events.lock().unwrap();
    assert_eq!(
        events.iter().filter(|event| *event == "subscribe").count(),
        2
    );
    assert_eq!(
        events.iter().filter(|event| *event == "snapshot").count(),
        2
    );
    assert_eq!(
        events
            .iter()
            .filter(|event| event.starts_with("reload:"))
            .count(),
        2
    );
    assert_eq!(events.iter().filter(|event| *event == "event").count(), 2);
}

#[test]
fn apply_reports_unknown_state_when_rollback_reload_is_not_confirmed() {
    let (_temp, paths) = apply_fixture();
    let events = Arc::new(Mutex::new(Vec::new()));
    let validator = RecordingValidator {
        live_include: paths.generated_include().to_owned(),
        events: Arc::clone(&events),
        failure: None,
    };
    let reloader =
        ScriptedReloader::online(events, vec![Some(ConfigLoaded { failed: true }), None]);

    let report = apply_active_bindings(&paths, &validator, &reloader).unwrap();

    assert_eq!(report.status, ApplyStatus::CommitStateUnknown);
    assert_eq!(
        fs::read_to_string(paths.generated_include()).unwrap(),
        "old include\n"
    );
    assert!(paths.journal().exists());
}

#[test]
fn production_rollback_load_timeout_is_bounded_reaped_and_reported_unknown() {
    let (temp, paths) = apply_fixture();
    let (executable, pid_file) = compile_rollback_hanging_niri(&temp);
    let reloader = NiriReloader::with_runtime(
        executable,
        Some(temp.path().join("niri.sock")),
        Duration::from_millis(100),
    );

    let report = apply_active_bindings(&paths, &InitializingValidator, &reloader).unwrap();

    assert_eq!(report.status, ApplyStatus::CommitStateUnknown);
    let pid = fs::read_to_string(pid_file).unwrap();
    assert!(!Path::new("/proc").join(pid.trim()).exists());
    assert!(paths.journal().exists());
}

#[test]
fn apply_offline_commits_files_but_leaves_reload_pending_for_online_reconciliation() {
    let (_temp, paths) = apply_fixture();
    let events = Arc::new(Mutex::new(Vec::new()));
    let validator = RecordingValidator {
        live_include: paths.generated_include().to_owned(),
        events: Arc::clone(&events),
        failure: None,
    };
    let reloader = ScriptedReloader::offline(events);

    let report = apply_active_bindings(&paths, &validator, &reloader).unwrap();

    assert_eq!(report.status, ApplyStatus::ReloadPending);
    assert!(paths.journal().exists());
    let journal = fs::read_to_string(paths.journal()).unwrap();
    assert!(journal.contains("\"phase\":\"reloadPending\""));

    let second = apply_active_bindings(&paths, &validator, &reloader).unwrap_err();
    assert_eq!(second.code(), "transaction_in_progress");
    assert_eq!(fs::read_to_string(paths.journal()).unwrap(), journal);
}

#[test]
fn apply_offline_initializer_journals_missing_settings_presets_and_include_together() {
    let temp = TempDir::new().unwrap();
    let config_root = temp.path().join("config");
    let state_root = temp.path().join("state");
    let niri_root = config_root.join("niri");
    fs::create_dir_all(&niri_root).unwrap();
    fs::create_dir_all(&state_root).unwrap();
    fs::write(
        niri_root.join("config.kdl"),
        "include optional=true \"sleepy-user-bindings.kdl\"\n",
    )
    .unwrap();
    let paths = BindingPaths::from_xdg_roots(config_root, state_root);
    let events = Arc::new(Mutex::new(Vec::new()));

    let report = apply_active_bindings(
        &paths,
        &InitializingValidator,
        &ScriptedReloader::offline(events),
    )
    .unwrap();

    assert_eq!(report.status, ApplyStatus::ReloadPending);
    assert!(paths.store().settings_path().is_file());
    assert!(paths.store().presets_path().is_file());
    assert!(paths.generated_include().is_file());
    let journal: serde_json::Value =
        serde_json::from_slice(&fs::read(paths.journal()).unwrap()).unwrap();
    assert!(journal["artifacts"]
        .as_array()
        .unwrap()
        .iter()
        .all(|artifact| artifact["oldExisted"] == false));
}

#[test]
fn offline_initializer_reconciles_an_existing_pending_journal_idempotently() {
    let (_temp, paths) = apply_fixture();
    let reloader = ScriptedReloader::offline(Arc::new(Mutex::new(Vec::new())));
    let first = initialize_bindings(&paths, &InitializingValidator, &reloader).unwrap();
    assert_eq!(first.status, ApplyStatus::ReloadPending);
    let settings = fs::read(paths.store().settings_path()).unwrap();
    let presets = fs::read(paths.store().presets_path()).unwrap();
    let include = fs::read(paths.generated_include()).unwrap();

    let second = initialize_bindings(&paths, &InitializingValidator, &reloader).unwrap();

    assert_eq!(second, first);
    assert_eq!(fs::read(paths.store().settings_path()).unwrap(), settings);
    assert_eq!(fs::read(paths.store().presets_path()).unwrap(), presets);
    assert_eq!(fs::read(paths.generated_include()).unwrap(), include);
    assert!(paths.journal().exists());
}

#[test]
fn initializer_after_confirmed_reconcile_is_an_exact_noop() {
    let (_temp, paths) = apply_fixture();
    let offline = ScriptedReloader::offline(Arc::new(Mutex::new(Vec::new())));
    initialize_bindings(&paths, &InitializingValidator, &offline).unwrap();
    let online_events = Arc::new(Mutex::new(Vec::new()));
    reconcile_bindings(
        &paths,
        &ScriptedReloader::online(online_events, vec![Some(ConfigLoaded { failed: false })]),
    )
    .unwrap();
    assert!(!paths.journal().exists());
    let artifacts = [
        paths.store().settings_path(),
        paths.store().presets_path(),
        paths.generated_include().to_owned(),
    ];
    let before = artifacts
        .iter()
        .map(|path| {
            (
                fs::read(path).unwrap(),
                fs::metadata(path).unwrap().modified().unwrap(),
            )
        })
        .collect::<Vec<_>>();
    let no_reload_events = Arc::new(Mutex::new(Vec::new()));

    let report = initialize_bindings(
        &paths,
        &ForbiddenValidator,
        &ScriptedReloader::offline(Arc::clone(&no_reload_events)),
    )
    .unwrap();

    assert_eq!(report.status, ApplyStatus::Committed);
    assert!(!paths.journal().exists());
    assert!(no_reload_events.lock().unwrap().is_empty());
    for (path, (bytes, modified)) in artifacts.iter().zip(before) {
        assert_eq!(fs::read(path).unwrap(), bytes);
        assert_eq!(fs::metadata(path).unwrap().modified().unwrap(), modified);
    }
}

#[test]
fn online_required_reconciliation_rejects_an_offline_stream_distinctly() {
    let (_temp, paths) = apply_fixture();
    let offline = ScriptedReloader::offline(Arc::new(Mutex::new(Vec::new())));
    let pending = apply_active_bindings(&paths, &InitializingValidator, &offline).unwrap();
    assert_eq!(pending.status, ApplyStatus::ReloadPending);

    let error = reconcile_bindings_online_required(&paths, &offline).unwrap_err();

    assert_eq!(error.code(), "niri_unavailable");
    assert!(paths.journal().exists());
}

struct FailingSnapshotStream;

impl ConfigEventStream for FailingSnapshotStream {
    fn await_initial_snapshot(&mut self, _timeout: Duration) -> Result<ConfigLoaded, String> {
        Err("event stream ended before CastsChanged".to_owned())
    }

    fn next_config_loaded(&mut self, _timeout: Duration) -> Result<Option<ConfigLoaded>, String> {
        unreachable!("request is forbidden before the initial barrier")
    }
}

struct FailingSnapshotReloader;

impl BindingReloader for FailingSnapshotReloader {
    fn subscribe(&self) -> Result<Option<Box<dyn ConfigEventStream>>, String> {
        Ok(Some(Box::new(FailingSnapshotStream)))
    }

    fn request_reload(&self, _trusted_config: &Path) -> Result<(), String> {
        unreachable!("request is forbidden before the initial barrier")
    }
}

#[test]
fn online_required_reconciliation_maps_a_missing_event_barrier_to_niri_unavailable() {
    let (_temp, paths) = apply_fixture();
    apply_active_bindings(
        &paths,
        &InitializingValidator,
        &ScriptedReloader::offline(Arc::new(Mutex::new(Vec::new()))),
    )
    .unwrap();

    let error = reconcile_bindings_online_required(&paths, &FailingSnapshotReloader).unwrap_err();

    assert_eq!(error.code(), "niri_unavailable");
    assert!(paths.journal().exists());
}

#[test]
fn apply_reconciliation_finishes_pending_candidate_once_and_is_idempotent() {
    let (_temp, paths) = apply_fixture();
    let initial_events = Arc::new(Mutex::new(Vec::new()));
    let validator = RecordingValidator {
        live_include: paths.generated_include().to_owned(),
        events: Arc::clone(&initial_events),
        failure: None,
    };
    let pending = apply_active_bindings(
        &paths,
        &validator,
        &ScriptedReloader::offline(initial_events),
    )
    .unwrap();
    assert_eq!(pending.status, ApplyStatus::ReloadPending);

    let events = Arc::new(Mutex::new(Vec::new()));
    let online = ScriptedReloader::online(
        Arc::clone(&events),
        vec![Some(ConfigLoaded { failed: false })],
    );
    let reconciled = reconcile_bindings(&paths, &online).unwrap().unwrap();

    assert_eq!(reconciled.status, ApplyStatus::Committed);
    assert!(!paths.journal().exists());
    assert!(reconcile_bindings(&paths, &online).unwrap().is_none());
    let events = events.lock().unwrap();
    assert_eq!(events[0..2], ["subscribe", "snapshot"]);
    assert!(events[2].starts_with("reload:"));
    assert_eq!(events[3], "event");
}

#[derive(Debug)]
struct FailOnceObserver {
    target: ApplyStage,
    failed: Mutex<bool>,
}

#[derive(Debug)]
struct FailNthObserver {
    target: ApplyStage,
    occurrence: usize,
    seen: Mutex<usize>,
}

impl ApplyObserver for FailNthObserver {
    fn reached(&self, stage: ApplyStage) -> Result<(), String> {
        if stage != self.target {
            return Ok(());
        }
        let mut seen = self.seen.lock().unwrap();
        *seen += 1;
        if *seen == self.occurrence {
            Err("simulated ENOSPC during atomic publication".to_owned())
        } else {
            Ok(())
        }
    }
}

impl ApplyObserver for FailOnceObserver {
    fn reached(&self, stage: ApplyStage) -> Result<(), String> {
        let mut failed = self.failed.lock().unwrap();
        if stage == self.target && !*failed {
            *failed = true;
            Err(format!("simulated termination at {stage:?}"))
        } else {
            Ok(())
        }
    }
}

#[derive(Debug)]
struct StageRecorder(Arc<Mutex<Vec<ApplyStage>>>);

impl ApplyObserver for StageRecorder {
    fn reached(&self, stage: ApplyStage) -> Result<(), String> {
        self.0.lock().unwrap().push(stage);
        Ok(())
    }
}

#[cfg(unix)]
#[derive(Debug)]
struct SwapWritableDirectoryObserver {
    source: std::path::PathBuf,
    moved: std::path::PathBuf,
    attacker: std::path::PathBuf,
    swapped: Mutex<bool>,
}

#[cfg(unix)]
#[derive(Debug)]
struct SwapNiriSourceEntryObserver {
    source: std::path::PathBuf,
    attacker: std::path::PathBuf,
    occurrence: usize,
    seen: Mutex<usize>,
}

#[cfg(unix)]
impl ApplyObserver for SwapNiriSourceEntryObserver {
    fn reached(&self, stage: ApplyStage) -> Result<(), String> {
        use std::os::unix::fs::symlink;

        if stage != ApplyStage::NiriSourceEntryEnumerated {
            return Ok(());
        }
        let mut seen = self.seen.lock().unwrap();
        *seen += 1;
        if *seen == self.occurrence {
            fs::rename(&self.source, self.source.with_extension("opened-original"))
                .map_err(|error| error.to_string())?;
            symlink(&self.attacker, &self.source).map_err(|error| error.to_string())?;
        }
        Ok(())
    }
}

#[cfg(unix)]
impl ApplyObserver for SwapWritableDirectoryObserver {
    fn reached(&self, stage: ApplyStage) -> Result<(), String> {
        use std::os::unix::fs::symlink;

        let mut swapped = self.swapped.lock().unwrap();
        if stage == ApplyStage::WritableDirectoriesOpened && !*swapped {
            fs::rename(&self.source, &self.moved).map_err(|error| error.to_string())?;
            symlink(&self.attacker, &self.source).map_err(|error| error.to_string())?;
            *swapped = true;
        }
        Ok(())
    }
}

#[cfg(unix)]
#[test]
fn generated_include_replacement_retains_open_niri_directory_across_path_swap() {
    let (_temp, base_paths) = apply_fixture();
    let source = base_paths.niri_root().to_owned();
    let parent = source.parent().unwrap();
    let moved = parent.join("niri-opened");
    let attacker = parent.join("attacker-niri");
    fs::create_dir(&attacker).unwrap();
    fs::write(
        attacker.join("config.kdl"),
        "include optional=true \"sleepy-user-bindings.kdl\"\n",
    )
    .unwrap();
    fs::write(
        attacker.join("sleepy-user-bindings.kdl"),
        "attacker-bytes\n",
    )
    .unwrap();
    let paths = base_paths.with_observer(Arc::new(SwapWritableDirectoryObserver {
        source,
        moved: moved.clone(),
        attacker: attacker.clone(),
        swapped: Mutex::new(false),
    }));

    let report = apply_active_bindings(
        &paths,
        &InitializingValidator,
        &ScriptedReloader::offline(Arc::new(Mutex::new(Vec::new()))),
    )
    .unwrap();

    assert_eq!(report.status, ApplyStatus::ReloadPending);
    assert!(fs::read_to_string(moved.join("sleepy-user-bindings.kdl"))
        .unwrap()
        .contains("spawn \"ghostty\""));
    assert_eq!(
        fs::read_to_string(attacker.join("sleepy-user-bindings.kdl")).unwrap(),
        "attacker-bytes\n"
    );
}

#[cfg(unix)]
#[test]
fn journal_and_sidecars_retain_open_state_directory_across_path_swap() {
    let (_temp, base_paths) = apply_fixture();
    let source = base_paths.store().state_root().join("sleepy");
    let moved = base_paths.store().state_root().join("sleepy-opened");
    let attacker = base_paths.store().state_root().join("attacker-sleepy");
    fs::create_dir(&attacker).unwrap();
    fs::write(attacker.join("presets.json"), b"attacker-bytes\n").unwrap();
    let paths = base_paths.with_observer(Arc::new(SwapWritableDirectoryObserver {
        source,
        moved: moved.clone(),
        attacker: attacker.clone(),
        swapped: Mutex::new(false),
    }));

    let report = apply_active_bindings(
        &paths,
        &InitializingValidator,
        &ScriptedReloader::offline(Arc::new(Mutex::new(Vec::new()))),
    )
    .unwrap();

    assert_eq!(report.status, ApplyStatus::ReloadPending);
    assert!(moved.join("bindings-transaction.json").is_file());
    assert_eq!(
        fs::read(attacker.join("presets.json")).unwrap(),
        b"attacker-bytes\n"
    );
    assert!(!attacker.join("bindings-transaction.json").exists());
}

#[cfg(unix)]
#[test]
fn binding_roots_reject_ancestor_symlinks_and_relative_xdg_paths() {
    use std::os::unix::fs::symlink;

    let temp = TempDir::new().unwrap();
    let real = temp.path().join("real");
    fs::create_dir(&real).unwrap();
    let linked = temp.path().join("linked");
    symlink(&real, &linked).unwrap();
    let linked_paths = BindingPaths::from_xdg_roots(linked.join("config"), real.join("state"));
    let linked_error = apply_active_bindings(
        &linked_paths,
        &InitializingValidator,
        &ScriptedReloader::offline(Arc::new(Mutex::new(Vec::new()))),
    )
    .unwrap_err();
    assert_eq!(linked_error.code(), "unsafe_path");

    let relative_paths = BindingPaths::from_xdg_roots("relative-config", "relative-state");
    let relative_error = apply_active_bindings(
        &relative_paths,
        &InitializingValidator,
        &ScriptedReloader::offline(Arc::new(Mutex::new(Vec::new()))),
    )
    .unwrap_err();
    assert_eq!(relative_error.code(), "unsafe_path");
}

#[cfg(unix)]
#[test]
fn niri_source_copy_never_follows_an_entry_swapped_after_enumeration() {
    for nested in [false, true] {
        let (_temp, base_paths) = apply_fixture();
        let source = if nested {
            let directory = base_paths.niri_root().join("nested");
            fs::create_dir(&directory).unwrap();
            let source = directory.join("extra.kdl");
            fs::write(&source, b"original nested\n").unwrap();
            source
        } else {
            base_paths.trusted_config().to_owned()
        };
        let attacker = base_paths.niri_root().join(if nested {
            "attacker-nested.kdl"
        } else {
            "attacker-root.kdl"
        });
        fs::write(&attacker, b"attacker bytes\n").unwrap();
        let occurrence = if nested { 3 } else { 1 };
        let paths = base_paths.with_observer(Arc::new(SwapNiriSourceEntryObserver {
            source,
            attacker,
            occurrence,
            seen: Mutex::new(0),
        }));

        let error = apply_active_bindings(
            &paths,
            &InitializingValidator,
            &ScriptedReloader::offline(Arc::new(Mutex::new(Vec::new()))),
        )
        .unwrap_err();

        assert_eq!(error.code(), "unsafe_path", "nested={nested}");
        assert!(!paths.journal().exists());
    }
}

#[test]
fn apply_journal_phase_sequence_is_closed_and_ordered() {
    let (_temp, base_paths) = apply_fixture();
    let stages = Arc::new(Mutex::new(Vec::new()));
    let paths = base_paths.with_observer(Arc::new(StageRecorder(Arc::clone(&stages))));
    let events = Arc::new(Mutex::new(Vec::new()));
    let validator = RecordingValidator {
        live_include: paths.generated_include().to_owned(),
        events: Arc::clone(&events),
        failure: None,
    };

    apply_active_bindings(&paths, &validator, &successful_online(events)).unwrap();

    let phases = stages
        .lock()
        .unwrap()
        .iter()
        .copied()
        .filter(|stage| {
            matches!(
                stage,
                ApplyStage::PreparedSynced
                    | ApplyStage::PresetCommittedSynced
                    | ApplyStage::SettingsCommittedSynced
                    | ApplyStage::BindingsCommittedSynced
                    | ApplyStage::ReloadPendingSynced
                    | ApplyStage::ReloadConfirmedSynced
            )
        })
        .collect::<Vec<_>>();
    assert_eq!(
        phases,
        [
            ApplyStage::PreparedSynced,
            ApplyStage::PresetCommittedSynced,
            ApplyStage::SettingsCommittedSynced,
            ApplyStage::BindingsCommittedSynced,
            ApplyStage::ReloadPendingSynced,
            ApplyStage::ReloadConfirmedSynced,
        ]
    );
}

#[test]
fn apply_fault_seams_reconcile_each_rename_fsync_reload_and_cleanup_boundary() {
    let stages = [
        ApplyStage::PreparedSynced,
        ApplyStage::PresetRenamed,
        ApplyStage::PresetDirectorySynced,
        ApplyStage::PresetCommittedSynced,
        ApplyStage::SettingsRenamed,
        ApplyStage::SettingsDirectorySynced,
        ApplyStage::SettingsCommittedSynced,
        ApplyStage::BindingsRenamed,
        ApplyStage::BindingsDirectorySynced,
        ApplyStage::BindingsCommittedSynced,
        ApplyStage::ReloadPendingSynced,
        ApplyStage::ReloadRequested,
        ApplyStage::ReloadConfirmedSynced,
        ApplyStage::ArtifactsRemoved,
        ApplyStage::ArtifactDirectoriesSynced,
        ApplyStage::JournalRemoved,
        ApplyStage::JournalDirectorySynced,
    ];

    for stage in stages {
        let (_temp, base_paths) = apply_fixture();
        let paths = base_paths.with_observer(Arc::new(FailOnceObserver {
            target: stage,
            failed: Mutex::new(false),
        }));
        let events = Arc::new(Mutex::new(Vec::new()));
        let validator = RecordingValidator {
            live_include: paths.generated_include().to_owned(),
            events: Arc::clone(&events),
            failure: None,
        };
        let online = ScriptedReloader::online(
            events,
            vec![
                Some(ConfigLoaded { failed: false }),
                Some(ConfigLoaded { failed: false }),
                Some(ConfigLoaded { failed: false }),
            ],
        );

        let first = apply_active_bindings(&paths, &validator, &online);
        assert!(first.is_err(), "fault at {stage:?} did not interrupt apply");

        let reconciled = reconcile_bindings(&paths, &online)
            .unwrap_or_else(|error| panic!("reconcile failed after {stage:?}: {error}"));
        assert!(
            reconciled.is_some() || !paths.journal().exists(),
            "fault at {stage:?} neither reconciled nor completed cleanup"
        );
        assert!(reconcile_bindings(&paths, &online).unwrap().is_none());
        assert!(!paths.journal().exists(), "journal remains after {stage:?}");
    }
}

#[test]
fn rejected_candidate_cannot_be_resurrected_after_any_rollback_install_crash() {
    let rollback_stages = [
        ApplyStage::RollbackPresetRenamed,
        ApplyStage::RollbackPresetDirectorySynced,
        ApplyStage::RollbackSettingsRenamed,
        ApplyStage::RollbackSettingsDirectorySynced,
        ApplyStage::RollbackBindingsRenamed,
        ApplyStage::RollbackBindingsDirectorySynced,
    ];

    for stage in rollback_stages {
        let (_temp, base_paths) = apply_fixture();
        let old_settings = fs::read(base_paths.store().settings_path()).unwrap();
        let old_presets = fs::read(base_paths.store().presets_path()).unwrap();
        let old_bindings = fs::read(base_paths.generated_include()).unwrap();
        let paths = base_paths.with_observer(Arc::new(FailOnceObserver {
            target: stage,
            failed: Mutex::new(false),
        }));
        let events = Arc::new(Mutex::new(Vec::new()));
        let validator = RecordingValidator {
            live_include: paths.generated_include().to_owned(),
            events: Arc::clone(&events),
            failure: None,
        };
        let rejected = ScriptedReloader::online(
            events,
            vec![
                Some(ConfigLoaded { failed: true }),
                Some(ConfigLoaded { failed: false }),
            ],
        );

        let error = apply_active_bindings(&paths, &validator, &rejected).unwrap_err();
        assert_eq!(
            error.code(),
            "fault_injected",
            "unexpected failure at {stage:?}"
        );
        assert!(paths.journal().exists());

        let restart_events = Arc::new(Mutex::new(Vec::new()));
        let recovered = reconcile_bindings(
            &paths,
            &ScriptedReloader::online(restart_events, vec![Some(ConfigLoaded { failed: false })]),
        )
        .unwrap()
        .unwrap();

        assert_eq!(
            recovered.status,
            ApplyStatus::RolledBackConfirmed,
            "candidate resurrected after {stage:?}"
        );
        assert_eq!(
            fs::read(paths.store().settings_path()).unwrap(),
            old_settings
        );
        assert_eq!(fs::read(paths.store().presets_path()).unwrap(), old_presets);
        assert_eq!(fs::read(paths.generated_include()).unwrap(), old_bindings);
        assert!(!paths.journal().exists());
    }
}

#[test]
fn sidecar_fsync_crashes_leave_a_discoverable_safe_preparation_record() {
    let sidecar_stages = [
        ApplyStage::PresetOldSidecarSynced,
        ApplyStage::PresetNewSidecarSynced,
        ApplyStage::SettingsOldSidecarSynced,
        ApplyStage::SettingsNewSidecarSynced,
        ApplyStage::BindingsOldSidecarSynced,
        ApplyStage::BindingsNewSidecarSynced,
    ];

    for stage in sidecar_stages {
        let (_temp, base_paths) = apply_fixture();
        let old_settings = fs::read(base_paths.store().settings_path()).unwrap();
        let old_presets = fs::read(base_paths.store().presets_path()).unwrap();
        let old_bindings = fs::read(base_paths.generated_include()).unwrap();
        let unrelated = base_paths
            .store()
            .settings_path()
            .parent()
            .unwrap()
            .join(".settings.json.00000000-0000-4000-8000-000000000000.old");
        fs::write(&unrelated, b"unrelated-user-bytes").unwrap();
        let paths = base_paths.with_observer(Arc::new(FailOnceObserver {
            target: stage,
            failed: Mutex::new(false),
        }));
        let events = Arc::new(Mutex::new(Vec::new()));
        let validator = RecordingValidator {
            live_include: paths.generated_include().to_owned(),
            events: Arc::clone(&events),
            failure: None,
        };

        let error = apply_active_bindings(&paths, &validator, &ScriptedReloader::offline(events))
            .unwrap_err();
        assert_eq!(
            error.code(),
            "fault_injected",
            "unexpected failure at {stage:?}"
        );
        assert!(
            paths.journal().exists(),
            "no preparation record at {stage:?}"
        );

        assert!(reconcile_bindings(
            &paths,
            &ScriptedReloader::offline(Arc::new(Mutex::new(Vec::new())))
        )
        .unwrap()
        .is_none());
        assert_eq!(
            fs::read(paths.store().settings_path()).unwrap(),
            old_settings
        );
        assert_eq!(fs::read(paths.store().presets_path()).unwrap(), old_presets);
        assert_eq!(fs::read(paths.generated_include()).unwrap(), old_bindings);
        assert_eq!(fs::read(&unrelated).unwrap(), b"unrelated-user-bytes");
        assert!(!paths.journal().exists());

        let retry_events = Arc::new(Mutex::new(Vec::new()));
        let retry_validator = RecordingValidator {
            live_include: paths.generated_include().to_owned(),
            events: Arc::clone(&retry_events),
            failure: None,
        };
        let retry =
            apply_active_bindings(&paths, &retry_validator, &successful_online(retry_events))
                .unwrap();
        assert_eq!(retry.status, ApplyStatus::Committed);
    }
}

#[test]
fn journal_and_sidecar_publication_recovers_every_actual_write_sync_boundary() {
    let boundaries = [
        ApplyStage::PublicationPartialWritten,
        ApplyStage::PublicationFileSyncStarted,
        ApplyStage::PublicationFileSynced,
        ApplyStage::PublicationRenamed,
        ApplyStage::PublicationDirectorySyncStarted,
        ApplyStage::PublicationDirectorySynced,
    ];
    for boundary in boundaries {
        for occurrence in 1..=7 {
            let (_temp, base_paths) = apply_fixture();
            let unrelated = base_paths
                .store()
                .state_root()
                .join("sleepy/unrelated.keep");
            fs::write(&unrelated, b"unrelated\n").unwrap();
            let paths = base_paths.clone().with_observer(Arc::new(FailNthObserver {
                target: boundary,
                occurrence,
                seen: Mutex::new(0),
            }));

            let error = apply_active_bindings(
                &paths,
                &InitializingValidator,
                &ScriptedReloader::offline(Arc::new(Mutex::new(Vec::new()))),
            )
            .unwrap_err();
            assert_eq!(error.code(), "fault_injected", "{boundary:?} #{occurrence}");

            let recovered = reconcile_bindings(
                &base_paths,
                &ScriptedReloader::offline(Arc::new(Mutex::new(Vec::new()))),
            )
            .unwrap();
            assert!(recovered.is_none(), "{boundary:?} #{occurrence}");
            assert_eq!(fs::read(&unrelated).unwrap(), b"unrelated\n");
            assert!(!base_paths.journal().exists());
            let retry = apply_active_bindings(
                &base_paths,
                &InitializingValidator,
                &ScriptedReloader::offline(Arc::new(Mutex::new(Vec::new()))),
            )
            .unwrap();
            assert_eq!(retry.status, ApplyStatus::ReloadPending);
        }
    }
}

#[cfg(unix)]
#[test]
fn apply_rejects_symlinked_writable_files_and_malicious_static_links() {
    use std::os::unix::fs::symlink;

    let (_temp, paths) = apply_fixture();
    let redirected = paths.store().state_root().join("attacker.kdl");
    fs::write(&redirected, "attacker\n").unwrap();
    fs::remove_file(paths.generated_include()).unwrap();
    symlink(&redirected, paths.generated_include()).unwrap();
    let events = Arc::new(Mutex::new(Vec::new()));
    let validator = RecordingValidator {
        live_include: paths.generated_include().to_owned(),
        events: Arc::clone(&events),
        failure: None,
    };
    let error =
        apply_active_bindings(&paths, &validator, &ScriptedReloader::offline(events)).unwrap_err();
    assert_eq!(error.code(), "unsafe_path");
    assert_eq!(fs::read_to_string(&redirected).unwrap(), "attacker\n");

    fs::remove_file(paths.generated_include()).unwrap();
    fs::write(paths.generated_include(), "old include\n").unwrap();
    symlink("/etc/passwd", paths.niri_root().join("malicious.kdl")).unwrap();
    let events = Arc::new(Mutex::new(Vec::new()));
    let validator = RecordingValidator {
        live_include: paths.generated_include().to_owned(),
        events: Arc::clone(&events),
        failure: None,
    };
    let error =
        apply_active_bindings(&paths, &validator, &ScriptedReloader::offline(events)).unwrap_err();
    assert_eq!(error.code(), "unsafe_path");
}

#[cfg(unix)]
#[test]
fn apply_rejects_group_writable_generated_state() {
    use std::os::unix::fs::PermissionsExt;

    let (_temp, paths) = apply_fixture();
    fs::set_permissions(paths.generated_include(), fs::Permissions::from_mode(0o666)).unwrap();
    let events = Arc::new(Mutex::new(Vec::new()));
    let validator = RecordingValidator {
        live_include: paths.generated_include().to_owned(),
        events: Arc::clone(&events),
        failure: None,
    };
    let error =
        apply_active_bindings(&paths, &validator, &ScriptedReloader::offline(events)).unwrap_err();
    assert_eq!(error.code(), "unsafe_path");
}

fn user_preset(id: &str, name: &str, bindings: &[(&str, &str)]) -> serde_json::Value {
    serde_json::json!({
        "schemaVersion": 1,
        "id": id,
        "name": name,
        "origin": "user",
        "basePresetId": "builtin.sleepy",
        "layouts": {},
        "drawers": {"leftQuickSettings": {}},
        "keybindings": bindings.iter().copied().collect::<BTreeMap<_, _>>(),
        "pluginRequirements": []
    })
}

fn successful_online(events: Arc<Mutex<Vec<String>>>) -> ScriptedReloader {
    ScriptedReloader::online(events, vec![Some(ConfigLoaded { failed: false })])
}

#[test]
fn apply_activation_is_the_only_activation_path_and_rolls_back_all_bytes_on_reload_failure() {
    let (_temp, paths) = apply_fixture();
    let id = "5268c988-5c83-4921-a592-2c3342e59d61";
    let store = StateStore::open(paths.store().clone(), Defaults::packaged()).unwrap();
    store
        .create_user_preset(user_preset(id, "Work", &[("app.terminal.open", "Mod+T")]))
        .unwrap();
    let unflagged = store.activate_preset(id).unwrap_err();
    assert_eq!(unflagged.code(), "apply_required");
    let old_settings = fs::read(paths.store().settings_path()).unwrap();
    let old_presets = fs::read(paths.store().presets_path()).unwrap();
    let old_bindings = fs::read(paths.generated_include()).unwrap();
    let events = Arc::new(Mutex::new(Vec::new()));
    let validator = RecordingValidator {
        live_include: paths.generated_include().to_owned(),
        events: Arc::clone(&events),
        failure: None,
    };
    let reloader = ScriptedReloader::online(
        events,
        vec![
            Some(ConfigLoaded { failed: true }),
            Some(ConfigLoaded { failed: false }),
        ],
    );

    let report = activate_and_apply(id, &paths, &validator, &reloader).unwrap();

    assert_eq!(report.status, ApplyStatus::RolledBackConfirmed);
    assert_eq!(report.active_preset_id, "builtin.sleepy");
    assert_eq!(
        fs::read(paths.store().settings_path()).unwrap(),
        old_settings
    );
    assert_eq!(fs::read(paths.store().presets_path()).unwrap(), old_presets);
    assert_eq!(fs::read(paths.generated_include()).unwrap(), old_bindings);
}

#[test]
fn apply_activation_and_active_update_commit_settings_preset_and_include_together() {
    let (_temp, paths) = apply_fixture();
    let id = "5268c988-5c83-4921-a592-2c3342e59d61";
    let store = StateStore::open(paths.store().clone(), Defaults::packaged()).unwrap();
    store
        .create_user_preset(user_preset(id, "Work", &[]))
        .unwrap();
    let events = Arc::new(Mutex::new(Vec::new()));
    let validator = RecordingValidator {
        live_include: paths.generated_include().to_owned(),
        events: Arc::clone(&events),
        failure: None,
    };
    let activated = activate_and_apply(
        id,
        &paths,
        &validator,
        &successful_online(Arc::clone(&events)),
    )
    .unwrap();
    assert_eq!(activated.status, ApplyStatus::Committed);
    assert_eq!(activated.active_preset_id, id);

    let updated = user_preset(
        id,
        "Focused",
        &[("surface.controlCenter.toggle", "Mod+Shift+C")],
    );
    let validator = RecordingValidator {
        live_include: paths.generated_include().to_owned(),
        events: Arc::clone(&events),
        failure: None,
    };
    update_active_bindings_and_apply(id, updated, &paths, &validator, &successful_online(events))
        .unwrap();

    let store = StateStore::open(paths.store().clone(), Defaults::packaged()).unwrap();
    assert_eq!(store.settings_json().unwrap()["activePresetId"], id);
    assert_eq!(store.preset_json(id).unwrap()["name"], "Focused");
    assert!(fs::read_to_string(paths.generated_include())
        .unwrap()
        .contains("toggleControlCenter"));
}

#[test]
fn apply_builtin_key_edit_is_uuid_copy_on_write_in_the_same_transaction() {
    let (_temp, paths) = apply_fixture();
    let events = Arc::new(Mutex::new(Vec::new()));
    let validator = RecordingValidator {
        live_include: paths.generated_include().to_owned(),
        events: Arc::clone(&events),
        failure: None,
    };

    let report = mutate_keybinding_and_apply(
        "builtin.sleepy",
        "app.terminal.open",
        Some("mod+t"),
        &paths,
        &validator,
        &successful_online(events),
    )
    .unwrap();

    assert_eq!(report.status, ApplyStatus::Committed);
    assert_ne!(report.active_preset_id, "builtin.sleepy");
    uuid::Uuid::parse_str(&report.active_preset_id).unwrap();
    let store = StateStore::open(paths.store().clone(), Defaults::packaged()).unwrap();
    let copy = store.preset_json(&report.active_preset_id).unwrap();
    assert_eq!(copy["origin"], "user");
    assert_eq!(copy["basePresetId"], "builtin.sleepy");
    assert_eq!(copy["keybindings"]["app.terminal.open"], "Mod+T");
    assert_eq!(
        store.preset_json("builtin.sleepy").unwrap()["keybindings"]["app.terminal.open"],
        "Mod+Return"
    );
    assert!(fs::read_to_string(paths.generated_include())
        .unwrap()
        .contains("Mod+T { spawn \"ghostty\"; }"));
}

#[test]
fn apply_builtin_full_update_is_uuid_copy_on_write_in_the_same_transaction() {
    let (_temp, paths) = apply_fixture();
    let mut replacement = serde_json::to_value(
        StateStore::open(paths.store().clone(), Defaults::packaged())
            .unwrap()
            .preset_json("builtin.sleepy")
            .unwrap(),
    )
    .unwrap();
    replacement["name"] = serde_json::json!("Customized Sleepy");
    replacement["keybindings"]["app.terminal.open"] = serde_json::json!("Mod+T");
    let events = Arc::new(Mutex::new(Vec::new()));
    let validator = RecordingValidator {
        live_include: paths.generated_include().to_owned(),
        events: Arc::clone(&events),
        failure: None,
    };

    let report = update_active_bindings_and_apply(
        "builtin.sleepy",
        replacement,
        &paths,
        &validator,
        &successful_online(events),
    )
    .unwrap();

    assert_ne!(report.active_preset_id, "builtin.sleepy");
    let store = StateStore::open(paths.store().clone(), Defaults::packaged()).unwrap();
    let copy = store.preset_json(&report.active_preset_id).unwrap();
    assert_eq!(copy["name"], "Customized Sleepy");
    assert_eq!(copy["origin"], "user");
    assert_eq!(copy["basePresetId"], "builtin.sleepy");
    assert_eq!(
        store.preset_json("builtin.sleepy").unwrap()["keybindings"]["app.terminal.open"],
        "Mod+Return"
    );
}

#[test]
fn apply_import_replace_of_active_preset_uses_the_journal_path() {
    let (_temp, paths) = apply_fixture();
    let id = "5268c988-5c83-4921-a592-2c3342e59d61";
    let store = StateStore::open(paths.store().clone(), Defaults::packaged()).unwrap();
    store
        .create_user_preset(user_preset(id, "Imported", &[]))
        .unwrap();
    let events = Arc::new(Mutex::new(Vec::new()));
    let validator = RecordingValidator {
        live_include: paths.generated_include().to_owned(),
        events: Arc::clone(&events),
        failure: None,
    };
    activate_and_apply(
        id,
        &paths,
        &validator,
        &successful_online(Arc::clone(&events)),
    )
    .unwrap();
    let replacement = user_preset(id, "Replacement", &[("window.close", "Mod+Shift+Q")]);
    let validator = RecordingValidator {
        live_include: paths.generated_include().to_owned(),
        events: Arc::clone(&events),
        failure: None,
    };

    import_replace_active_and_apply(replacement, &paths, &validator, &successful_online(events))
        .unwrap();

    let store = StateStore::open(paths.store().clone(), Defaults::packaged()).unwrap();
    assert_eq!(store.preset_json(id).unwrap()["name"], "Replacement");
    assert!(fs::read_to_string(paths.generated_include())
        .unwrap()
        .contains("Mod+Shift+Q { close-window; }"));
}

#[test]
fn apply_repair_backs_up_malformed_original_bytes_and_never_eagerly_opens_them() {
    let (_temp, paths) = apply_fixture();
    let malformed_settings = b"{ definitely malformed settings";
    let malformed_presets = b"[ definitely malformed presets";
    fs::write(paths.store().settings_path(), malformed_settings).unwrap();
    fs::write(paths.store().presets_path(), malformed_presets).unwrap();
    let bundle = RepairBundle {
        settings: serde_json::json!({
            "schemaVersion": 1,
            "activePresetId": "builtin.sleepy",
            "appearanceMode": "dark",
            "paletteSource": "sleepy",
            "reducedMotion": false,
            "effectsProfile": "full",
            "panelVisibility": "always",
            "webSearchEnabled": true
        }),
        presets: Vec::new(),
    };
    let events = Arc::new(Mutex::new(Vec::new()));
    let validator = RecordingValidator {
        live_include: paths.generated_include().to_owned(),
        events: Arc::clone(&events),
        failure: None,
    };

    let report = repair_state(
        bundle,
        &paths,
        &validator,
        &ScriptedReloader::offline(events),
    )
    .unwrap();

    assert_eq!(report.status, ApplyStatus::ReloadPending);
    let recovery_entries = fs::read_dir(paths.recovery_root())
        .unwrap()
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    assert_eq!(recovery_entries.len(), 1);
    let recovery = recovery_entries[0].path();
    assert_eq!(
        fs::read(recovery.join("settings.json")).unwrap(),
        malformed_settings
    );
    assert_eq!(
        fs::read(recovery.join("presets.json")).unwrap(),
        malformed_presets
    );
    assert!(StateStore::open(paths.store().clone(), Defaults::packaged()).is_ok());
}

#[test]
fn apply_repair_reload_failure_restores_malformed_bytes_and_confirms_previous_config() {
    let (_temp, paths) = apply_fixture();
    let malformed_settings = b"malformed settings must survive rollback";
    let malformed_presets = b"malformed presets must survive rollback";
    fs::write(paths.store().settings_path(), malformed_settings).unwrap();
    fs::write(paths.store().presets_path(), malformed_presets).unwrap();
    let bundle = RepairBundle {
        settings: serde_json::json!({
            "schemaVersion": 1,
            "activePresetId": "builtin.sleepy",
            "appearanceMode": "dark",
            "paletteSource": "sleepy",
            "reducedMotion": false,
            "effectsProfile": "full",
            "panelVisibility": "always",
            "webSearchEnabled": true
        }),
        presets: Vec::new(),
    };
    let events = Arc::new(Mutex::new(Vec::new()));
    let validator = RecordingValidator {
        live_include: paths.generated_include().to_owned(),
        events: Arc::clone(&events),
        failure: None,
    };
    let pending = repair_state(
        bundle,
        &paths,
        &validator,
        &ScriptedReloader::offline(Arc::clone(&events)),
    )
    .unwrap();
    assert_eq!(pending.status, ApplyStatus::ReloadPending);
    let reloader = ScriptedReloader::online(
        events,
        vec![
            Some(ConfigLoaded { failed: true }),
            Some(ConfigLoaded { failed: false }),
        ],
    );

    let report = reconcile_bindings(&paths, &reloader).unwrap().unwrap();

    assert_eq!(report.status, ApplyStatus::RolledBackConfirmed);
    assert_eq!(report.active_preset_id, "unknown");
    assert_eq!(
        fs::read(paths.store().settings_path()).unwrap(),
        malformed_settings
    );
    assert_eq!(
        fs::read(paths.store().presets_path()).unwrap(),
        malformed_presets
    );
    assert!(!paths.journal().exists());
}
