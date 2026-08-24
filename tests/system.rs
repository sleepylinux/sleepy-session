use std::{
    collections::{BTreeMap, HashMap},
    fs,
    os::unix::fs::PermissionsExt,
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc, Mutex,
    },
    time::{Duration, Instant},
};

use sleepy_sdk::{
    CapabilityId, CapabilityState, MediaTransport, PowerProfile, SessionAction,
    SessionActionRequest, SessionActionStatus, SystemMutation,
};
use sleepy_session::system::{
    CommandOutput, CommandRunner, CommandSpec, ProcessCommandRunner, RunControl, RunnerError,
    RunnerErrorKind, SystemErrorKind, SystemFacade,
};

#[derive(Clone, Default)]
struct ScriptedRunner {
    responses: SharedResponses,
    calls: Arc<Mutex<Vec<CommandSpec>>>,
}

type CommandKey = (String, Vec<String>);
type SharedResponses = Arc<Mutex<HashMap<CommandKey, ScriptedResponse>>>;

#[derive(Clone)]
struct ScriptedResponse {
    delay: Duration,
    result: Result<CommandOutput, RunnerError>,
}

impl ScriptedRunner {
    fn output(self, program: &str, args: &[&str], stdout: &str) -> Self {
        self.responses.lock().unwrap().insert(
            key(program, args),
            ScriptedResponse {
                delay: Duration::ZERO,
                result: Ok(CommandOutput {
                    status: 0,
                    stdout: stdout.as_bytes().to_vec(),
                    stderr: Vec::new(),
                }),
            },
        );
        self
    }

    fn failure(self, program: &str, args: &[&str], response: ScriptedResponse) -> Self {
        self.responses
            .lock()
            .unwrap()
            .insert(key(program, args), response);
        self
    }

    fn calls(&self) -> Vec<CommandSpec> {
        self.calls.lock().unwrap().clone()
    }
}

impl CommandRunner for ScriptedRunner {
    fn run(&self, command: &CommandSpec) -> Result<CommandOutput, RunnerError> {
        self.execute(command, None)
    }

    fn run_controlled(
        &self,
        command: &CommandSpec,
        control: &RunControl,
    ) -> Result<CommandOutput, RunnerError> {
        self.execute(command, Some(control))
    }
}

impl ScriptedRunner {
    fn execute(
        &self,
        command: &CommandSpec,
        control: Option<&RunControl>,
    ) -> Result<CommandOutput, RunnerError> {
        self.calls.lock().unwrap().push(command.clone());
        let response = self
            .responses
            .lock()
            .unwrap()
            .get(&(command.program.clone(), command.args.clone()))
            .cloned()
            .unwrap_or_else(|| panic!("unexpected command: {command:?}"));
        let started = Instant::now();
        while started.elapsed() < response.delay {
            if control
                .is_some_and(|control| control.is_cancelled() || control.remaining().is_zero())
            {
                return Err(RunnerError::timeout("scripted request cancelled"));
            }
            std::thread::sleep(Duration::from_millis(5));
        }
        response.result
    }
}

fn key(program: &str, args: &[&str]) -> (String, Vec<String>) {
    (
        program.to_owned(),
        args.iter().map(|arg| (*arg).to_owned()).collect(),
    )
}

fn executable_on_path(program: &str) -> std::path::PathBuf {
    std::env::split_paths(&std::env::var_os("PATH").expect("tests require PATH"))
        .map(|directory| directory.join(program))
        .find(|candidate| candidate.is_file())
        .unwrap_or_else(|| panic!("{program} is not available on PATH"))
}

fn base_runner() -> ScriptedRunner {
    ScriptedRunner::default()
        .output(
            "nmcli",
            &["--terse", "--fields", "WIFI", "general"],
            include_str!("fixtures/system/nmcli-valid.txt"),
        )
        .output(
            "nmcli",
            &[
                "--terse",
                "--fields",
                "IN-USE,SSID,SIGNAL",
                "device",
                "wifi",
                "list",
                "--rescan",
                "no",
            ],
            "*:Sleepy WiFi:73\n:nope:20\n",
        )
        .output(
            "bluetoothctl",
            &["show"],
            include_str!("fixtures/system/bluetoothctl-valid.txt"),
        )
        .output(
            "bluetoothctl",
            &["devices", "Connected"],
            "Device AA:BB:CC:DD:EE:FF Moonbuds\n",
        )
        .output(
            "wpctl",
            &["get-volume", "@DEFAULT_AUDIO_SINK@"],
            include_str!("fixtures/system/wpctl-volume-valid.txt"),
        )
        .output(
            "wpctl",
            &["get-volume", "@DEFAULT_AUDIO_SOURCE@"],
            "Volume: 0.31 [MUTED]\n",
        )
        .output(
            "wpctl",
            &["status", "--name"],
            "Audio\n ├─ Sinks:\n │  * 52. Built-in Audio [vol: 0.42]\n │    77. USB DAC [vol: 0.30]\n ├─ Sources:\n",
        )
        .output(
            "brightnessctl",
            &["--machine-readable", "info"],
            include_str!("fixtures/system/brightnessctl-valid.txt"),
        )
        .output(
            "systemctl",
            &["--user", "is-active", "gammastep.service"],
            include_str!("fixtures/system/gammastep-active.txt"),
        )
        .output(
            "powerprofilesctl",
            &["get"],
            include_str!("fixtures/system/powerprofilesctl-valid.txt"),
        )
        .output(
            "powerprofilesctl",
            &["list"],
            "  performance:\n    Driver: platform_profile\n    Degraded: no\n* balanced:\n    Driver: platform_profile\n  power-saver:\n    Driver: platform_profile\n",
        )
        .output(
            "upower",
            &["--show-info", "/org/freedesktop/UPower/devices/DisplayDevice"],
            include_str!("fixtures/system/upower-valid.txt"),
        )
        .output(
            "playerctl",
            &[
                "metadata",
                "--format",
                "{{status}}\\t{{title}}\\t{{artist}}",
            ],
            include_str!("fixtures/system/playerctl-valid.txt"),
        )
        .output("test", &["-x", "swaylock"], "")
        .output("test", &["-x", "niri"], "")
        .output("test", &["-x", "systemctl"], "")
        .output("swaylock", &["--version"], "swaylock 1.8\n")
        .output("niri", &["--version"], "niri 26.04\n")
        .output("systemctl", &["--version"], "systemd 260\n")
}

#[test]
fn snapshot_uses_fixed_argv_and_parses_complete_typed_state() {
    let runner = base_runner();
    let facade = SystemFacade::new(runner.clone());

    let snapshot = facade.snapshot(41).unwrap();

    assert_eq!(snapshot.schema_version, 1);
    assert_eq!(snapshot.generation, 41);
    assert_eq!(
        snapshot.network.as_ref().unwrap().connected_name.as_deref(),
        Some("Sleepy WiFi")
    );
    assert_eq!(
        snapshot
            .bluetooth
            .as_ref()
            .unwrap()
            .connected_device
            .as_deref(),
        Some("Moonbuds")
    );
    assert_eq!(
        snapshot.audio.as_ref().unwrap().output_device_id.as_deref(),
        Some("52")
    );
    assert_eq!(snapshot.display.as_ref().unwrap().brightness, Some(0.5));
    assert_eq!(
        snapshot.power.as_ref().unwrap().current_profile,
        Some(PowerProfile::Balanced)
    );
    assert_eq!(snapshot.media.as_ref().unwrap().title, "Night Drive");
    assert_eq!(snapshot.capabilities.len(), 12);
    assert!(snapshot.diagnostics.is_empty());
    assert!(snapshot
        .session_actions
        .values()
        .all(|state| *state == CapabilityState::Available));

    let calls = runner.calls();
    assert!(calls
        .iter()
        .all(|call| call.timeout <= Duration::from_millis(1200)));
    assert!(calls
        .iter()
        .all(|call| call.program != "sh" && call.program != "bash"));
    assert!(calls
        .iter()
        .all(|call| call.env == vec![("LC_ALL".into(), "C".into())]));
    let json = serde_json::to_string(&snapshot).unwrap();
    assert_eq!(
        sleepy_sdk::validate_system_snapshot(&json).unwrap(),
        snapshot
    );
}

#[test]
fn snapshot_is_partial_and_structured_when_one_probe_is_malformed() {
    let runner = base_runner().output(
        "brightnessctl",
        &["--machine-readable", "info"],
        "garbage\n",
    );
    let snapshot = SystemFacade::new(runner).snapshot(7).unwrap();

    assert!(snapshot.network.is_some());
    assert!(snapshot.display.is_some());
    assert_eq!(snapshot.display.unwrap().brightness, None);
    assert_eq!(
        snapshot.capabilities[&CapabilityId::DisplayBrightness],
        CapabilityState::Error
    );
    assert_eq!(
        snapshot.diagnostics[&CapabilityId::DisplayBrightness].kind,
        sleepy_sdk::CapabilityErrorKind::Parse
    );
}

#[test]
fn snapshot_has_one_total_deadline_and_late_older_generation_is_stale() {
    let delayed = ScriptedResponse {
        delay: Duration::from_millis(1500),
        result: Ok(CommandOutput {
            status: 0,
            stdout: b"enabled\n".to_vec(),
            stderr: vec![],
        }),
    };
    let runner = base_runner().failure(
        "nmcli",
        &["--terse", "--fields", "WIFI", "general"],
        delayed,
    );
    let calls = runner.calls.clone();
    let facade = Arc::new(SystemFacade::new(runner));
    let old = Arc::clone(&facade);
    let started = Instant::now();
    let old_request = std::thread::spawn(move || old.snapshot(10));
    std::thread::sleep(Duration::from_millis(30));
    assert_eq!(facade.snapshot(11).unwrap().generation, 11);
    let error = old_request.join().unwrap().unwrap_err();
    assert_eq!(error.kind(), SystemErrorKind::Stale);
    assert!(started.elapsed() < Duration::from_millis(1400));
    std::thread::sleep(Duration::from_millis(400));
    let wifi_list_calls = calls
        .lock()
        .unwrap()
        .iter()
        .filter(|call| {
            call.program == "nmcli"
                && call.args
                    == [
                        "--terse",
                        "--fields",
                        "IN-USE,SSID,SIGNAL",
                        "device",
                        "wifi",
                        "list",
                        "--rescan",
                        "no",
                    ]
        })
        .count();
    assert_eq!(
        wifi_list_calls, 0,
        "stale and deadline-expired requests must be cancelled before a second probe command"
    );
}

#[test]
fn mutation_uses_typed_fixed_command_then_confirmed_same_generation_readback() {
    let runner = base_runner().output("nmcli", &["radio", "wifi", "on"], "");
    let facade = SystemFacade::new(runner.clone());

    let result = facade
        .mutate(90, SystemMutation::NetworkEnabled(true))
        .unwrap();

    assert_eq!(result.generation, 90);
    assert_eq!(result.snapshot.generation, 90);
    assert_eq!(result.mutation, SystemMutation::NetworkEnabled(true));
    let calls = runner.calls();
    let mutation_index = calls
        .iter()
        .position(|call| call.program == "nmcli" && call.args == ["radio", "wifi", "on"])
        .unwrap();
    let readback_index = calls
        .iter()
        .rposition(|call| {
            call.program == "nmcli" && call.args == ["--terse", "--fields", "WIFI", "general"]
        })
        .unwrap();
    assert!(mutation_index < readback_index);
    let json = serde_json::to_string(&result).unwrap();
    assert_eq!(
        sleepy_sdk::validate_system_mutation_result(&json).unwrap(),
        result
    );
}

#[test]
fn executable_adapter_fixture_matrix_classifies_every_outcome() {
    struct AdapterCase {
        name: &'static str,
        program: &'static str,
        args: &'static [&'static str],
        malformed: &'static str,
        capability: CapabilityId,
    }
    let cases = [
        AdapterCase {
            name: "network",
            program: "nmcli",
            args: &["--terse", "--fields", "WIFI", "general"],
            malformed: include_str!("fixtures/system/nmcli-malformed.txt"),
            capability: CapabilityId::NetworkEnabled,
        },
        AdapterCase {
            name: "bluetooth",
            program: "bluetoothctl",
            args: &["show"],
            malformed: include_str!("fixtures/system/bluetoothctl-malformed.txt"),
            capability: CapabilityId::BluetoothEnabled,
        },
        AdapterCase {
            name: "audio-output",
            program: "wpctl",
            args: &["get-volume", "@DEFAULT_AUDIO_SINK@"],
            malformed: include_str!("fixtures/system/wpctl-volume-malformed.txt"),
            capability: CapabilityId::AudioVolume,
        },
        AdapterCase {
            name: "audio-microphone",
            program: "wpctl",
            args: &["get-volume", "@DEFAULT_AUDIO_SOURCE@"],
            malformed: include_str!("fixtures/system/wpctl-volume-malformed.txt"),
            capability: CapabilityId::AudioMicrophoneLevel,
        },
        AdapterCase {
            name: "audio-devices",
            program: "wpctl",
            args: &["status", "--name"],
            malformed: include_str!("fixtures/system/wpctl-status-duplicate-id.txt"),
            capability: CapabilityId::AudioOutputDevice,
        },
        AdapterCase {
            name: "brightness",
            program: "brightnessctl",
            args: &["--machine-readable", "info"],
            malformed: include_str!("fixtures/system/brightnessctl-malformed.txt"),
            capability: CapabilityId::DisplayBrightness,
        },
        AdapterCase {
            name: "night-light",
            program: "systemctl",
            args: &["--user", "is-active", "gammastep.service"],
            malformed: include_str!("fixtures/system/gammastep-malformed.txt"),
            capability: CapabilityId::DisplayNightLightEnabled,
        },
        AdapterCase {
            name: "power-profile",
            program: "powerprofilesctl",
            args: &["get"],
            malformed: include_str!("fixtures/system/powerprofilesctl-malformed.txt"),
            capability: CapabilityId::PowerProfile,
        },
        AdapterCase {
            name: "battery",
            program: "upower",
            args: &[
                "--show-info",
                "/org/freedesktop/UPower/devices/DisplayDevice",
            ],
            malformed: include_str!("fixtures/system/upower-malformed.txt"),
            capability: CapabilityId::BatteryStatus,
        },
        AdapterCase {
            name: "media",
            program: "playerctl",
            args: &[
                "metadata",
                "--format",
                "{{status}}\\t{{title}}\\t{{artist}}",
            ],
            malformed: include_str!("fixtures/system/playerctl-malformed.txt"),
            capability: CapabilityId::MediaTransport,
        },
    ];

    for case in cases {
        let valid = SystemFacade::new(base_runner()).snapshot(1).unwrap();
        assert_eq!(
            valid.capabilities[&case.capability],
            CapabilityState::Available,
            "{} valid",
            case.name
        );

        let outcomes = [
            (
                "unsupported",
                ScriptedResponse {
                    delay: Duration::ZERO,
                    result: Err(RunnerError::spawn("fixture executable missing")),
                },
                CapabilityState::Unavailable,
                sleepy_sdk::CapabilityErrorKind::Unsupported,
            ),
            (
                "malformed",
                ScriptedResponse {
                    delay: Duration::ZERO,
                    result: Ok(CommandOutput {
                        status: 0,
                        stdout: case.malformed.as_bytes().to_vec(),
                        stderr: vec![],
                    }),
                },
                CapabilityState::Error,
                sleepy_sdk::CapabilityErrorKind::Parse,
            ),
            (
                "nonzero",
                ScriptedResponse {
                    delay: Duration::ZERO,
                    result: Ok(CommandOutput {
                        status: 8,
                        stdout: vec![],
                        stderr: b"fixture failure".to_vec(),
                    }),
                },
                CapabilityState::Error,
                sleepy_sdk::CapabilityErrorKind::Command,
            ),
            (
                "timeout",
                ScriptedResponse {
                    delay: Duration::ZERO,
                    result: Err(RunnerError::timeout("fixture timeout")),
                },
                CapabilityState::Error,
                sleepy_sdk::CapabilityErrorKind::Timeout,
            ),
        ];
        for (outcome, response, expected_state, expected_kind) in outcomes {
            let snapshot =
                SystemFacade::new(base_runner().failure(case.program, case.args, response))
                    .snapshot(1)
                    .unwrap();
            assert_eq!(
                snapshot.capabilities[&case.capability], expected_state,
                "{} {outcome}",
                case.name
            );
            assert_eq!(
                snapshot.diagnostics[&case.capability].kind, expected_kind,
                "{} {outcome}",
                case.name
            );
        }
    }

    for (outcome, response, expected) in [
        ("valid", None, CapabilityState::Available),
        (
            "unsupported",
            Some(Err(RunnerError::spawn("missing"))),
            CapabilityState::Unavailable,
        ),
        (
            "malformed-payload-ignored",
            Some(Ok(CommandOutput {
                status: 0,
                stdout: b"not a version".to_vec(),
                stderr: vec![],
            })),
            CapabilityState::Available,
        ),
        (
            "nonzero",
            Some(Ok(CommandOutput {
                status: 8,
                stdout: vec![],
                stderr: vec![],
            })),
            CapabilityState::Error,
        ),
        (
            "timeout",
            Some(Err(RunnerError::timeout("timeout"))),
            CapabilityState::Busy,
        ),
    ] {
        let runner = match response {
            None => base_runner(),
            Some(result) => base_runner().failure(
                "swaylock",
                &["--version"],
                ScriptedResponse {
                    delay: Duration::ZERO,
                    result,
                },
            ),
        };
        let snapshot = SystemFacade::new(runner).snapshot(1).unwrap();
        assert_eq!(
            snapshot.session_actions[&SessionAction::Lock],
            expected,
            "session {outcome}"
        );
    }
}

#[test]
fn process_runner_enforces_timeout_without_a_shell() {
    let command = CommandSpec {
        program: "sleep".to_owned(),
        args: vec!["1".to_owned()],
        env: vec![("LC_ALL".to_owned(), "C".to_owned())],
        timeout: Duration::from_millis(20),
    };
    let started = Instant::now();
    let error = ProcessCommandRunner.run(&command).unwrap_err();
    assert_eq!(error.kind(), RunnerErrorKind::Timeout);
    assert!(started.elapsed() < Duration::from_millis(300));
}

#[test]
fn process_runner_rejects_output_over_the_capture_limit() {
    let command = CommandSpec {
        program: "seq".to_owned(),
        args: vec!["1".to_owned(), "20000".to_owned()],
        env: vec![("LC_ALL".to_owned(), "C".to_owned())],
        timeout: Duration::from_secs(1),
    };
    let error = ProcessCommandRunner.run(&command).unwrap_err();
    assert_eq!(error.kind(), RunnerErrorKind::Io);
    assert!(error.message().contains("bounded capture limit"));
}

#[test]
fn process_runner_kills_and_reaps_a_superseded_real_child() {
    let root = tempfile::TempDir::new().unwrap();
    let script = root.path().join("observable-child");
    let pid_file = root.path().join("pid");
    let survived = root.path().join("survived");
    fs::write(
        &script,
        format!(
            "#!{}\necho $$ > \"$1\"\nwhile :; do :; done\necho survived > \"$2\"\n",
            executable_on_path("sh").display()
        ),
    )
    .unwrap();
    fs::set_permissions(&script, fs::Permissions::from_mode(0o755)).unwrap();

    let latest = Arc::new(AtomicU64::new(1));
    let control = RunControl::for_generation(
        Instant::now() + Duration::from_secs(5),
        1,
        Arc::clone(&latest),
    );
    let command = CommandSpec {
        program: script.to_string_lossy().into_owned(),
        args: vec![
            pid_file.to_string_lossy().into_owned(),
            survived.to_string_lossy().into_owned(),
        ],
        env: vec![("LC_ALL".to_owned(), "C".to_owned())],
        timeout: Duration::from_secs(5),
    };
    let child = std::thread::spawn(move || ProcessCommandRunner.run_controlled(&command, &control));
    let readiness_deadline = Instant::now() + Duration::from_secs(4);
    while !pid_file.exists() && Instant::now() < readiness_deadline {
        std::thread::sleep(Duration::from_millis(5));
    }
    assert!(pid_file.exists(), "child did not start before its deadline");
    let pid: i32 = fs::read_to_string(&pid_file)
        .unwrap()
        .trim()
        .parse()
        .unwrap();
    latest.store(2, Ordering::SeqCst);
    let error = child.join().unwrap().unwrap_err();
    assert_eq!(error.kind(), RunnerErrorKind::Cancelled);
    assert!(!survived.exists());
    let alive = unsafe { libc::kill(pid, 0) };
    assert_eq!(alive, -1, "cancelled child PID must have been reaped");
    assert_eq!(
        std::io::Error::last_os_error().raw_os_error(),
        Some(libc::ESRCH)
    );
}

#[test]
fn mutation_does_not_return_optimistic_state_when_readback_disagrees() {
    let runner = base_runner().output("nmcli", &["radio", "wifi", "off"], "");
    let error = SystemFacade::new(runner)
        .mutate(91, SystemMutation::NetworkEnabled(false))
        .unwrap_err();
    assert_eq!(error.kind(), SystemErrorKind::Command);
}

#[test]
fn malformed_mutation_readback_preserves_the_parse_error_kind() {
    let runner = base_runner()
        .output("nmcli", &["radio", "wifi", "on"], "")
        .output(
            "nmcli",
            &["--terse", "--fields", "WIFI", "general"],
            include_str!("fixtures/system/nmcli-malformed.txt"),
        );
    let error = SystemFacade::new(runner)
        .mutate(92, SystemMutation::NetworkEnabled(true))
        .unwrap_err();
    assert_eq!(error.kind(), SystemErrorKind::Parse);
}

#[test]
fn mutation_never_returns_an_sdk_invalid_approximately_equal_result() {
    let runner = base_runner()
        .output(
            "wpctl",
            &["set-volume", "@DEFAULT_AUDIO_SINK@", "0.420"],
            "",
        )
        .output("wpctl", &["set-volume", "@DEFAULT_AUDIO_SINK@", "0.42"], "")
        .output(
            "wpctl",
            &["get-volume", "@DEFAULT_AUDIO_SINK@"],
            "Volume: 0.421\n",
        );
    let error = SystemFacade::new(runner)
        .mutate(92, SystemMutation::AudioVolume(0.42))
        .unwrap_err();
    assert_eq!(error.kind(), SystemErrorKind::Command);
}

#[test]
fn output_device_must_be_an_id_advertised_by_fresh_snapshot() {
    let runner = base_runner();
    let error = SystemFacade::new(runner)
        .mutate(5, SystemMutation::AudioOutputDevice("USB DAC".into()))
        .unwrap_err();
    assert_eq!(error.kind(), SystemErrorKind::Unsupported);
}

#[test]
fn every_typed_mutation_has_a_fixed_command_contract() {
    let cases = [
        (
            SystemMutation::BluetoothEnabled(true),
            "bluetoothctl",
            vec!["power", "on"],
        ),
        (
            SystemMutation::AudioVolume(0.6),
            "wpctl",
            vec!["set-volume", "@DEFAULT_AUDIO_SINK@", "0.6"],
        ),
        (
            SystemMutation::AudioMuted(true),
            "wpctl",
            vec!["set-mute", "@DEFAULT_AUDIO_SINK@", "1"],
        ),
        (
            SystemMutation::AudioMicrophoneLevel(0.4),
            "wpctl",
            vec!["set-volume", "@DEFAULT_AUDIO_SOURCE@", "0.4"],
        ),
        (
            SystemMutation::AudioMicrophoneMuted(false),
            "wpctl",
            vec!["set-mute", "@DEFAULT_AUDIO_SOURCE@", "0"],
        ),
        (
            SystemMutation::AudioOutputDevice("77".into()),
            "wpctl",
            vec!["set-default", "77"],
        ),
        (
            SystemMutation::DisplayBrightness(0.75),
            "brightnessctl",
            vec!["set", "75%"],
        ),
        (
            SystemMutation::DisplayNightLightEnabled(false),
            "systemctl",
            vec!["--user", "stop", "gammastep.service"],
        ),
        (
            SystemMutation::PowerProfile(PowerProfile::Performance),
            "powerprofilesctl",
            vec!["set", "performance"],
        ),
        (
            SystemMutation::MediaTransport(MediaTransport::Next),
            "playerctl",
            vec!["next"],
        ),
    ];
    for (mutation, program, args) in cases {
        assert_eq!(
            sleepy_session::system::mutation_command(&mutation).unwrap(),
            CommandSpec::new(program, args)
        );
    }
}

#[test]
fn every_mutation_returns_only_an_sdk_valid_fresh_readback() {
    let cases: Vec<(SystemMutation, &str, Vec<&str>)> = vec![
        (
            SystemMutation::NetworkEnabled(true),
            "nmcli",
            vec!["radio", "wifi", "on"],
        ),
        (
            SystemMutation::BluetoothEnabled(true),
            "bluetoothctl",
            vec!["power", "on"],
        ),
        (
            SystemMutation::AudioVolume(0.42),
            "wpctl",
            vec!["set-volume", "@DEFAULT_AUDIO_SINK@", "0.42"],
        ),
        (
            SystemMutation::AudioMuted(false),
            "wpctl",
            vec!["set-mute", "@DEFAULT_AUDIO_SINK@", "0"],
        ),
        (
            SystemMutation::AudioMicrophoneLevel(0.31),
            "wpctl",
            vec!["set-volume", "@DEFAULT_AUDIO_SOURCE@", "0.31"],
        ),
        (
            SystemMutation::AudioMicrophoneMuted(true),
            "wpctl",
            vec!["set-mute", "@DEFAULT_AUDIO_SOURCE@", "1"],
        ),
        (
            SystemMutation::AudioOutputDevice("52".into()),
            "wpctl",
            vec!["set-default", "52"],
        ),
        (
            SystemMutation::DisplayBrightness(0.5),
            "brightnessctl",
            vec!["set", "50%"],
        ),
        (
            SystemMutation::DisplayNightLightEnabled(true),
            "systemctl",
            vec!["--user", "start", "gammastep.service"],
        ),
        (
            SystemMutation::PowerProfile(PowerProfile::Balanced),
            "powerprofilesctl",
            vec!["set", "balanced"],
        ),
        (
            SystemMutation::MediaTransport(MediaTransport::Next),
            "playerctl",
            vec!["next"],
        ),
    ];
    for (index, (mutation, program, arguments)) in cases.into_iter().enumerate() {
        let runner = base_runner().output(program, &arguments, "");
        let result = SystemFacade::new(runner.clone())
            .mutate((index + 1) as u64, mutation)
            .unwrap();
        sleepy_sdk::validate_system_mutation_result(&serde_json::to_string(&result).unwrap())
            .unwrap();
        let calls = runner.calls();
        let mutation_index = calls
            .iter()
            .position(|call| call.program == program && call.args == arguments)
            .unwrap();
        assert!(
            calls.iter().skip(mutation_index + 1).any(|call| {
                matches!(
                    call.program.as_str(),
                    "nmcli"
                        | "bluetoothctl"
                        | "wpctl"
                        | "brightnessctl"
                        | "systemctl"
                        | "powerprofilesctl"
                        | "playerctl"
                )
            }),
            "mutation {index} must be followed by a fresh probe"
        );
    }
}

#[test]
fn confirmed_session_actions_use_fixed_commands_and_never_snapshot() {
    let cases = [
        (SessionAction::Lock, "swaylock", vec!["--daemonize"]),
        (
            SessionAction::Logout,
            "niri",
            vec!["msg", "action", "quit", "--skip-confirmation"],
        ),
        (SessionAction::Reboot, "systemctl", vec!["reboot"]),
        (SessionAction::PowerOff, "systemctl", vec!["poweroff"]),
    ];
    for (action, program, args) in cases {
        let runner = ScriptedRunner::default().output(program, &args, "");
        let facade = SystemFacade::new(runner.clone());
        let result = facade
            .perform(
                33,
                SessionActionRequest {
                    schema_version: 1,
                    action,
                    confirmed: true,
                },
            )
            .unwrap();
        assert_eq!(result.generation, 33);
        assert_eq!(result.status, SessionActionStatus::Initiated);
        assert_eq!(runner.calls(), vec![CommandSpec::new(program, args)]);
    }
}

#[test]
fn unconfirmed_session_action_is_rejected_before_execution() {
    let runner = ScriptedRunner::default();
    let error = SystemFacade::new(runner.clone())
        .perform(
            1,
            SessionActionRequest {
                schema_version: 1,
                action: SessionAction::PowerOff,
                confirmed: false,
            },
        )
        .unwrap_err();
    assert_eq!(error.kind(), SystemErrorKind::ConfirmationRequired);
    assert!(runner.calls().is_empty());
}

#[test]
fn generation_zero_is_rejected_for_every_operation() {
    let facade = SystemFacade::new(ScriptedRunner::default());
    assert_eq!(
        facade.snapshot(0).unwrap_err().kind(),
        SystemErrorKind::InvalidGeneration
    );
    assert_eq!(
        facade
            .mutate(0, SystemMutation::AudioMuted(true))
            .unwrap_err()
            .kind(),
        SystemErrorKind::InvalidGeneration
    );
    assert_eq!(
        facade
            .perform(
                0,
                SessionActionRequest {
                    schema_version: 1,
                    action: SessionAction::Lock,
                    confirmed: true
                }
            )
            .unwrap_err()
            .kind(),
        SystemErrorKind::InvalidGeneration
    );
}

#[test]
fn lower_and_equal_generations_are_rejected_before_destructive_execution() {
    let runner = ScriptedRunner::default().output("swaylock", &["--daemonize"], "");
    let facade = SystemFacade::new(runner.clone());
    facade
        .perform(
            10,
            SessionActionRequest {
                schema_version: 1,
                action: SessionAction::Lock,
                confirmed: true,
            },
        )
        .unwrap();
    let accepted_calls = runner.calls().len();

    for generation in [9, 10] {
        let error = facade
            .perform(
                generation,
                SessionActionRequest {
                    schema_version: 1,
                    action: SessionAction::PowerOff,
                    confirmed: true,
                },
            )
            .unwrap_err();
        assert_eq!(error.kind(), SystemErrorKind::Stale);
        assert_eq!(runner.calls().len(), accepted_calls);
    }
}

#[test]
fn nonzero_timeout_and_parse_failures_keep_distinct_diagnostics() {
    let nonzero = ScriptedResponse {
        delay: Duration::ZERO,
        result: Ok(CommandOutput {
            status: 10,
            stdout: vec![],
            stderr: b"not available".to_vec(),
        }),
    };
    let timeout = ScriptedResponse {
        delay: Duration::ZERO,
        result: Err(RunnerError::timeout("nmcli exceeded its deadline")),
    };
    let command_snapshot =
        SystemFacade::new(base_runner().failure("bluetoothctl", &["show"], nonzero))
            .snapshot(1)
            .unwrap();
    assert_eq!(
        command_snapshot.diagnostics[&CapabilityId::BluetoothEnabled].kind,
        sleepy_sdk::CapabilityErrorKind::Command
    );
    let timeout_snapshot = SystemFacade::new(base_runner().failure(
        "nmcli",
        &["--terse", "--fields", "WIFI", "general"],
        timeout,
    ))
    .snapshot(2)
    .unwrap();
    assert_eq!(
        timeout_snapshot.diagnostics[&CapabilityId::NetworkEnabled].kind,
        sleepy_sdk::CapabilityErrorKind::Timeout
    );
}

#[test]
fn capability_map_is_closed_and_complete() {
    let expected = BTreeMap::from([
        (CapabilityId::NetworkEnabled, CapabilityState::Available),
        (CapabilityId::BluetoothEnabled, CapabilityState::Available),
        (CapabilityId::AudioVolume, CapabilityState::Available),
        (CapabilityId::AudioMuted, CapabilityState::Available),
        (
            CapabilityId::AudioMicrophoneLevel,
            CapabilityState::Available,
        ),
        (
            CapabilityId::AudioMicrophoneMuted,
            CapabilityState::Available,
        ),
        (CapabilityId::AudioOutputDevice, CapabilityState::Available),
        (CapabilityId::DisplayBrightness, CapabilityState::Available),
        (
            CapabilityId::DisplayNightLightEnabled,
            CapabilityState::Available,
        ),
        (CapabilityId::PowerProfile, CapabilityState::Available),
        (CapabilityId::BatteryStatus, CapabilityState::Available),
        (CapabilityId::MediaTransport, CapabilityState::Available),
    ]);
    assert_eq!(
        SystemFacade::new(base_runner())
            .snapshot(1)
            .unwrap()
            .capabilities,
        expected
    );
}

#[test]
fn absent_backlight_disables_only_brightness() {
    let unsupported = ScriptedResponse {
        delay: Duration::ZERO,
        result: Ok(CommandOutput {
            status: 2,
            stdout: vec![],
            stderr: b"No devices found".to_vec(),
        }),
    };
    let snapshot = SystemFacade::new(base_runner().failure(
        "brightnessctl",
        &["--machine-readable", "info"],
        unsupported,
    ))
    .snapshot(1)
    .unwrap();
    assert_eq!(
        snapshot.capabilities[&CapabilityId::DisplayBrightness],
        CapabilityState::Unavailable
    );
    assert_eq!(
        snapshot.capabilities[&CapabilityId::DisplayNightLightEnabled],
        CapabilityState::Available
    );
    assert!(snapshot.display.unwrap().night_light_enabled);
}

#[test]
fn absent_battery_does_not_disable_power_profiles() {
    let unsupported = ScriptedResponse {
        delay: Duration::ZERO,
        result: Ok(CommandOutput {
            status: 1,
            stdout: vec![],
            stderr: b"device not found".to_vec(),
        }),
    };
    let snapshot = SystemFacade::new(base_runner().failure(
        "upower",
        &[
            "--show-info",
            "/org/freedesktop/UPower/devices/DisplayDevice",
        ],
        unsupported,
    ))
    .snapshot(1)
    .unwrap();
    assert_eq!(
        snapshot.capabilities[&CapabilityId::BatteryStatus],
        CapabilityState::Unavailable
    );
    assert_eq!(
        snapshot.capabilities[&CapabilityId::PowerProfile],
        CapabilityState::Available
    );
    assert_eq!(
        snapshot.power.unwrap().current_profile,
        Some(PowerProfile::Balanced)
    );
}

#[test]
fn missing_microphone_preserves_healthy_output_audio_capabilities() {
    let runner = base_runner().failure(
        "wpctl",
        &["get-volume", "@DEFAULT_AUDIO_SOURCE@"],
        ScriptedResponse {
            delay: Duration::ZERO,
            result: Err(RunnerError::spawn("no microphone")),
        },
    );
    let snapshot = SystemFacade::new(runner).snapshot(1).unwrap();
    assert_eq!(
        snapshot.capabilities[&CapabilityId::AudioVolume],
        CapabilityState::Available
    );
    assert_eq!(
        snapshot.capabilities[&CapabilityId::AudioMuted],
        CapabilityState::Available
    );
    assert_eq!(
        snapshot.capabilities[&CapabilityId::AudioMicrophoneLevel],
        CapabilityState::Unavailable
    );
    assert_eq!(
        snapshot.capabilities[&CapabilityId::AudioMicrophoneMuted],
        CapabilityState::Unavailable
    );
    assert_eq!(snapshot.audio.unwrap().volume, 0.42);
}

#[test]
fn duplicate_or_multiple_default_audio_devices_are_parse_errors_only_for_devices() {
    for fixture in [
        include_str!("fixtures/system/wpctl-status-duplicate-id.txt"),
        include_str!("fixtures/system/wpctl-status-two-defaults.txt"),
    ] {
        let runner = base_runner().output("wpctl", &["status", "--name"], fixture);
        let snapshot = SystemFacade::new(runner).snapshot(1).unwrap();
        assert_eq!(
            snapshot.capabilities[&CapabilityId::AudioVolume],
            CapabilityState::Available
        );
        assert_eq!(
            snapshot.capabilities[&CapabilityId::AudioOutputDevice],
            CapabilityState::Error
        );
        assert_eq!(
            snapshot.diagnostics[&CapabilityId::AudioOutputDevice].kind,
            sleepy_sdk::CapabilityErrorKind::Parse
        );
    }
}

#[test]
fn power_current_profile_must_be_in_available_profiles() {
    let runner = base_runner().output(
        "powerprofilesctl",
        &["list"],
        include_str!("fixtures/system/powerprofilesctl-missing-current.txt"),
    );
    let snapshot = SystemFacade::new(runner).snapshot(1).unwrap();
    assert_eq!(
        snapshot.capabilities[&CapabilityId::PowerProfile],
        CapabilityState::Error
    );
    assert_eq!(
        snapshot.diagnostics[&CapabilityId::PowerProfile].kind,
        sleepy_sdk::CapabilityErrorKind::Parse
    );
    assert_eq!(
        snapshot.capabilities[&CapabilityId::BatteryStatus],
        CapabilityState::Available
    );
}

#[test]
fn duplicate_power_profile_headers_degrade_only_the_profile_capability() {
    let runner = base_runner().output(
        "powerprofilesctl",
        &["list"],
        include_str!("fixtures/system/powerprofilesctl-duplicate.txt"),
    );
    let snapshot = SystemFacade::new(runner).snapshot(1).unwrap();
    assert_eq!(
        snapshot.capabilities[&CapabilityId::PowerProfile],
        CapabilityState::Error
    );
    assert_eq!(
        snapshot.diagnostics[&CapabilityId::PowerProfile].kind,
        sleepy_sdk::CapabilityErrorKind::Parse
    );
    assert_eq!(
        snapshot.capabilities[&CapabilityId::BatteryStatus],
        CapabilityState::Available
    );
    assert_eq!(
        snapshot.capabilities[&CapabilityId::NetworkEnabled],
        CapabilityState::Available
    );
    sleepy_sdk::validate_system_snapshot(&serde_json::to_string(&snapshot).unwrap()).unwrap();
}

#[test]
fn upower_state_mapping_is_closed_and_unknown_wire_state_is_nullable() {
    for (state, expected) in [
        ("charging", Some(true)),
        ("fully-charged", Some(true)),
        ("pending-charge", Some(true)),
        ("discharging", Some(false)),
        ("pending-discharge", Some(false)),
        ("empty", Some(false)),
        ("unknown", None),
    ] {
        let output = format!("state: {state}\npercentage: 50%\n");
        let runner = base_runner().output(
            "upower",
            &[
                "--show-info",
                "/org/freedesktop/UPower/devices/DisplayDevice",
            ],
            &output,
        );
        let snapshot = SystemFacade::new(runner).snapshot(1).unwrap();
        assert_eq!(snapshot.power.unwrap().charging, expected, "state {state}");
    }
}

#[test]
fn unrecognized_upower_state_degrades_only_battery() {
    let runner = base_runner().output(
        "upower",
        &[
            "--show-info",
            "/org/freedesktop/UPower/devices/DisplayDevice",
        ],
        include_str!("fixtures/system/upower-malformed.txt"),
    );
    let snapshot = SystemFacade::new(runner).snapshot(1).unwrap();
    assert_eq!(
        snapshot.capabilities[&CapabilityId::BatteryStatus],
        CapabilityState::Error
    );
    assert_eq!(
        snapshot.diagnostics[&CapabilityId::BatteryStatus].kind,
        sleepy_sdk::CapabilityErrorKind::Parse
    );
    assert_eq!(
        snapshot.capabilities[&CapabilityId::PowerProfile],
        CapabilityState::Available
    );
    assert_eq!(
        snapshot.capabilities[&CapabilityId::AudioVolume],
        CapabilityState::Available
    );
}

#[test]
fn inactive_gammastep_is_available_and_false() {
    let inactive = ScriptedResponse {
        delay: Duration::ZERO,
        result: Ok(CommandOutput {
            status: 3,
            stdout: b"inactive\n".to_vec(),
            stderr: vec![],
        }),
    };
    let snapshot = SystemFacade::new(base_runner().failure(
        "systemctl",
        &["--user", "is-active", "gammastep.service"],
        inactive,
    ))
    .snapshot(1)
    .unwrap();
    assert_eq!(
        snapshot.capabilities[&CapabilityId::DisplayNightLightEnabled],
        CapabilityState::Available
    );
    assert!(!snapshot.display.unwrap().night_light_enabled);
}

#[test]
fn busy_adapter_is_not_collapsed_into_command_error() {
    let busy = ScriptedResponse {
        delay: Duration::ZERO,
        result: Ok(CommandOutput {
            status: 75,
            stdout: vec![],
            stderr: vec![],
        }),
    };
    let snapshot = SystemFacade::new(base_runner().failure("bluetoothctl", &["show"], busy))
        .snapshot(1)
        .unwrap();
    assert_eq!(
        snapshot.capabilities[&CapabilityId::BluetoothEnabled],
        CapabilityState::Busy
    );
    assert_eq!(
        snapshot.diagnostics[&CapabilityId::BluetoothEnabled].kind,
        sleepy_sdk::CapabilityErrorKind::Busy
    );
}

#[test]
fn busy_mutation_returns_the_strict_busy_error_kind() {
    let busy = ScriptedResponse {
        delay: Duration::ZERO,
        result: Ok(CommandOutput {
            status: 75,
            stdout: vec![],
            stderr: vec![],
        }),
    };
    let runner = base_runner().failure("nmcli", &["radio", "wifi", "on"], busy);
    let error = SystemFacade::new(runner)
        .mutate(1, SystemMutation::NetworkEnabled(true))
        .unwrap_err();
    assert_eq!(error.kind(), SystemErrorKind::Busy);
    assert_eq!(error.code(), "busy");
}

#[test]
fn missing_lock_program_marks_only_lock_unavailable() {
    let runner = base_runner().failure(
        "swaylock",
        &["--version"],
        ScriptedResponse {
            delay: Duration::ZERO,
            result: Err(RunnerError::spawn("swaylock is unavailable")),
        },
    );
    let snapshot = SystemFacade::new(runner).snapshot(1).unwrap();
    assert_eq!(
        snapshot.session_actions[&SessionAction::Lock],
        CapabilityState::Unavailable
    );
    assert_eq!(
        snapshot.session_actions[&SessionAction::Logout],
        CapabilityState::Available
    );
}
