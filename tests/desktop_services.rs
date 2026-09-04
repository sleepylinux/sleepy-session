use std::{
    collections::{BTreeMap, VecDeque},
    fs::{self, OpenOptions},
    io,
    os::unix::fs::PermissionsExt,
    path::PathBuf,
    process::Command,
    sync::{
        atomic::{AtomicBool, AtomicUsize, Ordering},
        Arc, Mutex as StdMutex,
    },
    time::{Duration, Instant},
};

use async_trait::async_trait;
use fs2::FileExt;
use sleepy_sdk::{
    validate_desktop_envelope, validate_desktop_result, AppearanceCommand, AudioCommand,
    BluetoothCommand, CapabilityAvailability, CapabilityFailure, CapabilityRecord, CapabilityValue,
    DesktopCommand, DesktopDomainUpdate as SdkDomainUpdate, DesktopEnvelope, DesktopEvent,
    DesktopRequest, DesktopResultStatus, DesktopSessionCommand, DesktopSystemUpdate,
    DesktopUtilityUpdate, EventCause, EventCauseKind, LauncherCommand, LockState, MediaCommand,
    MediaTransport, NetworkAccessPoint, NetworkCommand, NetworkRuntimeState, NetworkSnapshot,
    RuntimeCapabilityId, StableId, UtilityCommand, DESKTOP_WIRE_VERSION, WIRE_SCHEMA_VERSION,
};
use sleepy_session::desktop::adapters::AppearanceProducer;
use sleepy_session::desktop::core::{state_from_record, CoreSystemProducer};
use sleepy_session::desktop::resources::parse_host_resources;
use sleepy_session::desktop::secret_agent::{
    NetworkSecretExchange, SecretBroker, SecretRequestLease, SecretSocket, SecretZeroizeObserver,
    MAX_SECRET_FRAME,
};
use sleepy_session::desktop::utilities::{ProductionUtilityService, UtilityProducer};
use sleepy_session::desktop::{audio, bluetooth, display, media, network, power};
use sleepy_session::desktop::{
    DesktopControlAuthority, DesktopDomainId, DesktopDomainState, DesktopDomainValue,
    DesktopMutationExecutor, DesktopMutationOutcome, DesktopProducer, DesktopProducerContext,
    DesktopRegistry, DesktopStateAuthority, ProducerError,
};
use sleepy_session::sessiond::supervisor::PreparedDesktopSockets;
use sleepy_session::system::{
    CommandOutput, CommandRunner, CommandSpec, ProcessCommandRunner, RunControl, RunnerError,
    SystemFacade,
};
use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader};
use tokio::net::UnixStream;
use tokio::sync::mpsc;

#[derive(Clone)]
struct StaticProducer {
    domain: DesktopDomainId,
    state: DesktopDomainState,
    delay: Duration,
}

struct ReconnectingProducer {
    domain: DesktopDomainId,
    calls: Arc<AtomicUsize>,
}

struct PanickingProducer {
    domain: DesktopDomainId,
    calls: Arc<AtomicUsize>,
}

struct DrainTrackingProducer {
    domain: DesktopDomainId,
    drained: Arc<AtomicUsize>,
}

struct UncooperativeAsyncProducer(DesktopDomainId);

struct SingleUpdateProducer {
    domain: DesktopDomainId,
    update_sent: Arc<AtomicBool>,
}

struct RegisteredBlockingProducer {
    domain: DesktopDomainId,
    started: Arc<AtomicBool>,
    finished: Arc<AtomicBool>,
    test_release: Arc<AtomicBool>,
}

struct IdleReadbackExecutor;

struct PausedIdleObservationProducer {
    sampled: Arc<AtomicBool>,
    release: Arc<tokio::sync::Notify>,
    enqueued: Arc<AtomicBool>,
}

struct LauncherReadbackExecutor;

struct PausedLauncherObservationProducer {
    sampled: Arc<AtomicBool>,
    release: Arc<tokio::sync::Notify>,
    enqueued: Arc<AtomicBool>,
}

#[async_trait]
impl DesktopProducer for ReconnectingProducer {
    fn domain(&self) -> DesktopDomainId {
        self.domain
    }

    async fn initial(&self) -> DesktopDomainState {
        DesktopDomainState::terminal(
            self.domain,
            CapabilityAvailability::Unavailable,
            "fixture unavailable",
        )
        .unwrap()
    }

    async fn run(
        &self,
        _sender: mpsc::Sender<sleepy_session::desktop::DesktopDomainUpdate>,
        cancellation: DesktopProducerContext,
    ) -> Result<(), ProducerError> {
        let call = self.calls.fetch_add(1, Ordering::SeqCst);
        if call == 0 {
            return Err(ProducerError::new("fixture transient disconnect"));
        }
        cancellation.cancelled().await;
        Ok(())
    }
}

#[async_trait]
impl DesktopProducer for PanickingProducer {
    fn domain(&self) -> DesktopDomainId {
        self.domain
    }

    async fn initial(&self) -> DesktopDomainState {
        DesktopDomainState::terminal(
            self.domain,
            CapabilityAvailability::Unavailable,
            "fixture unavailable",
        )
        .unwrap()
    }

    async fn run(
        &self,
        _sender: mpsc::Sender<sleepy_session::desktop::DesktopDomainUpdate>,
        _cancellation: DesktopProducerContext,
    ) -> Result<(), ProducerError> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        panic!("fixture producer panic")
    }
}

#[async_trait]
impl DesktopProducer for DrainTrackingProducer {
    fn domain(&self) -> DesktopDomainId {
        self.domain
    }

    async fn initial(&self) -> DesktopDomainState {
        DesktopDomainState::terminal(
            self.domain,
            CapabilityAvailability::Unavailable,
            "fixture unavailable",
        )
        .unwrap()
    }

    async fn run(
        &self,
        _sender: mpsc::Sender<sleepy_session::desktop::DesktopDomainUpdate>,
        cancellation: DesktopProducerContext,
    ) -> Result<(), ProducerError> {
        cancellation.cancelled().await;
        tokio::time::sleep(Duration::from_millis(25)).await;
        self.drained.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }
}

#[async_trait]
impl DesktopProducer for UncooperativeAsyncProducer {
    fn domain(&self) -> DesktopDomainId {
        self.0
    }

    async fn initial(&self) -> DesktopDomainState {
        DesktopDomainState::terminal(
            self.0,
            CapabilityAvailability::Unavailable,
            "fixture unavailable",
        )
        .unwrap()
    }

    async fn run(
        &self,
        _sender: mpsc::Sender<sleepy_session::desktop::DesktopDomainUpdate>,
        _cancellation: DesktopProducerContext,
    ) -> Result<(), ProducerError> {
        std::future::pending().await
    }
}

#[async_trait]
impl DesktopProducer for SingleUpdateProducer {
    fn domain(&self) -> DesktopDomainId {
        self.domain
    }

    async fn initial(&self) -> DesktopDomainState {
        DesktopDomainState::available(self.domain, DesktopDomainValue::empty(self.domain)).unwrap()
    }

    async fn run(
        &self,
        sender: mpsc::Sender<sleepy_session::desktop::DesktopDomainUpdate>,
        cancellation: DesktopProducerContext,
    ) -> Result<(), ProducerError> {
        sender
            .send(sleepy_session::desktop::DesktopDomainUpdate::unversioned(
                DesktopDomainState::available(self.domain, DesktopDomainValue::empty(self.domain))
                    .unwrap(),
            ))
            .await
            .map_err(|_| ProducerError::new("desktop state authority stopped"))?;
        self.update_sent.store(true, Ordering::SeqCst);
        cancellation.cancelled().await;
        Ok(())
    }
}

#[async_trait]
impl DesktopProducer for RegisteredBlockingProducer {
    fn domain(&self) -> DesktopDomainId {
        self.domain
    }

    async fn initial(&self) -> DesktopDomainState {
        DesktopDomainState::available(self.domain, DesktopDomainValue::empty(self.domain)).unwrap()
    }

    async fn run(
        &self,
        _sender: mpsc::Sender<sleepy_session::desktop::DesktopDomainUpdate>,
        cancellation: DesktopProducerContext,
    ) -> Result<(), ProducerError> {
        let started = Arc::clone(&self.started);
        let finished = Arc::clone(&self.finished);
        let test_release = Arc::clone(&self.test_release);
        let _worker =
            cancellation.spawn_blocking(Instant::now() + Duration::from_secs(30), move |control| {
                started.store(true, Ordering::SeqCst);
                while !control.is_cancelled() && !test_release.load(Ordering::SeqCst) {
                    std::thread::sleep(Duration::from_millis(1));
                }
                finished.store(true, Ordering::SeqCst);
            });
        std::future::pending().await
    }
}

#[async_trait]
impl DesktopProducer for PausedIdleObservationProducer {
    fn domain(&self) -> DesktopDomainId {
        DesktopDomainId::IdleInhibit
    }

    async fn initial(&self) -> DesktopDomainState {
        DesktopDomainState::available(
            DesktopDomainId::IdleInhibit,
            DesktopDomainValue::IdleInhibit(false),
        )
        .unwrap()
    }

    async fn run(
        &self,
        sender: mpsc::Sender<sleepy_session::desktop::DesktopDomainUpdate>,
        context: DesktopProducerContext,
    ) -> Result<(), ProducerError> {
        let observation = context.begin_observation();
        self.sampled.store(true, Ordering::SeqCst);
        self.release.notified().await;
        sender
            .send(
                observation
                    .finish(
                        DesktopDomainState::available(
                            DesktopDomainId::IdleInhibit,
                            DesktopDomainValue::IdleInhibit(false),
                        )
                        .unwrap(),
                    )
                    .map_err(|error| ProducerError::new(error.to_string()))?,
            )
            .await
            .map_err(|_| ProducerError::new("desktop state authority stopped"))?;
        self.enqueued.store(true, Ordering::SeqCst);
        context.cancelled().await;
        Ok(())
    }
}

#[async_trait]
impl DesktopMutationExecutor for IdleReadbackExecutor {
    async fn execute(
        &self,
        _request: &DesktopRequest,
    ) -> Result<DesktopMutationOutcome, ProducerError> {
        Ok(DesktopMutationOutcome::Confirmed(vec![
            DesktopDomainState::available(
                DesktopDomainId::IdleInhibit,
                DesktopDomainValue::IdleInhibit(true),
            )
            .unwrap(),
        ]))
    }
}

#[async_trait]
impl DesktopProducer for PausedLauncherObservationProducer {
    fn domain(&self) -> DesktopDomainId {
        DesktopDomainId::Launcher
    }

    async fn initial(&self) -> DesktopDomainState {
        DesktopDomainState::available(
            DesktopDomainId::Launcher,
            DesktopDomainValue::Launcher(Vec::new()),
        )
        .unwrap()
    }

    async fn run(
        &self,
        sender: mpsc::Sender<sleepy_session::desktop::DesktopDomainUpdate>,
        context: DesktopProducerContext,
    ) -> Result<(), ProducerError> {
        let observation = context.begin_observation();
        self.sampled.store(true, Ordering::SeqCst);
        self.release.notified().await;
        sender
            .send(
                observation
                    .finish(
                        DesktopDomainState::available(
                            DesktopDomainId::Launcher,
                            DesktopDomainValue::Launcher(Vec::new()),
                        )
                        .unwrap(),
                    )
                    .map_err(|error| ProducerError::new(error.to_string()))?,
            )
            .await
            .map_err(|_| ProducerError::new("desktop state authority stopped"))?;
        self.enqueued.store(true, Ordering::SeqCst);
        context.cancelled().await;
        Ok(())
    }
}

#[async_trait]
impl DesktopMutationExecutor for LauncherReadbackExecutor {
    async fn execute(
        &self,
        _request: &DesktopRequest,
    ) -> Result<DesktopMutationOutcome, ProducerError> {
        Ok(DesktopMutationOutcome::Confirmed(vec![
            DesktopDomainState::available(
                DesktopDomainId::Launcher,
                DesktopDomainValue::Launcher(Vec::new()),
            )
            .unwrap(),
        ]))
    }
}

#[derive(Clone)]
struct BlockingProcessRunner {
    pid_path: Arc<PathBuf>,
}

#[derive(Clone)]
struct DescendantPipeRunner {
    parent_pid_path: Arc<PathBuf>,
    descendant_pid_path: Arc<PathBuf>,
}

#[derive(Clone)]
struct EscapedDescendantRunner {
    parent_pid_path: Arc<PathBuf>,
    descendant_pid_path: Arc<PathBuf>,
}

#[derive(Clone)]
struct FastEscapedDescendantRunner {
    descendant_pid_path: Arc<PathBuf>,
}

#[derive(Clone)]
struct DelayedZombieDescendantRunner {
    descendant_pid_path: Arc<PathBuf>,
}

impl CommandRunner for BlockingProcessRunner {
    fn run(&self, command: &CommandSpec) -> Result<CommandOutput, RunnerError> {
        self.run_controlled(command, &RunControl::for_timeout(Duration::from_secs(30)))
    }

    fn run_controlled(
        &self,
        _command: &CommandSpec,
        control: &RunControl,
    ) -> Result<CommandOutput, RunnerError> {
        let mut command = CommandSpec::new(
            "sh",
            [
                "-c".to_owned(),
                "printf '%s' \"$$\" > \"$1\"; exec sleep 30".to_owned(),
                "sleepy-producer-test".to_owned(),
                self.pid_path.to_string_lossy().into_owned(),
            ],
        );
        command.timeout = Duration::from_secs(30);
        ProcessCommandRunner.run_controlled(&command, control)
    }
}

impl CommandRunner for DescendantPipeRunner {
    fn run(&self, command: &CommandSpec) -> Result<CommandOutput, RunnerError> {
        self.run_controlled(command, &RunControl::for_timeout(Duration::from_secs(30)))
    }

    fn run_controlled(
        &self,
        _command: &CommandSpec,
        control: &RunControl,
    ) -> Result<CommandOutput, RunnerError> {
        let mut command = CommandSpec::new(
            "sh",
            [
                "-c".to_owned(),
                "printf '%s' \"$$\" > \"$1\"; sleep 30 & printf '%s' \"$!\" > \"$2\"; wait"
                    .to_owned(),
                "sleepy-producer-descendant-test".to_owned(),
                self.parent_pid_path.to_string_lossy().into_owned(),
                self.descendant_pid_path.to_string_lossy().into_owned(),
            ],
        );
        command.timeout = Duration::from_secs(30);
        ProcessCommandRunner.run_controlled(&command, control)
    }
}

impl CommandRunner for EscapedDescendantRunner {
    fn run(&self, command: &CommandSpec) -> Result<CommandOutput, RunnerError> {
        self.run_controlled(command, &RunControl::for_timeout(Duration::from_secs(30)))
    }

    fn run_controlled(
        &self,
        _command: &CommandSpec,
        control: &RunControl,
    ) -> Result<CommandOutput, RunnerError> {
        let mut command = CommandSpec::new(
            "sh",
            [
                "-c".to_owned(),
                "printf '%s' \"$$\" > \"$1\"; setsid sh -c 'printf \"%s\" \"$$\" > \"$1\"; exec sleep 30' sleepy-escaped \"$2\" & wait"
                    .to_owned(),
                "sleepy-producer-escaped-descendant-test".to_owned(),
                self.parent_pid_path.to_string_lossy().into_owned(),
                self.descendant_pid_path.to_string_lossy().into_owned(),
            ],
        );
        command.timeout = Duration::from_secs(30);
        ProcessCommandRunner.run_controlled(&command, control)
    }
}

impl CommandRunner for FastEscapedDescendantRunner {
    fn run(&self, command: &CommandSpec) -> Result<CommandOutput, RunnerError> {
        self.run_controlled(command, &RunControl::for_timeout(Duration::from_secs(30)))
    }

    fn run_controlled(
        &self,
        _command: &CommandSpec,
        control: &RunControl,
    ) -> Result<CommandOutput, RunnerError> {
        let mut command = CommandSpec::new(
            "sh",
            [
                "-c".to_owned(),
                "setsid sh -c '(sh -c '\\''printf \"%s\" \"$$\" > \"$1\"; exec sleep 30'\\'' sleepy-fast \"$1\" &) ; exit 0' sleepy-stage \"$1\" & while [ ! -s \"$1\" ]; do sleep 0.001; done; exit 0"
                    .to_owned(),
                "sleepy-fast-escaped-root".to_owned(),
                self.descendant_pid_path.to_string_lossy().into_owned(),
            ],
        );
        command.timeout = Duration::from_secs(30);
        ProcessCommandRunner.run_controlled(&command, control)
    }
}

impl CommandRunner for DelayedZombieDescendantRunner {
    fn run(&self, command: &CommandSpec) -> Result<CommandOutput, RunnerError> {
        self.run_controlled(command, &RunControl::for_timeout(Duration::from_secs(30)))
    }

    fn run_controlled(
        &self,
        _command: &CommandSpec,
        control: &RunControl,
    ) -> Result<CommandOutput, RunnerError> {
        let mut command = CommandSpec::new(
            "sh",
            [
                "-c".to_owned(),
                "setsid sh -c '(sh -c '\\''printf \"%s\" \"$$\" > \"$1\"; sleep 0.12; exit 0'\\'' sleepy-delayed \"$1\" &) ; exit 0' sleepy-stage \"$1\" & while [ ! -s \"$1\" ]; do sleep 0.001; done; exit 0"
                    .to_owned(),
                "sleepy-delayed-escaped-root".to_owned(),
                self.descendant_pid_path.to_string_lossy().into_owned(),
            ],
        );
        command.timeout = Duration::from_secs(30);
        ProcessCommandRunner.run_controlled(&command, control)
    }
}

#[derive(Clone)]
struct ScriptedRunner {
    outputs: Arc<StdMutex<VecDeque<Result<CommandOutput, RunnerError>>>>,
    seen: Arc<StdMutex<Vec<CommandSpec>>>,
}

impl ScriptedRunner {
    fn new(outputs: impl IntoIterator<Item = Result<CommandOutput, RunnerError>>) -> Self {
        Self {
            outputs: Arc::new(StdMutex::new(outputs.into_iter().collect())),
            seen: Arc::new(StdMutex::new(Vec::new())),
        }
    }
}

impl CommandRunner for ScriptedRunner {
    fn run(&self, command: &CommandSpec) -> Result<CommandOutput, RunnerError> {
        self.seen.lock().unwrap().push(command.clone());
        self.outputs
            .lock()
            .unwrap()
            .pop_front()
            .expect("scripted command output")
    }
}

fn command_output(stdout: &[u8]) -> Result<CommandOutput, RunnerError> {
    Ok(CommandOutput {
        status: 0,
        stdout: stdout.to_vec(),
        stderr: Vec::new(),
    })
}

#[async_trait]
impl DesktopProducer for StaticProducer {
    fn domain(&self) -> DesktopDomainId {
        self.domain
    }

    async fn initial(&self) -> DesktopDomainState {
        tokio::time::sleep(self.delay).await;
        self.state.clone()
    }

    async fn run(
        &self,
        _sender: mpsc::Sender<sleepy_session::desktop::DesktopDomainUpdate>,
        cancellation: DesktopProducerContext,
    ) -> Result<(), ProducerError> {
        cancellation.cancelled().await;
        Ok(())
    }
}

fn producer(domain: DesktopDomainId, state: DesktopDomainState) -> Arc<dyn DesktopProducer> {
    Arc::new(StaticProducer {
        domain,
        state,
        delay: Duration::ZERO,
    })
}

fn available_registry() -> Arc<DesktopRegistry> {
    Arc::new(
        DesktopRegistry::new(
            DesktopDomainId::ALL
                .into_iter()
                .map(|domain| {
                    producer(
                        domain,
                        DesktopDomainState::available(domain, DesktopDomainValue::empty(domain))
                            .unwrap(),
                    )
                })
                .collect(),
        )
        .unwrap(),
    )
}

#[test]
fn every_rich_system_mutation_has_a_narrow_fixed_argv_contract() {
    let cases = [
        (
            network::mutation_spec(&NetworkCommand::SetWifiEnabled { enabled: true }).unwrap(),
            ("nmcli", vec!["radio", "wifi", "on"]),
        ),
        (
            network::mutation_spec(&NetworkCommand::ScanWifi).unwrap(),
            ("nmcli", vec!["device", "wifi", "rescan"]),
        ),
        (
            network::mutation_spec(&NetworkCommand::ConnectWifi {
                access_point_id: StableId("wifi-ap:AA-BB-CC-DD-EE-FF".into()),
            })
            .unwrap(),
            (
                "nmcli",
                vec!["device", "wifi", "connect", "AA:BB:CC:DD:EE:FF"],
            ),
        ),
        (
            network::mutation_spec(&NetworkCommand::Disconnect {
                connection_id: StableId(
                    "nm-connection:123e4567-e89b-12d3-a456-426614174000".into(),
                ),
            })
            .unwrap(),
            (
                "nmcli",
                vec![
                    "connection",
                    "down",
                    "uuid",
                    "123e4567-e89b-12d3-a456-426614174000",
                ],
            ),
        ),
        (
            bluetooth::mutation_spec(&BluetoothCommand::SetPowered { powered: false }).unwrap(),
            ("bluetoothctl", vec!["power", "off"]),
        ),
        (
            bluetooth::mutation_spec(&BluetoothCommand::Scan).unwrap(),
            ("bluetoothctl", vec!["scan", "on"]),
        ),
        (
            bluetooth::mutation_spec(&BluetoothCommand::Pair {
                device_id: StableId("bluetooth:01-23-45-67-89-AB".into()),
            })
            .unwrap(),
            ("bluetoothctl", vec!["pair", "01:23:45:67:89:AB"]),
        ),
        (
            bluetooth::mutation_spec(&BluetoothCommand::Connect {
                device_id: StableId("bluetooth:01-23-45-67-89-AB".into()),
            })
            .unwrap(),
            ("bluetoothctl", vec!["connect", "01:23:45:67:89:AB"]),
        ),
        (
            bluetooth::mutation_spec(&BluetoothCommand::Disconnect {
                device_id: StableId("bluetooth:01-23-45-67-89-AB".into()),
            })
            .unwrap(),
            ("bluetoothctl", vec!["disconnect", "01:23:45:67:89:AB"]),
        ),
        (
            audio::mutation_spec(&AudioCommand::SetDefaultNode {
                node_id: StableId("audio-node:42".into()),
            })
            .unwrap(),
            ("wpctl", vec!["set-default", "42"]),
        ),
        (
            audio::mutation_spec(&AudioCommand::SetNodeVolume {
                node_id: StableId("audio-node:42".into()),
                level: 0.25,
            })
            .unwrap(),
            ("wpctl", vec!["set-volume", "42", "0.250000"]),
        ),
        (
            audio::mutation_spec(&AudioCommand::SetNodeMuted {
                node_id: StableId("audio-node:42".into()),
                muted: true,
            })
            .unwrap(),
            ("wpctl", vec!["set-mute", "42", "1"]),
        ),
        (
            audio::mutation_spec(&AudioCommand::SetStreamVolume {
                stream_id: StableId("audio-stream:77".into()),
                level: 0.75,
            })
            .unwrap(),
            ("wpctl", vec!["set-volume", "77", "0.750000"]),
        ),
        (
            audio::mutation_spec(&AudioCommand::SetStreamMuted {
                stream_id: StableId("audio-stream:77".into()),
                muted: false,
            })
            .unwrap(),
            ("wpctl", vec!["set-mute", "77", "0"]),
        ),
        (
            media::mutation_spec(&MediaCommand::Transport {
                player_id: StableId("mpris:org.mpris.MediaPlayer2.firefox.instance1".into()),
                transport: MediaTransport::Next,
            })
            .unwrap(),
            (
                "playerctl",
                vec![
                    "--player",
                    "org.mpris.MediaPlayer2.firefox.instance1",
                    "next",
                ],
            ),
        ),
    ];

    for (spec, (program, args)) in cases {
        assert_eq!(spec.program, program);
        assert_eq!(spec.args, args);
        assert!(spec
            .env
            .iter()
            .any(|(key, value)| key == "LC_ALL" && value == "C"));
        assert!(spec.timeout <= Duration::from_secs(10));
        assert!(spec.max_output_bytes <= 64 * 1024);
    }

    for invalid in ["", "--help", "wifi-ap:not-a-mac", "audio-node:1;id"] {
        assert!(network::mutation_spec(&NetworkCommand::ConnectWifi {
            access_point_id: StableId(invalid.into()),
        })
        .is_err());
    }

    let brightness = display::brightness_spec(0.42).unwrap();
    assert_eq!(brightness.program, "brightnessctl");
    assert_eq!(brightness.args, ["set", "42.0000%"]);
    assert!(display::brightness_spec(f64::NAN).is_err());
    assert_eq!(
        display::brightness_spec_for_output("DP-1", 0.42)
            .unwrap_err()
            .kind(),
        io::ErrorKind::Unsupported
    );
    let profile = power::mutation_spec(sleepy_sdk::PowerProfile::Performance);
    assert_eq!(profile.program, "powerprofilesctl");
    assert_eq!(profile.args, ["set", "performance"]);
}

#[test]
fn rich_system_parsers_emit_complete_stable_sdk_snapshots() {
    let network = network::parse_snapshot(
        b"enabled\n",
        b"*:AA\\:BB\\:CC\\:DD\\:EE\\:FF:Sleepy\\:Lab:87:WPA2\n",
        b"123e4567-e89b-12d3-a456-426614174000:Sleepy Lab:802-11-wireless:wlan0\n",
    )
    .unwrap();
    assert!(network.wifi_enabled);
    assert_eq!(network.access_points[0].id, "wifi-ap:AA-BB-CC-DD-EE-FF");
    assert_eq!(network.access_points[0].ssid, "Sleepy:Lab");
    assert_eq!(network.access_points[0].signal_level, 0.87);
    assert!(network.access_points[0].secured);
    assert_eq!(
        network.connections[0].id,
        "nm-connection:123e4567-e89b-12d3-a456-426614174000"
    );

    let bluetooth = bluetooth::parse_snapshot(
        b"Controller 00:11:22:33:44:55 sleepy\n\tPowered: yes\n\tDiscovering: no\n",
        b"Device 01:23:45:67:89:AB Sleepy Headphones\n",
        &BTreeMap::from([(
            "01:23:45:67:89:AB".to_owned(),
            b"\tName: Sleepy Headphones\n\tPaired: yes\n\tConnected: yes\n".to_vec(),
        )]),
    )
    .unwrap();
    assert!(bluetooth.powered);
    assert!(!bluetooth.scanning);
    assert_eq!(bluetooth.devices[0].id, "bluetooth:01-23-45-67-89-AB");
    assert!(bluetooth.devices[0].paired && bluetooth.devices[0].connected);

    let audio = audio::parse_snapshot(
        b"Audio\n Sinks:\n  * 42. Sleepy Speakers [vol: 0.50]\n Sources:\n  * 43. Sleepy Microphone [vol: 0.25]\n Streams:\n    77. Browser [vol: 0.75]\n",
        &BTreeMap::from([
            ("42".to_owned(), b"Volume: 0.500000\n".to_vec()),
            ("43".to_owned(), b"Volume: 0.250000 [MUTED]\n".to_vec()),
            ("77".to_owned(), b"Volume: 0.750000\n".to_vec()),
        ]),
    )
    .unwrap();
    assert_eq!(audio.nodes.len(), 2);
    assert_eq!(audio.streams.len(), 1);
    assert_eq!(audio.nodes[0].id, "audio-node:42");
    assert_eq!(audio.nodes[1].id, "audio-node:43");
    assert_eq!(audio.streams[0].id, "audio-stream:77");
    assert_eq!(audio.streams[0].node_id, "audio-node:42");
    assert!(audio.nodes[1].muted);

    let media = media::parse_snapshot(
        b"org.mpris.MediaPlayer2.firefox.instance1\n",
        &BTreeMap::from([(
            "org.mpris.MediaPlayer2.firefox.instance1".to_owned(),
            b"Firefox\tA title\tAn artist\tPlaying\t50\t100\n".to_vec(),
        )]),
    )
    .unwrap();
    assert_eq!(media.players.len(), 1);
    assert_eq!(
        media.players[0].id,
        "mpris:org.mpris.MediaPlayer2.firefox.instance1"
    );
    assert!(media.players[0].playing);
    assert_eq!(media.players[0].progress, 0.5);
}

#[test]
fn per_item_probes_validate_dedupe_and_bound_identifiers_before_subprocesses() {
    let bluetooth_rows = (0..1_025)
        .map(|index| {
            format!(
                "Device 02:00:00:{:02X}:{:02X}:{:02X} Device\n",
                (index >> 16) & 0xff,
                (index >> 8) & 0xff,
                index & 0xff
            )
        })
        .collect::<String>();
    let bluetooth_runner = ScriptedRunner::new([
        command_output(b"\tPowered: yes\n\tDiscovering: no\n"),
        command_output(bluetooth_rows.as_bytes()),
    ]);
    assert!(bluetooth::probe(&bluetooth_runner).is_err());
    assert_eq!(bluetooth_runner.seen.lock().unwrap().len(), 2);

    let audio_runner = ScriptedRunner::new([command_output(
        b"Audio\n Sinks:\n  * 42. Speakers\n  42. Duplicate\n",
    )]);
    assert!(audio::probe(&audio_runner).is_err());
    assert_eq!(audio_runner.seen.lock().unwrap().len(), 1);

    let player_rows = (0..257)
        .map(|index| format!("org.mpris.MediaPlayer2.fixture{index}\n"))
        .collect::<String>();
    let media_runner = ScriptedRunner::new([command_output(player_rows.as_bytes())]);
    assert!(media::probe(&media_runner).is_err());
    assert_eq!(media_runner.seen.lock().unwrap().len(), 1);
}

#[test]
fn rich_system_mutations_execute_then_require_complete_confirmed_readback() {
    let network_runner = ScriptedRunner::new([
        command_output(b""),
        command_output(b"enabled\n"),
        command_output(b"*:AA\\:BB\\:CC\\:DD\\:EE\\:FF:Sleepy:100:WPA2\n"),
        command_output(b"123e4567-e89b-12d3-a456-426614174000:Sleepy:802-11-wireless:wlan0\n"),
    ]);
    let network = network::mutate(&network_runner, &NetworkCommand::ScanWifi).unwrap();
    assert_eq!(network.access_points.len(), 1);
    assert_eq!(network_runner.seen.lock().unwrap().len(), 4);

    let wrong_access_point = ScriptedRunner::new([
        command_output(b""),
        command_output(b"enabled\n"),
        command_output(b"*:11\\:22\\:33\\:44\\:55\\:66:Other:100:WPA2\n"),
        command_output(b"123e4567-e89b-12d3-a456-426614174000:Other:802-11-wireless:wlan0\n"),
    ]);
    assert_eq!(
        network::mutate(
            &wrong_access_point,
            &NetworkCommand::ConnectWifi {
                access_point_id: StableId("wifi-ap:AA-BB-CC-DD-EE-FF".into()),
            },
        )
        .unwrap_err()
        .kind(),
        io::ErrorKind::Other
    );

    let bluetooth_runner = ScriptedRunner::new([
        command_output(b""),
        command_output(b"\tPowered: yes\n\tDiscovering: no\n"),
        command_output(b"Device 01:23:45:67:89:AB Headphones\n"),
        command_output(b"\tName: Headphones\n\tPaired: yes\n\tConnected: yes\n"),
    ]);
    let bluetooth = bluetooth::mutate(
        &bluetooth_runner,
        &BluetoothCommand::Connect {
            device_id: StableId("bluetooth:01-23-45-67-89-AB".into()),
        },
    )
    .unwrap();
    assert!(bluetooth.devices[0].connected);

    let audio_runner = ScriptedRunner::new([
        command_output(b""),
        command_output(
            b"Audio\n Sinks:\n  * 42. Speakers\n Sources:\n  * 43. Microphone\n Streams:\n    77. Browser\n",
        ),
        command_output(b"Volume: 0.5\n"),
        command_output(b"Volume: 0.4\n"),
        command_output(b"Volume: 0.3 [MUTED]\n"),
    ]);
    let audio = audio::mutate(
        &audio_runner,
        &AudioCommand::SetStreamMuted {
            stream_id: StableId("audio-stream:77".into()),
            muted: true,
        },
    )
    .unwrap();
    assert!(audio.streams[0].muted);

    let media_runner = ScriptedRunner::new([
        command_output(b"org.mpris.MediaPlayer2.test\n"),
        command_output(b"Test\tTitle\tArtist\tPlaying\t25\t100\n"),
        command_output(b""),
        command_output(b"org.mpris.MediaPlayer2.test\n"),
        command_output(b"Test\tTitle\tArtist\tPaused\t25\t100\n"),
    ]);
    let media = media::mutate(
        &media_runner,
        &MediaCommand::Transport {
            player_id: StableId("mpris:org.mpris.MediaPlayer2.test".into()),
            transport: MediaTransport::PlayPause,
        },
    )
    .unwrap();
    assert!(!media.players[0].playing);

    let ignored_media_command = ScriptedRunner::new([
        command_output(b"org.mpris.MediaPlayer2.test\n"),
        command_output(b"Test\tTitle\tArtist\tPlaying\t25\t100\n"),
        command_output(b""),
        command_output(b"org.mpris.MediaPlayer2.test\n"),
        command_output(b"Test\tTitle\tArtist\tPlaying\t25\t100\n"),
    ]);
    assert_eq!(
        media::mutate(
            &ignored_media_command,
            &MediaCommand::Transport {
                player_id: StableId("mpris:org.mpris.MediaPlayer2.test".into()),
                transport: MediaTransport::PlayPause,
            },
        )
        .unwrap_err()
        .kind(),
        io::ErrorKind::Other
    );

    let confirmed_next_track_identity = ScriptedRunner::new([
        command_output(b"org.mpris.MediaPlayer2.test\n"),
        command_output(b"Test\tSame title\tArtist\tPlaying\t25\t100\t/org/mpris/Track/1\n"),
        command_output(b""),
        command_output(b"org.mpris.MediaPlayer2.test\n"),
        command_output(b"Test\tSame title\tArtist\tPlaying\t25\t100\t/org/mpris/Track/2\n"),
    ]);
    let next = media::mutate(
        &confirmed_next_track_identity,
        &MediaCommand::Transport {
            player_id: StableId("mpris:org.mpris.MediaPlayer2.test".into()),
            transport: MediaTransport::Next,
        },
    )
    .unwrap();
    assert_eq!(next.players[0].title, "Same title");

    let ignored_next_with_progress_drift = ScriptedRunner::new([
        command_output(b"org.mpris.MediaPlayer2.test\n"),
        command_output(b"Test\tTitle\tArtist\tPlaying\t25\t100\n"),
        command_output(b""),
        command_output(b"org.mpris.MediaPlayer2.test\n"),
        command_output(b"Test\tTitle\tArtist\tPlaying\t26\t100\n"),
    ]);
    assert_eq!(
        media::mutate(
            &ignored_next_with_progress_drift,
            &MediaCommand::Transport {
                player_id: StableId("mpris:org.mpris.MediaPlayer2.test".into()),
                transport: MediaTransport::Next,
            },
        )
        .unwrap_err()
        .kind(),
        io::ErrorKind::Other
    );

    let power_runner = ScriptedRunner::new([
        command_output(b""),
        command_output(b"performance\n"),
        command_output(b"balanced:\n* performance:\n"),
    ]);
    let power = power::mutate(&power_runner, sleepy_sdk::PowerProfile::Performance).unwrap();
    assert_eq!(power.active_profile, sleepy_sdk::PowerProfile::Performance);
    assert_eq!(power_runner.seen.lock().unwrap().len(), 3);

    let timeout = ScriptedRunner::new([Err(RunnerError::timeout("fixture timeout"))]);
    assert_eq!(
        network::mutate(&timeout, &NetworkCommand::ScanWifi)
            .unwrap_err()
            .kind(),
        io::ErrorKind::TimedOut
    );
    let rejected = ScriptedRunner::new([]);
    assert_eq!(
        bluetooth::mutate(
            &rejected,
            &BluetoothCommand::Pair {
                device_id: StableId("--not-a-device".into()),
            },
        )
        .unwrap_err()
        .kind(),
        io::ErrorKind::InvalidInput
    );
    assert!(rejected.seen.lock().unwrap().is_empty());
}

#[test]
fn every_utility_action_has_a_typed_bounded_transport_contract() {
    use sleepy_session::desktop::{clipboard, tray, utilities};

    assert_eq!(
        clipboard::list_spec().args,
        Vec::<String>::from(["list".into()])
    );
    assert_eq!(
        clipboard::clear_spec().args,
        Vec::<String>::from(["wipe".into()])
    );
    assert_eq!(
        clipboard::decode_spec(&StableId("clipboard:42".into()))
            .unwrap()
            .args,
        Vec::<String>::from(["decode".into(), "42".into()])
    );
    assert!(clipboard::decode_spec(&StableId("clipboard:--help".into())).is_err());

    let entries = clipboard::parse_entries(b"42\ttext/plain\tA safe preview\t14\n").unwrap();
    assert_eq!(entries.len(), 1);
    assert_eq!(entries[0].id, "clipboard:42");
    assert_eq!(entries[0].preview, "A safe preview");
    assert_eq!(entries[0].mime_type, "text/plain");
    assert_eq!(entries[0].byte_length, 14);

    let screenshot = utilities::action_spec(
        &UtilityCommand::Screenshot {
            output_id: StableId("output:DP-1".into()),
        },
        "/run/user/1000/sleepy/captures/capture.png",
    )
    .unwrap()
    .unwrap();
    assert_eq!(screenshot.program, "sleepy-capture-helper");
    assert_eq!(
        screenshot.args,
        [
            "screenshot",
            "--interactive-consent",
            "--output-id",
            "DP-1",
            "--output-path",
            "/run/user/1000/sleepy/captures/capture.png"
        ]
    );
    let color = utilities::action_spec(&UtilityCommand::PickColor, "unused")
        .unwrap()
        .unwrap();
    assert_eq!(color.program, "sleepy-capture-helper");
    assert_eq!(
        color.args,
        ["pick-color", "--interactive-consent", "--clipboard"]
    );
    let recording = utilities::action_spec(
        &UtilityCommand::StartRecording {
            output_id: StableId("output:eDP-1".into()),
            target: sleepy_sdk::RecordingTarget::Region,
            region: Some(sleepy_sdk::RecordingRegion {
                x: -100,
                y: 20,
                width: 640,
                height: 480,
            }),
            audio: true,
        },
        "/run/user/1000/sleepy/captures/recording.mkv",
    )
    .unwrap()
    .unwrap();
    assert_eq!(recording.program, "sleepy-recording-helper");
    assert_eq!(
        recording.args,
        [
            "record",
            "--interactive-consent",
            "--output-id",
            "eDP-1",
            "--region",
            r#"{"x":-100,"y":20,"width":640,"height":480}"#,
            "--output-path",
            "/run/user/1000/sleepy/captures/recording.mkv",
            "--audio",
            "--status-fd",
            "1"
        ]
    );
    for command in [
        UtilityCommand::InvokeTrayMenu {
            item_id: StableId("tray:1a2b".into()),
            menu_id: StableId("tray-menu:1a2b:7".into()),
        },
        UtilityCommand::PasteClipboard {
            entry_id: StableId("clipboard:42".into()),
        },
        UtilityCommand::ClearClipboard,
        UtilityCommand::SetIdleInhibited { enabled: true },
        UtilityCommand::PauseRecording,
        UtilityCommand::StopRecording,
        UtilityCommand::DeleteRecording {
            recording_id: StableId("recording_20260901_12-34-56.mp4".into()),
        },
        UtilityCommand::SetGameMode { enabled: true },
    ] {
        assert!(utilities::action_spec(&command, "unused")
            .unwrap()
            .is_none());
    }

    assert_eq!(
        tray::split_registration("org.example.Item/StatusNotifierItem").unwrap(),
        ("org.example.Item", "/StatusNotifierItem")
    );
    assert_eq!(
        tray::split_registration("org.example.Item").unwrap(),
        ("org.example.Item", "/StatusNotifierItem")
    );
    assert!(tray::split_registration("--invalid").is_err());
}

#[test]
fn recording_deletion_is_confined_to_owned_regular_capture_files() {
    use sleepy_session::desktop::utilities::ProductionUtilityService;

    let temp = tempfile::tempdir().unwrap();
    let recording = "recording_20260901_12-34-56.mp4";
    fs::write(temp.path().join(recording), b"video").unwrap();
    let service = ProductionUtilityService::open(temp.path()).unwrap();
    service
        .execute(&UtilityCommand::DeleteRecording {
            recording_id: StableId(recording.into()),
        })
        .unwrap();
    assert!(!temp.path().join(recording).exists());

    let outside = tempfile::NamedTempFile::new().unwrap();
    let link = "recording_20260901_12-34-57.mp4";
    std::os::unix::fs::symlink(outside.path(), temp.path().join(link)).unwrap();
    let error = service
        .execute(&UtilityCommand::DeleteRecording {
            recording_id: StableId(link.into()),
        })
        .unwrap_err();
    assert_eq!(error.kind(), io::ErrorKind::PermissionDenied);
    assert!(outside.path().exists());
}

#[tokio::test]
async fn appearance_actions_persist_and_return_confirmed_mature_theme_readback() {
    use sleepy_session::{desktop::appearance::AppearanceService, theme::ThemeManager};

    let temp = tempfile::tempdir().unwrap();
    let config = temp.path().join("config");
    let state = temp.path().join("state");
    let manager = Arc::new(tokio::sync::Mutex::new(
        ThemeManager::open(&config, &state).unwrap(),
    ));
    let service = AppearanceService::open(Arc::clone(&manager), &state).unwrap();

    let applied = service
        .apply(&AppearanceCommand::ApplyTheme {
            theme_id: StableId("builtin.sleepy-light".into()),
        })
        .await
        .unwrap();
    assert_eq!(applied.theme.id, "builtin.sleepy-light");

    let reduced = service
        .apply(&AppearanceCommand::SetReducedMotion { enabled: true })
        .await
        .unwrap();
    assert!(reduced.theme.reduced_motion);
    let opaque = service
        .apply(&AppearanceCommand::SetOpaque { enabled: true })
        .await
        .unwrap();
    assert!(opaque.theme.opaque_fallback);
    let wallpaper = service
        .apply(&AppearanceCommand::SetWallpaper {
            wallpaper_id: StableId("wallpaper:sleepy-night".into()),
        })
        .await
        .unwrap();
    assert_eq!(wallpaper.wallpaper_id, "wallpaper:sleepy-night");

    let reopened = AppearanceService::open(
        Arc::new(tokio::sync::Mutex::new(
            ThemeManager::open(&config, &state).unwrap(),
        )),
        &state,
    )
    .unwrap();
    let snapshot = reopened.snapshot().await.unwrap();
    assert_eq!(snapshot.wallpaper_id, "wallpaper:sleepy-night");
    assert!(snapshot.theme.reduced_motion);
    assert!(snapshot.theme.opaque_fallback);
}

fn complete_registry_with(
    replacement: Option<(DesktopDomainId, Arc<dyn DesktopProducer>)>,
) -> Vec<Arc<dyn DesktopProducer>> {
    DesktopDomainId::ALL
        .into_iter()
        .map(|domain| {
            replacement
                .as_ref()
                .filter(|(id, _)| *id == domain)
                .map(|(_, producer)| Arc::clone(producer))
                .unwrap_or_else(|| {
                    producer(
                        domain,
                        DesktopDomainState::terminal(
                            domain,
                            CapabilityAvailability::Unsupported,
                            "fixture unsupported",
                        )
                        .unwrap(),
                    )
                })
        })
        .collect()
}

#[test]
fn registry_domain_ids_are_exhaustive_and_stably_ordered() {
    assert_eq!(
        DesktopDomainId::ALL,
        [
            DesktopDomainId::Network,
            DesktopDomainId::Bluetooth,
            DesktopDomainId::Audio,
            DesktopDomainId::Media,
            DesktopDomainId::Battery,
            DesktopDomainId::Brightness,
            DesktopDomainId::NightLight,
            DesktopDomainId::Power,
            DesktopDomainId::Osd,
            DesktopDomainId::Lock,
            DesktopDomainId::Hyprland,
            DesktopDomainId::Notifications,
            DesktopDomainId::Launcher,
            DesktopDomainId::Calendar,
            DesktopDomainId::Weather,
            DesktopDomainId::Appearance,
            DesktopDomainId::Resources,
            DesktopDomainId::Tray,
            DesktopDomainId::Clipboard,
            DesktopDomainId::Recording,
            DesktopDomainId::IdleInhibit,
            DesktopDomainId::GameMode,
            DesktopDomainId::Screenshot,
            DesktopDomainId::ColorPicker,
        ]
    );
}

#[test]
fn registry_rejects_every_missing_or_duplicate_domain() {
    let complete = complete_registry_with(None);
    assert!(DesktopRegistry::new(complete.clone()).is_ok());

    for missing in DesktopDomainId::ALL {
        let without = complete
            .iter()
            .filter(|producer| producer.domain() != missing)
            .cloned()
            .collect();
        let error = DesktopRegistry::new(without).unwrap_err();
        assert_eq!(
            error.kind(),
            io::ErrorKind::InvalidInput,
            "missing {missing:?}"
        );
    }

    for duplicate in DesktopDomainId::ALL {
        let mut with_duplicate = complete.clone();
        with_duplicate.push(
            complete
                .iter()
                .find(|producer| producer.domain() == duplicate)
                .unwrap()
                .clone(),
        );
        let error = DesktopRegistry::new(with_duplicate).unwrap_err();
        assert_eq!(
            error.kind(),
            io::ErrorKind::InvalidInput,
            "duplicate {duplicate:?}"
        );
    }
}

#[tokio::test]
async fn initial_registry_preserves_every_terminal_status_without_a_placeholder() {
    for domain in DesktopDomainId::ALL {
        for status in [
            CapabilityAvailability::Unavailable,
            CapabilityAvailability::Unsupported,
            CapabilityAvailability::PermissionDenied,
            CapabilityAvailability::Timeout,
            CapabilityAvailability::Parse,
            CapabilityAvailability::Error,
        ] {
            let state = DesktopDomainState::terminal(domain, status, "fixture terminal").unwrap();
            let registry = DesktopRegistry::new(complete_registry_with(Some((
                domain,
                producer(domain, state),
            ))))
            .unwrap();
            let states = registry.initial_states().await;
            assert_eq!(states[&domain].status(), status, "{domain:?}");
            assert_ne!(
                states[&domain].diagnostic(),
                Some("has not reported"),
                "{domain:?}"
            );
        }
    }
}

#[tokio::test]
async fn malformed_initial_state_localizes_parse_to_its_declared_owner() {
    let owner = DesktopDomainId::Network;
    let malformed = Arc::new(StaticProducer {
        domain: owner,
        state: DesktopDomainState::terminal(
            DesktopDomainId::Audio,
            CapabilityAvailability::Unavailable,
            "wrong owner",
        )
        .unwrap(),
        delay: Duration::ZERO,
    });
    let registry = DesktopRegistry::new(complete_registry_with(Some((owner, malformed)))).unwrap();
    let states = registry.initial_states().await;

    assert_eq!(states[&owner].status(), CapabilityAvailability::Parse);
    assert_eq!(
        states[&DesktopDomainId::Audio].status(),
        CapabilityAvailability::Unsupported
    );
}

#[tokio::test]
async fn invalid_initial_payload_is_localized_before_first_publication() {
    let invalid = DesktopDomainState::available(
        DesktopDomainId::Network,
        DesktopDomainValue::Network(NetworkSnapshot {
            wifi_enabled: true,
            scanning: false,
            access_points: vec![NetworkAccessPoint {
                id: "wifi-ap:AA-BB-CC-DD-EE-FF".into(),
                ssid: "Sleepy".into(),
                signal_level: f64::NAN,
                secured: true,
            }],
            connections: Vec::new(),
        }),
    )
    .unwrap();
    let registry = Arc::new(
        DesktopRegistry::new(complete_registry_with(Some((
            DesktopDomainId::Network,
            producer(DesktopDomainId::Network, invalid),
        ))))
        .unwrap(),
    );
    let temp = tempfile::tempdir().unwrap();
    let authority = DesktopStateAuthority::open(registry, temp.path().join("generation"), 8)
        .await
        .unwrap();
    let initial = authority.initialize().await.unwrap();
    validate_desktop_envelope(&serde_json::to_string(&initial).unwrap()).unwrap();
    let DesktopEvent::FullSnapshot(snapshot) = &initial.payload else {
        unreachable!()
    };
    assert_eq!(
        snapshot.system.network.status,
        CapabilityAvailability::Parse
    );
    assert_eq!(
        snapshot.system.audio.status,
        CapabilityAvailability::Unsupported
    );
}

#[tokio::test(start_paused = true)]
async fn a_slow_initial_producer_becomes_timeout_at_the_shared_two_second_deadline() {
    let domain = DesktopDomainId::Network;
    let slow: Arc<dyn DesktopProducer> = Arc::new(StaticProducer {
        domain,
        state: DesktopDomainState::terminal(
            domain,
            CapabilityAvailability::Unavailable,
            "must not win after deadline",
        )
        .unwrap(),
        delay: Duration::from_secs(60),
    });
    let registry = DesktopRegistry::new(complete_registry_with(Some((domain, slow)))).unwrap();
    let task = tokio::spawn(async move { registry.initial_states().await });

    tokio::time::advance(Duration::from_secs(2)).await;
    let states = task.await.unwrap();

    assert_eq!(states[&domain].status(), CapabilityAvailability::Timeout);
    assert_eq!(
        states[&domain].diagnostic(),
        Some("producer initial state exceeded the two second deadline")
    );
}

#[tokio::test]
async fn initialization_deadline_begins_before_generation_open_and_never_resets() {
    let temp = tempfile::tempdir().unwrap();
    let authority = DesktopStateAuthority::open(
        available_registry(),
        temp.path().join("desktop-generation"),
        8,
    )
    .await
    .unwrap();
    tokio::time::sleep(Duration::from_millis(2_050)).await;

    let started = Instant::now();
    let error = authority.initialize().await.unwrap_err();

    assert_eq!(error.kind(), io::ErrorKind::TimedOut);
    assert!(started.elapsed() < Duration::from_millis(100));
    assert_eq!(authority.current_generation(), 0);
}

#[tokio::test]
async fn completed_initialization_replays_after_the_first_publication_deadline() {
    let temp = tempfile::tempdir().unwrap();
    let authority = DesktopStateAuthority::open(
        available_registry(),
        temp.path().join("desktop-generation"),
        8,
    )
    .await
    .unwrap();
    let first = authority.initialize().await.unwrap();
    tokio::time::sleep(Duration::from_millis(2_050)).await;

    let started = Instant::now();
    let replay = authority.initialize().await.unwrap();

    assert_eq!(replay, first);
    assert!(started.elapsed() < Duration::from_millis(100));
    assert_eq!(authority.current_generation(), first.generation);
}

#[tokio::test]
async fn every_available_domain_assembles_one_sdk_valid_atomic_snapshot() {
    let producers = DesktopDomainId::ALL
        .into_iter()
        .map(|domain| {
            producer(
                domain,
                DesktopDomainState::available(domain, DesktopDomainValue::empty(domain)).unwrap(),
            )
        })
        .collect();
    let registry = DesktopRegistry::new(producers).unwrap();
    let states = registry.initial_states().await;
    let snapshot = registry.assemble(&states).unwrap();
    let envelope = DesktopEnvelope {
        schema_version: DESKTOP_WIRE_VERSION,
        generation: 1,
        event_id: "00000000-0000-4000-8000-000000000001".into(),
        emitted_at: "2026-08-31T00:00:00Z".into(),
        cause: EventCause {
            kind: EventCauseKind::Lifecycle,
            request_id: None,
        },
        payload: DesktopEvent::FullSnapshot(Box::new(snapshot)),
    };

    validate_desktop_envelope(&serde_json::to_string(&envelope).unwrap()).unwrap();
    let DesktopEvent::FullSnapshot(snapshot) = envelope.payload else {
        unreachable!()
    };
    assert_eq!(
        snapshot.system.brightness.status,
        CapabilityAvailability::Available
    );
    assert_eq!(
        snapshot.system.night_light.status,
        CapabilityAvailability::Available
    );
    assert_eq!(
        snapshot.utilities.screenshot.status,
        CapabilityAvailability::Available
    );
    assert_eq!(
        snapshot.utilities.color_picker.status,
        CapabilityAvailability::Available
    );
    assert!(
        !snapshot
            .compositor
            .hyprland
            .data
            .as_ref()
            .unwrap()
            .action_capabilities
            .toggle_fullscreen
    );
    assert!(
        !snapshot
            .compositor
            .hyprland
            .data
            .as_ref()
            .unwrap()
            .action_capabilities
            .toggle_group
    );
}

#[tokio::test]
async fn utility_subproducer_terminal_state_does_not_mask_siblings() {
    let registry = DesktopRegistry::new(complete_registry_with(Some((
        DesktopDomainId::Screenshot,
        producer(
            DesktopDomainId::Screenshot,
            DesktopDomainState::terminal(
                DesktopDomainId::Screenshot,
                CapabilityAvailability::PermissionDenied,
                "capture.permission-denied",
            )
            .unwrap(),
        ),
    ))))
    .unwrap();
    let states = registry.initial_states().await;
    let snapshot = registry.assemble(&states).unwrap();

    assert_eq!(
        snapshot.utilities.screenshot.status,
        CapabilityAvailability::PermissionDenied
    );
    assert_eq!(
        snapshot.utilities.color_picker.status,
        CapabilityAvailability::Unsupported
    );
    assert_eq!(
        snapshot.utilities.tray_items.status,
        CapabilityAvailability::Unsupported
    );
}

#[test]
fn available_state_rejects_a_value_owned_by_another_domain() {
    for domain in DesktopDomainId::ALL {
        let other = DesktopDomainId::ALL
            .into_iter()
            .find(|candidate| *candidate != domain)
            .unwrap();
        let error =
            DesktopDomainState::available(domain, DesktopDomainValue::empty(other)).unwrap_err();
        assert_eq!(error.kind(), io::ErrorKind::InvalidInput, "{domain:?}");
    }
}

#[tokio::test]
async fn desktop_authority_replays_one_full_snapshot_then_monotonic_domain_updates() {
    let temp = tempfile::tempdir().unwrap();
    let authority = DesktopStateAuthority::open(
        available_registry(),
        temp.path().join("desktop-generation"),
        8,
    )
    .await
    .unwrap();
    let initial = authority.initialize().await.unwrap();
    let mut subscriber = authority.subscribe().await.unwrap();
    assert_eq!(subscriber.recv().await.unwrap(), initial);
    assert!(matches!(initial.payload, DesktopEvent::FullSnapshot(_)));

    let update = authority
        .publish_domain(
            DesktopDomainState::available(
                DesktopDomainId::Network,
                DesktopDomainValue::Network(NetworkSnapshot {
                    wifi_enabled: true,
                    scanning: false,
                    access_points: Vec::new(),
                    connections: Vec::new(),
                }),
            )
            .unwrap(),
            EventCause {
                kind: EventCauseKind::External,
                request_id: None,
            },
        )
        .await
        .unwrap();
    assert!(update.generation > initial.generation);
    assert_eq!(subscriber.recv().await.unwrap(), update);
    assert!(matches!(
        update.payload,
        DesktopEvent::DomainUpdate(SdkDomainUpdate::System(DesktopSystemUpdate::Network(_)))
    ));

    let mut reconnect = authority.subscribe().await.unwrap();
    let replay = reconnect.recv().await.unwrap();
    assert_eq!(replay.generation, update.generation);
    assert_eq!(replay.cause.kind, EventCauseKind::Replay);
    assert!(matches!(replay.payload, DesktopEvent::FullSnapshot(_)));
}

#[tokio::test]
async fn concurrent_subscription_and_publication_never_duplicate_one_generation() {
    let temp = tempfile::tempdir().unwrap();
    let authority = DesktopStateAuthority::open(
        available_registry(),
        temp.path().join("desktop-generation"),
        64,
    )
    .await
    .unwrap();
    authority.initialize().await.unwrap();

    for index in 0..32 {
        let subscriber_authority = Arc::clone(&authority);
        let subscriber =
            tokio::spawn(async move { subscriber_authority.subscribe().await.unwrap() });
        let publish_authority = Arc::clone(&authority);
        let publication = tokio::spawn(async move {
            publish_authority
                .publish_domain(
                    DesktopDomainState::available(
                        DesktopDomainId::GameMode,
                        DesktopDomainValue::GameMode(index % 2 == 0),
                    )
                    .unwrap(),
                    EventCause {
                        kind: EventCauseKind::External,
                        request_id: None,
                    },
                )
                .await
                .unwrap()
        });
        let mut subscriber = subscriber.await.unwrap();
        let published = publication.await.unwrap();
        let replay = subscriber.recv().await.unwrap();
        assert!(replay.generation <= published.generation);
        if replay.generation < published.generation {
            assert_eq!(
                subscriber.recv().await.unwrap().generation,
                published.generation
            );
        } else {
            assert!(
                tokio::time::timeout(Duration::from_millis(5), subscriber.recv())
                    .await
                    .is_err()
            );
        }
    }
}

#[tokio::test(start_paused = true)]
async fn producer_runtime_reconnects_after_transient_failure_and_cancels_backoff() {
    let calls = Arc::new(AtomicUsize::new(0));
    let reconnecting: Arc<dyn DesktopProducer> = Arc::new(ReconnectingProducer {
        domain: DesktopDomainId::Network,
        calls: Arc::clone(&calls),
    });
    let registry = Arc::new(
        DesktopRegistry::new(complete_registry_with(Some((
            DesktopDomainId::Network,
            reconnecting,
        ))))
        .unwrap(),
    );
    let temp = tempfile::tempdir().unwrap();
    let authority =
        DesktopStateAuthority::open(Arc::clone(&registry), temp.path().join("generation"), 16)
            .await
            .unwrap();
    authority.initialize().await.unwrap();
    let runtime = registry.start(authority, 16).unwrap();
    tokio::task::yield_now().await;
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    tokio::time::advance(Duration::from_millis(100)).await;
    tokio::task::yield_now().await;
    assert!(calls.load(Ordering::SeqCst) >= 2);
    runtime.shutdown(Duration::from_secs(1)).await.unwrap();
}

#[tokio::test(start_paused = true)]
async fn producer_runtime_restarts_panics_and_gathers_all_owned_work() {
    let drained = Arc::new(AtomicUsize::new(0));
    let panic_calls = Arc::new(AtomicUsize::new(0));
    let mut producers = complete_registry_with(Some((
        DesktopDomainId::Network,
        Arc::new(PanickingProducer {
            domain: DesktopDomainId::Network,
            calls: Arc::clone(&panic_calls),
        }),
    )));
    *producers
        .iter_mut()
        .find(|producer| producer.domain() == DesktopDomainId::Bluetooth)
        .unwrap() = Arc::new(DrainTrackingProducer {
        domain: DesktopDomainId::Bluetooth,
        drained: Arc::clone(&drained),
    });
    let registry = Arc::new(DesktopRegistry::new(producers).unwrap());
    let temp = tempfile::tempdir().unwrap();
    let authority =
        DesktopStateAuthority::open(Arc::clone(&registry), temp.path().join("generation"), 16)
            .await
            .unwrap();
    authority.initialize().await.unwrap();
    let mut events = authority.subscribe().await.unwrap();
    events.recv().await.unwrap();
    let runtime = registry.start(authority, 16).unwrap();
    tokio::task::yield_now().await;

    let terminal = events.recv().await.unwrap();
    assert!(matches!(
        terminal.payload,
        DesktopEvent::DomainUpdate(SdkDomainUpdate::System(DesktopSystemUpdate::Network(
            ref state
        ))) if state.status == CapabilityAvailability::Error
    ));
    tokio::time::advance(Duration::from_millis(100)).await;
    tokio::task::yield_now().await;
    assert!(panic_calls.load(Ordering::SeqCst) >= 2);
    runtime.shutdown(Duration::from_secs(1)).await.unwrap();
    assert_eq!(drained.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn runtime_timeout_is_bounded_and_reaps_a_production_command_child() {
    let temp = tempfile::tempdir().unwrap();
    let pid_path = Arc::new(temp.path().join("producer-child.pid"));
    let core = Arc::new(
        CoreSystemProducer::new(
            DesktopDomainId::Network,
            Arc::new(SystemFacade::new(BlockingProcessRunner {
                pid_path: Arc::clone(&pid_path),
            })),
        )
        .unwrap(),
    ) as Arc<dyn DesktopProducer>;
    let uncooperative = Arc::new(UncooperativeAsyncProducer(DesktopDomainId::Bluetooth))
        as Arc<dyn DesktopProducer>;
    let registry = Arc::new(
        DesktopRegistry::new(
            DesktopDomainId::ALL
                .into_iter()
                .map(|domain| match domain {
                    DesktopDomainId::Network => Arc::clone(&core),
                    DesktopDomainId::Bluetooth => Arc::clone(&uncooperative),
                    _ => producer(
                        domain,
                        DesktopDomainState::terminal(
                            domain,
                            CapabilityAvailability::Unsupported,
                            "fixture unsupported",
                        )
                        .unwrap(),
                    ),
                })
                .collect(),
        )
        .unwrap(),
    );
    let authority =
        DesktopStateAuthority::open(available_registry(), temp.path().join("generation"), 16)
            .await
            .unwrap();
    authority.initialize().await.unwrap();
    let runtime = registry.start(authority, 16).unwrap();
    let child_start_deadline = Instant::now() + Duration::from_secs(3);
    while !pid_path.exists() {
        assert!(
            Instant::now() < child_start_deadline,
            "producer child did not start"
        );
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    let child_pid = std::fs::read_to_string(pid_path.as_ref())
        .unwrap()
        .parse::<libc::pid_t>()
        .unwrap();

    let before = Instant::now();
    let result = tokio::time::timeout(
        Duration::from_millis(350),
        runtime.shutdown(Duration::from_millis(100)),
    )
    .await
    .expect("desktop producer shutdown exceeded its hard deadline");

    assert_eq!(result.unwrap_err().kind(), io::ErrorKind::TimedOut);
    assert!(before.elapsed() < Duration::from_millis(250));
    assert_eq!(unsafe { libc::kill(child_pid, 0) }, -1);
    assert_eq!(io::Error::last_os_error().raw_os_error(), Some(libc::ESRCH));
}

#[tokio::test]
async fn shutdown_sets_the_registered_blocking_control_before_an_immediate_attempt_abort() {
    let started = Arc::new(AtomicBool::new(false));
    let finished = Arc::new(AtomicBool::new(false));
    let test_release = Arc::new(AtomicBool::new(false));
    let core = Arc::new(RegisteredBlockingProducer {
        domain: DesktopDomainId::Network,
        started: Arc::clone(&started),
        finished: Arc::clone(&finished),
        test_release: Arc::clone(&test_release),
    }) as Arc<dyn DesktopProducer>;
    let registry = Arc::new(
        DesktopRegistry::new(complete_registry_with(Some((
            DesktopDomainId::Network,
            core,
        ))))
        .unwrap(),
    );
    let temp = tempfile::tempdir().unwrap();
    let authority =
        DesktopStateAuthority::open(available_registry(), temp.path().join("generation"), 16)
            .await
            .unwrap();
    authority.initialize().await.unwrap();
    let runtime = registry.start(authority, 16).unwrap();
    let start_deadline = Instant::now() + Duration::from_secs(3);
    while !started.load(Ordering::SeqCst) {
        assert!(
            Instant::now() < start_deadline,
            "blocking producer seam did not start"
        );
        tokio::time::sleep(Duration::from_millis(1)).await;
    }

    let shutdown =
        tokio::time::timeout(Duration::from_millis(350), runtime.shutdown(Duration::ZERO)).await;
    let finished_before_test_release = finished.load(Ordering::SeqCst);
    test_release.store(true, Ordering::SeqCst);
    let cleanup_deadline = Instant::now() + Duration::from_secs(1);
    while !finished.load(Ordering::SeqCst) && Instant::now() < cleanup_deadline {
        tokio::time::sleep(Duration::from_millis(1)).await;
    }

    assert!(
        shutdown.is_ok(),
        "shutdown exceeded its fixed reap tolerance"
    );
    assert!(
        finished_before_test_release,
        "runtime abort detached the registered blocking producer worker"
    );
}

#[tokio::test]
async fn process_cancellation_kills_the_pipe_owning_group_and_reaps_descendant_output() {
    let temp = tempfile::tempdir().unwrap();
    let parent_pid_path = Arc::new(temp.path().join("producer-parent.pid"));
    let descendant_pid_path = Arc::new(temp.path().join("producer-descendant.pid"));
    let core = Arc::new(
        CoreSystemProducer::new(
            DesktopDomainId::Network,
            Arc::new(SystemFacade::new(DescendantPipeRunner {
                parent_pid_path: Arc::clone(&parent_pid_path),
                descendant_pid_path: Arc::clone(&descendant_pid_path),
            })),
        )
        .unwrap(),
    ) as Arc<dyn DesktopProducer>;
    let registry = Arc::new(
        DesktopRegistry::new(complete_registry_with(Some((
            DesktopDomainId::Network,
            core,
        ))))
        .unwrap(),
    );
    let authority =
        DesktopStateAuthority::open(available_registry(), temp.path().join("generation"), 16)
            .await
            .unwrap();
    authority.initialize().await.unwrap();
    let runtime = registry.start(authority, 16).unwrap();
    let child_start_deadline = Instant::now() + Duration::from_secs(3);
    while !parent_pid_path.exists() || !descendant_pid_path.exists() {
        assert!(
            Instant::now() < child_start_deadline,
            "pipe-owning process group did not start"
        );
        tokio::time::sleep(Duration::from_millis(1)).await;
    }
    let parent_pid = std::fs::read_to_string(parent_pid_path.as_ref())
        .unwrap()
        .parse::<libc::pid_t>()
        .unwrap();
    let descendant_pid = std::fs::read_to_string(descendant_pid_path.as_ref())
        .unwrap()
        .parse::<libc::pid_t>()
        .unwrap();

    let shutdown = tokio::time::timeout(
        Duration::from_millis(400),
        runtime.shutdown(Duration::from_millis(100)),
    )
    .await;
    let parent_alive = unsafe { libc::kill(parent_pid, 0) } == 0;
    let descendant_alive = unsafe { libc::kill(descendant_pid, 0) } == 0;
    if descendant_alive {
        unsafe {
            libc::kill(descendant_pid, libc::SIGKILL);
        }
    }

    assert!(!parent_alive, "direct command child was not reaped");
    assert!(
        !descendant_alive,
        "pipe-owning command descendant survived cancellation"
    );
    assert!(shutdown.is_ok(), "shutdown hung on descendant-held pipes");
    assert!(shutdown.unwrap().is_ok());
}

#[tokio::test]
async fn process_cancellation_reaps_an_escaped_descendant_without_reaping_unrelated_children() {
    let temp = tempfile::tempdir().unwrap();
    let parent_pid_path = Arc::new(temp.path().join("producer-parent.pid"));
    let descendant_pid_path = Arc::new(temp.path().join("producer-escaped.pid"));
    let core = Arc::new(
        CoreSystemProducer::new(
            DesktopDomainId::Network,
            Arc::new(SystemFacade::new(EscapedDescendantRunner {
                parent_pid_path: Arc::clone(&parent_pid_path),
                descendant_pid_path: Arc::clone(&descendant_pid_path),
            })),
        )
        .unwrap(),
    ) as Arc<dyn DesktopProducer>;
    let registry = Arc::new(
        DesktopRegistry::new(complete_registry_with(Some((
            DesktopDomainId::Network,
            core,
        ))))
        .unwrap(),
    );
    let authority =
        DesktopStateAuthority::open(available_registry(), temp.path().join("generation"), 16)
            .await
            .unwrap();
    authority.initialize().await.unwrap();
    let runtime = registry.start(authority, 16).unwrap();
    let child_start_deadline = Instant::now() + Duration::from_secs(3);
    while !parent_pid_path.exists() || !descendant_pid_path.exists() {
        assert!(
            Instant::now() < child_start_deadline,
            "escaped process tree did not start"
        );
        tokio::time::sleep(Duration::from_millis(1)).await;
    }
    let parent_pid = fs::read_to_string(parent_pid_path.as_ref())
        .unwrap()
        .parse::<libc::pid_t>()
        .unwrap();
    let descendant_pid = fs::read_to_string(descendant_pid_path.as_ref())
        .unwrap()
        .parse::<libc::pid_t>()
        .unwrap();
    let mut unrelated = Command::new("sh")
        .args(["-c", "exit 0"])
        .spawn()
        .expect("unrelated daemon child should start");
    tokio::time::sleep(Duration::from_millis(20)).await;

    let shutdown = tokio::time::timeout(
        Duration::from_millis(500),
        runtime.shutdown(Duration::from_millis(100)),
    )
    .await;
    let parent_alive = unsafe { libc::kill(parent_pid, 0) } == 0;
    let descendant_alive = unsafe { libc::kill(descendant_pid, 0) } == 0;
    if descendant_alive {
        unsafe {
            libc::kill(descendant_pid, libc::SIGKILL);
        }
    }
    let unrelated_status = unrelated
        .wait()
        .expect("runner reaped an unrelated daemon child");

    assert!(!parent_alive, "direct command child was not reaped");
    assert!(
        !descendant_alive,
        "session-escaped command descendant survived cancellation"
    );
    assert!(unrelated_status.success());
    assert!(shutdown.is_ok(), "shutdown exceeded its ownership bound");
    assert!(shutdown.unwrap().is_ok());
}

#[test]
fn process_runner_reaps_a_fast_double_forked_setsid_descendant() {
    let temp = tempfile::tempdir().unwrap();
    let descendant_pid_path = Arc::new(temp.path().join("fast-escaped.pid"));
    let runner = FastEscapedDescendantRunner {
        descendant_pid_path: Arc::clone(&descendant_pid_path),
    };
    let command = CommandSpec::new("unused", std::iter::empty::<String>());

    let output = runner.run(&command).unwrap();
    assert_eq!(output.status, 0);

    let deadline = Instant::now() + Duration::from_secs(1);
    while !descendant_pid_path.exists() && Instant::now() < deadline {
        std::thread::sleep(Duration::from_millis(1));
    }
    let descendant_pid = fs::read_to_string(descendant_pid_path.as_ref())
        .unwrap()
        .parse::<libc::pid_t>()
        .unwrap();
    let descendant_alive = unsafe { libc::kill(descendant_pid, 0) } == 0;
    if descendant_alive {
        unsafe {
            libc::kill(descendant_pid, libc::SIGKILL);
        }
    }

    assert!(
        !descendant_alive,
        "fast double-forked session descendant survived command completion"
    );
}

#[test]
fn process_runner_reaps_a_late_exiting_escaped_descendant_before_return() {
    let temp = tempfile::tempdir().unwrap();
    let descendant_pid_path = Arc::new(temp.path().join("delayed-escaped.pid"));
    let runner = DelayedZombieDescendantRunner {
        descendant_pid_path: Arc::clone(&descendant_pid_path),
    };
    let command = CommandSpec::new("unused", std::iter::empty::<String>());

    let output = runner.run(&command).unwrap();
    assert_eq!(output.status, 0);

    let deadline = Instant::now() + Duration::from_secs(1);
    while !descendant_pid_path.exists() && Instant::now() < deadline {
        std::thread::sleep(Duration::from_millis(1));
    }
    let descendant_pid = fs::read_to_string(descendant_pid_path.as_ref())
        .unwrap()
        .parse::<libc::pid_t>()
        .unwrap();
    std::thread::sleep(Duration::from_millis(180));
    let descendant_alive_or_zombie = unsafe { libc::kill(descendant_pid, 0) } == 0;
    if descendant_alive_or_zombie {
        let mut status = 0;
        unsafe {
            libc::waitpid(descendant_pid, &mut status, libc::WNOHANG);
        }
    }

    assert!(
        !descendant_alive_or_zombie,
        "late-exiting escaped command descendant was not reaped before runner return"
    );
}

#[tokio::test]
async fn idle_poll_sampled_before_a_confirmed_mutation_cannot_publish_after_it() {
    let sampled = Arc::new(AtomicBool::new(false));
    let release = Arc::new(tokio::sync::Notify::new());
    let enqueued = Arc::new(AtomicBool::new(false));
    let idle = Arc::new(PausedIdleObservationProducer {
        sampled: Arc::clone(&sampled),
        release: Arc::clone(&release),
        enqueued: Arc::clone(&enqueued),
    }) as Arc<dyn DesktopProducer>;
    let registry = Arc::new(
        DesktopRegistry::new(complete_registry_with(Some((
            DesktopDomainId::IdleInhibit,
            idle,
        ))))
        .unwrap(),
    );
    let temp = tempfile::tempdir().unwrap();
    let authority =
        DesktopStateAuthority::open(available_registry(), temp.path().join("generation"), 16)
            .await
            .unwrap();
    authority.initialize().await.unwrap();
    let control = DesktopControlAuthority::open(
        Arc::clone(&authority),
        Arc::new(IdleReadbackExecutor),
        temp.path().join("dedupe.json"),
        8,
    )
    .await
    .unwrap();
    let mut events = authority.subscribe().await.unwrap();
    events.recv().await.unwrap();
    let runtime = registry.start(Arc::clone(&authority), 16).unwrap();
    let sample_deadline = Instant::now() + Duration::from_secs(1);
    while !sampled.load(Ordering::SeqCst) {
        assert!(
            Instant::now() < sample_deadline,
            "idle poll did not reach the paused availability seam"
        );
        tokio::task::yield_now().await;
    }

    let request = DesktopRequest {
        schema_version: DESKTOP_WIRE_VERSION,
        request_id: "00000000-0000-4000-8000-000000000100".into(),
        expected_generation: authority.current_generation(),
        command: DesktopCommand::Utility(UtilityCommand::SetIdleInhibited { enabled: true }),
    };
    let result = control
        .handle_json(&serde_json::to_string(&request).unwrap())
        .await
        .unwrap();
    assert_eq!(result.status, DesktopResultStatus::Succeeded);
    let readback = events.recv().await.unwrap();
    assert!(matches!(
        readback.payload,
        DesktopEvent::DomainUpdate(SdkDomainUpdate::Utilities(
            DesktopUtilityUpdate::IdleInhibited(ref state)
        )) if state.data == Some(true)
    ));
    assert!(matches!(
        events.recv().await.unwrap().payload,
        DesktopEvent::CommandResult(_)
    ));

    release.notify_one();
    let enqueue_deadline = Instant::now() + Duration::from_secs(1);
    while !enqueued.load(Ordering::SeqCst) {
        assert!(
            Instant::now() < enqueue_deadline,
            "older idle observation was not enqueued"
        );
        tokio::task::yield_now().await;
    }
    assert!(
        tokio::time::timeout(Duration::from_millis(100), events.recv())
            .await
            .is_err(),
        "older idle observation published after the confirmed mutation"
    );
    assert_eq!(authority.current_generation(), result.generation);
    runtime.shutdown(Duration::from_secs(1)).await.unwrap();
}

#[tokio::test]
async fn launcher_search_sampled_before_a_confirmed_launch_cannot_publish_after_it() {
    let sampled = Arc::new(AtomicBool::new(false));
    let release = Arc::new(tokio::sync::Notify::new());
    let enqueued = Arc::new(AtomicBool::new(false));
    let launcher = Arc::new(PausedLauncherObservationProducer {
        sampled: Arc::clone(&sampled),
        release: Arc::clone(&release),
        enqueued: Arc::clone(&enqueued),
    }) as Arc<dyn DesktopProducer>;
    let registry = Arc::new(
        DesktopRegistry::new(complete_registry_with(Some((
            DesktopDomainId::Launcher,
            launcher,
        ))))
        .unwrap(),
    );
    let temp = tempfile::tempdir().unwrap();
    let authority =
        DesktopStateAuthority::open(available_registry(), temp.path().join("generation"), 16)
            .await
            .unwrap();
    authority.initialize().await.unwrap();
    let control = DesktopControlAuthority::open(
        Arc::clone(&authority),
        Arc::new(LauncherReadbackExecutor),
        temp.path().join("dedupe.json"),
        8,
    )
    .await
    .unwrap();
    let mut events = authority.subscribe().await.unwrap();
    events.recv().await.unwrap();
    let runtime = registry.start(Arc::clone(&authority), 16).unwrap();
    let sample_deadline = Instant::now() + Duration::from_secs(1);
    while !sampled.load(Ordering::SeqCst) {
        assert!(
            Instant::now() < sample_deadline,
            "launcher poll did not reach the paused search seam"
        );
        tokio::task::yield_now().await;
    }

    let request = DesktopRequest {
        schema_version: DESKTOP_WIRE_VERSION,
        request_id: "00000000-0000-4000-8000-000000000101".into(),
        expected_generation: authority.current_generation(),
        command: DesktopCommand::Launcher(LauncherCommand::Launch(
            serde_json::from_value(serde_json::json!({
                "schemaVersion": WIRE_SCHEMA_VERSION,
                "desktopId": "sleepy-test.desktop",
                "resources": []
            }))
            .unwrap(),
        )),
    };
    let result = control
        .handle_json(&serde_json::to_string(&request).unwrap())
        .await
        .unwrap();
    assert_eq!(result.status, DesktopResultStatus::Succeeded);
    let readback = events.recv().await.unwrap();
    assert!(matches!(
        readback.payload,
        DesktopEvent::DomainUpdate(SdkDomainUpdate::Launcher(_))
    ));
    assert!(matches!(
        events.recv().await.unwrap().payload,
        DesktopEvent::CommandResult(_)
    ));

    release.notify_one();
    let enqueue_deadline = Instant::now() + Duration::from_secs(1);
    while !enqueued.load(Ordering::SeqCst) {
        assert!(
            Instant::now() < enqueue_deadline,
            "older launcher observation was not enqueued"
        );
        tokio::task::yield_now().await;
    }
    assert!(
        tokio::time::timeout(Duration::from_millis(100), events.recv())
            .await
            .is_err(),
        "older launcher search observation published after the confirmed launch"
    );
    assert_eq!(authority.current_generation(), result.generation);
    runtime.shutdown(Duration::from_secs(1)).await.unwrap();
}

#[test]
fn real_clipboard_hanging_child_is_cancelled_and_reaped_before_runtime_shutdown_returns() {
    let temp = tempfile::tempdir().unwrap();
    let bin = temp.path().join("bin");
    fs::create_dir(&bin).unwrap();
    let cliphist = bin.join("cliphist");
    fs::write(
        &cliphist,
        "#!/bin/sh\nprintf '%s' \"$$\" > \"$SLEEPY_TEST_CLIPHIST_PID\"\nexec sleep 30\n",
    )
    .unwrap();
    fs::set_permissions(&cliphist, fs::Permissions::from_mode(0o700)).unwrap();
    let pid_path = temp.path().join("cliphist.pid");
    let original_path = std::env::var_os("PATH").unwrap_or_default();
    let command_path = std::env::join_paths(
        std::iter::once(bin.clone()).chain(std::env::split_paths(&original_path)),
    )
    .unwrap();
    let output = Command::new(std::env::current_exe().unwrap())
        .args([
            "--exact",
            "clipboard_runtime_shutdown_helper",
            "--nocapture",
        ])
        .env("PATH", command_path)
        .env("SLEEPY_TEST_CLIPHIST_PID", &pid_path)
        .output()
        .unwrap();
    if let Ok(pid) = fs::read_to_string(&pid_path)
        .and_then(|value| value.parse::<libc::pid_t>().map_err(io::Error::other))
    {
        if unsafe { libc::kill(pid, 0) } == 0 {
            unsafe {
                libc::kill(pid, libc::SIGKILL);
            }
        }
    }
    assert!(
        output.status.success(),
        "clipboard shutdown helper failed:\n{}\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
}

#[test]
fn clipboard_runtime_shutdown_helper() {
    let Some(pid_path) = std::env::var_os("SLEEPY_TEST_CLIPHIST_PID") else {
        return;
    };
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .unwrap();
    runtime.block_on(async move {
        let temp = tempfile::tempdir().unwrap();
        let service = Arc::new(
            ProductionUtilityService::open(temp.path().join("captures"))
                .expect("production utility service should open"),
        );
        let clipboard = Arc::new(
            UtilityProducer::new(DesktopDomainId::Clipboard, service)
                .expect("clipboard producer should construct"),
        ) as Arc<dyn DesktopProducer>;
        let registry = Arc::new(
            DesktopRegistry::new(complete_registry_with(Some((
                DesktopDomainId::Clipboard,
                clipboard,
            ))))
            .unwrap(),
        );
        let authority =
            DesktopStateAuthority::open(available_registry(), temp.path().join("generation"), 16)
                .await
                .unwrap();
        authority.initialize().await.unwrap();
        let producer_runtime = registry.start(authority, 16).unwrap();
        let start_deadline = Instant::now() + Duration::from_secs(4);
        while !PathBuf::from(&pid_path).exists() {
            assert!(
                Instant::now() < start_deadline,
                "real clipboard child did not start"
            );
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        let pid = fs::read_to_string(&pid_path)
            .unwrap()
            .parse::<libc::pid_t>()
            .unwrap();

        let started = Instant::now();
        let shutdown = tokio::time::timeout(
            Duration::from_millis(4_250),
            producer_runtime.shutdown(Duration::from_secs(4)),
        )
        .await
        .expect("clipboard runtime exceeded its one shutdown deadline plus tolerance");

        assert!(
            shutdown.is_ok(),
            "production clipboard worker did not drain"
        );
        assert!(started.elapsed() < Duration::from_millis(4_150));
        assert_eq!(unsafe { libc::kill(pid, 0) }, -1);
        assert_eq!(io::Error::last_os_error().raw_os_error(), Some(libc::ESRCH));
    });
}

#[tokio::test]
async fn shutdown_drains_an_in_flight_publication_before_it_can_advance_after_return() {
    let temp = tempfile::tempdir().unwrap();
    let generation_path = temp.path().join("generation");
    let authority = DesktopStateAuthority::open(available_registry(), &generation_path, 16)
        .await
        .unwrap();
    authority.initialize().await.unwrap();
    for enabled in (0..63).map(|index| index % 2 == 0) {
        authority
            .publish_domain(
                DesktopDomainState::available(
                    DesktopDomainId::GameMode,
                    DesktopDomainValue::GameMode(enabled),
                )
                .unwrap(),
                EventCause {
                    kind: EventCauseKind::External,
                    request_id: None,
                },
            )
            .await
            .unwrap();
    }
    let baseline_generation = authority.current_generation();
    let generation_lock = OpenOptions::new()
        .read(true)
        .write(true)
        .open(temp.path().join("generation.lock"))
        .unwrap();
    FileExt::lock_exclusive(&generation_lock).unwrap();

    let update_sent = Arc::new(AtomicBool::new(false));
    let updating = Arc::new(SingleUpdateProducer {
        domain: DesktopDomainId::Network,
        update_sent: Arc::clone(&update_sent),
    }) as Arc<dyn DesktopProducer>;
    let registry = Arc::new(
        DesktopRegistry::new(complete_registry_with(Some((
            DesktopDomainId::Network,
            updating,
        ))))
        .unwrap(),
    );
    let runtime = registry.start(Arc::clone(&authority), 16).unwrap();
    let update_deadline = Instant::now() + Duration::from_secs(3);
    while !update_sent.load(Ordering::SeqCst) {
        assert!(
            Instant::now() < update_deadline,
            "publication fixture did not enqueue its update"
        );
        tokio::time::sleep(Duration::from_millis(1)).await;
    }
    tokio::time::sleep(Duration::from_millis(25)).await;

    let shutdown_started = Instant::now();
    let shutdown = tokio::time::timeout(
        Duration::from_millis(400),
        runtime.shutdown(Duration::from_millis(100)),
    )
    .await;
    let shutdown_elapsed = shutdown_started.elapsed();
    FileExt::unlock(&generation_lock).unwrap();
    tokio::time::sleep(Duration::from_millis(100)).await;
    let generation_after_return = authority.current_generation();

    assert_eq!(generation_after_return, baseline_generation);
    // A synchronous cancellation wait can starve Tokio's timeout itself.
    assert!(
        shutdown_elapsed < Duration::from_millis(400),
        "publication shutdown blocked the executor for {shutdown_elapsed:?}"
    );
    assert!(
        shutdown.is_ok(),
        "publication shutdown exceeded its hard bound"
    );
    assert!(shutdown.unwrap().is_ok());
}

#[tokio::test]
async fn appearance_poll_contention_cannot_overwrite_a_confirmed_authority_readback() {
    use sleepy_session::desktop::appearance::AppearanceService;
    use sleepy_session::theme::ThemeManager;

    let temp = tempfile::tempdir().unwrap();
    let config = temp.path().join("config");
    let state = temp.path().join("state");
    let manager = Arc::new(tokio::sync::Mutex::new(
        ThemeManager::open(&config, &state).unwrap(),
    ));
    let service = Arc::new(AppearanceService::open(Arc::clone(&manager), &state).unwrap());
    let appearance = Arc::new(AppearanceProducer::new(service)) as Arc<dyn DesktopProducer>;
    let registry = Arc::new(
        DesktopRegistry::new(complete_registry_with(Some((
            DesktopDomainId::Appearance,
            appearance,
        ))))
        .unwrap(),
    );
    let authority =
        DesktopStateAuthority::open(available_registry(), temp.path().join("generation"), 16)
            .await
            .unwrap();
    authority.initialize().await.unwrap();
    let mut events = authority.subscribe().await.unwrap();
    events.recv().await.unwrap();
    let held_theme_lock = manager.lock().await;
    let runtime = registry.start(Arc::clone(&authority), 16).unwrap();
    tokio::time::sleep(Duration::from_millis(1_850)).await;
    let confirmed = authority
        .publish_domain(
            DesktopDomainState::available(
                DesktopDomainId::Appearance,
                DesktopDomainValue::empty(DesktopDomainId::Appearance),
            )
            .unwrap(),
            EventCause {
                kind: EventCauseKind::Request,
                request_id: Some("00000000-0000-4000-8000-000000000099".into()),
            },
        )
        .await
        .unwrap();
    assert_eq!(events.recv().await.unwrap(), confirmed);
    tokio::time::sleep(Duration::from_millis(250)).await;
    let contention_update = tokio::time::timeout(Duration::from_millis(25), events.recv()).await;
    let generation_after_poll = authority.current_generation();
    drop(held_theme_lock);
    runtime.shutdown(Duration::from_secs(1)).await.unwrap();

    assert!(
        contention_update.is_err(),
        "periodic lock contention emitted an authoritative update"
    );
    assert_eq!(generation_after_poll, confirmed.generation);
}

#[tokio::test]
async fn appearance_poll_cannot_leave_a_blocking_worker_past_bounded_shutdown() {
    use sleepy_session::desktop::appearance::AppearanceService;
    use sleepy_session::theme::ThemeManager;

    let temp = tempfile::tempdir().unwrap();
    let config = temp.path().join("config");
    let state = temp.path().join("state");
    let manager = Arc::new(tokio::sync::Mutex::new(
        ThemeManager::open(&config, &state).unwrap(),
    ));
    let service = Arc::new(AppearanceService::open(Arc::clone(&manager), &state).unwrap());
    let appearance = Arc::new(AppearanceProducer::new(service)) as Arc<dyn DesktopProducer>;
    let registry = Arc::new(
        DesktopRegistry::new(complete_registry_with(Some((
            DesktopDomainId::Appearance,
            appearance,
        ))))
        .unwrap(),
    );
    let authority =
        DesktopStateAuthority::open(available_registry(), temp.path().join("generation"), 16)
            .await
            .unwrap();
    authority.initialize().await.unwrap();
    let held_theme_lock = manager.lock().await;
    let baseline_owners = Arc::strong_count(&manager);
    let runtime = registry.start(authority, 16).unwrap();
    tokio::time::sleep(Duration::from_millis(2_050)).await;

    let started = Instant::now();
    let result = runtime.shutdown(Duration::from_millis(100)).await;

    assert!(result.is_ok());
    assert!(started.elapsed() < Duration::from_millis(250));
    assert_eq!(Arc::strong_count(&manager), baseline_owners);
    drop(held_theme_lock);
}

#[tokio::test]
async fn a_lagged_stream_is_disconnected_and_reconnect_gets_the_latest_snapshot() {
    let temp = tempfile::tempdir().unwrap();
    let authority = DesktopStateAuthority::open(
        available_registry(),
        temp.path().join("desktop-generation"),
        1,
    )
    .await
    .unwrap();
    authority.initialize().await.unwrap();
    let mut subscriber = authority.subscribe().await.unwrap();
    subscriber.recv().await.unwrap();

    for enabled in [true, false] {
        authority
            .publish_domain(
                DesktopDomainState::available(
                    DesktopDomainId::Network,
                    DesktopDomainValue::Network(NetworkSnapshot {
                        wifi_enabled: enabled,
                        scanning: false,
                        access_points: Vec::new(),
                        connections: Vec::new(),
                    }),
                )
                .unwrap(),
                EventCause {
                    kind: EventCauseKind::External,
                    request_id: None,
                },
            )
            .await
            .unwrap();
    }
    assert_eq!(
        subscriber.recv().await.unwrap_err().kind(),
        io::ErrorKind::InvalidData
    );

    let current = authority.current_generation();
    let mut reconnect = authority.subscribe().await.unwrap();
    let replay = reconnect.recv().await.unwrap();
    assert_eq!(replay.generation, current);
    assert!(matches!(replay.payload, DesktopEvent::FullSnapshot(_)));
}

#[tokio::test]
async fn invalid_domain_data_degrades_only_its_owner_before_any_publication() {
    let temp = tempfile::tempdir().unwrap();
    let authority = DesktopStateAuthority::open(
        available_registry(),
        temp.path().join("desktop-generation"),
        8,
    )
    .await
    .unwrap();
    authority.initialize().await.unwrap();
    let mut subscriber = authority.subscribe().await.unwrap();
    subscriber.recv().await.unwrap();

    let duplicate = NetworkAccessPoint {
        id: "duplicate".into(),
        ssid: "fixture".into(),
        signal_level: 0.5,
        secured: false,
    };
    let event = authority
        .publish_domain(
            DesktopDomainState::available(
                DesktopDomainId::Network,
                DesktopDomainValue::Network(NetworkSnapshot {
                    wifi_enabled: true,
                    scanning: false,
                    access_points: vec![duplicate.clone(), duplicate],
                    connections: Vec::new(),
                }),
            )
            .unwrap(),
            EventCause {
                kind: EventCauseKind::External,
                request_id: None,
            },
        )
        .await
        .unwrap();
    validate_desktop_envelope(&serde_json::to_string(&event).unwrap()).unwrap();
    match event.payload {
        DesktopEvent::DomainUpdate(SdkDomainUpdate::System(DesktopSystemUpdate::Network(
            capability,
        ))) => {
            assert_eq!(capability.status, CapabilityAvailability::Parse);
            assert!(capability.data.is_none());
        }
        other => panic!("unexpected localized update: {other:?}"),
    }
}

#[tokio::test]
async fn desktop_generation_never_reuses_a_value_after_authority_restart() {
    let temp = tempfile::tempdir().unwrap();
    let path = temp.path().join("desktop-generation");
    let first = DesktopStateAuthority::open(available_registry(), &path, 8)
        .await
        .unwrap();
    let first_generation = first.initialize().await.unwrap().generation;
    drop(first);

    let second = DesktopStateAuthority::open(available_registry(), &path, 8)
        .await
        .unwrap();
    let second_generation = second.initialize().await.unwrap().generation;
    assert!(second_generation > first_generation);
}

struct FakeMutationExecutor {
    calls: AtomicUsize,
}

struct AcknowledgingMutationExecutor;

struct TerminalMutationExecutor;

struct InvalidConfirmedMutationExecutor;

#[async_trait]
impl DesktopMutationExecutor for AcknowledgingMutationExecutor {
    async fn execute(
        &self,
        _request: &DesktopRequest,
    ) -> Result<DesktopMutationOutcome, ProducerError> {
        Ok(DesktopMutationOutcome::Acknowledged)
    }
}

#[async_trait]
impl DesktopMutationExecutor for TerminalMutationExecutor {
    async fn execute(
        &self,
        _request: &DesktopRequest,
    ) -> Result<DesktopMutationOutcome, ProducerError> {
        Ok(DesktopMutationOutcome::TerminalFailure {
            readbacks: vec![DesktopDomainState::terminal(
                DesktopDomainId::Lock,
                CapabilityAvailability::Error,
                "/private/backend/path leaked internally",
            )
            .unwrap()],
            diagnostic_code: "/private/backend/result detail".into(),
        })
    }
}

#[async_trait]
impl DesktopMutationExecutor for InvalidConfirmedMutationExecutor {
    async fn execute(
        &self,
        _request: &DesktopRequest,
    ) -> Result<DesktopMutationOutcome, ProducerError> {
        Ok(DesktopMutationOutcome::Confirmed(vec![
            DesktopDomainState::terminal(
                DesktopDomainId::Lock,
                CapabilityAvailability::Error,
                "fixture backend degraded",
            )
            .unwrap(),
        ]))
    }
}

#[async_trait]
impl DesktopMutationExecutor for FakeMutationExecutor {
    async fn execute(
        &self,
        _request: &DesktopRequest,
    ) -> Result<DesktopMutationOutcome, ProducerError> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        Ok(DesktopMutationOutcome::Confirmed(vec![
            DesktopDomainState::available(
                DesktopDomainId::Lock,
                DesktopDomainValue::Lock(LockState { secure: true }),
            )
            .unwrap(),
        ]))
    }
}

fn lock_request(expected_generation: u64, request_id: &str) -> DesktopRequest {
    DesktopRequest {
        schema_version: DESKTOP_WIRE_VERSION,
        request_id: request_id.into(),
        expected_generation,
        command: DesktopCommand::Session(DesktopSessionCommand::Lock),
    }
}

#[tokio::test]
async fn strict_control_rejects_malformed_and_stale_requests_without_execution() {
    let temp = tempfile::tempdir().unwrap();
    let state =
        DesktopStateAuthority::open(available_registry(), temp.path().join("generation"), 8)
            .await
            .unwrap();
    state.initialize().await.unwrap();
    let executor = Arc::new(FakeMutationExecutor {
        calls: AtomicUsize::new(0),
    });
    let control = DesktopControlAuthority::open(
        Arc::clone(&state),
        executor.clone(),
        temp.path().join("dedupe.json"),
        32,
    )
    .await
    .unwrap();

    assert_eq!(
        control
            .handle_json(r#"{"schemaVersion":3,"extra":true}"#)
            .await
            .unwrap_err()
            .kind(),
        io::ErrorKind::InvalidData
    );
    let stale = lock_request(
        state.current_generation() + 1,
        "00000000-0000-4000-8000-000000000011",
    );
    let result = control
        .handle_json(&serde_json::to_string(&stale).unwrap())
        .await
        .unwrap();
    assert_eq!(result.status, DesktopResultStatus::Failed);
    assert_eq!(executor.calls.load(Ordering::SeqCst), 0);
    validate_desktop_result(&serde_json::to_string(&result).unwrap()).unwrap();
}

#[tokio::test]
async fn successful_control_publishes_confirmed_readback_before_correlated_result() {
    let temp = tempfile::tempdir().unwrap();
    let state =
        DesktopStateAuthority::open(available_registry(), temp.path().join("generation"), 8)
            .await
            .unwrap();
    state.initialize().await.unwrap();
    let executor = Arc::new(FakeMutationExecutor {
        calls: AtomicUsize::new(0),
    });
    let control = DesktopControlAuthority::open(
        Arc::clone(&state),
        executor.clone(),
        temp.path().join("dedupe.json"),
        32,
    )
    .await
    .unwrap();
    let mut events = state.subscribe().await.unwrap();
    events.recv().await.unwrap();
    let request = lock_request(
        state.current_generation(),
        "00000000-0000-4000-8000-000000000012",
    );
    let result = control
        .handle_json(&serde_json::to_string(&request).unwrap())
        .await
        .unwrap();
    assert_eq!(result.status, DesktopResultStatus::Succeeded);
    assert_eq!(executor.calls.load(Ordering::SeqCst), 1);

    let readback = events.recv().await.unwrap();
    assert!(matches!(
        readback.payload,
        DesktopEvent::DomainUpdate(SdkDomainUpdate::System(DesktopSystemUpdate::Lock(_)))
    ));
    let outcome = events.recv().await.unwrap();
    assert!(outcome.generation > readback.generation);
    assert_eq!(outcome.generation, result.generation);
    assert_eq!(outcome.cause.kind, EventCauseKind::Request);
    assert_eq!(
        outcome.cause.request_id.as_deref(),
        Some(request.request_id.as_str())
    );
    assert!(matches!(outcome.payload, DesktopEvent::CommandResult(ref value) if *value == result));
    validate_desktop_envelope(&serde_json::to_string(&outcome).unwrap()).unwrap();
}

#[tokio::test]
async fn acknowledged_logind_transition_does_not_overwrite_power_domain() {
    let temp = tempfile::tempdir().unwrap();
    let state =
        DesktopStateAuthority::open(available_registry(), temp.path().join("generation"), 8)
            .await
            .unwrap();
    state.initialize().await.unwrap();
    let control = DesktopControlAuthority::open(
        Arc::clone(&state),
        Arc::new(AcknowledgingMutationExecutor),
        temp.path().join("dedupe.json"),
        8,
    )
    .await
    .unwrap();
    let mut events = state.subscribe().await.unwrap();
    events.recv().await.unwrap();
    let request = DesktopRequest {
        schema_version: DESKTOP_WIRE_VERSION,
        request_id: "00000000-0000-4000-8000-000000000061".into(),
        expected_generation: state.current_generation(),
        command: DesktopCommand::Session(DesktopSessionCommand::Reboot),
    };
    let result = control
        .handle_json(&serde_json::to_string(&request).unwrap())
        .await
        .unwrap();
    assert_eq!(result.status, DesktopResultStatus::Succeeded);
    assert!(matches!(
        events.recv().await.unwrap().payload,
        DesktopEvent::CommandResult(_)
    ));
}

#[tokio::test]
async fn terminal_readback_never_succeeds_and_public_diagnostics_are_redacted_codes() {
    let temp = tempfile::tempdir().unwrap();
    let state =
        DesktopStateAuthority::open(available_registry(), temp.path().join("generation"), 8)
            .await
            .unwrap();
    state.initialize().await.unwrap();
    let control = DesktopControlAuthority::open(
        Arc::clone(&state),
        Arc::new(TerminalMutationExecutor),
        temp.path().join("dedupe.json"),
        8,
    )
    .await
    .unwrap();
    let mut events = state.subscribe().await.unwrap();
    events.recv().await.unwrap();
    let request = lock_request(
        state.current_generation(),
        "00000000-0000-4000-8000-000000000062",
    );
    let result = control
        .handle_json(&serde_json::to_string(&request).unwrap())
        .await
        .unwrap();
    assert_eq!(result.status, DesktopResultStatus::Failed);
    assert_eq!(
        result.diagnostic.unwrap().message,
        "mutation.terminal-failure"
    );
    let update = events.recv().await.unwrap();
    let encoded = serde_json::to_string(&update).unwrap();
    assert!(encoded.contains("producer.failed"));
    assert!(!encoded.contains("/private/"));
}

#[tokio::test]
async fn invalid_confirmed_terminal_readback_is_failed_and_durably_completed() {
    let temp = tempfile::tempdir().unwrap();
    let state =
        DesktopStateAuthority::open(available_registry(), temp.path().join("generation"), 8)
            .await
            .unwrap();
    state.initialize().await.unwrap();
    let control = DesktopControlAuthority::open(
        Arc::clone(&state),
        Arc::new(InvalidConfirmedMutationExecutor),
        temp.path().join("dedupe.json"),
        8,
    )
    .await
    .unwrap();
    let request = lock_request(
        state.current_generation(),
        "00000000-0000-4000-8000-000000000063",
    );
    let encoded = serde_json::to_string(&request).unwrap();

    let first = control.handle_json(&encoded).await.unwrap();
    assert_eq!(first.status, DesktopResultStatus::Failed);
    assert_eq!(
        first.diagnostic.as_ref().unwrap().message,
        "mutation.readback-terminal"
    );
    assert_eq!(control.handle_json(&encoded).await.unwrap(), first);
}

#[tokio::test]
async fn duplicate_request_replays_the_exact_result_across_authority_restart() {
    let temp = tempfile::tempdir().unwrap();
    let generation_path = temp.path().join("generation");
    let dedupe_path = temp.path().join("dedupe.json");
    let request_id = "00000000-0000-4000-8000-000000000013";
    let first_state = DesktopStateAuthority::open(available_registry(), &generation_path, 8)
        .await
        .unwrap();
    first_state.initialize().await.unwrap();
    let first_executor = Arc::new(FakeMutationExecutor {
        calls: AtomicUsize::new(0),
    });
    let first_control = DesktopControlAuthority::open(
        Arc::clone(&first_state),
        first_executor.clone(),
        &dedupe_path,
        32,
    )
    .await
    .unwrap();
    let request = lock_request(first_state.current_generation(), request_id);
    let original = first_control
        .handle_json(&serde_json::to_string(&request).unwrap())
        .await
        .unwrap();
    assert_eq!(first_executor.calls.load(Ordering::SeqCst), 1);
    drop(first_control);
    drop(first_state);

    let second_state = DesktopStateAuthority::open(available_registry(), &generation_path, 8)
        .await
        .unwrap();
    second_state.initialize().await.unwrap();
    let second_executor = Arc::new(FakeMutationExecutor {
        calls: AtomicUsize::new(0),
    });
    let second_control =
        DesktopControlAuthority::open(second_state, second_executor.clone(), &dedupe_path, 32)
            .await
            .unwrap();
    let duplicate = lock_request(1, request_id);
    let replay = second_control
        .handle_json(&serde_json::to_string(&duplicate).unwrap())
        .await
        .unwrap();
    assert_eq!(replay, original);
    assert_eq!(second_executor.calls.load(Ordering::SeqCst), 0);
}

#[derive(Default)]
struct ZeroizeAudit {
    calls: AtomicUsize,
    nonzero_bytes: AtomicUsize,
}

impl SecretZeroizeObserver for ZeroizeAudit {
    fn after_zeroize(&self, bytes: &[u8]) {
        self.calls.fetch_add(1, Ordering::SeqCst);
        self.nonzero_bytes.fetch_add(
            bytes.iter().filter(|byte| **byte != 0).count(),
            Ordering::SeqCst,
        );
    }
}

#[tokio::test]
async fn secret_challenges_are_single_use_raw_bytes_and_all_buffers_are_zeroized() {
    let audit = Arc::new(ZeroizeAudit::default());
    let broker = SecretBroker::with_observer(audit.clone());
    let challenge = broker.issue().await.unwrap();
    assert_eq!(challenge.len(), 16);
    assert!(challenge.iter().any(|byte| *byte != 0));

    let sentinel = b"test-only-secret-sentinel";
    let mut response = challenge.to_vec();
    response.extend_from_slice(sentinel);
    let secret = broker.accept_response(response).await.unwrap();
    assert_eq!(secret.expose(), sentinel);
    drop(secret);
    assert_eq!(audit.calls.load(Ordering::SeqCst), 1);
    assert_eq!(audit.nonzero_bytes.load(Ordering::SeqCst), 0);

    let mut replay = challenge.to_vec();
    replay.extend_from_slice(sentinel);
    assert_eq!(
        broker.accept_response(replay).await.unwrap_err().kind(),
        io::ErrorKind::PermissionDenied
    );
    assert_eq!(audit.calls.load(Ordering::SeqCst), 2);
    assert_eq!(audit.nonzero_bytes.load(Ordering::SeqCst), 0);
}

#[tokio::test(start_paused = true)]
async fn secret_total_deadline_and_frame_bound_fail_closed_with_zeroization() {
    let audit = Arc::new(ZeroizeAudit::default());
    let broker = SecretBroker::with_observer(audit.clone());
    let challenge = broker.issue().await.unwrap();
    tokio::time::advance(Duration::from_secs(30)).await;
    let mut expired = challenge.to_vec();
    expired.extend_from_slice(b"test-only-expired");
    assert_eq!(
        broker.accept_response(expired).await.unwrap_err().kind(),
        io::ErrorKind::TimedOut
    );

    let oversized = vec![0x5a; MAX_SECRET_FRAME + 1];
    assert_eq!(
        broker.accept_response(oversized).await.unwrap_err().kind(),
        io::ErrorKind::InvalidData
    );
    assert_eq!(audit.calls.load(Ordering::SeqCst), 2);
    assert_eq!(audit.nonzero_bytes.load(Ordering::SeqCst), 0);
}

#[test]
fn core_system_adapter_preserves_typed_values_and_terminal_availability() {
    let available = state_from_record(
        DesktopDomainId::Network,
        CapabilityRecord {
            id: RuntimeCapabilityId::Network,
            status: CapabilityAvailability::Available,
            value: Some(CapabilityValue::Network(NetworkRuntimeState {
                wifi_enabled: true,
                ethernet_connected: false,
                connectivity: sleepy_sdk::Connectivity::Full,
                active_connection_id: Some("fixture-network".into()),
            })),
            diagnostic: None,
        },
    );
    assert_eq!(available.status(), CapabilityAvailability::Available);
    assert!(
        matches!(available.value(), Some(DesktopDomainValue::Network(value)) if value.wifi_enabled && value.connections.len() == 1)
    );

    for status in [
        CapabilityAvailability::Unavailable,
        CapabilityAvailability::Unsupported,
        CapabilityAvailability::PermissionDenied,
        CapabilityAvailability::Timeout,
        CapabilityAvailability::Parse,
        CapabilityAvailability::Error,
    ] {
        let terminal = state_from_record(
            DesktopDomainId::Network,
            CapabilityRecord {
                id: RuntimeCapabilityId::Network,
                status,
                value: None,
                diagnostic: Some(CapabilityFailure {
                    message: "fixture terminal".into(),
                }),
            },
        );
        assert_eq!(terminal.status(), status);
    }
}

#[test]
fn proc_resource_parser_is_bounded_typed_and_rejects_inconsistent_input() {
    let parsed = parse_host_resources(
        "cpu  10 2 3 40 5 0 0 0\n",
        "MemTotal: 1000 kB\nMemAvailable: 250 kB\n",
        "1.25 0.50 0.25 1/100 1\n",
    )
    .unwrap();
    assert_eq!(parsed, (45, 60, 0.75, 1.25));
    assert_eq!(
        parse_host_resources(
            "cpu  1 2\n",
            "MemTotal: 1000 kB\nMemAvailable: 250 kB\n",
            "1.25 0.50 0.25 1/100 1\n",
        )
        .unwrap_err()
        .kind(),
        io::ErrorKind::InvalidData
    );
}

#[tokio::test]
async fn prepared_desktop_sockets_serve_strict_v3_snapshot_and_request_frames() {
    let temp = tempfile::tempdir().unwrap();
    let state =
        DesktopStateAuthority::open(available_registry(), temp.path().join("generation"), 8)
            .await
            .unwrap();
    state.initialize().await.unwrap();
    let executor = Arc::new(FakeMutationExecutor {
        calls: AtomicUsize::new(0),
    });
    let control = DesktopControlAuthority::open(
        Arc::clone(&state),
        executor,
        temp.path().join("dedupe.json"),
        32,
    )
    .await
    .unwrap();
    let sockets = PreparedDesktopSockets::bind(temp.path(), unsafe { libc::geteuid() })
        .await
        .unwrap();

    let events = sockets.events();
    let event_state = Arc::clone(&state);
    let event_server = tokio::spawn(async move {
        events
            .serve_one(move |stream, context| {
                sleepy_session::desktop::serve_event_stream(
                    stream,
                    context,
                    Arc::clone(&event_state),
                )
            })
            .await
    });
    let stream = UnixStream::connect(temp.path().join("desktop.sock"))
        .await
        .unwrap();
    let mut reader = BufReader::new(stream);
    let mut line = String::new();
    reader.read_line(&mut line).await.unwrap();
    let replay = validate_desktop_envelope(line.trim()).unwrap();
    assert!(matches!(replay.payload, DesktopEvent::FullSnapshot(_)));
    drop(reader);
    event_server.await.unwrap().unwrap();

    let requests = sockets.requests();
    let request_control = Arc::clone(&control);
    let request_server = tokio::spawn(async move {
        requests
            .serve_one(move |stream, context| {
                sleepy_session::desktop::serve_control_stream(
                    stream,
                    context,
                    Arc::clone(&request_control),
                )
            })
            .await
    });
    let mut stream = UnixStream::connect(temp.path().join("desktop-control.sock"))
        .await
        .unwrap();
    let request = lock_request(
        state.current_generation(),
        "00000000-0000-4000-8000-000000000014",
    );
    stream
        .write_all(format!("{}\n", serde_json::to_string(&request).unwrap()).as_bytes())
        .await
        .unwrap();
    let mut reader = BufReader::new(stream);
    let mut line = String::new();
    reader.read_line(&mut line).await.unwrap();
    let result = validate_desktop_result(line.trim()).unwrap();
    assert_eq!(result.status, DesktopResultStatus::Succeeded);
    request_server.await.unwrap().unwrap();
}

#[derive(Default)]
struct AcceptFixtureExchange {
    calls: AtomicUsize,
}

#[async_trait]
impl NetworkSecretExchange for AcceptFixtureExchange {
    fn acquire_lease(&self) -> io::Result<SecretRequestLease> {
        Ok(SecretRequestLease::new(
            [0x51; 16],
            "/fixture/connection",
            "802-11-wireless-security",
            Instant::now() + Duration::from_secs(30),
        ))
    }

    async fn submit(
        &self,
        _lease: &SecretRequestLease,
        secret: sleepy_session::desktop::secret_agent::LockedSecret,
    ) -> io::Result<()> {
        assert_eq!(secret.expose(), b"test-only-wire-secret");
        self.calls.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }
}

#[tokio::test]
async fn secret_socket_uses_binary_one_shot_framing_and_mode_0600() {
    use std::os::unix::fs::PermissionsExt;

    let temp = tempfile::tempdir().unwrap();
    std::fs::set_permissions(temp.path(), std::fs::Permissions::from_mode(0o700)).unwrap();
    let exchange = Arc::new(AcceptFixtureExchange::default());
    let socket = SecretSocket::bind(
        temp.path().join("secret.sock"),
        unsafe { libc::geteuid() },
        SecretBroker::default(),
        exchange.clone(),
    )
    .await
    .unwrap();
    assert_eq!(
        std::fs::metadata(socket.path())
            .unwrap()
            .permissions()
            .mode()
            & 0o777,
        0o600
    );
    let serving = Arc::clone(&socket);
    let server = tokio::spawn(async move { serving.serve_one().await });
    let mut stream = UnixStream::connect(socket.path()).await.unwrap();
    let length = stream.read_u32().await.unwrap() as usize;
    assert_eq!(length, 16);
    let mut challenge = vec![0_u8; length];
    stream.read_exact(&mut challenge).await.unwrap();
    let sentinel = b"test-only-wire-secret";
    stream
        .write_u32(u32::try_from(challenge.len() + sentinel.len()).unwrap())
        .await
        .unwrap();
    stream.write_all(&challenge).await.unwrap();
    stream.write_all(sentinel).await.unwrap();
    drop(stream);
    server.await.unwrap().unwrap();
    assert_eq!(exchange.calls.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn secret_shutdown_cancels_and_joins_a_live_leased_handler_awaiting_response() {
    use std::os::unix::fs::PermissionsExt;

    let temp = tempfile::tempdir().unwrap();
    std::fs::set_permissions(temp.path(), std::fs::Permissions::from_mode(0o700)).unwrap();
    let exchange = Arc::new(AcceptFixtureExchange::default());
    let socket = SecretSocket::bind(
        temp.path().join("secret.sock"),
        unsafe { libc::geteuid() },
        SecretBroker::default(),
        exchange.clone(),
    )
    .await
    .unwrap();
    let serving = Arc::clone(&socket);
    let server = tokio::spawn(async move { serving.serve_one().await });
    let mut client = UnixStream::connect(socket.path()).await.unwrap();
    let challenge_length = client.read_u32().await.unwrap() as usize;
    assert_eq!(challenge_length, 16);
    let mut challenge = vec![0_u8; challenge_length];
    client.read_exact(&mut challenge).await.unwrap();

    let started = Instant::now();
    let report = socket.shutdown_and_drain().await.unwrap();

    assert!(started.elapsed() < Duration::from_secs(1));
    assert_eq!(report.completed, 1);
    assert_eq!(report.aborted, 0);
    assert_eq!(exchange.calls.load(Ordering::SeqCst), 0);
    assert_eq!(
        client.read_u8().await.unwrap_err().kind(),
        io::ErrorKind::UnexpectedEof
    );
    assert!(server.await.unwrap().is_err());
}
