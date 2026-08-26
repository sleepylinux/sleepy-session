mod audio;
mod bluetooth;
mod display;
mod media;
mod network;
mod night_light;
mod power;
mod resources;
mod runner;
mod session;

use std::{
    collections::BTreeMap,
    fmt,
    sync::{
        atomic::{AtomicU64, Ordering},
        mpsc, Arc,
    },
    time::{Duration, Instant},
};

pub use runner::{
    CommandOutput, CommandRunner, CommandSpec, ProcessCommandRunner, RunControl, RunnerError,
    RunnerErrorKind,
};
use sleepy_sdk::{
    validate_session_action_result, validate_system_mutation_result, validate_system_snapshot,
    AudioRuntimeState, AudioState, BatteryRuntimeState, BluetoothRuntimeState, BluetoothState,
    BrightnessRuntimeState, CapabilityAvailability, CapabilityDiagnostic, CapabilityErrorKind,
    CapabilityFailure, CapabilityId, CapabilityRecord, CapabilityState, CapabilityValue,
    MediaRuntimeState, MediaState, MediaTransport, NetworkRuntimeState, NetworkState,
    NightLightRuntimeState, PowerProfile, PowerProfileRuntimeState, PowerState,
    RuntimeCapabilityId, SessionAction, SessionActionRequest, SessionActionResult,
    SessionActionStatus, SystemMutation, SystemMutationResult, SystemSnapshot,
};

const SNAPSHOT_DEADLINE: Duration = Duration::from_millis(1200);
const SCHEMA_VERSION: u32 = 1;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SystemErrorKind {
    InvalidGeneration,
    Stale,
    Unsupported,
    Busy,
    Timeout,
    Parse,
    Command,
    ConfirmationRequired,
    InvalidRequest,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SystemError {
    kind: SystemErrorKind,
    message: String,
}

impl SystemError {
    fn new(kind: SystemErrorKind, message: impl Into<String>) -> Self {
        Self {
            kind,
            message: message.into(),
        }
    }

    pub fn kind(&self) -> SystemErrorKind {
        self.kind
    }

    pub fn message(&self) -> &str {
        &self.message
    }

    pub fn code(&self) -> &'static str {
        match self.kind {
            SystemErrorKind::InvalidGeneration => "invalid_generation",
            SystemErrorKind::Stale => "stale_generation",
            SystemErrorKind::Unsupported => "unsupported",
            SystemErrorKind::Busy => "busy",
            SystemErrorKind::Timeout => "timeout",
            SystemErrorKind::Parse => "parse",
            SystemErrorKind::Command => "command",
            SystemErrorKind::ConfirmationRequired => "confirmation_required",
            SystemErrorKind::InvalidRequest => "invalid_request",
        }
    }
}

impl fmt::Display for SystemError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "{}: {}", self.code(), self.message)
    }
}

impl std::error::Error for SystemError {}

#[derive(Debug, Clone)]
pub(crate) struct ProbeFailure {
    kind: CapabilityErrorKind,
    message: String,
    availability: Option<CapabilityAvailability>,
}

impl ProbeFailure {
    fn parse(message: impl Into<String>) -> Self {
        Self {
            kind: CapabilityErrorKind::Parse,
            message: message.into(),
            availability: None,
        }
    }

    fn unsupported(message: impl Into<String>) -> Self {
        Self {
            kind: CapabilityErrorKind::Unsupported,
            message: message.into(),
            availability: None,
        }
    }

    fn permission_denied(message: impl Into<String>) -> Self {
        Self {
            kind: CapabilityErrorKind::Command,
            message: format!("permission denied: {}", message.into()),
            availability: Some(CapabilityAvailability::PermissionDenied),
        }
    }

    fn command(message: impl Into<String>) -> Self {
        Self {
            kind: CapabilityErrorKind::Command,
            message: message.into(),
            availability: None,
        }
    }

    fn timeout() -> Self {
        Self {
            kind: CapabilityErrorKind::Timeout,
            message: "adapter probe exceeded the snapshot deadline".to_owned(),
            availability: None,
        }
    }

    fn diagnostic(&self) -> CapabilityDiagnostic {
        CapabilityDiagnostic {
            kind: self.kind,
            message: self.message.clone(),
        }
    }
}

pub(crate) fn run_checked<R: CommandRunner>(
    runner: &R,
    command: CommandSpec,
) -> Result<Vec<u8>, ProbeFailure> {
    match runner.run(&command) {
        Ok(output) if output.status == 0 => Ok(output.stdout),
        Ok(output) if output.status == 75 => Err(ProbeFailure {
            kind: CapabilityErrorKind::Busy,
            message: "adapter command reported a busy resource".to_owned(),
            availability: None,
        }),
        Ok(output) => Err(ProbeFailure {
            kind: CapabilityErrorKind::Command,
            message: format!("adapter command exited with status {}", output.status),
            availability: None,
        }),
        Err(error) => Err(ProbeFailure {
            kind: match error.kind() {
                RunnerErrorKind::Timeout | RunnerErrorKind::Cancelled => {
                    CapabilityErrorKind::Timeout
                }
                RunnerErrorKind::Spawn => CapabilityErrorKind::Unsupported,
                RunnerErrorKind::Io => CapabilityErrorKind::Command,
            },
            message: error.message().to_owned(),
            availability: None,
        }),
    }
}

pub struct SystemFacade<R: CommandRunner> {
    runner: R,
    latest_generation: Arc<AtomicU64>,
}

impl<R: CommandRunner> Clone for SystemFacade<R> {
    fn clone(&self) -> Self {
        Self {
            runner: self.runner.clone(),
            latest_generation: Arc::clone(&self.latest_generation),
        }
    }
}

impl<R: CommandRunner> SystemFacade<R> {
    pub fn new(runner: R) -> Self {
        Self {
            runner,
            latest_generation: Arc::new(AtomicU64::new(0)),
        }
    }

    /// Bounded, capability-local readback used by the event-driven session
    /// adapters. Unlike `snapshot`, this never probes unrelated providers.
    pub(crate) fn runtime_capability(&self, id: RuntimeCapabilityId) -> CapabilityRecord {
        let value = match id {
            RuntimeCapabilityId::Network => (|| {
                let state = network::probe(&self.runner)?;
                let ethernet_connected = network::ethernet_connected(&self.runner)?;
                let connectivity = network::connectivity(&self.runner)?;
                Ok(CapabilityValue::Network(NetworkRuntimeState {
                    wifi_enabled: state.enabled,
                    ethernet_connected,
                    connectivity,
                    active_connection_id: state.connected_name,
                }))
            })(),
            RuntimeCapabilityId::Bluetooth => bluetooth::probe(&self.runner).map(|state| {
                CapabilityValue::Bluetooth(BluetoothRuntimeState {
                    powered: state.enabled,
                    connected_device_ids: state.connected_device.into_iter().collect(),
                })
            }),
            RuntimeCapabilityId::Audio => (|| {
                let output = audio::probe_output(&self.runner)?;
                let input = audio::probe_microphone(&self.runner)?;
                let devices = audio::probe_devices(&self.runner)?;
                Ok(CapabilityValue::Audio(AudioRuntimeState {
                    output_level: output.level,
                    output_muted: output.muted,
                    input_level: input.level,
                    input_muted: input.muted,
                    default_output_id: devices.selected_id,
                }))
            })(),
            RuntimeCapabilityId::Brightness => display::probe_brightness(&self.runner)
                .map(|level| CapabilityValue::Brightness(BrightnessRuntimeState { level })),
            RuntimeCapabilityId::Battery => {
                power::probe_battery(&self.runner).map(|(level, charging)| {
                    CapabilityValue::Battery(BatteryRuntimeState {
                        percentage: (level.unwrap_or(0.0) * 100.0).round() as u8,
                        charging: charging.unwrap_or(false),
                        seconds_remaining: None,
                    })
                })
            }
            RuntimeCapabilityId::PowerProfile => {
                power::probe_profiles(&self.runner).map(|(active, available)| {
                    CapabilityValue::PowerProfile(PowerProfileRuntimeState {
                        active: active.map(profile_name).unwrap_or_default().to_owned(),
                        available: available
                            .into_iter()
                            .map(|profile| profile_name(profile).to_owned())
                            .collect(),
                    })
                })
            }
            RuntimeCapabilityId::Media => media::probe(&self.runner).map(|state| {
                CapabilityValue::Media(MediaRuntimeState {
                    player_id: "mpris".to_owned(),
                    title: state.title,
                    artist: state.artist.unwrap_or_default(),
                    playing: state.playing,
                })
            }),
            RuntimeCapabilityId::NightLight => night_light::probe(&self.runner)
                .map(|enabled| CapabilityValue::NightLight(NightLightRuntimeState { enabled })),
            RuntimeCapabilityId::Resources => {
                resources::probe(std::path::Path::new("/proc")).map(CapabilityValue::Resources)
            }
            _ => {
                return CapabilityRecord {
                    id,
                    status: CapabilityAvailability::Unsupported,
                    value: None,
                    diagnostic: Some(CapabilityFailure {
                        message: "no production readback is registered for this capability".into(),
                    }),
                }
            }
        };
        match value {
            Ok(value) => CapabilityRecord {
                id,
                status: CapabilityAvailability::Available,
                value: Some(value),
                diagnostic: None,
            },
            Err(error) => CapabilityRecord {
                id,
                status: error
                    .availability
                    .unwrap_or_else(|| availability(error.kind)),
                value: None,
                diagnostic: Some(CapabilityFailure {
                    message: error.message,
                }),
            },
        }
    }

    pub fn snapshot(&self, generation: u64) -> Result<SystemSnapshot, SystemError> {
        self.accept_generation(generation)?;
        self.snapshot_accepted(generation)
    }

    fn snapshot_accepted(&self, generation: u64) -> Result<SystemSnapshot, SystemError> {
        let request_runner = RequestRunner::new(
            self.runner.clone(),
            generation,
            Arc::clone(&self.latest_generation),
        );
        let (sender, receiver) = mpsc::channel();
        let mut workers = Vec::new();
        workers.push(spawn_probe(
            request_runner.clone(),
            sender.clone(),
            |runner| ProbeResult::Network(network::probe(&runner)),
        ));
        workers.push(spawn_probe(
            request_runner.clone(),
            sender.clone(),
            |runner| ProbeResult::Bluetooth(bluetooth::probe(&runner)),
        ));
        workers.push(spawn_probe(
            request_runner.clone(),
            sender.clone(),
            |runner| ProbeResult::AudioOutput(audio::probe_output(&runner)),
        ));
        workers.push(spawn_probe(
            request_runner.clone(),
            sender.clone(),
            |runner| ProbeResult::AudioMicrophone(audio::probe_microphone(&runner)),
        ));
        workers.push(spawn_probe(
            request_runner.clone(),
            sender.clone(),
            |runner| ProbeResult::AudioDevices(audio::probe_devices(&runner)),
        ));
        workers.push(spawn_probe(
            request_runner.clone(),
            sender.clone(),
            |runner| ProbeResult::Brightness(display::probe_brightness(&runner)),
        ));
        workers.push(spawn_probe(
            request_runner.clone(),
            sender.clone(),
            |runner| ProbeResult::NightLight(night_light::probe(&runner)),
        ));
        workers.push(spawn_probe(
            request_runner.clone(),
            sender.clone(),
            |runner| ProbeResult::PowerProfiles(power::probe_profiles(&runner)),
        ));
        workers.push(spawn_probe(
            request_runner.clone(),
            sender.clone(),
            |runner| ProbeResult::Battery(power::probe_battery(&runner)),
        ));
        workers.push(spawn_probe(
            request_runner.clone(),
            sender.clone(),
            |runner| ProbeResult::Media(media::probe(&runner)),
        ));
        workers.push(spawn_probe(request_runner, sender, |runner| {
            ProbeResult::Session(session::probe(&runner))
        }));

        let started = Instant::now();
        let mut parts = SnapshotParts::default();
        for _ in 0..11 {
            let remaining = SNAPSHOT_DEADLINE.saturating_sub(started.elapsed());
            if remaining.is_zero() {
                break;
            }
            match receiver.recv_timeout(remaining) {
                Ok(result) => parts.record(result),
                Err(_) => break,
            }
        }
        drop(receiver);
        for worker in workers {
            let _ = worker.join();
        }
        parts.fill_timeouts();
        self.ensure_current(generation)?;
        validate_assembled_snapshot(parts.into_snapshot(generation))
    }

    pub fn mutate(
        &self,
        generation: u64,
        mutation: SystemMutation,
    ) -> Result<SystemMutationResult, SystemError> {
        self.accept_generation(generation)?;
        if let SystemMutation::AudioOutputDevice(id) = &mutation {
            let before = self.snapshot_accepted(generation)?;
            let valid = before
                .audio
                .as_ref()
                .is_some_and(|audio| audio.output_devices.iter().any(|device| &device.id == id));
            if !valid {
                return Err(SystemError::new(
                    SystemErrorKind::Unsupported,
                    "audio output id is not advertised by the current snapshot",
                ));
            }
        }
        let command = mutation_command(&mutation)?;
        let request_runner = RequestRunner::new(
            self.runner.clone(),
            generation,
            Arc::clone(&self.latest_generation),
        );
        if let Err(error) = run_mutation(&request_runner, &command) {
            self.ensure_current(generation)?;
            return Err(error);
        }
        let snapshot = self.snapshot_accepted(generation)?;
        if !mutation_confirmed(&mutation, &snapshot) {
            if let Some(diagnostic) = snapshot.diagnostics.get(&mutation_capability(&mutation)) {
                return Err(SystemError::new(
                    match diagnostic.kind {
                        CapabilityErrorKind::Unsupported => SystemErrorKind::Unsupported,
                        CapabilityErrorKind::Busy => SystemErrorKind::Busy,
                        CapabilityErrorKind::Timeout => SystemErrorKind::Timeout,
                        CapabilityErrorKind::Parse => SystemErrorKind::Parse,
                        CapabilityErrorKind::Command => SystemErrorKind::Command,
                    },
                    diagnostic.message.clone(),
                ));
            }
            return Err(SystemError::new(
                SystemErrorKind::Command,
                "fresh readback did not confirm the requested mutation",
            ));
        }
        validate_assembled_mutation_result(SystemMutationResult {
            schema_version: SCHEMA_VERSION,
            generation,
            mutation,
            snapshot,
        })
    }

    pub fn perform(
        &self,
        generation: u64,
        request: SessionActionRequest,
    ) -> Result<SessionActionResult, SystemError> {
        self.accept_generation(generation)?;
        if request.schema_version != SCHEMA_VERSION {
            return Err(SystemError::new(
                SystemErrorKind::InvalidRequest,
                "session action schemaVersion must be 1",
            ));
        }
        if !request.confirmed {
            return Err(SystemError::new(
                SystemErrorKind::ConfirmationRequired,
                "session action requires the literal confirmed boundary",
            ));
        }
        let action = request.action;
        let request_runner = RequestRunner::new(
            self.runner.clone(),
            generation,
            Arc::clone(&self.latest_generation),
        );
        let result = match run_checked(&request_runner, session::command(action)) {
            Ok(_) => SessionActionResult {
                schema_version: SCHEMA_VERSION,
                generation,
                action,
                status: SessionActionStatus::Initiated,
                diagnostic: None,
            },
            Err(error) => SessionActionResult {
                schema_version: SCHEMA_VERSION,
                generation,
                action,
                status: SessionActionStatus::Failed,
                diagnostic: Some(error.diagnostic()),
            },
        };
        self.ensure_current(generation)?;
        validate_assembled_session_result(result)
    }

    fn accept_generation(&self, generation: u64) -> Result<(), SystemError> {
        if generation == 0 {
            return Err(SystemError::new(
                SystemErrorKind::InvalidGeneration,
                "generation must be a positive u64 supplied by the client",
            ));
        }
        let mut current = self.latest_generation.load(Ordering::SeqCst);
        loop {
            if generation <= current {
                return Err(SystemError::new(
                    SystemErrorKind::Stale,
                    "generation must be strictly greater than every accepted request",
                ));
            }
            match self.latest_generation.compare_exchange(
                current,
                generation,
                Ordering::SeqCst,
                Ordering::SeqCst,
            ) {
                Ok(_) => return Ok(()),
                Err(observed) => current = observed,
            }
        }
    }

    fn ensure_current(&self, generation: u64) -> Result<(), SystemError> {
        if generation < self.latest_generation.load(Ordering::SeqCst) {
            return Err(SystemError::new(
                SystemErrorKind::Stale,
                "request completed after a newer generation started",
            ));
        }
        Ok(())
    }
}

fn profile_name(profile: PowerProfile) -> &'static str {
    match profile {
        PowerProfile::PowerSaver => "power-saver",
        PowerProfile::Balanced => "balanced",
        PowerProfile::Performance => "performance",
    }
}

fn availability(kind: CapabilityErrorKind) -> CapabilityAvailability {
    match kind {
        CapabilityErrorKind::Unsupported => CapabilityAvailability::Unsupported,
        CapabilityErrorKind::Timeout => CapabilityAvailability::Timeout,
        CapabilityErrorKind::Parse => CapabilityAvailability::Parse,
        CapabilityErrorKind::Busy | CapabilityErrorKind::Command => CapabilityAvailability::Error,
    }
}

#[derive(Clone)]
struct RequestRunner<R: CommandRunner> {
    inner: R,
    generation: u64,
    latest_generation: Arc<AtomicU64>,
    deadline: Instant,
}

impl<R: CommandRunner> RequestRunner<R> {
    fn new(inner: R, generation: u64, latest_generation: Arc<AtomicU64>) -> Self {
        Self {
            inner,
            generation,
            latest_generation,
            deadline: Instant::now() + SNAPSHOT_DEADLINE,
        }
    }
}

impl<R: CommandRunner> CommandRunner for RequestRunner<R> {
    fn run(&self, command: &CommandSpec) -> Result<CommandOutput, RunnerError> {
        if self.generation < self.latest_generation.load(Ordering::SeqCst) {
            return Err(RunnerError::cancelled());
        }
        let remaining = self.deadline.saturating_duration_since(Instant::now());
        if remaining.is_zero() {
            return Err(RunnerError::timeout("snapshot deadline expired"));
        }
        let mut command = command.clone();
        command.timeout = command.timeout.min(remaining);
        let control = RunControl::for_generation(
            self.deadline,
            self.generation,
            Arc::clone(&self.latest_generation),
        );
        self.inner.run_controlled(&command, &control)
    }
}

fn spawn_probe<R, F>(
    runner: R,
    sender: mpsc::Sender<ProbeResult>,
    probe: F,
) -> std::thread::JoinHandle<()>
where
    R: CommandRunner,
    F: FnOnce(R) -> ProbeResult + Send + 'static,
{
    std::thread::spawn(move || {
        let _ = sender.send(probe(runner));
    })
}

#[derive(Default)]
struct SnapshotParts {
    network: Option<Result<NetworkState, ProbeFailure>>,
    bluetooth: Option<Result<BluetoothState, ProbeFailure>>,
    audio_output: Option<Result<audio::LevelState, ProbeFailure>>,
    audio_microphone: Option<Result<audio::LevelState, ProbeFailure>>,
    audio_devices: Option<Result<audio::DeviceState, ProbeFailure>>,
    brightness: Option<Result<f64, ProbeFailure>>,
    night_light: Option<Result<bool, ProbeFailure>>,
    power_profiles: Option<ProfileProbe>,
    battery: Option<BatteryProbe>,
    media: Option<Result<MediaState, ProbeFailure>>,
    session_actions: Option<BTreeMap<SessionAction, CapabilityState>>,
}

type ProfileProbe = Result<(Option<PowerProfile>, Vec<PowerProfile>), ProbeFailure>;
type BatteryProbe = Result<(Option<f64>, Option<bool>), ProbeFailure>;

enum ProbeResult {
    Network(Result<NetworkState, ProbeFailure>),
    Bluetooth(Result<BluetoothState, ProbeFailure>),
    AudioOutput(Result<audio::LevelState, ProbeFailure>),
    AudioMicrophone(Result<audio::LevelState, ProbeFailure>),
    AudioDevices(Result<audio::DeviceState, ProbeFailure>),
    Brightness(Result<f64, ProbeFailure>),
    NightLight(Result<bool, ProbeFailure>),
    PowerProfiles(ProfileProbe),
    Battery(BatteryProbe),
    Media(Result<MediaState, ProbeFailure>),
    Session(BTreeMap<SessionAction, CapabilityState>),
}

impl SnapshotParts {
    fn record(&mut self, result: ProbeResult) {
        match result {
            ProbeResult::Network(value) => self.network = Some(value),
            ProbeResult::Bluetooth(value) => self.bluetooth = Some(value),
            ProbeResult::AudioOutput(value) => self.audio_output = Some(value),
            ProbeResult::AudioMicrophone(value) => self.audio_microphone = Some(value),
            ProbeResult::AudioDevices(value) => self.audio_devices = Some(value),
            ProbeResult::Brightness(value) => self.brightness = Some(value),
            ProbeResult::NightLight(value) => self.night_light = Some(value),
            ProbeResult::PowerProfiles(value) => self.power_profiles = Some(value),
            ProbeResult::Battery(value) => self.battery = Some(value),
            ProbeResult::Media(value) => self.media = Some(value),
            ProbeResult::Session(actions) => self.session_actions = Some(actions),
        }
    }

    fn fill_timeouts(&mut self) {
        self.network
            .get_or_insert_with(|| Err(ProbeFailure::timeout()));
        self.bluetooth
            .get_or_insert_with(|| Err(ProbeFailure::timeout()));
        self.audio_output
            .get_or_insert_with(|| Err(ProbeFailure::timeout()));
        self.audio_microphone
            .get_or_insert_with(|| Err(ProbeFailure::timeout()));
        self.audio_devices
            .get_or_insert_with(|| Err(ProbeFailure::timeout()));
        self.brightness
            .get_or_insert_with(|| Err(ProbeFailure::timeout()));
        self.night_light
            .get_or_insert_with(|| Err(ProbeFailure::timeout()));
        self.power_profiles
            .get_or_insert_with(|| Err(ProbeFailure::timeout()));
        self.battery
            .get_or_insert_with(|| Err(ProbeFailure::timeout()));
        self.media
            .get_or_insert_with(|| Err(ProbeFailure::timeout()));
    }

    fn into_snapshot(self, generation: u64) -> SystemSnapshot {
        let mut capabilities = BTreeMap::new();
        let mut diagnostics = BTreeMap::new();
        let network = record_group(
            self.network.expect("filled"),
            &[CapabilityId::NetworkEnabled],
            &mut capabilities,
            &mut diagnostics,
        );
        let bluetooth = record_group(
            self.bluetooth.expect("filled"),
            &[CapabilityId::BluetoothEnabled],
            &mut capabilities,
            &mut diagnostics,
        );
        let audio_output = record_group(
            self.audio_output.expect("filled"),
            &[CapabilityId::AudioVolume, CapabilityId::AudioMuted],
            &mut capabilities,
            &mut diagnostics,
        );
        let audio_microphone = record_group(
            self.audio_microphone.expect("filled"),
            &[
                CapabilityId::AudioMicrophoneLevel,
                CapabilityId::AudioMicrophoneMuted,
            ],
            &mut capabilities,
            &mut diagnostics,
        );
        let audio_devices = record_group(
            self.audio_devices.expect("filled"),
            &[CapabilityId::AudioOutputDevice],
            &mut capabilities,
            &mut diagnostics,
        );
        let brightness = record_group(
            self.brightness.expect("filled"),
            &[CapabilityId::DisplayBrightness],
            &mut capabilities,
            &mut diagnostics,
        );
        let night_light = record_group(
            self.night_light.expect("filled"),
            &[CapabilityId::DisplayNightLightEnabled],
            &mut capabilities,
            &mut diagnostics,
        );
        let profiles = record_group(
            self.power_profiles.expect("filled"),
            &[CapabilityId::PowerProfile],
            &mut capabilities,
            &mut diagnostics,
        );
        let battery = record_group(
            self.battery.expect("filled"),
            &[CapabilityId::BatteryStatus],
            &mut capabilities,
            &mut diagnostics,
        );
        let media = record_group(
            self.media.expect("filled"),
            &[CapabilityId::MediaTransport],
            &mut capabilities,
            &mut diagnostics,
        );
        let display = (brightness.is_some() || night_light.is_some())
            .then(|| display::state(brightness, night_light.unwrap_or(false)));
        let audio = (audio_output.is_some()
            || audio_microphone.is_some()
            || audio_devices.is_some())
        .then(|| {
            let output = audio_output.unwrap_or(audio::LevelState {
                level: 0.0,
                muted: false,
            });
            let microphone = audio_microphone.unwrap_or(audio::LevelState {
                level: 0.0,
                muted: false,
            });
            let devices = audio_devices.unwrap_or(audio::DeviceState {
                selected_id: None,
                devices: Vec::new(),
            });
            AudioState {
                volume: output.level,
                muted: output.muted,
                microphone_level: microphone.level,
                microphone_muted: microphone.muted,
                output_device_id: devices.selected_id,
                output_devices: devices.devices,
            }
        });
        let power = (profiles.is_some() || battery.is_some()).then(|| {
            let (current_profile, available_profiles) = profiles.unwrap_or_default();
            let (battery_level, charging) = battery.unwrap_or_default();
            PowerState {
                battery_level,
                charging,
                current_profile,
                available_profiles,
            }
        });
        SystemSnapshot {
            schema_version: SCHEMA_VERSION,
            generation,
            capabilities,
            diagnostics,
            session_actions: self.session_actions.unwrap_or_else(|| {
                BTreeMap::from([
                    (SessionAction::Lock, CapabilityState::Busy),
                    (SessionAction::Logout, CapabilityState::Busy),
                    (SessionAction::Reboot, CapabilityState::Busy),
                    (SessionAction::PowerOff, CapabilityState::Busy),
                ])
            }),
            network,
            bluetooth,
            audio,
            display,
            power,
            media,
        }
    }
}

fn record_group<T>(
    result: Result<T, ProbeFailure>,
    capabilities_for_group: &[CapabilityId],
    capabilities: &mut BTreeMap<CapabilityId, CapabilityState>,
    diagnostics: &mut BTreeMap<CapabilityId, CapabilityDiagnostic>,
) -> Option<T> {
    match result {
        Ok(value) => {
            for capability in capabilities_for_group {
                capabilities.insert(*capability, CapabilityState::Available);
            }
            Some(value)
        }
        Err(error) => {
            let state = match error.kind {
                CapabilityErrorKind::Unsupported => CapabilityState::Unavailable,
                CapabilityErrorKind::Busy => CapabilityState::Busy,
                _ => CapabilityState::Error,
            };
            for capability in capabilities_for_group {
                capabilities.insert(*capability, state);
                diagnostics.insert(*capability, error.diagnostic());
            }
            None
        }
    }
}

fn validate_assembled_snapshot(snapshot: SystemSnapshot) -> Result<SystemSnapshot, SystemError> {
    let json = serde_json::to_string(&snapshot).map_err(|error| {
        SystemError::new(
            SystemErrorKind::Parse,
            format!("could not serialize assembled snapshot: {error}"),
        )
    })?;
    validate_system_snapshot(&json).map_err(|error| {
        SystemError::new(
            SystemErrorKind::Parse,
            format!("assembled snapshot violates the SDK contract: {error}"),
        )
    })
}

fn validate_assembled_mutation_result(
    result: SystemMutationResult,
) -> Result<SystemMutationResult, SystemError> {
    let json = serde_json::to_string(&result).map_err(|error| {
        SystemError::new(
            SystemErrorKind::Parse,
            format!("could not serialize assembled mutation result: {error}"),
        )
    })?;
    validate_system_mutation_result(&json).map_err(|error| {
        SystemError::new(
            SystemErrorKind::Parse,
            format!("assembled mutation result violates the SDK contract: {error}"),
        )
    })
}

fn validate_assembled_session_result(
    result: SessionActionResult,
) -> Result<SessionActionResult, SystemError> {
    let json = serde_json::to_string(&result).map_err(|error| {
        SystemError::new(
            SystemErrorKind::Parse,
            format!("could not serialize assembled session result: {error}"),
        )
    })?;
    validate_session_action_result(&json).map_err(|error| {
        SystemError::new(
            SystemErrorKind::Parse,
            format!("assembled session result violates the SDK contract: {error}"),
        )
    })
}

pub fn mutation_command(mutation: &SystemMutation) -> Result<CommandSpec, SystemError> {
    let command = match mutation {
        SystemMutation::NetworkEnabled(enabled) => {
            CommandSpec::new("nmcli", ["radio", "wifi", bool_word(*enabled, "on", "off")])
        }
        SystemMutation::BluetoothEnabled(enabled) => {
            CommandSpec::new("bluetoothctl", ["power", bool_word(*enabled, "on", "off")])
        }
        SystemMutation::AudioVolume(value) => CommandSpec::new(
            "wpctl",
            ["set-volume", "@DEFAULT_AUDIO_SINK@", &value.to_string()],
        ),
        SystemMutation::AudioMuted(muted) => CommandSpec::new(
            "wpctl",
            [
                "set-mute",
                "@DEFAULT_AUDIO_SINK@",
                bool_word(*muted, "1", "0"),
            ],
        ),
        SystemMutation::AudioMicrophoneLevel(value) => CommandSpec::new(
            "wpctl",
            ["set-volume", "@DEFAULT_AUDIO_SOURCE@", &value.to_string()],
        ),
        SystemMutation::AudioMicrophoneMuted(muted) => CommandSpec::new(
            "wpctl",
            [
                "set-mute",
                "@DEFAULT_AUDIO_SOURCE@",
                bool_word(*muted, "1", "0"),
            ],
        ),
        SystemMutation::AudioOutputDevice(id) => {
            if id.is_empty() || !id.bytes().all(|byte| byte.is_ascii_digit()) {
                return Err(SystemError::new(
                    SystemErrorKind::Unsupported,
                    "PipeWire sink id must be a numeric id advertised by the snapshot",
                ));
            }
            CommandSpec::new("wpctl", ["set-default", id])
        }
        SystemMutation::DisplayBrightness(value) => {
            CommandSpec::new("brightnessctl", ["set", &format!("{}%", value * 100.0)])
        }
        SystemMutation::DisplayNightLightEnabled(enabled) => CommandSpec::new(
            "systemctl",
            [
                "--user",
                bool_word(*enabled, "start", "stop"),
                "gammastep.service",
            ],
        ),
        SystemMutation::PowerProfile(profile) => CommandSpec::new(
            "powerprofilesctl",
            [
                "set",
                match profile {
                    PowerProfile::PowerSaver => "power-saver",
                    PowerProfile::Balanced => "balanced",
                    PowerProfile::Performance => "performance",
                },
            ],
        ),
        SystemMutation::MediaTransport(transport) => CommandSpec::new(
            "playerctl",
            [match transport {
                MediaTransport::PlayPause => "play-pause",
                MediaTransport::Next => "next",
                MediaTransport::Previous => "previous",
            }],
        ),
    };
    Ok(command)
}

fn bool_word(value: bool, yes: &'static str, no: &'static str) -> &'static str {
    if value {
        yes
    } else {
        no
    }
}

fn run_mutation<R: CommandRunner>(runner: &R, command: &CommandSpec) -> Result<(), SystemError> {
    match run_checked(runner, command.clone()) {
        Ok(_) => Ok(()),
        Err(error) => Err(SystemError::new(
            match error.kind {
                CapabilityErrorKind::Unsupported => SystemErrorKind::Unsupported,
                CapabilityErrorKind::Timeout => SystemErrorKind::Timeout,
                CapabilityErrorKind::Parse => SystemErrorKind::Parse,
                CapabilityErrorKind::Busy => SystemErrorKind::Busy,
                CapabilityErrorKind::Command => SystemErrorKind::Command,
            },
            error.message,
        )),
    }
}

fn mutation_confirmed(mutation: &SystemMutation, snapshot: &SystemSnapshot) -> bool {
    match mutation {
        SystemMutation::NetworkEnabled(value) => snapshot
            .network
            .as_ref()
            .is_some_and(|state| state.enabled == *value),
        SystemMutation::BluetoothEnabled(value) => snapshot
            .bluetooth
            .as_ref()
            .is_some_and(|state| state.enabled == *value),
        SystemMutation::AudioVolume(value) => snapshot
            .audio
            .as_ref()
            .is_some_and(|state| state.volume == *value),
        SystemMutation::AudioMuted(value) => snapshot
            .audio
            .as_ref()
            .is_some_and(|state| state.muted == *value),
        SystemMutation::AudioMicrophoneLevel(value) => snapshot
            .audio
            .as_ref()
            .is_some_and(|state| state.microphone_level == *value),
        SystemMutation::AudioMicrophoneMuted(value) => snapshot
            .audio
            .as_ref()
            .is_some_and(|state| state.microphone_muted == *value),
        SystemMutation::AudioOutputDevice(id) => snapshot
            .audio
            .as_ref()
            .is_some_and(|state| state.output_device_id.as_ref() == Some(id)),
        SystemMutation::DisplayBrightness(value) => {
            snapshot.display.as_ref().and_then(|state| state.brightness) == Some(*value)
        }
        SystemMutation::DisplayNightLightEnabled(value) => snapshot
            .display
            .as_ref()
            .is_some_and(|state| state.night_light_enabled == *value),
        SystemMutation::PowerProfile(value) => snapshot
            .power
            .as_ref()
            .is_some_and(|state| state.current_profile == Some(*value)),
        SystemMutation::MediaTransport(_) => snapshot.media.is_some(),
    }
}

fn mutation_capability(mutation: &SystemMutation) -> CapabilityId {
    match mutation {
        SystemMutation::NetworkEnabled(_) => CapabilityId::NetworkEnabled,
        SystemMutation::BluetoothEnabled(_) => CapabilityId::BluetoothEnabled,
        SystemMutation::AudioVolume(_) => CapabilityId::AudioVolume,
        SystemMutation::AudioMuted(_) => CapabilityId::AudioMuted,
        SystemMutation::AudioMicrophoneLevel(_) => CapabilityId::AudioMicrophoneLevel,
        SystemMutation::AudioMicrophoneMuted(_) => CapabilityId::AudioMicrophoneMuted,
        SystemMutation::AudioOutputDevice(_) => CapabilityId::AudioOutputDevice,
        SystemMutation::DisplayBrightness(_) => CapabilityId::DisplayBrightness,
        SystemMutation::DisplayNightLightEnabled(_) => CapabilityId::DisplayNightLightEnabled,
        SystemMutation::PowerProfile(_) => CapabilityId::PowerProfile,
        SystemMutation::MediaTransport(_) => CapabilityId::MediaTransport,
    }
}
