use std::{
    collections::{BTreeMap, VecDeque},
    ffi::OsString,
    fmt, io,
    path::Path,
    sync::{Arc, Mutex as StdMutex},
    time::{Duration, Instant as StdInstant},
};

use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use sleepy_sdk::{
    validate_desktop_envelope, validate_desktop_request, validate_desktop_result, AudioSnapshot,
    BatterySnapshot, BluetoothSnapshot, BrightnessSnapshot, CacheStatus, CalendarSnapshot,
    CapabilityAvailability, CapabilityFailure, ClipboardEntry, DesktopAppearanceSnapshot,
    DesktopCalendarSnapshot, DesktopCapability, DesktopCompositorSnapshot, DesktopCompositorUpdate,
    DesktopDomainUpdate as SdkDomainUpdate, DesktopEnvelope, DesktopEvent, DesktopLauncherSnapshot,
    DesktopNotificationSnapshot, DesktopOsdSnapshot, DesktopPowerSnapshot, DesktopRequest,
    DesktopResourceSnapshot, DesktopResult, DesktopResultStatus, DesktopSnapshot,
    DesktopSystemSnapshot, DesktopSystemUpdate, DesktopUtilitySnapshot, DesktopUtilityUpdate,
    DesktopWeatherSnapshot, EventCause, EventCauseKind, HyprlandActionCapabilities,
    HyprlandSnapshot, LauncherEntry, LockState, MediaSnapshot, NetworkSnapshot, NightLightSnapshot,
    PowerProfile, ProducerAvailability, ProviderStatus, RecordingState, RecordingStatus,
    ResourceSample, ThemeDocument, TrayItem, WeatherLocation, WeatherSnapshot,
    DESKTOP_WIRE_VERSION, WIRE_SCHEMA_VERSION,
};
use tokio::sync::{broadcast, mpsc, Mutex, Notify};
use tokio::{io::BufReader, net::UnixStream};
use tokio_util::sync::CancellationToken;

pub mod adapters;
pub mod appearance;
pub mod audio;
pub mod bluetooth;
pub mod clipboard;
pub mod core;
pub mod display;
pub mod media;
pub mod mutation;
pub mod network;
pub mod power;
pub mod resources;
pub mod secret_agent;
pub mod tray;
pub mod utilities;

const INITIAL_DEADLINE: Duration = Duration::from_secs(2);
const INITIAL_PROBE_BUDGET: Duration = Duration::from_millis(1_500);
const MAX_DESKTOP_FRAME_BYTES: usize = 896 * 1024;
const MAX_DESKTOP_SERIALIZED_ITEMS: usize = 20_000;
const MAX_DESKTOP_STRING_BYTES: usize = 768 * 1024;

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum DesktopDomainId {
    Network,
    Bluetooth,
    Audio,
    Media,
    Battery,
    Brightness,
    NightLight,
    Power,
    Osd,
    Lock,
    Hyprland,
    Notifications,
    Launcher,
    Calendar,
    Weather,
    Appearance,
    Resources,
    Tray,
    Clipboard,
    Recording,
    IdleInhibit,
    GameMode,
    Screenshot,
    ColorPicker,
}

impl DesktopDomainId {
    pub const ALL: [Self; 24] = [
        Self::Network,
        Self::Bluetooth,
        Self::Audio,
        Self::Media,
        Self::Battery,
        Self::Brightness,
        Self::NightLight,
        Self::Power,
        Self::Osd,
        Self::Lock,
        Self::Hyprland,
        Self::Notifications,
        Self::Launcher,
        Self::Calendar,
        Self::Weather,
        Self::Appearance,
        Self::Resources,
        Self::Tray,
        Self::Clipboard,
        Self::Recording,
        Self::IdleInhibit,
        Self::GameMode,
        Self::Screenshot,
        Self::ColorPicker,
    ];
}

#[derive(Debug, Clone, PartialEq)]
pub enum DesktopDomainValue {
    Network(NetworkSnapshot),
    Bluetooth(BluetoothSnapshot),
    Audio(AudioSnapshot),
    Media(MediaSnapshot),
    Battery(BatterySnapshot),
    Brightness(BrightnessSnapshot),
    NightLight(NightLightSnapshot),
    Power(DesktopPowerSnapshot),
    Osd(DesktopOsdSnapshot),
    Lock(LockState),
    Hyprland(HyprlandSnapshot),
    Notifications(DesktopNotificationSnapshot),
    Launcher(Vec<LauncherEntry>),
    Calendar(CalendarSnapshot),
    Weather(WeatherSnapshot),
    Appearance {
        theme: ThemeDocument,
        wallpaper_id: String,
    },
    Resources(Vec<ResourceSample>),
    Tray(Vec<TrayItem>),
    Clipboard(Vec<ClipboardEntry>),
    Recording(RecordingState),
    IdleInhibit(bool),
    GameMode(bool),
    Screenshot,
    ColorPicker,
}

impl DesktopDomainValue {
    pub fn domain(&self) -> DesktopDomainId {
        match self {
            Self::Network(_) => DesktopDomainId::Network,
            Self::Bluetooth(_) => DesktopDomainId::Bluetooth,
            Self::Audio(_) => DesktopDomainId::Audio,
            Self::Media(_) => DesktopDomainId::Media,
            Self::Battery(_) => DesktopDomainId::Battery,
            Self::Brightness(_) => DesktopDomainId::Brightness,
            Self::NightLight(_) => DesktopDomainId::NightLight,
            Self::Power(_) => DesktopDomainId::Power,
            Self::Osd(_) => DesktopDomainId::Osd,
            Self::Lock(_) => DesktopDomainId::Lock,
            Self::Hyprland(_) => DesktopDomainId::Hyprland,
            Self::Notifications(_) => DesktopDomainId::Notifications,
            Self::Launcher(_) => DesktopDomainId::Launcher,
            Self::Calendar(_) => DesktopDomainId::Calendar,
            Self::Weather(_) => DesktopDomainId::Weather,
            Self::Appearance { .. } => DesktopDomainId::Appearance,
            Self::Resources(_) => DesktopDomainId::Resources,
            Self::Tray(_) => DesktopDomainId::Tray,
            Self::Clipboard(_) => DesktopDomainId::Clipboard,
            Self::Recording(_) => DesktopDomainId::Recording,
            Self::IdleInhibit(_) => DesktopDomainId::IdleInhibit,
            Self::GameMode(_) => DesktopDomainId::GameMode,
            Self::Screenshot => DesktopDomainId::Screenshot,
            Self::ColorPicker => DesktopDomainId::ColorPicker,
        }
    }

    pub fn empty(domain: DesktopDomainId) -> Self {
        match domain {
            DesktopDomainId::Network => Self::Network(NetworkSnapshot {
                wifi_enabled: false,
                scanning: false,
                access_points: Vec::new(),
                connections: Vec::new(),
            }),
            DesktopDomainId::Bluetooth => Self::Bluetooth(BluetoothSnapshot {
                powered: false,
                scanning: false,
                devices: Vec::new(),
            }),
            DesktopDomainId::Audio => Self::Audio(AudioSnapshot {
                nodes: Vec::new(),
                streams: Vec::new(),
            }),
            DesktopDomainId::Media => Self::Media(MediaSnapshot {
                players: Vec::new(),
            }),
            DesktopDomainId::Battery => Self::Battery(BatterySnapshot {
                level: 0.0,
                charging: false,
                seconds_remaining: None,
            }),
            DesktopDomainId::Brightness => Self::Brightness(BrightnessSnapshot { level: 0.0 }),
            DesktopDomainId::NightLight => Self::NightLight(NightLightSnapshot { enabled: false }),
            DesktopDomainId::Power => Self::Power(DesktopPowerSnapshot {
                active_profile: PowerProfile::Balanced,
                available_profiles: vec![PowerProfile::Balanced],
            }),
            DesktopDomainId::Osd => Self::Osd(DesktopOsdSnapshot {
                current: None,
                history: Vec::new(),
            }),
            DesktopDomainId::Lock => Self::Lock(LockState { secure: false }),
            DesktopDomainId::Hyprland => Self::Hyprland(HyprlandSnapshot {
                action_capabilities: hyprland_action_capabilities(),
                monitors: Vec::new(),
                workspaces: Vec::new(),
                windows: Vec::new(),
            }),
            DesktopDomainId::Notifications => Self::Notifications(DesktopNotificationSnapshot {
                availability: available_producer(),
                dnd: false,
                active: Vec::new(),
            }),
            DesktopDomainId::Launcher => Self::Launcher(Vec::new()),
            DesktopDomainId::Calendar => Self::Calendar(empty_calendar()),
            DesktopDomainId::Weather => Self::Weather(empty_weather()),
            DesktopDomainId::Appearance => Self::Appearance {
                theme: crate::theme::ThemeManager::builtin("builtin.sleepy-dark")
                    .expect("static built-in theme"),
                wallpaper_id: "builtin.sleepy-default".into(),
            },
            DesktopDomainId::Resources => Self::Resources(Vec::new()),
            DesktopDomainId::Tray => Self::Tray(Vec::new()),
            DesktopDomainId::Clipboard => Self::Clipboard(Vec::new()),
            DesktopDomainId::Recording => Self::Recording(RecordingState {
                status: RecordingStatus::Inactive,
                recording_id: None,
                output_id: None,
            }),
            DesktopDomainId::IdleInhibit => Self::IdleInhibit(false),
            DesktopDomainId::GameMode => Self::GameMode(false),
            DesktopDomainId::Screenshot => Self::Screenshot,
            DesktopDomainId::ColorPicker => Self::ColorPicker,
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct DesktopDomainState {
    domain: DesktopDomainId,
    status: CapabilityAvailability,
    value: Option<DesktopDomainValue>,
    diagnostic: Option<String>,
}

impl DesktopDomainState {
    pub fn terminal(
        domain: DesktopDomainId,
        status: CapabilityAvailability,
        diagnostic: impl Into<String>,
    ) -> io::Result<Self> {
        if status == CapabilityAvailability::Available {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "available desktop domain state requires typed data",
            ));
        }
        let diagnostic = diagnostic.into();
        if diagnostic.trim().is_empty() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "terminal desktop domain state requires a diagnostic",
            ));
        }
        Ok(Self {
            domain,
            status,
            value: None,
            diagnostic: Some(diagnostic),
        })
    }

    pub fn available(domain: DesktopDomainId, value: DesktopDomainValue) -> io::Result<Self> {
        if value.domain() != domain {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "desktop domain value belongs to a different producer",
            ));
        }
        Ok(Self {
            domain,
            status: CapabilityAvailability::Available,
            value: Some(value),
            diagnostic: None,
        })
    }

    pub fn domain(&self) -> DesktopDomainId {
        self.domain
    }

    pub fn status(&self) -> CapabilityAvailability {
        self.status
    }

    pub fn diagnostic(&self) -> Option<&str> {
        self.diagnostic.as_deref()
    }

    pub fn value(&self) -> Option<&DesktopDomainValue> {
        self.value.as_ref()
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct DesktopDomainUpdate {
    pub state: DesktopDomainState,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProducerError {
    message: String,
}

impl ProducerError {
    pub fn new(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
        }
    }
}

impl fmt::Display for ProducerError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.message)
    }
}

impl std::error::Error for ProducerError {}

#[async_trait]
pub trait DesktopProducer: Send + Sync {
    fn domain(&self) -> DesktopDomainId;
    async fn initial(&self) -> DesktopDomainState;
    async fn run(
        &self,
        sender: mpsc::Sender<DesktopDomainUpdate>,
        cancellation: CancellationToken,
    ) -> Result<(), ProducerError>;
}

pub struct DesktopRegistry {
    producers: BTreeMap<DesktopDomainId, Arc<dyn DesktopProducer>>,
}

pub struct DesktopProducerRuntime {
    cancellation: CancellationToken,
    tasks: Vec<tokio::task::JoinHandle<()>>,
    aggregator: tokio::task::JoinHandle<io::Result<()>>,
}

pub struct DesktopStateAuthority {
    registry: Arc<DesktopRegistry>,
    transaction: Arc<StdMutex<DesktopAuthorityTransaction>>,
    initialized: Notify,
    events: broadcast::Sender<DesktopEnvelope>,
}

struct DesktopAuthorityTransaction {
    states: Option<BTreeMap<DesktopDomainId, DesktopDomainState>>,
    generations: crate::sessiond::GenerationAllocator,
    current_generation: u64,
    latest_snapshot: Option<DesktopEnvelope>,
}

pub struct DesktopSubscriber {
    replay: VecDeque<DesktopEnvelope>,
    events: broadcast::Receiver<DesktopEnvelope>,
}

#[async_trait]
pub trait DesktopMutationExecutor: Send + Sync + 'static {
    async fn execute(
        &self,
        request: &DesktopRequest,
    ) -> Result<DesktopMutationOutcome, ProducerError>;
}

#[derive(Debug, Clone, PartialEq)]
pub enum DesktopMutationOutcome {
    Confirmed(Vec<DesktopDomainState>),
    Acknowledged,
    TerminalFailure {
        readbacks: Vec<DesktopDomainState>,
        diagnostic_code: String,
    },
}

pub struct DesktopControlAuthority<E: DesktopMutationExecutor> {
    state: Arc<DesktopStateAuthority>,
    executor: Arc<E>,
    dedupe: Arc<StdMutex<DurableDedupe>>,
    serial: Mutex<()>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct DedupeDocument {
    schema_version: u32,
    records: VecDeque<DedupeRecord>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct DedupeRecord {
    request_id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    result: Option<DesktopResult>,
}

struct DurableDedupe {
    directory: crate::store::SecureDir,
    name: OsString,
    maximum_records: usize,
    document: DedupeDocument,
    fault: Option<DedupeFaultPoint>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum DedupeFaultPoint {
    FileSync,
    Rename,
    DirectorySync,
}

impl DesktopSubscriber {
    pub async fn recv(&mut self) -> io::Result<DesktopEnvelope> {
        if let Some(event) = self.replay.pop_front() {
            return Ok(event);
        }
        match self.events.recv().await {
            Ok(event) => Ok(event),
            Err(broadcast::error::RecvError::Lagged(_)) => Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "desktop event subscriber exceeded its bounded queue",
            )),
            Err(broadcast::error::RecvError::Closed) => Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "desktop event authority closed",
            )),
        }
    }
}

impl<E: DesktopMutationExecutor> DesktopControlAuthority<E> {
    pub async fn open(
        state: Arc<DesktopStateAuthority>,
        executor: Arc<E>,
        dedupe_path: impl AsRef<Path>,
        maximum_records: usize,
    ) -> io::Result<Arc<Self>> {
        if maximum_records == 0 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "desktop dedupe capacity must be positive",
            ));
        }
        let path = dedupe_path.as_ref().to_owned();
        let dedupe =
            tokio::task::spawn_blocking(move || DurableDedupe::open(&path, maximum_records))
                .await
                .map_err(join_error)??;
        Ok(Arc::new(Self {
            state,
            executor,
            dedupe: Arc::new(StdMutex::new(dedupe)),
            serial: Mutex::new(()),
        }))
    }

    pub async fn handle_json(&self, input: &str) -> io::Result<DesktopResult> {
        let request = validate_desktop_request(input)
            .map_err(|error| io::Error::new(io::ErrorKind::InvalidData, error))?;
        let _serial = self.serial.lock().await;
        if let Some(result) = self.lookup(&request.request_id).await? {
            return Ok(result);
        }

        let (generation_matches, current_generation) = self
            .state
            .checked_generation(request.expected_generation)
            .await?;
        if !generation_matches {
            let result = failed_result(
                &request.request_id,
                current_generation,
                "request.generation-stale",
            )?;
            self.complete(request.request_id, result.clone()).await?;
            return Ok(result);
        }

        self.begin(request.request_id.clone()).await?;
        let cause = EventCause {
            kind: EventCauseKind::Request,
            request_id: Some(request.request_id.clone()),
        };
        let execution = self.executor.execute(&request).await;
        let (status, diagnostic) = match execution {
            Ok(DesktopMutationOutcome::Confirmed(readbacks)) if !readbacks.is_empty() => {
                let terminal = readbacks
                    .iter()
                    .any(|readback| readback.status() != CapabilityAvailability::Available);
                for readback in readbacks {
                    self.state.publish_domain(readback, cause.clone()).await?;
                }
                if terminal {
                    (
                        DesktopResultStatus::Failed,
                        Some(CapabilityFailure {
                            message: "mutation.readback-terminal".into(),
                        }),
                    )
                } else {
                    (DesktopResultStatus::Succeeded, None)
                }
            }
            Ok(DesktopMutationOutcome::Confirmed(_)) => (
                DesktopResultStatus::Failed,
                Some(CapabilityFailure {
                    message: "mutation.readback-missing".into(),
                }),
            ),
            Ok(DesktopMutationOutcome::Acknowledged) => (DesktopResultStatus::Succeeded, None),
            Ok(DesktopMutationOutcome::TerminalFailure {
                readbacks,
                diagnostic_code,
            }) => {
                for readback in readbacks {
                    self.state.publish_domain(readback, cause.clone()).await?;
                }
                (
                    DesktopResultStatus::Failed,
                    Some(CapabilityFailure {
                        message: public_mutation_diagnostic(&diagnostic_code).into(),
                    }),
                )
            }
            Err(_error) => (
                DesktopResultStatus::Failed,
                Some(CapabilityFailure {
                    message: "mutation.backend-failed".into(),
                }),
            ),
        };
        let (_, result) = self
            .state
            .publish_command_result(request.request_id.clone(), status, diagnostic)
            .await?;
        self.complete(request.request_id, result.clone()).await?;
        Ok(result)
    }

    async fn lookup(&self, request_id: &str) -> io::Result<Option<DesktopResult>> {
        let dedupe = Arc::clone(&self.dedupe);
        let request_id = request_id.to_owned();
        tokio::task::spawn_blocking(move || {
            let dedupe = dedupe
                .lock()
                .map_err(|_| io::Error::other("desktop dedupe lock poisoned"))?;
            match dedupe.lookup(&request_id) {
                Some(Some(result)) => Ok(Some(result.clone())),
                Some(None) => Err(io::Error::new(
                    io::ErrorKind::AlreadyExists,
                    "desktop request outcome is indeterminate after interruption",
                )),
                None => Ok(None),
            }
        })
        .await
        .map_err(join_error)?
    }

    async fn begin(&self, request_id: String) -> io::Result<()> {
        let dedupe = Arc::clone(&self.dedupe);
        tokio::task::spawn_blocking(move || {
            dedupe
                .lock()
                .map_err(|_| io::Error::other("desktop dedupe lock poisoned"))?
                .begin(request_id)
        })
        .await
        .map_err(join_error)?
    }

    async fn complete(&self, request_id: String, result: DesktopResult) -> io::Result<()> {
        let dedupe = Arc::clone(&self.dedupe);
        tokio::task::spawn_blocking(move || {
            dedupe
                .lock()
                .map_err(|_| io::Error::other("desktop dedupe lock poisoned"))?
                .complete(&request_id, result)
        })
        .await
        .map_err(join_error)?
    }
}

impl DesktopStateAuthority {
    pub async fn open(
        registry: Arc<DesktopRegistry>,
        generation_path: impl AsRef<Path>,
        event_capacity: usize,
    ) -> io::Result<Arc<Self>> {
        if event_capacity == 0 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "desktop event capacity must be positive",
            ));
        }
        let generation_path = generation_path.as_ref().to_owned();
        let generations = tokio::task::spawn_blocking(move || {
            crate::sessiond::GenerationAllocator::open(generation_path, 64)
        })
        .await
        .map_err(join_error)??;
        let (events, _) = broadcast::channel(event_capacity);
        Ok(Arc::new(Self {
            registry,
            transaction: Arc::new(StdMutex::new(DesktopAuthorityTransaction {
                states: None,
                generations,
                current_generation: 0,
                latest_snapshot: None,
            })),
            initialized: Notify::new(),
            events,
        }))
    }

    pub async fn initialize(&self) -> io::Result<DesktopEnvelope> {
        let deadline = StdInstant::now() + INITIAL_DEADLINE;
        let probe_deadline = tokio::time::Instant::now() + INITIAL_PROBE_BUDGET;
        let states = self
            .registry
            .initial_states_until(probe_deadline, probe_deadline + Duration::from_millis(250))
            .await;
        let transaction = Arc::clone(&self.transaction);
        let registry = Arc::clone(&self.registry);
        let event = tokio::task::spawn_blocking(move || -> io::Result<DesktopEnvelope> {
            ensure_initial_deadline(deadline)?;
            let mut transaction = transaction
                .lock()
                .map_err(|_| io::Error::other("desktop authority transaction lock poisoned"))?;
            if let Some(existing) = transaction.latest_snapshot.clone() {
                return Ok(existing);
            }
            let states = registry.localize_contract_failures(states)?;
            ensure_initial_deadline(deadline)?;
            let snapshot = registry.assemble(&states)?;
            ensure_initial_deadline(deadline)?;
            let generation = transaction.generations.next_generation()?;
            ensure_initial_deadline(deadline)?;
            let event = validated_envelope(
                generation,
                EventCause {
                    kind: EventCauseKind::Replay,
                    request_id: None,
                },
                DesktopEvent::FullSnapshot(Box::new(snapshot)),
            )?;
            ensure_initial_deadline(deadline)?;
            transaction.states = Some(states);
            transaction.current_generation = generation;
            transaction.latest_snapshot = Some(event.clone());
            Ok(event)
        })
        .await
        .map_err(join_error)??;
        self.initialized.notify_waiters();
        Ok(event)
    }

    pub async fn subscribe(&self) -> io::Result<DesktopSubscriber> {
        loop {
            let notified = self.initialized.notified();
            let transaction = Arc::clone(&self.transaction);
            let event_sender = self.events.clone();
            if let Some(subscriber) = tokio::task::spawn_blocking(move || {
                let transaction = transaction
                    .lock()
                    .map_err(|_| io::Error::other("desktop authority transaction lock poisoned"))?;
                let events = event_sender.subscribe();
                Ok::<_, io::Error>(transaction.latest_snapshot.clone().map(|snapshot| {
                    DesktopSubscriber {
                        replay: VecDeque::from([snapshot]),
                        events,
                    }
                }))
            })
            .await
            .map_err(join_error)??
            {
                return Ok(subscriber);
            }
            notified.await;
        }
    }

    pub async fn publish_domain(
        &self,
        update: DesktopDomainState,
        cause: EventCause,
    ) -> io::Result<DesktopEnvelope> {
        let transaction = Arc::clone(&self.transaction);
        let registry = Arc::clone(&self.registry);
        let events = self.events.clone();
        tokio::task::spawn_blocking(move || {
            let mut transaction = transaction
                .lock()
                .map_err(|_| io::Error::other("desktop authority transaction lock poisoned"))?;
            let current = transaction.states.as_ref().ok_or_else(|| {
                io::Error::new(
                    io::ErrorKind::NotConnected,
                    "desktop authority has not initialized",
                )
            })?;
            let domain = update.domain();
            let mut staged = current.clone();
            staged.insert(domain, update);
            let snapshot = match registry.assemble(&staged) {
                Ok(snapshot) => snapshot,
                Err(_) => {
                    staged.insert(
                        domain,
                        DesktopDomainState::terminal(
                            domain,
                            CapabilityAvailability::Parse,
                            "producer.contract-invalid",
                        )?,
                    );
                    registry.assemble(&staged)?
                }
            };
            let generation = transaction.generations.next_generation()?;
            let incremental = validated_envelope(
                generation,
                cause,
                DesktopEvent::DomainUpdate(domain_update(domain, &snapshot)),
            )?;
            let replay = validated_envelope(
                generation,
                EventCause {
                    kind: EventCauseKind::Replay,
                    request_id: None,
                },
                DesktopEvent::FullSnapshot(Box::new(snapshot)),
            )?;
            transaction.states = Some(staged);
            transaction.latest_snapshot = Some(replay);
            transaction.current_generation = generation;
            let _ = events.send(incremental.clone());
            Ok(incremental)
        })
        .await
        .map_err(join_error)?
    }

    pub async fn publish_command_result(
        &self,
        request_id: String,
        status: DesktopResultStatus,
        diagnostic: Option<CapabilityFailure>,
    ) -> io::Result<(DesktopEnvelope, DesktopResult)> {
        let transaction = Arc::clone(&self.transaction);
        let registry = Arc::clone(&self.registry);
        let events = self.events.clone();
        tokio::task::spawn_blocking(move || {
            let mut transaction = transaction
                .lock()
                .map_err(|_| io::Error::other("desktop authority transaction lock poisoned"))?;
            let states = transaction.states.as_ref().ok_or_else(|| {
                io::Error::new(
                    io::ErrorKind::NotConnected,
                    "desktop authority has not initialized",
                )
            })?;
            let snapshot = registry.assemble(states)?;
            let generation = transaction.generations.next_generation()?;
            let result = DesktopResult {
                schema_version: DESKTOP_WIRE_VERSION,
                request_id: request_id.clone(),
                generation,
                status,
                diagnostic,
            };
            validate_desktop_result(&serde_json::to_string(&result).map_err(io::Error::other)?)
                .map_err(|error| io::Error::new(io::ErrorKind::InvalidData, error))?;
            let outcome = validated_envelope(
                generation,
                EventCause {
                    kind: EventCauseKind::Request,
                    request_id: Some(request_id),
                },
                DesktopEvent::CommandResult(result.clone()),
            )?;
            let replay = validated_envelope(
                generation,
                EventCause {
                    kind: EventCauseKind::Replay,
                    request_id: None,
                },
                DesktopEvent::FullSnapshot(Box::new(snapshot)),
            )?;
            transaction.latest_snapshot = Some(replay);
            transaction.current_generation = generation;
            let _ = events.send(outcome.clone());
            Ok((outcome, result))
        })
        .await
        .map_err(join_error)?
    }

    pub fn current_generation(&self) -> u64 {
        self.transaction
            .lock()
            .map(|transaction| transaction.current_generation)
            .unwrap_or(0)
    }

    async fn checked_generation(&self, expected: u64) -> io::Result<(bool, u64)> {
        let transaction = Arc::clone(&self.transaction);
        tokio::task::spawn_blocking(move || {
            let transaction = transaction
                .lock()
                .map_err(|_| io::Error::other("desktop authority transaction lock poisoned"))?;
            Ok((
                transaction.current_generation == expected,
                transaction.current_generation,
            ))
        })
        .await
        .map_err(join_error)?
    }
}

impl fmt::Debug for DesktopRegistry {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("DesktopRegistry")
            .field("domains", &self.producers.keys().collect::<Vec<_>>())
            .finish()
    }
}

impl DesktopRegistry {
    pub fn new(producers: Vec<Arc<dyn DesktopProducer>>) -> io::Result<Self> {
        let mut by_domain = BTreeMap::new();
        for producer in producers {
            let domain = producer.domain();
            if by_domain.insert(domain, producer).is_some() {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    format!("duplicate desktop producer for {domain:?}"),
                ));
            }
        }
        let missing = DesktopDomainId::ALL
            .into_iter()
            .filter(|domain| !by_domain.contains_key(domain))
            .collect::<Vec<_>>();
        if !missing.is_empty() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                format!("missing desktop producers: {missing:?}"),
            ));
        }
        Ok(Self {
            producers: by_domain,
        })
    }

    pub async fn initial_states(&self) -> BTreeMap<DesktopDomainId, DesktopDomainState> {
        let deadline = tokio::time::Instant::now() + INITIAL_DEADLINE;
        self.initial_states_until(deadline, deadline).await
    }

    async fn initial_states_until(
        &self,
        deadline: tokio::time::Instant,
        drain_deadline: tokio::time::Instant,
    ) -> BTreeMap<DesktopDomainId, DesktopDomainState> {
        let mut tasks = tokio::task::JoinSet::new();
        for (domain, producer) in &self.producers {
            let domain = *domain;
            let producer = Arc::clone(producer);
            tasks.spawn(async move { (domain, producer.initial().await) });
        }

        let mut states = BTreeMap::new();
        let mut deadline_elapsed = false;
        while !tasks.is_empty() {
            match tokio::time::timeout_at(deadline, tasks.join_next()).await {
                Ok(Some(Ok((domain, state)))) if state.domain() == domain => {
                    states.insert(domain, state);
                }
                Ok(Some(Ok((domain, _)))) => {
                    states.insert(
                        domain,
                        DesktopDomainState::terminal(
                            domain,
                            CapabilityAvailability::Parse,
                            "producer.contract-invalid",
                        )
                        .expect("static diagnostic"),
                    );
                }
                Ok(Some(Err(_))) => {}
                Ok(None) => break,
                Err(_) => {
                    deadline_elapsed = true;
                    while !tasks.is_empty() {
                        match tokio::time::timeout_at(drain_deadline, tasks.join_next()).await {
                            Ok(Some(_)) => {}
                            Ok(None) => break,
                            Err(_) => break,
                        }
                    }
                    tasks.abort_all();
                    while tasks.join_next().await.is_some() {}
                    break;
                }
            }
        }
        for domain in DesktopDomainId::ALL {
            states.entry(domain).or_insert_with(|| {
                DesktopDomainState::terminal(
                    domain,
                    if deadline_elapsed {
                        CapabilityAvailability::Timeout
                    } else {
                        CapabilityAvailability::Error
                    },
                    if deadline_elapsed {
                        "producer initial state exceeded the two second deadline"
                    } else {
                        "producer initial task failed"
                    },
                )
                .expect("static diagnostic")
            });
        }
        states
    }

    fn localize_contract_failures(
        &self,
        states: BTreeMap<DesktopDomainId, DesktopDomainState>,
    ) -> io::Result<BTreeMap<DesktopDomainId, DesktopDomainState>> {
        let mut baseline = BTreeMap::new();
        for domain in DesktopDomainId::ALL {
            baseline.insert(
                domain,
                DesktopDomainState::terminal(
                    domain,
                    CapabilityAvailability::Unsupported,
                    "producer.probe-isolation",
                )?,
            );
        }
        let mut localized = states;
        for domain in DesktopDomainId::ALL {
            let Some(candidate) = localized.get(&domain).cloned() else {
                continue;
            };
            let mut isolated = baseline.clone();
            isolated.insert(domain, candidate);
            if self.assemble(&isolated).is_err() {
                localized.insert(
                    domain,
                    DesktopDomainState::terminal(
                        domain,
                        CapabilityAvailability::Parse,
                        "producer.contract-invalid",
                    )?,
                );
            }
        }
        if let Err(error) = self.assemble(&localized) {
            if !matches!(
                error.to_string().as_str(),
                "desktop.frame-too-large" | "desktop.aggregate-budget-exceeded"
            ) {
                return Err(error);
            }
            let mut contributions = Vec::new();
            for domain in DesktopDomainId::ALL {
                if localized
                    .get(&domain)
                    .is_none_or(|state| state.status() != CapabilityAvailability::Available)
                {
                    continue;
                }
                let mut isolated = baseline.clone();
                isolated.insert(
                    domain,
                    localized
                        .get(&domain)
                        .expect("domain came from the exhaustive registry")
                        .clone(),
                );
                let bytes = self
                    .assemble(&isolated)
                    .ok()
                    .and_then(|snapshot| serde_json::to_vec(&snapshot).ok())
                    .map_or(0, |bytes| bytes.len());
                contributions.push((bytes, domain));
            }
            contributions.sort_by(|left, right| right.cmp(left));
            for (_, domain) in contributions {
                localized.insert(
                    domain,
                    DesktopDomainState::terminal(
                        domain,
                        CapabilityAvailability::Parse,
                        "producer.aggregate-budget-exceeded",
                    )?,
                );
                if self.assemble(&localized).is_ok() {
                    return Ok(localized);
                }
            }
            return Err(error);
        }
        Ok(localized)
    }

    pub fn assemble(
        &self,
        states: &BTreeMap<DesktopDomainId, DesktopDomainState>,
    ) -> io::Result<DesktopSnapshot> {
        for domain in DesktopDomainId::ALL {
            let state = states.get(&domain).ok_or_else(|| {
                io::Error::new(
                    io::ErrorKind::InvalidInput,
                    format!("desktop state omitted {domain:?}"),
                )
            })?;
            if state.domain() != domain {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "desktop state map key does not match its state domain",
                ));
            }
        }

        let snapshot = DesktopSnapshot {
            system: DesktopSystemSnapshot {
                network: capability(states, DesktopDomainId::Network, |value| match value {
                    DesktopDomainValue::Network(value) => Some(value.clone()),
                    _ => None,
                })?,
                bluetooth: capability(states, DesktopDomainId::Bluetooth, |value| match value {
                    DesktopDomainValue::Bluetooth(value) => Some(value.clone()),
                    _ => None,
                })?,
                audio: capability(states, DesktopDomainId::Audio, |value| match value {
                    DesktopDomainValue::Audio(value) => Some(value.clone()),
                    _ => None,
                })?,
                media: capability(states, DesktopDomainId::Media, |value| match value {
                    DesktopDomainValue::Media(value) => Some(value.clone()),
                    _ => None,
                })?,
                battery: capability(states, DesktopDomainId::Battery, |value| match value {
                    DesktopDomainValue::Battery(value) => Some(value.clone()),
                    _ => None,
                })?,
                brightness: capability(states, DesktopDomainId::Brightness, |value| match value {
                    DesktopDomainValue::Brightness(value) => Some(value.clone()),
                    _ => None,
                })?,
                night_light: capability(
                    states,
                    DesktopDomainId::NightLight,
                    |value| match value {
                        DesktopDomainValue::NightLight(value) => Some(value.clone()),
                        _ => None,
                    },
                )?,
                power: capability(states, DesktopDomainId::Power, |value| match value {
                    DesktopDomainValue::Power(value) => Some(value.clone()),
                    _ => None,
                })?,
                osd: capability(states, DesktopDomainId::Osd, |value| match value {
                    DesktopDomainValue::Osd(value) => Some(value.clone()),
                    _ => None,
                })?,
                lock: capability(states, DesktopDomainId::Lock, |value| match value {
                    DesktopDomainValue::Lock(value) => Some(value.clone()),
                    _ => None,
                })?,
            },
            compositor: DesktopCompositorSnapshot {
                hyprland: capability(states, DesktopDomainId::Hyprland, |value| match value {
                    DesktopDomainValue::Hyprland(value) => Some(value.clone()),
                    _ => None,
                })?,
            },
            notifications: notifications_snapshot(states)?,
            launcher: launcher_snapshot(states)?,
            calendar: calendar_snapshot(states)?,
            weather: weather_snapshot(states)?,
            appearance: appearance_snapshot(states)?,
            resources: resources_snapshot(states)?,
            utilities: utilities_snapshot(states)?,
        };
        validate_assembled_snapshot(snapshot)
    }

    pub fn start(
        self: &Arc<Self>,
        authority: Arc<DesktopStateAuthority>,
        capacity: usize,
    ) -> io::Result<DesktopProducerRuntime> {
        if capacity == 0 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "desktop producer queue capacity must be positive",
            ));
        }
        let cancellation = CancellationToken::new();
        let (sender, mut receiver) = mpsc::channel::<DesktopDomainUpdate>(capacity);
        let mut tasks = Vec::with_capacity(self.producers.len());
        for (domain, producer) in &self.producers {
            let domain = *domain;
            let producer = Arc::clone(producer);
            let sender = sender.clone();
            let token = cancellation.child_token();
            tasks.push(tokio::spawn(async move {
                let mut backoff = Duration::from_millis(100);
                loop {
                    let started = tokio::time::Instant::now();
                    if token.is_cancelled() {
                        return;
                    }
                    let result = producer.run(sender.clone(), token.child_token()).await;
                    if token.is_cancelled() {
                        return;
                    }
                    let diagnostic = match result {
                        Ok(()) => "producer.disconnected".to_owned(),
                        Err(error) => bounded_diagnostic(error.to_string()),
                    };
                    let state = DesktopDomainState::terminal(
                        domain,
                        CapabilityAvailability::Error,
                        diagnostic,
                    )
                    .expect("producer error has a diagnostic");
                    if sender.send(DesktopDomainUpdate { state }).await.is_err() {
                        return;
                    }
                    if started.elapsed() >= Duration::from_secs(10) {
                        backoff = Duration::from_millis(100);
                    }
                    tokio::select! {
                        biased;
                        _ = token.cancelled() => return,
                        _ = tokio::time::sleep(backoff) => {}
                    }
                    backoff = backoff.saturating_mul(2).min(Duration::from_secs(5));
                }
            }));
        }
        drop(sender);
        let token = cancellation.child_token();
        let aggregator = tokio::spawn(async move {
            loop {
                tokio::select! {
                    biased;
                    _ = token.cancelled() => return Ok(()),
                    update = receiver.recv() => {
                        let Some(update) = update else { return Ok(()); };
                        authority.publish_domain(
                            update.state,
                            EventCause { kind: EventCauseKind::External, request_id: None },
                        ).await?;
                    }
                }
            }
        });
        Ok(DesktopProducerRuntime {
            cancellation,
            tasks,
            aggregator,
        })
    }
}

impl DesktopProducerRuntime {
    pub async fn shutdown(mut self, timeout: Duration) -> io::Result<()> {
        self.cancellation.cancel();
        let deadline = tokio::time::Instant::now() + timeout;
        let mut failure = None;
        let mut timed_out_at = None;
        for index in 0..self.tasks.len() {
            match tokio::time::timeout_at(deadline, &mut self.tasks[index]).await {
                Ok(Ok(())) => {}
                Ok(Err(error)) => {
                    failure.get_or_insert_with(|| {
                        io::Error::other(format!("desktop producer task failed: {error}"))
                    });
                }
                Err(_) => {
                    timed_out_at = Some(index);
                    for task in &self.tasks[index..] {
                        task.abort();
                    }
                    for task in &mut self.tasks[index..] {
                        let _ = task.await;
                    }
                    failure.get_or_insert_with(|| {
                        io::Error::new(io::ErrorKind::TimedOut, "desktop producers did not drain")
                    });
                    break;
                }
            }
        }
        if timed_out_at.is_some() {
            self.aggregator.abort();
            let _ = (&mut self.aggregator).await;
        } else {
            match tokio::time::timeout_at(deadline, &mut self.aggregator).await {
                Ok(Ok(Ok(()))) => {}
                Ok(Ok(Err(error))) => {
                    failure.get_or_insert(error);
                }
                Ok(Err(error)) => {
                    failure.get_or_insert_with(|| {
                        io::Error::other(format!("desktop producer aggregator failed: {error}"))
                    });
                }
                Err(_) => {
                    self.aggregator.abort();
                    let _ = (&mut self.aggregator).await;
                    failure.get_or_insert_with(|| {
                        io::Error::new(
                            io::ErrorKind::TimedOut,
                            "desktop producer aggregator did not drain",
                        )
                    });
                }
            }
        }
        match failure {
            Some(error) => Err(error),
            None => Ok(()),
        }
    }
}

pub fn production_registry<B: crate::daily::DailyBackend>(
    system: Arc<crate::system::SystemFacade<crate::system::ProcessCommandRunner>>,
    daily: Arc<B>,
    notifications: Arc<Mutex<crate::notifications::NotificationEventService>>,
    appearance: Arc<appearance::AppearanceService>,
    osd: crate::osd::OsdPublicationHub,
    utilities: Arc<utilities::ProductionUtilityService>,
    cancellation: CancellationToken,
) -> io::Result<Arc<DesktopRegistry>> {
    use adapters::{
        AppearanceProducer, DailyProducer, HyprlandProducer, NotificationProducer, OsdProducer,
    };
    use core::CoreSystemProducer;

    let mut producers: Vec<Arc<dyn DesktopProducer>> =
        Vec::with_capacity(DesktopDomainId::ALL.len());
    for domain in [
        DesktopDomainId::Network,
        DesktopDomainId::Bluetooth,
        DesktopDomainId::Audio,
        DesktopDomainId::Media,
        DesktopDomainId::Battery,
        DesktopDomainId::Brightness,
        DesktopDomainId::NightLight,
        DesktopDomainId::Power,
    ] {
        producers.push(Arc::new(CoreSystemProducer::production(
            domain,
            Arc::clone(&system),
        )?));
    }
    producers.push(Arc::new(OsdProducer::new(osd)));
    producers.push(Arc::new(utilities::LogindProducer));
    producers.push(
        match crate::compositor::HyprlandAdapter::discover(cancellation) {
            Ok(adapter) => Arc::new(HyprlandProducer::new(adapter)),
            Err(error) => Arc::new(adapters::hyprland_terminal(error)?),
        },
    );
    producers.push(Arc::new(NotificationProducer::new(notifications)));
    for domain in [
        DesktopDomainId::Launcher,
        DesktopDomainId::Calendar,
        DesktopDomainId::Weather,
    ] {
        producers.push(Arc::new(DailyProducer::new(domain, Arc::clone(&daily))?));
    }
    producers.push(Arc::new(AppearanceProducer::new(appearance)));
    producers.push(Arc::new(resources::ResourceProducer::default()));
    for domain in [
        DesktopDomainId::Tray,
        DesktopDomainId::Clipboard,
        DesktopDomainId::Recording,
        DesktopDomainId::IdleInhibit,
        DesktopDomainId::GameMode,
        DesktopDomainId::Screenshot,
        DesktopDomainId::ColorPicker,
    ] {
        producers.push(Arc::new(utilities::UtilityProducer::new(
            domain,
            Arc::clone(&utilities),
        )?));
    }
    Ok(Arc::new(DesktopRegistry::new(producers)?))
}

fn state(
    states: &BTreeMap<DesktopDomainId, DesktopDomainState>,
    domain: DesktopDomainId,
) -> io::Result<&DesktopDomainState> {
    states.get(&domain).ok_or_else(|| {
        io::Error::new(
            io::ErrorKind::InvalidInput,
            format!("desktop state omitted {domain:?}"),
        )
    })
}

fn capability<T>(
    states: &BTreeMap<DesktopDomainId, DesktopDomainState>,
    domain: DesktopDomainId,
    extract: impl FnOnce(&DesktopDomainValue) -> Option<T>,
) -> io::Result<DesktopCapability<T>> {
    let state = state(states, domain)?;
    if state.status == CapabilityAvailability::Available {
        let data = state.value.as_ref().and_then(extract).ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                "available domain omitted its typed value",
            )
        })?;
        Ok(DesktopCapability {
            status: CapabilityAvailability::Available,
            data: Some(data),
            diagnostic: None,
        })
    } else {
        Ok(DesktopCapability {
            status: state.status,
            data: None,
            diagnostic: Some(CapabilityFailure {
                message: public_producer_diagnostic(state.status).into(),
            }),
        })
    }
}

fn producer_availability(state: &DesktopDomainState) -> ProducerAvailability {
    ProducerAvailability {
        status: state.status,
        diagnostic: (state.status != CapabilityAvailability::Available).then(|| {
            CapabilityFailure {
                message: public_producer_diagnostic(state.status).into(),
            }
        }),
    }
}

fn available_producer() -> ProducerAvailability {
    ProducerAvailability {
        status: CapabilityAvailability::Available,
        diagnostic: None,
    }
}

fn public_producer_diagnostic(status: CapabilityAvailability) -> &'static str {
    match status {
        CapabilityAvailability::Available => "producer.available",
        CapabilityAvailability::Unavailable => "producer.unavailable",
        CapabilityAvailability::Unsupported => "producer.unsupported",
        CapabilityAvailability::PermissionDenied => "producer.permission-denied",
        CapabilityAvailability::Timeout => "producer.timeout",
        CapabilityAvailability::Parse => "producer.parse-invalid",
        CapabilityAvailability::Error => "producer.failed",
    }
}

fn notifications_snapshot(
    states: &BTreeMap<DesktopDomainId, DesktopDomainState>,
) -> io::Result<DesktopNotificationSnapshot> {
    let state = state(states, DesktopDomainId::Notifications)?;
    let mut snapshot = match state.value.as_ref() {
        Some(DesktopDomainValue::Notifications(value)) => value.clone(),
        _ => DesktopNotificationSnapshot {
            availability: producer_availability(state),
            dnd: false,
            active: Vec::new(),
        },
    };
    snapshot.availability = producer_availability(state);
    Ok(snapshot)
}

fn launcher_snapshot(
    states: &BTreeMap<DesktopDomainId, DesktopDomainState>,
) -> io::Result<DesktopLauncherSnapshot> {
    let state = state(states, DesktopDomainId::Launcher)?;
    let entries = match state.value.as_ref() {
        Some(DesktopDomainValue::Launcher(value)) => value.clone(),
        _ => Vec::new(),
    };
    Ok(DesktopLauncherSnapshot {
        availability: producer_availability(state),
        entries,
    })
}

fn calendar_snapshot(
    states: &BTreeMap<DesktopDomainId, DesktopDomainState>,
) -> io::Result<DesktopCalendarSnapshot> {
    let state = state(states, DesktopDomainId::Calendar)?;
    let snapshot = match state.value.as_ref() {
        Some(DesktopDomainValue::Calendar(value)) => value.clone(),
        _ => empty_calendar(),
    };
    Ok(DesktopCalendarSnapshot {
        availability: producer_availability(state),
        snapshot,
    })
}

fn weather_snapshot(
    states: &BTreeMap<DesktopDomainId, DesktopDomainState>,
) -> io::Result<DesktopWeatherSnapshot> {
    let state = state(states, DesktopDomainId::Weather)?;
    let snapshot = match state.value.as_ref() {
        Some(DesktopDomainValue::Weather(value)) => value.clone(),
        _ => empty_weather(),
    };
    Ok(DesktopWeatherSnapshot {
        availability: producer_availability(state),
        snapshot,
    })
}

fn appearance_snapshot(
    states: &BTreeMap<DesktopDomainId, DesktopDomainState>,
) -> io::Result<DesktopAppearanceSnapshot> {
    let state = state(states, DesktopDomainId::Appearance)?;
    let (theme, wallpaper_id) = match state.value.as_ref() {
        Some(DesktopDomainValue::Appearance {
            theme,
            wallpaper_id,
        }) => (theme.clone(), wallpaper_id.clone()),
        _ => (
            crate::theme::ThemeManager::builtin("builtin.sleepy-dark")
                .expect("static built-in theme"),
            "unavailable".into(),
        ),
    };
    Ok(DesktopAppearanceSnapshot {
        availability: producer_availability(state),
        theme,
        wallpaper_id,
    })
}

fn resources_snapshot(
    states: &BTreeMap<DesktopDomainId, DesktopDomainState>,
) -> io::Result<DesktopResourceSnapshot> {
    let state = state(states, DesktopDomainId::Resources)?;
    let samples = match state.value.as_ref() {
        Some(DesktopDomainValue::Resources(value)) => value.clone(),
        _ => Vec::new(),
    };
    Ok(DesktopResourceSnapshot {
        availability: producer_availability(state),
        samples,
    })
}

fn utilities_snapshot(
    states: &BTreeMap<DesktopDomainId, DesktopDomainState>,
) -> io::Result<DesktopUtilitySnapshot> {
    Ok(DesktopUtilitySnapshot {
        tray_items: capability(states, DesktopDomainId::Tray, |value| match value {
            DesktopDomainValue::Tray(value) => Some(value.clone()),
            _ => None,
        })?,
        clipboard_entries: capability(states, DesktopDomainId::Clipboard, |value| match value {
            DesktopDomainValue::Clipboard(value) => Some(value.clone()),
            _ => None,
        })?,
        recording: capability(states, DesktopDomainId::Recording, |value| match value {
            DesktopDomainValue::Recording(value) => Some(value.clone()),
            _ => None,
        })?,
        idle_inhibited: capability(states, DesktopDomainId::IdleInhibit, |value| match value {
            DesktopDomainValue::IdleInhibit(value) => Some(*value),
            _ => None,
        })?,
        game_mode: capability(states, DesktopDomainId::GameMode, |value| match value {
            DesktopDomainValue::GameMode(value) => Some(*value),
            _ => None,
        })?,
        screenshot: producer_availability(state(states, DesktopDomainId::Screenshot)?),
        color_picker: producer_availability(state(states, DesktopDomainId::ColorPicker)?),
    })
}

fn empty_calendar() -> CalendarSnapshot {
    CalendarSnapshot {
        schema_version: WIRE_SCHEMA_VERSION,
        provider_id: "sleepy-calendar".into(),
        window_start: "1970-01-01T00:00:00Z".into(),
        window_end: "1970-01-02T00:00:00Z".into(),
        events: Vec::new(),
        source_errors: Vec::new(),
    }
}

fn empty_weather() -> WeatherSnapshot {
    WeatherSnapshot {
        schema_version: WIRE_SCHEMA_VERSION,
        provider_id: "sleepy-weather".into(),
        location: WeatherLocation {
            display_name: "Unconfigured".into(),
            latitude: 0.0,
            longitude: 0.0,
        },
        status: ProviderStatus::Offline,
        cache: CacheStatus::Missing,
        attribution: "Weather unavailable".into(),
        forecast: Vec::new(),
        diagnostic: Some(CapabilityFailure {
            message: "weather location is not configured".into(),
        }),
    }
}

fn validate_assembled_snapshot(snapshot: DesktopSnapshot) -> io::Result<DesktopSnapshot> {
    let envelope = DesktopEnvelope {
        schema_version: DESKTOP_WIRE_VERSION,
        generation: 1,
        event_id: "00000000-0000-4000-8000-000000000001".into(),
        emitted_at: "1970-01-01T00:00:00Z".into(),
        cause: EventCause {
            kind: EventCauseKind::Lifecycle,
            request_id: None,
        },
        payload: DesktopEvent::FullSnapshot(Box::new(snapshot.clone())),
    };
    let json = serde_json::to_string(&envelope).map_err(io::Error::other)?;
    validate_serialized_budget(&json)?;
    validate_desktop_envelope(&json)
        .map_err(|error| io::Error::new(io::ErrorKind::InvalidData, error))?;
    Ok(snapshot)
}

fn domain_update(domain: DesktopDomainId, snapshot: &DesktopSnapshot) -> SdkDomainUpdate {
    match domain {
        DesktopDomainId::Network => SdkDomainUpdate::System(DesktopSystemUpdate::Network(
            snapshot.system.network.clone(),
        )),
        DesktopDomainId::Bluetooth => SdkDomainUpdate::System(DesktopSystemUpdate::Bluetooth(
            snapshot.system.bluetooth.clone(),
        )),
        DesktopDomainId::Audio => {
            SdkDomainUpdate::System(DesktopSystemUpdate::Audio(snapshot.system.audio.clone()))
        }
        DesktopDomainId::Media => {
            SdkDomainUpdate::System(DesktopSystemUpdate::Media(snapshot.system.media.clone()))
        }
        DesktopDomainId::Battery => SdkDomainUpdate::System(DesktopSystemUpdate::Battery(
            snapshot.system.battery.clone(),
        )),
        DesktopDomainId::Brightness => SdkDomainUpdate::System(DesktopSystemUpdate::Brightness(
            snapshot.system.brightness.clone(),
        )),
        DesktopDomainId::NightLight => SdkDomainUpdate::System(DesktopSystemUpdate::NightLight(
            snapshot.system.night_light.clone(),
        )),
        DesktopDomainId::Power => {
            SdkDomainUpdate::System(DesktopSystemUpdate::Power(snapshot.system.power.clone()))
        }
        DesktopDomainId::Osd => {
            SdkDomainUpdate::System(DesktopSystemUpdate::Osd(snapshot.system.osd.clone()))
        }
        DesktopDomainId::Lock => {
            SdkDomainUpdate::System(DesktopSystemUpdate::Lock(snapshot.system.lock.clone()))
        }
        DesktopDomainId::Hyprland => SdkDomainUpdate::Compositor(
            DesktopCompositorUpdate::Hyprland(snapshot.compositor.hyprland.clone()),
        ),
        DesktopDomainId::Notifications => {
            SdkDomainUpdate::Notifications(snapshot.notifications.clone())
        }
        DesktopDomainId::Launcher => SdkDomainUpdate::Launcher(snapshot.launcher.clone()),
        DesktopDomainId::Calendar => SdkDomainUpdate::Calendar(snapshot.calendar.clone()),
        DesktopDomainId::Weather => SdkDomainUpdate::Weather(snapshot.weather.clone()),
        DesktopDomainId::Appearance => SdkDomainUpdate::Appearance(snapshot.appearance.clone()),
        DesktopDomainId::Resources => SdkDomainUpdate::Resources(snapshot.resources.clone()),
        DesktopDomainId::Tray => SdkDomainUpdate::Utilities(DesktopUtilityUpdate::TrayItems(
            snapshot.utilities.tray_items.clone(),
        )),
        DesktopDomainId::Clipboard => SdkDomainUpdate::Utilities(
            DesktopUtilityUpdate::ClipboardEntries(snapshot.utilities.clipboard_entries.clone()),
        ),
        DesktopDomainId::Recording => SdkDomainUpdate::Utilities(DesktopUtilityUpdate::Recording(
            snapshot.utilities.recording.clone(),
        )),
        DesktopDomainId::IdleInhibit => SdkDomainUpdate::Utilities(
            DesktopUtilityUpdate::IdleInhibited(snapshot.utilities.idle_inhibited.clone()),
        ),
        DesktopDomainId::GameMode => SdkDomainUpdate::Utilities(DesktopUtilityUpdate::GameMode(
            snapshot.utilities.game_mode.clone(),
        )),
        DesktopDomainId::Screenshot => SdkDomainUpdate::Utilities(
            DesktopUtilityUpdate::Screenshot(snapshot.utilities.screenshot.clone()),
        ),
        DesktopDomainId::ColorPicker => SdkDomainUpdate::Utilities(
            DesktopUtilityUpdate::ColorPicker(snapshot.utilities.color_picker.clone()),
        ),
    }
}

pub(crate) fn hyprland_action_capabilities() -> HyprlandActionCapabilities {
    HyprlandActionCapabilities {
        focus_window: true,
        move_window_to_workspace: true,
        close_window: true,
        focus_workspace: true,
        move_workspace_to_monitor: true,
        toggle_fullscreen: false,
        toggle_floating: true,
        toggle_pinned: true,
        toggle_group: false,
        exit: true,
    }
}

fn validated_envelope(
    generation: u64,
    cause: EventCause,
    payload: DesktopEvent,
) -> io::Result<DesktopEnvelope> {
    let event = DesktopEnvelope {
        schema_version: DESKTOP_WIRE_VERSION,
        generation,
        event_id: uuid::Uuid::new_v4().to_string(),
        emitted_at: utc_now()?,
        cause,
        payload,
    };
    let encoded = serde_json::to_string(&event).map_err(io::Error::other)?;
    validate_serialized_budget(&encoded)?;
    validate_desktop_envelope(&encoded)
        .map_err(|error| io::Error::new(io::ErrorKind::InvalidData, error))?;
    Ok(event)
}

fn validate_serialized_budget(encoded: &str) -> io::Result<()> {
    if encoded.len() > MAX_DESKTOP_FRAME_BYTES {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "desktop.frame-too-large",
        ));
    }
    let value: serde_json::Value = serde_json::from_str(encoded).map_err(io::Error::other)?;
    let mut items = 0_usize;
    let mut string_bytes = 0_usize;
    accumulate_serialized_budget(&value, 0, &mut items, &mut string_bytes)?;
    if items > MAX_DESKTOP_SERIALIZED_ITEMS || string_bytes > MAX_DESKTOP_STRING_BYTES {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "desktop.aggregate-budget-exceeded",
        ));
    }
    Ok(())
}

fn accumulate_serialized_budget(
    value: &serde_json::Value,
    depth: usize,
    items: &mut usize,
    string_bytes: &mut usize,
) -> io::Result<()> {
    if depth > 64 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "desktop.serialization-depth-exceeded",
        ));
    }
    match value {
        serde_json::Value::String(value) => {
            *string_bytes = string_bytes.checked_add(value.len()).ok_or_else(|| {
                io::Error::new(io::ErrorKind::InvalidData, "desktop.string-budget-overflow")
            })?;
        }
        serde_json::Value::Array(values) => {
            *items = items.checked_add(values.len()).ok_or_else(|| {
                io::Error::new(io::ErrorKind::InvalidData, "desktop.item-budget-overflow")
            })?;
            for value in values {
                accumulate_serialized_budget(value, depth + 1, items, string_bytes)?;
            }
        }
        serde_json::Value::Object(values) => {
            *items = items.checked_add(values.len()).ok_or_else(|| {
                io::Error::new(io::ErrorKind::InvalidData, "desktop.item-budget-overflow")
            })?;
            for (key, value) in values {
                *string_bytes = string_bytes.checked_add(key.len()).ok_or_else(|| {
                    io::Error::new(io::ErrorKind::InvalidData, "desktop.string-budget-overflow")
                })?;
                accumulate_serialized_budget(value, depth + 1, items, string_bytes)?;
            }
        }
        _ => {}
    }
    Ok(())
}

fn ensure_initial_deadline(deadline: StdInstant) -> io::Result<()> {
    if StdInstant::now() >= deadline {
        Err(io::Error::new(
            io::ErrorKind::TimedOut,
            "desktop.initialization-timeout",
        ))
    } else {
        Ok(())
    }
}

fn utc_now() -> io::Result<String> {
    let now = unsafe { libc::time(std::ptr::null_mut()) };
    if now == -1 {
        return Err(io::Error::last_os_error());
    }
    let mut broken_down = std::mem::MaybeUninit::<libc::tm>::uninit();
    let result = unsafe { libc::gmtime_r(&now, broken_down.as_mut_ptr()) };
    if result.is_null() {
        return Err(io::Error::last_os_error());
    }
    let value = unsafe { broken_down.assume_init() };
    Ok(format!(
        "{:04}-{:02}-{:02}T{:02}:{:02}:{:02}Z",
        value.tm_year + 1900,
        value.tm_mon + 1,
        value.tm_mday,
        value.tm_hour,
        value.tm_min,
        value.tm_sec
    ))
}

fn join_error(error: tokio::task::JoinError) -> io::Error {
    io::Error::other(format!("blocking desktop task failed: {error}"))
}

impl DurableDedupe {
    fn open(path: &Path, maximum_records: usize) -> io::Result<Self> {
        if !path.is_absolute() {
            return Err(io::Error::new(
                io::ErrorKind::PermissionDenied,
                "desktop dedupe path must be absolute",
            ));
        }
        let parent = path.parent().ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::InvalidInput,
                "desktop dedupe path has no parent",
            )
        })?;
        let name = path
            .file_name()
            .ok_or_else(|| {
                io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "desktop dedupe path has no name",
                )
            })?
            .to_owned();
        let directory = crate::store::SecureDir::open_writable(parent, true)
            .map_err(|error| io::Error::other(error.to_string()))?;
        directory
            .enforce_private_directory()
            .map_err(|error| io::Error::other(error.to_string()))?;
        directory
            .validate_private_file_if_present(&name)
            .map_err(|error| io::Error::new(io::ErrorKind::PermissionDenied, error))?;
        let document = match directory
            .read_optional(&name)
            .map_err(|error| io::Error::other(error.to_string()))?
        {
            Some(bytes) => {
                if bytes.len() > 4 * 1024 * 1024 {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        "desktop dedupe document exceeds its bounded size",
                    ));
                }
                let document: DedupeDocument = serde_json::from_slice(&bytes)
                    .map_err(|error| io::Error::new(io::ErrorKind::InvalidData, error))?;
                if document.schema_version != 1 || document.records.len() > maximum_records {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        "desktop dedupe document violates its schema or capacity",
                    ));
                }
                let mut ids = std::collections::BTreeSet::new();
                if document
                    .records
                    .iter()
                    .any(|record| !ids.insert(record.request_id.clone()))
                {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        "desktop dedupe document contains duplicate request IDs",
                    ));
                }
                for record in &document.records {
                    validate_canonical_request_id(&record.request_id)?;
                    if let Some(result) = &record.result {
                        let encoded = serde_json::to_string(result).map_err(io::Error::other)?;
                        validate_desktop_result(&encoded)
                            .map_err(|error| io::Error::new(io::ErrorKind::InvalidData, error))?;
                        if result.request_id != record.request_id {
                            return Err(io::Error::new(
                                io::ErrorKind::InvalidData,
                                "desktop dedupe key/result requestId mismatch",
                            ));
                        }
                    }
                }
                document
            }
            None => DedupeDocument {
                schema_version: 1,
                records: VecDeque::new(),
            },
        };
        Ok(Self {
            directory,
            name,
            maximum_records,
            document,
            fault: None,
        })
    }

    fn lookup(&self, request_id: &str) -> Option<&Option<DesktopResult>> {
        self.document
            .records
            .iter()
            .find(|record| record.request_id == request_id)
            .map(|record| &record.result)
    }

    fn begin(&mut self, request_id: String) -> io::Result<()> {
        validate_canonical_request_id(&request_id)?;
        if self.lookup(&request_id).is_some() {
            return Err(io::Error::new(
                io::ErrorKind::AlreadyExists,
                "desktop request is already recorded",
            ));
        }
        let mut staged = self.document.clone();
        Self::make_room(&mut staged, self.maximum_records)?;
        staged.records.push_back(DedupeRecord {
            request_id,
            result: None,
        });
        self.commit(staged)
    }

    fn complete(&mut self, request_id: &str, result: DesktopResult) -> io::Result<()> {
        validate_canonical_request_id(request_id)?;
        let encoded = serde_json::to_string(&result).map_err(io::Error::other)?;
        validate_desktop_result(&encoded)
            .map_err(|error| io::Error::new(io::ErrorKind::InvalidData, error))?;
        if result.request_id != request_id {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "desktop dedupe key/result requestId mismatch",
            ));
        }
        let mut staged = self.document.clone();
        if let Some(record) = staged
            .records
            .iter_mut()
            .find(|record| record.request_id == request_id)
        {
            record.result = Some(result);
        } else {
            Self::make_room(&mut staged, self.maximum_records)?;
            staged.records.push_back(DedupeRecord {
                request_id: request_id.to_owned(),
                result: Some(result),
            });
        }
        self.commit(staged)
    }

    fn make_room(document: &mut DedupeDocument, maximum_records: usize) -> io::Result<()> {
        while document.records.len() >= maximum_records {
            let index = document
                .records
                .iter()
                .position(|record| record.result.is_some())
                .ok_or_else(|| {
                    io::Error::new(
                        io::ErrorKind::WouldBlock,
                        "desktop dedupe capacity is occupied by pending requests",
                    )
                })?;
            document.records.remove(index);
        }
        Ok(())
    }

    fn commit(&mut self, staged: DedupeDocument) -> io::Result<()> {
        let bytes = serde_json::to_vec(&staged).map_err(io::Error::other)?;
        if bytes.len() > 4 * 1024 * 1024 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "desktop dedupe document exceeds its bounded size",
            ));
        }
        let fault = self.fault.take();
        let result = self.directory.atomic_replace(
            &self.name,
            &bytes,
            || inject_dedupe_fault(fault, DedupeFaultPoint::FileSync),
            || inject_dedupe_fault(fault, DedupeFaultPoint::Rename),
            || inject_dedupe_fault(fault, DedupeFaultPoint::DirectorySync),
        );
        if let Err(error) = result {
            if let Ok(Some(on_disk)) = self.directory.read_optional(&self.name) {
                if serde_json::from_slice::<DedupeDocument>(&on_disk)
                    .ok()
                    .as_ref()
                    == Some(&staged)
                {
                    self.directory
                        .sync()
                        .map_err(|sync_error| io::Error::other(sync_error.to_string()))?;
                    self.document = staged;
                }
            }
            return Err(io::Error::other(error.to_string()));
        }
        self.document = staged;
        Ok(())
    }
}

fn inject_dedupe_fault(
    configured: Option<DedupeFaultPoint>,
    current: DedupeFaultPoint,
) -> Result<(), crate::store::StoreError> {
    if configured == Some(current) {
        Err(crate::store::StoreError::io(format!(
            "injected desktop dedupe {current:?} fault"
        )))
    } else {
        Ok(())
    }
}

fn validate_canonical_request_id(request_id: &str) -> io::Result<()> {
    let parsed = uuid::Uuid::parse_str(request_id).map_err(|_| {
        io::Error::new(
            io::ErrorKind::InvalidData,
            "desktop dedupe requestId is not a UUID",
        )
    })?;
    if parsed.to_string() != request_id {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "desktop dedupe requestId is not canonical",
        ));
    }
    Ok(())
}

fn failed_result(request_id: &str, generation: u64, diagnostic: &str) -> io::Result<DesktopResult> {
    let result = DesktopResult {
        schema_version: DESKTOP_WIRE_VERSION,
        request_id: request_id.to_owned(),
        generation,
        status: DesktopResultStatus::Failed,
        diagnostic: Some(CapabilityFailure {
            message: diagnostic.to_owned(),
        }),
    };
    validate_desktop_result(&serde_json::to_string(&result).map_err(io::Error::other)?)
        .map_err(|error| io::Error::new(io::ErrorKind::InvalidData, error))?;
    Ok(result)
}

fn bounded_diagnostic(message: String) -> String {
    const MAXIMUM: usize = 1024;
    if message.len() <= MAXIMUM {
        return message;
    }
    let mut boundary = MAXIMUM;
    while !message.is_char_boundary(boundary) {
        boundary -= 1;
    }
    message[..boundary].to_owned()
}

fn public_mutation_diagnostic(code: &str) -> &'static str {
    match code {
        "mutation.readback-missing" => "mutation.readback-missing",
        "mutation.readback-terminal" => "mutation.readback-terminal",
        "brightness.output-target-unmapped" => "brightness.output-target-unmapped",
        "capture.portal-unavailable" => "capture.portal-unavailable",
        _ => "mutation.terminal-failure",
    }
}

pub async fn serve_event_stream(
    mut stream: UnixStream,
    context: crate::sessiond::supervisor::ConnectionContext,
    authority: Arc<DesktopStateAuthority>,
) -> io::Result<()> {
    let mut subscriber = authority.subscribe().await?;
    loop {
        let event = tokio::select! {
            biased;
            _ = context.cancellation.cancelled() => return Ok(()),
            event = subscriber.recv() => event?,
        };
        let frame = serde_json::to_vec(&event).map_err(io::Error::other)?;
        context.write_frame(&mut stream, &frame).await?;
    }
}

pub async fn serve_control_stream<E: DesktopMutationExecutor>(
    stream: UnixStream,
    context: crate::sessiond::supervisor::ConnectionContext,
    authority: Arc<DesktopControlAuthority<E>>,
) -> io::Result<()> {
    let (read, mut write) = stream.into_split();
    let mut read = BufReader::new(read);
    let frame = context.read_frame(&mut read).await?;
    let input = std::str::from_utf8(&frame)
        .map_err(|error| io::Error::new(io::ErrorKind::InvalidData, error))?;
    let result = authority.handle_json(input).await?;
    let response = serde_json::to_vec(&result).map_err(io::Error::other)?;
    context.write_frame(&mut write, &response).await
}

#[cfg(test)]
mod tests {
    use std::os::unix::fs::PermissionsExt;

    use super::*;

    fn result(request_id: &str) -> DesktopResult {
        DesktopResult {
            schema_version: DESKTOP_WIRE_VERSION,
            request_id: request_id.into(),
            generation: 7,
            status: DesktopResultStatus::Succeeded,
            diagnostic: None,
        }
    }

    #[test]
    fn dedupe_fault_boundaries_never_leave_memory_ahead_of_disk() {
        let request_id = "00000000-0000-4000-8000-000000000041";
        for (fault, materialized) in [
            (DedupeFaultPoint::FileSync, false),
            (DedupeFaultPoint::Rename, true),
            (DedupeFaultPoint::DirectorySync, true),
        ] {
            let temp = tempfile::tempdir().unwrap();
            let path = temp.path().join("dedupe.json");
            let mut dedupe = DurableDedupe::open(&path, 8).unwrap();
            dedupe.fault = Some(fault);
            assert!(dedupe.begin(request_id.into()).is_err());
            assert_eq!(
                dedupe.lookup(request_id).is_some(),
                materialized,
                "{fault:?}"
            );

            let reopened = DurableDedupe::open(&path, 8).unwrap();
            assert_eq!(
                reopened.lookup(request_id).is_some(),
                materialized,
                "{fault:?}"
            );
        }
    }

    #[test]
    fn dedupe_completion_faults_reconcile_key_and_correlated_result() {
        let request_id = "00000000-0000-4000-8000-000000000042";
        for fault in [
            DedupeFaultPoint::FileSync,
            DedupeFaultPoint::Rename,
            DedupeFaultPoint::DirectorySync,
        ] {
            let temp = tempfile::tempdir().unwrap();
            let path = temp.path().join("dedupe.json");
            let mut dedupe = DurableDedupe::open(&path, 8).unwrap();
            dedupe.begin(request_id.into()).unwrap();
            dedupe.fault = Some(fault);
            assert!(dedupe.complete(request_id, result(request_id)).is_err());

            let reopened = DurableDedupe::open(&path, 8).unwrap();
            match (fault, reopened.lookup(request_id).unwrap()) {
                (DedupeFaultPoint::FileSync, None) => {}
                (_, Some(value)) => assert_eq!(value.request_id, request_id),
                _ => panic!("unexpected dedupe state after {fault:?}"),
            }
        }
    }

    #[test]
    fn dedupe_load_rejects_invalid_ids_results_and_key_correlation() {
        let valid = "00000000-0000-4000-8000-000000000043";
        let other = "00000000-0000-4000-8000-000000000044";
        let cases = [
            DedupeRecord {
                request_id: "not-a-request-id".into(),
                result: None,
            },
            DedupeRecord {
                request_id: valid.into(),
                result: Some(result(other)),
            },
            DedupeRecord {
                request_id: valid.into(),
                result: Some(DesktopResult {
                    diagnostic: Some(CapabilityFailure {
                        message: "invalid diagnostic on success".into(),
                    }),
                    ..result(valid)
                }),
            },
        ];
        for record in cases {
            let temp = tempfile::tempdir().unwrap();
            let path = temp.path().join("dedupe.json");
            let document = DedupeDocument {
                schema_version: 1,
                records: VecDeque::from([record]),
            };
            std::fs::write(&path, serde_json::to_vec(&document).unwrap()).unwrap();
            std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o600)).unwrap();
            let error = match DurableDedupe::open(&path, 8) {
                Ok(_) => panic!("invalid dedupe record was accepted"),
                Err(error) => error,
            };
            assert_eq!(error.kind(), io::ErrorKind::InvalidData);
        }
    }

    #[test]
    fn serialized_desktop_budget_is_conservatively_below_transport_limit() {
        let oversized = serde_json::json!({
            "payload": "x".repeat(MAX_DESKTOP_FRAME_BYTES),
        });
        let encoded = serde_json::to_string(&oversized).unwrap();
        assert_eq!(
            validate_serialized_budget(&encoded).unwrap_err().kind(),
            io::ErrorKind::InvalidData
        );

        let too_many_items =
            serde_json::to_string(&vec![0; MAX_DESKTOP_SERIALIZED_ITEMS + 1]).unwrap();
        assert_eq!(
            validate_serialized_budget(&too_many_items)
                .unwrap_err()
                .kind(),
            io::ErrorKind::InvalidData
        );
    }
}
