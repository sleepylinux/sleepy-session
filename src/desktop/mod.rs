use std::{
    collections::{BTreeMap, VecDeque},
    ffi::OsString,
    fmt, io,
    path::Path,
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc, Mutex as StdMutex,
    },
    time::Duration,
};

use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use sleepy_sdk::{
    validate_desktop_envelope, validate_desktop_request, validate_desktop_result, AudioSnapshot,
    BatterySnapshot, BluetoothSnapshot, CacheStatus, CalendarSnapshot, CapabilityAvailability,
    CapabilityFailure, ClipboardEntry, DesktopAppearanceSnapshot, DesktopCalendarSnapshot,
    DesktopCapability, DesktopCompositorSnapshot, DesktopCompositorUpdate,
    DesktopDomainUpdate as SdkDomainUpdate, DesktopEnvelope, DesktopEvent, DesktopLauncherSnapshot,
    DesktopNotificationSnapshot, DesktopOsdSnapshot, DesktopPowerSnapshot, DesktopRequest,
    DesktopResourceSnapshot, DesktopResult, DesktopResultStatus, DesktopSnapshot,
    DesktopSystemSnapshot, DesktopSystemUpdate, DesktopUtilitySnapshot, DesktopWeatherSnapshot,
    DisplaySnapshot, EventCause, EventCauseKind, HyprlandSnapshot, LauncherEntry, LockState,
    MediaSnapshot, NetworkSnapshot, PowerProfile, ProducerAvailability, ProviderStatus,
    RecordingState, RecordingStatus, ResourceSample, ThemeDocument, TrayItem, WeatherLocation,
    WeatherSnapshot, DESKTOP_WIRE_VERSION, WIRE_SCHEMA_VERSION,
};
use tokio::sync::{broadcast, mpsc, Mutex, Notify, RwLock};
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

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum DesktopDomainId {
    Network,
    Bluetooth,
    Audio,
    Media,
    Battery,
    Display,
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
}

impl DesktopDomainId {
    pub const ALL: [Self; 21] = [
        Self::Network,
        Self::Bluetooth,
        Self::Audio,
        Self::Media,
        Self::Battery,
        Self::Display,
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
    ];
}

#[derive(Debug, Clone, PartialEq)]
pub enum DesktopDomainValue {
    Network(NetworkSnapshot),
    Bluetooth(BluetoothSnapshot),
    Audio(AudioSnapshot),
    Media(MediaSnapshot),
    Battery(BatterySnapshot),
    Display(DisplaySnapshot),
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
}

impl DesktopDomainValue {
    pub fn domain(&self) -> DesktopDomainId {
        match self {
            Self::Network(_) => DesktopDomainId::Network,
            Self::Bluetooth(_) => DesktopDomainId::Bluetooth,
            Self::Audio(_) => DesktopDomainId::Audio,
            Self::Media(_) => DesktopDomainId::Media,
            Self::Battery(_) => DesktopDomainId::Battery,
            Self::Display(_) => DesktopDomainId::Display,
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
            DesktopDomainId::Display => Self::Display(DisplaySnapshot {
                brightness: None,
                night_light_enabled: false,
            }),
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
    states: Mutex<Option<BTreeMap<DesktopDomainId, DesktopDomainState>>>,
    generations: Arc<StdMutex<crate::sessiond::GenerationAllocator>>,
    current_generation: AtomicU64,
    latest_snapshot: RwLock<Option<DesktopEnvelope>>,
    initialized: Notify,
    events: broadcast::Sender<DesktopEnvelope>,
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
    ) -> Result<Vec<DesktopDomainState>, ProducerError>;
}

pub struct DesktopControlAuthority<E: DesktopMutationExecutor> {
    state: Arc<DesktopStateAuthority>,
    executor: Arc<E>,
    dedupe: Arc<StdMutex<DurableDedupe>>,
    serial: Mutex<()>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct DedupeDocument {
    schema_version: u32,
    records: VecDeque<DedupeRecord>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
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

        if request.expected_generation != self.state.current_generation() {
            let result = failed_result(
                &request.request_id,
                self.state.current_generation(),
                "stale desktop generation",
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
            Ok(readbacks) if !readbacks.is_empty() => {
                for readback in readbacks {
                    self.state.publish_domain(readback, cause.clone()).await?;
                }
                (DesktopResultStatus::Succeeded, None)
            }
            Ok(_) => (
                DesktopResultStatus::Failed,
                Some(CapabilityFailure {
                    message: "mutation completed without confirmed readback".into(),
                }),
            ),
            Err(error) => (
                DesktopResultStatus::Failed,
                Some(CapabilityFailure {
                    message: bounded_diagnostic(error.to_string()),
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
            states: Mutex::new(None),
            generations: Arc::new(StdMutex::new(generations)),
            current_generation: AtomicU64::new(0),
            latest_snapshot: RwLock::new(None),
            initialized: Notify::new(),
            events,
        }))
    }

    pub async fn initialize(&self) -> io::Result<DesktopEnvelope> {
        let mut states_guard = self.states.lock().await;
        if let Some(existing) = self.latest_snapshot.read().await.clone() {
            return Ok(existing);
        }
        let states = self.registry.initial_states().await;
        let snapshot = self.registry.assemble(&states)?;
        let generation = self.allocate_generation().await?;
        let event = validated_envelope(
            generation,
            EventCause {
                kind: EventCauseKind::Replay,
                request_id: None,
            },
            DesktopEvent::FullSnapshot(Box::new(snapshot)),
        )?;
        *states_guard = Some(states);
        *self.latest_snapshot.write().await = Some(event.clone());
        self.current_generation.store(generation, Ordering::Release);
        self.initialized.notify_waiters();
        Ok(event)
    }

    pub async fn subscribe(&self) -> io::Result<DesktopSubscriber> {
        loop {
            let events = self.events.subscribe();
            if let Some(snapshot) = self.latest_snapshot.read().await.clone() {
                return Ok(DesktopSubscriber {
                    replay: VecDeque::from([snapshot]),
                    events,
                });
            }
            self.initialized.notified().await;
        }
    }

    pub async fn publish_domain(
        &self,
        update: DesktopDomainState,
        cause: EventCause,
    ) -> io::Result<DesktopEnvelope> {
        let mut states_guard = self.states.lock().await;
        let states = states_guard.as_mut().ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::NotConnected,
                "desktop authority has not initialized",
            )
        })?;
        let domain = update.domain();
        states.insert(domain, update);
        let snapshot = match self.registry.assemble(states) {
            Ok(snapshot) => snapshot,
            Err(_) => {
                states.insert(
                    domain,
                    DesktopDomainState::terminal(
                        domain,
                        CapabilityAvailability::Parse,
                        "producer update violated the desktop wire contract",
                    )?,
                );
                self.registry.assemble(states)?
            }
        };
        let generation = self.allocate_generation().await?;
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
        *self.latest_snapshot.write().await = Some(replay);
        self.current_generation.store(generation, Ordering::Release);
        let _ = self.events.send(incremental.clone());
        Ok(incremental)
    }

    pub async fn publish_command_result(
        &self,
        request_id: String,
        status: DesktopResultStatus,
        diagnostic: Option<CapabilityFailure>,
    ) -> io::Result<(DesktopEnvelope, DesktopResult)> {
        let states_guard = self.states.lock().await;
        let states = states_guard.as_ref().ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::NotConnected,
                "desktop authority has not initialized",
            )
        })?;
        let snapshot = self.registry.assemble(states)?;
        let generation = self.allocate_generation().await?;
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
        *self.latest_snapshot.write().await = Some(replay);
        self.current_generation.store(generation, Ordering::Release);
        let _ = self.events.send(outcome.clone());
        Ok((outcome, result))
    }

    pub fn current_generation(&self) -> u64 {
        self.current_generation.load(Ordering::Acquire)
    }

    async fn allocate_generation(&self) -> io::Result<u64> {
        let generations = Arc::clone(&self.generations);
        tokio::task::spawn_blocking(move || {
            generations
                .lock()
                .map_err(|_| io::Error::other("desktop generation lock poisoned"))?
                .next_generation()
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
                            CapabilityAvailability::Error,
                            "producer returned an invalid initial state",
                        )
                        .expect("static diagnostic"),
                    );
                }
                Ok(Some(Err(_))) => {}
                Ok(None) => break,
                Err(_) => {
                    deadline_elapsed = true;
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
                display: capability(states, DesktopDomainId::Display, |value| match value {
                    DesktopDomainValue::Display(value) => Some(value.clone()),
                    _ => None,
                })?,
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
                if let Err(error) = producer.run(sender.clone(), token).await {
                    let state = DesktopDomainState::terminal(
                        domain,
                        CapabilityAvailability::Error,
                        bounded_diagnostic(error.to_string()),
                    )
                    .expect("producer error has a diagnostic");
                    let _ = sender.send(DesktopDomainUpdate { state }).await;
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
        for index in 0..self.tasks.len() {
            match tokio::time::timeout_at(deadline, &mut self.tasks[index]).await {
                Ok(Ok(())) => {}
                Ok(Err(error)) => {
                    return Err(io::Error::other(format!(
                        "desktop producer task failed: {error}"
                    )));
                }
                Err(_) => {
                    for task in &self.tasks[index..] {
                        task.abort();
                    }
                    self.aggregator.abort();
                    return Err(io::Error::new(
                        io::ErrorKind::TimedOut,
                        "desktop producers did not drain",
                    ));
                }
            }
        }
        match tokio::time::timeout_at(deadline, &mut self.aggregator).await {
            Ok(Ok(result)) => result,
            Ok(Err(error)) => Err(io::Error::other(format!(
                "desktop producer aggregator failed: {error}"
            ))),
            Err(_) => {
                self.aggregator.abort();
                Err(io::Error::new(
                    io::ErrorKind::TimedOut,
                    "desktop producer aggregator did not drain",
                ))
            }
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
        DesktopDomainId::Display,
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
                message: state
                    .diagnostic
                    .clone()
                    .unwrap_or_else(|| "desktop producer failed without a diagnostic".into()),
            }),
        })
    }
}

fn producer_availability(state: &DesktopDomainState) -> ProducerAvailability {
    ProducerAvailability {
        status: state.status,
        diagnostic: (state.status != CapabilityAvailability::Available).then(|| {
            CapabilityFailure {
                message: state
                    .diagnostic
                    .clone()
                    .unwrap_or_else(|| "desktop producer failed without a diagnostic".into()),
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
    let utility_domains = [
        DesktopDomainId::Tray,
        DesktopDomainId::Clipboard,
        DesktopDomainId::Recording,
        DesktopDomainId::IdleInhibit,
        DesktopDomainId::GameMode,
    ];
    let failed = utility_domains.into_iter().find_map(|domain| {
        let state = states.get(&domain)?;
        (state.status != CapabilityAvailability::Available).then_some(state)
    });
    let availability = failed.map_or_else(available_producer, producer_availability);
    let tray_items = match state(states, DesktopDomainId::Tray)?.value.as_ref() {
        Some(DesktopDomainValue::Tray(value)) => value.clone(),
        _ => Vec::new(),
    };
    let clipboard_entries = match state(states, DesktopDomainId::Clipboard)?.value.as_ref() {
        Some(DesktopDomainValue::Clipboard(value)) => value.clone(),
        _ => Vec::new(),
    };
    let recording = match state(states, DesktopDomainId::Recording)?.value.as_ref() {
        Some(DesktopDomainValue::Recording(value)) => value.clone(),
        _ => RecordingState {
            status: RecordingStatus::Inactive,
            recording_id: None,
            output_id: None,
        },
    };
    let idle_inhibited = match state(states, DesktopDomainId::IdleInhibit)?.value.as_ref() {
        Some(DesktopDomainValue::IdleInhibit(value)) => *value,
        _ => false,
    };
    let game_mode = match state(states, DesktopDomainId::GameMode)?.value.as_ref() {
        Some(DesktopDomainValue::GameMode(value)) => *value,
        _ => false,
    };
    Ok(DesktopUtilitySnapshot {
        availability,
        tray_items,
        clipboard_entries,
        recording,
        idle_inhibited,
        game_mode,
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
        DesktopDomainId::Display => SdkDomainUpdate::System(DesktopSystemUpdate::Display(
            snapshot.system.display.clone(),
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
        DesktopDomainId::Tray
        | DesktopDomainId::Clipboard
        | DesktopDomainId::Recording
        | DesktopDomainId::IdleInhibit
        | DesktopDomainId::GameMode => SdkDomainUpdate::Utilities(snapshot.utilities.clone()),
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
    validate_desktop_envelope(&encoded)
        .map_err(|error| io::Error::new(io::ErrorKind::InvalidData, error))?;
    Ok(event)
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
        if self.lookup(&request_id).is_some() {
            return Err(io::Error::new(
                io::ErrorKind::AlreadyExists,
                "desktop request is already recorded",
            ));
        }
        self.make_room()?;
        self.document.records.push_back(DedupeRecord {
            request_id,
            result: None,
        });
        self.persist()
    }

    fn complete(&mut self, request_id: &str, result: DesktopResult) -> io::Result<()> {
        if let Some(record) = self
            .document
            .records
            .iter_mut()
            .find(|record| record.request_id == request_id)
        {
            record.result = Some(result);
        } else {
            self.make_room()?;
            self.document.records.push_back(DedupeRecord {
                request_id: request_id.to_owned(),
                result: Some(result),
            });
        }
        self.persist()
    }

    fn make_room(&mut self) -> io::Result<()> {
        while self.document.records.len() >= self.maximum_records {
            let index = self
                .document
                .records
                .iter()
                .position(|record| record.result.is_some())
                .ok_or_else(|| {
                    io::Error::new(
                        io::ErrorKind::WouldBlock,
                        "desktop dedupe capacity is occupied by pending requests",
                    )
                })?;
            self.document.records.remove(index);
        }
        Ok(())
    }

    fn persist(&self) -> io::Result<()> {
        let bytes = serde_json::to_vec(&self.document).map_err(io::Error::other)?;
        if bytes.len() > 4 * 1024 * 1024 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "desktop dedupe document exceeds its bounded size",
            ));
        }
        self.directory
            .atomic_replace(&self.name, &bytes, || Ok(()), || Ok(()), || Ok(()))
            .map_err(|error| io::Error::other(error.to_string()))
    }
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
