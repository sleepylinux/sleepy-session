use std::{
    collections::{BTreeMap, VecDeque},
    ffi::OsString,
    fmt, io,
    path::Path,
    sync::{
        atomic::{AtomicBool, AtomicU64, Ordering},
        Arc, Mutex as StdMutex,
    },
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
const PRODUCER_SHUTDOWN_TOLERANCE: Duration = Duration::from_millis(100);
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
    observation_revision: Option<u64>,
}

impl DesktopDomainUpdate {
    pub fn unversioned(state: DesktopDomainState) -> Self {
        Self {
            state,
            observation_revision: None,
        }
    }
}

pub struct DesktopObservation {
    domain: DesktopDomainId,
    revision: u64,
}

impl DesktopObservation {
    pub fn finish(self, state: DesktopDomainState) -> io::Result<DesktopDomainUpdate> {
        if state.domain() != self.domain {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "desktop observation belongs to a different producer",
            ));
        }
        Ok(DesktopDomainUpdate {
            state,
            observation_revision: Some(self.revision),
        })
    }
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
        context: DesktopProducerContext,
    ) -> Result<(), ProducerError>;
}

#[derive(Clone)]
pub struct DesktopProducerContext {
    cancellation: CancellationToken,
    blocking_cancellation: crate::system::RunCancellation,
    blocking_tasks: Arc<BlockingTaskTracker>,
    observation_domain: Option<DesktopDomainId>,
    observation_revision: Option<Arc<AtomicU64>>,
}

impl DesktopProducerContext {
    fn new(cancellation: CancellationToken, blocking_tasks: Arc<BlockingTaskTracker>) -> Self {
        Self {
            cancellation,
            blocking_cancellation: crate::system::RunCancellation::new(),
            blocking_tasks,
            observation_domain: None,
            observation_revision: None,
        }
    }

    fn for_domain(
        cancellation: CancellationToken,
        blocking_tasks: Arc<BlockingTaskTracker>,
        domain: DesktopDomainId,
        observation_revision: Arc<AtomicU64>,
    ) -> Self {
        Self {
            cancellation,
            blocking_cancellation: crate::system::RunCancellation::new(),
            blocking_tasks,
            observation_domain: Some(domain),
            observation_revision: Some(observation_revision),
        }
    }

    pub async fn cancelled(&self) {
        self.cancellation.cancelled().await;
    }

    pub fn is_cancelled(&self) -> bool {
        self.blocking_cancellation.is_cancelled() || self.cancellation.is_cancelled()
    }

    pub fn begin_observation(&self) -> DesktopObservation {
        DesktopObservation {
            domain: self
                .observation_domain
                .expect("producer runtime contexts carry an observation domain"),
            revision: self
                .observation_revision
                .as_ref()
                .expect("producer runtime contexts carry an observation revision")
                .load(Ordering::SeqCst),
        }
    }

    pub fn spawn_blocking<F, T>(
        &self,
        deadline: StdInstant,
        operation: F,
    ) -> tokio::task::JoinHandle<T>
    where
        F: FnOnce(crate::system::RunControl) -> T + Send + 'static,
        T: Send + 'static,
    {
        if self.cancellation.is_cancelled() || self.blocking_tasks.is_cancelled() {
            self.blocking_cancellation.cancel();
        }
        let control = crate::system::RunControl::for_cancellation(
            deadline,
            self.blocking_cancellation.clone(),
        );
        let worker = tokio::task::spawn_blocking(move || operation(control));
        self.blocking_tasks
            .register(worker.abort_handle(), self.blocking_cancellation.clone());
        worker
    }

    fn cancel(&self) {
        self.blocking_cancellation.cancel();
        self.cancellation.cancel();
    }
}

#[derive(Default)]
struct BlockingTaskTracker {
    cancelled: AtomicBool,
    tasks: StdMutex<Vec<BlockingTask>>,
}

struct BlockingTask {
    abort: tokio::task::AbortHandle,
    cancellation: crate::system::RunCancellation,
}

impl BlockingTaskTracker {
    fn register(
        &self,
        abort: tokio::task::AbortHandle,
        cancellation: crate::system::RunCancellation,
    ) {
        let mut tasks = self.tasks.lock().unwrap_or_else(|error| error.into_inner());
        tasks.retain(|task| !task.abort.is_finished());
        if self.cancelled.load(Ordering::SeqCst) {
            cancellation.cancel();
            abort.abort();
        }
        tasks.push(BlockingTask {
            abort,
            cancellation,
        });
    }

    fn is_cancelled(&self) -> bool {
        self.cancelled.load(Ordering::SeqCst)
    }

    fn cancel_and_abort_queued(&self) {
        self.cancelled.store(true, Ordering::SeqCst);
        let mut tasks = self.tasks.lock().unwrap_or_else(|error| error.into_inner());
        tasks.retain(|task| {
            if task.abort.is_finished() {
                false
            } else {
                task.cancellation.cancel();
                task.abort.abort();
                true
            }
        });
    }

    fn is_idle(&self) -> bool {
        let mut tasks = self.tasks.lock().unwrap_or_else(|error| error.into_inner());
        tasks.retain(|task| !task.abort.is_finished());
        tasks.is_empty()
    }

    async fn wait_until_idle(&self, deadline: tokio::time::Instant) -> bool {
        loop {
            if self.is_idle() {
                return true;
            }
            if tokio::time::Instant::now() >= deadline {
                return false;
            }
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
    }

    async fn drain(&self) {
        while !self.is_idle() {
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
    }
}

pub struct DesktopRegistry {
    producers: BTreeMap<DesktopDomainId, Arc<dyn DesktopProducer>>,
}

pub struct DesktopProducerRuntime {
    cancellation: CancellationToken,
    tasks: Vec<ProducerTask>,
    aggregator: tokio::task::JoinHandle<io::Result<()>>,
    aggregator_context: DesktopProducerContext,
    aggregator_blocking_tasks: Arc<BlockingTaskTracker>,
}

struct ProducerTask {
    wrapper: tokio::task::JoinHandle<()>,
    active_attempt: Arc<ActiveAttemptSlot>,
    blocking_tasks: Arc<BlockingTaskTracker>,
}

struct ActiveAttemptSlot {
    next_id: AtomicU64,
    active: StdMutex<Option<ActiveAttempt>>,
}

struct ActiveAttempt {
    id: u64,
    abort: tokio::task::AbortHandle,
    context: DesktopProducerContext,
}

impl ActiveAttemptSlot {
    fn new() -> Self {
        Self {
            next_id: AtomicU64::new(1),
            active: StdMutex::new(None),
        }
    }

    fn begin<T>(
        &self,
        parent: &CancellationToken,
        blocking_tasks: Arc<BlockingTaskTracker>,
        domain: DesktopDomainId,
        observation_revision: Arc<AtomicU64>,
        spawn: impl FnOnce(DesktopProducerContext) -> tokio::task::JoinHandle<T>,
    ) -> Option<(u64, tokio::task::JoinHandle<T>)>
    where
        T: Send + 'static,
    {
        let mut active = self
            .active
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        if parent.is_cancelled() {
            return None;
        }
        let id = self.next_id.fetch_add(1, Ordering::Relaxed);
        let context = DesktopProducerContext::for_domain(
            parent.child_token(),
            blocking_tasks,
            domain,
            observation_revision,
        );
        let attempt = spawn(context.clone());
        *active = Some(ActiveAttempt {
            id,
            abort: attempt.abort_handle(),
            context,
        });
        Some((id, attempt))
    }

    fn finish(&self, id: u64) {
        let mut active = self
            .active
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        if active.as_ref().is_some_and(|attempt| attempt.id == id) {
            active
                .as_ref()
                .expect("active attempt checked")
                .context
                .cancel();
            active.take();
        }
    }

    fn cancel(&self) {
        if let Some(attempt) = self
            .active
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .as_ref()
        {
            attempt.context.cancel();
        }
    }

    fn cancel_and_abort(&self) {
        if let Some(attempt) = self
            .active
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .as_ref()
        {
            attempt.context.cancel();
            attempt.abort.abort();
        }
    }
}

pub struct DesktopStateAuthority {
    registry: Arc<DesktopRegistry>,
    transaction: Arc<StdMutex<DesktopAuthorityTransaction>>,
    startup_deadline: StdInstant,
    initialization: Mutex<()>,
    initialized: Notify,
    events: broadcast::Sender<DesktopEnvelope>,
    observation_revisions: Arc<BTreeMap<DesktopDomainId, Arc<AtomicU64>>>,
    #[cfg(test)]
    publication_hook_scope: Arc<()>,
}

struct DesktopAuthorityTransaction {
    states: Option<BTreeMap<DesktopDomainId, DesktopDomainState>>,
    generations: crate::sessiond::GenerationAllocator,
    current_generation: u64,
    latest_snapshot: Option<DesktopEnvelope>,
}

struct PublicationEnvironment {
    registry: Arc<DesktopRegistry>,
    events: broadcast::Sender<DesktopEnvelope>,
    observation_revisions: Arc<BTreeMap<DesktopDomainId, Arc<AtomicU64>>>,
    hook_scope: PublicationHookScope,
}

#[cfg(test)]
#[derive(Clone, Copy, PartialEq, Eq)]
struct PublicationHookScope(usize);

#[cfg(not(test))]
#[derive(Clone, Copy)]
struct PublicationHookScope;

enum InitializationAssembly {
    Existing(DesktopEnvelope),
    Staged {
        states: BTreeMap<DesktopDomainId, DesktopDomainState>,
        generation: u64,
        event: DesktopEnvelope,
    },
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
        let startup_deadline = StdInstant::now() + INITIAL_DEADLINE;
        if event_capacity == 0 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "desktop event capacity must be positive",
            ));
        }
        let generation_path = generation_path.as_ref().to_owned();
        let generation_worker = tokio::task::spawn_blocking(move || {
            crate::sessiond::GenerationAllocator::open(generation_path, 64)
        });
        let generations = tokio::time::timeout_at(
            tokio::time::Instant::from_std(startup_deadline),
            generation_worker,
        )
        .await
        .map_err(|_| io::Error::new(io::ErrorKind::TimedOut, "desktop.initialization-timeout"))?
        .map_err(join_error)??;
        let (events, _) = broadcast::channel(event_capacity);
        let observation_revisions = Arc::new(
            DesktopDomainId::ALL
                .into_iter()
                .map(|domain| (domain, Arc::new(AtomicU64::new(0))))
                .collect(),
        );
        Ok(Arc::new(Self {
            registry,
            transaction: Arc::new(StdMutex::new(DesktopAuthorityTransaction {
                states: None,
                generations,
                current_generation: 0,
                latest_snapshot: None,
            })),
            startup_deadline,
            initialization: Mutex::new(()),
            initialized: Notify::new(),
            events,
            observation_revisions,
            #[cfg(test)]
            publication_hook_scope: Arc::new(()),
        }))
    }

    fn publication_hook_scope(&self) -> PublicationHookScope {
        publication_hook_scope(self)
    }

    pub async fn initialize(&self) -> io::Result<DesktopEnvelope> {
        let _initialization = self.initialization.lock().await;
        if let Some(existing) = self
            .transaction
            .lock()
            .map_err(|_| io::Error::other("desktop authority transaction lock poisoned"))?
            .latest_snapshot
            .clone()
        {
            return Ok(existing);
        }
        let deadline = self.startup_deadline;
        ensure_initial_deadline(deadline)?;
        let overall_deadline = tokio::time::Instant::from_std(deadline);
        let probe_deadline =
            (tokio::time::Instant::now() + INITIAL_PROBE_BUDGET).min(overall_deadline);
        let drain_deadline = (probe_deadline + Duration::from_millis(250)).min(overall_deadline);
        let states = self
            .registry
            .initial_states_until(probe_deadline, drain_deadline)
            .await;
        let transaction = Arc::clone(&self.transaction);
        let registry = Arc::clone(&self.registry);
        let assembly = tokio::task::spawn_blocking(move || -> io::Result<_> {
            ensure_initial_deadline(deadline)?;
            let mut transaction = transaction
                .lock()
                .map_err(|_| io::Error::other("desktop authority transaction lock poisoned"))?;
            if let Some(existing) = transaction.latest_snapshot.clone() {
                return Ok(InitializationAssembly::Existing(existing));
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
            Ok(InitializationAssembly::Staged {
                states,
                generation,
                event,
            })
        });
        let assembly = tokio::time::timeout_at(overall_deadline, assembly)
            .await
            .map_err(|_| io::Error::new(io::ErrorKind::TimedOut, "desktop.initialization-timeout"))?
            .map_err(join_error)??;
        let event = match assembly {
            InitializationAssembly::Existing(event) => event,
            InitializationAssembly::Staged {
                states,
                generation,
                event,
            } => {
                ensure_initial_deadline(deadline)?;
                let mut transaction = self.transaction.try_lock().map_err(|error| match error {
                    std::sync::TryLockError::Poisoned(_) => {
                        io::Error::other("desktop authority transaction lock poisoned")
                    }
                    std::sync::TryLockError::WouldBlock => {
                        io::Error::new(io::ErrorKind::TimedOut, "desktop.initialization-timeout")
                    }
                })?;
                ensure_initial_deadline(deadline)?;
                if let Some(existing) = transaction.latest_snapshot.clone() {
                    existing
                } else {
                    transaction.states = Some(states);
                    transaction.current_generation = generation;
                    transaction.latest_snapshot = Some(event.clone());
                    event
                }
            }
        };
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
        let observation_revisions = Arc::clone(&self.observation_revisions);
        let hook_scope = self.publication_hook_scope();
        tokio::task::spawn_blocking(move || {
            publish_domain_transaction(
                transaction,
                PublicationEnvironment {
                    registry,
                    events,
                    observation_revisions,
                    hook_scope,
                },
                DesktopDomainUpdate::unversioned(update),
                cause,
                None,
            )?
            .ok_or_else(|| io::Error::other("unversioned desktop publication was discarded"))
        })
        .await
        .map_err(join_error)?
    }

    async fn publish_domain_controlled(
        &self,
        update: DesktopDomainUpdate,
        cause: EventCause,
        context: &DesktopProducerContext,
    ) -> io::Result<Option<DesktopEnvelope>> {
        let transaction = Arc::clone(&self.transaction);
        let registry = Arc::clone(&self.registry);
        let events = self.events.clone();
        let observation_revisions = Arc::clone(&self.observation_revisions);
        let hook_scope = self.publication_hook_scope();
        context
            .spawn_blocking(StdInstant::now() + INITIAL_DEADLINE, move |control| {
                publish_domain_transaction(
                    transaction,
                    PublicationEnvironment {
                        registry,
                        events,
                        observation_revisions,
                        hook_scope,
                    },
                    update,
                    cause,
                    Some(&control),
                )
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

fn publish_domain_transaction(
    transaction: Arc<StdMutex<DesktopAuthorityTransaction>>,
    environment: PublicationEnvironment,
    update: DesktopDomainUpdate,
    cause: EventCause,
    control: Option<&crate::system::RunControl>,
) -> io::Result<Option<DesktopEnvelope>> {
    ensure_publication_active(control)?;
    let mut transaction = lock_publication_transaction(&transaction, control)?;
    ensure_publication_active(control)?;
    let current = transaction.states.as_ref().ok_or_else(|| {
        io::Error::new(
            io::ErrorKind::NotConnected,
            "desktop authority has not initialized",
        )
    })?;
    let domain = update.state.domain();
    let observation_revision = environment
        .observation_revisions
        .get(&domain)
        .expect("registry domains have observation revisions");
    if update
        .observation_revision
        .is_some_and(|expected| expected != observation_revision.load(Ordering::SeqCst))
    {
        return Ok(None);
    }
    let mut staged = current.clone();
    staged.insert(domain, update.state);
    let snapshot = match environment.registry.assemble(&staged) {
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
            environment.registry.assemble(&staged)?
        }
    };
    ensure_publication_active(control)?;
    let (generation, _commit) = match control {
        Some(control) => transaction
            .generations
            .next_generation_with_commit(control)?,
        None => (transaction.generations.next_generation()?, None),
    };
    after_desktop_generation_reserved(environment.hook_scope, control.is_some());
    let advances_observation_revision = cause.kind == EventCauseKind::Request;
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
    before_desktop_publication_commit(environment.hook_scope, control.is_some());
    if advances_observation_revision {
        observation_revision.fetch_add(1, Ordering::SeqCst);
    }
    transaction.states = Some(staged);
    transaction.latest_snapshot = Some(replay);
    transaction.current_generation = generation;
    let _ = environment.events.send(incremental.clone());
    Ok(Some(incremental))
}

#[cfg(test)]
type PublicationHook = Arc<dyn Fn() + Send + Sync + 'static>;

#[cfg(test)]
static BEFORE_DESKTOP_PUBLICATION_COMMIT_HOOK: std::sync::OnceLock<
    StdMutex<Option<(PublicationHookScope, PublicationHook)>>,
> = std::sync::OnceLock::new();

#[cfg(test)]
static AFTER_DESKTOP_GENERATION_RESERVED_HOOK: std::sync::OnceLock<
    StdMutex<Option<(PublicationHookScope, PublicationHook)>>,
> = std::sync::OnceLock::new();

#[cfg(test)]
static DESKTOP_PUBLICATION_HOOK_INSTALL_LOCK: std::sync::OnceLock<StdMutex<()>> =
    std::sync::OnceLock::new();

#[cfg(test)]
#[derive(Clone, Copy)]
enum PublicationHookSlot {
    AfterGenerationReserved,
    BeforeCommit,
}

#[cfg(test)]
struct PublicationCommitHookGuard {
    _install: std::sync::MutexGuard<'static, ()>,
    slot: PublicationHookSlot,
}

#[cfg(test)]
impl Drop for PublicationCommitHookGuard {
    fn drop(&mut self) {
        *self
            .slot
            .storage()
            .lock()
            .unwrap_or_else(|error| error.into_inner()) = None;
    }
}

#[cfg(test)]
fn install_before_desktop_publication_commit_hook(
    authority: &DesktopStateAuthority,
    hook: PublicationHook,
) -> PublicationCommitHookGuard {
    install_publication_hook(
        PublicationHookSlot::BeforeCommit,
        authority.publication_hook_scope(),
        hook,
    )
}

#[cfg(test)]
fn install_after_desktop_generation_reserved_hook(
    authority: &DesktopStateAuthority,
    hook: PublicationHook,
) -> PublicationCommitHookGuard {
    install_publication_hook(
        PublicationHookSlot::AfterGenerationReserved,
        authority.publication_hook_scope(),
        hook,
    )
}

#[cfg(test)]
fn install_publication_hook(
    slot: PublicationHookSlot,
    scope: PublicationHookScope,
    hook: PublicationHook,
) -> PublicationCommitHookGuard {
    let install = DESKTOP_PUBLICATION_HOOK_INSTALL_LOCK
        .get_or_init(|| StdMutex::new(()))
        .lock()
        .unwrap_or_else(|error| error.into_inner());
    *slot
        .storage()
        .lock()
        .unwrap_or_else(|error| error.into_inner()) = Some((scope, hook));
    PublicationCommitHookGuard {
        _install: install,
        slot,
    }
}

#[cfg(test)]
impl PublicationHookSlot {
    fn storage(self) -> &'static StdMutex<Option<(PublicationHookScope, PublicationHook)>> {
        match self {
            Self::AfterGenerationReserved => {
                AFTER_DESKTOP_GENERATION_RESERVED_HOOK.get_or_init(|| StdMutex::new(None))
            }
            Self::BeforeCommit => {
                BEFORE_DESKTOP_PUBLICATION_COMMIT_HOOK.get_or_init(|| StdMutex::new(None))
            }
        }
    }
}

#[cfg(test)]
fn publication_hook_scope(authority: &DesktopStateAuthority) -> PublicationHookScope {
    PublicationHookScope(Arc::as_ptr(&authority.publication_hook_scope) as usize)
}

#[cfg(not(test))]
fn publication_hook_scope(_authority: &DesktopStateAuthority) -> PublicationHookScope {
    PublicationHookScope
}

#[cfg(test)]
fn run_publication_hook(slot: PublicationHookSlot, scope: PublicationHookScope, controlled: bool) {
    if controlled {
        let hook = slot
            .storage()
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .as_ref()
            .and_then(|(expected, hook)| (*expected == scope).then(|| Arc::clone(hook)));
        if let Some(hook) = hook {
            hook();
        }
    }
}

#[cfg(test)]
fn after_desktop_generation_reserved(scope: PublicationHookScope, controlled: bool) {
    run_publication_hook(
        PublicationHookSlot::AfterGenerationReserved,
        scope,
        controlled,
    );
}

#[cfg(test)]
fn before_desktop_publication_commit(scope: PublicationHookScope, controlled: bool) {
    run_publication_hook(PublicationHookSlot::BeforeCommit, scope, controlled);
}

#[cfg(not(test))]
fn after_desktop_generation_reserved(_scope: PublicationHookScope, _controlled: bool) {}

#[cfg(not(test))]
fn before_desktop_publication_commit(_scope: PublicationHookScope, _controlled: bool) {}

fn lock_publication_transaction<'a>(
    transaction: &'a StdMutex<DesktopAuthorityTransaction>,
    control: Option<&crate::system::RunControl>,
) -> io::Result<std::sync::MutexGuard<'a, DesktopAuthorityTransaction>> {
    let Some(control) = control else {
        return transaction
            .lock()
            .map_err(|_| io::Error::other("desktop authority transaction lock poisoned"));
    };
    loop {
        ensure_publication_active(Some(control))?;
        match transaction.try_lock() {
            Ok(transaction) => return Ok(transaction),
            Err(std::sync::TryLockError::Poisoned(_)) => {
                return Err(io::Error::other(
                    "desktop authority transaction lock poisoned",
                ));
            }
            Err(std::sync::TryLockError::WouldBlock) => {
                std::thread::sleep(Duration::from_millis(1));
            }
        }
    }
}

fn ensure_publication_active(control: Option<&crate::system::RunControl>) -> io::Result<()> {
    match control {
        Some(control) if control.is_cancelled() => Err(io::Error::new(
            io::ErrorKind::Interrupted,
            "desktop publication was cancelled",
        )),
        Some(control) if control.remaining().is_zero() => Err(io::Error::new(
            io::ErrorKind::TimedOut,
            "desktop publication exceeded its deadline",
        )),
        _ => Ok(()),
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
            let active_attempt = Arc::new(ActiveAttemptSlot::new());
            let blocking_tasks = Arc::new(BlockingTaskTracker::default());
            let attempt_slot = Arc::clone(&active_attempt);
            let attempt_blocking_tasks = Arc::clone(&blocking_tasks);
            let observation_revision = Arc::clone(
                authority
                    .observation_revisions
                    .get(&domain)
                    .expect("registry domains have observation revisions"),
            );
            let wrapper = tokio::spawn(async move {
                let mut backoff = Duration::from_millis(100);
                loop {
                    let started = tokio::time::Instant::now();
                    if token.is_cancelled() {
                        return;
                    }
                    let run_producer = Arc::clone(&producer);
                    let run_sender = sender.clone();
                    let Some((attempt_id, attempt)) = attempt_slot.begin(
                        &token,
                        Arc::clone(&attempt_blocking_tasks),
                        domain,
                        Arc::clone(&observation_revision),
                        move |context| {
                            tokio::spawn(async move { run_producer.run(run_sender, context).await })
                        },
                    ) else {
                        return;
                    };
                    let result = attempt.await;
                    attempt_slot.finish(attempt_id);
                    if token.is_cancelled() {
                        return;
                    }
                    let diagnostic = match result {
                        Ok(Ok(())) => "producer.disconnected".to_owned(),
                        Ok(Err(error)) => bounded_diagnostic(error.to_string()),
                        Err(error) if error.is_panic() => "producer.panicked".to_owned(),
                        Err(_) => "producer.worker-failed".to_owned(),
                    };
                    let state = DesktopDomainState::terminal(
                        domain,
                        CapabilityAvailability::Error,
                        diagnostic,
                    )
                    .expect("producer error has a diagnostic");
                    if sender
                        .send(DesktopDomainUpdate::unversioned(state))
                        .await
                        .is_err()
                    {
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
            });
            tasks.push(ProducerTask {
                wrapper,
                active_attempt,
                blocking_tasks,
            });
        }
        drop(sender);
        let token = cancellation.child_token();
        let aggregator_blocking_tasks = Arc::new(BlockingTaskTracker::default());
        let aggregator_context = DesktopProducerContext::new(
            token.child_token(),
            Arc::clone(&aggregator_blocking_tasks),
        );
        let run_aggregator_context = aggregator_context.clone();
        let aggregator = tokio::spawn(async move {
            loop {
                tokio::select! {
                    biased;
                    _ = run_aggregator_context.cancelled() => return Ok(()),
                    update = receiver.recv() => {
                        let Some(update) = update else { return Ok(()); };
                        let publication = authority.publish_domain_controlled(
                            update,
                            EventCause { kind: EventCauseKind::External, request_id: None },
                            &run_aggregator_context,
                        ).await;
                        match publication {
                            Ok(_) => {}
                            Err(_) if run_aggregator_context.is_cancelled() => return Ok(()),
                            Err(error) => return Err(error),
                        }
                    }
                }
            }
        });
        Ok(DesktopProducerRuntime {
            cancellation,
            tasks,
            aggregator,
            aggregator_context,
            aggregator_blocking_tasks,
        })
    }
}

impl DesktopProducerRuntime {
    pub async fn shutdown(mut self, timeout: Duration) -> io::Result<()> {
        self.cancellation.cancel();
        for task in &self.tasks {
            task.active_attempt.cancel();
            task.blocking_tasks.cancel_and_abort_queued();
        }
        self.aggregator_context.cancel();
        self.aggregator_blocking_tasks.cancel_and_abort_queued();
        let deadline = tokio::time::Instant::now() + timeout;
        let reap_deadline = deadline + PRODUCER_SHUTDOWN_TOLERANCE;
        let mut failure = None;
        let mut timed_out_at = None;
        for index in 0..self.tasks.len() {
            match tokio::time::timeout_at(deadline, &mut self.tasks[index].wrapper).await {
                Ok(Ok(())) => {}
                Ok(Err(error)) => {
                    failure.get_or_insert_with(|| {
                        io::Error::other(format!("desktop producer task failed: {error}"))
                    });
                }
                Err(_) => {
                    timed_out_at = Some(index);
                    failure.get_or_insert_with(|| {
                        io::Error::new(io::ErrorKind::TimedOut, "desktop producers did not drain")
                    });
                    for task in &mut self.tasks[index..] {
                        task.active_attempt.cancel_and_abort();
                        task.blocking_tasks.cancel_and_abort_queued();
                    }
                    for task in &mut self.tasks[index..] {
                        match tokio::time::timeout_at(reap_deadline, &mut task.wrapper).await {
                            Ok(Ok(())) => {}
                            Ok(Err(error)) => {
                                failure.get_or_insert_with(|| {
                                    io::Error::other(format!(
                                        "desktop producer task failed while draining: {error}"
                                    ))
                                });
                            }
                            Err(_) => {
                                task.wrapper.abort();
                                let _ = (&mut task.wrapper).await;
                            }
                        }
                    }
                    break;
                }
            }
        }
        let aggregator_deadline = if timed_out_at.is_some() {
            reap_deadline
        } else {
            deadline
        };
        match tokio::time::timeout_at(aggregator_deadline, &mut self.aggregator).await {
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
                self.aggregator_context.cancel();
                self.aggregator_blocking_tasks.cancel_and_abort_queued();
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
        for task in &self.tasks {
            if !task.blocking_tasks.wait_until_idle(reap_deadline).await {
                failure.get_or_insert_with(|| {
                    io::Error::new(
                        io::ErrorKind::TimedOut,
                        "desktop producer blocking workers did not drain",
                    )
                });
                task.blocking_tasks.drain().await;
            }
        }
        if !self
            .aggregator_blocking_tasks
            .wait_until_idle(reap_deadline)
            .await
        {
            failure.get_or_insert_with(|| {
                io::Error::new(
                    io::ErrorKind::TimedOut,
                    "desktop publication worker did not drain",
                )
            });
            self.aggregator_blocking_tasks.drain().await;
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
                    return Ok(());
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
    use std::sync::atomic::{AtomicUsize, Ordering};

    use sleepy_sdk::{DesktopCommand, DesktopSessionCommand};

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

    struct TestProducer(DesktopDomainId);

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn active_attempt_slot_handoff_cannot_install_after_shutdown_cancellation() {
        let slot = Arc::new(ActiveAttemptSlot::new());
        let parent = CancellationToken::new();
        let blocking_tasks = Arc::new(BlockingTaskTracker::default());
        let runtime = tokio::runtime::Handle::current();
        let (spawn_entered, spawn_observed) = std::sync::mpsc::channel();
        let spawn_gate = Arc::new((StdMutex::new(false), std::sync::Condvar::new()));
        let begin_slot = Arc::clone(&slot);
        let begin_gate = Arc::clone(&spawn_gate);
        let begin = std::thread::spawn(move || {
            begin_slot.begin(
                &parent,
                blocking_tasks,
                DesktopDomainId::Network,
                Arc::new(AtomicU64::new(0)),
                move |context| {
                    spawn_entered.send(()).unwrap();
                    let (open, changed) = &*begin_gate;
                    let mut open = open.lock().unwrap();
                    while !*open {
                        open = changed.wait(open).unwrap();
                    }
                    runtime.spawn(async move { context.cancelled().await })
                },
            )
        });
        spawn_observed.recv_timeout(Duration::from_secs(1)).unwrap();

        let cancel_slot = Arc::clone(&slot);
        let (cancelled, cancellation_observed) = std::sync::mpsc::channel();
        let cancel = std::thread::spawn(move || {
            cancel_slot.cancel();
            cancelled.send(()).unwrap();
        });
        assert_eq!(
            cancellation_observed.recv_timeout(Duration::from_millis(20)),
            Err(std::sync::mpsc::RecvTimeoutError::Timeout),
            "shutdown escaped while an attempt was being installed"
        );

        let (open, changed) = &*spawn_gate;
        *open.lock().unwrap() = true;
        changed.notify_all();
        let (_, attempt) = begin.join().unwrap().unwrap();
        cancellation_observed
            .recv_timeout(Duration::from_secs(1))
            .unwrap();
        cancel.join().unwrap();
        tokio::time::timeout(Duration::from_millis(100), attempt)
            .await
            .expect("installed attempt missed shutdown cancellation")
            .unwrap();
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn uncontrolled_publications_do_not_enter_controlled_commit_hook() {
        let temp = tempfile::tempdir().unwrap();
        let authority =
            DesktopStateAuthority::open(test_registry(), temp.path().join("generation"), 8)
                .await
                .unwrap();
        authority.initialize().await.unwrap();
        let (entered_commit, commit_entered) = std::sync::mpsc::channel();
        let _hook = install_before_desktop_publication_commit_hook(
            &authority,
            Arc::new(move || {
                entered_commit.send(()).unwrap();
            }),
        );

        authority
            .publish_domain(
                DesktopDomainState::available(
                    DesktopDomainId::Network,
                    DesktopDomainValue::empty(DesktopDomainId::Network),
                )
                .unwrap(),
                EventCause {
                    kind: EventCauseKind::External,
                    request_id: None,
                },
            )
            .await
            .unwrap();

        assert_eq!(
            commit_entered.recv_timeout(Duration::from_millis(20)),
            Err(std::sync::mpsc::RecvTimeoutError::Timeout),
            "uncontrolled publication entered the controlled publication hook"
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn controlled_publication_commit_guard_spans_generation_reservation_and_final_commit() {
        let temp = tempfile::tempdir().unwrap();
        let authority =
            DesktopStateAuthority::open(test_registry(), temp.path().join("generation"), 8)
                .await
                .unwrap();
        authority.initialize().await.unwrap();
        for _ in 0..63 {
            authority
                .publish_domain(
                    DesktopDomainState::available(
                        DesktopDomainId::Network,
                        DesktopDomainValue::empty(DesktopDomainId::Network),
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
        let exhausted_generation = authority.current_generation();
        assert_eq!(exhausted_generation, 64);
        let revision = Arc::clone(
            authority
                .observation_revisions
                .get(&DesktopDomainId::Network)
                .unwrap(),
        );
        let context = DesktopProducerContext::for_domain(
            CancellationToken::new(),
            Arc::new(BlockingTaskTracker::default()),
            DesktopDomainId::Network,
            revision,
        );
        let update = context
            .begin_observation()
            .finish(
                DesktopDomainState::available(
                    DesktopDomainId::Network,
                    DesktopDomainValue::empty(DesktopDomainId::Network),
                )
                .unwrap(),
            )
            .unwrap();
        let (reserved_sender, reserved) = std::sync::mpsc::channel();
        let commit_gate = Arc::new((StdMutex::new(false), std::sync::Condvar::new()));
        let hook_gate = Arc::clone(&commit_gate);
        let _hook = install_after_desktop_generation_reserved_hook(
            &authority,
            Arc::new(move || {
                reserved_sender.send(()).unwrap();
                let (open, changed) = &*hook_gate;
                let mut open = open.lock().unwrap();
                while !*open {
                    open = changed.wait(open).unwrap();
                }
            }),
        );
        let publishing_context = context.clone();
        let publishing_authority = Arc::clone(&authority);
        let publishing = tokio::spawn(async move {
            publishing_authority
                .publish_domain_controlled(
                    update,
                    EventCause {
                        kind: EventCauseKind::External,
                        request_id: None,
                    },
                    &publishing_context,
                )
                .await
        });
        reserved.recv_timeout(Duration::from_secs(1)).unwrap();
        let cancelling_context = context.clone();
        let (cancelled_sender, cancelled) = std::sync::mpsc::channel();
        let cancellation = std::thread::spawn(move || {
            cancelling_context.cancel();
            cancelled_sender.send(()).unwrap();
        });
        let observed_cancellation = cancelled.recv_timeout(Duration::from_millis(20));
        let (open, changed) = &*commit_gate;
        *open.lock().unwrap() = true;
        changed.notify_all();
        cancellation.join().unwrap();
        let publication = publishing.await.unwrap();

        assert_eq!(
            observed_cancellation,
            Err(std::sync::mpsc::RecvTimeoutError::Timeout),
            "producer cancellation became observable between generation reservation and final publication"
        );
        assert!(publication.unwrap().is_some());
        assert!(authority.current_generation() > exhausted_generation);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn controlled_publication_commit_wins_before_cancellation_is_observable() {
        let temp = tempfile::tempdir().unwrap();
        let authority =
            DesktopStateAuthority::open(test_registry(), temp.path().join("generation"), 8)
                .await
                .unwrap();
        authority.initialize().await.unwrap();
        let revision = Arc::clone(
            authority
                .observation_revisions
                .get(&DesktopDomainId::Network)
                .unwrap(),
        );
        let context = DesktopProducerContext::for_domain(
            CancellationToken::new(),
            Arc::new(BlockingTaskTracker::default()),
            DesktopDomainId::Network,
            revision,
        );
        let update = context
            .begin_observation()
            .finish(
                DesktopDomainState::available(
                    DesktopDomainId::Network,
                    DesktopDomainValue::empty(DesktopDomainId::Network),
                )
                .unwrap(),
            )
            .unwrap();
        let (entered_commit, commit_entered) = std::sync::mpsc::channel();
        let commit_gate = Arc::new((StdMutex::new(false), std::sync::Condvar::new()));
        let hook_gate = Arc::clone(&commit_gate);
        let _hook = install_before_desktop_publication_commit_hook(
            &authority,
            Arc::new(move || {
                entered_commit.send(()).unwrap();
                let (open, changed) = &*hook_gate;
                let mut open = open.lock().unwrap();
                while !*open {
                    open = changed.wait(open).unwrap();
                }
            }),
        );
        let publishing_context = context.clone();
        let publishing_authority = Arc::clone(&authority);
        let publishing = tokio::spawn(async move {
            publishing_authority
                .publish_domain_controlled(
                    update,
                    EventCause {
                        kind: EventCauseKind::External,
                        request_id: None,
                    },
                    &publishing_context,
                )
                .await
        });
        commit_entered.recv_timeout(Duration::from_secs(1)).unwrap();
        let cancelling_context = context.clone();
        let (cancelled, cancellation_observed) = std::sync::mpsc::channel();
        let cancellation = std::thread::spawn(move || {
            cancelling_context.cancel();
            cancelled.send(()).unwrap();
        });
        let observed_cancellation = cancellation_observed.recv_timeout(Duration::from_millis(20));
        let (open, changed) = &*commit_gate;
        *open.lock().unwrap() = true;
        changed.notify_all();
        assert!(publishing.await.unwrap().unwrap().is_some());
        cancellation_observed
            .recv_timeout(Duration::from_secs(1))
            .ok();
        cancellation.join().unwrap();
        assert_eq!(
            observed_cancellation,
            Err(std::sync::mpsc::RecvTimeoutError::Timeout),
            "producer cancellation became observable during final publication commit"
        );
        assert!(authority.current_generation() > 1);
    }

    #[async_trait]
    impl DesktopProducer for TestProducer {
        fn domain(&self) -> DesktopDomainId {
            self.0
        }

        async fn initial(&self) -> DesktopDomainState {
            DesktopDomainState::available(self.0, DesktopDomainValue::empty(self.0)).unwrap()
        }

        async fn run(
            &self,
            _sender: mpsc::Sender<DesktopDomainUpdate>,
            cancellation: DesktopProducerContext,
        ) -> Result<(), ProducerError> {
            cancellation.cancelled().await;
            Ok(())
        }
    }

    struct CountingExecutor(Arc<AtomicUsize>);

    #[async_trait]
    impl DesktopMutationExecutor for CountingExecutor {
        async fn execute(
            &self,
            _request: &DesktopRequest,
        ) -> Result<DesktopMutationOutcome, ProducerError> {
            self.0.fetch_add(1, Ordering::SeqCst);
            Ok(DesktopMutationOutcome::Confirmed(vec![
                DesktopDomainState::available(
                    DesktopDomainId::Lock,
                    DesktopDomainValue::Lock(LockState { secure: true }),
                )
                .unwrap(),
            ]))
        }
    }

    fn test_registry() -> Arc<DesktopRegistry> {
        Arc::new(
            DesktopRegistry::new(
                DesktopDomainId::ALL
                    .into_iter()
                    .map(|domain| Arc::new(TestProducer(domain)) as Arc<dyn DesktopProducer>)
                    .collect(),
            )
            .unwrap(),
        )
    }

    #[tokio::test]
    async fn reconciled_control_begin_executes_once_and_replays_durable_completion() {
        for fault in [DedupeFaultPoint::Rename, DedupeFaultPoint::DirectorySync] {
            let temp = tempfile::tempdir().unwrap();
            let generation_path = temp.path().join("generation");
            let dedupe_path = temp.path().join("dedupe.json");
            let state = DesktopStateAuthority::open(test_registry(), &generation_path, 8)
                .await
                .unwrap();
            state.initialize().await.unwrap();
            let first_calls = Arc::new(AtomicUsize::new(0));
            let control = DesktopControlAuthority::open(
                Arc::clone(&state),
                Arc::new(CountingExecutor(Arc::clone(&first_calls))),
                &dedupe_path,
                8,
            )
            .await
            .unwrap();
            control.dedupe.lock().unwrap().fault = Some(fault);
            let request = DesktopRequest {
                schema_version: DESKTOP_WIRE_VERSION,
                request_id: "00000000-0000-4000-8000-000000000045".into(),
                expected_generation: state.current_generation(),
                command: DesktopCommand::Session(DesktopSessionCommand::Lock),
            };
            let encoded = serde_json::to_string(&request).unwrap();
            let first = control.handle_json(&encoded).await.unwrap();
            assert_eq!(first.status, DesktopResultStatus::Succeeded, "{fault:?}");
            assert_eq!(first_calls.load(Ordering::SeqCst), 1, "{fault:?}");
            drop(control);

            let replay_calls = Arc::new(AtomicUsize::new(0));
            let reopened = DesktopControlAuthority::open(
                state,
                Arc::new(CountingExecutor(Arc::clone(&replay_calls))),
                &dedupe_path,
                8,
            )
            .await
            .unwrap();
            let replay = reopened.handle_json(&encoded).await.unwrap();
            assert_eq!(replay, first, "{fault:?}");
            assert_eq!(replay_calls.load(Ordering::SeqCst), 0, "{fault:?}");
        }
    }

    #[test]
    fn dedupe_fault_boundaries_never_leave_memory_ahead_of_disk() {
        let request_id = "00000000-0000-4000-8000-000000000041";
        for (fault, reconciled) in [
            (DedupeFaultPoint::FileSync, false),
            (DedupeFaultPoint::Rename, true),
            (DedupeFaultPoint::DirectorySync, true),
        ] {
            let temp = tempfile::tempdir().unwrap();
            let path = temp.path().join("dedupe.json");
            let mut dedupe = DurableDedupe::open(&path, 8).unwrap();
            dedupe.fault = Some(fault);
            assert_eq!(
                dedupe.begin(request_id.into()).is_ok(),
                reconciled,
                "{fault:?}"
            );
            assert_eq!(dedupe.lookup(request_id).is_some(), reconciled, "{fault:?}");

            let reopened = DurableDedupe::open(&path, 8).unwrap();
            assert_eq!(
                reopened.lookup(request_id).is_some(),
                reconciled,
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
            let reconciled = fault != DedupeFaultPoint::FileSync;
            assert_eq!(
                dedupe.complete(request_id, result(request_id)).is_ok(),
                reconciled,
                "{fault:?}"
            );

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
