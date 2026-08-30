use std::{
    collections::{BTreeMap, HashMap, VecDeque},
    io,
    path::Path,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc, RwLock,
    },
    time::{Duration, Instant},
};

use serde::{Deserialize, Serialize};
use sleepy_sdk::{
    validate_osd_event, AudioRuntimeState, BrightnessRuntimeState, CapabilityAvailability,
    CapabilityValue, EventEnvelope, MediaRuntimeState, NiriEvent, OsdEvent, OsdKind,
    PowerProfileRuntimeState, RuntimeCapabilityId, SessionEvent, WIRE_SCHEMA_VERSION,
};
use tokio::{
    io::AsyncWriteExt,
    net::UnixStream,
    sync::{broadcast, mpsc, oneshot},
};
use tokio_util::sync::CancellationToken;

use crate::sessiond::{
    private_socket::{peer_uid, PrivateSocketEndpoint},
    supervisor::RequiredStartupTask,
    EventSubscriber, SocketDrainReport,
};

#[derive(Debug, Clone, PartialEq)]
pub struct FocusedOsdRequest {
    pub kind: OsdKind,
    pub level: Option<f64>,
    pub muted: Option<bool>,
    pub label: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OsdRouteError {
    MissingFocus,
    StaleFocus,
    InvalidEvent,
    RuntimeStopped,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct OsdPublication {
    pub sequence: u64,
    pub visible: Vec<OsdEvent>,
    pub overflow_by_output: BTreeMap<String, u64>,
}

#[derive(Clone)]
pub struct OsdPublicationHub {
    latest: Arc<RwLock<Option<OsdPublication>>>,
    events: broadcast::Sender<OsdPublication>,
}

pub struct OsdPublicationSubscriber {
    replay: Option<OsdPublication>,
    last_sequence: u64,
    events: broadcast::Receiver<OsdPublication>,
}

pub struct OsdSocket {
    endpoint: PrivateSocketEndpoint,
    hub: OsdPublicationHub,
    shutdown: CancellationToken,
    connections: tokio::sync::Mutex<Vec<tokio::task::JoinHandle<io::Result<()>>>>,
    serving: AtomicBool,
    serve_stopped: tokio::sync::Notify,
}

#[derive(Clone)]
pub struct OsdRuntimeHandle {
    requests: mpsc::Sender<RouteRequest>,
    publications: broadcast::Sender<OsdPublication>,
}

struct RouteRequest {
    request: FocusedOsdRequest,
    response: oneshot::Sender<Result<bool, OsdRouteError>>,
}

#[derive(Default)]
struct CapabilityOsdState {
    audio: Option<AudioRuntimeState>,
    brightness: Option<BrightnessRuntimeState>,
    media: Option<MediaRuntimeState>,
    power_profile: Option<PowerProfileRuntimeState>,
}

struct FocusedOutput {
    output_id: String,
    observed_at: Instant,
}

#[derive(Default)]
struct OutputQueue {
    current: Option<OsdEvent>,
    current_since: Option<Instant>,
    pending: VecDeque<OsdEvent>,
    overflow_count: u64,
}

pub struct OsdRouter {
    pending_capacity: usize,
    display_timeout: Duration,
    focus_ttl: Duration,
    focused_output: Option<FocusedOutput>,
    outputs: HashMap<String, OutputQueue>,
}

impl OsdRouter {
    pub fn new(pending_capacity: usize) -> Self {
        Self::with_timing(
            pending_capacity,
            Duration::from_secs(2),
            Duration::from_millis(250),
        )
    }

    pub fn with_timing(
        pending_capacity: usize,
        display_timeout: Duration,
        focus_ttl: Duration,
    ) -> Self {
        Self {
            pending_capacity,
            display_timeout: display_timeout.max(Duration::from_millis(1)),
            focus_ttl,
            focused_output: None,
            outputs: HashMap::new(),
        }
    }

    fn push_at(&mut self, event: OsdEvent, now: Instant) -> bool {
        let queue = self.outputs.entry(event.output_id.clone()).or_default();
        match &mut queue.current {
            None => {
                queue.current = Some(event);
                queue.current_since = Some(now);
                true
            }
            Some(current) if current.kind == event.kind => {
                *current = event;
                queue.current_since = Some(now);
                true
            }
            Some(_) if queue.pending.len() < self.pending_capacity => {
                queue.pending.push_back(event);
                true
            }
            Some(_) => {
                queue.overflow_count = queue.overflow_count.saturating_add(1);
                false
            }
        }
    }

    pub fn observe_niri_focus(&mut self, event: NiriEvent, observed_at: Instant) {
        self.focused_output = event.focused_output_id.and_then(|output_id| {
            (!output_id.trim().is_empty()).then_some(FocusedOutput {
                output_id,
                observed_at,
            })
        });
    }

    pub fn push_focused(
        &mut self,
        request: FocusedOsdRequest,
        now: Instant,
    ) -> Result<bool, OsdRouteError> {
        let focused = self
            .focused_output
            .as_ref()
            .ok_or(OsdRouteError::MissingFocus)?;
        let age = now
            .checked_duration_since(focused.observed_at)
            .ok_or(OsdRouteError::StaleFocus)?;
        if age > self.focus_ttl {
            return Err(OsdRouteError::StaleFocus);
        }
        let event = OsdEvent {
            schema_version: WIRE_SCHEMA_VERSION,
            output_id: focused.output_id.clone(),
            kind: request.kind,
            level: request.level,
            muted: request.muted,
            label: request.label,
        };
        let json = serde_json::to_string(&event).map_err(|_| OsdRouteError::InvalidEvent)?;
        validate_osd_event(&json).map_err(|_| OsdRouteError::InvalidEvent)?;
        Ok(self.push_at(event, now))
    }

    pub fn advance_time(&mut self, now: Instant) -> bool {
        let mut changed = false;
        for queue in self.outputs.values_mut() {
            while let Some(since) = queue.current_since {
                let Some(elapsed) = now.checked_duration_since(since) else {
                    break;
                };
                if elapsed < self.display_timeout {
                    break;
                }
                let next_since = since + self.display_timeout;
                queue.current = queue.pending.pop_front();
                queue.current_since = queue.current.as_ref().map(|_| next_since);
                changed = true;
                if queue.current.is_none() {
                    break;
                }
            }
        }
        changed
    }

    pub fn current(&self, output_id: &str) -> Option<&OsdEvent> {
        self.outputs.get(output_id)?.current.as_ref()
    }

    pub fn pending_len(&self, output_id: &str) -> usize {
        self.outputs
            .get(output_id)
            .map_or(0, |queue| queue.pending.len())
    }

    pub fn overflow_count(&self, output_id: &str) -> u64 {
        self.outputs
            .get(output_id)
            .map_or(0, |queue| queue.overflow_count)
    }

    pub fn complete(&mut self, output_id: &str) -> Option<&OsdEvent> {
        let queue = self.outputs.get_mut(output_id)?;
        queue.current = queue.pending.pop_front();
        queue.current_since = queue.current.as_ref().map(|_| Instant::now());
        queue.current.as_ref()
    }

    fn publication(&self, sequence: u64) -> OsdPublication {
        let mut visible = self
            .outputs
            .values()
            .filter_map(|queue| queue.current.clone())
            .collect::<Vec<_>>();
        visible.sort_by(|left, right| left.output_id.cmp(&right.output_id));
        let overflow_by_output = self
            .outputs
            .iter()
            .filter_map(|(output, queue)| {
                (queue.overflow_count > 0).then_some((output.clone(), queue.overflow_count))
            })
            .collect();
        OsdPublication {
            sequence,
            visible,
            overflow_by_output,
        }
    }
}

pub fn spawn_osd_runtime(
    events: EventSubscriber,
    pending_capacity: usize,
) -> (OsdRuntimeHandle, tokio::task::JoinHandle<()>) {
    spawn_osd_runtime_with_timing(
        events,
        pending_capacity,
        Duration::from_secs(2),
        Duration::from_millis(250),
    )
}

pub fn spawn_osd_runtime_with_timing(
    mut events: EventSubscriber,
    pending_capacity: usize,
    display_timeout: Duration,
    focus_ttl: Duration,
) -> (OsdRuntimeHandle, tokio::task::JoinHandle<()>) {
    let (requests, mut request_receiver) = mpsc::channel::<RouteRequest>(32);
    let (publications, _) = broadcast::channel(32);
    let handle = OsdRuntimeHandle {
        requests,
        publications: publications.clone(),
    };
    let task = tokio::spawn(async move {
        let mut router = OsdRouter::with_timing(pending_capacity, display_timeout, focus_ttl);
        let mut capabilities = CapabilityOsdState::default();
        let mut sequence = 0_u64;
        let mut timer = tokio::time::interval(Duration::from_millis(25));
        loop {
            tokio::select! {
                event = events.recv() => {
                    match event {
                        Ok(event) => {
                            let now = Instant::now();
                            if let SessionEvent::Niri(focus) = &event.payload {
                                router.observe_niri_focus(focus.clone(), now);
                            } else {
                                let mut routed = false;
                                for request in capabilities.requests(&event) {
                                    routed |= router.push_focused(request, now).is_ok();
                                }
                                if routed {
                                    sequence = sequence.saturating_add(1);
                                    let _ = publications.send(router.publication(sequence));
                                }
                            }
                        }
                        Err(broadcast::error::RecvError::Lagged(_)) => {
                            router.focused_output = None;
                        }
                        Err(broadcast::error::RecvError::Closed) => break,
                    }
                }
                request = request_receiver.recv() => {
                    let Some(request) = request else { break };
                    let result = router.push_focused(request.request, Instant::now());
                    if result.is_ok() {
                        sequence = sequence.saturating_add(1);
                        let _ = publications.send(router.publication(sequence));
                    }
                    let _ = request.response.send(result);
                }
                _ = timer.tick() => {
                    if router.advance_time(Instant::now()) {
                        sequence = sequence.saturating_add(1);
                        let _ = publications.send(router.publication(sequence));
                    }
                }
            }
        }
    });
    (handle, task)
}

impl CapabilityOsdState {
    fn requests(&mut self, event: &EventEnvelope) -> Vec<FocusedOsdRequest> {
        let SessionEvent::CapabilityUpdate(record) = &event.payload else {
            return Vec::new();
        };
        if record.status != CapabilityAvailability::Available {
            self.clear(record.id);
            return Vec::new();
        }
        match (record.id, &record.value) {
            (RuntimeCapabilityId::Audio, Some(CapabilityValue::Audio(state))) => {
                let output_changed = self.audio.as_ref().is_none_or(|previous| {
                    previous.output_level != state.output_level
                        || previous.output_muted != state.output_muted
                });
                let input_changed = self.audio.as_ref().is_none_or(|previous| {
                    previous.input_level != state.input_level
                        || previous.input_muted != state.input_muted
                });
                self.audio = Some(state.clone());
                let mut requests = Vec::with_capacity(2);
                if output_changed {
                    requests.push(FocusedOsdRequest {
                        kind: OsdKind::Volume,
                        level: Some(state.output_level),
                        muted: Some(state.output_muted),
                        label: level_label(state.output_level, state.output_muted),
                    });
                }
                if input_changed {
                    requests.push(FocusedOsdRequest {
                        kind: OsdKind::Microphone,
                        level: Some(state.input_level),
                        muted: Some(state.input_muted),
                        label: level_label(state.input_level, state.input_muted),
                    });
                }
                requests
            }
            (RuntimeCapabilityId::Brightness, Some(CapabilityValue::Brightness(state))) => {
                let changed = self.brightness.as_ref() != Some(state);
                self.brightness = Some(state.clone());
                changed
                    .then(|| FocusedOsdRequest {
                        kind: OsdKind::Brightness,
                        level: Some(state.level),
                        muted: None,
                        label: format!("{:.0}%", state.level * 100.0),
                    })
                    .into_iter()
                    .collect()
            }
            (RuntimeCapabilityId::Media, Some(CapabilityValue::Media(state))) => {
                let changed = self.media.as_ref() != Some(state);
                self.media = Some(state.clone());
                changed
                    .then(|| FocusedOsdRequest {
                        kind: OsdKind::Media,
                        level: None,
                        muted: None,
                        label: if state.artist.trim().is_empty() {
                            state.title.clone()
                        } else {
                            format!("{} — {}", state.title, state.artist)
                        },
                    })
                    .into_iter()
                    .collect()
            }
            (RuntimeCapabilityId::PowerProfile, Some(CapabilityValue::PowerProfile(state))) => {
                let changed = self.power_profile.as_ref() != Some(state);
                self.power_profile = Some(state.clone());
                changed
                    .then(|| FocusedOsdRequest {
                        kind: OsdKind::PowerProfile,
                        level: None,
                        muted: None,
                        label: state.active.clone(),
                    })
                    .into_iter()
                    .collect()
            }
            _ => Vec::new(),
        }
    }

    fn clear(&mut self, id: RuntimeCapabilityId) {
        match id {
            RuntimeCapabilityId::Audio => self.audio = None,
            RuntimeCapabilityId::Brightness => self.brightness = None,
            RuntimeCapabilityId::Media => self.media = None,
            RuntimeCapabilityId::PowerProfile => self.power_profile = None,
            _ => {}
        }
    }
}

impl OsdPublicationHub {
    pub fn new(capacity: usize) -> Self {
        let (events, _) = broadcast::channel(capacity.max(1));
        Self {
            latest: Arc::new(RwLock::new(None)),
            events,
        }
    }

    pub fn publish(&self, publication: OsdPublication) -> io::Result<()> {
        let mut latest = self
            .latest
            .write()
            .map_err(|_| io::Error::other("OSD publication state was poisoned"))?;
        if latest
            .as_ref()
            .is_some_and(|current| publication.sequence <= current.sequence)
        {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "OSD publication sequence must be strictly increasing",
            ));
        }
        *latest = Some(publication.clone());
        let _ = self.events.send(publication);
        Ok(())
    }

    pub fn subscribe(&self) -> io::Result<OsdPublicationSubscriber> {
        let events = self.events.subscribe();
        let replay = self
            .latest
            .read()
            .map_err(|_| io::Error::other("OSD publication state was poisoned"))?
            .clone();
        Ok(OsdPublicationSubscriber {
            replay,
            last_sequence: 0,
            events,
        })
    }
}

impl OsdPublicationSubscriber {
    pub async fn recv(&mut self) -> io::Result<OsdPublication> {
        if let Some(replay) = self.replay.take() {
            self.last_sequence = replay.sequence;
            return Ok(replay);
        }
        loop {
            let publication = self.events.recv().await.map_err(|error| match error {
                broadcast::error::RecvError::Closed => {
                    io::Error::new(io::ErrorKind::BrokenPipe, "OSD publication hub closed")
                }
                broadcast::error::RecvError::Lagged(count) => io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!("OSD publication subscriber lagged by {count}"),
                ),
            })?;
            if publication.sequence > self.last_sequence {
                self.last_sequence = publication.sequence;
                return Ok(publication);
            }
        }
    }
}

impl OsdSocket {
    pub async fn bind(
        path: impl AsRef<Path>,
        expected_uid: libc::uid_t,
        hub: OsdPublicationHub,
    ) -> io::Result<Self> {
        let endpoint = PrivateSocketEndpoint::bind(path, expected_uid).await?;
        Ok(Self {
            endpoint,
            hub,
            shutdown: CancellationToken::new(),
            connections: tokio::sync::Mutex::new(Vec::new()),
            serving: AtomicBool::new(false),
            serve_stopped: tokio::sync::Notify::new(),
        })
    }

    pub async fn serve(&self) -> io::Result<()> {
        self.serve_inner(None).await
    }

    pub async fn serve_with_startup(&self, startup: RequiredStartupTask) -> io::Result<()> {
        self.serve_inner(Some(startup)).await
    }

    async fn serve_inner(&self, startup: Option<RequiredStartupTask>) -> io::Result<()> {
        if self.serving.swap(true, Ordering::AcqRel) {
            return Err(io::Error::new(
                io::ErrorKind::AlreadyExists,
                "OSD socket is already being served",
            ));
        }
        let _guard = OsdServeGuard {
            serving: &self.serving,
            stopped: &self.serve_stopped,
        };
        let listener_shutdown = self.shutdown.child_token();
        if let Some(startup) = startup {
            startup.ready_and_wait().await?;
        }
        loop {
            let stream = tokio::select! {
                accepted = self.endpoint.accept() => accepted?,
                _ = listener_shutdown.cancelled() => return Ok(()),
            };
            let expected_uid = self.endpoint.expected_uid();
            let subscriber = self.hub.subscribe()?;
            let shutdown = self.shutdown.child_token();
            self.connections.lock().await.push(tokio::spawn(async move {
                serve_osd_stream(stream, expected_uid, subscriber, shutdown).await
            }));
        }
    }

    pub async fn shutdown_and_drain(&self, timeout: Duration) -> io::Result<SocketDrainReport> {
        let deadline = tokio::time::Instant::now() + timeout;
        self.shutdown.cancel();
        if self.serving.load(Ordering::Acquire) {
            tokio::time::timeout_at(deadline, async {
                while self.serving.load(Ordering::Acquire) {
                    let stopped = self.serve_stopped.notified();
                    if !self.serving.load(Ordering::Acquire) {
                        break;
                    }
                    stopped.await;
                }
            })
            .await
            .map_err(|_| io::Error::new(io::ErrorKind::TimedOut, "OSD accept loop did not stop"))?;
        }
        let mut handles = std::mem::take(&mut *self.connections.lock().await);
        let mut report = SocketDrainReport::default();
        while !handles.is_empty() {
            let mut handle = handles.remove(0);
            match tokio::time::timeout_at(deadline, &mut handle).await {
                Ok(_) => report.completed += 1,
                Err(_) => {
                    handle.abort();
                    let _ = handle.await;
                    report.aborted += 1;
                    for handle in handles {
                        handle.abort();
                        let _ = handle.await;
                        report.aborted += 1;
                    }
                    break;
                }
            }
        }
        Ok(report)
    }

    pub fn path(&self) -> &Path {
        self.endpoint.path()
    }
}

impl Drop for OsdSocket {
    fn drop(&mut self) {
        self.shutdown.cancel();
    }
}

struct OsdServeGuard<'a> {
    serving: &'a AtomicBool,
    stopped: &'a tokio::sync::Notify,
}

impl Drop for OsdServeGuard<'_> {
    fn drop(&mut self) {
        self.serving.store(false, Ordering::Release);
        self.stopped.notify_waiters();
    }
}

async fn serve_osd_stream(
    mut stream: UnixStream,
    expected_uid: libc::uid_t,
    mut subscriber: OsdPublicationSubscriber,
    shutdown: CancellationToken,
) -> io::Result<()> {
    if peer_uid(&stream)? != expected_uid {
        return Err(io::Error::new(
            io::ErrorKind::PermissionDenied,
            "OSD socket peer UID mismatch",
        ));
    }
    loop {
        let publication = tokio::select! {
            biased;
            publication = subscriber.recv() => publication?,
            _ = shutdown.cancelled() => return Ok(()),
        };
        let mut line = serde_json::to_vec(&publication)
            .map_err(|error| io::Error::new(io::ErrorKind::InvalidData, error))?;
        line.push(b'\n');
        stream.write_all(&line).await?;
    }
}

fn level_label(level: f64, muted: bool) -> String {
    if muted {
        "Muted".into()
    } else {
        format!("{:.0}%", level * 100.0)
    }
}

impl OsdRuntimeHandle {
    pub async fn route(&self, request: FocusedOsdRequest) -> Result<bool, OsdRouteError> {
        let (response, receiver) = oneshot::channel();
        self.requests
            .send(RouteRequest { request, response })
            .await
            .map_err(|_| OsdRouteError::RuntimeStopped)?;
        receiver.await.map_err(|_| OsdRouteError::RuntimeStopped)?
    }

    pub fn subscribe(&self) -> broadcast::Receiver<OsdPublication> {
        self.publications.subscribe()
    }
}
