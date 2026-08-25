use std::{
    collections::{BTreeMap, HashMap, VecDeque},
    time::{Duration, Instant},
};

use sleepy_sdk::{
    validate_osd_event, EventEnvelope, NiriEvent, OsdEvent, OsdKind, SessionEvent,
    WIRE_SCHEMA_VERSION,
};
use tokio::sync::{broadcast, mpsc, oneshot};

use crate::sessiond::EventSubscriber;

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

#[derive(Debug, Clone, PartialEq)]
pub struct OsdPublication {
    pub sequence: u64,
    pub visible: Vec<OsdEvent>,
    pub overflow_by_output: BTreeMap<String, u64>,
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
    mut events: EventSubscriber,
    pending_capacity: usize,
) -> (OsdRuntimeHandle, tokio::task::JoinHandle<()>) {
    let (requests, mut request_receiver) = mpsc::channel::<RouteRequest>(32);
    let (publications, _) = broadcast::channel(32);
    let handle = OsdRuntimeHandle {
        requests,
        publications: publications.clone(),
    };
    let task = tokio::spawn(async move {
        let mut router = OsdRouter::new(pending_capacity);
        let mut sequence = 0_u64;
        let mut timer = tokio::time::interval(Duration::from_millis(25));
        loop {
            tokio::select! {
                event = events.recv() => {
                    match event {
                        Ok(event) => observe_focus_event(&mut router, &event, Instant::now()),
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

fn observe_focus_event(router: &mut OsdRouter, event: &EventEnvelope, now: Instant) {
    if let SessionEvent::Niri(focus) = &event.payload {
        router.observe_niri_focus(focus.clone(), now);
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
