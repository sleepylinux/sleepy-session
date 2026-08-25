use std::{fmt, sync::Arc};

use sleepy_sdk::{EventCause, EventCauseKind, EventEnvelope, SessionEvent};
use tokio::sync::{broadcast, RwLock};

#[derive(Clone)]
pub struct EventHub {
    latest_snapshot: Arc<RwLock<EventEnvelope>>,
    sender: broadcast::Sender<EventEnvelope>,
}

pub struct EventSubscriber {
    replay: Option<EventEnvelope>,
    last_generation: u64,
    receiver: broadcast::Receiver<EventEnvelope>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PublishError {
    StaleGeneration { attempted: u64, current: u64 },
}

impl fmt::Display for PublishError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::StaleGeneration { attempted, current } => write!(
                formatter,
                "event generation {attempted} does not advance current generation {current}"
            ),
        }
    }
}

impl std::error::Error for PublishError {}

impl EventHub {
    pub fn new(mut initial_snapshot: EventEnvelope, capacity: usize) -> Self {
        if matches!(initial_snapshot.payload, SessionEvent::FullSnapshot(_)) {
            initial_snapshot.cause = EventCause {
                kind: EventCauseKind::Replay,
                request_id: None,
            };
        }
        let (sender, _) = broadcast::channel(capacity.max(1));
        Self {
            latest_snapshot: Arc::new(RwLock::new(initial_snapshot)),
            sender,
        }
    }

    pub async fn subscribe(&self) -> EventSubscriber {
        let receiver = self.sender.subscribe();
        let replay = self.latest_snapshot.read().await.clone();
        let last_generation = replay.generation;
        EventSubscriber {
            replay: Some(replay),
            last_generation,
            receiver,
        }
    }

    pub async fn publish(&self, event: EventEnvelope) -> Result<usize, PublishError> {
        {
            let mut replay = self.latest_snapshot.write().await;
            if event.generation <= replay.generation {
                return Err(PublishError::StaleGeneration {
                    attempted: event.generation,
                    current: replay.generation,
                });
            }
            if matches!(event.payload, SessionEvent::FullSnapshot(_)) {
                *replay = event.clone();
                advance_replay_envelope(&mut replay, &event);
            } else {
                let folded = match (&mut replay.payload, &event.payload) {
                    (
                        SessionEvent::FullSnapshot(snapshot),
                        SessionEvent::CapabilityUpdate(update),
                    ) => {
                        if let Some(current) = snapshot
                            .capabilities
                            .iter_mut()
                            .find(|current| current.id == update.id)
                        {
                            *current = update.clone();
                        } else {
                            snapshot.capabilities.push(update.clone());
                            snapshot
                                .capabilities
                                .sort_by_key(|capability| capability.id);
                        }
                        true
                    }
                    (SessionEvent::FullSnapshot(snapshot), SessionEvent::Niri(update)) => {
                        snapshot.focused_output_id = update.focused_output_id.clone();
                        true
                    }
                    (SessionEvent::FullSnapshot(_), _) => true,
                    _ => false,
                };
                if folded {
                    advance_replay_envelope(&mut replay, &event);
                }
            }
        }
        match self.sender.send(event) {
            Ok(receivers) => Ok(receivers),
            Err(_) => Ok(0),
        }
    }
}

fn advance_replay_envelope(replay: &mut EventEnvelope, event: &EventEnvelope) {
    replay.schema_version = event.schema_version;
    replay.generation = event.generation;
    replay.event_id = event.event_id.clone();
    replay.emitted_at = event.emitted_at.clone();
    replay.cause = EventCause {
        kind: EventCauseKind::Replay,
        request_id: None,
    };
}

impl EventSubscriber {
    pub async fn recv(&mut self) -> Result<EventEnvelope, broadcast::error::RecvError> {
        if let Some(replay) = self.replay.take() {
            return Ok(replay);
        }
        loop {
            let event = self.receiver.recv().await?;
            if event.generation > self.last_generation {
                self.last_generation = event.generation;
                return Ok(event);
            }
        }
    }
}
