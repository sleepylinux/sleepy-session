use std::{io, sync::Arc};

use sleepy_sdk::{
    CapabilityRecord, EventCause, EventEnvelope, RuntimeCapabilityId, SessionEvent,
    WIRE_SCHEMA_VERSION,
};
use tokio::sync::{Mutex, MutexGuard};

use super::{utc_now, EventHub, GenerationAllocator, PublishError};

struct AuthorityState {
    allocator: GenerationAllocator,
    current_generation: u64,
}

#[derive(Clone)]
pub struct GenerationAuthority {
    state: Arc<Mutex<AuthorityState>>,
    hub: EventHub,
}

pub struct GenerationGuard<'a> {
    state: MutexGuard<'a, AuthorityState>,
    hub: &'a EventHub,
}

impl GenerationAuthority {
    pub fn new(allocator: GenerationAllocator, current_generation: u64, hub: EventHub) -> Self {
        Self {
            state: Arc::new(Mutex::new(AuthorityState {
                allocator,
                current_generation,
            })),
            hub,
        }
    }

    pub async fn lock(&self) -> GenerationGuard<'_> {
        GenerationGuard {
            state: self.state.lock().await,
            hub: &self.hub,
        }
    }
}

impl GenerationGuard<'_> {
    pub fn current_generation(&self) -> u64 {
        self.state.current_generation
    }

    pub(crate) async fn current_capability(
        &self,
        id: RuntimeCapabilityId,
    ) -> Option<CapabilityRecord> {
        let snapshot = self.hub.latest_snapshot().await;
        let SessionEvent::FullSnapshot(snapshot) = snapshot.payload else {
            return None;
        };
        snapshot
            .capabilities
            .into_iter()
            .find(|capability| capability.id == id)
    }

    pub async fn publish(
        &mut self,
        cause: EventCause,
        payload: SessionEvent,
    ) -> io::Result<EventEnvelope> {
        let current_generation = self.state.current_generation;
        let generation = self.state.allocator.next_after(current_generation)?;
        let event = EventEnvelope {
            schema_version: WIRE_SCHEMA_VERSION,
            generation,
            event_id: uuid::Uuid::new_v4().to_string(),
            emitted_at: utc_now()?,
            cause,
            payload,
        };
        match self.hub.publish(event.clone()).await {
            Ok(_) => {
                self.state.current_generation = generation;
                Ok(event)
            }
            Err(PublishError::StaleGeneration { current, .. }) => {
                self.state.current_generation = current;
                Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "event hub rejected a non-monotonic generation",
                ))
            }
        }
    }
}
