use std::{future::Future, pin::Pin, sync::Arc, time::Duration};

use sleepy_sdk::{
    validate_event_envelope, CapabilityAvailability, CapabilityFailure, CapabilityRecord,
    EventCause, EventCauseKind, EventEnvelope, RuntimeCapabilityId, SessionEvent,
    WIRE_SCHEMA_VERSION,
};
use tokio::{sync::Mutex, time::Instant};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AdapterFailure {
    status: CapabilityAvailability,
    message: String,
}

impl AdapterFailure {
    pub fn new(status: CapabilityAvailability, message: impl Into<String>) -> Self {
        let status = if status == CapabilityAvailability::Available {
            CapabilityAvailability::Error
        } else {
            status
        };
        let message = message.into();
        Self {
            status,
            message: if message.trim().is_empty() {
                "adapter observation failed".into()
            } else {
                message
            },
        }
    }
}

pub trait CapabilityAdapter: Send + Sync + 'static {
    fn id(&self) -> RuntimeCapabilityId;

    fn observe(
        &self,
    ) -> Pin<Box<dyn Future<Output = Result<CapabilityRecord, AdapterFailure>> + Send + '_>>;
}

pub struct AdapterActor<A: CapabilityAdapter> {
    adapter: Arc<A>,
    operation_timeout: Duration,
    restart_delay: Duration,
    observation_lock: Mutex<()>,
    retry_not_before: Mutex<Option<Instant>>,
}

impl<A: CapabilityAdapter> AdapterActor<A> {
    pub fn new(adapter: Arc<A>, operation_timeout: Duration, restart_delay: Duration) -> Self {
        Self {
            adapter,
            operation_timeout,
            restart_delay,
            observation_lock: Mutex::new(()),
            retry_not_before: Mutex::new(None),
        }
    }

    pub async fn observe_once(&self) -> CapabilityRecord {
        let _observation = self.observation_lock.lock().await;
        let retry_not_before = *self.retry_not_before.lock().await;
        if let Some(not_before) = retry_not_before {
            tokio::time::sleep_until(not_before).await;
        }

        let id = self.adapter.id();
        let observed = tokio::time::timeout(self.operation_timeout, self.adapter.observe()).await;
        let record = match observed {
            Ok(Ok(record)) if valid_record_for(id, &record) => record,
            Ok(Ok(_)) => degraded(
                id,
                CapabilityAvailability::Parse,
                "adapter returned an invalid capability record",
            ),
            Ok(Err(error)) => degraded(id, error.status, &error.message),
            Err(_) => degraded(
                id,
                CapabilityAvailability::Timeout,
                "adapter observation exceeded its deadline",
            ),
        };

        let mut retry_not_before = self.retry_not_before.lock().await;
        *retry_not_before = (record.status != CapabilityAvailability::Available)
            .then(|| Instant::now() + self.restart_delay);
        record
    }
}

fn valid_record_for(id: RuntimeCapabilityId, record: &CapabilityRecord) -> bool {
    if record.id != id {
        return false;
    }
    let envelope = EventEnvelope {
        schema_version: WIRE_SCHEMA_VERSION,
        generation: 1,
        event_id: "018f3f4c-8af1-7f6b-bf42-1bd472868e61".into(),
        emitted_at: "2026-08-24T21:00:00Z".into(),
        cause: EventCause {
            kind: EventCauseKind::External,
            request_id: None,
        },
        payload: SessionEvent::CapabilityUpdate(record.clone()),
    };
    serde_json::to_string(&envelope)
        .ok()
        .and_then(|json| validate_event_envelope(&json).ok())
        .is_some()
}

fn degraded(
    id: RuntimeCapabilityId,
    status: CapabilityAvailability,
    message: &str,
) -> CapabilityRecord {
    CapabilityRecord {
        id,
        status,
        value: None,
        diagnostic: Some(CapabilityFailure {
            message: if message.trim().is_empty() {
                "adapter observation failed".into()
            } else {
                message.into()
            },
        }),
    }
}
