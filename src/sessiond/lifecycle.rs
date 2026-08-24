use std::{future::Future, io, pin::Pin, sync::Arc, time::Duration};

use sleepy_sdk::{
    EventCause, EventCauseKind, EventEnvelope, LifecycleEvent, LifecycleState, SessionEvent,
    WIRE_SCHEMA_VERSION,
};
use tokio::{sync::Mutex, task::JoinSet};

use super::{utc_now, EventHub, GenerationAllocator};

pub trait LifecycleReconciler: Send + Sync + 'static {
    fn reconcile(&self) -> Pin<Box<dyn Future<Output = io::Result<()>> + Send + '_>>;
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct ReconciliationReport {
    pub succeeded: usize,
    pub timed_out: usize,
    pub failed: usize,
}

struct LifecycleStateAuthority {
    allocator: GenerationAllocator,
    current_generation: u64,
}

pub struct ShutdownCoordinator {
    state: Mutex<LifecycleStateAuthority>,
    hub: EventHub,
    reconciliation_timeout: Duration,
}

impl ShutdownCoordinator {
    pub fn new(
        allocator: GenerationAllocator,
        current_generation: u64,
        hub: EventHub,
        reconciliation_timeout: Duration,
    ) -> Self {
        Self {
            state: Mutex::new(LifecycleStateAuthority {
                allocator,
                current_generation,
            }),
            hub,
            reconciliation_timeout,
        }
    }

    pub async fn reconcile(
        &self,
        reconcilers: &[Arc<dyn LifecycleReconciler>],
    ) -> io::Result<ReconciliationReport> {
        let mut state = self.state.lock().await;
        publish_lifecycle(&self.hub, &mut state, LifecycleState::Stopping).await?;

        let mut tasks = JoinSet::new();
        for reconciler in reconcilers {
            let reconciler = Arc::clone(reconciler);
            let timeout = self.reconciliation_timeout;
            tasks.spawn(async move {
                match tokio::time::timeout(timeout, reconciler.reconcile()).await {
                    Ok(Ok(())) => ReconcileOutcome::Succeeded,
                    Ok(Err(_)) => ReconcileOutcome::Failed,
                    Err(_) => ReconcileOutcome::TimedOut,
                }
            });
        }

        let mut report = ReconciliationReport::default();
        while let Some(outcome) = tasks.join_next().await {
            match outcome {
                Ok(ReconcileOutcome::Succeeded) => report.succeeded += 1,
                Ok(ReconcileOutcome::TimedOut) => report.timed_out += 1,
                Ok(ReconcileOutcome::Failed) | Err(_) => report.failed += 1,
            }
        }

        publish_lifecycle(&self.hub, &mut state, LifecycleState::Reconciled).await?;
        Ok(report)
    }
}

enum ReconcileOutcome {
    Succeeded,
    TimedOut,
    Failed,
}

async fn publish_lifecycle(
    hub: &EventHub,
    state: &mut LifecycleStateAuthority,
    lifecycle_state: LifecycleState,
) -> io::Result<()> {
    let generation = state.allocator.next_after(state.current_generation)?;
    let event = EventEnvelope {
        schema_version: WIRE_SCHEMA_VERSION,
        generation,
        event_id: uuid::Uuid::new_v4().to_string(),
        emitted_at: utc_now()?,
        cause: EventCause {
            kind: EventCauseKind::Lifecycle,
            request_id: None,
        },
        payload: SessionEvent::Lifecycle(LifecycleEvent {
            state: lifecycle_state,
        }),
    };
    hub.publish(event)
        .await
        .map_err(|_| io::Error::new(io::ErrorKind::BrokenPipe, "event hub closed"))?;
    state.current_generation = generation;
    Ok(())
}
