use std::{future::Future, io, pin::Pin, sync::Arc, time::Duration};

use sleepy_sdk::{EventCause, EventCauseKind, LifecycleEvent, LifecycleState, SessionEvent};
use tokio::task::JoinSet;

use super::GenerationAuthority;

pub trait LifecycleReconciler: Send + Sync + 'static {
    fn reconcile(&self) -> Pin<Box<dyn Future<Output = io::Result<()>> + Send + '_>>;
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct ReconciliationReport {
    pub succeeded: usize,
    pub timed_out: usize,
    pub failed: usize,
}

pub struct ShutdownCoordinator {
    authority: GenerationAuthority,
    reconciliation_timeout: Duration,
}

impl ShutdownCoordinator {
    pub fn new(authority: GenerationAuthority, reconciliation_timeout: Duration) -> Self {
        Self {
            authority,
            reconciliation_timeout,
        }
    }

    pub async fn reconcile(
        &self,
        reconcilers: &[Arc<dyn LifecycleReconciler>],
    ) -> io::Result<ReconciliationReport> {
        let mut authority = self.authority.lock().await;
        publish_lifecycle(&mut authority, LifecycleState::Stopping).await?;

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

        publish_lifecycle(&mut authority, LifecycleState::Reconciled).await?;
        Ok(report)
    }
}

enum ReconcileOutcome {
    Succeeded,
    TimedOut,
    Failed,
}

async fn publish_lifecycle(
    authority: &mut super::authority::GenerationGuard<'_>,
    lifecycle_state: LifecycleState,
) -> io::Result<()> {
    authority
        .publish(
            EventCause {
                kind: EventCauseKind::Lifecycle,
                request_id: None,
            },
            SessionEvent::Lifecycle(LifecycleEvent {
                state: lifecycle_state,
            }),
        )
        .await?;
    Ok(())
}
