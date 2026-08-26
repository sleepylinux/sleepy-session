use std::{future::Future, io, sync::Arc};

use tokio::{sync::Semaphore, task::JoinSet, time::Instant};

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct ConnectionDrainReport {
    pub completed: usize,
    pub aborted: usize,
}

pub(crate) struct ConnectionSupervisor {
    permits: Arc<Semaphore>,
    tasks: tokio::sync::Mutex<JoinSet<io::Result<()>>>,
}

impl ConnectionSupervisor {
    pub(crate) fn new(max_connections: usize) -> Self {
        assert!(max_connections > 0, "connection limit must be positive");
        Self {
            permits: Arc::new(Semaphore::new(max_connections)),
            tasks: tokio::sync::Mutex::new(JoinSet::new()),
        }
    }

    pub(crate) fn try_admit(&self) -> Option<tokio::sync::OwnedSemaphorePermit> {
        Arc::clone(&self.permits).try_acquire_owned().ok()
    }

    pub(crate) async fn spawn<F>(&self, permit: tokio::sync::OwnedSemaphorePermit, future: F)
    where
        F: Future<Output = io::Result<()>> + Send + 'static,
    {
        let mut tasks = self.tasks.lock().await;
        while let Some(result) = tasks.try_join_next() {
            log_connection_result(result);
        }
        tasks.spawn(async move {
            let _permit = permit;
            future.await
        });
    }

    pub(crate) async fn drain(&self, deadline: Instant) -> ConnectionDrainReport {
        let mut tasks = self.tasks.lock().await;
        let mut report = ConnectionDrainReport::default();
        while !tasks.is_empty() {
            match tokio::time::timeout_at(deadline, tasks.join_next()).await {
                Ok(Some(result)) => {
                    log_connection_result(result);
                    report.completed += 1;
                }
                Ok(None) => break,
                Err(_) => {
                    report.aborted += tasks.len();
                    tasks.abort_all();
                    while tasks.join_next().await.is_some() {}
                }
            }
        }
        report
    }
}

fn log_connection_result(result: Result<io::Result<()>, tokio::task::JoinError>) {
    match result {
        Ok(Ok(())) => {}
        Ok(Err(error)) => eprintln!(
            "event=connection_closed result=error kind={:?} message={}",
            error.kind(),
            error
        ),
        Err(error) => eprintln!("event=connection_closed result=join_error message={error}"),
    }
}
