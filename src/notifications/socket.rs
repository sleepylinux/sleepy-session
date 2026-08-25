// SPDX-License-Identifier: GPL-3.0-only

use super::{NotificationActionDispatcher, NotificationCommand, NotificationEventService};
use crate::sessiond::private_socket::{peer_uid, read_bounded_line, PrivateSocketEndpoint};
use serde::{Deserialize, Serialize};
use sleepy_sdk::{NotificationDocument, WIRE_SCHEMA_VERSION};
use std::{
    io,
    path::Path,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
    time::Duration,
};
use tokio::{io::AsyncWriteExt, net::UnixStream, sync::Mutex};

const MAX_LINE: usize = 256 * 1024;

#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct Request {
    schema_version: u32,
    request_id: String,
    operation: Operation,
}

#[derive(Deserialize)]
#[serde(
    tag = "type",
    content = "data",
    rename_all = "camelCase",
    rename_all_fields = "camelCase",
    deny_unknown_fields
)]
enum Operation {
    Snapshot,
    MarkRead { id: u64 },
    Dismiss { id: u64 },
    Archive { id: u64 },
    SetDnd { enabled: bool },
    InvokeAction { id: u64, action_id: String },
    PurgeArchive,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct Response {
    schema_version: u32,
    request_id: String,
    status: &'static str,
    #[serde(skip_serializing_if = "Option::is_none")]
    data: Option<Data>,
    #[serde(skip_serializing_if = "Option::is_none")]
    error: Option<ResponseError>,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct ResponseError {
    code: &'static str,
    message: String,
}

#[derive(Serialize)]
#[serde(
    tag = "type",
    content = "data",
    rename_all = "camelCase",
    rename_all_fields = "camelCase"
)]
enum Data {
    Snapshot(Snapshot),
    ActionInvoked { id: u64, action_id: String },
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct Snapshot {
    active: Vec<NotificationDocument>,
    archive: Vec<NotificationDocument>,
    unread_count: usize,
    groups: Vec<Group>,
    dnd: bool,
    popup_ids: Vec<u64>,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct Group {
    application_id: String,
    notification_ids: Vec<u64>,
}

pub struct NotificationSocket {
    endpoint: PrivateSocketEndpoint,
    service: Arc<Mutex<NotificationEventService>>,
    actions: Option<NotificationActionDispatcher>,
    shutdown: tokio::sync::broadcast::Sender<()>,
    connections: Mutex<Vec<tokio::task::JoinHandle<io::Result<()>>>>,
    serving: AtomicBool,
    stopped: tokio::sync::Notify,
}

impl NotificationSocket {
    pub async fn bind(
        path: impl AsRef<Path>,
        expected_uid: libc::uid_t,
        service: Arc<Mutex<NotificationEventService>>,
    ) -> io::Result<Self> {
        let (shutdown, _) = tokio::sync::broadcast::channel(1);
        Ok(Self {
            endpoint: PrivateSocketEndpoint::bind(path, expected_uid).await?,
            service,
            actions: None,
            shutdown,
            connections: Mutex::new(Vec::new()),
            serving: AtomicBool::new(false),
            stopped: tokio::sync::Notify::new(),
        })
    }
    pub fn with_action_dispatcher(mut self, actions: NotificationActionDispatcher) -> Self {
        self.actions = Some(actions);
        self
    }
    pub async fn serve_one(&self) -> io::Result<()> {
        let stream = self.endpoint.accept().await?;
        serve(
            stream,
            self.endpoint.expected_uid(),
            Arc::clone(&self.service),
            self.actions.clone(),
        )
        .await
    }
    pub async fn serve_n(&self, count: usize) -> io::Result<()> {
        for _ in 0..count {
            self.serve_one().await?;
        }
        Ok(())
    }
    pub async fn serve(&self) -> io::Result<()> {
        if self.serving.swap(true, Ordering::AcqRel) {
            return Err(io::Error::new(
                io::ErrorKind::AlreadyExists,
                "notification socket already serving",
            ));
        }
        let _guard = NotificationServing {
            flag: &self.serving,
            stopped: &self.stopped,
        };
        let mut shutdown = self.shutdown.subscribe();
        loop {
            let stream = tokio::select! { stream = self.endpoint.accept() => stream?, _ = shutdown.recv() => return Ok(()) };
            let expected_uid = self.endpoint.expected_uid();
            let service = Arc::clone(&self.service);
            let actions = self.actions.clone();
            self.connections.lock().await.push(tokio::spawn(async move {
                serve(stream, expected_uid, service, actions).await
            }));
        }
    }
    pub async fn shutdown_and_drain(&self, timeout: Duration) -> io::Result<usize> {
        let deadline = tokio::time::Instant::now() + timeout;
        let _ = self.shutdown.send(());
        if self.serving.load(Ordering::Acquire) {
            tokio::time::timeout_at(deadline, async {
                while self.serving.load(Ordering::Acquire) {
                    let notified = self.stopped.notified();
                    if !self.serving.load(Ordering::Acquire) {
                        break;
                    }
                    notified.await;
                }
            })
            .await
            .map_err(|_| {
                io::Error::new(
                    io::ErrorKind::TimedOut,
                    "notification accept loop did not stop",
                )
            })?;
        }
        let handles = std::mem::take(&mut *self.connections.lock().await);
        let count = handles.len();
        for mut handle in handles {
            if tokio::time::timeout_at(deadline, &mut handle)
                .await
                .is_err()
            {
                handle.abort();
                let _ = handle.await;
            }
        }
        Ok(count)
    }
    pub fn path(&self) -> &Path {
        self.endpoint.path()
    }
}

struct NotificationServing<'a> {
    flag: &'a AtomicBool,
    stopped: &'a tokio::sync::Notify,
}
impl Drop for NotificationServing<'_> {
    fn drop(&mut self) {
        self.flag.store(false, Ordering::Release);
        self.stopped.notify_waiters();
    }
}

async fn serve(
    stream: UnixStream,
    expected_uid: libc::uid_t,
    service: Arc<Mutex<NotificationEventService>>,
    actions: Option<NotificationActionDispatcher>,
) -> io::Result<()> {
    if peer_uid(&stream)? != expected_uid {
        return Err(io::Error::new(
            io::ErrorKind::PermissionDenied,
            "notification peer UID mismatch",
        ));
    }
    let (read, mut write) = stream.into_split();
    let bytes = read_bounded_line(
        read,
        MAX_LINE,
        Duration::from_secs(3),
        "invalid bounded notification request",
    )
    .await?;
    let request: Request = serde_json::from_slice(&bytes).map_err(invalid)?;
    if request.schema_version != WIRE_SCHEMA_VERSION
        || uuid::Uuid::parse_str(&request.request_id).is_err()
    {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "invalid notification schemaVersion or requestId",
        ));
    }
    let outcome: io::Result<Data> = async {
        Ok(match request.operation {
            Operation::Snapshot => current_snapshot(&service).await,
            Operation::MarkRead { id } => {
                require_id(id)?;
                service
                    .lock()
                    .await
                    .execute(NotificationCommand::MarkRead { id })
                    .await?;
                current_snapshot(&service).await
            }
            Operation::Dismiss { id } => {
                require_id(id)?;
                service
                    .lock()
                    .await
                    .execute(NotificationCommand::Dismiss { id })
                    .await?;
                current_snapshot(&service).await
            }
            Operation::Archive { id } => {
                require_id(id)?;
                service
                    .lock()
                    .await
                    .execute(NotificationCommand::Archive { id })
                    .await?;
                current_snapshot(&service).await
            }
            Operation::SetDnd { enabled } => {
                service
                    .lock()
                    .await
                    .execute(NotificationCommand::SetDnd { enabled })
                    .await?;
                current_snapshot(&service).await
            }
            Operation::PurgeArchive => {
                service
                    .lock()
                    .await
                    .execute(NotificationCommand::PurgeArchive)
                    .await?;
                current_snapshot(&service).await
            }
            Operation::InvokeAction { id, action_id } => {
                require_id(id)?;
                if action_id.trim().is_empty() {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidInput,
                        "actionId must not be empty",
                    ));
                }
                if let Some(dispatcher) = actions {
                    dispatcher.invoke(id, &action_id).await?;
                } else {
                    service
                        .lock()
                        .await
                        .provider()
                        .invoke_action(id, &action_id)?;
                }
                Data::ActionInvoked { id, action_id }
            }
        })
    }
    .await;
    let (status, data, error) = match outcome {
        Ok(data) => ("confirmed", Some(data), None),
        Err(error) => (
            "error",
            None,
            Some(ResponseError {
                code: match error.kind() {
                    io::ErrorKind::NotConnected => "expired",
                    io::ErrorKind::NotFound => "notFound",
                    io::ErrorKind::PermissionDenied => "permissionDenied",
                    _ => "error",
                },
                message: error.to_string(),
            }),
        ),
    };
    let mut response = serde_json::to_vec(&Response {
        schema_version: WIRE_SCHEMA_VERSION,
        request_id: request.request_id,
        status,
        data,
        error,
    })
    .map_err(invalid)?;
    response.push(b'\n');
    write.write_all(&response).await?;
    write.shutdown().await
}

fn snapshot(service: &NotificationEventService) -> Data {
    let store = service.provider().store();
    Data::Snapshot(Snapshot {
        active: store.active().to_vec(),
        archive: store.archive().to_vec(),
        unread_count: store.unread_count(),
        dnd: store.dnd(),
        groups: store
            .grouped_active()
            .into_iter()
            .map(|group| Group {
                application_id: group.application_id.to_owned(),
                notification_ids: group
                    .notifications
                    .into_iter()
                    .map(|item| item.id)
                    .collect(),
            })
            .collect(),
        popup_ids: store
            .active()
            .iter()
            .filter(|item| service.provider().popup_visible(item.id))
            .map(|item| item.id)
            .collect(),
    })
}
async fn current_snapshot(service: &Arc<Mutex<NotificationEventService>>) -> Data {
    let service = service.lock().await;
    snapshot(&service)
}
fn require_id(id: u64) -> io::Result<()> {
    if id == 0 {
        Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "notification id must be positive",
        ))
    } else {
        Ok(())
    }
}
fn invalid(error: impl std::fmt::Display) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, error.to_string())
}
