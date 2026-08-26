// SPDX-License-Identifier: GPL-3.0-only

use std::{
    io,
    path::Path,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
    time::{Duration, Instant},
};

use serde::{Deserialize, Serialize};
use sleepy_sdk::{ThemeDocument, WIRE_SCHEMA_VERSION};
use tokio::{
    io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader, ReadHalf, WriteHalf},
    net::UnixStream,
    sync::{broadcast, Mutex},
};

use crate::{
    sessiond::private_socket::{NoopBindObserver, PrivateSocketEndpoint},
    sessiond::{private_socket::peer_uid, GenerationAuthority},
    socket_supervisor::ConnectionSupervisor,
    system::RunControl,
    theme::{DesktopThemeSink, ThemeError, ThemeErrorKind, ThemeManager},
};

const MAX_LINE: usize = 256 * 1024;
const WRITE_TIMEOUT: Duration = Duration::from_secs(2);
const READ_TIMEOUT: Duration = Duration::from_secs(2);
const MAX_CONNECTIONS: usize = 16;

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ThemeRequest {
    pub schema_version: u32,
    pub request_id: String,
    pub operation: ThemeOperation,
}

#[derive(Debug, Deserialize)]
#[serde(
    tag = "type",
    content = "data",
    rename_all = "camelCase",
    rename_all_fields = "camelCase",
    deny_unknown_fields
)]
pub enum ThemeOperation {
    Get,
    List,
    Import {
        document: String,
    },
    CopyForEdit {
        theme_id: String,
        name: String,
    },
    Delete {
        theme_id: String,
    },
    Apply {
        theme_id: String,
        expected_generation: u64,
    },
}

impl ThemeOperation {
    fn is_mutation(&self) -> bool {
        !matches!(self, Self::Get | Self::List)
    }
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(
    tag = "type",
    content = "data",
    rename_all = "camelCase",
    rename_all_fields = "camelCase",
    deny_unknown_fields
)]
pub enum ThemeMessage {
    Candidate {
        schema_version: u32,
        request_id: String,
        theme: ThemeDocument,
    },
    Result {
        schema_version: u32,
        request_id: String,
        status: ThemeStatus,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        generation: Option<u64>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        theme: Option<ThemeDocument>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        themes: Option<Vec<ThemeDocument>>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        error: Option<String>,
    },
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub enum ThemeStatus {
    Confirmed,
    Reconciled,
    Unavailable,
    Error,
    Busy,
    Timeout,
    Cancelled,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct ThemeAcknowledgement {
    schema_version: u32,
    request_id: String,
    accepted: bool,
}

type Reader = Arc<Mutex<BufReader<ReadHalf<UnixStream>>>>;
type Writer = Arc<Mutex<WriteHalf<UnixStream>>>;

struct SocketThemeSink {
    request_id: String,
    reader: Reader,
    writer: Writer,
}

impl DesktopThemeSink for SocketThemeSink {
    fn acknowledge<'a>(
        &'a self,
        theme: &'a ThemeDocument,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<(), String>> + Send + 'a>> {
        Box::pin(async move {
            write_message(
                &self.writer,
                &ThemeMessage::Candidate {
                    schema_version: WIRE_SCHEMA_VERSION,
                    request_id: self.request_id.clone(),
                    theme: theme.clone(),
                },
            )
            .await
            .map_err(|error| error.to_string())?;
            let bytes = read_line(&self.reader)
                .await
                .map_err(|error| error.to_string())?;
            let acknowledgement: ThemeAcknowledgement = serde_json::from_slice(&bytes)
                .map_err(|error| format!("invalid theme acknowledgement: {error}"))?;
            if acknowledgement.schema_version != WIRE_SCHEMA_VERSION {
                return Err("unknown theme acknowledgement schema version".into());
            }
            if acknowledgement.request_id != self.request_id {
                return Err("theme acknowledgement requestId mismatch".into());
            }
            if !acknowledgement.accepted {
                return Err("desktop rejected theme candidate".into());
            }
            Ok(())
        })
    }
}

pub struct ThemeSocket {
    endpoint: PrivateSocketEndpoint,
    manager: Arc<Mutex<ThemeManager>>,
    authority: GenerationAuthority,
    shutdown: broadcast::Sender<()>,
    connections: ConnectionSupervisor,
    serving: AtomicBool,
    stopped: tokio::sync::Notify,
    cancelled: Arc<AtomicBool>,
}

impl ThemeSocket {
    pub async fn bind(
        path: impl AsRef<Path>,
        expected_uid: libc::uid_t,
        manager: ThemeManager,
        authority: GenerationAuthority,
    ) -> io::Result<Self> {
        let endpoint = PrivateSocketEndpoint::bind_with_observer(
            path,
            expected_uid,
            Arc::new(NoopBindObserver),
        )
        .await?;
        let (shutdown, _) = broadcast::channel(1);
        Ok(Self {
            endpoint,
            manager: Arc::new(Mutex::new(manager)),
            authority,
            shutdown,
            connections: ConnectionSupervisor::new(MAX_CONNECTIONS),
            serving: AtomicBool::new(false),
            stopped: tokio::sync::Notify::new(),
            cancelled: Arc::new(AtomicBool::new(false)),
        })
    }

    pub async fn serve(&self) -> io::Result<()> {
        if self.serving.swap(true, Ordering::AcqRel) {
            return Err(io::Error::new(
                io::ErrorKind::AlreadyExists,
                "theme socket already serving",
            ));
        }
        let _guard = ServeGuard { socket: self };
        let mut shutdown = self.shutdown.subscribe();
        loop {
            let stream = tokio::select! {
                accepted = self.endpoint.accept() => accepted?,
                _ = shutdown.recv() => return Ok(()),
            };
            let manager = Arc::clone(&self.manager);
            let authority = self.authority.clone();
            let expected_uid = self.endpoint.expected_uid();
            let connection_shutdown = self.shutdown.subscribe();
            let cancelled = Arc::clone(&self.cancelled);
            let Some(permit) = self.connections.try_admit() else {
                eprintln!("event=rejected_connection endpoint=theme reason=limit");
                drop(stream);
                continue;
            };
            self.connections
                .spawn(permit, async move {
                    serve_connection(
                        stream,
                        expected_uid,
                        manager,
                        authority,
                        connection_shutdown,
                        cancelled,
                    )
                    .await
                })
                .await;
        }
    }

    pub async fn shutdown_and_drain(&self, timeout: Duration) -> io::Result<usize> {
        let deadline = tokio::time::Instant::now() + timeout;
        self.cancelled.store(true, Ordering::SeqCst);
        let _ = self.shutdown.send(());
        if self.serving.load(Ordering::Acquire) {
            tokio::time::timeout_at(deadline, async {
                while self.serving.load(Ordering::Acquire) {
                    let stopped = self.stopped.notified();
                    if !self.serving.load(Ordering::Acquire) {
                        break;
                    }
                    stopped.await;
                }
            })
            .await
            .map_err(|_| {
                io::Error::new(io::ErrorKind::TimedOut, "theme accept loop did not stop")
            })?;
        }
        let report = self.connections.drain(deadline).await;
        Ok(report.completed + report.aborted)
    }

    pub fn path(&self) -> &Path {
        self.endpoint.path()
    }
}

struct ServeGuard<'a> {
    socket: &'a ThemeSocket,
}
impl Drop for ServeGuard<'_> {
    fn drop(&mut self) {
        self.socket.serving.store(false, Ordering::Release);
        self.socket.stopped.notify_waiters();
    }
}

async fn serve_connection(
    stream: UnixStream,
    expected_uid: libc::uid_t,
    manager: Arc<Mutex<ThemeManager>>,
    authority: GenerationAuthority,
    mut shutdown: broadcast::Receiver<()>,
    cancelled: Arc<AtomicBool>,
) -> io::Result<()> {
    if peer_uid(&stream)? != expected_uid {
        return Err(io::Error::new(
            io::ErrorKind::PermissionDenied,
            "theme socket peer UID mismatch",
        ));
    }
    let (read, write) = tokio::io::split(stream);
    let reader = Arc::new(Mutex::new(BufReader::new(read)));
    let writer = Arc::new(Mutex::new(write));
    let request_bytes = tokio::select! {
        biased;
        _ = shutdown.recv() => return Ok(()),
        line = tokio::time::timeout(READ_TIMEOUT, read_line(&reader)) => line
            .map_err(|_| io::Error::new(io::ErrorKind::TimedOut, "theme request read timed out"))??,
    };
    let request: ThemeRequest = serde_json::from_slice(&request_bytes)
        .map_err(|error| io::Error::new(io::ErrorKind::InvalidData, error))?;
    validate_request(&request)?;
    let sink = SocketThemeSink {
        request_id: request.request_id.clone(),
        reader,
        writer: Arc::clone(&writer),
    };
    let control = RunControl::for_request(Instant::now() + Duration::from_secs(2), cancelled);
    let mut manager = manager.lock().await;
    if request.operation.is_mutation() && manager.has_journal().map_err(other)? {
        let response = match manager
            .reconcile_controlled(&sink, &authority, &control)
            .await
        {
            Ok(outcome) if outcome.reconciled => result(
                &request.request_id,
                ThemeStatus::Reconciled,
                outcome.generation,
                Some(manager.current().map_err(other)?),
                None,
            ),
            Ok(_) => unreachable!("journal presence was checked under the manager lock"),
            Err(error) => error_result(&request.request_id, error),
        };
        return write_message(&writer, &response).await;
    }
    let response = match request.operation {
        ThemeOperation::Get => match manager.current() {
            Ok(theme) => result(
                &request.request_id,
                ThemeStatus::Confirmed,
                None,
                Some(theme),
                None,
            ),
            Err(error) => result(
                &request.request_id,
                ThemeStatus::Error,
                None,
                None,
                Some(error.to_string()),
            ),
        },
        ThemeOperation::List => match manager.list() {
            Ok(themes) => result_with_themes(&request.request_id, themes),
            Err(error) => result(
                &request.request_id,
                ThemeStatus::Error,
                None,
                None,
                Some(error.to_string()),
            ),
        },
        ThemeOperation::Import { document } => {
            match manager.import_async_controlled(&document, &control).await {
                Ok(theme) => result(
                    &request.request_id,
                    ThemeStatus::Confirmed,
                    None,
                    Some(theme),
                    None,
                ),
                Err(error) => error_result(&request.request_id, error),
            }
        }
        ThemeOperation::CopyForEdit { theme_id, name } => {
            match manager
                .copy_for_edit_async_controlled(&theme_id, &name, &control)
                .await
            {
                Ok(theme) => result(
                    &request.request_id,
                    ThemeStatus::Confirmed,
                    None,
                    Some(theme),
                    None,
                ),
                Err(error) => error_result(&request.request_id, error),
            }
        }
        ThemeOperation::Delete { theme_id } => {
            match manager.delete_async_controlled(&theme_id, &control).await {
                Ok(()) => result(
                    &request.request_id,
                    ThemeStatus::Confirmed,
                    None,
                    None,
                    None,
                ),
                Err(error) => error_result(&request.request_id, error),
            }
        }
        ThemeOperation::Apply {
            theme_id,
            expected_generation,
        } => match manager
            .apply_controlled(
                &theme_id,
                &request.request_id,
                expected_generation,
                &sink,
                &authority,
                &control,
            )
            .await
        {
            Ok(applied) => result(
                &request.request_id,
                ThemeStatus::Confirmed,
                Some(applied.generation),
                Some(applied.theme),
                None,
            ),
            Err(error) => error_result(&request.request_id, error),
        },
    };
    write_message(&writer, &response).await
}

fn validate_request(request: &ThemeRequest) -> io::Result<()> {
    if request.schema_version != WIRE_SCHEMA_VERSION {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "unknown theme request schema version",
        ));
    }
    uuid::Uuid::parse_str(&request.request_id)
        .map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "theme requestId must be UUID"))?;
    match &request.operation {
        ThemeOperation::Apply {
            theme_id,
            expected_generation,
        } => {
            if theme_id.trim().is_empty() || *expected_generation == 0 {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "invalid apply theme request",
                ));
            }
        }
        ThemeOperation::Get | ThemeOperation::List => {}
        ThemeOperation::CopyForEdit { theme_id, name }
            if theme_id.trim().is_empty() || name.trim().is_empty() =>
        {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "invalid theme copy request",
            ));
        }
        ThemeOperation::Delete { theme_id } if theme_id.trim().is_empty() => {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "invalid theme delete request",
            ));
        }
        ThemeOperation::Import { document } if document.len() > MAX_LINE => {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "theme import is too large",
            ));
        }
        _ => {}
    }
    Ok(())
}

fn result(
    request_id: &str,
    status: ThemeStatus,
    generation: Option<u64>,
    theme: Option<ThemeDocument>,
    error: Option<String>,
) -> ThemeMessage {
    ThemeMessage::Result {
        schema_version: WIRE_SCHEMA_VERSION,
        request_id: request_id.into(),
        status,
        generation,
        theme,
        themes: None,
        error,
    }
}

fn result_with_themes(request_id: &str, themes: Vec<ThemeDocument>) -> ThemeMessage {
    ThemeMessage::Result {
        schema_version: WIRE_SCHEMA_VERSION,
        request_id: request_id.into(),
        status: ThemeStatus::Confirmed,
        generation: None,
        theme: None,
        themes: Some(themes),
        error: None,
    }
}

fn error_result(request_id: &str, error: ThemeError) -> ThemeMessage {
    let status = match error.kind() {
        ThemeErrorKind::Busy => ThemeStatus::Busy,
        ThemeErrorKind::Timeout => ThemeStatus::Timeout,
        ThemeErrorKind::Cancelled => ThemeStatus::Cancelled,
        ThemeErrorKind::Other => ThemeStatus::Error,
    };
    result(request_id, status, None, None, Some(error.to_string()))
}

async fn read_line(reader: &Reader) -> io::Result<Vec<u8>> {
    let mut reader = reader.lock().await;
    let mut bytes = Vec::new();
    let count = (&mut *reader)
        .take((MAX_LINE + 1) as u64)
        .read_until(b'\n', &mut bytes)
        .await?;
    if count == 0 {
        return Err(io::Error::new(
            io::ErrorKind::UnexpectedEof,
            "theme client closed",
        ));
    }
    if bytes.len() > MAX_LINE || !bytes.ends_with(b"\n") {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "theme line exceeds limit",
        ));
    }
    bytes.pop();
    Ok(bytes)
}

async fn write_message(writer: &Writer, message: &ThemeMessage) -> io::Result<()> {
    let mut bytes = serde_json::to_vec(message)
        .map_err(|error| io::Error::new(io::ErrorKind::InvalidData, error))?;
    bytes.push(b'\n');
    let mut writer = writer.lock().await;
    tokio::time::timeout(WRITE_TIMEOUT, writer.write_all(&bytes))
        .await
        .map_err(|_| io::Error::new(io::ErrorKind::TimedOut, "theme response write timed out"))??;
    Ok(())
}

fn other(error: impl std::fmt::Display) -> io::Error {
    io::Error::other(error.to_string())
}
