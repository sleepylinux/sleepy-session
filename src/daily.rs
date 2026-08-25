// SPDX-License-Identifier: GPL-3.0-only

use std::{
    fs, io,
    path::{Path, PathBuf},
    sync::{Arc, Mutex},
    time::Duration,
};

use serde::{Deserialize, Serialize};
use serde_json::Value;
use sleepy_sdk::{DaemonCommand, DesktopLaunchRequest, WeatherLocation, WIRE_SCHEMA_VERSION};
use tokio::{
    io::{AsyncBufRead, AsyncBufReadExt, AsyncWriteExt, BufReader},
    net::UnixStream,
};

use crate::{
    calendar::IcsCalendarProvider,
    launcher::{DesktopEntryIndex, LaunchResources, LauncherMetrics},
    sessiond::private_socket::{peer_uid, NoopBindObserver, PrivateSocketEndpoint},
    system::{CommandRunner, CommandSpec, ProcessCommandRunner},
    weather::{CurlTransport, MetNoProvider, NominatimProvider, SystemClock},
};

const MAX_REQUEST_LINE: usize = 64 * 1024;

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct DailyRequest {
    pub schema_version: u32,
    pub request_id: String,
    pub operation: DailyOperation,
}

#[derive(Debug, Deserialize)]
#[serde(
    tag = "type",
    content = "data",
    rename_all = "camelCase",
    rename_all_fields = "camelCase",
    deny_unknown_fields
)]
pub enum DailyOperation {
    LauncherSearch {
        query: String,
    },
    Launch {
        request: DesktopLaunchRequest,
    },
    Overview {
        command: DaemonCommand,
    },
    Calendar {
        window_start: String,
        window_end: String,
    },
    Weather {
        location: WeatherLocation,
    },
    GeocodeSubmit {
        query: String,
    },
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct DailyResponse {
    pub schema_version: u32,
    pub request_id: String,
    pub status: DailyStatus,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub data: Option<Value>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum DailyStatus {
    Confirmed,
    Error,
}

pub trait DailyBackend: Send + Sync + 'static {
    fn handle(&self, operation: DailyOperation) -> io::Result<Value>;
}

pub struct ProductionDailyBackend {
    launcher: DesktopEntryIndex,
    metrics: Mutex<LauncherMetrics>,
    calendar: IcsCalendarProvider,
    weather: MetNoProvider<CurlTransport, SystemClock>,
    geocoder: NominatimProvider<CurlTransport, SystemClock>,
    runner: ProcessCommandRunner,
}

impl ProductionDailyBackend {
    pub fn open(state_dir: &Path, cache_dir: &Path) -> io::Result<Self> {
        let launcher = DesktopEntryIndex::scan_xdg(executable_available)?;
        let metrics = LauncherMetrics::open(&state_dir.join("sleepy/launcher.json"))?;
        let calendar_dir = std::env::var_os("SLEEPY_CALENDAR_DIR").map(PathBuf::from);
        let calendar_sources = calendar_dir.map_or_else(Vec::new, |directory| {
            fs::read_dir(directory)
                .into_iter()
                .flatten()
                .filter_map(Result::ok)
                .map(|entry| entry.path())
                .filter(|path| path.extension().is_some_and(|value| value == "ics"))
                .take(1024)
                .collect()
        });
        let user_agent = std::env::var("SLEEPY_PROVIDER_USER_AGENT")
            .unwrap_or_else(|_| "SleepyLinux/3 https://sleepylinux.org".into());
        let met_endpoint = std::env::var("SLEEPY_MET_ENDPOINT").unwrap_or_else(|_| {
            "https://api.met.no/weatherapi/locationforecast/2.0/compact".into()
        });
        let nominatim_endpoint = std::env::var("SLEEPY_NOMINATIM_ENDPOINT")
            .unwrap_or_else(|_| "https://nominatim.openstreetmap.org/search".into());
        Ok(Self {
            launcher,
            metrics: Mutex::new(metrics),
            calendar: IcsCalendarProvider::new(calendar_sources, 4096),
            weather: MetNoProvider::new(
                &met_endpoint,
                &user_agent,
                cache_dir.join("sleepy/met.json"),
                CurlTransport,
                SystemClock,
            )?,
            geocoder: NominatimProvider::new(
                &nominatim_endpoint,
                &user_agent,
                cache_dir.join("sleepy/nominatim.json"),
                CurlTransport,
                SystemClock,
            )?,
            runner: ProcessCommandRunner,
        })
    }
}

impl DailyBackend for ProductionDailyBackend {
    fn handle(&self, operation: DailyOperation) -> io::Result<Value> {
        match operation {
            DailyOperation::LauncherSearch { query } => {
                if query.len() > 512 || query.contains('\0') {
                    return Err(invalid("launcher query is invalid"));
                }
                let entries = self.launcher.entries();
                let ids = entries
                    .iter()
                    .map(|entry| entry.desktop_id.as_str())
                    .collect::<Vec<_>>();
                let ranked = self
                    .metrics
                    .lock()
                    .map_err(|_| io::Error::other("launcher metrics lock poisoned"))?
                    .rank(&query, &ids);
                let result = ranked
                    .into_iter()
                    .filter_map(|id| self.launcher.get(&id))
                    .take(100)
                    .collect::<Vec<_>>();
                serde_json::to_value(result).map_err(io::Error::other)
            }
            DailyOperation::Launch { request } => {
                if request.schema_version != WIRE_SCHEMA_VERSION {
                    return Err(invalid("unsupported launch schema"));
                }
                let resources = LaunchResources {
                    files: request
                        .resources
                        .iter()
                        .filter(|value| !value.contains("://"))
                        .cloned()
                        .collect(),
                    urls: request
                        .resources
                        .iter()
                        .filter(|value| value.contains("://"))
                        .cloned()
                        .collect(),
                };
                let argv = self.launcher.launch_argv(
                    &request.desktop_id,
                    request.action_id.as_deref(),
                    &resources,
                )?;
                let mut args = vec![
                    "--user".into(),
                    "--scope".into(),
                    "--collect".into(),
                    "--quiet".into(),
                    "--".into(),
                ];
                args.extend(argv);
                let mut command = CommandSpec::new("systemd-run", args);
                command.timeout = Duration::from_secs(3);
                let output = self.runner.run(&command).map_err(io::Error::other)?;
                if output.status != 0 {
                    return Err(io::Error::other("indexed application launch failed"));
                }
                self.metrics
                    .lock()
                    .map_err(|_| io::Error::other("launcher metrics lock poisoned"))?
                    .record_launch(&request.desktop_id, SystemClock::unix_now())?;
                Ok(serde_json::json!({"desktopId": request.desktop_id}))
            }
            DailyOperation::Overview { command } => {
                let action = overview_args(&command)?;
                let mut execute = CommandSpec::new("niri", action);
                execute.timeout = Duration::from_millis(1200);
                let result = self.runner.run(&execute).map_err(io::Error::other)?;
                if result.status != 0 {
                    return Err(io::Error::other("Niri overview command failed"));
                }
                confirm_overview_readback(&self.runner, &command)?;
                Ok(serde_json::json!({"confirmed": true}))
            }
            DailyOperation::Calendar {
                window_start,
                window_end,
            } => serde_json::to_value(self.calendar.snapshot(&window_start, &window_end)?)
                .map_err(io::Error::other),
            DailyOperation::Weather { location } => {
                serde_json::to_value(self.weather.snapshot(&location)?).map_err(io::Error::other)
            }
            DailyOperation::GeocodeSubmit { query } => {
                serde_json::to_value(self.geocoder.submit(&query)?).map_err(io::Error::other)
            }
        }
    }
}

impl SystemClock {
    fn unix_now() -> u64 {
        <Self as crate::weather::Clock>::now(&Self)
    }
}

fn overview_args(command: &DaemonCommand) -> io::Result<Vec<String>> {
    match command {
        DaemonCommand::FocusWindow { window_id } if *window_id > 0 => Ok(vec![
            "msg".into(),
            "action".into(),
            "focus-window".into(),
            "--id".into(),
            window_id.to_string(),
        ]),
        DaemonCommand::CloseWindow { window_id } if *window_id > 0 => Ok(vec![
            "msg".into(),
            "action".into(),
            "close-window".into(),
            "--id".into(),
            window_id.to_string(),
        ]),
        DaemonCommand::FocusWorkspace { workspace_id } if *workspace_id > 0 => Ok(vec![
            "msg".into(),
            "action".into(),
            "focus-workspace".into(),
            workspace_id.to_string(),
        ]),
        _ => Err(invalid("operation is not a typed overview command")),
    }
}

fn confirm_overview_readback<R: CommandRunner>(
    runner: &R,
    command: &DaemonCommand,
) -> io::Result<()> {
    let subject = match command {
        DaemonCommand::FocusWorkspace { .. } => "workspaces",
        DaemonCommand::FocusWindow { .. } | DaemonCommand::CloseWindow { .. } => "windows",
        _ => return Err(invalid("operation is not a typed overview command")),
    };
    let mut readback = CommandSpec::new("niri", ["msg", "--json", subject]);
    readback.timeout = Duration::from_millis(1200);
    let output = runner.run(&readback).map_err(io::Error::other)?;
    if output.status != 0 {
        return Err(io::Error::other("Niri overview readback failed"));
    }
    let values: Vec<Value> = serde_json::from_slice(&output.stdout)
        .map_err(|_| invalid("Niri overview readback is malformed"))?;
    let confirmed = match command {
        DaemonCommand::FocusWindow { window_id } => values.iter().any(|value| {
            value.get("id").and_then(Value::as_u64) == Some(*window_id)
                && value.get("is_focused").and_then(Value::as_bool) == Some(true)
        }),
        DaemonCommand::CloseWindow { window_id } => values
            .iter()
            .all(|value| value.get("id").and_then(Value::as_u64) != Some(*window_id)),
        DaemonCommand::FocusWorkspace { workspace_id } => values.iter().any(|value| {
            value.get("id").and_then(Value::as_u64) == Some(*workspace_id)
                && value.get("is_focused").and_then(Value::as_bool) == Some(true)
        }),
        _ => false,
    };
    if confirmed {
        Ok(())
    } else {
        Err(io::Error::new(
            io::ErrorKind::TimedOut,
            "Niri overview readback did not confirm the command",
        ))
    }
}

fn executable_available(program: &str) -> bool {
    if program.contains('/') {
        return Path::new(program).is_file();
    }
    std::env::var_os("PATH").is_some_and(|path| {
        std::env::split_paths(&path).any(|directory| directory.join(program).is_file())
    })
}

pub struct DailySocket<B> {
    endpoint: PrivateSocketEndpoint,
    backend: Arc<B>,
    shutdown: tokio::sync::broadcast::Sender<()>,
    tasks: tokio::sync::Mutex<Vec<tokio::task::JoinHandle<io::Result<()>>>>,
}

impl<B: DailyBackend> DailySocket<B> {
    pub async fn bind(
        path: impl AsRef<Path>,
        expected_uid: libc::uid_t,
        backend: Arc<B>,
    ) -> io::Result<Self> {
        let endpoint = PrivateSocketEndpoint::bind_with_observer(
            path,
            expected_uid,
            Arc::new(NoopBindObserver),
        )
        .await?;
        let (shutdown, _) = tokio::sync::broadcast::channel(1);
        Ok(Self {
            endpoint,
            backend,
            shutdown,
            tasks: tokio::sync::Mutex::new(Vec::new()),
        })
    }
    pub async fn serve(&self) -> io::Result<()> {
        let mut shutdown = self.shutdown.subscribe();
        loop {
            let stream = tokio::select! { accepted = self.endpoint.accept() => accepted?, _ = shutdown.recv() => return Ok(()) };
            let backend = Arc::clone(&self.backend);
            let uid = self.endpoint.expected_uid();
            self.tasks.lock().await.push(tokio::spawn(async move {
                serve_client(stream, uid, backend).await
            }));
        }
    }
    pub async fn shutdown_and_drain(&self, timeout: Duration) -> io::Result<()> {
        let _ = self.shutdown.send(());
        let deadline = tokio::time::Instant::now() + timeout;
        for mut task in std::mem::take(&mut *self.tasks.lock().await) {
            if tokio::time::timeout_at(deadline, &mut task).await.is_err() {
                task.abort();
                let _ = task.await;
            }
        }
        Ok(())
    }
}

async fn serve_client<B: DailyBackend>(
    stream: UnixStream,
    expected_uid: libc::uid_t,
    backend: Arc<B>,
) -> io::Result<()> {
    if peer_uid(&stream)? != expected_uid {
        return Err(io::Error::new(
            io::ErrorKind::PermissionDenied,
            "daily socket peer UID mismatch",
        ));
    }
    let (read, mut write) = stream.into_split();
    let mut reader = BufReader::new(read);
    while let Some(line) = read_bounded_line(&mut reader).await? {
        let response = match serde_json::from_slice::<DailyRequest>(&line) {
            Ok(request)
                if request.schema_version == WIRE_SCHEMA_VERSION
                    && uuid::Uuid::parse_str(&request.request_id).is_ok() =>
            {
                match backend.handle(request.operation) {
                    Ok(data) => DailyResponse {
                        schema_version: WIRE_SCHEMA_VERSION,
                        request_id: request.request_id,
                        status: DailyStatus::Confirmed,
                        data: Some(data),
                        error: None,
                    },
                    Err(error) => error_response(&request.request_id, &error.to_string()),
                }
            }
            Ok(request) => error_response(&request.request_id, "invalid daily request contract"),
            Err(error) => error_response("unknown", &format!("invalid daily request: {error}")),
        };
        let mut bytes = serde_json::to_vec(&response).map_err(io::Error::other)?;
        bytes.push(b'\n');
        write.write_all(&bytes).await?;
    }
    Ok(())
}

async fn read_bounded_line<R: AsyncBufRead + Unpin>(reader: &mut R) -> io::Result<Option<Vec<u8>>> {
    let mut line = Vec::with_capacity(MAX_REQUEST_LINE);
    loop {
        let available = reader.fill_buf().await?;
        if available.is_empty() {
            return Ok((!line.is_empty()).then_some(line));
        }
        let take = available
            .iter()
            .position(|byte| *byte == b'\n')
            .map_or(available.len(), |position| position + 1);
        if line.len().saturating_add(take) > MAX_REQUEST_LINE {
            return Err(invalid("daily request exceeded limit"));
        }
        line.extend_from_slice(&available[..take]);
        reader.consume(take);
        if line.last() == Some(&b'\n') {
            return Ok(Some(line));
        }
    }
}

fn error_response(request_id: &str, message: &str) -> DailyResponse {
    DailyResponse {
        schema_version: WIRE_SCHEMA_VERSION,
        request_id: request_id.into(),
        status: DailyStatus::Error,
        data: None,
        error: Some(message.chars().take(1024).collect()),
    }
}
fn invalid(message: &str) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidInput, message)
}
