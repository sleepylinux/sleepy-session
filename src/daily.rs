// SPDX-License-Identifier: GPL-3.0-only

use std::{
    fs, io,
    path::{Path, PathBuf},
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc, Mutex,
    },
    time::{Duration, Instant},
};

use serde::{Deserialize, Serialize};
use serde_json::Value;
use sha2::{Digest, Sha256};
use sleepy_sdk::{DaemonCommand, DesktopLaunchRequest, WeatherLocation, WIRE_SCHEMA_VERSION};
use tokio::{
    io::{AsyncBufRead, AsyncBufReadExt, AsyncWriteExt, BufReader},
    net::UnixStream,
};

use crate::{
    calendar::IcsCalendarProvider,
    launcher::{DesktopEntryIndex, LaunchResources, LauncherMetrics},
    overview::{
        overview_event_channel, ChannelOverviewEvents, NiriOverview, ProcessOverviewRunner,
    },
    sessiond::private_socket::{peer_uid, NoopBindObserver, PrivateSocketEndpoint},
    system::{CommandRunner, CommandSpec, ProcessCommandRunner, RunControl},
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
    Busy,
    Error,
}

pub trait DailyBackend: Send + Sync + 'static {
    fn handle_controlled(
        &self,
        operation: DailyOperation,
        control: &RunControl,
    ) -> io::Result<Value>;
}

pub struct ProductionDailyBackend {
    launcher: DesktopEntryIndex,
    metrics: Mutex<LauncherMetrics>,
    calendar: ProviderSlot<IcsCalendarProvider>,
    weather: ProviderSlot<MetNoProvider<CurlTransport, SystemClock>>,
    geocoder: ProviderSlot<NominatimProvider<CurlTransport, SystemClock>>,
    runner: ProcessCommandRunner,
    overview: Mutex<NiriOverview<ProcessOverviewRunner, ChannelOverviewEvents>>,
    fallback_overview_sender: Option<crate::overview::OverviewEventSender>,
}

enum ProviderSlot<T> {
    Available(T),
    Degraded(String),
}

impl<T> ProviderSlot<T> {
    fn get(&self, provider: &str) -> io::Result<&T> {
        match self {
            Self::Available(value) => Ok(value),
            Self::Degraded(message) => Err(io::Error::new(
                io::ErrorKind::NotConnected,
                format!("{provider} provider is degraded: {message}"),
            )),
        }
    }
}

impl ProductionDailyBackend {
    pub fn open(state_dir: &Path, cache_dir: &Path) -> io::Result<Self> {
        let (sender, events) = overview_event_channel(1);
        let mut backend = Self::open_with_overview(state_dir, cache_dir, events)?;
        backend.fallback_overview_sender = Some(sender);
        Ok(backend)
    }

    pub fn open_with_overview(
        state_dir: &Path,
        cache_dir: &Path,
        overview_events: ChannelOverviewEvents,
    ) -> io::Result<Self> {
        let launcher = DesktopEntryIndex::scan_xdg(executable_available)?;
        let metrics = LauncherMetrics::open(&state_dir.join("sleepy/launcher.json"))?;
        let calendar_dir = std::env::var_os("SLEEPY_CALENDAR_DIR").map(PathBuf::from);
        let calendar = match calendar_dir.map(enumerate_calendar_sources).transpose() {
            Ok(sources) => {
                ProviderSlot::Available(IcsCalendarProvider::new(sources.unwrap_or_default(), 4096))
            }
            Err(error) => ProviderSlot::Degraded(error.to_string()),
        };
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
            calendar,
            weather: provider_slot(MetNoProvider::new(
                &met_endpoint,
                &user_agent,
                cache_dir.join("sleepy/met.json"),
                CurlTransport,
                SystemClock,
            )),
            geocoder: provider_slot(NominatimProvider::new(
                &nominatim_endpoint,
                &user_agent,
                cache_dir.join("sleepy/nominatim.json"),
                CurlTransport,
                SystemClock,
            )),
            runner: ProcessCommandRunner,
            overview: Mutex::new(NiriOverview::new(
                ProcessOverviewRunner::default(),
                overview_events,
                Duration::from_millis(1500),
            )),
            fallback_overview_sender: None,
        })
    }
}

fn provider_slot<T>(result: io::Result<T>) -> ProviderSlot<T> {
    match result {
        Ok(provider) => ProviderSlot::Available(provider),
        Err(error) => ProviderSlot::Degraded(error.to_string()),
    }
}

fn enumerate_calendar_sources(directory: PathBuf) -> io::Result<Vec<PathBuf>> {
    let mut sources = Vec::new();
    for (entry_index, entry) in fs::read_dir(directory)?.enumerate() {
        if entry_index == 4096 {
            return Err(invalid("calendar directory entry count exceeded limit"));
        }
        let entry = entry?;
        let path = entry.path();
        if path.extension().is_some_and(|value| value == "ics") {
            if sources.len() == 1024 {
                return Err(invalid("calendar directory source count exceeded limit"));
            }
            sources.push(path);
        }
    }
    sources.sort();
    Ok(sources)
}

impl ProductionDailyBackend {
    fn handle_operation(
        &self,
        operation: DailyOperation,
        control: &RunControl,
    ) -> io::Result<Value> {
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
                let resources = classify_launch_resources(&request.resources)?;
                let argv = self.launcher.launch_argv(
                    &request.desktop_id,
                    request.action_id.as_deref(),
                    &resources,
                )?;
                let args = transient_launch_args(&request, argv);
                let mut command = CommandSpec::new("systemd-run", args);
                command.timeout = Duration::from_secs(3);
                let output = self
                    .runner
                    .run_controlled(&command, control)
                    .map_err(io::Error::other)?;
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
                self.overview
                    .lock()
                    .map_err(|_| io::Error::other("overview actor lock poisoned"))?
                    .execute_controlled(command, control)?;
                Ok(serde_json::json!({"confirmed": true}))
            }
            DailyOperation::Calendar {
                window_start,
                window_end,
            } => serde_json::to_value(self.calendar.get("calendar")?.snapshot_controlled(
                &window_start,
                &window_end,
                control,
            )?)
            .map_err(io::Error::other),
            DailyOperation::Weather { location } => serde_json::to_value(
                self.weather
                    .get("weather")?
                    .snapshot_controlled(&location, control)?,
            )
            .map_err(io::Error::other),
            DailyOperation::GeocodeSubmit { query } => serde_json::to_value(
                self.geocoder
                    .get("geocoding")?
                    .submit_controlled(&query, control)?,
            )
            .map_err(io::Error::other),
        }
    }
}

fn classify_launch_resources(resources: &[String]) -> io::Result<LaunchResources> {
    let mut classified = LaunchResources::default();
    for value in resources {
        if value.is_empty() || value.contains('\0') {
            return Err(invalid("launch resource is invalid"));
        }
        if has_uri_scheme(value) {
            classified.urls.push(value.clone());
        } else {
            classified.files.push(value.clone());
        }
    }
    Ok(classified)
}

fn has_uri_scheme(value: &str) -> bool {
    let Some((scheme, _)) = value.split_once(':') else {
        return false;
    };
    let mut bytes = scheme.bytes();
    bytes.next().is_some_and(|byte| byte.is_ascii_alphabetic())
        && bytes.all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'+' | b'-' | b'.'))
}

impl DailyBackend for ProductionDailyBackend {
    fn handle_controlled(
        &self,
        operation: DailyOperation,
        control: &RunControl,
    ) -> io::Result<Value> {
        self.handle_operation(operation, control)
    }
}

fn transient_launch_args(request: &DesktopLaunchRequest, argv: Vec<String>) -> Vec<String> {
    static NEXT_LAUNCH: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(1);
    let mut digest = Sha256::new();
    digest.update(request.desktop_id.as_bytes());
    if let Some(action) = &request.action_id {
        digest.update([0]);
        digest.update(action.as_bytes());
    }
    let hash = format!("{:x}", digest.finalize());
    let sequence = NEXT_LAUNCH.fetch_add(1, Ordering::Relaxed);
    let unit = format!(
        "sleepy-launch-{}-{}-{sequence}.service",
        &hash[..16],
        std::process::id()
    );
    let mut args = vec![
        "--user".into(),
        "--collect".into(),
        "--no-block".into(),
        "--quiet".into(),
        "--service-type=exec".into(),
        "--unit".into(),
        unit,
        "--".into(),
    ];
    args.extend(argv);
    args
}

impl SystemClock {
    fn unix_now() -> u64 {
        <Self as crate::weather::Clock>::now(&Self)
    }
}

fn executable_available(program: &str) -> bool {
    if program.contains('/') {
        return executable_regular_file(Path::new(program));
    }
    std::env::var_os("PATH").is_some_and(|path| {
        std::env::split_paths(&path)
            .any(|directory| executable_regular_file(&directory.join(program)))
    })
}

fn executable_regular_file(path: &Path) -> bool {
    use std::os::unix::ffi::OsStrExt;
    let Ok(metadata) = fs::metadata(path) else {
        return false;
    };
    if !metadata.file_type().is_file() {
        return false;
    }
    let Ok(path) = std::ffi::CString::new(path.as_os_str().as_bytes()) else {
        return false;
    };
    unsafe { libc::access(path.as_ptr(), libc::X_OK) == 0 }
}

pub struct DailySocket<B> {
    endpoint: PrivateSocketEndpoint,
    backend: Arc<B>,
    shutdown: tokio::sync::broadcast::Sender<()>,
    tasks: tokio::sync::Mutex<Vec<tokio::task::JoinHandle<io::Result<()>>>>,
    workers: Arc<tokio::sync::Semaphore>,
    admission: Arc<tokio::sync::Semaphore>,
    request_timeout: Duration,
    stopping: Arc<AtomicBool>,
}

impl<B: DailyBackend> DailySocket<B> {
    pub async fn bind(
        path: impl AsRef<Path>,
        expected_uid: libc::uid_t,
        backend: Arc<B>,
    ) -> io::Result<Self> {
        Self::bind_with_limits(path, expected_uid, backend, 8, Duration::from_secs(8)).await
    }

    pub async fn bind_with_limits(
        path: impl AsRef<Path>,
        expected_uid: libc::uid_t,
        backend: Arc<B>,
        max_workers: usize,
        request_timeout: Duration,
    ) -> io::Result<Self> {
        if max_workers == 0 || request_timeout.is_zero() {
            return Err(invalid("daily worker limits are invalid"));
        }
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
            workers: Arc::new(tokio::sync::Semaphore::new(max_workers)),
            admission: Arc::new(tokio::sync::Semaphore::new(max_workers)),
            request_timeout,
            stopping: Arc::new(AtomicBool::new(false)),
        })
    }
    pub async fn serve(&self) -> io::Result<()> {
        let mut shutdown = self.shutdown.subscribe();
        loop {
            let stream = tokio::select! { accepted = self.endpoint.accept() => accepted?, _ = shutdown.recv() => return Ok(()) };
            if self.stopping.load(Ordering::SeqCst) {
                return Ok(());
            }
            if peer_uid(&stream)? != self.endpoint.expected_uid() {
                continue;
            }
            let connection_permit = match Arc::clone(&self.admission).try_acquire_owned() {
                Ok(permit) => permit,
                Err(_) => {
                    reject_busy(stream).await?;
                    continue;
                }
            };
            let backend = Arc::clone(&self.backend);
            let workers = Arc::clone(&self.workers);
            let client_shutdown = self.shutdown.subscribe();
            let request_timeout = self.request_timeout;
            let stopping = Arc::clone(&self.stopping);
            let mut tasks = self.tasks.lock().await;
            let mut index = 0;
            while index < tasks.len() {
                if tasks[index].is_finished() {
                    let completed = tasks.remove(index);
                    let _ = completed.await;
                } else {
                    index += 1;
                }
            }
            if self.stopping.load(Ordering::SeqCst) {
                return Ok(());
            }
            tasks.push(tokio::spawn(async move {
                serve_client(
                    stream,
                    backend,
                    workers,
                    client_shutdown,
                    request_timeout,
                    stopping,
                    connection_permit,
                )
                .await
            }));
        }
    }
    pub async fn shutdown_and_drain(&self, timeout: Duration) -> io::Result<()> {
        self.stopping.store(true, Ordering::SeqCst);
        let _ = self.shutdown.send(());
        let deadline = tokio::time::Instant::now() + timeout;
        let mut deadline_missed = false;
        let mut first_error = None;
        for mut task in std::mem::take(&mut *self.tasks.lock().await) {
            let joined = match tokio::time::timeout_at(deadline, &mut task).await {
                Ok(joined) => joined,
                Err(_) => {
                    deadline_missed = true;
                    // Never detach a blocking request. Its RunControl has been
                    // cancelled by the connection task; join until its process /
                    // network cleanup (kill, wait and reader joins) is complete.
                    task.await
                }
            };
            match joined {
                Ok(Ok(())) => {}
                Ok(Err(error)) => {
                    first_error.get_or_insert(error);
                }
                Err(error) => {
                    first_error.get_or_insert_with(|| {
                        io::Error::other(format!("daily client task failed: {error}"))
                    });
                }
            }
        }
        if deadline_missed {
            first_error.get_or_insert_with(|| {
                io::Error::new(
                    io::ErrorKind::TimedOut,
                    "daily workers missed shutdown deadline",
                )
            });
        }
        first_error.map_or(Ok(()), Err)
    }
}

async fn serve_client<B: DailyBackend>(
    stream: UnixStream,
    backend: Arc<B>,
    workers: Arc<tokio::sync::Semaphore>,
    mut shutdown: tokio::sync::broadcast::Receiver<()>,
    request_timeout: Duration,
    stopping: Arc<AtomicBool>,
    _connection_permit: tokio::sync::OwnedSemaphorePermit,
) -> io::Result<()> {
    let (read, mut write) = stream.into_split();
    let mut reader = BufReader::new(read);
    loop {
        if stopping.load(Ordering::SeqCst) {
            return Ok(());
        }
        let line = tokio::select! {
            biased;
            _ = shutdown.recv() => return Ok(()),
            line = read_bounded_line(&mut reader) => line?,
        };
        let Some(line) = line else {
            return Ok(());
        };
        let response_deadline = Instant::now() + request_timeout;
        let response = match serde_json::from_slice::<DailyRequest>(&line) {
            Ok(request)
                if request.schema_version == WIRE_SCHEMA_VERSION
                    && uuid::Uuid::parse_str(&request.request_id).is_ok() =>
            {
                let permit = tokio::select! {
                    biased;
                    _ = shutdown.recv() => return Ok(()),
                    permit = Arc::clone(&workers).acquire_owned() => permit.map_err(|_| io::Error::new(io::ErrorKind::BrokenPipe, "daily worker pool closed"))?,
                };
                let cancelled = Arc::new(AtomicBool::new(false));
                let control = RunControl::for_request(response_deadline, Arc::clone(&cancelled));
                let worker_backend = Arc::clone(&backend);
                let operation = request.operation;
                let mut worker = tokio::task::spawn_blocking(move || {
                    let _permit = permit;
                    worker_backend.handle_controlled(operation, &control)
                });
                let handled = tokio::select! {
                    biased;
                    _ = shutdown.recv() => {
                        cancelled.store(true, Ordering::SeqCst);
                        let _ = worker.await.map_err(|error| io::Error::other(format!("daily worker failed: {error}")))?;
                        return Ok(());
                    }
                    _ = tokio::time::sleep(response_deadline.saturating_duration_since(Instant::now())) => {
                        cancelled.store(true, Ordering::SeqCst);
                        let _ = worker.await.map_err(|error| io::Error::other(format!("daily worker failed: {error}")))?;
                        Err(io::Error::new(io::ErrorKind::TimedOut, "daily request exceeded deadline"))
                    }
                    result = &mut worker => result.map_err(|error| io::Error::other(format!("daily worker failed: {error}")))?,
                };
                match handled {
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
        if stopping.load(Ordering::SeqCst) {
            return Ok(());
        }
        let mut bytes = serde_json::to_vec(&response).map_err(io::Error::other)?;
        bytes.push(b'\n');
        let remaining = response_deadline.saturating_duration_since(Instant::now());
        if remaining.is_zero() {
            return Err(io::Error::new(
                io::ErrorKind::TimedOut,
                "daily response exceeded request deadline",
            ));
        }
        tokio::select! {
            biased;
            _ = shutdown.recv() => return Ok(()),
            written = tokio::time::timeout(remaining, write.write_all(&bytes)) => {
                written
                    .map_err(|_| io::Error::new(io::ErrorKind::TimedOut, "daily response write exceeded request deadline"))??;
            }
        }
    }
}

async fn reject_busy(mut stream: UnixStream) -> io::Result<()> {
    let response = DailyResponse {
        schema_version: WIRE_SCHEMA_VERSION,
        request_id: "unknown".into(),
        status: DailyStatus::Busy,
        data: None,
        error: Some("daily service is busy".into()),
    };
    let mut bytes = serde_json::to_vec(&response).map_err(io::Error::other)?;
    bytes.push(b'\n');
    let _ = tokio::time::timeout(Duration::from_millis(100), stream.write_all(&bytes)).await;
    let _ = stream.shutdown().await;
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

#[cfg(test)]
mod tests {
    use super::*;
    use std::os::unix::fs::PermissionsExt;

    #[test]
    fn try_exec_requires_regular_executable_access() {
        let root = tempfile::tempdir().unwrap();
        let file = root.path().join("app");
        fs::write(&file, b"#!/bin/sh\n").unwrap();
        fs::set_permissions(&file, fs::Permissions::from_mode(0o600)).unwrap();
        assert!(!executable_available(file.to_str().unwrap()));
        fs::set_permissions(&file, fs::Permissions::from_mode(0o700)).unwrap();
        assert!(executable_available(file.to_str().unwrap()));
        assert!(!executable_available(root.path().to_str().unwrap()));
    }

    #[test]
    fn transient_launch_is_no_block_fixed_argv_with_inert_hostile_arguments() {
        let request: DesktopLaunchRequest = serde_json::from_value(serde_json::json!({
            "schemaVersion": WIRE_SCHEMA_VERSION,
            "desktopId": "safe.desktop",
            "resources": ["; touch /tmp/nope", "$(bad)"]
        }))
        .unwrap();
        let args = transient_launch_args(
            &request,
            vec!["viewer".into(), "; touch /tmp/nope".into(), "$(bad)".into()],
        );
        assert!(args.iter().any(|arg| arg == "--no-block"));
        assert!(!args.iter().any(|arg| arg == "--scope"));
        let separator = args.iter().position(|arg| arg == "--").unwrap();
        assert_eq!(
            &args[separator + 1..],
            ["viewer", "; touch /tmp/nope", "$(bad)"]
        );
        let unit = &args[args.iter().position(|arg| arg == "--unit").unwrap() + 1];
        assert!(unit.starts_with("sleepy-launch-") && unit.ends_with(".service"));
        assert!(unit
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'.')));
    }

    #[test]
    fn resource_classification_recognizes_all_valid_uri_schemes() {
        let resources = classify_launch_resources(&[
            "mailto:person@example.test".into(),
            "magnet:?xt=urn:btih:abc".into(),
            "geo:50.0,14.0".into(),
            "/tmp/local:name".into(),
            "relative-file".into(),
        ])
        .unwrap();
        assert_eq!(resources.urls.len(), 3);
        assert_eq!(resources.files, ["/tmp/local:name", "relative-file"]);
    }
}
