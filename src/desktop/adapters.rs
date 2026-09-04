// SPDX-License-Identifier: GPL-3.0-only

use std::{io, sync::Arc, time::Duration};

use async_trait::async_trait;
use serde_json::Value;
use sleepy_sdk::{
    CalendarSnapshot, CapabilityAvailability, DesktopNotificationSnapshot, DesktopOsdSnapshot,
    LauncherEntry, WeatherLocation, WeatherSnapshot,
};
use tokio::sync::{mpsc, Mutex};

use super::{
    join_error, DesktopDomainId, DesktopDomainState, DesktopDomainUpdate, DesktopDomainValue,
    DesktopProducer, DesktopProducerContext, ProducerError,
};
use crate::{
    compositor::{CompositorError, CompositorErrorKind, HyprlandAdapter, HyprlandEvent},
    daily::{DailyBackend, DailyOperation},
    notifications::NotificationEventService,
    osd::OsdPublicationHub,
    system::RunControl,
};

pub struct TerminalProducer {
    state: DesktopDomainState,
}

pub fn hyprland_terminal(error: CompositorError) -> io::Result<TerminalProducer> {
    TerminalProducer::new(
        DesktopDomainId::Hyprland,
        compositor_availability(error.kind()),
        error.to_string(),
    )
}

impl TerminalProducer {
    pub fn new(
        domain: DesktopDomainId,
        status: CapabilityAvailability,
        diagnostic: impl Into<String>,
    ) -> io::Result<Self> {
        Ok(Self {
            state: DesktopDomainState::terminal(domain, status, diagnostic)?,
        })
    }
}

#[async_trait]
impl DesktopProducer for TerminalProducer {
    fn domain(&self) -> DesktopDomainId {
        self.state.domain()
    }

    async fn initial(&self) -> DesktopDomainState {
        self.state.clone()
    }

    async fn run(
        &self,
        _sender: mpsc::Sender<DesktopDomainUpdate>,
        context: DesktopProducerContext,
    ) -> Result<(), ProducerError> {
        context.cancelled().await;
        Ok(())
    }
}

pub struct HyprlandProducer {
    adapter: HyprlandAdapter,
}

impl HyprlandProducer {
    pub fn new(adapter: HyprlandAdapter) -> Self {
        Self { adapter }
    }

    async fn snapshot(&self) -> DesktopDomainState {
        match self.adapter.snapshot().await {
            Ok(snapshot) => DesktopDomainState::available(
                DesktopDomainId::Hyprland,
                DesktopDomainValue::Hyprland(snapshot),
            )
            .expect("matching Hyprland domain"),
            Err(error) => DesktopDomainState::terminal(
                DesktopDomainId::Hyprland,
                compositor_availability(error.kind()),
                error.to_string(),
            )
            .expect("Hyprland errors have diagnostics"),
        }
    }
}

#[async_trait]
impl DesktopProducer for HyprlandProducer {
    fn domain(&self) -> DesktopDomainId {
        DesktopDomainId::Hyprland
    }

    async fn initial(&self) -> DesktopDomainState {
        self.snapshot().await
    }

    async fn run(
        &self,
        sender: mpsc::Sender<DesktopDomainUpdate>,
        context: DesktopProducerContext,
    ) -> Result<(), ProducerError> {
        let (events, mut received) = mpsc::channel::<HyprlandEvent>(64);
        let adapter = self.adapter.clone();
        let mut event_task = tokio::spawn(async move { adapter.run_events(events).await });
        loop {
            tokio::select! {
                biased;
                _ = context.cancelled() => {
                    event_task.abort();
                    let _ = event_task.await;
                    return Ok(());
                }
                result = &mut event_task => {
                    return result
                        .map_err(|error| ProducerError::new(format!("Hyprland event worker failed: {error}")))?
                        .map_err(|error| ProducerError::new(error.to_string()));
                }
                event = received.recv() => {
                    let Some(_event) = event else {
                        return Err(ProducerError::new("Hyprland event stream closed"));
                    };
                    let observation = context.begin_observation();
                    let update = observation
                        .finish(self.snapshot().await)
                        .map_err(|error| ProducerError::new(error.to_string()))?;
                    sender.send(update)
                        .await
                        .map_err(|_| ProducerError::new("desktop state authority stopped"))?;
                }
            }
        }
    }
}

pub struct NotificationProducer {
    service: Arc<Mutex<NotificationEventService>>,
}

impl NotificationProducer {
    pub fn new(service: Arc<Mutex<NotificationEventService>>) -> Self {
        Self { service }
    }

    async fn snapshot(&self) -> DesktopDomainState {
        let service = self.service.lock().await;
        let store = service.provider().store();
        DesktopDomainState::available(
            DesktopDomainId::Notifications,
            DesktopDomainValue::Notifications(DesktopNotificationSnapshot {
                availability: super::available_producer(),
                dnd: store.dnd(),
                active: store.active().to_vec(),
            }),
        )
        .expect("matching notification domain")
    }
}

#[async_trait]
impl DesktopProducer for NotificationProducer {
    fn domain(&self) -> DesktopDomainId {
        DesktopDomainId::Notifications
    }

    async fn initial(&self) -> DesktopDomainState {
        self.snapshot().await
    }

    async fn run(
        &self,
        sender: mpsc::Sender<DesktopDomainUpdate>,
        context: DesktopProducerContext,
    ) -> Result<(), ProducerError> {
        poll_updates(self, sender, context, Duration::from_secs(1)).await
    }
}

pub struct AppearanceProducer {
    service: Arc<super::appearance::AppearanceService>,
}

impl AppearanceProducer {
    pub fn new(service: Arc<super::appearance::AppearanceService>) -> Self {
        Self { service }
    }

    async fn initial_snapshot(&self) -> DesktopDomainState {
        appearance_state(self.service.snapshot().await)
    }

    async fn polling_snapshot(
        &self,
        context: &DesktopProducerContext,
    ) -> Option<DesktopDomainState> {
        match self.service.polling_snapshot(context).await {
            Err(error) if error.kind() == io::ErrorKind::WouldBlock => None,
            result => Some(appearance_state(result)),
        }
    }
}

fn appearance_state(
    result: io::Result<sleepy_sdk::DesktopAppearanceSnapshot>,
) -> DesktopDomainState {
    match result {
        Ok(snapshot) => DesktopDomainState::available(
            DesktopDomainId::Appearance,
            DesktopDomainValue::Appearance {
                theme: snapshot.theme,
                wallpaper_id: snapshot.wallpaper_id,
            },
        )
        .expect("matching appearance domain"),
        Err(error) => DesktopDomainState::terminal(
            DesktopDomainId::Appearance,
            CapabilityAvailability::Error,
            error.to_string(),
        )
        .expect("theme errors have diagnostics"),
    }
}

#[async_trait]
impl DesktopProducer for AppearanceProducer {
    fn domain(&self) -> DesktopDomainId {
        DesktopDomainId::Appearance
    }

    async fn initial(&self) -> DesktopDomainState {
        self.initial_snapshot().await
    }

    async fn run(
        &self,
        sender: mpsc::Sender<DesktopDomainUpdate>,
        context: DesktopProducerContext,
    ) -> Result<(), ProducerError> {
        poll_updates(self, sender, context, Duration::from_secs(2)).await
    }
}

pub struct DailyProducer<B: DailyBackend> {
    domain: DesktopDomainId,
    backend: Arc<B>,
    weather_location: Option<WeatherLocation>,
}

impl<B: DailyBackend> DailyProducer<B> {
    pub fn new(domain: DesktopDomainId, backend: Arc<B>) -> io::Result<Self> {
        if !matches!(
            domain,
            DesktopDomainId::Launcher | DesktopDomainId::Calendar | DesktopDomainId::Weather
        ) {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "daily producer was assigned an unrelated domain",
            ));
        }
        Ok(Self {
            domain,
            backend,
            weather_location: weather_location_from_environment(),
        })
    }

    async fn snapshot_value(
        &self,
        context: Option<&DesktopProducerContext>,
    ) -> io::Result<DesktopDomainValue> {
        if self.domain == DesktopDomainId::Weather && self.weather_location.is_none() {
            return Err(io::Error::new(
                io::ErrorKind::NotFound,
                "weather location is not configured",
            ));
        }
        let domain = self.domain;
        let backend = Arc::clone(&self.backend);
        let location = self.weather_location.clone();
        match context {
            Some(context) => {
                context
                    .spawn_blocking(
                        std::time::Instant::now() + Duration::from_secs(2),
                        move |control| daily_probe(domain, backend, location, &control),
                    )
                    .await
            }
            None => {
                tokio::task::spawn_blocking(move || {
                    daily_probe(
                        domain,
                        backend,
                        location,
                        &RunControl::for_timeout(Duration::from_secs(2)),
                    )
                })
                .await
            }
        }
        .map_err(join_error)?
    }

    async fn snapshot(&self, context: Option<&DesktopProducerContext>) -> DesktopDomainState {
        let domain = self.domain;
        match self.snapshot_value(context).await {
            Ok(value) => DesktopDomainState::available(domain, value)
                .unwrap_or_else(|error| terminal(domain, CapabilityAvailability::Parse, error)),
            Err(error) => terminal(domain, availability_for_io(&error), error),
        }
    }
}

#[async_trait]
impl<B: DailyBackend> DesktopProducer for DailyProducer<B> {
    fn domain(&self) -> DesktopDomainId {
        self.domain
    }

    async fn initial(&self) -> DesktopDomainState {
        self.snapshot(None).await
    }

    async fn run(
        &self,
        sender: mpsc::Sender<DesktopDomainUpdate>,
        context: DesktopProducerContext,
    ) -> Result<(), ProducerError> {
        poll_updates(self, sender, context, Duration::from_secs(30)).await
    }
}

pub struct OsdProducer {
    hub: OsdPublicationHub,
}

impl OsdProducer {
    pub fn new(hub: OsdPublicationHub) -> Self {
        Self { hub }
    }
}

#[async_trait]
impl DesktopProducer for OsdProducer {
    fn domain(&self) -> DesktopDomainId {
        DesktopDomainId::Osd
    }

    async fn initial(&self) -> DesktopDomainState {
        let mut subscriber = match self.hub.subscribe() {
            Ok(subscriber) => subscriber,
            Err(error) => {
                return terminal(DesktopDomainId::Osd, CapabilityAvailability::Error, error)
            }
        };
        match tokio::time::timeout(Duration::from_millis(1), subscriber.recv()).await {
            Ok(Ok(publication)) => osd_state(publication.visible),
            Ok(Err(error)) => terminal(DesktopDomainId::Osd, availability_for_io(&error), error),
            Err(_) => osd_state(Vec::new()),
        }
    }

    async fn run(
        &self,
        sender: mpsc::Sender<DesktopDomainUpdate>,
        context: DesktopProducerContext,
    ) -> Result<(), ProducerError> {
        let mut subscriber = self
            .hub
            .subscribe()
            .map_err(|error| ProducerError::new(error.to_string()))?;
        loop {
            tokio::select! {
                biased;
                _ = context.cancelled() => return Ok(()),
                publication = subscriber.recv() => {
                    let publication = publication.map_err(|error| ProducerError::new(error.to_string()))?;
                    let update = context
                        .begin_observation()
                        .finish(osd_state(publication.visible))
                        .map_err(|error| ProducerError::new(error.to_string()))?;
                    sender.send(update)
                        .await
                        .map_err(|_| ProducerError::new("desktop state authority stopped"))?;
                }
            }
        }
    }
}

#[async_trait]
trait PollingState {
    async fn polling_snapshot(
        &self,
        context: &DesktopProducerContext,
    ) -> Option<DesktopDomainState>;
}

#[async_trait]
impl PollingState for NotificationProducer {
    async fn polling_snapshot(
        &self,
        _context: &DesktopProducerContext,
    ) -> Option<DesktopDomainState> {
        Some(self.snapshot().await)
    }
}

#[async_trait]
impl PollingState for AppearanceProducer {
    async fn polling_snapshot(
        &self,
        context: &DesktopProducerContext,
    ) -> Option<DesktopDomainState> {
        self.polling_snapshot(context).await
    }
}

#[async_trait]
impl<B: DailyBackend> PollingState for DailyProducer<B> {
    async fn polling_snapshot(
        &self,
        context: &DesktopProducerContext,
    ) -> Option<DesktopDomainState> {
        match self.snapshot_value(Some(context)).await {
            Ok(value) => Some(
                DesktopDomainState::available(self.domain, value).unwrap_or_else(|error| {
                    terminal(self.domain, CapabilityAvailability::Parse, error)
                }),
            ),
            Err(error) if error.kind() == io::ErrorKind::WouldBlock => None,
            Err(error) => Some(terminal(self.domain, availability_for_io(&error), error)),
        }
    }
}

async fn poll_updates<P: PollingState + Sync>(
    producer: &P,
    sender: mpsc::Sender<DesktopDomainUpdate>,
    context: DesktopProducerContext,
    period: Duration,
) -> Result<(), ProducerError> {
    let mut previous = None;
    let mut interval = tokio::time::interval(period);
    interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    interval.tick().await;
    loop {
        tokio::select! {
            biased;
            _ = context.cancelled() => return Ok(()),
            _ = interval.tick() => {
                let observation = context.begin_observation();
                let Some(current) = producer.polling_snapshot(&context).await else {
                    continue;
                };
                if previous.as_ref() != Some(&current) {
                    let update = observation
                        .finish(current.clone())
                        .map_err(|error| ProducerError::new(error.to_string()))?;
                    sender.send(update)
                        .await
                        .map_err(|_| ProducerError::new("desktop state authority stopped"))?;
                    previous = Some(current);
                }
            }
        }
    }
}

fn daily_probe<B: DailyBackend>(
    domain: DesktopDomainId,
    backend: Arc<B>,
    location: Option<WeatherLocation>,
    control: &RunControl,
) -> io::Result<DesktopDomainValue> {
    let operation = daily_operation(domain, location)?;
    let value = backend.handle_controlled(operation, control)?;
    daily_value(domain, value)
}

fn daily_operation(
    domain: DesktopDomainId,
    location: Option<WeatherLocation>,
) -> io::Result<DailyOperation> {
    match domain {
        DesktopDomainId::Launcher => Ok(DailyOperation::LauncherSearch {
            query: String::new(),
        }),
        DesktopDomainId::Calendar => {
            let now = unix_time()?;
            Ok(DailyOperation::Calendar {
                window_start: format_utc(now)?,
                window_end: format_utc(now.saturating_add(30 * 24 * 60 * 60))?,
            })
        }
        DesktopDomainId::Weather => Ok(DailyOperation::Weather {
            location: location.ok_or_else(|| {
                io::Error::new(
                    io::ErrorKind::NotFound,
                    "weather location is not configured",
                )
            })?,
        }),
        _ => Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "daily operation has an unrelated domain",
        )),
    }
}

fn daily_value(domain: DesktopDomainId, value: Value) -> io::Result<DesktopDomainValue> {
    match domain {
        DesktopDomainId::Launcher => {
            let entries = value.as_array().ok_or_else(|| {
                io::Error::new(
                    io::ErrorKind::InvalidData,
                    "launcher result is not an array",
                )
            })?;
            let mut converted = Vec::with_capacity(entries.len());
            for entry in entries {
                let id = string_field(entry, "desktop_id")?;
                let name = string_field(entry, "name")?;
                let icon = entry
                    .get("icon")
                    .and_then(Value::as_str)
                    .filter(|value| !value.trim().is_empty())
                    .unwrap_or("application-x-executable")
                    .to_owned();
                converted.push(LauncherEntry { id, name, icon });
            }
            Ok(DesktopDomainValue::Launcher(converted))
        }
        DesktopDomainId::Calendar => serde_json::from_value::<CalendarSnapshot>(value)
            .map(DesktopDomainValue::Calendar)
            .map_err(|error| io::Error::new(io::ErrorKind::InvalidData, error)),
        DesktopDomainId::Weather => serde_json::from_value::<WeatherSnapshot>(value)
            .map(DesktopDomainValue::Weather)
            .map_err(|error| io::Error::new(io::ErrorKind::InvalidData, error)),
        _ => Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "daily value has an unrelated domain",
        )),
    }
}

fn string_field(value: &Value, snake: &str) -> io::Result<String> {
    let camel = match snake {
        "desktop_id" => "desktopId",
        other => other,
    };
    value
        .get(snake)
        .or_else(|| value.get(camel))
        .and_then(Value::as_str)
        .filter(|value| !value.trim().is_empty())
        .map(str::to_owned)
        .ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                "daily result omitted a string field",
            )
        })
}

fn osd_state(visible: Vec<sleepy_sdk::OsdEvent>) -> DesktopDomainState {
    DesktopDomainState::available(
        DesktopDomainId::Osd,
        DesktopDomainValue::Osd(DesktopOsdSnapshot {
            current: visible.first().cloned(),
            history: visible,
        }),
    )
    .expect("matching OSD domain")
}

fn terminal(
    domain: DesktopDomainId,
    status: CapabilityAvailability,
    error: impl std::fmt::Display,
) -> DesktopDomainState {
    DesktopDomainState::terminal(domain, status, error.to_string()).unwrap_or_else(|_| {
        DesktopDomainState::terminal(domain, status, "desktop provider failed")
            .expect("static diagnostic")
    })
}

fn compositor_availability(kind: CompositorErrorKind) -> CapabilityAvailability {
    match kind {
        CompositorErrorKind::Unavailable => CapabilityAvailability::Unavailable,
        CompositorErrorKind::UnsafeInstance => CapabilityAvailability::PermissionDenied,
        CompositorErrorKind::Timeout => CapabilityAvailability::Timeout,
        CompositorErrorKind::Parse
        | CompositorErrorKind::Inconsistent
        | CompositorErrorKind::Bounds => CapabilityAvailability::Parse,
        CompositorErrorKind::Unsupported => CapabilityAvailability::Unsupported,
        _ => CapabilityAvailability::Error,
    }
}

fn availability_for_io(error: &io::Error) -> CapabilityAvailability {
    match error.kind() {
        io::ErrorKind::NotFound | io::ErrorKind::NotConnected => {
            CapabilityAvailability::Unavailable
        }
        io::ErrorKind::PermissionDenied => CapabilityAvailability::PermissionDenied,
        io::ErrorKind::TimedOut => CapabilityAvailability::Timeout,
        io::ErrorKind::InvalidData => CapabilityAvailability::Parse,
        io::ErrorKind::Unsupported => CapabilityAvailability::Unsupported,
        _ => CapabilityAvailability::Error,
    }
}

fn weather_location_from_environment() -> Option<WeatherLocation> {
    let display_name = std::env::var("SLEEPY_WEATHER_LOCATION").ok()?;
    let latitude = std::env::var("SLEEPY_WEATHER_LATITUDE")
        .ok()?
        .parse()
        .ok()?;
    let longitude = std::env::var("SLEEPY_WEATHER_LONGITUDE")
        .ok()?
        .parse()
        .ok()?;
    Some(WeatherLocation {
        display_name,
        latitude,
        longitude,
    })
}

fn unix_time() -> io::Result<i64> {
    let value = unsafe { libc::time(std::ptr::null_mut()) };
    (value != -1)
        .then_some(value)
        .ok_or_else(io::Error::last_os_error)
}

fn format_utc(timestamp: i64) -> io::Result<String> {
    let mut value = std::mem::MaybeUninit::<libc::tm>::uninit();
    let result = unsafe { libc::gmtime_r(&timestamp, value.as_mut_ptr()) };
    if result.is_null() {
        return Err(io::Error::last_os_error());
    }
    let value = unsafe { value.assume_init() };
    Ok(format!(
        "{:04}-{:02}-{:02}T{:02}:{:02}:{:02}Z",
        value.tm_year + 1900,
        value.tm_mon + 1,
        value.tm_mday,
        value.tm_hour,
        value.tm_min,
        value.tm_sec
    ))
}
