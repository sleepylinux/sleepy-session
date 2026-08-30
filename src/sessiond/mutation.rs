use std::{error::Error, fmt, future::Future, io, pin::Pin, sync::Arc, time::Duration};

use super::{EventHub, GenerationAuthority};
use crate::compositor::{
    CompositorError, CompositorErrorKind, CompositorExecution, HyprlandAdapter,
};
use crate::system::{mutation_command, CommandRunner, ProcessCommandRunner, SystemFacade};
use sleepy_sdk::{
    validate_mutation_request, CapabilityValue, DaemonCommand, EventCause, EventCauseKind,
    HyprlandCommand, MutationFailure, MutationResult, MutationStatus, RuntimeCapabilityId,
    RuntimeSnapshot, SessionEvent, SystemMutation, WIRE_SCHEMA_VERSION,
};
use tokio_util::sync::CancellationToken;

pub trait MutationBackend: Send + Sync + 'static {
    fn execute<'a>(
        &'a self,
        command: &'a DaemonCommand,
    ) -> Pin<Box<dyn Future<Output = io::Result<()>> + Send + 'a>>;

    fn readback(&self) -> Pin<Box<dyn Future<Output = io::Result<RuntimeSnapshot>> + Send + '_>>;

    fn confirms(&self, command: &DaemonCommand, snapshot: &RuntimeSnapshot) -> bool;
}

pub struct MutationPipeline<B: MutationBackend> {
    authority: GenerationAuthority,
    backend: Arc<B>,
    operation_timeout: Duration,
}

pub struct ProductionMutationBackend {
    runner: ProcessCommandRunner,
    facade: SystemFacade<ProcessCommandRunner>,
    hub: EventHub,
    pending: std::sync::Mutex<Option<SystemMutation>>,
    hyprland: Option<HyprlandAdapter>,
    hyprland_diagnostic: String,
}

impl ProductionMutationBackend {
    pub fn new(hub: EventHub) -> Self {
        let (hyprland, hyprland_diagnostic) =
            match HyprlandAdapter::discover(CancellationToken::new()) {
                Ok(adapter) => (Some(adapter), String::new()),
                Err(error) => (None, error.to_string()),
            };
        Self {
            runner: ProcessCommandRunner,
            facade: SystemFacade::new(ProcessCommandRunner),
            hub,
            pending: std::sync::Mutex::new(None),
            hyprland,
            hyprland_diagnostic,
        }
    }

    pub fn with_hyprland(hub: EventHub, adapter: HyprlandAdapter) -> Self {
        Self {
            runner: ProcessCommandRunner,
            facade: SystemFacade::new(ProcessCommandRunner),
            hub,
            pending: std::sync::Mutex::new(None),
            hyprland: Some(adapter),
            hyprland_diagnostic: String::new(),
        }
    }

    pub async fn execute_hyprland(
        &self,
        command: HyprlandCommand,
    ) -> Result<CompositorExecution, CompositorError> {
        let adapter = self.hyprland.as_ref().ok_or_else(|| {
            CompositorError::new(
                CompositorErrorKind::Unavailable,
                if self.hyprland_diagnostic.is_empty() {
                    "Hyprland compositor is unavailable"
                } else {
                    &self.hyprland_diagnostic
                },
            )
        })?;
        adapter.execute(command).await
    }
}

impl MutationBackend for ProductionMutationBackend {
    fn execute<'a>(
        &'a self,
        command: &'a DaemonCommand,
    ) -> Pin<Box<dyn Future<Output = io::Result<()>> + Send + 'a>> {
        let mutation = match command {
            DaemonCommand::SetCapability { mutation } => mutation.clone(),
            _ => {
                return Box::pin(async {
                    Err(io::Error::new(
                        io::ErrorKind::InvalidInput,
                        "control socket accepts only setCapability",
                    ))
                })
            }
        };
        Box::pin(async move {
            let spec = mutation_command(&mutation).map_err(io::Error::other)?;
            let runner = self.runner;
            let output = tokio::task::spawn_blocking(move || runner.run(&spec))
                .await
                .map_err(|error| io::Error::other(format!("mutation task failed: {error}")))?
                .map_err(io::Error::other)?;
            if output.status != 0 {
                return Err(io::Error::other(format!(
                    "mutation command exited {}",
                    output.status
                )));
            }
            *self
                .pending
                .lock()
                .map_err(|_| io::Error::other("mutation state poisoned"))? = Some(mutation);
            Ok(())
        })
    }

    fn readback(&self) -> Pin<Box<dyn Future<Output = io::Result<RuntimeSnapshot>> + Send + '_>> {
        Box::pin(async move {
            let mutation = self
                .pending
                .lock()
                .map_err(|_| io::Error::other("mutation state poisoned"))?
                .clone()
                .ok_or_else(|| io::Error::other("mutation readback has no command"))?;
            let id = runtime_capability(&mutation);
            let facade = self.facade.clone();
            let record = tokio::task::spawn_blocking(move || facade.runtime_capability(id))
                .await
                .map_err(|error| io::Error::other(format!("readback task failed: {error}")))?;
            let envelope = self.hub.latest_snapshot().await;
            let SessionEvent::FullSnapshot(mut snapshot) = envelope.payload else {
                return Err(io::Error::other("event hub replay is not a full snapshot"));
            };
            let current = snapshot
                .capabilities
                .iter_mut()
                .find(|current| current.id == id)
                .ok_or_else(|| io::Error::other("runtime snapshot omitted capability"))?;
            *current = record;
            Ok(snapshot)
        })
    }

    fn confirms(&self, command: &DaemonCommand, snapshot: &RuntimeSnapshot) -> bool {
        let DaemonCommand::SetCapability { mutation } = command else {
            return false;
        };
        let value = snapshot
            .capabilities
            .iter()
            .find(|item| item.id == runtime_capability(mutation))
            .and_then(|item| item.value.as_ref());
        runtime_confirms(mutation, value)
    }
}

fn runtime_capability(mutation: &SystemMutation) -> RuntimeCapabilityId {
    match mutation {
        SystemMutation::NetworkEnabled(_) => RuntimeCapabilityId::Network,
        SystemMutation::BluetoothEnabled(_) => RuntimeCapabilityId::Bluetooth,
        SystemMutation::AudioVolume(_)
        | SystemMutation::AudioMuted(_)
        | SystemMutation::AudioMicrophoneLevel(_)
        | SystemMutation::AudioMicrophoneMuted(_)
        | SystemMutation::AudioOutputDevice(_) => RuntimeCapabilityId::Audio,
        SystemMutation::DisplayBrightness(_) => RuntimeCapabilityId::Brightness,
        SystemMutation::DisplayNightLightEnabled(_) => RuntimeCapabilityId::NightLight,
        SystemMutation::PowerProfile(_) => RuntimeCapabilityId::PowerProfile,
        SystemMutation::MediaTransport(_) => RuntimeCapabilityId::Media,
    }
}

fn runtime_confirms(mutation: &SystemMutation, value: Option<&CapabilityValue>) -> bool {
    match (mutation, value) {
        (SystemMutation::NetworkEnabled(expected), Some(CapabilityValue::Network(value))) => {
            value.wifi_enabled == *expected
        }
        (SystemMutation::BluetoothEnabled(expected), Some(CapabilityValue::Bluetooth(value))) => {
            value.powered == *expected
        }
        (SystemMutation::AudioVolume(expected), Some(CapabilityValue::Audio(value))) => {
            (value.output_level - expected).abs() < 0.001
        }
        (SystemMutation::AudioMuted(expected), Some(CapabilityValue::Audio(value))) => {
            value.output_muted == *expected
        }
        (SystemMutation::AudioMicrophoneLevel(expected), Some(CapabilityValue::Audio(value))) => {
            (value.input_level - expected).abs() < 0.001
        }
        (SystemMutation::AudioMicrophoneMuted(expected), Some(CapabilityValue::Audio(value))) => {
            value.input_muted == *expected
        }
        (SystemMutation::AudioOutputDevice(expected), Some(CapabilityValue::Audio(value))) => {
            value.default_output_id.as_deref() == Some(expected)
        }
        (SystemMutation::DisplayBrightness(expected), Some(CapabilityValue::Brightness(value))) => {
            (value.level - expected).abs() < 0.001
        }
        (
            SystemMutation::DisplayNightLightEnabled(expected),
            Some(CapabilityValue::NightLight(value)),
        ) => value.enabled == *expected,
        (SystemMutation::PowerProfile(expected), Some(CapabilityValue::PowerProfile(value))) => {
            value.active
                == match expected {
                    sleepy_sdk::PowerProfile::PowerSaver => "power-saver",
                    sleepy_sdk::PowerProfile::Balanced => "balanced",
                    sleepy_sdk::PowerProfile::Performance => "performance",
                }
        }
        (SystemMutation::MediaTransport(_), Some(CapabilityValue::Media(_))) => true,
        _ => false,
    }
}

#[derive(Debug)]
pub enum PipelineError {
    Contract(String),
    Io(io::Error),
}

impl<B: MutationBackend> MutationPipeline<B> {
    pub fn new(authority: GenerationAuthority, backend: Arc<B>) -> Self {
        Self::with_timeout(authority, backend, Duration::from_millis(1200))
    }

    pub fn with_timeout(
        authority: GenerationAuthority,
        backend: Arc<B>,
        operation_timeout: Duration,
    ) -> Self {
        Self {
            authority,
            backend,
            operation_timeout,
        }
    }

    pub async fn handle_json(&self, input: &str) -> Result<MutationResult, PipelineError> {
        let request = validate_mutation_request(input)
            .map_err(|error| PipelineError::Contract(error.to_string()))?;
        let mut authority = self.authority.lock().await;

        if request.expected_generation != authority.current_generation() {
            return Ok(rejected(
                &request.request_id,
                authority.current_generation(),
                "staleGeneration",
                "expectedGeneration does not match the daemon generation",
            ));
        }

        let deadline = tokio::time::Instant::now() + self.operation_timeout;
        match tokio::time::timeout_at(deadline, self.backend.execute(&request.command)).await {
            Ok(Ok(())) => {}
            Ok(Err(error)) => {
                return Ok(unconfirmed(
                    &request.request_id,
                    authority.current_generation(),
                    MutationStatus::Unknown,
                    "execute",
                    &error.to_string(),
                ));
            }
            Err(_) => {
                return Ok(unconfirmed(
                    &request.request_id,
                    authority.current_generation(),
                    MutationStatus::Unknown,
                    "executeTimeout",
                    "mutation execution exceeded its total deadline",
                ));
            }
        }
        let snapshot = match tokio::time::timeout_at(deadline, self.backend.readback()).await {
            Ok(Ok(snapshot)) => snapshot,
            Ok(Err(error)) => {
                return Ok(unconfirmed(
                    &request.request_id,
                    authority.current_generation(),
                    MutationStatus::Unknown,
                    "readback",
                    &error.to_string(),
                ));
            }
            Err(_) => {
                return Ok(unconfirmed(
                    &request.request_id,
                    authority.current_generation(),
                    MutationStatus::Unknown,
                    "readbackTimeout",
                    "mutation readback exceeded its total deadline",
                ));
            }
        };
        if !self.backend.confirms(&request.command, &snapshot) {
            return Ok(unconfirmed(
                &request.request_id,
                authority.current_generation(),
                MutationStatus::Unknown,
                "readbackMismatch",
                "mutation readback did not confirm the requested state",
            ));
        }

        let event = match authority
            .publish(
                EventCause {
                    kind: EventCauseKind::Request,
                    request_id: Some(request.request_id.clone()),
                },
                SessionEvent::FullSnapshot(snapshot),
            )
            .await
        {
            Ok(event) => event,
            Err(error) => {
                return Ok(unconfirmed(
                    &request.request_id,
                    authority.current_generation(),
                    MutationStatus::Unknown,
                    "publication",
                    &error.to_string(),
                ));
            }
        };
        let generation = event.generation;
        Ok(MutationResult {
            schema_version: WIRE_SCHEMA_VERSION,
            request_id: request.request_id,
            generation,
            status: MutationStatus::Confirmed,
            confirmed_event: Some(event),
            error: None,
        })
    }
}

fn rejected(request_id: &str, generation: u64, code: &str, message: &str) -> MutationResult {
    unconfirmed(
        request_id,
        generation,
        MutationStatus::Rejected,
        code,
        message,
    )
}

fn unconfirmed(
    request_id: &str,
    generation: u64,
    status: MutationStatus,
    code: &str,
    message: &str,
) -> MutationResult {
    MutationResult {
        schema_version: WIRE_SCHEMA_VERSION,
        request_id: request_id.into(),
        generation,
        status,
        confirmed_event: None,
        error: Some(MutationFailure {
            code: code.into(),
            message: if message.trim().is_empty() {
                "backend mutation failed".into()
            } else {
                message.into()
            },
        }),
    }
}

impl fmt::Display for PipelineError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Contract(message) => write!(formatter, "invalid mutation request: {message}"),
            Self::Io(error) => write!(formatter, "mutation pipeline I/O error: {error}"),
        }
    }
}

impl Error for PipelineError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Contract(_) => None,
            Self::Io(error) => Some(error),
        }
    }
}
