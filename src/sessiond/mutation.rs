use std::{error::Error, fmt, future::Future, io, pin::Pin, sync::Arc, time::Duration};

use sleepy_sdk::{
    validate_mutation_request, DaemonCommand, EventCause, EventCauseKind, EventEnvelope,
    MutationFailure, MutationResult, MutationStatus, RuntimeSnapshot, SessionEvent,
    WIRE_SCHEMA_VERSION,
};
use tokio::sync::Mutex;

use super::{EventHub, GenerationAllocator};

pub trait MutationBackend: Send + Sync + 'static {
    fn execute<'a>(
        &'a self,
        command: &'a DaemonCommand,
    ) -> Pin<Box<dyn Future<Output = io::Result<()>> + Send + 'a>>;

    fn readback(&self) -> Pin<Box<dyn Future<Output = io::Result<RuntimeSnapshot>> + Send + '_>>;

    fn confirms(&self, command: &DaemonCommand, snapshot: &RuntimeSnapshot) -> bool;
}

struct PipelineState {
    allocator: GenerationAllocator,
    current_generation: u64,
}

pub struct MutationPipeline<B: MutationBackend> {
    state: Mutex<PipelineState>,
    hub: EventHub,
    backend: Arc<B>,
    operation_timeout: Duration,
}

#[derive(Debug)]
pub enum PipelineError {
    Contract(String),
    Io(io::Error),
}

impl<B: MutationBackend> MutationPipeline<B> {
    pub fn new(
        allocator: GenerationAllocator,
        current_generation: u64,
        hub: EventHub,
        backend: Arc<B>,
    ) -> Self {
        Self::with_timeout(
            allocator,
            current_generation,
            hub,
            backend,
            Duration::from_millis(1200),
        )
    }

    pub fn with_timeout(
        allocator: GenerationAllocator,
        current_generation: u64,
        hub: EventHub,
        backend: Arc<B>,
        operation_timeout: Duration,
    ) -> Self {
        Self {
            state: Mutex::new(PipelineState {
                allocator,
                current_generation,
            }),
            hub,
            backend,
            operation_timeout,
        }
    }

    pub async fn handle_json(&self, input: &str) -> Result<MutationResult, PipelineError> {
        let request = validate_mutation_request(input)
            .map_err(|error| PipelineError::Contract(error.to_string()))?;
        let mut state = self.state.lock().await;

        if request.expected_generation != state.current_generation {
            return Ok(rejected(
                &request.request_id,
                state.current_generation,
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
                    state.current_generation,
                    MutationStatus::Unknown,
                    "execute",
                    &error.to_string(),
                ));
            }
            Err(_) => {
                return Ok(unconfirmed(
                    &request.request_id,
                    state.current_generation,
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
                    state.current_generation,
                    MutationStatus::Unknown,
                    "readback",
                    &error.to_string(),
                ));
            }
            Err(_) => {
                return Ok(unconfirmed(
                    &request.request_id,
                    state.current_generation,
                    MutationStatus::Unknown,
                    "readbackTimeout",
                    "mutation readback exceeded its total deadline",
                ));
            }
        };
        if !self.backend.confirms(&request.command, &snapshot) {
            return Ok(unconfirmed(
                &request.request_id,
                state.current_generation,
                MutationStatus::Unknown,
                "readbackMismatch",
                "mutation readback did not confirm the requested state",
            ));
        }

        let current_generation = state.current_generation;
        let generation = state
            .allocator
            .next_after(current_generation)
            .map_err(PipelineError::Io)?;
        let event = EventEnvelope {
            schema_version: WIRE_SCHEMA_VERSION,
            generation,
            event_id: uuid::Uuid::new_v4().to_string(),
            emitted_at: utc_now()?,
            cause: EventCause {
                kind: EventCauseKind::Request,
                request_id: Some(request.request_id.clone()),
            },
            payload: SessionEvent::FullSnapshot(snapshot),
        };

        state.current_generation = generation;
        let _ = self.hub.publish(event.clone()).await;
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

fn utc_now() -> Result<String, PipelineError> {
    let now = unsafe { libc::time(std::ptr::null_mut()) };
    if now == -1 {
        return Err(PipelineError::Io(io::Error::last_os_error()));
    }
    let mut broken_down = std::mem::MaybeUninit::<libc::tm>::uninit();
    let result = unsafe { libc::gmtime_r(&now, broken_down.as_mut_ptr()) };
    if result.is_null() {
        return Err(PipelineError::Io(io::Error::last_os_error()));
    }
    let value = unsafe { broken_down.assume_init() };
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
