mod adapter;
mod authority;
mod generation;
mod hub;
mod lifecycle;
mod mutation;
mod socket;

pub use authority::GenerationAuthority;
pub use generation::GenerationAllocator;
pub use hub::{EventHub, EventSubscriber, PublishError};
pub use lifecycle::{LifecycleReconciler, ReconciliationReport, ShutdownCoordinator};
pub use mutation::{MutationBackend, MutationPipeline, PipelineError};
pub use socket::{SessionSocket, SessionSocketBindObserver, SocketDrainReport};

use sleepy_sdk::{
    CapabilityAvailability, CapabilityFailure, CapabilityRecord, EventCause, EventCauseKind,
    EventEnvelope, RuntimeCapabilityId, RuntimeSnapshot, SessionEvent, WIRE_SCHEMA_VERSION,
};

pub fn initial_snapshot() -> RuntimeSnapshot {
    const IDS: [RuntimeCapabilityId; 10] = [
        RuntimeCapabilityId::Network,
        RuntimeCapabilityId::Bluetooth,
        RuntimeCapabilityId::Audio,
        RuntimeCapabilityId::Battery,
        RuntimeCapabilityId::Brightness,
        RuntimeCapabilityId::PowerProfile,
        RuntimeCapabilityId::Media,
        RuntimeCapabilityId::NightLight,
        RuntimeCapabilityId::Niri,
        RuntimeCapabilityId::Resources,
    ];

    RuntimeSnapshot {
        capabilities: IDS
            .into_iter()
            .map(|id| CapabilityRecord {
                id,
                status: CapabilityAvailability::Unsupported,
                value: None,
                diagnostic: Some(CapabilityFailure {
                    message: "capability has not reported yet".into(),
                }),
            })
            .collect(),
        focused_output_id: None,
    }
}

pub fn full_snapshot_event(generation: u64) -> std::io::Result<EventEnvelope> {
    Ok(EventEnvelope {
        schema_version: WIRE_SCHEMA_VERSION,
        generation,
        event_id: uuid::Uuid::new_v4().to_string(),
        emitted_at: utc_now()?,
        cause: EventCause {
            kind: EventCauseKind::Lifecycle,
            request_id: None,
        },
        payload: SessionEvent::FullSnapshot(initial_snapshot()),
    })
}

fn utc_now() -> std::io::Result<String> {
    let now = unsafe { libc::time(std::ptr::null_mut()) };
    if now == -1 {
        return Err(std::io::Error::last_os_error());
    }
    let mut broken_down = std::mem::MaybeUninit::<libc::tm>::uninit();
    let result = unsafe { libc::gmtime_r(&now, broken_down.as_mut_ptr()) };
    if result.is_null() {
        return Err(std::io::Error::last_os_error());
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
pub use adapter::{AdapterActor, AdapterFailure, CapabilityAdapter};
