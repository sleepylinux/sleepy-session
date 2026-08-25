use std::{
    fs,
    future::Future,
    os::unix::fs::PermissionsExt,
    pin::Pin,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc, Mutex,
    },
};

use sleepy_sdk::{
    BrightnessRuntimeState, CapabilityAvailability, CapabilityFailure, CapabilityRecord,
    CapabilityValue, Connectivity, DaemonCommand, EventCause, EventCauseKind, EventEnvelope,
    LifecycleEvent, LifecycleState, MutationStatus, NetworkRuntimeState, RuntimeCapabilityId,
    RuntimeSnapshot, SessionEvent, WIRE_SCHEMA_VERSION,
};
use sleepy_session::sessiond::{
    AdapterActor, AdapterFailure, CapabilityAdapter, EventHub, GenerationAllocator,
    GenerationAuthority, LifecycleReconciler, MutationBackend, MutationPipeline, SessionSocket,
    ShutdownCoordinator,
};

fn lifecycle(generation: u64, state: LifecycleState) -> EventEnvelope {
    EventEnvelope {
        schema_version: WIRE_SCHEMA_VERSION,
        generation,
        event_id: format!("018f3f4c-8af1-7f6b-bf42-1bd472868e6{generation}"),
        emitted_at: "2026-08-24T21:00:00Z".into(),
        cause: EventCause {
            kind: EventCauseKind::Lifecycle,
            request_id: None,
        },
        payload: SessionEvent::Lifecycle(LifecycleEvent { state }),
    }
}

fn snapshot_event(generation: u64, snapshot: RuntimeSnapshot) -> EventEnvelope {
    EventEnvelope {
        schema_version: WIRE_SCHEMA_VERSION,
        generation,
        event_id: format!("018f3f4c-8af1-7f6b-bf42-1bd472868e6{generation}"),
        emitted_at: "2026-08-24T21:00:00Z".into(),
        cause: EventCause {
            kind: EventCauseKind::Replay,
            request_id: None,
        },
        payload: SessionEvent::FullSnapshot(snapshot),
    }
}

fn authority(
    allocator: GenerationAllocator,
    generation: u64,
    hub: EventHub,
) -> GenerationAuthority {
    GenerationAuthority::new(allocator, generation, hub)
}

#[test]
fn generation_allocator_reserves_blocks_and_never_reuses_after_restart() {
    let temp = tempfile::tempdir().unwrap();
    let path = temp.path().join("generation");

    let mut first = GenerationAllocator::open(&path, 4).unwrap();
    assert_eq!(first.next_generation().unwrap(), 1);
    assert_eq!(first.next_generation().unwrap(), 2);
    drop(first);

    let mut restarted = GenerationAllocator::open(&path, 4).unwrap();
    assert_eq!(restarted.next_generation().unwrap(), 5);
    assert_eq!(fs::read_to_string(path).unwrap(), "9\n");
}

#[test]
fn generation_allocator_rejects_symlinked_state_without_touching_target() {
    use std::os::unix::fs::symlink;

    let temp = tempfile::tempdir().unwrap();
    let target = temp.path().join("target");
    fs::write(&target, b"1\n").unwrap();
    let path = temp.path().join("generation");
    symlink(&target, &path).unwrap();

    let error = GenerationAllocator::open(&path, 4)
        .err()
        .expect("generation state symlinks must fail closed");

    assert!(
        error.raw_os_error() == Some(libc::ELOOP)
            || error.kind() == std::io::ErrorKind::PermissionDenied
    );
    assert_eq!(fs::read(&target).unwrap(), b"1\n");
    assert!(fs::symlink_metadata(&path)
        .unwrap()
        .file_type()
        .is_symlink());
}

#[test]
fn generation_path_validation_precedes_all_redirected_directory_mutation() {
    use std::os::unix::fs::symlink;

    let temp = tempfile::tempdir().unwrap();
    let redirected = temp.path().join("redirected");
    fs::create_dir(&redirected).unwrap();
    let linked = temp.path().join("linked");
    symlink(&redirected, &linked).unwrap();

    let error = GenerationAllocator::open(linked.join("sleepy/generation"), 4)
        .err()
        .expect("an intermediate symlink must fail before path mutation");

    assert_eq!(error.kind(), std::io::ErrorKind::PermissionDenied);
    assert!(!redirected.join("sleepy").exists());
}

#[test]
fn generation_allocator_rejects_overpermissive_existing_state() {
    let temp = tempfile::tempdir().unwrap();
    let path = temp.path().join("generation");
    fs::write(&path, b"5\n").unwrap();
    fs::set_permissions(&path, fs::Permissions::from_mode(0o644)).unwrap();

    let error = GenerationAllocator::open(&path, 4)
        .err()
        .expect("overpermissive generation state must fail closed");

    assert_eq!(error.kind(), std::io::ErrorKind::PermissionDenied);
    assert_eq!(fs::read(&path).unwrap(), b"5\n");
}

#[test]
fn generation_allocator_retains_its_open_directory_across_path_swap() {
    let temp = tempfile::tempdir().unwrap();
    let state = temp.path().join("state");
    fs::create_dir(&state).unwrap();
    let path = state.join("generation");
    let mut allocator = GenerationAllocator::open(&path, 2).unwrap();
    assert_eq!(allocator.next_generation().unwrap(), 1);
    assert_eq!(allocator.next_generation().unwrap(), 2);

    let retained = temp.path().join("state-retained");
    fs::rename(&state, &retained).unwrap();
    fs::create_dir(&state).unwrap();
    let attacker_state = state.join("generation");
    fs::write(&attacker_state, b"1000\n").unwrap();
    fs::set_permissions(&attacker_state, fs::Permissions::from_mode(0o600)).unwrap();

    assert_eq!(allocator.next_generation().unwrap(), 3);
    assert_eq!(fs::read(retained.join("generation")).unwrap(), b"5\n");
    assert_eq!(fs::read(attacker_state).unwrap(), b"1000\n");
}

#[tokio::test]
async fn event_hub_replays_latest_snapshot_before_live_events() {
    let initial = lifecycle(1, LifecycleState::Ready);
    let hub = EventHub::new(initial.clone(), 8);
    let mut subscriber = hub.subscribe().await;

    assert_eq!(subscriber.recv().await.unwrap(), initial);

    let live = lifecycle(2, LifecycleState::Reconciled);
    hub.publish(live.clone()).await.unwrap();
    assert_eq!(subscriber.recv().await.unwrap(), live);
}

#[tokio::test]
async fn reconnect_replay_folds_prior_capability_updates_into_a_full_snapshot() {
    let initial = sleepy_session::sessiond::initial_snapshot();
    let hub = EventHub::new(snapshot_event(1, initial), 8);
    let degraded = CapabilityRecord {
        id: RuntimeCapabilityId::Network,
        status: CapabilityAvailability::Timeout,
        value: None,
        diagnostic: Some(CapabilityFailure {
            message: "network adapter deadline expired".into(),
        }),
    };
    hub.publish(EventEnvelope {
        schema_version: WIRE_SCHEMA_VERSION,
        generation: 2,
        event_id: "018f3f4c-8af1-7f6b-bf42-1bd472868e62".into(),
        emitted_at: "2026-08-24T21:00:01Z".into(),
        cause: EventCause {
            kind: EventCauseKind::External,
            request_id: None,
        },
        payload: SessionEvent::CapabilityUpdate(degraded.clone()),
    })
    .await
    .unwrap();

    let replay = hub.subscribe().await.recv().await.unwrap();
    assert_eq!(replay.generation, 2);
    let SessionEvent::FullSnapshot(snapshot) = replay.payload else {
        panic!("reconnect must start with a full snapshot");
    };
    assert_eq!(
        snapshot
            .capabilities
            .iter()
            .find(|capability| capability.id == RuntimeCapabilityId::Network),
        Some(&degraded)
    );
}

#[tokio::test]
async fn event_hub_never_rewinds_its_replay_snapshot_to_a_stale_generation() {
    let initial = sleepy_session::sessiond::initial_snapshot();
    let hub = EventHub::new(snapshot_event(2, initial.clone()), 8);
    let error = hub
        .publish(EventEnvelope {
            schema_version: WIRE_SCHEMA_VERSION,
            generation: 1,
            event_id: "018f3f4c-8af1-7f6b-bf42-1bd472868e61".into(),
            emitted_at: "2026-08-24T21:00:01Z".into(),
            cause: EventCause {
                kind: EventCauseKind::External,
                request_id: None,
            },
            payload: SessionEvent::CapabilityUpdate(CapabilityRecord {
                id: RuntimeCapabilityId::Network,
                status: CapabilityAvailability::Timeout,
                value: None,
                diagnostic: Some(CapabilityFailure {
                    message: "stale adapter result".into(),
                }),
            }),
        })
        .await
        .expect_err("the hub must explicitly reject a stale generation");
    assert!(error.to_string().contains("does not advance"));

    let replay = hub.subscribe().await.recv().await.unwrap();
    assert_eq!(replay.generation, 2);
    assert_eq!(replay.payload, SessionEvent::FullSnapshot(initial));
}

#[tokio::test]
async fn reconnect_snapshot_is_marked_as_replay_not_as_the_original_request() {
    let initial = sleepy_session::sessiond::initial_snapshot();
    let hub = EventHub::new(snapshot_event(1, initial.clone()), 8);
    hub.publish(EventEnvelope {
        schema_version: WIRE_SCHEMA_VERSION,
        generation: 2,
        event_id: "018f3f4c-8af1-7f6b-bf42-1bd472868e62".into(),
        emitted_at: "2026-08-24T21:00:01Z".into(),
        cause: EventCause {
            kind: EventCauseKind::Request,
            request_id: Some("018f3f4c-8af1-7f6b-bf42-1bd472868e65".into()),
        },
        payload: SessionEvent::FullSnapshot(initial),
    })
    .await
    .unwrap();

    let replay = hub.subscribe().await.recv().await.unwrap();
    assert_eq!(replay.cause.kind, EventCauseKind::Replay);
    assert!(replay.cause.request_id.is_none());
}

#[tokio::test]
async fn session_socket_is_private_and_starts_each_client_with_replay() {
    use tokio::io::AsyncBufReadExt;

    let temp = tempfile::tempdir().unwrap();
    let socket_path = temp.path().join("sleepy/session.sock");
    let initial = lifecycle(1, LifecycleState::Ready);
    let hub = EventHub::new(initial.clone(), 8);
    let socket = SessionSocket::bind(&socket_path, unsafe { libc::geteuid() }, hub)
        .await
        .unwrap();

    assert_eq!(
        fs::metadata(&socket_path).unwrap().permissions().mode() & 0o777,
        0o600
    );

    let server = tokio::spawn(async move { socket.serve_one().await });
    let stream = tokio::net::UnixStream::connect(&socket_path).await.unwrap();
    let mut lines = tokio::io::BufReader::new(stream).lines();
    let replay: EventEnvelope =
        serde_json::from_str(&lines.next_line().await.unwrap().unwrap()).unwrap();
    assert_eq!(replay, initial);
    server.abort();
}

#[tokio::test]
async fn session_socket_rejects_an_owner_uid_other_than_the_daemon_uid() {
    let temp = tempfile::tempdir().unwrap();
    let socket_path = temp.path().join("sleepy/session.sock");
    let hub = EventHub::new(lifecycle(1, LifecycleState::Ready), 8);
    let claimed_uid = unsafe { libc::geteuid() }.saturating_add(1);

    let error = SessionSocket::bind(&socket_path, claimed_uid, hub)
        .await
        .err()
        .expect("a daemon must not create a socket for a different owner UID");

    assert_eq!(error.kind(), std::io::ErrorKind::PermissionDenied);
    assert!(!socket_path.exists());
}

#[tokio::test]
async fn session_socket_shutdown_removes_the_owned_socket_path() {
    let temp = tempfile::tempdir().unwrap();
    let socket_path = temp.path().join("sleepy/session.sock");
    let hub = EventHub::new(lifecycle(1, LifecycleState::Ready), 8);
    let socket = SessionSocket::bind(&socket_path, unsafe { libc::geteuid() }, hub)
        .await
        .unwrap();
    assert!(socket_path.exists());

    drop(socket);

    assert!(!socket_path.exists());
}

#[tokio::test]
async fn session_socket_shutdown_waits_for_queued_client_output_and_connection_tasks() {
    use tokio::io::AsyncBufReadExt;

    let temp = tempfile::tempdir().unwrap();
    let socket_path = temp.path().join("sleepy/session.sock");
    let hub = EventHub::new(lifecycle(1, LifecycleState::Ready), 8);
    let socket = Arc::new(
        SessionSocket::bind(&socket_path, unsafe { libc::geteuid() }, hub.clone())
            .await
            .unwrap(),
    );
    let server_socket = Arc::clone(&socket);
    let server = tokio::spawn(async move { server_socket.serve().await });
    let stream = tokio::net::UnixStream::connect(&socket_path).await.unwrap();
    let mut lines = tokio::io::BufReader::new(stream).lines();
    lines.next_line().await.unwrap().unwrap();
    let queued = lifecycle(2, LifecycleState::Stopping);
    hub.publish(queued.clone()).await.unwrap();

    let report = socket
        .shutdown_and_drain(std::time::Duration::from_millis(200))
        .await
        .unwrap();
    let delivered: EventEnvelope =
        serde_json::from_str(&lines.next_line().await.unwrap().unwrap()).unwrap();

    assert_eq!(delivered, queued);
    assert_eq!(lines.next_line().await.unwrap(), None);
    assert_eq!(report.completed, 1);
    assert_eq!(report.aborted, 0);
    assert!(server.await.unwrap().is_ok());
}

#[tokio::test]
async fn session_socket_rejects_a_symlinked_parent_directory() {
    use std::os::unix::fs::symlink;

    let temp = tempfile::tempdir().unwrap();
    let redirected = temp.path().join("redirected");
    fs::create_dir(&redirected).unwrap();
    let linked = temp.path().join("sleepy");
    symlink(&redirected, &linked).unwrap();
    let socket_path = linked.join("session.sock");
    let hub = EventHub::new(lifecycle(1, LifecycleState::Ready), 8);

    let error = SessionSocket::bind(&socket_path, unsafe { libc::geteuid() }, hub)
        .await
        .err()
        .expect("socket parent symlinks must be rejected");

    assert_eq!(error.kind(), std::io::ErrorKind::PermissionDenied);
    assert!(!redirected.join("session.sock").exists());
}

#[tokio::test]
async fn socket_path_validation_precedes_all_redirected_directory_mutation() {
    use std::os::unix::fs::symlink;

    let temp = tempfile::tempdir().unwrap();
    let redirected = temp.path().join("redirected");
    fs::create_dir(&redirected).unwrap();
    let linked = temp.path().join("linked");
    symlink(&redirected, &linked).unwrap();
    let hub = EventHub::new(lifecycle(1, LifecycleState::Ready), 8);

    let error = SessionSocket::bind(
        linked.join("sleepy/session.sock"),
        unsafe { libc::geteuid() },
        hub,
    )
    .await
    .err()
    .expect("an intermediate symlink must fail before path mutation");

    assert_eq!(error.kind(), std::io::ErrorKind::PermissionDenied);
    assert!(!redirected.join("sleepy").exists());
}

#[tokio::test]
async fn second_daemon_refuses_to_unlink_a_live_private_socket() {
    let temp = tempfile::tempdir().unwrap();
    let socket_path = temp.path().join("sleepy/session.sock");
    let hub = EventHub::new(lifecycle(1, LifecycleState::Ready), 8);
    let first = SessionSocket::bind(&socket_path, unsafe { libc::geteuid() }, hub.clone())
        .await
        .unwrap();

    let error = SessionSocket::bind(&socket_path, unsafe { libc::geteuid() }, hub)
        .await
        .err()
        .expect("a live daemon endpoint must not be replaced");

    assert_eq!(error.kind(), std::io::ErrorKind::AlreadyExists);
    let client = tokio::net::UnixStream::connect(&socket_path).await.unwrap();
    drop(client);
    drop(first);
}

#[tokio::test]
async fn daemon_replaces_only_a_stale_owned_private_socket() {
    let temp = tempfile::tempdir().unwrap();
    let parent = temp.path().join("sleepy");
    fs::create_dir(&parent).unwrap();
    let socket_path = parent.join("session.sock");
    let stale = std::os::unix::net::UnixListener::bind(&socket_path).unwrap();
    fs::set_permissions(&socket_path, fs::Permissions::from_mode(0o600)).unwrap();
    drop(stale);
    let hub = EventHub::new(lifecycle(1, LifecycleState::Ready), 8);

    let socket = SessionSocket::bind(&socket_path, unsafe { libc::geteuid() }, hub)
        .await
        .unwrap();

    assert_eq!(
        fs::metadata(&socket_path).unwrap().permissions().mode() & 0o777,
        0o600
    );
    drop(socket);
}

#[test]
fn initial_snapshot_degrades_each_capability_independently() {
    let snapshot = sleepy_session::sessiond::initial_snapshot();
    assert_eq!(snapshot.capabilities.len(), 10);
    assert!(snapshot
        .capabilities
        .iter()
        .all(|capability| capability.status == CapabilityAvailability::Unsupported));
}

struct FakeMutationBackend {
    calls: Mutex<Vec<&'static str>>,
    confirms_readback: bool,
    fail_execute: bool,
    fail_readback: bool,
}

impl Default for FakeMutationBackend {
    fn default() -> Self {
        Self {
            calls: Mutex::new(Vec::new()),
            confirms_readback: true,
            fail_execute: false,
            fail_readback: false,
        }
    }
}

impl MutationBackend for FakeMutationBackend {
    fn execute<'a>(
        &'a self,
        _command: &'a DaemonCommand,
    ) -> Pin<Box<dyn Future<Output = std::io::Result<()>> + Send + 'a>> {
        Box::pin(async move {
            self.calls.lock().unwrap().push("execute");
            if self.fail_execute {
                return Err(std::io::Error::other(
                    "backend could not prove whether execution committed",
                ));
            }
            Ok(())
        })
    }

    fn readback(
        &self,
    ) -> Pin<Box<dyn Future<Output = std::io::Result<RuntimeSnapshot>> + Send + '_>> {
        Box::pin(async move {
            self.calls.lock().unwrap().push("readback");
            if self.fail_readback {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    "bounded readback could not be parsed",
                ));
            }
            Ok(sleepy_session::sessiond::initial_snapshot())
        })
    }

    fn confirms(&self, _command: &DaemonCommand, _snapshot: &RuntimeSnapshot) -> bool {
        self.confirms_readback
    }
}

#[tokio::test]
async fn mutation_pipeline_serializes_execute_readback_event_and_reply_generation() {
    let temp = tempfile::tempdir().unwrap();
    let generation_path = temp.path().join("generation");
    fs::write(&generation_path, "5\n").unwrap();
    fs::set_permissions(&generation_path, fs::Permissions::from_mode(0o600)).unwrap();
    let allocator = GenerationAllocator::open(&generation_path, 4).unwrap();
    let initial = lifecycle(4, LifecycleState::Ready);
    let hub = EventHub::new(initial, 8);
    let mut subscriber = hub.subscribe().await;
    subscriber.recv().await.unwrap();
    let backend = Arc::new(FakeMutationBackend::default());
    let pipeline = MutationPipeline::new(authority(allocator, 4, hub), backend.clone());

    let request = serde_json::json!({
        "schemaVersion": 2,
        "requestId": "018f3f4c-8af1-7f6b-bf42-1bd472868e65",
        "expectedGeneration": 4,
        "command": { "type": "setDnd", "data": { "enabled": true } }
    });
    let result = pipeline.handle_json(&request.to_string()).await.unwrap();
    let event = subscriber.recv().await.unwrap();

    assert_eq!(result.status, MutationStatus::Confirmed);
    assert_eq!(result.generation, 5);
    assert_eq!(result.confirmed_event.as_ref(), Some(&event));
    assert_eq!(event.generation, result.generation);
    assert_eq!(
        backend.calls.lock().unwrap().as_slice(),
        &["execute", "readback"]
    );
}

#[tokio::test]
async fn stale_mutation_is_rejected_without_backend_execution() {
    let temp = tempfile::tempdir().unwrap();
    let allocator = GenerationAllocator::open(temp.path().join("generation"), 4).unwrap();
    let hub = EventHub::new(lifecycle(8, LifecycleState::Ready), 8);
    let backend = Arc::new(FakeMutationBackend::default());
    let pipeline = MutationPipeline::new(authority(allocator, 8, hub), backend.clone());
    let request = serde_json::json!({
        "schemaVersion": 2,
        "requestId": "018f3f4c-8af1-7f6b-bf42-1bd472868e65",
        "expectedGeneration": 7,
        "command": { "type": "setDnd", "data": { "enabled": true } }
    });

    let result = pipeline.handle_json(&request.to_string()).await.unwrap();
    assert_eq!(result.status, MutationStatus::Rejected);
    assert_eq!(result.generation, 8);
    assert!(backend.calls.lock().unwrap().is_empty());
}

#[tokio::test]
async fn mutation_readback_disagreement_reports_unknown_without_committing_generation() {
    let temp = tempfile::tempdir().unwrap();
    let allocator = GenerationAllocator::open(temp.path().join("generation"), 4).unwrap();
    let hub = EventHub::new(
        snapshot_event(8, sleepy_session::sessiond::initial_snapshot()),
        8,
    );
    let mut subscriber = hub.subscribe().await;
    subscriber.recv().await.unwrap();
    let backend = Arc::new(FakeMutationBackend {
        calls: Mutex::new(Vec::new()),
        confirms_readback: false,
        fail_execute: false,
        fail_readback: false,
    });
    let pipeline = MutationPipeline::new(authority(allocator, 8, hub), backend.clone());
    let request = serde_json::json!({
        "schemaVersion": 2,
        "requestId": "018f3f4c-8af1-7f6b-bf42-1bd472868e65",
        "expectedGeneration": 8,
        "command": { "type": "setDnd", "data": { "enabled": true } }
    });

    let result = pipeline.handle_json(&request.to_string()).await.unwrap();

    assert_eq!(result.status, MutationStatus::Unknown);
    assert_eq!(result.generation, 8);
    assert!(result.confirmed_event.is_none());
    assert!(
        tokio::time::timeout(std::time::Duration::from_millis(20), subscriber.recv())
            .await
            .is_err()
    );
    assert_eq!(
        backend.calls.lock().unwrap().as_slice(),
        &["execute", "readback"]
    );
}

#[tokio::test]
async fn mutation_with_unprovable_readback_reports_unknown_never_rejected() {
    let temp = tempfile::tempdir().unwrap();
    let allocator = GenerationAllocator::open(temp.path().join("generation"), 4).unwrap();
    let hub = EventHub::new(
        snapshot_event(8, sleepy_session::sessiond::initial_snapshot()),
        8,
    );
    let backend = Arc::new(FakeMutationBackend {
        calls: Mutex::new(Vec::new()),
        confirms_readback: true,
        fail_execute: false,
        fail_readback: true,
    });
    let pipeline = MutationPipeline::new(authority(allocator, 8, hub), backend);
    let request = serde_json::json!({
        "schemaVersion": 2,
        "requestId": "018f3f4c-8af1-7f6b-bf42-1bd472868e65",
        "expectedGeneration": 8,
        "command": { "type": "setDnd", "data": { "enabled": true } }
    });

    let result = pipeline.handle_json(&request.to_string()).await.unwrap();

    assert_eq!(result.status, MutationStatus::Unknown);
    assert_eq!(result.generation, 8);
    assert_eq!(result.error.unwrap().code, "readback");
}

#[tokio::test]
async fn mutation_execution_error_reports_unknown_when_commit_is_unprovable() {
    let temp = tempfile::tempdir().unwrap();
    let allocator = GenerationAllocator::open(temp.path().join("generation"), 4).unwrap();
    let hub = EventHub::new(
        snapshot_event(8, sleepy_session::sessiond::initial_snapshot()),
        8,
    );
    let backend = Arc::new(FakeMutationBackend {
        calls: Mutex::new(Vec::new()),
        confirms_readback: true,
        fail_execute: true,
        fail_readback: false,
    });
    let pipeline = MutationPipeline::new(authority(allocator, 8, hub), backend);
    let request = serde_json::json!({
        "schemaVersion": 2,
        "requestId": "018f3f4c-8af1-7f6b-bf42-1bd472868e65",
        "expectedGeneration": 8,
        "command": { "type": "setDnd", "data": { "enabled": true } }
    });

    let result = pipeline.handle_json(&request.to_string()).await.unwrap();

    assert_eq!(result.status, MutationStatus::Unknown);
    assert_eq!(result.generation, 8);
    assert_eq!(result.error.unwrap().code, "execute");
}

struct HangingReadbackBackend;

impl MutationBackend for HangingReadbackBackend {
    fn execute<'a>(
        &'a self,
        _command: &'a DaemonCommand,
    ) -> Pin<Box<dyn Future<Output = std::io::Result<()>> + Send + 'a>> {
        Box::pin(async { Ok(()) })
    }

    fn readback(
        &self,
    ) -> Pin<Box<dyn Future<Output = std::io::Result<RuntimeSnapshot>> + Send + '_>> {
        Box::pin(std::future::pending())
    }

    fn confirms(&self, _command: &DaemonCommand, _snapshot: &RuntimeSnapshot) -> bool {
        true
    }
}

#[tokio::test]
async fn mutation_readback_has_a_total_deadline_and_reports_unknown_on_timeout() {
    let temp = tempfile::tempdir().unwrap();
    let allocator = GenerationAllocator::open(temp.path().join("generation"), 4).unwrap();
    let hub = EventHub::new(
        snapshot_event(8, sleepy_session::sessiond::initial_snapshot()),
        8,
    );
    let pipeline = MutationPipeline::with_timeout(
        authority(allocator, 8, hub),
        Arc::new(HangingReadbackBackend),
        std::time::Duration::from_millis(20),
    );
    let request = serde_json::json!({
        "schemaVersion": 2,
        "requestId": "018f3f4c-8af1-7f6b-bf42-1bd472868e65",
        "expectedGeneration": 8,
        "command": { "type": "setDnd", "data": { "enabled": true } }
    });

    let result = tokio::time::timeout(
        std::time::Duration::from_millis(250),
        pipeline.handle_json(&request.to_string()),
    )
    .await
    .expect("the pipeline must bound its own readback")
    .unwrap();

    assert_eq!(result.status, MutationStatus::Unknown);
    assert_eq!(result.generation, 8);
    assert_eq!(result.error.unwrap().code, "readbackTimeout");
}

struct HealthyNetworkAdapter;

impl CapabilityAdapter for HealthyNetworkAdapter {
    fn id(&self) -> RuntimeCapabilityId {
        RuntimeCapabilityId::Network
    }

    fn observe(
        &self,
    ) -> Pin<Box<dyn Future<Output = Result<CapabilityRecord, AdapterFailure>> + Send + '_>> {
        Box::pin(async {
            Ok(CapabilityRecord {
                id: RuntimeCapabilityId::Network,
                status: CapabilityAvailability::Available,
                value: Some(CapabilityValue::Network(NetworkRuntimeState {
                    wifi_enabled: true,
                    ethernet_connected: false,
                    connectivity: Connectivity::Full,
                    active_connection_id: None,
                })),
                diagnostic: None,
            })
        })
    }
}

struct HangingBluetoothAdapter;

impl CapabilityAdapter for HangingBluetoothAdapter {
    fn id(&self) -> RuntimeCapabilityId {
        RuntimeCapabilityId::Bluetooth
    }

    fn observe(
        &self,
    ) -> Pin<Box<dyn Future<Output = Result<CapabilityRecord, AdapterFailure>> + Send + '_>> {
        Box::pin(std::future::pending())
    }
}

struct MistypedNetworkAdapter;

impl CapabilityAdapter for MistypedNetworkAdapter {
    fn id(&self) -> RuntimeCapabilityId {
        RuntimeCapabilityId::Network
    }

    fn observe(
        &self,
    ) -> Pin<Box<dyn Future<Output = Result<CapabilityRecord, AdapterFailure>> + Send + '_>> {
        Box::pin(async {
            Ok(CapabilityRecord {
                id: RuntimeCapabilityId::Network,
                status: CapabilityAvailability::Available,
                value: Some(CapabilityValue::Brightness(BrightnessRuntimeState {
                    level: 0.5,
                })),
                diagnostic: None,
            })
        })
    }
}

struct RestartingNetworkAdapter {
    calls: AtomicUsize,
}

struct ConcurrentNetworkAdapter {
    active: AtomicUsize,
    maximum_active: AtomicUsize,
}

impl CapabilityAdapter for ConcurrentNetworkAdapter {
    fn id(&self) -> RuntimeCapabilityId {
        RuntimeCapabilityId::Network
    }

    fn observe(
        &self,
    ) -> Pin<Box<dyn Future<Output = Result<CapabilityRecord, AdapterFailure>> + Send + '_>> {
        Box::pin(async move {
            let active = self.active.fetch_add(1, Ordering::SeqCst) + 1;
            self.maximum_active.fetch_max(active, Ordering::SeqCst);
            tokio::time::sleep(std::time::Duration::from_millis(15)).await;
            self.active.fetch_sub(1, Ordering::SeqCst);
            Ok(CapabilityRecord {
                id: RuntimeCapabilityId::Network,
                status: CapabilityAvailability::Available,
                value: Some(CapabilityValue::Network(NetworkRuntimeState {
                    wifi_enabled: true,
                    ethernet_connected: false,
                    connectivity: Connectivity::Full,
                    active_connection_id: None,
                })),
                diagnostic: None,
            })
        })
    }
}

impl CapabilityAdapter for RestartingNetworkAdapter {
    fn id(&self) -> RuntimeCapabilityId {
        RuntimeCapabilityId::Network
    }

    fn observe(
        &self,
    ) -> Pin<Box<dyn Future<Output = Result<CapabilityRecord, AdapterFailure>> + Send + '_>> {
        Box::pin(async move {
            if self.calls.fetch_add(1, Ordering::SeqCst) == 0 {
                return Err(AdapterFailure::new(
                    CapabilityAvailability::Unavailable,
                    "network service disappeared",
                ));
            }
            Ok(CapabilityRecord {
                id: RuntimeCapabilityId::Network,
                status: CapabilityAvailability::Available,
                value: Some(CapabilityValue::Network(NetworkRuntimeState {
                    wifi_enabled: true,
                    ethernet_connected: false,
                    connectivity: Connectivity::Full,
                    active_connection_id: None,
                })),
                diagnostic: None,
            })
        })
    }
}

#[tokio::test]
async fn adapter_deadlines_degrade_only_the_failed_capability() {
    let healthy = AdapterActor::new(
        Arc::new(HealthyNetworkAdapter),
        std::time::Duration::from_millis(20),
        std::time::Duration::from_millis(5),
    );
    let hanging = AdapterActor::new(
        Arc::new(HangingBluetoothAdapter),
        std::time::Duration::from_millis(20),
        std::time::Duration::from_millis(5),
    );

    let (network, bluetooth) = tokio::join!(healthy.observe_once(), hanging.observe_once());

    assert_eq!(network.status, CapabilityAvailability::Available);
    assert_eq!(network.id, RuntimeCapabilityId::Network);
    assert_eq!(bluetooth.status, CapabilityAvailability::Timeout);
    assert_eq!(bluetooth.id, RuntimeCapabilityId::Bluetooth);
    assert!(bluetooth.value.is_none());
    assert!(bluetooth.diagnostic.unwrap().message.contains("deadline"));
}

#[tokio::test]
async fn adapter_actor_rejects_a_value_that_does_not_match_its_capability_id() {
    let actor = AdapterActor::new(
        Arc::new(MistypedNetworkAdapter),
        std::time::Duration::from_millis(20),
        std::time::Duration::from_millis(5),
    );

    let record = actor.observe_once().await;

    assert_eq!(record.id, RuntimeCapabilityId::Network);
    assert_eq!(record.status, CapabilityAvailability::Parse);
    assert!(record.value.is_none());
}

#[tokio::test]
async fn failed_adapter_restart_waits_for_its_backoff_deadline() {
    let restart_delay = std::time::Duration::from_millis(25);
    let actor = AdapterActor::new(
        Arc::new(RestartingNetworkAdapter {
            calls: AtomicUsize::new(0),
        }),
        std::time::Duration::from_millis(20),
        restart_delay,
    );
    assert_eq!(
        actor.observe_once().await.status,
        CapabilityAvailability::Unavailable
    );

    let started = tokio::time::Instant::now();
    assert_eq!(
        actor.observe_once().await.status,
        CapabilityAvailability::Available
    );

    assert!(started.elapsed() >= restart_delay);
}

#[tokio::test]
async fn each_adapter_actor_serializes_its_own_observations() {
    let adapter = Arc::new(ConcurrentNetworkAdapter {
        active: AtomicUsize::new(0),
        maximum_active: AtomicUsize::new(0),
    });
    let actor = AdapterActor::new(
        Arc::clone(&adapter),
        std::time::Duration::from_millis(50),
        std::time::Duration::from_millis(5),
    );

    let (first, second) = tokio::join!(actor.observe_once(), actor.observe_once());

    assert_eq!(first.status, CapabilityAvailability::Available);
    assert_eq!(second.status, CapabilityAvailability::Available);
    assert_eq!(adapter.maximum_active.load(Ordering::SeqCst), 1);
}

struct SuccessfulReconciler;

impl LifecycleReconciler for SuccessfulReconciler {
    fn reconcile(&self) -> Pin<Box<dyn Future<Output = std::io::Result<()>> + Send + '_>> {
        Box::pin(async { Ok(()) })
    }
}

struct HangingReconciler;

impl LifecycleReconciler for HangingReconciler {
    fn reconcile(&self) -> Pin<Box<dyn Future<Output = std::io::Result<()>> + Send + '_>> {
        Box::pin(std::future::pending())
    }
}

#[tokio::test]
async fn shutdown_reconciliation_is_bounded_and_publishes_ordered_lifecycle_events() {
    let temp = tempfile::tempdir().unwrap();
    let allocator = GenerationAllocator::open(temp.path().join("generation"), 4).unwrap();
    let hub = EventHub::new(
        snapshot_event(4, sleepy_session::sessiond::initial_snapshot()),
        8,
    );
    let mut subscriber = hub.subscribe().await;
    subscriber.recv().await.unwrap();
    let coordinator = ShutdownCoordinator::new(
        authority(allocator, 4, hub),
        std::time::Duration::from_millis(20),
    );
    let reconcilers: Vec<Arc<dyn LifecycleReconciler>> =
        vec![Arc::new(SuccessfulReconciler), Arc::new(HangingReconciler)];

    let started = tokio::time::Instant::now();
    let report = coordinator.reconcile(&reconcilers).await.unwrap();
    let stopping = subscriber.recv().await.unwrap();
    let reconciled = subscriber.recv().await.unwrap();

    assert!(started.elapsed() < std::time::Duration::from_millis(200));
    assert_eq!(report.succeeded, 1);
    assert_eq!(report.timed_out, 1);
    assert_eq!(report.failed, 0);
    assert_eq!(stopping.generation, 5);
    assert_eq!(reconciled.generation, 6);
    assert!(matches!(
        stopping.payload,
        SessionEvent::Lifecycle(LifecycleEvent {
            state: LifecycleState::Stopping
        })
    ));
    assert!(matches!(
        reconciled.payload,
        SessionEvent::Lifecycle(LifecycleEvent {
            state: LifecycleState::Reconciled
        })
    ));
}

#[tokio::test]
async fn mutation_never_confirms_when_an_independent_producer_advanced_the_hub() {
    let temp = tempfile::tempdir().unwrap();
    let hub = EventHub::new(
        snapshot_event(4, sleepy_session::sessiond::initial_snapshot()),
        8,
    );
    let lifecycle = ShutdownCoordinator::new(
        authority(
            GenerationAllocator::open(temp.path().join("lifecycle-generation"), 4).unwrap(),
            4,
            hub.clone(),
        ),
        std::time::Duration::from_millis(20),
    );
    lifecycle.reconcile(&[]).await.unwrap();
    let backend = Arc::new(FakeMutationBackend::default());
    let pipeline = MutationPipeline::new(
        authority(
            GenerationAllocator::open(temp.path().join("mutation-generation"), 4).unwrap(),
            4,
            hub.clone(),
        ),
        backend,
    );
    let request = serde_json::json!({
        "schemaVersion": 2,
        "requestId": "018f3f4c-8af1-7f6b-bf42-1bd472868e65",
        "expectedGeneration": 4,
        "command": { "type": "setDnd", "data": { "enabled": true } }
    });

    let result = pipeline.handle_json(&request.to_string()).await.unwrap();
    let replay = hub.subscribe().await.recv().await.unwrap();

    assert_eq!(result.status, MutationStatus::Unknown);
    assert!(result.confirmed_event.is_none());
    assert_eq!(replay.generation, 6);
}
