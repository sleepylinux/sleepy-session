// SPDX-License-Identifier: GPL-3.0-only

use std::{future::Future, io, os::unix::fs::PermissionsExt, pin::Pin, sync::Arc};

use sleepy_sdk::{
    CapabilityAvailability, CapabilityFailure, CapabilityRecord, DaemonCommand,
    RuntimeCapabilityId, RuntimeSnapshot,
};
use sleepy_session::sessiond::{
    full_snapshot_event, ControlSocket, EventHub, GenerationAllocator, GenerationAuthority,
    MutationBackend, MutationPipeline,
};
use tempfile::tempdir;
use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader};

struct Backend;
impl MutationBackend for Backend {
    fn execute<'a>(
        &'a self,
        _: &'a DaemonCommand,
    ) -> Pin<Box<dyn Future<Output = io::Result<()>> + Send + 'a>> {
        Box::pin(async { Ok(()) })
    }
    fn readback(&self) -> Pin<Box<dyn Future<Output = io::Result<RuntimeSnapshot>> + Send + '_>> {
        Box::pin(async { Ok(snapshot()) })
    }
    fn confirms(&self, _: &DaemonCommand, _: &RuntimeSnapshot) -> bool {
        true
    }
}

struct HugeBackend;
impl MutationBackend for HugeBackend {
    fn execute<'a>(
        &'a self,
        _: &'a DaemonCommand,
    ) -> Pin<Box<dyn Future<Output = io::Result<()>> + Send + 'a>> {
        Box::pin(async { Ok(()) })
    }
    fn readback(&self) -> Pin<Box<dyn Future<Output = io::Result<RuntimeSnapshot>> + Send + '_>> {
        Box::pin(async {
            let mut value = snapshot();
            for capability in &mut value.capabilities {
                capability.diagnostic = Some(CapabilityFailure {
                    // Ten diagnostics keep the serialized response below the
                    // one-MiB frame ceiling while exceeding the Unix socket's
                    // bounded kernel buffer when the peer does not read.
                    message: "x".repeat(92 * 1024),
                });
            }
            Ok(value)
        })
    }
    fn confirms(&self, _: &DaemonCommand, _: &RuntimeSnapshot) -> bool {
        true
    }
}

fn snapshot() -> RuntimeSnapshot {
    RuntimeSnapshot {
        capabilities: [
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
        ]
        .into_iter()
        .map(|id| CapabilityRecord {
            id,
            status: CapabilityAvailability::Unsupported,
            value: None,
            diagnostic: Some(CapabilityFailure {
                message: "fixture".into(),
            }),
        })
        .collect(),
        focused_output_id: Some("DP-1".into()),
    }
}

#[tokio::test]
async fn control_socket_publishes_confirmed_event_before_reply_and_rejects_stale_peer_request() {
    let temp = tempdir().unwrap();
    std::fs::set_permissions(temp.path(), std::fs::Permissions::from_mode(0o700)).unwrap();
    let generation_path = temp.path().join("generation");
    let mut allocator = GenerationAllocator::open(&generation_path, 16).unwrap();
    let initial = allocator.next_generation().unwrap();
    let hub = EventHub::new(full_snapshot_event(initial).unwrap(), 16);
    let authority = GenerationAuthority::new(allocator, initial, hub.clone());
    let pipeline = Arc::new(MutationPipeline::new(authority, Arc::new(Backend)));
    let path = temp.path().join("control.sock");
    let socket = Arc::new(
        ControlSocket::bind(&path, unsafe { libc::geteuid() }, pipeline)
            .await
            .unwrap(),
    );
    let serving = Arc::clone(&socket);
    let task = tokio::spawn(async move { serving.serve_one().await });
    let stream = tokio::net::UnixStream::connect(&path).await.unwrap();
    let (read, mut write) = tokio::io::split(stream);
    write.write_all(format!(
        "{{\"schemaVersion\":2,\"requestId\":\"018f3f4c-8af1-7f6b-bf42-1bd472868e65\",\"expectedGeneration\":{initial},\"command\":{{\"type\":\"setDnd\",\"data\":{{\"enabled\":true}}}}}}\n"
    ).as_bytes()).await.unwrap();
    let mut line = String::new();
    BufReader::new(read).read_line(&mut line).await.unwrap();
    let reply: serde_json::Value = serde_json::from_str(&line).unwrap();
    assert_eq!(reply["status"], "confirmed");
    assert_eq!(reply["generation"], reply["confirmedEvent"]["generation"]);
    assert_eq!(
        hub.latest_snapshot().await.generation,
        reply["generation"].as_u64().unwrap()
    );
    task.await.unwrap().unwrap();
}

#[tokio::test]
async fn control_socket_rejects_unknown_fields_and_versions_without_executing() {
    let temp = tempdir().unwrap();
    std::fs::set_permissions(temp.path(), std::fs::Permissions::from_mode(0o700)).unwrap();
    let mut allocator = GenerationAllocator::open(temp.path().join("generation"), 16).unwrap();
    let initial = allocator.next_generation().unwrap();
    let hub = EventHub::new(full_snapshot_event(initial).unwrap(), 16);
    let authority = GenerationAuthority::new(allocator, initial, hub);
    let pipeline = Arc::new(MutationPipeline::new(authority, Arc::new(Backend)));
    let path = temp.path().join("control.sock");
    let socket = Arc::new(
        ControlSocket::bind(&path, unsafe { libc::geteuid() }, pipeline)
            .await
            .unwrap(),
    );
    let serving = Arc::clone(&socket);
    let task = tokio::spawn(async move { serving.serve_one().await });
    let mut stream = tokio::net::UnixStream::connect(&path).await.unwrap();
    stream.write_all(b"{\"schemaVersion\":3,\"requestId\":\"018f3f4c-8af1-7f6b-bf42-1bd472868e65\",\"expectedGeneration\":1,\"command\":{\"type\":\"setDnd\",\"data\":{\"enabled\":true}},\"extra\":true}\n").await.unwrap();
    stream.shutdown().await.unwrap();
    assert!(task.await.unwrap().is_err());
}

#[tokio::test]
async fn control_socket_rejects_oversize_unterminated_input_without_waiting_for_eof() {
    let temp = tempdir().unwrap();
    std::fs::set_permissions(temp.path(), std::fs::Permissions::from_mode(0o700)).unwrap();
    let mut allocator = GenerationAllocator::open(temp.path().join("generation"), 16).unwrap();
    let initial = allocator.next_generation().unwrap();
    let authority = GenerationAuthority::new(
        allocator,
        initial,
        EventHub::new(full_snapshot_event(initial).unwrap(), 16),
    );
    let socket = Arc::new(
        ControlSocket::bind(
            temp.path().join("control.sock"),
            unsafe { libc::geteuid() },
            Arc::new(MutationPipeline::new(authority, Arc::new(Backend))),
        )
        .await
        .unwrap(),
    );
    let serving = Arc::clone(&socket);
    let task = tokio::spawn(async move { serving.serve().await });
    let mut client = tokio::net::UnixStream::connect(socket.path())
        .await
        .unwrap();
    client.write_all(&vec![b'x'; 256 * 1024 + 1]).await.unwrap();
    let mut closed = [0_u8; 1];
    let result = tokio::time::timeout(
        std::time::Duration::from_millis(250),
        client.read(&mut closed),
    )
    .await
    .expect("bounded reader must reject before peer EOF")
    .unwrap();
    assert_eq!(result, 0);
    assert_eq!(
        socket
            .shutdown_and_drain(std::time::Duration::from_millis(250))
            .await
            .unwrap(),
        1
    );
    task.await.unwrap().unwrap();
}

#[tokio::test]
async fn control_socket_reaps_completed_handlers_and_caps_concurrent_clients() {
    let temp = tempdir().unwrap();
    std::fs::set_permissions(temp.path(), std::fs::Permissions::from_mode(0o700)).unwrap();
    let mut allocator = GenerationAllocator::open(temp.path().join("generation"), 16).unwrap();
    let initial = allocator.next_generation().unwrap();
    let authority = GenerationAuthority::new(
        allocator,
        initial,
        EventHub::new(full_snapshot_event(initial).unwrap(), 16),
    );
    let socket = Arc::new(
        ControlSocket::bind(
            temp.path().join("control.sock"),
            unsafe { libc::geteuid() },
            Arc::new(MutationPipeline::new(authority, Arc::new(Backend))),
        )
        .await
        .unwrap(),
    );
    let serving = Arc::clone(&socket);
    let task = tokio::spawn(async move { serving.serve().await });

    for _ in 0..64 {
        let mut client = tokio::net::UnixStream::connect(socket.path())
            .await
            .unwrap();
        client.write_all(b"{}\n").await.unwrap();
        let mut response = Vec::new();
        client.read_to_end(&mut response).await.unwrap();
    }
    assert!(
        socket
            .shutdown_and_drain(std::time::Duration::from_millis(500))
            .await
            .unwrap()
            <= 1,
        "completed handlers must be reaped during steady-state serving"
    );
    task.await.unwrap().unwrap();

    let mut allocator = GenerationAllocator::open(temp.path().join("generation-2"), 16).unwrap();
    let initial = allocator.next_generation().unwrap();
    let authority = GenerationAuthority::new(
        allocator,
        initial,
        EventHub::new(full_snapshot_event(initial).unwrap(), 16),
    );
    let socket = Arc::new(
        ControlSocket::bind(
            temp.path().join("control-2.sock"),
            unsafe { libc::geteuid() },
            Arc::new(MutationPipeline::new(authority, Arc::new(Backend))),
        )
        .await
        .unwrap(),
    );
    let serving = Arc::clone(&socket);
    let task = tokio::spawn(async move { serving.serve().await });
    let mut clients = Vec::new();
    for _ in 0..16 {
        clients.push(
            tokio::net::UnixStream::connect(socket.path())
                .await
                .unwrap(),
        );
    }
    let mut rejected = tokio::net::UnixStream::connect(socket.path())
        .await
        .unwrap();
    tokio::time::sleep(std::time::Duration::from_millis(20)).await;
    let mut closed = [0_u8; 1];
    assert_eq!(
        tokio::time::timeout(
            std::time::Duration::from_millis(250),
            rejected.read(&mut closed)
        )
        .await
        .expect("the seventeenth idle client must be rejected")
        .unwrap(),
        0
    );
    drop(clients);
    socket
        .shutdown_and_drain(std::time::Duration::from_millis(500))
        .await
        .unwrap();
    task.await.unwrap().unwrap();
}

#[tokio::test]
async fn control_socket_times_out_a_non_reading_response_peer() {
    let temp = tempdir().unwrap();
    std::fs::set_permissions(temp.path(), std::fs::Permissions::from_mode(0o700)).unwrap();
    let mut allocator = GenerationAllocator::open(temp.path().join("generation"), 16).unwrap();
    let initial = allocator.next_generation().unwrap();
    let authority = GenerationAuthority::new(
        allocator,
        initial,
        EventHub::new(full_snapshot_event(initial).unwrap(), 16),
    );
    let socket = Arc::new(
        ControlSocket::bind(
            temp.path().join("control.sock"),
            unsafe { libc::geteuid() },
            Arc::new(MutationPipeline::new(authority, Arc::new(HugeBackend))),
        )
        .await
        .unwrap(),
    );
    let serving = Arc::clone(&socket);
    let task = tokio::spawn(async move { serving.serve_one().await });
    let mut client = tokio::net::UnixStream::connect(socket.path())
        .await
        .unwrap();
    client.write_all(format!(
        "{{\"schemaVersion\":2,\"requestId\":\"018f3f4c-8af1-7f6b-bf42-1bd472868e65\",\"expectedGeneration\":{initial},\"command\":{{\"type\":\"setDnd\",\"data\":{{\"enabled\":true}}}}}}\n"
    ).as_bytes()).await.unwrap();
    let result = tokio::time::timeout(std::time::Duration::from_millis(1500), task)
        .await
        .expect("response write must have a deadline")
        .unwrap();
    assert_eq!(result.unwrap_err().kind(), io::ErrorKind::TimedOut);
}
