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
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};

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
