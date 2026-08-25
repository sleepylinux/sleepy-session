// SPDX-License-Identifier: GPL-3.0-only

use sleepy_sdk::{
    NotificationAction, NotificationActionState, NotificationDocument, NotificationUrgency,
    WIRE_SCHEMA_VERSION,
};
use sleepy_session::{
    notifications::{
        FreedesktopNotificationProvider, NotificationEventService, NotificationSocket,
        NotificationStore, NotifyRequest,
    },
    sessiond::{full_snapshot_event, EventHub, GenerationAllocator, GenerationAuthority},
};
use std::{os::unix::fs::PermissionsExt, sync::Arc};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};

#[tokio::test]
async fn notification_socket_returns_real_history_and_mutates_shared_dnd_state() {
    let temp = tempfile::tempdir().unwrap();
    std::fs::set_permissions(temp.path(), std::fs::Permissions::from_mode(0o700)).unwrap();
    let store = NotificationStore::open(temp.path().join("notifications"), 500).unwrap();
    let provider = FreedesktopNotificationProvider::new(store).unwrap();
    let mut allocator = GenerationAllocator::open(temp.path().join("generation"), 16).unwrap();
    let generation = allocator.next_generation().unwrap();
    let authority = GenerationAuthority::new(
        allocator,
        generation,
        EventHub::new(full_snapshot_event(generation).unwrap(), 16),
    );
    let service = Arc::new(tokio::sync::Mutex::new(NotificationEventService::new(
        provider, authority,
    )));
    let path = temp.path().join("notification.sock");
    let socket = Arc::new(
        NotificationSocket::bind(&path, unsafe { libc::geteuid() }, Arc::clone(&service))
            .await
            .unwrap(),
    );
    let serving = Arc::clone(&socket);
    let task = tokio::spawn(async move { serving.serve_n(3).await });

    let snapshot = request(&path, r#"{"schemaVersion":2,"requestId":"018f3f4c-8af1-7f6b-bf42-1bd472868e65","operation":{"type":"snapshot"}}"#).await;
    assert_eq!(snapshot["status"], "confirmed");
    assert_eq!(snapshot["data"]["type"], "snapshot");
    assert_eq!(snapshot["data"]["data"]["active"], serde_json::json!([]));
    assert_eq!(snapshot["data"]["data"]["unreadCount"], 0);
    assert_eq!(snapshot["data"]["data"]["dnd"], false);

    let changed = request(&path, r#"{"schemaVersion":2,"requestId":"018f3f4c-8af1-7f6b-bf42-1bd472868e66","operation":{"type":"setDnd","data":{"enabled":true}}}"#).await;
    assert_eq!(changed["data"]["data"]["dnd"], true);
    assert!(service.lock().await.provider().store().dnd());

    let id = service
        .lock()
        .await
        .notify(NotifyRequest {
            origin: ":1.88".into(),
            notification: NotificationDocument {
                schema_version: WIRE_SCHEMA_VERSION,
                id: 0,
                application_id: "org.example.App".into(),
                summary: "Literal <b>text</b>".into(),
                body: "<script>inert</script>".into(),
                urgency: NotificationUrgency::Normal,
                created_at: "2026-08-24T21:00:00Z".into(),
                timeout_ms: None,
                read: false,
                archived: false,
                actions: vec![NotificationAction {
                    id: "open".into(),
                    label: "Open".into(),
                    state: NotificationActionState::Available,
                }],
            },
        })
        .await
        .unwrap()
        .id;
    service.lock().await.origin_lost(":1.88").await.unwrap();
    let expired = request(&path, &format!(r#"{{"schemaVersion":2,"requestId":"018f3f4c-8af1-7f6b-bf42-1bd472868e67","operation":{{"type":"invokeAction","data":{{"id":{id},"actionId":"open"}}}}}}"#)).await;
    assert_eq!(expired["status"], "error");
    assert_eq!(expired["error"]["code"], "expired");
    assert!(expired.get("data").is_none());
    task.await.unwrap().unwrap();
}

async fn request(path: &std::path::Path, line: &str) -> serde_json::Value {
    let stream = tokio::net::UnixStream::connect(path).await.unwrap();
    let (read, mut write) = tokio::io::split(stream);
    write
        .write_all(format!("{line}\n").as_bytes())
        .await
        .unwrap();
    let mut response = String::new();
    BufReader::new(read).read_line(&mut response).await.unwrap();
    serde_json::from_str(&response).unwrap()
}
