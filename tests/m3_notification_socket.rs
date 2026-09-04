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
    sessiond::{
        full_snapshot_event,
        supervisor::{DaemonLifecycle, DaemonNotification, DaemonNotifier, StartupBarrier},
        EventHub, GenerationAllocator, GenerationAuthority,
    },
};
use std::{io, os::unix::fs::PermissionsExt, sync::Arc, time::Duration};
use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader};

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

#[tokio::test]
async fn notification_socket_cannot_lose_shutdown_immediately_after_startup_release() {
    struct Notifier;
    impl DaemonNotifier for Notifier {
        fn notify(&self, _state: DaemonNotification) -> io::Result<()> {
            Ok(())
        }
    }

    let temp = tempfile::tempdir().unwrap();
    std::fs::set_permissions(temp.path(), std::fs::Permissions::from_mode(0o700)).unwrap();
    let store = NotificationStore::open(temp.path().join("notifications"), 500).unwrap();
    let provider = FreedesktopNotificationProvider::new(store).unwrap();
    let allocator = GenerationAllocator::open(temp.path().join("generation"), 16).unwrap();
    let authority = GenerationAuthority::new(
        allocator,
        0,
        EventHub::new(full_snapshot_event(0).unwrap(), 16),
    );
    let service = Arc::new(tokio::sync::Mutex::new(NotificationEventService::new(
        provider, authority,
    )));
    let path = temp.path().join("notification.sock");
    let socket = Arc::new(
        NotificationSocket::bind(&path, unsafe { libc::geteuid() }, service)
            .await
            .unwrap(),
    );
    let mut startup = StartupBarrier::new();
    let worker = startup.required_task("notification");
    let serving = Arc::clone(&socket);
    let task = tokio::spawn(async move { serving.serve_with_startup(worker).await });
    let lifecycle = DaemonLifecycle::new(Arc::new(Notifier));

    lifecycle
        .complete_startup(&[socket.path()], &mut startup, || async {
            socket
                .shutdown_and_drain(Duration::from_millis(100))
                .await
                .map(|_| ())
        })
        .await
        .unwrap();
    tokio::time::timeout(Duration::from_millis(100), task)
        .await
        .expect("notification accept worker must observe immediate shutdown")
        .unwrap()
        .unwrap();
    drop(socket);
    assert!(!path.exists());
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

#[tokio::test]
async fn notification_socket_rejects_oversize_unterminated_input_and_drains_connection() {
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
    let socket = Arc::new(
        NotificationSocket::bind(
            temp.path().join("notification.sock"),
            unsafe { libc::geteuid() },
            service,
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
async fn notification_socket_reaps_completed_handlers_and_caps_concurrent_clients() {
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
    let socket = Arc::new(
        NotificationSocket::bind(
            temp.path().join("notification.sock"),
            unsafe { libc::geteuid() },
            service,
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
        "completed notification handlers must be reaped during serving"
    );
    task.await.unwrap().unwrap();

    let store = NotificationStore::open(temp.path().join("notifications-2"), 500).unwrap();
    let provider = FreedesktopNotificationProvider::new(store).unwrap();
    let mut allocator = GenerationAllocator::open(temp.path().join("generation-2"), 16).unwrap();
    let generation = allocator.next_generation().unwrap();
    let authority = GenerationAuthority::new(
        allocator,
        generation,
        EventHub::new(full_snapshot_event(generation).unwrap(), 16),
    );
    let service = Arc::new(tokio::sync::Mutex::new(NotificationEventService::new(
        provider, authority,
    )));
    let socket = Arc::new(
        NotificationSocket::bind(
            temp.path().join("notification-2.sock"),
            unsafe { libc::geteuid() },
            service,
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
        .expect("the seventeenth notification client must be rejected")
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
async fn notification_socket_times_out_a_non_reading_response_peer() {
    let temp = tempfile::tempdir().unwrap();
    std::fs::set_permissions(temp.path(), std::fs::Permissions::from_mode(0o700)).unwrap();
    let store = NotificationStore::open(temp.path().join("notifications"), 500).unwrap();
    let provider = FreedesktopNotificationProvider::new(store).unwrap();
    let mut allocator = GenerationAllocator::open(temp.path().join("generation"), 32).unwrap();
    let generation = allocator.next_generation().unwrap();
    let authority = GenerationAuthority::new(
        allocator,
        generation,
        EventHub::new(full_snapshot_event(generation).unwrap(), 32),
    );
    let service = Arc::new(tokio::sync::Mutex::new(NotificationEventService::new(
        provider, authority,
    )));
    for index in 0..16 {
        service
            .lock()
            .await
            .notify(NotifyRequest {
                origin: format!(":1.{index}"),
                notification: NotificationDocument {
                    schema_version: WIRE_SCHEMA_VERSION,
                    id: 0,
                    application_id: "org.example.Large".into(),
                    summary: format!("Large {index}"),
                    body: "x".repeat(48 * 1024),
                    urgency: NotificationUrgency::Normal,
                    created_at: "2026-08-24T21:00:00Z".into(),
                    timeout_ms: None,
                    read: false,
                    archived: false,
                    actions: Vec::new(),
                },
            })
            .await
            .unwrap();
    }
    let socket = Arc::new(
        NotificationSocket::bind(
            temp.path().join("notification.sock"),
            unsafe { libc::geteuid() },
            service,
        )
        .await
        .unwrap(),
    );
    let serving = Arc::clone(&socket);
    let task = tokio::spawn(async move { serving.serve_one().await });
    let mut client = tokio::net::UnixStream::connect(socket.path())
        .await
        .unwrap();
    client.write_all(b"{\"schemaVersion\":2,\"requestId\":\"018f3f4c-8af1-7f6b-bf42-1bd472868e65\",\"operation\":{\"type\":\"snapshot\"}}\n").await.unwrap();
    let result = tokio::time::timeout(std::time::Duration::from_millis(1500), task)
        .await
        .expect("notification response write must have a deadline")
        .unwrap();
    assert_eq!(result.unwrap_err().kind(), std::io::ErrorKind::TimedOut);
}
