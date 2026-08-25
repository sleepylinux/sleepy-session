// SPDX-License-Identifier: GPL-3.0-only

use std::{os::unix::fs::PermissionsExt, sync::Arc, time::Duration};

use sleepy_session::{
    sessiond::{full_snapshot_event, EventHub, GenerationAllocator, GenerationAuthority},
    theme::ThemeManager,
    theme_socket::{ThemeMessage, ThemeSocket, ThemeStatus},
};
use tempfile::TempDir;
use tokio::{
    io::{AsyncBufReadExt, AsyncWriteExt, BufReader},
    net::UnixStream,
};

fn authority(temp: &TempDir) -> GenerationAuthority {
    GenerationAuthority::new(
        GenerationAllocator::open(temp.path().join("generation"), 16).unwrap(),
        1,
        EventHub::new(full_snapshot_event(1).unwrap(), 16),
    )
}

async fn line(reader: &mut BufReader<tokio::net::unix::OwnedReadHalf>) -> ThemeMessage {
    let mut value = String::new();
    reader.read_line(&mut value).await.unwrap();
    serde_json::from_str(&value).unwrap()
}

async fn send(write: &mut tokio::net::unix::OwnedWriteHalf, value: String) {
    write.write_all(value.as_bytes()).await.unwrap();
    write.write_all(b"\n").await.unwrap();
}

async fn start(
    temp: &TempDir,
    manager: ThemeManager,
) -> (
    Arc<ThemeSocket>,
    tokio::task::JoinHandle<std::io::Result<()>>,
) {
    let socket = Arc::new(
        ThemeSocket::bind(
            temp.path().join("runtime/sleepy/theme.sock"),
            unsafe { libc::geteuid() },
            manager,
            authority(temp),
        )
        .await
        .unwrap(),
    );
    let serving = Arc::clone(&socket);
    let task = tokio::spawn(async move { serving.serve().await });
    (socket, task)
}

#[tokio::test]
async fn apply_sends_candidate_waits_for_typed_ack_and_returns_confirmed_generation() {
    let temp = TempDir::new().unwrap();
    let manager =
        ThemeManager::open(temp.path().join("config"), temp.path().join("state")).unwrap();
    let (socket, task) = start(&temp, manager).await;
    let stream = UnixStream::connect(socket.path()).await.unwrap();
    let (read, mut write) = stream.into_split();
    let mut read = BufReader::new(read);
    let request_id = "d78951f8-c6f5-4f7d-8599-d72ed0b34803";
    send(
        &mut write,
        format!(r#"{{"schemaVersion":2,"requestId":"{request_id}","operation":{{"type":"apply","data":{{"themeId":"builtin.sleepy-light","expectedGeneration":1}}}}}}"#),
    )
    .await;
    match line(&mut read).await {
        ThemeMessage::Candidate {
            request_id: candidate_request,
            theme,
            ..
        } => {
            assert_eq!(candidate_request, request_id);
            assert_eq!(theme.id, "builtin.sleepy-light");
        }
        other => panic!("expected candidate, got {other:?}"),
    }
    send(
        &mut write,
        format!(r#"{{"schemaVersion":2,"requestId":"{request_id}","accepted":true}}"#),
    )
    .await;
    match line(&mut read).await {
        ThemeMessage::Result {
            status,
            generation,
            theme,
            ..
        } => {
            assert_eq!(status, ThemeStatus::Confirmed);
            assert!(generation.unwrap() > 1);
            assert_eq!(theme.unwrap().id, "builtin.sleepy-light");
        }
        other => panic!("expected result, got {other:?}"),
    }
    socket
        .shutdown_and_drain(Duration::from_secs(1))
        .await
        .unwrap();
    task.await.unwrap().unwrap();
    let reopened =
        ThemeManager::open(temp.path().join("config"), temp.path().join("state")).unwrap();
    assert_eq!(reopened.current().unwrap().id, "builtin.sleepy-light");
}

#[tokio::test]
async fn pending_restart_journal_reconciles_before_new_apply_is_accepted() {
    let temp = TempDir::new().unwrap();
    let manager =
        ThemeManager::open(temp.path().join("config"), temp.path().join("state")).unwrap();
    manager
        .seed_crash_journal_for_test("builtin.sleepy-light")
        .unwrap();
    let (socket, task) = start(&temp, manager).await;
    let stream = UnixStream::connect(socket.path()).await.unwrap();
    let (read, mut write) = stream.into_split();
    let mut read = BufReader::new(read);
    let request_id = "d78951f8-c6f5-4f7d-8599-d72ed0b34803";
    send(&mut write, format!(r#"{{"schemaVersion":2,"requestId":"{request_id}","operation":{{"type":"apply","data":{{"themeId":"builtin.sleepy-light","expectedGeneration":1}}}}}}"#)).await;
    assert!(
        matches!(line(&mut read).await, ThemeMessage::Candidate { theme, .. } if theme.id == "builtin.sleepy-dark")
    );
    send(
        &mut write,
        format!(r#"{{"schemaVersion":2,"requestId":"{request_id}","accepted":true}}"#),
    )
    .await;
    assert!(matches!(
        line(&mut read).await,
        ThemeMessage::Result {
            status: ThemeStatus::Reconciled,
            ..
        }
    ));
    socket
        .shutdown_and_drain(Duration::from_secs(1))
        .await
        .unwrap();
    task.await.unwrap().unwrap();
    let reopened =
        ThemeManager::open(temp.path().join("config"), temp.path().join("state")).unwrap();
    assert_eq!(reopened.current().unwrap().id, "builtin.sleepy-dark");
    assert!(!reopened.has_journal().unwrap());
}

#[tokio::test]
async fn acknowledgement_timeout_is_bounded_and_preserves_recoverable_journal() {
    let temp = TempDir::new().unwrap();
    let manager = ThemeManager::open_with_acknowledgement_timeout(
        temp.path().join("config"),
        temp.path().join("state"),
        Duration::from_millis(20),
    )
    .unwrap();
    let (socket, task) = start(&temp, manager).await;
    let stream = UnixStream::connect(socket.path()).await.unwrap();
    let (read, mut write) = stream.into_split();
    let mut read = BufReader::new(read);
    let request_id = "d78951f8-c6f5-4f7d-8599-d72ed0b34803";
    send(&mut write, format!(r#"{{"schemaVersion":2,"requestId":"{request_id}","operation":{{"type":"apply","data":{{"themeId":"builtin.sleepy-light","expectedGeneration":1}}}}}}"#)).await;
    assert!(
        matches!(line(&mut read).await, ThemeMessage::Candidate { theme, .. } if theme.id == "builtin.sleepy-light")
    );
    assert!(
        matches!(line(&mut read).await, ThemeMessage::Candidate { theme, .. } if theme.id == "builtin.sleepy-dark")
    );
    assert!(matches!(
        line(&mut read).await,
        ThemeMessage::Result {
            status: ThemeStatus::Unavailable,
            ..
        }
    ));
    socket
        .shutdown_and_drain(Duration::from_secs(1))
        .await
        .unwrap();
    task.await.unwrap().unwrap();
    let reopened =
        ThemeManager::open(temp.path().join("config"), temp.path().join("state")).unwrap();
    assert_eq!(reopened.current().unwrap().id, "builtin.sleepy-dark");
    assert!(reopened.has_journal().unwrap());
}

#[tokio::test]
async fn socket_is_private_and_unknown_fields_fail_closed_without_state_changes() {
    let temp = TempDir::new().unwrap();
    let manager =
        ThemeManager::open(temp.path().join("config"), temp.path().join("state")).unwrap();
    let (socket, task) = start(&temp, manager).await;
    assert_eq!(
        std::fs::metadata(socket.path())
            .unwrap()
            .permissions()
            .mode()
            & 0o777,
        0o600
    );
    let mut stream = UnixStream::connect(socket.path()).await.unwrap();
    stream
        .write_all(b"{\"schemaVersion\":2,\"requestId\":\"d78951f8-c6f5-4f7d-8599-d72ed0b34803\",\"operation\":{\"type\":\"delete\",\"data\":{\"themeId\":\"builtin.sleepy-dark\",\"force\":true}}}\n")
        .await
        .unwrap();
    let mut response = String::new();
    assert_eq!(
        BufReader::new(stream)
            .read_line(&mut response)
            .await
            .unwrap(),
        0
    );
    socket
        .shutdown_and_drain(Duration::from_secs(1))
        .await
        .unwrap();
    task.await.unwrap().unwrap();
    let reopened =
        ThemeManager::open(temp.path().join("config"), temp.path().join("state")).unwrap();
    assert_eq!(reopened.current().unwrap().id, "builtin.sleepy-dark");
    assert!(!reopened.has_journal().unwrap());
}
