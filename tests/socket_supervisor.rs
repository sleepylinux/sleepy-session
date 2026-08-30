// SPDX-License-Identifier: GPL-3.0-only

use std::{
    io,
    os::unix::fs::PermissionsExt,
    path::Path,
    pin::Pin,
    sync::{Arc, Mutex},
    task::{Context, Poll},
    time::Duration,
};

use sleepy_session::sessiond::supervisor::{
    ConnectionLimits, DaemonNotification, DaemonNotifier, EndpointKind, PeerCredentialProvider,
    PeerCredentials, PreparedDesktopSockets, ReadinessGate, SocketSupervisor,
};
use tokio::{
    io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt, ReadBuf},
    net::UnixStream,
    sync::{mpsc, oneshot},
};

static TEST_SERIAL: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

async fn serial_test() -> tokio::sync::MutexGuard<'static, ()> {
    TEST_SERIAL.lock().await
}

fn prepare_parent(path: &Path) {
    std::fs::create_dir_all(path).unwrap();
    std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o700)).unwrap();
}

async fn wait_for_metrics(supervisor: &SocketSupervisor, predicate: impl Fn(usize, usize) -> bool) {
    for _ in 0..10_000 {
        let metrics = supervisor.metrics();
        if predicate(metrics.active, metrics.tracked) {
            return;
        }
        tokio::task::yield_now().await;
    }
    let metrics = supervisor.metrics();
    panic!(
        "supervisor metrics did not converge: active={}, tracked={}",
        metrics.active, metrics.tracked
    );
}

async fn expect_prompt_refusal(stream: &mut UnixStream) {
    let mut closed = [0_u8; 1];
    let count = tokio::time::timeout(Duration::from_millis(250), stream.read(&mut closed))
        .await
        .expect("excess client did not receive bounded refusal")
        .unwrap();
    assert_eq!(count, 0, "excess client was not closed");
}

#[tokio::test]
async fn stream_limit_accepts_32_and_refuses_client_33_without_waiting_for_a_permit() {
    let _serial = serial_test().await;
    let temp = tempfile::tempdir().unwrap();
    let parent = temp.path().join("sleepy");
    prepare_parent(&parent);
    let supervisor = Arc::new(
        SocketSupervisor::bind(
            parent.join("desktop.sock"),
            unsafe { libc::geteuid() },
            EndpointKind::Stream,
            ConnectionLimits::stream(),
        )
        .await
        .unwrap(),
    );
    let serving = Arc::clone(&supervisor);
    let task = tokio::spawn(async move {
        serving
            .serve(|_stream, context| async move {
                context.cancellation.cancelled().await;
                Ok(())
            })
            .await
    });

    let mut clients = Vec::new();
    for _ in 0..32 {
        clients.push(UnixStream::connect(supervisor.path()).await.unwrap());
    }
    wait_for_metrics(&supervisor, |active, tracked| active == 32 && tracked == 32).await;

    let mut rejected = UnixStream::connect(supervisor.path()).await.unwrap();
    expect_prompt_refusal(&mut rejected).await;
    assert_eq!(supervisor.metrics().rejected, 1);

    let report = supervisor.shutdown_and_drain().await.unwrap();
    assert_eq!(report.aborted, 0);
    assert_eq!(report.completed, 32);
    assert!(task.await.unwrap().is_ok());
    drop(clients);
}

#[tokio::test]
async fn request_limit_accepts_16_and_refuses_client_17_without_waiting_for_a_permit() {
    let _serial = serial_test().await;
    let temp = tempfile::tempdir().unwrap();
    let parent = temp.path().join("sleepy");
    prepare_parent(&parent);
    let supervisor = Arc::new(
        SocketSupervisor::bind(
            parent.join("desktop-control.sock"),
            unsafe { libc::geteuid() },
            EndpointKind::Request,
            ConnectionLimits::request(),
        )
        .await
        .unwrap(),
    );
    let serving = Arc::clone(&supervisor);
    let task = tokio::spawn(async move {
        serving
            .serve(|_stream, context| async move {
                context.cancellation.cancelled().await;
                Ok(())
            })
            .await
    });

    let mut clients = Vec::new();
    for _ in 0..16 {
        clients.push(UnixStream::connect(supervisor.path()).await.unwrap());
    }
    wait_for_metrics(&supervisor, |active, tracked| active == 16 && tracked == 16).await;

    let mut rejected = UnixStream::connect(supervisor.path()).await.unwrap();
    expect_prompt_refusal(&mut rejected).await;

    let report = supervisor.shutdown_and_drain().await.unwrap();
    assert_eq!(report.aborted, 0);
    assert_eq!(report.completed, 16);
    assert!(task.await.unwrap().is_ok());
    drop(clients);
}

#[tokio::test]
async fn framed_reader_accepts_one_mib_and_closes_one_byte_over_without_peer_eof() {
    let _serial = serial_test().await;
    const ONE_MIB: usize = 1024 * 1024;

    let temp = tempfile::tempdir().unwrap();
    let parent = temp.path().join("sleepy");
    prepare_parent(&parent);
    let supervisor = Arc::new(
        SocketSupervisor::bind(
            parent.join("desktop-control.sock"),
            unsafe { libc::geteuid() },
            EndpointKind::Request,
            ConnectionLimits::request(),
        )
        .await
        .unwrap(),
    );
    let (results, mut received) = mpsc::unbounded_channel();
    let serving = Arc::clone(&supervisor);
    let task = tokio::spawn(async move {
        serving
            .serve(move |stream, context| {
                let results = results.clone();
                async move {
                    let (mut read, _) = stream.into_split();
                    let result = context.read_frame(&mut read).await.map(|frame| frame.len());
                    results.send(result).unwrap();
                    Ok(())
                }
            })
            .await
    });

    let mut exact = UnixStream::connect(supervisor.path()).await.unwrap();
    exact.write_all(&vec![b'x'; ONE_MIB]).await.unwrap();
    exact.write_all(b"\n").await.unwrap();
    assert_eq!(received.recv().await.unwrap().unwrap(), ONE_MIB);

    let mut oversized = UnixStream::connect(supervisor.path()).await.unwrap();
    let _ = oversized.write_all(&vec![b'x'; ONE_MIB + 1]).await;
    let _ = oversized.write_all(b"\n").await;
    assert_eq!(
        received.recv().await.unwrap().unwrap_err().kind(),
        io::ErrorKind::InvalidData
    );
    expect_prompt_refusal(&mut oversized).await;

    supervisor.shutdown_and_drain().await.unwrap();
    assert!(task.await.unwrap().is_ok());
}

#[tokio::test]
async fn configured_frame_limit_cannot_exceed_one_mib() {
    let _serial = serial_test().await;
    let temp = tempfile::tempdir().unwrap();
    let parent = temp.path().join("sleepy");
    prepare_parent(&parent);
    let path = parent.join("desktop-control.sock");
    let limits = ConnectionLimits {
        max_frame_bytes: 1024 * 1024 + 1,
        ..ConnectionLimits::request()
    };

    let error = SocketSupervisor::bind(
        &path,
        unsafe { libc::geteuid() },
        EndpointKind::Request,
        limits,
    )
    .await
    .err()
    .expect("the global frame ceiling must not be configurable upward");

    assert_eq!(error.kind(), io::ErrorKind::InvalidInput);
    assert!(!path.exists());
}

struct PendingReader;

impl AsyncRead for PendingReader {
    fn poll_read(
        self: Pin<&mut Self>,
        _cx: &mut Context<'_>,
        _buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        Poll::Pending
    }
}

struct PendingWriter;

impl AsyncWrite for PendingWriter {
    fn poll_write(
        self: Pin<&mut Self>,
        _cx: &mut Context<'_>,
        _buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        Poll::Pending
    }

    fn poll_flush(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Poll::Ready(Ok(()))
    }

    fn poll_shutdown(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Poll::Ready(Ok(()))
    }
}

#[tokio::test(start_paused = true)]
async fn framed_read_deadline_is_exactly_five_seconds() {
    let _serial = serial_test().await;
    let temp = tempfile::tempdir().unwrap();
    let parent = temp.path().join("sleepy");
    prepare_parent(&parent);
    let supervisor = Arc::new(
        SocketSupervisor::bind(
            parent.join("desktop-control.sock"),
            unsafe { libc::geteuid() },
            EndpointKind::Request,
            ConnectionLimits::request(),
        )
        .await
        .unwrap(),
    );
    let (started_tx, started_rx) = oneshot::channel();
    let (result_tx, mut result_rx) = oneshot::channel();
    let started_tx = Arc::new(Mutex::new(Some(started_tx)));
    let result_tx = Arc::new(Mutex::new(Some(result_tx)));
    let serving = Arc::clone(&supervisor);
    let task = tokio::spawn(async move {
        serving
            .serve(move |_stream, context| {
                let started_tx = Arc::clone(&started_tx);
                let result_tx = Arc::clone(&result_tx);
                async move {
                    started_tx.lock().unwrap().take().unwrap().send(()).unwrap();
                    let mut reader = PendingReader;
                    let kind = context.read_frame(&mut reader).await.unwrap_err().kind();
                    result_tx
                        .lock()
                        .unwrap()
                        .take()
                        .unwrap()
                        .send(kind)
                        .unwrap();
                    Ok(())
                }
            })
            .await
    });
    let _client = UnixStream::connect(supervisor.path()).await.unwrap();
    started_rx.await.unwrap();

    tokio::time::advance(Duration::from_millis(4_999)).await;
    tokio::task::yield_now().await;
    assert!(matches!(
        result_rx.try_recv(),
        Err(oneshot::error::TryRecvError::Empty)
    ));
    tokio::time::advance(Duration::from_millis(1)).await;
    assert_eq!(result_rx.await.unwrap(), io::ErrorKind::TimedOut);

    supervisor.shutdown_and_drain().await.unwrap();
    assert!(task.await.unwrap().is_ok());
}

#[tokio::test(start_paused = true)]
async fn framed_write_deadline_is_exactly_five_seconds() {
    let _serial = serial_test().await;
    let temp = tempfile::tempdir().unwrap();
    let parent = temp.path().join("sleepy");
    prepare_parent(&parent);
    let supervisor = Arc::new(
        SocketSupervisor::bind(
            parent.join("desktop.sock"),
            unsafe { libc::geteuid() },
            EndpointKind::Stream,
            ConnectionLimits::stream(),
        )
        .await
        .unwrap(),
    );
    let (started_tx, started_rx) = oneshot::channel();
    let (result_tx, mut result_rx) = oneshot::channel();
    let started_tx = Arc::new(Mutex::new(Some(started_tx)));
    let result_tx = Arc::new(Mutex::new(Some(result_tx)));
    let serving = Arc::clone(&supervisor);
    let task = tokio::spawn(async move {
        serving
            .serve(move |_stream, context| {
                let started_tx = Arc::clone(&started_tx);
                let result_tx = Arc::clone(&result_tx);
                async move {
                    started_tx.lock().unwrap().take().unwrap().send(()).unwrap();
                    let mut writer = PendingWriter;
                    let kind = context
                        .write_frame(&mut writer, b"frame")
                        .await
                        .unwrap_err()
                        .kind();
                    result_tx
                        .lock()
                        .unwrap()
                        .take()
                        .unwrap()
                        .send(kind)
                        .unwrap();
                    Ok(())
                }
            })
            .await
    });
    let _client = UnixStream::connect(supervisor.path()).await.unwrap();
    started_rx.await.unwrap();

    tokio::time::advance(Duration::from_millis(4_999)).await;
    tokio::task::yield_now().await;
    assert!(matches!(
        result_rx.try_recv(),
        Err(oneshot::error::TryRecvError::Empty)
    ));
    tokio::time::advance(Duration::from_millis(1)).await;
    assert_eq!(result_rx.await.unwrap(), io::ErrorKind::TimedOut);

    supervisor.shutdown_and_drain().await.unwrap();
    assert!(task.await.unwrap().is_ok());
}

#[derive(Clone)]
struct FixedCredentials(PeerCredentials);

impl PeerCredentialProvider for FixedCredentials {
    fn peer_credentials(&self, _stream: &UnixStream) -> io::Result<PeerCredentials> {
        Ok(self.0)
    }
}

#[tokio::test]
async fn peer_uid_mismatch_is_rejected_before_a_connection_task_is_spawned() {
    let _serial = serial_test().await;
    let temp = tempfile::tempdir().unwrap();
    let parent = temp.path().join("sleepy");
    prepare_parent(&parent);
    let expected_uid = unsafe { libc::geteuid() };
    let supervisor = Arc::new(
        SocketSupervisor::bind_with_peer_credentials(
            parent.join("desktop.sock"),
            expected_uid,
            EndpointKind::Stream,
            ConnectionLimits::stream(),
            Arc::new(FixedCredentials(PeerCredentials {
                pid: 4242,
                uid: expected_uid.saturating_add(1),
            })),
        )
        .await
        .unwrap(),
    );
    let serving = Arc::clone(&supervisor);
    let task = tokio::spawn(async move {
        serving
            .serve(|_stream, _context| async move {
                panic!("UID-mismatched peer reached the connection handler")
            })
            .await
    });

    let mut client = UnixStream::connect(supervisor.path()).await.unwrap();
    expect_prompt_refusal(&mut client).await;
    wait_for_metrics(&supervisor, |active, tracked| active == 0 && tracked == 0).await;
    assert_eq!(supervisor.metrics().rejected, 1);

    supervisor.shutdown_and_drain().await.unwrap();
    assert!(task.await.unwrap().is_ok());
}

#[tokio::test]
async fn connection_context_contains_real_linux_peer_pid_and_uid() {
    let _serial = serial_test().await;
    let temp = tempfile::tempdir().unwrap();
    let parent = temp.path().join("sleepy");
    prepare_parent(&parent);
    let supervisor = Arc::new(
        SocketSupervisor::bind(
            parent.join("desktop.sock"),
            unsafe { libc::geteuid() },
            EndpointKind::Stream,
            ConnectionLimits::stream(),
        )
        .await
        .unwrap(),
    );
    let (sent, mut received) = mpsc::unbounded_channel();
    let serving = Arc::clone(&supervisor);
    let task = tokio::spawn(async move {
        serving
            .serve(move |_stream, context| {
                let sent = sent.clone();
                async move {
                    sent.send((context.peer_pid, context.peer_uid)).unwrap();
                    Ok(())
                }
            })
            .await
    });

    let _client = UnixStream::connect(supervisor.path()).await.unwrap();
    assert_eq!(
        received.recv().await.unwrap(),
        (std::process::id(), unsafe { libc::geteuid() })
    );

    supervisor.shutdown_and_drain().await.unwrap();
    assert!(task.await.unwrap().is_ok());
}

fn open_fd_count() -> usize {
    std::fs::read_dir("/proc/self/fd").unwrap().count()
}

async fn reconnect_once(path: &Path) {
    let mut client = UnixStream::connect(path).await.unwrap();
    let mut bytes = Vec::new();
    client.read_to_end(&mut bytes).await.unwrap();
}

#[tokio::test]
async fn completed_tasks_and_file_descriptors_return_to_baseline_after_1000_reconnects() {
    let _serial = serial_test().await;
    let temp = tempfile::tempdir().unwrap();
    let parent = temp.path().join("sleepy");
    prepare_parent(&parent);
    let supervisor = Arc::new(
        SocketSupervisor::bind(
            parent.join("desktop.sock"),
            unsafe { libc::geteuid() },
            EndpointKind::Stream,
            ConnectionLimits::stream(),
        )
        .await
        .unwrap(),
    );
    let serving = Arc::clone(&supervisor);
    let task = tokio::spawn(async move {
        serving
            .serve(|_stream, _context| async move { Ok(()) })
            .await
    });

    reconnect_once(supervisor.path()).await;
    wait_for_metrics(&supervisor, |active, tracked| active == 0 && tracked == 0).await;
    let baseline = open_fd_count();

    for _ in 0..1_000 {
        reconnect_once(supervisor.path()).await;
    }
    wait_for_metrics(&supervisor, |active, tracked| active == 0 && tracked == 0).await;
    assert_eq!(open_fd_count(), baseline);
    assert!(supervisor.metrics().completed >= 1_001);

    let report = supervisor.shutdown_and_drain().await.unwrap();
    assert_eq!(report.aborted, 0);
    assert_eq!(report.completed, 0);
    assert!(task.await.unwrap().is_ok());
}

#[tokio::test(start_paused = true)]
async fn shutdown_drains_for_at_most_ten_seconds_then_aborts_stuck_tasks() {
    let _serial = serial_test().await;
    let temp = tempfile::tempdir().unwrap();
    let parent = temp.path().join("sleepy");
    prepare_parent(&parent);
    let supervisor = Arc::new(
        SocketSupervisor::bind(
            parent.join("desktop.sock"),
            unsafe { libc::geteuid() },
            EndpointKind::Stream,
            ConnectionLimits::stream(),
        )
        .await
        .unwrap(),
    );
    let (started_tx, started_rx) = oneshot::channel();
    let started_tx = Arc::new(Mutex::new(Some(started_tx)));
    let serving = Arc::clone(&supervisor);
    let serve_task = tokio::spawn(async move {
        serving
            .serve(move |_stream, _context| {
                let started_tx = Arc::clone(&started_tx);
                async move {
                    started_tx.lock().unwrap().take().unwrap().send(()).unwrap();
                    std::future::pending::<io::Result<()>>().await
                }
            })
            .await
    });
    let _client = UnixStream::connect(supervisor.path()).await.unwrap();
    started_rx.await.unwrap();

    let draining = Arc::clone(&supervisor);
    let drain_task = tokio::spawn(async move { draining.shutdown_and_drain().await });
    tokio::time::advance(Duration::from_millis(9_999)).await;
    tokio::task::yield_now().await;
    assert!(!drain_task.is_finished());
    tokio::time::advance(Duration::from_millis(1)).await;
    let report = drain_task.await.unwrap().unwrap();
    assert_eq!(report.completed, 0);
    assert_eq!(report.aborted, 1);
    assert_eq!(supervisor.metrics().active, 0);
    assert_eq!(supervisor.metrics().tracked, 0);
    assert!(serve_task.await.unwrap().is_ok());
}

#[derive(Default)]
struct RecordingNotifier {
    states: Mutex<Vec<DaemonNotification>>,
}

impl DaemonNotifier for RecordingNotifier {
    fn notify(&self, state: DaemonNotification) -> io::Result<()> {
        self.states.lock().unwrap().push(state);
        Ok(())
    }
}

#[tokio::test]
async fn readiness_requires_every_listed_listener_and_stopping_is_observable_without_systemd() {
    let _serial = serial_test().await;
    let temp = tempfile::tempdir().unwrap();
    let parent = temp.path().join("sleepy");
    prepare_parent(&parent);
    let socket = SocketSupervisor::bind(
        parent.join("session.sock"),
        unsafe { libc::geteuid() },
        EndpointKind::Stream,
        ConnectionLimits::stream(),
    )
    .await
    .unwrap();
    let recorder = Arc::new(RecordingNotifier::default());
    let gate = ReadinessGate::new(Arc::clone(&recorder));

    let missing = parent.join("desktop.sock");
    assert_eq!(
        gate.ready(&[socket.path(), &missing]).unwrap_err().kind(),
        io::ErrorKind::NotFound
    );
    assert!(recorder.states.lock().unwrap().is_empty());

    let desktop = SocketSupervisor::bind(
        &missing,
        unsafe { libc::geteuid() },
        EndpointKind::DesktopStream,
        ConnectionLimits::stream(),
    )
    .await
    .unwrap();
    gate.ready(&[socket.path(), desktop.path()]).unwrap();
    gate.stopping().unwrap();
    assert_eq!(
        *recorder.states.lock().unwrap(),
        vec![DaemonNotification::Ready, DaemonNotification::Stopping]
    );
}

#[tokio::test]
async fn prepared_desktop_pair_binds_both_v3_paths_privately_before_readiness() {
    let _serial = serial_test().await;
    let temp = tempfile::tempdir().unwrap();
    let parent = temp.path().join("sleepy");
    prepare_parent(&parent);

    let desktop = PreparedDesktopSockets::bind(&parent, unsafe { libc::geteuid() })
        .await
        .unwrap();
    assert_eq!(desktop.events().path(), parent.join("desktop.sock"));
    assert_eq!(
        desktop.requests().path(),
        parent.join("desktop-control.sock")
    );
    for path in desktop.listener_paths() {
        assert_eq!(
            std::fs::metadata(path).unwrap().permissions().mode() & 0o777,
            0o600
        );
    }

    let recorder = Arc::new(RecordingNotifier::default());
    ReadinessGate::new(Arc::clone(&recorder))
        .ready(&desktop.listener_paths())
        .unwrap();
    assert_eq!(
        *recorder.states.lock().unwrap(),
        vec![DaemonNotification::Ready]
    );
}
