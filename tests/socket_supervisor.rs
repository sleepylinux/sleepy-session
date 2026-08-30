// SPDX-License-Identifier: GPL-3.0-only

use std::{
    io,
    os::unix::fs::PermissionsExt,
    os::unix::io::AsRawFd,
    path::Path,
    pin::Pin,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc, Mutex,
    },
    task::{Context, Poll},
    time::Duration,
};

use sleepy_session::sessiond::supervisor::{
    ConnectionLimits, DaemonLifecycle, DaemonNotification, DaemonNotifier, EndpointKind,
    PeerCredentialProvider, PeerCredentials, PreparedDesktopSockets, ReadinessGate,
    SocketSupervisor, StartupBarrier,
};
use tokio::{
    io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt, BufReader, ReadBuf},
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
                    let (read, _) = stream.into_split();
                    let mut read = BufReader::new(read);
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
async fn framed_reader_preserves_a_coalesced_second_frame_after_a_boundary_sized_first_frame() {
    let _serial = serial_test().await;
    let temp = tempfile::tempdir().unwrap();
    let parent = temp.path().join("sleepy");
    prepare_parent(&parent);
    let limits = ConnectionLimits {
        max_frame_bytes: 4,
        ..ConnectionLimits::request()
    };
    let supervisor = Arc::new(
        SocketSupervisor::bind(
            parent.join("desktop-control.sock"),
            unsafe { libc::geteuid() },
            EndpointKind::Request,
            limits,
        )
        .await
        .unwrap(),
    );
    let (frames_tx, mut frames_rx) = mpsc::unbounded_channel();
    let serving = Arc::clone(&supervisor);
    let task = tokio::spawn(async move {
        serving
            .serve(move |stream, context| {
                let frames_tx = frames_tx.clone();
                async move {
                    let (read, _) = stream.into_split();
                    let mut read = BufReader::new(read);
                    let first = context.read_frame(&mut read).await?;
                    let second = context.read_frame(&mut read).await?;
                    frames_tx.send((first, second)).unwrap();
                    Ok(())
                }
            })
            .await
    });

    let mut client = UnixStream::connect(supervisor.path()).await.unwrap();
    client.write_all(b"abcd\nz\n").await.unwrap();
    client.shutdown().await.unwrap();

    assert_eq!(
        tokio::time::timeout(Duration::from_millis(250), frames_rx.recv())
            .await
            .expect("both coalesced frames must reach the same persistent reader")
            .unwrap(),
        (b"abcd".to_vec(), b"z".to_vec())
    );
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
                    let mut reader = BufReader::new(PendingReader);
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

#[tokio::test(start_paused = true)]
async fn real_unix_peer_with_an_in_cap_partial_frame_times_out_then_observes_eof_and_zero_tasks() {
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
            .serve(move |stream, context| {
                let started_tx = Arc::clone(&started_tx);
                let result_tx = Arc::clone(&result_tx);
                async move {
                    let (read, _) = stream.into_split();
                    let mut reader = BufReader::new(read);
                    started_tx.lock().unwrap().take().unwrap().send(()).unwrap();
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
    let mut client = UnixStream::connect(supervisor.path()).await.unwrap();
    client.write_all(b"partial-without-newline").await.unwrap();
    started_rx.await.unwrap();

    tokio::time::advance(Duration::from_millis(4_999)).await;
    tokio::task::yield_now().await;
    assert!(matches!(
        result_rx.try_recv(),
        Err(oneshot::error::TryRecvError::Empty)
    ));
    tokio::time::advance(Duration::from_millis(1)).await;
    assert_eq!(result_rx.await.unwrap(), io::ErrorKind::TimedOut);
    wait_for_metrics(&supervisor, |active, tracked| active == 0 && tracked == 0).await;
    assert_eq!(supervisor.metrics().available_permits, 16);
    assert_eq!(
        client.read_u8().await.unwrap_err().kind(),
        io::ErrorKind::UnexpectedEof
    );

    supervisor.shutdown_and_drain().await.unwrap();
    assert!(task.await.unwrap().is_ok());
}

#[tokio::test(start_paused = true)]
async fn real_unix_slow_reader_blocks_an_in_cap_write_until_timeout_then_observes_eof_and_zero_tasks(
) {
    let _serial = serial_test().await;
    const ONE_MIB: usize = 1024 * 1024;
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
    let payload = Arc::new(vec![b'x'; ONE_MIB]);
    let serving = Arc::clone(&supervisor);
    let task = tokio::spawn(async move {
        serving
            .serve(move |mut stream, context| {
                let started_tx = Arc::clone(&started_tx);
                let result_tx = Arc::clone(&result_tx);
                let payload = Arc::clone(&payload);
                async move {
                    let small_buffer: libc::c_int = 4 * 1024;
                    let result = unsafe {
                        libc::setsockopt(
                            stream.as_raw_fd(),
                            libc::SOL_SOCKET,
                            libc::SO_SNDBUF,
                            std::ptr::from_ref(&small_buffer).cast(),
                            std::mem::size_of_val(&small_buffer) as libc::socklen_t,
                        )
                    };
                    assert_eq!(result, 0, "failed to constrain the real Unix send buffer");
                    started_tx.lock().unwrap().take().unwrap().send(()).unwrap();
                    let kind = context
                        .write_frame(&mut stream, &payload)
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
    let mut client = UnixStream::connect(supervisor.path()).await.unwrap();
    started_rx.await.unwrap();

    tokio::time::advance(Duration::from_millis(4_999)).await;
    tokio::task::yield_now().await;
    assert!(matches!(
        result_rx.try_recv(),
        Err(oneshot::error::TryRecvError::Empty)
    ));
    tokio::time::advance(Duration::from_millis(1)).await;
    assert_eq!(result_rx.await.unwrap(), io::ErrorKind::TimedOut);
    wait_for_metrics(&supervisor, |active, tracked| active == 0 && tracked == 0).await;
    assert_eq!(supervisor.metrics().available_permits, 32);
    let mut partial = Vec::new();
    client.read_to_end(&mut partial).await.unwrap();
    assert!(!partial.is_empty());
    assert!(partial.len() < ONE_MIB + 1);
    assert_ne!(partial.last(), Some(&b'\n'));

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

#[tokio::test]
async fn shutdown_stops_and_reaps_a_pending_single_accept_path() {
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
            .serve_one(|_stream, _context| async move { Ok(()) })
            .await
    });
    tokio::task::yield_now().await;

    assert_eq!(
        supervisor.shutdown_and_drain().await.unwrap(),
        Default::default()
    );
    assert!(tokio::time::timeout(Duration::from_millis(250), task)
        .await
        .expect("shutdown must reap a pending supervised single accept")
        .unwrap()
        .is_ok());
    assert_eq!(supervisor.metrics().active, 0);
    assert_eq!(supervisor.metrics().tracked, 0);
}

#[tokio::test]
async fn active_single_accept_path_uses_supervisor_metrics_and_a_permit_until_shutdown() {
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
            .serve_one(|_stream, context| async move {
                context.cancellation.cancelled().await;
                Ok(())
            })
            .await
    });
    let _client = UnixStream::connect(supervisor.path()).await.unwrap();

    wait_for_metrics(&supervisor, |active, tracked| active == 1 && tracked == 1).await;
    assert_eq!(supervisor.metrics().available_permits, 15);
    assert_eq!(supervisor.shutdown_and_drain().await.unwrap().completed, 1);
    assert!(task.await.unwrap().is_ok());
    assert_eq!(supervisor.metrics().available_permits, 16);
}

#[tokio::test]
async fn aborting_the_outer_serve_future_explicitly_reaps_active_connections_and_accounting() {
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
    let mut client = UnixStream::connect(supervisor.path()).await.unwrap();
    wait_for_metrics(&supervisor, |active, tracked| active == 1 && tracked == 1).await;

    task.abort();
    assert!(task.await.unwrap_err().is_cancelled());
    wait_for_metrics(&supervisor, |active, tracked| active == 0 && tracked == 0).await;
    supervisor.shutdown_and_drain().await.unwrap();
    assert_eq!(supervisor.metrics().available_permits, 32);
    assert_eq!(
        tokio::time::timeout(Duration::from_millis(250), client.read_u8())
            .await
            .expect("outer serve abort must close the supervised connection")
            .unwrap_err()
            .kind(),
        io::ErrorKind::UnexpectedEof
    );
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

struct PanicWhenAborted;

impl Drop for PanicWhenAborted {
    fn drop(&mut self) {
        panic!("intentional panic while the handler future is being aborted");
    }
}

#[tokio::test(start_paused = true)]
async fn drain_report_counts_only_cancelled_joins_as_aborted() {
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
    let connection_index = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let serving = Arc::clone(&supervisor);
    let task = tokio::spawn(async move {
        serving
            .serve(move |_stream, _context| {
                let index = connection_index.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                async move {
                    if index == 1 {
                        let _panic_on_abort = PanicWhenAborted;
                        std::future::pending::<()>().await;
                    } else {
                        std::future::pending::<()>().await;
                    }
                    Ok(())
                }
            })
            .await
    });
    let _first = UnixStream::connect(supervisor.path()).await.unwrap();
    let _second = UnixStream::connect(supervisor.path()).await.unwrap();
    wait_for_metrics(&supervisor, |active, tracked| active == 2 && tracked == 2).await;

    let draining = Arc::clone(&supervisor);
    let drain = tokio::spawn(async move { draining.shutdown_and_drain().await });
    tokio::time::advance(Duration::from_secs(10)).await;
    let report = drain.await.unwrap().unwrap();

    assert_eq!(report.aborted, 1);
    assert_eq!(report.completed, 1);
    assert_eq!(supervisor.metrics().active, 0);
    assert_eq!(supervisor.metrics().tracked, 0);
    assert!(task.await.unwrap().is_ok());
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

struct EventNotifier {
    events: Arc<Mutex<Vec<&'static str>>>,
    fail_ready: bool,
    fail_stopping: bool,
}

impl DaemonNotifier for EventNotifier {
    fn notify(&self, state: DaemonNotification) -> io::Result<()> {
        let (event, fail) = match state {
            DaemonNotification::Ready => ("ready", self.fail_ready),
            DaemonNotification::Stopping => ("stopping", self.fail_stopping),
        };
        self.events.lock().unwrap().push(event);
        if fail {
            Err(io::Error::new(
                io::ErrorKind::BrokenPipe,
                "injected notifier failure",
            ))
        } else {
            Ok(())
        }
    }
}

#[tokio::test]
async fn daemon_lifecycle_waits_for_all_required_workers_then_readies_before_releasing_producers() {
    let _serial = serial_test().await;
    let temp = tempfile::tempdir().unwrap();
    let parent = temp.path().join("sleepy");
    prepare_parent(&parent);
    let listener = SocketSupervisor::bind(
        parent.join("session.sock"),
        unsafe { libc::geteuid() },
        EndpointKind::Stream,
        ConnectionLimits::stream(),
    )
    .await
    .unwrap();
    let events = Arc::new(Mutex::new(Vec::new()));
    let lifecycle = DaemonLifecycle::new(Arc::new(EventNotifier {
        events: Arc::clone(&events),
        fail_ready: false,
        fail_stopping: false,
    }));
    let mut startup = StartupBarrier::new();
    let mut workers = Vec::new();
    for name in [
        "session",
        "control",
        "osd",
        "daily",
        "theme",
        "notification",
        "desktop-events",
        "desktop-requests",
        "notification-dbus",
    ] {
        let required = startup.required_task(name);
        let events = Arc::clone(&events);
        workers.push(tokio::spawn(async move {
            events.lock().unwrap().push("worker-started");
            required.ready_and_wait().await.unwrap();
            events.lock().unwrap().push("worker-released");
        }));
    }

    lifecycle
        .complete_startup(&[listener.path()], &mut startup, || {
            let events = Arc::clone(&events);
            async move {
                events.lock().unwrap().push("producer-started");
                Ok(())
            }
        })
        .await
        .unwrap();
    for worker in workers {
        worker.await.unwrap();
    }

    let events = events.lock().unwrap();
    let ready = events.iter().position(|event| *event == "ready").unwrap();
    let producer = events
        .iter()
        .position(|event| *event == "producer-started")
        .unwrap();
    assert_eq!(
        events[..ready]
            .iter()
            .filter(|event| **event == "worker-started")
            .count(),
        9
    );
    assert!(ready < producer);
    assert!(events[ready + 1..].contains(&"worker-released"));
}

#[tokio::test]
async fn required_worker_start_failure_prevents_ready_and_producer_start() {
    let _serial = serial_test().await;
    let temp = tempfile::tempdir().unwrap();
    let parent = temp.path().join("sleepy");
    prepare_parent(&parent);
    let listener = SocketSupervisor::bind(
        parent.join("session.sock"),
        unsafe { libc::geteuid() },
        EndpointKind::Stream,
        ConnectionLimits::stream(),
    )
    .await
    .unwrap();
    let events = Arc::new(Mutex::new(Vec::new()));
    let lifecycle = DaemonLifecycle::new(Arc::new(EventNotifier {
        events: Arc::clone(&events),
        fail_ready: false,
        fail_stopping: false,
    }));
    let producer_started = Arc::new(AtomicBool::new(false));
    let mut startup = StartupBarrier::new();
    let waiting = startup.required_task("session");
    let failed = startup.required_task("notification-dbus");
    let waiting_task = tokio::spawn(async move { waiting.ready_and_wait().await });
    failed.fail(io::Error::new(
        io::ErrorKind::BrokenPipe,
        "injected worker start failure",
    ));

    let producer_flag = Arc::clone(&producer_started);
    let error = lifecycle
        .complete_startup(&[listener.path()], &mut startup, move || async move {
            producer_flag.store(true, Ordering::SeqCst);
            Ok(())
        })
        .await
        .unwrap_err();

    assert_eq!(error.kind(), io::ErrorKind::BrokenPipe);
    assert!(events.lock().unwrap().is_empty());
    assert!(!producer_started.load(Ordering::SeqCst));
    assert_eq!(
        waiting_task.await.unwrap().unwrap_err().kind(),
        io::ErrorKind::Interrupted
    );
}

#[tokio::test]
async fn ready_worker_disappearing_before_release_prevents_ready_and_producer_start() {
    let _serial = serial_test().await;
    let events = Arc::new(Mutex::new(Vec::new()));
    let lifecycle = DaemonLifecycle::new(Arc::new(EventNotifier {
        events: Arc::clone(&events),
        fail_ready: false,
        fail_stopping: false,
    }));
    let producer_started = Arc::new(AtomicBool::new(false));
    let mut startup = StartupBarrier::new();
    let worker = startup.required_task("notification-dbus");
    let (entered_sender, entered) = oneshot::channel();
    let worker_task = tokio::spawn(async move {
        entered_sender.send(()).unwrap();
        worker.ready_and_wait().await
    });
    entered.await.unwrap();
    tokio::task::yield_now().await;
    worker_task.abort();
    assert!(worker_task.await.unwrap_err().is_cancelled());

    let producer_flag = Arc::clone(&producer_started);
    let error = lifecycle
        .complete_startup(&[], &mut startup, move || async move {
            producer_flag.store(true, Ordering::SeqCst);
            Ok(())
        })
        .await
        .unwrap_err();

    assert_eq!(error.kind(), io::ErrorKind::BrokenPipe);
    assert!(events.lock().unwrap().is_empty());
    assert!(!producer_started.load(Ordering::SeqCst));
}

#[tokio::test]
async fn real_required_serve_start_failure_prevents_ready_and_producer_start() {
    let _serial = serial_test().await;
    let temp = tempfile::tempdir().unwrap();
    let parent = temp.path().join("sleepy");
    prepare_parent(&parent);
    let supervisor = Arc::new(
        SocketSupervisor::bind(
            parent.join("session.sock"),
            unsafe { libc::geteuid() },
            EndpointKind::Stream,
            ConnectionLimits::stream(),
        )
        .await
        .unwrap(),
    );
    let first_serving = Arc::clone(&supervisor);
    let first_task = tokio::spawn(async move {
        first_serving
            .serve(|_stream, _context| async move { Ok(()) })
            .await
    });
    tokio::task::yield_now().await;

    let events = Arc::new(Mutex::new(Vec::new()));
    let lifecycle = DaemonLifecycle::new(Arc::new(EventNotifier {
        events: Arc::clone(&events),
        fail_ready: false,
        fail_stopping: false,
    }));
    let producer_started = Arc::new(AtomicBool::new(false));
    let mut startup = StartupBarrier::new();
    let duplicate_startup = startup.required_task("duplicate-session");
    let duplicate_serving = Arc::clone(&supervisor);
    let duplicate_task = tokio::spawn(async move {
        duplicate_serving
            .serve_with_startup(duplicate_startup, |_stream, _context| async move { Ok(()) })
            .await
    });
    let producer_flag = Arc::clone(&producer_started);

    assert_eq!(
        lifecycle
            .complete_startup(&[supervisor.path()], &mut startup, move || async move {
                producer_flag.store(true, Ordering::SeqCst);
                Ok(())
            })
            .await
            .unwrap_err()
            .kind(),
        io::ErrorKind::BrokenPipe
    );
    assert_eq!(
        duplicate_task.await.unwrap().unwrap_err().kind(),
        io::ErrorKind::AlreadyExists
    );
    assert!(events.lock().unwrap().is_empty());
    assert!(!producer_started.load(Ordering::SeqCst));

    supervisor.shutdown_and_drain().await.unwrap();
    assert!(first_task.await.unwrap().is_ok());
}

#[tokio::test]
async fn notifier_failures_do_not_release_producers_and_stopping_failure_still_precedes_drain() {
    let _serial = serial_test().await;
    let temp = tempfile::tempdir().unwrap();
    let parent = temp.path().join("sleepy");
    prepare_parent(&parent);
    let listener = SocketSupervisor::bind(
        parent.join("session.sock"),
        unsafe { libc::geteuid() },
        EndpointKind::Stream,
        ConnectionLimits::stream(),
    )
    .await
    .unwrap();
    let events = Arc::new(Mutex::new(Vec::new()));
    let lifecycle = DaemonLifecycle::new(Arc::new(EventNotifier {
        events: Arc::clone(&events),
        fail_ready: true,
        fail_stopping: true,
    }));
    let producer_started = Arc::new(AtomicBool::new(false));
    let mut startup = StartupBarrier::new();
    let worker = startup.required_task("session");
    let worker_task = tokio::spawn(async move { worker.ready_and_wait().await });
    let producer_flag = Arc::clone(&producer_started);

    assert_eq!(
        lifecycle
            .complete_startup(&[listener.path()], &mut startup, move || async move {
                producer_flag.store(true, Ordering::SeqCst);
                Ok(())
            })
            .await
            .unwrap_err()
            .kind(),
        io::ErrorKind::BrokenPipe
    );
    assert!(!producer_started.load(Ordering::SeqCst));
    assert_eq!(
        worker_task.await.unwrap().unwrap_err().kind(),
        io::ErrorKind::Interrupted
    );

    let drain_events = Arc::clone(&events);
    assert_eq!(
        lifecycle
            .stop_and_drain(move || async move {
                drain_events.lock().unwrap().push("drain");
                Ok(())
            })
            .await
            .unwrap_err()
            .kind(),
        io::ErrorKind::BrokenPipe
    );
    assert_eq!(*events.lock().unwrap(), vec!["ready", "stopping", "drain"]);
}

#[tokio::test]
async fn fail_closed_v3_workers_start_before_ready_then_close_drain_and_remove_both_paths() {
    let _serial = serial_test().await;
    let temp = tempfile::tempdir().unwrap();
    let parent = temp.path().join("sleepy");
    prepare_parent(&parent);
    let desktop = PreparedDesktopSockets::bind(&parent, unsafe { libc::geteuid() })
        .await
        .unwrap();
    let events_path = desktop.events().path().to_owned();
    let requests_path = desktop.requests().path().to_owned();
    let event_log = Arc::new(Mutex::new(Vec::new()));
    let lifecycle = DaemonLifecycle::new(Arc::new(EventNotifier {
        events: Arc::clone(&event_log),
        fail_ready: false,
        fail_stopping: false,
    }));
    let mut startup = StartupBarrier::new();
    let events_worker = startup.required_task("desktop-events");
    let requests_worker = startup.required_task("desktop-requests");
    let events_socket = desktop.events();
    let serving_events = Arc::clone(&events_socket);
    let events_task = tokio::spawn(async move {
        serving_events
            .serve_with_startup(events_worker, |stream, _context| async move {
                drop(stream);
                Ok(())
            })
            .await
    });
    let requests_socket = desktop.requests();
    let serving_requests = Arc::clone(&requests_socket);
    let requests_task = tokio::spawn(async move {
        serving_requests
            .serve_with_startup(requests_worker, |stream, _context| async move {
                drop(stream);
                Ok(())
            })
            .await
    });

    lifecycle
        .complete_startup(&desktop.listener_paths(), &mut startup, || async { Ok(()) })
        .await
        .unwrap();
    for path in [&events_path, &requests_path] {
        let mut peer = UnixStream::connect(path).await.unwrap();
        assert_eq!(
            tokio::time::timeout(Duration::from_millis(250), peer.read_u8())
                .await
                .expect("fail-closed v3 handler must promptly close")
                .unwrap_err()
                .kind(),
            io::ErrorKind::UnexpectedEof
        );
    }
    wait_for_metrics(&events_socket, |active, tracked| {
        active == 0 && tracked == 0
    })
    .await;
    wait_for_metrics(&requests_socket, |active, tracked| {
        active == 0 && tracked == 0
    })
    .await;

    lifecycle
        .stop_and_drain(|| async {
            let (events, requests) = tokio::join!(
                events_socket.shutdown_and_drain(),
                requests_socket.shutdown_and_drain(),
            );
            events?;
            requests?;
            Ok(())
        })
        .await
        .unwrap();
    assert!(events_task.await.unwrap().is_ok());
    assert!(requests_task.await.unwrap().is_ok());
    drop(events_socket);
    drop(requests_socket);
    drop(desktop);
    assert!(!events_path.exists());
    assert!(!requests_path.exists());
    assert_eq!(*event_log.lock().unwrap(), vec!["ready", "stopping"]);
}
