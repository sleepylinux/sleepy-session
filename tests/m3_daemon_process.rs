use std::{
    io::{BufRead, BufReader},
    path::Path,
    process::{Command, Stdio},
    sync::mpsc,
    thread,
    time::{Duration, Instant},
};

use sleepy_sdk::{
    validate_event_envelope, EventCauseKind, LifecycleEvent, LifecycleState, SessionEvent,
};

#[test]
fn daemon_and_watch_client_replay_a_full_snapshot_and_children_are_reaped() {
    let temp = tempfile::tempdir().unwrap();
    let runtime = temp.path().join("runtime");
    let state = temp.path().join("state");
    std::fs::create_dir_all(&runtime).unwrap();
    std::fs::create_dir_all(&state).unwrap();

    let daemon = Command::new(env!("CARGO_BIN_EXE_sleepy-sessiond"))
        .env("XDG_RUNTIME_DIR", &runtime)
        .env("XDG_STATE_HOME", &state)
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();
    let mut daemon = ChildGuard(Some(daemon));
    let socket = runtime.join("sleepy/session.sock");
    wait_for_path(&socket, Duration::from_secs(2));

    let mut watcher = Command::new(env!("CARGO_BIN_EXE_sleepyctl"))
        .args(["events", "watch", "--format", "ndjson"])
        .env("XDG_RUNTIME_DIR", &runtime)
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();
    let stdout = watcher.stdout.take().unwrap();
    let mut watcher = ChildGuard(Some(watcher));
    let (sender, receiver) = mpsc::sync_channel(1);
    let reader = thread::spawn(move || {
        let mut line = String::new();
        let result = BufReader::new(stdout).read_line(&mut line);
        let _ = sender.send((result, line));
    });
    let (result, line) = receiver
        .recv_timeout(Duration::from_secs(2))
        .expect("watch client did not receive replay before deadline");
    result.unwrap();
    let event = validate_event_envelope(line.trim()).unwrap();
    assert!(matches!(event.payload, SessionEvent::FullSnapshot(_)));
    assert_eq!(event.cause.kind, EventCauseKind::Replay);

    watcher.kill_and_wait();
    reader.join().unwrap();
    daemon.kill_and_wait();
}

#[test]
fn daemon_sigint_reconciles_lifecycle_before_socket_cleanup() {
    let temp = tempfile::tempdir().unwrap();
    let runtime = temp.path().join("runtime");
    let state = temp.path().join("state");
    std::fs::create_dir_all(&runtime).unwrap();
    std::fs::create_dir_all(&state).unwrap();

    let daemon = Command::new(env!("CARGO_BIN_EXE_sleepy-sessiond"))
        .env("XDG_RUNTIME_DIR", &runtime)
        .env("XDG_STATE_HOME", &state)
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();
    let mut daemon = ChildGuard(Some(daemon));
    let socket = runtime.join("sleepy/session.sock");
    wait_for_path(&socket, Duration::from_secs(2));

    let mut watcher = Command::new(env!("CARGO_BIN_EXE_sleepyctl"))
        .args(["events", "watch", "--format", "ndjson"])
        .env("XDG_RUNTIME_DIR", &runtime)
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();
    let mut lines = BufReader::new(watcher.stdout.take().unwrap()).lines();
    let replay = validate_event_envelope(lines.next().unwrap().unwrap().trim()).unwrap();
    assert!(matches!(replay.payload, SessionEvent::FullSnapshot(_)));

    let daemon_pid = daemon.0.as_ref().unwrap().id() as libc::pid_t;
    assert_eq!(unsafe { libc::kill(daemon_pid, libc::SIGINT) }, 0);

    let stopping = validate_event_envelope(lines.next().unwrap().unwrap().trim()).unwrap();
    let reconciled = validate_event_envelope(lines.next().unwrap().unwrap().trim()).unwrap();
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
    assert!(stopping.generation > replay.generation);
    assert!(reconciled.generation > stopping.generation);

    let status = daemon.0.take().unwrap().wait().unwrap();
    assert!(status.success());
    assert!(!socket.exists());
    assert!(watcher.wait().unwrap().success());
}

struct ChildGuard(Option<std::process::Child>);

impl ChildGuard {
    fn kill_and_wait(&mut self) {
        if let Some(mut child) = self.0.take() {
            child.kill().unwrap();
            assert!(!child.wait().unwrap().success());
        }
    }
}

impl Drop for ChildGuard {
    fn drop(&mut self) {
        if let Some(mut child) = self.0.take() {
            let _ = child.kill();
            let _ = child.wait();
        }
    }
}

fn wait_for_path(path: &Path, deadline: Duration) {
    let start = Instant::now();
    while !path.exists() {
        assert!(start.elapsed() < deadline, "daemon socket did not appear");
        thread::sleep(Duration::from_millis(10));
    }
}
