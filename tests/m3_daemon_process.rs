use std::{
    ffi::CString,
    fs::OpenOptions,
    io::{BufRead, BufReader, Read, Write},
    os::unix::{ffi::OsStrExt, fs::PermissionsExt},
    path::Path,
    process::{Child, Command, Stdio},
    sync::{mpsc, Mutex},
    thread,
    time::{Duration, Instant},
};

use fs2::FileExt;
use sleepy_sdk::{
    validate_event_envelope, EventCauseKind, LifecycleEvent, LifecycleState, OsdKind, SessionEvent,
};
use sleepy_session::osd::OsdPublication;
use sleepy_session::theme_socket::{ThemeMessage, ThemeStatus};

static PROCESS_TEST_SERIAL: Mutex<()> = Mutex::new(());

fn serial_process_test() -> std::sync::MutexGuard<'static, ()> {
    PROCESS_TEST_SERIAL
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
}

#[test]
fn daemon_and_watch_client_replay_a_full_snapshot_and_children_are_reaped() {
    let _serial = serial_process_test();
    let bus = IsolatedBus::start();
    let temp = tempfile::tempdir().unwrap();
    let runtime = temp.path().join("runtime");
    let state = temp.path().join("state");
    std::fs::create_dir_all(&runtime).unwrap();
    std::fs::create_dir_all(&state).unwrap();

    let daemon = Command::new(env!("CARGO_BIN_EXE_sleepy-sessiond"))
        .env("DBUS_SESSION_BUS_ADDRESS", &bus.address)
        .env("XDG_RUNTIME_DIR", &runtime)
        .env("XDG_STATE_HOME", &state)
        .env("XDG_CONFIG_HOME", temp.path().join("config"))
        .env("XDG_CACHE_HOME", temp.path().join("cache"))
        .env("XDG_DATA_HOME", temp.path().join("data"))
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();
    let mut daemon = ChildGuard(Some(daemon));
    let socket = runtime.join("sleepy/session.sock");
    let theme_socket = runtime.join("sleepy/theme.sock");
    wait_for_path(&socket, Duration::from_secs(2));
    wait_for_path(&theme_socket, Duration::from_secs(2));

    let mut theme = std::os::unix::net::UnixStream::connect(&theme_socket).unwrap();
    theme
        .write_all(b"{\"schemaVersion\":2,\"requestId\":\"d78951f8-c6f5-4f7d-8599-d72ed0b34803\",\"operation\":{\"type\":\"get\"}}\n")
        .unwrap();
    let mut theme_response = String::new();
    BufReader::new(theme)
        .read_line(&mut theme_response)
        .unwrap();
    assert!(matches!(
        serde_json::from_str::<ThemeMessage>(&theme_response).unwrap(),
        ThemeMessage::Result {
            status: ThemeStatus::Confirmed,
            theme: Some(theme),
            ..
        } if theme.id == "builtin.sleepy-dark"
    ));

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
    let _serial = serial_process_test();
    let bus = IsolatedBus::start();
    let temp = tempfile::tempdir().unwrap();
    let runtime = temp.path().join("runtime");
    let state = temp.path().join("state");
    std::fs::create_dir_all(&runtime).unwrap();
    std::fs::create_dir_all(&state).unwrap();

    let daemon = Command::new(env!("CARGO_BIN_EXE_sleepy-sessiond"))
        .env("DBUS_SESSION_BUS_ADDRESS", &bus.address)
        .env("XDG_RUNTIME_DIR", &runtime)
        .env("XDG_STATE_HOME", &state)
        .env("XDG_CONFIG_HOME", temp.path().join("config"))
        .env("XDG_CACHE_HOME", temp.path().join("cache"))
        .env("XDG_DATA_HOME", temp.path().join("data"))
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();
    let mut daemon = ChildGuard(Some(daemon));
    let socket = runtime.join("sleepy/session.sock");
    let osd_socket = runtime.join("sleepy/osd.sock");
    let theme_socket = runtime.join("sleepy/theme.sock");
    wait_for_path(&socket, Duration::from_secs(2));
    wait_for_path(&osd_socket, Duration::from_secs(2));
    wait_for_path(&theme_socket, Duration::from_secs(2));

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

    let mut theme = std::os::unix::net::UnixStream::connect(&theme_socket).unwrap();
    theme
        .set_read_timeout(Some(Duration::from_secs(2)))
        .unwrap();
    theme
        .write_all(
            format!(
                "{{\"schemaVersion\":2,\"requestId\":\"d78951f8-c6f5-4f7d-8599-d72ed0b34803\",\"operation\":{{\"type\":\"apply\",\"data\":{{\"themeId\":\"builtin.sleepy-light\",\"expectedGeneration\":{}}}}}}}\n",
                replay.generation
            )
            .as_bytes(),
        )
        .unwrap();
    let mut theme_lines = BufReader::new(theme).lines();
    assert!(matches!(
        serde_json::from_str::<ThemeMessage>(&theme_lines.next().unwrap().unwrap()).unwrap(),
        ThemeMessage::Candidate { .. }
    ));

    let daemon_pid = daemon.0.as_ref().unwrap().id() as libc::pid_t;
    assert_eq!(unsafe { libc::kill(daemon_pid, libc::SIGINT) }, 0);

    let stopping = loop {
        let event = validate_event_envelope(lines.next().unwrap().unwrap().trim()).unwrap();
        if matches!(
            event.payload,
            SessionEvent::Lifecycle(LifecycleEvent {
                state: LifecycleState::Stopping
            })
        ) {
            break event;
        }
    };
    let reconciled = validate_event_envelope(lines.next().unwrap().unwrap().trim()).unwrap();
    assert!(matches!(
        reconciled.payload,
        SessionEvent::Lifecycle(LifecycleEvent {
            state: LifecycleState::Reconciled
        })
    ));
    assert!(stopping.generation > replay.generation);
    assert!(reconciled.generation > stopping.generation);
    assert!(
        lines.next().is_none(),
        "no event may follow lifecycle Reconciled"
    );

    let status = daemon.0.take().unwrap().wait().unwrap();
    assert!(status.success());
    assert!(!socket.exists());
    assert!(!osd_socket.exists());
    assert!(!theme_socket.exists());
    assert!(watcher.wait().unwrap().success());
}

#[test]
fn externally_held_theme_lock_does_not_block_daemon_shutdown() {
    let _serial = serial_process_test();
    let bus = IsolatedBus::start();
    let temp = tempfile::tempdir().unwrap();
    let runtime = temp.path().join("runtime");
    let state = temp.path().join("state");
    std::fs::create_dir_all(&runtime).unwrap();
    std::fs::create_dir_all(&state).unwrap();
    let daemon = Command::new(env!("CARGO_BIN_EXE_sleepy-sessiond"))
        .env("DBUS_SESSION_BUS_ADDRESS", &bus.address)
        .env("XDG_RUNTIME_DIR", &runtime)
        .env("XDG_STATE_HOME", &state)
        .env("XDG_CONFIG_HOME", temp.path().join("config"))
        .env("XDG_CACHE_HOME", temp.path().join("cache"))
        .env("XDG_DATA_HOME", temp.path().join("data"))
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();
    let mut daemon = ChildGuard(Some(daemon));
    let socket = runtime.join("sleepy/theme.sock");
    wait_for_path(&socket, Duration::from_secs(2));
    let lock_path = state.join("sleepy/themes/apply.lock");
    let lock = OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(false)
        .open(&lock_path)
        .unwrap();
    std::fs::set_permissions(&lock_path, std::fs::Permissions::from_mode(0o600)).unwrap();
    lock.lock_exclusive().unwrap();
    let mut theme = std::os::unix::net::UnixStream::connect(&socket).unwrap();
    theme
        .write_all(b"{\"schemaVersion\":2,\"requestId\":\"d78951f8-c6f5-4f7d-8599-d72ed0b34803\",\"operation\":{\"type\":\"delete\",\"data\":{\"themeId\":\"builtin.sleepy-light\"}}}\n")
        .unwrap();
    thread::sleep(Duration::from_millis(40));
    let mut child = daemon.0.take().unwrap();
    assert_eq!(
        unsafe { libc::kill(child.id() as libc::pid_t, libc::SIGINT) },
        0
    );
    let started = Instant::now();
    let status = loop {
        if let Some(status) = child.try_wait().unwrap() {
            break status;
        }
        assert!(started.elapsed() < Duration::from_secs(3));
        thread::sleep(Duration::from_millis(10));
    };
    assert!(status.success());
    assert!(!socket.exists());
}

#[test]
fn daemon_real_sources_reach_the_reconnectable_osd_socket() {
    let _serial = serial_process_test();
    let bus = IsolatedBus::start();
    let temp = tempfile::tempdir().unwrap();
    let runtime = temp.path().join("runtime");
    let state = temp.path().join("state");
    let bin = temp.path().join("bin");
    let marker = temp.path().join("audio-changed");
    let niri_hold = temp.path().join("niri-hold");
    let pw_hold = temp.path().join("pw-hold");
    let pw_count = temp.path().join("pw-count");
    std::fs::create_dir_all(&runtime).unwrap();
    std::fs::create_dir_all(&state).unwrap();
    std::fs::create_dir_all(&bin).unwrap();
    let fifo = CString::new(niri_hold.as_os_str().as_bytes()).unwrap();
    assert_eq!(unsafe { libc::mkfifo(fifo.as_ptr(), 0o600) }, 0);
    let _niri_hold = OpenOptions::new()
        .read(true)
        .write(true)
        .open(&niri_hold)
        .unwrap();
    let fifo = CString::new(pw_hold.as_os_str().as_bytes()).unwrap();
    assert_eq!(unsafe { libc::mkfifo(fifo.as_ptr(), 0o600) }, 0);
    let _pw_hold = OpenOptions::new()
        .read(true)
        .write(true)
        .open(&pw_hold)
        .unwrap();
    write_executable(
        &bin.join("niri"),
        "#!/bin/sh\n[ \"$1|$2|$3|$#\" = 'msg|--json|event-stream|3' ] || exit 64\nprintf '%s\\n' '{\"WorkspacesChanged\":{\"workspaces\":[{\"id\":9,\"output\":\"DP-9\"}]}}'\nprintf '%s\\n' '{\"WorkspaceActivated\":{\"id\":9,\"focused\":true}}'\nIFS= read -r ignored < \"$SLEEPY_NIRI_HOLD\"\n",
    );
    write_executable(
        &bin.join("pw-mon"),
        "#!/bin/sh\n[ \"$#\" -eq 0 ] || exit 64\ncount=0\nif [ -r \"$SLEEPY_PW_COUNT\" ]; then IFS= read -r count < \"$SLEEPY_PW_COUNT\"; fi\ncount=$((count + 1))\nprintf '%s\\n' \"$count\" > \"$SLEEPY_PW_COUNT\"\nif [ \"$count\" -eq 1 ]; then\n  printf '%065537d' 0\nelse\n  : > \"$SLEEPY_FIXTURE_MARKER\"\n  printf '%s\\n' changed\nfi\nIFS= read -r ignored < \"$SLEEPY_PW_HOLD\"\n",
    );
    write_executable(
        &bin.join("wpctl"),
        "#!/bin/sh\ncase \"$1|$2|$3|$#\" in\n  'get-volume|@DEFAULT_AUDIO_SINK@||2') if [ -e \"$SLEEPY_FIXTURE_MARKER\" ]; then level=0.80; else level=0.40; fi; printf 'Volume: %s\\n' \"$level\" ;;\n  'get-volume|@DEFAULT_AUDIO_SOURCE@||2') printf 'Volume: 0.60\\n' ;;\n  'status|--name||2') printf 'Sinks:\\n * 42. Fixture Speaker [vol: 0.80]\\n' ;;\n  *) exit 64 ;;\nesac\n",
    );
    write_executable(
        &bin.join("brightnessctl"),
        "#!/bin/sh\nprintf '%s\\n' 'backlight,intel_backlight,100,50%,50'\n",
    );
    write_executable(
        &bin.join("powerprofilesctl"),
        "#!/bin/sh\ncase \"$1\" in get) printf 'balanced\\n' ;; list) printf '* balanced:\\n  performance:\\n' ;; *) exit 2 ;; esac\n",
    );
    write_executable(
        &bin.join("playerctl"),
        "#!/bin/sh\nprintf 'Paused\\tTrack\\tArtist\\n'\n",
    );

    let path = format!("{}:/usr/bin:/bin", bin.display());
    let daemon = Command::new(env!("CARGO_BIN_EXE_sleepy-sessiond"))
        .env("DBUS_SESSION_BUS_ADDRESS", &bus.address)
        .env("XDG_RUNTIME_DIR", &runtime)
        .env("XDG_STATE_HOME", &state)
        .env("XDG_CONFIG_HOME", temp.path().join("config"))
        .env("XDG_CACHE_HOME", temp.path().join("cache"))
        .env("XDG_DATA_HOME", temp.path().join("data"))
        .env("PATH", path)
        .env("SLEEPY_FIXTURE_MARKER", &marker)
        .env("SLEEPY_NIRI_HOLD", &niri_hold)
        .env("SLEEPY_PW_HOLD", &pw_hold)
        .env("SLEEPY_PW_COUNT", &pw_count)
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();
    let mut daemon = ChildGuard(Some(daemon));
    let socket = runtime.join("sleepy/osd.sock");
    wait_for_path(&socket, Duration::from_secs(2));

    let stream = std::os::unix::net::UnixStream::connect(&socket).unwrap();
    stream
        // Nix checks run all Rust tests under a heavily loaded sandbox. Keep
        // the integration deadline bounded but above the adapter's proven
        // four-second total readback deadline plus one source restart.
        .set_read_timeout(Some(Duration::from_secs(10)))
        .unwrap();
    let mut lines = BufReader::new(stream).lines();
    let publication: OsdPublication = loop {
        let line = lines.next().unwrap().unwrap();
        let publication: OsdPublication = serde_json::from_str(&line).unwrap();
        if publication.visible.iter().any(|event| {
            event.output_id == "DP-9" && event.kind == OsdKind::Volume && event.level == Some(0.8)
        }) {
            break publication;
        }
    };
    assert!(publication.sequence > 0);
    assert!(
        std::fs::read_to_string(&pw_count)
            .unwrap()
            .trim()
            .parse::<u64>()
            .unwrap()
            >= 2
    );

    let daemon_pid = daemon.0.as_ref().unwrap().id() as libc::pid_t;
    assert_eq!(unsafe { libc::kill(daemon_pid, libc::SIGINT) }, 0);
    assert!(daemon.0.take().unwrap().wait().unwrap().success());
}

#[test]
fn malformed_provider_state_degrades_locally_without_blocking_daily_startup() {
    let _serial = serial_process_test();
    use std::io::Write;
    let bus = IsolatedBus::start();
    let temp = tempfile::tempdir().unwrap();
    let runtime = temp.path().join("runtime");
    let state = temp.path().join("state");
    let cache = temp.path().join("cache/sleepy");
    std::fs::create_dir_all(&runtime).unwrap();
    std::fs::create_dir_all(&state).unwrap();
    std::fs::create_dir_all(&cache).unwrap();
    for name in ["met.json", "nominatim.json"] {
        let path = cache.join(name);
        std::fs::write(&path, b"{malformed").unwrap();
        std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o600)).unwrap();
    }
    let daemon = Command::new(env!("CARGO_BIN_EXE_sleepy-sessiond"))
        .env("DBUS_SESSION_BUS_ADDRESS", &bus.address)
        .env("XDG_RUNTIME_DIR", &runtime)
        .env("XDG_STATE_HOME", &state)
        .env("XDG_CONFIG_HOME", temp.path().join("config"))
        .env("XDG_CACHE_HOME", temp.path().join("cache"))
        .env("XDG_DATA_HOME", temp.path().join("data"))
        .env("SLEEPY_CALENDAR_DIR", temp.path().join("missing-calendar"))
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();
    let mut daemon = ChildGuard(Some(daemon));
    let socket = runtime.join("sleepy/daily.sock");
    wait_for_path(&socket, Duration::from_secs(2));
    let mut stream = std::os::unix::net::UnixStream::connect(&socket).unwrap();
    stream
        .set_read_timeout(Some(Duration::from_secs(2)))
        .unwrap();
    stream.write_all(b"{\"schemaVersion\":2,\"requestId\":\"018f3f4c-8af1-7f6b-bf42-1bd472868e65\",\"operation\":{\"type\":\"launcherSearch\",\"data\":{\"query\":\"term\"}}}\n").unwrap();
    let mut line = String::new();
    BufReader::new(stream).read_line(&mut line).unwrap();
    let response: sleepy_session::daily::DailyResponse = serde_json::from_str(&line).unwrap();
    assert!(matches!(
        response.status,
        sleepy_session::daily::DailyStatus::Confirmed
    ));
    let mut calendar = std::os::unix::net::UnixStream::connect(&socket).unwrap();
    calendar
        .set_read_timeout(Some(Duration::from_secs(2)))
        .unwrap();
    calendar.write_all(b"{\"schemaVersion\":2,\"requestId\":\"018f3f4c-8af1-7f6b-bf42-1bd472868e66\",\"operation\":{\"type\":\"calendar\",\"data\":{\"windowStart\":\"2026-08-01T00:00:00Z\",\"windowEnd\":\"2026-09-01T00:00:00Z\"}}}\n").unwrap();
    let mut line = String::new();
    BufReader::new(calendar).read_line(&mut line).unwrap();
    let response: sleepy_session::daily::DailyResponse = serde_json::from_str(&line).unwrap();
    assert!(matches!(
        response.status,
        sleepy_session::daily::DailyStatus::Error
    ));
    assert!(response
        .error
        .unwrap()
        .contains("calendar provider is degraded"));
    daemon.kill_and_wait();
}

struct ChildGuard(Option<std::process::Child>);

struct IsolatedBus {
    address: String,
    child: Child,
}

impl IsolatedBus {
    fn start() -> Self {
        let mut command = Command::new("dbus-daemon");
        if let Some(config) = std::env::var_os("SLEEPY_DBUS_SESSION_CONF") {
            command.arg("--config-file").arg(config);
        } else {
            command.arg("--session");
        }
        let mut child = command
            .args(["--nofork", "--nopidfile", "--print-address=1"])
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .unwrap();
        let address = match BufReader::new(child.stdout.take().unwrap()).lines().next() {
            Some(Ok(address)) if !address.is_empty() => address,
            result => {
                let mut stderr = String::new();
                child
                    .stderr
                    .take()
                    .unwrap()
                    .read_to_string(&mut stderr)
                    .unwrap();
                let status = child.wait().unwrap();
                panic!("dbus-daemon did not publish an address ({result:?}, {status}): {stderr}");
            }
        };
        Self { address, child }
    }
}

impl Drop for IsolatedBus {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

fn write_executable(path: &Path, contents: &str) {
    std::fs::write(path, contents).unwrap();
    std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o700)).unwrap();
}

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
