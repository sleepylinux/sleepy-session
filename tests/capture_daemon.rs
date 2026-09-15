//! Explicit Linux user/mount namespace gate: no host runtime directory is changed.
use serde_json::{json, Value};
use std::{
    fs,
    io::{BufRead, BufReader},
    os::unix::fs::PermissionsExt,
    path::Path,
    process::{Child, Command, Stdio},
    time::{Duration, Instant},
};
struct ChildGuard(Child);
impl Drop for ChildGuard {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}
fn wait_for(mut condition: impl FnMut() -> bool) {
    let start = Instant::now();
    while !condition() {
        assert!(
            start.elapsed() < Duration::from_secs(5),
            "readiness deadline"
        );
        std::thread::sleep(Duration::from_millis(10));
    }
}
#[test]
#[ignore = "explicit gate requires Linux unprivileged user and mount namespaces"]
fn actual_daemon_sigterm_cancels_and_reaps_capture_helper() {
    if std::env::var_os("SLEEPY_CAPTURE_NAMESPACE_TEST").is_none() {
        let status=Command::new("unshare").args(["--user","--map-root-user","--mount",
                "--propagation",
                "private","sh","-c","mount -t tmpfs -o mode=755 tmpfs /run && mkdir -p /run/user/0 && chmod 700 /run/user/0 && exec \"$@\"","sh"])
            .arg(std::env::current_exe().unwrap()).args(["--ignored","--exact","actual_daemon_sigterm_cancels_and_reaps_capture_helper","--nocapture"])
            .env("SLEEPY_CAPTURE_NAMESPACE_TEST","1").status().unwrap();
        assert!(status.success());
        return;
    }
    assert_eq!(unsafe { libc::geteuid() }, 0);
    let temp = tempfile::tempdir().unwrap();
    let bin = temp.path().join("bin");
    fs::create_dir(&bin).unwrap();
    let pidfile = temp.path().join("helper.pid");
    let helper = bin.join("sleepy-capture-job-helper");
    fs::write(&helper,format!("#!/bin/sh\ntrap '' TERM\nprintf '%s\\n' $$ > '{}'\necho '{{\"state\":\"awaitingConsent\"}}'\nwhile :; do sleep 1; done\n",pidfile.display())).unwrap();
    fs::set_permissions(&helper, fs::Permissions::from_mode(0o700)).unwrap();
    let mut bus = ChildGuard(
        Command::new("dbus-daemon")
            .env("XDG_RUNTIME_DIR", "/run/user/0")
            .args(["--session", "--nofork", "--nopidfile", "--print-address=1"])
            .stdout(Stdio::piped())
            .spawn()
            .unwrap(),
    );
    let mut address = String::new();
    BufReader::new(bus.0.stdout.take().unwrap())
        .read_line(&mut address)
        .unwrap();
    assert!(!address.trim().is_empty());
    let stderr = fs::File::create(temp.path().join("daemon.log")).unwrap();
    let mut daemon = ChildGuard(
        Command::new(env!("CARGO_BIN_EXE_sleepy-sessiond"))
            .env_remove("NOTIFY_SOCKET")
            .env_remove("HYPRLAND_INSTANCE_SIGNATURE")
            .env("DBUS_SESSION_BUS_ADDRESS", address.trim())
            .env("DBUS_SYSTEM_BUS_ADDRESS", address.trim())
            .env("XDG_RUNTIME_DIR", "/run/user/0")
            .env("XDG_STATE_HOME", temp.path().join("state"))
            .env("XDG_CONFIG_HOME", temp.path().join("config"))
            .env("XDG_CACHE_HOME", temp.path().join("cache"))
            .env("XDG_DATA_HOME", temp.path().join("data"))
            .env("SLEEPY_CAPTURE_ENABLE", "1")
            .env(
                "PATH",
                format!("{}:{}", bin.display(), std::env::var("PATH").unwrap()),
            )
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(stderr)
            .spawn()
            .unwrap(),
    );
    wait_for(|| {
        Path::new("/run/user/0/sleepy/capture.sock").exists()
            || daemon.0.try_wait().unwrap().is_some()
    });
    assert!(
        daemon.0.try_wait().unwrap().is_none(),
        "{}",
        fs::read_to_string(temp.path().join("daemon.log")).unwrap()
    );
    let request = |command: Value| {
        let output = Command::new(env!("CARGO_BIN_EXE_sleepyctl"))
            .args([
                "capture",
                "request",
                &json!({"schemaVersion":1,"command":command}).to_string(),
            ])
            .env("XDG_RUNTIME_DIR", "/run/user/0")
            .output()
            .unwrap();
        assert!(
            output.status.success(),
            "{}",
            String::from_utf8_lossy(&output.stderr)
        );
        serde_json::from_slice::<Value>(&output.stdout).unwrap()
    };
    let id = "12345678-1234-4234-8234-123456789abc";
    assert_eq!(
        request(json!({"type":"begin","jobId":id,"outputId":"output:DP-1"}))["payload"]["job"]
            ["state"],
        "awaitingConsent"
    );
    let mut pid = 0;
    wait_for(|| {
        pid = fs::read_to_string(&pidfile)
            .ok()
            .filter(|s| s.ends_with('\n'))
            .and_then(|s| s.trim().parse().ok())
            .unwrap_or(0);
        pid > 0
    });
    std::thread::sleep(Duration::from_millis(950));
    let before = Instant::now();
    assert_eq!(
        request(json!({"type":"status","jobId":id}))["payload"]["job"]["state"],
        "awaitingConsent"
    );
    assert!(before.elapsed() < Duration::from_millis(900));
    assert_eq!(
        unsafe { libc::kill(daemon.0.id() as i32, libc::SIGTERM) },
        0
    );
    wait_for(|| daemon.0.try_wait().unwrap().is_some());
    assert!(
        daemon.0.wait().unwrap().success(),
        "{}",
        fs::read_to_string(temp.path().join("daemon.log")).unwrap()
    );
    assert_eq!(unsafe { libc::kill(pid, 0) }, -1);
    assert_eq!(
        std::io::Error::last_os_error().raw_os_error(),
        Some(libc::ESRCH)
    );
    assert!(!Path::new("/run/user/0/sleepy/capture.sock").exists());
    assert!(!Path::new(&format!("/run/user/0/sleepy/captures/screenshot-{id}.png")).exists());
}
