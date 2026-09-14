use serde_json::{json, Value};
use sleepy_session::doctor;
use std::{path::PathBuf, time::Duration};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::UnixListener,
};

fn fixture() -> Value {
    serde_json::from_str(include_str!("fixtures/doctor/full-snapshot.json")).unwrap()
}

async fn receive(bytes: Vec<u8>) -> doctor::Report {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("desktop.sock");
    let listener = UnixListener::bind(&path).unwrap();
    let server = tokio::spawn(async move {
        let (mut peer, _) = listener.accept().await.unwrap();
        let _ = peer.write_all(&bytes).await;
        peer.shutdown().await.unwrap();
        let mut incoming = Vec::new();
        peer.read_to_end(&mut incoming).await.unwrap();
        assert!(
            incoming.is_empty(),
            "doctor must never send a mutation or request"
        );
    });
    let report = doctor::inspect_socket(&path, Duration::from_secs(2)).await;
    server.await.unwrap();
    report
}

#[tokio::test]
async fn real_socket_snapshot_is_validated_and_private_values_never_leave_report() {
    let mut value = fixture();
    value["payload"]["data"]["system"]["network"]["data"]["accessPoints"][0]["ssid"] =
        json!("PRIVATE-SSID");
    value["payload"]["data"]["compositor"]["hyprland"]["data"]["windows"][0]["title"] =
        json!("PRIVATE-WINDOW");
    value["payload"]["data"]["system"]["bluetooth"] =
        json!({"status":"unsupported","diagnostic":{"message":"PRIVATE-MAC AA:BB:CC:DD:EE:FF"}});
    let report = receive(format!("{value}\n").into_bytes()).await;
    let output = serde_json::to_string(&report).unwrap() + &report.human();
    assert!(!output.contains("PRIVATE"));
    assert!(!output.contains("AA:BB"));
    assert!(!output.contains("accessPoints"));
    assert!(report.ok);
    assert!(output.contains("bluetooth"));
    assert!(output.contains("unsupported"));
    assert_eq!(report.generation, Some(7));
}

#[tokio::test]
async fn optional_absence_is_informational_but_core_and_probe_errors_fail() {
    for (domain, status, expected) in [
        ("battery", "unavailable", true),
        ("brightness", "unsupported", true),
        ("lock", "unavailable", false),
        ("audio", "timeout", false),
    ] {
        let mut value = fixture();
        value["payload"]["data"]["system"][domain] =
            json!({"status":status,"diagnostic":{"message":"PRIVATE-details"}});
        let report = receive(format!("{value}\n").into_bytes()).await;
        assert_eq!(report.ok, expected, "{domain} {status}: {}", report.human());
        assert!(!report.human().contains("PRIVATE"));
    }
}

#[tokio::test]
async fn malformed_wrong_version_truncated_and_oversized_frames_fail_closed() {
    let mut wrong = fixture();
    wrong["schemaVersion"] = json!(99);
    for bytes in [
        b"not-json\n".to_vec(),
        format!("{wrong}\n").into_bytes(),
        serde_json::to_vec(&fixture()).unwrap(),
        vec![b'x'; doctor::MAX_FRAME_BYTES + 1],
    ] {
        let report = receive(bytes).await;
        assert!(!report.ok);
        assert!(report.error.is_some());
        assert!(!report.human().contains("not-json"));
    }
}

#[tokio::test]
async fn slow_byte_trickle_cannot_reset_the_total_deadline() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("desktop.sock");
    let listener = UnixListener::bind(&path).unwrap();
    let server = tokio::spawn(async move {
        let (mut peer, _) = listener.accept().await.unwrap();
        loop {
            if peer.write_all(b" ").await.is_err() {
                break;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    });
    let report = doctor::inspect_socket(&path, Duration::from_millis(40)).await;
    assert_eq!(report.error.as_deref(), Some("timeout"));
    server.abort();
    let _ = server.await;
}

#[tokio::test]
async fn missing_socket_is_an_actionable_report_not_a_panic() {
    let report = doctor::inspect_socket(
        &PathBuf::from("/missing/sleepy-doctor.sock"),
        Duration::from_secs(2),
    )
    .await;
    assert!(!report.ok);
    assert_eq!(report.error.as_deref(), Some("session-unavailable"));
    assert!(report.human().contains("Sleepy desktop"));
}

#[test]
fn cli_human_json_and_exit_codes_use_only_the_existing_session_socket() {
    use std::{
        io::{Read, Write},
        os::unix::net::UnixListener,
        process::Command,
    };
    for json_output in [false, true] {
        let directory = tempfile::tempdir().unwrap();
        std::fs::create_dir(directory.path().join("sleepy")).unwrap();
        let listener = UnixListener::bind(directory.path().join("sleepy/desktop.sock")).unwrap();
        let server = std::thread::spawn(move || {
            let (mut peer, _) = listener.accept().unwrap();
            peer.set_read_timeout(Some(Duration::from_secs(2))).unwrap();
            writeln!(peer, "{}", fixture()).unwrap();
            let mut input = Vec::new();
            peer.read_to_end(&mut input).unwrap();
            assert!(input.is_empty());
        });
        let mut command = Command::new(env!("CARGO_BIN_EXE_sleepyctl"));
        command
            .arg("doctor")
            .env("XDG_RUNTIME_DIR", directory.path())
            .env("PATH", "");
        if json_output {
            command.arg("--json");
        }
        let output = command.output().unwrap();
        server.join().unwrap();
        assert!(output.status.success());
        assert!(output.stderr.is_empty());
        if json_output {
            let value: Value = serde_json::from_slice(&output.stdout).unwrap();
            assert_eq!(value["ok"], true);
            assert_eq!(value["generation"], 7);
        } else {
            assert!(String::from_utf8(output.stdout)
                .unwrap()
                .starts_with("Sleepy desktop: ready"));
        }
    }
    let output = Command::new(env!("CARGO_BIN_EXE_sleepyctl"))
        .args(["doctor", "--repair"])
        .env_remove("XDG_RUNTIME_DIR")
        .output()
        .unwrap();
    assert_eq!(output.status.code(), Some(2));
    let output = Command::new(env!("CARGO_BIN_EXE_sleepyctl"))
        .args(["doctor", "--json"])
        .env_remove("XDG_RUNTIME_DIR")
        .output()
        .unwrap();
    assert_eq!(output.status.code(), Some(1));
    let value: Value = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(value["error"], "runtime-unavailable");
}
