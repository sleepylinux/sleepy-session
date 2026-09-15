use serde_json::{json, Value};
use sleepy_session::capture::CaptureService;
use std::{fs, os::unix::fs::PermissionsExt, path::Path, time::Duration};
const ID: &str = "12345678-1234-4234-8234-123456789abc";
fn helper(dir: &Path, body: &str) -> std::path::PathBuf {
    let path = dir.join("helper");
    fs::write(&path, format!("#!/bin/sh\n{body}\n")).unwrap();
    fs::set_permissions(&path, fs::Permissions::from_mode(0o700)).unwrap();
    path
}
async fn request(service: &CaptureService, command: Value) -> Value {
    serde_json::from_str(
        &service
            .handle_json(&json!({"schemaVersion":1,"command":command}).to_string())
            .await
            .unwrap(),
    )
    .unwrap()
}
async fn begin(service: &CaptureService) -> Value {
    request(
        service,
        json!({"type":"begin","jobId":ID,"outputId":"output:DP-1"}),
    )
    .await
}
async fn status(service: &CaptureService) -> Value {
    request(service, json!({"type":"status","jobId":ID})).await
}
async fn terminal(service: &CaptureService) -> Value {
    tokio::time::timeout(Duration::from_secs(4), async {
        loop {
            let r = status(service).await;
            if matches!(
                r["payload"]["job"]["state"].as_str(),
                Some("completed" | "cancelled" | "failed")
            ) {
                return r;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap()
}
#[tokio::test]
async fn consent_wait_does_not_block_requests_and_cancel_reaps_owned_child() {
    let temp = tempfile::tempdir().unwrap();
    let h = helper(
        temp.path(),
        "echo '{\"state\":\"awaitingConsent\"}'; exec sleep 30",
    );
    let service = CaptureService::open(
        temp.path().join("captures"),
        Some(h),
        Duration::from_secs(120),
    )
    .unwrap();
    assert_eq!(
        begin(&service).await["payload"]["job"]["state"],
        "awaitingConsent"
    );
    tokio::time::sleep(Duration::from_millis(950)).await;
    tokio::time::timeout(Duration::from_millis(200),async {
        assert_eq!(status(&service).await["payload"]["job"]["state"],"awaitingConsent");
        assert_eq!(begin(&service).await["payload"]["job"]["jobId"],ID);
        assert_eq!(request(&service,json!({"type":"begin","jobId":"22345678-1234-4234-8234-123456789abc","outputId":"output:DP-1"})).await["payload"]["diagnostic"]["code"],"busy");
        request(&service,json!({"type":"cancel","jobId":ID})).await;
    }).await.unwrap();
    assert_eq!(
        terminal(&service).await["payload"]["job"]["state"],
        "cancelled"
    );
    service.shutdown().await;
    assert!(!temp
        .path()
        .join(format!("captures/screenshot-{ID}.png"))
        .exists());
}
#[tokio::test]
async fn completed_requires_real_png_and_successful_helper_exit() {
    let temp = tempfile::tempdir().unwrap();
    let h = helper(temp.path(), "echo '{\"state\":\"completed\"}'; exit 0");
    let service = CaptureService::open(
        temp.path().join("captures"),
        Some(h),
        Duration::from_secs(120),
    )
    .unwrap();
    begin(&service).await;
    assert_eq!(
        terminal(&service).await["payload"]["job"]["state"],
        "failed"
    );
    service.shutdown().await;
}
#[tokio::test]
async fn helper_crash_timeout_and_disconnected_output_are_terminal_failures() {
    for (body,expected) in [("exit 17","captureFailed"),("exec sleep 30","consentTimedOut"),("echo '{\"state\":\"failed\",\"diagnostic\":{\"code\":\"outputUnavailable\",\"message\":\"Output disconnected\"}}'; exit 1","outputUnavailable")] {
        let temp=tempfile::tempdir().unwrap(); let h=helper(temp.path(),body);
        let service=CaptureService::open(temp.path().join("captures"),Some(h),Duration::from_millis(100)).unwrap();
        begin(&service).await; let r=terminal(&service).await;
        assert_eq!(r["payload"]["job"]["state"],"failed"); assert_eq!(r["payload"]["job"]["diagnostic"]["code"],expected);
        service.shutdown().await;
    }
}
fn png_file(path: &Path) {
    let mut encoder = png::Encoder::new(fs::File::create(path).unwrap(), 2, 3);
    encoder.set_color(png::ColorType::Rgba);
    encoder.set_depth(png::BitDepth::Eight);
    encoder
        .write_header()
        .unwrap()
        .write_image_data(&[128; 24])
        .unwrap();
    fs::set_permissions(path, fs::Permissions::from_mode(0o600)).unwrap();
}
#[tokio::test]
async fn real_png_is_completed_with_dimensions_and_existing_file_is_never_overwritten() {
    let t = tempfile::tempdir().unwrap();
    let image = t.path().join("source.png");
    png_file(&image);
    let h = helper(
        t.path(),
        &format!(
            "echo '{{\"state\":\"capturing\"}}'; cat '{}' >&4; echo '{{\"state\":\"completed\"}}'",
            image.display()
        ),
    );
    let directory = t.path().join("captures");
    let service = CaptureService::open(directory.clone(), Some(h), Duration::from_secs(2)).unwrap();
    begin(&service).await;
    let r = terminal(&service).await;
    assert_eq!(r["payload"]["job"]["state"], "completed");
    assert_eq!(r["payload"]["job"]["result"]["width"], 2);
    assert_eq!(r["payload"]["job"]["result"]["height"], 3);
    assert_eq!(begin(&service).await, r);
    assert_eq!(
        request(
            &service,
            json!({"type":"begin","jobId":ID,"outputId":"output:DP-2"})
        )
        .await["payload"]["diagnostic"]["code"],
        "invalidRequest"
    );
    let second = "22345678-1234-4234-8234-123456789abc";
    let preexisting = directory.join(format!("screenshot-{second}.png"));
    fs::write(&preexisting, b"sentinel").unwrap();
    assert_eq!(
        request(
            &service,
            json!({"type":"begin","jobId":second,"outputId":"output:DP-1"})
        )
        .await["payload"]["diagnostic"]["code"],
        "invalidRequest"
    );
    assert_eq!(fs::read(preexisting).unwrap(), b"sentinel");
    service.shutdown().await;
}
#[tokio::test]
async fn symlink_invalid_permissions_and_truncated_png_are_not_results() {
    for mode in ["symlink", "public", "truncated"] {
        let t = tempfile::tempdir().unwrap();
        let image = t.path().join("source.png");
        png_file(&image);
        let original = fs::read(&image).unwrap();
        let body = match mode {
            "symlink" => format!(
                "ln -s '{}' \"$6\"; cat '{}' >&4",
                image.display(),
                image.display()
            ),
            "public" => format!("cat '{}' >&4; chmod 644 /proc/self/fd/4", image.display()),
            _ => "printf '\\211PNG\\r\\n\\032\\n' >&4".into(),
        };
        let h = helper(
            t.path(),
            &format!("{body}; echo '{{\"state\":\"completed\"}}'"),
        );
        let service =
            CaptureService::open(t.path().join("captures"), Some(h), Duration::from_secs(2))
                .unwrap();
        begin(&service).await;
        assert_eq!(
            terminal(&service).await["payload"]["job"]["state"],
            "failed"
        );
        assert_eq!(fs::read(image).unwrap(), original);
        assert_eq!(
            t.path()
                .join(format!("captures/screenshot-{ID}.png"))
                .symlink_metadata()
                .is_ok(),
            mode == "symlink"
        );
        service.shutdown().await;
    }
}
#[tokio::test]
async fn shutdown_reaps_term_ignoring_helper_without_touching_unrelated_child() {
    let t = tempfile::tempdir().unwrap();
    let pidfile = t.path().join("pid");
    let h=helper(t.path(),&format!("trap '' TERM; printf '%s\\n' $$ > '{}'; echo '{{\"state\":\"awaitingConsent\"}}'; while :; do sleep 1; done",pidfile.display()));
    let service =
        CaptureService::open(t.path().join("captures"), Some(h), Duration::from_secs(120)).unwrap();
    begin(&service).await;
    let pid = tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            if let Ok(s) = fs::read_to_string(&pidfile) {
                if s.ends_with('\n') {
                    if let Ok(p) = s.trim().parse::<i32>() {
                        break p;
                    }
                }
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .unwrap();
    let mut unrelated = std::process::Command::new("sleep")
        .arg("30")
        .spawn()
        .unwrap();
    tokio::time::timeout(Duration::from_secs(2), service.shutdown())
        .await
        .unwrap();
    assert_eq!(unsafe { libc::kill(pid, 0) }, -1);
    assert_eq!(
        std::io::Error::last_os_error().raw_os_error(),
        Some(libc::ESRCH)
    );
    assert!(unrelated.try_wait().unwrap().is_none());
    unrelated.kill().unwrap();
    unrelated.wait().unwrap();
}
#[tokio::test]
async fn malformed_requests_and_missing_helper_do_not_start_jobs() {
    let t = tempfile::tempdir().unwrap();
    let service =
        CaptureService::open(t.path().join("captures"), None, Duration::from_secs(120)).unwrap();
    assert_eq!(
        request(&service, json!({"type":"capabilities"})).await["payload"]["screenshot"],
        false
    );
    assert_eq!(
        begin(&service).await["payload"]["diagnostic"]["code"],
        "unavailable"
    );
    for command in [
        json!({"type":"begin","jobId":"../bad","outputId":"output:DP-1"}),
        json!({"type":"begin","jobId":ID,"outputId":"output:DP-1;bad"}),
        json!({"type":"status","jobId":ID,"extra":true}),
    ] {
        assert_eq!(
            request(&service, command).await["payload"]["diagnostic"]["code"],
            "invalidRequest"
        );
    }
    service.shutdown().await;
}
#[tokio::test]
async fn terminal_history_evicts_only_unchanged_owned_result() {
    let t = tempfile::tempdir().unwrap();
    let image = t.path().join("source.png");
    png_file(&image);
    let h = helper(
        t.path(),
        &format!(
            "cat '{}' >&4; echo '{{\"state\":\"completed\"}}'",
            image.display()
        ),
    );
    let directory = t.path().join("captures");
    let service = CaptureService::open(directory.clone(), Some(h), Duration::from_secs(2)).unwrap();
    for n in 0..18 {
        let id = format!("{n:08x}-1234-4234-8234-123456789abc");
        request(
            &service,
            json!({"type":"begin","jobId":id,"outputId":"output:DP-1"}),
        )
        .await;
        tokio::time::timeout(Duration::from_secs(2), async {
            loop {
                let r = request(&service, json!({"type":"status","jobId":id})).await;
                if r["payload"]["job"]["state"] == "completed" {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        })
        .await
        .unwrap();
        if n == 0 {
            let p = directory.join(format!("screenshot-{id}.png"));
            fs::rename(&p, directory.join("original-first.png")).unwrap();
            fs::write(p, b"replacement survives").unwrap();
        }
    }
    assert_eq!(
        request(
            &service,
            json!({"type":"status","jobId":"00000000-1234-4234-8234-123456789abc"})
        )
        .await["payload"]["diagnostic"]["code"],
        "notFound"
    );
    assert_eq!(
        fs::read(directory.join("screenshot-00000000-1234-4234-8234-123456789abc.png")).unwrap(),
        b"replacement survives"
    );
    assert!(!directory
        .join("screenshot-00000001-1234-4234-8234-123456789abc.png")
        .exists());
    service.shutdown().await;
}
#[tokio::test]
async fn real_socket_disconnect_fragmentation_and_client_schema_checks() {
    use sleepy_session::capture::{client_request, CaptureSocket};
    use std::sync::Arc;
    use tokio::io::AsyncWriteExt;
    let t = tempfile::tempdir().unwrap();
    let h = helper(
        t.path(),
        "echo '{\"state\":\"awaitingConsent\"}'; exec sleep 30",
    );
    let service =
        CaptureService::open(t.path().join("captures"), Some(h), Duration::from_secs(120)).unwrap();
    let path = t.path().join("capture.sock");
    let socket = Arc::new(CaptureSocket::bind(&path, service.clone()).await.unwrap());
    assert_eq!(
        fs::metadata(&path).unwrap().permissions().mode() & 0o777,
        0o600
    );
    let serving = socket.clone();
    let task = tokio::spawn(async move { serving.serve().await.unwrap() });
    let mut stream = tokio::net::UnixStream::connect(&path).await.unwrap();
    let input =
        json!({"schemaVersion":1,"command":{"type":"begin","jobId":ID,"outputId":"output:DP-1"}})
            .to_string();
    stream.write_all(&input.as_bytes()[..15]).await.unwrap();
    stream.write_all(&input.as_bytes()[15..]).await.unwrap();
    stream.write_all(b"\n").await.unwrap();
    drop(stream);
    let status_input =
        json!({"schemaVersion":1,"command":{"type":"status","jobId":ID}}).to_string();
    tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            let r: Value =
                serde_json::from_str(&client_request(&path, &status_input).await.unwrap()).unwrap();
            if r["payload"]["job"]["state"] == "awaitingConsent" {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    tokio::time::sleep(Duration::from_millis(950)).await;
    let caps = json!({"schemaVersion":1,"command":{"type":"capabilities"}}).to_string();
    tokio::time::timeout(Duration::from_millis(200), client_request(&path, &caps))
        .await
        .unwrap()
        .unwrap();
    assert!(client_request(&path, r#"{"schemaVersion":3}"#)
        .await
        .is_err());
    socket.shutdown().await.unwrap();
    task.await.unwrap();
}
#[tokio::test]
async fn cli_transport_rejects_invalid_or_oversized_reply() {
    use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
    for payload in [
        "{\"schemaVersion\":3}\n".to_owned(),
        "x".repeat(5000) + "\n",
    ] {
        let t = tempfile::tempdir().unwrap();
        let path = t.path().join("socket");
        let listener = tokio::net::UnixListener::bind(&path).unwrap();
        let server = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            let mut request = String::new();
            BufReader::new(&mut stream)
                .read_line(&mut request)
                .await
                .unwrap();
            let _ = stream.write_all(payload.as_bytes()).await;
        });
        assert!(sleepy_session::capture::client_request(
            &path,
            r#"{"schemaVersion":1,"command":{"type":"capabilities"}}"#
        )
        .await
        .is_err());
        server.await.unwrap();
    }
}
#[tokio::test]
async fn consent_wait_keeps_existing_mutation_pipeline_responsive() {
    use sleepy_sdk::{DaemonCommand, RuntimeSnapshot};
    use sleepy_session::sessiond::{
        full_snapshot_event, initial_snapshot, EventHub, GenerationAllocator, GenerationAuthority,
        MutationBackend, MutationPipeline,
    };
    use std::{future::Future, io, pin::Pin, sync::Arc};
    struct Backend;
    impl MutationBackend for Backend {
        fn execute<'a>(
            &'a self,
            _: &'a DaemonCommand,
        ) -> Pin<Box<dyn Future<Output = io::Result<()>> + Send + 'a>> {
            Box::pin(async { Ok(()) })
        }
        fn readback(
            &self,
        ) -> Pin<Box<dyn Future<Output = io::Result<RuntimeSnapshot>> + Send + '_>> {
            Box::pin(async { Ok(initial_snapshot()) })
        }
        fn confirms(&self, _: &DaemonCommand, _: &RuntimeSnapshot) -> bool {
            true
        }
    }
    let t = tempfile::tempdir().unwrap();
    let h = helper(
        t.path(),
        "echo '{\"state\":\"awaitingConsent\"}'; exec sleep 30",
    );
    let service =
        CaptureService::open(t.path().join("captures"), Some(h), Duration::from_secs(120)).unwrap();
    begin(&service).await;
    let mut allocator = GenerationAllocator::open(t.path().join("generation"), 16).unwrap();
    let generation = allocator.next_generation().unwrap();
    let hub = EventHub::new(full_snapshot_event(generation).unwrap(), 16);
    let authority = GenerationAuthority::new(allocator, generation, hub);
    let pipeline = MutationPipeline::new(authority, Arc::new(Backend));
    tokio::time::sleep(Duration::from_millis(950)).await;
    let input=json!({"schemaVersion":2,"requestId":ID,"expectedGeneration":generation,"command":{"type":"setDnd","data":{"enabled":true}}}).to_string();
    let reply = tokio::time::timeout(Duration::from_millis(200), pipeline.handle_json(&input))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(serde_json::to_value(reply).unwrap()["status"], "confirmed");
    assert_eq!(
        status(&service).await["payload"]["job"]["state"],
        "awaitingConsent"
    );
    service.shutdown().await;
}
#[tokio::test]
async fn cancellation_never_unlinks_unowned_replacement() {
    let t = tempfile::tempdir().unwrap();
    let h = helper(
        t.path(),
        "echo '{\"state\":\"awaitingConsent\"}'; exec sleep 30",
    );
    let directory = t.path().join("captures");
    let service =
        CaptureService::open(directory.clone(), Some(h), Duration::from_secs(120)).unwrap();
    begin(&service).await;
    let path = directory.join(format!("screenshot-{ID}.png"));
    fs::write(&path, b"replacement").unwrap();
    request(&service, json!({"type":"cancel","jobId":ID})).await;
    assert_eq!(
        terminal(&service).await["payload"]["job"]["state"],
        "cancelled"
    );
    assert_eq!(fs::read(path).unwrap(), b"replacement");
    service.shutdown().await;
}
#[tokio::test]
async fn cancellation_kills_descendant_even_when_leader_accepts_term() {
    let t = tempfile::tempdir().unwrap();
    let pidfile = t.path().join("descendant.pid");
    let h=helper(t.path(),&format!("sh -c 'trap \"\" TERM; printf \"%s\\n\" $$ > \"$1\"; exec sleep 30' sh '{}' &\necho '{{\"state\":\"awaitingConsent\"}}'; wait",pidfile.display()));
    let service =
        CaptureService::open(t.path().join("captures"), Some(h), Duration::from_secs(120)).unwrap();
    begin(&service).await;
    let pid = tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            if let Ok(s) = fs::read_to_string(&pidfile) {
                if s.ends_with('\n') {
                    if let Ok(p) = s.trim().parse::<i32>() {
                        break p;
                    }
                }
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .unwrap();
    request(&service, json!({"type":"cancel","jobId":ID})).await;
    terminal(&service).await;
    // A killed adopted descendant may remain a zombie until its init reaps it.
    let dead = || {
        fs::read_to_string(format!("/proc/{pid}/stat"))
            .map(|s| s.split(") ").nth(1).unwrap().starts_with('Z'))
            .unwrap_or(true)
    };
    let result = tokio::time::timeout(Duration::from_secs(1), async {
        while !dead() {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await;
    if result.is_err() {
        unsafe {
            libc::kill(pid, libc::SIGKILL);
        }
    }
    assert!(
        result.is_ok(),
        "Owned TERM-ignoring descendant survived cancellation"
    );
    service.shutdown().await;
}
#[tokio::test]
async fn partial_memfd_on_cancel_and_valid_png_on_failed_exit_leave_no_result() {
    for fail in [false, true] {
        let t = tempfile::tempdir().unwrap();
        let source = t.path().join("source.png");
        png_file(&source);
        let body = if fail {
            format!(
                "cat '{}' >&4; echo '{{\"state\":\"completed\"}}'; exit 19",
                source.display()
            )
        } else {
            "printf 'partial PNG' >&4; echo '{\"state\":\"capturing\"}'; exec sleep 30".into()
        };
        let h = helper(t.path(), &body);
        let directory = t.path().join("captures");
        let service =
            CaptureService::open(directory.clone(), Some(h), Duration::from_secs(120)).unwrap();
        begin(&service).await;
        if !fail {
            tokio::time::timeout(Duration::from_secs(2), async {
                while status(&service).await["payload"]["job"]["state"] != "capturing" {
                    tokio::time::sleep(Duration::from_millis(5)).await;
                }
            })
            .await
            .unwrap();
            request(&service, json!({"type":"cancel","jobId":ID})).await;
        }
        assert_eq!(
            terminal(&service).await["payload"]["job"]["state"],
            if fail { "failed" } else { "cancelled" }
        );
        assert_eq!(fs::read_dir(directory).unwrap().count(), 0);
        service.shutdown().await;
    }
}
