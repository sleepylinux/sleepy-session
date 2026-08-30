use std::{path::Path, time::Duration};

use sleepy_sdk::{HyprlandCommand, HyprlandSnapshot, StableId};
use sleepy_session::compositor::{
    parse_event_line, parse_full_snapshot, AdapterTiming, CompositorErrorKind, CompositorExecution,
    EventDisposition, HyprlandAdapter, HyprlandEvent, HyprlandPaths, MAX_COMMAND_RESPONSE_BYTES,
    MAX_EVENT_LINE_BYTES, MAX_INSTANCE_SIGNATURE_BYTES,
};
use sleepy_session::sessiond::{
    full_snapshot_event, EventHub, HyprlandSource, ProductionMutationBackend,
};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::UnixListener,
};
use tokio_util::sync::CancellationToken;

const MONITORS: &str = include_str!("fixtures/hyprland/monitors.json");
const WORKSPACES: &str = include_str!("fixtures/hyprland/workspaces.json");
const CLIENTS: &str = include_str!("fixtures/hyprland/clients.json");

fn parse_fixture() -> HyprlandSnapshot {
    parse_full_snapshot(
        MONITORS.as_bytes(),
        WORKSPACES.as_bytes(),
        CLIENTS.as_bytes(),
    )
    .expect("Hyprland fixture must map to the closed SDK snapshot")
}

fn test_timing() -> AdapterTiming {
    AdapterTiming {
        operation_timeout: Duration::from_millis(250),
        confirmation_poll: Duration::from_millis(5),
        reconnect_delay: Duration::from_millis(5),
        fallback_reconcile: Duration::from_secs(30),
    }
}

async fn command_fixture() -> (tempfile::TempDir, HyprlandPaths, UnixListener) {
    let directory = tempfile::tempdir().unwrap();
    let paths = HyprlandPaths::from_runtime_dir_and_signature(directory.path(), "fixture-instance")
        .unwrap();
    std::fs::create_dir_all(paths.command_socket().parent().unwrap()).unwrap();
    let listener = UnixListener::bind(paths.command_socket()).unwrap();
    (directory, paths, listener)
}

async fn serve_script(
    listener: UnixListener,
    script: Vec<(&'static [u8], Vec<u8>)>,
) -> Vec<Vec<u8>> {
    let mut requests = Vec::new();
    for (expected, response) in script {
        let (mut stream, _) = listener.accept().await.unwrap();
        let mut request = Vec::new();
        stream.read_to_end(&mut request).await.unwrap();
        assert_eq!(request, expected);
        requests.push(request);
        let _ = stream.write_all(&response).await;
    }
    requests
}

async fn serve_event_connections(listener: UnixListener, connections: Vec<Vec<Vec<u8>>>) {
    for payloads in connections {
        let (mut stream, _) = listener.accept().await.unwrap();
        for payload in payloads {
            let _ = stream.write_all(&payload).await;
        }
    }
}

async fn execute_case(
    command: HyprlandCommand,
    dispatches: Vec<&'static [u8]>,
    post_monitors: Vec<u8>,
    post_workspaces: Vec<u8>,
    post_clients: Vec<u8>,
) -> CompositorExecution {
    let (_directory, paths, listener) = command_fixture().await;
    let mut script = vec![
        (b"j/monitors" as &'static [u8], MONITORS.as_bytes().to_vec()),
        (b"j/workspaces", WORKSPACES.as_bytes().to_vec()),
        (b"j/clients", CLIENTS.as_bytes().to_vec()),
    ];
    script.extend(
        dispatches
            .into_iter()
            .map(|request| (request, b"ok".to_vec())),
    );
    script.extend([
        (b"j/monitors" as &'static [u8], post_monitors.clone()),
        (b"j/workspaces" as &'static [u8], post_workspaces.clone()),
        (b"j/clients" as &'static [u8], post_clients.clone()),
    ]);
    let server = tokio::spawn(serve_script(listener, script));
    let adapter = HyprlandAdapter::with_timing(paths, CancellationToken::new(), test_timing());

    let result = adapter.execute(command).await.unwrap();
    server.await.unwrap();
    let expected = parse_full_snapshot(&post_monitors, &post_workspaces, &post_clients).unwrap();
    assert_eq!(result, CompositorExecution::Snapshot(expected));
    result
}

async fn assert_stale_readback_is_unconfirmed(
    command: HyprlandCommand,
    dispatches: Vec<&'static [u8]>,
) {
    let (_directory, paths, listener) = command_fixture().await;
    let server = tokio::spawn(async move {
        let mut expected = vec![
            b"j/monitors" as &'static [u8],
            b"j/workspaces",
            b"j/clients",
        ];
        expected.extend(dispatches);
        for expected in expected {
            let (mut stream, _) = listener.accept().await.unwrap();
            let mut request = Vec::new();
            stream.read_to_end(&mut request).await.unwrap();
            assert_eq!(request, expected);
            let response = match expected {
                b"j/monitors" => MONITORS.as_bytes(),
                b"j/workspaces" => WORKSPACES.as_bytes(),
                b"j/clients" => CLIENTS.as_bytes(),
                _ => b"ok",
            };
            stream.write_all(response).await.unwrap();
        }
        loop {
            let (mut stream, _) = listener.accept().await.unwrap();
            let mut request = Vec::new();
            stream.read_to_end(&mut request).await.unwrap();
            let response = match request.as_slice() {
                b"j/monitors" => MONITORS.as_bytes(),
                b"j/workspaces" => WORKSPACES.as_bytes(),
                b"j/clients" => CLIENTS.as_bytes(),
                other => panic!("unexpected stale readback request {other:?}"),
            };
            stream.write_all(response).await.unwrap();
        }
    });
    let mut timing = test_timing();
    timing.operation_timeout = Duration::from_millis(40);
    let adapter = HyprlandAdapter::with_timing(paths, CancellationToken::new(), timing);
    let error = adapter.execute(command).await.unwrap_err();
    assert_eq!(error.kind(), CompositorErrorKind::Unconfirmed);
    server.abort();
    let _ = server.await;
}

#[test]
fn full_snapshot_maps_stable_ids_focus_special_workspace_and_window_state() {
    let snapshot = parse_fixture();

    assert_eq!(
        snapshot
            .monitors
            .iter()
            .map(|monitor| monitor.id.as_str())
            .collect::<Vec<_>>(),
        ["DP-1", "HDMI-A-1"]
    );
    assert_eq!(
        snapshot
            .monitors
            .iter()
            .filter(|monitor| monitor.focused)
            .map(|monitor| monitor.id.as_str())
            .collect::<Vec<_>>(),
        ["DP-1"]
    );
    assert_eq!(
        snapshot
            .workspaces
            .iter()
            .map(|workspace| (workspace.id.as_str(), workspace.name.as_str()))
            .collect::<Vec<_>>(),
        [("2", "2"), ("3", "3"), ("-99", "special:magic")]
    );
    assert_eq!(
        snapshot
            .workspaces
            .iter()
            .filter(|workspace| workspace.focused)
            .map(|workspace| workspace.id.as_str())
            .collect::<Vec<_>>(),
        ["-99"]
    );

    let terminal = snapshot
        .windows
        .iter()
        .find(|window| window.id == "0xabc")
        .unwrap();
    assert!(terminal.focused);
    assert!(terminal.fullscreen);
    assert!(terminal.floating);
    assert!(terminal.pinned);
    assert!(!terminal.grouped);
    assert_eq!(terminal.workspace_id, "-99");

    let browser = snapshot
        .windows
        .iter()
        .find(|window| window.id == "0xdef")
        .unwrap();
    assert!(browser.grouped);
    assert!(!browser.focused);
    assert_eq!(
        parse_fixture(),
        snapshot,
        "stable IDs must be deterministic"
    );
}

#[test]
fn malformed_consumed_fields_and_orphan_references_are_rejected() {
    let mut invalid_address: serde_json::Value = serde_json::from_str(CLIENTS).unwrap();
    invalid_address[0]["address"] = serde_json::json!("address:0xabc");
    let error = parse_full_snapshot(
        MONITORS.as_bytes(),
        WORKSPACES.as_bytes(),
        invalid_address.to_string().as_bytes(),
    )
    .unwrap_err();
    assert_eq!(error.kind(), CompositorErrorKind::Parse);

    let mut orphan: serde_json::Value = serde_json::from_str(WORKSPACES).unwrap();
    orphan[0]["monitor"] = serde_json::json!("attacker-output");
    let error = parse_full_snapshot(
        MONITORS.as_bytes(),
        orphan.to_string().as_bytes(),
        CLIENTS.as_bytes(),
    )
    .unwrap_err();
    assert_eq!(error.kind(), CompositorErrorKind::Parse);

    let mut invalid_group: serde_json::Value = serde_json::from_str(CLIENTS).unwrap();
    invalid_group[1]["grouped"] = serde_json::json!(["0xdef", "not-an-address"]);
    let error = parse_full_snapshot(
        MONITORS.as_bytes(),
        WORKSPACES.as_bytes(),
        invalid_group.to_string().as_bytes(),
    )
    .unwrap_err();
    assert_eq!(error.kind(), CompositorErrorKind::Parse);
}

#[test]
fn parser_enforces_sdk_collection_focus_and_numeric_bounds() {
    let monitor: serde_json::Value =
        serde_json::from_str::<serde_json::Value>(MONITORS).unwrap()[0].clone();
    let mut too_many = Vec::new();
    for index in 0..65 {
        let mut item = monitor.clone();
        item["id"] = serde_json::json!(index);
        item["name"] = serde_json::json!(format!("OUT-{index}"));
        item["focused"] = serde_json::json!(index == 0);
        item["activeWorkspace"] = serde_json::json!({ "id": 2, "name": "2" });
        item["specialWorkspace"] = serde_json::json!({ "id": 0, "name": "" });
        too_many.push(item);
    }
    let error = parse_full_snapshot(
        serde_json::to_string(&too_many).unwrap().as_bytes(),
        WORKSPACES.as_bytes(),
        CLIENTS.as_bytes(),
    )
    .unwrap_err();
    assert_eq!(error.kind(), CompositorErrorKind::Bounds);

    let mut zero_scale: serde_json::Value = serde_json::from_str(MONITORS).unwrap();
    zero_scale[0]["scale"] = serde_json::json!(0.0);
    let error = parse_full_snapshot(
        zero_scale.to_string().as_bytes(),
        WORKSPACES.as_bytes(),
        CLIENTS.as_bytes(),
    )
    .unwrap_err();
    assert_eq!(error.kind(), CompositorErrorKind::Parse);

    let mut ambiguous_focus: serde_json::Value = serde_json::from_str(CLIENTS).unwrap();
    ambiguous_focus[1]["focusHistoryID"] = serde_json::json!(0);
    let error = parse_full_snapshot(
        MONITORS.as_bytes(),
        WORKSPACES.as_bytes(),
        ambiguous_focus.to_string().as_bytes(),
    )
    .unwrap_err();
    assert_eq!(error.kind(), CompositorErrorKind::Parse);

    let mut combined_fullscreen: serde_json::Value = serde_json::from_str(CLIENTS).unwrap();
    combined_fullscreen[0]["fullscreen"] = serde_json::json!(3);
    let snapshot = parse_full_snapshot(
        MONITORS.as_bytes(),
        WORKSPACES.as_bytes(),
        combined_fullscreen.to_string().as_bytes(),
    )
    .unwrap();
    assert!(
        snapshot
            .windows
            .iter()
            .find(|window| window.id == "0xabc")
            .unwrap()
            .fullscreen
    );

    let mut invalid_fullscreen: serde_json::Value = serde_json::from_str(CLIENTS).unwrap();
    invalid_fullscreen[0]["fullscreen"] = serde_json::json!(4);
    let error = parse_full_snapshot(
        MONITORS.as_bytes(),
        WORKSPACES.as_bytes(),
        invalid_fullscreen.to_string().as_bytes(),
    )
    .unwrap_err();
    assert_eq!(error.kind(), CompositorErrorKind::Parse);
}

#[test]
fn current_instance_discovery_rejects_traversal_controls_and_overlong_signatures() {
    let runtime = Path::new("/run/user/1000");
    for signature in [
        "",
        ".",
        "..",
        "../instance",
        "instance/child",
        "instance\\child",
        "instance\nchild",
        "instance\0child",
    ] {
        let error = HyprlandPaths::from_runtime_dir_and_signature(runtime, signature).unwrap_err();
        assert_eq!(
            error.kind(),
            CompositorErrorKind::UnsafeInstance,
            "signature {signature:?} must be rejected"
        );
    }

    let overlong = "a".repeat(MAX_INSTANCE_SIGNATURE_BYTES + 1);
    let error = HyprlandPaths::from_runtime_dir_and_signature(runtime, &overlong).unwrap_err();
    assert_eq!(error.kind(), CompositorErrorKind::UnsafeInstance);
}

#[test]
fn current_instance_paths_use_only_the_documented_runtime_root_and_socket_names() {
    let paths = HyprlandPaths::from_runtime_dir_and_signature(
        Path::new("/run/user/1000"),
        "instance_1234567890_42",
    )
    .unwrap();

    assert_eq!(
        paths.command_socket(),
        Path::new("/run/user/1000/hypr/instance_1234567890_42/.socket.sock")
    );
    assert_eq!(
        paths.event_socket(),
        Path::new("/run/user/1000/hypr/instance_1234567890_42/.socket2.sock")
    );
}

#[test]
fn current_instance_discovery_rejects_relative_or_lexically_escaping_runtime_roots() {
    for runtime in [
        Path::new("relative"),
        Path::new("/"),
        Path::new("/run/user/1000/../other"),
    ] {
        let error =
            HyprlandPaths::from_runtime_dir_and_signature(runtime, "fixture-instance").unwrap_err();
        assert_eq!(error.kind(), CompositorErrorKind::UnsafeInstance);
    }
}

#[tokio::test]
async fn snapshot_uses_fresh_native_connections_and_exact_json_query_bytes() {
    let (_directory, paths, listener) = command_fixture().await;
    let server = tokio::spawn(serve_script(
        listener,
        vec![
            (b"j/monitors", MONITORS.as_bytes().to_vec()),
            (b"j/workspaces", WORKSPACES.as_bytes().to_vec()),
            (b"j/clients", CLIENTS.as_bytes().to_vec()),
        ],
    ));
    let adapter = HyprlandAdapter::with_timing(paths, CancellationToken::new(), test_timing());

    assert_eq!(adapter.snapshot().await.unwrap(), parse_fixture());
    assert_eq!(server.await.unwrap().len(), 3);
}

#[tokio::test]
async fn command_response_is_rejected_at_the_hard_cap() {
    let (_directory, paths, listener) = command_fixture().await;
    let oversized = vec![b' '; MAX_COMMAND_RESPONSE_BYTES + 1];
    let server = tokio::spawn(serve_script(listener, vec![(b"j/monitors", oversized)]));
    let adapter = HyprlandAdapter::with_timing(paths, CancellationToken::new(), test_timing());

    let error = adapter.snapshot().await.unwrap_err();
    assert_eq!(error.kind(), CompositorErrorKind::Bounds);
    server.await.unwrap();
}

#[tokio::test]
async fn missing_command_socket_is_terminal_unavailable_without_fallback_probing() {
    let directory = tempfile::tempdir().unwrap();
    let paths = HyprlandPaths::from_runtime_dir_and_signature(directory.path(), "missing-instance")
        .unwrap();
    let adapter = HyprlandAdapter::with_timing(paths, CancellationToken::new(), test_timing());

    let error = adapter.snapshot().await.unwrap_err();
    assert_eq!(error.kind(), CompositorErrorKind::Unavailable);
}

#[tokio::test]
async fn snapshot_deadline_and_cancellation_interrupt_blocked_responses() {
    let (_directory, paths, listener) = command_fixture().await;
    let accepted = tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await.unwrap();
        let mut request = Vec::new();
        stream.read_to_end(&mut request).await.unwrap();
        assert_eq!(request, b"j/monitors");
        tokio::time::sleep(Duration::from_secs(1)).await;
    });
    let mut timing = test_timing();
    timing.operation_timeout = Duration::from_millis(30);
    let adapter = HyprlandAdapter::with_timing(paths, CancellationToken::new(), timing);
    let error = adapter.snapshot().await.unwrap_err();
    assert_eq!(error.kind(), CompositorErrorKind::Timeout);
    accepted.abort();
    let _ = accepted.await;

    let (_directory, paths, listener) = command_fixture().await;
    let accepted = tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await.unwrap();
        let mut request = Vec::new();
        stream.read_to_end(&mut request).await.unwrap();
        tokio::time::sleep(Duration::from_secs(1)).await;
    });
    let cancellation = CancellationToken::new();
    let adapter = HyprlandAdapter::with_timing(paths, cancellation.clone(), test_timing());
    let snapshot = tokio::spawn(async move { adapter.snapshot().await });
    tokio::time::sleep(Duration::from_millis(10)).await;
    cancellation.cancel();
    let error = snapshot.await.unwrap().unwrap_err();
    assert_eq!(error.kind(), CompositorErrorKind::Cancelled);
    accepted.abort();
    let _ = accepted.await;
}

#[tokio::test]
async fn every_closed_action_uses_fixed_wire_bytes_and_confirmed_readback() {
    let mut clients: serde_json::Value = serde_json::from_str(CLIENTS).unwrap();
    clients[0]["focusHistoryID"] = serde_json::json!(1);
    clients[1]["focusHistoryID"] = serde_json::json!(0);
    execute_case(
        HyprlandCommand::FocusWindow {
            window_id: StableId("0xdef".into()),
        },
        vec![b"dispatch focuswindow address:0xdef"],
        MONITORS.as_bytes().to_vec(),
        WORKSPACES.as_bytes().to_vec(),
        clients.to_string().into_bytes(),
    )
    .await;

    let mut clients: serde_json::Value = serde_json::from_str(CLIENTS).unwrap();
    clients[1]["workspace"] = serde_json::json!({"id": -99, "name": "special:magic"});
    execute_case(
        HyprlandCommand::MoveWindowToWorkspace {
            window_id: StableId("0xdef".into()),
            workspace_id: StableId("-99".into()),
        },
        vec![b"dispatch movetoworkspacesilent special:magic,address:0xdef"],
        MONITORS.as_bytes().to_vec(),
        WORKSPACES.as_bytes().to_vec(),
        clients.to_string().into_bytes(),
    )
    .await;

    let mut clients: serde_json::Value = serde_json::from_str(CLIENTS).unwrap();
    clients.as_array_mut().unwrap().remove(0);
    clients[0]["focusHistoryID"] = serde_json::json!(0);
    execute_case(
        HyprlandCommand::CloseWindow {
            window_id: StableId("0xabc".into()),
        },
        vec![b"dispatch closewindow address:0xabc"],
        MONITORS.as_bytes().to_vec(),
        WORKSPACES.as_bytes().to_vec(),
        clients.to_string().into_bytes(),
    )
    .await;

    let mut monitors: serde_json::Value = serde_json::from_str(MONITORS).unwrap();
    monitors[0]["focused"] = serde_json::json!(false);
    monitors[1]["focused"] = serde_json::json!(true);
    execute_case(
        HyprlandCommand::FocusWorkspace {
            workspace_id: StableId("3".into()),
        },
        vec![b"dispatch workspace 3"],
        monitors.to_string().into_bytes(),
        WORKSPACES.as_bytes().to_vec(),
        CLIENTS.as_bytes().to_vec(),
    )
    .await;

    let mut workspaces: serde_json::Value = serde_json::from_str(WORKSPACES).unwrap();
    workspaces[1]["monitor"] = serde_json::json!("DP-1");
    execute_case(
        HyprlandCommand::MoveWorkspaceToMonitor {
            workspace_id: StableId("3".into()),
            monitor_id: StableId("DP-1".into()),
        },
        vec![b"dispatch moveworkspacetomonitor 3 DP-1"],
        MONITORS.as_bytes().to_vec(),
        workspaces.to_string().into_bytes(),
        CLIENTS.as_bytes().to_vec(),
    )
    .await;

    let mut clients: serde_json::Value = serde_json::from_str(CLIENTS).unwrap();
    clients[0]["fullscreen"] = serde_json::json!(0);
    execute_case(
        HyprlandCommand::ToggleFullscreen {
            window_id: StableId("0xabc".into()),
        },
        vec![
            b"dispatch focuswindow address:0xabc",
            b"dispatch fullscreen 0",
        ],
        MONITORS.as_bytes().to_vec(),
        WORKSPACES.as_bytes().to_vec(),
        clients.to_string().into_bytes(),
    )
    .await;

    let mut clients: serde_json::Value = serde_json::from_str(CLIENTS).unwrap();
    clients[0]["floating"] = serde_json::json!(false);
    execute_case(
        HyprlandCommand::ToggleFloating {
            window_id: StableId("0xabc".into()),
        },
        vec![b"dispatch togglefloating address:0xabc"],
        MONITORS.as_bytes().to_vec(),
        WORKSPACES.as_bytes().to_vec(),
        clients.to_string().into_bytes(),
    )
    .await;

    let mut clients: serde_json::Value = serde_json::from_str(CLIENTS).unwrap();
    clients[0]["pinned"] = serde_json::json!(false);
    execute_case(
        HyprlandCommand::TogglePinned {
            window_id: StableId("0xabc".into()),
        },
        vec![b"dispatch pin address:0xabc"],
        MONITORS.as_bytes().to_vec(),
        WORKSPACES.as_bytes().to_vec(),
        clients.to_string().into_bytes(),
    )
    .await;

    let mut clients: serde_json::Value = serde_json::from_str(CLIENTS).unwrap();
    clients[0]["grouped"] = serde_json::json!(["0xabc", "0xdef"]);
    execute_case(
        HyprlandCommand::ToggleGroup {
            window_id: StableId("0xabc".into()),
        },
        vec![
            b"dispatch focuswindow address:0xabc",
            b"dispatch togglegroup",
        ],
        MONITORS.as_bytes().to_vec(),
        WORKSPACES.as_bytes().to_vec(),
        clients.to_string().into_bytes(),
    )
    .await;
}

#[tokio::test]
async fn every_non_exit_action_family_times_out_unconfirmed_on_stale_readback() {
    for (command, dispatches) in [
        (
            HyprlandCommand::FocusWindow {
                window_id: StableId("0xdef".into()),
            },
            vec![b"dispatch focuswindow address:0xdef" as &'static [u8]],
        ),
        (
            HyprlandCommand::MoveWindowToWorkspace {
                window_id: StableId("0xdef".into()),
                workspace_id: StableId("-99".into()),
            },
            vec![b"dispatch movetoworkspacesilent special:magic,address:0xdef"],
        ),
        (
            HyprlandCommand::CloseWindow {
                window_id: StableId("0xabc".into()),
            },
            vec![b"dispatch closewindow address:0xabc"],
        ),
        (
            HyprlandCommand::FocusWorkspace {
                workspace_id: StableId("3".into()),
            },
            vec![b"dispatch workspace 3"],
        ),
        (
            HyprlandCommand::MoveWorkspaceToMonitor {
                workspace_id: StableId("3".into()),
                monitor_id: StableId("DP-1".into()),
            },
            vec![b"dispatch moveworkspacetomonitor 3 DP-1"],
        ),
        (
            HyprlandCommand::ToggleFullscreen {
                window_id: StableId("0xabc".into()),
            },
            vec![
                b"dispatch focuswindow address:0xabc",
                b"dispatch fullscreen 0",
            ],
        ),
        (
            HyprlandCommand::ToggleFloating {
                window_id: StableId("0xabc".into()),
            },
            vec![b"dispatch togglefloating address:0xabc"],
        ),
        (
            HyprlandCommand::TogglePinned {
                window_id: StableId("0xabc".into()),
            },
            vec![b"dispatch pin address:0xabc"],
        ),
        (
            HyprlandCommand::ToggleGroup {
                window_id: StableId("0xabc".into()),
            },
            vec![
                b"dispatch focuswindow address:0xabc",
                b"dispatch togglegroup",
            ],
        ),
    ] {
        assert_stale_readback_is_unconfirmed(command, dispatches).await;
    }
}

#[tokio::test]
async fn rejected_dispatch_and_unconfirmed_readback_never_report_success() {
    let (_directory, paths, listener) = command_fixture().await;
    let server = tokio::spawn(serve_script(
        listener,
        vec![
            (b"j/monitors", MONITORS.as_bytes().to_vec()),
            (b"j/workspaces", WORKSPACES.as_bytes().to_vec()),
            (b"j/clients", CLIENTS.as_bytes().to_vec()),
            (
                b"dispatch closewindow address:0xabc",
                b"permission denied".to_vec(),
            ),
        ],
    ));
    let adapter = HyprlandAdapter::with_timing(paths, CancellationToken::new(), test_timing());
    let error = adapter
        .execute(HyprlandCommand::CloseWindow {
            window_id: StableId("0xabc".into()),
        })
        .await
        .unwrap_err();
    assert_eq!(error.kind(), CompositorErrorKind::Rejected);
    server.await.unwrap();

    let (_directory, paths, listener) = command_fixture().await;
    let server = tokio::spawn(async move {
        for expected in [
            b"j/monitors" as &'static [u8],
            b"j/workspaces",
            b"j/clients",
            b"dispatch closewindow address:0xabc",
        ] {
            let (mut stream, _) = listener.accept().await.unwrap();
            let mut request = Vec::new();
            stream.read_to_end(&mut request).await.unwrap();
            assert_eq!(request, expected);
            let response = match expected {
                b"j/monitors" => MONITORS.as_bytes(),
                b"j/workspaces" => WORKSPACES.as_bytes(),
                b"j/clients" => CLIENTS.as_bytes(),
                _ => b"ok",
            };
            stream.write_all(response).await.unwrap();
        }
        loop {
            let (mut stream, _) = listener.accept().await.unwrap();
            let mut request = Vec::new();
            stream.read_to_end(&mut request).await.unwrap();
            let response = match request.as_slice() {
                b"j/monitors" => MONITORS.as_bytes(),
                b"j/workspaces" => WORKSPACES.as_bytes(),
                b"j/clients" => CLIENTS.as_bytes(),
                other => panic!("unexpected readback request {other:?}"),
            };
            stream.write_all(response).await.unwrap();
        }
    });
    let mut timing = test_timing();
    timing.operation_timeout = Duration::from_millis(40);
    let adapter = HyprlandAdapter::with_timing(paths, CancellationToken::new(), timing);
    let error = adapter
        .execute(HyprlandCommand::CloseWindow {
            window_id: StableId("0xabc".into()),
        })
        .await
        .unwrap_err();
    assert_eq!(error.kind(), CompositorErrorKind::Unconfirmed);
    server.abort();
    let _ = server.await;
}

#[tokio::test]
async fn command_targets_cannot_select_a_dispatcher_or_escape_a_fixed_argument() {
    let (_directory, paths, listener) = command_fixture().await;
    let server = tokio::spawn(serve_script(
        listener,
        vec![
            (b"j/monitors", MONITORS.as_bytes().to_vec()),
            (b"j/workspaces", WORKSPACES.as_bytes().to_vec()),
            (b"j/clients", CLIENTS.as_bytes().to_vec()),
        ],
    ));
    let adapter = HyprlandAdapter::with_timing(paths, CancellationToken::new(), test_timing());
    let error = adapter
        .execute(HyprlandCommand::FocusWindow {
            window_id: StableId("0xabc;dispatch exit".into()),
        })
        .await
        .unwrap_err();
    assert_eq!(error.kind(), CompositorErrorKind::Rejected);
    server.await.unwrap();
}

#[tokio::test]
async fn exit_uses_only_the_fixed_dispatcher_and_requires_both_sockets_to_disappear() {
    let (_directory, paths, listener) = command_fixture().await;
    let event_listener = UnixListener::bind(paths.event_socket()).unwrap();
    let server = tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await.unwrap();
        let mut request = Vec::new();
        stream.read_to_end(&mut request).await.unwrap();
        assert_eq!(request, b"dispatch exit");
        stream.write_all(b"ok").await.unwrap();
        drop(stream);
        drop(listener);
        drop(event_listener);
    });
    let adapter = HyprlandAdapter::with_timing(paths, CancellationToken::new(), test_timing());

    assert_eq!(
        adapter.execute(HyprlandCommand::Exit).await.unwrap(),
        CompositorExecution::Exited
    );
    server.await.unwrap();

    let (_directory, paths, listener) = command_fixture().await;
    let event_listener = UnixListener::bind(paths.event_socket()).unwrap();
    let server = tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await.unwrap();
        let mut request = Vec::new();
        stream.read_to_end(&mut request).await.unwrap();
        assert_eq!(request, b"dispatch exit");
        stream.write_all(b"ok").await.unwrap();
        drop(stream);
        tokio::time::sleep(Duration::from_secs(1)).await;
        drop((listener, event_listener));
    });
    let mut timing = test_timing();
    timing.operation_timeout = Duration::from_millis(40);
    let adapter = HyprlandAdapter::with_timing(paths, CancellationToken::new(), timing);
    let error = adapter.execute(HyprlandCommand::Exit).await.unwrap_err();
    assert_eq!(error.kind(), CompositorErrorKind::Unconfirmed);
    server.abort();
    let _ = server.await;
}

#[test]
fn every_consumed_event_family_is_validated_before_reconciliation() {
    for line in [
        "workspace>>3",
        "workspacev2>>3,3",
        "focusedmon>>DP-1,3",
        "focusedmonv2>>DP-1,3",
        "activewindow>>org.mozilla.firefox,Browser, with comma",
        "activewindowv2>>def",
        "activewindowv2>>",
        "openwindow>>abc,3,com.example.App,Title, with comma",
        "closewindow>>abc",
        "movewindow>>abc,3",
        "movewindowv2>>abc,3,3",
        "fullscreen>>1",
        "changefloatingmode>>abc,1",
        "pin>>abc,0",
        "togglegroup>>1,abc,def",
        "moveintogroup>>abc",
        "moveoutofgroup>>abc",
        "windowtitle>>abc",
        "windowtitlev2>>abc,Updated title, with comma",
        "monitoradded>>DP-2",
        "monitoraddedv2>>2,DP-2,Fixture Display",
        "monitorremoved>>DP-2",
        "monitorremovedv2>>2,DP-2,Fixture Display",
        "createworkspace>>4",
        "createworkspacev2>>4,4",
        "destroyworkspace>>4",
        "destroyworkspacev2>>4,4",
        "moveworkspace>>4,DP-2",
        "moveworkspacev2>>4,4,DP-2",
        "renameworkspace>>4,development",
        "activespecial>>special:magic,DP-1",
        "activespecialv2>>-99,special:magic,DP-1",
        "activespecialv2>>,,DP-1",
    ] {
        assert_eq!(
            parse_event_line(line.as_bytes()).unwrap(),
            EventDisposition::Reconcile,
            "{line} must trigger authoritative reconciliation"
        );
    }

    assert_eq!(
        parse_event_line(b"futureadditiveevent>>opaque,payload").unwrap(),
        EventDisposition::Ignore
    );
    for malformed in [
        "workspacev2>>not-an-id,name",
        "activewindowv2>>not-hex",
        "openwindow>>abc,missing-fields",
        "fullscreen>>2",
        "changefloatingmode>>abc,true",
        "pin>>abc,",
        "togglegroup>>1,not-hex",
        "monitoraddedv2>>id-only",
        "focusedmonv2>>DP-1,not-an-id",
        "renameworkspace>>0,name",
    ] {
        assert_eq!(
            parse_event_line(malformed.as_bytes()).unwrap_err().kind(),
            CompositorErrorKind::Parse,
            "{malformed} must not mutate cached compositor state"
        );
    }

    let oversized_group = format!(
        "togglegroup>>1,{}",
        std::iter::repeat_n("a", 16_385)
            .collect::<Vec<_>>()
            .join(",")
    );
    assert_eq!(
        parse_event_line(oversized_group.as_bytes())
            .unwrap_err()
            .kind(),
        CompositorErrorKind::Bounds
    );
}

#[tokio::test]
async fn known_event_reconciles_authoritative_state_and_cancellation_stops_the_loop() {
    let (_directory, paths, command_listener) = command_fixture().await;
    let event_listener = UnixListener::bind(paths.event_socket()).unwrap();
    let mut closed_clients: serde_json::Value = serde_json::from_str(CLIENTS).unwrap();
    closed_clients.as_array_mut().unwrap().remove(0);
    closed_clients[0]["focusHistoryID"] = serde_json::json!(0);
    let command_server = tokio::spawn(serve_script(
        command_listener,
        vec![
            (b"j/monitors", MONITORS.as_bytes().to_vec()),
            (b"j/workspaces", WORKSPACES.as_bytes().to_vec()),
            (b"j/clients", CLIENTS.as_bytes().to_vec()),
            (b"j/monitors", MONITORS.as_bytes().to_vec()),
            (b"j/workspaces", WORKSPACES.as_bytes().to_vec()),
            (b"j/clients", closed_clients.to_string().into_bytes()),
        ],
    ));
    let event_server = tokio::spawn(serve_event_connections(
        event_listener,
        vec![vec![b"closewindow>>abc\n".to_vec()]],
    ));
    let cancellation = CancellationToken::new();
    let adapter = HyprlandAdapter::with_timing(paths, cancellation.clone(), test_timing());
    let (sender, mut receiver) = tokio::sync::mpsc::channel(4);
    let events = tokio::spawn(async move { adapter.run_events(sender).await });

    let HyprlandEvent::Snapshot(initial) = receiver.recv().await.unwrap() else {
        panic!("initial event must be a snapshot");
    };
    assert_eq!(initial.windows.len(), 2);
    let HyprlandEvent::Snapshot(reconciled) = receiver.recv().await.unwrap() else {
        panic!("known event must publish reconciled state");
    };
    assert_eq!(
        reconciled
            .windows
            .iter()
            .map(|window| window.id.as_str())
            .collect::<Vec<_>>(),
        ["0xdef"]
    );

    cancellation.cancel();
    assert!(events.await.unwrap().is_ok());
    command_server.await.unwrap();
    event_server.await.unwrap();
}

#[tokio::test]
async fn malformed_and_overlong_event_lines_publish_localized_degradation() {
    for (line, expected) in [
        (
            b"activewindowv2>>not-an-address\n".to_vec(),
            CompositorErrorKind::Parse,
        ),
        (
            {
                let mut line = vec![b'x'; MAX_EVENT_LINE_BYTES + 1];
                line.push(b'\n');
                line
            },
            CompositorErrorKind::Bounds,
        ),
    ] {
        let (_directory, paths, command_listener) = command_fixture().await;
        let event_listener = UnixListener::bind(paths.event_socket()).unwrap();
        let command_server = tokio::spawn(serve_script(
            command_listener,
            vec![
                (b"j/monitors", MONITORS.as_bytes().to_vec()),
                (b"j/workspaces", WORKSPACES.as_bytes().to_vec()),
                (b"j/clients", CLIENTS.as_bytes().to_vec()),
            ],
        ));
        let event_server = tokio::spawn(serve_event_connections(event_listener, vec![vec![line]]));
        let cancellation = CancellationToken::new();
        let adapter = HyprlandAdapter::with_timing(paths, cancellation.clone(), test_timing());
        let (sender, mut receiver) = tokio::sync::mpsc::channel(4);
        let events = tokio::spawn(async move { adapter.run_events(sender).await });

        assert!(matches!(
            receiver.recv().await.unwrap(),
            HyprlandEvent::Snapshot(_)
        ));
        let HyprlandEvent::Degraded { kind, .. } = receiver.recv().await.unwrap() else {
            panic!("malformed event must degrade only compositor state");
        };
        assert_eq!(kind, expected);
        cancellation.cancel();
        assert!(events.await.unwrap().is_ok());
        command_server.await.unwrap();
        event_server.await.unwrap();
    }
}

#[tokio::test]
async fn fallback_timer_reconciles_without_an_event() {
    let (_directory, paths, command_listener) = command_fixture().await;
    let event_listener = UnixListener::bind(paths.event_socket()).unwrap();
    let command_server = tokio::spawn(serve_script(
        command_listener,
        vec![
            (b"j/monitors", MONITORS.as_bytes().to_vec()),
            (b"j/workspaces", WORKSPACES.as_bytes().to_vec()),
            (b"j/clients", CLIENTS.as_bytes().to_vec()),
            (b"j/monitors", MONITORS.as_bytes().to_vec()),
            (b"j/workspaces", WORKSPACES.as_bytes().to_vec()),
            (b"j/clients", CLIENTS.as_bytes().to_vec()),
        ],
    ));
    let event_server = tokio::spawn(async move {
        let (_stream, _) = event_listener.accept().await.unwrap();
        tokio::time::sleep(Duration::from_secs(1)).await;
    });
    let cancellation = CancellationToken::new();
    let mut timing = test_timing();
    timing.fallback_reconcile = Duration::from_millis(20);
    let adapter = HyprlandAdapter::with_timing(paths, cancellation.clone(), timing);
    let (sender, mut receiver) = tokio::sync::mpsc::channel(4);
    let events = tokio::spawn(async move { adapter.run_events(sender).await });

    assert!(matches!(
        receiver.recv().await.unwrap(),
        HyprlandEvent::Snapshot(_)
    ));
    assert!(matches!(
        receiver.recv().await.unwrap(),
        HyprlandEvent::Snapshot(_)
    ));
    cancellation.cancel();
    assert!(events.await.unwrap().is_ok());
    command_server.await.unwrap();
    event_server.abort();
    let _ = event_server.await;
}

#[tokio::test]
async fn event_disconnect_reconnects_and_publishes_a_new_full_snapshot() {
    let (_directory, paths, command_listener) = command_fixture().await;
    let event_listener = UnixListener::bind(paths.event_socket()).unwrap();
    let command_server = tokio::spawn(serve_script(
        command_listener,
        vec![
            (b"j/monitors", MONITORS.as_bytes().to_vec()),
            (b"j/workspaces", WORKSPACES.as_bytes().to_vec()),
            (b"j/clients", CLIENTS.as_bytes().to_vec()),
            (b"j/monitors", MONITORS.as_bytes().to_vec()),
            (b"j/workspaces", WORKSPACES.as_bytes().to_vec()),
            (b"j/clients", CLIENTS.as_bytes().to_vec()),
        ],
    ));
    let event_server = tokio::spawn(async move {
        let (first, _) = event_listener.accept().await.unwrap();
        drop(first);
        let (_second, _) = event_listener.accept().await.unwrap();
        tokio::time::sleep(Duration::from_secs(1)).await;
    });
    let cancellation = CancellationToken::new();
    let adapter = HyprlandAdapter::with_timing(paths, cancellation.clone(), test_timing());
    let (sender, mut receiver) = tokio::sync::mpsc::channel(4);
    let events = tokio::spawn(async move { adapter.run_events(sender).await });

    assert!(matches!(
        receiver.recv().await.unwrap(),
        HyprlandEvent::Snapshot(_)
    ));
    assert!(matches!(
        receiver.recv().await.unwrap(),
        HyprlandEvent::Degraded {
            kind: CompositorErrorKind::Unavailable,
            ..
        }
    ));
    assert!(matches!(
        receiver.recv().await.unwrap(),
        HyprlandEvent::Snapshot(_)
    ));
    cancellation.cancel();
    assert!(events.await.unwrap().is_ok());
    command_server.await.unwrap();
    event_server.abort();
    let _ = event_server.await;
}

#[tokio::test]
async fn a_lagged_event_receiver_forces_reconnect_and_authoritative_resynchronization() {
    let (_directory, paths, command_listener) = command_fixture().await;
    let event_listener = UnixListener::bind(paths.event_socket()).unwrap();
    let (third_connection_sender, third_connection_receiver) = tokio::sync::oneshot::channel();
    let command_server = tokio::spawn(async move {
        for _ in 0..12 {
            let (mut stream, _) = command_listener.accept().await.unwrap();
            let mut request = Vec::new();
            stream.read_to_end(&mut request).await.unwrap();
            let response = match request.as_slice() {
                b"j/monitors" => MONITORS.as_bytes(),
                b"j/workspaces" => WORKSPACES.as_bytes(),
                b"j/clients" => CLIENTS.as_bytes(),
                other => panic!("unexpected reconciliation request {other:?}"),
            };
            stream.write_all(response).await.unwrap();
        }
    });
    let event_server = tokio::spawn(async move {
        let (mut first, _) = event_listener.accept().await.unwrap();
        first.write_all(b"closewindow>>abc\n").await.unwrap();
        let (_second, _) = event_listener.accept().await.unwrap();
        let (_third, _) = event_listener.accept().await.unwrap();
        third_connection_sender.send(()).unwrap();
        tokio::time::sleep(Duration::from_secs(1)).await;
    });
    let cancellation = CancellationToken::new();
    let adapter = HyprlandAdapter::with_timing(paths, cancellation.clone(), test_timing());
    let (sender, mut receiver) = tokio::sync::mpsc::channel(1);
    let events = tokio::spawn(async move { adapter.run_events(sender).await });

    third_connection_receiver.await.unwrap();
    assert!(matches!(
        receiver.recv().await.unwrap(),
        HyprlandEvent::Snapshot(_)
    ));
    assert!(matches!(
        receiver.recv().await.unwrap(),
        HyprlandEvent::Snapshot(_)
    ));
    cancellation.cancel();
    assert!(events.await.unwrap().is_ok());
    command_server.await.unwrap();
    event_server.abort();
    let _ = event_server.await;
}

#[tokio::test]
async fn v3_source_lifecycle_publishes_available_and_localized_degraded_capabilities() {
    let (_directory, paths, command_listener) = command_fixture().await;
    let event_listener = UnixListener::bind(paths.event_socket()).unwrap();
    let command_server = tokio::spawn(serve_script(
        command_listener,
        vec![
            (b"j/monitors", MONITORS.as_bytes().to_vec()),
            (b"j/workspaces", WORKSPACES.as_bytes().to_vec()),
            (b"j/clients", CLIENTS.as_bytes().to_vec()),
        ],
    ));
    let (emit_sender, emit_receiver) = tokio::sync::oneshot::channel();
    let event_server = tokio::spawn(async move {
        let (mut stream, _) = event_listener.accept().await.unwrap();
        emit_receiver.await.unwrap();
        stream
            .write_all(b"activewindowv2>>not-hex\n")
            .await
            .unwrap();
        tokio::time::sleep(Duration::from_secs(1)).await;
    });
    let adapter = HyprlandAdapter::with_timing(paths, CancellationToken::new(), test_timing());
    let source = HyprlandSource::start(adapter);
    let mut state = source.subscribe();

    state.changed().await.unwrap();
    assert_eq!(
        state.borrow().status,
        sleepy_sdk::CapabilityAvailability::Available
    );
    assert_eq!(state.borrow().data.as_ref().unwrap(), &parse_fixture());
    emit_sender.send(()).unwrap();
    state.changed().await.unwrap();
    assert_eq!(
        state.borrow().status,
        sleepy_sdk::CapabilityAvailability::Parse
    );
    assert!(state.borrow().data.is_none());

    source
        .shutdown_and_join(Duration::from_millis(250))
        .await
        .unwrap();
    command_server.await.unwrap();
    event_server.abort();
    let _ = event_server.await;
}

#[tokio::test]
async fn absent_v3_compositor_is_terminal_immediately_without_disabling_lifecycle() {
    let source = HyprlandSource::unavailable(sleepy_session::compositor::CompositorError::new(
        CompositorErrorKind::Unavailable,
        "fixture compositor absent",
    ));
    let state = source.subscribe();
    assert_eq!(
        state.borrow().status,
        sleepy_sdk::CapabilityAvailability::Unavailable
    );
    assert!(state.borrow().data.is_none());
    assert_eq!(
        state.borrow().diagnostic.as_ref().unwrap().message,
        "fixture compositor absent"
    );
    source
        .shutdown_and_join(Duration::from_millis(50))
        .await
        .unwrap();
}

#[tokio::test]
async fn production_mutation_backend_routes_only_typed_v3_compositor_commands() {
    let (_directory, paths, listener) = command_fixture().await;
    let mut clients: serde_json::Value = serde_json::from_str(CLIENTS).unwrap();
    clients[0]["focusHistoryID"] = serde_json::json!(1);
    clients[1]["focusHistoryID"] = serde_json::json!(0);
    let post_clients = clients.to_string().into_bytes();
    let server = tokio::spawn(serve_script(
        listener,
        vec![
            (b"j/monitors", MONITORS.as_bytes().to_vec()),
            (b"j/workspaces", WORKSPACES.as_bytes().to_vec()),
            (b"j/clients", CLIENTS.as_bytes().to_vec()),
            (b"dispatch focuswindow address:0xdef", b"ok".to_vec()),
            (b"j/monitors", MONITORS.as_bytes().to_vec()),
            (b"j/workspaces", WORKSPACES.as_bytes().to_vec()),
            (b"j/clients", post_clients.clone()),
        ],
    ));
    let adapter = HyprlandAdapter::with_timing(paths, CancellationToken::new(), test_timing());
    let hub = EventHub::new(full_snapshot_event(1).unwrap(), 4);
    let backend = ProductionMutationBackend::with_hyprland(hub, adapter);

    let result = backend
        .execute_hyprland(HyprlandCommand::FocusWindow {
            window_id: StableId("0xdef".into()),
        })
        .await
        .unwrap();
    let CompositorExecution::Snapshot(snapshot) = result else {
        panic!("ordinary compositor commands must return confirmed snapshots");
    };
    assert!(snapshot
        .windows
        .iter()
        .any(|window| window.focused && window.id == "0xdef"));
    server.await.unwrap();
}
