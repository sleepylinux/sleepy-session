use sleepy_sdk::{NiriEvent, OsdEvent, OsdKind, WIRE_SCHEMA_VERSION};
use sleepy_session::{
    osd::{
        spawn_osd_runtime, spawn_osd_runtime_with_timing, FocusedOsdRequest, OsdPublication,
        OsdPublicationHub, OsdRouteError, OsdRouter, OsdSocket,
    },
    sessiond::{full_snapshot_event, EventHub, GenerationAllocator, GenerationAuthority},
};
use std::time::{Duration, Instant};
use tokio::io::{AsyncBufReadExt, BufReader};
use tokio::net::UnixStream;

fn event(output: &str, kind: OsdKind, label: &str) -> OsdEvent {
    OsdEvent {
        schema_version: WIRE_SCHEMA_VERSION,
        output_id: output.into(),
        kind,
        level: Some(0.5),
        muted: None,
        label: label.into(),
    }
}

fn route(router: &mut OsdRouter, event: OsdEvent, now: Instant) -> bool {
    router.observe_niri_focus(
        NiriEvent {
            focused_output_id: Some(event.output_id.clone()),
        },
        now,
    );
    router
        .push_focused(
            FocusedOsdRequest {
                kind: event.kind,
                level: event.level,
                muted: event.muted,
                label: event.label,
            },
            now,
        )
        .unwrap()
}

#[test]
fn each_output_has_one_visible_osd_and_same_kind_updates_in_place() {
    let mut router = OsdRouter::new(2);
    let now = Instant::now();
    route(&mut router, event("DP-1", OsdKind::Volume, "40%"), now);
    route(&mut router, event("DP-1", OsdKind::Volume, "50%"), now);
    route(&mut router, event("DP-1", OsdKind::Brightness, "60%"), now);
    route(
        &mut router,
        event("HDMI-A-1", OsdKind::Media, "Playing"),
        now,
    );

    assert_eq!(router.current("DP-1").unwrap().label, "50%");
    assert_eq!(router.pending_len("DP-1"), 1);
    assert_eq!(router.current("HDMI-A-1").unwrap().kind, OsdKind::Media);
    assert_eq!(router.complete("DP-1").unwrap().kind, OsdKind::Brightness);
}

#[test]
fn bounded_queue_rejects_newest_and_records_overflow() {
    let mut router = OsdRouter::new(1);
    let now = Instant::now();
    route(&mut router, event("DP-1", OsdKind::Volume, "40%"), now);
    route(&mut router, event("DP-1", OsdKind::Brightness, "50%"), now);
    assert!(!route(
        &mut router,
        event("DP-1", OsdKind::Media, "Track"),
        now
    ));

    assert_eq!(router.overflow_count("DP-1"), 1);
    assert_eq!(router.complete("DP-1").unwrap().kind, OsdKind::Brightness);
}

#[test]
fn timeout_advances_each_output_independently_in_fifo_order() {
    let start = Instant::now();
    let mut router =
        OsdRouter::with_timing(2, Duration::from_millis(100), Duration::from_millis(250));
    route(&mut router, event("DP-1", OsdKind::Volume, "40%"), start);
    route(
        &mut router,
        event("DP-1", OsdKind::Brightness, "50%"),
        start,
    );
    route(
        &mut router,
        event("HDMI-A-1", OsdKind::Media, "Playing"),
        start,
    );

    router.advance_time(start + Duration::from_millis(99));
    assert_eq!(router.current("DP-1").unwrap().kind, OsdKind::Volume);
    router.advance_time(start + Duration::from_millis(100));
    assert_eq!(router.current("DP-1").unwrap().kind, OsdKind::Brightness);
    assert!(router.current("HDMI-A-1").is_none());
    router.advance_time(start + Duration::from_millis(200));
    assert!(router.current("DP-1").is_none());
}

#[test]
fn all_osd_kinds_route_only_through_a_fresh_focused_output_event() {
    let start = Instant::now();
    let mut router = OsdRouter::with_timing(8, Duration::from_secs(1), Duration::from_millis(50));
    router.observe_niri_focus(
        NiriEvent {
            focused_output_id: Some("DP-1".into()),
        },
        start,
    );

    for (index, kind) in [
        OsdKind::Volume,
        OsdKind::Microphone,
        OsdKind::Brightness,
        OsdKind::Media,
        OsdKind::PowerProfile,
    ]
    .into_iter()
    .enumerate()
    {
        router
            .push_focused(
                FocusedOsdRequest {
                    kind,
                    level: matches!(
                        kind,
                        OsdKind::Volume | OsdKind::Microphone | OsdKind::Brightness
                    )
                    .then_some(0.5),
                    muted: matches!(kind, OsdKind::Volume | OsdKind::Microphone).then_some(false),
                    label: format!("event-{index}"),
                },
                start + Duration::from_millis(10),
            )
            .unwrap();
    }
    assert_eq!(router.current("DP-1").unwrap().kind, OsdKind::Volume);
    assert_eq!(router.pending_len("DP-1"), 4);

    let stale = router.push_focused(
        FocusedOsdRequest {
            kind: OsdKind::Volume,
            level: Some(0.7),
            muted: Some(false),
            label: "must not be misrouted".into(),
        },
        start + Duration::from_millis(51),
    );
    assert_eq!(stale.unwrap_err(), OsdRouteError::StaleFocus);
    assert_eq!(router.current("DP-1").unwrap().label, "event-0");
}

#[test]
fn missing_focus_is_local_to_routing_and_does_not_disturb_other_output_queues() {
    let start = Instant::now();
    let mut router = OsdRouter::with_timing(2, Duration::from_secs(1), Duration::from_millis(50));
    route(
        &mut router,
        event("HDMI-A-1", OsdKind::Media, "Playing"),
        start,
    );
    router.observe_niri_focus(
        NiriEvent {
            focused_output_id: None,
        },
        start,
    );

    let result = router.push_focused(
        FocusedOsdRequest {
            kind: OsdKind::PowerProfile,
            level: None,
            muted: None,
            label: "Balanced".into(),
        },
        start,
    );
    assert_eq!(result.unwrap_err(), OsdRouteError::MissingFocus);
    assert_eq!(router.current("HDMI-A-1").unwrap().label, "Playing");
}

#[tokio::test]
async fn daemon_osd_runtime_consumes_live_niri_focus_and_publishes_visible_state() {
    let temp = tempfile::tempdir().unwrap();
    let hub = EventHub::new(full_snapshot_event(0).unwrap(), 16);
    let events = hub.subscribe().await;
    let authority = GenerationAuthority::new(
        GenerationAllocator::open(temp.path().join("generation"), 16).unwrap(),
        0,
        hub,
    );
    let (runtime, task) = spawn_osd_runtime(events, 4);
    let mut publications = runtime.subscribe();

    assert_eq!(
        runtime
            .route(FocusedOsdRequest {
                kind: OsdKind::Volume,
                level: Some(0.4),
                muted: Some(false),
                label: "40%".into(),
            })
            .await
            .unwrap_err(),
        OsdRouteError::MissingFocus
    );
    authority
        .lock()
        .await
        .publish(
            sleepy_sdk::EventCause {
                kind: sleepy_sdk::EventCauseKind::External,
                request_id: None,
            },
            sleepy_sdk::SessionEvent::Niri(NiriEvent {
                focused_output_id: Some("DP-1".into()),
            }),
        )
        .await
        .unwrap();
    tokio::time::sleep(Duration::from_millis(10)).await;

    assert!(runtime
        .route(FocusedOsdRequest {
            kind: OsdKind::Volume,
            level: Some(0.5),
            muted: Some(false),
            label: "50%".into(),
        })
        .await
        .unwrap());
    let publication = publications.recv().await.unwrap();
    assert_eq!(publication.sequence, 1);
    assert_eq!(publication.visible.len(), 1);
    assert_eq!(publication.visible[0].output_id, "DP-1");
    assert_eq!(publication.visible[0].kind, OsdKind::Volume);
    assert_eq!(publication.visible[0].muted, Some(false));
    assert_eq!(publication.visible[0].label, "50%");
    tokio::time::sleep(Duration::from_millis(260)).await;
    assert_eq!(
        runtime
            .route(FocusedOsdRequest {
                kind: OsdKind::Brightness,
                level: Some(0.8),
                muted: None,
                label: "must not route to stale DP-1".into(),
            })
            .await
            .unwrap_err(),
        OsdRouteError::StaleFocus
    );
    task.abort();
}

#[tokio::test]
async fn production_runtime_routes_capability_updates_without_manual_requests() {
    let temp = tempfile::tempdir().unwrap();
    let hub = EventHub::new(full_snapshot_event(0).unwrap(), 16);
    let events = hub.subscribe().await;
    let authority = GenerationAuthority::new(
        GenerationAllocator::open(temp.path().join("generation-auto"), 16).unwrap(),
        0,
        hub,
    );
    let (runtime, task) = spawn_osd_runtime(events, 4);
    let mut publications = runtime.subscribe();

    authority
        .lock()
        .await
        .publish(
            sleepy_sdk::EventCause {
                kind: sleepy_sdk::EventCauseKind::External,
                request_id: None,
            },
            sleepy_sdk::SessionEvent::Niri(NiriEvent {
                focused_output_id: Some("DP-2".into()),
            }),
        )
        .await
        .unwrap();
    authority
        .lock()
        .await
        .publish(
            sleepy_sdk::EventCause {
                kind: sleepy_sdk::EventCauseKind::External,
                request_id: None,
            },
            sleepy_sdk::SessionEvent::CapabilityUpdate(sleepy_sdk::CapabilityRecord {
                id: sleepy_sdk::RuntimeCapabilityId::Brightness,
                status: sleepy_sdk::CapabilityAvailability::Available,
                value: Some(sleepy_sdk::CapabilityValue::Brightness(
                    sleepy_sdk::BrightnessRuntimeState { level: 0.72 },
                )),
                diagnostic: None,
            }),
        )
        .await
        .unwrap();

    let publication = tokio::time::timeout(Duration::from_millis(100), publications.recv())
        .await
        .expect("capability update was not routed into the production OSD stream")
        .unwrap();
    assert_eq!(publication.sequence, 1);
    assert_eq!(publication.visible.len(), 1);
    assert_eq!(publication.visible[0].output_id, "DP-2");
    assert_eq!(publication.visible[0].kind, OsdKind::Brightness);
    assert_eq!(publication.visible[0].level, Some(0.72));
    task.abort();
}

#[tokio::test]
async fn production_stream_routes_volume_mute_mic_brightness_media_and_profile_in_fifo_order() {
    let temp = tempfile::tempdir().unwrap();
    let hub = EventHub::new(full_snapshot_event(0).unwrap(), 32);
    let events = hub.subscribe().await;
    let authority = GenerationAuthority::new(
        GenerationAllocator::open(temp.path().join("generation-all"), 32).unwrap(),
        0,
        hub,
    );
    let (runtime, task) = spawn_osd_runtime_with_timing(
        events,
        8,
        Duration::from_millis(200),
        Duration::from_secs(2),
    );
    let mut publications = runtime.subscribe();
    publish_osd_input(
        &authority,
        sleepy_sdk::SessionEvent::Niri(NiriEvent {
            focused_output_id: Some("DP-3".into()),
        }),
    )
    .await;
    publish_osd_input(
        &authority,
        sleepy_sdk::SessionEvent::CapabilityUpdate(sleepy_sdk::CapabilityRecord {
            id: sleepy_sdk::RuntimeCapabilityId::Audio,
            status: sleepy_sdk::CapabilityAvailability::Available,
            value: Some(sleepy_sdk::CapabilityValue::Audio(
                sleepy_sdk::AudioRuntimeState {
                    output_level: 0.4,
                    output_muted: false,
                    input_level: 0.6,
                    input_muted: false,
                    default_output_id: Some("sink-1".into()),
                },
            )),
            diagnostic: None,
        }),
    )
    .await;
    let volume = publications.recv().await.unwrap();
    assert_eq!(volume.visible[0].kind, OsdKind::Volume);
    assert_eq!(volume.visible[0].level, Some(0.4));
    assert_eq!(volume.visible[0].muted, Some(false));

    publish_osd_input(
        &authority,
        sleepy_sdk::SessionEvent::CapabilityUpdate(sleepy_sdk::CapabilityRecord {
            id: sleepy_sdk::RuntimeCapabilityId::Audio,
            status: sleepy_sdk::CapabilityAvailability::Available,
            value: Some(sleepy_sdk::CapabilityValue::Audio(
                sleepy_sdk::AudioRuntimeState {
                    output_level: 0.4,
                    output_muted: true,
                    input_level: 0.6,
                    input_muted: false,
                    default_output_id: Some("sink-1".into()),
                },
            )),
            diagnostic: None,
        }),
    )
    .await;
    let muted = publications.recv().await.unwrap();
    assert_eq!(muted.visible[0].kind, OsdKind::Volume);
    assert_eq!(muted.visible[0].muted, Some(true));

    for event in [
        sleepy_sdk::SessionEvent::CapabilityUpdate(sleepy_sdk::CapabilityRecord {
            id: sleepy_sdk::RuntimeCapabilityId::Brightness,
            status: sleepy_sdk::CapabilityAvailability::Available,
            value: Some(sleepy_sdk::CapabilityValue::Brightness(
                sleepy_sdk::BrightnessRuntimeState { level: 0.7 },
            )),
            diagnostic: None,
        }),
        sleepy_sdk::SessionEvent::CapabilityUpdate(sleepy_sdk::CapabilityRecord {
            id: sleepy_sdk::RuntimeCapabilityId::Media,
            status: sleepy_sdk::CapabilityAvailability::Available,
            value: Some(sleepy_sdk::CapabilityValue::Media(
                sleepy_sdk::MediaRuntimeState {
                    player_id: "player".into(),
                    title: "Track".into(),
                    artist: "Artist".into(),
                    playing: true,
                },
            )),
            diagnostic: None,
        }),
        sleepy_sdk::SessionEvent::CapabilityUpdate(sleepy_sdk::CapabilityRecord {
            id: sleepy_sdk::RuntimeCapabilityId::PowerProfile,
            status: sleepy_sdk::CapabilityAvailability::Available,
            value: Some(sleepy_sdk::CapabilityValue::PowerProfile(
                sleepy_sdk::PowerProfileRuntimeState {
                    active: "balanced".into(),
                    available: vec!["balanced".into(), "performance".into()],
                },
            )),
            diagnostic: None,
        }),
    ] {
        publish_osd_input(&authority, event).await;
        publications.recv().await.unwrap();
    }

    for expected in [
        OsdKind::Microphone,
        OsdKind::Brightness,
        OsdKind::Media,
        OsdKind::PowerProfile,
    ] {
        let publication = tokio::time::timeout(Duration::from_millis(300), publications.recv())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(publication.visible[0].kind, expected);
        assert_eq!(publication.visible[0].output_id, "DP-3");
    }
    task.abort();
}

#[tokio::test]
async fn automatic_capability_routing_fails_closed_for_missing_and_stale_focus() {
    let temp = tempfile::tempdir().unwrap();
    let hub = EventHub::new(full_snapshot_event(0).unwrap(), 16);
    let events = hub.subscribe().await;
    let authority = GenerationAuthority::new(
        GenerationAllocator::open(temp.path().join("generation-focus"), 16).unwrap(),
        0,
        hub,
    );
    let (runtime, task) =
        spawn_osd_runtime_with_timing(events, 4, Duration::from_secs(1), Duration::from_millis(50));
    let mut publications = runtime.subscribe();
    publish_osd_input(
        &authority,
        sleepy_sdk::SessionEvent::CapabilityUpdate(sleepy_sdk::CapabilityRecord {
            id: sleepy_sdk::RuntimeCapabilityId::Brightness,
            status: sleepy_sdk::CapabilityAvailability::Available,
            value: Some(sleepy_sdk::CapabilityValue::Brightness(
                sleepy_sdk::BrightnessRuntimeState { level: 0.5 },
            )),
            diagnostic: None,
        }),
    )
    .await;
    assert!(
        tokio::time::timeout(Duration::from_millis(25), publications.recv())
            .await
            .is_err()
    );

    publish_osd_input(
        &authority,
        sleepy_sdk::SessionEvent::Niri(NiriEvent {
            focused_output_id: Some("DP-4".into()),
        }),
    )
    .await;
    publish_osd_input(
        &authority,
        sleepy_sdk::SessionEvent::CapabilityUpdate(sleepy_sdk::CapabilityRecord {
            id: sleepy_sdk::RuntimeCapabilityId::Brightness,
            status: sleepy_sdk::CapabilityAvailability::Available,
            value: Some(sleepy_sdk::CapabilityValue::Brightness(
                sleepy_sdk::BrightnessRuntimeState { level: 0.6 },
            )),
            diagnostic: None,
        }),
    )
    .await;
    let routed = publications.recv().await.unwrap();
    assert_eq!(routed.visible[0].output_id, "DP-4");
    assert_eq!(routed.visible[0].level, Some(0.6));

    tokio::time::sleep(Duration::from_millis(60)).await;
    publish_osd_input(
        &authority,
        sleepy_sdk::SessionEvent::CapabilityUpdate(sleepy_sdk::CapabilityRecord {
            id: sleepy_sdk::RuntimeCapabilityId::Brightness,
            status: sleepy_sdk::CapabilityAvailability::Available,
            value: Some(sleepy_sdk::CapabilityValue::Brightness(
                sleepy_sdk::BrightnessRuntimeState { level: 0.7 },
            )),
            diagnostic: None,
        }),
    )
    .await;
    assert!(
        tokio::time::timeout(Duration::from_millis(25), publications.recv())
            .await
            .is_err()
    );
    task.abort();
}

#[tokio::test]
async fn automatic_stream_publishes_bounded_queue_overflow() {
    let temp = tempfile::tempdir().unwrap();
    let hub = EventHub::new(full_snapshot_event(0).unwrap(), 16);
    let events = hub.subscribe().await;
    let authority = GenerationAuthority::new(
        GenerationAllocator::open(temp.path().join("generation-overflow"), 16).unwrap(),
        0,
        hub,
    );
    let (runtime, task) = spawn_osd_runtime(events, 0);
    let mut publications = runtime.subscribe();
    publish_osd_input(
        &authority,
        sleepy_sdk::SessionEvent::Niri(NiriEvent {
            focused_output_id: Some("DP-5".into()),
        }),
    )
    .await;
    publish_osd_input(
        &authority,
        sleepy_sdk::SessionEvent::CapabilityUpdate(sleepy_sdk::CapabilityRecord {
            id: sleepy_sdk::RuntimeCapabilityId::Brightness,
            status: sleepy_sdk::CapabilityAvailability::Available,
            value: Some(sleepy_sdk::CapabilityValue::Brightness(
                sleepy_sdk::BrightnessRuntimeState { level: 0.8 },
            )),
            diagnostic: None,
        }),
    )
    .await;
    publications.recv().await.unwrap();
    publish_osd_input(
        &authority,
        sleepy_sdk::SessionEvent::CapabilityUpdate(sleepy_sdk::CapabilityRecord {
            id: sleepy_sdk::RuntimeCapabilityId::Media,
            status: sleepy_sdk::CapabilityAvailability::Available,
            value: Some(sleepy_sdk::CapabilityValue::Media(
                sleepy_sdk::MediaRuntimeState {
                    player_id: "player".into(),
                    title: "Track".into(),
                    artist: "Artist".into(),
                    playing: true,
                },
            )),
            diagnostic: None,
        }),
    )
    .await;

    let overflow = tokio::time::timeout(Duration::from_millis(100), publications.recv())
        .await
        .expect("automatic queue overflow was not published")
        .unwrap();
    assert_eq!(overflow.overflow_by_output.get("DP-5"), Some(&1));
    assert_eq!(overflow.visible[0].kind, OsdKind::Brightness);
    task.abort();
}

async fn publish_osd_input(authority: &GenerationAuthority, payload: sleepy_sdk::SessionEvent) {
    authority
        .lock()
        .await
        .publish(
            sleepy_sdk::EventCause {
                kind: sleepy_sdk::EventCauseKind::External,
                request_id: None,
            },
            payload,
        )
        .await
        .unwrap();
}

#[tokio::test]
async fn osd_socket_replays_latest_then_monotonic_live_publications() {
    let temp = tempfile::tempdir().unwrap();
    let socket_path = temp.path().join("runtime/sleepy/osd.sock");
    let hub = OsdPublicationHub::new(8);
    hub.publish(OsdPublication {
        sequence: 7,
        visible: vec![OsdEvent {
            schema_version: WIRE_SCHEMA_VERSION,
            output_id: "DP-7".into(),
            kind: OsdKind::Brightness,
            level: Some(0.7),
            muted: None,
            label: "70%".into(),
        }],
        overflow_by_output: Default::default(),
    })
    .unwrap();
    let socket = std::sync::Arc::new(
        OsdSocket::bind(&socket_path, unsafe { libc::geteuid() }, hub.clone())
            .await
            .unwrap(),
    );
    let server = tokio::spawn({
        let socket = std::sync::Arc::clone(&socket);
        async move { socket.serve().await }
    });

    let stream = UnixStream::connect(&socket_path).await.unwrap();
    let mut lines = BufReader::new(stream).lines();
    let replay: OsdPublication =
        serde_json::from_str(&lines.next_line().await.unwrap().unwrap()).unwrap();
    assert_eq!(replay.sequence, 7);
    assert_eq!(replay.visible[0].output_id, "DP-7");

    hub.publish(OsdPublication {
        sequence: 8,
        visible: Vec::new(),
        overflow_by_output: Default::default(),
    })
    .unwrap();
    let live: OsdPublication =
        serde_json::from_str(&lines.next_line().await.unwrap().unwrap()).unwrap();
    assert_eq!(live.sequence, 8);

    drop(lines);
    socket
        .shutdown_and_drain(Duration::from_secs(1))
        .await
        .unwrap();
    server.await.unwrap().unwrap();
}

#[tokio::test]
async fn osd_socket_caps_stream_clients_and_rejects_the_thirty_third() {
    let temp = tempfile::tempdir().unwrap();
    let socket_path = temp.path().join("runtime/sleepy/osd.sock");
    let hub = OsdPublicationHub::new(8);
    hub.publish(OsdPublication {
        sequence: 1,
        visible: Vec::new(),
        overflow_by_output: Default::default(),
    })
    .unwrap();
    let socket = std::sync::Arc::new(
        OsdSocket::bind(&socket_path, unsafe { libc::geteuid() }, hub)
            .await
            .unwrap(),
    );
    let server = tokio::spawn({
        let socket = std::sync::Arc::clone(&socket);
        async move { socket.serve().await }
    });
    let mut clients = Vec::new();
    for _ in 0..32 {
        let mut lines = BufReader::new(UnixStream::connect(&socket_path).await.unwrap()).lines();
        assert!(
            tokio::time::timeout(Duration::from_millis(200), lines.next_line())
                .await
                .unwrap()
                .unwrap()
                .is_some()
        );
        clients.push(lines);
    }

    let mut rejected = BufReader::new(UnixStream::connect(&socket_path).await.unwrap()).lines();
    assert_eq!(
        tokio::time::timeout(Duration::from_millis(200), rejected.next_line())
            .await
            .expect("the over-limit OSD connection must not queue")
            .unwrap(),
        None
    );

    drop(clients);
    socket
        .shutdown_and_drain(Duration::from_secs(1))
        .await
        .unwrap();
    server.await.unwrap().unwrap();
}
