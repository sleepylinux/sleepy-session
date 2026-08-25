use sleepy_sdk::{NiriEvent, OsdEvent, OsdKind, WIRE_SCHEMA_VERSION};
use sleepy_session::{
    osd::{spawn_osd_runtime, FocusedOsdRequest, OsdRouteError, OsdRouter},
    sessiond::{full_snapshot_event, EventHub, GenerationAllocator, GenerationAuthority},
};
use std::time::{Duration, Instant};

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
