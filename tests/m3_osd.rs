use sleepy_sdk::{NiriEvent, OsdEvent, OsdKind, WIRE_SCHEMA_VERSION};
use sleepy_session::osd::{FocusedOsdRequest, OsdRouteError, OsdRouter};
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

#[test]
fn each_output_has_one_visible_osd_and_same_kind_updates_in_place() {
    let mut router = OsdRouter::new(2);
    router.push(event("DP-1", OsdKind::Volume, "40%"));
    router.push(event("DP-1", OsdKind::Volume, "50%"));
    router.push(event("DP-1", OsdKind::Brightness, "60%"));
    router.push(event("HDMI-A-1", OsdKind::Media, "Playing"));

    assert_eq!(router.current("DP-1").unwrap().label, "50%");
    assert_eq!(router.pending_len("DP-1"), 1);
    assert_eq!(router.current("HDMI-A-1").unwrap().kind, OsdKind::Media);
    assert_eq!(router.complete("DP-1").unwrap().kind, OsdKind::Brightness);
}

#[test]
fn bounded_queue_rejects_newest_and_records_overflow() {
    let mut router = OsdRouter::new(1);
    router.push(event("DP-1", OsdKind::Volume, "40%"));
    router.push(event("DP-1", OsdKind::Brightness, "50%"));
    assert!(!router.push(event("DP-1", OsdKind::Media, "Track")));

    assert_eq!(router.overflow_count("DP-1"), 1);
    assert_eq!(router.complete("DP-1").unwrap().kind, OsdKind::Brightness);
}

#[test]
fn timeout_advances_each_output_independently_in_fifo_order() {
    let start = Instant::now();
    let mut router =
        OsdRouter::with_timing(2, Duration::from_millis(100), Duration::from_millis(250));
    router.push_at(event("DP-1", OsdKind::Volume, "40%"), start);
    router.push_at(event("DP-1", OsdKind::Brightness, "50%"), start);
    router.push_at(event("HDMI-A-1", OsdKind::Media, "Playing"), start);

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
    router.push_at(event("HDMI-A-1", OsdKind::Media, "Playing"), start);
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
