use std::{
    collections::VecDeque,
    fs, io,
    sync::{Arc, Mutex},
    time::Duration,
};

use sleepy_sdk::{CacheStatus, DaemonCommand, ProviderStatus, WeatherLocation};
use sleepy_session::{
    calendar::IcsCalendarProvider,
    daily::{DailyBackend, DailyOperation, DailyResponse, DailySocket, DailyStatus},
    launcher::{DesktopEntryIndex, LaunchResources, LauncherMetrics},
    overview::{
        overview_event_channel, NiriOverview, ObservedOverviewEvent, OverviewEvent,
        OverviewEventSender, OverviewEventSource, OverviewRunner, ProcessOverviewRunner,
    },
    system::{CommandOutput, CommandRunner, CommandSpec, RunControl},
    weather::{
        HttpRequest, HttpResponse, HttpTransport, ManualClock, MetNoProvider, NominatimProvider,
    },
};
use tempfile::tempdir;

#[test]
fn desktop_index_honors_precedence_visibility_tryexec_and_actions() {
    let root = tempdir().unwrap();
    let high = root.path().join("high");
    let low = root.path().join("low");
    fs::create_dir_all(high.join("sub")).unwrap();
    fs::create_dir_all(&low).unwrap();
    fs::write(low.join("sub-demo.desktop"), entry("Low", "low-app %U", "")).unwrap();
    fs::write(
        high.join("sub/demo.desktop"),
        entry(
            "High",
            "safe-app --name %c %F",
            "OnlyShowIn=Sleepy;\nActions=New;\n[Desktop Action New]\nName=New window\nExec=safe-app --new %u\n",
        ),
    )
    .unwrap();
    fs::write(
        high.join("hidden.desktop"),
        entry("Hidden", "safe-app", "Hidden=true\n"),
    )
    .unwrap();
    fs::write(low.join("masked.desktop"), entry("Old", "safe-app", "")).unwrap();
    fs::write(
        high.join("masked.desktop"),
        entry("Masked", "safe-app", "Hidden=true\n"),
    )
    .unwrap();
    fs::write(
        high.join("foreign.desktop"),
        entry("Foreign", "safe-app", "NotShowIn=Sleepy;\n"),
    )
    .unwrap();
    fs::write(
        high.join("missing.desktop"),
        entry("Missing", "missing-app", "TryExec=definitely-not-here\n"),
    )
    .unwrap();

    let index = DesktopEntryIndex::scan(&[high, low], &["Sleepy".into()], |program| {
        program == "safe-app"
    })
    .unwrap();
    let entries = index.entries();
    assert_eq!(entries.len(), 1);
    assert_eq!(entries[0].desktop_id, "sub-demo.desktop");
    assert_eq!(entries[0].name, "High");
    assert_eq!(entries[0].actions[0].id, "New");
    assert!(
        index.get("masked.desktop").is_none(),
        "Hidden override is a tombstone"
    );
}

#[test]
fn desktop_exec_expands_field_codes_to_inert_argv_and_rejects_malformed_input() {
    let root = tempdir().unwrap();
    fs::write(
        root.path().join("safe.desktop"),
        entry("Safe App", "safe-app --title=%c %F %%", "Icon=safe-icon\n"),
    )
    .unwrap();
    fs::write(
        root.path().join("bad.desktop"),
        entry("Bad", "sh -c \"unterminated", ""),
    )
    .unwrap();
    let index = DesktopEntryIndex::scan(&[root.path().into()], &[], |_| true).unwrap();
    assert!(index.get("bad.desktop").is_none());
    let argv = index
        .launch_argv(
            "safe.desktop",
            None,
            &LaunchResources {
                files: vec!["$(touch /tmp/pwned)".into(), "; rm -rf nope".into()],
                urls: vec![],
            },
        )
        .unwrap();
    assert_eq!(
        argv,
        vec![
            "safe-app",
            "--title=Safe App",
            "$(touch /tmp/pwned)",
            "; rm -rf nope",
            "%"
        ]
    );

    let action = index
        .launch_argv(
            "safe.desktop",
            None,
            &LaunchResources {
                files: vec![],
                urls: vec!["https://example.test/a?x=$(bad)".into()],
            },
        )
        .unwrap();
    assert_eq!(action[0], "safe-app");
}

#[test]
fn desktop_exec_supports_icon_url_path_and_action_without_a_shell() {
    let root = tempdir().unwrap();
    fs::write(
        root.path().join("codes.desktop"),
        entry(
            "Codes",
            "viewer %i %u %k",
            "Icon=codes\nActions=Edit;\n[Desktop Action Edit]\nName=Edit\nExec=viewer --edit %f\n",
        ),
    )
    .unwrap();
    let index = DesktopEntryIndex::scan(&[root.path().into()], &[], |_| true).unwrap();
    let resources = LaunchResources {
        files: vec![";echo owned".into()],
        urls: vec!["https://example.test/$(owned)".into()],
    };
    let argv = index
        .launch_argv("codes.desktop", None, &resources)
        .unwrap();
    assert_eq!(argv[0], "viewer");
    assert_eq!(&argv[1..3], ["--icon", "codes"]);
    assert_eq!(argv[3], "https://example.test/$(owned)");
    assert!(argv[4].ends_with("codes.desktop"));
    assert_eq!(
        index
            .launch_argv("codes.desktop", Some("Edit"), &resources)
            .unwrap(),
        ["viewer", "--edit", ";echo owned"]
    );
}

#[test]
fn launcher_metrics_are_private_persistent_and_ranking_is_deterministic() {
    let root = tempdir().unwrap();
    let path = root.path().join("sleepy/launcher.json");
    let mut metrics = LauncherMetrics::open(&path).unwrap();
    metrics.record_launch("alpha.desktop", 20).unwrap();
    metrics.record_launch("alpha.desktop", 30).unwrap();
    metrics.record_launch("alpine.desktop", 40).unwrap();
    let mode = fs::metadata(&path).unwrap().permissions();
    use std::os::unix::fs::PermissionsExt;
    assert_eq!(mode.mode() & 0o777, 0o600);
    let reopened = LauncherMetrics::open(&path).unwrap();
    assert_eq!(
        reopened.rank("alp", &["alpine.desktop", "alpha.desktop"]),
        vec!["alpha.desktop", "alpine.desktop"]
    );
}

#[derive(Clone, Default)]
struct FakeOverviewRunner(Arc<Mutex<Vec<Vec<String>>>>);

impl OverviewRunner for FakeOverviewRunner {
    fn run(
        &self,
        program: &str,
        args: &[String],
        _timeout: Duration,
    ) -> io::Result<std::time::Instant> {
        let started_at = std::time::Instant::now();
        assert_eq!(program, "niri");
        self.0.lock().unwrap().push(args.to_vec());
        Ok(started_at)
    }
}

struct FakeOverviewEvents(Mutex<VecDeque<OverviewEvent>>);

impl OverviewEventSource for FakeOverviewEvents {
    fn next_event(&self, _timeout: Duration) -> io::Result<Option<ObservedOverviewEvent>> {
        Ok(self
            .0
            .lock()
            .unwrap()
            .pop_front()
            .map(ObservedOverviewEvent::now))
    }
}

#[test]
fn overview_uses_fixed_argv_and_waits_for_a_matching_event() {
    let runner = FakeOverviewRunner::default();
    let events = FakeOverviewEvents(Mutex::new(VecDeque::from([
        OverviewEvent::FocusChanged {
            output_id: "DP-1".into(),
            window_id: Some(8),
            workspace_id: 2,
            sequence: 9,
        },
        OverviewEvent::FocusChanged {
            output_id: "HDMI-A-1".into(),
            window_id: Some(42),
            workspace_id: 3,
            sequence: 11,
        },
    ])));
    let mut overview = NiriOverview::new(runner.clone(), events, Duration::from_millis(50));
    overview.observe(OverviewEvent::FocusChanged {
        output_id: "HDMI-A-1".into(),
        window_id: Some(8),
        workspace_id: 2,
        sequence: 10,
    });
    let event = overview
        .execute(DaemonCommand::FocusWindow { window_id: 42 })
        .unwrap();
    assert_eq!(event.sequence(), 11);
    assert_eq!(
        runner.0.lock().unwrap()[0],
        ["msg", "action", "focus-window", "--id", "42"]
    );
}

#[test]
fn overview_rejects_offline_stale_and_wrong_output_confirmations() {
    let runner = FakeOverviewRunner::default();
    let events = FakeOverviewEvents(Mutex::new(VecDeque::from([OverviewEvent::FocusChanged {
        output_id: "DP-2".into(),
        window_id: Some(42),
        workspace_id: 1,
        sequence: 11,
    }])));
    let mut overview = NiriOverview::new(runner, events, Duration::from_millis(5));
    overview.observe(OverviewEvent::Offline { sequence: 10 });
    assert!(overview
        .execute(DaemonCommand::CloseWindow { window_id: 1 })
        .is_err());
}

#[test]
fn overview_confirms_close_and_workspace_only_on_fresh_matching_output() {
    let runner = FakeOverviewRunner::default();
    let events = FakeOverviewEvents(Mutex::new(VecDeque::from([
        OverviewEvent::FocusChanged {
            output_id: "DP-2".into(),
            window_id: Some(9),
            workspace_id: 7,
            sequence: 2,
        },
        OverviewEvent::FocusChanged {
            output_id: "DP-1".into(),
            window_id: Some(9),
            workspace_id: 7,
            sequence: 3,
        },
    ])));
    let mut overview = NiriOverview::new(runner.clone(), events, Duration::from_millis(10));
    overview.observe(OverviewEvent::FocusChanged {
        output_id: "DP-1".into(),
        window_id: Some(9),
        workspace_id: 1,
        sequence: 1,
    });
    assert_eq!(
        overview
            .execute(DaemonCommand::FocusWorkspace { workspace_id: 7 })
            .unwrap()
            .sequence(),
        3
    );
    assert_eq!(
        runner.0.lock().unwrap()[0],
        ["msg", "action", "focus-workspace", "7"]
    );
}

#[test]
fn focus_window_can_confirm_on_the_windows_actual_new_output() {
    let events = FakeOverviewEvents(Mutex::new(VecDeque::from([OverviewEvent::FocusChanged {
        output_id: "DP-2".into(),
        window_id: Some(42),
        workspace_id: 9,
        sequence: 2,
    }])));
    let mut overview = NiriOverview::new(
        FakeOverviewRunner::default(),
        events,
        Duration::from_millis(10),
    );
    overview.observe(OverviewEvent::FocusChanged {
        output_id: "DP-1".into(),
        window_id: Some(1),
        workspace_id: 1,
        sequence: 1,
    });
    match overview
        .execute(DaemonCommand::FocusWindow { window_id: 42 })
        .unwrap()
    {
        OverviewEvent::FocusChanged { output_id, .. } => assert_eq!(output_id, "DP-2"),
        _ => panic!("unexpected confirmation"),
    }
}

#[derive(Clone)]
struct PublishingOverviewRunner(OverviewEventSender);

impl OverviewRunner for PublishingOverviewRunner {
    fn run(
        &self,
        _program: &str,
        _args: &[String],
        _timeout: Duration,
    ) -> io::Result<std::time::Instant> {
        let started_at = std::time::Instant::now();
        self.0.publish(OverviewEvent::FocusChanged {
            output_id: "DP-2".into(),
            window_id: Some(42),
            workspace_id: 9,
            sequence: 2,
        })?;
        Ok(started_at)
    }
}

#[derive(Clone)]
struct CausalCommandRunner(OverviewEventSender);

impl CommandRunner for CausalCommandRunner {
    fn run(
        &self,
        _command: &CommandSpec,
    ) -> Result<CommandOutput, sleepy_session::system::RunnerError> {
        unreachable!("causal overview must use the controlled start-reporting seam")
    }

    fn run_controlled_started(
        &self,
        command: &CommandSpec,
        _control: &RunControl,
    ) -> Result<(CommandOutput, std::time::Instant), sleepy_session::system::RunnerError> {
        assert_eq!(command.program, "niri");
        assert_eq!(
            command.args,
            ["msg", "action", "focus-window", "--id", "42"]
        );
        self.0
            .publish(OverviewEvent::FocusChanged {
                output_id: "DP-1".into(),
                window_id: Some(42),
                workspace_id: 1,
                sequence: 2,
            })
            .unwrap();
        let command_started = std::time::Instant::now();
        self.0
            .publish(OverviewEvent::FocusChanged {
                output_id: "DP-1".into(),
                window_id: Some(42),
                workspace_id: 1,
                sequence: 3,
            })
            .unwrap();
        Ok((
            CommandOutput {
                status: 0,
                stdout: Vec::new(),
                stderr: Vec::new(),
            },
            command_started,
        ))
    }
}

#[test]
fn overview_rejects_matching_event_observed_before_actual_command_start() {
    let (sender, events) = overview_event_channel(8);
    sender
        .publish(OverviewEvent::FocusChanged {
            output_id: "DP-1".into(),
            window_id: Some(1),
            workspace_id: 1,
            sequence: 1,
        })
        .unwrap();
    let mut overview = NiriOverview::new(
        ProcessOverviewRunner(CausalCommandRunner(sender)),
        events,
        Duration::from_millis(50),
    );
    assert_eq!(
        overview
            .execute(DaemonCommand::FocusWindow { window_id: 42 })
            .unwrap()
            .sequence(),
        3
    );
}

#[derive(Clone)]
struct MultiOutputPublishingRunner(OverviewEventSender);

impl OverviewRunner for MultiOutputPublishingRunner {
    fn run(
        &self,
        _program: &str,
        _args: &[String],
        _timeout: Duration,
    ) -> io::Result<std::time::Instant> {
        let started_at = std::time::Instant::now();
        self.0.publish(OverviewEvent::FocusChanged {
            output_id: "DP-2".into(),
            window_id: Some(9),
            workspace_id: 7,
            sequence: 3,
        })?;
        self.0.publish(OverviewEvent::FocusChanged {
            output_id: "DP-1".into(),
            window_id: Some(9),
            workspace_id: 7,
            sequence: 4,
        })?;
        Ok(started_at)
    }
}

#[test]
fn overview_folds_all_retained_events_before_routing_command() {
    let (sender, events) = overview_event_channel(8);
    sender
        .publish(OverviewEvent::FocusChanged {
            output_id: "DP-1".into(),
            window_id: Some(9),
            workspace_id: 1,
            sequence: 1,
        })
        .unwrap();
    sender
        .publish(OverviewEvent::WindowClosed {
            window_id: 100,
            sequence: 2,
        })
        .unwrap();
    let mut overview = NiriOverview::new(
        MultiOutputPublishingRunner(sender),
        events,
        Duration::from_millis(50),
    );
    assert_eq!(
        overview
            .execute(DaemonCommand::FocusWorkspace { workspace_id: 7 })
            .unwrap()
            .sequence(),
        4
    );
}

#[test]
fn production_channel_requires_an_event_published_after_command_submission() {
    let (sender, events) = overview_event_channel(8);
    sender
        .publish(OverviewEvent::FocusChanged {
            output_id: "DP-1".into(),
            window_id: Some(42),
            workspace_id: 1,
            sequence: 1,
        })
        .unwrap();
    let mut overview = NiriOverview::new(
        PublishingOverviewRunner(sender),
        events,
        Duration::from_millis(50),
    );
    let confirmed = overview
        .execute(DaemonCommand::FocusWindow { window_id: 42 })
        .unwrap();
    assert_eq!(confirmed.sequence(), 2);
}

#[test]
fn overview_channel_reports_lag_instead_of_retaining_stale_and_dropping_newest() {
    let (sender, events) = overview_event_channel(1);
    sender
        .publish(OverviewEvent::FocusChanged {
            output_id: "old".into(),
            window_id: Some(1),
            workspace_id: 1,
            sequence: 1,
        })
        .unwrap();
    sender
        .publish(OverviewEvent::FocusChanged {
            output_id: "new".into(),
            window_id: Some(2),
            workspace_id: 2,
            sequence: 2,
        })
        .unwrap();
    assert_eq!(
        events.try_event().unwrap_err().kind(),
        io::ErrorKind::InvalidData
    );
}

#[test]
fn launcher_bounds_resource_and_directory_amplification() {
    let root = tempdir().unwrap();
    fs::write(
        root.path().join("safe.desktop"),
        entry("Safe", "safe %F", ""),
    )
    .unwrap();
    let index = DesktopEntryIndex::scan(&[root.path().into()], &[], |_| true).unwrap();
    assert!(index
        .launch_argv(
            "safe.desktop",
            None,
            &LaunchResources {
                files: vec!["x".into(); 257],
                urls: vec![]
            }
        )
        .is_err());
    let deep = tempdir().unwrap();
    let mut path = deep.path().to_owned();
    for _ in 0..34 {
        path.push("d");
        fs::create_dir(&path).unwrap();
    }
    assert!(DesktopEntryIndex::scan(&[deep.path().into()], &[], |_| true).is_err());
}

#[test]
fn ics_expands_recurrence_exclusions_and_isolates_bad_sources() {
    let root = tempdir().unwrap();
    fs::write(
        root.path().join("good.ics"),
        "BEGIN:VCALENDAR\r\nBEGIN:VEVENT\r\nUID:daily\r\nSUMMARY:Standup\r\nDTSTART;TZID=Europe/Prague:20260825T090000\r\nDTEND;TZID=Europe/Prague:20260825T093000\r\nRRULE:FREQ=DAILY;COUNT=4\r\nEXDATE;TZID=Europe/Prague:20260827T090000\r\nRDATE;TZID=Europe/Prague:20260830T090000\r\nEND:VEVENT\r\nBEGIN:VEVENT\r\nUID:holiday\r\nSUMMARY:Holiday\r\nDTSTART;VALUE=DATE:20260826\r\nDTEND;VALUE=DATE:20260827\r\nEND:VEVENT\r\nEND:VCALENDAR\r\n",
    )
    .unwrap();
    fs::write(root.path().join("bad.ics"), "BEGIN:VEVENT\nDTSTART:nope\n").unwrap();
    let provider = IcsCalendarProvider::new(
        vec![root.path().join("bad.ics"), root.path().join("good.ics")],
        32,
    );
    let snapshot = provider
        .snapshot("2026-08-24T00:00:00Z", "2026-09-01T00:00:00Z")
        .unwrap();
    assert_eq!(snapshot.source_errors.len(), 1);
    assert_eq!(
        snapshot
            .events
            .iter()
            .filter(|event| event.id.starts_with("daily"))
            .count(),
        4
    );
    assert!(snapshot.events.iter().any(|event| event.all_day));
    assert!(snapshot
        .events
        .iter()
        .all(|event| event.starts_at.ends_with('Z')));
    assert!(snapshot
        .events
        .iter()
        .any(|event| event.starts_at == "2026-08-25T07:00:00Z"));
}

#[test]
fn ics_rejects_unbounded_expansion_without_hiding_other_sources() {
    let root = tempdir().unwrap();
    fs::write(root.path().join("huge.ics"), "BEGIN:VCALENDAR\nBEGIN:VEVENT\nUID:x\nSUMMARY:X\nDTSTART:20260801T000000Z\nDTEND:20260801T010000Z\nRRULE:FREQ=DAILY;COUNT=999999\nEND:VEVENT\nEND:VCALENDAR\n").unwrap();
    fs::write(root.path().join("one.ics"), "BEGIN:VCALENDAR\nBEGIN:VEVENT\nUID:y\nSUMMARY:Y\nDTSTART:20260825T000000Z\nDTEND:20260825T010000Z\nEND:VEVENT\nEND:VCALENDAR\n").unwrap();
    let snapshot = IcsCalendarProvider::new(
        vec![root.path().join("huge.ics"), root.path().join("one.ics")],
        16,
    )
    .snapshot("2026-08-01T00:00:00Z", "2026-09-01T00:00:00Z")
    .unwrap();
    assert_eq!(snapshot.events.len(), 1);
    assert_eq!(snapshot.source_errors.len(), 1);
}

#[test]
fn ics_recurs_at_local_civil_time_across_dst() {
    let root = tempdir().unwrap();
    fs::write(root.path().join("dst.ics"), "BEGIN:VCALENDAR\nBEGIN:VEVENT\nUID:dst\nSUMMARY:DST\nDTSTART;TZID=Europe/Prague:20261024T090000\nDTEND;TZID=Europe/Prague:20261024T093000\nRRULE:FREQ=DAILY;COUNT=3\nEND:VEVENT\nEND:VCALENDAR\n").unwrap();
    let snapshot = IcsCalendarProvider::new(vec![root.path().join("dst.ics")], 8)
        .snapshot("2026-10-23T00:00:00Z", "2026-10-28T00:00:00Z")
        .unwrap();
    let starts = snapshot
        .events
        .iter()
        .map(|event| event.starts_at.as_str())
        .collect::<Vec<_>>();
    assert_eq!(
        starts,
        [
            "2026-10-24T07:00:00Z",
            "2026-10-25T08:00:00Z",
            "2026-10-26T08:00:00Z"
        ]
    );
}

#[derive(Clone, Default)]
struct FakeHttp {
    calls: Arc<Mutex<Vec<HttpRequest>>>,
    responses: Arc<Mutex<VecDeque<io::Result<HttpResponse>>>>,
}

impl HttpTransport for FakeHttp {
    fn execute(&self, request: HttpRequest) -> io::Result<HttpResponse> {
        self.calls.lock().unwrap().push(request);
        self.responses.lock().unwrap().pop_front().unwrap()
    }
}

fn response(status: u16, headers: &[(&str, &str)], body: &str) -> io::Result<HttpResponse> {
    Ok(HttpResponse {
        status,
        headers: headers
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect(),
        body: body.as_bytes().to_vec(),
    })
}

const MET_BODY: &str = r#"{"properties":{"timeseries":[{"time":"2026-08-25T12:00:00Z","data":{"instant":{"details":{"air_temperature":18.5}},"next_1_hours":{"summary":{"symbol_code":"clearsky_day"}}}}]}}"#;

#[test]
fn met_no_uses_https_identifying_headers_rounding_and_conditional_cache() {
    let root = tempdir().unwrap();
    let http = FakeHttp::default();
    http.responses.lock().unwrap().push_back(response(
        200,
        &[("Expires", "200"), ("Last-Modified", "stamp")],
        MET_BODY,
    ));
    http.responses
        .lock()
        .unwrap()
        .push_back(response(304, &[("Expires", "400")], ""));
    let clock = ManualClock::new(100);
    let provider = MetNoProvider::new(
        "https://api.met.no/weatherapi/locationforecast/2.0/compact",
        "SleepyLinux/3 contact@example.test",
        root.path().join("met.json"),
        http.clone(),
        clock.clone(),
    )
    .unwrap();
    let location = WeatherLocation {
        display_name: "Prague".into(),
        latitude: 50.075538,
        longitude: 14.4378,
    };
    assert_eq!(
        provider.snapshot(&location).unwrap().cache,
        CacheStatus::Fresh
    );
    assert_eq!(
        provider.snapshot(&location).unwrap().cache,
        CacheStatus::Fresh
    );
    assert_eq!(
        http.calls.lock().unwrap().len(),
        1,
        "must not refresh before Expires"
    );
    clock.set(201);
    assert_eq!(
        provider.snapshot(&location).unwrap().status,
        ProviderStatus::Online
    );
    let calls = http.calls.lock().unwrap();
    assert!(calls[0].url.contains("lat=50.0755&lon=14.4378"));
    assert_eq!(
        calls[0].headers.get("User-Agent").unwrap(),
        "SleepyLinux/3 contact@example.test"
    );
    assert_eq!(calls[1].headers.get("If-Modified-Since").unwrap(), "stamp");
}

#[test]
fn met_no_preserves_safe_cache_as_stale_on_offline_and_429() {
    let root = tempdir().unwrap();
    let http = FakeHttp::default();
    http.responses
        .lock()
        .unwrap()
        .push_back(response(203, &[("Expires", "101")], MET_BODY));
    http.responses
        .lock()
        .unwrap()
        .push_back(response(429, &[], "busy"));
    let clock = ManualClock::new(100);
    let provider = MetNoProvider::new(
        "https://example.test/compact",
        "Sleepy/3 ops@example.test",
        root.path().join("met.json"),
        http,
        clock.clone(),
    )
    .unwrap();
    let location = WeatherLocation {
        display_name: "P".into(),
        latitude: 1.0,
        longitude: 2.0,
    };
    provider.snapshot(&location).unwrap();
    clock.set(102);
    let stale = provider.snapshot(&location).unwrap();
    assert_eq!(stale.status, ProviderStatus::Offline);
    assert_eq!(stale.cache, CacheStatus::Stale);
    assert_eq!(stale.forecast.len(), 1);
}

#[test]
fn met_no_preserves_cache_on_malformed_refresh_and_reopens_it() {
    let root = tempdir().unwrap();
    let path = root.path().join("met.json");
    let http = FakeHttp::default();
    http.responses
        .lock()
        .unwrap()
        .push_back(response(200, &[("Expires", "2")], MET_BODY));
    http.responses
        .lock()
        .unwrap()
        .push_back(response(200, &[], "{bad"));
    let clock = ManualClock::new(1);
    let location = WeatherLocation {
        display_name: "P".into(),
        latitude: 1.0,
        longitude: 2.0,
    };
    let provider = MetNoProvider::new(
        "https://example.test/compact",
        "Sleepy/3 ops@example.test",
        path.clone(),
        http,
        clock.clone(),
    )
    .unwrap();
    provider.snapshot(&location).unwrap();
    clock.set(3);
    let stale = provider.snapshot(&location).unwrap();
    assert_eq!(
        (stale.status, stale.cache, stale.forecast.len()),
        (ProviderStatus::Error, CacheStatus::Stale, 1)
    );

    let offline = FakeHttp::default();
    offline
        .responses
        .lock()
        .unwrap()
        .push_back(Err(io::Error::new(io::ErrorKind::NotConnected, "offline")));
    let reopened = MetNoProvider::new(
        "https://example.test/compact",
        "Sleepy/3 ops@example.test",
        path,
        offline,
        clock,
    )
    .unwrap();
    assert_eq!(reopened.snapshot(&location).unwrap().forecast.len(), 1);
}

#[test]
fn provider_configuration_rejects_http_and_generic_user_agents() {
    let root = tempdir().unwrap();
    let http = FakeHttp::default();
    let clock = ManualClock::new(0);
    assert!(MetNoProvider::new(
        "http://api.met.no/compact",
        "Sleepy/3 ops@example.test",
        root.path().join("a"),
        http.clone(),
        clock.clone()
    )
    .is_err());
    assert!(NominatimProvider::new(
        "https://example.test/search",
        "reqwest",
        root.path().join("b"),
        http,
        clock
    )
    .is_err());
}

#[test]
fn calendar_and_weather_implement_the_merged_sdk_provider_traits() {
    fn calendar<P: sleepy_sdk::CalendarProvider>(_provider: &P) {}
    fn weather<P: sleepy_sdk::WeatherProvider>(_provider: &P) {}
    let root = tempdir().unwrap();
    calendar(&IcsCalendarProvider::new(Vec::new(), 8));
    weather(
        &MetNoProvider::new(
            "https://example.test/compact",
            "Sleepy/3 ops@example.test",
            root.path().join("met.json"),
            FakeHttp::default(),
            ManualClock::new(0),
        )
        .unwrap(),
    );
}

#[test]
fn nominatim_requires_submit_rate_limits_caches_and_attributes() {
    let root = tempdir().unwrap();
    let http = FakeHttp::default();
    http.responses.lock().unwrap().push_back(response(
        200,
        &[],
        r#"[{"place_id":123,"display_name":"Prague, Czechia","lat":"50.08","lon":"14.44","type":"city"}]"#,
    ));
    let clock = ManualClock::new(10);
    let provider = NominatimProvider::new(
        "https://nominatim.openstreetmap.org/search",
        "Sleepy/3 contact@example.test",
        root.path().join("geo.json"),
        http.clone(),
        clock.clone(),
    )
    .unwrap();
    assert!(provider.autocomplete("Pra").is_err());
    let first = provider.submit("Prague").unwrap();
    assert_eq!(first[0].display_name, "Prague, Czechia");
    assert!(first[0].attribution.contains("OpenStreetMap"));
    assert!(
        provider.submit("Brno").is_err(),
        "global limiter must reject faster than 1 req/s"
    );
    let cached = provider.submit("Prague").unwrap();
    assert_eq!(cached, first);
    assert_eq!(http.calls.lock().unwrap().len(), 1);
    clock.set(11);
    assert!(
        provider.submit("email@example.com").is_err(),
        "sensitive text is out of contract"
    );
}

#[test]
fn met_cache_rejects_header_injection_and_implausible_expiry() {
    use std::os::unix::fs::PermissionsExt;
    let root = tempdir().unwrap();
    let path = root.path().join("met.json");
    fs::write(&path, r#"{"schemaVersion":1,"locationKey":"1,2","expiresAt":9999999999,"lastModified":"ok\nInjected: yes","forecast":[]}"#).unwrap();
    fs::set_permissions(&path, fs::Permissions::from_mode(0o600)).unwrap();
    assert!(MetNoProvider::new(
        "https://example.test/compact",
        "Sleepy/3 ops@example.test",
        path,
        FakeHttp::default(),
        ManualClock::new(1)
    )
    .is_err());
}

#[test]
fn provider_caches_reject_noncanonical_sdk_values() {
    use std::os::unix::fs::PermissionsExt;
    let root = tempdir().unwrap();
    let met = root.path().join("met-invalid.json");
    fs::write(&met, r#"{"schemaVersion":1,"locationKey":"91,2","expiresAt":2,"lastModified":null,"forecast":[{"at":"2026-08-25T14:00:00+02:00","temperatureC":18.0,"symbol":"clear"}]}"#).unwrap();
    fs::set_permissions(&met, fs::Permissions::from_mode(0o600)).unwrap();
    assert!(MetNoProvider::new(
        "https://example.test/compact",
        "Sleepy/3 ops@example.test",
        met,
        FakeHttp::default(),
        ManualClock::new(1)
    )
    .is_err());

    let geocode = root.path().join("geo-invalid.json");
    fs::write(&geocode, r#"{"schemaVersion":1,"queries":{"prague":[{"displayName":"Prague","latitude":91.0,"longitude":14.4,"attribution":"wrong"}]}}"#).unwrap();
    fs::set_permissions(&geocode, fs::Permissions::from_mode(0o600)).unwrap();
    assert!(NominatimProvider::new(
        "https://example.test/search",
        "Sleepy/3 ops@example.test",
        geocode,
        FakeHttp::default(),
        ManualClock::new(1)
    )
    .is_err());
}

#[test]
fn calendar_rejects_aggregate_source_amplification() {
    let sources = (0..1025)
        .map(|index| std::path::PathBuf::from(format!("missing-{index}.ics")))
        .collect();
    assert!(IcsCalendarProvider::new(sources, 8)
        .snapshot("2026-08-01T00:00:00Z", "2026-09-01T00:00:00Z")
        .is_err());
}

#[test]
fn calendar_honors_request_cancellation_and_stops_incremental_expansion() {
    use std::sync::atomic::AtomicBool;
    let root = tempdir().unwrap();
    fs::write(root.path().join("many.ics"), "BEGIN:VCALENDAR\nBEGIN:VEVENT\nUID:x\nSUMMARY:X\nDTSTART:20260801T000000Z\nDTEND:20260801T010000Z\nRRULE:FREQ=DAILY;COUNT=4096\nEND:VEVENT\nEND:VCALENDAR\n").unwrap();
    let cancelled = Arc::new(AtomicBool::new(true));
    let control = sleepy_session::system::RunControl::for_request(
        std::time::Instant::now() + Duration::from_secs(5),
        cancelled,
    );
    let error = IcsCalendarProvider::new(vec![root.path().join("many.ics")], 4096)
        .snapshot_controlled("2026-08-01T00:00:00Z", "2038-01-01T00:00:00Z", &control)
        .unwrap_err();
    assert_eq!(error.kind(), io::ErrorKind::Interrupted);
}

#[test]
fn calendar_enforces_serialized_budget_during_not_after_event_accumulation() {
    let root = tempdir().unwrap();
    let summary = "x".repeat(100_000);
    fs::write(root.path().join("large.ics"), format!("BEGIN:VCALENDAR\nBEGIN:VEVENT\nUID:x\nSUMMARY:{summary}\nDTSTART:20260801T000000Z\nDTEND:20260801T010000Z\nRRULE:FREQ=DAILY;COUNT=30\nEND:VEVENT\nEND:VCALENDAR\n")).unwrap();
    let snapshot = IcsCalendarProvider::new(vec![root.path().join("large.ics")], 64)
        .snapshot("2026-08-01T00:00:00Z", "2026-09-15T00:00:00Z")
        .unwrap();
    assert!(snapshot.events.is_empty());
    assert_eq!(snapshot.source_errors.len(), 1);
    assert_eq!(
        snapshot.source_errors[0].message,
        "calendar aggregate byte budget exceeded"
    );
    assert!(serde_json::to_vec(&snapshot).unwrap().len() <= 2 * 1024 * 1024);
}

#[test]
fn calendar_isolates_oversized_source_and_keeps_later_valid_events() {
    let root = tempdir().unwrap();
    let summary = "x".repeat(100_000);
    fs::write(root.path().join("a-large.ics"), format!("BEGIN:VCALENDAR\nBEGIN:VEVENT\nUID:large\nSUMMARY:{summary}\nDTSTART:20260801T000000Z\nDTEND:20260801T010000Z\nRRULE:FREQ=DAILY;COUNT=30\nEND:VEVENT\nEND:VCALENDAR\n")).unwrap();
    fs::write(root.path().join("b-valid.ics"), "BEGIN:VCALENDAR\nBEGIN:VEVENT\nUID:valid\nSUMMARY:Visible\nDTSTART:20260802T000000Z\nDTEND:20260802T010000Z\nEND:VEVENT\nEND:VCALENDAR\n").unwrap();
    let snapshot = IcsCalendarProvider::new(
        vec![
            root.path().join("a-large.ics"),
            root.path().join("b-valid.ics"),
        ],
        64,
    )
    .snapshot("2026-08-01T00:00:00Z", "2026-09-15T00:00:00Z")
    .unwrap();
    assert_eq!(snapshot.events.len(), 1);
    assert_eq!(snapshot.events[0].summary, "Visible");
    assert_eq!(snapshot.source_errors.len(), 1);
    assert_eq!(snapshot.source_errors[0].source_id, "a-large.ics");
}

fn entry(name: &str, exec: &str, extra: &str) -> String {
    format!("[Desktop Entry]\nType=Application\nName={name}\nExec={exec}\n{extra}")
}

struct FakeDaily;
impl DailyBackend for FakeDaily {
    fn handle_controlled(
        &self,
        operation: DailyOperation,
        _control: &sleepy_session::system::RunControl,
    ) -> io::Result<serde_json::Value> {
        match operation {
            DailyOperation::LauncherSearch { query } => Ok(serde_json::json!({"query": query})),
            _ => Err(io::Error::new(io::ErrorKind::Unsupported, "not configured")),
        }
    }
}

struct ControlledDaily {
    active: std::sync::atomic::AtomicUsize,
    peak: std::sync::atomic::AtomicUsize,
}

struct LargeDaily {
    completed: std::sync::atomic::AtomicBool,
}

impl DailyBackend for LargeDaily {
    fn handle_controlled(
        &self,
        _operation: DailyOperation,
        _control: &sleepy_session::system::RunControl,
    ) -> io::Result<serde_json::Value> {
        self.completed
            .store(true, std::sync::atomic::Ordering::SeqCst);
        Ok(serde_json::Value::String("x".repeat(4 * 1024 * 1024)))
    }
}

impl DailyBackend for ControlledDaily {
    fn handle_controlled(
        &self,
        _operation: DailyOperation,
        control: &sleepy_session::system::RunControl,
    ) -> io::Result<serde_json::Value> {
        use std::sync::atomic::Ordering;
        let active = self.active.fetch_add(1, Ordering::SeqCst) + 1;
        self.peak.fetch_max(active, Ordering::SeqCst);
        while !control.is_cancelled() && !control.remaining().is_zero() {
            std::thread::sleep(Duration::from_millis(2));
        }
        self.active.fetch_sub(1, Ordering::SeqCst);
        Err(io::Error::new(io::ErrorKind::Interrupted, "cancelled"))
    }
}

#[tokio::test]
async fn daily_socket_is_private_uid_checked_strict_and_reachable() {
    use tokio::io::{AsyncBufReadExt, AsyncWriteExt};
    let root = tempdir().unwrap();
    let path = root.path().join("sleepy/daily.sock");
    let socket = Arc::new(
        DailySocket::bind(&path, unsafe { libc::geteuid() }, Arc::new(FakeDaily))
            .await
            .unwrap(),
    );
    use std::os::unix::fs::PermissionsExt;
    assert_eq!(
        fs::metadata(&path).unwrap().permissions().mode() & 0o777,
        0o600
    );
    let serving = Arc::clone(&socket);
    let task = tokio::spawn(async move { serving.serve().await });
    let stream = tokio::net::UnixStream::connect(&path).await.unwrap();
    let (read, mut write) = stream.into_split();
    write.write_all(b"{\"schemaVersion\":2,\"requestId\":\"018f3f4c-8af1-7f6b-bf42-1bd472868e65\",\"operation\":{\"type\":\"launcherSearch\",\"data\":{\"query\":\"term\"}}}\n").await.unwrap();
    let mut lines = tokio::io::BufReader::new(read).lines();
    let response: DailyResponse =
        serde_json::from_str(&lines.next_line().await.unwrap().unwrap()).unwrap();
    assert!(matches!(response.status, DailyStatus::Confirmed));
    socket
        .shutdown_and_drain(Duration::from_secs(1))
        .await
        .unwrap();
    task.await.unwrap().unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn daily_socket_bounds_workers_keeps_runtime_responsive_and_joins_on_shutdown() {
    use std::sync::atomic::Ordering;
    use tokio::io::AsyncWriteExt;
    let root = tempdir().unwrap();
    let path = root.path().join("sleepy/daily.sock");
    let backend = Arc::new(ControlledDaily {
        active: std::sync::atomic::AtomicUsize::new(0),
        peak: std::sync::atomic::AtomicUsize::new(0),
    });
    let socket = Arc::new(
        DailySocket::bind_with_limits(
            &path,
            unsafe { libc::geteuid() },
            Arc::clone(&backend),
            2,
            Duration::from_secs(30),
        )
        .await
        .unwrap(),
    );
    let serving = Arc::clone(&socket);
    let serve = tokio::spawn(async move { serving.serve().await });
    let request = b"{\"schemaVersion\":2,\"requestId\":\"018f3f4c-8af1-7f6b-bf42-1bd472868e65\",\"operation\":{\"type\":\"launcherSearch\",\"data\":{\"query\":\"term\"}}}\n";
    let mut clients = Vec::new();
    // Admission is now bounded at the same limit as workers; excess peers are
    // covered by the dedicated Busy test below rather than queued here.
    for _ in 0..2 {
        let mut stream = tokio::net::UnixStream::connect(&path).await.unwrap();
        stream.write_all(request).await.unwrap();
        clients.push(stream);
    }
    tokio::time::timeout(Duration::from_millis(200), async {
        while backend.active.load(Ordering::SeqCst) < 2 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    let progress = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let progress_task = Arc::clone(&progress);
    tokio::time::sleep(Duration::from_millis(20)).await;
    tokio::spawn(async move {
        progress_task.fetch_add(1, Ordering::SeqCst);
    })
    .await
    .unwrap();
    assert_eq!(
        progress.load(Ordering::SeqCst),
        1,
        "blocking work stalled Tokio"
    );
    socket
        .shutdown_and_drain(Duration::from_secs(1))
        .await
        .unwrap();
    serve.await.unwrap().unwrap();
    assert_eq!(backend.active.load(Ordering::SeqCst), 0);
    assert!(backend.peak.load(Ordering::SeqCst) <= 2);
    drop(clients);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn daily_socket_rejects_excess_connections_before_spawning_queued_tasks() {
    use tokio::io::{AsyncBufReadExt, AsyncWriteExt};
    let root = tempdir().unwrap();
    let path = root.path().join("sleepy/daily.sock");
    let backend = Arc::new(ControlledDaily {
        active: std::sync::atomic::AtomicUsize::new(0),
        peak: std::sync::atomic::AtomicUsize::new(0),
    });
    let socket = Arc::new(
        DailySocket::bind_with_limits(
            &path,
            unsafe { libc::geteuid() },
            backend,
            1,
            Duration::from_secs(30),
        )
        .await
        .unwrap(),
    );
    let serving = Arc::clone(&socket);
    let serve = tokio::spawn(async move { serving.serve().await });
    let request = b"{\"schemaVersion\":2,\"requestId\":\"018f3f4c-8af1-7f6b-bf42-1bd472868e65\",\"operation\":{\"type\":\"launcherSearch\",\"data\":{\"query\":\"term\"}}}\n";
    let mut first = tokio::net::UnixStream::connect(&path).await.unwrap();
    first.write_all(request).await.unwrap();
    tokio::time::sleep(Duration::from_millis(20)).await;
    let second = tokio::net::UnixStream::connect(&path).await.unwrap();
    let mut line = String::new();
    tokio::time::timeout(
        Duration::from_millis(200),
        tokio::io::BufReader::new(second).read_line(&mut line),
    )
    .await
    .unwrap()
    .unwrap();
    let response: DailyResponse = serde_json::from_str(line.trim()).unwrap();
    assert!(matches!(response.status, DailyStatus::Busy));
    socket
        .shutdown_and_drain(Duration::from_secs(1))
        .await
        .unwrap();
    serve.await.unwrap().unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn daily_shutdown_does_not_wait_for_non_reading_clients_response_write() {
    use std::sync::atomic::Ordering;
    use tokio::io::AsyncWriteExt;
    let root = tempdir().unwrap();
    let path = root.path().join("sleepy/daily.sock");
    let backend = Arc::new(LargeDaily {
        completed: std::sync::atomic::AtomicBool::new(false),
    });
    let socket = Arc::new(
        DailySocket::bind_with_limits(
            &path,
            unsafe { libc::geteuid() },
            Arc::clone(&backend),
            1,
            Duration::from_secs(30),
        )
        .await
        .unwrap(),
    );
    let serving = Arc::clone(&socket);
    let serve = tokio::spawn(async move { serving.serve().await });
    let mut client = tokio::net::UnixStream::connect(&path).await.unwrap();
    client.write_all(b"{\"schemaVersion\":2,\"requestId\":\"018f3f4c-8af1-7f6b-bf42-1bd472868e65\",\"operation\":{\"type\":\"launcherSearch\",\"data\":{\"query\":\"term\"}}}\n").await.unwrap();
    tokio::time::timeout(Duration::from_secs(1), async {
        while !backend.completed.load(Ordering::SeqCst) {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    tokio::time::sleep(Duration::from_millis(20)).await;
    let draining = Arc::clone(&socket);
    let mut drain = tokio::spawn(async move {
        draining
            .shutdown_and_drain(Duration::from_millis(150))
            .await
    });
    let completed = tokio::time::timeout(Duration::from_millis(400), &mut drain).await;
    let finished_while_client_was_not_reading = completed.is_ok();
    drop(client);
    let drain_result = match completed {
        Ok(joined) => joined.unwrap(),
        Err(_) => drain.await.unwrap(),
    };
    serve.await.unwrap().unwrap();
    assert!(
        finished_while_client_was_not_reading,
        "daily drain remained stuck in an unbounded response write"
    );
    drain_result.unwrap();
}
