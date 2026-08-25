use std::{
    collections::HashMap,
    io::{BufRead, BufReader},
    process::{Child, Command, Stdio},
    sync::{Arc, Mutex},
    time::Duration,
};

use dbus::{arg::RefArg, arg::Variant, blocking::Connection, message::MatchRule};
use sleepy_session::{
    notifications::{
        FreedesktopNotificationProvider, NotificationDbusServer, NotificationEventService,
        NotificationStore, DBUS_NOTIFICATIONS_NAME,
    },
    sessiond::{full_snapshot_event, EventHub, GenerationAllocator, GenerationAuthority},
};

struct IsolatedBus {
    address: String,
    child: Child,
}

impl IsolatedBus {
    fn start() -> Self {
        let mut child = Command::new("dbus-daemon")
            .args(["--session", "--nofork", "--nopidfile", "--print-address=1"])
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .unwrap();
        let address = BufReader::new(child.stdout.take().unwrap())
            .lines()
            .next()
            .unwrap()
            .unwrap();
        Self { address, child }
    }
}

impl Drop for IsolatedBus {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn daemon_provider_owns_real_bus_name_and_serves_standard_plain_text_methods() {
    let bus = IsolatedBus::start();
    let temp = tempfile::tempdir().unwrap();
    let store = NotificationStore::open_default(temp.path().join("notifications")).unwrap();
    let provider = FreedesktopNotificationProvider::new(store).unwrap();
    let hub = EventHub::new(full_snapshot_event(0).unwrap(), 16);
    let allocator = GenerationAllocator::open(temp.path().join("generation"), 16).unwrap();
    let authority = GenerationAuthority::new(allocator, 0, hub);
    let service = Arc::new(tokio::sync::Mutex::new(NotificationEventService::new(
        provider, authority,
    )));
    let _server = NotificationDbusServer::start_at(
        &bus.address,
        Arc::clone(&service),
        tokio::runtime::Handle::current(),
    )
    .unwrap();

    let client = Connection::new_address(&bus.address).unwrap();
    let dbus = client.with_proxy(
        "org.freedesktop.DBus",
        "/org/freedesktop/DBus",
        Duration::from_secs(2),
    );
    let (names,): (Vec<String>,) = dbus
        .method_call("org.freedesktop.DBus", "ListNames", ())
        .unwrap();
    assert!(names.iter().any(|name| name == DBUS_NOTIFICATIONS_NAME));

    let proxy = client.with_proxy(
        DBUS_NOTIFICATIONS_NAME,
        "/org/freedesktop/Notifications",
        Duration::from_secs(2),
    );
    let (capabilities,): (Vec<String>,) = proxy
        .method_call(DBUS_NOTIFICATIONS_NAME, "GetCapabilities", ())
        .unwrap();
    assert!(capabilities
        .iter()
        .any(|capability| capability == "actions"));
    assert!(!capabilities
        .iter()
        .any(|capability| capability == "body-markup"));
    let (name, vendor, version, spec_version): (String, String, String, String) = proxy
        .method_call(DBUS_NOTIFICATIONS_NAME, "GetServerInformation", ())
        .unwrap();
    assert_eq!((name.as_str(), vendor.as_str()), ("Sleepy", "Sleepy Linux"));
    assert!(!version.is_empty());
    assert_eq!(spec_version, "1.2");

    let hints: HashMap<String, Variant<Box<dyn RefArg>>> = HashMap::new();
    let (id,): (u32,) = proxy
        .method_call(
            DBUS_NOTIFICATIONS_NAME,
            "Notify",
            (
                "Example",
                0_u32,
                "",
                "Literal",
                "<b>not markup</b>",
                vec!["open", "Open"],
                hints,
                5000_i32,
            ),
        )
        .unwrap();
    assert_eq!(id, 1);
    assert_eq!(
        service.lock().await.provider().store().active()[0].body,
        "<b>not markup</b>"
    );

    let _: () = proxy
        .method_call(DBUS_NOTIFICATIONS_NAME, "CloseNotification", (id,))
        .unwrap();
    assert_eq!(service.lock().await.provider().store().archive().len(), 1);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn standard_signals_are_emitted_and_sender_disconnect_expires_archived_actions() {
    let bus = IsolatedBus::start();
    let temp = tempfile::tempdir().unwrap();
    let store = NotificationStore::open_default(temp.path().join("notifications")).unwrap();
    let provider = FreedesktopNotificationProvider::new(store).unwrap();
    let hub = EventHub::new(full_snapshot_event(0).unwrap(), 16);
    let allocator = GenerationAllocator::open(temp.path().join("generation"), 16).unwrap();
    let authority = GenerationAuthority::new(allocator, 0, hub);
    let service = Arc::new(tokio::sync::Mutex::new(NotificationEventService::new(
        provider, authority,
    )));
    let server = NotificationDbusServer::start_at(
        &bus.address,
        Arc::clone(&service),
        tokio::runtime::Handle::current(),
    )
    .unwrap();

    let monitor = Connection::new_address(&bus.address).unwrap();
    let observed_actions = Arc::new(Mutex::new(Vec::<(u32, String)>::new()));
    let action_sink = Arc::clone(&observed_actions);
    monitor
        .add_match(
            MatchRule::new_signal(DBUS_NOTIFICATIONS_NAME, "ActionInvoked"),
            move |signal: (u32, String), _, _| {
                action_sink.lock().unwrap().push(signal);
                true
            },
        )
        .unwrap();
    let observed_closes = Arc::new(Mutex::new(Vec::<(u32, u32)>::new()));
    let close_sink = Arc::clone(&observed_closes);
    monitor
        .add_match(
            MatchRule::new_signal(DBUS_NOTIFICATIONS_NAME, "NotificationClosed"),
            move |signal: (u32, u32), _, _| {
                close_sink.lock().unwrap().push(signal);
                true
            },
        )
        .unwrap();

    let notifier = Connection::new_address(&bus.address).unwrap();
    let proxy = notifier.with_proxy(
        DBUS_NOTIFICATIONS_NAME,
        "/org/freedesktop/Notifications",
        Duration::from_secs(2),
    );
    let hints: HashMap<String, Variant<Box<dyn RefArg>>> = HashMap::new();
    let (id,): (u32,) = proxy
        .method_call(
            DBUS_NOTIFICATIONS_NAME,
            "Notify",
            (
                "Example",
                0_u32,
                "",
                "Action",
                "literal",
                vec!["open", "Open"],
                hints,
                5_000_i32,
            ),
        )
        .unwrap();
    server.invoke_action(id, "open").await.unwrap();
    wait_for_signal(&monitor, || !observed_actions.lock().unwrap().is_empty());
    assert_eq!(
        observed_actions.lock().unwrap().as_slice(),
        &[(id, "open".into())]
    );

    let expiring_hints: HashMap<String, Variant<Box<dyn RefArg>>> = HashMap::new();
    let (expiring_id,): (u32,) = proxy
        .method_call(
            DBUS_NOTIFICATIONS_NAME,
            "Notify",
            (
                "Example",
                0_u32,
                "",
                "Expiring",
                "literal",
                Vec::<String>::new(),
                expiring_hints,
                1_i32,
            ),
        )
        .unwrap();
    wait_for_signal(&monitor, || {
        observed_closes.lock().unwrap().contains(&(expiring_id, 1))
    });

    let _: () = proxy
        .method_call(DBUS_NOTIFICATIONS_NAME, "CloseNotification", (id,))
        .unwrap();
    wait_for_signal(&monitor, || {
        observed_closes.lock().unwrap().contains(&(id, 3))
    });
    assert!(observed_closes.lock().unwrap().contains(&(id, 3)));
    drop(proxy);
    drop(notifier);

    for _ in 0..100 {
        if service.lock().await.provider().store().archive()[0].actions[0].state
            == sleepy_sdk::NotificationActionState::Expired
        {
            return;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    panic!("notification actions did not expire after D-Bus sender disconnect");
}

fn wait_for_signal(connection: &Connection, ready: impl Fn() -> bool) {
    for _ in 0..100 {
        connection.process(Duration::from_millis(20)).unwrap();
        if ready() {
            return;
        }
    }
    panic!("D-Bus signal did not arrive before deadline");
}

#[test]
fn sleepy_sessiond_process_owns_the_notifications_name_on_its_session_bus() {
    let bus = IsolatedBus::start();
    let temp = tempfile::tempdir().unwrap();
    let runtime = temp.path().join("runtime");
    let state = temp.path().join("state");
    std::fs::create_dir(&runtime).unwrap();
    std::fs::create_dir(&state).unwrap();
    let daemon = Command::new(env!("CARGO_BIN_EXE_sleepy-sessiond"))
        .env("DBUS_SESSION_BUS_ADDRESS", &bus.address)
        .env("XDG_RUNTIME_DIR", &runtime)
        .env("XDG_STATE_HOME", &state)
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();
    let mut daemon = Some(daemon);
    let client = Connection::new_address(&bus.address).unwrap();
    let dbus = client.with_proxy(
        "org.freedesktop.DBus",
        "/org/freedesktop/DBus",
        Duration::from_millis(250),
    );
    let mut owned = false;
    for _ in 0..100 {
        let (names,): (Vec<String>,) = dbus
            .method_call("org.freedesktop.DBus", "ListNames", ())
            .unwrap();
        if names.iter().any(|name| name == DBUS_NOTIFICATIONS_NAME) {
            owned = true;
            break;
        }
        std::thread::sleep(Duration::from_millis(10));
    }
    if !owned {
        let mut child = daemon.take().unwrap();
        let _ = child.kill();
        let output = child.wait_with_output().unwrap();
        panic!(
            "sleepy-sessiond did not own notification name: {}",
            String::from_utf8_lossy(&output.stderr)
        );
    }
    let proxy = client.with_proxy(
        DBUS_NOTIFICATIONS_NAME,
        "/org/freedesktop/Notifications",
        Duration::from_secs(2),
    );
    let (name, _, _, _): (String, String, String, String) = proxy
        .method_call(DBUS_NOTIFICATIONS_NAME, "GetServerInformation", ())
        .unwrap();
    assert_eq!(name, "Sleepy");
    let mut child = daemon.take().unwrap();
    child.kill().unwrap();
    child.wait().unwrap();
}
