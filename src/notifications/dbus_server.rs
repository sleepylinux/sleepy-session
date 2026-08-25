use std::{
    collections::HashMap,
    ffi::CString,
    io,
    sync::{
        atomic::{AtomicBool, Ordering},
        mpsc, Arc,
    },
    thread,
    time::Duration,
};

use dbus::{
    arg::{RefArg, Variant},
    blocking::{stdintf::org_freedesktop_dbus::RequestNameReply, Connection},
    channel::{MatchingReceiver, Sender},
    message::MatchRule,
    strings::ErrorName,
    Message,
};
use sleepy_sdk::{
    NotificationAction, NotificationActionState, NotificationDocument, NotificationUrgency,
    WIRE_SCHEMA_VERSION,
};

use super::{
    NotificationCommand, NotificationEventService, NotifyRequest, DBUS_NOTIFICATIONS_NAME,
};

const OBJECT_PATH: &str = "/org/freedesktop/Notifications";

enum Control {
    ActionInvoked(u32, String),
}

pub struct NotificationDbusServer {
    stop: Arc<AtomicBool>,
    control: mpsc::Sender<Control>,
    thread: Option<thread::JoinHandle<()>>,
    service: Arc<tokio::sync::Mutex<NotificationEventService>>,
    failure: tokio::sync::watch::Receiver<Option<String>>,
}

#[derive(Clone)]
pub struct NotificationActionDispatcher {
    control: std::sync::mpsc::Sender<Control>,
    service: Arc<tokio::sync::Mutex<NotificationEventService>>,
}

impl NotificationActionDispatcher {
    pub async fn invoke(&self, id: u64, action: &str) -> io::Result<()> {
        let wire_id = u32::try_from(id).map_err(|_| {
            io::Error::new(
                io::ErrorKind::InvalidInput,
                "notification id exceeds D-Bus range",
            )
        })?;
        self.service
            .lock()
            .await
            .provider()
            .invoke_action(id, action)?;
        self.control
            .send(Control::ActionInvoked(wire_id, action.to_owned()))
            .map_err(|_| io::Error::new(io::ErrorKind::BrokenPipe, "D-Bus server stopped"))
    }
}

impl NotificationDbusServer {
    pub fn action_dispatcher(&self) -> NotificationActionDispatcher {
        NotificationActionDispatcher {
            control: self.control.clone(),
            service: Arc::clone(&self.service),
        }
    }
    pub fn start_session(
        service: Arc<tokio::sync::Mutex<NotificationEventService>>,
        runtime: tokio::runtime::Handle,
    ) -> io::Result<Self> {
        Self::start(
            Connection::new_session().map_err(dbus_error)?,
            service,
            runtime,
        )
    }

    pub fn start_at(
        address: &str,
        service: Arc<tokio::sync::Mutex<NotificationEventService>>,
        runtime: tokio::runtime::Handle,
    ) -> io::Result<Self> {
        Self::start(
            Connection::new_address(address).map_err(dbus_error)?,
            service,
            runtime,
        )
    }

    fn start(
        connection: Connection,
        service: Arc<tokio::sync::Mutex<NotificationEventService>>,
        runtime: tokio::runtime::Handle,
    ) -> io::Result<Self> {
        match connection
            .request_name(DBUS_NOTIFICATIONS_NAME, false, true, true)
            .map_err(dbus_error)?
        {
            RequestNameReply::PrimaryOwner | RequestNameReply::AlreadyOwner => {}
            _ => {
                return Err(io::Error::new(
                    io::ErrorKind::AddrInUse,
                    "org.freedesktop.Notifications is already owned",
                ));
            }
        }

        let stop = Arc::new(AtomicBool::new(false));
        let thread_stop = Arc::clone(&stop);
        let (control, controls) = mpsc::channel();
        let (failure_sender, failure) = tokio::sync::watch::channel(None::<String>);
        register_methods(&connection, Arc::clone(&service), runtime.clone());
        register_owner_loss(
            &connection,
            Arc::clone(&service),
            runtime.clone(),
            failure_sender.clone(),
        )?;
        let timer_service = Arc::clone(&service);
        let thread = thread::Builder::new()
            .name("sleepy-notifications-dbus".into())
            .spawn(move || {
                while !thread_stop.load(Ordering::Acquire) {
                    while let Ok(control) = controls.try_recv() {
                        match control {
                            Control::ActionInvoked(id, action) => {
                                if let Err(error) = send_action_invoked(&connection, id, &action) {
                                    report_failure(&failure_sender, error);
                                    return;
                                }
                            }
                        }
                    }
                    if let Err(error) = connection.process(Duration::from_millis(25)) {
                        report_failure(&failure_sender, dbus_error(error));
                        return;
                    }
                    if failure_sender.borrow().is_some() {
                        return;
                    }
                    let expired = runtime.block_on(async {
                        timer_service
                            .lock()
                            .await
                            .advance_popup_time(std::time::Instant::now())
                            .await
                    });
                    match expired {
                        Ok(expired) => {
                            for id in expired {
                                if let Ok(id) = u32::try_from(id) {
                                    if let Err(error) = send_notification_closed(&connection, id, 1)
                                    {
                                        report_failure(&failure_sender, error);
                                        return;
                                    }
                                }
                            }
                        }
                        Err(error) => {
                            report_failure(&failure_sender, error);
                            return;
                        }
                    }
                }
            })?;
        Ok(Self {
            stop,
            control,
            thread: Some(thread),
            service,
            failure,
        })
    }

    pub async fn invoke_action(&self, id: u32, action: &str) -> io::Result<()> {
        self.service
            .lock()
            .await
            .provider()
            .invoke_action(u64::from(id), action)?;
        self.control
            .send(Control::ActionInvoked(id, action.to_owned()))
            .map_err(|_| io::Error::new(io::ErrorKind::BrokenPipe, "D-Bus server stopped"))
    }

    pub async fn wait_for_failure(&mut self) -> io::Error {
        loop {
            if let Some(message) = self.failure.borrow().clone() {
                return io::Error::new(io::ErrorKind::BrokenPipe, message);
            }
            if self.failure.changed().await.is_err() {
                return io::Error::new(
                    io::ErrorKind::BrokenPipe,
                    "notification D-Bus server stopped unexpectedly",
                );
            }
        }
    }
}

impl Drop for NotificationDbusServer {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::Release);
        if let Some(thread) = self.thread.take() {
            let _ = thread.join();
        }
    }
}

fn register_methods(
    connection: &Connection,
    service: Arc<tokio::sync::Mutex<NotificationEventService>>,
    runtime: tokio::runtime::Handle,
) {
    let mut rule = MatchRule::new_method_call();
    rule.path = Some(OBJECT_PATH.into());
    rule.interface = Some(DBUS_NOTIFICATIONS_NAME.into());
    connection.start_receive(
        rule,
        Box::new(move |message, connection| {
            let reply = handle_method(&message, &service, &runtime, connection);
            let _ = connection.send(reply);
            true
        }),
    );
}

fn handle_method(
    message: &Message,
    service: &Arc<tokio::sync::Mutex<NotificationEventService>>,
    runtime: &tokio::runtime::Handle,
    connection: &Connection,
) -> Message {
    let result = match message.member().as_deref() {
        Some("GetCapabilities") => {
            Ok(message.return_with_args((vec!["actions".to_owned(), "body".to_owned()],)))
        }
        Some("GetServerInformation") => Ok(message.return_with_args((
            "Sleepy".to_owned(),
            "Sleepy Linux".to_owned(),
            env!("CARGO_PKG_VERSION").to_owned(),
            "1.2".to_owned(),
        ))),
        Some("Notify") => handle_notify(message, service, runtime),
        Some("CloseNotification") => handle_close(message, service, runtime, connection),
        _ => Err(io::Error::new(
            io::ErrorKind::Unsupported,
            "unknown notification method",
        )),
    };
    result.unwrap_or_else(|error| error_reply(message, &error))
}

type Hints = HashMap<String, Variant<Box<dyn RefArg>>>;

fn handle_notify(
    message: &Message,
    service: &Arc<tokio::sync::Mutex<NotificationEventService>>,
    runtime: &tokio::runtime::Handle,
) -> io::Result<Message> {
    let (application, replaces_id, _icon, summary, body, actions, hints, timeout): (
        String,
        u32,
        String,
        String,
        String,
        Vec<String>,
        Hints,
        i32,
    ) = message.read_all().map_err(invalid_args)?;
    if actions.len() % 2 != 0 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "notification actions must be key/label pairs",
        ));
    }
    let origin = message
        .sender()
        .map(|sender| sender.to_string())
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "missing D-Bus sender"))?;
    let urgency = match hints
        .get("urgency")
        .and_then(|value| value.0.as_u64())
        .unwrap_or(1)
    {
        0 => NotificationUrgency::Low,
        1 => NotificationUrgency::Normal,
        2 => NotificationUrgency::Critical,
        _ => {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "invalid notification urgency",
            ));
        }
    };
    let notification = NotificationDocument {
        schema_version: WIRE_SCHEMA_VERSION,
        id: u64::from(replaces_id),
        application_id: application,
        summary,
        body,
        urgency,
        created_at: utc_now()?,
        timeout_ms: Some(if timeout < 0 { 5_000 } else { timeout as u64 }),
        read: false,
        archived: false,
        actions: actions
            .chunks_exact(2)
            .map(|pair| NotificationAction {
                id: pair[0].clone(),
                label: pair[1].clone(),
                state: NotificationActionState::Available,
            })
            .collect(),
    };
    let outcome = runtime.block_on(async {
        service
            .lock()
            .await
            .notify(NotifyRequest {
                origin,
                notification,
            })
            .await
    })?;
    let id = u32::try_from(outcome.id)
        .map_err(|_| io::Error::other("notification ID exceeds D-Bus uint32"))?;
    Ok(message.return_with_args((id,)))
}

fn handle_close(
    message: &Message,
    service: &Arc<tokio::sync::Mutex<NotificationEventService>>,
    runtime: &tokio::runtime::Handle,
    connection: &Connection,
) -> io::Result<Message> {
    let (id,): (u32,) = message.read_all().map_err(invalid_args)?;
    runtime.block_on(async {
        service
            .lock()
            .await
            .execute(NotificationCommand::Dismiss { id: u64::from(id) })
            .await
    })?;
    send_notification_closed(connection, id, 3)?;
    Ok(message.method_return())
}

fn register_owner_loss(
    connection: &Connection,
    service: Arc<tokio::sync::Mutex<NotificationEventService>>,
    runtime: tokio::runtime::Handle,
    failure: tokio::sync::watch::Sender<Option<String>>,
) -> io::Result<()> {
    let rule = MatchRule::new_signal("org.freedesktop.DBus", "NameOwnerChanged");
    connection
        .add_match(
            rule,
            move |(name, old_owner, new_owner): (String, String, String), _, _| {
                if !old_owner.is_empty() && new_owner.is_empty() && name.starts_with(':') {
                    if let Err(error) =
                        runtime.block_on(async { service.lock().await.origin_lost(&name).await })
                    {
                        report_failure(&failure, error);
                    }
                }
                true
            },
        )
        .map(|_| ())
        .map_err(dbus_error)
}

fn report_failure(failure: &tokio::sync::watch::Sender<Option<String>>, error: io::Error) {
    let _ = failure.send(Some(error.to_string()));
}

fn send_action_invoked(connection: &Connection, id: u32, action: &str) -> io::Result<()> {
    send_signal(
        connection,
        "ActionInvoked",
        Message::new_signal(OBJECT_PATH, DBUS_NOTIFICATIONS_NAME, "ActionInvoked")
            .map_err(io::Error::other)?
            .append2(id, action.to_owned()),
    )
}

fn send_notification_closed(connection: &Connection, id: u32, reason: u32) -> io::Result<()> {
    send_signal(
        connection,
        "NotificationClosed",
        Message::new_signal(OBJECT_PATH, DBUS_NOTIFICATIONS_NAME, "NotificationClosed")
            .map_err(io::Error::other)?
            .append2(id, reason),
    )
}

fn send_signal(connection: &Connection, _name: &str, signal: Message) -> io::Result<()> {
    connection
        .send(signal)
        .map(|_| ())
        .map_err(|_| io::Error::new(io::ErrorKind::BrokenPipe, "failed to send D-Bus signal"))
}

fn error_reply(message: &Message, error: &io::Error) -> Message {
    let name = ErrorName::new("org.freedesktop.DBus.Error.InvalidArgs")
        .expect("static D-Bus error name is valid");
    let sanitized = error.to_string().replace('\0', " ");
    let text = CString::new(sanitized).expect("NUL bytes were removed");
    message.error(&name, &text)
}

fn invalid_args(error: impl std::fmt::Display) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidInput, error.to_string())
}

fn dbus_error(error: dbus::Error) -> io::Error {
    io::Error::other(error.to_string())
}

fn utc_now() -> io::Result<String> {
    let now = unsafe { libc::time(std::ptr::null_mut()) };
    if now == -1 {
        return Err(io::Error::last_os_error());
    }
    let mut broken_down = std::mem::MaybeUninit::<libc::tm>::uninit();
    let result = unsafe { libc::gmtime_r(&now, broken_down.as_mut_ptr()) };
    if result.is_null() {
        return Err(io::Error::last_os_error());
    }
    let value = unsafe { broken_down.assume_init() };
    Ok(format!(
        "{:04}-{:02}-{:02}T{:02}:{:02}:{:02}Z",
        value.tm_year + 1900,
        value.tm_mon + 1,
        value.tm_mday,
        value.tm_hour,
        value.tm_min,
        value.tm_sec
    ))
}
