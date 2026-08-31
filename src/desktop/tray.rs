// SPDX-License-Identifier: GPL-3.0-only

//! StatusNotifierItem/DBusMenu production adapter.

use std::{
    collections::BTreeMap,
    io,
    sync::Mutex,
    time::{Duration, Instant},
};

use dbus::{
    arg::{PropMap, RefArg, Variant},
    blocking::stdintf::org_freedesktop_dbus::Properties,
};
use sleepy_sdk::{StableId, TrayItem, TrayMenuNode};

use crate::system::RunControl;

const WATCHER_DESTINATION: &str = "org.kde.StatusNotifierWatcher";
const WATCHER_PATH: &str = "/StatusNotifierWatcher";
const WATCHER_INTERFACE: &str = "org.kde.StatusNotifierWatcher";
const ITEM_INTERFACE: &str = "org.kde.StatusNotifierItem";
const MENU_INTERFACE: &str = "com.canonical.dbusmenu";
const OPERATION_TIMEOUT: Duration = Duration::from_millis(1_750);
type DynamicMenuNode = (i32, PropMap, Vec<Variant<Box<dyn RefArg>>>);

#[derive(Debug, Clone)]
struct Target {
    service: String,
    path: String,
    actions: BTreeMap<String, MenuAction>,
}

#[derive(Debug, Clone)]
enum MenuAction {
    Activate,
    Menu { path: String, node_id: i32 },
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum TrayInvocation {
    Activate {
        service: String,
        path: String,
    },
    Menu {
        service: String,
        path: String,
        node_id: i32,
    },
}

#[derive(Default)]
pub struct TrayService {
    targets: Mutex<BTreeMap<String, Target>>,
}

impl TrayService {
    pub fn probe(&self) -> io::Result<Vec<TrayItem>> {
        self.probe_until(Instant::now() + OPERATION_TIMEOUT, None)
    }

    pub fn probe_controlled(&self, control: &RunControl) -> io::Result<Vec<TrayItem>> {
        ensure_active(Some(control))?;
        let remaining = control.remaining().min(OPERATION_TIMEOUT);
        if remaining.is_zero() {
            return Err(io::Error::new(
                io::ErrorKind::TimedOut,
                "tray probe exceeded its deadline",
            ));
        }
        self.probe_until(Instant::now() + remaining, Some(control))
    }

    fn probe_until(
        &self,
        deadline: Instant,
        control: Option<&RunControl>,
    ) -> io::Result<Vec<TrayItem>> {
        ensure_active(control)?;
        let connection = dbus::blocking::Connection::new_session().map_err(dbus_error)?;
        let watcher =
            connection.with_proxy(WATCHER_DESTINATION, WATCHER_PATH, remaining(deadline)?);
        let registrations: Vec<String> = watcher
            .get(WATCHER_INTERFACE, "RegisteredStatusNotifierItems")
            .map_err(dbus_error)?;
        if registrations.len() > 1_024 {
            return invalid("too many StatusNotifierItem registrations");
        }
        let mut targets = BTreeMap::new();
        let mut items = Vec::with_capacity(registrations.len());
        for registration in registrations {
            ensure_active(control)?;
            let (service, path) = split_registration(&registration)?;
            let proxy = connection.with_proxy(service, path, remaining(deadline)?);
            let title: String = proxy
                .get(ITEM_INTERFACE, "Title")
                .or_else(|_| proxy.get(ITEM_INTERFACE, "Id"))
                .map_err(dbus_error)?;
            if title.trim().is_empty() {
                return invalid("StatusNotifierItem title is empty");
            }
            let hash = stable_hash(service.as_bytes(), path.as_bytes());
            let id = format!("tray:{hash:016x}");
            let (menu, actions) =
                match menu_for_item(&connection, service, path, hash, &title, deadline, control) {
                    Ok(menu) => menu,
                    Err(error) if error.kind() == io::ErrorKind::NotFound => {
                        fallback_menu(hash, &title)
                    }
                    Err(error) => return Err(error),
                };
            if targets
                .insert(
                    id.clone(),
                    Target {
                        service: service.to_owned(),
                        path: path.to_owned(),
                        actions,
                    },
                )
                .is_some()
            {
                return invalid("StatusNotifierItem stable ID collision");
            }
            items.push(TrayItem {
                id,
                title: title.clone(),
                menu,
            });
        }
        ensure_active(control)?;
        *self
            .targets
            .lock()
            .map_err(|_| io::Error::other("tray target lock poisoned"))? = targets;
        Ok(items)
    }

    pub fn invoke(&self, item_id: &StableId, menu_id: &StableId) -> io::Result<Vec<TrayItem>> {
        let deadline = Instant::now() + OPERATION_TIMEOUT;
        self.invoke_with(
            item_id,
            menu_id,
            |invocation| {
                let connection = dbus::blocking::Connection::new_session().map_err(dbus_error)?;
                match invocation {
                    TrayInvocation::Activate { service, path } => {
                        let proxy = connection.with_proxy(service, path, remaining(deadline)?);
                        let _: () = proxy
                            .method_call(ITEM_INTERFACE, "Activate", (0_i32, 0_i32))
                            .map_err(dbus_error)?;
                    }
                    TrayInvocation::Menu {
                        service,
                        path,
                        node_id,
                    } => {
                        let proxy = connection.with_proxy(service, path, remaining(deadline)?);
                        let _: () = proxy
                            .method_call(
                                MENU_INTERFACE,
                                "Event",
                                (node_id, "clicked", Variant(0_i32), 0_u32),
                            )
                            .map_err(dbus_error)?;
                    }
                }
                Ok(())
            },
            || self.probe_until(deadline, None),
        )
    }

    fn invoke_with(
        &self,
        item_id: &StableId,
        menu_id: &StableId,
        transport: impl FnOnce(TrayInvocation) -> io::Result<()>,
        refresh: impl FnOnce() -> io::Result<Vec<TrayItem>>,
    ) -> io::Result<Vec<TrayItem>> {
        let invocation = self.invocation(item_id, menu_id)?;
        transport(invocation)?;
        refresh()
    }

    fn invocation(&self, item_id: &StableId, menu_id: &StableId) -> io::Result<TrayInvocation> {
        let targets = self
            .targets
            .lock()
            .map_err(|_| io::Error::other("tray target lock poisoned"))?;
        let target = targets.get(item_id.as_str()).ok_or_else(|| {
            io::Error::new(io::ErrorKind::InvalidInput, "unknown tray menu target")
        })?;
        let action = target.actions.get(menu_id.as_str()).ok_or_else(|| {
            io::Error::new(io::ErrorKind::InvalidInput, "unknown tray menu target")
        })?;
        Ok(match action {
            MenuAction::Activate => TrayInvocation::Activate {
                service: target.service.clone(),
                path: target.path.clone(),
            },
            MenuAction::Menu { path, node_id } => TrayInvocation::Menu {
                service: target.service.clone(),
                path: path.clone(),
                node_id: *node_id,
            },
        })
    }
}

fn menu_for_item(
    connection: &dbus::blocking::Connection,
    service: &str,
    item_path: &str,
    hash: u64,
    title: &str,
    deadline: Instant,
    control: Option<&RunControl>,
) -> io::Result<(TrayMenuNode, BTreeMap<String, MenuAction>)> {
    ensure_active(control)?;
    let item = connection.with_proxy(service, item_path, remaining(deadline)?);
    let menu_path: dbus::Path<'static> = item.get(ITEM_INTERFACE, "Menu").map_err(dbus_error)?;
    if menu_path == "/NO_DBUSMENU" {
        return Err(io::Error::new(io::ErrorKind::NotFound, "tray menu absent"));
    }
    let menu_path = menu_path.to_string();
    let proxy = connection.with_proxy(service, menu_path.as_str(), remaining(deadline)?);
    let (_, layout): (u32, DynamicMenuNode) = proxy
        .method_call(
            MENU_INTERFACE,
            "GetLayout",
            (0_i32, -1_i32, Vec::<String>::new()),
        )
        .map_err(dbus_error)?;
    ensure_active(control)?;
    let mut actions = BTreeMap::new();
    let mut count = 1_usize;
    let children = parse_children(&layout.2, hash, &menu_path, &mut actions, &mut count, 0)?;
    let root_id = format!("tray-menu:{hash:016x}:root");
    actions.insert(root_id.clone(), MenuAction::Activate);
    Ok((
        TrayMenuNode {
            id: root_id,
            label: title.to_owned(),
            enabled: true,
            children,
        },
        actions,
    ))
}

fn ensure_active(control: Option<&RunControl>) -> io::Result<()> {
    match control {
        Some(control) if control.is_cancelled() => Err(io::Error::new(
            io::ErrorKind::Interrupted,
            "tray probe was cancelled",
        )),
        Some(control) if control.remaining().is_zero() => Err(io::Error::new(
            io::ErrorKind::TimedOut,
            "tray probe exceeded its deadline",
        )),
        _ => Ok(()),
    }
}

fn parse_children(
    children: &[Variant<Box<dyn RefArg>>],
    hash: u64,
    menu_path: &str,
    actions: &mut BTreeMap<String, MenuAction>,
    count: &mut usize,
    depth: usize,
) -> io::Result<Vec<TrayMenuNode>> {
    if depth > 32 {
        return invalid("tray menu nesting exceeds 32 levels");
    }
    let mut nodes = Vec::new();
    for child in children {
        let mut fields = child.0.as_iter().ok_or_else(|| {
            io::Error::new(io::ErrorKind::InvalidData, "tray menu node malformed")
        })?;
        let node_id = fields
            .next()
            .and_then(RefArg::as_i64)
            .and_then(|value| i32::try_from(value).ok())
            .ok_or_else(|| {
                io::Error::new(io::ErrorKind::InvalidData, "tray menu node ID invalid")
            })?;
        let properties = fields.next().ok_or_else(|| {
            io::Error::new(io::ErrorKind::InvalidData, "tray menu properties omitted")
        })?;
        let descendants = fields.next().ok_or_else(|| {
            io::Error::new(io::ErrorKind::InvalidData, "tray menu children omitted")
        })?;
        let label = dynamic_property(properties, "label")
            .and_then(RefArg::as_str)
            .unwrap_or_default();
        if label.trim().is_empty() {
            continue;
        }
        *count += 1;
        if *count > 65_536 {
            return invalid("tray menu node count exceeds 65536");
        }
        let id = format!("tray-menu:{hash:016x}:{node_id}");
        let enabled = dynamic_property(properties, "enabled")
            .and_then(RefArg::as_i64)
            .map(|value| value != 0)
            .unwrap_or(true);
        let dynamic_children = descendants
            .as_iter()
            .map(|values| {
                values
                    .filter_map(|value| {
                        value
                            .as_iter()
                            .and_then(|mut variant| variant.next())
                            .map(|inner| Variant(inner.box_clone()))
                    })
                    .collect::<Vec<_>>()
            })
            .unwrap_or_default();
        let children = parse_children(
            &dynamic_children,
            hash,
            menu_path,
            actions,
            count,
            depth + 1,
        )?;
        if enabled {
            actions.insert(
                id.clone(),
                MenuAction::Menu {
                    path: menu_path.to_owned(),
                    node_id,
                },
            );
        }
        nodes.push(TrayMenuNode {
            id,
            label: label.to_owned(),
            enabled,
            children,
        });
    }
    Ok(nodes)
}

fn dynamic_property<'a>(properties: &'a dyn RefArg, name: &str) -> Option<&'a dyn RefArg> {
    let entries = properties.as_iter()?;
    for entry in entries {
        let mut pair = entry.as_iter()?;
        if pair.next()?.as_str()? != name {
            continue;
        }
        let value = pair.next()?;
        return value.as_iter().and_then(|mut variant| variant.next());
    }
    None
}

fn fallback_menu(hash: u64, title: &str) -> (TrayMenuNode, BTreeMap<String, MenuAction>) {
    let id = format!("tray-menu:{hash:016x}:activate");
    (
        TrayMenuNode {
            id: id.clone(),
            label: title.to_owned(),
            enabled: true,
            children: Vec::new(),
        },
        BTreeMap::from([(id, MenuAction::Activate)]),
    )
}

pub fn split_registration(value: &str) -> io::Result<(&str, &str)> {
    let (service, path) = match value.find('/') {
        Some(index) => (&value[..index], &value[index..]),
        None => (value, "/StatusNotifierItem"),
    };
    if !valid_bus_name(service) || !valid_object_path(path) {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "invalid StatusNotifierItem registration",
        ));
    }
    Ok((service, path))
}

fn valid_bus_name(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= 255
        && (value.starts_with(':') || value.contains('.'))
        && value.trim_start_matches(':').split('.').all(|component| {
            !component.is_empty()
                && !component.starts_with('-')
                && component
                    .bytes()
                    .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'_' | b'-'))
        })
}

fn stable_hash(service: &[u8], path: &[u8]) -> u64 {
    let mut hash = 0xcbf29ce484222325_u64;
    for byte in service.iter().chain([&0_u8]).chain(path) {
        hash ^= u64::from(*byte);
        hash = hash.wrapping_mul(0x100000001b3);
    }
    hash
}

fn dbus_error(error: dbus::Error) -> io::Error {
    let kind = match error.name() {
        Some("org.freedesktop.DBus.Error.ServiceUnknown")
        | Some("org.freedesktop.DBus.Error.NameHasNoOwner")
        | Some("org.freedesktop.DBus.Error.UnknownProperty")
        | Some("org.freedesktop.DBus.Error.UnknownMethod") => io::ErrorKind::NotFound,
        Some("org.freedesktop.DBus.Error.AccessDenied") => io::ErrorKind::PermissionDenied,
        Some("org.freedesktop.DBus.Error.NoReply") => io::ErrorKind::TimedOut,
        _ => io::ErrorKind::Other,
    };
    io::Error::new(kind, "StatusNotifierItem D-Bus request failed")
}

fn invalid<T>(message: &'static str) -> io::Result<T> {
    Err(io::Error::new(io::ErrorKind::InvalidData, message))
}

fn remaining(deadline: Instant) -> io::Result<Duration> {
    let remaining = deadline.saturating_duration_since(Instant::now());
    if remaining.is_zero() {
        Err(io::Error::new(
            io::ErrorKind::TimedOut,
            "tray operation exceeded its total deadline",
        ))
    } else {
        Ok(remaining)
    }
}

fn valid_object_path(value: &str) -> bool {
    value.starts_with('/')
        && !value.contains("//")
        && value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'/' | b'_'))
}

#[cfg(test)]
mod tests {
    use std::cell::RefCell;

    use super::*;

    #[test]
    fn injected_tray_transport_receives_only_the_cached_typed_target() {
        let item_id = StableId("tray:fixture".into());
        let menu_id = StableId("tray-menu:fixture:7".into());
        let service = TrayService {
            targets: Mutex::new(BTreeMap::from([(
                item_id.as_str().to_owned(),
                Target {
                    service: "org.example.Tray".into(),
                    path: "/StatusNotifierItem".into(),
                    actions: BTreeMap::from([(
                        menu_id.as_str().to_owned(),
                        MenuAction::Menu {
                            path: "/Menu".into(),
                            node_id: 7,
                        },
                    )]),
                },
            )])),
        };
        let invocations = RefCell::new(Vec::new());
        let refreshed = service
            .invoke_with(
                &item_id,
                &menu_id,
                |invocation| {
                    invocations.borrow_mut().push(invocation);
                    Ok(())
                },
                || Ok(Vec::new()),
            )
            .unwrap();
        assert!(refreshed.is_empty());
        assert_eq!(
            invocations.into_inner(),
            [TrayInvocation::Menu {
                service: "org.example.Tray".into(),
                path: "/Menu".into(),
                node_id: 7,
            }]
        );
    }
}
