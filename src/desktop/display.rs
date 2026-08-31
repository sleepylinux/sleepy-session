// SPDX-License-Identifier: GPL-3.0-only

use std::{
    io, thread,
    time::{Duration, Instant},
};

use dbus::blocking::stdintf::org_freedesktop_dbus::Properties;
use sleepy_sdk::{DisplayCommand, DisplaySnapshot};

use crate::system::{CommandRunner, CommandSpec};

const SYSTEMD_DESTINATION: &str = "org.freedesktop.systemd1";
const SYSTEMD_PATH: &str = "/org/freedesktop/systemd1";
const SYSTEMD_MANAGER: &str = "org.freedesktop.systemd1.Manager";
const SYSTEMD_UNIT: &str = "org.freedesktop.systemd1.Unit";
const GAMMASTEP_PATH: &str = "/org/freedesktop/systemd1/unit/gammastep_2eservice";
const DBUS_TIMEOUT: Duration = Duration::from_millis(750);

pub fn brightness_spec(level: f64) -> io::Result<CommandSpec> {
    if !level.is_finite() || !(0.0..=1.0).contains(&level) {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "brightness is not normalized",
        ));
    }
    let mut spec = CommandSpec::new("brightnessctl", ["set", &format!("{:.4}%", level * 100.0)]);
    spec.timeout = Duration::from_secs(5);
    Ok(spec)
}

pub fn probe<R: CommandRunner>(runner: &R) -> io::Result<DisplaySnapshot> {
    let output = super::network::run(
        runner,
        CommandSpec::new("brightnessctl", ["--machine-readable", "info"]),
    )?;
    let brightness = parse_brightness(&output)?;
    let night_light_enabled = night_light_state()?;
    Ok(DisplaySnapshot {
        brightness: Some(brightness),
        night_light_enabled,
    })
}

pub fn mutate<R: CommandRunner>(
    runner: &R,
    command: &DisplayCommand,
) -> io::Result<DisplaySnapshot> {
    match command {
        DisplayCommand::SetBrightness { level, .. } => {
            super::network::run(runner, brightness_spec(*level)?)?;
        }
        DisplayCommand::SetNightLightEnabled { enabled } => set_night_light(*enabled)?,
    }
    let snapshot = probe(runner)?;
    let confirmed = match command {
        DisplayCommand::SetBrightness { level, .. } => snapshot
            .brightness
            .is_some_and(|readback| (readback - level).abs() <= 0.005),
        DisplayCommand::SetNightLightEnabled { enabled } => {
            snapshot.night_light_enabled == *enabled
        }
    };
    if !confirmed {
        return Err(io::Error::other(
            "display readback did not confirm the requested state",
        ));
    }
    Ok(snapshot)
}

fn parse_brightness(output: &[u8]) -> io::Result<f64> {
    let text = std::str::from_utf8(output).map_err(|_| {
        io::Error::new(io::ErrorKind::InvalidData, "brightness output is not UTF-8")
    })?;
    let row = text
        .lines()
        .next()
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "brightness output is empty"))?;
    let fields = row.split(',').collect::<Vec<_>>();
    let percent = fields
        .get(3)
        .and_then(|field| field.strip_suffix('%'))
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "brightness row malformed"))?
        .parse::<f64>()
        .map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "brightness is not numeric"))?;
    if !percent.is_finite() || !(0.0..=100.0).contains(&percent) {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "brightness is outside 0..100",
        ));
    }
    Ok(percent / 100.0)
}

fn night_light_state() -> io::Result<bool> {
    let connection = dbus::blocking::Connection::new_session().map_err(dbus_error)?;
    let unit = connection.with_proxy(SYSTEMD_DESTINATION, GAMMASTEP_PATH, DBUS_TIMEOUT);
    let active: String = unit.get(SYSTEMD_UNIT, "ActiveState").map_err(dbus_error)?;
    match active.as_str() {
        "active" | "activating" | "reloading" => Ok(true),
        "inactive" | "deactivating" | "failed" => Ok(false),
        _ => Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "night-light unit returned an unknown state",
        )),
    }
}

fn set_night_light(enabled: bool) -> io::Result<()> {
    let connection = dbus::blocking::Connection::new_session().map_err(dbus_error)?;
    let manager = connection.with_proxy(SYSTEMD_DESTINATION, SYSTEMD_PATH, DBUS_TIMEOUT);
    let method = if enabled { "StartUnit" } else { "StopUnit" };
    let _: (dbus::Path<'static>,) = manager
        .method_call(SYSTEMD_MANAGER, method, ("gammastep.service", "replace"))
        .map_err(dbus_error)?;
    let deadline = Instant::now() + DBUS_TIMEOUT;
    loop {
        if night_light_state()? == enabled {
            return Ok(());
        }
        if Instant::now() >= deadline {
            return Err(io::Error::new(
                io::ErrorKind::TimedOut,
                "night-light state did not converge",
            ));
        }
        thread::sleep(Duration::from_millis(25));
    }
}

fn dbus_error(error: dbus::Error) -> io::Error {
    let kind = match error.name() {
        Some("org.freedesktop.DBus.Error.ServiceUnknown")
        | Some("org.freedesktop.DBus.Error.NameHasNoOwner")
        | Some("org.freedesktop.systemd1.NoSuchUnit") => io::ErrorKind::NotFound,
        Some("org.freedesktop.DBus.Error.AccessDenied") => io::ErrorKind::PermissionDenied,
        Some("org.freedesktop.DBus.Error.NoReply") => io::ErrorKind::TimedOut,
        _ => io::ErrorKind::Other,
    };
    io::Error::new(kind, "night-light D-Bus request failed")
}
