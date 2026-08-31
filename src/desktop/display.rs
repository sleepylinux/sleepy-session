// SPDX-License-Identifier: GPL-3.0-only

use std::{
    io, thread,
    time::{Duration, Instant},
};

use dbus::blocking::stdintf::org_freedesktop_dbus::Properties;
use sleepy_sdk::{BrightnessSnapshot, NightLightSnapshot};

use crate::system::{CommandRunner, CommandSpec, RunControl};

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

pub fn brightness_spec_for_output(output_id: &str, level: f64) -> io::Result<CommandSpec> {
    output_name(output_id)?;
    brightness_spec(level)?;
    Err(io::Error::new(
        io::ErrorKind::Unsupported,
        "brightness.output-target-unmapped",
    ))
}

pub fn output_name(output_id: &str) -> io::Result<&str> {
    let value = output_id.strip_prefix("output:").unwrap_or(output_id);
    if value.is_empty()
        || value.len() > 128
        || !value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.'))
    {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "brightness output ID is invalid",
        ));
    }
    Ok(value)
}

pub fn probe_brightness<R: CommandRunner>(runner: &R) -> io::Result<BrightnessSnapshot> {
    let output = super::network::run(
        runner,
        CommandSpec::new("brightnessctl", ["--machine-readable", "info"]),
    )?;
    Ok(BrightnessSnapshot {
        level: parse_brightness(&output)?,
    })
}

pub fn probe_night_light() -> io::Result<NightLightSnapshot> {
    Ok(NightLightSnapshot {
        enabled: night_light_state()?,
    })
}

pub(crate) fn probe_night_light_controlled(control: &RunControl) -> io::Result<NightLightSnapshot> {
    ensure_active(control)?;
    let timeout = control.remaining().min(DBUS_TIMEOUT);
    if timeout.is_zero() {
        return Err(io::Error::new(
            io::ErrorKind::TimedOut,
            "night-light probe exceeded its deadline",
        ));
    }
    let enabled = night_light_state_until(timeout)?;
    ensure_active(control)?;
    Ok(NightLightSnapshot { enabled })
}

pub fn mutate_brightness<R: CommandRunner>(
    runner: &R,
    level: f64,
) -> io::Result<BrightnessSnapshot> {
    super::network::run(runner, brightness_spec(level)?)?;
    let snapshot = probe_brightness(runner)?;
    if (snapshot.level - level).abs() > 0.005 {
        return Err(io::Error::other(
            "brightness readback did not confirm the requested state",
        ));
    }
    Ok(snapshot)
}

pub fn mutate_night_light(enabled: bool) -> io::Result<NightLightSnapshot> {
    set_night_light(enabled)?;
    let snapshot = probe_night_light()?;
    if snapshot.enabled != enabled {
        return Err(io::Error::other(
            "night-light readback did not confirm the requested state",
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
    night_light_state_until(DBUS_TIMEOUT)
}

fn night_light_state_until(timeout: Duration) -> io::Result<bool> {
    let connection = dbus::blocking::Connection::new_session().map_err(dbus_error)?;
    let unit = connection.with_proxy(SYSTEMD_DESTINATION, GAMMASTEP_PATH, timeout);
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

fn ensure_active(control: &RunControl) -> io::Result<()> {
    if control.is_cancelled() {
        Err(io::Error::new(
            io::ErrorKind::Interrupted,
            "night-light probe was cancelled",
        ))
    } else if control.remaining().is_zero() {
        Err(io::Error::new(
            io::ErrorKind::TimedOut,
            "night-light probe exceeded its deadline",
        ))
    } else {
        Ok(())
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
