// SPDX-License-Identifier: GPL-3.0-only

use std::{collections::BTreeMap, io, time::Duration};

use sleepy_sdk::{BluetoothCommand, BluetoothDevice, BluetoothSnapshot, StableId};

use crate::system::{CommandRunner, CommandSpec};

const DEVICE_PREFIX: &str = "bluetooth:";

pub fn mutation_spec(command: &BluetoothCommand) -> io::Result<CommandSpec> {
    let args = match command {
        BluetoothCommand::SetPowered { powered } => {
            vec!["power".into(), on_off(*powered).into()]
        }
        BluetoothCommand::Scan => vec!["scan".into(), "on".into()],
        BluetoothCommand::Pair { device_id } => vec!["pair".into(), decode(device_id)?],
        BluetoothCommand::Connect { device_id } => vec!["connect".into(), decode(device_id)?],
        BluetoothCommand::Disconnect { device_id } => {
            vec!["disconnect".into(), decode(device_id)?]
        }
    };
    let mut spec = CommandSpec::new("bluetoothctl", args);
    spec.timeout = Duration::from_secs(10);
    Ok(spec)
}

pub fn probe<R: CommandRunner>(runner: &R) -> io::Result<BluetoothSnapshot> {
    let show = super::network::run(runner, CommandSpec::new("bluetoothctl", ["show"]))?;
    let devices = super::network::run(runner, CommandSpec::new("bluetoothctl", ["devices"]))?;
    let mut details = BTreeMap::new();
    for (mac, _) in parse_device_rows(&devices)? {
        let output = super::network::run(
            runner,
            CommandSpec::new("bluetoothctl", ["info", mac.as_str()]),
        )?;
        details.insert(mac, output);
    }
    parse_snapshot(&show, &devices, &details)
}

pub fn mutate<R: CommandRunner>(
    runner: &R,
    command: &BluetoothCommand,
) -> io::Result<BluetoothSnapshot> {
    super::network::run(runner, mutation_spec(command)?)?;
    let snapshot = probe(runner)?;
    let confirmed = match command {
        BluetoothCommand::SetPowered { powered } => snapshot.powered == *powered,
        BluetoothCommand::Scan => snapshot.scanning,
        BluetoothCommand::Pair { device_id } => snapshot
            .devices
            .iter()
            .any(|device| device.id == device_id.as_str() && device.paired),
        BluetoothCommand::Connect { device_id } => snapshot
            .devices
            .iter()
            .any(|device| device.id == device_id.as_str() && device.connected),
        BluetoothCommand::Disconnect { device_id } => snapshot
            .devices
            .iter()
            .any(|device| device.id == device_id.as_str() && !device.connected),
    };
    if !confirmed {
        return Err(io::Error::other(
            "Bluetooth readback did not confirm the requested state",
        ));
    }
    Ok(snapshot)
}

pub fn parse_snapshot(
    show: &[u8],
    devices: &[u8],
    details: &BTreeMap<String, Vec<u8>>,
) -> io::Result<BluetoothSnapshot> {
    let show = text(show)?;
    let powered = yes_no(required_property(show, "Powered")?)?;
    let scanning = yes_no(required_property(show, "Discovering")?)?;
    let rows = parse_device_rows(devices)?;
    if rows.len() > 1_024 {
        return invalid("too many Bluetooth devices");
    }
    let mut parsed = Vec::with_capacity(rows.len());
    for (mac, fallback_name) in rows {
        let info = details.get(&mac).ok_or_else(|| {
            io::Error::new(io::ErrorKind::InvalidData, "Bluetooth details omitted")
        })?;
        let info = text(info)?;
        let name = property(info, "Name")
            .filter(|value| !value.is_empty())
            .unwrap_or(fallback_name.as_str())
            .to_owned();
        if name.is_empty() {
            return invalid("Bluetooth device name is empty");
        }
        parsed.push(BluetoothDevice {
            id: device_id(&mac)?,
            name,
            paired: yes_no(required_property(info, "Paired")?)?,
            connected: yes_no(required_property(info, "Connected")?)?,
        });
    }
    Ok(BluetoothSnapshot {
        powered,
        scanning,
        devices: parsed,
    })
}

pub(crate) fn device_id(mac: &str) -> io::Result<String> {
    super::network::canonical_mac(mac)
        .map(|value| format!("{DEVICE_PREFIX}{}", value.replace(':', "-")))
}

fn decode(id: &StableId) -> io::Result<String> {
    let value = id.as_str().strip_prefix(DEVICE_PREFIX).ok_or_else(|| {
        io::Error::new(io::ErrorKind::InvalidInput, "invalid Bluetooth device ID")
    })?;
    super::network::canonical_mac(&value.replace('-', ":"))
}

fn on_off(value: bool) -> &'static str {
    if value {
        "on"
    } else {
        "off"
    }
}

fn parse_device_rows(bytes: &[u8]) -> io::Result<Vec<(String, String)>> {
    let mut rows = Vec::new();
    for line in text(bytes)?.lines().filter(|line| !line.trim().is_empty()) {
        let mut fields = line.splitn(3, ' ');
        if fields.next() != Some("Device") {
            return invalid("Bluetooth device row is malformed");
        }
        let mac = fields
            .next()
            .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "Bluetooth MAC omitted"))?;
        let name = fields.next().unwrap_or_default().trim();
        super::network::canonical_mac(mac)?;
        rows.push((mac.to_owned(), name.to_owned()));
    }
    Ok(rows)
}

fn property<'a>(text: &'a str, name: &str) -> Option<&'a str> {
    let prefix = format!("{name}: ");
    text.lines()
        .map(str::trim)
        .find_map(|line| line.strip_prefix(&prefix))
}

fn required_property<'a>(text: &'a str, name: &str) -> io::Result<&'a str> {
    property(text, name)
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "Bluetooth property omitted"))
}

fn yes_no(value: &str) -> io::Result<bool> {
    match value {
        "yes" => Ok(true),
        "no" => Ok(false),
        _ => invalid("Bluetooth boolean property is malformed"),
    }
}

fn text(bytes: &[u8]) -> io::Result<&str> {
    std::str::from_utf8(bytes)
        .map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "Bluetooth output is not UTF-8"))
}

fn invalid<T>(message: &'static str) -> io::Result<T> {
    Err(io::Error::new(io::ErrorKind::InvalidData, message))
}
