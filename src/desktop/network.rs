// SPDX-License-Identifier: GPL-3.0-only

use std::{collections::BTreeSet, io, time::Duration};

use sleepy_sdk::{
    NetworkAccessPoint, NetworkCommand, NetworkConnection, NetworkConnectionKind, NetworkSnapshot,
    StableId,
};

use crate::system::{CommandRunner, CommandSpec, RunnerErrorKind};

const ACCESS_POINT_PREFIX: &str = "wifi-ap:";
const CONNECTION_PREFIX: &str = "nm-connection:";

pub fn mutation_spec(command: &NetworkCommand) -> io::Result<CommandSpec> {
    let args = match command {
        NetworkCommand::SetWifiEnabled { enabled } => {
            vec!["radio".into(), "wifi".into(), on_off(*enabled).into()]
        }
        NetworkCommand::ScanWifi => vec!["device".into(), "wifi".into(), "rescan".into()],
        NetworkCommand::ConnectWifi { access_point_id } => vec![
            "device".into(),
            "wifi".into(),
            "connect".into(),
            decode_mac(access_point_id, ACCESS_POINT_PREFIX)?,
        ],
        NetworkCommand::Disconnect { connection_id } => vec![
            "connection".into(),
            "down".into(),
            "uuid".into(),
            decode_uuid(connection_id)?,
        ],
    };
    Ok(bounded("nmcli", args))
}

pub fn probe<R: CommandRunner>(runner: &R) -> io::Result<NetworkSnapshot> {
    probe_readback(runner).map(|readback| readback.snapshot)
}

struct NetworkReadback {
    snapshot: NetworkSnapshot,
    active_access_points: BTreeSet<String>,
}

fn probe_readback<R: CommandRunner>(runner: &R) -> io::Result<NetworkReadback> {
    let radio = run(
        runner,
        CommandSpec::new("nmcli", ["--terse", "--fields", "WIFI", "general"]),
    )?;
    let access_points = run(
        runner,
        CommandSpec::new(
            "nmcli",
            [
                "--terse",
                "--escape",
                "yes",
                "--fields",
                "IN-USE,BSSID,SSID,SIGNAL,SECURITY",
                "device",
                "wifi",
                "list",
                "--rescan",
                "no",
            ],
        ),
    )?;
    let connections = run(
        runner,
        CommandSpec::new(
            "nmcli",
            [
                "--terse",
                "--escape",
                "yes",
                "--fields",
                "UUID,NAME,TYPE,DEVICE",
                "connection",
                "show",
                "--active",
            ],
        ),
    )?;
    parse_readback(&radio, &access_points, &connections)
}

pub fn mutate<R: CommandRunner>(
    runner: &R,
    command: &NetworkCommand,
) -> io::Result<NetworkSnapshot> {
    run(runner, mutation_spec(command)?)?;
    let readback = probe_readback(runner)?;
    let snapshot = &readback.snapshot;
    let confirmed = match command {
        NetworkCommand::SetWifiEnabled { enabled } => snapshot.wifi_enabled == *enabled,
        NetworkCommand::ScanWifi => !snapshot.scanning,
        NetworkCommand::ConnectWifi { access_point_id } => readback
            .active_access_points
            .contains(access_point_id.as_str()),
        NetworkCommand::Disconnect { connection_id } => snapshot
            .connections
            .iter()
            .all(|connection| connection.id != connection_id.as_str() || !connection.connected),
    };
    if !confirmed {
        return Err(io::Error::other(
            "NetworkManager readback did not confirm the requested state",
        ));
    }
    Ok(readback.snapshot)
}

pub fn parse_snapshot(
    radio: &[u8],
    access_points: &[u8],
    connections: &[u8],
) -> io::Result<NetworkSnapshot> {
    parse_readback(radio, access_points, connections).map(|readback| readback.snapshot)
}

fn parse_readback(
    radio: &[u8],
    access_points: &[u8],
    connections: &[u8],
) -> io::Result<NetworkReadback> {
    let wifi_enabled = match text(radio)?.trim() {
        "enabled" => true,
        "disabled" => false,
        _ => return invalid("NetworkManager returned an unknown Wi-Fi state"),
    };
    let mut parsed_access_points = Vec::new();
    let mut access_point_ids = BTreeSet::new();
    let mut active_access_points = BTreeSet::new();
    if wifi_enabled {
        for line in text(access_points)?.lines().filter(|line| !line.is_empty()) {
            let fields = split_escaped(line)?;
            if fields.len() != 5 || !matches!(fields[0].as_str(), "" | "*") {
                return invalid("NetworkManager access-point row is malformed");
            }
            if fields[2].is_empty() {
                continue;
            }
            let id = access_point_id(&fields[1])?;
            if !access_point_ids.insert(id.clone()) {
                continue;
            }
            if fields[0] == "*" {
                active_access_points.insert(id.clone());
            }
            let signal = fields[3]
                .parse::<u8>()
                .map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "invalid Wi-Fi signal"))?;
            if signal > 100 {
                return invalid("Wi-Fi signal is outside 0..100");
            }
            parsed_access_points.push(NetworkAccessPoint {
                id,
                ssid: fields[2].clone(),
                signal_level: f64::from(signal) / 100.0,
                secured: !matches!(fields[4].as_str(), "" | "--" | "NONE"),
            });
            if parsed_access_points.len() > 4_096 {
                return invalid("too many Wi-Fi access points");
            }
        }
    }
    let mut parsed_connections = Vec::new();
    let mut connection_ids = BTreeSet::new();
    for line in text(connections)?.lines().filter(|line| !line.is_empty()) {
        let fields = split_escaped(line)?;
        if fields.len() != 4 || fields[1].is_empty() {
            return invalid("NetworkManager connection row is malformed");
        }
        let kind = match fields[2].as_str() {
            "802-11-wireless" | "wifi" => NetworkConnectionKind::Wifi,
            "802-3-ethernet" | "ethernet" => NetworkConnectionKind::Ethernet,
            value if value.contains("vpn") || value == "wireguard" => NetworkConnectionKind::Vpn,
            _ => continue,
        };
        let id = connection_id(&fields[0])?;
        if !connection_ids.insert(id.clone()) {
            return invalid("duplicate NetworkManager connection UUID");
        }
        parsed_connections.push(NetworkConnection {
            id,
            name: fields[1].clone(),
            kind,
            connected: !fields[3].is_empty() && fields[3] != "--",
        });
    }
    Ok(NetworkReadback {
        snapshot: NetworkSnapshot {
            wifi_enabled,
            scanning: false,
            access_points: parsed_access_points,
            connections: parsed_connections,
        },
        active_access_points,
    })
}

pub(crate) fn access_point_id(mac: &str) -> io::Result<String> {
    canonical_mac(mac).map(|mac| format!("{ACCESS_POINT_PREFIX}{}", mac.replace(':', "-")))
}

pub(crate) fn connection_id(uuid: &str) -> io::Result<String> {
    let parsed = uuid::Uuid::parse_str(uuid)
        .map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "invalid connection UUID"))?;
    let canonical = parsed.hyphenated().to_string();
    if canonical != uuid {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "connection UUID is not canonical",
        ));
    }
    Ok(format!("{CONNECTION_PREFIX}{canonical}"))
}

fn decode_uuid(id: &StableId) -> io::Result<String> {
    let value = id
        .as_str()
        .strip_prefix(CONNECTION_PREFIX)
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "invalid connection ID"))?;
    connection_id(value)?;
    Ok(value.to_owned())
}

fn decode_mac(id: &StableId, prefix: &str) -> io::Result<String> {
    let encoded = id
        .as_str()
        .strip_prefix(prefix)
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "invalid device ID"))?;
    canonical_mac(&encoded.replace('-', ":"))
}

pub(crate) fn canonical_mac(value: &str) -> io::Result<String> {
    let bytes = value.as_bytes();
    if bytes.len() != 17
        || bytes
            .iter()
            .enumerate()
            .any(|(index, byte)| match index % 3 {
                2 => *byte != b':',
                _ => !byte.is_ascii_hexdigit(),
            })
    {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "invalid hardware address",
        ));
    }
    Ok(value.to_ascii_uppercase())
}

fn on_off(value: bool) -> &'static str {
    if value {
        "on"
    } else {
        "off"
    }
}

fn bounded(program: &str, args: Vec<String>) -> CommandSpec {
    let mut spec = CommandSpec::new(program, args);
    spec.timeout = Duration::from_secs(10);
    spec
}

pub(crate) fn run<R: CommandRunner>(runner: &R, spec: CommandSpec) -> io::Result<Vec<u8>> {
    match runner.run(&spec) {
        Ok(output) if output.status == 0 => Ok(output.stdout),
        Ok(_) => Err(io::Error::other("desktop adapter command failed")),
        Err(error) => Err(io::Error::new(
            match error.kind() {
                RunnerErrorKind::Timeout | RunnerErrorKind::Cancelled => io::ErrorKind::TimedOut,
                RunnerErrorKind::Spawn => io::ErrorKind::NotFound,
                RunnerErrorKind::Io => io::ErrorKind::Other,
            },
            "desktop adapter command could not complete",
        )),
    }
}

fn text(bytes: &[u8]) -> io::Result<&str> {
    std::str::from_utf8(bytes)
        .map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "adapter output is not UTF-8"))
}

fn split_escaped(line: &str) -> io::Result<Vec<String>> {
    let mut fields = vec![String::new()];
    let mut escaped = false;
    for character in line.chars() {
        if escaped {
            fields.last_mut().expect("one field").push(character);
            escaped = false;
        } else if character == '\\' {
            escaped = true;
        } else if character == ':' {
            fields.push(String::new());
        } else {
            fields.last_mut().expect("one field").push(character);
        }
    }
    if escaped {
        return invalid("adapter row ends with an incomplete escape");
    }
    Ok(fields)
}

fn invalid<T>(message: &'static str) -> io::Result<T> {
    Err(io::Error::new(io::ErrorKind::InvalidData, message))
}
