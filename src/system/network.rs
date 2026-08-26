use sleepy_sdk::{Connectivity, NetworkState};

use super::{run_checked, CommandRunner, CommandSpec, ProbeFailure};

pub(crate) fn probe<R: CommandRunner>(runner: &R) -> Result<NetworkState, ProbeFailure> {
    let radio = run_checked(
        runner,
        CommandSpec::new("nmcli", ["--terse", "--fields", "WIFI", "general"]),
    )?;
    let enabled = match text(&radio)?.trim() {
        "enabled" => true,
        "disabled" => false,
        _ => return Err(ProbeFailure::parse("nmcli returned an unknown Wi-Fi state")),
    };
    if !enabled {
        return Ok(NetworkState {
            enabled,
            connected_name: None,
            signal_level: None,
        });
    }
    let list = run_checked(
        runner,
        CommandSpec::new(
            "nmcli",
            [
                "--terse",
                "--fields",
                "IN-USE,SSID,SIGNAL",
                "device",
                "wifi",
                "list",
                "--rescan",
                "no",
            ],
        ),
    )?;
    let active = text(&list)?
        .lines()
        .find_map(|line| line.strip_prefix("*:"));
    let Some(active) = active else {
        return Ok(NetworkState {
            enabled,
            connected_name: None,
            signal_level: None,
        });
    };
    let (name, signal) = active
        .rsplit_once(':')
        .ok_or_else(|| ProbeFailure::parse("nmcli active network row is malformed"))?;
    let signal = signal
        .parse::<f64>()
        .map_err(|_| ProbeFailure::parse("nmcli signal is not numeric"))?;
    if !(0.0..=100.0).contains(&signal) {
        return Err(ProbeFailure::parse("nmcli signal is outside 0..100"));
    }
    Ok(NetworkState {
        enabled,
        connected_name: Some(name.to_owned()),
        signal_level: Some(signal / 100.0),
    })
}

pub(crate) fn ethernet_connected<R: CommandRunner>(runner: &R) -> Result<bool, ProbeFailure> {
    let devices = run_checked(
        runner,
        CommandSpec::new("nmcli", ["--terse", "--fields", "TYPE,STATE", "device"]),
    )?;
    Ok(text(&devices)?
        .lines()
        .any(|line| line == "ethernet:connected"))
}

pub(crate) fn connectivity<R: CommandRunner>(runner: &R) -> Result<Connectivity, ProbeFailure> {
    let connectivity = run_checked(
        runner,
        CommandSpec::new("nmcli", ["--terse", "--fields", "CONNECTIVITY", "general"]),
    )?;
    match text(&connectivity)?.trim() {
        "unknown" => Ok(Connectivity::Unknown),
        "none" => Ok(Connectivity::None),
        "portal" => Ok(Connectivity::Portal),
        "limited" => Ok(Connectivity::Limited),
        "full" => Ok(Connectivity::Full),
        _ => Err(ProbeFailure::parse(
            "nmcli returned an unknown connectivity state",
        )),
    }
}

fn text(output: &[u8]) -> Result<&str, ProbeFailure> {
    std::str::from_utf8(output).map_err(|_| ProbeFailure::parse("nmcli output is not UTF-8"))
}
