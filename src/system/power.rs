use std::collections::BTreeSet;

use sleepy_sdk::PowerProfile;

use super::{run_checked, CommandRunner, CommandSpec, ProbeFailure};

pub(crate) fn probe_profiles<R: CommandRunner>(
    runner: &R,
) -> Result<(Option<PowerProfile>, Vec<PowerProfile>), ProbeFailure> {
    let current = run_checked(runner, CommandSpec::new("powerprofilesctl", ["get"]))?;
    let available = run_checked(runner, CommandSpec::new("powerprofilesctl", ["list"]))?;
    let current_profile = Some(parse_profile(text(&current)?.trim())?);
    let available_profiles = text(&available)?
        .lines()
        .filter_map(|line| {
            let trimmed = line.trim();
            let token = trimmed
                .strip_prefix('*')
                .unwrap_or(trimmed)
                .trim()
                .strip_suffix(':')?;
            matches!(token, "power-saver" | "balanced" | "performance").then_some(token)
        })
        .map(parse_profile)
        .collect::<Result<Vec<_>, _>>()?;
    if available_profiles.is_empty() {
        return Err(ProbeFailure::parse("powerprofilesctl reported no profiles"));
    }
    let unique: BTreeSet<_> = available_profiles.iter().copied().collect();
    if unique.len() != available_profiles.len() {
        return Err(ProbeFailure::parse(
            "powerprofilesctl profile headers are not unique",
        ));
    }
    if current_profile.is_some_and(|current| !available_profiles.contains(&current)) {
        return Err(ProbeFailure::parse(
            "powerprofilesctl current profile is not available",
        ));
    }
    Ok((current_profile, available_profiles))
}

pub(crate) fn probe_battery<R: CommandRunner>(
    runner: &R,
) -> Result<(Option<f64>, Option<bool>), ProbeFailure> {
    let command = CommandSpec::new(
        "upower",
        [
            "--show-info",
            "/org/freedesktop/UPower/devices/DisplayDevice",
        ],
    );
    let battery = match runner.run(&command) {
        Ok(output) if output.status == 0 => output.stdout,
        Ok(output) if output.status == 1 => {
            return Err(ProbeFailure::unsupported("no battery is available"))
        }
        Ok(output) => {
            return Err(ProbeFailure {
                kind: sleepy_sdk::CapabilityErrorKind::Command,
                message: format!("upower exited with status {}", output.status),
            })
        }
        Err(error) => {
            return Err(ProbeFailure {
                kind: match error.kind() {
                    super::RunnerErrorKind::Timeout | super::RunnerErrorKind::Cancelled => {
                        sleepy_sdk::CapabilityErrorKind::Timeout
                    }
                    super::RunnerErrorKind::Spawn => sleepy_sdk::CapabilityErrorKind::Unsupported,
                    super::RunnerErrorKind::Io => sleepy_sdk::CapabilityErrorKind::Command,
                },
                message: error.message().to_owned(),
            })
        }
    };
    let battery = text(&battery)?;
    let state = field(battery, "state:")
        .ok_or_else(|| ProbeFailure::parse("UPower omitted battery state"))?;
    let percentage = field(battery, "percentage:");
    let battery_level = percentage
        .map(|value| {
            value
                .strip_suffix('%')
                .ok_or_else(|| ProbeFailure::parse("UPower percentage is malformed"))?
                .parse::<f64>()
                .map_err(|_| ProbeFailure::parse("UPower percentage is not numeric"))
                .and_then(|value| {
                    (0.0..=100.0)
                        .contains(&value)
                        .then_some(value / 100.0)
                        .ok_or_else(|| ProbeFailure::parse("UPower percentage is outside 0..100"))
                })
        })
        .transpose()?;
    // `charging` is the UI's "on external power / filling" signal. Pending
    // charge belongs with it; pending discharge belongs with discharging.
    // UPower's explicit `unknown` is represented by the SDK's nullable bool.
    let charging = match state {
        "charging" | "fully-charged" | "pending-charge" => Some(true),
        "discharging" | "pending-discharge" | "empty" => Some(false),
        "unknown" => None,
        _ => {
            return Err(ProbeFailure::parse(
                "UPower returned an unknown battery state",
            ))
        }
    };
    Ok((battery_level, charging))
}

fn parse_profile(value: &str) -> Result<PowerProfile, ProbeFailure> {
    match value {
        "power-saver" => Ok(PowerProfile::PowerSaver),
        "balanced" => Ok(PowerProfile::Balanced),
        "performance" => Ok(PowerProfile::Performance),
        _ => Err(ProbeFailure::parse(
            "powerprofilesctl returned an unknown profile",
        )),
    }
}

fn field<'a>(text: &'a str, key: &str) -> Option<&'a str> {
    text.lines()
        .find_map(|line| line.trim().strip_prefix(key).map(str::trim))
}

fn text(output: &[u8]) -> Result<&str, ProbeFailure> {
    std::str::from_utf8(output)
        .map_err(|_| ProbeFailure::parse("power adapter output is not UTF-8"))
}
