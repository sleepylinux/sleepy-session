// SPDX-License-Identifier: GPL-3.0-only

use std::{collections::BTreeSet, io, time::Duration};

use sleepy_sdk::{DesktopPowerSnapshot, PowerProfile};

use crate::system::{CommandRunner, CommandSpec};

pub fn mutation_spec(profile: PowerProfile) -> CommandSpec {
    let mut spec = CommandSpec::new("powerprofilesctl", ["set", profile_name(profile)]);
    spec.timeout = Duration::from_secs(5);
    spec
}

pub fn probe<R: CommandRunner>(runner: &R) -> io::Result<DesktopPowerSnapshot> {
    let current = super::network::run(runner, CommandSpec::new("powerprofilesctl", ["get"]))?;
    let available = super::network::run(runner, CommandSpec::new("powerprofilesctl", ["list"]))?;
    let active_profile = parse_profile(text(&current)?.trim())?;
    let available_profiles = text(&available)?
        .lines()
        .filter_map(|line| {
            line.trim()
                .strip_prefix('*')
                .unwrap_or(line.trim())
                .trim()
                .strip_suffix(':')
        })
        .filter(|value| matches!(*value, "power-saver" | "balanced" | "performance"))
        .map(parse_profile)
        .collect::<io::Result<Vec<_>>>()?;
    let unique = available_profiles.iter().copied().collect::<BTreeSet<_>>();
    if available_profiles.is_empty()
        || unique.len() != available_profiles.len()
        || !available_profiles.contains(&active_profile)
    {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "power profile readback is inconsistent",
        ));
    }
    Ok(DesktopPowerSnapshot {
        active_profile,
        available_profiles,
    })
}

pub fn mutate<R: CommandRunner>(
    runner: &R,
    profile: PowerProfile,
) -> io::Result<DesktopPowerSnapshot> {
    super::network::run(runner, mutation_spec(profile))?;
    let snapshot = probe(runner)?;
    if snapshot.active_profile != profile {
        return Err(io::Error::other(
            "power profile readback did not confirm the requested state",
        ));
    }
    Ok(snapshot)
}

fn profile_name(profile: PowerProfile) -> &'static str {
    match profile {
        PowerProfile::PowerSaver => "power-saver",
        PowerProfile::Balanced => "balanced",
        PowerProfile::Performance => "performance",
    }
}

fn parse_profile(value: &str) -> io::Result<PowerProfile> {
    match value {
        "power-saver" => Ok(PowerProfile::PowerSaver),
        "balanced" => Ok(PowerProfile::Balanced),
        "performance" => Ok(PowerProfile::Performance),
        _ => Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "power profile is unknown",
        )),
    }
}

fn text(bytes: &[u8]) -> io::Result<&str> {
    std::str::from_utf8(bytes).map_err(|_| {
        io::Error::new(
            io::ErrorKind::InvalidData,
            "power profile output is not UTF-8",
        )
    })
}
