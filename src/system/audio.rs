use std::collections::BTreeSet;

use sleepy_sdk::AudioOutputDevice;

use super::{run_checked, CommandRunner, CommandSpec, ProbeFailure};

#[derive(Debug, Clone, Copy)]
pub(crate) struct LevelState {
    pub(crate) level: f64,
    pub(crate) muted: bool,
}

#[derive(Debug, Clone)]
pub(crate) struct DeviceState {
    pub(crate) selected_id: Option<String>,
    pub(crate) devices: Vec<AudioOutputDevice>,
}

pub(crate) fn probe_output<R: CommandRunner>(runner: &R) -> Result<LevelState, ProbeFailure> {
    let output = run_checked(
        runner,
        CommandSpec::new("wpctl", ["get-volume", "@DEFAULT_AUDIO_SINK@"]),
    )?;
    let (level, muted) = parse_volume(&output)?;
    Ok(LevelState { level, muted })
}

pub(crate) fn probe_microphone<R: CommandRunner>(runner: &R) -> Result<LevelState, ProbeFailure> {
    let output = run_checked(
        runner,
        CommandSpec::new("wpctl", ["get-volume", "@DEFAULT_AUDIO_SOURCE@"]),
    )?;
    let (level, muted) = parse_volume(&output)?;
    Ok(LevelState { level, muted })
}

pub(crate) fn probe_devices<R: CommandRunner>(runner: &R) -> Result<DeviceState, ProbeFailure> {
    let status = run_checked(runner, CommandSpec::new("wpctl", ["status", "--name"]))?;
    let (selected_id, devices) = parse_sinks(&status)?;
    Ok(DeviceState {
        selected_id,
        devices,
    })
}

fn parse_volume(bytes: &[u8]) -> Result<(f64, bool), ProbeFailure> {
    let text = std::str::from_utf8(bytes)
        .map_err(|_| ProbeFailure::parse("wpctl volume output is not UTF-8"))?;
    let mut fields = text.split_whitespace();
    if fields.next() != Some("Volume:") {
        return Err(ProbeFailure::parse("wpctl volume output is malformed"));
    }
    let value = fields
        .next()
        .ok_or_else(|| ProbeFailure::parse("wpctl omitted volume"))?
        .parse::<f64>()
        .map_err(|_| ProbeFailure::parse("wpctl volume is not numeric"))?;
    if !(0.0..=1.0).contains(&value) {
        return Err(ProbeFailure::parse("wpctl volume is outside 0..1"));
    }
    Ok((value, text.contains("[MUTED]")))
}

fn parse_sinks(bytes: &[u8]) -> Result<(Option<String>, Vec<AudioOutputDevice>), ProbeFailure> {
    let text = std::str::from_utf8(bytes)
        .map_err(|_| ProbeFailure::parse("wpctl status output is not UTF-8"))?;
    let mut in_sinks = false;
    let mut devices = Vec::new();
    let mut ids = BTreeSet::new();
    for line in text.lines() {
        let trimmed = line.trim_start_matches([' ', '│', '├', '─']);
        if trimmed == "Sinks:" {
            in_sinks = true;
            continue;
        }
        if in_sinks && trimmed.ends_with(':') {
            break;
        }
        if !in_sinks || trimmed.is_empty() {
            continue;
        }
        let is_default = trimmed.starts_with('*');
        let row = trimmed.trim_start_matches('*').trim();
        let (id, rest) = row
            .split_once('.')
            .ok_or_else(|| ProbeFailure::parse("wpctl sink row is malformed"))?;
        if id.parse::<u64>().is_err() {
            return Err(ProbeFailure::parse("wpctl sink id is invalid"));
        }
        if !ids.insert(id.to_owned()) {
            return Err(ProbeFailure::parse("wpctl sink ids are not unique"));
        }
        let label = rest.split(" [").next().unwrap_or(rest).trim().to_owned();
        if label.is_empty() {
            return Err(ProbeFailure::parse("wpctl sink label is empty"));
        }
        devices.push(AudioOutputDevice {
            id: id.to_owned(),
            label,
            is_default,
        });
    }
    if devices.is_empty() {
        return Err(ProbeFailure::parse("wpctl did not report any sinks"));
    }
    let defaults: Vec<_> = devices.iter().filter(|device| device.is_default).collect();
    if defaults.len() != 1 {
        return Err(ProbeFailure::parse(
            "wpctl sinks require exactly one default",
        ));
    }
    let default = Some(defaults[0].id.clone());
    Ok((default, devices))
}
