// SPDX-License-Identifier: GPL-3.0-only

use std::{collections::BTreeMap, io, time::Duration};

use sleepy_sdk::{
    AudioCommand, AudioNode, AudioNodeKind, AudioSnapshot, AudioStream, StableId, SystemMutation,
};

use crate::system::{CommandRunner, CommandSpec};

const NODE_PREFIX: &str = "audio-node:";
const STREAM_PREFIX: &str = "audio-stream:";

pub fn mutation_spec(command: &AudioCommand) -> io::Result<CommandSpec> {
    let args = match command {
        AudioCommand::SetDefaultNode { node_id } => {
            vec!["set-default".into(), numeric_id(node_id, NODE_PREFIX)?]
        }
        AudioCommand::SetNodeVolume { node_id, level } => vec![
            "set-volume".into(),
            numeric_id(node_id, NODE_PREFIX)?,
            normalized(*level)?,
        ],
        AudioCommand::SetNodeMuted { node_id, muted } => vec![
            "set-mute".into(),
            numeric_id(node_id, NODE_PREFIX)?,
            boolean(*muted).into(),
        ],
        AudioCommand::SetStreamVolume { stream_id, level } => vec![
            "set-volume".into(),
            numeric_id(stream_id, STREAM_PREFIX)?,
            normalized(*level)?,
        ],
        AudioCommand::SetStreamMuted { stream_id, muted } => vec![
            "set-mute".into(),
            numeric_id(stream_id, STREAM_PREFIX)?,
            boolean(*muted).into(),
        ],
    };
    let mut spec = CommandSpec::new("wpctl", args);
    spec.timeout = Duration::from_secs(5);
    Ok(spec)
}

pub fn probe<R: CommandRunner>(runner: &R) -> io::Result<AudioSnapshot> {
    let status = super::network::run(runner, CommandSpec::new("wpctl", ["status", "--name"]))?;
    let identifiers = parse_identifiers(&status)?;
    let mut volumes = BTreeMap::new();
    for identifier in identifiers {
        let output = super::network::run(
            runner,
            CommandSpec::new("wpctl", ["get-volume", identifier.as_str()]),
        )?;
        volumes.insert(identifier, output);
    }
    parse_snapshot(&status, &volumes)
}

pub fn mutate<R: CommandRunner>(runner: &R, command: &AudioCommand) -> io::Result<AudioSnapshot> {
    super::network::run(runner, mutation_spec(command)?)?;
    let snapshot = probe(runner)?;
    let confirmed = match command {
        AudioCommand::SetDefaultNode { node_id } => snapshot
            .nodes
            .iter()
            .any(|node| node.id == node_id.as_str() && node.is_default),
        AudioCommand::SetNodeVolume { node_id, level } => snapshot
            .nodes
            .iter()
            .any(|node| node.id == node_id.as_str() && close(node.volume, *level)),
        AudioCommand::SetNodeMuted { node_id, muted } => snapshot
            .nodes
            .iter()
            .any(|node| node.id == node_id.as_str() && node.muted == *muted),
        AudioCommand::SetStreamVolume { stream_id, level } => snapshot
            .streams
            .iter()
            .any(|stream| stream.id == stream_id.as_str() && close(stream.volume, *level)),
        AudioCommand::SetStreamMuted { stream_id, muted } => snapshot
            .streams
            .iter()
            .any(|stream| stream.id == stream_id.as_str() && stream.muted == *muted),
    };
    if !confirmed {
        return Err(io::Error::other(
            "WirePlumber readback did not confirm the requested state",
        ));
    }
    Ok(snapshot)
}

pub fn mutate_legacy<R: CommandRunner>(
    runner: &R,
    mutation: &SystemMutation,
) -> io::Result<AudioSnapshot> {
    let (spec, target, target_id, level, muted, default) = match mutation {
        SystemMutation::AudioVolume(level) => (
            CommandSpec::new(
                "wpctl",
                ["set-volume", "@DEFAULT_AUDIO_SINK@", &format!("{level:.6}")],
            ),
            AudioNodeKind::Output,
            None,
            Some(*level),
            None,
            false,
        ),
        SystemMutation::AudioMuted(muted) => (
            CommandSpec::new(
                "wpctl",
                ["set-mute", "@DEFAULT_AUDIO_SINK@", boolean(*muted)],
            ),
            AudioNodeKind::Output,
            None,
            None,
            Some(*muted),
            false,
        ),
        SystemMutation::AudioMicrophoneLevel(level) => (
            CommandSpec::new(
                "wpctl",
                [
                    "set-volume",
                    "@DEFAULT_AUDIO_SOURCE@",
                    &format!("{level:.6}"),
                ],
            ),
            AudioNodeKind::Input,
            None,
            Some(*level),
            None,
            false,
        ),
        SystemMutation::AudioMicrophoneMuted(muted) => (
            CommandSpec::new(
                "wpctl",
                ["set-mute", "@DEFAULT_AUDIO_SOURCE@", boolean(*muted)],
            ),
            AudioNodeKind::Input,
            None,
            None,
            Some(*muted),
            false,
        ),
        SystemMutation::AudioOutputDevice(id) => {
            let target_id = node_id(id)?;
            (
                mutation_spec(&AudioCommand::SetDefaultNode {
                    node_id: StableId(target_id.clone()),
                })?,
                AudioNodeKind::Output,
                Some(target_id),
                None,
                None,
                true,
            )
        }
        _ => {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "legacy mutation is not an audio action",
            ))
        }
    };
    super::network::run(runner, spec)?;
    let snapshot = probe(runner)?;
    let confirmed = snapshot.nodes.iter().any(|node| {
        node.kind == target
            && target_id
                .as_ref()
                .is_none_or(|target_id| &node.id == target_id)
            && (!default || node.is_default)
            && level.is_none_or(|level| close(node.volume, level))
            && muted.is_none_or(|muted| node.muted == muted)
    });
    if !confirmed {
        return Err(io::Error::other(
            "legacy audio readback did not confirm the requested state",
        ));
    }
    Ok(snapshot)
}

pub fn parse_snapshot(
    status: &[u8],
    volumes: &BTreeMap<String, Vec<u8>>,
) -> io::Result<AudioSnapshot> {
    let rows = parse_rows(status)?;
    let default_output = rows
        .iter()
        .find(|row| row.kind == RowKind::Output && row.is_default)
        .map(|row| row.id.clone())
        .ok_or_else(|| {
            io::Error::new(io::ErrorKind::InvalidData, "audio output default omitted")
        })?;
    let mut nodes = Vec::new();
    let mut streams = Vec::new();
    for row in rows {
        let (volume, muted) = parse_volume(volumes.get(&row.id).ok_or_else(|| {
            io::Error::new(io::ErrorKind::InvalidData, "audio volume readback omitted")
        })?)?;
        match row.kind {
            RowKind::Output | RowKind::Input => nodes.push(AudioNode {
                id: node_id(&row.id)?,
                name: row.name,
                kind: if row.kind == RowKind::Output {
                    AudioNodeKind::Output
                } else {
                    AudioNodeKind::Input
                },
                volume,
                muted,
                is_default: row.is_default,
            }),
            RowKind::Stream => streams.push(AudioStream {
                id: stream_id(&row.id)?,
                name: row.name,
                node_id: node_id(&default_output)?,
                volume,
                muted,
            }),
        }
    }
    if nodes.len() > 4_096 || streams.len() > 16_384 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "audio snapshot exceeds its bound",
        ));
    }
    Ok(AudioSnapshot { nodes, streams })
}

pub(crate) fn node_id(value: &str) -> io::Result<String> {
    numeric(value).map(|value| format!("{NODE_PREFIX}{value}"))
}

pub(crate) fn stream_id(value: &str) -> io::Result<String> {
    numeric(value).map(|value| format!("{STREAM_PREFIX}{value}"))
}

fn numeric_id(id: &StableId, prefix: &str) -> io::Result<String> {
    numeric(
        id.as_str()
            .strip_prefix(prefix)
            .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "invalid audio ID"))?,
    )
}

fn numeric(value: &str) -> io::Result<String> {
    let parsed = value
        .parse::<u64>()
        .map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "invalid numeric audio ID"))?;
    if parsed.to_string() != value {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "audio ID is not canonical",
        ));
    }
    Ok(value.to_owned())
}

fn normalized(value: f64) -> io::Result<String> {
    if !value.is_finite() || !(0.0..=1.0).contains(&value) {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "audio level is not normalized",
        ));
    }
    Ok(format!("{value:.6}"))
}

fn boolean(value: bool) -> &'static str {
    if value {
        "1"
    } else {
        "0"
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum RowKind {
    Output,
    Input,
    Stream,
}

struct Row {
    id: String,
    name: String,
    kind: RowKind,
    is_default: bool,
}

fn parse_identifiers(status: &[u8]) -> io::Result<Vec<String>> {
    parse_rows(status).map(|rows| rows.into_iter().map(|row| row.id).collect())
}

fn parse_rows(status: &[u8]) -> io::Result<Vec<Row>> {
    let text = std::str::from_utf8(status)
        .map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "wpctl output is not UTF-8"))?;
    let mut section = None;
    let mut rows = Vec::new();
    for line in text.lines() {
        let trimmed = line.trim_start_matches([' ', '│', '├', '└', '─']);
        section = match trimmed {
            "Sinks:" => Some(RowKind::Output),
            "Sources:" => Some(RowKind::Input),
            "Streams:" => Some(RowKind::Stream),
            value if value.ends_with(':') => None,
            _ => section,
        };
        if matches!(trimmed, "Sinks:" | "Sources:" | "Streams:") || trimmed.is_empty() {
            continue;
        }
        let Some(kind) = section else { continue };
        let is_default = trimmed.starts_with('*');
        let row = trimmed.trim_start_matches('*').trim();
        let Some((id, remainder)) = row.split_once('.') else {
            continue;
        };
        numeric(id.trim())?;
        let name = remainder
            .split(" [")
            .next()
            .unwrap_or(remainder)
            .trim()
            .to_owned();
        if name.is_empty() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "audio row name is empty",
            ));
        }
        rows.push(Row {
            id: id.trim().to_owned(),
            name,
            kind,
            is_default,
        });
    }
    if !rows.iter().any(|row| row.kind == RowKind::Output) {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "wpctl omitted audio outputs",
        ));
    }
    Ok(rows)
}

fn parse_volume(bytes: &[u8]) -> io::Result<(f64, bool)> {
    let text = std::str::from_utf8(bytes)
        .map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "volume output is not UTF-8"))?;
    let mut fields = text.split_whitespace();
    if fields.next() != Some("Volume:") {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "volume output is malformed",
        ));
    }
    let volume = fields
        .next()
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "volume value omitted"))?
        .parse::<f64>()
        .map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "volume is not numeric"))?;
    if !volume.is_finite() || !(0.0..=1.0).contains(&volume) {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "volume is not normalized",
        ));
    }
    Ok((volume, text.contains("[MUTED]")))
}

fn close(left: f64, right: f64) -> bool {
    (left - right).abs() <= 0.005
}
