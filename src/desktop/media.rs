// SPDX-License-Identifier: GPL-3.0-only

use std::{
    collections::{BTreeMap, BTreeSet},
    io,
    time::Duration,
};

use sleepy_sdk::{MediaCommand, MediaPlayer, MediaSnapshot, MediaTransport};

use crate::system::{CommandRunner, CommandSpec};

const PLAYER_PREFIX: &str = "mpris:";
const MAX_PROBED_PLAYERS: usize = 64;

pub fn mutation_spec(command: &MediaCommand) -> io::Result<CommandSpec> {
    let MediaCommand::Transport {
        player_id,
        transport,
    } = command;
    let player = player_id
        .as_str()
        .strip_prefix(PLAYER_PREFIX)
        .filter(|value| valid_bus_name(value))
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "invalid MPRIS player ID"))?;
    let action = match transport {
        MediaTransport::PlayPause => "play-pause",
        MediaTransport::Next => "next",
        MediaTransport::Previous => "previous",
    };
    let mut spec = CommandSpec::new("playerctl", ["--player", player, action]);
    spec.timeout = Duration::from_secs(5);
    Ok(spec)
}

pub fn probe<R: CommandRunner>(runner: &R) -> io::Result<MediaSnapshot> {
    Ok(probe_observation(runner)?.snapshot)
}

struct MediaObservation {
    snapshot: MediaSnapshot,
    track_ids: BTreeMap<String, Option<String>>,
}

fn probe_observation<R: CommandRunner>(runner: &R) -> io::Result<MediaObservation> {
    let players = super::network::run(runner, CommandSpec::new("playerctl", ["--list-all"]))?;
    let mut metadata = BTreeMap::new();
    for player in player_names(&players)? {
        let output = super::network::run(
            runner,
            CommandSpec::new(
                "playerctl",
                [
                    "--player",
                    player.as_str(),
                    "metadata",
                    "--format",
                    "{{playerName}}\\t{{title}}\\t{{artist}}\\t{{status}}\\t{{position}}\\t{{mpris:length}}\\t{{mpris:trackid}}",
                ],
            ),
        )?;
        metadata.insert(player, output);
    }
    parse_observation(&players, &metadata)
}

pub fn mutate<R: CommandRunner>(runner: &R, command: &MediaCommand) -> io::Result<MediaSnapshot> {
    let before = probe_observation(runner)?;
    let MediaCommand::Transport {
        player_id,
        transport,
    } = command;
    let before_player = before
        .snapshot
        .players
        .iter()
        .find(|player| player.id == player_id.as_str())
        .ok_or_else(|| io::Error::other("MPRIS pre-state omitted the targeted player"))?;
    super::network::run(runner, mutation_spec(command)?)?;
    let after = probe_observation(runner)?;
    let after_player = after
        .snapshot
        .players
        .iter()
        .find(|player| player.id == player_id.as_str())
        .ok_or_else(|| io::Error::other("MPRIS readback omitted the targeted player"))?;
    let confirmed = match transport {
        MediaTransport::PlayPause => before_player.playing != after_player.playing,
        MediaTransport::Next | MediaTransport::Previous => {
            matches!(
                (
                    before.track_ids.get(player_id.as_str()).and_then(Option::as_ref),
                    after.track_ids.get(player_id.as_str()).and_then(Option::as_ref),
                ),
                (Some(before), Some(after)) if before != after
            )
        }
    };
    if !confirmed {
        return Err(io::Error::other(
            "MPRIS readback did not confirm the requested transport",
        ));
    }
    Ok(after.snapshot)
}

pub fn mutate_legacy<R: CommandRunner>(
    runner: &R,
    transport: MediaTransport,
) -> io::Result<MediaSnapshot> {
    let before = probe(runner)?;
    let [player] = before.players.as_slice() else {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "legacy media action requires exactly one MPRIS player",
        ));
    };
    mutate(
        runner,
        &MediaCommand::Transport {
            player_id: sleepy_sdk::StableId(player.id.clone()),
            transport,
        },
    )
}

pub fn parse_snapshot(
    players: &[u8],
    metadata: &BTreeMap<String, Vec<u8>>,
) -> io::Result<MediaSnapshot> {
    Ok(parse_observation(players, metadata)?.snapshot)
}

fn parse_observation(
    players: &[u8],
    metadata: &BTreeMap<String, Vec<u8>>,
) -> io::Result<MediaObservation> {
    let names = player_names(players)?;
    if names.len() > 256 {
        return invalid("too many MPRIS players");
    }
    let mut parsed = Vec::with_capacity(names.len());
    let mut track_ids = BTreeMap::new();
    for name in names {
        let bytes = metadata
            .get(&name)
            .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "MPRIS metadata omitted"))?;
        let text = std::str::from_utf8(bytes)
            .map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "MPRIS metadata is not UTF-8"))?
            .trim_end();
        let fields = text.splitn(7, '\t').collect::<Vec<_>>();
        if !(fields.len() == 6 || fields.len() == 7) || fields[0].trim().is_empty() {
            return invalid("MPRIS metadata row is malformed");
        }
        if fields[0].len() > 256 || fields[1].len() > 4_096 || fields[2].len() > 2_048 {
            return invalid("MPRIS metadata text exceeds its bound");
        }
        let playing = match fields[3] {
            "Playing" => true,
            "Paused" | "Stopped" => false,
            _ => return invalid("MPRIS playback state is unknown"),
        };
        let position = fields[4]
            .parse::<f64>()
            .map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "MPRIS position is invalid"))?;
        let length = fields[5]
            .parse::<f64>()
            .map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "MPRIS length is invalid"))?;
        let progress = if length > 0.0 { position / length } else { 0.0 };
        if !progress.is_finite() || !(0.0..=1.0).contains(&progress) {
            return invalid("MPRIS normalized progress is invalid");
        }
        let track_id = fields
            .get(6)
            .filter(|value| !value.is_empty())
            .map(|value| {
                if value.len() > 4_096 || !value.starts_with('/') {
                    return invalid("MPRIS track identity is malformed or exceeds its bound");
                }
                Ok((*value).to_owned())
            })
            .transpose()?;
        let stable_player_id = player_id(&name)?;
        track_ids.insert(stable_player_id.clone(), track_id);
        parsed.push(MediaPlayer {
            id: stable_player_id,
            identity: fields[0].to_owned(),
            title: fields[1].to_owned(),
            artist: fields[2].to_owned(),
            playing,
            progress,
        });
    }
    Ok(MediaObservation {
        snapshot: MediaSnapshot { players: parsed },
        track_ids,
    })
}

pub(crate) fn player_id(value: &str) -> io::Result<String> {
    valid_bus_name(value)
        .then(|| format!("{PLAYER_PREFIX}{value}"))
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "invalid MPRIS player name"))
}

fn valid_bus_name(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= 255
        && value.contains('.')
        && value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'.' | b'_' | b'-'))
        && !value.starts_with('-')
}

fn player_names(bytes: &[u8]) -> io::Result<Vec<String>> {
    let text = std::str::from_utf8(bytes).map_err(|_| {
        io::Error::new(io::ErrorKind::InvalidData, "MPRIS player list is not UTF-8")
    })?;
    let mut names = Vec::new();
    let mut identifiers = BTreeSet::new();
    for name in text.lines().filter(|line| !line.is_empty()) {
        player_id(name)?;
        if !identifiers.insert(name) {
            return invalid("duplicate MPRIS player name");
        }
        names.push(name.to_owned());
        if names.len() > MAX_PROBED_PLAYERS {
            return invalid("MPRIS player probe exceeds its call budget");
        }
    }
    Ok(names)
}

fn invalid<T>(message: &'static str) -> io::Result<T> {
    Err(io::Error::new(io::ErrorKind::InvalidData, message))
}
