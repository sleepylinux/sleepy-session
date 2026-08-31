// SPDX-License-Identifier: GPL-3.0-only

use std::{io, sync::Arc, time::Duration};

use async_trait::async_trait;
use sleepy_sdk::{
    AudioNode, AudioNodeKind, AudioSnapshot, BluetoothDevice, BluetoothSnapshot,
    CapabilityAvailability, CapabilityRecord, CapabilityValue, DesktopPowerSnapshot, MediaPlayer,
    MediaSnapshot, NetworkConnection, NetworkConnectionKind, NetworkSnapshot, PowerProfile,
    RuntimeCapabilityId,
};
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

use super::{
    DesktopDomainId, DesktopDomainState, DesktopDomainUpdate, DesktopDomainValue, DesktopProducer,
    ProducerError,
};
use crate::system::{
    CommandOutput, CommandRunner, CommandSpec, ProcessCommandRunner, RunControl, RunnerError,
    SystemFacade,
};

const REFRESH_INTERVAL: Duration = Duration::from_secs(2);
const PROBE_DEADLINE: Duration = Duration::from_millis(1_750);

#[derive(Clone)]
pub(crate) struct DeadlineRunner<R> {
    inner: R,
    control: RunControl,
}

impl<R> DeadlineRunner<R> {
    pub(crate) fn new(inner: R, timeout: Duration) -> Self {
        Self {
            inner,
            control: RunControl::for_timeout(timeout),
        }
    }
}

impl<R: CommandRunner> CommandRunner for DeadlineRunner<R> {
    fn run(&self, command: &CommandSpec) -> Result<CommandOutput, RunnerError> {
        if self.control.remaining().is_zero() {
            return Err(RunnerError::timeout(
                "desktop domain operation exceeded its total deadline",
            ));
        }
        self.inner.run_controlled(command, &self.control)
    }
}

pub struct CoreSystemProducer<R: CommandRunner = ProcessCommandRunner> {
    domain: DesktopDomainId,
    facade: Arc<SystemFacade<R>>,
}

impl CoreSystemProducer<ProcessCommandRunner> {
    pub fn production(
        domain: DesktopDomainId,
        facade: Arc<SystemFacade<ProcessCommandRunner>>,
    ) -> io::Result<Self> {
        Self::new(domain, facade)
    }
}

impl<R: CommandRunner> CoreSystemProducer<R> {
    pub fn new(domain: DesktopDomainId, facade: Arc<SystemFacade<R>>) -> io::Result<Self> {
        if !matches!(
            domain,
            DesktopDomainId::Network
                | DesktopDomainId::Bluetooth
                | DesktopDomainId::Audio
                | DesktopDomainId::Media
                | DesktopDomainId::Battery
                | DesktopDomainId::Display
                | DesktopDomainId::Power
        ) {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "core system producer was assigned a non-system domain",
            ));
        }
        Ok(Self { domain, facade })
    }

    async fn probe(&self) -> DesktopDomainState {
        let facade = Arc::clone(&self.facade);
        let domain = self.domain;
        match tokio::task::spawn_blocking(move || probe_domain(&facade, domain)).await {
            Ok(state) => state,
            Err(_) => DesktopDomainState::terminal(
                domain,
                CapabilityAvailability::Error,
                "core system probe worker failed",
            )
            .expect("static diagnostic"),
        }
    }
}

#[async_trait]
impl<R: CommandRunner> DesktopProducer for CoreSystemProducer<R> {
    fn domain(&self) -> DesktopDomainId {
        self.domain
    }

    async fn initial(&self) -> DesktopDomainState {
        self.probe().await
    }

    async fn run(
        &self,
        sender: mpsc::Sender<DesktopDomainUpdate>,
        cancellation: CancellationToken,
    ) -> Result<(), ProducerError> {
        let mut previous = None;
        let mut interval = tokio::time::interval(REFRESH_INTERVAL);
        interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        interval.tick().await;
        loop {
            tokio::select! {
                biased;
                _ = cancellation.cancelled() => return Ok(()),
                _ = interval.tick() => {
                    let current = self.probe().await;
                    if previous.as_ref() != Some(&current) {
                        sender.send(DesktopDomainUpdate { state: current.clone() })
                            .await
                            .map_err(|_| ProducerError::new("desktop state authority stopped"))?;
                        previous = Some(current);
                    }
                }
            }
        }
    }
}

pub fn probe_domain<R: CommandRunner>(
    facade: &SystemFacade<R>,
    domain: DesktopDomainId,
) -> DesktopDomainState {
    let runner = DeadlineRunner::new(facade.runner(), PROBE_DEADLINE);
    let rich = match domain {
        DesktopDomainId::Network => super::network::probe(&runner).map(DesktopDomainValue::Network),
        DesktopDomainId::Bluetooth => {
            super::bluetooth::probe(&runner).map(DesktopDomainValue::Bluetooth)
        }
        DesktopDomainId::Audio => super::audio::probe(&runner).map(DesktopDomainValue::Audio),
        DesktopDomainId::Media => super::media::probe(&runner).map(DesktopDomainValue::Media),
        DesktopDomainId::Display => super::display::probe(&runner).map(DesktopDomainValue::Display),
        DesktopDomainId::Power => super::power::probe(&runner).map(DesktopDomainValue::Power),
        _ => return probe_legacy_domain(facade, domain),
    };
    match rich {
        Ok(value) => DesktopDomainState::available(domain, value).unwrap_or_else(|error| {
            DesktopDomainState::terminal(domain, CapabilityAvailability::Parse, error.to_string())
                .expect("validated terminal state")
        }),
        Err(error) => {
            DesktopDomainState::terminal(domain, availability_for_io(&error), error.to_string())
                .expect("adapter error has diagnostic")
        }
    }
}

fn probe_legacy_domain<R: CommandRunner>(
    facade: &SystemFacade<R>,
    domain: DesktopDomainId,
) -> DesktopDomainState {
    let runtime_id = match domain {
        DesktopDomainId::Network => RuntimeCapabilityId::Network,
        DesktopDomainId::Bluetooth => RuntimeCapabilityId::Bluetooth,
        DesktopDomainId::Audio => RuntimeCapabilityId::Audio,
        DesktopDomainId::Media => RuntimeCapabilityId::Media,
        DesktopDomainId::Battery => RuntimeCapabilityId::Battery,
        _ => unreachable!("constructor limits legacy core domains"),
    };
    state_from_record(domain, facade.runtime_capability(runtime_id))
}

fn availability_for_io(error: &io::Error) -> CapabilityAvailability {
    match error.kind() {
        io::ErrorKind::NotFound | io::ErrorKind::NotConnected => {
            CapabilityAvailability::Unavailable
        }
        io::ErrorKind::PermissionDenied => CapabilityAvailability::PermissionDenied,
        io::ErrorKind::TimedOut => CapabilityAvailability::Timeout,
        io::ErrorKind::InvalidData | io::ErrorKind::InvalidInput => CapabilityAvailability::Parse,
        io::ErrorKind::Unsupported => CapabilityAvailability::Unsupported,
        _ => CapabilityAvailability::Error,
    }
}

pub fn state_from_record(domain: DesktopDomainId, record: CapabilityRecord) -> DesktopDomainState {
    if record.status != CapabilityAvailability::Available {
        return terminal_from_record(domain, record);
    }
    let value = match (domain, record.value) {
        (DesktopDomainId::Network, Some(CapabilityValue::Network(value))) => {
            let connections = value
                .active_connection_id
                .into_iter()
                .map(|id| NetworkConnection {
                    name: id.clone(),
                    id,
                    kind: NetworkConnectionKind::Wifi,
                    connected: true,
                })
                .collect();
            Some(DesktopDomainValue::Network(NetworkSnapshot {
                wifi_enabled: value.wifi_enabled,
                scanning: false,
                access_points: Vec::new(),
                connections,
            }))
        }
        (DesktopDomainId::Bluetooth, Some(CapabilityValue::Bluetooth(value))) => {
            let devices = value
                .connected_device_ids
                .into_iter()
                .map(|id| BluetoothDevice {
                    name: id.clone(),
                    id,
                    paired: true,
                    connected: true,
                })
                .collect();
            Some(DesktopDomainValue::Bluetooth(BluetoothSnapshot {
                powered: value.powered,
                scanning: false,
                devices,
            }))
        }
        (DesktopDomainId::Audio, Some(CapabilityValue::Audio(value))) => {
            let output_id = value
                .default_output_id
                .unwrap_or_else(|| "default-output".into());
            Some(DesktopDomainValue::Audio(AudioSnapshot {
                nodes: vec![
                    AudioNode {
                        id: output_id,
                        name: "Default output".into(),
                        kind: AudioNodeKind::Output,
                        volume: value.output_level,
                        muted: value.output_muted,
                        is_default: true,
                    },
                    AudioNode {
                        id: "default-input".into(),
                        name: "Default input".into(),
                        kind: AudioNodeKind::Input,
                        volume: value.input_level,
                        muted: value.input_muted,
                        is_default: true,
                    },
                ],
                streams: Vec::new(),
            }))
        }
        (DesktopDomainId::Media, Some(CapabilityValue::Media(value))) => {
            Some(DesktopDomainValue::Media(MediaSnapshot {
                players: vec![MediaPlayer {
                    id: value.player_id,
                    identity: "MPRIS".into(),
                    title: value.title,
                    artist: value.artist,
                    playing: value.playing,
                    progress: 0.0,
                }],
            }))
        }
        (DesktopDomainId::Battery, Some(CapabilityValue::Battery(value))) => {
            Some(DesktopDomainValue::Battery(sleepy_sdk::BatterySnapshot {
                level: f64::from(value.percentage) / 100.0,
                charging: value.charging,
                seconds_remaining: value.seconds_remaining,
            }))
        }
        (DesktopDomainId::Power, Some(CapabilityValue::PowerProfile(value))) => {
            let active = parse_profile(&value.active);
            let available = value
                .available
                .iter()
                .filter_map(|profile| parse_profile(profile))
                .collect::<Vec<_>>();
            active
                .filter(|active| available.contains(active))
                .map(|active_profile| {
                    DesktopDomainValue::Power(DesktopPowerSnapshot {
                        active_profile,
                        available_profiles: available,
                    })
                })
        }
        _ => None,
    };
    value
        .and_then(|value| DesktopDomainState::available(domain, value).ok())
        .unwrap_or_else(|| {
            DesktopDomainState::terminal(
                domain,
                CapabilityAvailability::Parse,
                "system adapter returned a mismatched or invalid typed value",
            )
            .expect("static diagnostic")
        })
}

fn terminal_from_record(domain: DesktopDomainId, record: CapabilityRecord) -> DesktopDomainState {
    DesktopDomainState::terminal(
        domain,
        record.status,
        record.diagnostic.map_or_else(
            || "system provider failed".into(),
            |failure| failure.message,
        ),
    )
    .expect("runtime terminal status and diagnostic")
}

fn parse_profile(value: &str) -> Option<PowerProfile> {
    match value {
        "power-saver" => Some(PowerProfile::PowerSaver),
        "balanced" => Some(PowerProfile::Balanced),
        "performance" => Some(PowerProfile::Performance),
        _ => None,
    }
}
