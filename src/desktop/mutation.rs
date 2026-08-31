// SPDX-License-Identifier: GPL-3.0-only

use std::sync::Arc;

use async_trait::async_trait;
use sleepy_sdk::{
    BluetoothCommand, DesktopCommand, DesktopRequest, DesktopSystemCommand, DesktopSystemMutation,
    DisplayCommand, HyprlandCommand, NetworkCommand, NotificationCommand as SdkNotificationCommand,
    PowerCommand, StableId, SystemMutation,
};
use tokio::sync::Mutex;

use super::{
    adapters::DailyProducer, DesktopDomainId, DesktopDomainState, DesktopDomainValue,
    DesktopMutationExecutor, DesktopProducer, ProducerError,
};
use crate::{
    compositor::{CompositorExecution, HyprlandAdapter},
    daily::{DailyBackend, DailyOperation},
    notifications::{NotificationActionDispatcher, NotificationCommand, NotificationEventService},
    system::{ProcessCommandRunner, RunControl, SystemFacade},
};

pub struct ProductionDesktopMutationExecutor<B: DailyBackend> {
    system: Arc<SystemFacade<ProcessCommandRunner>>,
    daily: Arc<B>,
    notifications: Arc<Mutex<NotificationEventService>>,
    notification_actions: Arc<Mutex<Option<NotificationActionDispatcher>>>,
    hyprland: Option<HyprlandAdapter>,
    logind: super::utilities::ProductionLogind,
    utilities: Arc<super::utilities::ProductionUtilityService>,
    appearance: Arc<super::appearance::AppearanceService>,
}

impl<B: DailyBackend> ProductionDesktopMutationExecutor<B> {
    pub fn new(
        system: Arc<SystemFacade<ProcessCommandRunner>>,
        daily: Arc<B>,
        notifications: Arc<Mutex<NotificationEventService>>,
        notification_actions: Arc<Mutex<Option<NotificationActionDispatcher>>>,
        hyprland: Option<HyprlandAdapter>,
        utilities: Arc<super::utilities::ProductionUtilityService>,
        appearance: Arc<super::appearance::AppearanceService>,
    ) -> Self {
        Self {
            system,
            daily,
            notifications,
            notification_actions,
            hyprland,
            logind: super::utilities::ProductionLogind,
            utilities,
            appearance,
        }
    }

    async fn execute_system(
        &self,
        _generation: u64,
        command: &DesktopSystemCommand,
    ) -> Result<Vec<DesktopDomainState>, ProducerError> {
        if let DesktopSystemCommand::Domain(DesktopSystemMutation::Display(
            DisplayCommand::SetBrightness { output_id, .. },
        )) = command
        {
            self.validate_output(output_id.as_str()).await?;
        }
        let runner = self.system.runner();
        let command = command.clone();
        let state = tokio::task::spawn_blocking(move || execute_system_command(&runner, command))
            .await
            .map_err(|error| ProducerError::new(format!("system mutation worker failed: {error}")))?
            .map_err(|error| ProducerError::new(error.to_string()))?;
        Ok(vec![state])
    }

    async fn validate_output(&self, output_id: &str) -> Result<(), ProducerError> {
        let adapter = self
            .hyprland
            .as_ref()
            .ok_or_else(|| ProducerError::new("Hyprland output authority is unavailable"))?;
        let snapshot = adapter
            .snapshot()
            .await
            .map_err(|error| ProducerError::new(error.to_string()))?;
        if snapshot
            .monitors
            .iter()
            .any(|monitor| monitor.id == output_id)
        {
            Ok(())
        } else {
            Err(ProducerError::new(
                "utility command referenced an unknown output",
            ))
        }
    }

    async fn execute_hyprland(
        &self,
        command: &HyprlandCommand,
    ) -> Result<Vec<DesktopDomainState>, ProducerError> {
        let adapter = self
            .hyprland
            .as_ref()
            .ok_or_else(|| ProducerError::new("Hyprland action service is unavailable"))?;
        match adapter
            .execute(command.clone())
            .await
            .map_err(|error| ProducerError::new(error.to_string()))?
        {
            CompositorExecution::Snapshot(snapshot) => Ok(vec![DesktopDomainState::available(
                DesktopDomainId::Hyprland,
                super::DesktopDomainValue::Hyprland(snapshot),
            )
            .expect("matching Hyprland domain")]),
            CompositorExecution::Exited => Ok(vec![DesktopDomainState::terminal(
                DesktopDomainId::Hyprland,
                sleepy_sdk::CapabilityAvailability::Unavailable,
                "Hyprland exit was confirmed by both IPC sockets disappearing",
            )
            .expect("static diagnostic")]),
        }
    }

    async fn execute_notification(
        &self,
        command: &SdkNotificationCommand,
    ) -> Result<Vec<DesktopDomainState>, ProducerError> {
        let mapped = match command {
            SdkNotificationCommand::SetDnd { enabled } => {
                NotificationCommand::SetDnd { enabled: *enabled }
            }
            SdkNotificationCommand::Archive { notification_id } => NotificationCommand::Archive {
                id: *notification_id,
            },
            SdkNotificationCommand::InvokeAction {
                notification_id,
                action_id,
            } => {
                let dispatcher =
                    self.notification_actions
                        .lock()
                        .await
                        .clone()
                        .ok_or_else(|| {
                            ProducerError::new("notification action dispatcher is unavailable")
                        })?;
                dispatcher
                    .invoke(*notification_id, action_id.as_str())
                    .await
                    .map_err(|error| ProducerError::new(error.to_string()))?;
                let service = self.notifications.lock().await;
                let store = service.provider().store();
                return Ok(vec![DesktopDomainState::available(
                    DesktopDomainId::Notifications,
                    super::DesktopDomainValue::Notifications(
                        sleepy_sdk::DesktopNotificationSnapshot {
                            availability: super::available_producer(),
                            dnd: store.dnd(),
                            active: store.active().to_vec(),
                        },
                    ),
                )
                .expect("matching notification domain")]);
            }
        };
        let mut service = self.notifications.lock().await;
        service
            .execute(mapped)
            .await
            .map_err(|error| ProducerError::new(error.to_string()))?;
        let store = service.provider().store();
        Ok(vec![DesktopDomainState::available(
            DesktopDomainId::Notifications,
            super::DesktopDomainValue::Notifications(sleepy_sdk::DesktopNotificationSnapshot {
                availability: super::available_producer(),
                dnd: store.dnd(),
                active: store.active().to_vec(),
            }),
        )
        .expect("matching notification domain")])
    }

    async fn execute_launcher(
        &self,
        request: &sleepy_sdk::DesktopLaunchRequest,
    ) -> Result<Vec<DesktopDomainState>, ProducerError> {
        let daily = Arc::clone(&self.daily);
        let request = request.clone();
        tokio::task::spawn_blocking(move || {
            daily.handle_controlled(
                DailyOperation::Launch { request },
                &RunControl::for_timeout(std::time::Duration::from_secs(4)),
            )
        })
        .await
        .map_err(|error| ProducerError::new(format!("launcher worker failed: {error}")))?
        .map_err(|error| ProducerError::new(error.to_string()))?;
        let producer = DailyProducer::new(DesktopDomainId::Launcher, Arc::clone(&self.daily))
            .map_err(|error| ProducerError::new(error.to_string()))?;
        Ok(vec![producer.initial().await])
    }
}

#[async_trait]
impl<B: DailyBackend> DesktopMutationExecutor for ProductionDesktopMutationExecutor<B> {
    async fn execute(
        &self,
        request: &DesktopRequest,
    ) -> Result<Vec<DesktopDomainState>, ProducerError> {
        match &request.command {
            DesktopCommand::System(command) => {
                self.execute_system(request.expected_generation, command)
                    .await
            }
            DesktopCommand::Compositor(command) => self.execute_hyprland(command).await,
            DesktopCommand::Notification(command) => self.execute_notification(command).await,
            DesktopCommand::Launcher(sleepy_sdk::LauncherCommand::Launch(request)) => {
                self.execute_launcher(request).await
            }
            DesktopCommand::Session(command) => {
                let logind = self.logind;
                let command = *command;
                let state = tokio::task::spawn_blocking(move || logind.execute(command))
                    .await
                    .map_err(|error| {
                        ProducerError::new(format!("logind action worker failed: {error}"))
                    })?
                    .map_err(|error| ProducerError::new(error.to_string()))?;
                Ok(vec![state])
            }
            DesktopCommand::Appearance(command) => {
                let snapshot = self
                    .appearance
                    .apply(command)
                    .await
                    .map_err(|error| ProducerError::new(error.to_string()))?;
                Ok(vec![DesktopDomainState::available(
                    DesktopDomainId::Appearance,
                    super::DesktopDomainValue::Appearance {
                        theme: snapshot.theme,
                        wallpaper_id: snapshot.wallpaper_id,
                    },
                )
                .expect("matching appearance domain")])
            }
            DesktopCommand::Utility(command) => {
                if let sleepy_sdk::UtilityCommand::StartRecording { output_id }
                | sleepy_sdk::UtilityCommand::Screenshot { output_id } = command
                {
                    self.validate_output(output_id.as_str()).await?;
                }
                let service = Arc::clone(&self.utilities);
                let command = command.clone();
                let state = tokio::task::spawn_blocking(move || service.execute(&command))
                    .await
                    .map_err(|error| {
                        ProducerError::new(format!("utility action worker failed: {error}"))
                    })?
                    .map_err(|error| ProducerError::new(error.to_string()))?;
                // One-shot utilities (screenshot/color pick) have no result payload in the
                // v3 SDK. Their bounded process exit is the confirmation; the returned state
                // is still published even when an independent utility in that domain is absent.
                Ok(vec![state])
            }
        }
    }
}

fn execute_system_command(
    runner: &ProcessCommandRunner,
    command: DesktopSystemCommand,
) -> std::io::Result<DesktopDomainState> {
    let runner = super::core::DeadlineRunner::new(*runner, std::time::Duration::from_secs(10));
    let (domain, value) = match command {
        DesktopSystemCommand::Domain(mutation) => match mutation {
            DesktopSystemMutation::Network(command) => (
                DesktopDomainId::Network,
                DesktopDomainValue::Network(super::network::mutate(&runner, &command)?),
            ),
            DesktopSystemMutation::Bluetooth(command) => (
                DesktopDomainId::Bluetooth,
                DesktopDomainValue::Bluetooth(super::bluetooth::mutate(&runner, &command)?),
            ),
            DesktopSystemMutation::Audio(command) => (
                DesktopDomainId::Audio,
                DesktopDomainValue::Audio(super::audio::mutate(&runner, &command)?),
            ),
            DesktopSystemMutation::Media(command) => (
                DesktopDomainId::Media,
                DesktopDomainValue::Media(super::media::mutate(&runner, &command)?),
            ),
            DesktopSystemMutation::Display(command) => (
                DesktopDomainId::Display,
                DesktopDomainValue::Display(super::display::mutate(&runner, &command)?),
            ),
            DesktopSystemMutation::Power(PowerCommand::SetProfile { profile }) => (
                DesktopDomainId::Power,
                DesktopDomainValue::Power(super::power::mutate(&runner, profile)?),
            ),
        },
        DesktopSystemCommand::Legacy(mutation) => match mutation {
            SystemMutation::NetworkEnabled(enabled) => (
                DesktopDomainId::Network,
                DesktopDomainValue::Network(super::network::mutate(
                    &runner,
                    &NetworkCommand::SetWifiEnabled { enabled },
                )?),
            ),
            SystemMutation::BluetoothEnabled(powered) => (
                DesktopDomainId::Bluetooth,
                DesktopDomainValue::Bluetooth(super::bluetooth::mutate(
                    &runner,
                    &BluetoothCommand::SetPowered { powered },
                )?),
            ),
            mutation @ (SystemMutation::AudioVolume(_)
            | SystemMutation::AudioMuted(_)
            | SystemMutation::AudioMicrophoneLevel(_)
            | SystemMutation::AudioMicrophoneMuted(_)
            | SystemMutation::AudioOutputDevice(_)) => (
                DesktopDomainId::Audio,
                DesktopDomainValue::Audio(super::audio::mutate_legacy(&runner, &mutation)?),
            ),
            SystemMutation::DisplayBrightness(level) => (
                DesktopDomainId::Display,
                DesktopDomainValue::Display(super::display::mutate(
                    &runner,
                    &DisplayCommand::SetBrightness {
                        output_id: StableId("legacy-global".to_owned()),
                        level,
                    },
                )?),
            ),
            SystemMutation::DisplayNightLightEnabled(enabled) => (
                DesktopDomainId::Display,
                DesktopDomainValue::Display(super::display::mutate(
                    &runner,
                    &DisplayCommand::SetNightLightEnabled { enabled },
                )?),
            ),
            SystemMutation::PowerProfile(profile) => (
                DesktopDomainId::Power,
                DesktopDomainValue::Power(super::power::mutate(&runner, profile)?),
            ),
            SystemMutation::MediaTransport(transport) => (
                DesktopDomainId::Media,
                DesktopDomainValue::Media(super::media::mutate_legacy(&runner, transport)?),
            ),
        },
    };
    DesktopDomainState::available(domain, value)
}
