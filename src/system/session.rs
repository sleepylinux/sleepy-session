use std::collections::BTreeMap;

use sleepy_sdk::{CapabilityState, SessionAction};

use super::{CommandRunner, CommandSpec, RunnerErrorKind};

pub(crate) fn probe<R: CommandRunner>(runner: &R) -> BTreeMap<SessionAction, CapabilityState> {
    let lock = availability(runner, CommandSpec::new("swaylock", ["--version"]));
    let logout = availability(runner, CommandSpec::new("niri", ["--version"]));
    let system = availability(runner, CommandSpec::new("systemctl", ["--version"]));
    BTreeMap::from([
        (SessionAction::Lock, lock),
        (SessionAction::Logout, logout),
        (SessionAction::Reboot, system),
        (SessionAction::PowerOff, system),
    ])
}

fn availability<R: CommandRunner>(runner: &R, command: CommandSpec) -> CapabilityState {
    match runner.run(&command) {
        Ok(output) if output.status == 0 => CapabilityState::Available,
        Ok(output) if output.status == 75 => CapabilityState::Busy,
        Ok(_) => CapabilityState::Error,
        Err(error) if error.kind() == RunnerErrorKind::Spawn => CapabilityState::Unavailable,
        Err(error)
            if matches!(
                error.kind(),
                RunnerErrorKind::Timeout | RunnerErrorKind::Cancelled
            ) =>
        {
            CapabilityState::Busy
        }
        Err(_) => CapabilityState::Error,
    }
}

pub(crate) fn command(action: SessionAction) -> CommandSpec {
    match action {
        SessionAction::Lock => CommandSpec::new("swaylock", ["--daemonize"]),
        SessionAction::Logout => {
            CommandSpec::new("niri", ["msg", "action", "quit", "--skip-confirmation"])
        }
        SessionAction::Reboot => CommandSpec::new("systemctl", ["reboot"]),
        SessionAction::PowerOff => CommandSpec::new("systemctl", ["poweroff"]),
    }
}
