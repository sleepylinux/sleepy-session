// SPDX-License-Identifier: GPL-3.0-only

use std::{
    io,
    time::{Duration, Instant},
};

use sleepy_sdk::DaemonCommand;

use crate::system::{CommandRunner, CommandSpec, ProcessCommandRunner};

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum OverviewEvent {
    FocusChanged {
        output_id: String,
        window_id: Option<u64>,
        workspace_id: u64,
        sequence: u64,
    },
    WindowClosed {
        window_id: u64,
        sequence: u64,
    },
    Offline {
        sequence: u64,
    },
}

impl OverviewEvent {
    pub fn sequence(&self) -> u64 {
        match self {
            Self::FocusChanged { sequence, .. }
            | Self::WindowClosed { sequence, .. }
            | Self::Offline { sequence } => *sequence,
        }
    }
}

pub trait OverviewRunner: Clone + Send + Sync + 'static {
    fn run(&self, program: &str, args: &[String], timeout: Duration) -> io::Result<()>;
}

#[derive(Clone, Default)]
pub struct ProcessOverviewRunner<R = ProcessCommandRunner>(pub R);

impl<R: CommandRunner> OverviewRunner for ProcessOverviewRunner<R> {
    fn run(&self, program: &str, args: &[String], timeout: Duration) -> io::Result<()> {
        if program != "niri" {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "overview runner only permits niri",
            ));
        }
        let mut command = CommandSpec::new(program, args.iter().cloned());
        command.timeout = timeout;
        let output = self.0.run(&command).map_err(io::Error::other)?;
        if output.status == 0 {
            Ok(())
        } else {
            Err(io::Error::other(format!(
                "Niri command exited with status {}",
                output.status
            )))
        }
    }
}

pub trait OverviewEventSource: Send + Sync + 'static {
    fn next_event(&self, timeout: Duration) -> io::Result<Option<OverviewEvent>>;
}

pub struct NiriOverview<R, E> {
    runner: R,
    events: E,
    timeout: Duration,
    sequence: u64,
    focused_output: Option<String>,
    online: bool,
}

impl<R: OverviewRunner, E: OverviewEventSource> NiriOverview<R, E> {
    pub fn new(runner: R, events: E, timeout: Duration) -> Self {
        Self {
            runner,
            events,
            timeout,
            sequence: 0,
            focused_output: None,
            online: true,
        }
    }

    pub fn observe(&mut self, event: OverviewEvent) {
        if event.sequence() <= self.sequence {
            return;
        }
        self.sequence = event.sequence();
        match event {
            OverviewEvent::FocusChanged { output_id, .. } => {
                self.online = true;
                self.focused_output = Some(output_id);
            }
            OverviewEvent::Offline { .. } => self.online = false,
            OverviewEvent::WindowClosed { .. } => self.online = true,
        }
    }

    pub fn execute(&mut self, command: DaemonCommand) -> io::Result<OverviewEvent> {
        if !self.online {
            return Err(io::Error::new(
                io::ErrorKind::NotConnected,
                "Niri event stream is offline",
            ));
        }
        let args = fixed_args(&command)?;
        let baseline = self.sequence;
        let routed_output = self.focused_output.clone();
        let deadline = Instant::now() + self.timeout;
        self.runner.run("niri", &args, self.timeout)?;
        loop {
            let remaining = deadline.saturating_duration_since(Instant::now());
            if remaining.is_zero() {
                return Err(io::Error::new(
                    io::ErrorKind::TimedOut,
                    "Niri did not confirm the command",
                ));
            }
            let event = self.events.next_event(remaining)?.ok_or_else(|| {
                io::Error::new(io::ErrorKind::TimedOut, "Niri did not confirm the command")
            })?;
            if event.sequence() <= baseline {
                continue;
            }
            if matches!(event, OverviewEvent::Offline { .. }) {
                self.observe(event);
                return Err(io::Error::new(
                    io::ErrorKind::NotConnected,
                    "Niri went offline before confirmation",
                ));
            }
            if confirms(&command, &event, routed_output.as_deref()) {
                self.observe(event.clone());
                return Ok(event);
            }
            self.observe(event);
        }
    }
}

fn fixed_args(command: &DaemonCommand) -> io::Result<Vec<String>> {
    let mut args = vec!["msg".into(), "action".into()];
    match command {
        DaemonCommand::FocusWindow { window_id } if *window_id > 0 => {
            args.extend(["focus-window".into(), "--id".into(), window_id.to_string()]);
        }
        DaemonCommand::CloseWindow { window_id } if *window_id > 0 => {
            args.extend(["close-window".into(), "--id".into(), window_id.to_string()]);
        }
        DaemonCommand::FocusWorkspace { workspace_id } if *workspace_id > 0 => {
            args.extend(["focus-workspace".into(), workspace_id.to_string()]);
        }
        _ => {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "command is not a typed Niri overview action",
            ))
        }
    }
    Ok(args)
}

fn confirms(command: &DaemonCommand, event: &OverviewEvent, expected_output: Option<&str>) -> bool {
    match (command, event) {
        (
            DaemonCommand::FocusWindow { window_id },
            OverviewEvent::FocusChanged {
                output_id,
                window_id: actual,
                ..
            },
        ) => {
            actual == &Some(*window_id)
                && expected_output.is_none_or(|expected| expected == output_id)
        }
        (
            DaemonCommand::CloseWindow { window_id },
            OverviewEvent::WindowClosed {
                window_id: actual, ..
            },
        ) => window_id == actual,
        (
            DaemonCommand::FocusWorkspace { workspace_id },
            OverviewEvent::FocusChanged {
                output_id,
                workspace_id: actual,
                ..
            },
        ) => workspace_id == actual && expected_output.is_none_or(|expected| expected == output_id),
        _ => false,
    }
}
