// SPDX-License-Identifier: GPL-3.0-only

use std::{
    io,
    sync::{mpsc, Mutex},
    time::{Duration, Instant},
};

use sleepy_sdk::DaemonCommand;

use crate::system::{CommandRunner, CommandSpec, ProcessCommandRunner, RunControl};

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

    fn run_controlled(
        &self,
        program: &str,
        args: &[String],
        timeout: Duration,
        _control: &RunControl,
    ) -> io::Result<()> {
        self.run(program, args, timeout)
    }
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

    fn run_controlled(
        &self,
        program: &str,
        args: &[String],
        timeout: Duration,
        control: &RunControl,
    ) -> io::Result<()> {
        if program != "niri" {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "overview runner only permits niri",
            ));
        }
        let mut command = CommandSpec::new(program, args.iter().cloned());
        command.timeout = timeout;
        let output = self
            .0
            .run_controlled(&command, control)
            .map_err(io::Error::other)?;
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

    fn try_event(&self) -> io::Result<Option<OverviewEvent>> {
        Ok(None)
    }
}

#[derive(Clone)]
pub struct OverviewEventSender(mpsc::SyncSender<OverviewEvent>);

pub struct ChannelOverviewEvents(Mutex<mpsc::Receiver<OverviewEvent>>);

pub fn overview_event_channel(capacity: usize) -> (OverviewEventSender, ChannelOverviewEvents) {
    let (sender, receiver) = mpsc::sync_channel(capacity.max(1));
    (
        OverviewEventSender(sender),
        ChannelOverviewEvents(Mutex::new(receiver)),
    )
}

impl OverviewEventSender {
    pub fn publish(&self, event: OverviewEvent) -> io::Result<()> {
        match self.0.try_send(event) {
            Ok(()) | Err(mpsc::TrySendError::Full(_)) => Ok(()),
            Err(mpsc::TrySendError::Disconnected(_)) => Err(io::Error::new(
                io::ErrorKind::BrokenPipe,
                "overview event receiver stopped",
            )),
        }
    }
}

impl OverviewEventSource for ChannelOverviewEvents {
    fn next_event(&self, timeout: Duration) -> io::Result<Option<OverviewEvent>> {
        match self
            .0
            .lock()
            .map_err(|_| io::Error::other("overview event receiver lock poisoned"))?
            .recv_timeout(timeout)
        {
            Ok(event) => Ok(Some(event)),
            Err(mpsc::RecvTimeoutError::Timeout) => Ok(None),
            Err(mpsc::RecvTimeoutError::Disconnected) => Err(io::Error::new(
                io::ErrorKind::NotConnected,
                "Niri overview event source stopped",
            )),
        }
    }

    fn try_event(&self) -> io::Result<Option<OverviewEvent>> {
        match self
            .0
            .lock()
            .map_err(|_| io::Error::other("overview event receiver lock poisoned"))?
            .try_recv()
        {
            Ok(event) => Ok(Some(event)),
            Err(mpsc::TryRecvError::Empty) => Ok(None),
            Err(mpsc::TryRecvError::Disconnected) => Err(io::Error::new(
                io::ErrorKind::NotConnected,
                "Niri overview event source stopped",
            )),
        }
    }
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
        let control = RunControl::for_request(
            Instant::now() + self.timeout,
            std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false)),
        );
        self.execute_controlled(command, &control)
    }

    pub fn execute_controlled(
        &mut self,
        command: DaemonCommand,
        control: &RunControl,
    ) -> io::Result<OverviewEvent> {
        while let Some(event) = self.events.try_event()? {
            self.observe(event);
        }
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
        self.runner
            .run_controlled("niri", &args, self.timeout, control)?;
        loop {
            if control.is_cancelled() || control.remaining().is_zero() {
                return Err(io::Error::new(
                    io::ErrorKind::Interrupted,
                    "Niri confirmation cancelled",
                ));
            }
            let remaining = deadline.saturating_duration_since(Instant::now());
            let remaining = remaining.min(control.remaining());
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
        ) => actual == &Some(*window_id) && !output_id.is_empty(),
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
