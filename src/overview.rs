// SPDX-License-Identifier: GPL-3.0-only

use std::{
    collections::VecDeque,
    io,
    sync::{Arc, Condvar, Mutex},
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
    fn run(&self, program: &str, args: &[String], timeout: Duration) -> io::Result<Instant>;

    fn run_controlled(
        &self,
        program: &str,
        args: &[String],
        timeout: Duration,
        _control: &RunControl,
    ) -> io::Result<Instant> {
        self.run(program, args, timeout)
    }
}

#[derive(Clone, Default)]
pub struct ProcessOverviewRunner<R = ProcessCommandRunner>(pub R);

impl<R: CommandRunner> OverviewRunner for ProcessOverviewRunner<R> {
    fn run(&self, program: &str, args: &[String], timeout: Duration) -> io::Result<Instant> {
        if program != "niri" {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "overview runner only permits niri",
            ));
        }
        let mut command = CommandSpec::new(program, args.iter().cloned());
        command.timeout = timeout;
        let started_at = Instant::now();
        let output = self.0.run(&command).map_err(io::Error::other)?;
        if output.status == 0 {
            Ok(started_at)
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
    ) -> io::Result<Instant> {
        if program != "niri" {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "overview runner only permits niri",
            ));
        }
        let mut command = CommandSpec::new(program, args.iter().cloned());
        command.timeout = timeout;
        let (output, started_at) = self
            .0
            .run_controlled_started(&command, control)
            .map_err(io::Error::other)?;
        if output.status == 0 {
            Ok(started_at)
        } else {
            Err(io::Error::other(format!(
                "Niri command exited with status {}",
                output.status
            )))
        }
    }
}

#[derive(Debug, Clone)]
pub struct ObservedOverviewEvent {
    event: OverviewEvent,
    observed_at: Instant,
}

impl ObservedOverviewEvent {
    pub fn now(event: OverviewEvent) -> Self {
        Self {
            event,
            observed_at: Instant::now(),
        }
    }

    fn sequence(&self) -> u64 {
        self.event.sequence()
    }
}

pub trait OverviewEventSource: Send + Sync + 'static {
    fn next_event(&self, timeout: Duration) -> io::Result<Option<ObservedOverviewEvent>>;

    fn try_event(&self) -> io::Result<Option<ObservedOverviewEvent>> {
        Ok(None)
    }

    fn prepare_request(
        &self,
        current_sequence: u64,
    ) -> io::Result<(u64, Vec<ObservedOverviewEvent>)> {
        Ok((current_sequence, Vec::new()))
    }
}

#[derive(Clone)]
pub struct OverviewEventSender(Arc<OverviewChannel>);

pub struct ChannelOverviewEvents {
    channel: Arc<OverviewChannel>,
    cursor: Mutex<u64>,
}

struct OverviewChannel {
    state: Mutex<OverviewChannelState>,
    changed: Condvar,
}

struct OverviewChannelState {
    events: VecDeque<ObservedOverviewEvent>,
    capacity: usize,
    latest_sequence: u64,
    dropped_through: u64,
}

pub fn overview_event_channel(capacity: usize) -> (OverviewEventSender, ChannelOverviewEvents) {
    let channel = Arc::new(OverviewChannel {
        state: Mutex::new(OverviewChannelState {
            events: VecDeque::new(),
            capacity: capacity.max(1),
            latest_sequence: 0,
            dropped_through: 0,
        }),
        changed: Condvar::new(),
    });
    (
        OverviewEventSender(Arc::clone(&channel)),
        ChannelOverviewEvents {
            channel,
            cursor: Mutex::new(0),
        },
    )
}

impl OverviewEventSender {
    pub fn publish(&self, event: OverviewEvent) -> io::Result<()> {
        let observed = ObservedOverviewEvent::now(event);
        let mut state = self
            .0
            .state
            .lock()
            .map_err(|_| io::Error::other("overview event channel lock poisoned"))?;
        if observed.sequence() <= state.latest_sequence {
            return Ok(());
        }
        if state.events.len() == state.capacity {
            if let Some(dropped) = state.events.pop_front() {
                state.dropped_through = dropped.sequence();
            }
        }
        state.latest_sequence = observed.sequence();
        state.events.push_back(observed);
        self.0.changed.notify_all();
        Ok(())
    }
}

impl OverviewEventSource for ChannelOverviewEvents {
    fn next_event(&self, timeout: Duration) -> io::Result<Option<ObservedOverviewEvent>> {
        let mut cursor = self
            .cursor
            .lock()
            .map_err(|_| io::Error::other("overview cursor lock poisoned"))?;
        let state = self
            .channel
            .state
            .lock()
            .map_err(|_| io::Error::other("overview event channel lock poisoned"))?;
        let (state, _) = self
            .channel
            .changed
            .wait_timeout_while(state, timeout, |state| {
                state.latest_sequence <= *cursor && state.dropped_through <= *cursor
            })
            .map_err(|_| io::Error::other("overview event channel lock poisoned"))?;
        next_channel_event(&state, &mut cursor)
    }

    fn try_event(&self) -> io::Result<Option<ObservedOverviewEvent>> {
        let mut cursor = self
            .cursor
            .lock()
            .map_err(|_| io::Error::other("overview cursor lock poisoned"))?;
        let state = self
            .channel
            .state
            .lock()
            .map_err(|_| io::Error::other("overview event channel lock poisoned"))?;
        next_channel_event(&state, &mut cursor)
    }

    fn prepare_request(
        &self,
        current_sequence: u64,
    ) -> io::Result<(u64, Vec<ObservedOverviewEvent>)> {
        let mut cursor = self
            .cursor
            .lock()
            .map_err(|_| io::Error::other("overview cursor lock poisoned"))?;
        let state = self
            .channel
            .state
            .lock()
            .map_err(|_| io::Error::other("overview event channel lock poisoned"))?;
        if *cursor < state.dropped_through {
            *cursor = state.dropped_through;
            return Err(lagged_error());
        }
        let retained = state
            .events
            .iter()
            .filter(|event| event.sequence() > *cursor)
            .cloned()
            .collect();
        *cursor = state.latest_sequence;
        Ok((current_sequence.max(state.latest_sequence), retained))
    }
}

fn next_channel_event(
    state: &OverviewChannelState,
    cursor: &mut u64,
) -> io::Result<Option<ObservedOverviewEvent>> {
    if *cursor < state.dropped_through {
        *cursor = state.dropped_through;
        return Err(lagged_error());
    }
    let event = state
        .events
        .iter()
        .find(|event| event.sequence() > *cursor)
        .cloned();
    if let Some(event) = &event {
        *cursor = event.sequence();
    }
    Ok(event)
}

fn lagged_error() -> io::Error {
    io::Error::new(
        io::ErrorKind::InvalidData,
        "Niri overview event subscription lagged",
    )
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
        let (baseline, retained) = self.events.prepare_request(self.sequence)?;
        for observed in retained {
            self.observe(observed.event);
        }
        if !self.online {
            return Err(io::Error::new(
                io::ErrorKind::NotConnected,
                "Niri event stream is offline",
            ));
        }
        let args = fixed_args(&command)?;
        let baseline = baseline.max(self.sequence);
        let routed_output = self.focused_output.clone();
        let deadline = Instant::now() + self.timeout;
        let command_started_at =
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
            let observed = self.events.next_event(remaining)?.ok_or_else(|| {
                io::Error::new(io::ErrorKind::TimedOut, "Niri did not confirm the command")
            })?;
            let event = observed.event;
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
            if observed.observed_at >= command_started_at
                && confirms(&command, &event, routed_output.as_deref())
            {
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
