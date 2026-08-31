// SPDX-License-Identifier: GPL-3.0-only

use std::{io, sync::Mutex, time::Duration};

use async_trait::async_trait;
use sleepy_sdk::{CapabilityAvailability, ResourceSample};
use tokio::sync::mpsc;

use super::{
    DesktopDomainId, DesktopDomainState, DesktopDomainUpdate, DesktopDomainValue, DesktopProducer,
    DesktopProducerContext, ProducerError,
};

const REFRESH_INTERVAL: Duration = Duration::from_secs(2);

#[derive(Debug, Clone, Copy)]
struct CpuCounters {
    idle: u64,
    total: u64,
}

pub struct ResourceProducer {
    previous: Mutex<Option<CpuCounters>>,
}

impl Default for ResourceProducer {
    fn default() -> Self {
        Self {
            previous: Mutex::new(None),
        }
    }
}

impl ResourceProducer {
    async fn probe(&self, context: Option<&DesktopProducerContext>) -> DesktopDomainState {
        let reading = match context {
            Some(context) => {
                context
                    .spawn_blocking(
                        std::time::Instant::now() + Duration::from_secs(2),
                        |control| {
                            if control.is_cancelled() {
                                return Err(io::Error::new(
                                    io::ErrorKind::Interrupted,
                                    "resource polling was cancelled",
                                ));
                            }
                            let reading = read_host_resources()?;
                            if control.is_cancelled() {
                                return Err(io::Error::new(
                                    io::ErrorKind::Interrupted,
                                    "resource polling was cancelled",
                                ));
                            }
                            Ok(reading)
                        },
                    )
                    .await
            }
            None => tokio::task::spawn_blocking(read_host_resources).await,
        };
        match reading {
            Ok(Ok((current, memory_usage, load_one))) => {
                let cpu_usage = {
                    let mut previous = self.previous.lock().unwrap();
                    let usage = previous.map_or(0.0, |previous| cpu_usage(previous, current));
                    *previous = Some(current);
                    usage
                };
                DesktopDomainState::available(
                    DesktopDomainId::Resources,
                    DesktopDomainValue::Resources(vec![ResourceSample {
                        id: "host".into(),
                        cpu_usage,
                        memory_usage,
                        load_one,
                    }]),
                )
                .expect("matching resource domain")
            }
            Ok(Err(error)) => DesktopDomainState::terminal(
                DesktopDomainId::Resources,
                availability_for_io(&error),
                format!("resource probe failed: {error}"),
            )
            .expect("nonempty resource diagnostic"),
            Err(_) => DesktopDomainState::terminal(
                DesktopDomainId::Resources,
                CapabilityAvailability::Error,
                "resource probe worker failed",
            )
            .expect("static diagnostic"),
        }
    }
}

#[async_trait]
impl DesktopProducer for ResourceProducer {
    fn domain(&self) -> DesktopDomainId {
        DesktopDomainId::Resources
    }

    async fn initial(&self) -> DesktopDomainState {
        self.probe(None).await
    }

    async fn run(
        &self,
        sender: mpsc::Sender<DesktopDomainUpdate>,
        context: DesktopProducerContext,
    ) -> Result<(), ProducerError> {
        let mut interval = tokio::time::interval(REFRESH_INTERVAL);
        interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        interval.tick().await;
        loop {
            tokio::select! {
                biased;
                _ = context.cancelled() => return Ok(()),
                _ = interval.tick() => {
                    sender.send(DesktopDomainUpdate { state: self.probe(Some(&context)).await })
                        .await
                        .map_err(|_| ProducerError::new("desktop state authority stopped"))?;
                }
            }
        }
    }
}

fn read_host_resources() -> io::Result<(CpuCounters, f64, f64)> {
    let stat = std::fs::read_to_string("/proc/stat")?;
    let meminfo = std::fs::read_to_string("/proc/meminfo")?;
    let loadavg = std::fs::read_to_string("/proc/loadavg")?;
    parse_resources(&stat, &meminfo, &loadavg)
}

pub fn parse_host_resources(
    stat: &str,
    meminfo: &str,
    loadavg: &str,
) -> io::Result<(u64, u64, f64, f64)> {
    let (cpu, memory, load) = parse_resources(stat, meminfo, loadavg)?;
    Ok((cpu.idle, cpu.total, memory, load))
}

fn parse_resources(
    stat: &str,
    meminfo: &str,
    loadavg: &str,
) -> io::Result<(CpuCounters, f64, f64)> {
    let cpu_line = stat
        .lines()
        .find(|line| line.starts_with("cpu "))
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "missing aggregate CPU line"))?;
    let counters = cpu_line
        .split_ascii_whitespace()
        .skip(1)
        .map(|value| {
            value.parse::<u64>().map_err(|_| {
                io::Error::new(io::ErrorKind::InvalidData, "invalid aggregate CPU counter")
            })
        })
        .collect::<Result<Vec<_>, _>>()?;
    if counters.len() < 4 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "aggregate CPU line has too few counters",
        ));
    }
    let total = counters.iter().try_fold(0_u64, |total, value| {
        total
            .checked_add(*value)
            .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "CPU counters overflow"))
    })?;
    let idle = counters[3]
        .checked_add(*counters.get(4).unwrap_or(&0))
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "CPU idle counters overflow"))?;
    let memory_total = meminfo_value(meminfo, "MemTotal:")?;
    let memory_available = meminfo_value(meminfo, "MemAvailable:")?;
    if memory_total == 0 || memory_available > memory_total {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "memory counters are inconsistent",
        ));
    }
    let memory_usage = 1.0 - memory_available as f64 / memory_total as f64;
    let load_one = loadavg
        .split_ascii_whitespace()
        .next()
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "load average is empty"))?
        .parse::<f64>()
        .map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "load average is invalid"))?;
    if !load_one.is_finite() || load_one < 0.0 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "load average is outside its valid range",
        ));
    }
    Ok((CpuCounters { idle, total }, memory_usage, load_one))
}

fn meminfo_value(input: &str, key: &str) -> io::Result<u64> {
    input
        .lines()
        .find_map(|line| {
            let mut fields = line.split_ascii_whitespace();
            (fields.next()? == key).then(|| fields.next())?
        })
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "memory counter is missing"))?
        .parse::<u64>()
        .map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "memory counter is invalid"))
}

fn cpu_usage(previous: CpuCounters, current: CpuCounters) -> f64 {
    let total = current.total.saturating_sub(previous.total);
    let idle = current.idle.saturating_sub(previous.idle);
    if total == 0 || idle > total {
        0.0
    } else {
        (1.0 - idle as f64 / total as f64).clamp(0.0, 1.0)
    }
}

fn availability_for_io(error: &io::Error) -> CapabilityAvailability {
    match error.kind() {
        io::ErrorKind::NotFound | io::ErrorKind::PermissionDenied => {
            CapabilityAvailability::Unavailable
        }
        io::ErrorKind::InvalidData => CapabilityAvailability::Parse,
        io::ErrorKind::TimedOut => CapabilityAvailability::Timeout,
        _ => CapabilityAvailability::Error,
    }
}
