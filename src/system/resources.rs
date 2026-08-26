use std::{fs, io, path::Path};

use sleepy_sdk::ResourceRuntimeState;

use super::ProbeFailure;

pub(crate) fn probe(root: &Path) -> Result<ResourceRuntimeState, ProbeFailure> {
    let stat = read(root.join("stat"))?;
    let meminfo = read(root.join("meminfo"))?;
    let loadavg = read(root.join("loadavg"))?;

    Ok(ResourceRuntimeState {
        cpu_usage: if root == Path::new("/proc") {
            let first = parse_cpu_times(&stat)?;
            std::thread::sleep(std::time::Duration::from_millis(100));
            let second = parse_cpu_times(&read(root.join("stat"))?)?;
            usage_between(first, second)?
        } else {
            lifetime_cpu_usage(&stat)?
        },
        memory_usage: parse_memory_usage(&meminfo)?,
        load_one: parse_load_one(&loadavg)?,
    })
}

fn read(path: impl AsRef<Path>) -> Result<String, ProbeFailure> {
    fs::read_to_string(path.as_ref()).map_err(|error| match error.kind() {
        io::ErrorKind::NotFound => ProbeFailure::unsupported("procfs resource data is unavailable"),
        io::ErrorKind::PermissionDenied => {
            ProbeFailure::permission_denied("procfs resource data is not readable")
        }
        _ => ProbeFailure::command(format!("could not read procfs resource data: {error}")),
    })
}

fn parse_cpu_times(stat: &str) -> Result<(u64, u64), ProbeFailure> {
    let line = stat
        .lines()
        .next()
        .ok_or_else(|| ProbeFailure::parse("/proc/stat is empty"))?;
    let mut fields = line.split_whitespace();
    if fields.next() != Some("cpu") {
        return Err(ProbeFailure::parse("/proc/stat omitted aggregate CPU data"));
    }
    let times = fields
        .map(|field| {
            field
                .parse::<u64>()
                .map_err(|_| ProbeFailure::parse("/proc/stat CPU time is not numeric"))
        })
        .collect::<Result<Vec<_>, _>>()?;
    if times.len() < 4 {
        return Err(ProbeFailure::parse("/proc/stat CPU data is incomplete"));
    }
    let total = times.iter().try_fold(0_u64, |total, value| {
        total
            .checked_add(*value)
            .ok_or_else(|| ProbeFailure::parse("/proc/stat CPU total overflowed"))
    })?;
    if total == 0 {
        return Err(ProbeFailure::parse("/proc/stat CPU total is zero"));
    }
    let idle = times[3].saturating_add(times.get(4).copied().unwrap_or(0));
    Ok((total, idle))
}

fn lifetime_cpu_usage(stat: &str) -> Result<f64, ProbeFailure> {
    let (total, idle) = parse_cpu_times(stat)?;
    Ok((total.saturating_sub(idle) as f64 / total as f64).clamp(0.0, 1.0))
}

fn usage_between(first: (u64, u64), second: (u64, u64)) -> Result<f64, ProbeFailure> {
    let total = second.0.saturating_sub(first.0);
    let idle = second.1.saturating_sub(first.1);
    if total == 0 || idle > total {
        return Err(ProbeFailure::parse(
            "/proc/stat CPU counters did not advance coherently",
        ));
    }
    Ok(((total - idle) as f64 / total as f64).clamp(0.0, 1.0))
}

fn parse_memory_usage(meminfo: &str) -> Result<f64, ProbeFailure> {
    let total = meminfo_value(meminfo, "MemTotal:")?;
    let available = meminfo_value(meminfo, "MemAvailable:")?;
    if total == 0 || available > total {
        return Err(ProbeFailure::parse(
            "/proc/meminfo memory values are incoherent",
        ));
    }
    Ok((total - available) as f64 / total as f64)
}

fn meminfo_value(meminfo: &str, key: &str) -> Result<u64, ProbeFailure> {
    let value = meminfo
        .lines()
        .find_map(|line| line.strip_prefix(key))
        .ok_or_else(|| ProbeFailure::parse(format!("/proc/meminfo omitted {key}")))?;
    let mut fields = value.split_whitespace();
    let number = fields
        .next()
        .ok_or_else(|| ProbeFailure::parse(format!("/proc/meminfo {key} is empty")))?
        .parse::<u64>()
        .map_err(|_| ProbeFailure::parse(format!("/proc/meminfo {key} is not numeric")))?;
    if !matches!(fields.next(), None | Some("kB")) || fields.next().is_some() {
        return Err(ProbeFailure::parse(format!(
            "/proc/meminfo {key} has an invalid unit"
        )));
    }
    Ok(number)
}

fn parse_load_one(loadavg: &str) -> Result<f64, ProbeFailure> {
    let value = loadavg
        .split_whitespace()
        .next()
        .ok_or_else(|| ProbeFailure::parse("/proc/loadavg is empty"))?
        .parse::<f64>()
        .map_err(|_| ProbeFailure::parse("/proc/loadavg one-minute load is not numeric"))?;
    if !value.is_finite() || value < 0.0 {
        return Err(ProbeFailure::parse(
            "/proc/loadavg one-minute load is invalid",
        ));
    }
    Ok(value)
}
