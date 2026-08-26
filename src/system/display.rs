use sleepy_sdk::DisplayState;

use super::{CommandRunner, CommandSpec, ProbeFailure, RunnerErrorKind};

pub(crate) fn probe_brightness<R: CommandRunner>(runner: &R) -> Result<f64, ProbeFailure> {
    let command = CommandSpec::new("brightnessctl", ["--machine-readable", "info"]);
    let output = match runner.run(&command) {
        Ok(output) if output.status == 0 => output.stdout,
        Ok(output) if output.status == 2 => {
            return Err(ProbeFailure::unsupported(
                "no backlight device is available",
            ))
        }
        Ok(output) => {
            return Err(ProbeFailure {
                kind: sleepy_sdk::CapabilityErrorKind::Command,
                message: format!("brightnessctl exited with status {}", output.status),
            })
        }
        Err(error) => {
            return Err(ProbeFailure {
                kind: match error.kind() {
                    RunnerErrorKind::Timeout | RunnerErrorKind::Cancelled => {
                        sleepy_sdk::CapabilityErrorKind::Timeout
                    }
                    RunnerErrorKind::Spawn => sleepy_sdk::CapabilityErrorKind::Unsupported,
                    RunnerErrorKind::Io => sleepy_sdk::CapabilityErrorKind::Command,
                },
                message: error.message().to_owned(),
            })
        }
    };
    let text = std::str::from_utf8(&output)
        .map_err(|_| ProbeFailure::parse("brightnessctl output is not UTF-8"))?;
    let row = text
        .lines()
        .next()
        .ok_or_else(|| ProbeFailure::parse("brightnessctl output is empty"))?;
    let fields: Vec<_> = row.split(',').collect();
    if fields.len() < 5 {
        return Err(ProbeFailure::parse(
            "brightnessctl machine row is malformed",
        ));
    }
    let percent = fields[3]
        .strip_suffix('%')
        .ok_or_else(|| ProbeFailure::parse("brightnessctl percentage is malformed"))?
        .parse::<f64>()
        .map_err(|_| ProbeFailure::parse("brightnessctl percentage is not numeric"))?;
    if !(0.0..=100.0).contains(&percent) {
        return Err(ProbeFailure::parse(
            "brightnessctl percentage is outside 0..100",
        ));
    }
    Ok(percent / 100.0)
}

pub(crate) fn state(brightness: Option<f64>, night_light_enabled: bool) -> DisplayState {
    DisplayState {
        brightness,
        night_light_enabled,
    }
}
