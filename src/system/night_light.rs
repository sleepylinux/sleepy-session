use super::{CommandRunner, CommandSpec, ProbeFailure, RunnerErrorKind};

pub(crate) fn probe<R: CommandRunner>(runner: &R) -> Result<bool, ProbeFailure> {
    let command = CommandSpec::new("systemctl", ["--user", "is-active", "gammastep.service"]);
    let output = match runner.run(&command) {
        Ok(output) if output.status == 0 || output.status == 3 => output.stdout,
        Ok(output) => {
            return Err(ProbeFailure {
                kind: sleepy_sdk::CapabilityErrorKind::Command,
                message: format!("systemctl exited with status {}", output.status),
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
    match std::str::from_utf8(&output)
        .map_err(|_| ProbeFailure::parse("systemctl output is not UTF-8"))?
        .trim()
    {
        "active" => Ok(true),
        "inactive" | "failed" => Ok(false),
        _ => Err(ProbeFailure::parse(
            "systemctl returned an unknown service state",
        )),
    }
}
