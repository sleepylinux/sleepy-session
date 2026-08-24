use sleepy_sdk::BluetoothState;

use super::{run_checked, CommandRunner, CommandSpec, ProbeFailure};

pub(crate) fn probe<R: CommandRunner>(runner: &R) -> Result<BluetoothState, ProbeFailure> {
    let show = run_checked(runner, CommandSpec::new("bluetoothctl", ["show"]))?;
    let show = std::str::from_utf8(&show)
        .map_err(|_| ProbeFailure::parse("bluetoothctl output is not UTF-8"))?;
    let powered = show
        .lines()
        .find_map(|line| line.trim().strip_prefix("Powered: "))
        .ok_or_else(|| ProbeFailure::parse("bluetoothctl omitted Powered"))?;
    let enabled = match powered {
        "yes" => true,
        "no" => false,
        _ => {
            return Err(ProbeFailure::parse(
                "bluetoothctl returned an invalid Powered value",
            ))
        }
    };
    if !enabled {
        return Ok(BluetoothState {
            enabled,
            connected_device: None,
        });
    }
    let devices = run_checked(
        runner,
        CommandSpec::new("bluetoothctl", ["devices", "Connected"]),
    )?;
    let devices = std::str::from_utf8(&devices)
        .map_err(|_| ProbeFailure::parse("bluetoothctl devices output is not UTF-8"))?;
    let connected_device = devices.lines().next().and_then(|line| {
        let mut fields = line.splitn(3, ' ');
        (fields.next() == Some("Device"))
            .then(|| fields.nth(1).map(str::to_owned))
            .flatten()
    });
    Ok(BluetoothState {
        enabled,
        connected_device,
    })
}
