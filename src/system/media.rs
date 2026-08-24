use sleepy_sdk::MediaState;

use super::{run_checked, CommandRunner, CommandSpec, ProbeFailure};

pub(crate) fn probe<R: CommandRunner>(runner: &R) -> Result<MediaState, ProbeFailure> {
    let output = run_checked(
        runner,
        CommandSpec::new(
            "playerctl",
            [
                "metadata",
                "--format",
                "{{status}}\\t{{title}}\\t{{artist}}",
            ],
        ),
    )?;
    let text = std::str::from_utf8(&output)
        .map_err(|_| ProbeFailure::parse("playerctl output is not UTF-8"))?
        .trim_end();
    let mut fields = text.splitn(3, '\t');
    let status = fields
        .next()
        .ok_or_else(|| ProbeFailure::parse("playerctl omitted status"))?;
    let title = fields
        .next()
        .ok_or_else(|| ProbeFailure::parse("playerctl omitted title"))?;
    let artist = fields.next().unwrap_or_default();
    if title.is_empty() {
        return Err(ProbeFailure::parse("playerctl title is empty"));
    }
    let playing = match status {
        "Playing" => true,
        "Paused" | "Stopped" => false,
        _ => {
            return Err(ProbeFailure::parse(
                "playerctl returned an unknown playback status",
            ))
        }
    };
    Ok(MediaState {
        title: title.to_owned(),
        artist: (!artist.is_empty()).then(|| artist.to_owned()),
        playing,
    })
}
