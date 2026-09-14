//! Read-only, privacy-preserving summary of the daemon's existing v3 snapshot.
use std::{path::Path, time::Duration};

use serde::Serialize;
use sleepy_sdk::{validate_desktop_envelope, CapabilityAvailability as Status, DesktopEvent};
use tokio::{io::AsyncReadExt, net::UnixStream};

pub const MAX_FRAME_BYTES: usize = 1024 * 1024;
pub const DEADLINE: Duration = Duration::from_secs(2);

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct Report {
    pub ok: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub generation: Option<u64>,
    pub checks: Vec<Check>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<&'static str>,
}

#[derive(Debug, Serialize)]
pub struct Check {
    pub capability: &'static str,
    pub status: Status,
    pub required: bool,
}

impl Report {
    pub fn failure(code: &'static str) -> Self {
        Self {
            ok: false,
            generation: None,
            checks: Vec::new(),
            error: Some(code),
        }
    }

    pub fn human(&self) -> String {
        let mut text = format!(
            "Sleepy desktop: {}\n",
            if self.ok { "ready" } else { "needs attention" }
        );
        if let Some(error) = self.error {
            let explanation = match error {
                "timeout" => "Session snapshot exceeded the two-second deadline.",
                "invalid-snapshot" => "Session sent an invalid or unsupported desktop snapshot.",
                "frame-too-large" => "Session snapshot exceeded the 1 MiB limit.",
                "peer-mismatch" => "Session socket belongs to a different user.",
                "runtime-unavailable" => "Run doctor inside your Sleepy desktop user session.",
                _ => "Session is unavailable. Check sleepy-session.service in your user session.",
            };
            text.push_str(explanation);
            text.push('\n');
        }
        for check in &self.checks {
            let status = match check.status {
                Status::Available => "available",
                Status::Unavailable => "unavailable",
                Status::Unsupported => "unsupported",
                Status::PermissionDenied => "permission denied",
                Status::Timeout => "timeout",
                Status::Parse => "invalid provider response",
                Status::Error => "provider error",
            };
            text.push_str(&format!(
                "  {}: {}{}\n",
                check.capability,
                status,
                if !check.required
                    && matches!(check.status, Status::Unavailable | Status::Unsupported)
                {
                    " (optional)"
                } else {
                    ""
                }
            ));
        }
        text
    }
}

/// One total deadline covers connecting, peer authentication and every byte.
/// No bytes are sent to the daemon and no provider is reprobed.
pub async fn inspect_socket(path: &Path, timeout: Duration) -> Report {
    match tokio::time::timeout(timeout, read_snapshot(path)).await {
        Ok(Ok(report)) => report,
        Ok(Err(code)) => Report::failure(code),
        Err(_) => Report::failure("timeout"),
    }
}

async fn read_snapshot(path: &Path) -> Result<Report, &'static str> {
    let mut stream = UnixStream::connect(path)
        .await
        .map_err(|_| "session-unavailable")?;
    let peer = stream.peer_cred().map_err(|_| "peer-mismatch")?;
    // SAFETY: geteuid has no arguments or memory-safety preconditions.
    if peer.uid() != unsafe { libc::geteuid() } {
        return Err("peer-mismatch");
    }
    let mut frame = Vec::new();
    loop {
        let mut chunk = [0; 4096];
        let count = stream
            .read(&mut chunk)
            .await
            .map_err(|_| "session-unavailable")?;
        if count == 0 {
            return Err("invalid-snapshot");
        }
        let end = chunk[..count].iter().position(|byte| *byte == b'\n');
        let length = end.unwrap_or(count);
        if frame.len() + length > MAX_FRAME_BYTES {
            return Err("frame-too-large");
        }
        frame.extend_from_slice(&chunk[..length]);
        if end.is_some() {
            break;
        }
    }
    let text = std::str::from_utf8(&frame).map_err(|_| "invalid-snapshot")?;
    let envelope = validate_desktop_envelope(text).map_err(|_| "invalid-snapshot")?;
    let DesktopEvent::FullSnapshot(snapshot) = envelope.payload else {
        return Err("invalid-snapshot");
    };
    let mut checks = Vec::new();
    let mut add = |capability, status, required| {
        checks.push(Check {
            capability,
            status,
            required,
        })
    };
    add("hyprland", snapshot.compositor.hyprland.status, true);
    add("lock", snapshot.system.lock.status, true);
    add("network", snapshot.system.network.status, false);
    add("audio", snapshot.system.audio.status, false);
    add("bluetooth", snapshot.system.bluetooth.status, false);
    add("battery", snapshot.system.battery.status, false);
    add("brightness", snapshot.system.brightness.status, false);
    add("power", snapshot.system.power.status, false);
    add("media", snapshot.system.media.status, false);
    add("night-light", snapshot.system.night_light.status, false);
    add("resources", snapshot.resources.availability.status, false);
    add("screenshot", snapshot.utilities.screenshot.status, false);
    add("recording", snapshot.utilities.recording.status, false);
    let ok = checks.iter().all(|check| match check.status {
        Status::Available => true,
        Status::Unavailable | Status::Unsupported => !check.required,
        _ => false,
    });
    Ok(Report {
        ok,
        generation: Some(envelope.generation),
        checks,
        error: None,
    })
}
