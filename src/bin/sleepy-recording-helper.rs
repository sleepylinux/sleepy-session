// SPDX-License-Identifier: GPL-3.0-only
//! Fixed-argument recording worker owned by sleepy-sessiond.

use std::{
    io::{self, Write},
    path::PathBuf,
    process::Stdio,
    time::Duration,
};
use tokio::{
    process::Command,
    signal::unix::{signal, SignalKind},
    time::{sleep, Instant},
};

#[derive(Debug, PartialEq)]
struct Recording {
    output: String,
    path: PathBuf,
    region: Option<sleepy_sdk::RecordingRegion>,
    audio: bool,
}

fn invalid() -> io::Error {
    io::Error::new(io::ErrorKind::InvalidInput, "invalid capture request")
}

fn parse(args: &[String]) -> io::Result<Recording> {
    if args.first().map(String::as_str) != Some("record") {
        return Err(invalid());
    }
    let mut result = Recording {
        output: String::new(),
        path: PathBuf::new(),
        region: None,
        audio: false,
    };
    let mut consent = false;
    let mut status = false;
    let mut seen = std::collections::HashSet::new();
    let mut i = 1;
    while i < args.len() {
        let option = &args[i];
        if !seen.insert(option) {
            return Err(invalid());
        }
        i += 1;
        match option.as_str() {
            "--interactive-consent" => consent = true,
            "--audio" => result.audio = true,
            "--output-id" | "--output-path" | "--region" | "--status-fd" => {
                let value = args.get(i).ok_or_else(invalid)?;
                i += 1;
                match option.as_str() {
                    "--output-id" => result.output = value.clone(),
                    "--output-path" => result.path = PathBuf::from(value),
                    "--status-fd" if value == "1" => status = true,
                    "--region" => {
                        result.region = Some(serde_json::from_str(value).map_err(|_| invalid())?)
                    }
                    _ => return Err(invalid()),
                }
            }
            _ => return Err(invalid()),
        }
    }
    if !consent
        || !status
        || result.output.is_empty()
        || result.output.len() > 128
        || !result
            .output
            .bytes()
            .all(|c| c.is_ascii_alphanumeric() || b"-_.".contains(&c))
        || result.output.starts_with('-')
        || !result.path.is_absolute()
        || result.path.extension().and_then(|s| s.to_str()) != Some("mp4")
        || result.region.is_some_and(|r| !r.is_valid())
    {
        return Err(invalid());
    }
    Ok(result)
}

fn recorder_args(recording: &Recording) -> Vec<String> {
    let mut args = vec!["-w".into()];
    if let Some(r) = recording.region {
        args.extend([
            "region".into(),
            "-region".into(),
            format!("{}x{}+{}+{}", r.width, r.height, r.x, r.y),
        ]);
    } else {
        args.push(recording.output.clone());
    }
    args.extend(["-f".into(), "60".into(), "-c".into(), "mp4".into()]);
    if recording.audio {
        args.extend(["-a".into(), "default_output".into()]);
    }
    args.extend(["-o".into(), recording.path.to_string_lossy().into_owned()]);
    args
}

fn state(value: &str) -> io::Result<()> {
    let mut stdout = io::stdout().lock();
    writeln!(stdout, "STATE {value}")?;
    stdout.flush()
}

async fn record(recording: Recording) -> io::Result<()> {
    // The daemon spawns us on a short-lived Tokio blocking thread. PDEATHSIG
    // would track that thread, so monitor reparenting of the process instead.
    let owner = unsafe { libc::getppid() };
    if owner <= 1 {
        return Err(io::Error::other("capture owner exited"));
    }
    // Install handlers before spawning so early stop/pause signals cannot orphan a worker.
    let mut stop = signal(SignalKind::interrupt())?;
    let mut terminate = signal(SignalKind::terminate())?;
    let mut pause = signal(SignalKind::user_defined1())?;
    let mut command = Command::new("gpu-screen-recorder");
    command
        .args(recorder_args(&recording))
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .kill_on_drop(true);
    // A session daemon timeout can kill this helper; the recorder must die with it.
    let parent = std::process::id() as libc::pid_t;
    unsafe {
        command.pre_exec(move || {
            if libc::prctl(libc::PR_SET_PDEATHSIG, libc::SIGINT) != 0 {
                return Err(io::Error::last_os_error());
            }
            if libc::getppid() != parent {
                return Err(io::Error::other("capture owner exited"));
            }
            Ok(())
        });
    }
    // The daemon chooses a new private filename. Reject existing files and symlinks.
    let file = std::fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(&recording.path)?;
    drop(file);
    struct PendingOutput(Option<PathBuf>);
    impl Drop for PendingOutput {
        fn drop(&mut self) {
            if let Some(path) = self.0.take() {
                let _ = std::fs::remove_file(path);
            }
        }
    }
    let mut pending_output = PendingOutput(Some(recording.path.clone()));
    let mut child = command.spawn()?;
    let started = Instant::now();
    loop {
        if unsafe { libc::getppid() } != owner {
            return Err(io::Error::other("capture owner exited"));
        }
        if child.try_wait()?.is_some() {
            return Err(io::Error::other("recorder exited before producing output"));
        }
        if std::fs::metadata(&recording.path)?.len() > 0 {
            break;
        }
        if started.elapsed() > Duration::from_secs(4) {
            return Err(io::Error::new(
                io::ErrorKind::TimedOut,
                "recorder produced no output",
            ));
        }
        tokio::select! {
            _ = stop.recv() => { child.kill().await?; return Err(io::Error::other("recording cancelled")); },
            _ = terminate.recv() => { child.kill().await?; return Err(io::Error::other("recording cancelled")); },
            _ = sleep(Duration::from_millis(20)) => {},
        }
    }
    state("recording")?;
    pending_output.0 = None;
    let mut paused = false;
    loop {
        tokio::select! {
            status = child.wait() => {
                return if status?.success() { Ok(()) } else { Err(io::Error::other("recorder failed")) };
            },
            _ = pause.recv() => {
                if let Some(pid) = child.id() {
                    if unsafe { libc::kill(pid as libc::pid_t, libc::SIGUSR2) } != 0 { return Err(io::Error::last_os_error()); }
                    paused = !paused;
                    state(if paused { "paused" } else { "recording" })?;
                }
            },
            _ = stop.recv() => break,
            _ = terminate.recv() => break,
            _ = sleep(Duration::from_millis(250)) => {
                if unsafe { libc::getppid() } != owner { break; }
            },
        }
    }
    if let Some(pid) = child.id() {
        unsafe {
            libc::kill(pid as libc::pid_t, libc::SIGINT);
        }
    }
    let status = tokio::time::timeout(Duration::from_secs(4), child.wait())
        .await
        .map_err(|_| io::Error::new(io::ErrorKind::TimedOut, "recorder did not finalize"))??;
    if !status.success() {
        return Err(io::Error::other("recorder finalization failed"));
    }
    Ok(())
}

#[tokio::main(flavor = "current_thread")]
async fn main() {
    let args = std::env::args().skip(1).collect::<Vec<_>>();
    let result = match parse(&args) {
        Ok(request) => record(request).await,
        Err(error) => Err(error),
    };
    if result.is_err() {
        eprintln!("capture failed");
        std::process::exit(1);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    fn request() -> Vec<String> {
        [
            "record",
            "--interactive-consent",
            "--output-id",
            "DP-1",
            "--output-path",
            "/tmp/recording_test.mp4",
            "--status-fd",
            "1",
        ]
        .map(String::from)
        .to_vec()
    }
    #[test]
    fn modes_generate_different_fixed_recorder_arguments() {
        let mut request = parse(&request()).unwrap();
        assert_eq!(
            recorder_args(&request),
            [
                "-w",
                "DP-1",
                "-f",
                "60",
                "-c",
                "mp4",
                "-o",
                "/tmp/recording_test.mp4"
            ]
        );
        request.region = Some(sleepy_sdk::RecordingRegion {
            x: -100,
            y: 20,
            width: 640,
            height: 480,
        });
        request.audio = true;
        assert_eq!(
            recorder_args(&request),
            [
                "-w",
                "region",
                "-region",
                "640x480+-100+20",
                "-f",
                "60",
                "-c",
                "mp4",
                "-a",
                "default_output",
                "-o",
                "/tmp/recording_test.mp4"
            ]
        );
    }
    #[test]
    fn rejects_unknown_duplicate_and_injectable_options() {
        for tail in [
            vec!["--audio", "--audio"],
            vec!["--output-id", "-help"],
            vec!["--exec", "sh"],
            vec!["--status-fd", "2"],
        ] {
            let mut args = request();
            args.extend(tail.into_iter().map(String::from));
            assert!(parse(&args).is_err());
        }
    }
}
