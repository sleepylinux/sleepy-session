// SPDX-License-Identifier: GPL-3.0-only
use std::{fs, os::unix::fs::PermissionsExt, path::PathBuf, process::Stdio, time::Duration};
use tokio::{
    io::{AsyncBufReadExt, BufReader},
    process::Command,
};

fn fixture(script: &str) -> (tempfile::TempDir, String) {
    let dir = tempfile::tempdir().unwrap();
    let path = std::env::var_os("PATH").unwrap();
    let shell = std::env::split_paths(&path)
        .map(|p| p.join("sh"))
        .find(|p| p.is_file())
        .unwrap();
    let backend = dir.path().join("gpu-screen-recorder");
    fs::write(&backend, format!("#!{}\n{}", shell.display(), script)).unwrap();
    fs::set_permissions(&backend, fs::Permissions::from_mode(0o700)).unwrap();
    let search_path = format!("{}:{}", dir.path().display(), path.to_string_lossy());
    (dir, search_path)
}

fn helper(path: &str, output: PathBuf) -> Command {
    let mut command = Command::new(env!("CARGO_BIN_EXE_sleepy-recording-helper"));
    command
        .env("PATH", path)
        .args([
            "record",
            "--interactive-consent",
            "--output-id",
            "DP-1",
            "--status-fd",
            "1",
            "--output-path",
        ])
        .arg(output)
        .stdout(Stdio::piped())
        .stderr(Stdio::null())
        .kill_on_drop(true);
    command
}

#[tokio::test]
async fn helper_acknowledges_output_and_controls_only_its_child() {
    let (dir, path) = fixture(
        r#"
while [ "$#" -gt 0 ]; do
    if [ "$1" = '-o' ]; then shift; output="$1"; fi
    shift
done
trap 'exit 0' INT TERM
trap ':' USR2
printf 'fixture-video' > "$output"
while :; do sleep 0.02; done
"#,
    );
    let output = dir.path().join("recording_test.mp4");
    let mut child = helper(&path, output.clone()).spawn().unwrap();
    let mut lines = BufReader::new(child.stdout.take().unwrap()).lines();
    for (index, expected) in ["STATE recording", "STATE paused", "STATE recording"]
        .into_iter()
        .enumerate()
    {
        let line = tokio::time::timeout(Duration::from_secs(5), lines.next_line())
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        assert_eq!(line, expected);
        if index < 2 {
            assert_eq!(
                unsafe { libc::kill(child.id().unwrap() as i32, libc::SIGUSR1) },
                0
            );
        }
    }
    assert_eq!(
        unsafe { libc::kill(child.id().unwrap() as i32, libc::SIGINT) },
        0
    );
    assert!(tokio::time::timeout(Duration::from_secs(5), child.wait())
        .await
        .unwrap()
        .unwrap()
        .success());
    assert_eq!(fs::read(output).unwrap(), b"fixture-video");
}

#[tokio::test]
async fn exited_backend_never_reports_recording_and_existing_file_is_preserved() {
    let (dir, path) = fixture("exit 1\n");
    let output = dir.path().join("recording_test.mp4");
    let result = helper(&path, output.clone()).output().await.unwrap();
    assert!(!result.status.success());
    assert!(result.stdout.is_empty());
    assert!(
        !output.exists(),
        "a failed start must not appear in recording history"
    );
    fs::write(&output, b"keep").unwrap();
    let result = helper(&path, output.clone()).output().await.unwrap();
    assert!(!result.status.success());
    assert_eq!(fs::read(output).unwrap(), b"keep");
}
