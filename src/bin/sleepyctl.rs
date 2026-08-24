use std::{
    io::{self, BufRead, BufReader, Write},
    os::unix::net::UnixStream,
    path::PathBuf,
    process::ExitCode,
};

use serde_json::json;
use sleepy_session::cli;

fn main() -> ExitCode {
    let arguments: Vec<String> = std::env::args().skip(1).collect();
    if arguments == ["events", "watch"] || arguments == ["events", "watch", "--format", "ndjson"] {
        return match watch_events() {
            Ok(()) => ExitCode::SUCCESS,
            Err(error) => {
                eprintln!("sleepyctl events watch: {error}");
                ExitCode::from(1)
            }
        };
    }

    match cli::run(arguments) {
        Ok(output) => {
            println!(
                "{}",
                serde_json::to_string(&output).expect("JSON values serialize")
            );
            ExitCode::SUCCESS
        }
        Err(error) => {
            let mut output = json!({
                "error": { "code": error.code(), "message": error.message() }
            });
            if let Some(details) = error.details() {
                output["error"]["details"] = details.clone();
            }
            eprintln!(
                "{}",
                serde_json::to_string(&output).expect("JSON errors serialize")
            );
            ExitCode::from(1)
        }
    }
}

fn watch_events() -> io::Result<()> {
    let runtime_dir = std::env::var_os("XDG_RUNTIME_DIR")
        .map(PathBuf::from)
        .ok_or_else(|| io::Error::new(io::ErrorKind::NotFound, "XDG_RUNTIME_DIR is not set"))?;
    let stream = UnixStream::connect(runtime_dir.join("sleepy/session.sock"))?;
    let mut reader = BufReader::new(stream);
    let mut stdout = io::stdout().lock();
    let mut line = String::new();
    loop {
        line.clear();
        if reader.read_line(&mut line)? == 0 {
            return Ok(());
        }
        stdout.write_all(line.as_bytes())?;
        stdout.flush()?;
    }
}
