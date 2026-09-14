use std::{
    io::{self, BufRead, BufReader, Write},
    os::unix::net::UnixStream,
    path::PathBuf,
    process::ExitCode,
};

use serde_json::json;
use sleepy_session::{cli, doctor};

fn main() -> ExitCode {
    let arguments: Vec<String> = std::env::args().skip(1).collect();
    if arguments.first().map(String::as_str) == Some("doctor") {
        let json = match arguments.as_slice() {
            [_] => false,
            [_, flag] if flag == "--json" => true,
            _ => {
                eprintln!("Usage: sleepyctl doctor [--json]");
                return ExitCode::from(2);
            }
        };
        let report = match std::env::var_os("XDG_RUNTIME_DIR").map(PathBuf::from) {
            Some(path) if path.is_absolute() => {
                match tokio::runtime::Builder::new_current_thread()
                    .enable_all()
                    .build()
                {
                    Ok(runtime) => runtime.block_on(doctor::inspect_socket(
                        &path.join("sleepy/desktop.sock"),
                        doctor::DEADLINE,
                    )),
                    Err(_) => doctor::Report::failure("runtime-unavailable"),
                }
            }
            _ => doctor::Report::failure("runtime-unavailable"),
        };
        if json {
            println!(
                "{}",
                serde_json::to_string(&report).expect("doctor report serializes")
            );
        } else {
            print!("{}", report.human());
        }
        return if report.ok {
            ExitCode::SUCCESS
        } else {
            ExitCode::from(1)
        };
    }
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
