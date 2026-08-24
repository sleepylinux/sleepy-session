use std::process::ExitCode;

use serde_json::json;
use sleepy_session::cli;

fn main() -> ExitCode {
    match cli::run(std::env::args().skip(1).collect()) {
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
