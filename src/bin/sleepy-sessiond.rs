use std::{env, io, path::PathBuf, process::ExitCode};

use sleepy_session::sessiond::{
    full_snapshot_event, EventHub, GenerationAllocator, GenerationAuthority, SessionSocket,
    ShutdownCoordinator,
};

#[tokio::main]
async fn main() -> ExitCode {
    match run().await {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) => {
            eprintln!("sleepy-sessiond: {error}");
            ExitCode::from(1)
        }
    }
}

async fn run() -> io::Result<()> {
    let runtime_dir = required_path("XDG_RUNTIME_DIR")?;
    let state_dir = state_home()?;
    let socket_path = runtime_dir.join("sleepy/session.sock");
    let generation_path = state_dir.join("sleepy/session-generation");

    let mut allocator = GenerationAllocator::open(generation_path, 1024)?;
    let generation = allocator.next_generation()?;
    let hub = EventHub::new(full_snapshot_event(generation)?, 256);
    let authority = GenerationAuthority::new(allocator, generation, hub.clone());
    let socket = SessionSocket::bind(&socket_path, unsafe { libc::geteuid() }, hub.clone()).await?;
    let shutdown = ShutdownCoordinator::new(authority, std::time::Duration::from_secs(2));
    tokio::select! {
        result = socket.serve() => result,
        signal = tokio::signal::ctrl_c() => {
            signal?;
            shutdown.reconcile(&[]).await?;
            socket.shutdown_and_drain(std::time::Duration::from_secs(2)).await?;
            Ok(())
        }
    }
}

fn required_path(name: &str) -> io::Result<PathBuf> {
    env::var_os(name).map(PathBuf::from).ok_or_else(|| {
        io::Error::new(
            io::ErrorKind::NotFound,
            format!("required environment variable {name} is not set"),
        )
    })
}

fn state_home() -> io::Result<PathBuf> {
    if let Some(path) = env::var_os("XDG_STATE_HOME") {
        return Ok(PathBuf::from(path));
    }
    Ok(required_path("HOME")?.join(".local/state"))
}
