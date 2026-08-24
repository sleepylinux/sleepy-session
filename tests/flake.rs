use std::fs;
use std::path::Path;

fn read_repository_file(path: &str) -> String {
    fs::read_to_string(Path::new(env!("CARGO_MANIFEST_DIR")).join(path)).unwrap()
}

#[test]
fn dependency_contract_pins_the_reviewed_gpl_sdk_revision() {
    let flake = read_repository_file("flake.nix");
    let manifest = read_repository_file("Cargo.toml");
    let lockfile = read_repository_file("Cargo.lock");

    assert!(
        flake.contains("github:sleepylinux/sleepy-sdk/2edbe8310eee69c40e4f75924da67a57942bd1c3")
    );
    assert!(manifest.contains("rev = \"2edbe8310eee69c40e4f75924da67a57942bd1c3\""));
    assert!(lockfile.contains("#2edbe8310eee69c40e4f75924da67a57942bd1c3"));
    assert_eq!(flake.matches("github:sleepylinux/sleepy-sdk/").count(), 1);
    assert_eq!(manifest.matches("sleepy-sdk =").count(), 1);
    assert_eq!(lockfile.matches("name = \"sleepy-sdk\"").count(), 1);
}

#[test]
fn flake_exposes_the_session_and_user_unit_packages() {
    let flake = read_repository_file("flake.nix");

    assert!(flake.contains("allowBuiltinFetchGit = true"));
    assert!(flake.contains("sleepy-session = package"));
    assert!(flake.contains("sleepy-session-user-unit = userUnit"));
}
