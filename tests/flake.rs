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
        flake.contains("github:sleepylinux/sleepy-sdk/7abd3194055b4cbeef2a021b73ba7f4ce3923d02")
    );
    assert!(manifest.contains("rev = \"7abd3194055b4cbeef2a021b73ba7f4ce3923d02\""));
    assert!(lockfile.contains("#7abd3194055b4cbeef2a021b73ba7f4ce3923d02"));
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

#[test]
fn flake_exposes_a_mandatory_niri_26_04_bindings_check() {
    let flake = read_repository_file("flake.nix");

    assert!(flake.contains("niri-bindings = packageFor system true"));
    assert!(flake.contains("assert pkgs.niri.version == \"26.04\""));
    assert!(flake.contains("compiler_registry_validates_with_niri_26_04 -- --exact --ignored"));
}

#[test]
fn flake_puts_the_dbus_daemon_on_the_native_check_path() {
    let flake = read_repository_file("flake.nix");

    assert!(flake.contains("nativeBuildInputs = [ pkgs.pkg-config pkgs.dbus ]"));
    assert!(flake.contains("buildInputs = [ pkgs.dbus ]"));
    assert!(flake.contains("SLEEPY_DBUS_SESSION_CONF = \"${pkgs.dbus}/share/dbus-1/session.conf\""));
}
