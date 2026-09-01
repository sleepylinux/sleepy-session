use std::fs;
use std::path::Path;

fn read_repository_file(path: &str) -> String {
    fs::read_to_string(Path::new(env!("CARGO_MANIFEST_DIR")).join(path)).unwrap()
}

#[test]
fn dependency_contract_pins_the_reviewed_gpl_sdk_revision() {
    const SDK_REVISION: &str = "d935d3d83ef3c01627cd315230607c4b04554d42";
    let flake = read_repository_file("flake.nix");
    let manifest = read_repository_file("Cargo.toml");
    let lockfile = read_repository_file("Cargo.lock");

    assert!(flake.contains(&format!("github:sleepylinux/sleepy-sdk/{SDK_REVISION}")));
    assert!(manifest.contains(&format!("rev = \"{SDK_REVISION}\"")));
    assert!(lockfile.contains(&format!("#{SDK_REVISION}")));
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

#[test]
fn process_supervision_fixtures_use_nix_visible_check_tools() {
    let flake = read_repository_file("flake.nix");
    let fixtures = read_repository_file("tests/desktop_services.rs");

    assert!(flake.contains("nativeCheckInputs = [ pkgs.coreutils pkgs.util-linux ]"));
    assert!(!fixtures.contains("/bin/sleep"));
}
