use std::fs;

#[test]
fn flake_pins_the_sdk_source_and_exposes_the_session_and_user_unit_packages() {
    let flake = fs::read_to_string("flake.nix").unwrap();

    assert!(
        flake.contains("github:sleepylinux/sleepy-sdk/4c4f7989b957f41f3748ddfb092b0348e2ba9e88")
    );
    assert!(flake.contains("allowBuiltinFetchGit = true"));
    assert!(flake.contains("sleepy-session = package"));
    assert!(flake.contains("sleepy-session-user-unit = userUnit"));
}
