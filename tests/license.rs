use std::fs;
use std::path::Path;

fn read(path: impl AsRef<Path>) -> String {
    fs::read_to_string(path).expect("license artifact should be readable")
}

#[test]
fn license_and_package_metadata_are_gpl_v3_only() {
    let repository = Path::new(env!("CARGO_MANIFEST_DIR"));
    let authoritative_license = repository.join("../../sleepy/LICENSE");

    assert_eq!(
        read(repository.join("LICENSE")),
        read(authoritative_license),
        "LICENSE must match the authoritative Sleepy GPLv3 text byte-for-byte"
    );

    let cargo_metadata = read(repository.join("Cargo.toml"));
    assert!(cargo_metadata.contains("license = \"GPL-3.0-only\""));
    assert!(!cargo_metadata.contains("MIT"));
    assert!(!cargo_metadata.contains("GPL-3.0-or-later"));

    let nix_metadata = read(repository.join("flake.nix"));
    assert!(nix_metadata.contains("license = pkgs.lib.licenses.gpl3Only;"));
    assert!(!nix_metadata.contains("licenses.mit"));
    assert!(!nix_metadata.contains("gpl3Plus"));
}
