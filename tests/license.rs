use sha2::{Digest, Sha256};
use std::fs;
use std::path::Path;

const GPL_V3_LICENSE_SIZE: usize = 34_674;
const GPL_V3_LICENSE_SHA256: &str =
    "fb981668c18a279e285fc4d83fba1e836cc84dd4daa73c9697d3cfd2d8aca6e0";

fn read(path: impl AsRef<Path>) -> String {
    fs::read_to_string(path).expect("license artifact should be readable")
}

#[test]
fn license_and_package_metadata_are_gpl_v3_only() {
    let repository = Path::new(env!("CARGO_MANIFEST_DIR"));
    let license = fs::read(repository.join("LICENSE")).expect("LICENSE should be readable");

    assert_eq!(
        license.len(),
        GPL_V3_LICENSE_SIZE,
        "LICENSE has an unexpected size"
    );
    assert_eq!(
        format!("{:x}", Sha256::digest(&license)),
        GPL_V3_LICENSE_SHA256,
        "LICENSE must be the canonical GNU GPLv3 text"
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
