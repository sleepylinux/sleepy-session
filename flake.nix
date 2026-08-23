{
  description = "Sleepy Linux durable settings and preset session store";

  inputs = {
    nixpkgs.url = "github:NixOS/nixpkgs/nixos-unstable";
    sleepy-sdk = {
      url = "github:sleepylinux/sleepy-sdk/4c4f7989b957f41f3748ddfb092b0348e2ba9e88";
      flake = false;
    };
  };

  outputs = { self, nixpkgs, sleepy-sdk }:
    let
      systems = [ "x86_64-linux" "aarch64-linux" ];
      forAllSystems = nixpkgs.lib.genAttrs systems;
      packageFor = system:
        let
          pkgs = import nixpkgs { inherit system; };
        in
        pkgs.rustPlatform.buildRustPackage {
          pname = "sleepy-session";
          version = "0.1.0";
          src = self;
          cargoLock = {
            lockFile = ./Cargo.lock;
            # Cargo.lock fixes the SDK revision; this avoids an invented vendor hash.
            allowBuiltinFetchGit = true;
          };
          passthru.sleepy-sdk-source = sleepy-sdk;
        };
    in
    {
      packages = forAllSystems (system:
        let
          pkgs = import nixpkgs { inherit system; };
          package = packageFor system;
          userUnit = pkgs.writeTextDir "share/systemd/user/sleepy-session.service" ''
            [Unit]
            Description=Initialize Sleepy session settings state

            [Service]
            Type=oneshot
            ExecStart=${package}/bin/sleepyctl settings show
            RemainAfterExit=yes

            [Install]
            WantedBy=default.target
          '';
        in {
          default = package;
          sleepy-session = package;
          sleepy-session-user-unit = userUnit;
        });

      checks = forAllSystems (system: {
        build = packageFor system;
      });
    };
}
