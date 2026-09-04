{
  description = "Sleepy Linux durable settings and preset session store";

  inputs = {
    nixpkgs.url = "github:NixOS/nixpkgs/nixos-unstable";
    sleepy-sdk = {
      url = "github:sleepylinux/sleepy-sdk/1ee5b424887eb6f7acfe3b931b37a2c610ff6498";
      flake = false;
    };
  };

  outputs = { self, nixpkgs, sleepy-sdk }:
    let
      systems = [ "x86_64-linux" "aarch64-linux" ];
      forAllSystems = nixpkgs.lib.genAttrs systems;
      packageFor = system: withNiriContract:
        let
          pkgs = import nixpkgs { inherit system; };
          niriContract = assert pkgs.niri.version == "26.04"; pkgs.niri;
        in
        pkgs.rustPlatform.buildRustPackage ({
          pname = "sleepy-session";
          version = "0.1.0";
          src = self;
          cargoLock = {
            lockFile = ./Cargo.lock;
            # Cargo.lock fixes the SDK revision; this avoids an invented vendor hash.
            allowBuiltinFetchGit = true;
          };
          nativeBuildInputs = [ pkgs.pkg-config pkgs.dbus ];
          nativeCheckInputs = [ pkgs.coreutils pkgs.util-linux ];
          buildInputs = [ pkgs.dbus ];
          SLEEPY_DBUS_SESSION_CONF = "${pkgs.dbus}/share/dbus-1/session.conf";
          TZDIR = "${pkgs.tzdata}/share/zoneinfo";
          passthru.sleepy-sdk-source = sleepy-sdk;
          meta.license = pkgs.lib.licenses.gpl3Only;
        } // pkgs.lib.optionalAttrs withNiriContract {
          SLEEPY_NIRI_CONTRACT = "${niriContract}/bin/niri";
          checkPhase = ''
            runHook preCheck
            cargo test --offline --release --test bindings \
              compiler_registry_validates_with_niri_26_04 -- --exact --ignored
            runHook postCheck
          '';
        });
    in
    {
      packages = forAllSystems (system:
        let
          pkgs = import nixpkgs { inherit system; };
          package = packageFor system false;
          userUnit = (pkgs.writeTextDir "share/systemd/user/sleepy-session.service" ''
            [Unit]
            Description=Initialize Sleepy session settings state

            [Service]
            Type=oneshot
            ExecStart=${package}/bin/sleepyctl settings show
            RemainAfterExit=yes

            [Install]
            WantedBy=default.target
          '').overrideAttrs (old: {
            meta = (old.meta or {}) // {
              license = pkgs.lib.licenses.gpl3Only;
            };
          });
        in {
          default = package;
          sleepy-session = package;
          sleepy-session-user-unit = userUnit;
        });

      checks = forAllSystems (system: {
        build = packageFor system false;
        niri-bindings = packageFor system true;
      });
    };
}
