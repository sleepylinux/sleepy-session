# Sleepy Session

`sleepy-session` is the typed, user-scoped service boundary for the Sleepy
desktop. `sleepy-sessiond` publishes snapshot-first desktop event streams and
accepts versioned commands defined by `sleepy-sdk`; `sleepyctl` is the CLI
client and diagnostic interface. The daemon also owns durable settings and
preset state below XDG paths:

- settings: `$XDG_CONFIG_HOME/sleepy/settings.json`
- user presets: `$XDG_STATE_HOME/sleepy/presets.json`

When those XDG variables are unset, the standard `$HOME/.config` and
`$HOME/.local/state` fallbacks apply. Initial defaults are written only when a
document does not yet exist. Writes validate first, sync a same-directory
temporary file, atomically rename it into place, then sync the parent directory.
If syncing the parent directory fails after the rename, the CLI reports the
structured `commit_state_unknown` error: the candidate may already be visible,
so callers must re-read state rather than blindly retrying a mutation.

The immutable built-in preset is `builtin.sleepy`. User presets have canonical
hyphenated UUID identifiers. Unknown document fields and malformed data are
rejected by the `sleepy-sdk` v1 validators.

## Runtime boundary

The daemon creates private, same-UID runtime sockets under the user's XDG
runtime directory. The main event/command transport carries complete versioned
snapshots, monotonic generations and UUID-correlated replies. Dedicated
`daily.sock`, `osd.sock`, and `theme.sock` boundaries keep launcher/calendar/
weather operations, ordered OSD publications, and theme transactions isolated.
Unknown schema versions, unknown fields, stale generations and peer UID
mismatches fail closed.

The daemon owns actions that need sequencing or confinement: recording
start/pause/stop and same-user recording deletion, idle inhibition, game mode,
lock, suspend only after confirmed lock, logout, reboot and power-off. It also
reconciles system-domain mutations rather than asking QML to invent successful
state. Hyprland dispatch, NetworkManager, PipeWire, MPRIS, tray and other
ordinary desktop providers remain direct shell integrations; they are not
proxied merely for naming consistency.

`sleepy-locker` is a separate supervised native process. It exclusively owns
the Wayland session-lock protocol and PAM authentication. Passwords never enter
QML, daemon JSON, process arguments, environment variables or logs, and the
desktop protocol intentionally defines no unlock command.

## CLI

All successful commands print one JSON document to standard output. Failures
print `{ "error": { "code", "message" } }` JSON to standard error.

```sh
sleepyctl settings show
sleepyctl presets list
sleepyctl presets duplicate builtin.sleepy "My preset"
sleepyctl presets rename <uuid> "New name"
sleepyctl presets activate <preset-id>
sleepyctl events watch --format ndjson
```

Successful mutations are acknowledged only after the authoritative provider
has accepted them; clients then consume the resulting snapshot/event. There is
no arbitrary shell-command request and no optimistic state mutation contract.

## Supervision

The flake exports the daemon, CLI and user units consumed by the Sleepy NixOS /
Home Manager modules. UWSM starts the Hyprland session; systemd user ordering
makes the session daemon ready before the Quickshell shell, while the locker is
independently supervised so restarting or crashing the shell cannot unlock the
session.

## Development

```sh
cargo fmt --check
cargo clippy --all-targets -- -D warnings
cargo test
cargo build --release
```

The repository includes a standalone Nix flake. Run `nix flake check` on a
Nix-enabled host in addition to the Rust checks. The flake pins the exact
reviewed `sleepy-sdk` source and permits Nix to fetch the Git dependency
recorded in `Cargo.lock`; no placeholder vendor hash is used.

## License

Licensed under GPL-3.0-only. See [LICENSE](LICENSE).
