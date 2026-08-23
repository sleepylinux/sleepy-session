# Sleepy Session

`sleepy-session` owns durable, user-owned settings and preset state for the
Sleepy desktop. It consumes the reviewed `sleepy-sdk` contract revision and
stores v1 documents below XDG paths:

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

## CLI

All successful commands print one JSON document to standard output. Failures
print `{ "error": { "code", "message" } }` JSON to standard error.

```sh
sleepyctl settings show
sleepyctl presets list
sleepyctl presets duplicate builtin.sleepy "My preset"
sleepyctl presets rename <uuid> "New name"
sleepyctl presets activate <preset-id>
```

## Service boundary

`org.sleepy.Session1` is reserved as the future D-Bus service name. This
milestone intentionally provides no D-Bus transport or daemon: `sleepyctl` is
the observable local interface. The flake exports a `sleepy-session-user-unit`
package containing an optional systemd user oneshot that initializes and checks
the state through `sleepyctl settings show`.

## Development

```sh
cargo fmt --check
cargo clippy --all-targets -- -D warnings
cargo test
cargo build --release
```

The repository includes a standalone Nix flake. Nix was unavailable during the
initial local verification; CI or a Nix-enabled host must run `nix flake check`.
The flake also pins the non-flake `sleepy-sdk` source to the reviewed revision
and permits Nix to fetch the Git dependency recorded in `Cargo.lock`; no
placeholder vendor hash is used.
