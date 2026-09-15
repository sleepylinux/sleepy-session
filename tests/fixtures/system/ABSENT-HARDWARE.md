# Absence fixtures

The original two fixtures below are minimal examples derived from upstream renderers, **not
captured VM output**. The prior VM doctor observed `audio=parse`, `battery=parse`,
and `bluetooth=timeout`; it did not retain raw provider output, so these tests do
not establish the exact cause of each VM diagnostic.

- `wpctl-empty-audio.txt`: WirePlumber 0.5.15
  [`status_run`](https://github.com/PipeWire/wireplumber/blob/0.5.15/src/tools/wpctl.c)
  emits Audio Sinks/Sources/Streams headings even when their iterators yield no
  nodes. Video is a separate namespace. Empty/truncated/malformed responses and
  populated audio without a default output remain errors.
- `upower-display-absent.txt`: UPower 1.91.3
  [`up_daemon_update_display_battery`](https://gitlab.freedesktop.org/upower/upower/-/blob/v1.91.3/src/up-daemon.c)
  leaves the aggregate type unknown with no contributing battery/UPS;
  `up_daemon_get_warning_level_local` returns none for that type.
  [`up_device_to_text`](https://gitlab.freedesktop.org/upower/upower/-/blob/v1.91.3/libupower-glib/up-device.c)
  emits the generic headers and kind, but no battery state for unknown type.
  The timestamp is illustrative. Battery `state: unknown` is a separate valid
  case already covered by the state mapping test.

Bluetooth tests use BlueZ 5.87's
[`check_default_ctrl`/`cmd_show`](https://github.com/bluez/bluez/blob/5.87/client/main.c):
no controller prints `No default controller available` and exits with status 1.
Other command failures, malformed responses, and hung providers stay errors.
A fixed read-only
[`NameHasOwner`](https://dbus.freedesktop.org/doc/dbus-specification.html#bus-messages-name-has-owner)
call to the bus checks service ownership without activating BlueZ.
[`busctl`'s terse format](https://github.com/systemd/systemd/blob/v258/man/busctl.xml)
encodes boolean replies as `b true`/`b false`. The systemd package providing
busctl is already in Sleepy's session runtime PATH. The existing shared deadline
still bounds a service that disappears after the ownership check.

These versions match the installed alpha's root nixpkgs revision
`2c423e03bbafcff28bfadc6781a4a8257f205cb5` (systemd format reference v258).

## Captured installed VM absence

`upower-display-absent-vm.txt` is the verbatim UPower output from the installed
candidate VM on 2026-09-15 (the command was `upower --show-info
/org/freedesktop/UPower/devices/DisplayDevice`). Client and daemon both reported
version **1.91.3**. This is distinct from the source-derived fixture above.

- Image source: `7231d4b2bf320440365b0f51a323f3466c724dd1`.
- ISO SHA-256: `8237aeac149bc6d281a13b55bd01da1fa5cc998768525efb3bd52048af1a8608`.
- The image used sleepy-session `004e81dbfd10ebcec569129aa9eb6ae1559dab5f`.
- Capture: root workspace `work/debug-usability/read-providers-report.txt`,
  first command's stdout only; the following version lines confirmed 1.91.3.

The aggregate has kind `unknown`, no state, warning level `none`, and a valid
`power supply: no` header. Rejecting that boolean caused `battery=parse` on
this VM. Both valid boolean values are accepted; malformed boolean, state and
warning output remain parse errors. The fixture retains the actual timestamp,
zero percentage and missing-battery icon. This proves the parser regression,
not a complete new installed-system acceptance run.
