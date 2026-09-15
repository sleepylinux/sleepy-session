# PipeWire observation-client feedback regression

`pw-dump-client-churn-vm.json` contains the first 16 complete JSON batches from
`timeout 1 pw-dump --monitor --no-colors` in the disposable installed Sleepy VM,
2026-09-15, image source `bfa319a6fba977ebd61e172aabd7163c10faa9d5`, PipeWire 1.6.8.
The initial graph is followed by client additions/changes/removals while the old
`pw-mon` source repeatedly launches `wpctl` readbacks. Stopping that monitor alone
reduced task creation from 1708 to 93 in a two-second observation.

The fixture retains only object IDs/types, null removal markers, empty info
objects and metadata keys. All names, device details, values and application
properties were removed. The raw capture is private diagnostic evidence and is
not included. Tests require initial graph notification, then no notifications for
these client-only batches, including fragmented input.

Separate semantic fixtures cover external volume/mute updates, device removal
and default sink/source metadata. `pw-dump` is used because `pw-mon` does not
subscribe to metadata updates or offer a type filter. Frames are limited to
1 MiB, nesting to 64 and retained relevant object IDs to 4096; malformed/oversized
input follows the existing degraded-state/restart supervision.
