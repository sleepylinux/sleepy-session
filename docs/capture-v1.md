# Opt-in asynchronous capture

Desktop v3 remains closed and unchanged. Its existing screenshot/color-picker
helper stays unavailable unless independently installed. Capture v1 is a separate
session-owned endpoint, enabled only by `SLEEPY_CAPTURE_ENABLE=1`:
`/run/user/UID/sleepy/capture.sock` (0600, same-UID peers, private runtime directory).
The daemon resolves `sleepy-capture-job-helper` from its package PATH at startup.
Without that helper, capture capabilities report screenshot=false.

Use `sleepyctl capture request JSON` with the SDK capture-v1 request. One bounded
JSON reply is printed; malformed requests/replies or transport errors fail the CLI.
A valid protocol error remains a JSON `payload.type=error`, which callers inspect.
The transaction has a two-second total deadline. `begin` returns immediately;
`status` polls the job and `cancel` signals it. Neither consent nor polling takes
the desktop mutation lock. Disconnecting a requester does not abandon its job.

There is one active job. Repeating its UUID/output returns the same job; changing
the output for that UUID is rejected. Another active UUID receives busy. A job has
120 seconds total helper lifetime for consent/capture/delivery. Sixteen job records
are retained; evicting a completed job deletes only its recorded matching inode,
never a replacement file. PNG results are temporary: copy a wanted result before
job eviction or session end. Unknown/orphan files are never swept.

## Fixed helper boundary

The only helper argv is:

```
sleepy-capture-job-helper --job-id UUID --output-id output:NAME \
  --output /run/user/UID/sleepy/captures/screenshot-UUID.png --output-fd 4
```

`--output` is correlation metadata, **not a pathname the helper may open**.
FD 4 is a daemon-owned anonymous mode-0600 memfd. The helper writes PNG bytes there
only after actual user consent. Stdout is newline-delimited JSON with `state`
(awaitingConsent, capturing, completed, cancelled, failed), optionally a bounded
SDK diagnostic. Completed requires the final completed message AND exit success.
Qt logs belong on stderr. Stdout frames are limited to 4096 bytes/16 messages;
stderr is drained with a 64 KiB ceiling. NOTIFY_SOCKET is removed from the child.

Cancellation, timeout and shutdown terminate only the owned process group. The
leader remains unreaped until that group is killed, pinning its PID against reuse;
the leader is then reaped before publishing terminal state. A parent-death signal
also protects the direct helper on daemon crash. The helper has a 32 MiB file-size
limit. After successful completion and child cleanup the daemon seals the memfd,
checks ownership/mode, decodes the full PNG (dimensions <=32768, decoded buffer
<=128 MiB), and uses existing exclusive atomic publication. Cancellation/failure
closes the memfd without creating a result pathname. No existing file or symlink
is overwritten. Normal daemon SIGTERM follows the existing orderly stop lifecycle.

## Verification

`cargo test --locked --test capture` uses executable helper fixtures, real processes,
Unix sockets and PNG bytes; it covers nonblocking consent, cancellation, duplicate
IDs, invalid outputs/messages, decode/publication security, bounded history and
owned-child cleanup. It does not establish graphical consent or compositor capture.

The supplemental real daemon/CLI SIGTERM test is explicit because Nix sandboxes
may forbid Linux user/mount namespaces:

```
cargo test --locked --test capture_daemon -- --ignored --nocapture
```

It uses a private-propagation mount namespace, fresh tmpfs `/run`, isolated D-Bus,
real sleepy-sessiond/sleepyctl and a fixture helper. Host runtime files are not
changed. It is a process integration check, not a VM boot or graphical capture proof.
Those remain required in root acceptance with the packaged consent helper.
