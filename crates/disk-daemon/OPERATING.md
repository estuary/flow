# Operating `flow-disk-daemon`

What the daemon needs from a host, what it exposes while it runs, and the
handful of decisions a unit file has to make. For what it *is*, see the
[README](README.md).

## Privileges

The daemon needs `CAP_SYS_ADMIN` to serve a `ublk` device and to mount a
filesystem over it. It does not need, and must not be given, `CAP_DAC_OVERRIDE`
or `CAP_SYS_MODULE`.

Running as root satisfies everything. Running as a dedicated UID with ambient
`CAP_SYS_ADMIN` needs a udev rule granting that UID `/dev/ublk-control` and
`/dev/ublkc*`, because `CAP_SYS_ADMIN` does not bypass file permissions. Nothing
in the daemon chowns anything, and it writes nowhere but its own image and mount
directories.

The host needs Linux 6.2 or later with `ublk_drv` loaded, e2fsprogs 1.47 or
later, and an image directory on a filesystem which punches holes. Each of those
is checked at startup and refused with the command which fixes it, rather than
being discovered by a client's first session.

## The socket's directory is the access control

The session socket is created world-writable, deliberately. A client connecting
to a Unix socket needs write permission on it; the daemon's clients are not the
privileged user it runs as, and the daemon has no user model of its own. So
**the permissions of the directory holding the socket decide who may open a
session**, and nothing else does.

A session costs a device, a thread, a mount, and local storage. A directory any
user can traverse is therefore a local denial of service. Give the socket a
directory owned by the daemon's user and grouped to its clients, mode `0750`.

Systemd socket activation is the better answer where it is available: `ListenStream=`
with `SocketMode=0660` and `SocketGroup=` puts the same decision in the unit file
where an operator will look for it. The daemon does not implement socket
activation today.

## Flags

`flow-disk-daemon serve --help` is authoritative. The ones with no default, which
must be supplied:

| Flag | |
| --- | --- |
| `--uds-path` | Socket the session service listens on. |
| `--image-dir` | Where a disk's sparse image is created. Stripe several drives beneath it rather than naming each one. |
| `--mount-dir` | Where a disk's filesystem is mounted. The daemon owns everything under it, and reclaims what a previous daemon left. |
| `--floor-label` | Journal-spec label the recovery floor is written to. Required, because bounded recovery depends on it and a general-purpose daemon carries no system's label vocabulary. Flow's is `estuary.dev/truncated-at`. |

The rest are optional: `--admin-port` (loopback only), `--log-format`, and the
compaction policy `--horizon-open-ratio` / `--horizon-copy-ratio` /
`--horizon-minimum-bytes`. Policy is safe to change between restarts, because
every disk derives its state from its journal and never from configuration.

Device size and block size are deliberately *not* flags. They are durable
per-disk facts a session supplies at `Open`, so that changing a flag cannot
reinterpret every disk on the host at once.

## The unit file

- **`TasksMax`.** Systemd derives its default from the host's `kernel.pid_max`,
  so it differs between machines and should be set rather than inherited. What a
  disk costs in threads is measured by the soak test, which reports it. On a
  ten-core host:

  | | threads | owners | io-wq workers | descriptors |
  | --- | --- | --- | --- | --- |
  | idle | 12 | 0 | 1 | 13 |
  | serving 6 disks | 39 | 6 | 19 | 44 |
  | after the last closed | 20 | 0 | 5 | 16 |

  The base is the main thread, a tokio worker per CPU, and one `io_uring` worker
  for the control ring. Each disk then adds the one owner thread the kernel
  requires of it, and about three `io_uring` workers — but those come out of the
  *shared* pool this crate registers a ceiling of 128 bounded workers for, so
  they stop growing at around forty disks. A handful of tokio blocking-pool
  threads, which device creation and `syncfs` use, accounts for the rest.

  A hundred disks on that host is therefore roughly 230 to 250 threads: twelve of
  base, a hundred owners, the workers already at their 128 ceiling, and the
  blocking pool. Beyond about forty disks the owner count is the only term still
  growing, one per disk. Set `TasksMax` from that, with room to spare.

  The last row is not a leak. A few workers and blocking-pool threads outlive the
  last disk and idle out on their own; what must reach zero, and does, is the
  owner threads.
- **`TimeoutStopSec`** must exceed the daemon's own 30-second drain, or systemd
  will `SIGKILL` a daemon which was about to report which disks were stuck.
  60 seconds is comfortable.
- **`LimitNOFILE`.** A disk holds four descriptors — its image, its character
  device, its ring, and its wake — plus its share of a broker connection per
  endpoint, which measured at about five per disk in the table above.
- **`Restart=on-failure`** is safe. A daemon which is killed outright leaves its
  mounts and character devices behind, and the next daemon to take the same
  `--mount-dir` unmounts what it finds and deletes the device each mount point
  names, once the kernel confirms the process which served it is gone.
- No `PrivateTmp`, and no `PrivateMounts`: the mounts the daemon returns must be
  visible to whoever places them in a sandbox.

## Stopping

`SIGTERM` and `SIGINT` both drain. The socket is unlinked first, so a client
which reconnects cannot open a disk the daemon is about to stop serving. Every
session then ends with `UNAVAILABLE`, and each tears its disk down before its
stream closes, so the drain is finished exactly when the last device is gone.

It is bounded at 30 seconds. If a session is still live at that point the daemon
exits non-zero, naming each disk which did not tear down and the phase it was in,
because its device is still on the host. If none is, the wait was on a client
which had not closed its connection: nothing was left behind, so the daemon warns
and exits cleanly.

A session never outlives a drain, because every broker call it makes gives up
when the session ends. That matters most during a broker outage: without it, a
disk whose appends were retrying could not be unmounted until the outage ended.

## Observing

With `--admin-port` set, `http://127.0.0.1:<port>/` is a dashboard of the live
sessions, and `/metrics` is a Prometheus scrape. Both are loopback-only and carry
no authentication.

Each live session appears as a `Disk.Session` handler labelled with its journal,
showing which phase it is in — `opening`, `serving`, `publishing`, `committing`,
`closing` — and the device number it was given. A handler's verbosity can be
raised from the dashboard without restarting the daemon.

Metrics are labelled by journal, except the host's:

| Metric | |
| --- | --- |
| `disk_daemon_allocated_bytes` | What a disk's image holds. |
| `disk_daemon_recovery_range_bytes` | Journal bytes above the floor, which a recovery must read. It is what a horizon exists to bound. |
| `disk_daemon_floor_seconds` | Wall-clock second a replay would seek from. |
| `disk_daemon_horizon_pending_blocks` | Blocks a disk's open horizon still owes. Zero when none is open. |
| `disk_daemon_horizons_completed` | Horizons discharged, each of which advanced the floor. |
| `disk_daemon_appended_records` / `_bytes` | What a disk's writer has appended. |
| `disk_daemon_publishes` / `_commits` | Deltas cut, and acknowledgements confirmed. |
| `disk_daemon_admission_stalls` | Mutations the capture channel or a cut refused. A rising count is a disk whose device is being held back by its journal. |
| `disk_daemon_parked_requests` | Device requests waiting on that. |
| `disk_daemon_host_allocated_bytes` | What every live disk of the host holds, which the allocated bitmaps know exactly and `st_blocks` cannot. |
| `disk_daemon_image_dir_free_bytes` | Free space of the filesystem holding the images. |
| `disk_daemon_devices` / `_devices_max` | Devices served, and the kernel's `ublks_max`. The latter counts unprivileged devices only, so it does not bind these; it is reported because it is the first thing an operator reaches for. |

There is deliberately no maximum disk count and no local-capacity policy.
Capacity is answered by scaling the host, and the daemon reports rather than
enforces: alert on `disk_daemon_image_dir_free_bytes` against
`disk_daemon_host_allocated_bytes`.

## What a client sees when a session fails

Every failure is terminal for its session. The gRPC code is the part a client can
act on:

| Code | Meaning |
| --- | --- |
| `INVALID_ARGUMENT` | What the session asked for. Retrying the same request cannot succeed. |
| `FAILED_PRECONDITION` | A session's own state: a second `Open`, or a request before `Open`. |
| `ABORTED` | Another session has taken this disk's journal. This one must not reopen it. |
| `UNAUTHENTICATED` | A broker refused the credential. Refresh it and open again. |
| `UNAVAILABLE` | The daemon is draining, or could not reach a broker. Another host may. |
| `INTERNAL` | The daemon, its device, or its host failed. |

The violations the journal writer detects — a second `Publish` with one
outstanding, a `Commit` with nothing published, and a `Commit` whose bytes differ
from what was published — arrive as `INTERNAL` today, with the reason in the
message. They are the client's to fix and ought to be `FAILED_PRECONDITION`; that
they are not is a rough edge rather than the intent.

A credential must be replaced **before** it expires, which is what `Broker` is
for. The daemon holds no delta waiting for one to arrive, and a client cannot
make its disk quiescent — ext4 writes back and discards on its own schedule — so
the writer's next append can fall anywhere. Nor does an expired credential
reliably arrive as `UNAUTHENTICATED`, because a broker is free to refuse whatever
it was doing rather than the credential: gazette answers an expired token on an
append with `DeadlineExceeded`, since what timed out is the pipeline the append
was waiting for. Treat `UNAUTHENTICATED` as a hint, and expiry as something to
prevent rather than to detect.

## Driving a disk by hand

`flow-disk-daemon client` opens one session and drives it from stdin, which is
how a disk is exercised on real hardware without a runtime:

```console
$ flow-disk-daemon client --uds-path /run/disks/daemon.sock \
    --journal acmeCo/disk/scratch --fragment-store s3://bucket/prefix/ \
    --broker-endpoint https://broker.example --broker-credential "$TOKEN"
mounted /var/lib/disks/disk-3
publish
published 220
commit
committed
quit
closed
```

It prints one line per event and holds the acknowledgement itself, so a commit is
a word rather than a paste. `quit`, end-of-input, and `SIGINT` all end the
session, which is what unmounts the disk and destroys its device.
