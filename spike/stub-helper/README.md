# spike/stub-helper/

`localhost/flow-sandbox-stub:spike`: a stand-in for the libkrun helper that
lets WP05's runtime switch be tested without a VM. WP05 changes only the launch
line and the dial; this image is the smallest thing that satisfies both.

- `Containerfile` - the reactor image plus `socat`.
- `entrypoint.sh` - accepts and ignores the helper CLI, then bridges
  `/sock/init.sock` to a `flow-connector-init` chroot'd into `/rootfs`.

## What it is faithful to

The mounts of CONTRACTS "Helper CLI" (`/rootfs`, `/init`, `/sock`), the
connector-init readiness byte on stderr, and the gRPC control channel arriving
over `/sock/init.sock`.

## What it is not

No VM, tap, nftables, resolver, flow-init, scratch disk, or dependency image.
It ignores every helper flag, including `--exec`. The connector inherits *this*
image's environment, not its own image's `Env` (libkrun's init applies that),
so only self-contained connectors run under it - a static Go connector is all
WP05 needs. `derive-python` needs the real helper, which is WP06.

The one thing here that the real helper does not do is `chroot`, which needs
CAP_SYS_CHROOT - already in podman's default set, so the stub adds no
capability to the launch line WP05 generates.
