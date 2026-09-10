# spike/flow-init

The guest init. libkrun's own init hands it the VM as root, and it gives the
connector image what podman gives a container today - network, mounts,
environment, user - before exec'ing the connector. The root needs nothing done
to it: it is podman's per-container writable layer over the image, served
read-write over virtiofs. See CONTRACTS.md "flow-init" for the CLI, PLAN.md
for where it sits.

Built for `x86_64-unknown-linux-musl` and injected into the guest root at
`/flow-init` by the helper shim, which also builds its argv
(`spike/helper/shim/src/main.rs`, `guest_cmd`). The connector image may hold no
libc, no shell and no dynamic loader, so the binary is static and depends on
`libc` alone.

## Layout

`src/main.rs` is the whole sequence, top to bottom, and ends in `execv`:

- `cli.rs`   the CONTRACTS CLI. Everything after `--` is the workload's argv.
- `net.rs`   eth0, the default route, and the IPv6 sysctls, over the classic
             `ifreq`/`rtentry` ioctls.
- `root.rs`  the mounts and the two files podman would write: `/etc`, venv,
             scratch, deps.
- `sys.rs`   the syscall wrappers the rest is written in terms of.

Verify with `spike/tasks/flow-init-test.sh`, which boots derive-python under
the helper and asserts what the connector finds.

## Non-obvious details

- **No overlay, and no `pivot_root`.** The root arrives writable, so there is
  nothing to lay over it - and a guest-side overlayfs could not work anyway:
  every FUSE inode carries `S_NOATIME`, so overlayfs copies the lower's
  fileattr flags on copy-up, and libkrun answers that ioctl with `EOPNOTSUPP`,
  which overlayfs treats as fatal (`failed to retrieve lower fileattr (/etc,
  err=-95)`). WP04 carried a one-line libkrun patch for it; WP04b removed
  both the overlay and the patch.
- **Nothing here may change the root of the shared mount namespace.**
  libkrun's init reports the workload's exit code through an ioctl on `/`, and
  only when `statfs("/")` returns virtiofs magic. A `pivot_root` in this
  namespace leaves init looking at something else, so it skips the report
  silently and the VM exits 0 whatever the workload returned. WP04 hit this
  and worked around it with `unshare(CLONE_NEWNS)`; there is no pivot to
  unshare from now, and `flow-init-test.sh` still asserts the exit code.
- **Root writes are bounded by host disk, not by the VM's memory.** They land
  in podman's container layer, exactly as a container's do today, which is why
  `TMPDIR` and `UV_CACHE_DIR` point at the sized, disposable scratch disk.
- **The scratch disk is chowned to the image's user.** mkfs leaves its root
  owned by root, and `TMPDIR` points there.
- **The dependency set arrives as a block device, not a share.** `--deps-dev`
  mounts `/dev/vdb` read-only at `/opt/venv`. WP08 measured a cold
  `import pandas` at 2.4x-2.9x of podman over virtiofs and 0.9x on ext4 over
  virtio-blk; the cost was per-file metadata round trips, which a block device
  answers from the guest's own caches. No `chown` here, unlike scratch: it is
  read-only and built world-readable.
- **Exit codes are a shell's.** 127 for a workload that is not there, 126 for
  one that cannot be run, 125 for a failure of flow-init's own - the codes
  libkrun's init would have used.
