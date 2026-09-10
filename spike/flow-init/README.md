# spike/flow-init

The guest init. libkrun's own init hands it the VM as root, and it gives the
connector image what podman gives a container today - network, a writable
root, mounts, environment, user - before exec'ing the connector. See
CONTRACTS.md "flow-init" for the CLI, PLAN.md for where it sits.

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
- `root.rs`  the mounts: overlay root, `pivot_root`, `/etc`, venv, scratch,
             deps.
- `sys.rs`   the syscall wrappers the rest is written in terms of.

Verify with `spike/tasks/flow-init-test.sh`, which boots derive-python under
the helper and asserts what the connector finds.

## Non-obvious details

- **A mount namespace of its own, first.** libkrun's init shares this
  namespace and still needs its original root: on workload exit its
  `set_exit_code` reports the code through an ioctl on `/`, and only when
  `statfs("/")` returns virtiofs magic. Pivot the shared namespace and it
  silently skips the report - the VM then exits 0 whatever the workload
  returned. `unshare(CLONE_NEWNS)` keeps init's view intact.
- **Then `MS_REC | MS_PRIVATE`.** libkrun's init ends by marking the tree
  `MS_REC | MS_SHARED`, which the new namespace inherits; both `MS_MOVE` and
  `pivot_root` refuse a mount whose parent propagates (EINVAL).
- **devtmpfs is the only writable place before the overlay**, which is why the
  tmpfs staging directory is `/dev/.flow`: the image's root share is
  read-only.
- **/dev is remounted, not moved.** The tmpfs holding the new root lives inside
  /dev, and `MS_MOVE` refuses a target within the mount being moved. devtmpfs
  is one kernel-wide instance, so a second mount shows the same nodes; devpts
  and shm are remade over it.
- **Overlay copy-up needs a libkrun patch.** Every FUSE inode carries
  `S_NOATIME`, so overlayfs copies the lower's fileattr flags on copy-up;
  libkrun answers that ioctl with `EOPNOTSUPP`, which overlayfs treats as
  fatal. `spike/helper/Dockerfile` turns it into `ENOTTY`. Without it nothing
  can be written to the root at all.
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
