# guest-init

`flow-guest-init` is the init of a connector micro-VM. libkrun's own init hands
it the guest as root, and it gives the connector image what a container runtime
gives a container today - network, mounts, environment, user - before exec'ing
the workload and becoming it. The VMM injects it into the guest root at
`/flow-guest-init` and builds its argv.

## Roadmap

- `src/main.rs`: the sequence, top to bottom, ending in `execv`. Also the
  errno-to-exit-code map and the stderr framing.
- `src/cli.rs`: the CLI. Everything after `--` is the workload's argv.
- `src/net.rs`: eth0, the default route and the IPv6 sysctls, over the classic
  `ifreq`/`rtentry` ioctls.
- `src/root.rs`: the two files a container runtime would write into `/etc`, the
  scratch mount, the connector mount, and the optional persistent-disk mount.
- `src/sys.rs`: the syscall wrappers the rest is written in terms of.

## Non-obvious details

- **libkrun's init ran first.** `/dev`, `/proc`, `/sys`, `/sys/fs/cgroup`,
  `/dev/pts` and `/dev/shm` are mounted, `lo` is up, and the image's `Env` and
  `WorkingDir` are applied. None of that is repeated here.
- **Nothing here may change the root of the shared mount namespace.** libkrun's
  init reports the workload's exit code through an ioctl on `/`, and only when
  `statfs("/")` returns virtiofs magic. A `pivot_root`, or a mount over `/`,
  leaves it looking at something else, so it skips the report silently and the
  VM exits 0 whatever the workload returned. This is why `--persistent-disk`
  refuses `/`, and every share mount resolves its destination before mounting
  to reject dot components or image-provided symlinks that name `/`.
- **The root arrives writable and needs nothing done to it.** It is the
  container runtime's per-container layer over the image, served read-write
  over virtiofs. Writes to it are bounded by host disk and nothing else, which
  is why `TMPDIR` and `UV_CACHE_DIR` are pointed at the sized, disposable
  scratch disk instead.
- **The connector mount is required, and mounted where the host has it.** Every
  connector run receives one, and `CONNECTOR_MOUNT` in the workload's
  environment names it by its host path, so `--connector-mount` is not optional:
  a guest without the mount would hand the workload a name that resolves to
  nothing. It is mounted `ro,nodev,nosuid` - read-only because the host owns
  every byte of it, and not `noexec`, because `flow-connector-init` is executed
  from it. `crates/connector` is the authority for what the mount contains.
- **Scratch is chowned, the persistent disk is not.** mkfs leaves the scratch
  root owned by root while the workload runs as the image's user. The
  persistent disk is the task's own, and its owner formats its root for the
  client and reads a later `chown` as a delta.
- **Scratch has no `nodev`, `nosuid` or `noexec`.** The writable root also lacks
  these flags, so restricting scratch alone would leave the same access on `/`.
- **Exit codes are a shell's.** 127 for a workload that is not there, 126 for
  one that cannot be run, 125 for a failure of this binary's own - the codes
  libkrun's init would have used.
- **Its stderr never begins a line with a space.** The launcher reads that as
  the connector's readiness signal, and everything written here precedes it, so
  clap's indented argument lists are re-framed rather than printed as they come.
- **Static, and `libc` plus `clap` only.** The connector image it is injected
  into may hold no libc, no shell and no dynamic loader at all.
