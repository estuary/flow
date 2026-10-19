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
- `src/root.rs`: the scratch mount and what is bound from it, the copy of
  `/etc` holding the two files a container runtime would write, the connector
  mount, and the optional persistent-disk mount.
- `src/sys.rs`: the syscall wrappers the rest is written in terms of.

## Non-obvious details

- **libkrun's init ran first.** `/dev`, `/proc`, `/sys`, `/sys/fs/cgroup`,
  `/dev/pts` and `/dev/shm` are mounted, `lo` is up, and the image's `Env` and
  `WorkingDir` are applied. None of that is repeated here. The VMM resolves
  `HOME` into that `Env`.
- **`HOSTNAME` is the image's, or the kernel's.** libkrun's init names the
  kernel after the image's `HOSTNAME`, or `localhost` without one. An image
  value, even an empty one, stays as podman keeps it; a missing one is set to
  the kernel's hostname before `--as-root-exec` or the workload runs.
- **Nothing here may change the root of the shared mount namespace.** libkrun's
  init reports the workload's exit code through an ioctl on `/`, and only when
  `statfs("/")` returns virtiofs magic. A `pivot_root`, or a mount over `/`,
  leaves it looking at something else, so it skips the report silently and the
  VM exits 0 whatever the workload returned. This is why `--persistent-disk`
  refuses `/`, and every share mount resolves its destination before mounting
  to reject dot components or image-provided symlinks that name `/`.
- **The root arrives read-only.** The VMM enforces this outside the guest.
  The sized, disposable scratch disk is mounted at `/scratch`, root-owned:
  - `/tmp` and `/var/tmp` are bound from `/scratch/tmp` and `/scratch/var-tmp`
    (1777), for a workload that names them rather than `TMPDIR`. `TMPDIR` is
    `/tmp` whatever the image's `Env` says.
  - `UV_CACHE_DIR` is `/scratch/uv-cache`, the workload's own and apart from
    `TMPDIR`: uv refuses a project inside its cache, and derive-python makes
    its projects in `TMPDIR`.
  - `/etc` is copied to `/scratch/etc`, which gets the generated
    `resolv.conf` and `hosts` and is bound back over `/etc` read-only. An
    image commonly has no `/etc/hosts`, since a container runtime mounts its
    own, and nothing can be created in a read-only root. The copy keeps
    directories, regular files and symlinks with their owners and modes, and
    refuses anything else.

  libkrun's own `/dev/shm` is RAM, bounded by guest memory. The image must
  have `/tmp` and `/var/tmp` directories.
- **Mount points are the VMM's.** `/scratch`, the connector mount and the
  persistent disk's guest path are virtual directories the VMM adds to the
  root share, so nothing here creates a directory outside scratch.
- **The connector mount is required, and mounted where the VMM says.** Every
  connector run receives one, and `CONNECTOR_MOUNT` in the workload's
  environment names it at `/connector-mount`, whatever its host path, so
  `--connector-mount` is not optional:
  a guest without the mount would hand the workload a name that resolves to
  nothing. It is mounted `ro,nodev,nosuid` - read-only because the host owns
  every byte of it, and not `noexec`, because `flow-connector-init` is executed
  from it. `crates/connector` is the authority for what the mount contains.
- **The user's groups come from the VMM.** It resolves `--supplementary-gid`
  from the image's own account files; nothing here reads them. The list
  replaces the guest's, set before the gid and the uid, since nothing can set
  it once the uid has dropped. `--run-as-root` skips all three and still gives
  uv's cache to `--uid`/`--gid`, leaving `HOME` the image user's.
- **uv's cache is chowned, the persistent disk is not.** mkfs leaves the
  scratch root owned by root, and it stays so; the workload runs as the
  image's user. The persistent disk is the task's own, and its owner formats
  its root for the client and reads a later `chown` as a delta.
- **Scratch is `nodev,nosuid`, and not `noexec`.** uv runs a project's
  environment from `TMPDIR`. `/etc` is read-only besides.
- **Exit codes are a shell's.** 127 for a workload that is not there, 126 for
  one that cannot be run, 125 for a failure of this binary's own - the codes
  libkrun's init would have used.
- **Its stderr never begins a line with a space.** The launcher reads that as
  the connector's readiness signal, and everything written here precedes it, so
  clap's indented argument lists are re-framed rather than printed as they come.
- **Static, and `libc` plus `clap` only.** The connector image it is injected
  into may hold no libc, no shell and no dynamic loader at all.
