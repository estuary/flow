# STATUS

Ledger for the spike. Sub-sessions append to the log; the master thread
maintains the table. Newest log entries at the bottom.

## Packages

| WP | Title                                   | Depends on   | Status | Branch              | Notes |
|----|-----------------------------------------|--------------|--------|---------------------|-------|
| 00 | Environment                             | -            | done   | daveg/libkrun-spike | a19095ede57 |
| 01 | connector-init --vsock-port             | -            | todo   | daveg/libkrun-spike |       |
| 02 | Egress ruleset + resolver (netns)       | -            | todo   | daveg/libkrun-spike |       |
| 03 | Helper image + shim                     | -            | done   | daveg/libkrun-spike | a1e1ea562f2; libkrun 1.19.4 build folded into WP04 step 0 |
| 04 | flow-init                               | 03 (to test) | done   | daveg/libkrun-spike | ba94256e321; libkrun 1.19.4 + ENOTTY patch |
| 05 | runtime-next spike switch               | 01 (real)    | todo   | daveg/libkrun-spike |       |
| 06 | Integration: experiments 1-4            | 00-05        | todo   | daveg/libkrun-spike |       |
| 07 | Egress from the guest: experiments 6-8  | 02, 04, 06   | todo   | daveg/libkrun-spike |       |
| 08 | virtiofs import matrix: experiment 5    | 03, 04       | todo   | daveg/libkrun-spike |       |
| 09 | Churn and density: experiments 9-10     | 00, 06       | todo   | daveg/libkrun-spike |       |
| 10 | Storage, exposure, crash: exp 11-13     | 06, 11       | todo   | daveg/libkrun-spike |       |
| 11 | libkrun source read: experiment 12      | -            | todo   | daveg/libkrun-spike |       |
| 12 | Report                                  | all          | todo   | daveg/libkrun-spike |       |

Sequential order, one session at a time, biggest unknowns first:
00, 03, 04, 08, 01, 05, 06, 02, 07, 11, 10, 09, 12. (03 answers "does it
boot from an image mount here"; 08 answers "is virtiofs fast enough" before
any runtime work is spent.)

## Known unknowns (resolve and record here)

- RESOLVED (WP03): `krun_set_exec` wins, and the two must not be combined -
  when `KRUN_INIT` is set, init keeps the kernel cmdline's argv and ignores
  `Cmd` entirely. The shim uses `Cmd` alone.
- RESOLVED (WP03): Fedora 43 packages libkrun 1.19.0 and libkrunfw 5.5.0, so
  neither is built from source. See WP03's question about the 1.19.0 / 1.19.4
  delta.
- RESOLVED (WP00): `vsock_loopback` loads on this box; WP01 can test
  `--vsock-port` host-side against CID 1.
- What does the libkrun README's "does not provide any protection against the
  guest attempting to access other directories in the same filesystem" mean
  concretely for the read-only image share? (WP11)

## Log

<!-- Append entries below. Format:
### YYYY-MM-DD WPnn: <one-line outcome>
- shipped: ...
- verification: <command> -> <result>
- deviations from CONTRACTS.md: none | ...
- questions for master: none | ...
-->

### 2026-09-09 WP00: box provisioned; one command sets it up, one checks it.

- shipped:
  - `spike/tasks/env-common.sh` - names, subnets, image list, reactor dir,
    sourced by the other three scripts.
  - `spike/tasks/env-setup.sh` - idempotent. podman 4.9.3 + uidmap,
    slirp4netns, netavark, aardvark-dns from noble; rootful `podman.socket`;
    networks `flow-connectors` (10.89.0.0/24) and `spike-testnet2`
    (198.51.100.0/24); self-signed cert and two 1 KiB bodies generated into
    `spike/tasks/nginx/`; `spike-nginx` at 198.51.100.10 on 80 and 443;
    six images pulled; `/var/tmp/flow-spike/reactor` created and confirmed
    ext4; `vsock_loopback` modprobed.
  - `spike/tasks/env-check.sh` - read-only, one `FAIL` line per problem,
    exit 1 if any. Includes a live fake-reactor CapEff check.
  - `spike/tasks/fake-reactor.sh CMD...` - per CONTRACTS "Fake reactor".
    `--network=host --user root:root --no-hosts`, no `--cap-add`, no
    `--privileged`, no `--device`; `/run/podman`, `$SPIKE_REACTOR_DIR` at
    the same path, and the repo's `target/` at `/flow-target`.
  - `spike/tasks/nginx/nginx.conf` committed; cert, key and `www/` are
    generated and gitignored.

- verification:
  ```
  $ spike/tasks/env-setup.sh && spike/tasks/env-check.sh
  ok    podman 4.9.3
  ok    /run/podman/podman.sock
  ok    /dev/kvm
  ok    /dev/net/tun
  ok    network flow-connectors
  ok    network spike-testnet2 198.51.100.0/24
  ok    http://198.51.100.10/probe -> 1024 bytes
  ok    https://198.51.100.10/probe -> 1024 bytes
  ok    https://198.51.100.10/ -> 1024 bytes
  ok    /var/tmp/flow-spike/reactor is ext4
  ok    fake reactor CapEff 00000000800405fb
  ok    vsock_loopback loaded

  env-check.sh: ok

  $ spike/tasks/fake-reactor.sh podman ps
  CONTAINER ID  IMAGE                             COMMAND   ...  NAMES
  75ba4423c99e  docker.io/library/nginx:latest    nginx ... ...  spike-nginx
  619144b6b043  ghcr.io/estuary/reactor:v0.6...   podman ps ...  vigorous_goldstine

  $ spike/tasks/fake-reactor.sh grep CapEff /proc/self/status
  CapEff:	00000000800405fb

  $ curl -sk https://198.51.100.10/probe | wc -c
  1024
  ```

- image digests (pulled 2026-09-09):
  ```
  ghcr.io/estuary/reactor@sha256:7f5d773418042d355fe5bd8fe3da262b2e679374b224bff055d8f6d369ab06f0
  ghcr.io/estuary/derive-python@sha256:c26548740a9e967274f6d7b9c79bed73630bf61bd187c3afca7564577ef364c7
  ghcr.io/estuary/source-hello-world@sha256:ebac9e3e9f16cec5d27d59de6e40186bab142fd93d3de0c4bfbea73e06b79941
  ghcr.io/estuary/materialize-sqlite@sha256:54d2853ccf0319e33ef122cd5f0db1b89c1f87d4fce7168d779204e0bece179f
  docker.io/library/busybox@sha256:1cfa4e2b09e127b9c4ed43578d3f3c18e7d44ea47b9ea98475c0cbe9086525f8
  docker.io/library/nginx@sha256:05b8cb60c354a44ab824ea6e7dc69b46d50762cdbe728a347a5b656e6fb3d7c4
  ```
  Reactor tag: `ghcr.io/estuary/reactor:v0.6.13-127-g6339ac87653`. Its config
  has no ENTRYPOINT (`Cmd=[/bin/bash]`, `WorkingDir=/tmp`), so
  `fake-reactor.sh CMD...` needs no `--entrypoint` override. It ships a podman
  client and coreutils.

- known unknowns resolved:
  - `vsock_loopback` loads on this box. WP01's host-side AF_VSOCK test of
    `flow-connector-init --vsock-port` is available.

- deviations from CONTRACTS.md: none. Two departures from the WP00 brief:
  - The brief says to confirm the reactor filesystem with `stat -f -c %T`.
    That reports ext4 as `ext2/ext3` (ext2/3/4 share a superblock magic), so
    both scripts use `findmnt -no FSTYPE -T`, which distinguishes them.
    `/var/tmp/flow-spike/reactor` is ext4 on `/dev/root`.
  - The brief points at the Quadlet `Image=` line for the reactor tag. There
    is no Quadlet on this box and PLAN's inputs do not quote the line, so
    env-common.sh pins the newest published tag (above) and honors a
    `SPIKE_REACTOR_IMAGE` override.

- questions for master:
  - Should the reactor image be repinned to production's actual tag? If the
    Quadlet `Image=` line is available, set `SPIKE_REACTOR_IMAGE` in
    `spike/tasks/env-common.sh`; nothing else needs to change.
  - `flow-connectors` got 10.89.0.0/24, inside the baseline denylist's
    10.0.0.0/8. That is production's shape, but it means WP02's "helper's own
    eth0 subnet" exception and the 10/8 baseline drop overlap. Confirm the
    exception is meant to be narrower than the drop (it must be, or the
    helper's uplink is unusable), and that WP02 orders them accordingly.
  - `docker.io/library/busybox` and `nginx` are pinned by tag, not digest.
    Fine for a spike, but experiment numbers will name the digests above.

### 2026-09-09 master: WP00 accepted; answers to its questions

- Reactor image: keep the newest tag. What the fake reactor must match is
  the capability set and the podman client, and both come from the same
  noble packages at any tag. Production's tag at inspection time is now
  recorded in PLAN.md inputs for the report.
- 10.89.0.0/24 inside 10/8: there is no eth0-subnet exception, so nothing to
  order. The baseline entries are DESTINATION drops in the forward chain; the
  guest's masqueraded traffic to non-baseline destinations still leaves via
  eth0 untouched. The only exception anywhere is guest -> helper udp/53 on
  the tap, and it lives in the input chain. What the overlap does require:
  the baseline set must be `flags interval` with `auto-merge` (or deduped
  before load), since nft rejects overlapping interval elements otherwise.
  Added to CONTRACTS.md and WP02.md.
- busybox and nginx by tag: fine. The report names the digests WP00 logged.
- Next: WP03.

### 2026-09-09 WP03: a busybox image boots as a libkrun guest, launched from the fake reactor.

- shipped:
  - `spike/helper/Dockerfile` -> `localhost/flow-sandbox-helper:spike`. Fedora
    43 base; nftables, e2fsprogs, iproute, socat, tcpdump, conntrack-tools.
  - `spike/helper/shim/` - the Rust shim, PID 1 of the helper container.
    `main.rs` is the whole sequence; `sys.rs` holds libkrun's ABI, `cli.rs` the
    CONTRACTS CLI, `image.rs` the inspect-to-`.krun_config.json` translation,
    `disk.rs` the O_TMPFILE scratch disk, `net.rs` the tap and egress execs,
    `console.rs` the console descriptors and `--debug` tee.
  - `spike/helper/guest/vsock-echo.c` - static AF_VSOCK echo, injected as
    `/vsock-echo`.
  - `spike/helper/stubs/` - placeholder `flow-init` (WP04) and
    `flow-sandbox-egress` / `flow-sandbox-resolver` (WP02). The egress stub
    loads an empty `inet flow_sandbox` table so the nft path is exercised; it
    enforces nothing.
  - `spike/helper/README.md`.
  - `spike/tasks/helper-build.sh`, `spike/tasks/helper-smoke.sh`,
    `spike/tasks/helper-common.sh`.

- verification: `helper-smoke.sh` runs the four WP03 cases plus the helper
  interior and SIGKILL checks, and passes identically launched with `sudo
  podman` (`--host`) and through `fake-reactor.sh` (default). The reactor's
  privilege level - podman's default caps, the API socket as the only
  privilege - is enough to launch the helper.

  ```
  $ spike/tasks/helper-build.sh && spike/tasks/helper-smoke.sh
  helper-smoke.sh: launching via reactor, guest docker.io/library/busybox:latest

  ok    vsock: guest port 49092 echoes through fs_bda06bd050acead8/sock/init.sock
  ok    exit code: the guest workload's 7 reached the helper's caller
  ok    devices: vda present in /proc/partitions
  ok    devices: fs, blk, net, vsock, console, balloon and rng all on the bus
        guest /proc/partitions and virtio bus:
        major minor  #blocks  name

         254        0    1048576 vda
        ---
        virtio0 virtio:d00000005v00000000
        virtio1 virtio:d00000004v00000000
        virtio2 virtio:d00000003v00000000
        virtio3 virtio:d0000001Av00000000
        virtio4 virtio:d0000001Av00000000
        virtio5 virtio:d00000002v00000000
        virtio6 virtio:d00000013v00000000
        virtio7 virtio:d00000001v00000000
  ok    stdio: the workload's stderr reached the helper's fd 2
  ok    stdio: the workload's stdout landed on fd 1, alongside the kernel console
  ok    stdio: no line on the helper's stderr begins with a space
        helper stderr:
        err
  ok    helper: nft ruleset holds table inet flow_sandbox
  ok    helper: tap0 is 192.0.2.1/30
        df used KiB: before=26768080 running=26769004 after=26768116
  ok    SIGKILL: scratch dir empty and 924 KiB returned to the filesystem

  helper-smoke.sh: ok
  ```

  Checks the brief lists that are not in the script, run by hand at this commit
  against the same busybox guest:

  ```
  --debug                       -> 0 stderr lines begin with a space;
                                   "kernel: hello" tees correctly
  --venv-dax --thp-disable      -> boots; guest mounts `-o dax` virtiofs venv
                                   and reads a file from it
  /dev/vda                      -> mounts ext4, writable
  guest env                     -> PATH is the image's Config.Env value,
                                   HOME=/, pwd=/ (busybox has no WorkingDir)
  --policy <missing file>       -> "flow-sandbox-helper: reading ...: No such
                                   file or directory", exit 2
  ```

- known unknowns resolved:
  - **`krun_set_exec` wins over `Cmd`; they must not be combined.** From
    `src/init_blob/init/init.c` in v1.19.4: init sets `exec_argv = argv` (the
    kernel cmdline's), then `if (KRUN_INIT) exec_argv[0] = KRUN_INIT; else if
    (config_argv) exec_argv = config_argv;`. `krun_set_exec` is what sets
    `KRUN_INIT`, so calling it discards `Cmd` entirely and keeps only the
    cmdline argv with argv[0] replaced. The shim therefore never calls
    `krun_set_exec` (nor `krun_set_env`, which would otherwise slurp the
    helper's own environment into the guest); `Cmd`, `WorkingDir` and `Env`
    come from `/.krun_config.json` alone. Verified: every smoke case runs this
    way.
  - **libkrun is packaged; we do not build it.** Fedora 43 ships
    `libkrun-1.19.0-1.fc43` and `libkrunfw-5.5.0-1.fc43`. 1.19.0's header
    differs from the v1.19.4 the design was written against by exactly one
    addition, `krun_add_virtiofs4`, which the shim does not call - so the ABI
    is the one PLAN assumes. See the question below about the delta.

- measurements at this commit:
  - `mkfs.ext4 -E lazy_itable_init=0 -O ^has_journal -m 0 -q -F` on a 1024 MiB
    image allocates **664 KiB** and flags all nine groups `ITABLE_ZEROED`, so
    the guest's `ext4lazyinit` has nothing to write after mount and the backing
    file stays sparse. `lazy_journal_init=0` was dropped: it is meaningless
    once the journal is gone. Under the smoke test the whole scratch disk costs
    ~900 KiB of the host filesystem while the VM runs, and all of it comes back
    on SIGKILL.
  - Guest virtio bus: 8 devices - balloon, rng, console, two fs (root and
    venv), blk, vsock, net.

- deviations from CONTRACTS.md: none. Departures from the WP03 brief:
  - The brief has the shim call `krun_add_vsock(ctx, 0)` directly. libkrun 1.19
    rejects that unless `krun_disable_implicit_vsock` is called first, so the
    shim calls it; the resulting configuration is what the brief describes.
  - The brief's smoke test uses `socat` for the unix-socket echo. socat is in
    the helper image but not on this host, so `helper-smoke.sh` uses python3.
    It also does not `shutdown(SHUT_WR)` - see the finding below.
  - `SPIKE_HELPER_IMAGE` lives in a new `spike/tasks/helper-common.sh` rather
    than `env-common.sh`, which is outside WP03's paths. Fold it in when the two
    are next touched together.

- findings other packages need:
  - **The read-only root has no writable mount points.** libkrun's init
    `mkdir`s `/dev`, `/proc` and `/sys` before mounting them, and cannot on a
    read-only share. busybox has no `/proc` or `/sys`; distroless images have
    none of the three. The shim injects the missing ones with
    `krun_fs_add_overlay_dir`. WP04: the same applies to every mount point
    flow-init needs before the overlayfs is up.
  - **hvc0 and the `krun-stdout` port share one host descriptor.** The kernel
    console and the workload's stdout are interleaved on fd 1 by construction,
    so `--debug`'s `kernel: ` prefix lands on both. Only the workload's stderr,
    which the reactor reads, is separate.
  - **libkrun's vsock unix proxy has no half-close.** EOF on the host side of
    `/sock/init.sock` (a `shutdown(SHUT_WR)`) is read as a full close and sends
    OP_RST to the guest, killing the connection before any reply. gRPC never
    half-closes, so WP05 is unaffected, but a test client must not.
  - **A stale `<id>/sock/init.sock` makes the helper fail with `EEXIST`.**
    `krun_add_vsock_port2` will not bind over an existing socket file. The
    runtime creates the directory fresh per connector, so this only bites a
    relaunch into a reused directory. The shim does not unlink it: the
    directory belongs to the caller.
  - The stub `flow-sandbox-resolver` exits 0 immediately. The shim spawns it
    and never waits, so WP02's real one may block forever as CONTRACTS says.

- questions for master:
  - Fedora's libkrun is 1.19.0, four patch releases behind the v1.19.4 in PLAN
    and CONTRACTS. Of the 24 commits between them, one touches Linux virtio-fs
    behavior: "virtio-fs: restore the original thread credentials, not euid/gid
    0". Everything else is macOS, CI, or build fixes, plus the unused
    `krun_add_virtiofs4`. Is 1.19.0 acceptable for the experiments, or should
    the Dockerfile build v1.19.4 from source (a builder stage; libkrunfw still
    from Fedora)? WP11's source read should probably settle it.
  - Where does WP05 write the policy JSON? CONTRACTS mounts `/init` but names
    only `flow-connector-init` and `image-inspect.json` in it, and PLAN's
    `podman run` passes `--policy <json>` with no mount of its own.
    `helper-smoke.sh` writes `<id>/init/policy.json` and passes
    `--policy /init/policy.json`. Confirm, or name another location.
  - flow-init's `--as-root-exec` (CONTRACTS "flow-init") has no route through
    the helper CLI, which WP07's root probes will need. Add a passthrough flag,
    or is `FLOW_SANDBOX_SPIKE_HELPER_ARGS` plus some other mechanism intended?
    Left alone rather than widening the CLI unilaterally.
  - PLAN.md, CONTRACTS.md and WP02.md carry uncommitted edits from the master
    thread's WP00 acceptance. This commit leaves them untouched; STATUS.md is
    committed because the brief requires appending to it.

### 2026-09-10 master: WP03 accepted; answers to its questions

- libkrun version: build v1.19.4 from source in a builder stage, libkrunfw
  from Fedora. Reasons, in order: 1.19.3's "virtiofs attribute caching" is
  on experiment 5's path and would make us measure a since-fixed cost as
  the design's cost; WP11 audits v1.19.4 source and the running binary must
  be what was audited; the 1.19.1 credentials fix is a no-op for a root
  helper but costs nothing. Folded into WP04 as step 0 since WP04 already
  touches the Dockerfile. libkrun's Makefile gates block and net devices
  behind `BLK=1 NET=1`; Fedora's package had them on, the build must too.
- Policy JSON: `<id>/init/policy.json`, passed as `--policy /init/policy.json`.
  Now in CONTRACTS.
- `--as-root-exec CMD` is added to the helper CLI as a verbatim passthrough
  to flow-init. WP04 implements it in the shim alongside flow-init, since it
  is the one building flow-init's argv. Now in CONTRACTS.
- Findings absorbed: `krun_disable_implicit_vsock` precedes
  `krun_add_vsock(ctx, 0)` (PLAN updated); mount points on the read-only
  root are injected as overlay dirs (WP04 brief); workload stdout shares fd 1
  with the kernel console (CONTRACTS); the vsock unix proxy treats
  half-close as close (CONTRACTS, test clients only); `<id>` is never reused
  because a stale `init.sock` is EEXIST (WP05 brief).
- Next: WP04.

### 2026-09-10 WP04: a connector image boots to its own connector-init, with a writable root, mounts and the image's user - after a one-line libkrun patch.

- shipped:
  - `spike/flow-init/` - the guest init, Rust, `x86_64-unknown-linux-musl`,
    static, `libc` only. `src/main.rs` is the sequence and ends in `execv`;
    `cli.rs` is the CONTRACTS CLI, `net.rs` the `ifreq`/`rtentry` ioctls,
    `root.rs` the mounts and `pivot_root`, `sys.rs` the syscall wrappers.
    `spike/flow-init/README.md`.
  - `spike/helper/Dockerfile` - step 0a: a builder stage that builds libkrun
    v1.19.4 from source (`make BLK=1 NET=1`), libkrunfw still Fedora's; the
    `libkrun` package is no longer installed. Also the real `/flow-init`
    instead of WP03's stub, and the libkrun patch below.
  - shim step 0b: `--as-root-exec CMD` in `cli.rs`, passed into the flow-init
    argv verbatim in `main.rs`'s `guest_cmd`. Nothing else in the shim.
  - `spike/tasks/flow-init-test.sh` - 30 assertions over 6 boots of
    derive-python. `--host` and reactor launch both pass.

- libkrun version, exactly: tag `v1.19.4`, commit
  `728df8125077d0db44265f6e997c72b81b65c015`, Makefile `FULL_VERSION=1.19.4`,
  installed as `/usr/lib64/libkrun.so.1.19.4` with SONAME `libkrun.so.1`,
  `libkrun.pc` `Version: 1.19.4`. Build deps beyond WP03's: `clang-devel`
  (bindgen's libclang) and `glibc-static` (libkrun's static init blob).
  libkrunfw is unchanged: `libkrunfw-5.5.0-1.fc43`, `libkrunfw.so.5.5.0`,
  guest kernel `6.12.91`. The library carries the one-line patch below.

- verification:
  ```
  $ spike/tasks/helper-build.sh && spike/tasks/flow-init-test.sh
  flow-init-test.sh: launching via reactor, guest ghcr.io/estuary/derive-python:dev

  ok    route: default via 192.0.2.1 on eth0
  ok    root: overlay is /
  ok    root: the image's user writes to the overlay
  ok    root: /etc stays root-owned under the dropped uid
  ok    mounts: venv virtiofs at /venv, read-only
  ok    mounts: /venv holds the share, not an empty mount point
  ok    mounts: /venv rejects a write
  ok    mounts: /dev/vda ext4 at /scratch, read-write
  ok    mounts: /scratch takes the workload's writes
  ok    mounts: df sees the scratch disk
  ok    mounts: devtmpfs, devpts and shm under /dev
  ok    user: the image's uid:gid, no supplementary groups
  ok    resolution: nameserver is the helper
  ok    resolution: /etc/hosts names localhost
  ok    network: IPv6 is disabled
  ok    env: TMPDIR is the scratch disk
  ok    env: UV_CACHE_DIR overrides the image's value
  ok    env: PATH is the image's (PATH=/usr/local/bin:/usr/local/sbin:...)
  ok    stderr: no line begins with a space
  ok    --run-as-root: the workload keeps uid 0
  ok    --run-as-root: a root-owned lower directory copies up
  ok    --as-root-exec: runs as guest root, after the mounts
  ok    --as-root-exec: the workload still drops to the image's user
  ok    default workload: connector-init's exit code reached the caller
  ok    default workload: connector-init got its image-inspect and vsock port
  ok    --venv-dax: /venv is mounted with dax
  ok    --venv-dax: the share reads through the DAX window
  ok    exit code: a workload that is not there exits 127
  ok    exit code: the failure is one line on stderr

  flow-init-test.sh: ok

  $ spike/tasks/flow-init-test.sh --host
  flow-init-test.sh: ok

  $ spike/tasks/helper-smoke.sh          # step 0a: WP03 still passes
  helper-smoke.sh: ok                    # all 10 checks, unchanged
  ```
  What the guest sees, from the probe boot:
  ```
  eth0  00000000  010200C0  0003  ...          (default via 192.0.2.1)
  eth0  000200C0  00000000  0001  ... FCFFFFFF (192.0.2.0/30 on link)
  proc /proc proc rw,nosuid,nodev,noexec,relatime
  sysfs /sys sysfs rw,nosuid,nodev,noexec,relatime
  cgroup2 /sys/fs/cgroup cgroup2 rw,nosuid,nodev,noexec,relatime
  overlay / overlay rw,relatime,lowerdir=/,upperdir=/dev/.flow/upper,workdir=/dev/.flow/work,uuid=on
  devtmpfs /dev devtmpfs rw,relatime,size=520920k,nr_inodes=130230,mode=755
  devpts /dev/pts devpts rw,relatime,mode=620,ptmxmode=666
  shm /dev/shm tmpfs rw,relatime
  venv /venv virtiofs ro,relatime            (with --venv-dax: ro,relatime,dax=always)
  /dev/vda /scratch ext4 rw,relatime
  uid=65534(nobody) gid=65534(nogroup) groups=65534(nogroup)
  ```
  Checks run by hand at this commit, not in the script:
  ```
  busybox as the guest      -> boots: /proc and /sys arrive as the shim's
                               injected overlay dirs, /etc is created, the
                               image has no `User` so the workload keeps uid 0
  /flow-init with no argv   -> "flow-init: --guest-ip is required; usage: ..."
  ```

- findings other packages need:
  - **flow-init must own its mount namespace, or every exit code is lost.**
    libkrun's init reports the workload's code with an ioctl on `/`, and only
    when `statfs("/")` returns virtiofs magic (`init.c`'s `set_exit_code`,
    v1.19.4 line 1124). flow-init shares that namespace, so a `pivot_root`
    there leaves init looking at the overlay: it skips the report silently and
    the VM exits **0** whatever the workload returned. `unshare(CLONE_NEWNS)`
    first, and the code comes back. This is invisible in a passing test - the
    first symptom was `--exec /nonexistent` exiting 0 instead of 127 - so it
    is worth WP05 and WP06 knowing it exists.
  - **libkrun's init marks the whole tree `MS_REC | MS_SHARED`** (`init.c`
    line 1401). Both `MS_MOVE` and `pivot_root` refuse a mount whose parent
    propagates, so flow-init makes the tree private right after unsharing.
    Without it: `mount /proc at /dev/.flow/root/proc: Invalid argument`.
  - **`/dev` cannot be `MS_MOVE`d into the new root**, because the tmpfs
    holding that root is inside `/dev` and MS_MOVE refuses a target under the
    mount being moved. devtmpfs is one kernel-wide instance, so flow-init
    mounts it again at `<newroot>/dev` and remakes `devpts` and `shm` over it.
    Departure from the WP04 brief's step 2, which said to move all three.
  - **The scratch disk needs a `chown`.** mkfs leaves the ext4 root owned by
    root and mode 0755, so with `TMPDIR=/scratch` the dropped workload could
    not write its own temp files. flow-init chowns the mount point to the
    image's uid:gid. Not in the brief or CONTRACTS; WP08's uv cache and WP07's
    probes both depend on it.
  - `/etc/hosts` gets `127.0.0.1 localhost` only. The guest's own name is the
    kernel default, which *is* `localhost`, and no `::1` line is written
    because IPv6 is off and a client that tried it first would eat a connect
    timeout on every localhost lookup.
  - Exit codes are a shell's, matching libkrun's init: 127 when the workload
    is not there, 126 when it cannot be run, 125 for flow-init's own failures,
    one line on stderr and never a leading space.
  - flow-init is **537 KiB** in the image (musl, stripped). The shim mmaps it
    and hands the pointer to libkrun for the VM's life, so that is per-VM host
    memory, shared page cache aside.

- deviations from CONTRACTS.md:
  - **libkrun is patched.** One line, in the builder stage, with the reasoning
    in the Dockerfile: `passthrough.rs`'s unknown-ioctl arm returns `ENOTTY`
    instead of `EOPNOTSUPP`. Without it there is no writable root at all -
    see the question below. CONTRACTS says libkrun "is v1.19.4 built from
    source"; it is now v1.19.4 plus that line.
  - Everything else in CONTRACTS "flow-init" is as written, with the two
    additions above (`unshare`, scratch `chown`) and `/dev` remounted rather
    than moved.
  - Departures from the WP04 brief:
    - `spike/tasks/helper-build.sh` gained one line,
      `--build-context flow-init=$SPIKE_DIR/flow-init`: the helper's build
      context is `spike/helper`, and `spike/flow-init` is outside it, so the
      Dockerfile reaches the sources with `COPY --from=flow-init`. podman
      4.9.3 supports it. The brief's "may touch" list did not include the
      script; nothing else in it changed.
    - The brief's verification asserts `etc-writable` under the dropped uid.
      As uid 65534 that is EACCES, exactly as it is under podman today, so the
      script asserts the parity (`etc-denied-as-user`), proves the overlay is
      writable where the image allows it (`/tmp`), and asserts `etc-writable`
      in the `--run-as-root` case, which is what proves copy-up of a
      root-owned lower directory.
    - Two cases the brief did not list, because the packages next in line
      depend on them: the default workload argv (no `--exec`, so
      connector-init's argv and exit code come back through libkrun) and
      `--venv-dax`.
  - `spike/helper/stubs/flow-init` is now dead: the Dockerfile copies the real
    binary. Left in place - it is outside WP04's paths.
  - `spike/helper/README.md` is now stale in two places (libkrun "1.19.0 ...
    used as-is rather than built from source", and the `stubs/` line naming
    flow-init). Also outside WP04's paths.

- questions for master:
  - **The libkrun patch. This is the decision this session needs reviewed.**
    Overlayfs over a virtiofs lower cannot copy anything up on this kernel:
    ```
    overlayfs: failed to retrieve lower fileattr (/etc, err=-95)
    touch: cannot touch '/etc/x': Operation not supported
    ```
    Every FUSE inode carries `S_NOATIME`, which is in overlayfs's
    `OVL_COPY_I_FLAGS_MASK`, so every copy-up first copies the lower's
    fileattr flags (`FS_IOC_GETFLAGS`). libkrun's passthrough fs answers any
    ioctl it does not recognize with `EOPNOTSUPP`
    (`src/devices/src/virtio/fs/linux/passthrough.rs:2192`), and overlayfs
    tolerates only `ENOTTY` and `EINVAL` there. Verified against the
    unpatched build first; `-o userxattr` and a tmpfs-on-tmpfs overlay were
    ruled out as causes (tmpfs supports `trusted.*` and `user.*` xattrs here,
    and a tmpfs lower copies up fine). The patch turns that arm into
    `ENOTTY` - which is what a filesystem with no inode flags, tmpfs
    included, returns - and everything works. Options as I see them:
    (a) carry the patch through the spike and open an upstream PR, which is
    where I have left it; (b) build the writable root some other way, e.g.
    tmpfs over each path that must be writable, seeded from the image, which
    is image-specific and loses the "podman-shaped root" property; (c) treat
    it as a gate failure for the design. If (a), production needs either an
    upstreamed fix or a vendored libkrun, which is a real cost to name in the
    report.
  - `--as-root-exec` ignores its command's exit status, per CONTRACTS
    ("run `CMD` ... then continues to the workload"). A failing probe is
    therefore silent unless it says so itself. Confirm that is wanted for
    WP07, or should a non-zero status fail the launch?
  - The guest hostname is the kernel default `localhost`. podman gives a
    container the short container id. If anything in the reactor or a
    connector cares, flow-init should `sethostname`; nothing in CONTRACTS
    asks for it, so it does not.

### 2026-09-10 master: WP04 accepted; the libkrun patch stays

- The patch: option (a). Carry it through the spike and open the upstream
  PR now, while it is one line. Rationale: a FUSE server that does not
  implement FUSE_IOCTL yields ENOTTY to the caller (the kernel maps the
  server's ENOSYS to ENOTTY), and the VFS itself returns ENOTTY for an
  unsupported ioctl. libkrun implementing the op and answering EOPNOTSUPP
  for unknown commands is the deviation; overlayfs is right to treat it as
  a real error. So the design (overlayfs writable root over a read-only
  virtiofs image) is sound and the bug is libkrun's. Production cost is
  honest and small: we already build libkrun from source in the helper
  image, so a one-line patch until upstream lands is a Dockerfile line, not
  a new capability. Named in PLAN's report section as an open problem with
  owner "us: upstream PR", and option (b) recorded as the fallback if
  upstream refuses.
- `--as-root-exec` keeps ignoring its command's exit status. WP07's probes
  report through their own JSON lines; a failing probe is visible there.
- Hostname stays `localhost`. Nothing in the spike reads it. Noted in
  CONTRACTS so a connector that does is a phase-2 `sethostname`, not a
  surprise.
- Housekeeping for WP06 (broad "small fixes" remit): delete the dead
  `spike/helper/stubs/flow-init`; fix `spike/helper/README.md` (libkrun is
  built from source and patched; stubs line). Recorded in WP06's brief.
- flow-init's `unshare(CLONE_NEWNS)` finding is now in CONTRACTS: without
  it every guest exit code is silently 0. WP05 and WP06 must not "fix"
  that away.
- Next: WP08 (experiment 5). It is the last big unknown before the plumbing
  packages, and it needs nothing from WP01/05/06.

### 2026-09-10 WP08: experiment 5 FAILS at 2.88x, and the diagnosis is unambiguous - virtiofs metadata, not CPU, not the layer, not DAX. A block device passes at 0.89x.

- shipped:
  - `spike/derived/Dockerfile` -> `localhost/derive-python-pandas:spike`:
    derive-python plus a pinned `pandas==3.0.5` venv at `/opt/venv`, installed
    with `--compile-bytecode`, `chmod -R a+rX`, and `USER nobody` restored so
    the derived image's `Config.User` is byte-identical to derive-python's.
  - `spike/derived/bench.py` - the measurement of the brief's step 2. Runs
    unchanged as the guest workload and under a plain `podman run`.
  - `spike/derived/cpu.py` - an IO-free loop, so "the guest is slow" can be
    separated from "virtiofs is slow".
  - `spike/derived/summarize.py` - either CSV into the report's table; groups
    by whichever of `cell`/`stage` the file carries.
  - `spike/derived/README.md`.
  - `spike/tasks/exp5-common.sh`, `exp5-build.sh` (image plus the identical
    venv exported to a host directory for the share cells), `exp5-run.sh` (the
    matrix), `exp5-diag.sh` (the breakdown).
  - `spike/report/exp5.md`, `spike/report/data/exp5-runs.csv` (80 runs),
    `spike/report/data/exp5-diag.csv` (75 runs).

- verification:
  ```
  $ spike/tasks/exp5-build.sh && spike/tasks/exp5-run.sh

  cell                import median   import p95    pass median    wall median   n
  primary                    1115.5       1133.7           40.0         2456.0  10
  primary-nothp              1117.2       1140.9           40.0         2466.5  10
  share                       930.4        960.9           38.2         2436.0  10
  share-nothp                 939.0        964.3           38.5         2434.0  10
  share-dax                   959.3        985.4           38.3         2492.5  10
  share-dax-nothp             966.0        994.9           38.8         2504.5  10
  baseline                    387.1        391.1           11.2          968.0  10
  baseline-root               390.1        392.2           11.2          970.0  10

  gate: primary 1115.5 ms / baseline 387.1 ms = 2.88x -> FAIL (limit 2.00x)

  $ spike/tasks/exp5-diag.sh

  stage               import median   import p95    pass median     cpu median   n
  primary-cold               1106.4       1130.8           39.3              -   5
  primary-warm                513.9        535.2           28.6              -   5
  primary-cold2               672.2        674.3           28.3              -   5
  primary-blk-cold            347.6        354.0           28.0              -   5
  primary-blk-warm            250.0        252.1           28.2              -   5
  primary-cpu                     -            -              -          557.4   5
  share-cold                  951.3        962.4           38.3              -   5
  share-warm                  473.8        531.1           35.1              -   5
  share-cold2                 508.2        571.3           27.4              -   5
  share-blk-cold              344.4        352.1           25.8              -   5
  share-blk-warm              241.7        247.3           25.6              -   5
  share-cpu                       -            -              -          556.6   5
  container-cold              389.6        394.0           11.3              -   5
  container-warm              219.3        220.0           10.9              -   5
  container-cpu                   -            -              -          558.4   5
  ```
  A first full matrix at the same commit, before `cpu.py` joined `bench.py` in
  the shares, agreed within 2% on every cell (primary 1115.7, share 952.0,
  share-dax 962.5, baseline 389.9). Both CSVs stamp every row with the commit.

  The reactor launch path was checked too, since the number has to be
  independent of it: `exp5-run.sh --reactor --runs 2 --cells share` imports in
  951.1 and 985.5 ms against the host-launched cell's 930.4 median / 960.9 p95,
  and pays its extra ~290 ms in the wall column (2728 vs 2436). The matrix and
  diag numbers above are host launches.

- the gate, and what it forces:
  - **The gate fails and no variant saves it.** Layer 2.88x, separate share
    2.40x, share+DAX 2.48x, all against a 2.00x limit.
  - **It is not the guest.** The CPU control is 557.4 / 556.6 ms in the guest
    against 558.4 ms in a container - identical. Nothing about running under
    libkrun costs measurable CPU at this workload.
  - **It is not the image layer, and not podman's overlayfs under virtiofs.**
    The `share` cell reads the venv from a plain host directory with no image,
    no podman overlay and no guest overlay on the read path, and still fails at
    2.40x. PLAN's "unpack the image to a plain host directory once per tag"
    option is thereby already measured, and it does not pass.
  - **It is not DAX.** `share-dax` is a few percent *worse* than `share` in
    both matrix runs. PLAN's stated fallback ("if the separate-share DAX
    variant passes, the venv becomes a separate share") does not apply, and by
    extension root DAX is unlikely to be worth libkrun 2.0 or
    `krun_set_kernel`: a 512 MiB window over a 144 MiB venv bought nothing.
  - **It is not THP.** Every `-nothp` cell is within noise of its pair.
  - **It is virtiofs metadata.** Warm virtiofs - every byte already in the
    guest's page cache - is still 2.2-2.3x the warm container. `import pandas`
    pulls in 594 modules, so it is thousands of path lookups and opens, each a
    round trip. Data bandwidth is not the cost; per-file work is.
  - **A block device passes at 0.89x.** The identical venv, copied inside the
    guest onto the ext4 scratch disk and read cold after `drop_caches`, imports
    in 347.6 ms - *faster than podman today* (389.6 ms). Same guest, same
    kernel, same bytes; only the transport differs.
  - So the redesign the failure forces is: **ship the dependency set as a
    read-only block image (ext4/erofs/squashfs) built once per tag and attached
    as a second virtio-blk device**, the way the scratch disk already is. That
    is a second `krun_add_disk3` in the shim, a mount in flow-init instead of
    the `venv` virtiofs, and an artifact builder. It is not a change to the
    design's shape, and it is not a libkrun problem.
  - Residual if only the venv moves: the interpreter and stdlib stay on the
    virtiofs root, which the `pass` column prices at ~15 ms per process
    (25.8-28.0 ms vs the container's 10.9). Cheap next to the import, but the
    same effect, and it caps what a venv-only fix wins.

- root DAX, concretely (the brief asks for this explicitly): the shim adds the
  root as an ordinary share, `krun_add_virtiofs3(ctx, "/dev/root", ...)`, so it
  *could* pass a DAX window. What it cannot do is make the guest mount with
  `dax`: the root is mounted from libkrun's own kernel command line, which
  reads `... rootfstype=virtiofs rw quiet no-kvmapf init=/init.krun ...` with
  no `rootflags=` and no libkrun 1.19 API to add one. Verified by `cat
  /proc/cmdline` in a running guest. Root DAX therefore needs libkrun 2.0's
  configurable cmdline or `krun_set_kernel` - and per the above, is probably
  not worth either.

- deviations from CONTRACTS.md: none. Departures from the WP08 brief:
  - **Step 4's no-overlay cell was not run.** The `share` cell already answers
    step 4's question ("if overlayfs-over-virtiofs is a suspect"): its read
    path has no overlayfs and no image, and it fails at 2.40x. Isolating the
    overlay could only redistribute the ~17% between "guest overlay" and "extra
    podman layer"; it cannot move the gate. It would also have meant adding a
    flag to `spike/flow-init/`, which is not in WP08's "may touch" list, while
    the brief's step 4 says to add one - so the two read against each other and
    I took the reading that spends nothing. Say the word and it is a
    `--no-overlay` flag plus one cell.
  - Two cells beyond the brief's list, both cheap and both load-bearing:
    `baseline-root` (`--user 0`), because the guest cells run as root and the
    plain baseline runs as the image's `nobody` - the 1% between them retires
    the question; and the whole of `exp5-diag.sh`, which is what turns "it
    failed" into "it failed for this reason, and here is the fix".
  - `--compile-bytecode` on both venv installs. Without it the read-only share
    cells would recompile 1848 files on every boot and the matrix would measure
    that instead of virtiofs. Named in the report.
  - `spike/tasks/exp5-common.sh` holds WP08's names (derived image, pandas pin,
    build-output dir). Matches the brief's `spike/tasks/exp5-*.sh` glob.
  - `spike/derived/summarize.py` and `cpu.py` live under `spike/derived/` (in
    the brief's paths) rather than `spike/tasks/`, whose glob only admits
    `exp5-*.sh`.

- findings other packages need:
  - **The shim opens `/init/flow-connector-init` even when `--exec` replaces
    the workload**, so every launch needs the file to exist. Harmless once
    known; it cost a debugging round here. WP09's benchmarks will hit it too.
  - **`printf | mapfile` cannot build a podman argv whose last element is a
    multi-line script** - mapfile splits it into one element per line, and the
    guest silently runs only the first. `exp5-diag.sh` builds its array
    directly. WP03/WP04's scripts carry the warning in a comment; this is the
    trap it warns about, sprung.
  - The `--as-root-exec` path was not needed: `--exec /bin/sh -c ...` with
    `--run-as-root` covers a root workload, and `drop_caches` works from there.
  - flow-init's scratch `chown` (WP04) is what lets the diag copy the venv onto
    `/scratch` at all. It earned its keep immediately.

- housekeeping seen, not touched: `$SPIKE_REACTOR_DIR` holds 68 stale
  `fs_*/` directories (2.5 MB) from WP03 and WP04 runs - all carry the `venv/`
  subdir those scripts create and predate this session. WP08's runs clean up
  after themselves. Worth a line in WP06's housekeeping remit, or an
  `env-check.sh` warning.

- questions for master:
  - **This is a gate failure, so PLAN's Decision section applies, and the
    branch it names is not the branch the data points at.** PLAN offers
    separate-share DAX, unpack-per-tag, and root DAX; the matrix retires all
    three. The option that passes - a per-tag read-only block image on a second
    virtio-blk - is not in PLAN. Does that become a WP08 follow-up (build one
    ext4 image, add `krun_add_disk3` and a flow-init mount, rerun the primary
    cell), or does the master thread want it in the report as the recommended
    redesign and sequenced after the remaining gates?
  - Related: should the *root* also move off virtiofs? The `pass` column says
    virtiofs costs ~15 ms per process for interpreter and stdlib, which is
    small here but is paid by every connector process, and it would grow with
    experiment 3's protocol traffic if anything else stats the root. Not
    measured; a whole-root block image is a much bigger change than a venv one
    (it gives up the "podman-shaped root" property WP04 established, and the
    overlayfs/ENOTTY patch exists to serve exactly that).
  - The 2x limit was chosen before any of this was measured. 2.88x is a clear
    miss, but for the record: the absolute miss is 728 ms of one-time import
    cost per connector start, against a 5-second boot budget (experiment 2)
    that the wall column says is currently ~2.5 s. If the block-device fix is
    expensive to build, is 2.88x actually disqualifying, or was 2x a proxy for
    "boot plus import stays under the budget"? Not arguing for moving the
    goalposts - asking whether the goalpost is the right one, once.
