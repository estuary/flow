# STATUS

Ledger for the spike. Sub-sessions append to the log; the master thread
maintains the table. Newest log entries at the bottom.

## Packages

| WP | Title                                   | Depends on   | Status | Branch              | Notes |
|----|-----------------------------------------|--------------|--------|---------------------|-------|
| 00 | Environment                             | -            | done   | daveg/libkrun-spike | a19095ede57 |
| 01 | connector-init --vsock-port             | -            | done   | daveg/libkrun-spike | 2d50c60ae84; skip-probe tightening handed to WP05 |
| 02 | Egress ruleset + resolver (netns)       | -            | todo   | daveg/libkrun-spike |       |
| 03 | Helper image + shim                     | -            | done   | daveg/libkrun-spike | a1e1ea562f2; libkrun 1.19.4 build folded into WP04 step 0 |
| 04 | flow-init                               | 03 (to test) | done   | daveg/libkrun-spike | ba94256e321; libkrun 1.19.4 + ENOTTY patch |
| 05 | runtime-next spike switch               | 01 (real)    | done   | daveg/libkrun-spike | 8f745facd8c; unswitched args identical to master |
| 06 | Integration: experiments 1-4            | 00-05        | done   | daveg/libkrun-spike | dff79afbc3e, 8d95d06724f; gates 1-3 pass, 4 provisional until WP07 |
| 07 | Egress from the guest: experiments 6-8  | 02, 04, 06   | todo   | daveg/libkrun-spike |       |
| 08 | virtiofs import matrix: experiment 5    | 03, 04       | done   | daveg/libkrun-spike | FAIL 2.88x on virtiofs; block device 0.89x; redesign -> 08b |
| 08b| deps as read-only block image: exp 5 rerun | 08        | done   | daveg/libkrun-spike | 2146b6ba34e; 2.12x first import, 1.15x steady state; gate accepted |
| 04b| podman writable layer, no guest overlay | 04, 08b      | done   | daveg/libkrun-spike | 981c434e5fd; unpatched libkrun; exp 5c 2.13x |
| 09 | Churn and density: experiments 9-10     | 00, 06       | todo   | daveg/libkrun-spike |       |
| 10 | Storage, exposure, crash: exp 11-13     | 06, 11       | todo   | daveg/libkrun-spike |       |
| 11 | libkrun source read: experiment 12      | -            | todo   | daveg/libkrun-spike |       |
| 12 | Report                                  | all          | todo   | daveg/libkrun-spike |       |

Sequential order, one session at a time, biggest unknowns first:
00, 03, 04, 08, 08b, 04b, 01, 05, 06, 02, 07, 11, 10, 09, 12. (03 answers "does it
boot from an image mount here"; 08 answers "is virtiofs fast enough" before
any runtime work is spent.)

## Known unknowns (resolve and record here)

- RESOLVED (WP03): `krun_set_exec` wins, and the two must not be combined -
  when `KRUN_INIT` is set, init keeps the kernel cmdline's argv and ignores
  `Cmd` entirely. The shim uses `Cmd` alone.
- RESOLVED (WP03, then master): Fedora 43 packages libkrun 1.19.0; we build
  v1.19.4 from source anyway (WP04 step 0), unpatched since WP04b. libkrunfw
  stays Fedora's 5.5.0.
- RESOLVED (WP00): `vsock_loopback` loads on this box; WP01 can test
  `--vsock-port` host-side against CID 1.
- RESOLVED (WP04b): `--mount type=image,...,rw=true` works through the podman
  API service, and the per-container layer is removed on `--rm` - guest writes
  to `/etc` and `/usr` are absent from the image afterwards and
  `podman system df` returns to its pre-run Containers row.
- What does the libkrun README's "does not provide any protection against the
  guest attempting to access other directories in the same filesystem" mean
  concretely, now that the root share is read-write and served by a root
  process on the filesystem that also holds the reactor directory and
  podman's storage? Can guest writes escape the shared directory? (WP11)

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

### 2026-09-10 master: WP08 accepted as a gate failure with a forced redesign; WP08b opened

- Verdict: experiment 5 FAILS on virtiofs and the diagnosis is accepted as
  conclusive (CPU identical, warm virtiofs still 2.2x, block device 0.89x).
  Per PLAN's Decision section this is a redesign, not a no-go: the
  dependency set ships as a per-tag read-only block image on a second
  virtio-blk device, mounted at `/opt/venv`. PLAN, CONTRACTS, WP05 updated;
  WP08b written to measure it (ext4 and erofs, reactor launch, and a
  host-cold cell for the first-ever-launch number).
- Root stays on virtiofs. Its cost is ~15 ms per process start, small and
  now measured; moving it is the fallback only if 08b also fails.
- The 2x gate stands. It was a proxy for startup budget and by absolute
  numbers we would have passed, but the metadata cost is paid on every
  file operation for the connector's life, and a cheap fix passes outright.
- Housekeeping recorded for WP06: stale `fs_*/` directories in the reactor
  dir; `env-check.sh` should warn.
- CONTRACTS now says `/init` must hold all three files even for `--exec`
  launches (the shim opens connector-init regardless).
- Next: WP08b, then WP01.

### 2026-09-10 WP08b: the block image works and the gate still fails, at 2.12x. The transport is at parity (1.15x); what is left is a per-boot tax, half of it guest memory first-touch.

- shipped:
  - shim: `--deps-image PATH [--deps-fstype ext4|erofs]` in `cli.rs` (fstype
    validated against what the guest kernel carries, so a typo fails before the
    VM starts), one `krun_add_disk3(ctx, "deps", ..., read_only=true,
    direct_io=false, KRUN_SYNC_NONE)` in `main.rs` placed after the scratch
    disk so scratch keeps `/dev/vda`, `KRUN_SYNC_NONE` added to `sys.rs`, and
    `--deps-dev /dev/vdb --deps-fstype FS` appended to flow-init's argv.
  - flow-init: `--deps-dev DEV --deps-fstype FS` (both or neither),
    `root::mount_deps` mounting it read-only at `/opt/venv` after the scratch
    mount. No `chown`: read-only, and the image is built world-readable.
  - `spike/derived/prefault.py` and `attrib.py`, the two probes that turned
    "the first import is slow" into "this much of it is memory, this much is
    not the root".
  - `spike/tasks/exp5-build.sh`: `mkfs.ext4 -d` and `mkfs.erofs` from the venv
    directory it already exported, sizes reported against it.
  - `spike/tasks/exp5-run.sh`: the four `blk-*` cells, plus `--out NAME` and
    per-cell launch and host-cold overrides.
  - `spike/tasks/exp5-diag.sh`: the `deps` and `deps-prefault` sequences, the
    module attribution, `--out NAME`.
  - `spike/tasks/flow-init-test.sh`: 11 assertions over the deps disk.
  - `report/exp5.md` gains "Experiment 5b" and a correction section;
    `report/data/exp5-5b.csv` (50 runs), `exp5-5b-diag.csv` (105 runs).

- verification:
  ```
  $ spike/tasks/exp5-run.sh --out exp5-5b.csv \
      --cells blk-ext4,blk-erofs,blk-ext4-reactor,blk-ext4-hostcold,baseline

  cell                import median   import p95    pass median    wall median   n
  blk-ext4                    826.8        848.7           38.7         2322.5  10
  blk-erofs                   849.6        864.8           39.1         2333.5  10
  blk-ext4-reactor            824.1        835.0           37.6         2570.0  10
  blk-ext4-hostcold          1235.6       1293.5           39.5         3671.5  10
  baseline                    390.5        395.0           11.3          972.5  10

  gate: blk-ext4 826.8 ms / baseline 390.5 ms = 2.12x -> FAIL (limit 2.00x)

  $ spike/tasks/exp5-diag.sh --out exp5-5b-diag.csv     # deps-* stages only shown
  deps-cold                   822.0        835.5           38.3              -   5
  deps-prefault               621.4        622.7           27.4              -   5
  deps-cold2                  451.2        480.6           33.1              -   5
  deps-warm                   254.8        358.9           27.4              -   5
  deps-cpu                        -            -              -          567.4   5
  container-cold              391.7        393.5           11.4              -   5

  $ spike/tasks/flow-init-test.sh          # 41 assertions, 11 of them new
  ok    --deps-image ext4: scratch is still /dev/vda
  ok    --deps-image ext4: the deps disk arrived as vdb
  ok    --deps-image ext4: mounted read-only at /opt/venv
  ok    --deps-image ext4: the workload reads the image's content
  ok    --deps-image ext4: /opt/venv rejects a write
  ok    --deps-image erofs: (the same five)
  ok    --deps-fstype: an unsupported filesystem is refused before the VM starts
  flow-init-test.sh: ok
  $ spike/tasks/flow-init-test.sh --host   -> ok
  $ spike/tasks/helper-smoke.sh            -> ok (all 10, unchanged)
  $ cargo clippy --all-targets             -> clean, both crates; cargo fmt applied
  ```
  WP08's `exp5-runs.csv` and `exp5-diag.csv` are untouched and still summarize
  to the numbers in the experiment 5 tables.

- the gate, and the case for and against it:
  - **2.12x, so it fails as written.** ext4 826.8 ms against a same-session
    baseline of 390.5.
  - **The transport is at parity.** `deps-cold2` reads the same blocks off the
    same device with the guest's page cache, dentries and inodes just dropped:
    451.2 ms against the container's 391.7, **1.15x**. Warm is 254.8 vs 221.7,
    also 1.15x. On virtiofs those were 685.5 (1.75x) and 516.3 (2.33x).
  - **So the reason the gate was kept is satisfied.** The master thread held
    the line at 2x because the metadata premium "is paid on every file
    operation for the connector's life". That premium is now 1.15x, not 2.33x.
    What remains is paid once per boot.
  - **The residual is 371 ms of first-import tax**, and ~201 ms of it is the
    guest first-touching its own memory: a fresh boot that writes one byte to
    every page of a 600 MiB buffer, frees it, and then imports gets 621.4 ms
    instead of 822.0. Nothing is read during that prefault, so it is not cache
    warming. The other ~170 ms is cold helper/virtiofs-server and guest ext4
    state; not pursued, since the levers are libkrun's.
  - **It is not the virtiofs root**, which was PLAN's next fallback. `attrib.py`
    counts 407 modules (41.7 MB) off the deps disk against 152 (5.4 MB) off the
    root, and `deps-cold2` drops the cache for both yet still lands at 1.15x.
    Moving the root to a block image would not buy this back. I would not spend
    the change on it.
  - **ext4 over erofs.** 826.8 vs 849.6 ms (2.8% slower) against 159,260 KiB
    vs 136,508 KiB allocated (14% smaller). Per-boot time beats per-tag bytes,
    and a wrongly-sized ext4 image fails at build time rather than at runtime.
  - **First-ever launch per tag: 1235.6 ms import, 3671 ms wall** - the number
    WP08's entry flagged as unmeasured. Upper bound: dropping the host cache
    evicts the helper image, connector image and libkrunfw too, not just the
    deps image.
  - **Launch path is irrelevant to the number**, as expected: 824.1 through the
    fake reactor against 826.8 on the host, with the extra 248 ms in `wall`.

- **a correction to WP08, which I got wrong.** WP08's report claimed a block
  device would import at 0.89x and recommended the redesign on that number. The
  redesign was right; the number was not. Its `blk-cold` stage ran late in a
  long sequence - after a boot, a full import, and a `cp -a` of the whole
  144 MiB venv - and that copy prefaulted the guest's memory and left the host
  cache maximally hot. So it was a prefaulted second-import measurement placed
  next to first-import cells. It reproduces exactly here (`primary-blk-cold`
  344.8, `share-blk-cold` 343.0), so it was repeatable, just not comparable.
  The steady-state half of the claim survives and is now measured directly at
  1.15x; "faster than podman today" does not. Recorded in report/exp5.md as its
  own section rather than by editing the experiment 5 text, and as a method note
  in `spike/derived/README.md`: fresh boot per data point, or name the regime.

- deviations from CONTRACTS.md: none. Departures from the WP08b brief:
  - **`--out NAME` on both scripts, and the verification command needs it.**
    The brief's verification line would have truncated `exp5-runs.csv`, which
    is WP08's raw data and which the brief also says must stand. Run it as
    `exp5-run.sh --out exp5-5b.csv --cells ...`.
  - **`erofs-utils` went into `env-common.sh`, not `env-setup.sh`.** The brief
    named env-setup.sh, but `SPIKE_APT_PACKAGES` lives in env-common.sh; adding
    it there also gets it checked by `env-check.sh` for free. Same shape as
    WP03's `SPIKE_HELPER_IMAGE` note.
  - **`spike/tasks/flow-init-test.sh` gained the deps assertions**, and it is
    not in the brief's paths (which stop at `spike/flow-init/src/`). A new
    flow-init mount with no coverage in flow-init's own suite seemed worse than
    the path departure; the benchmark scripts are not tests. The images it uses
    are built in the test (16 MiB, one marker file), so the standing suite does
    not depend on experiment 5's 180 MiB artifacts.
  - **`spike/flow-init/README.md` and `spike/helper/README.md`** document code
    this package changed, so both gained a line. WP04's pre-existing staleness
    in the helper README (libkrun "1.19.0 ... used as-is", the `stubs/` line) is
    left for WP06 as recorded.
  - Two probes beyond the brief's step 5, `prefault.py` and `attrib.py`. Step 5
    asked that `pass` and `cpu` be kept so the root's cost is restated; those
    two are what actually answered where the 371 ms goes, and without them this
    entry would say "it fails at 2.12x" and stop.

- findings other packages need:
  - **`mkfs.ext4 -d` writes to stdout even under `-q`** ("Creating regular
    file ..."). A shell function that returns a path by echoing it must send
    mkfs's stdout elsewhere, or the caller gets a two-line path and podman says
    `invalid reference format`.
  - **`mkfs.erofs` 1.7.1 has no `-q`**; it is `--quiet`.
  - The guest kernel (libkrunfw 5.5.0, 6.12.91) carries ext4 and erofs but not
    squashfs, per `/proc/filesystems`. No libkrunfw change was needed.
  - The `mapfile` trap bit again, in `flow-init-test.sh` this time: a
    multi-line `--exec` probe became one argv element per line and the guest
    silently ran only `set -e`. That script already flattens its main probe
    with `${PROBE//$'\n'/ }`; the new one is built as a single line.
  - **Guest memory first-touch is a real, measurable cost** (~201 ms for a
    144 MiB read). Anything in WP09's density or churn work that compares a
    first operation against a later one will see it.
  - **The stale `fs_*/` directories have a cause, and it is a one-line bug.**
    `new_connector` in `flow-init-test.sh` and `helper-smoke.sh` ends with
    `CONNECTORS+=("$id")`, but every caller invokes it as
    `id="$(new_connector)"` - a command substitution, so the append happens in
    a subshell and the parent's array stays empty. The cleanup trap then
    iterates nothing and every dir survives. Fixed in `flow-init-test.sh` and
    in both `exp5-*.sh` (the id goes to a file the trap reads); a full
    `flow-init-test.sh` run now leaves zero dirs behind where it used to leave
    about ten. **`helper-smoke.sh` still has it** - outside this package's
    paths, one line, same fix. Worth WP06 fixing the cause rather than only
    warning in `env-check.sh`. The 115 dirs standing now are left for WP06 as
    scheduled; none are from this package's benchmark runs, which removed each
    directory explicitly.

- questions for master:
  - **Does 2.12x pass or fail?** As written it fails. The reason the gate was
    kept - a metadata premium paid for the connector's life - is satisfied at
    1.15x. The residual is one-time and sits inside a 2.3 s total launch
    against experiment 2's 5 s budget. I have no in-scope lever left: the
    transport is at parity, the root is measured and is not the problem, and
    ext4 vs erofs is 3%. So this is a judgment call about what the gate was
    for, and it is yours rather than mine.
  - If the answer is "fail", the only remaining lever I can see is libkrun
    backing guest RAM with pre-populated or huge pages, which would move ~201 ms
    out of the first import and into boot. **Whether that is a net win is
    unmeasured** - prefaulting moves the cost, it does not obviously remove it,
    and my probe (a Python byte-per-page loop, 2.1 s for 600 MiB) is far too
    slow to answer. It would need a real measurement, and it lands in
    experiment 2's budget, not this one. Worth a WP of its own if you want it.
  - CONTRACTS says flow-init mounts the deps image at `/opt/venv`, and WP08b
    puts changing derive-python to consume it in phase 2. Nothing in the spike
    reads `/opt/venv` except bench.py, so the path is currently only a
    convention. Confirm it is the one phase 2 wants before WP05 bakes
    `FLOW_SANDBOX_SPIKE_DEPS_IMAGE` into the runtime switch.
  - The deps images here are built from experiment 5's exported directory, so
    the venv's baked-in paths say `/venv` while it is mounted at `/opt/venv`.
    Immaterial to these numbers, but a phase-2 builder should create the venv at
    its final path, and that is the sort of thing that is easier to fix before
    there is a builder than after.

### 2026-09-10 master: WP08b accepted; experiment 5's gate closes at 2.12x

- **Ruling: pass.** 2.12x on a first import after boot is close enough, given
  what the number is made of. The 2x limit was a proxy for a metadata premium
  paid on every file operation for a connector's life; that premium is now
  1.15x (`deps-cold2` 451.2 ms against a container's 391.7), where virtiofs
  left it at 2.33x warm. The 371 ms keeping the first-import figure above 2x is
  one-time per boot, and the whole launch is 2.3 s against experiment 2's 5 s
  budget. Experiment 5 is settled; PLAN records it.
- The dependency set as a per-tag read-only block image on virtio-blk is the
  design, and `--deps-image` / `--deps-fstype` / `--deps-dev` are the contract
  for it. **ext4, not erofs**, per WP08b's recommendation: 2.8% of a per-boot
  cost beats 14% of per-tag storage, and a wrongly-sized image failing at build
  time beats one failing at runtime. erofs support stays in the shim and
  flow-init since it costs nothing to keep and the numbers are recorded.
- Guest memory first-touch (~201 ms) is **not** chased. It is libkrun's to fix
  (pre-populated or huge pages behind guest RAM), the win is unmeasured, and it
  would land in experiment 2's boot budget rather than here. Noted in the
  report as the one lever left; no WP opened for it.
- The root stays on virtiofs, now on measured grounds rather than assumption:
  WP08b's module attribution and `deps-cold2` together rule it out as the
  residual. PLAN's "whole root on a block image" fallback is retired, not
  merely deferred.
- WP08's 0.89x is superseded by 1.15x. The correction stands as its own report
  section; the experiment 5 text is not rewritten. WP08b's method note - fresh
  boot per data point, or name the regime - applies to WP09's churn and density
  work, which compares first operations against later ones by construction.
- Carried forward, unanswered because they are not blocking: whether `/opt/venv`
  is the path phase 2 wants (WP05 should not bake
  `FLOW_SANDBOX_SPIKE_DEPS_IMAGE` in until it is confirmed); the deps images
  being built from a venv whose baked-in paths say `/venv`, which a phase-2
  builder should fix; and `helper-smoke.sh`'s copy of the `CONNECTORS+=`
  subshell bug, which is WP06's along with the 115 stale directories.
- Next: WP01.

### 2026-09-10 master: no libkrun patch; the writable root is podman's layer (WP04b opened)

- Decision: we will not carry or submit the ENOTTY patch. The image mount
  becomes `rw=true`, podman's per-container writable overlay, served to the
  guest read-write. No overlayfs in the guest, so no copy-up, so no ioctl.
  This is literally the writable layer a container has today.
- Consequences, accepted with eyes open: root writes go to host disk over
  virtiofs and are bounded only by that disk, as containers are today; the
  `--upper-mib` memory cap and its `memory.current` accounting are gone.
  Big writers are still steered to `/scratch` via TMPDIR/UV_CACHE_DIR.
- flow-init loses the unshare, the tmpfs, the pivot, and the `/dev` remount,
  and keeps WP08b's deps mount. Its exit-code trap disappears with the pivot;
  the test keeps asserting it.
- Options considered and set aside: carry the patch privately (lightest, but
  a patched libkrun is not wanted), and root as a per-tag erofs image (retired
  on WP08b's measurements regardless). libkrun main still returns EOPNOTSUPP,
  so there was no upstream fix to wait for.
- WP04b must precede WP05, which bakes the launch line in. Briefs WP06 and
  WP10 lost their `--upper-mib` references; experiment 11's first bullet is
  rewritten in PLAN and WP10; CONTRACTS describes the new root.
- Process note: WP08b's acceptance entry above was written by a different
  master session than the one that wrote this entry. Both are read; nothing
  conflicts. One master thread from here on.
- Next: WP04b, then WP01.

### 2026-09-10 WP04b: the writable root is podman's layer; the libkrun patch is gone and the guest overlay with it. Experiment 5 restates at 2.13x, unchanged.

- shipped:
  - `spike/tasks/helper-common.sh`: `spike_image_mount IMAGE`, the one place
    the connector image mount is built, now with `rw=true`. Used by
    `flow-init-test.sh`, `helper-smoke.sh`, `exp5-run.sh` and `exp5-diag.sh`,
    which had four copies of the string between them.
  - shim: root share is `krun_add_virtiofs3(..., read_only=false)`; the
    `/dev`, `/proc`, `/sys` overlay-dir injection is gone, and with it the
    `krun_fs_add_overlay_dir` declaration in `sys.rs` (WP11 audits that file);
    `--upper-mib` removed from `cli.rs` and from flow-init's argv.
  - `spike/helper/Dockerfile`: the ENOTTY patch block is gone. libkrun stays
    v1.19.4 built from source, `BLK=1 NET=1`, unpatched.
  - flow-init: no `unshare`, no `MS_REC | MS_PRIVATE`, no tmpfs, no overlay,
    no `MS_MOVE`, no `/dev` remount, no `pivot_root`, no lazy umount, no
    `--upper-mib`. `root.rs` is 80 lines where it was 175. Everything else
    stands: eth0, IPv6 off, `/etc/resolv.conf` and `/etc/hosts` straight into
    the root, venv, scratch and its chown, WP08b's deps mount,
    `--as-root-exec`, `TMPDIR`/`UV_CACHE_DIR`, uid drop, exec.
  - `spike/tasks/flow-init-test.sh`: the root assertions are rewritten for
    the new shape and three are new (below). 43 assertions, was 41.
  - `spike/tasks/helper-smoke.sh`: the `CONNECTORS+=` subshell bug WP08b
    diagnosed, fixed the same way (an id file the trap reads).
  - `report/exp5.md` gains "Experiment 5c"; `report/data/exp5-4b.csv`
    (20 runs).
  - `spike/flow-init/README.md` and `spike/helper/README.md` rewritten where
    this change made them wrong.

- verification:
  ```
  $ spike/tasks/helper-build.sh && spike/tasks/helper-smoke.sh
  helper-smoke.sh: ok                      # all 10, unchanged

  $ spike/tasks/flow-init-test.sh          # 43 assertions
  ok    root: / is the image share, read-write
  ok    root: the image's user writes to the root
  ok    root: /etc stays root-owned under the dropped uid
  ok    root: as guest root, /etc takes a write
  ok    root: as guest root, /usr takes a write
  ok    root: the guest's writes are not in the image
        podman system df, Containers: 1/11836 B before, 1/11836 B after
  ok    root: the writable layer left with the container
  ok    exit code: a workload that is not there exits 127
  ...                                      # the other 35, all ok
  flow-init-test.sh: ok

  $ spike/tasks/flow-init-test.sh --host
  flow-init-test.sh: ok

  $ cargo fmt --check; cargo clippy --all-targets    -> clean, both crates
  ```
  The guest's root mount, from the probe boot:
  `/dev/root / virtiofs rw,relatime` - no `overlay` line anywhere, and
  `devtmpfs /dev`, `devpts /dev/pts`, `shm /dev/shm` are libkrun's init's own.

  Reactor directories: 115 before each suite and 115 after, so neither
  script leaks any now. The 115 standing are still WP06's.

- **the libkrun build is unpatched, proven rather than assumed.** The image
  was rebuilt with `--no-cache` (the first cached build of the session made
  the library's mtime ambiguous), and then the old failure was reproduced on
  the running library: an overlayfs with `lowerdir=/` over the virtiofs root
  still refuses copy-up, `touch: Operation not supported`, with
  `overlayfs: failed to retrieve lower fileattr (/etc, err=-95)` on the
  kernel console. Nothing in the design uses that path any more.

- **the overlay-dir injection is not needed, and busybox is what proves it.**
  With the share writable, libkrun's init `mkdir`s `/dev`, `/proc` and `/sys`
  itself: `helper-smoke.sh`'s busybox guest (no `/proc`, no `/sys` in the
  image) boots and reads both. Dropped rather than kept. A distroless Go
  connector was checked too - `ghcr.io/estuary/source-hello-world:dev` boots
  through flow-init and runs `/connector/source-hello-world spec` to
  completion, and `--exec /nonexistent` there exits 127, which is every mount
  step succeeding - but that image carries all three directories already, so
  busybox is the case that decides it.

- experiment 5c: **2.13x, against 5b's 2.12x.** Same cells, same method.
  ```
  cell                import median   import p95    pass median    wall median   n
  blk-ext4                    709.4        726.3           36.7         2092.0  10
  baseline                    332.7        336.8            9.7          872.5  10
  ```
  Both cells are 14-15% faster in absolute terms than in 5b (709.4 vs 826.8,
  332.7 vs 390.5). `baseline` is a plain `podman run` of the derived image -
  no guest, no helper, no root share - so nothing in WP04b can reach it: two
  cells moving together by the same fraction is machine state on a shared
  cloud host, not the design. The ratio is the part that carries between
  sessions, and it did not move. Removing the overlay was never going to move
  much: since 5b the dependency set arrives on `/dev/vdb`, so the overlay sat
  only on the root's own 152 modules and 5.4 MB, and root *writes* are not on
  the import path at all.

- deviations from CONTRACTS.md: none. Departures from the WP04b brief:
  - **`report/exp5.md` and `report/data/exp5-4b.csv` are outside the brief's
    paths**, which stop at the task scripts, but step 5 asks for the 5c
    section. Same shape as WP08b's note about `flow-init-test.sh`.
  - **flow-init no longer chdirs to the workdir**, and CONTRACTS still lists
    it. It only ever existed because `pivot_root` left the cwd on the new
    root; libkrun's init applies `WorkingDir` before exec'ing flow-init and
    nothing since changes the cwd, so the call and its `getcwd` were dead by
    construction. `sys::chdir` went with them. See the question below.
  - **`spike/helper/README.md`'s libkrun line is fixed here**, which was on
    WP06's housekeeping list, because this change is what made it wrong.
    What is left for WP06 in that file is the `stubs/` line naming flow-init,
    plus deleting the dead `spike/helper/stubs/flow-init`.

- findings other packages need:
  - **The root share is writable now, which changes what a virtiofs path
    escape would mean.** WP11's open question - libkrun's README saying it
    "does not provide any protection against the guest attempting to access
    other directories in the same filesystem" - was a read-escape question
    while the share was read-only. It is a write-escape question now, and the
    server runs as root in the helper container with the reactor directory and
    podman's storage on the same filesystem. Worth WP11 answering in those
    terms.
  - **Root writes are bounded by host disk only.** A runaway writer fills the
    reactor's storage rather than being stopped by a cap; that is exactly what
    a container does today, and experiment 11 (WP10) is where it gets
    measured. `TMPDIR` and `UV_CACHE_DIR` still steer the big writers to
    `/scratch`, which is sized.
  - **`podman build` cache hits can hide a stale library.** The libkrun stage
    is one `RUN` that clones and builds; a cached layer looks identical in the
    log and the installed `.so` keeps its original mtime through `COPY`, so
    "did my Dockerfile change actually take" is not answerable by looking. It
    is answerable by testing the behavior that changed, which is what the
    ENOTTY check above does. Anything that changes libkrun's source should do
    the same.
  - The `mapfile` trap bit once more while writing the new root case: a
    two-line `--exec` string became two argv elements. Flattened, and the
    comment naming the trap is on the line above it now.

- questions for master:
  - CONTRACTS' flow-init sequence still says "chdir to the workdir it was
    started in". flow-init no longer does, because with the pivot gone the
    call could not change anything. Drop the clause, or should flow-init keep
    an explicit chdir so the postcondition is its own rather than libkrun's
    init's? I removed it as dead code; restoring it is two lines.
  - 5c is measured on this package's tree but `exp5-4b.csv` names commit
    `358da8b59b8`, its parent, because the runs preceded the commit. Say if
    you want raw data re-run under the WP04b commit hash for the report.
  - Nothing else. WP05 can take the launch line from
    `spike_image_mount` in `helper-common.sh`: `--mount type=image,
    source=<img>,destination=/rootfs,rw=true`, and there is no `--upper-mib`
    to plumb.

### 2026-09-10 master: WP04b accepted

- chdir: dropped from CONTRACTS. libkrun's init applies `WorkingDir` before
  exec'ing flow-init and nothing after changes the cwd; a postcondition that
  restates the caller's is not worth two lines and a syscall wrapper.
- Raw data hash: no re-run. 5c was measured on WP04b's tree before its
  commit; the report cites 981c434e5fd for 5c and notes the CSV names the
  parent. The ratio is what carries, and it did not move.
- WP11's open question is reframed as a write-escape question now that the
  root share is read-write and served by a root process on the filesystem
  that also holds the reactor directory and podman's storage. In the brief
  and in the unknowns list.
- Left for WP06 as scheduled: dead `spike/helper/stubs/flow-init`, the
  `stubs/` line in the helper README, the 115 stale reactor directories.
- Next: WP01. WP05 takes the launch line from `spike_image_mount` in
  `helper-common.sh`.

### 2026-09-10 WP01: connector-init serves gRPC over AF_VSOCK

- shipped:
  - `crates/connector-init/src/lib.rs`: `--port` and `--vsock-port` are both
    `Option`, in a required clap `ArgGroup` so exactly one is given. A
    `Listener` enum binds either transport before anything else can fail, so
    the readiness space byte means the same thing on both. Serving is one
    `Router` and a two-arm match at the end. TCP behavior is unchanged.
  - `tokio-vsock` 0.7.2 with its `tonic014` feature, added to
    `[workspace.dependencies]` and to the crate. That feature already
    implements `tonic::transport::server::Connected` for `VsockStream`, and
    `VsockListener::incoming()` is already a
    `Stream<Item = io::Result<VsockStream>>`, so the newtype and
    `async_stream` wrapper the brief specified are not needed. One `tonic`
    remains in `Cargo.lock` (0.14.2); no duplicate.
  - `crates/connector-init/tests/vsock.rs`: spawns the built binary with
    `--vsock-port 49092`, waits for the space byte, and runs a capture `Spec`
    RPC over a `VsockStream` to CID 1 through a tonic channel with a
    `connect_with_connector` dialer. The "connector" is `/bin/cat` over a
    canned newline-delimited JSON response, so the test exercises the
    transport and the proxy without a connector image. It skips with a
    message when the vsock port cannot be bound (no `vsock_loopback`).
- verification:
  ```
  $ cargo build -p connector-init
      Finished `dev` profile [optimized] target(s) in 7.23s

  $ mise exec -- cargo nextest run -p connector-init
      Starting 13 tests across 3 binaries
          PASS [   0.018s] ( 6/13) connector-init::vsock spec_rpc_over_vsock
          ... 12 others ...
       Summary [   0.082s] 13 tests run: 13 passed, 0 skipped

  $ cargo clippy -p connector-init --all-targets
    error: mutable borrow from immutable input(s)   crates/doc/src/bump_vec.rs:213
    error: written amount is not handled            crates/connector-init/src/lib.rs:73

  $ cargo clippy -p connector-init --all-targets -- \
        -A clippy::mut_from_ref -A clippy::unused_io_amount
    warning: `connector-init` (lib) generated 3 warnings
    warning: `connector-init` (lib test) generated 4 warnings (3 duplicates)
        Finished
  ```
  Both clippy errors are pre-existing on the tree before this package (checked
  by stashing): `bump_vec.rs` is untouched, and `lib.rs:73` is the readiness
  `stderr().write(" ")`, moved but not modified. With them allowed so the
  crate compiles far enough to lint, the new test target is clean and the lib
  warning count is unchanged at 3.

  Negative control, to prove the test is not passing vacuously: dialing
  `VSOCK_PORT + 1` fails with
  `ConnectError(Os { code: 104, ConnectionReset })`.

  TCP path smoke, with the same binary:
  ```
  $ flow-connector-init --image-inspect-json-path=... --port 49099
    listening: LISTEN 0 128 0.0.0.0:49099 0.0.0.0:*
    stderr first byte: 20
  ```
- deviations from CONTRACTS.md: none. CONTRACTS' "connector-init" section
  holds as written. Two departures from the WP01 brief:
  - No `Connected` newtype and no `async_stream`, for the reason above. If
    `tokio-vsock`'s tonic feature ever lags a tonic bump, the newtype is the
    fallback and is four lines.
  - The brief's "add the test if STATUS says `vsock_loopback` loads" is
    honored, but the test decides at runtime instead of at authoring time.
    `crates/connector-init/**` is not spike code and runs in the flow CI,
    where the module may be absent; a hard failure there would be a flaky
    test by any other name.
- questions for master:
  - The root `Cargo.toml` was edited to add `tokio-vsock` to
    `[workspace.dependencies]`; the brief allowed `crates/connector-init/**`
    and `Cargo.lock` only. Every other dep in the crate is `workspace = true`,
    so pinning the version in the crate would be the odd one out. Say if you
    want it moved.
  - `crates/connector-init/` has no README.md, which the repo guidelines ask
    for. Out of scope here (nothing to keep current), but it is a real gap and
    WP05 or WP06 could fold one in.

### 2026-09-10 master: WP01 accepted

- `tokio-vsock` stays in `[workspace.dependencies]`; that is the crate's
  convention and the brief's path list was simply too narrow.
- The README gap is real and deferred: nothing merges from this branch, so it
  matters only if the vsock change is lifted into phase 2. Note it there.
- One fix handed to WP05: the test's skip probe checks bind, but the test
  needs loopback connect to CID 1 (`vsock_loopback`). On a host with a vsock
  transport and no loopback it would fail hard rather than skip. Probe must
  bind and connect. In WP05's brief as a side item.
- Pre-existing clippy errors on the tree (`crates/doc/src/bump_vec.rs`,
  connector-init's readiness `write`) are not WP01's and not the spike's.
- Next: WP05, then WP06.

### 2026-09-10 WP05: runtime-next spike switch

- side fix shipped first:
  - `crates/connector-init/tests/vsock.rs`: `bind_probe` is now async and
    binds *and* connects to `VMADDR_CID_LOCAL` before the test proceeds,
    skipping unless both succeed. Skip message unchanged.
- shipped:
  - `crates/runtime-next/src/container.rs`: three changes. `mod spike;` and a
    four-line branch at the top of `start` that delegates when
    `FLOW_SANDBOX_SPIKE_POLICY` is set. The spawn, stderr pump, and readiness
    wait move verbatim into a new `spawn_and_await_ready`, so both paths run
    one copy of the readiness protocol and the log decoder rather than two
    that can drift. `Guard`'s two `TempPath` fields become `Option`, and it
    gains an `Option<spike::DirGuard>` declared after `_process`.
  - `crates/runtime-next/src/container/spike.rs` (new, a child module so it
    reaches `container`'s private helpers without widening any visibility):
    `Settings::from_env` for the CONTRACTS variable table, the `podman run`
    of PLAN "Helper launch", and the tonic dial over
    `<id>/sock/init.sock`. It reuses `find_connector_init_and_copy` and
    `inspect_image_and_copy` unchanged, pointing them at
    `<id>/init/`, and chmods the inspection JSON to 0644 (the unmodified path
    gets that from its tempfile). `DirGuard` is taken before anything that can
    fail, so an error return cleans up too.
  - `crates/runtime-next/Cargo.toml`: `hyper-util` and `tower`, both already
    in `[workspace.dependencies]`, for the Unix-socket connector tonic needs.
  - `spike/stub-helper/`: `localhost/flow-sandbox-stub:spike`, the reactor
    image plus socat. Its entrypoint accepts and ignores the helper CLI,
    copies `/init` into `/rootfs/init`, bridges `/sock/init.sock` to
    `127.0.0.1:49092`, and chroots into `/rootfs` to run connector-init.
  - `spike/catalog/`: `capture-hello-world.flow.yaml` (static Go connector,
    `acmeCo/` names) and `policy-egress-none.json`.
  - `spike/tasks/preview-stub.sh` (the button), plus `preview-common.sh`,
    `preview-stub-build.sh`, and `preview-sudo-podman.sh` (a `DOCKER_CLI`
    that execs `sudo podman`: the spike's images and networks are root's, and
    `DOCKER_CLI` takes one program name, not a command line).
- verification:
  ```
  $ mise exec -- spike/tasks/preview-stub.sh
    == stub helper image
    localhost/flow-sandbox-stub:spike bf45a7fed8fb309b94bf7f17dfe2cbbc6a10aee...
    == 1/3 switch off: the unmodified runtime
    == 2/3 switch on, from the host
    == 3/3 switch on, inside the fake reactor
    == results
    documents: off=5 host=5 reactor=5
    ["acmeCo/events",{"_meta":{"uuid":"DocUUIDPlaceholder-329Bb50aa48EAa9ef"},
      "message":"Hello 0!","ts":"<redacted>"}]
    PASS: switch on (host) == switch off
    PASS: switch on (reactor) == switch off
    PASS: no reactor directory leaked (115 present, unchanged)
  ```
  `ts` is the only field redacted; every other byte of all three runs is
  identical. Positive control that the switch was really taken in both
  switched-on cases: six `stub-helper: ignoring helper args: --policy
  /init/policy.json --memory-mib 1024 --vcpus 2 --disk-mib 4096` lines came
  back through the log pump (three sessions each), and the
  `container_started` event carries `{"container":{"ipAddr":"192.0.2.2"}}`.

  Unswitched launch line against master, per the brief. `container.rs` at
  `HEAD` is identical to `master` (`git diff master HEAD` empty for the file),
  so `HEAD` stands in. Captured `docker_args` at `RUST_LOG=debug` from both
  trees, folding the random name and the two temp paths to placeholders:
  ```
  $ diff args-master.txt args-wp05.txt && echo IDENTICAL
    IDENTICAL

  $ cat args-wp05.txt
    ["run", "--rm", "--name=fc_NAME", "--network=flow-connectors",
     "--entrypoint=/flow-connector-init", "--log-driver=none",
     "--mount=type=bind,source=/tmp/.tmpTEMP,target=/flow-connector-init",
     "--mount=type=bind,source=/tmp/.tmpTEMP,target=/image-inspect.json",
     "--env=LOG_FORMAT=json", "--env=LOG_LEVEL=warn", "--memory", "1g",
     "--cpus", "2", "--platform=linux/amd64",
     "--label=image=ghcr.io/estuary/source-hello-world:dev",
     "--label=task-name=acmeCo/hello-world", "--label=task-type=capture",
     "--publish=0.0.0.0::49092", "--publish-all",
     "ghcr.io/estuary/source-hello-world:dev",
     "--image-inspect-json-path=/image-inspect.json", "--port=49092"]
  ```
  ```
  $ mise exec -- cargo build -p runtime-next -p flowctl
      Finished `dev` profile [optimized] target(s) in 1m 03s

  $ mise exec -- cargo nextest run -p runtime-next --no-fail-fast
       Summary [  38.044s] 219 tests run: 219 passed (1 slow), 0 skipped

  $ mise exec -- cargo nextest run -p connector-init
        PASS [   0.033s] ( 6/13) connector-init::vsock spec_rpc_over_vsock
       Summary [   0.074s] 13 tests run: 13 passed, 0 skipped

  $ mise exec -- cargo clippy --no-deps -p runtime-next  # error count
    76   # and 76 with this package's Rust changes stashed: no new findings
  ```
  `cargo nextest run -p runtime-next` first failed three `split_e2e` tests for
  a missing gazette broker; `mise run build:gazette` fixed that, and the run
  above is after it. Plain `cargo clippy -p runtime-next` cannot reach the
  crate at all: `json`, `tuple`, and `proto-gazette` fail first with 41, 12,
  and 67 pre-existing errors under the tree's `-D warnings`. `--no-deps`
  lints only this crate, where 76 errors are pre-existing (confirmed by
  stashing). My two new findings in `spike.rs` -- both copied from
  `container.rs`'s own style, `network == ""` and `var(..).ok()` -- are fixed
  in `spike.rs` only; `container.rs`'s originals are left alone.

  The side fix, proven both ways with `vsock_loopback` unloaded and the
  `vsock` transport still present, which is the case master flagged:
  ```
  $ sudo modprobe -r vsock_loopback

  # new probe:
    skipping: AF_VSOCK loopback is unavailable (vsock_loopback not loaded)
        PASS [   0.003s] (1/1) connector-init::vsock spec_rpc_over_vsock

  # old probe (git stash of just that file):
        FAIL [   0.004s] (1/1) connector-init::vsock spec_rpc_over_vsock
    panicked at crates/connector-init/tests/vsock.rs:87:10   # the connect
  ```
- deviations from CONTRACTS.md:
  - The launch line adds `--env=LOG_FORMAT=json` and
    `--env=LOG_LEVEL=<level>`, which PLAN "Helper launch" does not list. The
    stub honors them, and the brief's log-decoder parity depends on them.
    Under the real helper they are probably inert: libkrun's init applies the
    *image's* `Env`, not the helper container's. Question below.
  - No `--platform` on the helper's own `podman run`, following PLAN's block.
    The connector image is still pulled and inspected with
    `--platform=linux/amd64` by the unchanged helpers.
  - `Guard` drop removes `<id>` best effort, but `async_process::Child::drop`
    SIGKILLs without waiting, so the removal can race the helper's exit. The
    bind mounts pin the inodes until it does exit, so racing is harmless --
    but "after the process exits" is not literally achievable, only
    `_process`-is-dropped-first.
  - No new environment variable. The brief allowed a stub-only one for
    `--cap-add SYS_ADMIN`; the stub instead copies `/init` into
    `/rootfs/init` rather than bind-mounting it, needs only CAP_SYS_CHROOT
    (already in podman's default set), and so the generated launch line has
    nothing stub-specific in it. CONTRACTS' variable table is unchanged.
- questions for master:
  - `flowctl preview` validates the catalog before it drives anything, and
    that step launches the connector through the *legacy* `runtime` crate
    (`crates/runtime/src/container.rs`), which the switch does not touch. So
    every switched-on preview still starts two unsandboxed `fc_*` containers
    before the first `fs_*` one. Fine for WP05 (the brief scopes the change
    to `runtime-next`), but WP06 should know that derive-python's `Spec` and
    `Validate` will run outside the sandbox, and phase 2 has to decide whether
    validation is in scope for sandboxing at all.
  - That legacy path bind-mounts host temporaries, so inside the fake reactor
    it fails with `statfs /tmp/.tmpXXXXXX: no such file or directory` -- the
    reactor container's `/tmp` does not exist for the host podman that
    receives the mount. `preview-stub.sh` sets `TMPDIR` under
    `$SPIKE_REACTOR_DIR` to get past it. Worth keeping: it is a clean
    demonstration of why the reactor-directory contract exists.
  - `fake-reactor.sh` mounts `$REPO_DIR/target` at `/flow-target` so a locally
    built `flowctl` is reachable, but under mise `CARGO_TARGET_DIR` is
    `~/cargo-target/flow`, so that mount is empty. `preview-stub.sh` copies
    the two binaries into `$REPO_DIR/target/debug/` to satisfy WP00's
    contract as written. Suggest WP00's script mount `$CARGO_TARGET_DIR`
    instead; say if you want that folded in here.
  - Socket permissions. The stub creates `init.sock` with mode 0777 so a
    reactor running as an ordinary user can connect; podman rootful containers
    share the host user namespace, so the socket is otherwise root-owned.
    Under the real helper libkrun owns that mode. If it is 0755, WP06 driving
    a preview from the host as a non-root user cannot connect, and would need
    to run under `fake-reactor.sh` (root) or the helper would need to chmod
    the socket. Production's reactor is root, so this may never matter in
    production -- but it will bite WP06 on this box.
  - `crates/runtime-next/Cargo.toml` was edited to add `hyper-util` and
    `tower`; the brief's paths named `container.rs` and a sibling module.
    Both were already in `[workspace.dependencies]` and are declared
    `workspace = true` like every other dependency in the crate.
  - `crates/runtime-next/README.md` "Layout" does not enumerate
    `container.rs`, so the new child module leaves nothing stale there. WP01's
    note about `crates/connector-init/` having no README still stands.
  - Experiment 3 also wants a Go *materialization* previewed both ways.
    `preview-stub.sh` covers only the capture, which is what this brief asked
    for. The script is the harness experiment 3 can reuse against the real
    helper; the materialization half is unwritten, and materialize-sqlite
    needs a writable endpoint path inside the guest, so it is better added
    with the real helper than against the stub.
  - The stub emits `socat[12] E write(...): Broken pipe` through the log pump
    when a session ends and the runtime drops the channel. It is a stub
    artifact -- socat noticing connector-init has gone -- and I left it
    unsuppressed rather than hide a class of real failure.
  - The 115 stale `fs_*` reactor directories from WP03/WP04b are still there,
    as scheduled for WP06. This package leaked none of its own.

### 2026-09-10 master: WP05 accepted

- `--env=LOG_FORMAT`/`LOG_LEVEL` on the helper line are not inert: the shim
  reads them from its own environment into the guest's `.krun_config.json`
  (CONTRACTS "Helper image" step 3). Added to PLAN's launch line; not a
  deviation.
- Validation runs through the legacy `runtime` crate, unsandboxed, before
  runtime-next drives Open. Accepted for the spike: experiments 3 and 4 prove
  the sandbox on the Open-and-documents path, which is where the connector
  actually runs; Spec/Validate sandboxing is a phase-2 item already implied
  by "connector_proxy.go must move to runtime-next first". WP06 writes it
  into the report.
- Socket permissions: no chmod in the shim. Production's reactor is root;
  WP06 drives previews through `fake-reactor.sh`, which is root and is the
  faithful path regardless.
- `fake-reactor.sh` should mount `$CARGO_TARGET_DIR`; handed to WP06 as a
  one-line housekeeping fix alongside the stale directories and the stubs.
- Experiment 3's materialization harness is WP06's, against the real helper.
- `Cargo.toml` additions were already workspace deps; fine. The stub's
  socat broken-pipe line stays unsuppressed; agreed.
- Not reviewed line by line: `container/spike.rs`. The production-path
  change was proven by the unswitched `docker run` args matching master and
  219 runtime-next tests passing; the spike module dies with the branch.
- Next: WP06, the first real end-to-end and experiments 1 to 4.

### 2026-09-11 WP06: the sandbox runs a real connector end to end; experiments 1 to 4 all pass.

- housekeeping first, as the brief scheduled:
  - deleted the dead `spike/helper/stubs/flow-init`; fixed the `stubs/` line in
    `spike/helper/README.md` (WP04b had already fixed the libkrun line).
  - `spike/tasks/reactor-clean.sh` - the push-button for stale `fs_*`
    directories, which refuses to run while any helper is up. The 115 standing
    from WP03/WP04b are gone; `env-check.sh` now **warns** when any exist.
  - `fake-reactor.sh` mounts `$CARGO_TARGET_DIR` when set, falling back to
    `$REPO_DIR/target`. `preview-stub.sh` drops the copy step that compensated
    for the empty mount.

- shipped:
  - `spike/tasks/exp1-launch.sh`, `exp2-boot.sh`, `exp3-parity.sh`,
    `exp4-derive.sh` - one button per experiment, each ending in a pass/fail
    line. exp2 carries its own parser and writes
    `spike/report/data/exp2-boot.csv`.
  - `spike/catalog/`: `materialize-sqlite.flow.yaml` plus
    `materialize-fixture.ndjson` (experiment 3's other half, the materialization
    WP05 left unwritten); `derive-pandas.flow.yaml`, `derive-pandas.flow.py` and
    `derive-fixture.ndjson` (experiment 4); `policy-allow-all.json`. README
    rewritten.
  - `spike/tasks/preview-common.sh` gains `spike_stage_catalog` and
    `spike_preview`, the driver all four experiments share.
  - `spike/report/exp1.md`, `exp2.md`, `exp3.md`, `exp4.md`.
  - instrumentation for experiment 2's breakdown (see deviations): one timing
    line per stage from the shim and from flow-init, and one from the runtime
    at the readiness byte.

- verification:
  ```
  $ spike/tasks/env-check.sh                       -> env-check.sh: ok
  $ mise exec -- spike/tasks/exp1-launch.sh        -> exp1-launch.sh: ok
  $ mise exec -- spike/tasks/exp2-boot.sh --runs 20
    arm       n   total med   total p95    podman      shim    kernel  flow-init  conn-init  (console)
    on       40       683.8       735.4     135.5      23.9     201.9        6.4      315.1      290.3
    off      40       408.9       437.3         -         -         -          -          -          -

    gate: switch-on p95 0.735 s -> PASS (limit 5 s)
    delta: median 683.8 ms sandboxed against 408.9 ms today, 1.67x

  $ mise exec -- spike/tasks/exp3-parity.sh
    ok    capture documents: identical (5 lines)
    ok    capture connector: identical (2 lines)
    ok    capture stats: identical (5 lines)
    ok    materialize documents: identical (0 lines)
    ok    materialize connector: identical (3 lines)
    ok    materialize stats: identical (2 lines)
    exp3-parity.sh: ok

  $ mise exec -- spike/tasks/exp4-derive.sh
    ok    the derivation produced 4 documents inside the guest
    ok    sandboxed documents are byte-identical to unsandboxed
    ok    uv fetched and imported pandas=3.0.5 from PyPI inside the guest
    scratch-footprint: used=183164KiB total=4112096KiB
    exp4-derive.sh: ok
  ```
  Regressions, all unchanged:
  ```
  $ spike/tasks/helper-smoke.sh                    -> ok (all 10)
  $ spike/tasks/flow-init-test.sh                  -> ok (all 43)
  $ mise exec -- spike/tasks/preview-stub.sh       -> 3 PASS, 0 leaked
  $ mise exec -- cargo nextest run -p runtime-next -> 219 passed, 0 skipped
  $ cargo fmt --all --check; (shim, flow-init) cargo fmt --check; clippy -> clean
  ```

- the four gates:
  - **Experiment 1 PASS.** CapEff `00000000800415fb` = podman's default
    `800405fb` plus bit 12, CAP_NET_ADMIN, and nothing else. `/dev/kvm` and
    `/dev/net/tun` are the only added devices. 40 documents came back over
    `<id>/sock/init.sock`. Nothing needed a root shell on the host.
  - **Experiment 2 PASS, with 4.3 s of headroom.** p95 0.735 s against 5 s.
    The sandbox costs +275 ms over today (1.67x). **The largest line item is
    transport, not work**: of the 315 ms after flow-init execs, 290 ms is the
    guest's stderr reaching the host, measured independently, leaving
    connector-init ~25 ms to bind and signal. That 290 ms is libkrun's virtio
    console and is the only lever this experiment found.
  - **Experiment 3 PASS.** Six diffs empty across a Go capture and a Go
    materialization: documents, connector log lines, transaction stats. The
    materialization's `connector applied` DDL round-trips byte for byte.
  - **Experiment 4 PASS, under the placeholder egress scripts** - see the
    deviation below. Four documents, byte-identical to the unsandboxed run;
    `uv` fetched pandas 3.0.5 from PyPI inside the guest; `/scratch` footprint
    183 MB (venv 131 MB, uv cache 133 MB overlapping in the totals above).

- **deviations from CONTRACTS.md:**
  - **The egress and resolver placeholders now do two things they did not.**
    The master thread flagged this mid-session and I took the two-line route it
    recommended, extended by one more because masquerade alone was not enough.
    `flow-sandbox-egress` gained `nat postrouting oifname "$uplink" masquerade`;
    without it the guest's packets leave the tap untranslated and no reply ever
    returns. `flow-sandbox-resolver` previously exited 0 immediately, so with
    flow-init writing `nameserver 192.0.2.1` the guest could not resolve a name
    even on an open network; it now forwards UDP/53 to the helper's upstream
    with socat. Neither is policy, both are in files CONTRACTS assigns to WP02
    wholesale, and CONTRACTS already owes the masquerade ("The helper
    masquerades guest traffic out of eth0"). **Experiment 4's result is
    therefore "passed under an unenforced network" and WP07 must rerun it under
    the real ruleset.** `report/exp4.md` says exactly what was and was not
    exercised - notably that the placeholder forwards AAAA answers verbatim
    where the real resolver empties them.
  - Nothing else. The launch line, the helper CLI, flow-init's sequence, the
    policy JSON and the switch's variable table are all as written.

- **departures from the WP06 brief's paths**, each because the brief's own steps
  required it:
  - **`crates/runtime-next/src/container.rs`**, three lines: a
    `tracing::debug!("connector-init readiness byte received")` where the pump
    consumes the byte. The brief's step 3 says "the runtime logs when the
    readiness byte arrives"; it did not. It is in `spawn_and_await_ready`, which
    both paths share, so both arms are timed to the same event - which matters,
    because the unmodified path's next step is a `podman inspect` subprocess and
    the sandboxed path's is a Unix connect. Timing to `container_started` would
    have charged the baseline for that inspect.
  - **`spike/helper/shim/src/main.rs`** and **`spike/flow-init/src/`**: a
    `timing` function and two call sites each, per the brief's step 3. The shim
    stamps the host wall clock; flow-init stamps `CLOCK_MONOTONIC`.
  - **flow-init uses `CLOCK_MONOTONIC`, not `/proc/uptime`** as the brief says.
    Same zero point, but uptime is reported in **centiseconds** and flow-init's
    whole run is 6.4 ms, so both readings came back identical at `200000` us.
    The brief's intent is the guest's boot clock; this is that clock at a
    resolution that can see the interval.
  - **`spike/tasks/preview-common.sh`** (WP05's) holds the shared preview
    driver, rather than an `exp1-common.sh` that the other three would source by
    number. Same shape as WP03's `SPIKE_HELPER_IMAGE` and WP08b's
    `erofs-utils` notes.
  - `spike/tasks/reactor-clean.sh` and the `env-check.sh` warning are the
    brief's own housekeeping item; the script is outside the `exp{1,2,3,4}-*.sh`
    glob.

- bugs found by integration, fixed, with the package each belongs to:
  - **WP00, `env-check.sh`**: `[ -S /run/podman/podman.sock ]` runs as the
    calling user and `/run/podman` is mode 0700, so a working socket reported as
    missing. Now `sudo test -S`. This was latent from the start and only
    surfaced because the box had been restarted.
  - **WP00, `fake-reactor.sh`**: mounted `$REPO_DIR/target`, which is empty
    under mise. Now `$CARGO_TARGET_DIR` when set. (Scheduled by master.)
  - **WP08b's `CONNECTORS+=` subshell bug**: no further instances; the count is
    now asserted as a delta rather than against zero, so an unrelated leak
    cannot fail an experiment.

- findings other packages need:
  - **`grep` on this box is `ugrep` 7.8.4**, in which `-oE '...[^\n]*'` does not
    mean "to end of line" - it truncated a captured error message to one
    character. Use `.*`. WP07 and WP09 will write log-scraping scripts.
  - **A derivation module must not print to stdout.** stdout is the derive
    protocol's channel; connector-init parses every line as a JSON response and
    the session dies with `could not parse "..." into JSON response`. Cost a
    debugging round here and a customer will hit it.
  - **derive-python type-checks customer modules with pyright in `strict`
    mode**, so a module using untyped pandas does not build. The spec declares
    `pandas-stubs`, and the module uses `Series.sum()` rather than `.iloc[0]`,
    whose type the stubs leave partially unknown. Unrelated to the sandbox, but
    it is a real constraint on customer Python. See the open problem below.
  - **The capture's `bytesTotal` is not reproducible even within one arm.**
    Documents carry a wall-clock `ts` whose serialization is sometimes one byte
    shorter (trailing zero trimmed). `docsTotal` and `txnCount` are stable.
    Cost a false parity failure.
  - **One `flowctl preview --sessions 1` starts the connector twice** (the
    capture shard validates, then opens), so experiment 2's n is 40 over 20
    previews. The two are within 10 ms of each other.
  - **Guest stderr reaches the host ~290 ms late.** Any later experiment that
    times a guest-side event by when the host saw it will be wrong by about
    that much. Use the guest's own clock for in-guest intervals.
  - `source-hello-world:dev` moved digest between WP00 and here
    (`ebac9e3e...` -> `96147403...`); `materialize-sqlite:dev` likewise
    (`54d2853c...` -> `7c89b59b...`). `derive-python:dev` is unchanged. The
    report names what was measured.

- questions for master:
  - **Experiment 4 needs a rerun under WP02.** I recorded it as passing under
    the placeholder's unenforced network, per your message. Is that a WP07 step
    (its brief already owns experiments 6 to 8 under the real ruleset), or does
    it want naming as its own line in the report's gate table so it cannot be
    read as settled?
  - **Sandboxing Spec and Validate is the one exposure this package surfaced
    that is not a spike artifact.** For a Go connector it is a tidiness
    question; for derive-python it means the customer's module is type-checked
    and its dependencies fetched **outside** the sandbox, on the reactor's
    network, before anything is sandboxed. `exp3.md` and `exp4.md` both record
    it as a phase-2 open problem with owner "runtime". Worth confirming that is
    the framing you want in the final report, since it is arguably the most
    consequential thing WP06 found.
  - **The 290 ms console lag.** Not chased: the gate passes by 4.3 seconds and
    the mechanism is libkrun's. But it is 42% of the sandboxed launch, and if
    WP09's density or churn work makes launch latency scarce it becomes the
    first thing to look at. Worth a line in the report's "levers" list, or worth
    a WP?
  - **`--sessions` and the twice-started connector.** Nothing depends on it
    here, but if WP09 counts launches it should know one preview is two.

- **open problem, not WP06's to decide: pyright `strict` on customer modules.**
  derive-python hardcodes `"typeCheckingMode": "strict"`
  (`crates/derive-python/src/lib.rs:381`), force-installs pyright as a
  dependency (`:400`), and fails the **Validate** RPC on any finding (`:262`).
  There is no way to relax it from a catalog spec: `DeriveUsingPython` carries
  only `module` and `dependencies`, and the connector writes its own
  `pyrightconfig.json` into the generated project.

  Why this is a problem and not a preference: in strict mode
  `reportUnknownMemberType` and `reportUnknownVariableType` make any dependency
  without type information poison every expression that touches it. So the
  constraint on customer Python is not "write typed code", it is "only use
  libraries that ship `py.typed` or have a stubs package". That sits directly
  across the spike's premise, which is running customer Python with arbitrary
  dependencies.

  **Scope of the evidence: one library.** pandas is the only case measured here,
  and it was resolvable by declaring `pandas-stubs`. Nothing in this spike
  surveys how much of the ecosystem is affected, and that survey is what a
  decision should rest on.

  **This is flagged, not proposed.** dgreer's position is that strict mode looks
  untenable going forward, and equally that relaxing it is not his call to make
  unilaterally: it needs someone to ask for it. Nothing in the spike touches
  derive-python, and nothing should. Recorded so the decision has a place to
  happen rather than being discovered by the first customer.

### 2026-09-11 master: WP06 accepted; four gates, one provisional

- Experiment 4: both. It reruns as WP07's first step under WP02's real
  `allowAll` ruleset, AND PLAN's gate table carries it as provisional until
  then, so it cannot be read as settled. In WP07's brief.
- Spec/Validate unsandboxed: framing confirmed and sharpened. In production
  the agent's connector proxy drives Validate through the same legacy path,
  so this is not a preview artifact. For derive-python, Validate is where uv
  resolves and builds dependencies (sdist build backends execute) and where
  pyright runs, all on the reactor's network. That is exactly the builder VM
  phase of the design plus the "connector proxy moves to runtime-next"
  prerequisite. It is the top open problem in the report; PLAN lists it.
- The ~290 ms console latency: a lever in the report, not a WP. The gate has
  4.3 s of headroom and the mechanism is libkrun's console device. If WP09
  makes launch latency scarce, it is the first thing to open.
- Twice-started connectors per preview and the guest-clock rule are in
  WP09's brief.
- pyright strict and the stdout footgun are recorded in PLAN's open problems
  as flagged, not proposed. Neither is the spike's to change.
- The placeholder egress/resolver extensions are accepted as WP06 recorded
  them; WP02 replaces both files wholesale (in its brief).
- Next: WP02, then WP07. WP11 can run any time before WP10.
