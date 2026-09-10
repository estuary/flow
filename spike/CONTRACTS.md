# CONTRACTS

Interfaces between work packages. A package may rely on anything here without
reading another package's code. Change a contract only by recording the
deviation in STATUS.md for the master thread to propagate.

## Paths and names

- Reactor directory stand-in: `$SPIKE_REACTOR_DIR`, default
  `/var/tmp/flow-spike/reactor`. Must be ext4 or xfs (O_TMPFILE). It plays
  `/mnt/local/reactor`: the reactor writes per-connector state under it, and
  it is mounted at the SAME path inside the fake reactor.
- Per-connector directory: `$SPIKE_REACTOR_DIR/<id>/` with subdirs
  `init/` (holds `flow-connector-init`, `image-inspect.json`, and
  `policy.json`), `sock/` (helper creates `init.sock` here), `scratch/`
  (helper opens an O_TMPFILE here). The runtime creates it before launch and
  removes it after exit. `<id>` is unique per launch and never reused: a
  stale `sock/init.sock` makes libkrun fail with EEXIST, and the shim does
  not unlink files it does not own.
- Helper image: `localhost/flow-sandbox-helper:spike`.
- Stub helper image (WP05 testing only): `localhost/flow-sandbox-stub:spike`.
- Derived image (WP08): `localhost/derive-python-pandas:spike`.
- Podman networks: `flow-connectors` (default bridge settings, as production);
  `spike-testnet2` with `--subnet 198.51.100.0/24`.
- nginx: container `spike-nginx` on `spike-testnet2` at `198.51.100.10`,
  TLS on 443 with a self-signed cert, plain HTTP on 80, serving a 1 KiB file
  at `/` and `/probe`.
- Helper container names: `fs_<16 hex>` (today's connectors are `fc_<hex>`).

## Tap network

- Subnet `192.0.2.0/30` (TEST-NET-1). Helper `192.0.2.1`, guest `192.0.2.2`.
- Guest MAC `02:f1:0f:00:00:02`. Tap name inside the helper: `tap0`.
- Helper uplink interface: `eth0` (podman's veth). The helper masquerades
  guest traffic out of eth0.

## Helper CLI (WP03 provides, WP05 invokes)

```
flow-sandbox-helper --policy PATH --memory-mib N --vcpus N --disk-mib N \
    --upper-mib N [--deps-image PATH [--deps-fstype ext4|erofs]] \
    [--venv-dax] [--thp-disable] [--run-as-root] [--debug] \
    [--as-root-exec CMD] [--no-flow-init] [--exec ARGV...]
```
`PATH` is `/init/policy.json`; the runtime writes it next to connector-init.

- Mounts it expects, all provided by the caller's `podman run`:
  `/rootfs` (connector image via `--mount type=image`), `/init` (dir with
  `flow-connector-init`, `image-inspect.json`, `policy.json`; the shim opens
  `flow-connector-init` even when `--exec` replaces the workload, so all
  three must exist for every launch), `/venv` (dir, may be empty; spike
  tooling only: probe and bench scripts under `/venv/spike/`), `/sock`
  (dir), `/scratch-backing` (dir), and optionally `/deps.img` (a read-only
  bind of the per-tag dependency disk image, see `--deps-image`).
- `--deps-image PATH`: attach PATH as a read-only virtio-blk device after the
  scratch disk (so scratch stays `/dev/vda`, deps is `/dev/vdb`). flow-init
  mounts it read-only at `/opt/venv`. `--deps-fstype` defaults to `ext4`.
  This is the production transport for the dependency set; WP08 measured
  virtiofs at 2.4x-2.9x for it and a block device at 0.9x.
- Default workload argv:
  `/flow-connector-init --image-inspect-json-path=/image-inspect.json --vsock-port=49092`
- `--exec ARGV...` replaces the workload argv. flow-init still does all of its
  setup first. Used for probes and benchmarks.
- `--no-flow-init` runs `--exec` argv directly as libkrun's workload with no
  flow-init (WP03 smoke tests before flow-init exists).
- `--run-as-root` makes flow-init skip the uid/gid drop (probes that need
  guest root, e.g. sysctl, drop_caches).
- `--as-root-exec CMD` is passed to flow-init verbatim: it runs `CMD` via
  `/bin/sh -c` as guest root after mounts and before the uid drop, then
  continues to the workload.
- `--venv-dax` gives the `venv` virtiofs a DAX window (512 MiB) and tells
  flow-init to mount it with `dax`.
- `--thp-disable` calls `prctl(PR_SET_THP_DISABLE)` before starting the VM.
  Default is THP left alone.
- `--debug` tees the guest kernel console to stderr with prefix `kernel: `
  and raises libkrun's log level. Without it, kernel console goes to the
  helper's stdout only.
- stdout: guest kernel console and the workload's stdout, interleaved (they
  share one host descriptor by libkrun's construction). stderr: the
  workload's stderr, byte for byte (connector-init logs and its readiness
  byte), plus helper diagnostics that never begin with a space.
- Exit code: the guest workload's exit code. libkrun's init uses 125 (init
  setup failed), 126 (not executable), 127 (not found). The helper exits 2
  for its own failures before the VM starts.
- vsock: guest port 49092 is mapped to `/sock/init.sock` with libkrun
  listening on the socket (`krun_add_vsock_port2(..., listen=true)`). The
  reactor connects; the guest never initiates. libkrun's unix proxy treats a
  host-side half-close (`shutdown(SHUT_WR)`) as a full close and resets the
  guest connection. gRPC never half-closes; hand-written test clients must
  not either.
- libkrun in the helper image is v1.19.4 built from source (`BLK=1 NET=1`)
  plus one patch carried in `spike/helper/Dockerfile`: the virtiofs
  passthrough answers unknown ioctls with ENOTTY instead of EOPNOTSUPP.
  Without it overlayfs cannot copy up from a virtiofs lower (it needs
  FS_IOC_GETFLAGS to fail with ENOTTY or EINVAL), so there is no writable
  root. libkrunfw is Fedora's package. The shim carries its own ABI
  declarations.
- Egress: the helper execs `flow-sandbox-egress` and `flow-sandbox-resolver`
  (below) before starting the VM. With `egress: none` no resolver runs.

## Egress binaries (WP02 provides, WP03 invokes)

```
flow-sandbox-egress --policy PATH --tap tap0 --uplink eth0 \
    --guest-ip 192.0.2.2 --helper-ip 192.0.2.1
```
Loads the complete nftables ruleset for the policy (table `inet flow_sandbox`)
via `nft -f -`, replacing any prior table of that name. Exits 0 when loaded.
Must be idempotent. The set that the resolver feeds is
`inet flow_sandbox resolved` (type ipv4_addr, flags timeout).

```
flow-sandbox-resolver --policy PATH --listen 192.0.2.1:53 --upstream IP:PORT
```
UDP DNS forwarder. Runs until killed. Behavior:
- A answers: every address is checked against the baseline denylist and the
  helper's own subnets. Any hit makes the whole answer fail (REFUSED); no set
  element is added. Otherwise each address is added to `resolved` with
  timeout `clamp(ttl, ttlFloorSecs, ttlCapSecs)` BEFORE the answer is sent,
  and the answer's TTLs are rewritten to the same value.
- AAAA queries answer NOERROR with an empty answer section.
- Everything else (CNAME chains, TXT, SRV, ...) is forwarded unchanged.
- Upstream is whatever `/etc/resolv.conf` in the helper names (aardvark-dns on
  `flow-connectors`); the shim passes it explicitly.

## Policy JSON

```json
{
  "egress": "public",
  "allowAll": false,
  "declaredCidrs": [ { "cidr": "198.51.100.10/32", "ports": [443] } ],
  "connectionsPerMinute": null,
  "distinctDestinationsPerMinute": null,
  "ttlFloorSecs": 90,
  "ttlCapSecs": 3600
}
```
- `egress`: `none` (drop everything, no resolver) or `public` (DNS-gated).
- `allowAll`: `true` accepts all forwarded traffic from the guest. Masquerade
  and anti-spoof still apply. Experiment 4 only.
- `declaredCidrs`: reachable without resolution, on the listed ports only.
  Must not overlap the baseline. `ports` is TCP.
- Rate limits: `null` means unlimited. Semantics per PLAN experiment 8.
- Deny action everywhere is `drop`.

Baseline denylist (in the ruleset, not the policy): 0.0.0.0/8, 10.0.0.0/8,
100.64.0.0/10, 127.0.0.0/8, 169.254.0.0/16, 172.16.0.0/12, 192.168.0.0/16,
224.0.0.0/4, 240.0.0.0/4, the helper's tap and eth0 subnets as found on its
interfaces at start, and tcp/25 to anywhere. These are destination drops in
the forward chain; the guest's masqueraded traffic to non-baseline
destinations still leaves through eth0. The only exception anywhere is
`192.0.2.2 -> 192.0.2.1 udp/53`, in the input chain. The helper's eth0
subnet (10.89.0.0/24 on `flow-connectors`) sits inside 10/8, so the baseline
set is `flags interval` with `auto-merge`, or entries are deduplicated before
load; nft rejects overlapping interval elements otherwise.

Structural rules that hold for any policy: only TCP and UDP cross the tap;
guest packets must have source `192.0.2.2`; no IPv6; no ICMP; nothing inbound
to the guest except replies to its own connections; the guest reaches the
helper only on udp/53.

## flow-init (WP04 provides, WP03 invokes)

Injected at `/flow-init` in the guest root. Runs as guest root, as a child of
libkrun's init, which has already mounted `/dev`, `/proc`, `/sys`,
`/sys/fs/cgroup`, `/dev/pts`, `/dev/shm` and brought up `lo`.

```
/flow-init --guest-ip 192.0.2.2/30 --gateway 192.0.2.1 --nameserver 192.0.2.1 \
    --upper-mib N --uid U --gid G [--venv-dax] [--run-as-root] \
    [--deps-dev /dev/vdb --deps-fstype FS] [--as-root-exec 'CMD'] -- ARGV...
```
In order: static eth0 and default route; IPv6 disabled; capped tmpfs upper
over the read-only virtiofs root via overlayfs, then `pivot_root`; write
`/etc/resolv.conf` (`nameserver <ns>`) and `/etc/hosts`; `mkdir -p /venv
/scratch`; mount virtiofs tag `venv` at `/venv` read-only (with `dax` when
asked); mount `/dev/vda` ext4 at `/scratch`; if `--deps-dev`, `mkdir -p
/opt/venv` and mount it there read-only with `--deps-fstype`; if
`--as-root-exec`, run it via
`/bin/sh -c` and wait (probes that need root before the drop); set
`TMPDIR=/scratch` and `UV_CACHE_DIR=/scratch`; chdir to the workdir it was
started in; `setgroups([])`, `setgid`, `setuid` unless `--run-as-root`; exec
ARGV. Environment is inherited from libkrun's init (which applies the image's
`Env` from `/.krun_config.json`) and passed through.

On any failure: one line to stderr (no leading space), exit 125.

Facts WP04 established that later packages must not undo:
- flow-init runs in its own mount namespace (`unshare(CLONE_NEWNS)`, then the
  tree is made private). libkrun's init reports the workload's exit code only
  while ITS `/` is still virtiofs; a `pivot_root` in the shared namespace
  makes every guest exit code silently 0.
- `/dev` is mounted again inside the new root rather than moved (the tmpfs
  holding the new root lives under `/dev`); devpts and shm are remade there.
- The scratch mount point is chowned to the image's uid:gid after mount, or a
  dropped workload cannot write `TMPDIR`.
- `/etc/hosts` is `127.0.0.1 localhost` only; the guest hostname is the
  kernel default `localhost`. Nothing in the spike reads it. If a connector
  turns out to, that is a `sethostname` in phase 2.
- `--as-root-exec` ignores its command's exit status by design; probes
  report their own results.

## connector-init (WP01 provides)

`--vsock-port N` serves the same gRPC services over AF_VSOCK on port N (any
CID). Exactly one of `--port` and `--vsock-port` is required. The readiness
byte (a single space on stderr) is written after the listener is bound, as
today. Nothing else changes.

## runtime-next spike switch (WP05 provides)

Enabled when `FLOW_SANDBOX_SPIKE_POLICY` is set to a policy JSON path. Then
`container::start` in `runtime-next` launches the helper instead of the
connector, per PLAN "Helper launch", and dials `<id>/sock/init.sock` instead
of TCP. Other variables, all with defaults:

| Variable                          | Default                            |
|-----------------------------------|------------------------------------|
| FLOW_SANDBOX_SPIKE_HELPER_IMAGE   | localhost/flow-sandbox-helper:spike |
| FLOW_SANDBOX_SPIKE_REACTOR_DIR    | /var/tmp/flow-spike/reactor        |
| FLOW_SANDBOX_SPIKE_MEMORY_MIB     | 1024                               |
| FLOW_SANDBOX_SPIKE_MEMORY_OVERHEAD_MIB | 256 (until WP09 measures)     |
| FLOW_SANDBOX_SPIKE_VCPUS          | 2                                  |
| FLOW_SANDBOX_SPIKE_DISK_MIB       | 4096                               |
| FLOW_SANDBOX_SPIKE_UPPER_MIB      | 256                                |
| FLOW_SANDBOX_SPIKE_VENV_DIR       | (empty dir under `<id>/`)          |
| FLOW_SANDBOX_SPIKE_DEPS_IMAGE     | (unset: no deps disk) host path, bound read-only at `/deps.img`, passed as `--deps-image /deps.img` |
| FLOW_SANDBOX_SPIKE_DEPS_FSTYPE    | ext4                               |
| FLOW_SANDBOX_SPIKE_HELPER_ARGS    | (extra helper args, space-split)   |

The unmodified code path must be byte-identical when the variable is unset.
`runtime::Container.ip_addr` is set to `192.0.2.2` and `network_ports` to
empty; nothing in the spike exercises connector network ports.

## Fake reactor (WP00 provides)

`spike/tasks/fake-reactor.sh CMD ARGS...` runs CMD inside the production
reactor image the way the Quadlet unit does: `--network=host`, podman's
default capabilities, `/run/podman` mounted, `$SPIKE_REACTOR_DIR` mounted at
the same path, `CONTAINER_HOST=unix:///run/podman/podman.sock`,
`DOCKER_CLI=podman`, plus the repo's `target/` directory mounted so a locally
built `flowctl` and `flow-connector-init` can be used. All `FLOW_SANDBOX_SPIKE_*`
variables in the caller's environment are passed through.

## Probes and benchmarks (WP02 provides probes; WP07/WP08/WP09 consume)

- `spike/egress/probes.py`: runs a named probe set against expectations and
  prints one JSON line per probe: `{"probe": str, "expect": str, "result":
  str, "pass": bool, "ms": int}`. Exit 0 iff all pass. Python 3.12+ stdlib
  only, so it runs unchanged in a veth netns on the host and inside the
  derive-python guest. Parameters (target addresses, which set) come from
  argv, never hardcoded.
- Anything the guest must run for an experiment is placed under the venv
  directory at `spike/` and invoked via `--exec /usr/local/bin/python
  /venv/spike/<script>`. derive-python's interpreter lives at
  `/usr/local/bin/python`.

## Report data

`spike/report/expN.md` per experiment with the pass/fail line first, then
tables. Raw runs as CSV under `spike/report/data/expN-*.csv`. Every number
names the commit it was measured at.
