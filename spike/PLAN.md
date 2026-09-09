# Spike: libkrun sandbox for customer Python connectors

## Purpose

Prove, on real hardware, that the launcher design in the sandbox issue works
end to end: a podman-hosted libkrun helper boots our connector image as a
guest, connector-init answers the reactor over vsock, egress is enforced by
nftables in the helper's netns with a DNS-gated allow set, and the cost per
connector is one we can carry. Produce numbers, not opinions.

The spike is throwaway code on branch `daveg/libkrun-spike`. Nothing here
merges. The output is a short report and a go/no-go.

## What the spike is not

- Not a runtime integration. The only change to real code is a spike-only
  switch in `runtime-next`'s container launcher and a `--vsock-port` flag on
  connector-init, both of which die with the branch.
- Not the builder VM, the policy model, or the egress IP pool. Those have
  their own phases.
- Not a security audit. It proves the mechanisms behave as designed. A
  separate review decides whether the design is sufficient.
- Not AWS. The current AWS reactor instance family exposes no `/dev/kvm`
  (Nitro non-metal). AWS gets validated separately once this works on GCP.

## Inputs established before the spike

How production launches connectors today, from a live reactor:

- The reactor is a Quadlet podman container: `--network=host`, `--read-only`,
  podman's default capability set (no NET_ADMIN, no SYS_ADMIN), with
  `/run/podman`, `/mnt/local/reactor`, and `/mnt/container` bind-mounted in.
- It drives the HOST's podman through the `/run/podman` API socket. Connector
  containers land in host storage, on network `flow-connectors`, under
  `--cgroup-parent estuary-connectors.slice`. The socket is the privilege.
- `/mnt/local/reactor` is `rm -rf`'d on every reactor restart. Anything the
  launcher creates per connector lives under it.
- Host: Ubuntu noble, podman 4.9.3, nftables 1.0.9, `/dev/net/tun` present.

libkrun 1.19 facts the design relies on:

- `krun_fs_add_overlay_file` injects a host-memory-backed file at a root-level
  path of the root virtiofs (this is how libkrun serves its own `/init.krun`).
- `krun_add_virtiofs3` with tag `/dev/root` and `read_only=true` gives a
  read-only root share. `krun_set_root` is read-write.
- libkrun's guest init mounts dev/proc/sys/cgroup2/devpts/shm, brings up `lo`,
  forks the workload, and on exit reports the code via a virtiofs ioctl and
  reboots. It does NOT do static network config, resolv.conf, extra mounts,
  or uid drop. Env/WorkingDir/Cmd come from `/.krun_config.json`.
- With a port map and no explicit `krun_add_vsock`, the implicit vsock config
  enables `HIJACK_INET`, leaving host-side TSI proxies live even with a tap.
  `krun_add_vsock(ctx, 0)` turns them off.
- libkrun opens/creates the tap itself inside `krun_start_enter` (which never
  returns) and sets no address.
- Kernel cmdline is fixed (`panic=-1 ... console=hvc0 rootfstype=virtiofs rw
  quiet`); `krun_append_kernel_cmdline` is 2.0-only. Root DAX is therefore
  not reachable in 1.19. libkrunfw kernel is 6.12: ext4, overlayfs,
  virtio-blk, vsock, FUSE DAX, free-page reporting on; no MAGIC_SYSRQ.
- The balloon device is on by default and reports free pages via
  `madvise(MADV_DONTNEED)`.

## Environment

- The GCP dev box: Ubuntu 24.04, kernel 7.0.0-gcp, `/dev/kvm` present, vmx,
  8 vCPU, 15 GiB. Install podman 4.9.3 from noble (matches production).
  A local directory stands in for `/mnt/local/reactor`.
- Fake reactor: the production reactor image, run as the Quadlet unit runs it
  (`--network=host`, default caps, `/run/podman` and the reactor dir mounted).
  `flowctl preview` runs inside it. Everything the spike launches is launched
  from there. If a step only works from a root shell on the host, that is a
  finding.
- Images: `ghcr.io/estuary/derive-python`, one Go capture
  (`source-hello-world` or similar), one Go materialization with a local
  endpoint (`materialize-sqlite` or similar). A derived image
  (derive-python plus a venv layer containing pandas) for experiment 5.
- libkrun v1.19.x with libkrunfw, in a helper image we build for the spike.
- An nginx container on its own podman network with subnet 198.51.100.0/24
  (TEST-NET-2: not RFC1918, not in the baseline, routable on the host).

## The thing under test

### Helper launch

The spike switch in `runtime-next/src/container.rs` writes connector-init and
the image-inspect JSON under `/mnt/local/reactor/<id>/`, as today, and runs:

```
podman run --rm --name=<name> --network=flow-connectors --log-driver=none \
  --cgroup-parent estuary-connectors.slice \
  --label=image=... --label=task-name=... --label=task-type=... \
  --device /dev/kvm --device /dev/net/tun --cap-add NET_ADMIN \
  --sysctl net.ipv4.ip_forward=1 \
  --memory <memoryMib + overhead>m --cpus <vcpus> \
  --mount type=image,source=<connector image>,destination=/rootfs \
  --mount type=bind,source=/mnt/local/reactor/<id>/init,target=/init,ro \
  --mount type=bind,source=<host venv dir or empty>,target=/venv,ro \
  --mount type=bind,source=/mnt/local/reactor/<id>/sock,target=/sock \
  --mount type=bind,source=/mnt/local/reactor/<id>/scratch,target=/scratch-backing \
  <helper image> --policy <json> --memory-mib <n> --vcpus <n> --disk-mib <n>
```

It then waits for connector-init's readiness byte on stderr exactly as today
and dials `/sock/init.sock` with tonic instead of TCP. No TCP anywhere between
reactor and guest. `memoryMib` is the guest's RAM; the cgroup limit adds a
fixed overhead that experiment 10 measures.

### Helper image

libkrun, libkrunfw, `nft`, e2fsprogs, a small resolver, `flow-init` (our
static guest init), and the shim. The shim, in order:

1. Creates a persistent tap, assigns the helper end of a /30, brings it up,
   loads the nftables ruleset for the policy, starts the resolver on the
   helper address (not started when `egress: none`). All rules exist before
   the guest's first packet.
2. Opens an `O_TMPFILE` in `/scratch-backing`, sizes it to `diskMib`, and
   runs `mkfs.ext4` on `/proc/self/fd/N` with options that avoid lazy
   inode-table init (which would otherwise grow the sparse file from inside
   the guest after mount). The file has no name; SIGKILL frees it.
3. Resolves the image's `USER` to numeric uid:gid from the image's
   `/etc/passwd` and `/etc/group`; builds `/.krun_config.json` with the
   image's `Env` plus `LOG_FORMAT`/`LOG_LEVEL`, `WorkingDir`, and
   `Cmd=/flow-init`.
4. Configures libkrun:
   - `krun_set_vm_config(vcpus, memoryMib)`
   - `krun_add_virtiofs3("/dev/root", "/rootfs", 0, read_only)`
   - `krun_add_virtiofs3("venv", "/venv", <shm or 0>, read_only)`
   - overlay files at the root: `/flow-init`, `/flow-connector-init`,
     `/image-inspect.json`, `/.krun_config.json`
   - `krun_add_disk3("scratch", "/proc/self/fd/N", RAW, rw, ...)`
   - `krun_add_net_tap("tapN", mac, features, 0)`
   - `krun_add_vsock(ctx, 0)` then
     `krun_add_vsock_port2(ctx, 49092, "/sock/init.sock", listen=true)`
   - `krun_disable_implicit_console` then
     `krun_add_virtio_console_default(devnull, stdout, stderr)`: kernel
     console goes to helper stdout (which the reactor discards today), the
     workload's stderr goes straight to fd 2 so connector-init's logs and
     readiness byte reach the reactor unchanged. A spike `--debug` tees.
   - `krun_set_exec("/flow-init", ...)` and `krun_start_enter`.

### flow-init (guest, runs as root under libkrun's init)

Static binary, no libc or shell assumed in the image. In order:

1. Static eth0: guest end of the /30, default route via the helper, using the
   `SIOCSIFADDR`/`SIOCSIFNETMASK`/`SIOCSIFFLAGS`/`SIOCADDRT` ioctls. Disable
   IPv6 via sysctl.
2. Mount a size-capped tmpfs, overlay it over the read-only virtiofs root,
   `pivot_root`. The guest sees a writable, ephemeral root like a container.
3. Write `/etc/resolv.conf` (nameserver = helper address) and `/etc/hosts`.
   Create `/venv` and `/scratch`. Mount the `venv` virtiofs (with `dax` in
   the experiment 5 variant) and `/dev/vda` ext4 at `/scratch`.
4. Set `TMPDIR=/scratch` and `UV_CACHE_DIR=/scratch`, chdir to the workdir,
   drop to the image's uid:gid, and exec
   `/flow-connector-init --image-inspect-json-path=/image-inspect.json
   --vsock-port=49092`.

Exit path needs no code: connector-init's watchdog exits, flow-init exits
with its code, libkrun's init reports it and reboots, the VMM exits with it,
podman removes the container.

### Driver

`flowctl preview` against a local catalog holding the three connectors, run
inside the fake reactor. The spike switch on and off is the A/B for
experiment 3.

## Experiments

Each experiment names a pass condition. "Gate" means a failure is a no-go or
a redesign. "Measure" means record the number and move on.

### 1. Launch through the podman API from the reactor's privilege level (gate)

From the fake reactor, the helper starts, the guest boots, connector-init
answers on `/sock/init.sock`. Inspect the running helper: `CapEff` is
podman's default set plus NET_ADMIN and nothing else; `/dev/kvm` and
`/dev/net/tun` are the only added devices. Record anything the API service
refused. (The service is root and honors these flags for any client, so this
is expected to pass; it exists to pin the exact flags and to catch anything
that needed the host shell.)

### 2. Boot latency (gate, under 5 seconds)

Time from `podman run` to connector-init's readiness byte, warm image cache,
20 launches. Report median and p95; pass is p95 under 5 seconds. Breakdown:
podman create to start, kernel boot to flow-init exec, flow-init done,
connector-init ready. Baseline: the same image under today's plain `podman
run` to the same byte, 20 launches, so the report shows the delta and not
just the absolute.

### 3. Protocol parity with Go connectors (gate)

`flowctl preview` of the Go capture (Spec, Validate, Open, documents) and
the Go materialization (Spec, Validate, Open, a transaction against its local
endpoint) with the switch on and off. Pass: identical outputs, and connector
logs arrive through the same decoder. This proves the launcher is not
derive-python specific and that connector-init, the codec, and the console
log path work inside the guest.

### 4. derive-python end to end, permissive network (gate)

Preview a derivation with at least one non-trivial dependency (pandas) with
the ruleset in allow-all mode, so `uv` fetches from PyPI inside the guest.
Pass: the derivation produces documents. This is the smoke test of Python,
the network stack, and connector-init together. Record the `/scratch`
footprint for information; it does not size `diskMib`, since production
guests receive prebuilt dependencies.

### 5. virtiofs cold import (gate, no worse than 2x)

Primary: boot the derived image (derive-python plus venv layer) as the root.
In a fresh guest (guest cache cold, host cache warm), time `import pandas`
and `python -c pass`; the second isolates interpreter-and-stdlib cost on the
root share from site-packages cost. Baseline: same two commands in a fresh
plain `podman run` of the derived image. Pass: guest no worse than 2x on the
import.

Variants, run regardless of pass: the venv as a separate read-only share,
without and with `dax`; and THP on versus off in the helper. Report the full
matrix. Note in the report that DAX cannot be applied to the root in libkrun
1.19, so if DAX is what passes, the venv must be a separate share (or we
supply our own kernel command line).

### 6. Egress rules proven from inside the guest (gate)

Deny action is `drop`. "Must fail" means the lookup or connect errors or
times out AND no matching packet leaves the helper (verify with a capture on
the helper's eth0 and conntrack). With a `public` policy and the baseline
loaded, run each probe as the connector user inside the guest:

- Connect to a name resolved through the helper resolver, port 443. Must
  succeed.
- Connect to a raw public IP that was never resolved. Must fail.
- Resolve a name that answers with an RFC1918 address (a sibling container's
  name via aardvark-dns is a ready-made case). Lookup must fail with an
  error; no set entry is added.
- Connect to the metadata server (169.254.169.254:80). Must fail.
- Connect to the helper's tap-side address on any port other than 53, to the
  helper's eth0 address on any port, and to the bridge gateway (the host)
  on the reactor's port. All must fail.
- `ping` a public address. Must fail (no ICMP crosses the tap).
- Send a UDP packet with a spoofed source address (from flow-init as root,
  before dropping privileges). Must be dropped.
- Any IPv6 traffic. Must be dropped (and the guest has IPv6 disabled; the
  resolver strips AAAA).
- Open a listener in the guest and connect to it from inside the helper
  container. Must fail (no inbound).
- Connect to port 25 on any address. Must fail.
- Connect to an address inside a declared CIDR without resolving it. Must
  succeed on the declared port and fail on any other port.
- TTL: resolve a name with a short TTL. Set element timeout is
  `clamp(ttl, 90s, 1h)`. Connect after 90 seconds must fail. Resolve again
  and connect must succeed. A connection opened before expiry stays open.

Baseline denylist, written down: 0.0.0.0/8, 10/8, 100.64/10, 127/8,
169.254/16, 172.16/12, 192.168/16, 224/4, 240/4, the helper's own tap and
eth0 subnets (derived from its interfaces at start, not assumed), and tcp/25
everywhere.

### 7. `none` mode (gate)

With `egress: none`, a DNS lookup from Python and a connect to a raw IP both
fail, and no packet leaves the helper. Record how long each takes to surface
an error: with drop, expect the resolver's retry budget and the TCP handshake
timeout. That latency is an accepted cost and belongs in the report.

### 8. Rate and fan-out limits (gate)

With `connectionsPerMinute: 60` and `distinctDestinationsPerMinute: 5`, a
Python loop that resolves and connects to 20 distinct hosts sees the sixth
distinct destination refused and connections beyond the rate refused within
the minute (nft `limit` is a token bucket with a burst, so "exactly the
61st" is not the claim). Both recover after the window. Report the nftables
constructs used (expected: `limit rate` and a dynamic set with `size 5` and
a timeout) so the runtime implementation copies them.

### 9. Connection churn (measure)

One HTTPS request per new connection at 50 connections per second for 10
minutes, through the tap, to the TEST-NET-2 nginx declared as a /32 in the
policy (this also exercises the declared-CIDR path). Report throughput over
time, helper CPU and RSS at start and end, and the vCPU count used. No
userspace proxy is in this path, so this is a sanity check on the tap and
virtio-net, not a go/no-go.

### 10. Density (measure)

Boot idle derive-python guests with `memoryMib: 512` until host memory
reaches 80% (this box has 15 GiB; report the count reached). Report host
RSS per helper (cgroup `memory.current`), thread count per helper, total
host memory delta, and whether free-page reporting reclaims after the guest
drops its page cache. Note that virtiofs page cache is charged to the
helper's cgroup and is reclaimable, so read `memory.current` accordingly.
Derive the cgroup overhead constant (helper `memory.current` minus guest
touched RAM) that the launcher adds to `memoryMib`. Repeat with guests each
holding one long-lived TLS connection to nginx. Run once with THP on and
once off. No gate; the numbers inform `resources` defaults.

### 11. Storage behavior (gate)

- The writable upper layer fills at its cap and the write fails; the
  helper's `memory.current` reflects it.
- `/scratch` fills at `diskMib` and the write fails; host memory does not
  grow to match.
- Guest writes under `/usr` and `/etc` succeed (they land in the upper
  layer) and do not appear in the host's image mount afterwards.
- Writes under `/venv` fail (read-only share).
- After `podman run --rm` exits, the scratch directory is empty and `df`
  shows the space returned.

### 12. Control channel and device exposure (gate)

- Connecting from the guest to any vsock port other than the one we mapped
  is refused.
- From the guest, send a TSI proxy-create datagram to the vsock control
  port. The helper opens nothing (`ss -tunap` before and after). This
  confirms `krun_add_vsock(ctx, 0)` took.
- The guest sees exactly the devices we configured (`lsblk`,
  `ls /sys/bus/virtio/devices`: two fs, one blk, one net, vsock, console,
  balloon, rng) and no more.
- Read libkrun's vsock muxer and virtiofs device sources for anything they
  expose to the guest beyond mapped ports and added shares, including the
  virtiofs ioctl handlers (the exit-code ioctl, and the "remove root dir"
  request a 1.18 release restricted). Record findings, including "none
  found."

### 13. Helper crash and cleanup (gate)

Kill the helper with SIGKILL mid-transaction. Pass: the fake reactor observes
the socket close, the guest is gone, the tap and nftables rules are gone with
the netns, the scratch space is returned (O_TMPFILE, no cleanup code), and
the reactor removes the socket file. Then, as guest root, set
`vm.panic_on_oom=1` and run a memory hog (the guest kernel has no sysrq).
Pass: the kernel panics, `panic=-1` reboots it, and the helper exits
non-zero promptly rather than hanging.

## Report

One document containing:

- The numbers from experiments 2, 5, 9, and 10 as tables.
- A pass/fail line per gate with a one-sentence note where it matters.
- The exact `podman run` flags, the nftables ruleset, the libkrun call
  sequence, and the mkfs options that passed, so phase 2 starts from them.
- The measured cgroup overhead constant and the THP result.
- Anything that only worked from a root shell on the host.
- Open problems found along the way, each with a proposed owner.

## Decision

Go if every gate passes. If a single gate fails, the report says which
redesign it forces rather than declaring a no-go:

- Experiment 1 fails: record which flag the API service refused. There is
  no capability question to answer; the reactor already holds the socket.
- Experiment 2 fails: the breakdown decides. Time in podman create/start is
  a podman question; time in the guest kernel means a slimmer libkrunfw; time
  in virtiofs or flow-init means experiment 5's variants decide.
- Experiment 5 fails on the primary: if the separate-share DAX variant
  passes, the venv becomes a separate share rather than an image layer. If
  the root share is the slow part, options are unpacking the image to a
  plain host directory once per tag (removes podman's overlayfs beneath
  virtiofs) or supplying our own kernel command line for root DAX (libkrun
  2.0 or `krun_set_kernel`). Everything else is unchanged.
- Experiments 6, 7, 8, 11, 12, or 13 fail: bugs in the spike's ruleset,
  shim, or flow-init, not in the design. Fix and rerun.
- Experiment 3 or 4 fails: something about connector-init, the codec, or
  Python inside the guest is wrong. Fix and rerun; if the fix requires a
  libkrun change, that is a no-go on libkrun and the report says so.
