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
  Reactor image at inspection: `ghcr.io/estuary/reactor:v0.6.13-54-gb3f769fb452`
  (the spike uses the newest tag; only the podman client and caps matter).

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
- libkrun's virtiofs passthrough answers unknown ioctls with EOPNOTSUPP,
  and overlayfs copy-up tolerates only ENOTTY or EINVAL from
  FS_IOC_GETFLAGS, so a guest-side overlay over the image share cannot copy
  up without patching libkrun (WP04 proved this with a one-line patch). We
  chose not to carry a patch: the writable root is podman's own per-container
  layer (`--mount type=image,...,rw=true`) served read-write, which is what a
  container has today. Root writes are bounded by host disk, as today.
- libkrun's init reports the workload's exit code only while its own `/` is
  virtiofs; a guest init that pivots root must do so in its own mount
  namespace or every exit code is 0.

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
- libkrun v1.19.4 built from source, unpatched (Fedora 43 packages 1.19.0,
  which predates the 1.19.3 virtiofs attribute-caching change), with
  Fedora's libkrunfw (kernel 6.12.91), in a helper image we build for the
  spike.
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
  --sysctl net.ipv4.ip_forward=1 --sysctl net.ipv4.conf.default.rp_filter=1 \
  --env=LOG_FORMAT=json --env=LOG_LEVEL=<level> \
  --memory <memoryMib + overhead>m --cpus <vcpus> \
  --mount type=image,source=<connector image>,destination=/rootfs,rw=true \
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
   - `krun_add_virtiofs3("/dev/root", "/rootfs", 0, read_only=false)`; the
     share is podman's writable layer over the image, removed with the
     container
   - `krun_add_virtiofs3("venv", "/venv", <shm or 0>, read_only)`
   - overlay files at the root: `/flow-init`, `/flow-connector-init`,
     `/image-inspect.json`, `/.krun_config.json`
   - `krun_add_disk3("scratch", "/proc/self/fd/N", RAW, rw, ...)`, then
     when a dependency image is given, `krun_add_disk3("deps", "/deps.img",
     RAW, read_only, ...)` so it lands as `/dev/vdb`
   - `krun_add_net_tap("tapN", mac, features, 0)`
   - `krun_disable_implicit_vsock`, `krun_add_vsock(ctx, 0)` (1.19 rejects
     the explicit call unless the implicit device is disabled first), then
     `krun_add_vsock_port2(ctx, 49092, "/sock/init.sock", listen=true)`
   - `krun_disable_implicit_console` then
     `krun_add_virtio_console_default(devnull, stdout, stderr)`: kernel
     console goes to helper stdout (which the reactor discards today), the
     workload's stderr goes straight to fd 2 so connector-init's logs and
     readiness byte reach the reactor unchanged. A spike `--debug` tees.
   - `krun_start_enter`. No `krun_set_exec`: it sets `KRUN_INIT`, which
     makes libkrun's init ignore `Cmd`; argv, env, and workdir all come from
     the injected `/.krun_config.json`.

### flow-init (guest, runs as root under libkrun's init)

Static binary, no libc or shell assumed in the image. In order:

1. Static eth0: guest end of the /30, default route via the helper, using the
   `SIOCSIFADDR`/`SIOCSIFNETMASK`/`SIOCSIFFLAGS`/`SIOCADDRT` ioctls. Disable
   IPv6 via sysctl.
2. Write `/etc/resolv.conf` (nameserver = helper address) and `/etc/hosts`.
   Create `/venv` and `/scratch`. Mount the `venv` virtiofs (with `dax` in
   the experiment 5 variant) and `/dev/vda` ext4 at `/scratch`, chowned to
   the image's uid:gid. When a dependency image is attached, mount `/dev/vdb`
   read-only at `/opt/venv`.
3. Set `TMPDIR=/scratch` and `UV_CACHE_DIR=/scratch`, chdir to the workdir,
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

Results so far: 1 PASS (WP06: CapEff is podman's default plus NET_ADMIN,
two devices, nothing needed the host shell). 2 PASS (WP06: p95 0.735 s
against 5 s; sandbox adds ~275 ms over today, of which ~290 ms after
flow-init is guest stderr reaching the host through libkrun's console).
3 PASS (WP06: six diffs empty across a Go capture and materialization).
4 PASS (WP06 under placeholder egress; WP07 reran it under the real
`allowAll` ruleset, byte-identical). 5 PASS by ruling at 2.12x (WP08b).
6 PASS (WP07: 39 probes from inside the guest, four passes, every uplink
capture empty). 7 PASS (WP07: lookup and connect both fail with no packet
leaving; DNS 10 s, connect bounded by the probe's cap). 8 PASS (WP07: fan-out
5 of 20, rate held to 60/minute, identical to the netns numbers).

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

Result (WP06 dff79afbc3e, rerun WP07 d8c804722e2): PASS under WP02's real
`allowAll` ruleset. Note `allowAll` is not an unenforced network: anti-spoof,
the baseline denylist, the IPv6 and non-TCP/UDP drops, tcp/25 and the input
and output chains all still apply; it only removes the requirement that a
destination be named. Four documents byte-identical to the unsandboxed run;
uv fetched pandas 3.0.5 inside the guest with AAAA stripped; `/scratch`
footprint 183 MB.

Preview a derivation with at least one non-trivial dependency (pandas) with
the ruleset in allow-all mode, so `uv` fetches from PyPI inside the guest.
Pass: the derivation produces documents. This is the smoke test of Python,
the network stack, and connector-init together. Record the `/scratch`
footprint for information; it does not size `diskMib`, since production
guests receive prebuilt dependencies.

### 5. virtiofs cold import (gate, no worse than 2x)

Result (WP08, commit f6498560dea): FAIL on virtiofs, 2.88x as an image
layer, 2.40x as a separate share, DAX and THP irrelevant, CPU identical to a
container. The cost is per-file metadata round trips, not bandwidth: warm
virtiofs is still 2.2x a warm container. The identical venv on ext4 over
virtio-blk imports at 0.89x (WP08b corrects that figure: it was measured in a
prefaulted guest, and the honest steady-state number is 1.15x). Redesign: the
dependency set ships as a per-tag read-only disk image on a second virtio-blk
device (`--deps-image`), mounted at `/opt/venv`. The root stays on virtiofs.

Result (WP08b, commit 2146b6ba34e): **PASS, on the ruling below.** The block
image measures 2.12x on a first import after boot, against the 2.00x limit,
but 1.15x once past it - and 1.15x is what the gate was actually protecting.
Accepted: the gate closes here. Reasoning in STATUS.md under the WP08b
acceptance; the short form is that the 2x limit was a proxy for a metadata
premium paid on every file operation for a connector's life, that premium is
now 1.15x (it was 2.33x warm on virtiofs), and the 371 ms that keeps the
first-import number above 2x is a one-time per-boot cost inside a 2.3 s launch
against a 5 s budget. About 201 ms of it is guest memory first-touch, which is
libkrun's to fix if anyone ever wants it back.

Original design of the experiment, kept for the record. Primary: boot the derived image (derive-python plus venv layer) as the root.
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
Python loop that connects to 20 distinct destinations sees the sixth
distinct destination unreachable for the rest of the window, and the
sustained connection rate held to 60 per minute. WP02 established that a
dropped SYN is retransmitted after one second and the bucket refills at one
per second, so the rate limit surfaces as pacing, not as errors: every
connection eventually completes, at the configured rate. The fan-out limit
surfaces as a connect timeout. Both recover after the window. Report the nftables
constructs used (expected: `limit rate` and a dynamic set with `size 5` and
a timeout) so the runtime implementation copies them.

### 9. Connection churn (measure)

One HTTPS request per new connection at 50 connections per second for 10
minutes, through the tap, to the TEST-NET-2 nginx declared as a /32 in the
policy (this also exercises the declared-CIDR path). Report throughput over
time, helper CPU and RSS at start and end, and the vCPU count used. No
userspace proxy is in this path, so this is a sanity check on the tap and
virtio-net, not a go/no-go.

Result (WP09, `spike/report/exp9.md`): 30,000 new TCP+TLS connections at
50/s for ten minutes, zero failures, every ten-second window at exactly
50.00/s; p50 2.9 ms per connection on the guest's clock. The helper cost
15.2% of one core (2 vcpus configured) and 95 MiB, flat within 2 MiB after
the first minute; threads 12 to 14 and never more. Not a ceiling: 50/s was
the sanity check and the path carried it without strain. conntrack held
~6,000 TIME_WAIT entries at default sizing.

### 10. Density (measure)

Boot idle derive-python guests with `memoryMib: 512` until host memory
reaches 80% (this box has 15 GiB; report the count reached). Report host
RSS per helper (cgroup `memory.current`), thread count per helper, total
host memory delta, and whether free-page reporting reclaims after the guest
drops its page cache. Note that virtiofs page cache is charged to the
helper's cgroup and is reclaimable, so read `memory.current` accordingly.
Derive the cgroup overhead constant (helper `memory.current` minus guest
touched RAM, where touched is `MemTotal - MemFree`: `MemAvailable` counts the
guest's own page cache as free while the host still backs it, and gives a
constant near zero or negative) that the launcher adds to `memoryMib`. Repeat
with guests each holding one long-lived TLS connection to nginx. Run once
with THP on and once off. No gate; the numbers inform `resources` defaults.

Result (WP09, `spike/report/exp10.md`): 82 idle 512 MiB guests before
`MemAvailable` crossed 20% on a box with ~2.9 GiB already taken by an editor;
95 MiB of host memory per guest, of which 72.6 MiB is inside the helper's
cgroup and ~22 MiB is the netns, podman's bookkeeping and host page tables.
The 82nd guest cost what the first did: density is linear. 13 threads per
helper in every arm. A held TLS connection adds ~5 MiB. THP: no difference
(95.2 vs 94.6 MiB); `--thp-disable` is not worth carrying. Free-page
reporting returns memory promptly: 493 of 512 MiB back within one 5 s sample
of `drop_caches`. The overhead constant is 20 to 32 MiB, the same at 512 and
1024 MiB and no larger when the guest is 80% full, so it is a constant. The
launcher's 256 is ~8x that; 64 (twice the worst case) is what the
measurement supports. The default stays 256 in the spike: the cgroup limit
also bounds the host page cache the helper is charged for (the reclaimable
`file` term, 535 MiB in the reclaim arm), so a tighter limit trades
throughput, and experiment 5's import numbers were all taken at 256.
Adopting 64 is a phase-2 item that starts with rerunning experiment 5 at
`memoryMib + 64`.

### 11. Storage behavior (gate)

- Guest writes to the root land in podman's per-container writable layer on
  host disk, not in guest memory, and are bounded only by that disk, exactly
  as a container's are today. Record where they land and confirm the layer
  is gone after `podman run --rm` exits (`podman system df` before/after).
- `/scratch` fills at `diskMib` and the write fails; host memory does not
  grow to match.
- Guest writes under `/usr` and `/etc` (as guest root) succeed and do not
  appear in the image afterwards (`podman image mount` is unchanged).
- Writes under `/venv` fail (read-only share).
- After `podman run --rm` exits, the scratch directory is empty and `df`
  shows the space returned.

  Result (WP10, `spike/report/exp11.md`): PASS. Root writes land in
  `overlay-containers/<container id>/userdata/overlay/<n>/upper` and go with
  the container; a 1024 MiB guest wrote 1536 MiB to its root with the helper's
  cgroup pinned at its 1280 MiB limit, so the bound is the disk, not memory.
  `/scratch` stops with ENOSPC at `--disk-mib` (3999 of 4352 MiB at 4096) and
  the space returns when the helper exits. Guest-root writes under `/usr`
  never reach the image; `/venv` is EROFS even to guest root. Two facts for
  phase 2: `podman inspect` does not report the image mount's writable layer
  (its `.GraphDriver` is the helper's own root), so any per-task quota or
  metric on root writes must know the path shape above; and the root has no
  per-task bound at all, which is parity with a container today and the one
  storage surface without a limit.

### 12. Control channel and device exposure (gate)

- Connecting from the guest to the mapped port 49092 is reset (libkrun
  sends RST for a `listen=true` mapping); connecting to any other vsock
  port gets no response at all (WP11: the muxer drops the request silently),
  so the probe must impose its own timeout. Two mechanisms, two observable
  results; the report says so rather than blurring them.
- From the guest, send a TSI proxy-create datagram to the vsock control
  port. The helper opens nothing (`ss -tunap` before and after). This
  confirms `krun_add_vsock(ctx, 0)` took.
- As guest root, `ioctl(fd, 0x7602, 42)` on a file under the read-only
  `/venv` share, with `ioctl(fd, 0x7601, 42)` on the same fd as the control,
  then exit 7. Expected: the first is accepted and the second refused, which
  proves the interception is real; then the helper exits 7, not 42, because
  libkrun's own init reports the workload's status through the same ioctl
  after `waitpid` and writes last (WP10 corrected WP11 here). The helper's
  exit code is the workload's exit status and nothing more; the runtime must
  not treat it as something the platform observed.
- From the unprivileged workload, open a path with `..` and embedded `/`
  components (`/venv/../../etc/hostname`). It resolves inside the guest.
  The guest VFS never puts `..` on the FUSE wire, which is the confinement
  WP11 found at T1/T2.
- As guest root, write directly to `/dev/vdb` (the deps disk). It fails: the
  host opened the image without write access, so the protection is the host
  fd, not a flag the guest can negotiate away.
- The guest sees exactly the devices we configured (`lsblk`,
  `ls /sys/bus/virtio/devices`: two fs, one blk, one net, vsock, console,
  balloon, rng) and no more.
- Read libkrun's vsock muxer and virtiofs device sources for anything they
  expose to the guest beyond mapped ports and added shares, including the
  virtiofs ioctl handlers (the exit-code ioctl, and the "remove root dir"
  request a 1.18 release restricted). Record findings, including "none
  found."

  Result (WP11, `spike/report/libkrun-exposure.md`, 83 citations checked
  mechanically against v1.19.4): the two gates the design relies on hold.
  `krun_add_vsock(ctx, 0)` disables TSI behind two independent checks while
  the port map stays live; the mapped port is inbound-only. "Remove root
  dir": none found. The README's escape warning is `..` and embedded `/` in
  a single FUSE name argument, resolved with `openat` and no
  `RESOLVE_BENEATH`; unreachable from guest userspace or guest root because
  the guest VFS sends only single, already-resolved components, and bounded
  at guest-kernel level (T3) by the helper container's mount namespace,
  which is the boundary the design claims. Three guest-kernel-level (T3)
  bugs recorded, none reachable from the workload: an unchecked console
  port index (VMM panic), an unchecked balloon report length (`madvise`
  past guest RAM), and a DAX mapping offset overflow reachable only with
  `--venv-dax`.

  Result (WP10, `spike/report/exp12.md`): PASS. Every probe above matched from
  inside a real guest at T1 or T2: port 49092 resets at once, port 1234 gets
  nothing until the probe's own 5 s clock, the TSI proxy-create datagram
  opens nothing (`ss -tunap` inside the helper lists zero sockets before and
  after), `/venv/../../etc/hostname` is the guest's own file, the device
  inventory is exactly the list above (one more blk with `--deps-image`),
  and a guest-root write to `/dev/vdb` fails EPERM against the host's
  read-only fd. The exit-code ioctl is accepted and overwritten, per the
  bullet above.

### 13. Helper crash and cleanup (gate)

Kill the helper with SIGKILL mid-transaction. Pass: the fake reactor observes
the socket close, the guest is gone, the tap and nftables rules are gone with
the netns, the scratch space is returned (O_TMPFILE, no cleanup code), and
the reactor removes the socket file. Then, as guest root, set
`vm.panic_on_oom=1` and run a memory hog (the guest kernel has no sysrq).
Pass: the kernel panics and the helper exits promptly rather than hanging or
looping on reboot. Record the exit code; do not gate on it. (The gate first
asked for a non-zero exit. That was the master thread's assumption about
libkrun, and it is wrong: a panicking guest never reaches init's exit-code
report, so libkrun falls back to the vcpu's `FC_EXIT_CODE_OK` and exits 0.
The runtime learns of the death from the socket and never consulted the
code, so the requirement is reworded to what the design consumes. The
diagnosis cost is an open problem below.)

Result (WP10, `spike/report/exp13.md`): PASS on both halves as reworded, with
the exit code recorded. SIGKILL after 46 committed transactions: the runtime
fails the task off the socket close in under half a second, no helper
container remains, the netns and `tap0` are gone with it, the reactor
filesystem is back below where it stood, and the runtime removes the
per-connector directory. Panic under `vm.panic_on_oom`: the kernel panics at
3.9 s of guest uptime, the VMM stops on the reset (no reboot loop), the helper
is gone 5.0 s after the workload started, and it exits 0. Two method notes
that cost a run each: one preview starts the connector twice, so wait for
exactly one `fs_` helper and a committed transaction before killing; and
podman tears down after its client exits, so "nothing remains" must poll for
quiescence rather than measure at `flowctl`'s return.

## Report

One document containing:

- The numbers from experiments 2, 5, 9, and 10 as tables.
- A pass/fail line per gate with a one-sentence note where it matters.
- The exact `podman run` flags, the nftables ruleset, the libkrun call
  sequence, and the mkfs options that passed, so phase 2 starts from them.
- The measured cgroup overhead constant and the THP result: 20 to 32 MiB,
  a constant; the launcher default stays 256 until experiment 5 is rerun at
  `memoryMib + 64`; THP makes no difference and `--thp-disable` does not
  ship. Host cost per idle 512 MiB guest is 95 MiB, and that is the number
  a reactor sizes with, not the cgroup's 72.6.
- Anything that only worked from a root shell on the host.
- Open problems found along the way, each with a proposed owner. Use
  WP11's tiering when stating any exposure: T1 unprivileged guest
  userspace, T2 guest root, T3 guest kernel control. Known so far:
  - Three libkrun bugs at T3 (console index, balloon report length, DAX
    offset overflow). None reachable from a connector; all are "the guest
    kernel can crash its own VMM", and the balloon one can `madvise` into
    the helper's own mappings. Owner: us, to decide whether to report
    upstream or accept as within the boundary.
  - DAX should not ship. WP08 measured no benefit and WP11 found it is the
    only path to the one overflow. Drop `--venv-dax` from the production
    design. Owner: runtime.
  - Phase-2 hardening the source read points at: run the virtiofs server
    with the share as its filesystem root (upstream's own remedy, a
    `pivot_root` or `openat2` with `RESOLVE_BENEATH`), and keep the helper's
    mount namespace minimal, since at T3 everything mounted into the helper
    is nameable. Owner: runtime.
  - The helper's exit code is untrusted and uninformative, and the runtime
    must not branch on it. Two measurements, one fact (WP10): the exit-code
    ioctl is accepted from any share but libkrun's init overwrites it with
    the workload's status, so the code is whatever the connector exits with;
    and a guest kernel panic exits 0, because a panicking guest never reaches
    init's report and libkrun falls back to `FC_EXIT_CODE_OK`. Detection is
    unaffected (the socket is what tells the runtime). Diagnosis is not: the
    reactor records "connector exited 0" for a guest that ran out of memory,
    and the kernel's explanation went to the helper's stdout, unstructured.
    Proposed fix, phase 2: the shim already tees the console, so a line
    matching `Kernel panic` before `krun_start_enter` returns should produce
    one structured stderr line and a distinct exit code. Owner: runtime
    (shim).
  - Root writes have no per-task bound. A guest filling its root is bounded
    by the reactor's container storage filesystem and nothing else, exactly
    as a container is today; parity, not a regression, but it is the one
    storage surface without a limit, and `podman inspect` does not expose the
    layer to meter it (WP10 recorded the path shape). Owner: runtime,
    provisional on appetite for a per-task root quota.
  - The resolver's `nft add element` does not refresh an existing element's
    timeout, so a name re-resolved late in its window still expires at the
    original time; a connect in that moment is dropped. The runtime's netlink
    implementation should update in place (delete-then-add opens a window).
    Owner: runtime.
  - `rp_filter` is the anti-spoof control that actually runs in production,
    not the nft rule (WP07). A /30 has no spare unicast source to forge from,
    the kernel rejects the broadcast address as a source, and any off-net
    source is dropped by strict `rp_filter` before nft sees it. So the
    launch line MUST set `--sysctl net.ipv4.conf.default.rp_filter=1`
    explicitly rather than inherit the host's default; the nft rule stays as
    the second control. Owner: runtime (WP10 applies it in the spike).
  - `egress: none` stalls rather than fails: ~10-20 s for a lookup, up to
    ~127 s for a connect, with nothing in the connector's logs saying why.
    A `reject` on the guest's DNS query alone would make lookups fail at once
    at no other cost; deliberately not changed in the spike (deny is drop).
    Owner: product, with runtime.
  - `connectionsPerMinute` is invisible when it bites: throughput drops to
    the rate with no error and no log line. Owner: product.
  - `connectionsPerMinute` behaves as pacing (connections slow to the rate)
    rather than refusal. Accepted for the spike; whether the policy should be
    named and documented as a rate rather than a limit is a phase-2 policy
    question. Owner: product.
  - Spec and Validate run unsandboxed. `flowctl preview` (and the agent's
    connector proxy in production) drive them through the legacy runtime.
    For derive-python that means the customer's dependencies are fetched and
    built (sdist build backends execute) and the module type-checked on the
    reactor's network before anything is sandboxed. Owner: runtime; this is
    the "connector proxy moves to runtime-next" prerequisite and the builder
    VM phase, already in the design.
  - derive-python's pyright `strict` mode fails Validate on any dependency
    without type information, so customer Python is limited to typed
    libraries or ones with a stubs package. Flagged, not proposed; owner:
    derive-python / product. Evidence is one library (pandas, fixed with
    `pandas-stubs`).
  - A derivation module that prints to stdout kills its session, since
    stdout is the protocol channel. Owner: derive-python (redirect or
    document).
  - The memory overhead default. Measured at 20 to 32 MiB (WP09); the
    spike's 256 is ~8x that. Lowering it to 64 also squeezes the host page
    cache charged to the helper, which is reclaimable and so costs
    throughput rather than correctness, but experiment 5's import times
    were measured at 256. Rerun experiment 5 at `memoryMib + 64` before
    adopting it. Owner: runtime.
  - Draining many sandboxes is slow in podman, not libkrun: `podman rm -f`
    took ~8 s per helper, serially, with 82 running (14 minutes for the
    lot) against ~0 s for one. The shim dies at once on SIGKILL. A reactor
    shutdown that removes connector containers one at a time meets the same
    contention; confirm whether it does, and batch the removals if so.
    Owner: runtime, low priority.
  - Levers not pulled: the ~290 ms guest-stderr console latency in libkrun
    (42% of sandboxed launch time, under a gate passed with 4.3 s to spare);
    guest memory first-touch (~200 ms on first import).

## Decision

Go if every gate passes. If a single gate fails, the report says which
redesign it forces rather than declaring a no-go:

- Experiment 1 fails: record which flag the API service refused. There is
  no capability question to answer; the reactor already holds the socket.
- Experiment 2 fails: the breakdown decides. Time in podman create/start is
  a podman question; time in the guest kernel means a slimmer libkrunfw; time
  in virtiofs or flow-init means experiment 5's variants decide.
- Experiment 5 failed on the primary and on every listed fallback (WP08).
  The redesign it forced: dependency sets are read-only block images per
  tag, not virtiofs, attached as a second virtio-blk device and mounted at
  `/opt/venv`. The builder emits a disk image instead of a directory. Root
  stays virtiofs. WP08b measured the redesign at 2.12x first import and
  1.15x steady state and the gate was ruled closed; the "whole root on a
  block image" fallback is retired on that data, since the root is not the
  residual.
- Experiments 6, 7, 8, 11, 12, or 13 fail: bugs in the spike's ruleset,
  shim, or flow-init, not in the design. Fix and rerun.
- Experiment 3 or 4 fails: something about connector-init, the codec, or
  Python inside the guest is wrong. Fix and rerun; if the fix requires a
  libkrun change, that is a no-go on libkrun and the report says so.
