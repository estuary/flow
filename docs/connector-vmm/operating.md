# Operating connector VMMs

How a data plane's hosts and reactors are prepared to run tasks which select
`vmm: true`, how VMM launches are owned and cleaned up, how to diagnose them,
and the rollout and security limits of this support. What tasks declare is in
[tasks.md](tasks.md).

## Three separate states

- **Code availability.** Reactors and flowctl built from this repository can
  launch VMMs. Nothing does so unless `CONNECTOR_VMM_IMAGE` is set, and no
  deployment template in this repository sets it.
- **Operator preparation.** A host has KVM, a rootful podman, a published VMM
  image, a static `flow-connector-init`, the state and mount paths below, and
  the host boundary installed at boot. Host provisioning and the reactor's
  deployment (its container, unit and environment) are managed outside this
  repository.
- **Production activation.** An operator sets `CONNECTOR_VMM_*` on a data
  plane's reactors, after [the rollout rules](#rollout) hold.

## Hosts

Measured, and the only arrangement proven: an x86_64 GCP VM running Ubuntu
24.04 (kernel 7.0), rootful Podman 4.9.3 with netavark 1.4.0 (whose firewall
driver writes iptables-nft 1.8.10), nft 1.0.9, systemd 255, with Docker 29.1.3
beside it. The VMM image is Fedora 43 with libkrun 1.19.4 and libkrunfw 5.5.0.
The reactor in that proof was a local stack's systemd user unit driving
podman through sudo.

Required:

- **linux/amd64.** The VMM image is built for amd64 only, and connector images
  are pulled as `linux/amd64`.
- **KVM.** `/dev/kvm` (and `/dev/net/tun`) usable by containers of the rootful
  engine. `flow-connector-vmm` checks KVM before it starts libkrun.
- **Rootful podman** as the VMM engine, in the host's PID namespace. VMM
  containers need rootful semantics: `type=image` mounts, devices,
  `CAP_NET_ADMIN`, sysctls and bridge networking.
- **nftables** in the host kernel with the `inet` and `bridge` families, for
  the [host boundary](#the-host-boundary).
- **`openat2`** (Linux 5.6), allowed by the engine's seccomp profile, as
  Podman 4.9.3's default allows it. `flow-connector-vmm` reads a connector
  image's account files through it, and fails the launch without it.

Not measured, so not claimed: Podman 5 or netavark's nftables driver; other
distributions and kernels; hosts with IPv6 disabled; metadata services other
than GCP's; Docker restarting; ordering across a host reboot; a reactor in a
container driving the host's podman service; a remote podman; a launcher
whose podman runs in a PID namespace other than the host's.

## Images and binaries

- **The VMM image**, `ghcr.io/estuary/connector-vmm`, is built from
  `docker/connector-vmm.Dockerfile` by `ci:docker-images` for amd64 only, and
  pushed by the platform build from `master`, tagged as that build's version.
  podman pulls it, with the engine's own registry credentials, the first time
  a boundary command or launch runs it. Check that the host's engine can pull
  the exact tag before activating.
- **The connector image.** `using: python` derivations on V2 run
  `ghcr.io/estuary/derive-python:stable`. The launcher pulls it with the VMM
  engine (a `:local` tag is not pulled).
- **A static `flow-connector-init`**, found beside the launching program or on
  its `PATH`, is copied into every VMM's connector mount and run inside the
  guest, within the connector's image, which need not hold a compatible libc.
  There is no fallback image. The reactor image ships the static musl build
  beside `flowctl-go`; a workspace debug build is dynamically linked, and the
  lookup passes it over.
- **The boundary and the VMM image are one version.** `boundary verify`
  compares the host's tables with exactly what that image's `install` writes,
  so tables and image are upgraded together (see
  [Upgrading](#upgrading-the-boundary-and-image)).

## Configuration

Read from the environment of every process which launches connectors: each
reactor (its V2 task services and its connector proxy), and flowctl for local
commands. Read once, when a service is constructed.

| Variable | Default | Accepted |
|---|---|---|
| `CONNECTOR_VMM_IMAGE` | unset: no VMM capability, and nothing below is read | an image reference the engine can pull |
| `CONNECTOR_VMM_STATE_DIR` | none; required with the image | absolute, UTF-8, no comma, no trailing slash, not `/`, at most 72 bytes |
| `CONNECTOR_VMM_PODMAN` | `podman` | the program run for every VMM engine command |
| `CONNECTOR_VMM_DISK_MIB` | `2048` | digits, 1 to 8796093022207 (the scratch disk, sparse) |
| `CONNECTOR_VMM_MEMORY_OVERHEAD_MIB` | `256` | digits, 1 to 4294967295 |

An empty `CONNECTOR_VMM_*` value means unset. These are shared with ordinary
connectors and read as they read them:

| Variable | Default | In a VMM |
|---|---|---|
| `CONNECTOR_MEMORY_LIMIT` | `1g` | the container's `--memory`, verbatim. Guest RAM is its whole MiB less the overhead, which must leave more than zero and fit 32 bits. Read as podman reads `--memory`: fractions, exponents, units `b k m g t p` with optional `b` or `ib`, any case, one optional space. Only hexadecimal is refused. |
| `CONNECTOR_CPU_LIMIT` | `2` | the container's `--cpus`, verbatim. Guest vCPUs are its value rounded up, 1 to 255. A decimal number with at most nine fraction digits: forms podman also takes, such as `.5` or `1e0`, are refused. |
| `CONNECTOR_CGROUP_PARENT` | unset | the container's `--cgroup-parent` |

For example, `1g` and `2` give a 768 MiB, 2-vCPU guest in a 1 GiB, 2-CPU
container; `1.5g` gives 1280 MiB of guest RAM; a CPU limit of `1.5` gives
2 vCPUs within a 1.5-CPU quota.

**A malformed value fails service construction.** A reactor builds its
connector proxy's service at startup, so it fails to start, and its ordinary
tasks with it; flowctl fails at startup. Whether the host can actually run a
VMM is checked at each launch, not here.

`CONNECTOR_VMM_PODMAN` is run with the launcher's environment, less
`CONSUMER_AUTH_KEYS`, `BROKER_AUTH_KEYS`, `SOPS_AGE_KEY` and `FLOW_AUTH_TOKEN`.
Everything else is inherited, so engine selection, registry authentication,
proxies and a wrapper's own settings still reach it. A wrapper (such as one
running `sudo -n podman`) must keep its stdin open until its command finishes,
as `exec`, sudo and podman do: the commands which create a VMM's network and
container are handed the launch's locked ownership record as stdin.

## Paths, filesystems and permissions

- **`CONNECTOR_VMM_STATE_DIR` must already exist** as a directory; the
  launcher does not create it. It holds each launch's `fv_<16 hex>`
  directory and `fv_<16 hex>.owner` record.
- **A local filesystem with `flock` and `O_TMPFILE`.** Each launch proves the
  latter by opening and sizing an `O_TMPFILE` scratch disk in its own
  `scratch/` before going further. Make it persistent if records are to
  survive a reboot: a lost record leaves its network, and whatever else it
  named, for manual cleanup.
- **Socket length.** `<STATE_DIR>/fv_<16 hex>/sock/init.sock` must fit a
  Unix socket address, so `STATE_DIR` is at most 72 bytes.
- **Connector mounts** are made at `$TMPDIR/connector-mounts-<euid>/mount-fv_<id>`.
  `TMPDIR` must not contain a comma.
- **Same paths for launcher and engine.** The engine binds `STATE_DIR`
  subdirectories and the connector mount into each container by host path,
  so both `STATE_DIR` and `TMPDIR` must name the same directories for the
  launcher and for podman.
- **One user per `STATE_DIR`.** The launcher's UID creates `fv_<id>` (0711)
  with `init/` (0700), `sock/` (0711) and `scratch/` (0700), and the mount
  directories (0711) with `flow-connector-init` (0555) and
  `image-inspect.json` (0444). The VMM makes `init.sock` mode 0777 inside the
  0711 `sock/`, which the launcher's UID dials. A launch under another UID
  cannot remove these, so recovery across users fails and is reported.
- **A local engine.** Creation fencing relies on podman's client running the
  create itself. A remote podman service may finish a create after its client
  died: that can leak a labeled resource, never misattribute one.
- **The engine in the host's PID namespace.** podman records container PIDs as
  it sees them; launchers sharing a store must see the same PID numbering.

## Networks and engines

Ordinary connectors are unchanged: they run under `DOCKER_CLI` (default
`docker`) on the reactor's configured network (`--flow.network`). VMM
containers run under `CONNECTOR_VMM_PODMAN`, each alone on a bridge network of
its own, `fv_<id>`, whose interface is `fvm<12 hex>`. They never join the
ordinary network. The `fvm` interface prefix is reserved on the host: podman
does not notice a non-podman link of the same name, and the boundary treats
every `fvm*` interface as a VMM's.

Each VMM's nameserver is podman's resolver on its own bridge's gateway, the
one destination on the host the boundary admits (UDP/53).

## The host boundary

`flow-connector-vmm boundary` maintains two host nftables tables,
`inet flow_vmm_boundary` and `bridge flow_vmm_boundary`. Keyed on the `fvm*`
interfaces rather than addresses, they keep every VMM from the host, private,
link-local and other special-purpose addresses, sibling VMMs, IPv6, TCP/25 and
inbound connections, even after a VMM has removed its own rules. The rules
are in [`crates/connector-vmm`](../../crates/connector-vmm/README.md#the-host-boundary).

Run it from the VMM image of the launchers' configuration, as root, in the
host's network namespace:

```
podman run --rm --network=host --log-driver=none --read-only \
  --cap-drop=all --cap-add=CAP_NET_ADMIN <CONNECTOR_VMM_IMAGE> boundary <install|verify|remove>
```

- **Install** at boot, before any launcher starts. One nft transaction
  replaces both tables, so a reinstall never passes through a permissive
  state, then verifies. Exit 2 means a table was absent while `fvm*`
  interfaces existed: those VMMs ran unprotected and must be replaced.
- **Verify** is run by the launcher before it creates every VMM's network, on
  every data plane (public, private and local), uncached. A failure refuses
  that launch, and so every VMM task on that reactor, with "the host's VMM
  network boundary did not verify, so this data plane cannot run VMM
  connectors" and the differences found. Ordinary tasks continue. Launchers
  never install or repair the tables. The tables could still be removed
  between a verify and the container attaching; the next verify notices.
- **Ownership.** The tables are host-wide provisioned state, owned by the
  host's provisioning, not by any launcher or task. Every owner of one build
  writes identical tables.
- **Remove** only after stopping every launcher on the host (reactors, and any
  flowctl using its engine) and confirming that they have stopped. `remove`
  refuses while any `fvm*` interface exists, but the absence of bridges does
  not prove it safe: a launch between its verify and its network, or with a
  network created and not yet started, has no bridge yet.

### Upgrading the boundary and image

`verify` from an image passes only against tables identical to what that
image installs. When a build changes the tables, change the host's tables and
the launchers' `CONNECTOR_VMM_IMAGE` together: in between, a launch whose
image disagrees with the host's tables is refused rather than run.
Reinstalling identical tables under traffic was measured to drop nothing.

## Ordinary tooling stays unprivileged

Builds, `mise run ci:nextest-run` and `mise run local:stack` need no KVM,
sudo or rootful podman, and a local stack's reactor has no `CONNECTOR_VMM_*`.
Privileged proof is opt-in: `mise run ci:connector-vmm-kvm` (the component
suite) and `mise run ci:connector-vmm-kvm --platform` (a local stack whose
reactor and flowctl launch VMMs). See
[`crates/connector-vmm-tests`](../../crates/connector-vmm-tests/README.md).

## Lifecycle and recovery

The mechanism is in [`crates/connector`](../../crates/connector/README.md).
In operating terms:

- **A launch owns only what it proves it made.** Its record
  `<STATE_DIR>/fv_<id>.owner` is held under `flock` for the launch's life, and
  its network and container carry its token in the `dev.estuary.vmm-owner`
  label. A live owner's record is never taken, so live launches, including
  other stacks' and other reactors' sharing a host, are never touched.
- **Creation is fenced.** podman's network and container creates hold the
  record's lock, so a create which outlives its launcher (a root podman
  client survives its parent) keeps the record locked until it has finished.
- **A launch which ends normally is torn down by its owner**: Validate, Spec,
  Discover, and a session whose client goes away. Failures to release are
  logged to the task's logs, naming the record kept.
- **A stopped shard or an exited preview is not torn down.** The reactor drops
  a stopped shard's runtime, and flowctl exits after a preview, before
  teardown runs. The VMM ends by itself within seconds of its last RPC and
  `--rm` removes its container; its network definition, state directory,
  record and connector mount remain.
- **Recovery is triggered only by the next VMM launch using the same
  `STATE_DIR`.** Before its own claim, each launch tries the lock of every
  record there and releases those whose owner is gone. Nothing else triggers
  it: not a reactor start, not a timer. A restart alone cleans up nothing
  until a VMM launch follows it, and leftovers are bounded by the VMMs alive
  at the host's last VMM launch. Records beneath a `STATE_DIR` that no longer
  launches, or that was changed, wait indefinitely.
- **Leftovers on a host that will launch no more VMMs.** Either run one more
  VMM launch with the same `STATE_DIR`, `TMPDIR` and engine (for example a
  `flowctl raw spec` of an eligible task), or, with every launcher stopped and
  every record unlocked (`flock -n <record> true` succeeds), remove the
  containers, then the networks, labeled `dev.estuary.vmm-owner=<token>` with
  each record's token, then its `fv_<id>` directory, its mount and the record.
- **Unrecorded `fv_*` resources** (from a launcher which predates records)
  are never touched; [`crates/connector`](../../crates/connector/README.md)
  lists how an operator removes them with launchers stopped.

### Crash and restart behavior

Measured on the local stack only: its reactor is a systemd user unit and
reaches podman through sudo. A SIGKILL of the unit (`systemctl --user kill`)
reports access denied for root's processes, which it cannot signal; systemd
restarts the unit after its `RestartSec` (about 9 seconds there) beside them,
without waiting. The VMM, its conmon and container keep running under podman,
outside the unit's cgroup, until connector-init's idle exit or recovery. The
restarted
reactor's own next VMM launch (in practice, the relaunch of its shard)
recovered everything the dead one owned. Production's arrangement (a
reactor container driving the host's podman) has not been measured: its kill
semantics, restart ordering and remote-engine fencing are unproven.

## Connector mount contract

`CONNECTOR_MOUNT` names a directory, bound read-only at the same absolute path
on the host, in the VMM container and in the guest. It holds
`flow-connector-init`, `image-inspect.json` and, where a producer supplies
credentials, `task-update.json`.

- A producer replaces a file atomically: it writes a new file beside it and
  renames it into place.
- A connector reopens `task-update.json` each time it needs it. A descriptor
  held open keeps reading the generation it opened.
- Measured on one host through the real guest: a replacement reached a fresh
  open about 5 seconds after the rename (5.05 s every time), and a file first
  created after a read was seen at the next one. Those are observations of
  virtiofs caching there, not a latency guarantee.
- Replacement visibility was proven with synthetic files only. No producer
  writes `task-update.json` for VMM launches yet; real credential production
  and refresh are separate work.

## Troubleshooting

| Message | Meaning |
|---|---|
| "this data plane does not support VMM execution" | `CONNECTOR_VMM_IMAGE` is unset for the process which served the request. |
| "connector image '...' is not eligible for VMM execution as a ..." | The task's image or type is not eligible (see [tasks.md](tasks.md#which-tasks-are-eligible)). |
| "the task declares egress, which ordinary execution cannot enforce" | `egress` without `vmm: true`. Catalog validation reports it first, as "declares egress, which its connector's execution cannot enforce". |
| "task egress.hosts ..." or "declares an invalid egress host" | An invalid host name; the message names the rule. |
| "connector Started with execution ..., but ... was requested" | The serving reactor ignored or changed the execution settings: it predates them. See [Rollout](#rollout). |
| "CONNECTOR_VMM_STATE_DIR is required when CONNECTOR_VMM_IMAGE is set", or another variable named with its value | Configuration refused at service construction. |
| "CONNECTOR_VMM_STATE_DIR ..." (FailedPrecondition) | The state directory is missing or not a directory. |
| "opening an O_TMPFILE scratch disk in ..." or "sizing a scratch disk in ..." | The state directory's filesystem cannot hold the scratch disk. |
| "VMM execution runs a static flow-connector-init beside this program or on its PATH" | No `flow-connector-init` found, or only dynamically linked ones, which the message lists. |
| "the host's VMM network boundary did not verify, ..." | Tables absent, or from another build; the verifier's lines name each difference. |
| "flow-connector-vmm: KVM is unavailable to this VMM: ..." then "the VMM exited before flow-connector-init started" | `/dev/kvm` missing or unusable in the VMM container. |
| "failed to connect to the VMM's connector-init at .../init.sock: No such file or directory" | Usually a libkrun panic before readiness; read the lines before it. See below. |
| "timeout waiting for the VMM to become ready" | No readiness within 60 seconds. |

**Misleading dial errors.** A libkrun panic before the guest is ready prints a
backtrace whose indented lines look like connector-init's readiness byte to
the launcher, which then fails dialing a socket never bound. Missing KVM is
caught before libkrun runs; any other early panic still ends this way. The
launch fails safely, but its final message misdirects: the panic lines
before it are the cause.

Egress: each launch logs the egress it applies, with each name's source, and
the VMM logs refused names at `warn`, at most 32 per VMM (see
[tasks.md](tasks.md#what-a-vmm-task-logs)). A name expected to work and
refused is either undeclared or resolving to a non-public address.

Recovery reports go to the reactor's own logs (tracing), not the task's:

- "released the resources of a dead VMM launch": recovery worked.
- "failed to release a resource of a dead VMM launch; its record is kept":
  retried by the next launch. Look for what still holds the resource, such as
  another container on the VMM's network.
- "a VMM ownership record is malformed, so nothing it names is released":
  reported at every launch until an operator inspects and removes it.
- "creating a VMM network failed; retrying under a new id": a name or bridge
  collision; at most three attempts.

## Rollout

**Upgrade every reactor before any VMM-selected request can reach it.** That
means every reactor which could serve the plane's Spec, Validate, Discover,
Apply or Open, not only the reactors running published shards. A reactor
built before this support ignores the execution settings: it may start the
connector ordinarily, and exchange Spec or Validate with it, before the
client notices that `Started` lacks the requested execution and refuses it.
Apply and Open of a published task on such a reactor run ordinarily, with no
check at all. Execution settings travel as a whole, so the same holds for
`egress`: a reactor which cannot read a declaration cannot enforce it.

A control plane built before this support refuses drafts carrying `vmm` or
`egress` as unknown fields, so publishing them waits for its upgrade too.

Configure every reactor of a plane alike. Capability is the plane's: a VMM
task whose request reaches a reactor without `CONNECTOR_VMM_IMAGE`, or whose
host fails preparation, fails there.

VMM tasks run only on the V2 runtime, so the plane must already run V2 tasks
(each reactor with its runtime sidecar, `--flow.sidecar-port`).

Before activation on a production arrangement, verify what the local proof
could not:

- How the reactor reaches podman (CLI in the reactor's container, or the
  host's podman API service), whether creation fencing holds with it, and
  that the engine runs in the host's PID namespace.
- That `STATE_DIR` and `TMPDIR` are the same paths in the reactor and on the
  host, on a local, persistent filesystem with `flock` and `O_TMPFILE`.
- The reactor container's UID, capabilities, namespaces, seccomp and LSM
  profile, and where its credential files live.
- The engine's own defaults: `containers.conf` environment (podman forwards
  proxy variables to containers by default) and `mounts.conf` default mounts,
  which apply to VMM containers too.
- KVM device access for the engine's containers, the `fvm` prefix reserved,
  the boundary installed at boot before the reactor, and the VMM image
  pullable by tag.
- A crash and restart of the production reactor, followed by a VMM launch
  which recovers what it left.

**Rolling back.** Before any reactor returns to a build without this support,
stop VMM-selected requests from reaching it: disable or delete the plane's VMM
tasks, and stop publishing them, first, since an older reactor runs them
ordinarily. To turn VMM
execution off while keeping the build, unset `CONNECTOR_VMM_IMAGE`; VMM tasks
then fail, and never fall back to ordinary execution. Rolling the VMM image
back means reinstalling that image's boundary.

## Security limits

What the design holds: the host boundary's network exclusions, outside the
VMM's authority. They were proven locally against VMMs which had flushed their
own rules, and against traffic forwarded from their guests.

What a compromised VMM can do: remove its own name filtering and any inner
limits, then reach any public destination, and exfiltrate through permitted
destinations or DNS. There are no host-side limits on connection rates or
distinct destinations; outer abuse controls are follow-up work.

What was measured of a VMM's authority, locally: the VMM process runs as host
UID 0 in the host's user namespace, with private PID, mount, network, IPC,
UTS and cgroup namespaces, podman's default capabilities plus
`CAP_NET_ADMIN`, a seccomp filter and the engine's default AppArmor profile.
Probes with those credentials did not reach the launcher's (synthetic)
signing keys, a host-only file, a sibling VMM's mounts or the engine's
management sockets. That is evidence about one host's arrangement, not a
guarantee against
escape: any further flaw in namespaces, the OCI runtime, KVM, virtiofs or the
kernel has root consequences on the host.

Other limits:

- Removing four named secrets from engine subprocesses reduces copies of
  them; it neither removes secrets it does not name nor restrains whoever
  controls a rootful engine, which is root on the host.
- Different UIDs alone do not protect a known 0444 file under 0711 paths:
  another host UID can open it. Connector mounts rely on mount namespaces and
  unreachable paths, not on file ownership.
- The guest's writable root, podman's container layer, is bounded by host
  disk alone; `CONNECTOR_VMM_DISK_MIB` bounds only the scratch disk.
- Stronger confinement (a non-root or user-namespaced VMM, a restricted
  broker in front of the engine), writable-root limits and outer abuse
  controls are follow-ups.
