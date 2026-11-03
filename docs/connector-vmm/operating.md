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

The tested host configuration is an x86_64 GCP VM running Ubuntu
24.04 (kernel 7.0), rootful Podman 4.9.3 with netavark 1.4.0 (whose firewall
driver writes iptables-nft 1.8.10), nft 1.0.9, systemd 255, with Docker 29.1.3
beside it. The VMM image is Fedora 43 with libkrun 1.19.4 and libkrunfw 5.6.2.
The integration tests used a local stack's reactor running as a systemd
user unit and driving podman through sudo.

Required:

- **linux/amd64.** The VMM image is built for amd64 only, and connector images
  are pulled as `linux/amd64`.
- **KVM.** `/dev/kvm` (and `/dev/net/tun`) usable by containers of the rootful
  engine. `flow-connector-vmm` checks KVM before it starts libkrun.
- **Rootful podman** as the VMM engine, in the host's PID namespace. VMM
  containers need rootful semantics: image-backed volumes, devices,
  `CAP_NET_ADMIN`, sysctls and bridge networking.
- **nftables** in the host kernel with the `inet` and `bridge` families, for
  the [host boundary](#the-host-boundary).
- **`openat2`** (Linux 5.6), allowed by the engine's seccomp profile, as
  Podman 4.9.3's default allows it. `flow-connector-vmm` reads a connector
  image's account files through it, and fails the launch without it.

Configurations not yet tested include Podman 5 or netavark's nftables
driver; other distributions and kernels; hosts with IPv6 disabled; metadata services other
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
| `CONNECTOR_MEMORY_LIMIT` | `1g` | the container's `--memory`, as the decimal byte count it is read as. Guest RAM is its whole MiB less the overhead, which must leave more than zero and fit 32 bits. Whole bytes (digits), or a number of MiB or GiB suffixed lowercase `m` or `g`, which may have up to nine fraction digits; a fraction is floored to whole bytes. Other spellings podman takes, such as `1G`, `1gb`, `512k`, `1e3m` or `512 m`, are refused, as is a fraction without `m` or `g`. |
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
proxies and a wrapper's own settings still reach it.

## Paths, filesystems and permissions

- **`CONNECTOR_VMM_STATE_DIR` must already exist** as a real directory owned
  by the launcher's UID, mode 0700. Links and other permissions are refused
  before recovery or launch. The launcher does not create it. It holds each
  launch's `fv_<16 hex>` directory and mode-0600 `fv_<16 hex>.owner` record.
  Provision it outside any scratch tree cleared when the reactor starts.
  Existing records remain recoverable by the same UID beneath this private
  directory.
- **A local filesystem with `flock` and `O_TMPFILE`.** Each launch proves the
  latter by opening and sizing an `O_TMPFILE` scratch disk in its own
  `scratch/` before going further. Make it persistent if records are to
  survive a reboot. After a record is lost, recovery removes its network,
  rootfs volume and container, which carry the `STATE_DIR` label, and
  revokes its grant. Its directories are left for manual cleanup.
- **Socket length.** `<STATE_DIR>/fv_<16 hex>/sock/init.sock` must fit a
  Unix socket address, so `STATE_DIR` is at most 72 bytes.
- **Connector mounts** are made at `$TMPDIR/connector-mounts-<euid>/mount-fv_<id>`.
  The shared parent is owned by the launcher, mode 0700. Each child is 0711
  and explicitly bound into its container, so image-defined UIDs can read it
  without access to the host ancestor. Ordinary connectors use this same
  private parent. `TMPDIR` must not contain a comma. All launchers sharing
  that parent must use this permission contract; an older launcher can restore
  its mode to 0711.
- **Same paths for launcher and engine.** The engine binds `STATE_DIR`
  subdirectories and the connector mount into each container by host path,
  so both `STATE_DIR` and `TMPDIR` must name the same directories for the
  launcher and for podman.
- **One user per `STATE_DIR`.** The launcher's UID creates `fv_<id>` (0711)
  with `init/`, `sock/` and `scratch/` (all 0700), and the mount
  directories (0711) with `flow-connector-init` (0555) and
  `image-inspect.json` (0444). The VMM makes `init.sock` mode 0777 inside the
  0700 `sock/`, which the launcher's UID dials. A launch under another UID
  cannot remove these, so recovery across users fails and is reported.
- **The engine in the host's PID namespace.** podman records container PIDs as
  it sees them; launchers sharing a store must see the same PID numbering.

## Networks and engines

Ordinary connectors run under `DOCKER_CLI` (default
`docker`) on the reactor's configured network (`--flow.network`). VMM
containers run under `CONNECTOR_VMM_PODMAN`, each alone on a bridge network of
its own, `fv_<id>`, whose interface is `fvm<12 hex>`. They never join the
ordinary network. The `fvm` interface prefix is reserved on the host: podman
does not notice a non-podman link of the same name, and the boundary treats
every `fvm*` interface as a VMM's.

Static outbound routing is a host-provisioning responsibility. The guest is
masqueraded to its VMM container's bridge address before host policy routing.
That address must select the plane's designated NAT path while preserving
explicit internal routes. See [static outbound routing](static-outbound.md)
for the deployment candidate, packet paths, and isolated regression command.

Each VMM's nameserver is podman's resolver on its own bridge's gateway, the only
guest-initiated destination on the host the boundary admits (UDP/53).
Replies to host-authorized container networking connections are also allowed.

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
Privileged integration tests are opt-in: `mise run ci:connector-vmm-kvm`
(the component suite) and `mise run ci:connector-vmm-kvm --platform` (a local stack whose
reactor and flowctl launch VMMs). See
[`crates/connector-vmm-tests`](../../crates/connector-vmm-tests/README.md).

## Lifecycle and recovery

The mechanism is in [`crates/connector`](../../crates/connector/README.md).
In operating terms:

- **A launch owns only what it proves it made.** Its record
  `<STATE_DIR>/fv_<id>.owner` is held under `flock` for the launch's life, and
  its network, rootfs volume and container carry its token in the `dev.estuary.vmm-owner`
  label, and its `STATE_DIR` in the `dev.estuary.vmm-state-dir` label. A live
  owner's record is never taken, so live launches, including other stacks'
  and other reactors' sharing a host, are never touched.
- **A create can finish after its launch is released.** A root podman client
  survives its parent, and the podman service finishes a create whose client
  has died. Either way, a network, volume or container can appear after
  recovery has released its launch and removed the record. A later recovery
  pass removes it (below).
- **A launch which ends normally is torn down by its owner**: Validate, Spec,
  Discover, and a session whose client goes away. Failures to release are
  logged to the task's logs, naming the record kept.
- **A stopped shard is not torn down.** The reactor drops a stopped shard's
  runtime before teardown runs. The VMM ends by itself within seconds of its
  last RPC and `--rm` removes its container; its rootfs volume, network
  definition, state directory, record and connector mount remain for
  recovery. An exiting flowctl waits up to 30 seconds for its own VMMs'
  teardowns; a killed one leaves them to recovery.
- **Each reactor recovers its `STATE_DIR` in the background, for as long as
  it runs.** Its connector proxy, which it builds at startup whatever shards
  it runs, owns one recovery. A pass starts at once, and another 30 seconds
  after each pass ends; passes never overlap. A launch recovers nothing, so
  it never waits on a pass. A pass first checks `STATE_DIR` as a launch
  does, and touches nothing if that fails. It tries the lock of every record
  there and releases those whose owner is gone. It then removes each
  container, network and rootfs volume labeled with its `STATE_DIR` whose
  `fv_<id>.owner` is gone. So leftovers are retried every pass, indefinitely,
  launches or not. A reactor which stops abandons its pass where it is,
  waiting on nothing; a record outlives every step, so the next pass, after a
  restart, finishes. Reactors sharing a `STATE_DIR` each recover it; a record
  is released by whichever takes its lock. A VMM-capable flowctl also
  recovers its `STATE_DIR` while it runs, at best: it may exit mid-pass.
- **A late grant is recorded before it is removed.** A remote `podman run`
  sends its create and start apart, so an ingress grant's container can be
  created, or started, after its launch's release has revoked the grant and
  removed the record. Grant containers are kept after they exit, unlike
  every other boundary run, so their labels remain. A pass which finds one
  with no record first writes a record naming its owner token and the ID of
  its image, then removes the container, so a start not yet served fails,
  then revokes with that image, and removes that record last.
- **Verify and revoke runs are scoped to their launch too.** Their containers
  are `fv_<id>-verify` and `fv_<id>-revoke`, with the launch's owner token and
  `STATE_DIR` labels, and `--rm` removes each once it exits. One podman
  created and never started, as when its client died first, stays. While
  its record remains, the record's release removes it with the launch's
  other containers; once the record is gone, a pass removes it as it is,
  since neither grants anything.
- **Exited grant containers are durable evidence.** Until a pass has
  recorded it, an `fv_<id>-ingress` container's labels may be the only
  record of a grant on the host. Nothing else may remove one: not
  `podman container prune` or `podman system prune`, a reactor's or host's
  startup cleanup, nor an operator clearing exited containers. One removed
  unrecorded leaves its grant installed, with nothing to find it by but its
  nftables table, `flow_vmm_ingress_<token>`, which `boundary revoke
  --owner=<token>` from any VMM image removes. A recorded grant whose image
  has since been removed fails its revoke at every pass; revoke it so, then
  remove its record.
- **Leftovers on a host that will run no more reactors.** Either run a
  VMM-capable flowctl command with the same `STATE_DIR`, `TMPDIR` and engine
  which outlasts its first recovery pass (for example a `flowctl raw spec` of
  an eligible task, which launches a VMM), or, with every launcher stopped and
  every record unlocked (`flock -n <record> true` succeeds), remove the
  containers labeled `dev.estuary.vmm-owner=<token>` with each record's
  token, then revoke its grant if it names an `ingress_image`
  (`boundary revoke --owner=<token>` from that image), then remove the
  networks and rootfs volumes so labeled, then its `fv_<id>` directory, its
  mount and the record. Last, remove those labeled
  `dev.estuary.vmm-state-dir=<STATE_DIR>` whose `fv_<id>.owner` is gone,
  revoking the grant of each `fv_<id>-ingress` container by its
  `dev.estuary.vmm-owner` label before removing it.
- **Unrecorded `fv_*` resources without the `dev.estuary.vmm-state-dir`
  label** (from a launcher which predates it) are never touched;
  [`crates/connector`](../../crates/connector/README.md) lists how an operator
  removes them with launchers stopped.

### Crash and restart behavior

Measured on the local stack only: its reactor is a systemd user unit and
reaches podman through sudo. A SIGKILL of the unit (`systemctl --user kill`)
reports access denied for root's processes, which it cannot signal; systemd
restarts the unit after its `RestartSec` (about 9 seconds there) beside them,
without waiting. The VMM, its conmon and container keep running under podman,
outside the unit's cgroup, until connector-init's idle exit or recovery.

The restarted reactor's first recovery pass, at its startup, releases what
the dead one owned, and a create still running then is removed by a later
pass. The KVM suite checks this with processes killed mid-create and
mid-release, and a recovery then started with no launch. Through a podman API
service, it checks late grants, created or started after their release, and a
create the service accepted and held past its client's death and the
release of its record. A kill and restart of the reactor unit itself has not
been measured with this recovery. Production's arrangement (a
reactor container driving the host's podman) has not been tested for kill
semantics or restart ordering.

## Connector mount contract

`CONNECTOR_MOUNT` names a directory, bound read-only at the same absolute path
on the host and in the VMM container, and at `/connector-mount` in the guest. It holds
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
- With a task identity and configured `TaskUpdate`, ordinary and VMM launches
  write the credential before startup and periodically replace it until
  teardown. Task-less Spec calls and local preview have no credential.
- Host paths may be visible in bridge names, ownership records, engine argv,
  container metadata and debug logs. The private host ancestor protects the
  source even when its exact path is known. Processes running as the launcher
  UID, host root and users controlling the engine are trusted. A connector's
  mount namespace exposes only its own child directory.

## Troubleshooting

| Message | Meaning |
|---|---|
| "this data plane does not support VMM execution" | `CONNECTOR_VMM_IMAGE` is unset for the process which served the request. |
| "connector image '...' is not eligible for VMM execution as a ..." | The task's image or type is not eligible (see [tasks.md](tasks.md#which-tasks-are-eligible)). |
| "the task declares egress, which ordinary execution cannot enforce" | `egress` without `vmm: true`. Catalog validation reports it first, as "declares egress, which its connector's execution cannot enforce". |
| "task egress.hosts ..." or "declares an invalid egress host" | An invalid host name; the message names the rule. |
| "task egress.cidrs ..." or "declares an invalid egress CIDR" | An invalid address range; the message names the rule. |
| "the task declares egress.cidrs, which only private data planes accept" | `egress.cidrs` on a public data plane, which never accepts one. |
| "connector Started with execution ..., but ... was requested" | The serving reactor ignored or changed the execution settings: it predates them. See [Rollout](#rollout). |
| "CONNECTOR_VMM_STATE_DIR is required when CONNECTOR_VMM_IMAGE is set", or another variable named with its value | Configuration refused at service construction. |
| "CONNECTOR_VMM_STATE_DIR ..." (FailedPrecondition) | The state directory is missing, a link, owned by another UID, or not mode 0700. |
| "opening an O_TMPFILE scratch disk in ..." or "sizing a scratch disk in ..." | The state directory's filesystem cannot hold the scratch disk. |
| "VMM execution runs a static flow-connector-init beside this program or on its PATH" | No `flow-connector-init` found, or only dynamically linked ones, which the message lists. |
| "the host's VMM network boundary did not verify, ..." | Tables absent, or from another build; the verifier's lines name each difference. |
| "flow-connector-vmm: KVM is unavailable to this VMM: ..." then "the VMM exited (exit status: 2) before flow-connector-init answered its health check" | `/dev/kvm` missing or unusable in the VMM container. |
| "the VMM exited (exit status: 0) before flow-connector-init answered its health check" | Possible cause: connector-init's idle watchdog ended the guest because no connector RPC reached it within 5 seconds of its start. Check debug logs for connector-init's start and exit times. |
| "timeout waiting for the VMM to become ready" | No readiness within 60 seconds. |

Egress: each launch logs the egress it applies, with each name's source, and
the VMM logs refused names at `warn`, at most 32 per VMM (see
[tasks.md](tasks.md#what-a-vmm-task-logs)). A name expected to work and
refused is either undeclared or resolving to a non-public address outside
the task's `egress.cidrs`.

Recovery reports go to the reactor's own logs (tracing), not the task's. Its
info lines show only where the connector proxy's log level admits them. Each
error names its `subject`. A problem which persists is logged when first
seen, then again once ten minutes have passed (`repeats_after`), though every
pass retries it:

- "released the resources of a dead VMM launch": recovery worked.
- "failed to release a resource of a dead VMM launch; its record is kept":
  retried by every pass. Look for what still holds the resource, such as
  another container on the VMM's network.
- "a VMM ownership record is malformed, so nothing it names is released":
  reported until an operator inspects and removes it.
- "removed a VMM resource whose record is gone": a podman service finished a
  create after its launch was released, and recovery removed what it made.
- "recorded a VMM ingress grant whose record was gone": a grant's container
  was created after its launch's release; recovery recorded, removed and
  revoked it.
- "failed to list VMM resources by their state directory", "failed to
  remove a VMM resource whose record is gone", or "failed to record a VMM
  ingress grant whose record is gone, so it is kept": retried by every pass.
- "a VMM resource has no record that can be looked for, so it is kept": it
  carries this `STATE_DIR`'s label, but either its name is not a launch's or
  the state directory could not be read. Reported until an operator inspects
  it.
- "the VMM state directory is unsafe to recover, so nothing is released":
  `CONNECTOR_VMM_STATE_DIR` fails the checks a launch makes of it.
- "creating a VMM network failed; retrying under a new id": a name or bridge
  collision; at most three attempts.

A reactor whose recovery has started exports:

| Metric | Meaning |
|---|---|
| `flow_vmm_recovery_failed_passes_total` | Passes which left a problem. |
| `flow_vmm_recovery_pending` | Problems the last pass left: resources, records or listings to retry, or to inspect. |
| `flow_vmm_recovery_last_success_timestamp_seconds` | When the last pass which left no problem ended, or zero. |

A stale last success, with pending problems, means leftovers are
accumulating; the reactor's logs name them. Neither fails the reactor's
readiness nor holds up a launch.

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

Before production activation, verify the deployment details not covered by
the local integration tests:

- How the reactor reaches podman (CLI in the reactor's container, or the
  host's podman API service), and that the engine runs in the host's PID
  namespace.
- That `STATE_DIR` and `TMPDIR` are the same paths in the reactor and on the
  host, on a local filesystem with `flock` and `O_TMPFILE`. Keep `STATE_DIR`
  private to the launcher and persistent across reactor restarts. Reconcile
  any startup scratch cleanup with VMM resource recovery before enabling it.
- That nothing on the host prunes containers, images or volumes the
  launchers own: exited `fv_<id>-ingress` containers (above), VMM images a
  recorded grant names, rootfs volumes, and `STATE_DIR` and `TMPDIR`
  beneath a cleanup of scratch space. Audit the hosts' timers, the
  deployment's provisioning and any startup cleanup for them.
- The reactor container's UID, capabilities, namespaces, seccomp and LSM
  profile, and where its credential files live.
- The engine's own defaults: `containers.conf` environment (podman forwards
  proxy variables to containers by default) and `mounts.conf` default mounts,
  which apply to VMM containers too.
- KVM device access for the engine's containers, the `fvm` prefix reserved,
  the boundary installed at boot before the reactor, and the VMM image
  pullable by tag.
- A crash and restart of the production reactor, whose recovery then
  releases what it left with no launch, and its recovery metrics scraped.
- That the plane's `CONNECTOR_MEMORY_LIMIT` fits the
  [VMM grammar](#configuration) before enabling `CONNECTOR_VMM_IMAGE`.

**Rolling back.** Before any reactor returns to a build without this support,
stop VMM-selected requests from reaching it: disable or delete the plane's VMM
tasks, and stop publishing them, first, since an older reactor runs them
ordinarily. To turn VMM
execution off while keeping the build, unset `CONNECTOR_VMM_IMAGE`; VMM tasks
then fail, and never fall back to ordinary execution. Rolling the VMM image
back means reinstalling that image's boundary.

## Security limits

What the design holds: the host boundary's network exclusions, outside the
VMM's authority. Local integration tests check these exclusions against VMMs
which have flushed their own rules, and against traffic forwarded from their
guests.

What a compromised VMM can do: remove its own name filtering, then reach any
public destination, and exfiltrate through permitted destinations or DNS.
Connection rates and distinct destinations are not limited.

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
- Each connector receives its own read-only mount. On the host, the
  launcher-owned 0700 ancestor blocks other UIDs even when the child path is
  known. A process sharing the launcher UID can read its credentials.
- The guest's root is read-only in podman and in libkrun, so no guest,
  not even one with a compromised kernel, writes podman's storage. Everything
  a guest writes to disk goes to its scratch disk (`/tmp`, `/var/tmp`,
  `TMPDIR`, uv's cache), bounded by `CONNECTOR_VMM_DISK_MIB`, or to a
  persistent disk, bounded only by whatever quota its host directory has.
- That bound is per VMM. Scratch disks are sparse, so the state directory's
  filesystem must hold the sum of those in use; a host that fills first shows
  its guests I/O errors. `/dev/shm`, and any tmpfs guest root mounts, are RAM,
  bounded by guest memory and the container's `memory.max`.
- Stronger confinement (a non-root or user-namespaced VMM, a restricted
  broker in front of the engine), aggregate scratch capacity, persistent-disk
  quotas and outer abuse controls are follow-ups.
