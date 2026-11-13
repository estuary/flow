# Operating connector VMMs

How a data plane's hosts and reactors are prepared to run tasks which select
`vmm: true`, how VMM launches are owned and cleaned up, how to diagnose them,
and the rollout and security limits of this support. What tasks declare is in
[tasks.md](tasks.md).

## Three separate states

- **Code availability.** Reactors and flowctl built from this repository can
  launch VMMs. Nothing does so unless `CONNECTOR_VMM_STATE_DIR` is set, and no
  deployment template in this repository sets it.
- **Operator preparation.** A host has KVM, a rootful podman, a published VMM
  image, a static `flow-connector-init`, the state and mount paths below, and
  the host boundary installed at boot. Host provisioning and the reactor's
  deployment (its container, unit and environment) are managed outside this
  repository.
- **Production activation.** An operator sets `CONNECTOR_VMM_*` on a data
  plane's reactors, after [the rollout rules](#rollout) hold.

## Hosts

Tested on x86_64 Ubuntu 24.04 with rootful Podman 4.9.3. Required:

- **linux/amd64.** The VMM image is built for amd64 only, and connector images
  are pulled as `linux/amd64`.
- **KVM.** `/dev/kvm` (and `/dev/net/tun`) usable by containers of the rootful
  engine. `flow-connector-vmm` checks KVM before it starts libkrun.
- **Rootful podman** as the VMM engine, in the host's PID namespace. VMM
  containers need rootful semantics: image-backed volumes, devices,
  capabilities, sysctls and bridge networking. Each VMM container drops
  podman's default capabilities and gets `CAP_CHOWN`, `CAP_DAC_OVERRIDE`,
  `CAP_FOWNER`, `CAP_FSETID`, `CAP_NET_ADMIN`, `CAP_NET_BIND_SERVICE`,
  `CAP_SETGID` and `CAP_SETUID`; [`crates/connector-vmm`](../../crates/connector-vmm/README.md#disks-and-shares)
  says what each is for.
- **nftables** in the host kernel with the `inet` and `bridge` families, for
  the [host boundary](#the-host-boundary).
- **`openat2`** (Linux 5.6), allowed by the engine's seccomp profile, as
  Podman 4.9.3's default allows it. `flow-connector-vmm` reads a connector
  image's account files through it, and fails the launch without it.

## Images and binaries

- **The VMM image** is `ghcr.io/estuary/connector-vmm:<version>`, amd64
  only, where `<version>` is the reactor's own build: `git describe` of its
  source, which the platform build also tags the image with. Nothing
  configures it. The engine pulls it with its own registry credentials when
  a boundary command or launch first needs it, so check that the host can
  pull the tag before activating. A released flowctl is built as its release
  tag, which no image is published under, so it needs `CONNECTOR_VMM_IMAGE`.
- **The connector images** are `ghcr.io/estuary/capture-python`,
  `derive-python` and `materialize-python`, at `:stable` or their
  `custom-image-tag` shard flag's tag. The VMM engine pulls each.
- **A static `flow-connector-init`**, beside the launching program or on its
  `PATH`, is copied into every VMM's connector mount. The reactor image
  ships it beside `flowctl-go`.

## Configuration

Read from the environment of every process which launches connectors: each
reactor (its V2 task services and its connector proxy), and flowctl for local
commands. Read once, when a service is constructed.

| Variable | Default | Accepted |
|---|---|---|
| `CONNECTOR_VMM_STATE_DIR` | unset: no VMM capability, and nothing below is read | absolute, UTF-8, no comma, no trailing slash, not `/`, at most 72 bytes |
| `CONNECTOR_VMM_IMAGE` | `ghcr.io/estuary/connector-vmm:<version>`, this build's image | an image reference the engine can pull; for development and tests |
| `CONNECTOR_VMM_PODMAN` | `podman` | the program run for every VMM engine command |
| `CONNECTOR_VMM_DISK_MIB` | `2048` | digits, 1 to 8796093022207 (the scratch disk, sparse) |
| `CONNECTOR_VMM_MEMORY_OVERHEAD_MIB` | `256` | digits, 1 to 4294967295 |

An empty `CONNECTOR_VMM_*` value means unset. These are shared with ordinary
connectors and read as they read them:

| Variable | Default | In a VMM |
|---|---|---|
| `CONNECTOR_MEMORY_LIMIT` | `1g` | the container's `--memory`. Guest RAM is its whole MiB less the overhead. Read as the Docker CLI and podman read it: a decimal number, then optionally `b`, or `k`, `m`, `g`, `t` or `p` with an optional `b` or `ib`. |
| `CONNECTOR_CPU_LIMIT` | `2` | the container's `--cpus`. Guest vCPUs are its value rounded up, 1 to 255. |
| `CONNECTOR_CGROUP_PARENT` | unset | the container's `--cgroup-parent` |

**A malformed value fails service construction**, except
`CONNECTOR_MEMORY_LIMIT`. A reactor builds its connector proxy's service at
startup, so it fails to start, and its ordinary tasks with it; flowctl fails
at startup. A memory limit which cannot size a VMM (`0`, or one at most the
overhead) refuses each VMM start instead, with "this data plane cannot size
a VMM", while ordinary tasks run under it.

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
  with `sock/` and `scratch/` (both 0700), and the mount
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
explicit internal routes.

Each VMM's nameserver is podman's resolver on its own bridge's gateway, the only
guest-initiated destination on the host the boundary admits (UDP/53).
Replies to host-authorized container networking connections are also allowed.

## The host boundary

`flow-connector-vmm boundary` maintains two host nftables tables,
`inet flow_vmm_boundary` and `bridge flow_vmm_boundary`. Keyed on the `fvm*`
interfaces rather than addresses, they keep every VMM from the host, private,
link-local and other special-purpose addresses, Azure's platform address
`168.63.129.16`, sibling VMMs, IPv6, TCP/25 and inbound connections, even
after a VMM has removed its own rules. Declared CIDRs can authorize private
and Azure platform destinations. The rules are in [`crates/connector-vmm`](../../crates/connector-vmm/README.md#the-host-boundary).

Run it from the VMM image of the launchers' configuration, as root, in the
host's network namespace:

```
podman run --rm --network=host --log-driver=none --read-only \
  --cap-drop=all --cap-add=CAP_NET_ADMIN ghcr.io/estuary/connector-vmm:<version> boundary <install|verify|remove>
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
image installs. When a build changes the tables, change the host's tables
with the reactors: in between, a launch whose
image disagrees with the host's tables is refused rather than run.

Every reactor upgrade is a VMM image upgrade too: a reactor launches the
image of its own build.

The image pins the upstream versions of libkrun, libkrunfw and nftables.
Moving nftables changes how `verify` reads the host's tables;
[`crates/connector-vmm`](../../crates/connector-vmm/README.md#image) has
the checks such a move must pass.

## Lifecycle and recovery

[`crates/connector`](../../crates/connector/README.md) points at the
mechanism. What an operator needs:

- **A launch owns what its record names.** `<STATE_DIR>/fv_<id>.owner` is
  held under `flock` for the launch's life. Its network, rootfs volume and
  containers carry its token in the `dev.estuary.vmm-owner` label and its
  `STATE_DIR` in `dev.estuary.vmm-state-dir`. A live owner's resources are
  never touched.
- **A stopped shard's VMM waits for recovery.** The reactor drops a stopped
  shard's runtime before teardown runs. The VMM exits within seconds of its
  last RPC; its volume, network, state directory, record and mount remain
  for the next recovery pass.
- **Each reactor recovers its `STATE_DIR` for as long as it runs.** A pass
  starts at startup and another 30 seconds after each ends. It releases
  records whose owner is gone, then labeled resources whose record is gone.
  Failures are retried every pass.
- **Never prune exited `fv_<id>-ingress` containers.** Until a pass records
  one, its labels may be the only record of a host grant. If one is removed
  unrecorded, its grant stays installed as the nftables table
  `flow_vmm_ingress_<token>`, which `boundary revoke --owner=<token>` from
  any VMM image removes.
- **Keep an older VMM image while records name it.** Recovery revokes a
  grant with the image ID its record holds.
- **Cleaning a host which will run no more reactors.** With every launcher
  stopped and every record unlocked (`flock -n <record> true` succeeds),
  for each record: remove the containers labeled with its token, revoke its
  grant if it names an `ingress_image`, remove the networks and volumes so
  labeled, then its `fv_<id>` directory, mount and record. Last, remove
  what is labeled with the `STATE_DIR` and has no record, revoking each
  `fv_<id>-ingress` container's grant before removing it.

## Troubleshooting

| Message | Meaning |
|---|---|
| "this data plane does not support VMM execution" | `CONNECTOR_VMM_STATE_DIR` is unset for the process which served the request. |
| "connector image '...' is not eligible for VMM execution as a ..." | The task's image or type is not eligible (see [tasks.md](tasks.md#which-tasks-are-eligible)). |
| "the task declares egress, which ordinary execution cannot enforce" | `egress` without `vmm: true`. Catalog validation reports it first, as "declares egress, which its connector's execution cannot enforce". |
| "the task requests VMM execution but declares no egress" | `vmm: true` without `egress`. Declare the hosts the task needs, or `egress: {}` for none. |
| "task egress.hosts ..." or "declares an invalid egress host" | An invalid host name; the message names the rule. |
| "task egress.cidrs ..." or "declares an invalid egress CIDR" | An invalid address range; the message names the rule. |
| "the task declares egress.cidrs, which only private data planes accept" | `egress.cidrs` on a public data plane, which never accepts one. |
| "connector Started with execution ..., but ... was requested" | The serving reactor ignored or changed the execution settings: it predates them. See [Rollout](#rollout). |
| A `CONNECTOR_VMM_*` variable named with its value | Configuration refused at service construction. |
| "CONNECTOR_VMM_STATE_DIR ..." (FailedPrecondition) | The state directory is missing, a link, owned by another UID, or not mode 0700. |
| "opening an O_TMPFILE scratch disk in ..." or "sizing a scratch disk in ..." | The state directory's filesystem cannot hold the scratch disk. |
| "VMM execution runs a static flow-connector-init beside this program or on its PATH" | No `flow-connector-init` found, or only dynamically linked ones, which the message lists. |
| "the host's VMM network boundary did not verify, ..." | Tables absent, or from another build; the verifier's lines name each difference. |
| "pulling VMM image ..." or "inspecting VMM image ..." (FailedPrecondition) | The engine lacks this build's VMM image and cannot pull it, or cannot name its ID. |
| "this data plane cannot size a VMM: CONNECTOR_MEMORY_LIMIT ..." | The memory limit cannot size a VMM's guest (see [Configuration](#configuration)). Ordinary tasks are unaffected. |
| "flow-connector-vmm: KVM is unavailable to this VMM: ..." then "the VMM exited (exit status: 2) before flow-connector-init was ready" | `/dev/kvm` missing or unusable in the VMM container. |
| "the VMM exited (exit status: 0) before flow-connector-init was ready" | Possible cause: connector-init's idle watchdog ended the guest because no connector RPC reached it within 5 seconds of its start. Check debug logs for connector-init's start and exit times. |
| "timeout waiting for the VMM to become ready" | No readiness within 60 seconds. |

Egress: each launch logs the egress it applies, with each name's source, and
the VMM logs refused names at `warn`, at most 32 per VMM (see
[tasks.md](tasks.md#what-a-vmm-task-logs)). A name expected to work and
refused is either undeclared, declared by a task whose `egress` is empty, or
resolving to a non-public address outside the task's `egress.cidrs`.

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
task whose request reaches a reactor without `CONNECTOR_VMM_STATE_DIR`, or whose
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
  launchers own: exited `fv_<id>-ingress` containers, VMM images a
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
- That the plane's `CONNECTOR_MEMORY_LIMIT` sizes a guest (see
  [Configuration](#configuration)), else VMM starts are refused.

**Rolling back.** Before any reactor returns to a build without this support,
stop VMM-selected requests from reaching it: disable or delete the plane's VMM
tasks, and stop publishing them, first, since an older reactor runs them
ordinarily. To turn VMM
execution off while keeping the build, unset `CONNECTOR_VMM_STATE_DIR`; VMM tasks
then fail, and never fall back to ordinary execution. Rolling the VMM image
back means reinstalling that image's boundary.

## Security limits

- The host boundary's network exclusions hold outside the VMM's authority,
  including against a VMM which has flushed its own rules.
- A compromised VMM can remove its own name filtering and reach any public
  destination. Connection rates and distinct destinations are not limited.
- The VMM process is host UID 0 in the host's user namespace, confined by
  namespaces, the capabilities above, seccomp and the engine's AppArmor
  profile. A flaw in any of those, the OCI runtime, KVM or virtiofs has root
  consequences on the host.
- Whoever controls the rootful engine is root on the host. Removing four
  named secrets from engine subprocesses only reduces copies of them.
- A guest writes only to its scratch disk, bounded by
  `CONNECTOR_VMM_DISK_MIB`, or a persistent disk. Scratch disks are sparse,
  so the state directory's filesystem must hold the sum of those in use.
