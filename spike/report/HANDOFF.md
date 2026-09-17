# libkrun sandbox: handoff to phase 2

For the person writing the phase-2 development plan. Read this first, then
`REPORT.md` for any number or configuration, then `../CONTRACTS.md` when you
start writing code. Those three files are the handoff; everything else under
`spike/` is provenance (the ledger `../STATUS.md`, the plan `../PLAN.md`, the
per-experiment reports `exp*.md`) and the report cites it where it matters.

The spike's decision is **go**: every gate passes, libkrun v1.19.4 is
unpatched, and no libkrun change is on the critical path. This document says
what exists, what carries forward, and what phase 2 has to build.

Design issue: (link to be added by the author).

This document assumes PR 3490 ("connector: a V2 connector service, served
in-process and over the network") lands much as it stands. It moves the
connector launcher out of `runtime-next` into a new `crates/connector` crate
and has the reactor serve a `connector.Connector` protocol. The carry-forward
row for the spike switch and items R1, R2 and R3 are written against that
shape; if the PR changes materially, revisit those four.

Three decisions of 2026-09-17 are folded in below and marked with that date:
there is no builder and no pre-flight dependency install (B1 is dropped;
dependencies install at task start over allowed egress, onto the scratch
disk); CPU and memory limits come from the launcher's existing
`CONNECTOR_MEMORY_LIMIT` and `CONNECTOR_CPU_LIMIT` environment variables
rather than anything new; and the egress list travels as an image label, read
at inspect time the way `FLOW_RUNTIME_PROTOCOL` already is, rather than as a
catalog-model change.

---

## 1. What was built, in one paragraph

A connector image is booted as a libkrun micro-VM inside a **helper
container** that the reactor launches through the host podman API exactly as
it launches connectors today, with three additions to the launch line:
`/dev/kvm`, `/dev/net/tun`, and `CAP_NET_ADMIN`. Inside the helper a small
Rust **shim** creates a tap, loads an **nftables ruleset** from a policy JSON,
starts a **DNS resolver** that is the guest's only nameserver and admits
resolved addresses into the ruleset for their TTL, formats a scratch disk on an
unnamed temp file, and calls libkrun. The connector image is the guest's root
over virtiofs, served read-write from podman's own per-container layer. A
static **flow-init** runs first in the guest: static network, mounts, uid
drop, then `exec` of the image's command. **connector-init**, the same binary
connectors run under today, answers the reactor's gRPC over a vsock port that
libkrun maps to a Unix socket in the helper. Customer Python dependencies
are installed by the connector at start (derive-python runs `uv` into a
temporary directory) onto the **scratch disk**, a virtio-blk ext4 volume, over
egress that allows the package index. The spike measured them as a per-tag
read-only block image instead; the decision of 2026-09-17 dropped that.

Five things the spike changed in the design, all measured (`REPORT.md` 4.6, 4.1,
6, 7.10, 7.3):

- Dependencies live on a **block device**, not on virtiofs. virtiofs failed
  the import gate at 2.88x; a block device passes (2.12x on the first import
  after boot, 1.15x in steady state, measured as a per-tag read-only image).
  The delivery changed on 2026-09-17: no prebuilt image, the connector
  installs at start onto the scratch disk, which is the same transport. The
  transport finding is what carries; see B1.
- The writable root is **podman's per-container layer** served read-write.
  There is no overlay inside the guest and no libkrun patch.
- **libkrun is unpatched.** The one-line ENOTTY patch WP04 needed for a guest
  overlay went away with the overlay.
- **DAX is out.** No measured benefit, and it is the only path to a libkrun
  mapping overflow.
- **`rp_filter=1` is set explicitly on the launch line.** It is the anti-spoof
  control that actually runs; the nft rule is unreachable in a /30 topology.

---

## 2. What carries forward

| what | where | size | verdict | why |
|---|---|---|---|---|
| connector-init `--vsock-port` | `crates/connector-init/src/lib.rs`, `tests/vsock.rs` | small | **keep** | Already in the crate with tests. Serves gRPC on AF_VSOCK when asked; unchanged otherwise. |
| helper shim | `spike/helper/shim/src/*.rs` | ~900 lines Rust | **keep, move into `crates/`, harden** | Tap, ruleset, resolver spawn, scratch disk, image config, the libkrun call sequence of `REPORT.md` 4.3. Hardening is section 3 items R5 and R6. Drop the spike-only flags `--venv-dax` and `--thp-disable`, and, with the builder gone, `--deps-image` and `--deps-fstype`. |
| flow-init | `spike/flow-init/src/*.rs` | ~550 lines Rust, static musl | **keep, move into `crates/`** | Network, `/etc`, mounts, chown, uid drop, exec. It does no `pivot_root` and must not (CONTRACTS "flow-init"). Drop `--deps-dev`, `--deps-fstype` and the `/opt/venv` mount with them. |
| egress ruleset generator | `spike/egress/src/{ruleset,policy,cidr,ifaddrs}.rs`, `src/bin/egress.rs` | ~550 lines Rust | **keep** | Policy JSON in, `inet flow_sandbox` out, applied once with `nft -f` before the VM boots. The rendered ruleset in `REPORT.md` 4.2 is the spec. |
| egress resolver | `spike/egress/src/dns.rs`, `src/bin/resolver.rs` | ~450 lines Rust | **keep the DNS half, rewrite the nft half** | It forks one `nft` process per answered query, and `nft add element` does not refresh an existing element's timeout (open problem 4). Phase 2 drives the set over netlink and updates in place. |
| runtime-next spike switch | `crates/runtime-next/src/container/spike.rs`, the hook in `container.rs` | ~330 lines Rust | **port into `crates/connector`, then productionize** | The launch line, the per-connector directory, the socket dial and the cleanup guard are right. PR 3490 deletes the `runtime-next` file the hook lives in and re-homes the launcher as `crates/connector/src/container.rs`, whose `start` takes a `StartContext` (container network, log level, `LogSink`, plane, process, task name) in place of the generic logger the spike threads through. The port is mechanical; a rebase hits a modify/delete conflict on `container.rs`. The trigger is wrong either way: an env var, a policy file path, and sizes from more env vars. Items R1 and R2 replace them. |
| helper image build | `spike/helper/Dockerfile` | | **replace** | Builds libkrun v1.19.4 from source on Fedora 43 with libkrunfw 5.5.0 from Fedora, tagged `localhost/...:spike`. Phase 2 needs a CI build, a registry image, and pinned versions of both (item H1). |
| deps image build | `spike/tasks/exp5-build.sh`, the mkfs line in `REPORT.md` 4.4 | | **not carried** | Decided 2026-09-17: no builder, no per-tag image (B1). The mkfs options survive anyway, because the shim already formats the scratch disk with them. |
| stub helper | `spike/stub-helper/` | 70 lines | **keep as a test double** | Satisfies the runtime switch with no VM: same mounts, same socket, `socat` to a chroot'd connector-init. It is how the runtime side can be tested on a machine without KVM. |
| catalog fixtures | `spike/catalog/` | | **keep as test fixtures** | Fictitious `acmeCo/` specs for a Go capture, a Go materialization and a pandas derivation, with fixture files. Experiments 1 to 4 drive `flowctl preview` with them. |
| benchmark image and instruments | `spike/derived/` | | **evidence; rerun only if R7 reopens** | `bench.py`, `prefault.py`, `attrib.py` and the pandas image are what experiment 5 measures with. Any rerun should measure the production shape, a venv `uv` installed onto `/scratch`, not a prebuilt image. |
| probe suite and test internet | `spike/egress/probes.py`, `testnet.py`, `spike/tasks/egress-netns-test.sh` | ~650 lines Python | **seed of the integration tests** | Four named probe sets, one JSON line each, run where the connector runs. Item C1. |
| push-button scripts | `spike/tasks/*.sh` (34) | | **evidence, not product** | Each is one experiment or one check, and each is green as of its last run. Their assertions are what the integration tests should assert; the scripts themselves are not carried. `env-setup.sh` documents how the box was provisioned. |
| citation checker | `spike/tasks/check-exposure-citations.sh` | | **keep while `libkrun-exposure.md` is cited** | Checks all 96 file and line-range citations in the source read against the libkrun tag. Rerun it if the report is quoted against a newer libkrun. |

Nothing under `spike/` merges to master as it stands. The Rust that carries
forward moves into `crates/` under its own names and its own tests.

---

## 3. Phase-2 work, by owner

Each item names the report section or open problem it rests on. Items are
mechanism we build unless marked **policy**, which is a decision security or
product owns and the mechanism must be able to carry either way. Items marked
**provisional** depend on appetite that has not been decided.

### Runtime

- **R1. Egress policy from an image label** (decided 2026-09-17; replaces the
  catalog-model item). The connector image carries its egress list as a
  label, read at inspect time the way the runtime already reads
  `FLOW_RUNTIME_PROTOCOL` (`crates/runtime/src/container.rs`), on the pattern
  of John's secrets handling. The plumbing exists: the launcher already runs
  `inspect`, and the shim already receives that output verbatim as
  `/init/image-inspect.json` and parses `Config` from it (`image.rs`); adding
  `Labels` is a struct field. The shim turns the label into the policy JSON of
  CONTRACTS "Policy JSON", which gains one thing the spike did not have: a
  **name allowlist**. Today `egress: public` admits any name the guest
  resolves; with a list, the resolver answers only names on it (suffix match)
  and refuses the rest, so `@resolved` never holds anything off the list. The
  baseline denylist, anti-spoof, and the IPv6 and non-TCP/UDP drops all still
  apply underneath. derive-python's list must include the package index
  (`pypi.org`, `files.pythonhosted.org`), since dependencies now install at
  start (B1). Two cases PR 3490 raised dissolve: a task-less `Spec` has an
  image and so has a policy, and `Validate` installs dependencies over the
  same allowed egress a run does. **Trust rule (mechanism):** a label is
  written by whoever built the image, so honor it only on images from our own
  registry; a non-Estuary image gets the strictest policy regardless of what
  it claims. **Policy:** what each image's list is, whether derive-python's
  arbitrary customer code gets a list or `public`, and whether a task may
  ever add destinations of its own, are product's and security's to set; the
  mechanism carries a list, `public`, or `none` alike.
- **R2. The per-launch decision, in `crates/connector`.** Port the spike
  switch into `crates/connector/src/container.rs` once PR 3490 lands, and
  retire the `FLOW_SANDBOX_SPIKE_*` variables (CONTRACTS "runtime-next spike
  switch"). What replaces each: the policy comes from the image label (R1);
  memory and CPU come from the launcher's existing `CONNECTOR_MEMORY_LIMIT`
  and `CONNECTOR_CPU_LIMIT` (decided 2026-09-17: connector limits are already
  set this way, per reactor through the environment, and the sandbox adds
  nothing to them); the disk size is the one new knob, in the same style.
  "Sandbox this launch" is the presence of the egress label on the image,
  which gives per-image rollout with no catalog change; `StartContext.plane`
  already gates non-Estuary images and is where R1's trust rule belongs.
  Every caller reaches `container::start` through the connector service
  (runtime-next, flowctl, the catalog tests, and the control plane once it
  adopts the protocol), so a switch there covers all of them at once. One
  semantic to pin down and write down: `CONNECTOR_MEMORY_LIMIT` is the
  container's cgroup cap today. Keep it that, and give the guest
  `limit - 256 MiB` of RAM (decision 3), so a sandboxed connector under the
  1 GiB default sees 768 MiB. The alternative, guest RAM equal to the limit
  with the cap 256 above it, quietly makes every sandboxed connector exceed
  the configured limit. The per-connector directory moves from
  `/var/tmp/flow-spike/reactor` to the reactor's own ephemeral directory,
  which is removed on restart and matches the contract that `<id>` is never
  reused (`REPORT.md` 4.1).
- **R3. Spec and Validate through `connector.Connector`** (open problem 5).
  Today they run through the legacy `runtime` crate via the agent's V1
  connector proxy and are unsandboxed; for derive-python that is where the
  customer's dependencies are fetched and built and the module type-checked.
  PR 3490 supplies the mechanism: the reactor serves `connector.Connector`
  in-process, on each task's Unix socket, and through a Go proxy on its
  public address, authorized by reactor-issued bearer tokens. The PR keeps the
  V1 proxy unchanged and defers control-plane adoption to a follow-up after
  the reactor rollout. Once the control plane adopts it, Spec and Validate go
  through the same `container::start` as tasks and the switch of R2 covers
  them with no further runtime work. With the policy on the image (R1), Spec
  and Validate carry it like any other launch, so nothing else is ours here.
  Until adoption lands,
  customer Python runs unsandboxed during validation, and the report says so
  in accepted costs. Owner: control plane (agent) for adoption, runtime for
  the policy.
- **R4. Resolver over netlink** (open problem 4). Keep the DNS handling (A
  records, TTL clamp `max(90 s, ttl)` capped at 1 h, AAAA emptied, RFC1918
  and helper-subnet answers refused); replace the per-query `nft` process with
  netlink set updates that refresh an existing element's timeout in place.
- **R5. Shim hardening from the source read** (open problem 8). Serve each
  virtiofs share with the share as its filesystem root (`pivot_root` or
  `openat2` with `RESOLVE_BENEATH`, upstream's own remedy), and keep the
  helper's mount namespace minimal, because at guest-kernel level everything
  mounted into the helper is nameable (open problem 11). Bounded, not urgent:
  the escape is unreachable from the connector or from guest root.
- **R6. Guest panic diagnosis** (open problem 1). The shim tees the console
  already; a `Kernel panic` line before `krun_start_enter` returns should
  produce one structured stderr line and a distinct exit code. And the runtime
  must never branch on the helper's exit code: it is the guest workload's exit
  status, and 0 on a panic.
- **R7. The memory overhead default: 256 stays** (open problem 2, `REPORT.md`
  4.5, decision 3). Decided, so phase 2 ships 256 and does not rerun experiment
  5 for it now. The guest's `uv` run is the largest generator of the page
  cache this cap bounds, and with B1 dropped (2026-09-17) that run is
  permanent, so 256 was sized against the workload production actually has.
  Reopen only if helpers are seen pinned at the cap in production. When it is
  reopened, run experiment 5 at 64, 128 and 256, for a curve rather than a coin
  flip. Not answered either way, and the more interesting question: whether the
  cap should scale with `diskMib` rather than being a constant added to
  `memoryMib`. Experiment 11 pinned the cgroup at its limit on disk size, not
  on memory size.
- **R8. Keep `rp_filter` explicit** (open problem 3). The launch line sets
  `--sysctl net.ipv4.conf.default.rp_filter=1` and phase 2 must keep it; there
  is no other route to the tap's setting from inside the container.
- **R9. Batched teardown** (open problem 7, low priority). `podman rm -f` took
  about 8 s per helper, serially, with 82 running. Confirm whether reactor
  shutdown removes connector containers one at a time, and batch if so.
- **R10. Per-task bound on root writes** (open problem 6, **provisional**).
  Today a guest filling its root is bounded by the reactor's container storage
  and nothing else, as a container is. The writable layer lives at
  `overlay-containers/<container id>/userdata/overlay/<n>/upper` and is not
  reported by `podman inspect`. Build only if a quota is wanted.

### Builder

- **B1. Dropped (2026-09-17): no builder, no pre-flight dependency install.**
  The spike's plan was a per-tag read-only dependency image built ahead of
  time (`REPORT.md` 4.4, 4.6). It is not happening: there is nowhere to store
  the images, and a formal no-network sandbox is something only we would
  value, not customers. What replaces it is what experiment 4 already ran:
  derive-python invokes `uv` at start into a temporary directory, and because
  flow-init points `TMPDIR` and `UV_CACHE_DIR` at `/scratch`, the venv and
  uv's cache land on the scratch disk, a virtio-blk ext4 volume, with the
  package index reachable through the egress list (R1). Consequences: the
  deps disk and `--deps-image`, `--deps-fstype` and `--deps-dev` are removed
  from the shim and flow-init; `diskMib` is now sized by dependency installs
  (experiment 4 measured about 180 MiB for one mid-sized dependency, venv and
  cache each about half, so the 4096 MiB default is ample and heavy stacks
  are the case to watch); every start pays the install, as it does today, and
  the cache dies with the guest, as a container's does today. Not measured in
  this exact shape: import time for a venv freshly written onto `/scratch`.
  It is the same block transport experiment 5 passed with, and pages `uv`
  just wrote are already in the guest's page cache, so it should sit at or
  below the block-image figures; a run of `bench.py` in that shape closes the
  point if anyone needs it closed.

### Helper image and packaging

- **H1. Build and publish the helper image in CI.** Pin libkrun (v1.19.4 is
  what was measured and read) and libkrunfw (5.5.0, guest kernel 6.12.91), and
  record both in the image. libkrun 2.x offers a configurable kernel command
  line, which the design does not need; do not upgrade for it. The three
  guest-kernel-level bugs in open problem 9 are not being reported upstream
  (decision 1), so there is nothing to track here unless that changes.

### Ops

- **O1. KVM on the GCP reactor instances.** The spike ran on a dev box with
  `/dev/kvm`; the production reactor family does not have nested
  virtualization enabled today. The reactor's own Quadlet unit does not change:
  the reactor drives the host podman, and it is the helper container that
  receives `/dev/kvm` and `/dev/net/tun`.
- **O2. AWS** (open problem 17). The current AWS reactor family exposes no
  `/dev/kvm`. Nothing in the spike was or could be validated there; the
  instance-family question needs an answer before this ships anywhere but GCP.

### CI and test

- **C1. A KVM-capable runner and an integration suite.** None of the spike's
  assertions can run without `/dev/kvm`. The suite should assert what the
  buttons asserted: the launch's capabilities and devices (experiment 1),
  protocol parity by diff against an unsandboxed run (experiment 3), the egress
  probe sets from inside a guest (experiment 6), storage bounds (experiment
  11), the control-channel probes (experiment 12), and cleanup after SIGKILL
  (experiment 13). The runtime side of the switch tests against the stub
  helper on ordinary runners.

### Product and derive-python (policy, not mechanism)

- **P1. Blocked connections stall rather than fail** (open problem 13).
  **Decided: `drop` stays** (decision 2). A `reject` on the guest's DNS query
  alone would have made lookups fail at once. The mechanism can still do
  either, so this is reversible if a customer ever makes the stall the more
  expensive half.
- **P2. `connectionsPerMinute` is pacing, and invisible when it bites** (open
  problem 14). Whether it is named as a rate rather than a limit, and whether
  the policy is surfaced in the task's logs at startup.
- **P3. pyright strict** (open problem 15) and **stdout as the protocol
  channel** (open problem 16) are derive-python's, unrelated to the sandbox,
  and both will be met by the first customer who brings an untyped library or
  a `print`.

---

## 4. Decisions

Seven, collected here so the plan is not blocked on finding them in the
ledger. The first four were answered 2026-09-16, the last three 2026-09-17.

1. **The three guest-kernel-level libkrun bugs are not reported upstream.**
   Console port index, balloon report length, DAX offset overflow (open problem
   9). None is reachable from a connector. The standing rule is narrower than
   the spike's recommendation of an issue each: report nothing unless it
   actively bites us and is clearly wrong. Revisit per bug if one ever does.
2. **`egress: none` keeps `drop`.** The guest's DNS query is dropped and not
   rejected, so a blocked lookup stalls and then times out rather than failing
   at once. This is what the spike chose, and accepted cost 6 stands (P1, open
   problem 13).
3. **The overhead default stays at 256 MiB.** Not 64. The measured overhead
   constant is 20 to 32 MiB, but the cgroup limit is a cap and not a
   reservation: a reactor sizes on the 95 MiB of host memory per idle 512 MiB
   guest that experiment 10 measured directly, so lowering the cap buys no
   density. What the cap also bounds is the host page cache charged to the
   helper, which is the `import` path's working set, and every experiment 5
   number was taken at 256. Keeping 256 costs a runaway guest about 190 MiB of
   extra reclaimable page cache. R7 says when to reopen it.
4. **AWS ownership is being worked.** Until it lands, O2 stands as the report
   proposes: runtime to drive, ops for the instance family and images.
5. **No builder and no pre-flight dependency install.** Nowhere to store the
   images, and customers will not value a no-network sandbox; the package
   index goes in the allowed egress instead. B1 is dropped and its
   consequences are recorded there.
6. **CPU and memory limits are the launcher's existing
   `CONNECTOR_MEMORY_LIMIT` and `CONNECTOR_CPU_LIMIT`.** Already supported,
   per reactor through the environment; the sandbox adds only a disk-size
   knob (R2). The memory semantic is R2's to pin down.
7. **The egress list travels as an image label**, read at inspect time like
   `FLOW_RUNTIME_PROTOCOL`, on the pattern of John's secrets handling. R1 is
   rewritten around it, and the trust rule for non-Estuary images is part of
   the mechanism.

---

## 5. Reproducing anything

Every measurement has a button in `spike/tasks/`, named after its experiment,
and every button needs a host with `/dev/kvm`, rootful podman, and the fixtures
`spike/tasks/env-setup.sh` installs (podman networks, a test nginx on
TEST-NET-2, the images). `spike/tasks/env-check.sh` says whether a box is
ready. Each `report/exp*.md` names the commit and the script it was measured
with, and the raw data is under `report/data/`. `../STATUS.md` is the ledger of
how every result was reached, including the ones that were wrong first.
