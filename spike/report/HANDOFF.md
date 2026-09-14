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
arrive as a **per-tag read-only ext4 image** on a second block device, mounted
at `/opt/venv`.

Five things the spike changed in the design, all measured (`REPORT.md` 4.6, 4.1,
6, 7.10, 7.3):

- Dependency sets ship as per-tag read-only **block images**, not virtiofs.
  virtiofs failed the import gate at 2.88x; the block image is 2.12x on the
  first import and 1.15x in steady state.
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
| helper shim | `spike/helper/shim/src/*.rs` | ~900 lines Rust | **keep, move into `crates/`, harden** | Tap, ruleset, resolver spawn, scratch disk, image config, the libkrun call sequence of `REPORT.md` 4.3. Hardening is section 3 items R5 and R6. Drop the spike-only flags `--venv-dax` and `--thp-disable`. |
| flow-init | `spike/flow-init/src/*.rs` | ~550 lines Rust, static musl | **keep, move into `crates/`** | Network, `/etc`, mounts, chown, uid drop, exec. It does no `pivot_root` and must not (CONTRACTS "flow-init"). |
| egress ruleset generator | `spike/egress/src/{ruleset,policy,cidr,ifaddrs}.rs`, `src/bin/egress.rs` | ~550 lines Rust | **keep** | Policy JSON in, `inet flow_sandbox` out, applied once with `nft -f` before the VM boots. The rendered ruleset in `REPORT.md` 4.2 is the spec. |
| egress resolver | `spike/egress/src/dns.rs`, `src/bin/resolver.rs` | ~450 lines Rust | **keep the DNS half, rewrite the nft half** | It forks one `nft` process per answered query, and `nft add element` does not refresh an existing element's timeout (open problem 4). Phase 2 drives the set over netlink and updates in place. |
| runtime-next spike switch | `crates/runtime-next/src/container/spike.rs`, the hook in `container.rs` | ~330 lines Rust | **productionize** | The launch line, the per-connector directory, the socket dial and the cleanup guard are right. The trigger is wrong: an env var, a policy file path, and sizes from more env vars. Section 3 items R1 and R2 replace them. |
| helper image build | `spike/helper/Dockerfile` | | **replace** | Builds libkrun v1.19.4 from source on Fedora 43 with libkrunfw 5.5.0 from Fedora, tagged `localhost/...:spike`. Phase 2 needs a CI build, a registry image, and pinned versions of both (item H1). |
| deps image build | `spike/tasks/exp5-build.sh`, the mkfs line in `REPORT.md` 4.4 | | **seed of the builder** | The command is right; the venv it packaged was created at `/venv` and mounted at `/opt/venv`. The builder creates it at its final path (item B1). |
| stub helper | `spike/stub-helper/` | 70 lines | **keep as a test double** | Satisfies the runtime switch with no VM: same mounts, same socket, `socat` to a chroot'd connector-init. It is how the runtime side can be tested on a machine without KVM. |
| catalog fixtures | `spike/catalog/` | | **keep as test fixtures** | Fictitious `acmeCo/` specs for a Go capture, a Go materialization and a pandas derivation, with fixture files. Experiments 1 to 4 drive `flowctl preview` with them. |
| benchmark image and instruments | `spike/derived/` | | **keep until item R7 is done** | `bench.py`, `prefault.py`, `attrib.py` and the pandas image are what experiment 5 measures with. The overhead-default decision needs one more run of them. |
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

- **R1. Egress policy in the catalog model.** Today the policy is a JSON file
  per CONTRACTS "Policy JSON" (`egress`, `allowAll`, `declaredCidrs`, the two
  rate limits, the TTL floor and cap). It needs a model type, a place in the
  built task spec, and validation that a declared CIDR does not overlap the
  baseline (the egress binary already refuses that at load; the control plane
  should refuse it at publish). **Policy:** which tasks are sandboxed, and what
  a task may declare, is product's and security's to set; the mechanism carries
  any answer.
- **R2. The per-task launch decision.** Replace `FLOW_SANDBOX_SPIKE_POLICY` and
  the other env vars (CONTRACTS "runtime-next spike switch") with a decision
  made from the built spec: sandbox or not, the policy from R1, and
  `memoryMib`, `vcpus`, `diskMib` from the spec with defaults. The per-connector
  directory moves from `/var/tmp/flow-spike/reactor` to the reactor's own
  ephemeral directory, which is removed on restart and matches the contract
  that `<id>` is never reused (`REPORT.md` 4.1).
- **R3. Spec and Validate onto runtime-next** (open problem 5). Today they run
  through the legacy `runtime` crate and are unsandboxed; for derive-python
  that is where the customer's dependencies are fetched and built and the
  module type-checked. This is the "connector proxy moves to runtime-next"
  prerequisite. Until it lands, customer Python runs unsandboxed during
  validation, and the report says so in accepted costs.
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
- **R7. The memory overhead default** (open problem 2, `REPORT.md` 4.5).
  Measured at 20 to 32 MiB; the spike ships 256; 64 is what the measurement
  supports. Rerun experiment 5 at `memoryMib + 64` first, because the cgroup
  limit also bounds the host page cache the helper is charged for and every
  import number was taken at 256. A reactor sizes on 95 MiB of host memory per
  idle 512 MiB guest, not on the cgroup figure.
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

- **B1. The dependency-image pipeline** (`REPORT.md` 4.4, 4.6). Per connector
  tag: resolve and install the customer's dependencies into a venv created at
  `/opt/venv`, then `mkfs.ext4 -d <venv> -O ^has_journal -E lazy_itable_init=0
  -m 0` sized at `du -sk` plus 10% plus 24 MiB. ext4, not erofs. Where the
  images are stored and how they reach a reactor is undecided; the spike bound
  a host path read-only at `/deps.img`. This builder is where the work R3
  removes from the reactor's network goes, and it should itself run sandboxed,
  since it executes the customer's build backends.

### Helper image and packaging

- **H1. Build and publish the helper image in CI.** Pin libkrun (v1.19.4 is
  what was measured and read) and libkrunfw (5.5.0, guest kernel 6.12.91), and
  record both in the image. libkrun 2.x offers a configurable kernel command
  line, which the design does not need; do not upgrade for it. If the three
  guest-kernel-level bugs in open problem 9 are reported upstream, track their
  fixes here.

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

- **P1. Blocked connections stall rather than fail** (open problem 13). A
  `reject` on the guest's DNS query alone would make lookups fail at once. The
  spike keeps deny as `drop` by decision; the mechanism can do either.
- **P2. `connectionsPerMinute` is pacing, and invisible when it bites** (open
  problem 14). Whether it is named as a rate rather than a limit, and whether
  the policy is surfaced in the task's logs at startup.
- **P3. pyright strict** (open problem 15) and **stdout as the protocol
  channel** (open problem 16) are derive-python's, unrelated to the sandbox,
  and both will be met by the first customer who brings an untyped library or
  a `print`.

---

## 4. Decisions waiting on the author

Four, collected here so the plan is not blocked on finding them in the
ledger:

1. **Report the three guest-kernel-level libkrun bugs upstream?** Console port
   index, balloon report length, DAX offset overflow (open problem 9). None is
   reachable from a connector. The spike's recommendation is an issue each, no
   patches.
2. **Should `egress: none` reject the guest's DNS query** instead of dropping
   it (P1)? The spike chose drop.
3. **Does the overhead default move to 64** after the experiment 5 rerun (R7)?
4. **Who owns AWS** (O2)? The report proposes runtime to drive, ops for the
   instance family and images.

---

## 5. Reproducing anything

Every measurement has a button in `spike/tasks/`, named after its experiment,
and every button needs a host with `/dev/kvm`, rootful podman, and the fixtures
`spike/tasks/env-setup.sh` installs (podman networks, a test nginx on
TEST-NET-2, the images). `spike/tasks/env-check.sh` says whether a box is
ready. Each `report/exp*.md` names the commit and the script it was measured
with, and the raw data is under `report/data/`. `../STATUS.md` is the ledger of
how every result was reached, including the ones that were wrong first.
