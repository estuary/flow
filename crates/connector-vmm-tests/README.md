# connector-vmm-tests

Integration tests for connector execution in real KVM guests. Unit tests in
`connector`, `connector-vmm`, and `guest-init` cover configuration and state
transitions; this suite checks their interaction with the kernel, container
engine, and platform.

## Running

- `mise run ci:connector-vmm-kvm` runs the KVM and launcher tests, with two
  tests at a time. Individual tests may launch several VMMs.
- Append a test-name filter to run a subset. Filtered runs skip
  connector-init's separate vsock test.
- `mise run ci:connector-vmm-kvm --platform` previews, publishes and runs
  Python captures, derivations and materializations on this checkout's local
  stack. The task starts and stops the stack and refuses to start if it is
  already running.
- `mise run ci:connector-vmm-kvm --routing-deployment /path/to/est-dry-dock`
  also runs the dual-NIC static outbound routing regression. The deployment
  checkout must contain `roles/common/templates/flow-vmm-routing.j2`.
  This requires host `nft` and Python `jinja2`. It starts a rootful Podman API
  service with private storage inside an isolated host network namespace.
  Its ordinary connector and real KVM guests contact a source-recording
  endpoint through simulated public and static-NAT paths. It applies the
  rendered deployment script and checks destination enforcement, internal
  routing, ingress, isolation, concurrent launches, cleanup, route recreation,
  and API restart/network reload. The resource journal also recovers the lab
  after its owner dies. See
  [the investigation](../../docs/connector-vmm/static-outbound.md).
- `mise exec -- cargo nextest run -p connector-vmm-tests` runs the harness's
  unit tests. The default nextest profile excludes KVM and platform tests.

The KVM task requires x86_64 Linux, `/dev/kvm`, passwordless sudo, rootful
podman, Docker buildx, python3, `x86_64-linux-musl-gcc`, and conntrack. The
platform test also requires the local stack's prerequisites and public
network access. These integration tests are opt-in; GitHub Actions and
`ci:platform-test` do not run them. Missing prerequisites fail the task.

`--allow-missing-vsock-loopback` permits connector-init's separate vsock test
to skip when its kernel module cannot load. The KVM suite still exercises
connector-init over a guest's vsock connection.

## Coverage and entry points

- `tests/kvm.rs`: guest boot, devices, filesystem and connector-mount
  isolation, working directories, RPC, teardown, egress allowlists and expiry,
  declared CIDRs over TCP and UDP with their host grant's lifecycle, host-only declared TCP
  ingress with bidirectional traffic under `egress: none`, and host isolation
  after a VMM removes its own rules. Connector-mount tests check that host
  file replacements become visible inside the guest. `no_new_privileges`
  runs controlled setuid, setgid and file-capability programs from the
  workload, and as the same user in a plain podman container as the control.
  Root workloads come from images whose `User` is root.
  `spec_rpc_through_init_sock`, `production_guest` and
  `production_refuses_test_controls` run the packaged VMM image, with
  connector-init as the workload where they boot a guest.
- `kvm::azure_platform_egress`: DNS refusal and direct TCP/UDP exclusion,
  including after the VMM removes its rules, plus explicit `/32` and `/0` grants.
- `tests/dns_answers/`, `tests/dns_capacity/`: refused answers and capacity
  overflow must not partially authorize addresses in the kernel. Resolver
  unit tests cover TTL retention, CNAME capacity, and reclamation with explicit
  expiry states; `kvm::egress_ttl` checks real kernel expiry.
- `tests/launcher.rs`: launches through `connector::Service`, readiness,
  socket and credential-source access restricted to the launcher's UID, task
  egress (refused when omitted, nothing when empty, and the union of the
  connector's, image's and task's hosts otherwise), Spec of each Python
  connector built from this checkout, ingress of the built spec's ports and
  not the image's, cancellation,
  ownership, and recovery after process death, a destroyed runtime or a stalled container query,
  including labeled resources whose record is gone. Recovery runs as single
  passes (`connector::Vmm::recover`), in processes killed mid-pass, or as a
  `connector::VmmRecovery` whose later passes, with no launch, catch late
  creates and retry failures. Late ingress grants are created, and started,
  apart through a rootful podman API service each test starts over the
  host's store (`launcher::Api`), with recovery reaching it too, and the
  service's own events order them. A create the service accepted, of a
  launch's container or of its `--rm` boundary verify's, and holds on a POSIX
  lock of a private store's containers (`/proc/locks` shows the service
  waiting), outlives its killed client and its launch's release, and lands
  for a later pass to remove.
  Release state-machine tests in `connector` cover
  failure combinations; the integration test checks actual command termination
  and recovery without asserting elapsed time.
- `tests/replacement/`: an old generation's cleanup must leave its running
  replacement intact.
- `tests/platform.rs`: through flowctl and the reactor, with fixtures under
  `fixtures/platform/`:
  - the legacy `module` derivation, on the published `derive-python:stable`:
    preview, publication, derived documents, logs, cleanup, and
    `flowctl generate` placing a nested catalog's files beside the
    derivation's specification;
  - a capture project (`examples/capture-python`'s): local and control-plane
    Discover, preview, publication, and its cursor resumed by a session
    restarted after its VMM is killed, and by a later publication's;
  - a derivation project which lists the `uv.lock` the test makes for it,
    whose sessions install it frozen, and whose connector state survives a
    killed VMM;
  - a materialization project (`examples/materialize-python`'s) into that
    example's API, served in a container and reached by a declared CIDR:
    a multi-session preview with standard and delta updates, then live
    greetings replayed into the idempotent API after a killed VMM;
  - `flowctl generate --task` of each project, starters of each type, and the
    legacy derivation from one catalog of nested projects. A task refused
    its VMM, on a machine without VMM capability or for want of declared
    egress, fails `generate` and `preview` without a launch.

  Projects run this checkout's images, which the task builds as `:local`.
  The podman wrapper verifies each connector mount synchronously before
  container creation.
- `tests/release.rs`, `tests/wrapper.rs`: resource cleanup and the podman
  wrapper, tested against stand-in commands.

## Harness

`mise/tasks/ci/connector-vmm-kvm` builds the binaries and images, installs or
verifies the host boundary, stages controlled endpoints, and writes the run
configuration. It builds two VMM images which differ only in
`flow-connector-vmm`: the packaged build, which `--platform` uses, and the
suite's, built with connector-vmm's `test-controls` feature in a target
directory of its own. It checks that both carry this run's `flow-guest-init`.
Its companion `connector-vmm-kvm-lib.sh` tracks and releases
resources. Cargo and tests run as the invoking user; privileged operations
use `sudo -n`.

`src/launch.rs` defines reference container launch lines also used by
`connector`'s plan tests. `src/run.rs` owns test VMMs and their resources.
Tests removing connector images use `run::stop` to check container and
image-volume removal before removing the image.
`src/launcher.rs` and `fixtures/podman.sh` provide launcher control and fault
injection; `launcher::before_connector_init` runs a test's script in the
workload before connector-init. Recovery holds match the seeded orphans'
tokens so a failed launch can release its own resources. `launcher::remote`
has the wrapper drive an API service instead of podman itself; a test's own
service stops with it, or after five idle minutes should the test die, and
a private store's directories are on the resource list. `src/host.rs` reads
kernel state; `src/guest.rs` talks to the stdlib Python probe server in
`fixtures/probes.py`.

`src/dns.rs` and `src/endpoint.rs` serve controlled DNS, TCP and UDP traffic.
`src/netns.rs` and `fixtures/bind.py` bind sockets in the endpoint namespace.
The endpoints use addresses under `192.31.196.240/28`, routed one hop from
the host so host-isolation rules apply. Excluded decoys use
`198.18.255.240/28` and the simulated Azure platform address
`168.63.129.16/32`; a declared CIDR may reach them, as it would private
space. All ranges are blackholed beneath their test routes;
the namespace routes only back to the host.

## Resources and diagnostics

Runtime resources live under `/var/tmp/connector-vmm-kvm`. This short path
keeps guest Unix socket paths within their limit. A lock excludes concurrent
runs. Output is retained under `$CARGO_TARGET_DIR/connector-vmm-kvm/run`.

Tests release their own resources. The task's exit and interrupt traps also
release tracked resources in reverse order. Cleanup refuses to proceed while
a tracked stack or launch owner is still active. Failed removals remain in
the resource list for the next invocation to retry. Resources are never
selected for removal by age or a name pattern. Existing verified host-boundary
tables are adopted and left installed; tables created by the task are removed
when safe. Pulled base images and build caches remain available for later runs.
