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
- `mise run ci:connector-vmm-kvm --platform` previews and publishes a Python
  derivation on this checkout's local stack. The task starts and stops the
  stack and refuses to start if it is already running.
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
  isolation, RPC, teardown, egress allowlists and expiry, and host isolation
  after a VMM removes its own rules. Connector-mount tests check that host
  file replacements become visible inside the guest.
- `tests/dns_answers/`, `tests/dns_capacity/`: refused answers and capacity
  overflow must not partially authorize addresses in the kernel. Resolver
  unit tests cover TTL retention, CNAME capacity, and reclamation with explicit
  expiry states; `kvm::egress_ttl` checks real kernel expiry.
- `tests/launcher.rs`: launches through `connector::Service`, readiness,
  socket access restricted to the launcher's UID, task egress, cancellation,
  ownership, and recovery after process death or a stalled container query.
  Release state-machine tests in `connector` cover
  failure combinations; the integration test checks actual command termination
  and recovery without asserting elapsed time.
- `tests/replacement/`: an old generation's cleanup must leave its running
  replacement intact.
- `tests/platform.rs`: preview, publication, derived documents, logs, and
  cleanup through flowctl and the reactor, and `flowctl generate` placing a
  nested catalog's generated files beside the derivation's specification.
  The podman wrapper verifies each connector mount synchronously before
  container creation.
- `tests/release.rs`, `tests/wrapper.rs`: resource cleanup and the podman
  wrapper, tested against stand-in commands.

## Harness

`mise/tasks/ci/connector-vmm-kvm` builds the binaries and images, installs or
verifies the host boundary, stages controlled endpoints, and writes the run
configuration. Its companion `connector-vmm-kvm-lib.sh` tracks and releases
resources. Cargo and tests run as the invoking user; privileged operations
use `sudo -n`.

`src/launch.rs` defines reference container launch lines also used by
`connector`'s plan tests. `src/run.rs` owns test VMMs and their resources.
Tests removing connector images use `run::stop` to check container and
image-volume removal before removing the image.
`src/launcher.rs` and `fixtures/podman.sh` provide launcher control and fault
injection. Recovery holds match the seeded orphans' tokens so a failed launch
can release its own resources. `src/host.rs` reads kernel state; `src/guest.rs`
talks to the stdlib Python probe server in `fixtures/probes.py`.

`src/dns.rs` and `src/endpoint.rs` serve controlled DNS and TCP traffic.
`src/netns.rs` and `fixtures/bind.py` bind sockets in the endpoint namespace.
The endpoints use addresses under `192.31.196.240/28`, routed one hop from
the host so host-isolation rules apply. Excluded decoys use
`198.18.255.240/28`. Both ranges are blackholed beneath their test routes;
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
