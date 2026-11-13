# connector-vmm-tests

Integration tests for connector execution in real KVM guests. Unit tests in
`connector`, `connector-vmm`, and `connector-guest-init` cover configuration and state
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
- `mise exec -- cargo nextest run -p connector-vmm-tests` runs the harness's
  unit tests. The default nextest profile excludes KVM and platform tests.

The KVM task requires x86_64 Linux, `/dev/kvm`, passwordless sudo, rootful
podman, Docker buildx, python3, `x86_64-linux-musl-gcc`, and conntrack. The
platform test also requires the local stack's prerequisites and public
network access. These integration tests are opt-in and developer-run;
GitHub Actions and `ci:platform-test` do not run them, and nothing requires
their results. Missing prerequisites fail the task. The task builds its own
images from this checkout, so it does not test a published image.

`--allow-missing-vsock-loopback` permits connector-init's separate vsock test
to skip when its kernel module cannot load. The KVM suite still exercises
connector-init over a guest's vsock connection.

## Tests

- `tests/kvm.rs`: one VMM per test, launched on the launcher's lines
  (`connector::vmm_argv`): guest boot, users, mounts and storage, RPC over
  `init.sock`, egress allowlists, CIDRs and TTL expiry against the real
  kernel, host ingress, and host isolation after a VMM removes its own
  rules.
- `tests/launcher.rs`: launches through `connector::Service` against real
  podman: socket and credential access by UID, task egress, Spec of each
  Python connector, ownership records, and recovery after killed owners,
  killed recoveries and late ingress grants.
- `tests/platform.rs` (`--platform`): a Python capture, derivation and
  materialization through flowctl and a local stack's reactor, with
  fixtures under `fixtures/platform/`.
- `tests/release.rs`: the task's resource cleanup, against stand-in
  commands.
- `tests/publish.rs`: `ci:docker-images` never pushes a VMM image which did
  not pass `ci:connector-vmm-gate`, against a stand-in docker.

Unit tests own what needs no VM: `connector`'s `readiness_tests.rs`
(readiness, cancellation and refused launches against a scripted engine)
and `connector-vmm`'s resolver, ruleset and boundary tests.

## Harness

- `mise/tasks/ci/connector-vmm-kvm` builds the binaries and images, installs
  or verifies the host boundary, stages the endpoints, and writes the run
  configuration. It builds the packaged VMM image, which `--platform` uses,
  and one with connector-vmm's `test-controls` feature for the suite.
  `connector-vmm-kvm-lib.sh` tracks and releases resources.
- `src/run.rs` owns test VMMs and their resources.
- `src/launcher.rs` and `fixtures/podman.sh` wrap podman for the launcher
  tests: recorded calls, holds, faults, and `launcher::Api`, a test-owned
  podman API service.
- `src/host.rs` reads kernel state. `src/guest.rs` talks to the probe server
  in `fixtures/probes.py`.
- `src/dns.rs`, `src/endpoint.rs`, `src/netns.rs` and `fixtures/bind.py`
  serve controlled DNS, TCP and UDP in a network namespace one routed hop
  from the host. Endpoints use `192.31.196.240/28`, which the ruleset
  treats as public, and excluded decoys use `198.18.255.240/28`. Both are
  blackholed beneath their test routes.

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
