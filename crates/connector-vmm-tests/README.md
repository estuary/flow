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
- `mise run ci:connector-vmm-kvm --platform` discovers, dry-runs, publishes
  and runs Python captures, derivations and materializations through the
  control plane of this checkout's local stack, whose reactor runs them in
  VMMs. The task starts and stops the stack and refuses to start if it is
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
  materialization through a local stack's control plane and reactor, with
  fixtures under `fixtures/platform/`. flowctl launches no VMM: each test
  attributes every launch to the reactor, and checks its egress, data,
  restarts, teardown and pruning.
- `tests/release.rs`: the task's claim of its root and its resource
  cleanup, against stand-in commands and inert temporary fixtures.
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
  `connector-vmm-kvm-lib.sh` claims the task's root, and tracks and
  releases resources.
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
keeps guest Unix socket paths within their limit. Output is retained under
`$CARGO_TARGET_DIR/connector-vmm-kvm/run`.

The task trusts only the invoking user, this checkout and root. Any user may
create the root's path in `/var/tmp` first, so `claim` in
`connector-vmm-kvm-lib.sh` takes the root only as follows:

- Each directory above it is root's or the invoking user's, and writable by
  others only if sticky.
- `mkdir` creates the root with mode 0700, or fails if anything is there.
- One lstat then shows the root to be a directory of the invoking user's,
  with mode 0700. Its sticky parent lets no one else rename or replace it.
- `lock`, `resources`, `members`, `run.json` and `platform.json` are each
  absent, or a regular file of that user's, writable by no one else, with no
  other name. The lock is created once and never replaced, and excludes
  concurrent runs host-wide.

Tests check the root and each file the same way before they read `run.json`
or `platform.json`, append to `members` and `resources`, or read or lock a
launch's record. The two launcher tests which check access from another UID
start from the launcher directory, opened as root, so the private root takes
no part in what they measure. The task writes `run.json`, `platform.json`
and the rewritten list to a new file, and renames it into place.

Each producer (the task, `run::record` and `fixtures/podman.sh`) appends an
entry to `members`, then to `resources`, before it creates the resource. The
task also vouches in `members` for the VMM image ID it loaded and the
test-controls tag it built. Tests release their own resources, and the
task's exit and interrupt traps release what the list holds, newest first.
A release reads the list and `members` once each, and acts on no entry
unless every entry passes these checks:

- `members` holds it, exactly as the list does.
- Its kind and name are ones the task or a test records. Images must match
  an exact repository and tag recipe.
- An image ID or boundary is the loaded image `members` vouches for. A
  test-controls tag or grant is the built tag it vouches for.
- A stack or the reactor's drop-in is this checkout's. The drop-in's
  directories beneath `$HOME` are not links, and only the invoking user or
  root can change them.
- A route, the endpoints' namespace and their link are the task's own.
- A directory lies beneath the root, through directories only the invoking
  user or root can change.

Otherwise, or if either file cannot be read whole or holds a NUL byte, the
list is kept whole and nothing is stopped, tried or removed. Cleanup also
refuses to proceed while a tracked stack or launch owner is still active, or
if either file changed while the stack stopped. It also refuses while
a launch's state directory or record is a link or not the invoking user's,
or a record has another name. A failed removal, an image's or builder's
included, keeps its entry for the next invocation to retry. It also keeps the
images, boundary and directories that a kept container, network, volume,
grant or boundary may still use. Once the list is empty, so is `members`.
Resources are never selected for removal by age or a name pattern. Existing
verified host-boundary tables are adopted and left installed; tables created
by the task are removed when safe. Pulled base images and build caches remain
available for later runs.

Limitations:

- Root and the invoking user's own processes are trusted, and nothing here
  defends against them. `members` binds entries to what this user's
  producers wrote. It does not prove that a resource exists or is unused.
- One user at a time can run the task on a host. A root of another user's,
  or anything else at its path, is refused until its owner or root removes
  it.
- A stack which another checkout's interrupted run recorded is refused. Run
  the task from that checkout to release it.
- `--platform` refuses a drop-in directory beneath `$HOME` which is a link,
  or which others can write, such as a group-writable `~/.config/systemd`.
- An image a new test builds needs its recipe in `bound`, in
  `connector-vmm-kvm-lib.sh`, or release refuses the list.
- The ignored test `a_root_another_user_owns_is_refused` chowns its own
  temporary root with sudo. Run it explicitly:
  `mise exec -- cargo nextest run -p connector-vmm-tests --test release --run-ignored ignored-only`.

### A root left by an earlier version

Older task versions created a root-owned directory. The current task refuses
it and preserves its recovery records. Before retiring it, arrange exclusive
host access and establish that the old task and its launchers have stopped.
Use trusted run output and live host state to identify remaining harness
resources. Treat the old ledger as an unverified inventory. After releasing
confirmed resources and preserving anything unrecognized, a host administrator
can retire the old directory and allow the task to create its private root.
