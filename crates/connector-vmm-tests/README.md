# connector-vmm-tests

The connector VMM's KVM integration suite. It boots real guests under
`flow-connector-vmm` on the reference launch line, and asserts what the unit
tests of `connector-vmm` and `guest-init` cannot: an applied ruleset, a
resolver writing a real kernel's set, a guest that boots, what the host can see
of all three, and what the host boundary still refuses a VMM that has removed
its own ruleset.

## Running it

This suite is opt-in and is not run by GitHub Actions or `ci:platform-test`.

`mise run ci:connector-vmm-kvm` is the only way in, and the only task in the
repository that needs KVM, podman or sudo. It needs an x86_64 host with
`/dev/kvm`, passwordless sudo, rootful podman, docker buildx, python3,
`x86_64-linux-musl-gcc` from `musl-tools` and `conntrack`, and fails with one line naming
whichever is missing. Missing KVM never becomes a
skipped suite.

- With a test-name argument it runs only matching tests and says the run is
  partial; of the suite, it also skips connector-init's vsock test.
- `--allow-missing-vsock-loopback` lets that one test skip where the module
  cannot load. The suite's own guest still proves connector-init serving over
  vsock, through `spec_rpc_through_init_sock`.
- `--platform` runs [the platform proof](#the-platform) instead of the suite,
  on this checkout's local stack, which it starts and stops. It also needs
  Docker, the stack's own prerequisites, and the public network.

`mise run ci:nextest-run` compiles the suite, but its default profile excludes
`connector-vmm-tests::kvm`, `connector-vmm-tests::launcher` and
`connector-vmm-tests::platform`; only the harness's unit tests run there. The
`connector-vmm-kvm` and `connector-vmm-platform` profiles in
`.config/nextest.toml` run the rest, and a test started any other way fails
naming the task.

## Roadmap

- `mise/tasks/ci/connector-vmm-kvm`: the prerequisites, the three binaries, the
  VMM image (buildx, cached), the fake VMM image, the test guest image, the
  host boundary, the endpoint namespace, `run.json`, then connector-init's
  vsock test and this suite, with the static `flow-connector-init` first on
  `PATH`. With `--platform`, the stack, its reactor's drop-in and
  `platform.json` instead.
- `mise/tasks/ci/connector-vmm-kvm-lib.sh` and `tests/release.rs`: the
  task's resources list and its release, and the release's refusals, failures
  and retries, run against stand-in commands. `tests/wrapper.rs` likewise runs
  `fixtures/podman.sh` against stand-ins for sudo and podman.
- `tests/kvm.rs`: the assertions, one test per behaviour. The `boundary_*`
  tests are the host boundary's.
- `tests/launcher.rs`: `crates/connector`'s VMM launches, driven through an
  in-process `connector::Service`, and through other processes which the
  tests kill. See [The launcher](#the-launcher) and
  [Recovery](#recovery).
- `tests/platform.rs` and `fixtures/platform/`: Python derivations through
  flowctl and a local stack's reactor, which the tests also kill, starve of
  protection, and run ordinary tasks beside. See [The platform](#the-platform).
  `tests/common/rpc.rs`: the RPC both test binaries hold on a VMM's socket.
- `src/launcher.rs`, `fixtures/podman.sh` and `fixtures/gate.py`: each
  launcher test's own directory, the podman its service runs (with holds,
  faults and a readiness gate), what a launch left behind,
  and the host footprint of a running one.
- `src/launch.rs`: the reference lines: a VMM's own network, its `podman run`,
  and `boundary` from the VMM image. Their snapshots are what a launcher must
  reproduce byte for byte, apart from ids and the container's limits;
  `crates/connector`'s VMM plan is tested against them.
- `src/run.rs`: `run.json`, the resources list, staging, the `Vmm` guard and
  its network, sidecar containers, scratch network namespaces, waits, and the
  stderr framing check.
- `src/netns.rs` and `fixtures/bind.py`: sockets bound in another network
  namespace and passed back to the unprivileged test.
- `src/guest.rs`: the control channel to `fixtures/probes.py`, and `once_in`,
  which runs one probe as root in a container's network namespace.
- `src/host.rs`: what the host reads of a running VMM (mountinfo, conntrack,
  rule counters, `@resolved`, who holds an inode), over pure parsers. Inode
  checks include birth times so a reused number cannot pass for a leaked file.
- `src/dns.rs`, `src/endpoint.rs`: the scripted nameserver and the TCP
  endpoints the guest reaches.
- `fixtures/probes.py`: the guest side, stdlib Python. `fixtures/guest.Containerfile`
  adds it to `derive-python:stable`. `fixtures/policies/`: the policies tests
  launch with.

## Privilege model

Every podman call and every touch of the state directory goes through
`sudo -n`; cargo, nextest and the tests run as the invoking user. Rootful
podman is the production shape, and so are its semantics: `type=image` mounts,
`CapEff`, `rp_filter`, masquerade out of a bridge. The suite dials `init.sock`
unprivileged, which works because the VMM sets `umask(0)` and the launcher's
`sock/` directory is traversable. A launcher test's service makes its state
directories and connector mount unprivileged, in a directory root created for
it, and reaches podman only through its `fixtures/podman.sh`. An owner in a
PID namespace of its own (`sudo unshare --pid`, then `setpriv` back to the
invoking user) has its podman run by systemd in the host's PID namespace, as
podman records container PIDs as it sees them and no process can rejoin an
ancestor PID namespace.

## The host boundary

The task installs `flow-connector-vmm boundary` before any VMM network exists,
and records it, unless the host already has tables that verify, which it uses
and never removes. Tables that are present but do not verify are another
owner's, and the task stops rather than replace them. Every VMM a test starts
is on a network of its own, named as its container is, whose bridge carries
the `fvm` prefix, so the whole suite runs behind the boundary, as production
would.

- `boundary_after_flush` flushes two VMMs' rulesets, then probes from one of
  them (as root in its network namespace, which is more than the VMM may do
  there) and from its guest, once the VMM masquerades it again. Every
  destination that must stay unreachable has a listener that would accept
  without the boundary: the host's addresses, the other VMM, a second
  container on the first VMM's bridge, an ordinary container, a decoy in an
  excluded range, SMTP, and the bridge's IPv6 link-local. It repeats the
  meaningful cases from spoofed sources, rerouted and pinned next hops and a
  new MAC. Public traffic, fragmented UDP and the VMM's own gateway nameserver
  work.
- `boundary_detects_tampering` installs into a scratch namespace and checks
  that verify refuses each edit, that install over a vanished table names the
  VMM bridges it found, and that remove refuses beside one. The host's own
  tables are never edited.
- `boundary_holds_across_reinstalls` replaces the host's tables eight times,
  and reloads netavark's rules, under a held flow and a blocked dialer. The
  replacements are identical, including when the tables were adopted.

## The launcher

`tests/launcher.rs` asks an in-process `connector::Service`, configured with a
VMM, for a derivation's Spec of `derive-python:stable`, the one eligible
connector. The service's state directory, TMPDIR and podman are each test's
own, in `launcher-<hex>` beneath the run's directory. That podman is
`fixtures/podman.sh`: rootful podman through sudo, which appends each network
and container a launch is about to create to the resources list and logs every
call as the launcher made it. While a test's control files ask, it holds a
step's first call, or its Nth, as the test's user or as root beneath sudo; fails
a step; holds the VMM's guest before connector-init starts (below); creates
the VMM's container without `/dev/kvm`; or runs the boundary's verification in
a named network namespace. A hold catches one call, never another launch's. The test process stays
unprivileged and dials `init.sock` itself.

The readiness gate (`launcher::gate_readiness`) uses `--as-root-exec` to
stop a guest shell before connector-init starts. `fixtures/gate.py` observes
its checkpoint line on `start --attach`'s stderr and marks the hold. The guest
stays alive without starting init's health service or idle watchdog.

- With the fake image (`spec_through_the_fake`): the launcher's whole call
  sequence (snapshotted), real `boundary verify` from the fake, its own
  labeled network, the record, connector mount and state, `create` then
  `start --attach`, readiness, the socket dial and the Spec exchange through
  connector-init, `Started` echoing VMM execution, and a teardown and release
  finished before the session's end. What the fake does not stand in for is
  listed in [`crates/connector-vmm`](../connector-vmm/README.md#images).
- With the real image (`spec_through_the_vmm`): the same, through a booted
  guest in which derive-python runs as its image's user, `nobody`. Every engine
  call checks that synthetic platform secrets are absent before sudo.
- Abandoned starts: while the network is created (it is finished, then
  removed, and the VMM never runs), while the container is created (it is
  finished, then removed, and never started), while the VMM is up but its
  guest is held before connector-init, and after `Started` (the whole host footprint,
  below, is measured gone). Each leaves nothing. The tests read the session
  while awaiting their held step, so startup failures report their error and
  logs. `a_failed_start_ends_the_wait_for_readiness` covers a failed start.
- Failures: a removal podman refuses (a sidecar on the VMM's network) is an
  error log to the session naming the record, the rest is removed, and the
  record is kept as it was and let go, for the next launch to finish once the
  sidecar is gone; a VMM image whose `boundary verify` fails is refused as
  `FailedPrecondition` before any network; a VMM which exits before readiness
  (an empty scratch disk) fails with its own diagnostics in the session's
  logs.
- Task egress (`task_egress_on_a_public_plane` and three more), on services
  of public and private planes. The launcher plans and writes the policy from
  the request's execution, and the test reads it back while podman holds the
  start. The test's podman appends only two test-only VMM flags to `create`
  (`launcher::vmm_flags`): `--resolver-upstream`, to the scripted
  nameserver, and `--as-root-exec`, a probe which resolves and connects to
  each name as guest root before connector-init starts and logs what it
  found. Declared hosts: an exact name and not its subdomain, every name
  beneath a wildcard's base and not the base, and never a name answering
  with the decoy. A public plane without a declaration holds the task to
  derive-python's own hosts; a private one leaves it any public destination
  but still never the decoy, by name or address, until the task declares
  egress, even an empty one. The launch's egress log line and the resolver's
  refusal lines are asserted from the session's logs.

## Recovery

A recovery test kills a launch's owner as a crash would, with SIGKILL, then
has another launch beneath the same state directory release what it left,
and checks it is gone before the test's own cleanup. The owner is this test
binary in another process, running only `owner_process`, which reports on
stdout and holds its session until its stdin closes. Recovering launches are
in-process, except in `owners_alike_by_pid`.

- Killed while podman, held, is about to create the network
  (`a_kill_while_the_network_is_created`) or container
  (`a_kill_while_the_container_is_created`): the orphaned command holds the
  record's lock, a launch meanwhile passes it over, and the next, once the
  command has created what it was creating, releases it.
- Killed once podman created the container but before the owner read its ID
  (`a_kill_before_the_container_id_is_read`): found by its label.
- Killed with its guest held before connector-init, and while serving a session: the whole
  footprint is gone, the container by the recovering launch's own removal of
  its ID. A live owner sharing the state directory, and one beneath another
  as another stack's would be, are untouched, then end cleanly.
- A failed container query (a fault in the recovering launch's podman, for
  the dead owner's token only) leaves everything, running; the next launch
  releases it.
- Owners in PID namespaces of their own, each PID 1: a live one is spared by
  a launch sharing its PID, and a dead one is released by another.
- Records which prove nothing: a well-formed record naming an unlabeled
  container and network of its name and marking no directory is removed with
  nothing else; records of another version, naming a foreign mount, claiming
  another id, or torn, each marking directories made, are kept with all they
  name; so are a live owner's, and a state directory with no record.

The footprint of a running launch (`launcher::footprint`) is its container by
ID, network, network namespace, VMM and conmon processes (by PID and start
time), cgroup, container storage layer, bridge and host veths (by name and
index), state, record and mount. The VMM's scratch disk is an unnamed file it
holds open, so it goes with the VMM's process, and the guest's tap with the
network namespace. A resource counts as gone only when it was seen to be
absent: a podman listing, root's `lstat`, `/proc` read or `ip` which fails
fails the test, rather than reading as a clean host. connector-init ends a VMM which serves no RPC within
seconds of its start or its last RPC, which would race a release of a running
container, so tests which need one keep an RPC open on its `init.sock`.

The host's boundary tables are only ever verified here: a missing or edited
table is the `boundary_detects_tampering` test's, in a scratch namespace.

## The platform

`mise run ci:connector-vmm-kvm --platform` runs `tests/platform.rs` through
flowctl and this checkout's reactor, which serves both the agent's Validate and
the tasks' shards, one test at a time against one stack. It refuses a running
stack: the task starts and stops its own, wiping state as `local:stop` does.

The reactor's `connector-vmm.conf` drop-in adds `CONNECTOR_VMM_*`, `TMPDIR`
and a `PATH` starting with the static `flow-connector-init`. flowctl runs
beside that static binary, avoiding the dynamically linked workspace build.
Both launchers share `platform-<hex>/` under `/var/tmp/connector-vmm-kvm`,
with `fixtures/podman.sh` recording calls and resources, and shared `state/`
and `tmp/` directories so each launcher can recover the other's leftovers. A
test may give a flowctl a `launcher-<hex>/` podman, state directory or TMPDIR
of its own instead, as another stack's would be.

`fixtures/platform/` captures greetings with `source-hello-world` and numbers
them with a VMM Python derivation. Its `humanize` dependency is absent from
the image, forcing installation from PyPI. The fixture declares `example.org`
to restrict the local plane's otherwise open public egress; its documents
report resolution of `pypi.org` (a default), `example.org` (declared) and
`example.com` (neither). Tests after the first stage the fixtures under a
prefix of their own, `acmeCo/vmm-<test>/`, and write variants of the numbers
derivation: renamed, and for `ordinary-numbers` without `vmm` or `egress`.
`ordinary.flow.yaml` adds that ordinary derivation and a `materialize-postgres`
materialization of greetings and of both derivations into the stack's
database.

- `a_python_derivation_through_the_platform`: preview, publication and derived
  greetings, launch attribution, egress policy and the static init mount.
- `a_preview_killed_while_its_container_is_created`: an orphaned create holds
  its record's lock; another launcher skips it, then recovers its resources
  after the command finishes.
- `a_reactor_killed_while_serving`: shard recovery and resumed derivation,
  preserving live previews in shared and independent state directories and
  ordinary tasks. Also checks recovery after killing a serving preview.
- `a_reactor_killed_while_its_network_is_created`: a rootful network create
  outlives the reactor; both launchers skip its locked record and the reactor
  recovers it after the command finishes.
- `task_update_through_the_mount`: synthetic updates staged read-only and
  renamed over `task-update.json`, read by the image's user through
  `CONNECTOR_MOUNT`. Covers a file present at session start and one created
  after the first read. Fresh opens see replacements within 30 seconds;
  held descriptors retain their first generation.
- `refused_launches_beside_ordinary_tasks`: missing KVM, a missing boundary and
  an altered boundary refuse preview, publication validation and shard
  relaunch. A live `docker events` subscription checks there is no ordinary
  container fallback; ordinary tasks keep materializing. Boundary faults use
  a scratch network namespace, leaving the host's boundary intact.

The reactor is a systemd user unit: its user cannot kill root's processes.
`systemctl kill` may report access denied while killing the reactor and its
user-owned clients. systemd restarts the unit while root's orphaned processes
remain. The VMM runs under podman until recovery removes it or connector-init
ends it after its RPCs close.

Preview exit and shard deletion can drop runtimes before session teardown;
see [connector recovery](../connector/README.md). The tests release what those
leave with a later launch, and each checks every launch it made is gone before
it ends.

What each command printed, every podman's calls, the reactor's journal and
each test's evidence are kept in `$CARGO_TARGET_DIR/connector-vmm-kvm/run`.
Cleanup stops the stack before removing VMM resources and the host boundary.

## The run, and cleaning up after it

Everything lives under `/var/tmp/connector-vmm-kvm`, which is short so that
`state/fv_<16 hex>/sock/init.sock` fits a Unix socket address, and on the root
filesystem so the scratch backing supports `O_TMPFILE` and records birth times.

Each test removes what it created when it ends, pass or fail. The task's exit
and interrupt trap removes whatever is listed, newest first, and a start that
finds a non-empty list prints it and removes exactly those entries. Nothing is
removed by age or by name pattern. nextest runs each test in its own process
group, so a task killed outright leaves its running tests to finish and clean
up on their own, holding the lock until they do. Their build, like every
other command which may start a daemon, runs without the lock, so that no
daemon holds it past the run.

A listed stack is stopped first, and must then be shown stopped: none of its
units still running, by a query which succeeded, since `local:stop` carries on
past units which fail to stop. No launch beneath a platform or launcher directory may
still hold its record's lock either, whether a launcher or a podman command
one fenced. Otherwise nothing at all is removed, the boundary included, since
a launcher could still make a VMM network once it had gone: the list is kept
whole, the run fails saying why, and the next run tries again.

Past those checks, each entry is removed or shown already gone, by a listing
which succeeded rather than a lookup whose failure reads as absence. Any other
entry is kept in the list, the run fails saying why, and the next run tries
again. A VMM, network or boundary which is kept may still use what was made
before it, so that is kept untried: state directories and the ownership
records in them, the boundary, and images, among them the VMM image a kept
boundary's removal runs.

Built images leave with the run; pulled base images and the buildx cache stay.
Built images and the buildx builder only take space, so failing to remove them
is reported without failing the run, and their entries are dropped.
Launch output lands in `$CARGO_TARGET_DIR/connector-vmm-kvm/run`, and so do
the launcher tests' podman calls, each as `launcher-<hex>.calls`, copied
before the release removes the test's directory.

## Controlled endpoints

The ruleset admits only globally reachable addresses, so every destination a
guest may reach belongs to somebody. The suite borrows unused addresses in
AS112-v4, `192.31.196.0/24`, which IANA marks globally reachable and the
baseline deliberately omits. They live in the network namespace
`connector-vmm-kvm`, one routed hop from the host over the veth pair
`vmm-kvm0`/`vmm-kvm1`, because the host boundary refuses a VMM anything
addressed to the host itself. The host's end, `192.31.196.254`, is the address
endpoints see VMM traffic masqueraded to. The decoy `198.18.255.241`, in an
excluded range, is routed the same way, so only the boundary keeps a VMM from
it. Both `/28`s are blackholed beneath their routes and the namespace routes
only back to the host, so nothing addressed there can leave the host, even if
the link goes away mid-run.

The scripted nameserver, which a VMM forwards to through `--resolver-upstream`,
and the TCP endpoints listen there on ephemeral ports, on sockets
`fixtures/bind.py` creates as root in the namespace and hands to the
unprivileged test (`src/netns.rs`). The nameserver answers only what a test
scripts, and never forwards. Only `public_https_smoke` uses the internet,
through the VMM's gateway resolver as a production VMM would, so an outage
fails that test alone.

## What this does not prove

- **Refresh without a gap.** A refresh that extends authorization under a
  held connection is shown here, but an established flow survives expiry
  anyway. That no window exists rests on the delete and add being one
  nf_tables transaction, not on anything this suite observes.
- **Share confinement.** The `..` probes check the guest's path handling. The
  guest's own VFS resolves `..`, so they say nothing about a guest whose kernel
  is compromised.
- **Name enforcement after a compromised VMM.** The allowlist and TTL claims
  assume the VMM is intact. After a flush the boundary still excludes private,
  special-use and host destinations, siblings and IPv6, but any public
  address is reachable.
- **The launcher against a broken boundary, on the host.** The platform
  tests take a launch's verification to a missing and an altered boundary,
  in a namespace of the test's; no test takes the host's own tables away from
  a launch.
- **Docker.** The suite checks ordinary containers on podman networks; it
  does not exercise Docker's rules.
- **A production reactor's death.** The platform tests kill a local stack's
  reactor, a systemd user unit driving podman through sudo, and flowctl
  previews. A reactor in a container driving the host's podman service, a
  remote podman, a host reboot, and a launcher whose podman runs in a PID
  namespace other than the host's are not exercised.
- **A public plane's reactor.** The stack's reactor is a local plane's. A
  public plane's egress is shown only by the launcher tests' in-process
  services, and its Python admission only by `crates/connector`'s unit tests.
- **Readiness after a libkrun panic.** `flow-connector-vmm` checks for KVM
  before libkrun runs, and no test here makes libkrun panic after that. That a
  backtrace never passes for readiness, which is connector-init's health and
  not stderr, is shown without KVM by `crates/connector`'s readiness tests.
- **Credentials.** `task-update.json` is synthetic: no producer mints or
  refreshes it, and nothing here reaches the APIs it names.
- **Teardown of a session whose runtime goes first.** A stopped shard's and a
  finished preview's session launches are dropped with their runtimes (see
  [`crates/connector`](../connector/README.md)), so the platform proof shows
  them released by the next launch sharing their state directory, not by
  their owners.
- **Other connectors.** Only `derive-python` is eligible. The launcher tests
  run its Spec; the platform proof runs its Validate, a preview and a
  published task, and nothing else.
- **Real destinations.** The controlled endpoints are borrowed global
  addresses kept on the host, not services on the internet.
