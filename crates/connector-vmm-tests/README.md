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

- With a test-name argument it runs only matching tests, says the run is
  partial, and skips connector-init's vsock test.
- `--allow-missing-vsock-loopback` lets that one test skip where the module
  cannot load. The suite's own guest still proves connector-init serving over
  vsock, through `spec_rpc_through_init_sock`.

`mise run ci:nextest-run` compiles the suite, but its default profile excludes
`connector-vmm-tests::kvm` and `connector-vmm-tests::launcher`; only the
harness's unit tests run there. The `connector-vmm-kvm` profile in
`.config/nextest.toml` runs the rest, and a KVM test started any other way
fails naming the task.

## Roadmap

- `mise/tasks/ci/connector-vmm-kvm`: the prerequisites, the three binaries, the
  VMM image (buildx, cached), the fake VMM image, the test guest image, the
  host boundary, the endpoint namespace, `run.json`, then connector-init's
  vsock test and this suite, with the static `flow-connector-init` first on
  `PATH`.
- `tests/kvm.rs`: the assertions, one test per behaviour. The `boundary_*`
  tests are the host boundary's.
- `tests/launcher.rs`: `crates/connector`'s VMM launches, driven through an
  in-process `connector::Service`, and through other processes which the
  tests kill. See [The launcher](#the-launcher) and
  [Recovery](#recovery).
- `src/launcher.rs`, `fixtures/podman.sh` and `fixtures/gate.py`: each
  launcher test's own directory, the podman its service runs (with holds,
  faults and a readiness gate), what a launch left behind, and the host
  footprint of a running one.
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
  rule counters, `@resolved`, who holds an inode), over pure parsers.
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
and container a launch is about to create to the resources list, logs every
call, and holds a step, or withholds the VMM's readiness byte through
`fixtures/gate.py`, for as long as the test asks. The test process stays
unprivileged and dials `init.sock` itself.

- With the fake image (`spec_through_the_fake`): the launcher's whole call
  sequence (snapshotted), real `boundary verify` from the fake, its own
  labeled network, the record, connector mount and state, `create` then
  `start --attach`, readiness, the socket dial and the Spec exchange through
  connector-init, `Started` echoing VMM execution, and a teardown and release
  finished before the session's end. What the fake does not stand in for is
  listed in [`crates/connector-vmm`](../connector-vmm/README.md#images).
- With the real image (`spec_through_the_vmm`): the same, through a booted
  guest in which derive-python runs as its image's user, `nobody`.
- Abandoned starts: while the network is created (it is finished, then
  removed, and the VMM never runs), while the container is created (it is
  finished, then removed, and never started), while the VMM is up but its
  readiness is withheld, and after `Started` (the whole host footprint,
  below, is measured gone). Each leaves nothing.
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
- Killed with its readiness withheld, and while serving a session: the whole
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

## The run, and cleaning up after it

Everything lives under `/var/tmp/connector-vmm-kvm`, which is short so that
`state/fv_<16 hex>/sock/init.sock` fits a Unix socket address, and on the root
filesystem so the scratch backing supports `O_TMPFILE`.

Each test removes what it created when it ends, pass or fail. The task's exit
and interrupt trap removes whatever is listed, newest first, and a start that
finds a non-empty list prints it and removes exactly those entries. Nothing is
removed by age or by name pattern. nextest runs each test in its own process
group, so a task killed outright leaves its running tests to finish and clean
up on their own, holding the lock until they do.

Built images leave with the run; pulled base images and the buildx cache stay.
Launch output lands in `$CARGO_TARGET_DIR/connector-vmm-kvm/run`.

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
- **The launcher against a broken boundary.** The boundary tests use the
  reference lines, and the launcher tests refuse a verify which fails. No test
  takes the host's own tables away from a launch.
- **Docker.** The suite checks ordinary containers on podman networks; it
  does not exercise Docker's rules.
- **The reactor's own death.** Owners here are `connector::Service`s in test
  processes, not a reactor, preview or proxy process killed and restarted.
  Nor does anything here reboot the host, use a remote podman service, or run
  a launcher whose podman is in a PID namespace other than the host's.
- **Other connectors.** Only `derive-python` is eligible, so only its Spec
  runs through the launcher; no Validate, Open or published task does.
- **Real destinations.** The controlled endpoints are borrowed global
  addresses kept on the host, not services on the internet.
