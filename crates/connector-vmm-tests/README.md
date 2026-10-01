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
`connector-vmm-tests::kvm`; only the harness's unit tests run there. The
`connector-vmm-kvm` profile in `.config/nextest.toml` runs the rest, and a
KVM test started any other way fails naming the task.

## Roadmap

- `mise/tasks/ci/connector-vmm-kvm`: the prerequisites, the three binaries, the
  VMM image (buildx, cached), the test guest image, the host boundary, the
  endpoint namespace, `run.json`, then connector-init's vsock test and this
  suite.
- `tests/kvm.rs`: the assertions, one test per behaviour. The `boundary_*`
  tests are the host boundary's.
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
`sock/` directory is traversable.

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
- **The launcher's side.** The boundary tests use the reference lines; that a
  launcher verifies before every VMM network it creates is the launcher's to
  prove.
- **Docker.** The suite checks ordinary containers on podman networks; it
  does not exercise Docker's rules.
- **Crash recovery.** Harness cleanup is a test's own. Recovering what a killed
  reactor leaves behind is the launcher's.
- **Real destinations.** The controlled endpoints are borrowed global
  addresses kept on the host, not services on the internet.
