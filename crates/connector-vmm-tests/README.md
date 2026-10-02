# connector-vmm-tests

The connector VMM's KVM integration suite. It boots real guests under
`flow-connector-vmm` on the reference launch line, and asserts what the unit
tests of `connector-vmm` and `guest-init` cannot: an applied ruleset, a
resolver writing a real kernel's set, a guest that boots, and what the host
can see of all three.

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
  VMM image (buildx, cached), the test guest image, the network and endpoints,
  `run.json`, then connector-init's vsock test and this suite.
- `tests/kvm.rs`: the assertions, one test per behaviour.
- `src/launch.rs`: the reference launch line. Its snapshots are what a
  launcher must reproduce byte for byte, apart from ids.
- `src/run.rs`: `run.json`, the resources list, staging, the `Vmm` guard,
  waits, and the stderr framing check.
- `src/guest.rs`: the control channel to `fixtures/probes.py`.
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
baseline deliberately omits. They sit on a dummy link on the host, `vmm-kvm0`,
under a blackhole route for `192.31.196.240/28`, so nothing addressed there can
leave the host, even if the link goes away mid-run.

The scripted nameserver, which a VMM forwards to through `--resolver-upstream`,
and the TCP endpoints bind there unprivileged on ephemeral ports. The
nameserver answers only what a test scripts, and never forwards. Only
`public_https_smoke` uses the internet, through podman's resolver as a
production VMM would, so an outage fails that test alone.

## What this does not prove

- **Refresh without a gap.** A refresh that extends authorization under a
  held connection is shown here, but an established flow survives expiry
  anyway. That no window exists rests on the delete and add being one
  nf_tables transaction, not on anything this suite observes.
- **Share confinement.** The `..` probes check the guest's path handling. The
  guest's own VFS resolves `..`, so they say nothing about a guest whose kernel
  is compromised.
- **Enforcement after a compromised VMM.** Every egress claim assumes the VMM
  is intact. A VMM holding `CAP_NET_ADMIN` can remove its own rules, and
  enforcement that survives that belongs outside it, in the launcher's network
  integration.
- **Crash recovery.** Harness cleanup is a test's own. Recovering what a killed
  reactor leaves behind is the launcher's.
- **Real destinations.** The controlled endpoints are borrowed global
  addresses kept on the host, not services on the internet.
