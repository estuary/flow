# connector-vmm-tests

The connector VMM's KVM integration suite. It boots real guests under
`flow-connector-vmm` on the reference launch line, and asserts what the unit
tests of `connector-vmm` and `guest-init` cannot: an applied ruleset, a
resolver writing a real kernel's set, a guest that boots, a root even guest
root cannot write, what the host can see of all of these, and what the host
boundary still refuses a VMM that has removed its own ruleset.

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
- `--ignored c11::lifecycle_cycles` runs the suite's one opt-in test,
  [lifecycle cycles](#lifecycle-cycles), and nothing else.
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

## A noisy tenant

`noisy_tenant` runs a real derive-python VMM, its derive RPC held open,
which must answer a Spec over its `init.sock` once a second, identical to its
first and within a budget of ten times the slowest of a twelve-second paired
baseline (at least two seconds; a budget over five seconds ends the test as
inconclusive). Beside it, one at a time, an offender of 256 MiB guest RAM and
scratch disk and one host CPU of quota (`--cpus=1`, two vCPUs): four busy
workers (`spin`) for over twenty seconds, its cgroup's usage at the quota and
throttled; a guest process touching 384 MiB, more than the guest has and less
than its container's 512 MiB, whose death is attributed to the guest (its
`vmstat` `oom_kill`, or kernel text when the guest dies) or to the host cgroup
(the offender's and its parent's `memory.events`, and `dmesg`), only the first
passing; and, with 10 GiB free beneath the suite's root, its scratch disk
filled to ENOSPC, then held full for twenty seconds by a 1 MiB allocation
refused every second (a write of a few bytes can still fit what the fill
leaves). Each offender is removed, `podman rm` included, within 60 seconds,
the scratch file unreferenced and backing empty after the fill, and a fresh
one must start and write. `tests/c07/` writes each variant's probes and
counters to `c07-<variant>.evidence` as they happen. A Spec is connector
work, not a derivation's throughput.

## A host cgroup OOM

`host_oom` (`tests/c08/`) launches three derive-python VMMs (512 MiB guest,
768 MiB container) through the in-process launcher, sharing one state
directory: a sibling, an offender, and later a fresh launch. Each session
stays open after its Spec. The offender's own cgroup loses its swap
allowance and has `memory.max` lowered to 16 MiB, under the guest RAM it
has touched, so the host's memcg killer ends the VMM while its session is
live. The proof is the offender's `memory.events` `oom_kill`, read through a
descriptor opened before the fault (the `--rm` container's cgroup is gone
within a second), with its parent's counter and the `dmesg` line naming the
VMM's pid in that cgroup. Then the session must fail within 30 seconds of
the kill and end within 60 seconds of the fault. The test waits up to the
same 60 seconds for the owner to release the VMM. Whether or not it does,
the fresh launch follows, with its own 180 seconds to start. If the owner
left anything, that launch must remove it. Nothing of the dead VMM may
outlast the launch, and the fresh launch must serve the same Spec. The
sibling answers a Spec once a second
throughout, with probes before the fault, within it and after the fresh
launch's Spec, and keeps its processes, footprint and a zero `oom_kill`.
Lowering `memory.max` takes the kernel's limit-write OOM path, not that of
a charge in a faulting vCPU thread. The killer and counters are the same.
Afterwards the test removes the empty, root-owned `oom` file that conmon
leaves in the test's working directory on an OOM, but only if it was absent
before the test (`oom_marker` checks which files count).

## A log flood

`tests/c09/` floods a derive-python VMM's console from `--as-root-exec`,
before connector-init starts, through the in-process launcher: the `start
--attach` client's stderr, `crates/connector`'s stderr pump, and the
session's bounded response channel, read by the session's own receiver. The
guest writes a line of one space, `SERVING`, a forged `started connector
container` and a line split over two writes, then up to 48 MiB of stderr
(1 MiB raw lines, 256 KiB JSON-looking lines, split lines, short JSON logs,
each marked with its sequence, guest time and the bytes before it) and
8 MiB of stdout, within 30 seconds, then a done line and an eight-second
hold. The offender has 768 MiB and one CPU. `baseline` writes four logs;
`slow_reader` reads at most 50 logs a second, and its guest must take at
least half the consumer's time to write what it reads. Both require every
line's sequence (decoded bytes differ from raw ones, so they are reported,
not compared), at most 24 MiB written and unread (guest clock mapped at the
first mark), the forged lines as ordinary logs, Started no sooner than the
guest's exit, a valid Spec, and release. `stopped_reader` stops reading
after 2 MiB for twelve seconds while a sibling answers a Spec, and requires
conmon's reads and the VMM's writes to grow at most 4 MiB over the last
eight (the guest held), conmon's reads at most 24 MiB past the stop, then
cancels and requires release within 60 seconds.
Each requires this process's RSS, and the client's and conmon's, to grow at
most 48 MiB, and is inconclusive without complete samples of them and the
container's cgroup once a second of flood, and four over the hold or the
stopped window. `sampling_unwinds` shows a failure inside a sampled test
unwinds rather than waiting on its sampler. Lines are capped at 1 MiB, so no test here shows the bound for
longer ones: the pump reads a whole line before sending it.

## Credential rotation

`tests/c13/` renames synthetic `task-update.json` generations (index,
`acmeCo-` token, 512 B to 16 KiB payload with its crc32) into a probe
guest's connector mount, 20 to 50 ms apart, by stage and rename, while the
guest's `rotation` probe opens the file afresh in a loop beside one
descriptor held from its first open. `b0_initial_and_one_replacement` and
`rotate_hundred_generations` (initially missing, then 100 generations)
match every distinct fresh read byte for byte against what was published,
and require the held descriptor to keep its first generation and fresh reads
to reach the newest within 30 seconds. virtiofs revalidates the name about
every 5 seconds, so fresh reads skip most generations; that is reported, not
required. `cancel_mid_rotation` tears the VMM down, and `crash_mid_rotation`
SIGKILLs it, once 20 generations are out and the guest has read 50 times and
seen index 10 or later, of 400; the background reader's stderr counts
(length and crc32 against each generation, held reads against the first)
must stay clean. The writer must end on its own failed write after
teardown, or be stopped and joined within 5 seconds after a crash; the mount
must be gone and refuse a late write; a fresh guest reads the newest
generation. The rotator is the test's own: the runtime's refresh task
(`connector::protocol::refresh_task_update`) is not run here. Results are
written to `c13.evidence`.

## A cut package download

`tests/c14/` opens a derive-python derivation in a VMM through the
in-process launcher, sending a derive `Open` itself, so derive-python's own
`uv run` installs its dependencies. One is `c14pkg`, a 3 MiB synthetic wheel
(`fixtures/c14_wheel.py`, the same bytes every run) which the image lacks,
declared as a PEP 508 direct reference to the test's HTTP server on a
controlled address: a derivation's `UV_*` environment is reserved, and a
direct dependency URL lets this fixture choose its package source without a
`UV_*` override. pydantic and pyright still come from PyPI, so these tests
need the public network. The server records each
request's line, User-Agent and body bytes written, and stops and joins when
dropped. `b0_endpoint_cuts_and_stops` checks it alone, without a VMM.
`b0_installs_from_the_endpoint` must fetch the wheel whole, open within 120
seconds of `Started`, and derive documents only a whole payload can render.
`cut_mid_body_then_relaunch` declares the whole length and closes after
1 MiB on every request, uv's retries included. Then the session must fail
within 120 seconds, never open or publish, and log uv's failed download of
`c14pkg` and no finished download or install. Then the same `Open` and URL,
served whole, must install afresh and derive the same documents. uv's cache
is on the VMM's scratch disk, which goes with the VMM, so this shows a
relaunch needs no repair, not that a damaged cache recovers. Results are
written to `c14-<test>.evidence`.

## An answer that changes

`tests/c15/` (`dns_answer_changes`) scripts one allowed name, under
`ttl.json`, through a public address, the decoy, the decoy beside that
address, a second public address beside the decoy (both orders), a CNAME
chain to a third public address and one to the decoy, while the guest holds a
stream to the first address and makes fresh connections every round. Every
scripted TTL is 1 s, which the floor clamps to the 3 s promised. Every
answer naming the decoy must be refused whole: the decoy and the second
address never enter `@resolved` (read as nft text, to the millisecond, every
500 ms) and their listeners see only the host's own control connection; a
condemned answer must not extend the first address, nor make its CNAME
target askable. An authorized address must keep connecting until its latest
answer's promise can lapse, and stop 100 ms after it can have; the held
stream must survive. Each listener dates its accepts and counts its bytes,
the guest's raw DNS responses are kept as hex, and results are written to
`c15-dns_answer_changes.evidence`.

## Authorization that fills up

`tests/c16/` (`dns_capacity`) fills the resolver's two caps, remembered
addresses and remembered CNAME targets, each to exactly `RESOLVED_SIZE`
(1024), under `capacity.json` with every scripted TTL 45 s. Address answers
carry 64 addresses each from 192.31.200.0/21, which the test blackholes in
the VMM's own network namespace and the guest never dials; target answers
carry 32 CNAME records each, sharing one owner, and touch no address. That
shape is synthetic stress of the parser and the cap, not a recursive
resolver's chain. Past either cap an answer
must be SERVFAIL and authorize nothing: the kernel's own `@resolved` count
stays at 1024, an overrun endpoint and the decoy never enter it and their
listeners see only the host's control connection, and every target of an
overrun answer is refused by name without reaching the upstream. A held address still refreshes and connects at the cap. The
kernel set must empty by 2 s past the last expiry, and once the resolver's
10 s reclaim has passed, a fresh name connects to a fresh endpoint and both
caps fill to exactly 1024 again. Forty forbidden names exercise the refusal
log, which must report the first 32 refused names and one line saying it
stopped, and never a capacity SERVFAIL. Memory of the VMM process and its
cgroup is sampled before, at, and after the caps, and everything is written
to `c16-dns_capacity.evidence`.

## When the resolver fails

`tests/c17/` covers the resolver's two failures, which must end differently.
Both run under `ttl.json` with `--debug`, so decision lines are on stderr.

`kernel_update_failure` tampers only with the VMM's own network namespace. It
first proves that namespace is entered through the VMM's pid, and that it is
neither the host's nor the test's. One `nft -f` transaction then deletes the
single rule reading `@resolved` (an accept, so `policy drop` remains) and the
set itself; the host's `flow_vmm_boundary` must stay byte-identical. The guest
then asks for a name never asked before, whose answer needs an insert. The
VMM must exit within 30 s with the set-update error framed on stderr. The
nameserver must have answered the query once, while the guest gets no answer
and stderr has no decision for it. Its listener must see only the host's
control connection. A fresh launch must then authorize the name and connect.

`upstream_timeout` uses `dns::silence` to make the scripted nameserver receive
one name's query and never answer it. The guest must time out at its 8 s.
The resolver must log `dropped=no answer` between 4.5 s and 7 s, as it gives
up after its 5 s `UPSTREAM_TIMEOUT`, and must never retry or insert. The VMM
must keep its pid and starttime, its held stream and its other names. Once the
nameserver answers again, the name must be authorized and connect. Results go
to `c17-<test>.evidence`.

## Refresh under CPU pressure

`tests/c18/` (`ttl_refresh_pressure`) refreshes a one-second TTL, clamped to
`ttl.json`'s 3 s floor, every 1.5 s while fresh connections, each with its own
probe name, run between queries at most ten a second. After an unthrottled
baseline, which fixes the connect-latency budget, the VMM's own container
`cpu.max` is set to a quarter CPU and one guest worker spins, for 25 s.
`cpu.stat` must show throttling over that window and over the expiry that
follows. Each attempt is judged against the answers the guest had seen: one
sent at least its budget before the earliest possible kernel expiry (query
sent plus TTL) must connect within the budget, and one sent after the latest
(answer seen plus TTL) must not; those in between are recorded only. Every
1.5 s window must hold at least two covered attempts. With refreshes stopped
and the pressure on, the 500 ms sampler of `@resolved`, read through
`nsenter` from the host's cgroup, must see the address gone by 2 s past the
latest expiry, and every reading must sit inside the promises. The quota is
restored, the worker killed, and a new answer must connect. A second name
then answers 10 s, 3 s, and a mix with the decoy: the shorter answer must not
shorten the kernel's timeout, the refused ones authorize nothing, and the
longer promise still connects. The resolver handles one query at a time and
the scripted nameserver cannot delay an answer, so reordered in-flight answers
are not covered. Results go to `c18-ttl_refresh_pressure.evidence`.

## The launcher

`tests/launcher.rs` asks an in-process `connector::Service`, configured with a
VMM, for a derivation's Spec of `derive-python:stable`, the one eligible
connector. The service's state directory, TMPDIR and podman are each test's
own, in `launcher-<hex>` beneath the run's directory. That podman is
`fixtures/podman.sh`: rootful podman through sudo, which appends each network
and container a launch is about to create to the resources list and logs every
call as the launcher made it. While a test's control files ask, it holds a
step's first call, or its Nth, as the test's user or as root beneath sudo, or
once its command has finished (`launcher::hold_after`, which marks the hold
with the script's PID and the command's status); fails a step; stalls every
call of a step naming what the test lists (`launcher::stall`, each marked with
the script's PID, when it stalled and the call) until the launcher's timeout
kills it; holds the VMM's guest before connector-init starts (below); creates
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
- `mounts_the_inspected_image_after_a_tag_changes` holds container creation,
  repoints a private local tag from derive-python to a capture image, then
  verifies the guest still serves the inspected derivation's Spec. The wrapper
  skips pulling that test-created tag, which exists only in local storage.
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
- A recovering launch killed partway through its release
  (`a_recovery_killed_*`), beside a live owner's VMM sharing the state
  directory, which must serve a Spec over its `init.sock` at the gate and
  after recovery, and a container, network, state directory and mount no
  record names:
  before and after its container removal, before its network removal (the
  orphaned command running after or before the next launch), and after it,
  when only the mount, state and record remain. The next launch releases
  the rest and removes the record last; the rest is untouched. No engine call
  separates the mount's, state's and record's removals, so no gate falls
  between them. Each writes `c01-<gate>.evidence` to the run's directory.
- Eight launchers released at once by a barrier
  (`eight_launches_recover_shared_orphans`) beneath a state directory
  holding a serving VMM's record and orphans at four stages (claimed only,
  a network, a created container, a serving VMM), which the test keeps
  locked until the race so that their own launches recover none. Each
  launcher's first `ps` is held: the four that take an orphan wait holding
  its lock while the other four launch past them. Each orphan is released
  by exactly the launch which took it, all eight start, and the serving
  VMM answers a Spec over its `init.sock` throughout (`c02.evidence`).
- A network or container create held as root beneath sudo
  (`a_root_held_*_create_outlives_two_launcher_deaths`) outlives the
  SIGKILL of its owner and then of a launch which recovers beside it. The
  same root process (by PID, start time and UID; the root hold marks its
  PID) keeps the record's flock on its stdin throughout, and nothing the
  record names is touched. Let go, it makes what it was making, under the
  record's token, and the next launch releases that and the killed launch's
  claim (`c03-<step>.evidence`).
- A recovering launch whose engine stalls (`a_stalled_*`), its dead owner,
  live peer and recovering launches all real VMMs booting a guest (the dead
  owner's and peer's containers checked to be of the VMM's image by ID),
  beside the peer, which must serve a Spec
  before the stall, every five seconds through it (no two more than ten
  apart), and after, and the
  resources no record names: in the dead owner's container query or
  container removal, which keeps everything; or in its network removal, once
  its container is gone, which keeps the network and the record but removes
  the mount and state. Each stalled podman is watched from its mark until
  connector's 60s command timeout kills it, within a second before and three
  seconds after, and none of its children outlive it. The record is kept
  unchanged, and the launch starts; once the stall is cleared, the next
  launch releases the rest. `stalled_queries_of_three_records_each_time_out`
  stalls three dead records' queries, which time out one after another:
  nothing bounds the scan as a whole, so the launch waits about three
  timeouts, and alone has a longer nextest backstop. Its dead owners are
  killed before their networks, so never boot a guest; its peer and
  recovering launches are real VMMs, the peer probed throughout. The stall
  catches the call in the podman wrapper before it runs the engine, so these
  show the launcher ending a stalled wrapper, not a hung engine's descendants
  (`c05-<variant>.evidence`, `c05-records.evidence`).

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

## Lifecycle cycles

`c11::lifecycle_cycles` (`tests/c11/`) is ignored, so the suite skips it; run
it with `mise run ci:connector-vmm-kvm --ignored c11::lifecycle_cycles`. After
a warm launch, it runs 100 cycles in an order shuffled from a fixed seed, 20
at each boundary, every launch of a real VMM beneath one shared state
directory and TMPDIR: abandoned while podman holds the network create, or
once the container is created (neither boots a guest); abandoned with the
guest held before connector-init, or after `Started`; or with its owner
process SIGKILLed while serving, recovered by the next launch, which must
serve a Spec. Each cycle has its own podman for its holds, notes its
boundary's proof in `c11.evidence` before acting, and must then leave the
shared directories empty and, once booted, nothing of its footprint or a
reference to its scratch disk. A watchdog ends the test if a cycle outlives
120 s. This process hosts every in-process launch and recovery, so its
descriptors are bounded every ten cycles (baseline plus 8) and its resident
memory at the end (plus 64 MiB). Host-wide counts are logged, not asserted.
A final launch must serve a Spec and leave nothing.

## Replacement generations

`c12::replacement_races_old_cleanup` (`tests/c12/`) approximates a task's
republication through the launcher alone: no reactor, shard or publication
orders its sessions; the test does. Each generation of the one task (`<spec>`)
is an owner process beneath a shared state directory and TMPDIR, with its own
podman, and the launcher's own fresh id and token. Each declares one
controlled name, alternating between two endpoints. Its guest probes both
before connector-init, and only its own endpoint may see it connect; its
policy file and `Started` must name only its own. After two healthy
generations, the old generation's container removal is held while its
replacement starts and serves, and then its VMM is SIGKILLed. Its container is
created with `--rm`, so podman removes it on exit; the hold keeps its network,
state and record. In the first transition the old owner lives on and, let go,
finishes its whole teardown. In the second the old owner is killed too, its
orphaned removal is let go, and a last launch recovers what it left beside the
newest generation. Each hold lasts at most 30 s. Before and after each release
and recovery, the replacement must keep every process (by PID and start time)
and the rest of its footprint, its token, record lock and policy, and serve a
Spec within 5 s. Ordered events are written to `c12.evidence`.

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
- `tests/c06/`: final data across crashes. Two hello-world captures write
  `A {n}!` and `B {n}!` into one collection, each disabled once it has every
  `n < 100`; a VMM Python derivation totals those per key (sum, maximize,
  last write) into Postgres. Each test exports its committed input, requires
  every `n` exactly once, and compares the committed derived documents and,
  after `raw sync-now`, the table with totals computed from that input:
  healthy (`c06_1_b0_*`); the VMM SIGKILLed, or the reactor killed, while the
  derivation holds one read before its transaction can flush
  (`c06_fault_2_v1_*`, `c06_fault_3_r1_*`), released by a file in the
  successor's connector mount; and the reactor killed once the first batch's
  outputs are committed, before the second is captured (`c06_fault_4_r2_*`).
  The batches' keys are disjoint, so no key is updated across that restart.
- `tests/c06/`, same keys (`c06_updates_*`): batch B's `m` key onto batch A's
  keys (`m % 10`), so B updates every row A materialized. Batch A's committed
  outputs and, after `raw sync-now`, its rows must equal its totals before
  B's capture alone is published, and every B greeting must follow that:
  healthy (`c06_updates_1_b0_*`), and the reactor's unit SIGKILLed and
  restarted between the batches (`c06_updates_2_r1_*`). After the restart,
  before B, the shard must run a new VMM and A's committed outputs and rows
  must be unchanged. The table's OID and file node must not change.

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
