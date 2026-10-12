# connector-vmm-tests

Opt-in tests that boot real guests on the reference VMM launch line. They prove
network policy, DNS authorization, image accounts, read-only image roots,
writable scratch, vsock RPC and enforcement outside a compromised VMM.

`mise run ci:connector-vmm-kvm` owns setup and teardown. It requires x86_64 KVM,
passwordless sudo, rootful podman, docker buildx, Python, musl-tools and
conntrack. Missing prerequisites fail the task rather than silently skip tests.
The normal nextest profile excludes privileged tests and runs harness unit tests.

## Roadmap

- `tests/kvm.rs`: guest behavior, isolation, host boundary tampering and reinstalls.
- `src/launch.rs`: reference network, launch and host boundary commands with snapshots.
- `src/run.rs`: staged run configuration, resource tracking and VMM lifecycle.
- `src/host.rs`: host observations of mounts, descriptors, conntrack and nftables.
- `src/guest.rs`, `fixtures/probes.py`: guest probe RPC.
- `src/dns.rs`, `src/endpoint.rs`, `src/netns.rs`: controlled nameservers and endpoints.
- `fixtures/guest.Containerfile`: the probe image based on derive-python.

The task owns the host boundary and controlled endpoint namespace for one run.
Each guest has its own network and state directory. Test-name filters are partial
runs; the task records resources before creation so interrupted runs can clean up.

## Connector launcher

`tests/launcher.rs` drives `connector::Service` through real and fake VMM images,
including guest-health readiness, startup faults, killed owners and recovery.
`src/launcher.rs` owns per-test state and fault controls; `fixtures/podman.sh`
and `fixtures/gate.py` wrap the engine and control readiness. `tests/common/rpc.rs`
holds a connector RPC open for lifecycle tests.

A launch claims a durable ownership record and retains its lock across engine
commands. Recovery releases only resources proven to belong to a dead owner;
failed cleanup is retained for retry. Connector mounts follow the same owner.

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
