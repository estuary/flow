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
