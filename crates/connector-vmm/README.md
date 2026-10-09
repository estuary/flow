# connector-vmm

`flow-connector-vmm` is the process that runs one connector inside a micro-VM.
It owns everything between the container the runtime launches and the guest
that `flow-guest-init` takes over: the network the guest is given, the ruleset
that bounds it, the disks and shares it sees, and the libkrun call sequence
that starts it.

`run` is the whole launch. `check-launch` checks shared launch prerequisites
without creating resources; see [Launch constraints](#launch-constraints).
`print-ruleset` compiles a connector's egress policy
into the nftables ruleset that the VM will run behind, without touching the
kernel, so the policy can be reviewed and snapshot-tested on any machine.
`boundary` installs, verifies and removes the host's tables around every VMM
container's network; it runs on the host, not in a VMM container. See
[The host boundary](#the-host-boundary), and
[`docs/connector-vmm/operating.md`](../../docs/connector-vmm/operating.md)
for when a host's provisioning runs it.

## Roadmap

- `src/main.rs`: the CLI, the stderr framing, and exit 2 for every failure
  before a VM starts.
- `src/launch.rs`: the ordered sequence `run` executes, split into `check`,
  the read-only prefix `check-launch` shares, the IO that builds a `Machine`
  and the pure `enter` that turns one into libkrun calls.
- `src/krun.rs`: libkrun's ABI behind a trait, loaded with `dlopen`.
- `src/image.rs`: the image's OCI config, the guest argv and
  `.krun_config.json`.
- `src/disk.rs`: the `O_TMPFILE` scratch disk and its `mkfs.ext4`.
- `src/net.rs`: the addresses the tap, the ruleset and the resolver are pinned
  to, plus the tap, the ruleset apply and the upstream nameserver.
- `src/console.rs`: the console descriptors and the `--debug` tee.
- The policy JSON, its host-name rules and the baseline come from
  [`crates/egress`](../egress/README.md).
- `src/ruleset.rs`: policy in, `inet flow_egress` text out, for the VMM's own
  network namespace.
- `src/boundary.rs`: the host's `inet` and `bridge` `flow_vmm_boundary` tables,
  as the nft JSON document `boundary install` applies and the comparison
  `boundary verify` makes. `src/boundary/listing.json` is the kernel's listing
  of them.
- `src/resolver/mod.rs`: the thread, the gate, the CNAME memory and the
  lifetime of an authorization.
- `src/resolver/dns.rs`: reads a message, rewrites TTLs in place, synthesizes
  the replies the resolver invents. Never re-encodes a message.
- `src/resolver/nftset.rs`: the nfnetlink batch, by hand.
- `docker/connector-vmm.Dockerfile`, `docker/connector-vmm-fake.Dockerfile` and
  `fake-entrypoint.sh`: the two images; see [Images](#images).

## Launch constraints

`run` and `check-launch` share `launch::Args` and `launch::check`: read-only
root and connector mount, default IPv6, policy and `image-inspect.json`.
These checks need no KVM, libkrun or `CAP_NET_ADMIN`; the fake invokes them
before staging. Passing covers common prerequisites. Real-only KVM, libkrun,
network setup and image account resolution follow in `run`.

`launch::run` loads libkrun before creating resources, finishes the network
before booting the guest, and sweeps inherited descriptors before starting any
worker threads. `launch::sweep` retains stdio and the scratch disk at fd 3;
`resolver::start` waits until DNS is listening before launch continues.

`launch::check_kvm` opens `/dev/kvm` and asks its API version before libkrun
is loaded, failing with `KVM is unavailable to this VMM`. Without KVM libkrun
panics inside `krun_start_enter`, across its C boundary, and aborts with a
backtrace which says nothing of KVM. The launch, whose readiness is
connector-init's health and not stderr, then reports only that the VMM exited.

`launch::enter` disables implicit vsock networking to prevent TSI INET
hijacking and configures separate workload stderr. See `image::guest_argv`
for why argv travels through `.krun_config.json` instead of `krun_set_exec`,
and `launch::Overlay` for the lifetime of memory handed to libkrun.

libkrun is loaded at runtime so builds need no installed library. `src/krun.rs`
records the header version used for its ABI bindings.

## The image's user

`image::load` resolves the image's `User` against the image's own
`/etc/passwd` and `/etc/group`, opened with `openat2` and `RESOLVE_IN_ROOT`
beneath `/rootfs`. Links and `..` resolve as they will in the guest, against
the image root and never above it, and the kernel holds that through the whole
walk, so no link reaches the VMM image's own files. There is no fallback: a
host kernel without `openat2` (Linux 5.6) fails every launch with an error
naming it. A missing file, or a link to nothing, reads as empty, as in a
scratch image: numeric ids still resolve and names are refused. As in podman,
a name resolves only from a seven-field passwd entry above any line without
seven, while a uid resolves from a line of any length. The lookup
sees the image alone, so a link into something the guest mounts later, such as
the connector mount or `/proc`, reads whatever the image has at that path.

`image::resolve_user` uses the same databases for supplementary groups,
matching rootful podman 4.9 over runc. A colon in `User` suppresses memberships:
`user:` keeps the passwd gid, and `:group` selects uid 0 in that group. The list
travels as repeated `--supplementary-gid` flags; `guest-init` sets it with
`setgroups` before `setgid` and `setuid`. `--run-as-root` skips all three.

`HOME` comes from the resolved passwd entry, including empty fields. Without
an entry, an explicit uid defaults to `WorkingDir`; an empty user defaults to
`/`. The image's nonempty `HOME` wins. `image::krun_config` appends defaults
because libkrun applies `HOME` and `TERM` last-wins. `--run-as-root` retains
the image user's `HOME`.

## Disks and shares

| mount | | what it is |
|---|---|---|
| `/` | ro | the VMM image. `--read-only`, checked at startup. |
| `/init` | ro | `policy.json` and nothing else, read once. |
| connector mount | ro | shared into the guest at its own host path, checked at startup. `crates/connector` is the authority for its contents and the environment contract; `image::krun_config` gives those variables precedence over the image's. |
| `/rootfs` | rw | podman's per-container image layer, served as the guest's root. |
| `/sock` | rw | libkrun binds `init.sock` here. |
| `/scratch-backing` | rw | holds the `O_TMPFILE` scratch, sized by `--disk-mib` and formatted ext4. `src/disk.rs` has the descriptor and format details. Reclaimed when the VM exits. |
| `/persistent-disk` | rw | only with `--persistent-disk`. `guest-init` mounts it `nodev,nosuid,noexec` and does not chown it. |

Those four are the entire writable set, because the launch line passes
`--read-only-tmpfs=false`: podman's `/tmp`, `/run` and `/var/tmp` are read-only
too, as are `/dev` and `/dev/shm`. Device and proc interfaces are not storage
and are not in that set - the only added devices are `/dev/kvm` and
`/dev/net/tun`, and `/proc/sys` stays read-only, which is why
`net::check_ipv6_disabled` reads a knob it cannot set. Capabilities are
podman's default plus `CAP_NET_ADMIN`; the absence of `CAP_SYS_ADMIN` is what
rules out a mount namespace or `pivot_root` in this process.

**A read-only container root does not make the guest's root read-only.**
`/rootfs` is shared read-write and is the guest's `/`. Guest mount flags belong
to `guest-init::root`.

## What a guest escape reaches

A guest with a compromised kernel is assumed (not measured) to reach code
execution in this process via virtiofs and `/proc`; from there
`nft flush ruleset` succeeds (measured). The baseline exclusions then hold only
because the host boundary enforces them outside this process's authority. A
VMM that has flushed its rules can still reach any public address, including
names its policy never allowed, and its guest can too once the VMM
masquerades it again: the boundary excludes networks, and says nothing about
which public destinations a connector uses or how often.

## Launcher requirements

Each VMM container is alone on a podman network of its own, created with
`--interface-name` beginning `fvm` (`boundary::BRIDGE_PREFIX`) and podman's
resolver left enabled. That interface name is what places the container
behind the host boundary, so no other interface on the host may use the
prefix. Before creating that network the launcher runs `boundary verify` from
the same VMM image, in the host's network namespace, and launches only if it
exits zero; nothing is cached between launches. The reference lines for all
three are in [`crates/connector-vmm-tests`](../connector-vmm-tests/README.md)'s
`src/launch.rs`; `crates/connector`'s `vmm::launch` is the launcher.

The launcher owns access control on `/sock`: the VMM sets `umask(0)` so
unprivileged clients can connect to `/sock/init.sock`. The socket outlives the
container, and libkrun refuses to bind over one (`krun_add_vsock_port2: File
exists`, exit 2), so each launch needs an empty `/sock`. Its host-side path must
fit a Unix socket address, under 108 bytes.

Pass `--sysctl net.ipv6.conf.default.disable_ipv6=1` so the tap inherits disabled
IPv6. Podman mounts `/proc/sys` read-only, preventing the VMM from setting this
itself. `net::check_ipv6_disabled` reads `default` in `launch::check`, before
anything is created, and the tap's own once `run` has created it. The
container's `eth0` exists before the sysctl applies and keeps an IPv6
link-local address (measured); the host boundary drops what it sends.

`launch::check_read_only` verifies the container root and connector bind are
read-only before VM setup. The integration suite,
[`crates/connector-vmm-tests`](../connector-vmm-tests/README.md), checks the full
mount matrix.

## Images

`ci:docker-images` builds both with the flow tags, for `linux/amd64` only.

`connector-vmm` is Fedora 43 with libkrun built from source, the guest kernel
from Fedora's `libkrunfw`, and the commands `run` invokes before the VM (`ip`,
`nft`, `mkfs.ext4`). `flow-guest-init` sits at `launch::GUEST_INIT`. The
Dockerfile pins libkrun's commit and lockfile, and its version ARGs are also
the `dev.estuary.libkrun` and `dev.estuary.libkrunfw` labels. Packaging
constraints:

- Fedora's updates repository keeps only the newest `libkrunfw`, so the image
  stops building once it moves past the pin. Moving the pin changes the guest
  kernel, and is meant to be deliberate.
- `flow-connector-vmm` is built on the Ubuntu runner and runs against Fedora's
  glibc, which must be at least as new.
- Nothing in the image is written at runtime; the container root is read-only.

`connector-vmm-fake` implements `run`'s launch contract without a VM, for
exercising a launcher where KVM is unavailable. `fake-entrypoint.sh`, at the
real binary's path, invokes Rust `check-launch` before staging. It then serves
`/sock/init.sock` at mode 0777 and hands the workload `CONNECTOR_MOUNT`,
`LOG_FORMAT` and `LOG_LEVEL`. connector-init runs chrooted into `/rootfs`, from
a copy of the connector mount staged at the mount's own path, and socat bridges
the socket to its TCP port. It needs none of the launch line's devices or
capabilities. `boundary` is not faked: the image carries the real binary and
Ubuntu's `nft`, and the entrypoint runs it, so a launcher verifies with the
fake exactly as it would with the real image. What it does not stand in for:

- The guest. No VM, tap, ruleset, resolver, scratch disk or guest init, and
  connector-init also listens on the container's network, as an ordinary
  connector's does.
- The connector's image config. The connector runs as root, in `/`, with this
  image's environment plus the contract's: its own `Env`, `User` and
  `WorkingDir` are not applied, so only self-contained connectors run under it.
- The share. The connector mount is a writable copy, and nested mounts within
  it are not reproduced. `task-update.json` is followed into it once a second,
  which gives a connector the runtime's rewrites but says nothing about whether
  the real VMM's share shows them to a guest.
- Flags a launcher does not pass. `--persistent-disk` and the test-only flags
  are refused, not ignored.
- Supervision. socat and the follow loop run beside connector-init, which is
  PID 1, and nothing notices if either dies.

## The host boundary

`boundary install` puts two tables in the host's network namespace,
`inet flow_vmm_boundary` and `bridge flow_vmm_boundary`, that bound every VMM
container from outside the container's authority. Rules match the host
interface a packet crosses, `fvm*`, never an address, because a VMM holding
`CAP_NET_ADMIN` chooses its own addresses, routes, MAC and ruleset. Keyed that
way the tables hold nothing about a particular network or launch: one
installation, made before any VMM network exists, covers every VMM bridge
the host will have, and every owner installs identical tables.

| hook | drops, for traffic from or to an `fvm*` bridge |
|---|---|
| inet prerouting (raw) | IPv6; a source that does not route back out of the bridge it came in on; IP fragments (none arrive while conntrack reassembles) |
| inet input | everything addressed to the host, except UDP/53 to the address of the bridge it arrived on |
| inet forward | anything to another VMM bridge or back out of its own; `egress::baseline` destinations; TCP/25; IPv6 into a VMM; any packet into a VMM that is not a reply |
| inet output | IPv6, and anything that is not a reply, from the host into a VMM |
| bridge prerouting, output | frames that are neither IPv4 nor ARP |
| bridge forward | every frame from one port of a VMM bridge to another |

The one accept is the VMM's nameserver: podman's resolver on the gateway of the
VMM's own bridge, which podman writes into the container's `resolv.conf` and
`net::upstream_nameserver` reads. The rule names the arrival bridge rather than
an address, so it cannot disagree with the resolver's configuration. Every
other rule drops, and a drop in any base chain is final, so netavark's, Docker's
and anyone else's accepts do not undo them. The chains run at filter priority
-10 so their counters see packets first; the drops would hold at any priority.

One bridge per VMM is what makes the sibling guarantee hold at layer 2. On a
shared bridge a VMM could rewrite the host's neighbour and forwarding entries
for a sibling's address and take its replies; alone on its bridge, the only
entries it can disturb are its own.

`install` applies both tables as one nft transaction that declares, deletes and
redefines them, so a reinstall or an upgrade never passes through a permissive
state, then verifies. If either table was absent while `fvm*` interfaces
existed, it installs anyway and exits 2 naming them: those VMMs ran without the
boundary and must be replaced. `verify` lists the ruleset and compares both
tables, less handles and counter values, against exactly what `install`
applies; a dormant table, a missing set element, a moved priority, an added
rule or chain are all failures, and each is named in a framed line. `remove`
refuses while any `fvm*` interface exists, and removes both tables in one
transaction otherwise. It does not know about VMM networks created but not
yet started, so an operator stops launchers before removing it.

Verification happens before launch, not continuously: tables removed between a
verify and the container starting would go unnoticed until the next verify.
A verify and a VMM image come from the same build, so a host upgrades the
tables and its launchers' VMM image together. Host-side limits on new
connections or distinct destinations are not here.

## The policy

`/init/policy.json` is parsed by `egress::load`. Its fields, the host-name
rules of `allowedNames` and the baseline are documented in
[`crates/egress`](../egress/README.md).

`egress: none` renders the same skeleton with the acceptance chain, the
`resolved` set and the guest's DNS rule all absent. `allowAll` adds one accept
at the head of the acceptance chain and removes the requirement that a
destination be *named* - not the requirement that it be public.

## The ruleset, in one paragraph

One table, `inet flow_egress`, replaced wholesale on every apply. The forward
chain drops by policy and works through anti-spoof, established/related, IPv6,
anything that is not TCP or UDP, the baseline exclusions, tcp/25, and then the
rate limits, before jumping to `egress_accept` where the only three accepts
live: `allowAll`, `@resolved` and `@declared`. `@resolved` is empty until the
resolver puts something in it. The input chain drops by policy and lets exactly
one thing in from the tap: the guest's DNS query to the VMM. The output chain
lets nothing out to the tap that is not an established reply, so nothing in the
VMM can open a connection to the guest. Postrouting masquerades the guest out
of the uplink.

## Public destinations only

A connector may reach public unicast addresses and nothing else. Every private
address is treated as sensitive even where the service behind it authenticates,
and there is no known sensitive public-IP service, so the boundary is drawn at
reachability rather than at a list of things worth protecting.

`egress::baseline` is that boundary: every prefix IANA's IPv4 Special-Purpose
Address Registry marks as not globally reachable, plus multicast (listed in
[`crates/egress`](../egress/README.md)), plus the VMM's own interface subnets
read at start.

These exclusions win over everything. `allowAll` does not lift them, a declared
CIDR inside one is refused rather than rendered, and an answer from the
resolver naming one is refused rather than added to `@resolved`. In the
rendered text the guarantee is positional: `ip daddr @baseline counter drop`
precedes every rule that can reach `egress_accept`, and that chain is reachable
only by the jump below it. `ruleset::tests::every_accept_is_accounted_for`
asserts both over every policy shape.

The guest's DNS query to the VMM is the one local destination it may address,
and it is in the input chain rather than the forward chain because a packet
addressed to a local address never reaches the forward hook. Upstream queries
are sent by the VMM's own resolver over the uplink, so the guest is never given
a path into a private network to reach a nameserver.

## The resolver

`resolver::start` binds the guest's nameserver address and a netlink socket,
then hands both to a thread that is never joined. It returns once it is
listening, so a launcher that calls it before entering the VM knows the guest
will have a nameserver. There is no way back from a failure after that point: a
guest whose nameserver has died must not keep running, so a failed set update, a
lost socket or a panic prints one framed line and aborts the process.

The query gate and TTL authorization rules are documented in
`src/resolver/mod.rs`. Only answer-section records authorize destinations;
remembered CNAME targets match exactly. Address and target counts share the
ruleset's `RESOLVED_SIZE` cap.

`Resolver::plan` preserves outstanding TTLs. An expiry is dated from the clock
after the kernel acknowledged the batch that set it, and the kernel started that
element's timer before acknowledging, so a recorded expiry is always later than
the kernel's own. That ordering, rather than any bound on elapsed time, is what
makes an exclusive insert safe: an address recorded as expired is certainly gone
from the kernel. `nftset::Netlink` refreshes addresses with atomic
delete-and-add batches under one deadline for the whole call; `nftset::retry`
handles elements expiring before the delete commits. The netlink module
documents acknowledgment and timeout behavior observed in the kernel.

DNS is UDP-only: the ruleset does not admit TCP retries for oversized answers.

A refused name is reported to the task, once per name, as a `warn` JSON log
line on stderr, which the launcher's stderr pump hands on: refused for its name,
or because its answer named an address the baseline excludes. That address is
not reported, since the upstream may be the host's own resolver. The guest
chooses its question names, so `resolver::Refusals` reports at most 32
distinct names, then one line saying it has stopped. Each line is one write,
so the guest's console output cannot interleave within it. `--debug`'s
per-query decision lines are separate, and test-only.

## Non-obvious details

- **`declared` matches TCP only.** The rule is
  `ip daddr . tcp dport @declared`, so a declared port admits TCP and nothing
  else. UDP destinations have no declared path.
- **`ttlFloorSecs` may not be zero.** An nft set element with `timeout 0s`
  never expires, so a zero floor would turn one answer into a permanent hole
  in `resolved`. `ttlCapSecs` is bounded for the same reason.
- **IPv6 is dropped, not filtered.** The guest has it disabled, the forward
  chain drops it outright, and `--vmm-subnet` refuses an IPv6 argument because
  there is nothing for it to exclude.
- **Nothing here needs the kernel.** `print-ruleset` and every test in this
  crate run without KVM, podman or root: the resolver's tests speak to a fake
  upstream nameserver on loopback and record what the resolver asked of the set
  rather than writing to one, and the launch sequence is snapshotted through a
  recording implementation of the libkrun trait. An applied ruleset, a batch
  that reaches a real kernel, and a guest that actually boots are proven by the
  integration suite, [`crates/connector-vmm-tests`](../connector-vmm-tests/README.md).
- **The descriptor sweep is tested in a subprocess.** It closes descriptors the
  test harness owns, so it cannot run inside one.

## What the allowlist is, and is not

It is a light-touch destination-disclosure mechanism: it makes a connector's
intended destinations visible and reviewable. It is not an exfiltration
control.

- Declaring a domain delegates its public addresses, and its CNAME targets, to
  whoever controls that domain; a wildcard delegates every name beneath its
  base the same way. It is not proof of ownership or of benign intent toward
  the addresses that come back.
- DNS exfiltration through forwarded queries is accepted. The resolver forwards
  the query shapes it does not gate, and a connector that wants to leak data
  through a name it is allowed to resolve can.
- A remembered CNAME target is whatever an allowed zone pointed at. That is not
  new reach: whoever controls an allowed zone could already answer its own name
  with any public address, and the baseline check runs on every answer either
  way.
- Nothing here prevents exfiltration to an allowed destination.
- Protecting the wider internet from abuse originating in a connector is a
  lower priority than protecting Estuary's infrastructure and other customers,
  and the approach to it is monitoring and response rather than prevention at
  this layer.

The properties this layer does carry are the ones above: no private or
special-use destination, no IPv6, no protocol other than TCP and UDP, no
inbound connection, and no path to the VMM itself except the one DNS rule. Of
these, the host boundary holds the destination, IPv6 and inbound properties
even against a VMM that has removed this layer.
