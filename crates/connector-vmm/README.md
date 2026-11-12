# connector-vmm

`flow-connector-vmm` is the process that runs one connector inside a micro-VM.
It owns everything between the container the runtime launches and the guest
that `flow-connector-guest-init` takes over: the network the guest is given, the ruleset
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
- `src/policy.rs`: the `--egress-host` and `--egress-cidr` flags, the policy
  they validate into, and the `baseline` and `excluded` prefixes. Host-name
  and CIDR rules are `models::egress`'s.
- `src/ruleset.rs`: policy in, `inet flow_egress` text out, for the VMM's own
  network namespace.
- `src/boundary.rs`: the host's `inet` and `bridge` `flow_vmm_boundary` tables,
  as the nft JSON document `boundary install` applies and the comparison
  `boundary verify` makes. `src/boundary/listing.json` is the kernel's listing
  of them.
- `src/ingress.rs`: launch-owned host input and output chains authorizing declared TCP
  ports with a reserved packet mark.
- `src/resolver/mod.rs`: the thread, the gate, the CNAME memory and the
  lifetime of an authorization.
- `src/resolver/dns.rs`: reads a message with `domain`, rewrites TTLs in
  place, synthesizes the replies the resolver invents. Never re-encodes a
  message.
- `src/resolver/nftset.rs`: the nfnetlink batch, as typed
  `netlink-packet-netfilter` messages, and its acknowledgments, read with
  `netlink-packet-core`'s checked decoders.
- `docker/connector-vmm.Dockerfile`, `docker/connector-vmm-fake.Dockerfile` and
  `fake-entrypoint.sh`: the two images; see [Images](#images).

## Launch constraints

`run` and `check-launch` share `launch::Args` and `launch::check`: read-only
root, image and connector mount, default IPv6, policy, `image-inspect.json`,
and the mount points a persistent disk's guest path needs.
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
backtrace which says nothing of KVM.

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
travels as repeated `--supplementary-gid` flags; `connector-guest-init` sets it with
`setgroups` before `setgid` and `setuid`.

`HOME` comes from the resolved passwd entry, including empty fields. Without
an entry, an explicit uid defaults to `WorkingDir`; an empty user defaults to
`/`. The image's nonempty `HOME` wins. `image::krun_config` appends defaults
because libkrun applies `HOME` and `TERM` last-wins.

## Disks and shares

| mount | | what it is |
|---|---|---|
| `/` | ro | the VMM image. `--read-only`, checked at startup. |
| connector mount | ro | shared into the guest at `/connector-mount`, checked at startup. `crates/connector` is the authority for its contents and the environment contract; `image::krun_config` gives those variables precedence over the image's. |
| `/rootfs` | ro | a private image-backed volume, mounted `readonly`, checked at startup and served read-only as the guest's root. |
| `/sock` | rw | libkrun binds `init.sock` here. |
| `/scratch-backing` | rw | holds the `O_TMPFILE` scratch, sized by `--disk-mib` and formatted ext4. `src/disk.rs` has the descriptor and format details. Guest discard releases deleted data blocks; the file is reclaimed when the VM exits. |
| `/persistent-disk` | rw | only with `--persistent-disk`. `connector-guest-init` mounts it `nodev,nosuid,noexec` and does not chown it. |

Scratch uses libkrun's `KRUN_SYNC_NONE`: guest flush requests need no host
hardware durability because the disk dies with the VM. The newly truncated
sparse file reads as zero, so `mkfs.ext4 -E assume_storage_prezeroed=1` marks
inode tables initialized without writing zeros or scheduling guest lazy init.
The guest mounts scratch with `discard`; libkrun's raw block backend punches
holes in the backing file when ext4 frees data. Filesystem metadata remains
allocated. These settings apply only to disposable scratch, never persistent
storage.

Those three are the entire writable set, because the launch line passes
`--read-only-tmpfs=false`: podman's `/tmp`, `/run` and `/var/tmp` are read-only
too, as are `/dev` and `/dev/shm`. Device and proc interfaces are not storage
and are not in that set - the only added devices are `/dev/kvm` and
`/dev/net/tun`, and `/proc/sys` stays read-only, which is why
`net::check_ipv6_disabled` reads a knob it cannot set. The absence of
`CAP_SYS_ADMIN` is what rules out a mount namespace or `pivot_root` in this
process.

**Capabilities are explicit.** The launch line drops podman's defaults and
adds only what this process uses, libkrun's virtiofs server among it. That
server runs as host root and performs a guest's file operations itself.

| capability | used for |
|---|---|
| `CAP_NET_ADMIN` | the tap, `ip` and `nft`, and the resolver's set updates |
| `CAP_NET_BIND_SERVICE` | the resolver's port 53; podman leaves `ip_unprivileged_port_start` at 1024 |
| `CAP_DAC_OVERRIDE` | the launcher's mode-0700 `/sock` and `/scratch-backing`, and image files of any mode, which the server opens for any guest user |
| `CAP_SETUID`, `CAP_SETGID` | the server creates a nonroot guest user's files on a writable share as that user, and refuses them without these |
| `CAP_FOWNER` | the server's chmod and times on such a user's files |
| `CAP_FSETID` | its chmod keeps a setgid bit such a user sets |
| `CAP_CHOWN` | its chown, for a guest's |

`CAP_KILL`, `CAP_SETPCAP`, `CAP_SYS_CHROOT` and `CAP_SETFCAP`, the rest of
podman 4.9's defaults, are dropped. Nothing here uses the first three.
Without the last, a root guest cannot set a file capability on its
persistent disk, which it mounts `nosuid`. These are the VMM's host
privileges. The guest's root and connector-guest-init's `NO_NEW_PRIVS` are the guest
kernel's, and confer none of them.

**The guest's root is read-only outside the guest, twice.** podman mounts the
image read-only, and libkrun's share of it refuses every write,
so neither guest root nor a compromised guest kernel can write podman's
storage. The guest's mount points come from libkrun too: `/scratch`,
`/connector-mount` and a persistent disk's guest path are virtual directories
in the root share. A virtual directory hides an image entry of its name, so
`launch::check` refuses a persistent disk whose guest path begins with one the
image has, or with one of the VMM's own. The writable layout the guest builds
from its scratch disk belongs to `connector-guest-init::root`.

## What a guest escape reaches

A guest with a compromised kernel is assumed (not measured) to reach code
execution in this process via virtiofs and `/proc`; from there
`nft flush ruleset` succeeds (measured). The baseline exclusions then hold only
because the host boundary enforces them outside this process's authority,
less the CIDRs its launch was granted. A VMM that has flushed its rules can
still reach any public address, including names its policy never allowed,
and its guest can too once the VMM masquerades it again: the boundary
excludes networks, and says nothing about which public destinations a
connector uses or how often.

## Launcher requirements

Each VMM container is alone on a podman network of its own, created with
`--interface-name` beginning `fvm` (`boundary::BRIDGE_PREFIX`) and podman's
resolver left enabled. That interface name is what places the container
behind the host boundary, so no other interface on the host may use the
prefix. Before creating that network the launcher runs `boundary verify` from
the same VMM image, in the host's network namespace, and launches only if it
exits zero; nothing is cached between launches. `crates/connector`'s
`vmm::argv` builds the lines for all three, and its `vmm::launch` is the
launcher.

The launcher must own `/sock` at mode 0700: the VMM sets `umask(0)` so the
unprivileged launcher can connect to `/sock/init.sock`. The socket outlives the
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

`ci:docker-images` builds `connector-vmm` with the flow tags, for
`linux/amd64` only, and never the fake. It loads `connector-vmm` rather than
pushing it, while bake publishes the other images. It then runs
`ci:connector-vmm-gate` on the loaded image's ID. It pushes the same image
only once the gate passes, the image reports `EXPECTED_COMMIT`'s build, and
every tag still names that ID. That push is `docker push` of the loaded
image, not bake's, so the provenance attestation bake attaches to the images
it pushes is not guaranteed for this one. A publication refuses custom bake
arguments and a package that differs from what `ci:package` recorded. A
local build with its own arguments may select other targets, and then gates
nothing.

The gate needs no KVM and leaves the host's tables alone: in a container's
own network namespace, with only `CAP_NET_ADMIN`, the candidate's `boundary`
installs, verifies, and refuses each tampering the gate makes with the
candidate's own `nft`. The client times out after
`CONNECTOR_VMM_GATE_TIMEOUT` seconds (300). Cleanup uses the exact container
name and fails if the container may remain.

`ci:connector-vmm-candidate` builds this crate and `connector-guest-init`, builds the
image from those two files alone, and gates it by ID. A pull request's
Platform workflow runs it when the pull request changes this crate, a crate
it builds from, the image, or the build and publication scripts.

`connector-vmm` is Fedora 43 with libkrun built from source, the guest kernel
from Fedora's `libkrunfw`, and the commands `run` invokes before the VM (`ip`,
`nft`, `mkfs.ext4`). `flow-connector-guest-init` sits at `launch::GUEST_INIT`. The
Dockerfile pins libkrun's commit and lockfile, and its version ARGs are also
the `dev.estuary.libkrun`, `dev.estuary.libkrunfw` and `dev.estuary.nftables`
labels. Packaging constraints:

- Fedora's updates repository keeps only the newest `libkrunfw`, so the image
  stops building once it moves past the pin. Moving the pin changes the guest
  kernel, and is meant to be deliberate.
- `NFTABLES_VERSION` pins nft's upstream version, not Fedora's release of it.
  dnf installs the newest release of that version: `1.1.3-6.fc43.1` from
  updates when it was pinned, with `1.1.3-5.fc43` in Fedora 43's release
  repository behind it, so the pin does not stop the build as libkrunfw's
  does. `boundary verify` compares the tables as this nft lists them, so a
  new version is a deliberate move: change the ARG and run
  `mise run ci:connector-vmm-kvm`. Its verifies,
  `boundary_detects_tampering` and `boundary_holds_across_reinstalls` must
  pass with `boundary::expected` unchanged. A listing which spells the same
  rule differently gets a normalization in `boundary::ours` for that spelling
  alone, as Ubuntu's nft 1.0.9 has for `l4proto` 6, with its own listing
  fixture; a rule which differs in meaning is a failure.
- `flow-connector-vmm` is built on the Ubuntu runner and runs against Fedora's
  glibc, which must be at least as new.
- Nothing in the image is written at runtime; the container root is read-only.
- `run`'s test controls, `--debug`, `--resolver-upstream` and `--exec`, parse
  only with the `test-controls` feature. Only the KVM suite's own image
  enables it, and `ci:package` refuses a binary that accepts `--exec`.

`connector-vmm-fake` implements `run`'s launch contract without a VM, for
exercising a launcher where KVM is unavailable. Only `ci:connector-vmm-kvm`
builds it, from its own package directory; nothing publishes it. `fake-entrypoint.sh`, at the
real binary's path, invokes Rust `check-launch` before staging. It then serves
`/sock/init.sock` at mode 0777 and hands the workload `CONNECTOR_MOUNT`,
`LOG_FORMAT` and `LOG_LEVEL`. `/rootfs` is read-only, so connector-init runs
chrooted into a copy of it in `/scratch-backing`, from a copy of the connector
mount at `/connector-mount`, and socat bridges the socket to TCP port 19092,
outside Linux's default automatic source-port range. The
copy costs the image's size on the state directory's filesystem for each
launch, and is given the owner of `/scratch-backing` so an unprivileged
launcher can remove it. It needs none of the launch line's devices, but
its chroot needs `CAP_SYS_CHROOT`, which the launch line leaves out and the
real VMM never uses: the suite's podman wrapper adds it to the fake's
launches alone, as any engine wrapper running the fake must. `boundary` is
not faked: the image carries the real binary and
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
| inet input | everything addressed to the host, except UDP/53 to the bridge gateway and TCP replies to authorized host ingress |
| inet forward | anything to another VMM bridge or back out of its own; `policy::excluded` destinations; `policy::baseline` destinations without a granted mark; TCP/25; IPv6 into a VMM; any packet into a VMM that is not a reply |
| inet output | IPv6, and connections from the host into a VMM without a reply state or an ingress authorization |
| bridge prerouting, output | frames that are neither IPv4 nor ARP |
| bridge forward | every frame from one port of a VMM bridge to another |

Podman's resolver on the arrival bridge is the DNS exception. Host ingress
and a task's declared CIDRs are authorized by `boundary grant --owner TOKEN
--bridge BRIDGE`, with `--port PORT` for each declared TCP port and `--cidr
CIDR` for each declared range. It creates a host table named
`flow_vmm_ingress_TOKEN`. The shared boundary clears reserved packet mark
`0x46564d49` at priority -12 on local input, output and forwarding across VMM
bridges. The launch's chains at priority -11 mark IPv4 TCP packets to declared
ports in the original direction, established replies from those ports, and
packets forwarded from its bridge to its CIDRs. The shared boundary admits
those marked packets at priority -10: to the host for ingress, and past the
baseline drop for CIDRs, though never past `excluded`, another VMM bridge or
TCP/25. Every packet must match current permissions, so stale conntrack state
cannot authorize a replacement. A guest-supplied authorization mark is
cleared. Forwarded traffic into a VMM is never authorized.

`boundary revoke --owner TOKEN` atomically removes that owner's table and is
idempotent. The launcher records the VMM image needed for recovery before
granting access, and stops the container before revoking permissions. A failed
revoke retains the record and network for retry. Other launch tokens are
untouched. The shared chains run at filter priority -10; drops in any base
chain are final, regardless of other tables' accepts.

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
tables and its launchers' VMM image together. Connection rates and distinct
destinations are not limited.

`verify` first prints `flow-connector-vmm build ` and this binary's build on
stdout, whatever the tables: `FLOW_BUILD_IDENTITY` as
`mise/tasks/build/identity-lib.sh` set it at compile time (HEAD's commit,
suffixed `-dirty` for modified files or untracked source), or `unidentified`
when it was unset or empty. A launcher pairs it with its own (`crates/connector`'s
`check_pairing`). `ci:docker-images` and `ci:connector-vmm-candidate` read
it the same way.

## The policy

A launcher passes the policy as flags of `run`: `--egress-host` for each name
the guest may reach and `--egress-cidr` for each IPv4 range, both repeatable.
`policy::check` validates and normalizes them with `models::egress::hosts` and
`models::egress::cidrs`, whose doc comments hold the rules. With neither flag
the resolver forwards no queries and grants no destinations.

The resolver compares each name with a question's name as read from the wire,
lowercased and without its root. An exact name matches only itself, so `db`
never admits `db.corp.example`: no search list is applied. A wildcard matches
whole labels beneath its base. `resolver::dns` escapes a dot or backslash
inside a label, so one label cannot pose as two.

An `--egress-cidr` adds the `declared` and `excluded` sets to the ruleset.

A resolved address stays reachable for its record's TTL, clamped to between 90
seconds and an hour. `--ttl-floor-secs` and `--ttl-cap-secs` change those
bounds only in a `test-controls` build.

## The ruleset, in one paragraph

One table, `inet flow_egress`, replaced wholesale on every apply. The forward
chain drops by policy and works through anti-spoof, established/related, IPv6,
anything that is not TCP or UDP, the `excluded` prefixes, the baseline
exclusions outside `@declared` and tcp/25, before jumping to `egress_accept`
where the only two accepts live: `@resolved` and `@declared`. `@resolved` is empty until the
resolver puts something in it. The input chain drops by policy and lets exactly
one thing in from the tap: the guest's DNS query to the VMM. The output chain
lets nothing out to the tap that is not an established reply, so nothing in the
VMM can open a connection to the guest. Declared TCP ports are DNATed from the
uplink to the guest before the egress exclusions. Their replies use the
established connection, whatever the egress policy. Postrouting masquerades
the guest out of the uplink.

## Public destinations only, unless a CIDR is declared

Without a declared CIDR, a connector may reach public unicast addresses and
nothing else. Every private address is treated as sensitive even where the
service behind it authenticates, and there is no known sensitive public-IP
service, so the boundary is drawn at reachability rather than at a list of
things worth protecting.

`policy::baseline` is that boundary: every prefix IANA's IPv4 Special-Purpose
Address Registry marks as not globally reachable, plus multicast and Azure's
host-node virtual IP, plus the VMM's own interface subnets
read at start. A declared CIDR lifts it where they meet, except for
`policy::excluded`: the prefixes which are not routed destinations or hold the
metadata service or the tap, plus the VMM's own subnets.

| prefix | what it is | within a CIDR |
|---|---|---|
| `0.0.0.0/8` | "this network"; `0.0.0.0/32` is this host | excluded |
| `10.0.0.0/8` | private use (RFC 1918) | reachable |
| `100.64.0.0/10` | shared address space, carrier NAT (RFC 6598) | reachable |
| `127.0.0.0/8` | loopback | excluded |
| `168.63.129.16/32` | [Azure platform virtual IP](https://learn.microsoft.com/en-us/azure/virtual-network/what-is-ip-address-168-63-129-16) (WireServer, DNS, DHCP, health probes) | reachable |
| `169.254.0.0/16` | link local, and every cloud's instance metadata service | excluded |
| `172.16.0.0/12` | private use (RFC 1918) | reachable |
| `192.0.0.0/24` | IETF protocol assignments: DS-Lite, NAT64/DNS64 discovery | reachable |
| `192.0.2.0/24` | documentation (TEST-NET-1), and the tap `192.0.2.0/30` | excluded |
| `192.88.99.0/24` | deprecated 6to4 relay anycast | reachable |
| `192.168.0.0/16` | private use (RFC 1918) | reachable |
| `198.18.0.0/15` | benchmarking (RFC 2544); several vendors use it as private space | reachable |
| `198.51.100.0/24` | documentation (TEST-NET-2) | reachable |
| `203.0.113.0/24` | documentation (TEST-NET-3) | reachable |
| `224.0.0.0/4` | multicast; not a unicast destination | excluded |
| `240.0.0.0/4` | reserved, including `255.255.255.255` | excluded |

Three registry entries are deliberately absent because IANA marks them
globally reachable: `192.31.196.0/24` (AS112-v4), `192.52.193.0/24` (AMT) and
`192.175.48.0/24` (AS112 direct delegation).

A resolver answer naming a baseline address
outside the CIDRs, or an excluded one, is refused rather than added to
`@resolved`. In the rendered text the guarantee is positional:
`ip daddr @excluded counter drop` and the baseline drop precede every rule
that can reach `egress_accept`, the baseline drop spares only `@declared`, and
that chain is reachable only by the jump below it.
`ruleset::tests::every_accept_is_accounted_for` asserts these over every
policy shape.

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
ruleset's `RESOLVED_SIZE` cap. With a declared CIDR, an unlisted name's `A`
question also goes upstream, and its answer stands only if every address lies
within the CIDRs. Such an answer remembers no CNAME target, so it cannot make
a later public answer authorized, and addresses within the CIDRs take no room
in `@resolved`, which `@declared` already covers.

`Resolver::plan` preserves outstanding TTLs. An expiry is dated from the clock
after the kernel acknowledged the batch that set it, and the kernel started that
element's timer before acknowledging, so a recorded expiry is always later than
the kernel's own. That ordering, rather than any bound on elapsed time, is what
makes an exclusive insert safe: an address recorded as expired is certainly gone
from the kernel. `nftset::Netlink` refreshes addresses with atomic
delete-and-add batches under one deadline for the whole call; `nftset::retry`
handles elements expiring before the delete commits. `nftset::Acknowledgments`
decides from the kernel's replies whether a batch committed. The netlink module
documents acknowledgment and timeout behavior observed in the kernel.

DNS is UDP-only. The guest's `resolv.conf` enables `options edns0`, and
queries preserve the client's EDNS advertisement upstream. Complete UDP
answers pass the query and address gates. Responses with nonzero header
rcodes pass through unchanged and authorize nothing. A truncated answer returns
`SERVFAIL`, without authorizing addresses or remembering CNAME targets,
including for non-A queries. This avoids a TCP retry the firewall cannot
serve. Large answers that require TCP are unsupported; EDNS does not remove
that compatibility limit. General DNS compatibility requires TCP support
([RFC 7766](https://www.rfc-editor.org/rfc/rfc7766.html#section-5)).

A refused name is reported to the task, once per name, as a `warn` JSON log
line on stderr, which the launcher's stderr pump hands on: refused for its name,
or because its answer named an address the baseline excludes. That address is
not reported, since the upstream may be the host's own resolver. The guest
chooses its question names, so `resolver::Refusals` reports at most 32
distinct names, then one line saying it has stopped. Each line is one write,
so the guest's console output cannot interleave within it. `--debug`'s
per-query decision lines are separate, and test-only.

## Non-obvious details

- **`declared` matches every port.** The rule is `ip daddr @declared`, so a
  declared CIDR admits TCP and UDP on any port; the earlier drops still take
  everything else, and tcp/25. `models::egress::cidrs` leaves its prefixes disjoint,
  so the set needs no `auto-merge`.
- **The TTL floor may not be zero.** An nft set element with `timeout 0s`
  never expires, so a zero floor would turn one answer into a permanent hole
  in `resolved`. The TTL cap is bounded for the same reason.
- **IPv6 is dropped, not filtered.** The guest has it disabled, the forward
  chain drops it outright, and `--vmm-subnet` refuses an IPv6 argument because
  there is nothing for it to exclude.
- **Nothing here needs the kernel.** `print-ruleset` and every test in this
  crate run without KVM, podman or root: the resolver's tests speak to a fake
  upstream nameserver on loopback and record what the resolver asked of the set
  rather than writing to one. Capacity reclamation uses explicit expiry states;
  the integration suite checks the kernel set and real expiry. The launch
  sequence is snapshotted through a recording implementation of the libkrun
  trait. An applied ruleset, a batch
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
- DNS exfiltration through permitted names is accepted. A permitted name may be
  queried for any record type, and a connector that wants to leak data through
  a name it is allowed to resolve can. With a declared CIDR, any name may be
  queried for `A`.
- A remembered CNAME target is whatever an allowed zone pointed at. That is not
  new reach: whoever controls an allowed zone could already answer its own name
  with any public address, and the baseline check runs on every `A` answer
  either way.
- Nothing here prevents exfiltration to an allowed destination.
- Protecting the wider internet from abuse originating in a connector is a
  lower priority than protecting Estuary's infrastructure and other customers,
  and the approach to it is monitoring and response rather than prevention at
  this layer.

The properties this layer does carry are the ones above: no private or
special-use destination outside the task's CIDRs, no excluded destination at
all, no IPv6, no protocol other than TCP and UDP, no inbound connection except
host-authorized declared TCP ports, and no guest-initiated path to the VMM
itself except the DNS rule. The host boundary holds the destination, IPv6 and
ingress restrictions even against a VMM that has removed this layer.
