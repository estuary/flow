# connector-vmm

`flow-connector-vmm` runs one connector inside a libkrun micro-VM. It owns
everything between the container the runtime launches and the guest that
`flow-connector-guest-init` takes over: the guest's network, the ruleset
that bounds it, its disks and shares, and the libkrun calls that start it.

- `run` is the whole launch.
- `print-ruleset` compiles an egress policy into its nftables ruleset
  without touching the kernel.
- `boundary` installs, verifies and removes the host's tables around every
  VMM network, and grants and revokes a launch's host permissions. It runs
  in the host's network namespace.

`crates/connector`'s `vmm` module is the launcher.
[`docs/connector-vmm/operating.md`](../../docs/connector-vmm/operating.md)
covers host preparation, and
[`crates/connector-vmm-tests`](../connector-vmm-tests/README.md) the
integration suite.

## Roadmap

- `src/main.rs`: the CLI, the stderr framing, and exit 2 for every failure
  before a VM starts.
- `src/launch.rs`: the ordered sequence `run` executes, split into `check`,
  its read-only checks, the IO that builds a `Machine`
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
- `docker/connector-vmm.Dockerfile`: the image; see [Image](#image).

## Disks and shares

| mount | | what it is |
|---|---|---|
| `/` | ro | the VMM image. `--read-only`, checked at startup. |
| connector mount | ro | shared into the guest at `/connector-mount`, checked at startup. `crates/connector` is the authority for its contents and the environment contract; `image::krun_config` gives those variables precedence over the image's. |
| `/rootfs` | ro | a private image-backed volume, mounted `readonly`, checked at startup and served read-only as the guest's root. |
| `/sock` | rw | libkrun binds `init.sock` here. |
| `/scratch-backing` | rw | holds the `O_TMPFILE` scratch, sized by `--disk-mib` and formatted ext4. `src/disk.rs` has the descriptor and format details. Guest discard releases deleted data blocks; the file is reclaimed when the VM exits. |
| `/persistent-disk` | rw | only with `--persistent-disk`. `connector-guest-init` mounts it `nodev,nosuid,noexec` and does not chown it. |

These are the entire writable set: the launch line passes
`--read-only-tmpfs=false`. `launch::check` verifies the read-only mounts
before VM setup. The guest's writable layout over its scratch disk belongs
to `connector-guest-init::root`.

The launch line drops podman's default capabilities and adds only these.
libkrun's virtiofs server runs in this process as host root and performs a
guest's file operations itself.

| capability | used for |
|---|---|
| `CAP_NET_ADMIN` | the tap, `ip` and `nft`, and the resolver's set updates |
| `CAP_NET_BIND_SERVICE` | the resolver's port 53; podman leaves `ip_unprivileged_port_start` at 1024 |
| `CAP_DAC_OVERRIDE` | the launcher's mode-0700 `/sock` and `/scratch-backing`, and image files of any mode, which the server opens for any guest user |
| `CAP_SETUID`, `CAP_SETGID` | the server creates a nonroot guest user's files on a writable share as that user, and refuses them without these |
| `CAP_FOWNER` | the server's chmod and times on such a user's files |
| `CAP_FSETID` | its chmod keeps a setgid bit such a user sets |
| `CAP_CHOWN` | its chown, for a guest's |

## What the launcher provides

`crates/connector`'s `vmm::argv` builds each line.

- A podman network per VMM, whose interface name begins `fvm`
  (`boundary::BRIDGE_PREFIX`). The name is what places the container behind
  the host boundary, so no other host interface may use the prefix.
- `boundary verify` from the same VMM image, in the host's network
  namespace, before the network is created. Nothing is cached between
  launches.
- An empty `/sock` which the launcher owns at mode 0700, whose host path
  fits a Unix socket address.
- `--sysctl net.ipv6.conf.default.disable_ipv6=1`, which
  `net::check_ipv6_disabled` checks.

## The host boundary

`boundary install` puts `inet flow_vmm_boundary` and
`bridge flow_vmm_boundary` in the host's network namespace. Rules match the
`fvm*` interface a packet crosses and never an address, so one installation
covers every VMM bridge. `src/boundary.rs` documents install, verify, remove
and the packet mark by which `boundary grant` authorizes a launch's declared
ingress ports and CIDRs.

| hook | drops, for traffic from or to an `fvm*` bridge |
|---|---|
| inet prerouting (raw) | IPv6; a source that does not route back out of the bridge it came in on; IP fragments (none arrive while conntrack reassembles) |
| inet input | everything addressed to the host, except UDP/53 to the bridge gateway and TCP replies to authorized host ingress |
| inet forward | anything to another VMM bridge or back out of its own; `policy::excluded` destinations; `policy::baseline` destinations without a granted mark; TCP/25; IPv6 into a VMM; any packet into a VMM that is not a reply |
| inet output | IPv6, and connections from the host into a VMM without a reply state or an ingress authorization |
| bridge prerouting, output | frames that are neither IPv4 nor ARP |
| bridge forward | every frame from one port of a VMM bridge to another |

`verify` runs before each launch and not continuously. It passes only
against tables identical to what this image's `install` applies.

## The policy

`run` takes `--egress-host` for each name the guest may resolve and
`--egress-cidr` for each IPv4 range, validated by `models::egress`. With
neither, the resolver forwards nothing and grants nothing. `src/ruleset.rs`
renders `inet flow_egress` for the VMM's own namespace, and
`src/resolver/mod.rs` documents the query gate and how an answer authorizes
an address for its TTL.

Without a declared CIDR a guest reaches public unicast addresses only.
`policy::baseline` is every prefix IANA's IPv4 Special-Purpose Address
Registry marks not globally reachable, plus multicast, Azure's host-node
virtual IP, and the VMM's own subnets. A declared CIDR lifts the baseline
where they meet, except for `policy::excluded`.

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

`192.31.196.0/24`, `192.52.193.0/24` and `192.175.48.0/24` are absent
because IANA marks them globally reachable.

## What this layer does and does not guarantee

- The allowlist discloses a connector's intended destinations. It is not an
  exfiltration control: a permitted name delegates to whoever controls its
  zone, and DNS queries of permitted names can carry data.
- This layer permits no private or special-use destination outside the
  task's CIDRs, no excluded destination, no IPv6, no protocol but TCP and
  UDP, and no inbound connection but host-authorized declared TCP ports.
- A guest with a compromised kernel is assumed to reach code execution in
  this process, which can flush its own ruleset. The host boundary then
  still holds the excluded and baseline destinations, IPv6 and ingress. It
  does not hold the name allowlist: such a VMM reaches any public address.
- DNS is UDP only. A truncated answer returns `SERVFAIL`.

## Image

`docker/connector-vmm.Dockerfile` is Fedora 43 with libkrun built from
source, Fedora's `libkrunfw` guest kernel, and `ip`, `nft` and `mkfs.ext4`.
`ci:docker-images` builds it for `linux/amd64`, runs `ci:connector-vmm-gate`
on it, and pushes it only if the gate passes.

- Fedora's updates repository keeps only the newest `libkrunfw`, so the
  build stops once it moves past the pin. Moving the pin changes the guest
  kernel.
- `boundary verify` compares tables as the image's nft lists them. After
  changing `NFTABLES_VERSION`, run `mise run ci:connector-vmm-kvm` and
  `ci:connector-vmm-gate`; both must pass with `boundary::expected`
  unchanged.
- `flow-connector-vmm` is built on the Ubuntu runner and runs against
  Fedora's glibc, which must be at least as new.
- `--debug`, `--resolver-upstream` and `--exec` parse only with the
  `test-controls` feature, which `ci:package` refuses.
