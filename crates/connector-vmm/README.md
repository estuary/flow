# connector-vmm

`flow-connector-vmm` is the process that runs one connector inside a micro-VM.
It owns everything between the container the runtime launches and the guest
that `flow-guest-init` takes over: the network the guest is given, the ruleset
that bounds it, the disks and shares it sees, and the libkrun call sequence
that starts it.

Today it holds the ruleset and the resolver that fills it. `print-ruleset`
compiles a connector's egress policy into the nftables ruleset that the VM will
run behind, without touching the kernel, so the policy can be reviewed and
snapshot-tested on any machine. The resolver is the guest's only nameserver and
the only thing that puts an address into that ruleset's `resolved` set.

## Roadmap

- `src/main.rs`: the CLI, the stderr framing, and exit 2 for every failure
  before a VM starts.
- `src/net.rs`: the addresses the tap, the ruleset and the resolver are pinned
  to. Nothing else declares them.
- `src/policy.rs`: the policy JSON, the baseline exclusions, and every refusal.
- `src/ruleset.rs`: policy in, `inet flow_egress` text out. Nothing else in the
  crate renders nft syntax.
- `src/resolver/mod.rs`: the thread, the gate, the CNAME memory and the
  lifetime of an authorization.
- `src/resolver/dns.rs`: reads a message, rewrites TTLs in place, synthesizes
  the replies the resolver invents. Never re-encodes a message.
- `src/resolver/nftset.rs`: the nfnetlink batch, by hand.

Not yet implemented: interface subnet discovery, scratch disk, image config,
libkrun bindings, and the `run` subcommand that starts a VM and, with it, the
resolver.

## The policy

```json
{
  "egress": "public",
  "allowAll": false,
  "allowedNames": ["pypi.org", "files.pythonhosted.org", "*.acmeco.example"],
  "declaredCidrs": [ { "cidr": "93.184.216.0/24", "ports": [443] } ],
  "connectionsPerMinute": null,
  "distinctDestinationsPerMinute": null,
  "ttlFloorSecs": 90,
  "ttlCapSecs": 3600
}
```

`allowedNames` matches DNS question names on label boundaries. A bare name
admits only itself; `*.acmeco.example` admits names at any depth beneath
`acmeco.example`, but not the base. List both forms to admit both.

Wildcards over public suffixes are refused because unrelated registrants may
control names beneath them. The check uses the `psl` crate's ICANN and private
rules and applies only to the wildcard base.

`egress: none` renders the same skeleton with the acceptance chain, the
`resolved` set and the guest's DNS rule all absent. `allowAll` adds one accept
at the head of the acceptance chain and removes the requirement that a
destination be *named* - not the requirement that it be public.

Only `egress`, `allowAll` and the two TTL bounds have a producer today.
`allowedNames` is populated by hand; `declaredCidrs`,
`connectionsPerMinute` and `distinctDestinationsPerMinute` are carried at full
shape, validated and snapshot-tested, but nothing generates them until the
catalog model that owns them exists.

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

`baseline` is that boundary. It holds every prefix IANA's IPv4 Special-Purpose
Address Registry marks as not globally reachable, plus multicast, plus the
VMM's own interface subnets read at start:

| prefix | what it is |
|---|---|
| `0.0.0.0/8` | "this network"; `0.0.0.0/32` is this host |
| `10.0.0.0/8` | private use (RFC 1918) |
| `100.64.0.0/10` | shared address space, carrier NAT (RFC 6598) |
| `127.0.0.0/8` | loopback |
| `169.254.0.0/16` | link local, and every cloud's instance metadata service |
| `172.16.0.0/12` | private use (RFC 1918) |
| `192.0.0.0/24` | IETF protocol assignments: DS-Lite, NAT64/DNS64 discovery |
| `192.0.2.0/24` | documentation (TEST-NET-1), and the tap `192.0.2.0/30` |
| `192.88.99.0/24` | deprecated 6to4 relay anycast |
| `192.168.0.0/16` | private use (RFC 1918) |
| `198.18.0.0/15` | benchmarking (RFC 2544); several vendors use it as private space |
| `198.51.100.0/24` | documentation (TEST-NET-2) |
| `203.0.113.0/24` | documentation (TEST-NET-3) |
| `224.0.0.0/4` | multicast; not a unicast destination |
| `240.0.0.0/4` | reserved, including `255.255.255.255` |

Three registry entries are deliberately absent because IANA marks them
globally reachable: `192.31.196.0/24` (AS112-v4), `192.52.193.0/24` (AMT) and
`192.175.48.0/24` (AS112 direct delegation).

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

`Resolver::plan` preserves outstanding TTLs. An expiry is recorded only after
the kernel acknowledged the batch that set it, and the kernel started that
element's timer before acknowledging, so a recorded expiry is always later than
the kernel's own. That ordering, rather than any bound on elapsed time, is what
makes an exclusive insert safe: an address recorded as expired is certainly gone
from the kernel. `nftset::Netlink` refreshes addresses with atomic
delete-and-add batches under one deadline for the whole call; `nftset::retry`
handles elements expiring before the delete commits. The netlink module
documents acknowledgment and timeout behavior observed in the kernel.

DNS is UDP-only: the ruleset does not admit TCP retries for oversized answers.

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
  rather than writing to one. An applied ruleset, and a batch that reaches a
  real kernel, require an integration environment.

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
inbound connection, and no path to the VMM itself except the one DNS rule.
