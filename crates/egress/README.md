# egress

Egress declarations a connector's network is held to: which host names and
address ranges it may reach, the policy document that carries them into a VMM,
and the destinations no declaration reaches. Pure parsing and validation, with no IO beyond reading
a policy file, so the launcher in [`crates/connector`](../connector/README.md)
and the VMM in [`crates/connector-vmm`](../connector-vmm/README.md) hold one
copy of these semantics: what a launcher writes is exactly what the VMM parses.

## Roadmap

- `src/lib.rs`: `hosts` and `cidrs`, the rules for names and address ranges
  wherever they are declared; `public_policy` and `any_public_policy`, the
  documents a launcher writes; `parse` and `load`, the VMM's side, with every
  refusal; `baseline` and `excluded`.

## Host names

`allowedNames` matches DNS question names on label boundaries. A bare name
admits only itself; `*.acmeco.example` admits names at any depth beneath
`acmeco.example`, but not the base. List both forms to admit both.

Wildcards over public suffixes are refused because unrelated registrants may
control names beneath them. The check uses the `psl` crate's ICANN and private
rules and applies only to the wildcard base.

`hosts` applies these rules to any list of declared names and names its source
in every refusal: `allowedNames` within a policy, an image label, or a task's
`egress.hosts`, which catalog validation checks one name at a time and the
connector service checks again. Names are lowercased and repeats dropped. Non-ASCII names must be written in punycode,
and a name ending in an all-numeric label is refused as an address.

## Address ranges

`cidrs` applies to a task's `egress.cidrs`, a policy's `declaredCidrs` and
`boundary grant --cidr`, naming its source in every refusal. A range is an
IPv4 address and a decimal prefix length. IPv6, a bare address, a netmask and
bits set below the prefix are refused. A range within another, or repeated,
is dropped and the rest ordered by address, so no two overlap and each can be
an nft interval set element without merging. Nothing is refused for
overlapping the baseline: the exclusions below win where they meet a range.

## The policy document

```json
{
  "egress": "public",
  "allowAll": false,
  "allowedNames": ["pypi.org", "files.pythonhosted.org", "*.acmeco.example"],
  "declaredCidrs": ["10.0.0.0/8", "192.168.4.7/32"],
  "ttlFloorSecs": 90,
  "ttlCapSecs": 3600
}
```

A launcher writes one of two shapes, everything else at its default.
`public_policy` writes `egress: public`, `allowedNames`, and `declaredCidrs`
when the task declares any; empty lists admit no name and no range, and the
resolver refuses each query. `any_public_policy` writes `egress: public` and
`allowAll`: no name gate, and the baseline below still holds. The TTL bounds
are carried at full shape, validated and snapshot-tested for the VMM and its
test suites, but no launcher writes them.

## Task egress

A task may declare `egress: {hosts: [...], cidrs: [...]}` beside `vmm`; see
[`docs/connector-vmm/tasks.md`](../../docs/connector-vmm/tasks.md) for the
user-facing description.

- Declared hosts add to those of the connector and its image; they never
  remove one. An empty or missing list adds nothing, and `egress: {}` is
  still a declaration.
- A name is a host, with no port, address or range: every port of it is
  reachable, at public addresses and within the task's CIDRs.
- A CIDR admits every address within it, on any TCP or UDP port, whether or
  not a name resolves to it. Only private and local planes accept a nonempty
  list: the connector service refuses one on a public plane, where private
  networks are Estuary's.
- Nothing a task declares reaches an `excluded` destination below, or TCP
  port 25.
- An execution which cannot enforce a declaration refuses the task, rather
  than run it unenforced. Only VMM execution enforces one today, and nothing
  infers `vmm` from `egress`.
- A task which declares nothing is the data plane's to decide: a public plane
  holds it to its connector's and image's hosts, and a private or local one
  leaves it any public destination.

## Public destinations only, unless a CIDR is declared

Without a declared CIDR, a connector may reach public unicast addresses and
nothing else. Every private address is treated as sensitive even where the
service behind it authenticates, and there is no known sensitive public-IP
service, so the boundary is drawn at reachability rather than at a list of
things worth protecting.

`baseline` is that boundary. It holds every prefix IANA's IPv4 Special-Purpose
Address Registry marks as not globally reachable, plus multicast, plus the
VMM's own interface subnets read at start. A declared CIDR lifts it where they
meet, except for `excluded`: the prefixes which are not a destination a
network routes to, or which hold the metadata service or the tap, plus the
VMM's own subnets.

| prefix | what it is | within a CIDR |
|---|---|---|
| `0.0.0.0/8` | "this network"; `0.0.0.0/32` is this host | excluded |
| `10.0.0.0/8` | private use (RFC 1918) | reachable |
| `100.64.0.0/10` | shared address space, carrier NAT (RFC 6598) | reachable |
| `127.0.0.0/8` | loopback | excluded |
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

`connector-vmm` folds the VMM's own subnets in when it starts and enforces the
result; see its README for how the ruleset and resolver hold it.
