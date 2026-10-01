# egress

Egress declarations a connector's network is held to: which host names it may
reach, the policy document that carries them into a VMM, and the destinations
no declaration reaches. Pure parsing and validation, with no IO beyond reading
a policy file, so the launcher in [`crates/connector`](../connector/README.md)
and the VMM in [`crates/connector-vmm`](../connector-vmm/README.md) hold one
copy of these semantics: what a launcher writes is exactly what the VMM parses.

## Roadmap

- `src/lib.rs`: `hosts`, the host-name rules, wherever names are declared;
  `public_policy` and `any_public_policy`, the documents a launcher writes;
  `parse` and `load`, the VMM's side, with every refusal; `baseline` and
  `check_declared`.

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

## The policy document

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

A launcher writes one of two shapes, everything else at its default.
`public_policy` writes `egress: public` and `allowedNames`; an empty list
admits no name, and the resolver refuses each query. `any_public_policy`
writes `egress: public` and `allowAll`: no name gate, and the baseline below
still holds. `declaredCidrs`, the two rate limits and the TTL bounds are
carried at full shape, validated and snapshot-tested for the VMM and its test
suites, but no launcher writes them and no image or task declaration reaches
them.

## Task egress

A task may declare `egress: {hosts: [...]}` beside `vmm`.

- Declared hosts add to those of the connector and its image; they never
  remove one. An empty list adds nothing, and is still a declaration.
- A name is a host, with no port, address or range: every port of it is
  reachable, at public addresses only. Nothing a task declares reaches a
  baseline destination below, or TCP port 25.
- An execution which cannot enforce a declaration refuses the task, rather
  than run it unenforced. Only VMM execution enforces one today, and nothing
  infers `vmm` from `egress`.
- A task which declares nothing is the data plane's to decide: a public plane
  holds it to its connector's and image's hosts, and a private or local one
  leaves it any public destination.

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

`connector-vmm` folds the VMM's own subnets in when it starts and enforces the
result; see its README for how the ruleset and resolver hold it.
