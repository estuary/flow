# Running a task's connector in a VMM

A task may ask for its connector to run inside a micro-VM (a VMM) rather than
an ordinary container, and may declare the host names that connector reaches.
This page describes those two catalog fields, `vmm` and `egress`, and what a
data plane does with them.

A data plane runs VMM tasks only once its operator has prepared its hosts and
configured its reactors, as described in [operating.md](operating.md). Until
then every task requesting a VMM is refused there.

## Selecting VMM execution

`vmm` and `egress` sit beside each other in the task's model: within `derive`
for a derivation, and at the top level of a capture or materialization,
beside `endpoint`. Today only Python derivations can use them (see
[Which tasks are eligible](#which-tasks-are-eligible)).

```yaml
collections:
  acmeCo/anvils/enriched:
    schema: enriched.schema.yaml
    key: [/id]
    derive:
      using:
        python:
          module: enriched.py
          dependencies:
            httpx: ">=0.27"
      vmm: true
      egress:
        hosts:
          - api.acmeco.example
          - "*.cdn.acmeco.example"
      shards:
        flags:
          enable-runtime-v2: "true"
      transforms:
        - name: fromAnvils
          source: acmeCo/anvils
          shuffle: any
```

Quote a wildcard host in YAML: a plain scalar beginning with `*` is an alias.

Selection is binary:

- `vmm` omitted or `false`: ordinary execution, exactly as before. `false` is
  accepted and normalized away.
- `vmm: true`: every invocation of the task's connector runs in a VMM, or
  fails with an error. That covers Spec, Validate, Discover, Apply and Open,
  so publication, preview and the running task all use it. There is no
  automatic fallback to an ordinary container.

## Which tasks are eligible

A task may request a VMM only when all of these hold:

- It runs on the V2 runtime. A derivation selects V2 with the shard flag
  `enable-runtime-v2: "true"`. Publication refuses `vmm` on a task whose
  shards select V1: "requests VMM execution, which requires the V2 runtime,
  but its shards select the V1 runtime".
- It is a derivation whose connector image is the repository
  `ghcr.io/estuary/derive-python`, under any tag or digest. A `using: python`
  derivation on V2 runs `ghcr.io/estuary/derive-python:stable`.

TypeScript and SQLite derivations, local connectors, and every capture and
materialization image are refused: "connector image '...' is not eligible for
VMM execution as a ...".

## Data plane capability and task selection

Whether a data plane can run VMMs is its operator's configuration. Whether a
task wants one is the task's. The two are independent:

| Data plane | Task requests VMM | Result |
|---|---|---|
| not VMM-capable | no | ordinary execution, subject to the plane's admission rules |
| not VMM-capable | yes | error: "this data plane does not support VMM execution" |
| VMM-capable | no | ordinary execution, subject to the plane's admission rules |
| VMM-capable | yes | VMM execution, or an error |

A capable private or BYOC data plane runs ordinary and VMM tasks side by
side. Enabling VMMs on a plane moves no existing task into one.

Publication validates through the target data plane, so publishing a VMM task
to a plane without capability fails. A disabled task's connector is not
called at publication, so it publishes, and is refused when it is next
validated or started.

Public data planes refuse ordinary Python derivations ("Python derivations may
only run in private data-planes"). An eligible derivation requesting a VMM is
the exception, on a public plane whose operator has enabled VMM execution.

## Declaring egress

`egress` lists host names the connector may reach:

```yaml
egress:
  hosts:
    - api.acmeco.example
```

- `hosts` is required whenever `egress` is present; `egress: {}` is an error.
- `egress: null` is the same as omitting it.
- `hosts: []` is a declaration that adds no hosts. It is not deny-all: the
  connector's own and its image's hosts still apply, and it still turns on
  filtering on a plane which would otherwise not filter.
- Repeats and differences of case are tolerated.

Declaring egress never turns on VMM execution. Only VMM execution enforces a
declaration today, so a task declaring `egress` without `vmm: true` is refused
at publication: "declares egress, which its connector's execution cannot
enforce; egress is enforced only by VMM execution (`vmm: true`)". The
declaration is not specific to VMMs: another enforcer may honor the same
field later.

### What a VMM connector may reach

The hosts a VMM connector may reach are the union of three sources:

1. **Connector defaults**, built in per eligible connector.
   `derive-python`'s are `pypi.org` and `files.pythonhosted.org`, so
   dependencies install.
2. **Image declarations**: the image's `dev.estuary.egress-hosts` label, a
   JSON array of host names, for example
   `["api.acmeco.example", "*.cdn.acmeco.example"]`. A missing, blank or
   empty label adds nothing; anything else that isn't valid names fails the
   launch.
3. **Task additions**: `egress.hosts`.

Whether that union applies depends on the plane and on whether the task
declares egress:

| Data plane | `egress` | The VMM connector may reach |
|---|---|---|
| public | omitted or `null` | connector defaults plus image declarations, by name |
| private, or local | omitted or `null` | any public destination, by name or address |
| any | declared, including `hosts: []` | connector defaults, image declarations and task additions, by name |

On every plane, nothing a task declares lifts the platform's exclusions.
Private, loopback, link-local (including every cloud's instance metadata
service), multicast and other special-purpose IPv4 addresses, the host, other
VMMs, IPv6 and TCP port 25 are never reachable. A permitted name which
resolves to an excluded address is refused. These exclusions are enforced
twice: inside the VMM, and on the host outside the VMM's authority.

There are no settings for ports, address ranges, rates, DNS TTLs, allow-all
or deny-all. A permitted name is reachable on any port.

### Host names

- A name such as `api.acmeco.example` matches only itself.
- `*.acmeco.example` matches every name beneath `acmeco.example`, at any
  depth, but not `acmeco.example` itself. List both to reach both.
- A wildcard may not cover a public suffix, judged by the Public Suffix List's
  ICANN and private sections: `*.com`, `*.co.uk` and `*.github.io` are
  refused. Only the wildcard's base is checked, so `*.amazonaws.com` is
  accepted.
- Names are ASCII. Write an internationalized name in its punycode (`xn--`)
  form.
- No trailing dot, no single-label names, no addresses (a name ending in an
  all-numeric label is refused), no wildcard other than a leading `*.`.
  Labels are letters, digits and `-` (case does not matter), not beginning or
  ending with `-`, at most 63 bytes; a name is at most 253 bytes.

Publication checks each host and reports an invalid one at its index, as
"declares an invalid egress host". The data plane checks again before a
launch.

### What enforcement is, and is not

A VMM's resolver answers only permitted names, and only the addresses those
answers return become reachable. Permitting a name trusts whoever controls
it: its addresses, and the CNAME targets it points to. This makes a
connector's intended destinations visible and reviewable. It is not an
exfiltration control: data can leave through a permitted destination, or
through DNS queries. Name filtering runs inside the VMM, so a connector which
compromises its VMM can remove it; the platform's exclusions above still
hold.

## Previewing locally

`flowctl preview` runs a task's connectors on your own machine, as a local
data plane. A task with `vmm: true` previews only on a machine configured for
VMM execution as a reactor host would be (see [operating.md](operating.md));
elsewhere it is refused with "this data plane does not support VMM
execution". To preview the derivation's logic without one, omit both `vmm`
and `egress` in a local copy: local planes run Python derivations ordinarily,
with no egress enforcement.

A local plane, like a private one, lets an undeclared task reach any public
destination, so a local preview does not reproduce a public plane's filtering.
Declare `egress` (even `hosts: []`) to preview with name filtering.

## What a VMM task logs

Each VMM launch logs, at `info`, the egress it applies and where each name
came from:

```
VMM egress permits only these host names: pypi.org, files.pythonhosted.org (connector defaults); api.acmeco.example, *.cdn.acmeco.example (task egress.hosts)
```

or "VMM egress permits any public destination: the task declares no egress,
and this data plane does not require one", followed by "started connector
container" naming its `vmm`.

The VMM reports names it refused, at `warn`, once per name:

- "refused DNS name `<name>`, which this connector's egress does not permit;
  a task may permit it in egress.hosts"
- "refused DNS name `<name>`, which resolved to an address that is not public"

After 32 distinct names it logs "further refused DNS names are not reported"
and stops.

Errors which name a data plane's own preparation (its KVM, its network
boundary, its configuration) are the operator's; see
[operating.md](operating.md#troubleshooting).
