# Running a task's connector in a VMM

A task may ask for its connector to run inside a micro-VM (a VMM) rather than
an ordinary container, and may declare the host names and address ranges that
connector reaches.
This page describes those two catalog fields, `vmm` and `egress`, and what a
data plane does with them.

A data plane runs VMM tasks only once its operator has prepared its hosts and
configured its reactors, as described in [operating.md](operating.md). Until
then every task requesting a VMM is refused there.

## Selecting VMM execution

`vmm` and `egress` sit beside each other in the task's model: within `derive`
for a derivation, and at the top level of a capture or materialization,
beside `endpoint`. Today only Python captures, derivations and
materializations can use `vmm` (see
[Which tasks are eligible](#which-tasks-are-eligible)), and a task which uses
it must also declare `egress`.

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
          - pypi.org
          - files.pythonhosted.org
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
  so publication, discovery and the running task all use it. flowctl never
  runs the connector on your machine (see
  [Working locally with flowctl](#working-locally-with-flowctl)). There is no
  automatic fallback to an ordinary container. The task must declare
  `egress` (see [Declaring egress](#declaring-egress)).

## Which tasks are eligible

A task may request a VMM only when all of these hold:

- It runs on the V2 runtime. A derivation selects V2 with the shard flag
  `enable-runtime-v2: "true"`. Publication refuses `vmm` on a task whose
  shards select V1: "requests VMM execution, which requires the V2 runtime,
  but its shards select the V1 runtime".
- Its connector image is the Python connector of its own task type, by
  repository, under any tag or digest:

  | Task | Repository | Model |
  |---|---|---|
  | capture | `ghcr.io/estuary/capture-python` | `endpoint: python` |
  | derivation | `ghcr.io/estuary/derive-python` | `using: python`, a project or a legacy `module` |
  | materialization | `ghcr.io/estuary/materialize-python` | `endpoint: python` |

  A Python task runs its repository's `:stable` tag, or the tag its
  `custom-image-tag` shard flag names.

TypeScript and SQLite derivations, local connectors, every other image, and a
Python image requested as another type of task are refused: "connector image
'...' is not eligible for VMM execution as a ...".

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

Public data planes refuse ordinary Python tasks of every type ("Python
derivations may only run in private data-planes, or within a VMM
(`vmm: true`)"). An eligible task requesting a VMM is the exception, on a public
plane whose operator has enabled VMM execution. Private planes run Python
tasks either way, as each task requests.

## Declaring egress

`egress` lists host names and IPv4 address ranges the connector may reach:

```yaml
egress:
  hosts:
    - api.acmeco.example
  cidrs:
    - 10.20.0.0/16
```

- Every task with `vmm: true` declares `egress`, on every plane. One which
  omits it, or sets `egress: null`, is refused before its connector starts:
  "the task requests VMM execution but declares no egress".
- `hosts` and `cidrs` are each optional, and a missing list is an empty one.
- `egress: {}`, or both lists empty, permits no external destination at all.
- Any other declaration permits exactly what it lists, as described below.
- Repeats and differences of case are tolerated, and so are overlapping
  ranges.

Declaring egress never turns on VMM execution. Only VMM execution enforces a
declaration today, so a task declaring `egress` without `vmm: true` is refused
at publication: "declares egress, which its connector's execution cannot
enforce; egress is enforced only by VMM execution (`vmm: true`)". The
declaration is not specific to VMMs: another enforcer may honor the same
field later.

### What a VMM connector may reach

A VMM connector may reach only what its task declares: each name in
`egress.hosts`, and every address within `egress.cidrs` (see
[Address ranges](#address-ranges)). Neither the connector nor its image adds
a host.

The declaration alone decides, on every plane:

| `egress` | The VMM connector may reach |
|---|---|
| omitted or `null` | nothing: the task is refused before its connector starts |
| `{}`, or both lists empty | no external destination |
| any other | the names in `egress.hosts`, and any address within `egress.cidrs` |

On every plane, nothing a task declares lifts the platform's exclusions.
Loopback, link-local (including the metadata address `169.254.169.254`),
multicast and other addresses which are not destinations, the metadata
addresses `100.100.100.200` (Alibaba Cloud) and `192.0.0.192` (Oracle Compute
Classic), Azure's host platform address `168.63.129.16`, the host running the
connector, other VMMs, IPv6 and TCP port 25 are never reachable. Private and
other special-purpose IPv4 addresses are reachable only within
`egress.cidrs`. A permitted name which resolves to an unreachable address is
refused. These exclusions are enforced twice: inside the VMM, and on the host
outside the VMM's authority.

Obtaining the host instance's credentials through cloud metadata is
unsupported.

There are no settings for ports, rates, DNS TTLs, allow-all or deny-all. A
permitted name, and an address within a declared range, is reachable on any
port.

### Python dependencies

A Python connector installs its project's dependencies as it starts.
Installing from PyPI typically needs both `pypi.org`, its index, and
`files.pythonhosted.org`, which serves its files, in `egress.hosts`. A task
using a private index can list its own hosts. Dependencies already available
in the image require no network access.

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
- A service name such as `_mongodb._tcp.db.acmeco.example`, which SRV and
  similar lookups query, is permitted only by a wildcard over it, such as
  `*.db.acmeco.example`.

Publication checks each host and reports an invalid one at its index, as
"declares an invalid egress host". The data plane checks again before a
launch.

### Address ranges

`egress.cidrs` opens private networks, such as a database in your own VPC, to
the connector.

- Each is an IPv4 address and a prefix length, such as `10.20.0.0/16`, or
  `10.20.3.4/32` for one address. IPv6 ranges, bare addresses, netmasks and
  addresses with bits set below the prefix (`10.20.3.4/16`) are refused.
- An address within a range is reachable unless it is excluded above, on
  any TCP or UDP port except TCP port 25, whether the connector uses the
  address directly or a name resolving to it. A name need not be listed in
  `hosts` when every address it resolves to lies within the declared ranges.
- Broad ranges, including `10.0.0.0/8` and `0.0.0.0/0`, are accepted. The
  same exclusions apply to them as to an exact `/32`.
- Only private data planes accept a range, as do the local data planes of
  development stacks. A public data plane refuses any nonempty `cidrs` when the connector starts,
  including at publication: "the task declares egress.cidrs, which only
  private data planes accept". Publication checks the syntax of each range
  and reports an invalid one at its index, as "declares an invalid egress
  CIDR".

### What enforcement is, and is not

A VMM's resolver answers only permitted names, and only the addresses those
answers return become reachable. A permitted name may be queried for any
record type. Only its address answers are checked against the exclusions
above; other answers, such as TXT or SRV, are returned unfiltered. With
`egress.cidrs`, any other name may be asked for its IPv4 address, and its
answer is returned only if every address lies within the declared ranges. One
address outside them refuses the whole answer, and so does an address a listed
name resolves to that is neither public nor within the ranges. A record type
other than an IPv4 address, such as SRV, needs the name in `hosts`.
Permitting a name trusts whoever controls it: its addresses, and the CNAME
targets it points to. This makes a connector's intended destinations visible
and reviewable. It is not an exfiltration control: data can leave through a
permitted destination, or through DNS queries. Name filtering runs inside the
VMM, so a connector which compromises its VMM can remove it; the platform's
exclusions above still hold.

## Working locally with flowctl

flowctl cannot run a connector on your machine in a VMM. `flowctl preview`,
`flowctl generate`, `flowctl raw spec`, `flowctl raw discover` and `flowctl
raw oauth` refuse a selected task with `vmm: true`. They do so as soon as the
task is selected, before contacting the control plane, starting a connector
or writing a file:

```
Task "acmeCo/anvils/enriched" requests VMM sandboxing (vmm: true). flowctl cannot run this connector locally in a sandbox.

To keep sandboxing, publish the original spec to a VMM-enabled data plane. For ordinary local execution, make a local copy of the spec, set vmm to false or omit it, and remove egress. Local execution cannot enforce those egress rules. Only run Python code you trust this way.
```

Selecting another task of the same catalog works as before.

`flowctl raw build` and `flowctl raw test` validate connectors through
flowctl's local connector service. A VMM request fails validation for that
task. `raw build` records the error in its build database. `raw test` validates only derivations, so neither a VMM capture or
materialization nor a disabled task stops it. A VMM task which declares no
`egress` reports that first.

`flowctl catalog publish`, `catalog test`, `draft author` and `discover` run no
connector locally, whatever the task. They check the catalog's structure and
upload `vmm` and `egress` as written, and the data plane validates each
connector. A Python task's `config` is still checked for plaintext secrets
before upload, against its declared `spec.configSchema`.

In a local copy which omits `vmm`, a Python task runs ordinarily, with nothing
enforcing egress. A copy which keeps `egress`, even `egress: {}`, is refused,
as on every plane.

## Network ports

A VMM task is reachable on the ports of its built spec, and on no others. A
Python task's are its `spec.networkPorts`, whether or not its image `EXPOSE`s
them; a port its image exposes but its spec doesn't declare is unreachable.
Apply and Open carry a built spec; Spec, Discover and Validate open no ports.
The reactor's proxy reaches them at the VMM's address, and
they are never published on the host's interfaces.

## What a VMM task logs

Each VMM launch logs, at `info`, the egress it applies:

```
VMM egress permits only these host names: pypi.org, files.pythonhosted.org, api.acmeco.example, *.cdn.acmeco.example (task egress.hosts); and any address within 10.20.0.0/16 (task egress.cidrs)
```

or, for `egress: {}`, "VMM egress permits no external destination: the task
declares empty egress", followed by "started connector container" naming its
`vmm`.

The VMM reports names it refused, at `warn`, once per name:

- "refused DNS name `<name>`, which this connector's egress does not permit;
  a task may permit it in egress.hosts". Where `<name>` is exactly `pypi.org`
  or `files.pythonhosted.org`, it adds "installing packages from PyPI
  typically requires both pypi.org and files.pythonhosted.org in
  egress.hosts".
- "refused DNS name `<name>`, which resolved to an address that is not public"

After 32 distinct names it logs "further refused DNS names are not reported"
and stops.

Errors which name a data plane's own preparation (its KVM, its network
boundary, its configuration) are the operator's; see
[operating.md](operating.md#troubleshooting).
