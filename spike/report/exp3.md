# Experiment 3: protocol parity with Go connectors

**PASS.** Six diffs, all empty: documents, connector log lines, and transaction
stats, for a Go capture and a Go materialization, each previewed with the switch
on and off. The connector's own output arrives through the same decoder in both
arms, byte for byte after masking clocks and container identities.

Measured at commit `6a2a47d6af6` (WP06's parent; the runs precede the commit),
`spike/tasks/exp3-parity.sh`. Connectors:
`ghcr.io/estuary/source-hello-world@sha256:96147403c20ca42faa2943b4d18d2bec8c4ff072892e7a09d240b43b674ad1db`
and
`ghcr.io/estuary/materialize-sqlite@sha256:7c89b59b3788fe6bd1ebcd72f1a1e249772ff9bb92e9908a3ed6486e682d8e54`.

## Results

| task                   | comparison | lines | result |
|------------------------|------------|-------|--------|
| capture                | documents  | 5     | identical |
| capture                | connector  | 2     | identical |
| capture                | stats      | 5     | identical |
| materialize            | documents  | 0     | identical |
| materialize            | connector  | 3     | identical |
| materialize            | stats      | 2     | identical |

- **documents** is `flowctl preview` stdout. The capture emits five; the
  materialization emits none and commits to its endpoint instead.
- **connector** is every log line the connector itself produced, as the decoder
  rendered it. The materialization's is the richest payload the two Go
  connectors generate: `connector applied` carrying the `CREATE TABLE` DDL it
  ran, newlines and all, round-tripped through connector-init, the codec, and
  the log decoder.
- **stats** is the runtime's per-transaction accounting. Equality here says the
  transactions were the same, not just the chatter around them.

The materialization commits to a SQLite file at `/tmp/sqlite.db`, which with the
switch on is inside the guest's writable root - podman's per-container layer of
the connector image. So the parity result also exercises the writable root under
a real workload rather than at `touch`.

## What is masked, and one thing that turned out to carry no signal

Masked: the leading timestamp, `handler{id=N}`, container names, IP addresses
and ports, temporary paths, `ts` / `lastPublishedAt` / `lastSourcePublishedAt`,
and `openSecondsTotal`.

Also masked, after it produced a false failure: `bytesTotal`. The capture stamps
each document with a wall clock whose serialization is sometimes one byte
shorter (a trailing zero in the nanosecond field is trimmed), so byte totals
differ between two runs of the **same** arm. `docsTotal` and `txnCount`, which
do carry signal, are left alone and are identical.

## The one difference, which is by contract

`started connector container` differs, and is deliberately not part of any diff:

```
off  {"ipAddr":"10.89.0.84","networkPorts":null,"mappedHostPorts":{"49092":"127.0.0.1:36095"}}
on   {"ipAddr":"192.0.2.2","networkPorts":null,"mappedHostPorts":null}
```

CONTRACTS "runtime-next spike switch" specifies exactly this: `ip_addr` is the
guest's tap address and `network_ports` is empty, since nothing in the spike
exercises connector network ports. That is the launcher changing, which is the
point of the spike; the protocol above it does not.

## Scope: Spec and Validate run unsandboxed, in both arms

`flowctl preview` validates the catalog through the **legacy `runtime` crate**
before runtime-next drives anything, and the switch does not touch that crate.
So every switched-on preview starts two unsandboxed `fc_*` containers (Spec,
Validate) before the sandboxed `fs_*` one (Open onward).

What this experiment proves is therefore parity on the **Open-and-documents
path**, which is where the connector actually runs and where all of the
protocol traffic is. Spec and Validate parity is trivially "same code": with the
switch on they run the identical unsandboxed launcher they run with it off.

**Open problem for phase 2, owner: runtime.** Sandboxing Spec and Validate needs
the legacy `runtime` crate's container launcher to move to runtime-next first
(the `connector_proxy.go` work already implied by that migration). Until then a
customer's Python code runs unsandboxed during validation, which is a real
exposure and not merely a tidiness question. Recorded here and in `exp4.md`.
