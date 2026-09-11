# Experiment 7: `egress: none`

**PASS.** With `egress: none` the guest cannot resolve a name and cannot open a
connection, and not one IP packet attributable to it reaches the helper's
uplink. The cost of that is the point of the experiment, and it is **10 to 15
seconds per failure, with no error until the budget runs out**.

Measured at commit `8703b322029` (WP07's parent; the runs precede this package's
commit), `spike/tasks/exp7-none.sh`, guest
`ghcr.io/estuary/derive-python@sha256:c26548740a9e967274f6d7b9c79bed73630bf61bd187c3afca7564577ef364c7`
(Python 3.14.5, glibc). Raw probe data: `data/exp7-probes.csv`.

## The probes

| probe | expected | observed | seconds | packets left helper |
|---|---|---|---|---|
| dns-any | lookup errors | `error:gai-3` (EAI_AGAIN) | 10.0 | 0 |
| connect-raw-ip | blocked | timeout | 15.0 | 0 |
| connect-metadata | blocked | timeout | 15.0 | 0 |

**Both connect figures are the probe's own cap, not the kernel's.** The deny
action is `drop`, so nothing is refused and the guest has no way to learn that
the destination is unreachable: with `tcp_syn_retries=6` a dropped SYN takes
about 127 seconds to surface as `ETIMEDOUT`. What the table says is "at least
15 seconds", which is all the experiment set out to bound. A connector that does
not set its own connect timeout will sit there for over two minutes.

The DNS figure is real rather than capped: 10.0 seconds is glibc's own budget in
this image, and `input/input-drop 2` says it spent it on exactly two queries -
one A and one AAAA, sent together, timing out together. WP02 measured 20.0 s for
the same probe in a namespace on this host's resolver configuration, which sent
a second round. **The number depends on the image's `resolv.conf`, not on the
sandbox**, so it is a range to expect rather than a constant.

## Counters

```
forward   baseline 8  forward-drop 8
input     input-drop 2
output    no-inbound 9
postrouting    masquerade 0
```

- `baseline 8` is the metadata server: `169.254.0.0/16` is in the baseline, so
  those SYNs are dropped by the denylist rule.
- `forward-drop 8` is nginx at `198.51.100.10`, which is not in the helper's
  baseline and instead falls off the end of a chain that, in `none` mode, has no
  accept in it at all.
- `input-drop 2` is the pair of DNS queries. `egress: none` **drops** the
  guest's query rather than refusing it, deliberately: a refusal would let the
  guest fail fast, and this number is exactly the cost PLAN asked to measure.
- `masquerade 0`: nothing was ever forwarded, so nothing was ever translated.

## The resolver does not run

CONTRACTS says no resolver runs for `egress: none`, and the shim reads only the
`egress` field of the policy to decide it. Asserted rather than assumed: a
resolver that had started would have written a `listening on` line to the
helper's stderr, and none appears. So in `none` mode the guest's nameserver
address points at nothing, which is why the query is dropped in the input chain
rather than answered with a refusal.

## What this means for the design

`egress: none` is correct and cheap to enforce - it is the same skeleton with
the accepts, the resolver and the DNS input rule removed - but it is not
*ergonomic*. A connector misconfigured onto a `none` policy does not fail, it
hangs, and the first symptom an operator sees is a task making no progress. Two
things would change that, neither of them this spike's to decide:

- **`reject` instead of `drop`** for the guest's DNS query specifically. It is
  the one packet whose refusal tells the guest nothing it does not already know
  (there is no resolver), and it would turn a 10 second stall into an immediate
  `SERVFAIL`. The connect timeouts would remain.
- **Surfacing the policy in the task's logs at startup**, so "no egress" is a
  visible fact rather than something inferred from latency.

Recorded as an open question, not a recommendation.
