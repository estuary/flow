# Experiment 8: rate and fan-out limits

**PASS.** Both limits hold, and they fail in opposite ways - which is the
finding the runtime has to build on:

- **`connectionsPerMinute` produces slowness, not errors.** nftables drops the
  SYN; TCP retransmits it a second later into a bucket that has refilled by
  then. 80 connection attempts, 79 SYNs dropped, **80 completed**, 79.9 seconds
  wall clock: exactly 60 per minute. No connector ever sees a failure.
- **`distinctDestinationsPerMinute` produces a timeout.** The sixth distinct
  destination cannot be added to a set with `size 5`, the rule's verdict never
  runs, and the packet falls to the chain's drop policy. It stays unreachable
  for the rest of the window.

Both recover when the window passes. Measured at commit `8703b322029` (WP07's
parent; the runs precede this package's commit), `spike/tasks/exp8-limits.sh`,
guest
`ghcr.io/estuary/derive-python@sha256:c26548740a9e967274f6d7b9c79bed73630bf61bd187c3afca7564577ef364c7`,
nftables v1.1.3 inside the helper. Raw probe data: `data/exp8-probes.csv`.

Policy: `examples/ratelimit.json`, `connectionsPerMinute: 60`,
`distinctDestinationsPerMinute: 5`, `declaredCidrs: [203.0.113.0/24:443]`. The
twenty fan-out destinations are TEST-NET-3 /32s the host owns and listens on
none of, so a SYN that arrives comes back RST in about 2 ms and "reached" is
unambiguous.

## The nft constructs, verbatim

These are the two lines the runtime implementation should copy, read back out of
the kernel rather than out of the generator:

```
ct state new limit rate over 60/minute burst 5 packets counter drop comment "rate-limit"
ct state new update @dests { ip daddr } counter jump egress_accept comment "fan-out"
```

with

```
set dests {
        type ipv4_addr
        size 5
        flags dynamic,timeout
        timeout 1m
}
```

Three details in there are load-bearing:

- **`update`, not `add`.** `update` refreshes an element that is already
  present; `add` does not, so a busy destination would age out mid-conversation.
- **The verdict is on the same rule as the set update.** When the set is full
  the update fails, the `jump` never happens, and the packet reaches the chain's
  `policy drop`. That is what makes a full set mean "deny" rather than "skip the
  fan-out rule and carry on to the accepts".
- **`burst 5 packets`** is nft's default burst for a `limit rate`, and it is why
  the first handful of connections in an idle window go straight through.

## Fan-out

| probe | expected | observed | ms |
|---|---|---|---|
| fanout-01 .. fanout-05 | reachable | refused (2, 1, 0, 0, 0 ms) | 0-2 |
| fanout-06 .. fanout-20 | blocked | timeout, all fifteen | 2002 |
| fanout-recovers (after the window) | reachable | refused | 0 |

Five of twenty, which is the configured limit, and the cut is exactly at the
boundary - arrival order decides who gets a slot. The capture on
`203.0.113.29`, a destination past the limit that no later probe revisits, was
empty: the sixth-onward destinations were not merely unanswered, nothing was
sent to them.

At the end of the run the set held two elements, `203.0.113.11` and
`203.0.113.30`, each with about 59 s left - the first destination the rate probe
kept hammering, and the one the recovery probe re-reached after the window.

## Rate

| probe | expected | observed | seconds |
|---|---|---|---|
| rate-limit | held at the configured rate | `limited:80-reached-0-blocked-60-per-minute` | 79.9 |
| rate-recovers | reachable | refused | 0.0 |

Read the result string as: 80 attempts reached the destination, 0 were blocked
outright, and the sustained rate was 60 per minute. `forward/rate-limit 79` is
the nft counter for dropped SYNs over the same run - so nearly every connection
was dropped once and succeeded on its retransmission.

**This is a rate limiter that never produces an error.** For `connectionsPerMinute`
that is the right behaviour and master has confirmed it: the purpose is to cap
what our egress addresses can be used for, and pacing a bursty connector does
that without spurious failures. But it is worth being explicit about the
consequence: a connector that expects to open 600 connections a minute against a
60/minute policy will not fail, it will take ten times as long, and nothing in
its logs will say why. The visible symptom is throughput, not an error.

`forward/forward-drop 29` is the fan-out drops: fifteen destinations, about two
SYNs each inside the probe's 2 second cap.

## What this does not cover

- **Fan-out by name.** PLAN's wording is "resolves and connects to 20 distinct
  hosts"; twenty resolvable names pointing at hosts we may hammer do not exist,
  so this connects to twenty declared destinations instead. The limits sit in
  front of every accept path, `@resolved` and `@declared` alike, so the
  constructs under test are identical - but a name-driven run would additionally
  exercise the resolver's own rate of `nft add element` calls, which WP02
  measured separately at ~1 ms each.
- **Interaction between the two limits under contention.** Each was driven on
  its own. A connector at both limits simultaneously is a load question, not a
  correctness one.
- **What the limits should be.** Whether `connectionsPerMinute` is the right
  knob to expose, given that it surfaces as latency rather than as a policy
  violation, is a phase-2 product question. It is in the report's open problems.
