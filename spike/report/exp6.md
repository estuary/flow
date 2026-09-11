# Experiment 6: egress rules proven from inside the guest

**PASS.** 39 probes across four passes, all passing, with no forbidden packet
reaching the helper's uplink in any of them. Every rule the design relies on was
exercised from inside a real libkrun guest, across the real tap, by a process
running as the connector image's own user.

WP02 proved the same ruleset in a veth namespace pair. This is the same probe
suite, unmodified, in the arrangement we intend to ship - which is the only
arrangement in which "the guest cannot reach X" is a claim about the product.

Measured at commit `8703b322029` (WP07's parent; the runs precede this package's
commit), `spike/tasks/exp6-egress.sh`, guest
`ghcr.io/estuary/derive-python@sha256:c26548740a9e967274f6d7b9c79bed73630bf61bd187c3afca7564577ef364c7`
(Python 3.14.5), helper `localhost/flow-sandbox-helper:spike`, nftables v1.1.3
inside the helper. Raw probe data: `data/exp6-probes.csv`.

## What the guest was wired to

```
[host]  testnet.py DNS on 10.89.0.1:5353 (the flow-connectors gateway),
        forwarding every name it is not authoritative for to aardvark-dns on
        10.89.0.1:53, so public names and container names both resolve.
        203.0.113.5:8080, a TCP server that never hangs up first, and the
        address the one short-TTL name answers with.
        A listener on 10.89.0.1:9000, standing in for the reactor's own port,
        so `connect-gateway` proves a drop rather than an absent service.
        `spike-sibling`, a throwaway container whose aardvark name answers with
        an RFC1918 address.
   |
[helper] flow-connectors, uplink pinned to 10.89.0.90, tap0 192.0.2.1/30,
        flow-sandbox-egress + flow-sandbox-resolver, --resolver-upstream
        pointing at testnet.py.
   |
[guest] 192.0.2.2/30, nameserver 192.0.2.1, probes.py as uid 65534 (`nobody`).
```

Three targets are worth calling out because they make the probes real rather
than vacuous:

- **169.254.169.254:80 answers on this box.** It is GCP; the metadata server is
  genuinely reachable from the helper's network, and connects in 3 ms from a
  container on `flow-connectors`. The probe is therefore a test of the ruleset,
  not of an absent service.
- **198.51.100.10:443 (nginx) answers** and connects in 7 ms from the same
  place. `connect-unresolved-ip` is a real reach at a real service.
- **203.0.113.5:443 refuses in 2 ms** (the host owns the /32 and listens on no
  such port), so `connect-undeclared-ip` distinguishes "the ruleset dropped it"
  (timeout) from "the host said no" (refused).

## public, as the connector image's own user

18 probes, all pass. `--slow`, so the TTL probes wait out the 90 second floor.

| probe | expected | observed | ms | packets left helper |
|---|---|---|---|---|
| dns-public | resolves | `ok:104.20.23.154,172.66.147.243` | 18 | allowed |
| connect-resolved-443 | connects | connected | 13 | allowed |
| connect-resolved-25 | blocked | timeout | 3003 | 0 |
| connect-unresolved-ip | blocked | timeout | 3003 | 0 |
| dns-rfc1918 | lookup errors | `error:gai-3` (EAI_AGAIN) | 2 | n/a, no connect attempted |
| connect-metadata | blocked | timeout | 3003 | 0 |
| connect-helper-tap | blocked | timeout | 3003 | n/a, helper's own address |
| connect-helper-uplink | blocked | timeout | 3003 | n/a, helper's own address |
| dns-helper-uplink | blocked | timeout | 3003 | n/a, helper's own address |
| dns-aaaa-empty | NOERROR, no records | empty | 0 | n/a |
| connect-gateway | blocked | timeout | 3003 | 0 |
| connect-ipv6 | not connected | `error:EADDRNOTAVAIL` | 0 | 0 |
| inbound-listener | nothing connects | no-connection | 5005 | n/a, see below |
| dns-ttl-clamped | within the clamp | `ttl:90` | 2 | n/a |
| ttl-connect-before | connects | connected | 0 | allowed |
| ttl-connect-after-expiry | blocked | timeout | 3006 | not captured, see note |
| ttl-held-connection-survives | still passes traffic | connected | 1 | allowed |
| ttl-reresolve-connect | connects | connected | 3 | allowed |

Every `timeout` is the probe's own 3 second cap, not the kernel's: the deny
action is `drop`, so nothing is refused and the real failure latency is bounded
only by the guest's SYN budget (~127 s at `tcp_syn_retries=6`). The numbers
above are lower bounds.

Counters after the pass:

```
forward   anti-spoof 0  replies 19  no-ipv6 0  tcp-udp-only 0  baseline 6
          smtp 3  egress 9  forward-drop 6
egress_accept  resolved 3
input     loopback 0  anti-spoof 0  replies 6  tap-dns 7  input-drop 7
output    no-inbound 136
postrouting    masquerade 3
```

- `input/input-drop 7` is the guest reaching for the helper itself - the tap
  address on 443, the uplink address on 443, the uplink address on 53 - and
  being dropped by the input chain, which accepts exactly one thing from the
  tap.
- `output/no-inbound 136` is the knocker: `socat` from inside the helper to
  `192.0.2.2:34567`, over and over for the duration of the run. **124 attempts,
  none connected**, including the five seconds in which the guest was actually
  listening on that port. Nothing in the helper can open a connection to the
  guest.
- The capture on the host side of the helper's veth was empty for the whole
  pass.

**Note on `ttl-connect-after-expiry`.** Its destination is 203.0.113.5, which
the guest is *supposed* to reach earlier in the same pass, so the capture filter
deliberately excludes it and there is no packet-count evidence for that one
probe. The evidence is the timeout plus `forward-drop` advancing: the set
element expired, the address stopped matching `@resolved`, and the chain's drop
policy took it.

## public, as guest root

16 probes, all pass. Same set with `--run-as-root`, which adds the two that need
raw sockets. The point is not only the two extra probes: **the whole set behaves
identically whether the workload is the image's user or root**, which is what
"the guest is confined by the helper, not by the guest's own privilege" means.

| probe | expected | observed | ms | packets left helper |
|---|---|---|---|---|
| icmp-blocked | blocked | timeout | 3003 | 0 |
| spoofed-source | datagram handed to the kernel | sent | 0 | 0 |

The other 14 are byte-identical in verdict to the pass above; see
`data/exp6-probes.csv`.

```
forward   anti-spoof 0  tcp-udp-only 1  baseline 6  smtp 3  forward-drop 3
```

`tcp-udp-only 1` is the ICMP echo: it crossed the tap, reached the forward
chain, and was dropped there because the rule admits only TCP and UDP. That is
the rule the PLAN asks for ("no ICMP crosses the tap"), doing its job.

`anti-spoof 0` is the interesting one, and it is not a gap.

## The anti-spoof rule cannot fire in this topology, and that is fine

`tap0`'s `rp_filter` inside the helper is **1**, inherited from the host
(`net.ipv4.conf.default.rp_filter=1` on this box). Two independent things stop a
forged source before nftables is reached:

1. **A source inside the tap's /30 is impossible.** `192.0.2.0/30` has exactly
   four addresses: `.0` is the network, `.1` the helper, `.2` the guest, `.3` the
   broadcast. The brief's `192.0.2.3` is the subnet broadcast address, and Linux
   rejects a broadcast address as a *source* in `fib_validate_source` whatever
   `rp_filter` says - `ip route get 192.0.2.3 from 192.0.2.2 iif tap0` returns
   `broadcast`, not a unicast route. There is no spare unicast address in a /30
   to forge from. This is a consequence of the /30, not of the ruleset.
2. **A source outside the /30 fails the strict `rp_filter`**, because the
   reverse route for it is `eth0` and the packet arrived on `tap0`.

So in the configuration we intend to ship, a spoofed packet dies in the routing
layer and the nft rule never counts it. Confirmed both ways: forging from
`192.0.2.3` and from `198.51.100.99`, with `rp_filter` inherited, leaves
`forward/anti-spoof` at 0 and the capture empty in all four combinations.

To show that the rule itself is real rather than merely unreached, a fourth pass
relaxes `rp_filter` and forges an ordinary off-net source:

| pass | rp_filter on tap0 | spoof source | forward/anti-spoof | packets left helper |
|---|---|---|---|---|
| public-root | 1 (inherited) | 192.0.2.3 | 0 | 0 |
| spoof-nft | 0 (set at container creation) | 203.0.113.99 | **1** | 0 |

`rp_filter` has to be set with `--sysctl` on the `podman run`, and this took two
wrong turns worth recording: the helper image has no `sysctl` binary, and
podman mounts `/proc/sys` read-only in the container, so writing the knob
through `/proc` from a `podman exec` silently does nothing. `tap0` also does not
exist yet at container creation, so it is `conf.default.rp_filter=0` that the
tap inherits - setting `conf.all` alone is not enough, because the effective
value is the maximum of the two.

**For the runtime:** the nft anti-spoof rule is a third line of defence behind
two kernel ones. It costs nothing and should stay. But the control that actually
runs in production is `rp_filter`, which the helper inherits rather than sets -
so if the host's default were ever 0, enforcement would silently move from the
kernel to the one rule proven here to work. Setting it explicitly on the tap in
the shim remains the phase-2 item master already recorded.

## The no-ipv6 rule never fires either, for the same kind of reason

`connect-ipv6` returns `EADDRNOTAVAIL` in 0 ms and `forward/no-ipv6` stays at 0.
flow-init disables IPv6 in the guest, so the socket cannot be given a source
address and no packet is ever offered to the tap. The forward chain's `no-ipv6`
drop is the backstop for a guest that somehow turned it back on. Two controls,
the outer one unreached - and `dns-aaaa-empty` confirms the third: the resolver
answers AAAA queries NOERROR with nothing in them, so a client that prefers IPv6
is not even tempted.

## TTL behaviour

The one thing the public internet will not promise is a TTL below the clamp
floor, so `spike/egress/testnet.py` serves `expiry.spike.invalid` with a 5 second
TTL pointing at a TCP server that never hangs up first. The helper's resolver was
pointed at it with a spike-only `--resolver-upstream`; every other name in the
run still went through it to the real aardvark-dns and the real upstream.

| what | result |
|---|---|
| TTL the guest sees for a 5 s record | 90 s, the floor, rewritten by the resolver |
| connect before expiry | connected |
| connect 95 s later, without re-resolving | timeout - the set element is gone |
| the connection opened before expiry | still passes traffic, answered a HEAD |
| re-resolve, then connect | connected |

`clamp(ttl, 90, 3600)` as CONTRACTS specifies, and an established flow is not
cut when its element expires - conntrack's `established` accept sits ahead of
the set lookup. A connector holding a long-lived connection does not lose it to
a DNS clock.

## declaredCidrs

3 probes, all pass. `examples/declared.json` declares `198.51.100.10/32` on port
443 only.

| probe | expected | observed | ms | packets left helper |
|---|---|---|---|---|
| connect-declared-port | connects without resolving | connected | 2 | allowed |
| connect-declared-other-port | blocked | timeout | 3003 | 0 |
| connect-undeclared-ip | blocked | timeout | 3003 | 0 |

`egress_accept/declared 1`, `forward/forward-drop 6`. The declared entry is
`ipv4_addr . inet_service`, so the port is part of the match rather than a
separate rule: declaring a CIDR does not open it.

Note that this policy **fails to load on the host** and loads inside the helper,
because the binary really does refuse a declared CIDR that overlaps the
baseline, and on this box the host owns `198.51.100.0/24`. Inside the helper the
only subnets are the tap and `10.89.0.0/24`. Run the binaries where they run in
production.

## What this does not cover

- **Per-probe packet counts for the destinations a pass is allowed to reach.**
  The capture filter is per-pass and excludes them by construction; for those
  probes the evidence is the nft counters.
- **IPv6 beyond a connect attempt.** The guest cannot form one, so there is
  nothing to send. The ruleset's IPv6 drop is asserted only by inspection.
- **A guest that attacks the tap itself** - ARP, a second address, a route of
  its own. Structurally out of reach of a probe suite that runs as a process
  inside the guest; it is a phase-2 question for the runtime.
