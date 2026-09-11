# spike/egress

The sandbox's network policy, and the proof that it holds. Two binaries that
run in the helper container before the VM starts, a probe suite that runs
where the connector runs, and a test that puts both in a pair of network
namespaces with no libkrun anywhere.

Nothing here knows about VMs. The guest is just the far end of a tap.

## What is where

| path                     | what it is                                                     |
|--------------------------|----------------------------------------------------------------|
| `src/bin/egress.rs`      | `flow-sandbox-egress`: policy JSON in, `inet flow_sandbox` out  |
| `src/bin/resolver.rs`    | `flow-sandbox-resolver`: the guest's only nameserver            |
| `src/ruleset.rs`         | the whole ruleset, rendered as text; nothing else touches nft   |
| `src/policy.rs`          | the policy JSON of CONTRACTS.md, and the baseline denylist      |
| `src/dns.rs`             | enough DNS to read A records and patch their TTLs in place      |
| `src/ifaddrs.rs`         | the helper's own subnets, read rather than assumed              |
| `probes.py`              | the probe suite: four named sets, one JSON line per probe       |
| `examples/*.json`        | the four policies the probe sets expect                         |
| `testnet.py`             | test-only stand-in internet (see below); not part of the sandbox |

Push-button: `spike/tasks/egress-build.sh` builds, and
`spike/tasks/egress-netns-test.sh` proves. Neither takes arguments it needs.

## The ruleset, in one paragraph

One table, `inet flow_sandbox`, replaced wholesale on every apply. The forward
chain drops by policy and works through anti-spoof, established/related,
IPv6, anything that is not TCP or UDP, the baseline denylist, tcp/25, and then
the rate limits, before jumping to `egress_accept` where the only three
accepts live: `allowAll`, `@resolved`, and `@declared`. `@resolved` is empty
until the resolver puts something in it. The input chain drops by policy and
lets exactly one thing in from the tap: the guest's DNS to the helper. The
output chain lets nothing out to the tap that is not an established reply, so
nothing in the helper can open a connection to the guest. Postrouting
masquerades the guest out of the uplink.

Two places that catch people:

- The baseline covers *forwarded* traffic to the helper's own subnets, e.g. a
  sibling connector on the same podman bridge. The helper's own addresses are
  covered by the input chain instead, because packets addressed to a local
  address never reach the forward hook.
- `egress: none` is the same skeleton with the accepts, the resolver and the
  DNS input rule all absent. The missing DNS rule is deliberate: dropping the
  query rather than refusing it is what makes the guest pay its resolver's
  full retry budget, which is the number PLAN experiment 7 asks for.

## The resolver

Answers are gated, not just forwarded. An A answer with any address inside the
baseline or the helper's own subnets fails whole, as REFUSED - one denied
address condemns the answer, because a partial answer tells the guest exactly
which name resolves inward. Otherwise every address is added to `@resolved`
with `clamp(ttl, ttlFloorSecs, ttlCapSecs)` as its element timeout *before*
the answer is sent, and the answer's TTLs are rewritten to the same value so
the guest's own cache expires with the set element. AAAA is answered NOERROR
with nothing in it; everything else is forwarded byte for byte.

It shells out to `nft add element` once per answered query (~1 ms on this
box). Two spike-shaped consequences the runtime should not inherit:

- a re-resolved name does not get a fresh timeout. `nft add element` on an
  element that already exists succeeds and leaves the original expiry alone,
  so a name resolved again at second 80 of its 90 still expires at 90. A
  netlink update fixes this; `delete` then `add` would open a window where
  the address is unreachable.
- one query at a time, sequentially. Fine in front of one connector.

## Probes

`probes.py --set {public,none,declared,ratelimit}`, every target from argv,
Python 3.12 stdlib only, so the same file runs in a netns here and inside the
derive-python guest later. One JSON line per probe; exit 0 iff all pass.

The distinction that matters in every result: `timeout` means the ruleset
dropped the packet, `refused` means a host answered RST. Nothing treats one as
the other, and `blocked` only ever passes on `timeout`.

Two probes need root (raw sockets) and are simply absent from the output when
it is not available: `icmp-blocked` and `spoofed-source`. The spoofed-source
probe cannot see its own outcome - a forged packet gets no reply either way -
so it reports that the packet was sent, and the helper's `anti-spoof` counter
is the observation. `--slow` adds the probes that wait out a TTL (95 s) or a
rate window (65 s).

## testnet.py, and why the test is not pointed at the real internet alone

The netns test resolves through the real upstream for everything except one
name. Two probes need what public infrastructure will not promise:

- the clamp floor is only observable with a TTL below it, and public TTLs
  change without telling us;
- "a connection opened before expiry stays open" needs a server that holds an
  idle connection for more than 90 seconds, and public servers hang up in
  seconds - which looks exactly like the ruleset cutting the flow.

So `testnet.py` answers one `.invalid` name with a 5 second TTL pointing at a
TCP server that never hangs up, and forwards every other query to the box's
real resolver. The public-name, RFC1918-name and metadata probes are all
still real.

## Running it

```
spike/tasks/egress-netns-test.sh                # all four sets, slow probes included
spike/tasks/egress-netns-test.sh --quick        # skip the 95 s and 65 s waits
spike/tasks/egress-netns-test.sh --set public
```

It needs sudo, and it leaves nothing behind: namespaces, veths, the two
iptables rules it inserts on the host and `/etc/netns/spike-eg-guest` all go
away on exit, including on error. `spike-nginx` must be up for the declared
set (`spike/tasks/env-setup.sh`).
