# Experiment 9: connection churn

**MEASURE, and nothing went wrong.** 30,000 HTTPS connections, one per request,
at 50 per second for ten minutes through the tap to the declared-CIDR nginx.
Every one completed; none failed. The offered rate held exactly - all 60
ten-second windows carried 500 connections, 50.00/s, with no window low and no
window catching up after one. The helper cost **15.2% of one core** and
**95 MiB** of cgroup memory, and neither moved after the first minute.

Measured at commit `817c6c65bab`, `spike/tasks/exp9-churn.sh`, guest
`ghcr.io/estuary/derive-python@sha256:c26548740a9e967274f6d7b9c79bed73630bf61bd187c3afca7564577ef364c7`,
helper `localhost/flow-sandbox-helper:spike` (libkrun v1.19.4, built
2026-09-11). Raw data: `data/exp9-churn.csv` (the guest's windows),
`data/exp9-cgroup.csv` (the host's cgroup samples).

## Shape of the run

| | |
|---|---|
| target | `https://198.51.100.10/probe`, 1 KiB body, self-signed cert not verified |
| policy | `egress/examples/declared.json` - `198.51.100.10/32` on port 443 |
| offered | 50 connections/s for 600 s = 30,000, one new TCP+TLS connection each |
| guest | derive-python, 1024 MiB, **2 vcpu**, `--disk-mib 4096` |
| helper cgroup limit | 1280 MiB (`--memory 1024m + 256m`) |

Nothing is pooled: every request is a fresh `socket.create_connection`, a fresh
TLS handshake, one `GET` with `Connection: close`, and a close. The path under
test is therefore connection *setup* through virtio-net and the nftables forward
chain, which is what PLAN asked for - there is no userspace proxy anywhere in
it.

## Throughput

| | offered | achieved |
|---|---|---|
| rate | 50/s | **50.0/s** |
| connections | 30,000 | 30,000 ok, **0 failed** |
| per-window rate | 50.00/s | min 50.00, median 50.00, max 50.00 over 60 windows |

Latency per connection, measured on the guest's own clock (WP06: host arrival is
~290 ms late and cannot time anything in-guest):

| p50 | p95 | max |
|---|---|---|
| 2.9 ms | 3.3 ms | 26.5 ms |

2.9 ms is a full TCP handshake, TLS 1.3 handshake, request, 1 KiB response and
close, guest to nginx and back. The 26.5 ms maximum is a single outlier.

The pacer works against a fixed schedule (`start + n/rate`), not a sleep per
request, so a slow window cannot push later windows back and hide itself. Every
window came out at exactly 500, which is the strongest statement this experiment
can make: the tap did not drop, stall, or batch.

## What the helper cost

Sampled every 10 s from the helper's cgroup on the host:

| | start (t=0) | steady state | end (t=593) |
|---|---|---|---|
| `memory.current` | 49.4 MiB | 93.9 - 95.9 MiB | 94.9 MiB |
| `memory.stat anon` | 46.8 MiB | - | 89.5 MiB |
| threads | 12 | 14 | 14 |
| CPU | - | 15.4% of one core (median), 13.3 - 20.3% | 90.9 s total |

- **CPU: 90.9 s of CPU for 600 s of wall clock = 15.2% of one core**, against
  the 2 vcpus configured. Per connection that is 3.0 ms of host CPU for a
  handshake the guest saw take 2.9 ms, so essentially all of it is the guest's
  own work plus the tap copy; the VMM is not adding a multiple.
- **Memory is flat.** It rises once, over the first minute, from 49 MiB to
  ~95 MiB, and then holds within a 2.0 MiB band for the remaining nine minutes.
  30,000 connections through the tap leak nothing measurable.
- **Threads: 12 at the first sample, 14 for the rest, and never anything else.**
  Connection churn does not create threads. The two that appear are libkrun's,
  not per-connection.

`t=0` is taken as soon as the container is executable, which is before the guest
has opened its first connection - that is why the first row is low. The ramp
between it and the steady state is the guest first-touching memory, the same
term WP08b found in boot latency.

## What this does and does not say

- **The tap is not a bottleneck at this rate, and nothing degrades over time.**
  That was the question. 50/s sustained for ten minutes with zero failures and
  flat memory is a clean answer.
- **It is not a ceiling.** 50/s was PLAN's number and the path carried it
  without visible strain (15% of one core), so the experiment says nothing about
  where the limit actually is. If a phase-2 question needs the ceiling, it needs
  a different run - raise the rate until windows stop coming out at the offered
  value.
- **It exercises the declared-CIDR path**, as PLAN intended: nginx is reachable
  as a `/32` on 443 with no DNS resolution involved, so `egress_accept`'s
  declared rule carried all 30,000 connections and the resolver was never asked
  anything.
- **conntrack held.** 50 new connections a second against the forward chain's
  `ct state` rules, for ten minutes, with no failure - so the conntrack table
  absorbed roughly 6,000 concurrent `TIME_WAIT` entries without complaint at
  default sizing. Worth knowing, not worth tuning here.

## Method note, for whoever reads the CSV

The per-window counts are written **after the worker pool drains**, not as the
run proceeds. The pacer runs a bounded queue ahead of real time, so a window
reported at the moment its last job is *enqueued* is missing every request still
sitting in the queue: the first version of this script reported a flat 38.50/s
for 59 windows and 50.00/s for the last one, which is the queue depth, not the
network. The totals were right in both versions; only the per-window series was
wrong. Anything else that reports a rate from inside a paced driver should count
completions, not submissions.
