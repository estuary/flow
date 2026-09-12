# Experiment 10: density

**MEASURE.** **82 idle 512 MiB guests** fit on this box before the host's
available memory crossed 20%, and the 82nd cost the same as the first: 95 MiB of
host memory, 72.6 MiB of helper cgroup, 13 threads. **THP makes no difference**
(95.2 vs 94.6 MiB per guest). A guest holding one long-lived TLS connection
costs about **5 MiB more** than an idle one. Free-page reporting works: a guest
that drops 512 MiB of page cache gives **493 MiB** of it back to the host within
seconds.

**The overhead constant is small and flat: ~20 to 32 MiB, independent of
`memoryMib` and of how much of it the guest is using.** The launcher's current
`FLOW_SANDBOX_SPIKE_MEMORY_OVERHEAD_MIB` default of 256 is roughly 8x that.

Measured at commit `817c6c65bab`, `spike/tasks/exp10-density.sh`, guest
`ghcr.io/estuary/derive-python@sha256:c26548740a9e967274f6d7b9c79bed73630bf61bd187c3afca7564577ef364c7`,
helper `localhost/flow-sandbox-helper:spike` (libkrun v1.19.4). Host:
15,983 MiB `MemTotal`, 8 cores. Raw data: `data/exp10-density.csv`,
`data/exp10-arms.csv`, `data/exp10-reclaim.csv`, `data/exp10-overhead.csv`.

**The box was not idle.** An editor's `rust-analyzer` held ~2.9 GiB throughout,
so each pass started from ~10,900 MiB available rather than ~15,000. The *count*
reached is therefore specific to this box and this moment; the *per-guest cost*
is the number that travels, and it is measured as an increment, not a quotient
of the total.

## The four passes

`memoryMib: 512`, 1 vcpu, `--disk-mib 1024`, helper cgroup limit 768 MiB
(`512m + 256m`, as the spike switch sets it). Launched one at a time, each
measured as it landed, stopping when `MemAvailable` fell below 20% of
`MemTotal` (3,196 MiB).

| arm | guests | stopped at | cgroup median | cgroup max | threads | host MiB/guest |
|---|---|---|---|---|---|---|
| idle, THP on | **82** | floor | 72.6 MiB | 90.9 MiB | 13 | **95.2** |
| idle, THP off | **82** | floor | 72.5 MiB | 75.5 MiB | 13 | **94.6** |
| TLS held, THP on | 20 | cap | 78.0 MiB | 82.4 MiB | 13 | 100.6 |
| TLS held, THP off | 20 | cap | 77.8 MiB | 79.1 MiB | 13 | 99.5 |

The TLS arms stop at 20 because PLAN asked for 20, not because anything ran out.
Every one of those guests completed a real handshake before it was counted -
`status 200, 1024 bytes, TLS_AES_256_GCM_SHA384` - and held the connection open
with a periodic GET, so nginx's `keepalive_timeout` never closed it.

### Cost per guest does not grow with the number of guests

| arm | first 10 guests | last 10 guests |
|---|---|---|
| idle, THP on | 95.0 MiB each | 94.8 MiB each |
| idle, THP off | 95.2 MiB each | 89.7 MiB each |
| cgroup, idle THP on | 73.0 MiB median | 72.5 MiB median |

This is the most useful line in the experiment. There is no per-guest tax that
accumulates: helper number 82 costs what helper number 1 cost, and its cgroup is
the same size. Density on this design is linear, and the limit is simply host
RAM divided by the per-guest cost.

### Host cost is ~22 MiB more than the helper's cgroup

95 MiB leaves the host per guest; 72.6 MiB of it is inside the helper's cgroup.
The remaining ~22 MiB is what the *container* costs outside it - the netns,
podman's own bookkeeping, and the host page tables for the VM's mapping. Anyone
sizing a reactor should use the 95, not the 72.6.

### THP

No difference worth acting on. The medians are within 0.1 MiB and the host cost
within 0.6 MiB. THP on does widen the tail - one helper reached 90.9 MiB against
75.5 MiB with THP off - which is huge pages rounding up, not a leak. **Nothing
here argues for setting `--thp-disable` in production, and nothing argues
against it.**

## Reclaim: does memory come back?

One 1024 MiB guest, `--run-as-root`: write a 512 MiB file to `/scratch`, drop
caches, read the file back into the page cache, hold 90 s, then
`echo 3 > /proc/sys/vm/drop_caches`.

| | `memory.current` | `memory.stat anon` |
|---|---|---|
| page cache full | 1,138 MiB | 602 MiB |
| after `drop_caches` | 645 MiB | 110 MiB |
| **returned** | **493 MiB** | **492 MiB** |

**Free-page reporting works, and it is prompt.** The cgroup sits flat at
1,138 MiB for the whole 90-second hold - no gradual decay, no drift - and then
falls to 645 MiB within one 5-second sample of the drop. 493 of the 512 MiB the
guest stopped using came back.

The two halves of `memory.current` behave differently and the distinction
matters: `anon` is the guest's RAM and it is what free-page reporting returns.
The ~535 MiB of `file` that remains is the *host's* page cache for the scratch
`O_TMPFILE`, charged to the helper's cgroup; the host reclaims that under
pressure on its own terms. A helper sitting at its cgroup limit is therefore not
necessarily a helper in trouble.

## The overhead constant

Each guest reports its own `/proc/meminfo`; the helper's `memory.current` minus
the RAM the guest says it has touched is everything else the sandbox costs.
Measured in two regimes, because an idle guest and a busy one are different
questions, and four guests each at 512 and 1024 MiB.

| mode | memoryMib | n | `memory.current` | `anon` | guest touched | overhead |
|---|---|---|---|---|---|---|
| idle | 512 | 4 | 72.7 MiB | 69.5 MiB | 74.6 MiB | 23.7 MiB |
| idle | 1024 | 4 | 80.1 MiB | 77.2 MiB | 75.1 MiB | 30.6 MiB |
| 80% full | 512 | 4 | 487.3 MiB | 482.9 MiB | 493.0 MiB | 20.0 MiB |
| 80% full | 1024 | 4 | 901.6 MiB | 895.6 MiB | 902.5 MiB | 24.7 MiB |

(The "guest touched" column is `MemTotal - MemAvailable`, PLAN's definition. The
"overhead" column uses `MemTotal - MemFree`; see below.)

| regime | min | median | max |
|---|---|---|---|
| idle, `MemTotal - MemAvailable` | -2.9 | 3.8 | 5.8 MiB |
| idle, `MemTotal - MemFree` | 22.7 | **29.5** | 31.5 MiB |
| 80% full, `MemTotal - MemAvailable` | -6.1 | -2.6 | 0.8 MiB |
| 80% full, `MemTotal - MemFree` | 19.6 | **23.1** | 26.4 MiB |

**Read the `MemFree` rows.** PLAN's `MemTotal - MemAvailable` comes out at
roughly zero, and occasionally negative, which cannot be right - it says the
sandbox is free. The reason is that `MemAvailable` counts the guest's own page
cache and reclaimable slab as available, while the host is still backing every
one of those pages. Subtracting it therefore charges the guest's cache to
"overhead" in the wrong direction. `MemTotal - MemFree` is everything the guest
has touched, and what is left over is genuinely the VMM's.

The useful conclusions:

- **The constant is ~20 to 32 MiB, and it is a constant.** It does not scale
  with `memoryMib` (512 and 1024 give the same figure) and it does not grow when
  the guest actually uses its memory (the 80%-full rows are, if anything,
  slightly *lower*). That is the shape a launcher wants.
- **`FLOW_SANDBOX_SPIKE_MEMORY_OVERHEAD_MIB = 256` is about 8x the measurement.**
  The number this package would propose is **64 MiB** - twice the worst case
  observed, which leaves room for the guest kernel and the VMM to grow without
  pretending 256 MiB of headroom is doing anything.
- **But the cgroup limit is not only about overhead.** It also bounds the page
  cache the helper is charged for - the `file` term above, which reached 535 MiB
  in the reclaim arm and which WP10's experiment 11 saw pin a helper at its
  limit. That cache is reclaimable, so a tight limit costs throughput rather
  than correctness. Anyone lowering 256 to 64 should expect helpers to sit at
  their limit more often and should be sure that is understood as normal.
- A guest's `MemTotal` is 99.6 - 100.8% of the `memoryMib` asked for, so the
  guest kernel's own reservation is already inside the allocation and does not
  need to be added on top.

## Threads

**13 per helper, in every arm, at every guest count.** Idle, TLS-holding, THP on
or off. Thread count is not a scaling term; 82 guests is ~1,070 threads on the
host.

## Method notes

- **`MemAvailable` is sampled after a host `sync`** (WP10). Without it, dirty
  page cache from a guest that has just written counts as memory spent, and the
  floor is crossed early for the wrong reason.
- **The per-guest number is an increment, not a quotient.** Each guest's host
  delta is recorded as it lands, so a box that is not idle (this one, with
  ~2.9 GiB of editor) shifts the starting point but not the cost.
- **Nothing was measured after teardown began.** podman removes containers,
  netns and layers asynchronously after its client returns (WP10), so each arm
  polls to quiescence before reading the recovered figure. All four arms
  returned the memory they took, to within 100 MiB of where they started.
- **No helper hit its cgroup limit in any density arm**, asserted per guest from
  `memory.events`. Every `memory.current` above is what the sandbox wanted, not
  what the host allowed it.

## An operational finding that is not about memory

**Tearing down 82 helpers took about 14 minutes.** `podman rm -f` on a running
helper takes ~8 seconds each at this density, serially, against ~0 seconds for a
single helper (WP10's experiment 13 measured podman settling in 0.0 s after
`flowctl` returned). The contention is podman's, not libkrun's - the shim dies
immediately on SIGKILL.

This is a spike-harness detail (the script removes containers one at a time;
`podman rm -f id1 id2 ...` would let podman parallelize), and it does not affect
any number above, all of which are taken while the helpers are running. It is
recorded because a reactor draining many sandboxes at once will meet the same
contention, and 8 seconds per container is the kind of thing that turns a
rolling restart into an outage. **Open problem, owner runtime, low priority**:
confirm whether a reactor shutdown path removes connector containers serially.
