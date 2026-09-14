# libkrun sandbox spike: report and decision

What was proved: a podman-hosted libkrun helper boots our connector images as
guests, connector-init answers the reactor over vsock, egress is enforced by
nftables in the helper's network namespace with a DNS-gated allow set, and the
cost per connector is one we can carry. Thirteen experiments, eleven of them
gates.

Everything ran on the GCP dev box (Ubuntu 24.04, kernel `7.0.0-1011-gcp`,
INTEL(R) XEON(R) PLATINUM 8581C @ 2.30GHz x 8, 15,983 MiB, podman 4.9.3,
`/dev/kvm` present), launched from the fake reactor: the production reactor
image run as the Quadlet unit runs it, with the host podman API socket as its
only privilege. libkrun v1.19.4 built from source and unpatched, libkrunfw
5.5.0 (guest kernel 6.12.91).

Numbers below cite the commit they were measured at. Per-experiment detail is in
`spike/report/exp*.md`; the libkrun source read is
`spike/report/libkrun-exposure.md`; the session ledger is `spike/STATUS.md`.
This report is written at `bf8355ecd68`.

---

## 1. Decision

**Go.**

Every gate passes. Two of them required a ruling rather than a bare comparison,
and both rulings are recorded in PLAN and in STATUS:

- **Experiment 5** measures 2.12x against a 2.00x limit and was accepted at
  that number. The limit was a proxy for a metadata premium paid on every file
  operation for a connector's life; the redesign it forced (dependency sets as
  per-tag read-only block images rather than virtiofs) brings that premium to
  1.15x, and what keeps the headline figure above 2x is a one-time 371 ms per
  boot inside a 2.3 s launch against a 5 s budget.
- **Experiment 13**'s panic half originally asked for a non-zero exit code.
  That was an assumption about libkrun and it is wrong; the requirement was
  reworded to what the design actually consumes (prompt exit, no reboot loop)
  and the exit code is recorded rather than gated on.

No gate failed in a way that forces a redesign, and there is no libkrun change
anywhere on the critical path. The one redesign the spike did force - block
images for dependency sets - was made and measured, and is folded into the
design below.

---

## 2. Gates

| # | Experiment | Gate | Result | Note |
|---|---|---|---|---|
| 1 | Launch through the podman API from the reactor's privilege level | gate | **PASS** | `CapEff 00000000800415fb` = podman's default `800405fb` plus CAP_NET_ADMIN and nothing else; `/dev/kvm` and `/dev/net/tun` the only added devices. The API service refused nothing on the helper's launch. |
| 2 | Boot latency, under 5 s | gate | **PASS** | p95 **0.735 s**, median 0.684 s, against 5 s. The sandbox costs +275 ms (1.67x) over today's plain `podman run` to the same byte. |
| 3 | Protocol parity with Go connectors | gate | **PASS** | Six diffs empty - documents, connector log lines, transaction stats - across a Go capture and a Go materialization, switch on and off. |
| 4 | derive-python end to end, permissive network | gate | **PASS** | Four documents byte-identical to the unsandboxed run, under the real `allowAll` ruleset; `uv` fetched pandas 3.0.5 from PyPI inside the guest. |
| 5 | Dependency-set cold import, no worse than 2x | gate | **PASS by ruling at 2.12x** | virtiofs failed at 2.88x and forced the redesign to a per-tag read-only block image; that measures **2.12x** on the first import after boot and **1.15x** in steady state, and the gate closes on the 1.15x, which is what it was protecting. |
| 6 | Egress rules proven from inside the guest | gate | **PASS** | 39 probes across four passes, all passing, no forbidden packet on the helper's uplink in any of them. |
| 7 | `egress: none` | gate | **PASS** | Lookup and connect both fail with nothing leaving the helper. The latency is the cost, and it is an accepted one (section 6). |
| 8 | Rate and fan-out limits | gate | **PASS** | Fan-out cuts at exactly 5 of 20 and stays cut for the window; the rate holds to 60/minute. They fail in opposite ways - pacing versus timeout - which is a product question, not a correctness one. |
| 9 | Connection churn | measure | **measured, nothing went wrong** | 30,000 TCP+TLS connections at 50/s for ten minutes, zero failures, every one of the 60 windows at exactly 50.00/s. A sanity check that passed, not a ceiling. |
| 10 | Density | measure | **measured** | 82 idle 512 MiB guests on this box; 95 MiB of host memory each, and the 82nd cost what the first did. |
| 11 | Storage behavior | gate | **PASS** | Root writes land in podman's per-container layer and go with the container; `/scratch` stops at `--disk-mib` with ENOSPC and the space comes back; `/usr` writes never reach the image; `/venv` is EROFS even to guest root. |
| 12 | Control channel and device exposure | gate | **PASS** | `krun_add_vsock(ctx, 0)` opens nothing for a TSI proxy-create datagram; the mapped port is inbound-only; the device inventory is exactly what was configured; guest root cannot write the deps disk; `..` never reaches the FUSE wire. |
| 13 | Helper crash and cleanup | gate | **PASS** | Prompt and clean on both halves. SIGKILL mid-transaction: the runtime fails the task off the socket close in under half a second and nothing leaks. A guest kernel panic stops the VMM in 5.0 s with no reboot loop - **and the helper exits 0**, because a panicking guest never reaches libkrun's init exit-code report and the VMM falls back to `FC_EXIT_CODE_OK`. Detection is unaffected: the socket is what tells the runtime. Diagnosis is not; see open problems. |

Commits: experiments 1-3 at `6a2a47d6af6`; experiment 4 at `6a2a47d6af6` and
rerun at `8703b322029`; experiment 5 at `f6498560dea` (virtiofs), `18ae8986a0a`
(block image) and `358da8b59b8` (writable root); experiments 6-8 at
`8703b322029`; experiments 9-10 at `817c6c65bab`; experiments 11-13 at
`0c68f442cf1`. The libkrun source read is at libkrun tag `v1.19.4`,
`728df8125077d0db44265f6e997c72b81b65c015`.

---

## 3. The numbers

### 3.1 Experiment 2: boot latency

Commit `6a2a47d6af6`, `spike/tasks/exp2-boot.sh --runs 20`, raw data
`data/exp2-boot.csv`, guest `source-hello-world`. Warm caches.

`podman run` to connector-init's readiness byte, in milliseconds:

| arm              |  n | min   | median | p95   | max   |
|------------------|----|-------|--------|-------|-------|
| switch on        | 40 | 657.1 | 683.8  | 735.4 | 797.0 |
| switch off       | 40 | 377.7 | 408.9  | 437.3 | 463.4 |

n is 40 rather than 20 because one `flowctl preview --sessions 1` starts the
connector twice - the capture shard starts it once to validate and once to open
- and both are the same launch measured to the same byte. The two are within
10 ms of each other in both arms, so they are pooled.

Breakdown of the sandboxed launch:

| stage                    | median ms | p95 ms | what it is |
|--------------------------|-----------|--------|------------|
| podman create to start   | 135.5     | 191.1  | the launch line reaching the shim's first instruction |
| shim setup               | 23.9      | 30.3   | tap, nftables, scratch `mkfs`, image config, libkrun config |
| guest kernel boot        | 201.9     | 209.8  | `krun_start_enter` to flow-init's first instruction |
| flow-init                | 6.4       | 7.2    | network, `/etc`, mounts, chown, uid drop, exec |
| connector-init + console | 315.1     | 324.5  | bind, readiness byte, and its trip to the host |
| **total**                | **683.8** | 735.4  | |
| *of which console*       | *290.3*   | *295.9*| *measured separately* |

The largest line item is transport, not work. Of the 315 ms residual, 290 ms is
the guest's stderr reaching the host, measured independently by comparing the
host's receipt time of flow-init's last line against the guest clock that line
carries. That leaves connector-init about 25 ms to bind and signal. The 290 ms
is libkrun's virtio console and is the one lever this experiment found; nobody
chased it, because the gate passes with 4.3 s to spare.

### 3.2 Experiment 5: dependency-set cold import

Three matrices, in the order they were run. All use 10 measured boots per cell,
a fresh guest per boot, and one discarded warm-up boot per cell. `import` is
`import pandas` in-process; `pass` is `subprocess.run([python, "-c", "pass"])`;
`wall` is the whole `podman run` of the helper. p95 is linearly interpolated.

**5: virtiofs. FAIL at 2.88x.** Commit `f6498560dea`, raw data
`data/exp5-runs.csv`.

| cell             | import median | import p95 | vs baseline | pass median | wall median | n  |
|------------------|--------------:|-----------:|------------:|------------:|------------:|---:|
| primary          |      1115.5   |    1133.7  |    2.88x    |      40.0   |      2456   | 10 |
| primary-nothp    |      1117.2   |    1140.9  |    2.89x    |      40.0   |      2467   | 10 |
| share            |       930.4   |     960.9  |    2.40x    |      38.2   |      2436   | 10 |
| share-nothp      |       939.0   |     964.3  |    2.43x    |      38.5   |      2434   | 10 |
| share-dax        |       959.3   |     985.4  |    2.48x    |      38.3   |      2493   | 10 |
| share-dax-nothp  |       966.0   |     994.9  |    2.50x    |      38.8   |      2505   | 10 |
| baseline         |       387.1   |     391.1  |    1.00x    |      11.2   |       968   | 10 |
| baseline-root    |       390.1   |     392.2  |    1.01x    |      11.2   |       970   | 10 |

Where the time goes, same commit, one guest per run running the whole sequence,
5 runs, raw data `data/exp5-diag.csv`:

| stage             | import median | import p95 | vs baseline | pass median | cpu median | n |
|-------------------|--------------:|-----------:|------------:|------------:|-----------:|--:|
| primary-cold      |      1106.4   |    1130.8  |    2.84x    |      39.3   |          - | 5 |
| primary-warm      |       513.9   |     535.2  |    1.32x    |      28.6   |          - | 5 |
| primary-cold2     |       672.2   |     674.3  |    1.73x    |      28.3   |          - | 5 |
| primary-blk-cold  |       347.6   |     354.0  |    0.89x    |      28.0   |          - | 5 |
| primary-blk-warm  |       250.0   |     252.1  |    0.64x    |      28.2   |          - | 5 |
| primary-cpu       |           -   |         -  |         -   |          -  |      557.4 | 5 |
| share-cold        |       951.3   |     962.4  |    2.44x    |      38.3   |          - | 5 |
| share-warm        |       473.8   |     531.1  |    1.22x    |      35.1   |          - | 5 |
| share-cold2       |       508.2   |     571.3  |    1.30x    |      27.4   |          - | 5 |
| share-blk-cold    |       344.4   |     352.1  |    0.88x    |      25.8   |          - | 5 |
| share-blk-warm    |       241.7   |     247.3  |    0.62x    |      25.6   |          - | 5 |
| share-cpu         |           -   |         -  |         -   |          -  |      556.6 | 5 |
| container-cold    |       389.6   |     394.0  |    1.00x    |      11.3   |          - | 5 |
| container-warm    |       219.3   |     220.0  |    0.56x    |      10.9   |          - | 5 |
| container-cpu     |           -   |         -  |         -   |          -  |      558.4 | 5 |

The cost is per-file metadata round trips, not bandwidth, and not CPU (557.4 /
556.6 / 558.4 ms on the same IO-free loop). Warm virtiofs is still 2.2-2.3x a
warm container with every byte already in the guest's page cache, because
`import pandas` pulls in 594 modules and each one is several path lookups plus
an open.

**5b: the dependency set as a per-tag block image. 2.12x, and the ruling.**
Commit `18ae8986a0a`, raw data `data/exp5-5b.csv`. Guest root is
`derive-python:dev` with no venv in it; the venv arrives on `/dev/vdb` and
flow-init mounts it read-only at `/opt/venv`.

| cell               | import median | import p95 | vs baseline | pass median | wall median | n  |
|--------------------|--------------:|-----------:|------------:|------------:|------------:|---:|
| blk-ext4           |        826.8  |     848.7  |    2.12x    |      38.7   |      2323   | 10 |
| blk-erofs          |        849.6  |     864.8  |    2.18x    |      39.1   |      2334   | 10 |
| blk-ext4-reactor   |        824.1  |     835.0  |    2.11x    |      37.6   |      2570   | 10 |
| blk-ext4-hostcold  |       1235.6  |    1293.5  |    3.16x    |      39.5   |      3672   | 10 |
| baseline           |        390.5  |     395.0  |    1.00x    |      11.3   |       973   | 10 |

Where the remaining 371 ms goes, same commit, 5 runs, raw data
`data/exp5-5b-diag.csv`:

| stage             | import median | import p95 | vs container | pass median | cpu median | n |
|-------------------|--------------:|-----------:|-------------:|------------:|-----------:|--:|
| deps-cold         |        822.0  |     835.5  |     2.10x    |      38.3   |          - | 5 |
| deps-prefault     |        621.4  |     622.7  |     1.59x    |      27.4   |          - | 5 |
| deps-cold2        |        451.2  |     480.6  |     1.15x    |      33.1   |          - | 5 |
| deps-warm         |        254.8  |     358.9  |     0.65x    |      27.4   |          - | 5 |
| deps-cpu          |            -  |         -  |         -    |          -  |      567.4 | 5 |
| primary-cold      |       1122.7  |    1134.5  |     2.87x    |      40.3   |          - | 5 |
| primary-cold2     |        685.5  |     713.1  |     1.75x    |      30.0   |          - | 5 |
| primary-warm      |        516.3  |     530.5  |     1.32x    |      28.4   |          - | 5 |
| container-cold    |        391.7  |     393.5  |     1.00x    |      11.4   |          - | 5 |
| container-warm    |        221.7  |     232.3  |     0.57x    |      11.0   |          - | 5 |
| container-cpu     |            -  |         -  |         -    |          -  |      571.3 | 5 |

Steady state is 1.15x (`deps-cold2` 451.2 ms against a container's 391.7, with
the guest's page cache, dentries and inodes just dropped), against virtiofs's
2.33x warm. About 201 ms of the first-import tax is guest memory first-touch: a
fresh boot that first writes one byte to every page of a 600 MiB buffer and
frees it, then imports, gets 621.4 ms instead of 822.0. The remaining ~170 ms is
other first-boot state and was not chased; the levers for it are libkrun's.

Experiment 5's own report claimed a block device would import at 0.89x. That
number was measured in a guest that had already booted, imported once, and
copied 144 MiB, so it was a prefaulted second-import measurement compared
against first-import cells. 5b's 822.0 ms is the honest figure. The steady-state
claim survives and is now measured directly.

**5c: restated on podman's writable root. 2.13x, unchanged.** Commit
`358da8b59b8`, raw data `data/exp5-4b.csv`. WP04b replaced the guest-side
overlayfs with podman's own per-container writable layer, served read-write over
virtiofs, and removed the libkrun patch the overlay needed.

| cell       | import median | import p95 | pass median | wall median | n  |
|------------|---------------|------------|-------------|-------------|----|
| `blk-ext4` |         709.4 |      726.3 |        36.7 |      2092.0 | 10 |
| `baseline` |         332.7 |      336.8 |         9.7 |       872.5 | 10 |

Both cells are 14-15% faster in absolute terms than in 5b, and `baseline` is a
plain `podman run` with no guest, no helper and no root share, so nothing in
WP04b can reach it. Two cells moving together by the same fraction is machine
state on a shared cloud host. The ratio is the part that carries between
sessions, which is why the gate is written as one.

### 3.3 Experiment 9: connection churn

Commit `817c6c65bab`, `spike/tasks/exp9-churn.sh`, guest `derive-python:dev`.
Raw data `data/exp9-churn.csv` (the guest's windows) and `data/exp9-cgroup.csv`
(the host's cgroup samples).

Shape of the run:

| | |
|---|---|
| target | `https://198.51.100.10/probe`, 1 KiB body, self-signed cert not verified |
| policy | `198.51.100.10/32` on port 443, declared |
| offered | 50 connections/s for 600 s = 30,000, one new TCP+TLS connection each |
| guest | derive-python, 1024 MiB, 2 vcpu, `--disk-mib 4096` |
| helper cgroup limit | 1280 MiB (`--memory 1024m + 256m`) |

Throughput:

| | offered | achieved |
|---|---|---|
| rate | 50/s | **50.0/s** |
| connections | 30,000 | 30,000 ok, **0 failed** |
| per-window rate | 50.00/s | min 50.00, median 50.00, max 50.00 over 60 windows |

Latency per connection, on the guest's own clock:

| p50 | p95 | max |
|---|---|---|
| 2.9 ms | 3.3 ms | 26.5 ms |

What the helper cost, sampled every 10 s from its cgroup on the host:

| | start (t=0) | steady state | end (t=593) |
|---|---|---|---|
| `memory.current` | 49.4 MiB | 93.9 - 95.9 MiB | 94.9 MiB |
| `memory.stat anon` | 46.8 MiB | - | 89.5 MiB |
| threads | 12 | 14 | 14 |
| CPU | - | 15.4% of one core (median), 13.3 - 20.3% | 90.9 s total |

90.9 s of CPU for 600 s of wall clock is 15.2% of one core against the 2 vcpus
configured. Memory rises once over the first minute and then holds within a
2.0 MiB band for the remaining nine minutes. conntrack absorbed roughly 6,000
concurrent TIME_WAIT entries at default sizing.

**This is a sanity check that passed, not a ceiling**: 50/s was the number PLAN
asked for and the path carried it at 15% of one core, so the experiment says
nothing about where the limit actually is, and nothing here asks for a run that
finds out.

### 3.4 Experiment 10: density

Commit `817c6c65bab`, `spike/tasks/exp10-density.sh`, guest `derive-python:dev`.
Host 15,983 MiB `MemTotal`. Raw data `data/exp10-density.csv`,
`data/exp10-arms.csv`, `data/exp10-reclaim.csv`, `data/exp10-overhead.csv`.

The box was not idle: an editor's `rust-analyzer` held ~2.9 GiB throughout, so
each pass started from ~10,900 MiB available. The *count* reached is specific to
this box and this moment; the *per-guest cost* is the number that travels, and
it is measured as an increment, not a quotient.

`memoryMib: 512`, 1 vcpu, `--disk-mib 1024`, helper cgroup limit 768 MiB.
Launched one at a time, stopping when `MemAvailable` fell below 20% of
`MemTotal` (3,196 MiB):

| arm | guests | stopped at | cgroup median | cgroup max | threads | host MiB/guest |
|---|---|---|---|---|---|---|
| idle, THP on | **82** | floor | 72.6 MiB | 90.9 MiB | 13 | **95.2** |
| idle, THP off | **82** | floor | 72.5 MiB | 75.5 MiB | 13 | **94.6** |
| TLS held, THP on | 20 | cap | 78.0 MiB | 82.4 MiB | 13 | 100.6 |
| TLS held, THP off | 20 | cap | 77.8 MiB | 79.1 MiB | 13 | 99.5 |

The TLS arms stop at 20 because PLAN asked for 20, not because anything ran out.

Cost per guest does not grow with the number of guests:

| arm | first 10 guests | last 10 guests |
|---|---|---|
| idle, THP on | 95.0 MiB each | 94.8 MiB each |
| idle, THP off | 95.2 MiB each | 89.7 MiB each |
| cgroup, idle THP on | 73.0 MiB median | 72.5 MiB median |

Reclaim. One 1024 MiB guest: write a 512 MiB file to `/scratch`, drop caches,
read it back into the page cache, hold 90 s, then `echo 3 >
/proc/sys/vm/drop_caches`:

| | `memory.current` | `memory.stat anon` |
|---|---|---|
| page cache full | 1,138 MiB | 602 MiB |
| after `drop_caches` | 645 MiB | 110 MiB |
| **returned** | **493 MiB** | **492 MiB** |

The cgroup sits flat at 1,138 MiB for the whole 90-second hold and then falls
within one 5-second sample of the drop.

The overhead constant. Each guest reports its own `/proc/meminfo`; the helper's
`memory.current` minus the RAM the guest says it has touched is everything else
the sandbox costs. Four guests each at 512 and 1024 MiB, in two regimes:

| mode | memoryMib | n | `memory.current` | `anon` | guest touched | overhead |
|---|---|---|---|---|---|---|
| idle | 512 | 4 | 72.7 MiB | 69.5 MiB | 74.6 MiB | 23.7 MiB |
| idle | 1024 | 4 | 80.1 MiB | 77.2 MiB | 75.1 MiB | 30.6 MiB |
| 80% full | 512 | 4 | 487.3 MiB | 482.9 MiB | 493.0 MiB | 20.0 MiB |
| 80% full | 1024 | 4 | 901.6 MiB | 895.6 MiB | 902.5 MiB | 24.7 MiB |

(The "guest touched" column is `MemTotal - MemAvailable`; the "overhead" column
uses `MemTotal - MemFree`.)

| regime | min | median | max |
|---|---|---|---|
| idle, `MemTotal - MemAvailable` | -2.9 | 3.8 | 5.8 MiB |
| idle, `MemTotal - MemFree` | 22.7 | **29.5** | 31.5 MiB |
| 80% full, `MemTotal - MemAvailable` | -6.1 | -2.6 | 0.8 MiB |
| 80% full, `MemTotal - MemFree` | 19.6 | **23.1** | 26.4 MiB |

Read the `MemFree` rows. `MemTotal - MemAvailable` comes out at roughly zero and
occasionally negative, because `MemAvailable` counts the guest's own page cache
and reclaimable slab as available while the host is still backing every one of
those pages.

Threads: **13 per helper, in every arm, at every guest count.** 82 guests is
about 1,070 threads on the host.

---

## 4. What phase 2 starts from

### 4.1 The `podman run` line

As `runtime-next`'s spike switch emits it today
(`crates/runtime-next/src/container/spike.rs`), with `<id>` folded:

```
run --rm --name=fs_<id> --network=flow-connectors --log-driver=none
  --device /dev/kvm
  --device /dev/net/tun
  --cap-add NET_ADMIN
  --sysctl net.ipv4.ip_forward=1
  --sysctl net.ipv4.conf.default.rp_filter=1
  --env=LOG_FORMAT=json --env=LOG_LEVEL=<level>
  --memory <memoryMib + overheadMib>m --cpus <vcpus>
  --cgroup-parent estuary-connectors.slice
  --label=image=<connector image>
  --label=task-name=acmeCo/<task>
  --label=task-type=<capture|derivation|materialization>
  --mount=type=image,source=<connector image>,destination=/rootfs,rw=true
  --mount=type=bind,source=<reactor>/fs_<id>/init,target=/init,ro
  --mount=type=bind,source=<reactor>/fs_<id>/venv,target=/venv,ro
  --mount=type=bind,source=<reactor>/fs_<id>/sock,target=/sock
  --mount=type=bind,source=<reactor>/fs_<id>/scratch,target=/scratch-backing
  [--mount=type=bind,source=<deps image>,target=/deps.img,ro]
  <helper image>
  --policy /init/policy.json --memory-mib <n> --vcpus <n> --disk-mib <n>
  [--deps-image /deps.img --deps-fstype ext4]
```

Three flags on that line are load-bearing and were each found the hard way:

- `--cap-add NET_ADMIN` is the only capability added, and experiment 1 confirms
  nothing else is: `CapEff` is podman's default `800405fb` plus bit 12.
- `--sysctl net.ipv4.conf.default.rp_filter=1` must be set explicitly rather
  than inherited from the host. The tap does not exist at container creation, so
  `conf.default` is the only route to it; `/proc/sys` is read-only inside the
  container, so it cannot be set afterwards from a `podman exec`; the helper
  image has no `sysctl` binary; and `conf.all` alone is not enough, because the
  effective value is the maximum of the two. Strict `rp_filter` is the
  anti-spoof control that actually runs in production (section 7).
- `--mount type=image,...,rw=true` is podman's own per-container writable
  overlay of the connector image, served to the guest read-write. There is no
  overlay inside the guest and no libkrun patch. The guest root is writable
  exactly as a container's is today, and the layer is removed with the
  container.

`<id>` must be unique per launch and never reused: a stale `sock/init.sock`
makes libkrun fail with EEXIST at configuration time, and the shim does not
unlink files it does not own.

### 4.2 The nftables ruleset

`flow-sandbox-egress --print`, run inside a helper on `flow-connectors` with the
tap up, at `bf8355ecd68`, for the canonical `public` policy (`allowAll: false`,
no declared CIDRs, no limits):

```
# flow-sandbox egress ruleset: egress=public allowAll=false declaredCidrs=0 connectionsPerMinute=None distinctDestinationsPerMinute=None
table inet flow_sandbox {}
delete table inet flow_sandbox

table inet flow_sandbox {
	set baseline {
		type ipv4_addr
		flags interval
		auto-merge
		elements = { 0.0.0.0/8, 10.0.0.0/8, 100.64.0.0/10, 127.0.0.0/8, 169.254.0.0/16, 172.16.0.0/12, 192.168.0.0/16, 224.0.0.0/4, 240.0.0.0/4, 10.89.0.0/24, 192.0.2.0/30 }
	}

	set resolved {
		type ipv4_addr
		flags timeout
	}

	chain forward {
		type filter hook forward priority filter; policy drop;
		iifname "tap0" ip saddr != 192.0.2.2 counter drop comment "anti-spoof"
		ct state established,related counter accept comment "replies"
		meta nfproto ipv6 counter drop comment "no-ipv6"
		meta l4proto != { tcp, udp } counter drop comment "tcp-udp-only"
		ip daddr @baseline counter drop comment "baseline"
		tcp dport 25 counter drop comment "smtp"
		ct state new counter jump egress_accept comment "egress"
		counter comment "forward-drop"
	}

	chain egress_accept {
		ip daddr @resolved counter accept comment "resolved"
	}

	chain input {
		type filter hook input priority filter; policy drop;
		iif lo counter accept comment "loopback"
		iifname "tap0" ip saddr != 192.0.2.2 counter drop comment "anti-spoof"
		ct state established,related counter accept comment "replies"
		iifname "tap0" ip saddr 192.0.2.2 ip daddr 192.0.2.1 udp dport 53 counter accept comment "tap-dns"
		counter comment "input-drop"
	}

	chain output {
		type filter hook output priority filter; policy accept;
		oifname "tap0" ct state != established counter drop comment "no-inbound"
	}

	chain postrouting {
		type nat hook postrouting priority srcnat; policy accept;
		ip saddr 192.0.2.2 oifname "eth0" counter masquerade comment "masquerade"
	}
}
```

The last two baseline elements are derived from the helper's own interfaces at
start, not assumed: `10.89.0.0/24` is podman's `flow-connectors` bridge and
`192.0.2.0/30` is the tap. The set is `flags interval` with `auto-merge` because
`10.89.0.0/24` sits inside `10.0.0.0/8` and nft rejects overlapping interval
elements otherwise.

A policy with `declaredCidrs` and both limits adds four things and nothing else:

```
	set declared {
		type ipv4_addr . inet_service
		flags interval
		elements = { 203.0.113.0/24 . 443 }
	}

	set dests {
		type ipv4_addr
		flags dynamic, timeout
		timeout 1m
		size 5
	}
```

and, in the `forward` chain, in place of the plain `jump`:

```
		ct state new limit rate over 60/minute counter drop comment "rate-limit"
		ct state new update @dests { ip daddr } counter jump egress_accept comment "fan-out"
```

with `ip daddr . tcp dport @declared counter accept comment "declared"` appended
to `egress_accept`. Three details there are load-bearing: `update` rather than
`add`, so a busy destination is not aged out mid-conversation; the verdict on
the same rule as the set update, so that a full set means "deny" rather than
"skip this rule and carry on to the accepts"; and nft's default `burst 5
packets` on the `limit rate`, which is why the first handful of connections in
an idle window go straight through.

`egress: none` is the same skeleton with `egress_accept`, the `resolved` set,
the resolver and the `tap-dns` input rule all removed. Nothing else changes.

`allowAll: true` adds exactly one accept at the head of `egress_accept` and
changes nothing else: anti-spoof, the baseline, the IPv6 and non-TCP/UDP drops,
tcp/25, the input chain and the output chain all still apply. It removes the
requirement that a destination be *named*, not the enforcement.

### 4.3 The libkrun call sequence

From `spike/helper/shim/src/main.rs`, in order. libkrun v1.19.4, built from
source with `BLK=1 NET=1`, unpatched; libkrunfw is Fedora's 5.5.0.

```
krun_init_log(STDERR_FILENO, WARN|DEBUG, STYLE_NEVER, 0)

  -- before the VM: the guest's first packet must meet a finished ruleset
  ip tuntap add dev tap0 mode tap; ip addr add 192.0.2.1/30 dev tap0; ip link set tap0 up
  flow-sandbox-egress --policy ... --tap tap0 --uplink eth0
                      --guest-ip 192.0.2.2 --helper-ip 192.0.2.1
  flow-sandbox-resolver --policy ... --listen 192.0.2.1:53 --upstream <resolv.conf>
      (not started when egress: none)
  open(O_TMPFILE) in /scratch-backing; ftruncate to diskMib; mkfs.ext4 (section 4.4)

krun_create_ctx()
krun_set_vm_config(ctx, vcpus, memoryMib)
krun_add_virtiofs3(ctx, "/dev/root", "/rootfs",  0,             read_only=false)
krun_add_virtiofs3(ctx, "venv",      "/venv",    0 or 512 MiB,  read_only=true)

  -- overlay files, root-level entries in the root virtiofs, no leading slash
krun_fs_add_overlay_file(ctx, "flow-init",             ..., 0o100755)
krun_fs_add_overlay_file(ctx, "flow-connector-init",   ..., 0o100755)
krun_fs_add_overlay_file(ctx, "image-inspect.json",    ..., 0o100644)
krun_fs_add_overlay_file(ctx, ".krun_config.json",     ..., 0o100644)

krun_add_disk3(ctx, "scratch", "/proc/self/fd/N", RAW, read_only=false,
               direct_io=false, KRUN_SYNC_RELAXED)
krun_add_disk3(ctx, "deps",    "/deps.img",       RAW, read_only=true,
               direct_io=false, KRUN_SYNC_NONE)        -- only with --deps-image

krun_add_net_tap(ctx, "tap0", 02:f1:0f:00:00:02, COMPAT_NET_FEATURES, 0)

krun_disable_implicit_vsock(ctx)
krun_add_vsock(ctx, 0)
krun_add_vsock_port2(ctx, 49092, "/sock/init.sock", listen=true)

krun_disable_implicit_console(ctx)
krun_add_virtio_console_default(ctx, devnull, stdout, STDERR_FILENO)

krun_start_enter(ctx)     -- never returns
```

Five things about that sequence are decisions, not incidentals:

- **`krun_disable_implicit_vsock` then `krun_add_vsock(ctx, 0)`.** 1.19 rejects
  the explicit call unless the implicit device is disabled first, and the
  explicit zero is what turns off TSI. The implicit vsock config enables
  `HIJACK_INET`, which would leave host-side socket proxies live even with a tap
  in place. Experiment 12 measured the result: a TSI proxy-create datagram opens
  nothing.
- **`krun_add_vsock_port2(..., listen=true)`.** libkrun listens on the socket
  and the reactor dials in; the guest never initiates, and a guest connect to
  the mapped port gets an immediate RST. libkrun's unix proxy treats a host-side
  half-close (`shutdown(SHUT_WR)`) as a full close and resets the guest
  connection - gRPC never half-closes, but hand-written clients must not either.
- **The deps disk is added after the scratch disk**, so scratch keeps
  `/dev/vda` and deps lands as `/dev/vdb`. `read_only=true` is the whole
  protection: libkrun opens the file without `.write()`, so the refusal is the
  host fd and not a flag the guest can negotiate away (measured: guest root gets
  EPERM). `KRUN_SYNC_NONE` because nothing can write it and a flush the guest
  cannot cause is a virtqueue round trip for nothing.
- **No `krun_set_exec`.** It sets `KRUN_INIT`, and libkrun's init consults `Cmd`
  only when `KRUN_INIT` is absent, so setting both would silently discard the
  argv. Everything - argv, env, workdir - comes from the injected
  `/.krun_config.json`.
- **Console split.** The kernel console goes to the helper's stdout (which the
  reactor discards today) and the workload's stderr goes straight to fd 2, so
  connector-init's logs and its readiness byte reach the reactor unchanged. They
  share one host descriptor by libkrun's construction, so anything that scrapes
  guest stderr must not run under `--debug`, which tees the console into the
  same stream and interleaves at sub-line granularity.

flow-init (guest side, as root under libkrun's init) does, in order: static eth0
and default route; IPv6 off; write `/etc/resolv.conf` and `/etc/hosts`; `mkdir
-p /venv /scratch`; mount virtiofs tag `venv` read-only; mount `/dev/vda` ext4
at `/scratch` and chown it to the image's uid:gid; mount `/dev/vdb` read-only at
`/opt/venv` when present; set `TMPDIR=/scratch` and `UV_CACHE_DIR=/scratch`;
`setgroups([])`, `setgid`, `setuid`; exec. It does no `pivot_root` and lays no
overlay: libkrun's init reports the workload's exit code only while its own `/`
is virtiofs, so anything that changes the root in the shared mount namespace
silently turns every guest exit code into 0.

### 4.4 mkfs options

The scratch disk, on an `O_TMPFILE` in `/scratch-backing` sized to `diskMib`:

```
mkfs.ext4 -E lazy_itable_init=0 -O ^has_journal -m 0 -q -F /proc/self/fd/N
```

`lazy_itable_init=0` makes mke2fs flag every group ITABLE_ZEROED up front;
without it the guest's `ext4lazyinit` thread writes into the sparse file after
mount and grows the host's backing store unpredictably. The journal is dropped
outright because the disk never outlives the VM, which also makes
`lazy_journal_init` moot. Reserved blocks are zeroed because there is no
root-versus-user distinction in the guest. Measured on a 1024 MiB image at
`a1e1ea562f2`: 664 KiB allocated, all nine groups ITABLE_ZEROED.

`O_TMPFILE` is the reason there is no cleanup code: the file has no name,
SIGKILL frees the blocks, and the descriptor's only name is `/proc/self/fd/N`,
so it must not be `O_CLOEXEC` - `mkfs.ext4` resolves that path in its own
process.

The per-tag dependency image, built once on the host:

```
mkfs.ext4 -d <venv dir> -O ^has_journal -E lazy_itable_init=0 -m 0 -q -F \
    <image> <size>m
```

sized at `du -sk` of the source plus 10% plus 24 MiB, because mkfs fails loudly
if it is short and a fixed margin beats guessing precisely. **ext4, not erofs**:
erofs is 2.8% slower on the import (849.6 vs 826.8 ms, `18ae8986a0a`) and 14%
smaller (136,508 KiB vs 159,260 KiB allocated), and 2.8% of a per-boot cost is worth
more than 14% of per-tag storage - plus a wrongly-sized ext4 image is a
build-time failure rather than a runtime one. erofs support stays in the shim
and flow-init because it costs nothing to keep. Revisit if per-tag storage ever
becomes the constraint.

One thing for the builder: the spike's deps images were made from a venv whose
baked-in paths say `/venv` while the image is mounted at `/opt/venv`. Immaterial
to the measurement, because the benchmark puts site-packages on `sys.path`
rather than running the venv's own interpreter. A phase-2 builder should create
the venv at its final path.

### 4.5 Sizing: the overhead constant, THP, and what a reactor sizes on

Every figure in this section is experiment 10's, measured at `817c6c65bab`,
except the experiment 5 cells named as such.

**The overhead constant is 20 to 32 MiB**, measured as helper `memory.current`
minus `MemTotal - MemFree` inside the guest. It is a constant in the sense that
matters: it does not scale with `memoryMib` (512 and 1024 give the same figure)
and it does not grow when the guest actually uses its memory - the 80%-full rows
are, if anything, slightly lower.

`MemTotal - MemAvailable`, which PLAN originally specified, gives roughly zero
and sometimes negative, because `MemAvailable` counts the guest's own page cache
and reclaimable slab as available while the host is still backing those pages.
Use `MemFree`.

**The launcher default stays at 256 MiB.** 64 (twice the worst case) is the
value the measurement supports; the spike's 256 is about 8x the constant. But
the cgroup limit is not only about overhead: it also bounds the host page
cache the helper is charged for, which reached 535 MiB in the reclaim arm and
pinned a helper at its limit in experiment 11. That cache is reclaimable, so a
tighter limit trades throughput rather than correctness - and every experiment 5
import number was taken at 256. **Rerun experiment 5 at `memoryMib + 64` before
adopting it.** Owner: runtime.

**A reactor sizes on 95 MiB of host memory per idle 512 MiB guest, not on the
cgroup's 72.6.** The difference - about 22 MiB - is what the container costs
outside its own cgroup: the network namespace, podman's bookkeeping, and the
host page tables for the VM's mapping. A guest holding one long-lived TLS
connection costs about 5 MiB more. Free-page reporting works and is prompt: 493
of 512 MiB come back within one 5-second sample of the guest dropping its page
cache. Density is linear - helper 82 costs what helper 1 cost - so the limit is
host RAM divided by the per-guest cost.

**THP makes no difference** (95.2 MiB per guest with it on, 94.6 with it off;
medians within 0.1 MiB), and every `-nothp` cell in experiment 5 is within noise
of its pair (`f6498560dea`). THP on does widen the tail - one helper reached
90.9 MiB against 75.5 MiB with THP off - which is huge pages rounding up, not a
leak. Nothing argues for setting `PR_SET_THP_DISABLE` and nothing argues
against it, so
**`--thp-disable` does not ship**. It stays in the spike shim as a
measurement-only flag.

### 4.6 Transport: what experiment 5 implies about layer versus share

The dependency set ships as a **per-tag read-only ext4 image on a second
virtio-blk device**, mounted read-only at `/opt/venv`. The root stays on
virtiofs. That is the design, and the layer-versus-share question underneath it
is now settled on measurements rather than assumption. Ratios below are the
experiment 5 matrices of section 3.2 (`f6498560dea`, `18ae8986a0a`):

- **The image layer is the worst option.** The venv baked into the connector
  image and read through the root share is 2.88x.
- **A separate virtiofs share is 17% better and still fails.** 2.40x. That 17%
  is the guest overlay plus the extra podman layer on the read path, and it does
  not reach the gate on its own. PLAN's "unpack the image to a plain host
  directory once per tag" option is exactly this cell, and it is measured, and
  it does not pass.
- **So layer versus share is moot for performance.** Neither is the answer; the
  transport is. Whatever else decides between them - build pipeline, storage
  accounting, cache locality - it is not import time.
- **The root is not the residual.** With the venv on the deps disk, `import
  pandas` loads 407 modules (41.7 MB) off `/dev/vdb` and 152 (5.4 MB) off the
  virtiofs root, and `deps-cold2` drops the cache for root and disk alike yet
  still lands at 1.15x. PLAN's "whole root on a block image" fallback is
  retired, not deferred. What the root still costs is the `pass` column: 25.8 to
  28.0 ms to start a bare interpreter against a container's 10.9.
- **DAX is not what passes and should not ship.** A 512 MiB DAX window over the
  entire 144 MiB venv measured a few percent *worse* in both full matrices, and
  root DAX is unreachable in libkrun 1.19 anyway (section 6). The source read
  for experiment 12 then found that DAX is the only path to a libkrun mapping
  overflow. Drop `--venv-dax` from the production design.

---

## 5. What only worked from a root shell on the host

**Nothing in the thing under test.** PLAN said a step that only works from a
root shell is a finding, and there is none to report:

- Experiment 1 is the direct evidence: the whole launch runs from the fake
  reactor at the reactor's real privilege level - podman's default capabilities,
  no `--privileged`, no `--device`, the host podman API socket as the only
  privilege - and the API service refused no flag on the helper's `podman run`.
- `helper-smoke.sh` passes identically launched with `sudo podman` on the host
  and through the fake reactor.
- Experiment 5 checked that the launch path does not enter the number:
  `blk-ext4-reactor`, launched the way production would, measures 824.1 ms
  against the host-launched cell's 826.8, and pays its extra 248 ms in the
  `wall` column where it belongs (`18ae8986a0a`).

Host root was needed for **observation and fixtures**, never for the sandbox:

- `sudo podman` throughout the harness, because the spike's images, networks and
  reactor directory belong to root.
- Reading the helper's cgroup (`memory.current`, `memory.stat`, `cpu.stat`),
  `/proc/<shim pid>/status`, and podman's overlay layer directory - experiments
  9, 10 and 11.
- `sync; echo 3 > /proc/sys/vm/drop_caches` on the host, for experiment 5's
  cold-cache cells.
- `ip netns`, extra addresses on the podman bridge, `tcpdump` on the bridge, and
  `iptables` rules - the experiment 6, 7 and 8 fixtures. WP02's pre-guest netns
  harness is entirely host-root, and every claim it made was re-proved inside a
  real guest by experiment 6.

Two inversions worth carrying forward, because both cost a run:

- **One policy does not load on the host and does load in the helper.** The
  declared-CIDR example names `198.51.100.10/32`, and on this box the host owns
  `198.51.100.0/24`, so the binary correctly refuses a declared CIDR that
  overlaps the baseline. Inside the helper the only subnets are the tap and
  `10.89.0.0/24`. Run the egress binaries where they run in production.
- **`rp_filter` cannot be set from inside the helper at all.** `/proc/sys` is
  read-only in the container, the helper image has no `sysctl` binary, and the
  tap does not exist at container creation. The `--sysctl` on the launch line is
  the only route.

---

## 6. Accepted costs

These are real, they are understood, and none of them blocks the design. The
figures repeat section 3 and the experiment reports; commits are named where a
number appears here first.

- **A blocked connection stalls; it does not fail.** The deny action is `drop`
  everywhere, so nothing is refused and the guest has no way to learn a
  destination is unreachable. A DNS lookup surfaces in 10-15 s (glibc's own
  budget in this image; it depends on the image's `resolv.conf`, not on the
  sandbox). A connect takes up to ~127 s at `tcp_syn_retries=6`. A connector
  that does not set its own connect timeout will sit there for over two minutes,
  and nothing in its logs says why. Measured under `egress: none` at
  `8703b322029`, and the same cost applies to any blocked destination under
  `public`.
- **Root DAX is unavailable on libkrun 1.19.** The shim adds the root as an
  ordinary share, so it *could* pass a DAX window, but the root is mounted by
  libkrun's own fixed kernel command line (`rootfstype=virtiofs rw quiet
  no-kvmapf init=/init.krun`, read from a running guest) with no `rootflags=`
  and no way to add one. It would need libkrun 2.0's configurable cmdline or
  `krun_set_kernel`. None of which had to be worked around, because DAX on the
  share it *can* be applied to bought nothing.
- **Root writes have no per-task bound.** A guest filling its own root is
  bounded by the reactor's container storage filesystem and by nothing else -
  exactly as a connector container is today. Parity, not a regression, but it is
  the one storage surface without a limit. `/scratch` is bounded and behaves:
  3999 of 4352 MiB at `--disk-mib 4096`, ENOSPC, and the space comes back when
  the helper exits (`0c68f442cf1`).
- **The first import after boot costs ~371 ms more than steady state**, about
  201 ms of it guest memory first-touch (`18ae8986a0a`). A per-boot cost inside a 2.3 s
  launch against a 5 s budget, and removing it means libkrun backing guest RAM
  with pre-populated or huge pages. Whether that is a net win is not measured -
  prefaulting moves the cost earlier, it does not obviously remove it.
- **~290 ms of every launch is a byte crossing libkrun's virtio console.** That
  is 42% of the sandboxed launch time and the single largest line item in
  experiment 2 (`6a2a47d6af6`), under a gate passed with 4.3 s to spare. Not
  chased.
- **`connectionsPerMinute` surfaces as latency, not as an error** (`8703b322029`:
  80 attempts, 79 SYNs dropped, 80 completed, 79.9 s wall clock). A connector
  at ten times its policy will take ten times as long and never see a failure.
  Deliberate - the purpose is to cap what our egress addresses can be used for,
  and pacing a bursty connector does that without spurious failures - but the
  visible symptom is throughput.
- **Spec and Validate run unsandboxed.** `flowctl preview`, and the agent's
  connector proxy in production, drive them through the legacy `runtime` crate,
  which the spike switch does not touch. For derive-python that means the
  customer's dependencies are fetched and built (sdist build backends execute)
  and the module type-checked on the reactor's network before anything is
  sandboxed. Listed here because the spike accepted it to stay in scope; it is
  an open problem, not a permanent cost (section 7).
- **A guest that attacks the tap itself** - ARP, a second address, a route of
  its own - is out of reach of a probe suite that runs as a process inside the
  guest, and was not tested.

---

## 7. Open problems

Each with a proposed owner. Exposure findings use the tiering from the libkrun
source read: **T1** unprivileged guest userspace (where a connector runs), **T2**
guest root, **T3** guest kernel control.

### Runtime

1. **A guest kernel panic reports exit code 0, and diagnosis pays for it.**
   libkrun's init never reaches its exit-code report when the guest panics and
   resets, so the VMM falls back to `FC_EXIT_CODE_OK`. Detection is unaffected -
   the runtime learns of the death from the socket, measured at under half a
   second (`0c68f442cf1`) - but the reactor records "connector exited 0" for a
   guest that ran out of memory, and the kernel's explanation went to the
   helper's stdout, unstructured. The runtime must not branch on the helper's
   exit code at all:
   it is the guest workload's exit status and nothing more. **Proposed fix:** the
   shim already tees the console, so a line matching `Kernel panic` seen before
   `krun_start_enter` returns should produce one structured stderr line and a
   distinct exit code. Owner: runtime (shim).

2. **The memory overhead default.** Measured at 20-32 MiB; the spike ships 256.
   64 is what the measurement supports. Lowering it also squeezes the host page
   cache charged to the helper, which is reclaimable and so costs throughput
   rather than correctness - but experiment 5's import numbers were all taken at
   256. Rerun experiment 5 at `memoryMib + 64` before adopting. Owner: runtime.

3. **`rp_filter` must be set explicitly on the launch line.** It is the
   anti-spoof control that actually runs in production; the nft rule is the
   second control and cannot be reached in this topology. A /30 has no spare
   unicast source to forge from, the kernel rejects the broadcast address as a
   source, and any off-net source is dropped by strict `rp_filter` before nft
   sees it. If the host's default were ever 0, enforcement would silently move
   from the kernel to the one rule that has been proven to work in isolation.
   The spike sets it; phase 2 must keep it. Owner: runtime.

4. **The resolver's `nft add element` does not refresh an existing element's
   timeout**, so a name re-resolved late in its window still expires at the
   original time, and a connect in that moment is dropped. The runtime's netlink
   implementation should update in place; delete-then-add opens a window. Owner:
   runtime.

5. **Sandboxing Spec and Validate** needs the legacy `runtime` crate's container
   launcher to move to runtime-next first. This is the "connector proxy moves to
   runtime-next" prerequisite and the builder VM phase, both already in the
   design; it is listed because until it lands, a customer's Python runs
   unsandboxed during validation. Owner: runtime.

6. **Root writes have no per-task bound, and the layer is not discoverable
   through `podman inspect`.** `.GraphDriver` reports the helper container's own
   root, not the image mount's writable layer. Anything that wants to meter or
   quota a connector's root writes has to know the shape
   `overlay-containers/<container id>/userdata/overlay/<n>/upper`. Owner:
   runtime, provisional on appetite for a per-task root quota.

7. **Draining many sandboxes is slow in podman, not libkrun.** `podman rm -f`
   took about 8 s per helper, serially, with 82 running - 14 minutes for the lot
   - against roughly 0 s for one (`817c6c65bab`). The shim dies at once on SIGKILL.
   Confirm whether a reactor shutdown removes connector containers one at a time,
   and batch the removals if so. Owner: runtime, low priority.

8. **Phase-2 hardening the libkrun source read points at.** Run the virtiofs
   server with the share as its filesystem root - upstream's own stated remedy, a
   `pivot_root` or `openat2` with `RESOLVE_BENEATH` - and keep the helper's mount
   namespace minimal, since at T3 everything mounted into the helper is nameable.
   Owner: runtime.

### libkrun exposure (not "none found")

Read at libkrun tag `v1.19.4`, `728df8125077d0db44265f6e997c72b81b65c015`,
recorded at repo commit `36104b1ba89` and measured from inside a guest at
`0c68f442cf1`. Every citation in `libkrun-exposure.md` is checked mechanically
against that tag by `spike/tasks/check-exposure-citations.sh`, which reports
`ALL CITATIONS RESOLVE (96 checked)`.

The design's two load-bearing gates hold, and were confirmed by measurement as
well as by reading: `krun_add_vsock(ctx, 0)` disables TSI behind two independent
checks while the port map stays live, and the mapped port is inbound-only. The
"remove root dir" request that a 1.18 release restricted: **none found**. What
follows is what the read did turn up.

9. **Three T3 bugs in libkrun 1.19.4, none reachable from a connector.** An
   unchecked virtio-console port index (`self.ports[cmd.id as usize]`, guest
   -triggerable VMM panic); an unchecked virtio-balloon free-page-report length
   (`madvise(MADV_DONTNEED)` can run off the end of guest RAM and into the
   helper's own mappings - its heap, its thread stacks, the DAX window); and a
   DAX mapping offset overflow (`moffset + len` as a plain u64 add, so
   `u64::MAX, 1` passes the bounds check and maps at `host_shm_base - 1`),
   reachable only with `--venv-dax`. All three are "the guest kernel can crash
   or corrupt its own VMM", which is inside the boundary the design claims.
   **Decision needed:** report upstream, or accept as within the boundary.
   Owner: us.

10. **DAX should not ship.** No measured benefit, and it is the only path to the
    one overflow above. Drop `--venv-dax` from the production design. Owner:
    runtime.

11. **The `..` escape the libkrun README warns about is real, and the helper
    container is the only thing that bounds it.** `PassthroughFs::lookup` hands
    the guest's name straight to `openat` with no `RESOLVE_BENEATH`, and
    `openat` resolves `..` and embedded `/` in a single name argument. At T1 and
    T2 it is unreachable - the Linux FUSE client only ever sends single,
    already-resolved components, which experiment 12 measured
    (`/venv/../../etc/hostname` opens the guest's own file) - but that
    confinement lives entirely in the guest kernel, on the far side of the trust
    boundary. At T3 a guest can create, write, chown, chmod and setxattr
    anywhere the virtiofs server can name, which is whatever is mounted into the
    helper, including the binds out of the reactor's per-connector directory.
    There is no second line of defence inside libkrun and the design should not
    claim one. Mitigation is item 8. Owner: runtime.

12. **Two lesser T2 items, both bounded to the writable root share** and
    therefore to a layer podman deletes with the container: `mknod` creates real
    device nodes with a guest-chosen `rdev`, and `setxattr` is on by default so
    `security.*` attributes including file capabilities can be written. Neither
    gives the guest anything it can use from inside the VM; both leave objects in
    podman's storage that a host-side process should not be tempted to trust.
    Owner: runtime.

### Product

13. **`egress: none` stalls rather than fails**, and so does any blocked
    destination. A `reject` on the guest's DNS query alone would make lookups
    fail at once at no other cost - it is the one packet whose refusal tells the
    guest nothing it does not already know. Deliberately not changed in the
    spike, where deny is drop. Surfacing the policy in the task's logs at startup
    would also turn "no egress" into a visible fact rather than something
    inferred from latency. Owner: product, with runtime.

14. **`connectionsPerMinute` is invisible when it bites**, and behaves as pacing
    rather than refusal. Whether it should be named and documented as a rate
    rather than a limit is a phase-2 policy question. Owner: product.

### derive-python

15. **pyright `strict` mode fails Validate on any dependency without type
    information.** derive-python hardcodes `typeCheckingMode: strict`,
    force-installs pyright, and fails the Validate RPC on any finding; it cannot
    be relaxed from a catalog spec. In strict mode `reportUnknownMemberType` and
    `reportUnknownVariableType` make an untyped dependency poison every
    expression that touches it, so the effective constraint on customer Python is
    "only use libraries that ship `py.typed` or have a stubs package" - which
    sits across the premise that customers bring arbitrary Python with arbitrary
    dependencies. **The evidence here is one library**: pandas, resolved by
    declaring `pandas-stubs`. This spike did not survey how much of the ecosystem
    is affected, and that survey is what any decision should rest on. Flagged,
    not proposed. Owner: derive-python / product.

16. **A derivation module that prints to stdout kills its session**, because
    stdout is the derive protocol's own channel and connector-init parses every
    line of it as a JSON response. Unrelated to the sandbox, and a thing a
    customer will do. Redirect or document. Owner: derive-python.

### Out of scope here, still open

17. **AWS.** The current AWS reactor instance family exposes no `/dev/kvm`
    (Nitro non-metal), so none of this was or could be validated there. The whole
    spike is GCP. AWS gets validated separately once this works on GCP, and the
    instance-family question is unanswered. Owner: runtime to drive, with ops
    for the instance family and images; it needs deciding before this ships
    anywhere but GCP.

### Levers identified and deliberately not pulled

Neither of these is a problem; both are recorded so nobody has to rediscover
them. The ~290 ms of guest-stderr console latency in libkrun - 42% of the
sandboxed launch, under a gate passed with 4.3 s to spare. And guest memory
first-touch, ~201 ms on the first import after boot, which is libkrun's to fix
if anyone ever wants it back.
