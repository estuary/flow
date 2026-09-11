# Experiment 2: boot latency

**PASS.** p95 of the sandboxed launch is **0.735 s** against a 5 s limit, with a
median of 0.684 s. Today's plain `podman run` to the same byte is 0.409 s
median, so the sandbox costs **+275 ms, or 1.67x**, with 4.3 s of headroom left
against the gate.

Measured at commit `6a2a47d6af6` (WP06's parent; the runs precede the commit),
`spike/tasks/exp2-boot.sh --runs 20`, raw data
`spike/report/data/exp2-boot.csv`, guest
`ghcr.io/estuary/source-hello-world@sha256:96147403c20ca42faa2943b4d18d2bec8c4ff072892e7a09d240b43b674ad1db`.
Warm caches: every image, and the helper's, already resident.

## Totals

`podman run` to connector-init's readiness byte, in milliseconds.

| arm              |  n | min   | median | p95   | max   |
|------------------|----|-------|--------|-------|-------|
| switch on        | 40 | 657.1 | 683.8  | 735.4 | 797.0 |
| switch off       | 40 | 377.7 | 408.9  | 437.3 | 463.4 |

n is 40 rather than 20 because one `flowctl preview --sessions 1` starts the
connector twice - the capture shard starts it once to validate and once to open
- and both are the same launch measured to the same byte. The two are within
10 ms of each other in both arms, so they are pooled.

## Breakdown of the sandboxed launch

| stage                    | median ms | p95 ms | what it is |
|--------------------------|-----------|--------|------------|
| podman create to start   | 135.5     | 191.1  | the launch line reaching the shim's first instruction |
| shim setup               | 23.9      | 30.3   | tap, nftables, scratch `mkfs`, image config, libkrun config |
| guest kernel boot        | 201.9     | 209.8  | `krun_start_enter` to flow-init's first instruction |
| flow-init                | 6.4       | 7.2    | network, `/etc`, mounts, chown, uid drop, exec |
| connector-init + console | 315.1     | 324.5  | bind, readiness byte, and its trip to the host |
| **total**                | **683.8** | 735.4  | |
| *of which console*       | *290.3*   | *295.9*| *measured separately, see below* |

## The largest line item is transport, not work

The 315 ms residual is not connector-init binding a vsock listener. Of it,
**290 ms is the guest's stderr reaching the host**, measured independently: the
harness compares the host's receipt time of flow-init's last line against the
guest clock that line carries. Subtracting it leaves connector-init about
**25 ms** to bind and signal, which is the right order for the work.

So the sandboxed launch decomposes into ~136 ms of podman, ~24 ms of shim,
~202 ms of guest kernel, ~6 ms of flow-init, ~25 ms of connector-init, and
~290 ms of waiting for a byte to come out of the guest.

That last number is the one lever in this experiment, and it is libkrun's: the
readiness byte crosses the same virtio console path as every other guest stderr
write, and the reactor cannot act on the guest until it arrives. Nobody chased
the mechanism - batching, buffering, or a first-flush cost - because the gate
passes by 4.3 seconds and the finding is only worth acting on if some later
experiment makes launch latency scarce.

## Clocks

Three, deliberately:

- **T0 and T_ready** are the runtime's own host wall clock, logged either side
  of the launch in `spawn_and_await_ready`, which both arms share. Timing both
  arms to the same two events is what makes the delta meaningful, and it
  excludes the dial that follows readiness - which matters, because on the
  unmodified path that dial includes a `podman inspect` subprocess and on the
  sandboxed path it is a Unix connect.
- **The shim** stamps its start and its `krun_start_enter` on that same host
  clock, so podman's share and the shim's are host-measured.
- **flow-init** stamps `CLOCK_MONOTONIC`, the guest's time since boot. It
  shares a zero point with `/proc/uptime`, which the brief named, but not its
  resolution: uptime is reported in centiseconds and flow-init's whole run is
  6.4 ms, which rounds to nothing. Guest lines reach the host ~290 ms late, so
  their host-side arrival times are not usable for in-guest intervals and are
  not used.

## Notes

- The distributions are tight: p95/median is 1.08 sandboxed and 1.07 not.
  Nothing here is bimodal or occasionally pathological.
- `podman create to start` at 135.5 ms is the largest host-side cost and is a
  podman question, not a libkrun one. It is paid today as well, inside the
  409 ms baseline; what the sandbox adds on top is the guest.
- The guest kernel's 202 ms is libkrunfw 5.5.0's 6.12.91 with its stock
  configuration. PLAN's "time in the guest kernel means a slimmer libkrunfw"
  branch applies if this ever needs to come down.
