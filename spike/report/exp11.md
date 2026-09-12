# Experiment 11: storage behavior

**PASS.** Everything the guest writes lands on host disk in a place that goes
away with the container, and nothing it writes is bounded by memory. A guest
with 1024 MiB of RAM wrote 1536 MiB to its root and 3999 MiB to `/scratch`; the
root write landed in podman's per-container layer, the scratch write stopped at
`--disk-mib` with `ENOSPC`, and the host's available memory never fell by more
than the helper's own cgroup limit. Writes under `/usr` never reach the image.
Writes under `/venv` fail even for guest root.

Measured at commit `0c68f442cf1` (this package's parent; the runs precede its
commit), `spike/tasks/exp11-storage.sh`, guest
`docker.io/library/busybox@sha256:1cfa4e2b09e127b9c4ed43578d3f3c18e7d44ea47b9ea98475c0cbe9086525f8`.
Raw data: `data/exp11-storage.csv`.

busybox rather than a connector image: every claim here is about what podman and
libkrun do with bytes, not about what a connector does with them, and busybox
boots in a fraction of the time with a shell, `dd`, and a `/usr` to write into.

## The claims

| claim | measured |
|---|---|
| root writes land in podman's per-container layer | 512 MiB written, 512 MiB in the layer |
| root writes are bounded by disk, not by guest memory | 1536 MiB written into a 1024 MiB guest, no failure |
| the layer goes with the container | layer directory absent after exit; 0 MiB net on the reactor filesystem |
| `/scratch` stops at `--disk-mib` | `dd: error writing '/scratch/fill': No space left on device` at 3999 of 4352 MiB |
| host memory does not back the disk | 3995 MiB of disk, 1022 MiB of `MemAvailable` |
| guest-root writes under `/usr` do not reach the image | `/usr/spike` absent from `podman image mount` |
| `/venv` is read-only to guest root | `touch /venv/x` -> EROFS |

## Where the bytes actually go

The guest's root is a `--mount type=image,...,rw=true`, and **podman does not
report that mount's writable layer in `podman inspect`**. `.GraphDriver` is the
*helper container's own* root (the helper image), and `.Mounts` names the image
mount only by image reference. The layer is here:

```
/var/lib/containers/storage/overlay-containers/<container id>/userdata/overlay/<n>/upper
```

and the helper's `/proc/mounts` confirms it: `/rootfs` is an `overlay` whose
source is the sibling `.../overlay/<n>/merge`. The script asserts that
correspondence on every fill pass, because every number below is a `du` of that
directory and a wrong directory would give a number that looks fine.

That directory is created by podman at container start and removed with the
container. After each pass the path does not exist and `df` on the reactor
filesystem is back where it started.

## The two root fills

| pass | written | layer after | cgroup `memory.current` | `anon` | `file` |
|---|---|---|---|---|---|
| root-fill | 512 MiB | 512 MiB | 1105 MiB | 573 MiB | 512 MiB |
| root-over-ram | 1536 MiB | 1536 MiB | 1279 MiB | 1033 MiB | 233 MiB |

The cgroup limit is 1280 MiB (`--memory 1024m + 256m`, as CONTRACTS' switch
sets it).

**The 512 MiB pass on its own does not make PLAN's claim.** PLAN says the write
is "bounded only by that disk" and not "in guest memory", and a 512 MiB write
fits inside a 1024 MiB guest either way. The second pass is the one that
decides: 1536 MiB is more than the whole guest's RAM and more than the helper's
whole cgroup, and it completes at 230 MB/s with the container never exceeding
its limit. What moves between the two passes is the split: at 512 MiB the page
cache (`file`) holds the whole write; at 1536 MiB the kernel reclaims it down to
233 MiB and hands the rest to disk. The guest's own RAM (`anon`, 1033 MiB) is
what it always was.

## `/scratch`, and the measurement that was wrong first

`--disk-mib 4096` here, which is CONTRACTS' production default rather than the
1024 the other harnesses use. That is not a detail:

| `--disk-mib` | written before ENOSPC | `df` growth | `MemAvailable` drop |
|---|---|---|---|
| 1024 | 989 MiB | 985 MiB | 977 MiB |
| 4096 | 3999 MiB | 3995 MiB | 1022 MiB |

At 1024 MiB the host's available memory falls by almost exactly what was
written, and the honest reading of that row is **not** "memory backed the
disk" - it is that a guest writing 1 GiB touches 1 GiB of its own RAM, which is
anonymous memory in the helper and genuinely unavailable. The two quantities are
the same size by coincidence. At 4096 MiB they separate: four times the disk,
the same memory, and the cgroup pinned at its 1280 MiB limit throughout. The
bound on host memory is the helper's cgroup; the bound on the write is the disk.

`MemAvailable` is sampled after a host `sync`, because dirty page cache counts
against it until it is written back and the guest's `dd` had just made a
gigabyte of it. Without the flush the figure reports the disk being behind, not
memory being consumed.

After the helper exits, `<id>/scratch/` is empty and all 3995 MiB are back: the
scratch disk is an `O_TMPFILE` and there is no cleanup code anywhere.

## The image, and `/venv`

Guest root wrote `/usr/spike` and the workload exited 0. `podman image mount` on
busybox afterwards does not have the file: the write went to the container's
layer, which is what "the guest root is writable exactly as a container's is"
means. `touch /venv/x` fails EROFS **as guest root**, which is stronger than the
experiment asked for - the read-only virtiofs wrapper does not care who is
asking.

## What this means for the design

- **Per-connector disk is real and is the reactor's to size.** A task can fill
  `--disk-mib` and will be told `ENOSPC`; it cannot spend host memory to do it.
  What the experiment does *not* bound is the root: a guest writing to its own
  root is bounded by the reactor's container storage filesystem and by nothing
  else, exactly as a connector is today. That is parity, not a regression, but
  it is the one storage surface with no per-task limit on it.
- **The layer path is not discoverable through `podman inspect`.** Anything in
  the runtime that wants to account for, or clean up after, a connector's root
  writes has to know the `overlay-containers/<id>/userdata/overlay/<n>/upper`
  shape. Nothing in the spike needs that; a future quota or metric would.
