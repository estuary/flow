# Experiment 5: virtiofs cold import

**FAIL.** `import pandas` in the guest is **2.88x** the plain-podman baseline
(1115.5 ms vs 387.1 ms, medians, n=10), against a limit of 2.00x. Every variant
fails: the venv as a separate virtiofs share is 2.40x, DAX changes nothing, THP
changes nothing.

The failure is virtiofs, and only virtiofs. The guest's CPU is within noise of
the container's (557 ms vs 558 ms on an IO-free loop), and the *same venv*
copied inside the guest onto the ext4 scratch disk imports cold in **347.6 ms
- 0.89x of the container baseline measured in that same run, i.e. faster than
podman**. So the guest, its kernel and its page cache are all fine at this
workload: the entire gap is the transport under site-packages, and a block
device closes it.

Measured at commit `f6498560dea0a87ed98ebd24be7293d514e79fc4`.

## The matrix

10 measured boots per cell, each a fresh guest (so the guest page cache is cold
by construction), preceded by one discarded warm-up boot per cell so the host
page cache is in the same state the baseline is measured in. `import` is
`import pandas` in-process; `pass` is `subprocess.run([python, "-c", "pass"])`;
`wall` is the whole `podman run` of the helper, for context only. p95 is
linearly interpolated between order statistics, not nearest-rank, which at
n=10 would just report the slowest boot.

Raw runs: `data/exp5-runs.csv`. Reproduce with `spike/tasks/exp5-build.sh &&
spike/tasks/exp5-run.sh`.

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

Cells, all with `--memory-mib 1024 --vcpus 2 --disk-mib 1024 --upper-mib 256
--run-as-root`:

- `primary` - `localhost/derive-python-pandas:spike` as the guest root, venv
  imported from `/opt/venv`, i.e. through the guest overlay over the read-only
  virtiofs root. This is the shape the design assumes.
- `share` - `ghcr.io/estuary/derive-python:dev` as the guest root and the
  identical venv as the `venv` virtiofs share, imported from `/venv`. No image
  layer and no guest overlay on the read path.
- `share-dax` - the same with `--venv-dax` (512 MiB DAX window). Confirmed
  applied: the guest shows `venv /venv virtiofs ro,relatime,dax=always`.
- `-nothp` - the same with `--thp-disable`.
- `baseline` - `podman run --rm` of the derived image, fresh container per run,
  bench.py bind-mounted. `baseline-root` adds `--user 0`, and exists only
  because the guest cells run as root and the plain baseline runs as the
  image's `nobody`: the 1% between them says that difference is not in play.

The guests are launched with `sudo podman` on the host. The launch path does
not enter the number, which is a `perf_counter` delta inside the guest:
`exp5-run.sh --reactor --runs 2 --cells share`, launching through
`fake-reactor.sh` the way production would, imports in 951.1 and 985.5 ms
against the host-launched cell's 930.4 median / 960.9 p95, and pays its extra
~290 ms in the `wall` column (2728 vs 2436) where it belongs.

A first full matrix at this same commit, run before `cpu.py` joined `bench.py`
in the shares, agreed within 2% on every cell (primary 1115.7,
share 952.0, share-dax 962.5, baseline 389.9), which is roughly the spread
inside a single cell. The tables here are the later run, the one the committed
CSVs hold.

Every cell uses the image's `/usr/local/bin/python` and puts the venv's
site-packages on `sys.path` rather than running the venv's own interpreter, so
the transport is the only variable. site-packages ships precompiled `.pyc`
(`uv pip install --compile-bytecode`); without that the read-only share cells
would recompile 1848 files on every boot and the number would measure that
instead.

## Where the time goes

One guest per run, running the whole sequence, so the stages share a boot and
differ only in cache state and transport. 5 runs. Raw runs:
`data/exp5-diag.csv`. Reproduce with `spike/tasks/exp5-diag.sh`.

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

Stages: `cold` is the first import after boot (the matrix's number); `warm` is
immediately again in the same guest; `cold2` is after `sync; echo 3 >
/proc/sys/vm/drop_caches`, still from virtiofs; `blk-cold` copies the venv to
`/scratch` (ext4 on virtio-blk, the O_TMPFILE scratch disk), drops caches
again, and imports the copy; `blk-warm` imports it once more; `cpu` is an
IO-free arithmetic loop.

What the stages settle:

- **Not CPU.** 557.4 / 556.6 / 558.4 ms on the same loop. The guest's vCPUs are
  the host's, and nothing about the VM costs measurable CPU at this workload.
  The whole import gap has to be IO.
- **Not the image layer, and not podman's overlayfs beneath virtiofs.** The
  `share` cell reads the venv from a plain host directory bind-mounted into the
  helper - no image, no podman overlay, no guest overlay on the read path - and
  is still 2.40x. PLAN's "unpack the image to a plain host directory once per
  tag" option is therefore already measured, and it does not pass.
- **Not DAX.** `share-dax` is 959.3 ms against `share`'s 930.4, and 962.5
  against 952.0 in the earlier matrix: a few percent worse in both runs, never
  better. PLAN's fallback ("if the separate-share DAX
  variant passes, the venv becomes a separate share") does not apply: DAX is
  not what passes. Nor, by extension, is root DAX likely to be worth libkrun
  2.0 or `krun_set_kernel`, since a 512 MiB window over the entire 144 MiB venv
  bought nothing.
- **Not THP.** Every `-nothp` cell is within noise of its pair.
- **Not bandwidth - metadata.** Warm virtiofs is *still* 2.2-2.3x the warm
  container (513.9 / 473.8 vs 219.3), with every byte already in the guest's
  page cache. What survives caching is per-file work: `import pandas` pulls in
  594 modules, and each one is several path lookups plus an open, so the import
  is thousands of FUSE round-trips. On the block device the same warm import is
  250.0 ms, 1.14x of the container. That is the whole story: virtiofs answers
  metadata over a virtqueue, ext4-on-virtio-blk answers it out of the guest's
  own caches.
- **A block device passes with room to spare.** `blk-cold` is 347.6 ms, 0.89x
  of the baseline - the identical venv, cold, read from ext4 on virtio-blk
  inside the same guest that needs 1106.4 ms to read it over virtiofs. All
  three transports are measured with the host's page cache warm, which is the
  comparison PLAN asks for; the one asymmetry is that the block cell's blocks
  were written by the guest itself moments earlier, so a first-ever read of a
  per-tag artifact would start colder on the host - as it equally would over
  virtiofs.
- `cold2` (672.2 / 508.2) landing between `cold` and `warm` says there is a
  first-boot penalty on top of the empty-cache penalty: `cold2`'s guest cache
  is just as empty, yet it imports 300-440 ms faster. Probably the helper's
  virtiofs server holding its own open fds by then, plus whatever the guest
  still does in its first second. Not chased further - it does not move the
  gate.

The residual after moving site-packages: the `pass` column in the `blk` stages
is 25.8-28.0 ms against the container's 10.9, because the interpreter and
stdlib are still on the virtiofs root. Cheap next to the import, but it is the
same effect and it sets a floor on how much a venv-only fix can win.

## Notes the decision needs

- **Root DAX is unavailable in libkrun 1.19**, as PLAN assumed, and here is
  the mechanism. The shim adds the root as an ordinary share
  (`krun_add_virtiofs3(ctx, "/dev/root", ...)`), so it *could* pass a DAX
  window; what it cannot do is make the guest mount with `dax`, because the
  root is mounted by libkrun's own kernel command line. That command line,
  read from `/proc/cmdline` in a running guest, is
  `... rootfstype=virtiofs rw quiet no-kvmapf init=/init.krun ...` - no
  `rootflags=` at all, and libkrun 1.19 offers no way to add one. Root DAX
  therefore needs libkrun 2.0's configurable cmdline or `krun_set_kernel`.
  None of which had to be worked around, because DAX on the share it *can* be
  applied to bought nothing.
- **The measurement already includes libkrun's virtiofs attribute caching.**
  This is v1.19.4 (built from source, plus WP04's one-line ENOTTY patch), and
  the attribute-caching work that the master thread flagged as being "on
  experiment 5's path" landed in 1.19.3. There is no known-fixed cost hiding in
  these numbers.
- **The layer-vs-share decision is now moot for performance and still live for
  everything else.** The separate share is 2.40x against the layer's 2.88x -
  real, about 17%, and worth having if the venv stays on virtiofs, but it does
  not reach the gate on its own. The 17% is the guest overlay plus the extra
  podman layer on the read path; PLAN's option to unpack per tag captures part
  of it and the `share` cell measures the whole of it.
- **The extra no-overlay cell of the WP08 brief's step 4 was not run**, because
  the `share` cell already answers the question step 4 asks. Its read path has
  no overlayfs and no image at all, and it fails at 2.40x, so isolating the
  overlay could only redistribute the 17% between "overlay" and "extra podman
  layer" - it cannot move the gate, and it would have meant adding a flag to
  flow-init, outside WP08's paths. If the master thread still wants the split,
  it is a `--no-overlay` flag in flow-init and one more cell here.
- **What the numbers point at is a block-device venv**, not a virtiofs tuning
  knob: build the dependency set into a read-only ext4 (or erofs/squashfs)
  image once per tag and attach it as a second virtio-blk device, the way the
  scratch disk is already attached. Cold import 347.6 ms, 0.89x of podman
  today. That is a change to the helper (a second `krun_add_disk`), to
  flow-init (mount it instead of the `venv` virtiofs) and to whatever builds
  the artifact - not a change to the design's shape - and it is untested here
  only in the sense that nothing yet *builds* such an image; the read path is
  exactly the one `blk-cold` measures. The interpreter and stdlib would stay on
  virtiofs, at the ~15 ms per process the `pass` column shows, unless the whole
  root moves too.

## Environment

- Host: `7.0.0-1011-gcp`, INTEL(R) XEON(R) PLATINUM 8581C @ 2.30GHz x 8,
  16 GiB, podman 4.9.3, reactor dir on ext4.
- Helper: libkrun 1.19.4 built from source with the WP04 ENOTTY patch,
  libkrunfw 5.5.0, guest kernel 6.12.91.
- Guest images: `ghcr.io/estuary/derive-python:dev` (Python 3.14.5, uv
  0.11.16) and `localhost/derive-python-pandas:spike` built from it.
- Venv: `pandas==3.0.5`, `numpy==2.5.3`, `python-dateutil==2.9.0.post0`,
  `six==1.17.0`; 144 MiB, 1848 `.pyc`, 65 `.so`. The image layer and the
  exported host directory are built by the same interpreter with the same pin
  and agree to within 4 KiB, which is the `spike/` directory of bench scripts
  the exported copy also carries.
