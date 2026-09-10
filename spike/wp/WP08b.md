# WP08b: dependency set as a read-only block image (experiment 5, rerun)

## Goal
Turn experiment 5 from "fails on virtiofs, block device would pass" into a
measured pass on the transport the design will actually use: the dependency
set shipped as a per-tag read-only disk image on a second virtio-blk device.

## Read
- `spike/report/exp5.md` (all of it; it is the case for this package).
- CONTRACTS.md: "Helper CLI" (`--deps-image`, `--deps-fstype`), "flow-init"
  (`--deps-dev`), "Paths and names".
- PLAN.md: experiment 5 and its Decision bullet, both now rewritten.
- STATUS.md entries for WP04 and WP08 (the scratch chown, the
  `/init/flow-connector-init` gotcha, the `mapfile` trap).

## May touch
`spike/helper/shim/src/` (the deps disk: CLI flag, one `krun_add_disk3`, the
flow-init argv), `spike/flow-init/src/` (the deps mount), `spike/derived/**`,
`spike/tasks/exp5-*.sh`, `spike/report/exp5.md` (append a "5b" section; do
not rewrite the failure), `spike/report/data/`. `spike/tasks/env-setup.sh`
only to add `erofs-utils` to the host packages.

## Steps
1. Shim: `--deps-image PATH [--deps-fstype ext4|erofs]`. When given, after the
   scratch disk: `krun_add_disk3(ctx, "deps", PATH, KRUN_DISK_FORMAT_RAW,
   read_only=true, direct_io=false, KRUN_SYNC_NONE)`. Scratch stays first so
   it remains `/dev/vda`; deps is `/dev/vdb`. Pass `--deps-dev /dev/vdb
   --deps-fstype FS` into flow-init's argv.
2. flow-init: `--deps-dev DEV --deps-fstype FS`. After the pivot and the
   scratch mount: `mkdir -p /opt/venv`, mount DEV there read-only with FS.
   Absent flag, nothing changes.
3. Build the images from the venv directory `exp5-build.sh` already exports:
   `mkfs.ext4 -d <dir> -O ^has_journal -E lazy_itable_init=0 -m 0 -q -F
   deps.ext4 <size>` and `mkfs.erofs deps.erofs <dir>` (both host-side;
   `apt install erofs-utils`). Record image sizes against the directory size.
4. Cells, 10 fresh guests each as before, `ghcr.io/estuary/derive-python:dev`
   as the guest root, venv NOT in the image, bench importing from `/opt/venv`:
   - `blk-ext4`: `--deps-image deps.ext4`
   - `blk-erofs`: `--deps-image deps.erofs --deps-fstype erofs`
   - `blk-ext4-reactor`: same as `blk-ext4` launched via `fake-reactor.sh`
   - re-run `baseline` in the same session so the ratio is same-day.
   Host page cache warm (one discarded boot per cell), as in WP08. Add one
   `blk-ext4-hostcold` cell: `echo 3 > /proc/sys/vm/drop_caches` on the HOST
   before each boot, so the report also has the first-ever-launch-per-tag
   number the WP08 entry flagged as unmeasured.
5. Keep `pass` and the `cpu` control in the bench so the root's ~15 ms per
   process cost is restated alongside the new import number.

## Verification
`spike/tasks/exp5-run.sh --cells blk-ext4,blk-erofs,blk-ext4-reactor,blk-ext4-hostcold,baseline`
produces the table; `report/exp5.md` gains a "5b" section whose first line is
the gate verdict for `blk-ext4` against `baseline`. Pass: under 2.00x. Also
state the ext4-vs-erofs delta and the host-cold number as information.
Re-run `spike/tasks/flow-init-test.sh` and `helper-smoke.sh`: still green with
no `--deps-image` given.

## Out of scope
Moving the root off virtiofs. Changing derive-python to consume `/opt/venv`
(phase 2). The builder VM.
