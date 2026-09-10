# spike/derived

WP08's sources: the derived connector image of PLAN experiment 5 and the
instruments that measure it. Results in `spike/report/exp5.md`.

## What is here

- `Dockerfile` - `localhost/derive-python-pandas:spike`: derive-python plus a
  pinned pandas venv at `/opt/venv`, precompiled. The shape the design assumes
  for customer dependencies, where the venv is a layer of the connector image
  and so is read over the same virtiofs root share as the interpreter.
- `bench.py` - the measurement: cold `import pandas` and
  `subprocess.run([python, "-c", "pass"])`, one JSON line on stdout. Runs
  unchanged as the guest workload and under a plain `podman run`.
- `cpu.py` - the CPU control: an IO-free loop, so a slow import can be
  attributed to the transport rather than to the vCPUs.
- `prefault.py` - first-touches a given number of MiB of anonymous memory and
  frees it. Isolates the part of a first import that is the guest faulting in
  host pages rather than reading anything (WP08b: ~201 ms of 371).
- `attrib.py` - imports pandas once and counts where the modules came from,
  deps disk versus virtiofs root. Answers "is the root what is left?" with a
  count instead of an argument.
- `summarize.py` - either experiment-5 CSV into the report's table. Groups by
  whichever of `cell`/`stage` the file has and summarizes whichever measures it
  carries, so one reader serves both.

## Entry points

`spike/tasks/exp5-build.sh` builds the image, exports the identical venv to a
host directory (built at `/venv` inside a container of the same image, so the
interpreter and the venv's baked-in paths both match what the guest sees) for
the separate-share cells, and turns that directory into the two per-tag disk
images (`deps.ext4`, `deps.erofs`) the block cells attach.
`spike/tasks/exp5-run.sh` runs the matrix, `spike/tasks/exp5-diag.sh` the
breakdown that says where the time went. Both take `--out NAME`: experiment 5's
CSVs are the record of a failure that stands, so 5b writes its own
(`exp5-5b.csv`, `exp5-5b-diag.csv`).

## Non-obvious details

- Every cell runs the image's `/usr/local/bin/python` and puts the venv's
  site-packages on `sys.path` rather than running the venv's own interpreter.
  The transport is then the only variable between cells.
- `--compile-bytecode` at install time is load-bearing. Without it the
  read-only share cells would recompile 1848 files on every boot and the
  numbers would measure that instead of virtiofs.
- `bench.py` times `python -c pass` *before* importing pandas, and it is still
  a warm-cache number: the running interpreter already read its own binary and
  startup stdlib to get there. Cold interpreter cost lives in boot time
  (experiment 2).
- The guest's stdout carries the kernel console interleaved with the workload's
  (CONTRACTS "Helper CLI"), so callers pick the JSON out by its leading `{`.
- Both scripts run inside the guest, where no stderr line may begin with a
  space (the reactor reads a leading space as connector-init's readiness byte).
  Checked: argparse's indented option list goes to stdout, and the usage line
  it prints to stderr on a bad argument is short enough not to wrap. Keep it
  that way - a flag long enough to make argparse wrap the usage would break the
  rule silently.
- `exp5-diag.sh` builds its podman argv as a bash array rather than through
  `printf | mapfile` the way `exp5-run.sh` does: its workload is a multi-line
  shell script, and mapfile would split it into one argv element per line.
- **A stage that runs late in a guest's life is not comparable to a fresh-boot
  cell.** Experiment 5's `blk-cold` stage measured 347.6 ms and was reported as
  a block device beating podman; 5b's `deps-cold`, the same read as the first
  thing after boot, is 822.0. The difference is that `blk-cold` ran after a
  boot, a full import and a 144 MiB `cp -a`, which prefaulted the guest's
  memory. Fresh boot per data point, or say which regime the number is from.
- `mkfs.ext4 -d` prints "Creating regular file ..." on stdout even under `-q`,
  so a function that returns the image path by echoing it must send mkfs's
  stdout elsewhere.
