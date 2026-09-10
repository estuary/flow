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
- `summarize.py` - either experiment-5 CSV into the report's table. Groups by
  whichever of `cell`/`stage` the file has and summarizes whichever measures it
  carries, so one reader serves both.

## Entry points

`spike/tasks/exp5-build.sh` builds the image and exports the identical venv to
a host directory (built at `/venv` inside a container of the same image, so the
interpreter and the venv's baked-in paths both match what the guest sees) for
the separate-share cells. `spike/tasks/exp5-run.sh` runs the matrix,
`spike/tasks/exp5-diag.sh` the breakdown that says where the time went.

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
