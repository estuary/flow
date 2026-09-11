#!/usr/bin/env bash
# Experiment 2: boot latency. Time from `podman run` to connector-init's
# readiness byte, warm caches, with the switch on and again with it off, and a
# breakdown of where the sandboxed launch spends it.
#
# Clocks, and why there are two of them:
#   - T0 and T_ready are the runtime's own host wall clock, logged either side
#     of the launch. Both arms are timed to the same two events, so the delta
#     between them means something.
#   - The shim stamps its start and its `krun_start_enter` on that same host
#     clock, so podman's share and the shim's are host-measured too.
#   - flow-init stamps the guest's time since boot, the only clock here that
#     measures from a fixed guest event. Guest lines reach the host hundreds of
#     milliseconds late (libkrun batches the console), so their host-side
#     arrival times are meaningless and are not used.
#
# Run under mise: mise exec -- spike/tasks/exp2-boot.sh [--runs N] [--out NAME]
set -euo pipefail
source "$(dirname "${BASH_SOURCE[0]}")/preview-common.sh"

SPEC=capture-hello-world.flow.yaml
POLICY=policy-egress-none.json
RUNS=20
OUT_CSV=exp2-boot.csv

while [ $# -gt 0 ]; do
    case "$1" in
    --runs) RUNS="$2" && shift 2 ;;
    --out) OUT_CSV="$2" && shift 2 ;;
    *) echo "usage: exp2-boot.sh [--runs N] [--out NAME]" >&2 && exit 2 ;;
    esac
done

COMMIT=$(git -C "$REPO_DIR" rev-parse --short HEAD)
DATA_DIR="$SPIKE_DIR/report/data"
mkdir -p "$DATA_DIR"

OUT="$(mktemp -d)"
trap 'rm -rf "$OUT"' EXIT

step() { printf '\n== %s\n' "$*"; }

spike_stage_catalog

# `runtime_next` carries T0 and the readiness byte; `ops` carries the helper's
# and flow-init's stderr through the log decoder. Anything else is noise at this
# volume.
SPIKE_PREVIEW_GUEST_ENV=(RUST_LOG=runtime_next=debug,ops=warn)

# One preview per run, and every container launch inside it is a data point: the
# capture shard starts the connector once to validate and once to open, and both
# are the same launch measured to the same byte.
for arm in on off; do
    policy="$POLICY"
    [ "$arm" = off ] && policy=""

    step "$arm: $RUNS previews"
    for run in $(seq 1 "$RUNS"); do
        spike_preview "$policy" "$SPEC" --sessions 1 \
            >/dev/null 2>"$OUT/$arm.$run.log" ||
            { echo "preview failed; see $OUT/$arm.$run.log" >&2 && exit 1; }
        printf '.'
    done
    printf '\n'
done

step "parse"
python3 - "$OUT" "$COMMIT" "$DATA_DIR/$OUT_CSV" <<'PY'
import csv, datetime, pathlib, re, statistics, sys

logs_dir, commit, out_csv = pathlib.Path(sys.argv[1]), sys.argv[2], sys.argv[3]

ANSI = re.compile(r"\x1b\[[0-9;]*m")
STAMP = re.compile(r"^\d{4}-\d\d-\d\dT\d\d:\d\d:\d\d\.\d{6}Z")
T0 = "runtime_next::container: invoking docker"
READY = "connector-init readiness byte received"
SHIM = re.compile(r"flow-sandbox-helper: timing stage=(\S+) wall_us=(\d+)")
INIT = re.compile(r"flow-init: timing stage=(\S+) boot_us=(\d+)")


def wall_micros(line):
    """The runtime's own timestamp as epoch microseconds, so it shares an origin
    with the shim's `wall_us`."""
    m = STAMP.match(line)
    if not m:
        return None
    when = datetime.datetime.strptime(m.group(0), "%Y-%m-%dT%H:%M:%S.%fZ")
    return int(when.replace(tzinfo=datetime.timezone.utc).timestamp() * 1_000_000)


def launches(path):
    """Split a preview log into one segment per container launch. A segment runs
    from `invoking docker` to the next one, which is late enough to catch the
    guest's own lines: they arrive after the readiness byte but well before the
    following launch."""
    segments = []
    for line in ANSI.sub("", path.read_text(errors="replace")).splitlines():
        if T0 in line:
            segments.append({"t0": wall_micros(line)})
        elif not segments:
            continue
        elif READY in line:
            segments[-1].setdefault("ready", wall_micros(line))
        elif m := SHIM.search(line):
            segments[-1].setdefault("shim_" + m.group(1), int(m.group(2)))
        elif m := INIT.search(line):
            segments[-1].setdefault("init_" + m.group(1), int(m.group(2)))
            # ... and when the host saw it, which is a different thing: the
            # guest's stderr crosses the console device in batches.
            segments[-1].setdefault("host_" + m.group(1), wall_micros(line))
    return segments


rows = []
for path in sorted(logs_dir.glob("*.log")):
    arm, run, _ = path.name.split(".")
    for index, seg in enumerate(launches(path), start=1):
        if seg.get("t0") is None or seg.get("ready") is None:
            continue
        row = {
            "arm": arm,
            "run": run,
            "launch": index,
            "commit": commit,
            "total_us": seg["ready"] - seg["t0"],
        }
        # The sandboxed arm alone has a breakdown. `podman` is the launch line
        # reaching the shim's first instruction; `shim` is everything it does
        # before the VM; `kernel` is the guest's boot, measured from
        # krun_start_enter by the guest's own clock at flow-init's first line;
        # `flow_init` is its mounts and uid drop; the remainder is
        # connector-init binding its listener and writing the byte.
        if all(k in seg for k in ("shim_start", "shim_krun_start_enter",
                                  "init_start", "init_exec")):
            row["podman_us"] = seg["shim_start"] - seg["t0"]
            row["shim_us"] = seg["shim_krun_start_enter"] - seg["shim_start"]
            row["kernel_us"] = seg["init_start"]
            row["flow_init_us"] = seg["init_exec"] - seg["init_start"]
            row["connector_init_us"] = row["total_us"] - (
                row["podman_us"] + row["shim_us"] + row["kernel_us"] + row["flow_init_us"]
            )
            # How long the guest's own last line took to reach the host, by the
            # two clocks that bracket it. The readiness byte is a guest stderr
            # write too, so this is how much of `connector_init_us` above is
            # transport rather than connector-init's work.
            row["console_lag_us"] = seg["host_exec"] - (
                seg["shim_krun_start_enter"] + seg["init_exec"]
            )
        rows.append(row)

fields = ["arm", "run", "launch", "commit", "total_us", "podman_us", "shim_us",
          "kernel_us", "flow_init_us", "connector_init_us", "console_lag_us"]
with open(out_csv, "w", newline="") as handle:
    writer = csv.DictWriter(handle, fieldnames=fields, restval="")
    writer.writeheader()
    writer.writerows(rows)
print(f"wrote {out_csv}: {len(rows)} launches")


def stat(values, which):
    if not values:
        return "-"
    values = sorted(values)
    if which == "median":
        return f"{statistics.median(values) / 1000:.1f}"
    return f"{values[min(len(values) - 1, int(round(0.95 * (len(values) - 1))))] / 1000:.1f}"


print()
print(f"{'arm':<6}{'n':>5}{'total med':>12}{'total p95':>12}"
      f"{'podman':>10}{'shim':>10}{'kernel':>10}{'flow-init':>11}{'conn-init':>11}"
      f"{'(console)':>11}")
for arm in ("on", "off"):
    subset = [r for r in rows if r["arm"] == arm]
    columns = [stat([r["total_us"] for r in subset], "median"),
               stat([r["total_us"] for r in subset], "p95")]
    for key in ("podman_us", "shim_us", "kernel_us", "flow_init_us",
                "connector_init_us", "console_lag_us"):
        columns.append(stat([r[key] for r in subset if key in r], "median"))
    print(f"{arm:<6}{len(subset):>5}{columns[0]:>12}{columns[1]:>12}"
          f"{columns[2]:>10}{columns[3]:>10}{columns[4]:>10}{columns[5]:>11}"
          f"{columns[6]:>11}{columns[7]:>11}")

on = sorted(r["total_us"] for r in rows if r["arm"] == "on")
off = sorted(r["total_us"] for r in rows if r["arm"] == "off")
p95 = on[min(len(on) - 1, int(round(0.95 * (len(on) - 1))))] / 1e6
print()
print(f"gate: switch-on p95 {p95:.3f} s -> {'PASS' if p95 < 5 else 'FAIL'} (limit 5 s)")
print(f"delta: median {statistics.median(on) / 1000:.1f} ms sandboxed "
      f"against {statistics.median(off) / 1000:.1f} ms today, "
      f"{statistics.median(on) / statistics.median(off):.2f}x")
sys.exit(0 if p95 < 5 else 1)
PY
