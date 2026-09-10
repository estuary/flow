#!/usr/bin/env bash
# Run experiment 5's matrix and write the raw runs as CSV.
#
#   exp5-run.sh                    every cell, 10 measured runs each
#   exp5-run.sh --runs 3           fewer runs, for checking the harness
#   exp5-run.sh --cells share,baseline
#   exp5-run.sh --reactor          launch the guests through fake-reactor.sh
#
# Each guest cell boots a fresh VM per run, so the guest page cache is cold by
# construction; one throwaway boot per cell warms the host's cache first, which
# is the state the baseline is measured in. What is measured is inside the
# guest (bench.py's perf_counter deltas), so the launch path does not enter the
# number; --host is the default only because it is the shorter path.
#
# Writes $SPIKE_DIR/report/data/exp5-runs.csv and prints the summary table.
set -euo pipefail
source "$(dirname "${BASH_SOURCE[0]}")/exp5-common.sh"

RUNS=10
LAUNCH=host
CELLS=primary,primary-nothp,share,share-nothp,share-dax,share-dax-nothp,baseline,baseline-root

while [ $# -gt 0 ]; do
    case "$1" in
        --runs)    RUNS="$2"; shift 2 ;;
        --cells)   CELLS="$2"; shift 2 ;;
        --reactor) LAUNCH=reactor; shift ;;
        *) echo "usage: exp5-run.sh [--runs N] [--cells LIST] [--reactor]" >&2; exit 2 ;;
    esac
done

COMMIT="$(git -C "$REPO_DIR" rev-parse --short HEAD)"
CSV="$SPIKE_DIR/report/data/exp5-runs.csv"
WORK="$(mktemp -d)"
CONNECTORS=()
FAILURES=0

cleanup() {
    local id
    for id in "${CONNECTORS[@]:-}"; do
        [ -n "$id" ] || continue
        sudo podman rm -f "$id" >/dev/null 2>&1 || true
        sudo rm -rf "${SPIKE_REACTOR_DIR:?}/$id"
    done
    rm -rf "$WORK"
}
trap cleanup EXIT

# ------------------------------------------------------------------ the cells
#
# cell -> guest image | venv share | bench.py's --venv | extra helper args.
# The interpreter is the image's in every case; only where site-packages comes
# from, and the helper flags around it, change.
cell_spec() {
    case "$1" in
        primary)          echo "$SPIKE_DERIVED_IMAGE|$SPIKE_EXP5_BENCH|/opt/venv|" ;;
        primary-nothp)    echo "$SPIKE_DERIVED_IMAGE|$SPIKE_EXP5_BENCH|/opt/venv|--thp-disable" ;;
        share)            echo "$SPIKE_GUEST_IMAGE|$SPIKE_EXP5_VENV|/venv|" ;;
        share-nothp)      echo "$SPIKE_GUEST_IMAGE|$SPIKE_EXP5_VENV|/venv|--thp-disable" ;;
        share-dax)        echo "$SPIKE_GUEST_IMAGE|$SPIKE_EXP5_VENV|/venv|--venv-dax" ;;
        share-dax-nothp)  echo "$SPIKE_GUEST_IMAGE|$SPIKE_EXP5_VENV|/venv|--venv-dax --thp-disable" ;;
        *) return 1 ;;
    esac
}

# Same per-connector directory the runtime builds, as WP03 and WP04's scripts
# do. `<id>` is never reused: a stale sock/init.sock is EEXIST.
new_connector() {
    local image="$1" id dir
    id="fs_$(head -c8 /dev/urandom | od -An -tx1 | tr -d ' \n')"
    dir="$SPIKE_REACTOR_DIR/$id"

    sudo mkdir -p "$dir/init" "$dir/sock" "$dir/scratch"
    sudo podman inspect "$image" | sudo tee "$dir/init/image-inspect.json" >/dev/null
    # The shim injects /init/flow-connector-init into the guest root whether or
    # not --exec replaces the workload, so the file has to exist. A placeholder:
    # no cell in this experiment runs it.
    printf '#!/bin/sh\nexit 0\n' | sudo tee "$dir/init/flow-connector-init" >/dev/null
    printf '%s\n' '{"egress":"public","allowAll":false,"declaredCidrs":[],"connectionsPerMinute":null,"distinctDestinationsPerMinute":null,"ttlFloorSecs":90,"ttlCapSecs":3600}' \
        | sudo tee "$dir/init/policy.json" >/dev/null

    CONNECTORS+=("$id")
    printf '%s' "$id"
}

# One boot. Prints bench.py's JSON line on success.
run_guest() {
    local cell="$1" tag="$2" spec image venvdir venvarg extra id argv=()
    spec="$(cell_spec "$cell")"
    IFS='|' read -r image venvdir venvarg extra <<<"$spec"
    id="$(new_connector "$image")"

    # PLAN "Helper launch", minus the labels and cgroup parent that only the
    # reactor cares about. --run-as-root per the WP08 brief's cells.
    mapfile -t argv < <(printf '%s\n' \
        run --rm "--name=$id" "--network=$SPIKE_NET_CONNECTORS" --log-driver=none \
        --device /dev/kvm --device /dev/net/tun --cap-add NET_ADMIN \
        --sysctl net.ipv4.ip_forward=1 \
        "--mount=type=image,source=$image,destination=/rootfs" \
        "--mount=type=bind,source=$SPIKE_REACTOR_DIR/$id/init,target=/init,ro" \
        "--mount=type=bind,source=$venvdir,target=/venv,ro" \
        "--mount=type=bind,source=$SPIKE_REACTOR_DIR/$id/sock,target=/sock" \
        "--mount=type=bind,source=$SPIKE_REACTOR_DIR/$id/scratch,target=/scratch-backing" \
        "$SPIKE_HELPER_IMAGE" \
        --policy /init/policy.json --memory-mib 1024 --vcpus 2 --disk-mib 1024 \
        --upper-mib 256 --run-as-root $extra \
        --exec "$SPIKE_GUEST_PYTHON" /venv/spike/bench.py \
        --venv "$venvarg" --label "$cell")

    local rc=0 started ended
    started="$(date +%s%N)"
    set +e
    if [ "$LAUNCH" = host ]; then
        sudo podman "${argv[@]}" >"$WORK/$tag.out" 2>"$WORK/$tag.err"
    else
        "$SPIKE_TASKS_DIR/fake-reactor.sh" podman "${argv[@]}" >"$WORK/$tag.out" 2>"$WORK/$tag.err"
    fi
    rc=$?
    set -e
    ended="$(date +%s%N)"

    sudo podman rm -f "$id" >/dev/null 2>&1 || true
    sudo rm -rf "${SPIKE_REACTOR_DIR:?}/$id"

    if [ "$rc" -ne 0 ]; then
        printf 'FAIL  %s/%s: the helper exited %s\n' "$cell" "$tag" "$rc" >&2
        sed 's/^/      /' "$WORK/$tag.err" >&2
        return 1
    fi
    # A file, not a variable: the caller reads run_one's stdout through a
    # command substitution, so anything assigned here dies with the subshell.
    printf '%s\n' $(( (ended - started) / 1000000 )) >"$WORK/$tag.wall"
    # Guest stdout interleaves the kernel console with the workload's, so the
    # JSON line is picked out rather than read whole.
    grep -m1 '^{' "$WORK/$tag.out"
}

# One baseline container. --user 0 for `baseline-root`, which exists only to
# show that reading as root rather than the image's `nobody` changes nothing,
# since the guest cells run as root and the plain baseline does not.
run_baseline() {
    local cell="$1" tag="$2" argv=() rc=0 started ended
    argv=(run --rm --log-driver=none
        --entrypoint "$SPIKE_GUEST_PYTHON"
        "--mount=type=bind,source=$SPIKE_EXP5_BENCH/spike/bench.py,target=/bench.py,ro")
    [ "$cell" = baseline-root ] && argv+=(--user 0)
    argv+=("$SPIKE_DERIVED_IMAGE" /bench.py --venv /opt/venv --label "$cell")

    started="$(date +%s%N)"
    set +e
    sudo podman "${argv[@]}" >"$WORK/$tag.out" 2>"$WORK/$tag.err"
    rc=$?
    set -e
    ended="$(date +%s%N)"

    if [ "$rc" -ne 0 ]; then
        printf 'FAIL  %s/%s: podman exited %s\n' "$cell" "$tag" "$rc" >&2
        sed 's/^/      /' "$WORK/$tag.err" >&2
        return 1
    fi
    printf '%s\n' $(( (ended - started) / 1000000 )) >"$WORK/$tag.wall"
    grep -m1 '^{' "$WORK/$tag.out"
}

run_one() {
    case "$1" in
        baseline|baseline-root) run_baseline "$@" ;;
        *) run_guest "$@" ;;
    esac
}

# ------------------------------------------------------------------- the runs
say "exp5-run.sh: commit $COMMIT, launch $LAUNCH, $RUNS runs per cell"
say ""

mkdir -p "$(dirname "$CSV")"
printf 'commit,cell,run,import_pandas_ms,python_c_pass_ms,helper_wall_ms,launch\n' >"$CSV"

IFS=, read -r -a cell_list <<<"$CELLS"
for cell in "${cell_list[@]}"; do
    cell_spec "$cell" >/dev/null 2>&1 || case "$cell" in
        baseline|baseline-root) ;;
        *) echo "exp5-run.sh: unknown cell $cell" >&2; exit 2 ;;
    esac

    # The throwaway: warms the host page cache for this cell's image and share,
    # and is discarded.
    if ! run_one "$cell" "$cell-warm" >/dev/null; then
        FAILURES=$((FAILURES + 1))
        say "exp5-run.sh: $cell skipped, its warm-up boot failed"
        continue
    fi

    for run in $(seq 1 "$RUNS"); do
        line=""
        if ! line="$(run_one "$cell" "$cell-$run")"; then
            FAILURES=$((FAILURES + 1))
            continue
        fi
        row="$(printf '%s\n' "$line" | python3 -c '
import json, sys
row = json.loads(sys.stdin.read())
print("%s,%s,%s,%s,%s,%s,%s" % (
    sys.argv[1], row["label"], sys.argv[2],
    row["import_pandas_ms"], row["python_c_pass_ms"], sys.argv[3], sys.argv[4]))
' "$COMMIT" "$run" "$(cat "$WORK/$cell-$run.wall")" "$LAUNCH")"
        printf '%s\n' "$row" >>"$CSV"
        IFS=, read -r _ _ _ import_ms pass_ms _ _ <<<"$row"
        printf '  %-18s run %-3s import %8.1f ms   python -c pass %6.1f ms\n' \
            "$cell" "$run" "$import_ms" "$pass_ms"
    done
    say ""
done

say "exp5-run.sh: raw runs in $CSV"
say ""
python3 "$SPIKE_DIR/derived/summarize.py" "$CSV"
say ""
if [ "$FAILURES" -eq 0 ]; then
    say "exp5-run.sh: ok"
else
    say "exp5-run.sh: $FAILURES failed run(s)"
fi
exit $((FAILURES > 0))
