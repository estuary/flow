#!/usr/bin/env bash
# Why experiment 5's matrix came out where it did, and which fix it points at.
#
#   exp5-diag.sh              5 runs of each diagnostic
#   exp5-diag.sh --runs 1
#
# The matrix says the guest is ~2.9x the baseline on `import pandas`. It does
# not say whether that is virtiofs moving bytes, virtiofs answering metadata,
# or the guest's CPU. Each guest here runs one sequence, so the stages share a
# boot and differ only in cache state and transport:
#
#   cold      first import, cold guest page cache        (the matrix's number)
#   warm      immediately again, same guest              isolates transport
#   cold2     after drop_caches, still from virtiofs     shows the drop worked
#   blk-cold  the venv copied to /scratch (virtio-blk,
#             ext4), then drop_caches, then imported     virtiofs vs block
#   blk-warm  again from /scratch                        block, warm
#   cpu       an IO-free loop                            the CPU control
#
# The container side runs the same cold import and the same CPU loop, so the
# CPU ratio can be read against the import ratio.
#
# Writes $SPIKE_DIR/report/data/exp5-diag.csv.
set -euo pipefail
source "$(dirname "${BASH_SOURCE[0]}")/exp5-common.sh"

RUNS=5
while [ $# -gt 0 ]; do
    case "$1" in
        --runs) RUNS="$2"; shift 2 ;;
        *) echo "usage: exp5-diag.sh [--runs N]" >&2; exit 2 ;;
    esac
done

COMMIT="$(git -C "$REPO_DIR" rev-parse --short HEAD)"
CSV="$SPIKE_DIR/report/data/exp5-diag.csv"
WORK="$(mktemp -d)"
CONNECTORS=()

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

# The whole sequence, as one guest workload. `sync` before each drop_caches so
# nothing pending is written back during the import being timed. The stages run
# as separate processes because `import pandas` is cached in the one that did
# it.
guest_sequence() {
    local venv="$1" prefix="$2"
    cat <<SEQ
set -e
P=$SPIKE_GUEST_PYTHON
V=$venv
\$P /venv/spike/bench.py --venv \$V --label $prefix-cold
\$P /venv/spike/bench.py --venv \$V --label $prefix-warm
sync; echo 3 >/proc/sys/vm/drop_caches
\$P /venv/spike/bench.py --venv \$V --label $prefix-cold2
cp -a \$V /scratch/venv
sync; echo 3 >/proc/sys/vm/drop_caches
\$P /venv/spike/bench.py --venv /scratch/venv --label $prefix-blk-cold
\$P /venv/spike/bench.py --venv /scratch/venv --label $prefix-blk-warm
\$P /venv/spike/cpu.py --label $prefix-cpu
SEQ
}

new_connector() {
    local image="$1" id dir
    id="fs_$(head -c8 /dev/urandom | od -An -tx1 | tr -d ' \n')"
    dir="$SPIKE_REACTOR_DIR/$id"
    sudo mkdir -p "$dir/init" "$dir/sock" "$dir/scratch"
    sudo podman inspect "$image" | sudo tee "$dir/init/image-inspect.json" >/dev/null
    printf '#!/bin/sh\nexit 0\n' | sudo tee "$dir/init/flow-connector-init" >/dev/null
    printf '%s\n' '{"egress":"public","allowAll":false,"declaredCidrs":[],"connectionsPerMinute":null,"distinctDestinationsPerMinute":null,"ttlFloorSecs":90,"ttlCapSecs":3600}' \
        | sudo tee "$dir/init/policy.json" >/dev/null
    CONNECTORS+=("$id")
    printf '%s' "$id"
}

# --disk-mib is larger than the matrix's: the sequence copies the venv onto it.
run_guest_sequence() {
    local cell="$1" tag="$2" image="$3" venvdir="$4" venvarg="$5" id argv=() rc=0
    id="$(new_connector "$image")"
    # Built as an array, not through `printf | mapfile` the way the matrix
    # script does: the sequence is a multi-line shell script, and mapfile would
    # split it into one argv element per line.
    argv=(
        run --rm "--name=$id" "--network=$SPIKE_NET_CONNECTORS" --log-driver=none
        --device /dev/kvm --device /dev/net/tun --cap-add NET_ADMIN
        --sysctl net.ipv4.ip_forward=1
        "--mount=type=image,source=$image,destination=/rootfs"
        "--mount=type=bind,source=$SPIKE_REACTOR_DIR/$id/init,target=/init,ro"
        "--mount=type=bind,source=$venvdir,target=/venv,ro"
        "--mount=type=bind,source=$SPIKE_REACTOR_DIR/$id/sock,target=/sock"
        "--mount=type=bind,source=$SPIKE_REACTOR_DIR/$id/scratch,target=/scratch-backing"
        "$SPIKE_HELPER_IMAGE"
        --policy /init/policy.json --memory-mib 1024 --vcpus 2 --disk-mib 2048
        --upper-mib 256 --run-as-root
        --exec /bin/sh -c "$(guest_sequence "$venvarg" "$cell")"
    )

    set +e
    sudo podman "${argv[@]}" >"$WORK/$tag.out" 2>"$WORK/$tag.err"
    rc=$?
    set -e
    sudo podman rm -f "$id" >/dev/null 2>&1 || true
    sudo rm -rf "${SPIKE_REACTOR_DIR:?}/$id"

    if [ "$rc" -ne 0 ]; then
        printf 'FAIL  %s/%s: the helper exited %s\n' "$cell" "$tag" "$rc" >&2
        sed 's/^/      /' "$WORK/$tag.err" >&2
        return 1
    fi
    if ! grep '^{' "$WORK/$tag.out"; then
        printf 'FAIL  %s/%s: no JSON line on guest stdout\n' "$cell" "$tag" >&2
        sed 's/^/      /' "$WORK/$tag.out" >&2
        return 1
    fi
}

# The container side: one fresh container per run, warm host cache, same two
# instruments.
run_container_sequence() {
    local tag="$1" rc=0
    set +e
    sudo podman run --rm --user 0 --log-driver=none \
        --entrypoint /bin/sh \
        "--mount=type=bind,source=$SPIKE_EXP5_BENCH/spike,target=/spike,ro" \
        "$SPIKE_DERIVED_IMAGE" -c \
        "set -e
         $SPIKE_GUEST_PYTHON /spike/bench.py --venv /opt/venv --label container-cold
         $SPIKE_GUEST_PYTHON /spike/bench.py --venv /opt/venv --label container-warm
         $SPIKE_GUEST_PYTHON /spike/cpu.py --label container-cpu" \
        >"$WORK/$tag.out" 2>"$WORK/$tag.err"
    rc=$?
    set -e
    if [ "$rc" -ne 0 ]; then
        printf 'FAIL  container/%s: podman exited %s\n' "$tag" "$rc" >&2
        sed 's/^/      /' "$WORK/$tag.err" >&2
        return 1
    fi
    grep '^{' "$WORK/$tag.out"
}

# cpu.py's JSON has no import fields and bench.py's has no cpu field, so both
# shapes go through one writer with empty cells where a stage has nothing.
to_csv() {
    python3 -c '
import json, sys
for line in sys.stdin:
    row = json.loads(line)
    print("%s,%s,%s,%s,%s,%s" % (
        sys.argv[1], row["label"], sys.argv[2],
        row.get("import_pandas_ms", ""), row.get("python_c_pass_ms", ""),
        row.get("cpu_ms", "")))
' "$COMMIT" "$1"
}

mkdir -p "$(dirname "$CSV")"
printf 'commit,stage,run,import_pandas_ms,python_c_pass_ms,cpu_ms\n' >"$CSV"

say "exp5-diag.sh: commit $COMMIT, $RUNS runs of each sequence"
say ""

for run in $(seq 1 "$RUNS"); do
    run_guest_sequence primary "primary-$run" \
        "$SPIKE_DERIVED_IMAGE" "$SPIKE_EXP5_BENCH" /opt/venv | to_csv "$run" >>"$CSV"
    run_guest_sequence share "share-$run" \
        "$SPIKE_GUEST_IMAGE" "$SPIKE_EXP5_VENV" /venv | to_csv "$run" >>"$CSV"
    run_container_sequence "container-$run" | to_csv "$run" >>"$CSV"
    say "  run $run done"
done

say ""
say "exp5-diag.sh: raw runs in $CSV"
say ""
python3 "$SPIKE_DIR/derived/summarize.py" "$CSV"
say ""
say "exp5-diag.sh: ok"
