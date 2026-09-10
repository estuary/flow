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
# The `deps` sequence (WP08b) runs the same cold/warm/cold2 against the real
# per-tag block image on /dev/vdb, with no copy step. It exists because the
# `blk-cold` stage above turned out to flatter the block device: it is measured
# late in a guest's life, after a boot and a full import, where `cold` is
# measured first. Comparing the two says how much of a cold import is the
# transport and how much is simply being first.
#
# The container side runs the same cold import and the same CPU loop, so the
# CPU ratio can be read against the import ratio.
#
# Writes $SPIKE_DIR/report/data/<--out>. WP08's breakdown was measured before
# the deps disk existed, so its CSV stands: WP08b writes exp5-5b-diag.csv.
set -euo pipefail
source "$(dirname "${BASH_SOURCE[0]}")/exp5-common.sh"

RUNS=5
OUT=exp5-diag.csv
while [ $# -gt 0 ]; do
    case "$1" in
        --runs) RUNS="$2"; shift 2 ;;
        --out)  OUT="$2"; shift 2 ;;
        *) echo "usage: exp5-diag.sh [--runs N] [--out NAME]" >&2; exit 2 ;;
    esac
done

COMMIT="$(git -C "$REPO_DIR" rev-parse --short HEAD)"
CSV="$SPIKE_DIR/report/data/$OUT"
WORK="$(mktemp -d)"
CONNECTORS="$WORK/connectors"
: >"$CONNECTORS"

# Reads the id file rather than an array: callers invoke new_connector through
# a command substitution, so anything it appends to a shell variable dies with
# that subshell and the trap would have nothing to clean.
cleanup() {
    local id
    while read -r id; do
        [ -n "$id" ] || continue
        sudo podman rm -f "$id" >/dev/null 2>&1 || true
        sudo rm -rf "${SPIKE_REACTOR_DIR:?}/$id"
    done <"$CONNECTORS"
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

# A fresh boot that first-touches 600 MiB of anonymous memory and frees it,
# then imports. Those host pages stay assigned to the guest, so the page cache
# the import fills is no longer faulting them in for the first time. It has to
# be its own boot: the point is what the *first* import pays.
prefault_sequence() {
    local prefix="$1"
    cat <<SEQ
set -e
P=$SPIKE_GUEST_PYTHON
\$P /venv/spike/prefault.py 600 >/dev/null
\$P /venv/spike/bench.py --venv /opt/venv --label $prefix
SEQ
}

# No copy step: the venv is already on /dev/vdb where flow-init mounted it, so
# `cold` here is the production first import and `cold2` is the same read after
# the guest's cache is thrown away.
deps_sequence() {
    local prefix="$1"
    cat <<SEQ
set -e
P=$SPIKE_GUEST_PYTHON
\$P /venv/spike/bench.py --venv /opt/venv --label $prefix-cold
\$P /venv/spike/bench.py --venv /opt/venv --label $prefix-warm
sync; echo 3 >/proc/sys/vm/drop_caches
\$P /venv/spike/bench.py --venv /opt/venv --label $prefix-cold2
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
    printf '%s\n' "$id" >>"$CONNECTORS"
    printf '%s' "$id"
}

# --disk-mib is larger than the matrix's: the sequence copies the venv onto it.
run_guest_sequence() {
    local cell="$1" tag="$2" image="$3" venvdir="$4" sequence="$5" deps="${6:-}"
    local id argv=() deps_mount=() deps_flag=() rc=0
    id="$(new_connector "$image")"
    if [ -n "$deps" ]; then
        deps_mount=("--mount=type=bind,source=$deps,target=$SPIKE_DEPS_IMG,ro")
        deps_flag=(--deps-image "$SPIKE_DEPS_IMG")
    fi
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
        "${deps_mount[@]}"
        "$SPIKE_HELPER_IMAGE"
        --policy /init/policy.json --memory-mib 1024 --vcpus 2 --disk-mib 2048
        --upper-mib 256 --run-as-root
        "${deps_flag[@]}"
        --exec /bin/sh -c "$sequence"
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
        "$SPIKE_DERIVED_IMAGE" "$SPIKE_EXP5_BENCH" \
        "$(guest_sequence /opt/venv primary)" | to_csv "$run" >>"$CSV"
    run_guest_sequence share "share-$run" \
        "$SPIKE_GUEST_IMAGE" "$SPIKE_EXP5_VENV" \
        "$(guest_sequence /venv share)" | to_csv "$run" >>"$CSV"
    run_guest_sequence deps "deps-$run" \
        "$SPIKE_GUEST_IMAGE" "$SPIKE_EXP5_BENCH" \
        "$(deps_sequence deps)" "$SPIKE_EXP5_EXT4" | to_csv "$run" >>"$CSV"
    run_guest_sequence deps-prefault "deps-prefault-$run" \
        "$SPIKE_GUEST_IMAGE" "$SPIKE_EXP5_BENCH" \
        "$(prefault_sequence deps-prefault)" "$SPIKE_EXP5_EXT4" | to_csv "$run" >>"$CSV"
    run_container_sequence "container-$run" | to_csv "$run" >>"$CSV"
    say "  run $run done"
done

say ""
say 'exp5-diag.sh: where the modules of an "import pandas" come from, with the'
say '               venv on the deps disk (one boot, counts and bytes, no timings):'
run_guest_sequence attrib attrib \
    "$SPIKE_GUEST_IMAGE" "$SPIKE_EXP5_BENCH" \
    "$SPIKE_GUEST_PYTHON /venv/spike/attrib.py" "$SPIKE_EXP5_EXT4" \
    | python3 -m json.tool | sed 's/^/      /'

say ""
say "exp5-diag.sh: raw runs in $CSV"
say ""
python3 "$SPIKE_DIR/derived/summarize.py" "$CSV"
say ""
say "exp5-diag.sh: ok"
