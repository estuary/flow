#!/usr/bin/env bash
# Experiment 10: density (measure).
#
# What a helper costs when it is doing nothing, which is the state a connector
# spends most of its life in. Four arms:
#
#   idle      boot idle guests one at a time until the host's MemAvailable
#             crosses the floor, recording each helper's cgroup as it lands.
#             Run twice, once with THP left alone and once with --thp-disable.
#   tls       the same, with 20 guests each holding one long-lived TLS
#             connection to nginx - a connector that has dialled its endpoint
#             and is waiting, rather than one that has not.
#   reclaim   one guest fills its page cache from its own disk and then drops
#             it, while the host watches the helper's memory.current. This is
#             the test of libkrun's free-page reporting: does guest memory that
#             the guest has stopped using come back to the host.
#   overhead  the constant the launcher must add to `memoryMib`. Each guest
#             reports its own MemTotal - MemAvailable; the helper's
#             memory.current minus that is everything else the sandbox costs.
#
# There is no gate. The pass/fail line asserts the run was VALID - the guests
# booted, the cgroup limit never bound (a helper that was being reclaimed would
# report a number the host chose, not one the sandbox cost), and the host
# recovered afterwards.
#
# Run under mise: mise exec -- spike/tasks/exp10-density.sh [--arms a,b] [--max-guests N]
set -euo pipefail

XP_GUEST_IMAGE=ghcr.io/estuary/derive-python:dev
source "$(dirname "${BASH_SOURCE[0]}")/exp11-common.sh"

GUEST_PYTHON=/usr/local/bin/python

# PLAN's density shape: small guests, one vcpu, because the question is how many
# idle connectors fit rather than how fast one runs.
DENSITY_MEMORY_MIB=512
DENSITY_VCPUS=1
DENSITY_DISK_MIB=1024

# Stop launching at 20% of MemTotal still available (PLAN's 80% used), and never
# past the cap however much memory is left: an arm that miscounts should stop,
# not take the box down with it.
FLOOR_PCT=20
MAX_GUESTS=100
TLS_GUESTS=20
OVERHEAD_GUESTS=4
REPORT_PERIOD=20

ARMS=idle,tls,reclaim,overhead

while [ $# -gt 0 ]; do
    case "$1" in
    --arms) ARMS="$2"; shift 2 ;;
    --max-guests) MAX_GUESTS="$2"; shift 2 ;;
    --tls-guests) TLS_GUESTS="$2"; shift 2 ;;
    *) echo "unknown argument: $1" >&2; exit 2 ;;
    esac
done
has_arm() { [[ ",$ARMS," == *",$1,"* ]]; }

trap xp_cleanup EXIT

GUEST="$XP_WORK/density.py"
DENSITY_CSV="$SPIKE_DIR/report/data/exp10-density.csv"
RECLAIM_CSV="$SPIKE_DIR/report/data/exp10-reclaim.csv"
OVERHEAD_CSV="$SPIKE_DIR/report/data/exp10-overhead.csv"
SUMMARY_CSV="$SPIKE_DIR/report/data/exp10-arms.csv"

MEM_TOTAL_KIB="$(awk '/^MemTotal:/ {print $2}' /proc/meminfo)"
FLOOR_KIB=$((MEM_TOTAL_KIB * FLOOR_PCT / 100))

# ------------------------------------------------------------- guest workload

cat >"$GUEST" <<'GUESTPY'
# Experiment 10's guest-side workloads. One JSON line per event on stderr, which
# CONTRACTS gives the host byte for byte; stdout carries the kernel console
# interleaved and cannot be parsed. argv[1] names the mode.
#
# Every mode reports the guest's own /proc/meminfo periodically. MemTotal minus
# MemAvailable is the RAM the guest has actually touched, and it is the term the
# host has to subtract from the helper's cgroup before anything left over can be
# called the sandbox's overhead.
import http.client, json, os, ssl, sys, time


def meminfo():
    d = {}
    for line in open("/proc/meminfo"):
        k, _, v = line.partition(":")
        d[k] = int(v.split()[0])
    return d


def emit(**kw):
    m = meminfo()
    kw.update(total_kib=m["MemTotal"], available_kib=m["MemAvailable"],
              free_kib=m["MemFree"], cached_kib=m["Cached"])
    print(json.dumps(kw), file=sys.stderr, flush=True)


def report_forever(period, **extra):
    while True:
        time.sleep(period)
        emit(event="meminfo", **extra)


mode = sys.argv[1]

if mode == "idle":
    emit(event="ready", mode="idle")
    report_forever(float(sys.argv[2]))

elif mode == "fill":
    # Touch a given percentage of the guest's OWN RAM and hold it. An idle guest
    # answers "what does a helper cost when the guest uses almost none of its
    # memoryMib", which is not the question the launcher's overhead constant is
    # asking: that constant has to hold when a connector is using what it was
    # given. Every page is written, not just allocated, or the host never backs
    # it and the measurement is of nothing.
    pct, period = float(sys.argv[2]), float(sys.argv[3])
    target_kib = int(meminfo()["MemTotal"] * pct / 100)
    chunks, got = [], 0
    while got < target_kib:
        n_kib = min(64 << 10, target_kib - got)
        b = bytearray(n_kib << 10)
        for off in range(0, len(b), 4096):
            b[off] = 1
        chunks.append(b)
        got += n_kib
    emit(event="ready", mode="fill", filled_kib=got)
    report_forever(period)

elif mode == "tls":
    # http.client keeps the connection open between requests, so the periodic
    # GET both proves the connection is still the original one and keeps nginx
    # from closing it on keepalive_timeout. A socket the server has dropped is
    # not the thing PLAN asked to measure.
    host, port, period = sys.argv[2], int(sys.argv[3]), float(sys.argv[4])
    ctx = ssl.create_default_context()
    ctx.check_hostname = False
    ctx.verify_mode = ssl.CERT_NONE
    conn = http.client.HTTPSConnection(host, port, context=ctx, timeout=15)
    conn.request("GET", "/probe")
    r = conn.getresponse()
    body = r.read()
    emit(event="ready", mode="tls", status=r.status, bytes=len(body),
         cipher=conn.sock.cipher()[0])
    n = 1
    while True:
        time.sleep(period)
        conn.request("GET", "/probe")
        r = conn.getresponse()
        r.read()
        n += 1
        emit(event="meminfo", requests=n, reused=True)

elif mode == "reclaim":
    # Fill the guest's page cache from its own disk, hold, then drop it. The
    # host watches the helper's memory.current across both halves: the fill is
    # the guest touching RAM it did not have before, and the drop is the guest
    # ceasing to need it. Whether the second shows up on the host is entirely
    # libkrun's free-page reporting.
    size_mib, hold = int(sys.argv[2]), float(sys.argv[3])
    path = "/scratch/big"
    buf = b"\xa5" * (1 << 20)
    with open(path, "wb") as f:
        for _ in range(size_mib):
            f.write(buf)
        f.flush()
        os.fsync(f.fileno())
    # Drop what the write left behind, so the read below is a real fill from
    # disk rather than a re-hit of cache the writer already warmed.
    open("/proc/sys/vm/drop_caches", "w").write("3\n")
    time.sleep(3)
    emit(event="written", size_mib=size_mib)

    read = 0
    with open(path, "rb") as f:
        while True:
            b = f.read(1 << 20)
            if not b:
                break
            read += len(b)
    emit(event="filled", read_mib=read >> 20)
    time.sleep(hold)

    open("/proc/sys/vm/drop_caches", "w").write("3\n")
    time.sleep(3)
    emit(event="dropped")
    report_forever(5.0)

else:
    raise SystemExit("unknown mode: %s" % mode)
GUESTPY

XP_VENV_FILE="$GUEST"

say "experiment 10: density"
say "  guest:    $XP_GUEST_IMAGE"
say "  host:     $((MEM_TOTAL_KIB / 1024)) MiB MemTotal, floor $FLOOR_PCT% = $((FLOOR_KIB / 1024)) MiB MemAvailable"
say "  arms:     $ARMS"

: >"$XP_WORK/density.rows"
: >"$XP_WORK/overhead.rows"
: >"$XP_WORK/reclaim.rows"
: >"$XP_WORK/arms.rows"

# ------------------------------------------------------------------- helpers

# Launches one guest and waits for it to say it is up. Sets GUEST_ID. Returns 1
# if the guest never reported, which is how an arm learns it has run out of room
# rather than hanging on a guest that will not boot.
launch_guest() {
    local tag="$1"
    shift
    GUEST_ID="$(xp_new_sandbox)"
    xp_start "$tag" "$GUEST_ID" "$@"
    local _
    for _ in $(seq 600); do
        grep -q '"event": "ready"\|"event": "written"' "$XP_WORK/$tag.err" 2>/dev/null && return 0
        kill -0 "$XP_PID" 2>/dev/null || return 1
        sleep 0.1
    done
    return 1
}

# Tears down every guest launched so far and waits for podman to finish: removal,
# netns teardown and layer reclaim all happen after the client returns (WP10), so
# a host measurement taken at the kill measures the teardown, not the aftermath.
teardown_guests() {
    local id _ settled=0
    while read -r id; do
        [ -n "$id" ] || continue
        sudo podman rm -f "$id" >/dev/null 2>&1 || true
        # The per-connector directory goes too. xp_cleanup would do it at exit,
        # but this script empties the id file after each arm, so anything left
        # here is a stale `fs_` directory nothing will ever collect.
        sudo rm -rf "${SPIKE_REACTOR_DIR:?}/$id"
    done <"$XP_SANDBOXES"
    # Polling podman, not `wait` on the run clients: removal, netns teardown and
    # layer reclaim all happen after the client returns (WP10), and callers
    # invoke this through a command substitution, where `wait` has no children of
    # its own to wait for anyway.
    for _ in $(seq 600); do
        if [ "$(sudo podman ps --format '{{.Names}}' | grep -c '^fs_' || true)" -eq 0 ]; then
            settled=1
            break
        fi
        sleep 0.2
    done
    : >"$XP_SANDBOXES"
    printf '%s' "$settled"
}

# The last meminfo the guest reported, as `touched_kib total_kib`. The guest's
# own clock and its own numbers: the host cannot see inside the VM, and guest
# stderr arrives late enough (WP06) that pairing it with a host sample by
# arrival time would be wrong.
guest_touched_kib() {
    local tag="$1"
    grep -h '"total_kib"' "$XP_WORK/$tag.err" 2>/dev/null | tail -1 |
        python3 -c '
import json, sys
line = sys.stdin.read().strip()
if not line:
    print("0 0")
else:
    m = json.loads(line)
    print(m["total_kib"] - m["available_kib"], m["total_kib"])
'
}

# The same last report, as `total available free cached`. The overhead arm needs
# all four: PLAN subtracts MemTotal - MemAvailable, but MemAvailable counts the
# guest's page cache as free while the HOST is still backing every one of those
# pages. That difference is the width of the bracket the constant sits in, so
# both ends are recorded rather than one.
guest_meminfo_kib() {
    local tag="$1"
    grep -h '"total_kib"' "$XP_WORK/$tag.err" 2>/dev/null | tail -1 |
        python3 -c '
import json, sys
line = sys.stdin.read().strip()
if not line:
    print("0 0 0 0")
else:
    m = json.loads(line)
    print(m["total_kib"], m["available_kib"], m["free_kib"], m["cached_kib"])
'
}

# `max` is the number of times the cgroup limit stopped an allocation. Any
# non-zero value means the helper was being reclaimed while it was measured, and
# memory.current is then the limit the host imposed rather than the memory the
# sandbox wanted.
xp_memory_max_events() {
    local n
    n="$(sudo awk '$1=="max" {print $2}' "$(xp_cgroup "$1")/memory.events" 2>/dev/null || true)"
    printf '%s' "${n:-0}"
}

# ------------------------------------------------------------ density arm

# Launches guests one at a time until MemAvailable crosses the floor, recording
# each helper as it lands. $1 tags the pass; the rest are extra helper arguments.
density_pass() {
    local arm="$1" limit="$2"
    shift 2
    local base_avail n=0 id tag avail mem anon thr touched total clipped=0 reason=cap

    base_avail="$(xp_synced_mem_available_kib)"
    step "$arm: launching up to $limit guests (${DENSITY_MEMORY_MIB} MiB, $DENSITY_VCPUS vcpu)"
    note "MemAvailable at start: $((base_avail / 1024)) MiB"

    while [ "$n" -lt "$limit" ]; do
        tag="$arm-$n"
        if ! launch_guest "$tag" "$@"; then
            note "guest $n never reported ready; stopping"
            tail -5 "$XP_WORK/$tag.err" 2>/dev/null | sed 's/^/      /'
            reason=failed
            break
        fi
        id="$GUEST_ID"
        n=$((n + 1))

        mem="$(xp_memory_current "$id")"
        anon="$(xp_memory_stat "$id" anon)"
        thr="$(xp_threads "$id")"
        read -r touched total < <(guest_touched_kib "$tag")
        avail="$(xp_synced_mem_available_kib)"
        [ "$(xp_memory_max_events "$id")" -eq 0 ] || clipped=$((clipped + 1))

        printf '%s,%d,%s,%s,%s,%s,%s,%s\n' \
            "$arm" "$n" "$(xp_kib "$mem")" "$(xp_kib "$anon")" "$thr" \
            "$touched" "$avail" "$((base_avail - avail))" >>"$XP_WORK/density.rows"

        if [ "$avail" -lt "$FLOOR_KIB" ]; then
            reason=floor
            break
        fi
    done

    local end_avail delta per
    end_avail="$(xp_synced_mem_available_kib)"
    delta=$((base_avail - end_avail))
    per=$(python3 -c "print('%.1f' % ($delta / 1024 / max($n, 1)))")

    note "guests:       $n (stopped: $reason)"
    note "MemAvailable: $((base_avail / 1024)) -> $((end_avail / 1024)) MiB, delta $((delta / 1024)) MiB"
    note "host cost:    ${per} MiB per guest, measured from the host"
    if [ "$clipped" -eq 0 ]; then
        ok "$arm: no helper hit its cgroup limit; every memory.current is the sandbox's own"
    else
        fail "$arm: $clipped helper(s) were reclaimed at the cgroup limit; their memory.current is clipped"
    fi

    local settled recovered
    settled="$(teardown_guests)"
    recovered="$(xp_synced_mem_available_kib)"
    if [ "$settled" -eq 1 ]; then
        ok "$arm: every helper is gone; MemAvailable back to $((recovered / 1024)) MiB ($(( (recovered - end_avail) / 1024 )) MiB returned)"
    else
        fail "$arm: helpers were still running 2 minutes after removal"
    fi

    printf '%s,%d,%s,%s,%s,%s,%s\n' "$arm" "$n" "$reason" \
        "$((base_avail / 1024))" "$((end_avail / 1024))" "$((recovered / 1024))" "$per" \
        >>"$XP_WORK/arms.rows"
}

if has_arm idle; then
    XP_MEMORY_MIB=$DENSITY_MEMORY_MIB
    XP_VCPUS=$DENSITY_VCPUS
    XP_DISK_MIB=$DENSITY_DISK_MIB
    XP_OVERHEAD_MIB=256

    density_pass idle-thp-on "$MAX_GUESTS" \
        --exec "$GUEST_PYTHON" /venv/spike/density.py idle "$REPORT_PERIOD"
    density_pass idle-thp-off "$MAX_GUESTS" --thp-disable \
        --exec "$GUEST_PYTHON" /venv/spike/density.py idle "$REPORT_PERIOD"
fi

# --------------------------------------------------------------- TLS arm

if has_arm tls; then
    XP_MEMORY_MIB=$DENSITY_MEMORY_MIB
    XP_VCPUS=$DENSITY_VCPUS
    XP_DISK_MIB=$DENSITY_DISK_MIB
    XP_OVERHEAD_MIB=256
    XP_POLICY_FILE="$SPIKE_DIR/egress/examples/declared.json"

    density_pass tls-thp-on "$TLS_GUESTS" \
        --exec "$GUEST_PYTHON" /venv/spike/density.py tls "$SPIKE_NGINX_IP" 443 "$REPORT_PERIOD"
    density_pass tls-thp-off "$TLS_GUESTS" --thp-disable \
        --exec "$GUEST_PYTHON" /venv/spike/density.py tls "$SPIKE_NGINX_IP" 443 "$REPORT_PERIOD"

    # The `ready` event is only emitted after a 200 comes back, so every guest
    # counted above completed a handshake; this prints what one of them
    # negotiated, so the report can say the connections were real TLS.
    note "handshake: $(grep -h '"event": "ready"' "$XP_WORK"/tls-thp-on-0.err 2>/dev/null |
        python3 -c 'import json,sys; l=sys.stdin.read().strip(); m=json.loads(l) if l else {}; print("status %s, %s bytes, %s" % (m.get("status"), m.get("bytes"), m.get("cipher")))' || true)"

    XP_POLICY_FILE=""
fi

# ----------------------------------------------------------- reclaim arm

if has_arm reclaim; then
    XP_MEMORY_MIB=1024
    XP_VCPUS=2
    XP_DISK_MIB=4096
    # Room above the guest's RAM so the cgroup limit is not what ends the arm:
    # the question is whether the GUEST gives memory back, and a helper being
    # reclaimed by the host would answer a different one.
    XP_OVERHEAD_MIB=1024
    FILL_MIB=512
    HOLD_SECS=90

    step "reclaim: a guest fills its page cache from disk, then drops it"
    if ! launch_guest reclaim --run-as-root \
        --exec "$GUEST_PYTHON" /venv/spike/density.py reclaim "$FILL_MIB" "$HOLD_SECS"; then
        fail "reclaim: the guest never wrote its file"
        tail -10 "$XP_WORK/reclaim.err" 2>/dev/null | sed 's/^/      /'
    else
        rid="$GUEST_ID"
        # Sampled every 5 s across both halves: free-page reporting is periodic,
        # so the shape of the decay is the answer, not a single reading after.
        phase=written
        t0="$(date +%s)"
        for _ in $(seq 80); do
            grep -q '"event": "filled"' "$XP_WORK/reclaim.err" 2>/dev/null && { phase=filled; break; }
            sleep 0.5
        done
        for _ in $(seq 48); do
            grep -q '"event": "dropped"' "$XP_WORK/reclaim.err" 2>/dev/null && phase=dropped
            elapsed=$(( $(date +%s) - t0 ))
            printf '%s,%s,%s,%s,%s\n' "$elapsed" "$phase" \
                "$(xp_kib "$(xp_memory_current "$rid")")" \
                "$(xp_kib "$(xp_memory_stat "$rid" anon)")" \
                "$(guest_touched_kib reclaim | cut -d' ' -f1)" >>"$XP_WORK/reclaim.rows"
            # Keep sampling for a minute past the drop: free-page reporting is
            # periodic, so what matters is the shape of the decay, not the first
            # reading after.
            if [ "$phase" = dropped ] && [ "$elapsed" -gt $((HOLD_SECS + 60)) ]; then
                break
            fi
            sleep 5
        done

        read -r peak_mib peak_anon post_mib post_anon < <(python3 -c '
import sys
rows = [l.strip().split(",") for l in open(sys.argv[1]) if l.strip()]
filled = [r for r in rows if r[1] == "filled"]
dropped = [r for r in rows if r[1] == "dropped"]
peak = max(filled, key=lambda r: int(r[2])) if filled else ["", "", "0", "0"]
post = dropped[-1] if dropped else ["", "", "0", "0"]
print(int(peak[2]) // 1024, int(peak[3]) // 1024, int(post[2]) // 1024, int(post[3]) // 1024)
' "$XP_WORK/reclaim.rows")

        note "fill:     ${FILL_MIB} MiB read from /scratch into the guest's page cache"
        note "peak:     ${peak_mib} MiB memory.current (${peak_anon} MiB anon) while the cache was full"
        note "after:    ${post_mib} MiB memory.current (${post_anon} MiB anon) after drop_caches"
        note "returned: $((peak_mib - post_mib)) MiB"
        if [ "$((peak_mib - post_mib))" -gt $((FILL_MIB / 4)) ]; then
            ok "reclaim: free-page reporting returned $((peak_mib - post_mib)) MiB of the ${FILL_MIB} MiB the guest stopped using"
        else
            note "reclaim: only $((peak_mib - post_mib)) MiB of ${FILL_MIB} MiB came back; guest memory is not returned on drop_caches alone"
        fi
        teardown_guests >/dev/null
    fi
fi

# ---------------------------------------------------------- overhead arm

if has_arm overhead; then
    step "overhead: helper memory.current minus the RAM the guest says it touched"
    XP_VCPUS=1
    XP_DISK_MIB=$DENSITY_DISK_MIB
    # Generous, so the limit cannot bind: this arm exists to find out how much a
    # helper wants, and a helper held at a ceiling reports the ceiling.
    XP_OVERHEAD_MIB=1024

    # Both regimes: an idle guest, which is what PLAN's density pass boots, and a
    # guest holding 80% of its own RAM, which is what the constant has to survive.
    # The first alone would say the sandbox costs nothing, because a guest that
    # has not touched its memory has not cost the host anything to lend it.
    for spec in "512 idle" "1024 idle" "512 fill" "1024 fill"; do
        read -r mib mode <<<"$spec"
        XP_MEMORY_MIB=$mib
        for i in $(seq "$OVERHEAD_GUESTS"); do
            tag="ovh-$mode-$mib-$i"
            argv=(idle 5)
            [ "$mode" = fill ] && argv=(fill 80 5)
            if ! launch_guest "$tag" --exec "$GUEST_PYTHON" /venv/spike/density.py "${argv[@]}"; then
                fail "overhead: a ${mib} MiB $mode guest never reported ready"
                continue
            fi
            id="$GUEST_ID"
            # One report period plus slack: the first meminfo a guest emits is
            # from before it has finished touching what it is going to touch.
            sleep 20
            mem="$(xp_memory_current "$id")"
            anon="$(xp_memory_stat "$id" anon)"
            file="$(xp_memory_stat "$id" file)"
            thr="$(xp_threads "$id")"
            read -r g_total g_avail g_free g_cached < <(guest_meminfo_kib "$tag")
            printf '%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s\n' "$mode" "$mib" "$i" \
                "$(xp_kib "$mem")" "$(xp_kib "$anon")" "$(xp_kib "$file")" "$thr" \
                "$((g_total - g_avail))" "$((g_total - g_free))" "$g_total" "$g_cached" \
                >>"$XP_WORK/overhead.rows"
        done
    done
    teardown_guests >/dev/null

    if [ -s "$XP_WORK/overhead.rows" ]; then
        python3 - "$XP_WORK/overhead.rows" <<'PY' | sed 's/^/      /'
import sys

# Columns: mode, memoryMib, n, current, anon, file, threads, touched(avail),
# touched(free), guest total, guest cached - KiB except mode, memoryMib, n, threads.
rows = [l.strip().split(",") for l in open(sys.argv[1]) if l.strip()]
print("%-6s %-10s %4s %9s %9s %9s %7s %11s %11s" % (
    "mode", "memoryMib", "n", "current", "anon", "touched", "threads",
    "ovh(avail)", "ovh(free)"))
per_mode = {}
for mode in ("idle", "fill"):
    for mib in sorted({int(r[1]) for r in rows if r[0] == mode}):
        grp = [r for r in rows if r[0] == mode and int(r[1]) == mib]
        a = [(int(r[3]) - int(r[7])) / 1024 for r in grp]
        f = [(int(r[3]) - int(r[8])) / 1024 for r in grp]
        per_mode.setdefault(mode, [[], []])
        per_mode[mode][0] += a
        per_mode[mode][1] += f
        print("%-6s %-10d %4d %8.1fM %8.1fM %8.1fM %7s %10.1fM %10.1fM" % (
            mode, mib, len(grp),
            sum(int(r[3]) for r in grp) / len(grp) / 1024,
            sum(int(r[4]) for r in grp) / len(grp) / 1024,
            sum(int(r[7]) for r in grp) / len(grp) / 1024,
            grp[0][6], sum(a) / len(a), sum(f) / len(f)))
print("")
for mode, label in (("idle", "idle guests (PLAN's density shape)"),
                    ("fill", "guests holding 80% of their own RAM")):
    if mode not in per_mode:
        continue
    hi, lo = sorted(per_mode[mode][0]), sorted(per_mode[mode][1])
    print("%s, n=%d" % (label, len(hi)))
    print("   MemTotal-MemAvailable:  min %6.1f  median %6.1f  max %6.1f MiB" % (
        hi[0], hi[len(hi) // 2], hi[-1]))
    print("   MemTotal-MemFree:       min %6.1f  median %6.1f  max %6.1f MiB" % (
        lo[0], lo[len(lo) // 2], lo[-1]))
PY
        max_ov="$(python3 -c '
import sys
rows = [l.strip().split(",") for l in open(sys.argv[1]) if l.strip()]
print(int(max((int(r[3]) - int(r[7])) / 1024 for r in rows if r[0] == "fill")))
' "$XP_WORK/overhead.rows")"
        ok "overhead: measured on $(wc -l <"$XP_WORK/overhead.rows") guests; worst case at full occupancy ${max_ov} MiB"
    else
        fail "overhead: no guest reported"
    fi
fi

# ------------------------------------------------------------------- output

step "raw data"
{
    printf 'arm,guest_n,memory_current_kib,memory_anon_kib,threads,guest_touched_kib,host_available_kib,host_delta_kib\n'
    cat "$XP_WORK/density.rows"
} >"$DENSITY_CSV"
{
    printf 'arm,guests,stopped,avail_start_mib,avail_end_mib,avail_recovered_mib,host_mib_per_guest\n'
    cat "$XP_WORK/arms.rows"
} >"$SUMMARY_CSV"
{
    printf 't_secs,phase,memory_current_kib,memory_anon_kib,guest_touched_kib\n'
    cat "$XP_WORK/reclaim.rows"
} >"$RECLAIM_CSV"
{
    printf 'mode,memory_mib,guest_n,memory_current_kib,memory_anon_kib,memory_file_kib,threads,touched_avail_kib,touched_free_kib,guest_total_kib,guest_cached_kib\n'
    cat "$XP_WORK/overhead.rows"
} >"$OVERHEAD_CSV"

say "  $DENSITY_CSV   ($(($(wc -l <"$DENSITY_CSV") - 1)) rows)"
say "  $SUMMARY_CSV      ($(($(wc -l <"$SUMMARY_CSV") - 1)) rows)"
say "  $RECLAIM_CSV   ($(($(wc -l <"$RECLAIM_CSV") - 1)) rows)"
say "  $OVERHEAD_CSV  ($(($(wc -l <"$OVERHEAD_CSV") - 1)) rows)"

if [ -s "$XP_WORK/arms.rows" ]; then
    step "arms"
    python3 - "$XP_WORK/arms.rows" "$XP_WORK/density.rows" <<'PY' | sed 's/^/      /'
import sys

arms = [l.strip().split(",") for l in open(sys.argv[1]) if l.strip()]
rows = [l.strip().split(",") for l in open(sys.argv[2]) if l.strip()]
print("%-14s %7s %8s %11s %11s %9s %9s" % (
    "arm", "guests", "stopped", "cgroup med", "cgroup max", "threads", "host/guest"))
for a in arms:
    grp = [r for r in rows if r[0] == a[0]]
    if not grp:
        continue
    cur = sorted(int(r[2]) for r in grp)
    print("%-14s %7s %8s %10.1fM %10.1fM %9s %8sM" % (
        a[0], a[1], a[2], cur[len(cur) // 2] / 1024, cur[-1] / 1024, grp[0][4], a[6]))
PY
fi

xp_finish exp10-density.sh
