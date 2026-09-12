#!/usr/bin/env bash
# Experiment 11: storage behavior (gate).
#
# Four claims, all about where the guest's bytes end up:
#   root-fill      writes to the guest root land in podman's per-container
#                  writable layer on host disk, and that layer goes with the
#                  container.
#   root-over-ram  the same write, larger than the whole guest's RAM. The bound
#                  is the host's disk, not memory - which is the half of the
#                  claim a 512 MiB write cannot make on its own.
#   scratch-fill   /scratch stops at --disk-mib and the host's memory does not
#                  grow to match; the space comes back when the helper exits.
#   image          guest-root writes under /usr do not reach the image, and
#                  /venv rejects them outright.
#
# Pass: every check below prints ok.
#
#   spike/tasks/exp11-storage.sh
set -euo pipefail
source "$(dirname "${BASH_SOURCE[0]}")/exp11-common.sh"

trap xp_cleanup EXIT

CSV="$SPIKE_DIR/report/data/exp11-storage.csv"
printf 'pass,metric,unit,value\n' >"$CSV"
record() { printf '%s,%s,%s,%s\n' "$1" "$2" "$3" "$4" >>"$CSV"; }

mib() { printf '%s' "$(($1 / 1024))"; }

say "experiment 11: storage behavior"
say "  guest image:  $XP_GUEST_IMAGE"
say "  helper:       $SPIKE_HELPER_IMAGE, ${XP_MEMORY_MIB} MiB guest + ${XP_OVERHEAD_MIB} MiB overhead"
say "  reactor dir:  $SPIKE_REACTOR_DIR"

# ------------------------------------------------------- writes to the guest root

# MIB is the count dd writes; the pass asserts the layer grew by about that much
# and that the container's own memory limit was never the constraint.
root_fill() {
    local tag="$1" mib="$2" id cid layer rc=0
    local fs_before fs_after mem_current mem_anon mem_file layer_kib

    id="$(xp_new_sandbox)"
    fs_before="$(xp_fs_used)"
    xp_start "$tag" "$id" --run-as-root --exec /bin/sh -c \
        "$(xp_guest_script "dd if=/dev/zero of=/etc/fill bs=1M count=$mib && sync")"

    if ! xp_await_container "$id"; then
        fail "$tag: the helper container never became executable"
        sed 's/^/      /' "$XP_WORK/$tag.err" | head -20
        wait "$XP_PID" || true
        return 1
    fi
    cid="$(xp_cid "$id")"

    if ! xp_await_marker "$tag" "$XP_PID"; then
        fail "$tag: the fill never finished"
        sed 's/^/      /' "$XP_WORK/$tag.err" | head -20
        wait "$XP_PID" || true
        return 1
    fi

    layer="$(xp_layer_dir "$cid")"
    layer_kib="$(xp_layer_kib "$cid")"
    mem_current="$(xp_memory_current "$cid")"
    mem_anon="$(xp_memory_stat "$cid" anon)"
    mem_file="$(xp_memory_stat "$cid" file)"

    # The path xp_layer_dir derives from the container id, against the mount the
    # helper actually has at /rootfs. If these ever diverge every number below
    # is measuring the wrong directory.
    local mounted
    mounted="$(sudo podman exec "$id" awk '$2=="/rootfs" {print $1}' /proc/mounts)"
    if [ "$mounted" = "${layer%/upper}/merge" ]; then
        ok "$tag: /rootfs is the overlay whose upper dir is $layer"
    else
        fail "$tag: /rootfs is $mounted, but the layer was looked for at $layer"
    fi

    sudo podman kill "$id" >/dev/null 2>&1 || true
    wait "$XP_PID" || rc=$?
    fs_after="$(xp_fs_used)"

    note "dd:            $(grep -E 'records (in|out)|bytes' "$XP_WORK/$tag.err" | tr '\n' ' ')"
    note "layer:         $(mib "$layer_kib") MiB in $layer"
    note "cgroup:        current $(mib "$(xp_kib "$mem_current")") MiB, anon $(mib "$(xp_kib "$mem_anon")") MiB, file $(mib "$(xp_kib "$mem_file")") MiB, limit $((XP_MEMORY_MIB + XP_OVERHEAD_MIB)) MiB"
    note "df(reactor):   before $(mib "$fs_before") MiB, after $(mib "$fs_after") MiB"
    record "$tag" written MiB "$mib"
    record "$tag" layer MiB "$(mib "$layer_kib")"
    record "$tag" memory_current MiB "$(mib "$(xp_kib "$mem_current")")"
    record "$tag" memory_anon MiB "$(mib "$(xp_kib "$mem_anon")")"
    record "$tag" memory_file MiB "$(mib "$(xp_kib "$mem_file")")"

    if grep -q "$XP_DONE_MARKER rc=0" "$XP_WORK/$tag.err"; then
        ok "$tag: the guest wrote $mib MiB to /etc/fill and the write succeeded"
    else
        fail "$tag: dd failed: $(grep "$XP_DONE_MARKER" "$XP_WORK/$tag.err")"
    fi
    if [ "$layer_kib" -ge $((mib * 1024 * 95 / 100)) ]; then
        ok "$tag: podman's per-container layer holds $(mib "$layer_kib") MiB of the $mib MiB written"
    else
        fail "$tag: the layer holds only $(mib "$layer_kib") MiB of $mib MiB"
    fi
    if [ "$(xp_kib "$mem_current")" -le $(((XP_MEMORY_MIB + XP_OVERHEAD_MIB) * 1024)) ]; then
        ok "$tag: the helper cgroup stayed inside its $((XP_MEMORY_MIB + XP_OVERHEAD_MIB)) MiB limit while writing $mib MiB"
    else
        fail "$tag: cgroup memory.current $(mib "$(xp_kib "$mem_current")") MiB exceeds the limit"
    fi
    if sudo test -e "$layer"; then
        fail "$tag: the writable layer is still on disk after the helper exited"
    else
        ok "$tag: the writable layer is gone with the container ($(mib $((fs_after - fs_before))) MiB net on the reactor filesystem)"
    fi
}

step "512 MiB to the guest root, as guest root"
root_fill root-fill 512

step "$((XP_MEMORY_MIB + 512)) MiB to the guest root: more than the guest's whole RAM"
root_fill root-over-ram $((XP_MEMORY_MIB + 512))

# ------------------------------------------------------------------ /scratch

# CONTRACTS' production default, and four times the guest's RAM on purpose: at
# 1024 MiB the host's MemAvailable falls by about what was written, and the
# reason is the guest's own page cache (anon memory in the helper), not the
# disk. Only a disk several times the size of the guest can tell "memory backed
# the write" from "the guest touched all of its RAM".
XP_DISK_MIB=4096

step "filling /scratch past --disk-mib $XP_DISK_MIB"
id="$(xp_new_sandbox)"
sudo sync
fs_before="$(xp_fs_used)"
mem_before="$(xp_mem_available_kib)"
over=$((XP_DISK_MIB + 256))
xp_start scratch "$id" --exec /bin/sh -c \
    "$(xp_guest_script "dd if=/dev/zero of=/scratch/fill bs=1M count=$over")"

if xp_await_container "$id" && xp_await_marker scratch "$XP_PID"; then
    # Dirty page cache counts against MemAvailable until it is written back, and
    # the guest's dd has just made a gigabyte of it. Flush first, or the figure
    # says "memory backed the disk" when what it means is "the disk is behind".
    sudo sync
    fs_during="$(xp_fs_used)"
    mem_during="$(xp_mem_available_kib)"
    cid="$(xp_cid "$id")"
    mem_anon="$(xp_memory_stat "$cid" anon)"
    mem_current="$(xp_memory_current "$cid")"
    sudo podman kill "$id" >/dev/null 2>&1 || true
    wait "$XP_PID" || true
    fs_after="$(xp_fs_used)"
    entries="$(sudo ls -A "$SPIKE_REACTOR_DIR/$id/scratch" | wc -l)"

    grew=$((fs_during - fs_before))
    lost=$((mem_before - mem_during))
    note "dd:           $(grep -iE 'no space|records out' "$XP_WORK/scratch.err" | tr '\n' ' ')"
    note "df(reactor):  before $(mib "$fs_before") MiB, at the wall $(mib "$fs_during") MiB (+$(mib "$grew")), after exit $(mib "$fs_after") MiB"
    note "MemAvailable: before $(mib "$mem_before") MiB, at the wall $(mib "$mem_during") MiB (-$(mib "$lost"))"
    note "cgroup:       current $(mib "$(xp_kib "$mem_current")") MiB, anon $(mib "$(xp_kib "$mem_anon")") MiB, limit $((XP_MEMORY_MIB + XP_OVERHEAD_MIB)) MiB"
    record scratch disk_mib MiB "$XP_DISK_MIB"
    record scratch requested MiB "$over"
    record scratch memory_anon MiB "$(mib "$(xp_kib "$mem_anon")")"
    record scratch disk_growth MiB "$(mib "$grew")"
    record scratch memavailable_drop MiB "$(mib "$lost")"

    if grep -qi 'no space left on device' "$XP_WORK/scratch.err"; then
        ok "scratch: the write stopped at the end of the disk (ENOSPC), not at the end of host memory"
    else
        fail "scratch: dd did not report ENOSPC: $(grep "$XP_DONE_MARKER" "$XP_WORK/scratch.err")"
    fi
    if [ "$grew" -ge $((XP_DISK_MIB * 1024 * 90 / 100)) ]; then
        ok "scratch: the reactor filesystem carried the $(mib "$grew") MiB"
    else
        fail "scratch: the reactor filesystem grew only $(mib "$grew") MiB, so this proves nothing"
    fi
    if [ "$lost" -lt $((grew / 2)) ]; then
        ok "scratch: host MemAvailable fell $(mib "$lost") MiB against $(mib "$grew") MiB of disk, and is bounded by the helper's $((XP_MEMORY_MIB + XP_OVERHEAD_MIB)) MiB cgroup, not by the write"
    else
        fail "scratch: host MemAvailable fell $(mib "$lost") MiB, matching the $(mib "$grew") MiB written"
    fi
    if [ "$entries" -ne 0 ]; then
        fail "scratch: $entries entries left under $id/scratch"
    elif [ $((fs_after - fs_before)) -lt $((grew / 2)) ]; then
        ok "scratch: $id/scratch is empty and $(mib $((fs_during - fs_after))) MiB came back to the filesystem"
    else
        fail "scratch: $(mib $((fs_after - fs_before))) MiB still held after the helper exited"
    fi
else
    fail "scratch: the fill never finished"
    sed 's/^/      /' "$XP_WORK/scratch.err" | head -20
    wait "$XP_PID" || true
fi

# --------------------------------------------------------- the image, and /venv

step "guest-root writes under /usr, and the image afterwards"
id="$(xp_new_sandbox)"
rc=0
xp_run usr "$id" --run-as-root --exec /bin/sh -c 'echo x > /usr/spike && echo ok' || rc=$?
if [ "$rc" -eq 0 ] && grep -qx 'ok' "$XP_WORK/usr.out"; then
    ok "usr: guest root wrote /usr/spike and the workload exited 0"
else
    fail "usr: the workload exited $rc"
    sed 's/^/      /' "$XP_WORK/usr.err" | head -20
fi

mnt="$(sudo podman image mount "$XP_GUEST_IMAGE")"
if sudo test -e "$mnt/usr/spike"; then
    fail "usr: /usr/spike is in the image at $mnt"
else
    ok "usr: /usr/spike is not in the image ($mnt); the write stayed in the container's layer"
fi
sudo podman image umount "$XP_GUEST_IMAGE" >/dev/null

step "writes under /venv"
id="$(xp_new_sandbox)"
rc=0
xp_run venv "$id" --run-as-root --exec /bin/sh -c 'touch /venv/x' || rc=$?
if [ "$rc" -eq 0 ]; then
    fail "venv: touch /venv/x succeeded"
elif grep -qi 'read-only file system' "$XP_WORK/venv.err"; then
    ok "venv: touch /venv/x failed EROFS even as guest root (exit $rc)"
else
    fail "venv: touch /venv/x failed $rc, but not with EROFS: $(head -3 "$XP_WORK/venv.err")"
fi

say ""
say "raw data: $CSV"
xp_finish exp11-storage.sh
