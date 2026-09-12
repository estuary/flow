#!/usr/bin/env bash
# Experiment 12: control channel and device exposure (gate).
#
# WP11 read libkrun 1.19.4 and said what the guest can and cannot reach. This
# turns those claims into measurements, from inside a real guest, on the probe
# list its report ends with (spike/report/libkrun-exposure.md, "For WP10").
# Every probe here is T1 or T2 - the connector's own user, or guest root. The
# T3 findings in that report are deliberately out of reach and stay in the
# report's open problems.
#
# Four passes:
#   user    as the connector image's own user: the two vsock connect outcomes,
#           the `..` escape attempt, and the device inventory.
#   tsi     the TSI proxy-create datagram, with `ss -tunap` inside the helper
#           snapshotted either side of it.
#   deps    as guest root, with a read-only deps disk attached: a direct write
#           to /dev/vdb, and the device inventory with the second blk device.
#   ioctl   as guest root: ioctl(fd, 0x7602, 42) on the READ-ONLY /venv share,
#           against an unclaimed command on the same fd as a control, then an
#           exit code that is neither. PLAN expects the helper to exit 42; see
#           report/exp12.md for why it exits the workload's own status instead.
#
# Pass: every check below prints ok.
#
#   spike/tasks/exp12-exposure.sh
set -euo pipefail

# derive-python, not busybox: every probe here is a Python program, and
# CONTRACTS puts the guest's interpreter at /usr/local/bin/python in this image.
XP_GUEST_IMAGE=ghcr.io/estuary/derive-python:dev
source "$(dirname "${BASH_SOURCE[0]}")/exp11-common.sh"

GUEST_PYTHON=/usr/local/bin/python
PROBES="$XP_WORK/exp12.py"
DEPS_IMG="$XP_WORK/deps.ext4"

trap xp_cleanup EXIT

CSV="$SPIKE_DIR/report/data/exp12-probes.csv"
printf 'set,probe,expect,result,pass\n' >"$CSV"

# PLAN experiment 12's device list: two virtiofs (root and venv), one blk
# (scratch), one net, vsock, console, balloon, rng. Device ids are the virtio
# spec's, as the `device` file under each bus entry reports them.
EXPECTED_DEVICES="balloon:1 blk:1 console:1 fs:2 net:1 rng:1 vsock:1"
EXPECTED_DEVICES_DEPS="balloon:1 blk:2 console:1 fs:2 net:1 rng:1 vsock:1"

# The guest-side probes. Written out here rather than shipped as a file because
# WP10's paths are `exp{11,12,13}-*.sh`; xp_new_sandbox stages it under
# /venv/spike/, which is where CONTRACTS puts anything the guest has to run.
cat >"$PROBES" <<'GUEST'
# Experiment 12's guest-side probes. One JSON line per probe on stderr, which
# CONTRACTS gives the workload byte for byte; stdout carries the kernel console
# interleaved and cannot be parsed. argv[1] names the set.
import errno, fcntl, json, os, socket, struct, subprocess, sys, time

VMADDR_CID_HOST = 2
MAPPED_PORT = 49092
TSI_PROXY_CREATE = 1024
VIRTIO_IOC_EXIT_CODE_REQ = 0x7602

failures = 0


def emit(probe, expect, result):
    global failures
    ok = result == expect
    if not ok:
        failures += 1
    print(json.dumps({"probe": probe, "expect": expect, "result": result, "pass": ok}),
          file=sys.stderr, flush=True)


def oserr(e):
    return "error:%s" % errno.errorcode.get(e.errno, e.errno)


def emit_one_of(probe, codes, result):
    global failures
    ok = result in ["error:%s" % c for c in codes]
    if not ok:
        failures += 1
    print(json.dumps({"probe": probe, "expect": "error:" + "|".join(codes),
                      "result": result, "pass": ok}), file=sys.stderr, flush=True)


# A blocking AF_VSOCK connect has no default timeout and an unmapped port draws
# no response at all, so the probe imposes its own (WP11, muxer.rs:548-583).
def vsock_connect(port, timeout=5.0):
    sock = socket.socket(socket.AF_VSOCK, socket.SOCK_STREAM)
    sock.settimeout(timeout)
    try:
        sock.connect((VMADDR_CID_HOST, port))
        return "connected"
    except socket.timeout:
        return "timeout"
    except OSError as e:
        return oserr(e)
    finally:
        sock.close()


# virtio device ids, from the spec.
VIRTIO_NAMES = {1: "net", 2: "blk", 3: "console", 4: "rng", 5: "balloon",
                0x13: "vsock", 0x1a: "fs"}


def device_inventory():
    counts = {}
    base = "/sys/bus/virtio/devices"
    for name in sorted(os.listdir(base)):
        with open(os.path.join(base, name, "device")) as f:
            ident = int(f.read().strip(), 16)
        key = VIRTIO_NAMES.get(ident, "id-0x%x" % ident)
        counts[key] = counts.get(key, 0) + 1
    return " ".join("%s:%d" % kv for kv in sorted(counts.items()))


def run_user():
    emit("vsock-mapped-port", "error:ECONNRESET", vsock_connect(MAPPED_PORT))
    emit("vsock-unmapped-port", "timeout", vsock_connect(1234))

    # The confinement WP11 found at T1: the guest VFS resolves `..` itself and
    # never puts it on the FUSE wire, so this stays inside the guest.
    try:
        inside = os.stat("/etc/hostname")
        escaped = os.stat("/venv/../../etc/hostname")
        same = (inside.st_dev, inside.st_ino) == (escaped.st_dev, escaped.st_ino)
        emit("dotdot-file", "same-file", "same-file" if same else "different-file")
    except OSError as e:
        emit("dotdot-file", "same-file", oserr(e))
    try:
        root = os.stat("/")
        above = os.stat("/..")
        same = (root.st_dev, root.st_ino) == (above.st_dev, above.st_ino)
        emit("dotdot-root", "same-file", "same-file" if same else "different-file")
    except OSError as e:
        emit("dotdot-root", "same-file", oserr(e))

    emit("devices", sys.argv[2], device_inventory())
    print("lsblk: %s" % subprocess.run(["lsblk", "-nro", "NAME,SIZE,RO,TYPE"],
                                       capture_output=True, text=True).stdout.replace("\n", "; "),
          file=sys.stderr, flush=True)


def run_tsi():
    # The host must snapshot `ss -tunap` before the datagram goes out, so say
    # when the socket is ready and then wait.
    try:
        sock = socket.socket(socket.AF_VSOCK, socket.SOCK_DGRAM, 0)
    except OSError as e:
        # A stronger negative than the experiment asked for: the guest kernel
        # does not offer the datagram socket at all.
        emit("tsi-proxy-create", "sent", "socket-%s" % oserr(e))
        print("tsi-armed", file=sys.stderr, flush=True)
        time.sleep(float(sys.argv[2]))
        return
    print("tsi-armed", file=sys.stderr, flush=True)
    time.sleep(float(sys.argv[2]))
    try:
        # WP11's exact bytes: peer_port u32 LE, family u16 LE = LINUX_AF_INET,
        # type u16 LE = SOCK_STREAM. Eight bytes, not six: the parser guards on
        # `>= 6` but reads through byte 8.
        sock.sendto(struct.pack("<IHH", 12345, 2, 1),
                    (VMADDR_CID_HOST, TSI_PROXY_CREATE))
        emit("tsi-proxy-create", "sent", "sent")
    except OSError as e:
        emit("tsi-proxy-create", "sent", oserr(e))
    finally:
        sock.close()


def run_deps():
    # The deps image is protected by the host fd, not by a flag the guest can
    # negotiate away: libkrun opens it without write access when read_only.
    try:
        fd = os.open("/dev/vdb", os.O_WRONLY)
        try:
            os.write(fd, b"\0" * 4096)
            result = "wrote"
        finally:
            os.close(fd)
    except OSError as e:
        result = oserr(e)
    # Which errno the guest kernel reports is its business; that the write does
    # not reach the image is the claim. Report the one we got, and accept any of
    # the refusals a read-only virtio-blk can produce.
    emit_one_of("vdb-write", ("EBADF", "EROFS", "EACCES", "EIO", "EPERM"), result)
    emit("devices-deps", sys.argv[2], device_inventory())


def run_ioctl():
    # Any fd on any share, including the read-only ones: AugmentFs intercepts
    # 0x7602 before the read-only wrapper is consulted and stores `arg` as the
    # VM's exit code, with no check on inode, handle or uid.
    fd = os.open("/venv/spike/exp12.py", os.O_RDONLY)
    try:
        try:
            fcntl.ioctl(fd, VIRTIO_IOC_EXIT_CODE_REQ, 42)
            emit("exit-code-ioctl", "accepted", "accepted")
        except OSError as e:
            emit("exit-code-ioctl", "accepted", oserr(e))
        # The control: a neighbouring command on the same fd of the same
        # read-only share. AugmentFs does not claim it, so it reaches the
        # passthrough and comes back EOPNOTSUPP. That contrast is what shows
        # 0x7602 above was intercepted and stored, rather than quietly ignored.
        try:
            fcntl.ioctl(fd, VIRTIO_IOC_EXIT_CODE_REQ - 1, 42)
            emit("unclaimed-ioctl", "refused", "accepted")
        except OSError as e:
            emit("unclaimed-ioctl", "refused", "refused")
            print("unclaimed-ioctl raw: %s" % oserr(e), file=sys.stderr, flush=True)
    finally:
        os.close(fd)
    # Exit with a value that is neither 0 nor the 42 the ioctl asked for, so the
    # helper's exit code says which of the two writers to the atomic won.
    sys.exit(7)


{"user": run_user, "tsi": run_tsi, "deps": run_deps, "ioctl": run_ioctl}[sys.argv[1]]()
sys.exit(1 if failures else 0)
GUEST
XP_VENV_FILE="$PROBES"

# A small read-only ext4 disk to attach as the deps image. Built here rather
# than reused from experiment 5 so the script stands alone; its content does not
# matter, only that the host opened it without write access.
say "experiment 12: control channel and device exposure"
say "  guest image:  $XP_GUEST_IMAGE"
say "  helper:       $SPIKE_HELPER_IMAGE"
truncate -s 16M "$DEPS_IMG"
mkfs.ext4 -q -F "$DEPS_IMG"
chmod 644 "$DEPS_IMG"

# One JSON line per probe out of a pass's stderr, appended to the CSV.
collect() {
    local tag="$1"
    grep '^{"probe"' "$XP_WORK/$tag.err" | sed 's/^/      /'
    grep '^{"probe"' "$XP_WORK/$tag.err" | python3 -c '
import csv, json, sys
writer = csv.writer(sys.stdout, lineterminator="\n")
for line in sys.stdin:
    row = json.loads(line)
    writer.writerow([sys.argv[1], row["probe"], row["expect"], row["result"], row["pass"]])
' "$tag" >>"$CSV"
}

assert_probes() {
    local tag="$1" bad
    bad="$(grep -c '"pass": false' "$XP_WORK/$tag.err" || true)"
    if [ "${bad:-0}" -eq 0 ]; then
        ok "$tag: every probe matched its expectation"
    else
        fail "$tag: $bad probe(s) did not"
    fi
}

# -------------------------------------------- as the connector image's own user

step "vsock, the \`..\` escape attempt, and the device inventory"
id="$(xp_new_sandbox)"
rc=0
xp_run user "$id" --exec "$GUEST_PYTHON" /venv/spike/exp12.py user "$EXPECTED_DEVICES" || rc=$?
collect user
note "$(grep '^lsblk:' "$XP_WORK/user.err" || true)"
assert_probes user
if [ "$rc" -eq 0 ]; then
    ok "user: the workload exited 0"
else
    fail "user: the workload exited $rc"
    grep -v '^{"probe"' "$XP_WORK/user.err" | sed 's/^/      /' | head -10
fi

# ------------------------------------------------------ the TSI control port

step "TSI proxy-create datagram to vsock control port 1024"
id="$(xp_new_sandbox)"
XP_TSI_WAIT=8
xp_start tsi "$id" --exec /bin/sh -c \
    "$(xp_guest_script "$GUEST_PYTHON /venv/spike/exp12.py tsi $XP_TSI_WAIT")"

if ! xp_await_container "$id"; then
    fail "tsi: the helper container never became executable"
    wait "$XP_PID" || true
else
    # The guest says when its socket exists and then waits XP_TSI_WAIT seconds,
    # so the "before" snapshot is taken with certainty ahead of the datagram.
    armed=0
    for _ in $(seq 200); do
        grep -q '^tsi-armed' "$XP_WORK/tsi.err" 2>/dev/null && { armed=1; break; }
        sleep 0.2
    done
    [ "$armed" -eq 1 ] || fail "tsi: the guest never armed its socket"
    sudo podman exec "$id" ss -tunap >"$XP_WORK/tsi.ss.before" 2>/dev/null || true
    sudo podman exec "$id" sh -c 'ls /proc/1/fd | wc -l' >"$XP_WORK/tsi.fd.before" 2>/dev/null || true

    if xp_await_marker tsi "$XP_PID"; then
        sudo podman exec "$id" ss -tunap >"$XP_WORK/tsi.ss.after" 2>/dev/null || true
        sudo podman exec "$id" sh -c 'ls /proc/1/fd | wc -l' >"$XP_WORK/tsi.fd.after" 2>/dev/null || true
    else
        fail "tsi: the datagram pass never finished"
    fi
    sudo podman kill "$id" >/dev/null 2>&1 || true
    wait "$XP_PID" || true

    collect tsi
    note "ss -tunap inside the helper: $(($(grep -c . "$XP_WORK/tsi.ss.before") - 1)) socket(s) before, $(($(grep -c . "$XP_WORK/tsi.ss.after") - 1)) after"
    note "open fds on the VMM process: $(cat "$XP_WORK/tsi.fd.before") before, $(cat "$XP_WORK/tsi.fd.after") after"
    # `ss` prints a header even with nothing to list, so its presence is what
    # separates "no sockets" from "ss did not run".
    if ! grep -q Netid "$XP_WORK/tsi.ss.before" || ! grep -q Netid "$XP_WORK/tsi.ss.after" ||
        ! grep -qE '^[1-9][0-9]*$' "$XP_WORK/tsi.fd.before"; then
        fail "tsi: the snapshots did not run inside the helper; the comparison proves nothing"
    elif diff -q "$XP_WORK/tsi.ss.before" "$XP_WORK/tsi.ss.after" >/dev/null &&
        [ "$(cat "$XP_WORK/tsi.fd.after")" -le "$(cat "$XP_WORK/tsi.fd.before")" ]; then
        # The fd count falls over the interval as the guest closes the files it
        # opened at startup and the virtiofs server releases their handles. What
        # the probe asserts is that it does not RISE: a proxy would be a new fd,
        # and an AF_INET one would also be a new row in ss.
        ok "tsi: no socket and no new fd in the helper; krun_add_vsock(ctx, 0) opened nothing"
    else
        fail "tsi: the helper opened something"
        diff "$XP_WORK/tsi.ss.before" "$XP_WORK/tsi.ss.after" | sed 's/^/      /' | head -10
    fi
fi

# ------------------------------------------- guest root against the deps disk

step "a direct write to the deps disk, as guest root"
id="$(xp_new_sandbox)"
XP_PODMAN_ARGS=("--mount=type=bind,source=$DEPS_IMG,target=/deps.img,ro")
rc=0
xp_run deps "$id" --run-as-root --deps-image /deps.img \
    --exec "$GUEST_PYTHON" /venv/spike/exp12.py deps "$EXPECTED_DEVICES_DEPS" || rc=$?
XP_PODMAN_ARGS=()
collect deps
assert_probes deps
if [ "$rc" -ne 0 ]; then
    grep -v '^{"probe"' "$XP_WORK/deps.err" | sed 's/^/      /' | head -10
fi

# ------------------------------------------------------- the exit-code ioctl

step "ioctl(fd, 0x7602, 42) on the read-only /venv share, as guest root"
id="$(xp_new_sandbox)"
rc=0
xp_run ioctl "$id" --run-as-root \
    --exec "$GUEST_PYTHON" /venv/spike/exp12.py ioctl || rc=$?
collect ioctl
assert_probes ioctl
# PLAN expects 42 here. It is 7, and the reason is not that the ioctl failed:
# libkrun's own init reports the workload's status through the SAME ioctl, on
# `/`, after waitpid (src/init_blob/init/init.c:1141-1152), so it writes the
# atomic last and the guest's value is overwritten. The conclusion the report
# needs is unchanged and simpler: the helper's exit code is whatever the
# workload exits with, which is still a value the guest chooses.
if [ "$rc" -eq 7 ]; then
    ok "ioctl: the helper exited 7, the workload's own status - libkrun's init writes the atomic last"
elif [ "$rc" -eq 42 ]; then
    fail "ioctl: the helper exited 42, so the guest's ioctl now outlives init's report; exp12.md says otherwise"
else
    fail "ioctl: the helper exited $rc, expected the workload's 7"
    grep -v '^{"probe"' "$XP_WORK/ioctl.err" | sed 's/^/      /' | head -10
fi

say ""
say "raw data: $CSV"
xp_finish exp12-exposure.sh
