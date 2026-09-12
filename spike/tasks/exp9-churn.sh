#!/usr/bin/env bash
# Experiment 9: connection churn (measure).
#
# One HTTPS request per NEW connection, paced at 50 per second for 10 minutes,
# from inside the guest through the tap to the TEST-NET-2 nginx, which the
# policy declares as a /32 on port 443. No userspace proxy sits in this path, so
# what is under test is the tap, virtio-net and the nftables forward chain: can
# they carry sustained connection setup, and what does the helper cost while
# they do.
#
# There is no gate. The script still ends in a pass/fail line, and what it
# asserts is that the run was VALID - the guest really opened a new connection
# per request, the rate held, and the helper survived - not that any number came
# out a particular way.
#
# Run under mise: mise exec -- spike/tasks/exp9-churn.sh [--seconds N] [--rate N]
set -euo pipefail

# derive-python for its interpreter: the workload is a Python script, and this
# is the image the design is for.
XP_GUEST_IMAGE=ghcr.io/estuary/derive-python:dev
source "$(dirname "${BASH_SOURCE[0]}")/exp11-common.sh"

XP_MEMORY_MIB=1024
XP_VCPUS=2
XP_DISK_MIB=4096

SECONDS_TOTAL=600
RATE=50
WINDOW=10
GUEST_PYTHON=/usr/local/bin/python
TARGET="$SPIKE_NGINX_IP"

while [ $# -gt 0 ]; do
    case "$1" in
    --seconds) SECONDS_TOTAL="$2"; shift 2 ;;
    --rate) RATE="$2"; shift 2 ;;
    *) echo "unknown argument: $1" >&2; exit 2 ;;
    esac
done

trap xp_cleanup EXIT

CHURN="$XP_WORK/churn.py"
CSV="$SPIKE_DIR/report/data/exp9-churn.csv"
CGROUP_CSV="$SPIKE_DIR/report/data/exp9-cgroup.csv"

# The guest-side driver, staged under /venv/spike/ by xp_new_sandbox. Stdlib
# only, and every line it reports goes to stderr: CONTRACTS gives the workload's
# stderr to the host byte for byte, while stdout carries the kernel console
# interleaved and cannot be parsed.
cat >"$CHURN" <<'GUEST'
# Experiment 9's guest-side connection churn driver.
#
# A pacer hands one deadline per 1/rate second to a small pool of workers, each
# of which opens a NEW TCP connection, completes a TLS handshake, sends one GET
# and reads the body. Nothing is pooled or reused; the point of the experiment
# is connection setup, so `Connection: close` and a fresh socket every time.
#
# Pacing is against a fixed schedule (start + n/rate), not a sleep per request,
# so a slow window does not push every later request back - the achieved rate
# reported per window is then a real measure of what the path carried.
import json, socket, ssl, sys, threading, time
from collections import deque

host, port, rate, seconds, window = (
    sys.argv[1], int(sys.argv[2]), float(sys.argv[3]), float(sys.argv[4]), float(sys.argv[5]))

# Self-signed, per CONTRACTS: the experiment is about connection setup cost, and
# a real chain would only add a fixed verification term we are not measuring.
ctx = ssl.create_default_context()
ctx.check_hostname = False
ctx.verify_mode = ssl.CERT_NONE

REQUEST = ("GET /probe HTTP/1.1\r\nHost: %s\r\nConnection: close\r\n\r\n" % host).encode()

lock = threading.Lock()
# Per-window counters, keyed by the window index the request's DEADLINE fell in,
# so a request that completes late is still charged to the second it was owed.
windows = {}
errors = {}
latencies = deque()


def record(widx, ok, err, ms):
    with lock:
        w = windows.setdefault(widx, [0, 0])
        w[0 if ok else 1] += 1
        if err:
            errors[err] = errors.get(err, 0) + 1
        if ok:
            latencies.append(ms)


def one_request():
    t0 = time.monotonic()
    sock = socket.create_connection((host, port), timeout=10)
    try:
        tls = ctx.wrap_socket(sock, server_hostname=host)
        try:
            tls.sendall(REQUEST)
            body = b""
            while True:
                chunk = tls.recv(65536)
                if not chunk:
                    break
                body += chunk
        finally:
            tls.close()
    finally:
        sock.close()
    if b" 200 " not in body.split(b"\r\n", 1)[0]:
        raise RuntimeError("status: %r" % body.split(b"\r\n", 1)[0][:60])
    return (time.monotonic() - t0) * 1000.0


def worker(jobs):
    while True:
        job = jobs.get()
        if job is None:
            return
        widx, deadline = job
        # A job pulled off the queue long after its deadline is one the pool
        # could not keep up with; running it anyway would understate the
        # shortfall by borrowing from a later window.
        now = time.monotonic()
        if now < deadline:
            time.sleep(deadline - now)
        try:
            record(widx, True, None, one_request())
        except Exception as e:  # noqa: BLE001 - every failure mode is data here
            record(widx, False, "%s: %s" % (type(e).__name__, e), 0.0)


import queue  # noqa: E402 - kept next to its only use

# Bounded, so the pacer blocks rather than queueing an unbounded backlog if the
# path stalls: a backlog would hide the stall and then discharge as a false burst.
jobs = queue.Queue(maxsize=int(rate * 2))
pool = [threading.Thread(target=worker, args=(jobs,), daemon=True) for _ in range(16)]
for t in pool:
    t.start()

start = time.monotonic()
total = int(rate * seconds)
last_progress = 0.0
for n in range(total):
    deadline = start + n / rate
    jobs.put((int((deadline - start) // window), deadline))
    # Liveness only, and explicitly cumulative: the authoritative per-window
    # table is printed after the pool drains. The pacer runs a full queue ahead
    # of real time, so a window reported from here would be missing every
    # request still sitting in the queue - which is what makes it the wrong
    # place to report from.
    now = time.monotonic()
    if now - last_progress >= 30:
        last_progress = now
        with lock:
            print(json.dumps({"progress": True, "t": int(now - start),
                              "ok": sum(c[0] for c in windows.values()),
                              "err": sum(c[1] for c in windows.values())}),
                  file=sys.stderr, flush=True)

for _ in pool:
    jobs.put(None)
for t in pool:
    t.join()
elapsed = time.monotonic() - start

# Every window, now that every request charged to one has finished. Each is
# keyed by the guest's own monotonic clock, never the host's arrival time (WP06).
with lock:
    for w in range(int(seconds // window)):
        c = windows.get(w, [0, 0])
        print(json.dumps({"window": w, "t": int(w * window), "ok": c[0], "err": c[1],
                          "rate": round(c[0] / window, 2)}), file=sys.stderr, flush=True)
    ok = sum(c[0] for c in windows.values())
    err = sum(c[1] for c in windows.values())
    lat = sorted(latencies)

summary = {
    "summary": True, "ok": ok, "err": err, "elapsed": round(elapsed, 1),
    "rate": round(ok / elapsed, 2) if elapsed else 0,
    "ms_p50": round(lat[len(lat) // 2], 1) if lat else None,
    "ms_p95": round(lat[int(len(lat) * 0.95)], 1) if lat else None,
    "ms_max": round(lat[-1], 1) if lat else None,
    "errors": dict(sorted(errors.items(), key=lambda kv: -kv[1])[:5]),
}
print(json.dumps(summary), file=sys.stderr, flush=True)
GUEST

XP_VENV_FILE="$CHURN"

# CONTRACTS "Policy JSON" with the nginx /32 declared on 443 - the same file
# experiment 6 proved the declared-CIDR path with, so churn exercises a ruleset
# that has already been shown to be the right one.
XP_POLICY_FILE="$SPIKE_DIR/egress/examples/declared.json"

say "experiment 9: connection churn"
say "  target:   https://$TARGET/probe (declared /32, port 443)"
say "  rate:     $RATE conn/s for ${SECONDS_TOTAL}s, one new connection each"
say "  guest:    $XP_GUEST_IMAGE, ${XP_MEMORY_MIB} MiB, $XP_VCPUS vcpu"
say "  policy:   $XP_POLICY_FILE"

step "churn"
id="$(xp_new_sandbox)"
xp_start churn "$id" --exec "$GUEST_PYTHON" /venv/spike/churn.py \
    "$TARGET" 443 "$RATE" "$SECONDS_TOTAL" "$WINDOW"

if ! xp_await_container "$id"; then
    fail "the helper container never became executable"
    xp_finish exp9-churn.sh
fi

# Sampled on the host for the whole run: the cgroup is the helper's cost, and it
# is gone the moment the container is. cpu.stat is cumulative, so the rate for a
# window is the difference between two samples divided by the interval.
#
# The cgroup path and the shim's pid are resolved once and then read as files.
# Going through `podman inspect` for each of the sixty samples would cost a
# subprocess a second and, once the container exits, print "no such object" to
# stderr for every remaining one - and the sample it returned would be a row of
# zeroes that looks like a helper whose memory went to nothing.
CGROUP_DIR="$(xp_cgroup "$id")"
SHIM_PID="$(xp_shim_pid "$id")"
: >"$XP_WORK/cgroup.csv"

# Returns 1 once the cgroup is gone, which is how the sampler learns the run is
# over without racing podman's teardown.
sample_cgroup() {
    local t="$1" cpu mem anon threads
    cpu="$(sudo awk '/^usage_usec/ {print $2}' "$CGROUP_DIR/cpu.stat" 2>/dev/null || true)"
    [ -n "$cpu" ] || return 1
    mem="$(sudo cat "$CGROUP_DIR/memory.current" 2>/dev/null || printf '0')"
    anon="$(sudo awk '$1=="anon" {print $2}' "$CGROUP_DIR/memory.stat" 2>/dev/null || printf '0')"
    threads="$(sudo awk '/^Threads:/ {print $2}' "/proc/$SHIM_PID/status" 2>/dev/null || printf '0')"
    printf '%s,%s,%s,%s,%s\n' "$t" "$cpu" "$mem" "$anon" "$threads" >>"$XP_WORK/cgroup.csv"
}

sampler_start="$(date +%s)"
sample_cgroup 0 || true
while kill -0 "$XP_PID" 2>/dev/null; do
    sleep "$WINDOW"
    sample_cgroup "$(( $(date +%s) - sampler_start ))" || break
done

rc=0
wait "$XP_PID" || rc=$?

# ------------------------------------------------------------------- results

step "results"

summary="$(grep -h '"summary": true' "$XP_WORK/churn.err" | tail -1 || true)"
if [ -z "$summary" ]; then
    fail "the guest driver printed no summary; it did not finish"
    tail -20 "$XP_WORK/churn.err" | sed 's/^/      /'
    xp_finish exp9-churn.sh
fi

read -r conn_ok conn_err achieved elapsed p50 p95 pmax errs < <(python3 -c '
import json, sys
s = json.loads(sys.argv[1])
print(s["ok"], s["err"], s["rate"], s["elapsed"], s["ms_p50"], s["ms_p95"], s["ms_max"],
      ";".join("%s=%d" % (k.split(":")[0], v) for k, v in s["errors"].items()) or "-")
' "$summary")

# The windows, as the guest timed them, and the cgroup, as the host sampled it.
{
    printf 'window,t_secs,ok,err,rate\n'
    grep -h '"window"' "$XP_WORK/churn.err" |
        python3 -c '
import json, sys
for line in sys.stdin:
    try:
        w = json.loads(line)
    except ValueError:
        continue
    if "window" in w:
        print("%d,%d,%d,%d,%.2f" % (w["window"], w["t"], w["ok"], w["err"], w["rate"]))
'
} >"$CSV"
{
    printf 't_secs,cpu_usec,memory_current_bytes,memory_anon_bytes,threads\n'
    cat "$XP_WORK/cgroup.csv"
} >"$CGROUP_CSV"

windows_n="$(($(wc -l <"$CSV") - 1))"
samples_n="$(wc -l <"$XP_WORK/cgroup.csv")"

read -r cpu_first cpu_last mem_first mem_last thr_first thr_last < <(python3 -c '
import sys
rows = [l.strip().split(",") for l in open(sys.argv[1]) if l.strip()]
first, last = rows[0], rows[-1]
print(first[1], last[1], first[2], last[2], first[4], last[4])
' "$XP_WORK/cgroup.csv")

cpu_secs="$(python3 -c "print('%.1f' % ((int('$cpu_last') - int('$cpu_first')) / 1e6))")"
cpu_pct="$(python3 -c "print('%.1f' % (100 * (int('$cpu_last') - int('$cpu_first')) / 1e6 / max($elapsed, 1)))")"

note "connections:  $conn_ok ok, $conn_err failed, over ${elapsed}s"
note "rate:         ${achieved}/s achieved against ${RATE}/s offered"
note "latency:      p50 ${p50} ms, p95 ${p95} ms, max ${pmax} ms"
note "errors:       $errs"
note "helper cpu:   ${cpu_secs}s over the run = ${cpu_pct}% of one core ($XP_VCPUS vcpu configured)"
note "helper mem:   $(xp_kib "$mem_first") KiB -> $(xp_kib "$mem_last") KiB memory.current"
note "helper threads: $thr_first -> $thr_last"
note "helper exit:  $rc"
note "windows:      $CSV ($windows_n rows)"
note "cgroup:       $CGROUP_CSV ($samples_n rows)"

# What makes the run valid, rather than what makes a number good.
if [ "$conn_ok" -gt 0 ] && [ "$conn_err" -eq 0 ]; then
    ok "churn: all $conn_ok connections completed, none failed"
elif [ "$conn_ok" -gt 0 ]; then
    fail "churn: $conn_err of $((conn_ok + conn_err)) connections failed ($errs)"
else
    fail "churn: no connection completed"
fi
# 95% of the offered rate: the pacer is a fixed schedule, so anything materially
# below it means the path could not carry the offered load, which is the finding.
if [ "$(python3 -c "print(1 if float('$achieved') >= 0.95 * $RATE else 0)")" -eq 1 ]; then
    ok "churn: the offered rate held (${achieved}/s of ${RATE}/s)"
else
    fail "churn: the path carried only ${achieved}/s of the ${RATE}/s offered"
fi
if [ "$rc" -eq 0 ]; then
    ok "churn: the helper survived the run and exited 0"
else
    fail "churn: the helper exited $rc"
    tail -10 "$XP_WORK/churn.err" | sed 's/^/      /'
fi
if [ "$windows_n" -ge 2 ] && [ "$samples_n" -ge 2 ]; then
    ok "churn: $windows_n guest windows and $samples_n cgroup samples recorded"
else
    fail "churn: too few samples to report a time series"
fi

xp_finish exp9-churn.sh
