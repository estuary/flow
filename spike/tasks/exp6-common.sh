# Shared driver for experiments 6, 7 and 8: WP02's probe suite run from inside a
# real guest, across the real tap, against the real ruleset. Sourced, never
# executed.
#
# Named for experiment 6 because WP07's paths are `exp{6,7,8}-*.sh`; exp7 and
# exp8 source it as well. Fold it into a plainer name whenever these three are
# next touched together (as WP03's SPIKE_HELPER_IMAGE and WP05's
# preview-common.sh are waiting to be).
#
# The shape of a run:
#
#   [host]  testnet.py DNS on 10.89.0.1:5353 (the flow-connectors gateway),
#           forwarding to aardvark-dns on :53 so container names and public
#           names both resolve; TCP sink and short-TTL target on 203.0.113.5.
#           A listener on the gateway's "reactor port" so `connect-gateway`
#           proves a drop rather than an absent service. `spike-sibling`, a
#           throwaway container whose aardvark name answers RFC1918.
#      |
#   [helper container, flow-connectors, --ip pinned so the probes can be told
#           its uplink address before it exists] tap0 192.0.2.1/30, the WP02
#           binaries, and the ruleset under test.
#      |
#   [guest] probes.py as the connector image's own user, or as root for the
#           pass that needs raw sockets.
#
# Assertions are made on the host, as helper-smoke.sh does: the nft counters are
# read with `podman exec` while the helper is still up, and the capture is taken
# on the host side of its veth (see eg_start_capture for why not inside it).

source "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/helper-common.sh"

EG_GUEST_IMAGE=ghcr.io/estuary/derive-python:dev
EG_GUEST_PYTHON=/usr/local/bin/python
EG_EGRESS_DIR="$SPIKE_DIR/egress"

# CONTRACTS "Tap network".
EG_HELPER_IP=192.0.2.1
EG_GUEST_IP=192.0.2.2
# The brief's source: inside the tap's /30, so that rp_filter cannot be what
# stops it. It turns out nothing can be spoofed from inside a /30 - .0 is the
# network and .3 the broadcast address, leaving only the helper's .1 and the
# guest's own .2 - and the kernel rejects a broadcast address as a source before
# nft is reached. See report/exp6.md.
EG_SPOOF_SOURCE=192.0.2.3
# The source used by the one pass that relaxes rp_filter, which is the only
# configuration in which the nft anti-spoof rule can be observed at all.
EG_SPOOF_SOURCE_OFFNET=203.0.113.99

# Pinned rather than discovered: probes.py is handed the helper's uplink address
# on the command line, which is fixed before the container exists.
EG_HELPER_UPLINK_IP=10.89.0.90
EG_GATEWAY=10.89.0.1
# "the reactor's port": any host port that is open. A listener is started on it
# for the duration, so the probe distinguishes "dropped" from "nothing there".
EG_GATEWAY_PORT=9000

EG_SIBLING=spike-sibling
EG_PUBLIC_NAME=example.com
EG_RFC1918_NAME="$EG_SIBLING"
EG_METADATA_IP=169.254.169.254
EG_IPV6_IP=2606:4700:4700::1111
EG_INBOUND_PORT=34567

# testnet.py's two halves. DNS answers on the gateway address, which is the
# helper's own traffic and so never crosses the forward chain. The TCP sink and
# the short-TTL name's address are TEST-NET-3 /32s owned by the host on the
# bridge, because that IS guest traffic and 10/8 is in the baseline.
EG_TESTNET_DNS_PORT=5353
EG_TTL_TARGET_IP=203.0.113.5
EG_TTL_TARGET_PORT=8080
EG_SHORT_TTL_NAME=expiry.spike.invalid

# Experiment 8's fan-out destinations, declared as 203.0.113.0/24 in
# examples/ratelimit.json. The host owns each /32 and listens on none of them,
# so a SYN that arrives comes back RST in ~2 ms: "reached" is unambiguous and
# costs nothing.
EG_FANOUT_FIRST=11
EG_FANOUT_LAST=30

# Experiment 6's declared set. examples/declared.json declares the nginx /32 on
# port 443 only; 203.0.113.5 is the undeclared control, reachable (the host RSTs
# it) so that a timeout can only be the ruleset.
EG_UNDECLARED_IP="$EG_TTL_TARGET_IP"

# The workload says when it is done, then idles: the container dies with the
# workload and nft lives in the container, so the counters and the capture have
# to be read before it exits.
EG_DONE_MARKER=probes-finished
EG_DRAIN_SECS=20

# The bridge device carrying flow-connectors, discovered once the sibling
# container has brought it into existence.
EG_BRIDGE=

# Extra arguments for the podman run and for the helper, set per pass.
EG_PODMAN_ARGS=()
EG_HELPER_ARGS=()
EG_PROBE_ARGS=()

EG_WORK="$(mktemp -d)"
EG_FAILURES=0
EG_SANDBOXES="$EG_WORK/sandboxes"
: >"$EG_SANDBOXES"

say()  { printf '%s\n' "$*"; }
ok()   { printf 'ok    %s\n' "$*"; }
fail() { printf 'FAIL  %s\n' "$*"; EG_FAILURES=$((EG_FAILURES + 1)); }
step() { printf '\n== %s\n' "$*"; }

# ---------------------------------------------------------------- host fixtures

# The bridge that carries flow-connectors, so the TEST-NET-3 /32s land where the
# helper's default route reaches them.
eg_bridge() { sudo podman network inspect "$SPIKE_NET_CONNECTORS" --format '{{.NetworkInterface}}'; }

eg_fanout_ips() {
    local list="" octet
    for octet in $(seq "$EG_FANOUT_FIRST" "$EG_FANOUT_LAST"); do
        list="${list:+$list,}203.0.113.$octet"
    done
    printf '%s' "$list"
}

eg_fixtures_up() {
    local octet

    # Started first, and kept for the whole run: podman creates the bridge and
    # aardvark-dns binds the gateway address only while a container is on the
    # network, and the host fixtures below need both. Its aardvark name is also
    # the RFC1918 answer the resolver has to refuse.
    sudo podman rm -f "$EG_SIBLING" >/dev/null 2>&1 || true
    sudo podman run -d --rm --name "$EG_SIBLING" --network "$SPIKE_NET_CONNECTORS" \
        docker.io/library/busybox:latest sleep 3600 >/dev/null
    EG_BRIDGE="$(eg_bridge)"

    sudo ip addr add "$EG_TTL_TARGET_IP/32" dev "$EG_BRIDGE" 2>/dev/null || true
    for octet in $(seq "$EG_FANOUT_FIRST" "$EG_FANOUT_LAST"); do
        sudo ip addr add "203.0.113.$octet/32" dev "$EG_BRIDGE" 2>/dev/null || true
    done

    python3 "$EG_EGRESS_DIR/testnet.py" \
        --dns-listen "$EG_GATEWAY:$EG_TESTNET_DNS_PORT" --upstream "$EG_GATEWAY:53" \
        --name "$EG_SHORT_TTL_NAME" --address "$EG_TTL_TARGET_IP" --ttl 5 \
        --tcp-listen "$EG_TTL_TARGET_IP:$EG_TTL_TARGET_PORT" \
        >"$EG_WORK/testnet.log" 2>&1 &

    # Stands in for the reactor's own listener on the bridge gateway.
    python3 -c '
import socket, sys
# `python3 -c` puts "-c" at argv[0], so the tag the teardown greps for is
# argv[1] and the address follows it.
sock = socket.socket()
sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
sock.bind((sys.argv[2], int(sys.argv[3])))
sock.listen(8)
while True:
    peer, _ = sock.accept()
    peer.close()
' spike-gateway-listener "$EG_GATEWAY" "$EG_GATEWAY_PORT" \
        >"$EG_WORK/gateway-listener.log" 2>&1 &

    local _
    for _ in $(seq 40); do
        ss -lun 2>/dev/null | grep -q "$EG_GATEWAY:$EG_TESTNET_DNS_PORT" &&
            ss -ltn 2>/dev/null | grep -q "$EG_GATEWAY:$EG_GATEWAY_PORT" && return 0
        sleep 0.1
    done
    fail "host fixtures did not come up: $(cat "$EG_WORK/testnet.log" "$EG_WORK/gateway-listener.log")"
}

eg_fixtures_down() {
    local octet id
    pkill -f "testnet.py --dns-listen" 2>/dev/null || true
    pkill -f "spike-gateway-listener" 2>/dev/null || true
    sudo pkill -f "tcpdump -n -l -i" 2>/dev/null || true

    while read -r id; do
        [ -n "$id" ] || continue
        sudo podman rm -f "$id" >/dev/null 2>&1 || true
        sudo rm -rf "${SPIKE_REACTOR_DIR:?}/$id"
    done <"$EG_SANDBOXES"

    # Before the sibling goes, because the bridge - and every address on it -
    # disappears with the last container on the network.
    if [ -n "${EG_BRIDGE:-}" ]; then
        sudo ip addr del "$EG_TTL_TARGET_IP/32" dev "$EG_BRIDGE" 2>/dev/null || true
        for octet in $(seq "$EG_FANOUT_FIRST" "$EG_FANOUT_LAST"); do
            sudo ip addr del "203.0.113.$octet/32" dev "$EG_BRIDGE" 2>/dev/null || true
        done
    fi
    sudo podman rm -f "$EG_SIBLING" >/dev/null 2>&1 || true
    rm -rf "$EG_WORK"
}

# ---------------------------------------------------------------- the sandbox

# Plays the runtime: the per-connector directory CONTRACTS describes, plus
# probes.py under the venv where the guest will find it. Echoes the id.
eg_new_sandbox() {
    local policy="$1" id dir
    id="fs_$(head -c8 /dev/urandom | od -An -tx1 | tr -d ' \n')"
    dir="$SPIKE_REACTOR_DIR/$id"

    sudo mkdir -p "$dir/init" "$dir/sock" "$dir/scratch" "$dir/venv/spike"
    sudo podman inspect "$EG_GUEST_IMAGE" | sudo tee "$dir/init/image-inspect.json" >/dev/null
    sudo cp "$policy" "$dir/init/policy.json"
    sudo cp "$EG_EGRESS_DIR/probes.py" "$dir/venv/spike/probes.py"
    sudo chmod 644 "$dir/init/image-inspect.json" "$dir/init/policy.json" \
        "$dir/venv/spike/probes.py"

    # The shim opens flow-connector-init even when --exec replaces the workload,
    # so a file has to be there; nothing execs it here.
    printf '#!/bin/sh\nexit 125\n' | sudo tee "$dir/init/flow-connector-init" >/dev/null
    sudo chmod 755 "$dir/init/flow-connector-init"

    printf '%s\n' "$id" >>"$EG_SANDBOXES"
    printf '%s' "$id"
}

# PLAN "Helper launch", minus the labels and cgroup parent that only matter to
# the reactor, plus the pinned uplink address this harness needs.
eg_helper_argv() {
    local id="$1"
    shift
    local dir="$SPIKE_REACTOR_DIR/$id"
    printf '%s\n' \
        run --rm "--name=$id" "--network=$SPIKE_NET_CONNECTORS" "--ip=$EG_HELPER_UPLINK_IP" \
        --log-driver=none \
        --device /dev/kvm --device /dev/net/tun --cap-add NET_ADMIN \
        --sysctl net.ipv4.ip_forward=1 \
        "${EG_PODMAN_ARGS[@]}" \
        "$(spike_image_mount "$EG_GUEST_IMAGE")" \
        "--mount=type=bind,source=$dir/init,target=/init,ro" \
        "--mount=type=bind,source=$dir/venv,target=/venv,ro" \
        "--mount=type=bind,source=$dir/sock,target=/sock" \
        "--mount=type=bind,source=$dir/scratch,target=/scratch-backing" \
        "$SPIKE_HELPER_IMAGE" \
        --policy /init/policy.json --memory-mib 1024 --vcpus 2 --disk-mib 1024 \
        "$@"
}

eg_await_container() {
    local id="$1" _
    for _ in $(seq 150); do
        sudo podman exec "$id" true >/dev/null 2>&1 && return 0
        sleep 0.2
    done
    return 1
}

# The probes write to the workload's stderr, which CONTRACTS gives them byte for
# byte; stdout carries the kernel console interleaved and cannot be parsed.
eg_await_marker() {
    local tag="$1" pid="$2" _
    for _ in $(seq 1200); do
        grep -q "^$EG_DONE_MARKER" "$EG_WORK/$tag.err" 2>/dev/null && return 0
        # The guest idles for EG_DRAIN_SECS after the marker, so a helper that
        # has already exited will never write one.
        kill -0 "$pid" 2>/dev/null || return 1
        sleep 0.5
    done
    return 1
}

# Captures on the HOST side of the helper's veth, on the bridge that carries
# flow-connectors, rather than on eth0 inside the helper: podman 4.x drops
# CAP_NET_RAW from its default set, and adding it back would hand the helper a
# capability production does not give it and experiment 1 measured the absence
# of. The bridge sees every frame the helper's veth puts on it, so the claim is
# the same one, observed from outside the thing under test.
eg_start_capture() {
    local tag="$1" filter="$2"
    sudo tcpdump -n -l -i "$EG_BRIDGE" "$filter" \
        >"$EG_WORK/$tag.capture" 2>"$EG_WORK/$tag.capture.err" &
    # tcpdump must be attached before the first probe packet.
    local _
    for _ in $(seq 40); do
        grep -q 'listening on' "$EG_WORK/$tag.capture.err" 2>/dev/null && return 0
        sleep 0.1
    done
    fail "$tag: tcpdump did not attach: $(tail -1 "$EG_WORK/$tag.capture.err")"
}

# Connects to the guest's listener over and over for the whole run, so whichever
# second `inbound-listener` is binding gets knocked on. Each line is one attempt:
# the assertion is that none of them says `connected`.
eg_start_knocker() {
    local tag="$1" id="$2"
    sudo podman exec "$id" sh -c '
while :; do
    if socat -T1 - TCP:'"$EG_GUEST_IP:$EG_INBOUND_PORT"',connect-timeout=1 </dev/null >/dev/null 2>&1
    then echo connected; else echo failed; fi
done' >"$EG_WORK/$tag.knock" 2>/dev/null &
}

eg_snapshot_counters() {
    local tag="$1" id="$2"
    sudo podman exec "$id" nft -j list table inet flow_sandbox | python3 -c '
import json, sys
for item in json.load(sys.stdin)["nftables"]:
    rule = item.get("rule")
    if not rule:
        continue
    packets = 0
    for expression in rule["expr"]:
        if "counter" in expression:
            packets = expression["counter"]["packets"]
    print("%s\t%s\t%d" % (rule["chain"], rule.get("comment", ""), packets))
' >"$EG_WORK/$tag.counters"
    sudo podman exec "$id" sh -c 'cat /proc/sys/net/ipv4/conf/tap0/rp_filter' \
        >"$EG_WORK/$tag.rp_filter" 2>/dev/null || true
    # The ruleset as the kernel holds it, not as the binary printed it: the
    # reports quote the constructs the runtime has to copy.
    sudo podman exec "$id" nft list table inet flow_sandbox \
        >"$EG_WORK/$tag.ruleset" 2>/dev/null || true
}

eg_stop_watchers() {
    sudo pkill -f "tcpdump -n -l -i $EG_BRIDGE" 2>/dev/null || true
    sudo pkill -f "TCP:$EG_GUEST_IP:$EG_INBOUND_PORT" 2>/dev/null || true
    sleep 0.5
}

# eg_run TAG POLICY CAPTURE_FILTER
#
# Callers set EG_PODMAN_ARGS, EG_HELPER_ARGS and EG_PROBE_ARGS first, EG_KNOCK=1 when the set
# contains `inbound-listener`, and EG_PRE_EXEC to a command run inside the helper
# as soon as it is up. Returns probes.py's exit status.
eg_run() {
    local tag="$1" policy="$2" filter="$3"
    local id rc=0 guest_cmd helper_pid

    id="$(eg_new_sandbox "$policy")"
    # One line, because eg_helper_argv prints one argument per line and the
    # caller reads them back with mapfile: a newline here would split the script
    # across argv elements and probes.py would be handed the remainder.
    guest_cmd='rc=0; '"$EG_GUEST_PYTHON"' /venv/spike/probes.py "$@" >&2 || rc=$?;'
    guest_cmd="$guest_cmd printf '%s rc=%s\\n' $EG_DONE_MARKER \"\$rc\" >&2;"
    guest_cmd="$guest_cmd sleep $EG_DRAIN_SECS; exit \$rc"

    local argv=()
    mapfile -t argv < <(eg_helper_argv "$id" "${EG_HELPER_ARGS[@]}" \
        --exec /bin/sh -c "$guest_cmd" probes "${EG_PROBE_ARGS[@]}")

    "$SPIKE_TASKS_DIR/fake-reactor.sh" podman "${argv[@]}" \
        >"$EG_WORK/$tag.out" 2>"$EG_WORK/$tag.err" &
    helper_pid=$!

    if ! eg_await_container "$id"; then
        fail "$tag: the helper container never became executable"
        sed 's/^/      /' "$EG_WORK/$tag.err" | head -20
        wait "$helper_pid" || true
        return 1
    fi

    if [ -n "${EG_PRE_EXEC:-}" ]; then
        sudo podman exec "$id" sh -c "$EG_PRE_EXEC" >/dev/null 2>&1 ||
            fail "$tag: EG_PRE_EXEC failed: $EG_PRE_EXEC"
    fi
    eg_start_capture "$tag" "$filter"
    [ -n "${EG_KNOCK:-}" ] && eg_start_knocker "$tag" "$id"

    if eg_await_marker "$tag" "$helper_pid"; then
        eg_snapshot_counters "$tag" "$id"
    else
        fail "$tag: the probes never finished"
    fi
    eg_stop_watchers

    wait "$helper_pid" || rc=$?
    sudo rm -rf "${SPIKE_REACTOR_DIR:?}/$id"

    say ""
    say "--- probes: $tag (guest clock)"
    grep '^{"probe"' "$EG_WORK/$tag.err" | sed 's/^/      /'
    if [ "$rc" -eq 0 ]; then
        ok "$tag: every probe passed"
    else
        fail "$tag: $(grep -c '"pass": false' "$EG_WORK/$tag.err" || true) probe(s) failed (helper exit $rc)"
    fi
    say "--- counters: $tag (chain, rule comment, packets)"
    sed 's/^/      /' "$EG_WORK/$tag.counters" 2>/dev/null
    return "$rc"
}

# ---------------------------------------------------------------- assertions

# One probe's `result` field out of a run's JSON lines.
eg_probe_result() {
    python3 -c '
import json, sys
for line in open(sys.argv[1]):
    if line.startswith("{\"probe\""):
        row = json.loads(line)
        if row["probe"] == sys.argv[2]:
            print(row["result"])
' "$EG_WORK/$1.err" "$2"
}

eg_counter_of() {
    awk -v chain="$2" -v name="$3" -F'\t' '$1==chain && $2==name {print $3}' \
        "$EG_WORK/$1.counters" 2>/dev/null
}

eg_assert_counter() {
    local tag="$1" chain="$2" name="$3" value
    value="$(eg_counter_of "$tag" "$chain" "$name")"
    if [ -n "$value" ] && [ "$value" -gt 0 ]; then
        ok "$tag: counter $chain/$name = $value"
    else
        fail "$tag: counter $chain/$name = ${value:-absent}, expected above zero"
    fi
}

# PLAN experiment 6's second half of "must fail": not just that the guest saw an
# error, but that nothing reached the uplink.
eg_assert_capture_empty() {
    local tag="$1" count
    count="$(grep -c . "$EG_WORK/$tag.capture" 2>/dev/null || true)"
    if [ "${count:-0}" -eq 0 ]; then
        ok "$tag: no forbidden packet left the helper's uplink"
    else
        fail "$tag: $count packet(s) left the helper's uplink"
        head -5 "$EG_WORK/$tag.capture" | sed 's/^/      /'
    fi
}

eg_assert_no_inbound() {
    local tag="$1" connected attempts
    connected="$(grep -c '^connected' "$EG_WORK/$tag.knock" 2>/dev/null || true)"
    attempts="$(grep -c . "$EG_WORK/$tag.knock" 2>/dev/null || true)"
    if [ "${attempts:-0}" -lt 5 ]; then
        fail "$tag: the knocker made only ${attempts:-0} attempts; not evidence"
    elif [ "${connected:-0}" -eq 0 ]; then
        ok "$tag: socat from inside the helper failed on all $attempts attempts to $EG_GUEST_IP:$EG_INBOUND_PORT"
    else
        fail "$tag: socat reached the guest's listener $connected time(s)"
    fi
}

eg_report_rp_filter() {
    local tag="$1"
    say "      tap0 rp_filter inside the helper: $(cat "$EG_WORK/$tag.rp_filter" 2>/dev/null || echo unknown)"
}

# One CSV row per probe, for spike/report/data/. Written with the csv module
# because a `result` carries the addresses a name resolved to, commas included.
eg_write_csv() {
    local path="$1"
    shift
    printf 'set,probe,expect,result,pass,ms\n' >"$path"
    local tag
    for tag in "$@"; do
        grep '^{"probe"' "$EG_WORK/$tag.err" 2>/dev/null | python3 -c '
import csv, json, sys
writer = csv.writer(sys.stdout, lineterminator="\n")
for line in sys.stdin:
    row = json.loads(line)
    writer.writerow([sys.argv[1], row["probe"], row["expect"], row["result"],
                     row["pass"], row["ms"]])
' "$tag" >>"$path"
    done
}

eg_finish() {
    local name="$1"
    printf '\n'
    if [ "$EG_FAILURES" -ne 0 ]; then
        printf '%s: %d failure(s)\n' "$name" "$EG_FAILURES" >&2
        exit 1
    fi
    printf '%s: ok\n' "$name"
}
