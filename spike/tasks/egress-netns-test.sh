#!/usr/bin/env bash
# Prove the egress ruleset, the resolver and the probe suite on this host with
# no VM anywhere: two network namespaces stand in for the helper container and
# the guest, joined by a veth pair that plays the tap.
#
#   egress-netns-test.sh                every set, including the slow probes
#   egress-netns-test.sh --quick        skip the probes that wait out a TTL or a rate window
#   egress-netns-test.sh --set public   one set only
#
# Topology. Every address is TEST-NET-1/2/3 or the box's own RFC1918 space, and
# the helper's uplink deliberately sits inside 10/8 the way podman's
# `flow-connectors` bridge does:
#
#   [host] spike-eg-up0 10.89.222.1/30 ......... eth0 10.89.222.2/30 [helper ns]
#          + 203.0.113.11-30/32 (fan-out                tap0 192.0.2.1/30
#            destinations: no listener, so                   |
#            a reached SYN comes back RST)                   |
#          + 203.0.113.5:8080 (testnet.py's patient          |
#            TCP server; its DNS listens on                   |
#            10.89.222.1:5353)                                |
#          + masquerade out to the internet                  |
#          podman2 198.51.100.1/24 - spike-nginx      eth0 192.0.2.2/30 [guest ns]
#            198.51.100.10:443,:80
#
# The guest namespace is the connector: it runs probes.py and nothing else. The
# helper namespace is the sandbox helper: it runs flow-sandbox-egress and
# flow-sandbox-resolver, exactly as the shim runs them, and holds the ruleset
# under test.
set -euo pipefail
source "$(dirname "${BASH_SOURCE[0]}")/env-common.sh"

EGRESS_DIR="$SPIKE_DIR/egress"
BIN_DIR="$EGRESS_DIR/target/release"

NS_HELPER=spike-eg-helper
NS_GUEST=spike-eg-guest
HOST_UPLINK=spike-eg-up0
TMP_HELPER_ETH=spike-eg-h0
TMP_TAP=spike-eg-t0
TMP_GUEST_ETH=spike-eg-g0

UPLINK_SUBNET=10.89.222.0/30
HOST_IP=10.89.222.1
HELPER_UPLINK_IP=10.89.222.2
HELPER_IP=192.0.2.1
GUEST_IP=192.0.2.2
GATEWAY_PORT=9000

FANOUT_NET=203.0.113
FANOUT_FIRST=11
FANOUT_LAST=30
# testnet.py's TCP sink, and the address its one synthetic name resolves to.
TTL_TARGET_IP=203.0.113.5
TESTNET_DNS_PORT=5353
# Unprivileged: testnet.py runs as whoever ran this script.
TTL_TARGET_PORT=8080
UNDECLARED_IP=198.51.100.11
METADATA_IP=169.254.169.254
IPV6_IP=2606:4700:4700::1111
SPOOF_SOURCE=198.51.100.99
INBOUND_PORT=34567

# Real names, resolved through the real upstream behind testnet.py: the point
# of the exercise is a resolver in front of DNS as it actually behaves. The
# RFC1918 name is this box's own, read at run time so no hostname is checked in.
PUBLIC_NAME=example.com
RFC1918_NAME="$(hostname -f)"
# The one name testnet.py answers for: a 5 second TTL that stays 5 seconds,
# pointing at a server that will not hang up during the 90 second wait. Every
# other name in the run is resolved by the real upstream behind it.
SHORT_TTL_NAME=expiry.spike.invalid

SLOW=--slow
ONLY_SET=
while [ $# -gt 0 ]; do
    case "$1" in
    --quick) SLOW="" && shift ;;
    --set) ONLY_SET="$2" && shift 2 ;;
    *) echo "usage: egress-netns-test.sh [--quick] [--set public|none|declared|ratelimit]" >&2
       exit 2 ;;
    esac
done

WORK="$(mktemp -d)"
FAILURES=0
say()  { printf '%s\n' "$*"; }
ok()   { printf 'ok    %s\n' "$*"; }
fail() { printf 'FAIL  %s\n' "$*"; FAILURES=$((FAILURES + 1)); }

in_helper() { sudo ip netns exec "$NS_HELPER" "$@"; }
in_guest()  { sudo ip netns exec "$NS_GUEST" "$@"; }

# ---------------------------------------------------------------- teardown

teardown() {
    sudo pkill -f 'flow-sandbox-resolver --policy' 2>/dev/null || true
    sudo pkill -f spike-eg-knocker 2>/dev/null || true
    pkill -f "testnet.py --dns-listen" 2>/dev/null || true
    sudo pkill -f "tcpdump.*$NS_HELPER-capture" 2>/dev/null || true

    sudo ip netns del "$NS_GUEST" 2>/dev/null || true
    sudo ip netns del "$NS_HELPER" 2>/dev/null || true
    for link in "$HOST_UPLINK" "$TMP_HELPER_ETH" "$TMP_TAP" "$TMP_GUEST_ETH"; do
        sudo ip link del "$link" 2>/dev/null || true
    done
    sudo rm -rf "/etc/netns/$NS_GUEST"

    # Inserted at the head of the host's chains, which docker and netavark also
    # own; deleting by rule text leaves their rules alone.
    sudo iptables -D FORWARD -s "$UPLINK_SUBNET" -j ACCEPT 2>/dev/null || true
    sudo iptables -D FORWARD -d "$UPLINK_SUBNET" -j ACCEPT 2>/dev/null || true
    sudo iptables -t nat -D POSTROUTING -s "$UPLINK_SUBNET" \
        ! -d "$UPLINK_SUBNET" -j MASQUERADE 2>/dev/null || true
}
trap 'teardown; rm -rf "$WORK"' EXIT

# ---------------------------------------------------------------- setup

setup() {
    teardown  # whatever an aborted earlier run left behind

    sudo ip netns add "$NS_HELPER"
    sudo ip netns add "$NS_GUEST"

    sudo ip link add "$HOST_UPLINK" type veth peer name "$TMP_HELPER_ETH"
    sudo ip link set "$TMP_HELPER_ETH" netns "$NS_HELPER" name eth0
    sudo ip addr add "$HOST_IP/30" dev "$HOST_UPLINK"
    sudo ip link set "$HOST_UPLINK" up
    sudo ip addr add "$TTL_TARGET_IP/32" dev "$HOST_UPLINK"
    for octet in $(seq "$FANOUT_FIRST" "$FANOUT_LAST"); do
        sudo ip addr add "$FANOUT_NET.$octet/32" dev "$HOST_UPLINK"
    done

    sudo ip link add "$TMP_TAP" type veth peer name "$TMP_GUEST_ETH"
    sudo ip link set "$TMP_TAP" netns "$NS_HELPER" name tap0
    sudo ip link set "$TMP_GUEST_ETH" netns "$NS_GUEST" name eth0

    in_helper ip link set lo up
    in_helper ip addr add "$HELPER_UPLINK_IP/30" dev eth0
    in_helper ip link set eth0 up
    in_helper ip addr add "$HELPER_IP/30" dev tap0
    in_helper ip link set tap0 up
    in_helper ip route add default via "$HOST_IP"
    in_helper sysctl -qw net.ipv4.ip_forward=1
    # Reverse-path filtering would drop the spoofed-source probe before nft
    # ever sees it, and nft is what this test is about.
    in_helper sysctl -qw net.ipv4.conf.all.rp_filter=0
    in_helper sysctl -qw net.ipv4.conf.tap0.rp_filter=0

    in_guest ip link set lo up
    in_guest ip addr add "$GUEST_IP/30" dev eth0
    in_guest ip link set eth0 up
    in_guest ip route add default via "$HELPER_IP"

    # `ip netns exec` bind-mounts this over /etc/resolv.conf, which is how the
    # guest ends up with the helper as its only nameserver, as flow-init writes
    # it in the real thing.
    sudo mkdir -p "/etc/netns/$NS_GUEST"
    printf 'nameserver %s\n' "$HELPER_IP" | sudo tee "/etc/netns/$NS_GUEST/resolv.conf" >/dev/null

    start_testnet
    sudo iptables -I FORWARD 1 -s "$UPLINK_SUBNET" -j ACCEPT
    sudo iptables -I FORWARD 1 -d "$UPLINK_SUBNET" -j ACCEPT
    sudo iptables -t nat -I POSTROUTING 1 -s "$UPLINK_SUBNET" \
        ! -d "$UPLINK_SUBNET" -j MASQUERADE
}

# The stand-in internet: one synthetic name and one patient TCP server, with
# everything else forwarded to the box's real resolver. Host namespace, bound
# only to addresses the helper reaches.
start_testnet() {
    python3 "$EGRESS_DIR/testnet.py" \
        --dns-listen "$HOST_IP:$TESTNET_DNS_PORT" --upstream "$(real_nameserver)" \
        --name "$SHORT_TTL_NAME" --address "$TTL_TARGET_IP" --ttl 5 \
        --tcp-listen "$TTL_TARGET_IP:$TTL_TARGET_PORT" >"$WORK/testnet.log" 2>&1 &
    for _ in $(seq 20); do
        ss -lun 2>/dev/null | grep -q "$HOST_IP:$TESTNET_DNS_PORT" && return 0
        sleep 0.1
    done
    fail "testnet.py did not start: $(cat "$WORK/testnet.log")"
}

# The box's real resolver, which testnet.py forwards to and the helper never
# talks to directly. The systemd stub at 127.0.0.53 is useless from another
# namespace, so this reads what resolved itself forwards to.
real_nameserver() {
    local address
    address="$(resolvectl status 2>/dev/null \
        | awk '/Current DNS Server:/ { print $4; exit }')"
    if [ -z "$address" ]; then
        address="$(awk '/^nameserver/ && $2 !~ /^127\./ { print $2; exit }' /etc/resolv.conf)"
    fi
    [ -n "$address" ] || { echo "no usable upstream nameserver" >&2; exit 2; }
    printf '%s:53' "$address"
}

# ---------------------------------------------------------------- the run

load_policy() {
    local policy="$1"
    in_helper "$BIN_DIR/flow-sandbox-egress" --policy "$policy" \
        --tap tap0 --uplink eth0 --guest-ip "$GUEST_IP" --helper-ip "$HELPER_IP"
}

start_resolver() {
    local policy="$1" log="$2"
    : >"$log"
    in_helper "$BIN_DIR/flow-sandbox-resolver" --policy "$policy" \
        --listen "$HELPER_IP:53" --upstream "$HOST_IP:$TESTNET_DNS_PORT" --debug \
        >"$log" 2>&1 &
    # The resolver has to own the socket before the first probe resolves.
    for _ in $(seq 20); do
        grep -q 'listening on' "$log" && return 0
        sleep 0.1
    done
    fail "resolver did not start: $(tail -1 "$log")"
}

# Connects to the guest from the helper, over and over, for as long as the
# probes run: the inbound-listener probe passes only if none of these lands.
start_knocker() {
    in_helper python3 -c '
import socket, sys, time
while True:
    sock = socket.socket(); sock.settimeout(0.5)
    try:
        sock.connect((sys.argv[2], int(sys.argv[3])))
    except OSError:
        pass
    sock.close()
    time.sleep(0.2)
' spike-eg-knocker "$GUEST_IP" "$INBOUND_PORT" >/dev/null 2>&1 &
}

start_capture() {
    local name="$1" filter="$2"
    in_helper tcpdump -n -i eth0 -U -w "$WORK/$NS_HELPER-capture-$name.pcap" \
        "$filter" >/dev/null 2>&1 &
    sleep 0.5  # tcpdump must be attached before the first probe packet
}

# Packets that left the helper's uplink at all, whatever the guest thinks
# happened: PLAN experiment 6's second half of "must fail".
assert_capture_empty() {
    local name="$1" file="$WORK/$NS_HELPER-capture-$name.pcap"
    sudo pkill -f "tcpdump.*$NS_HELPER-capture" 2>/dev/null || true
    sleep 0.5
    local count
    count="$(sudo tcpdump -n -r "$file" 2>/dev/null | wc -l)"
    if [ "$count" -eq 0 ]; then
        ok "$name: nothing forbidden left the helper's uplink"
    else
        fail "$name: $count packets left the helper's uplink"
        sudo tcpdump -n -r "$file" 2>/dev/null | head -5 | sed 's/^/      /'
    fi
}

# comment -> packet count, for every counter in the loaded table.
counters() {
    in_helper nft -j list table inet flow_sandbox | python3 -c '
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
'
}

counter_of() { awk -v chain="$1" -v name="$2" -F'\t' '$1==chain && $2==name {print $3}' "$WORK/counters"; }

assert_counter() {
    local chain="$1" name="$2" value
    value="$(counter_of "$chain" "$name")"
    if [ -n "$value" ] && [ "$value" -gt 0 ]; then
        ok "counter $chain/$name = $value"
    else
        fail "counter $chain/$name = ${value:-absent}, expected above zero"
    fi
}

run_probes() {
    local name="$1"
    shift
    say ""
    say "--- probes: $name"
    if in_guest python3 "$EGRESS_DIR/probes.py" "$@" | tee "$WORK/probes-$name.jsonl"; then
        ok "$name: every probe passed"
    else
        fail "$name: $(grep -c '"pass": false' "$WORK/probes-$name.jsonl") probe(s) failed"
    fi
    counters >"$WORK/counters"
    say "--- counters: $name (chain, rule comment, packets)"
    sed 's/^/      /' "$WORK/counters"
}

fanout_ips() {
    local list=""
    for octet in $(seq "$FANOUT_FIRST" "$FANOUT_LAST"); do
        list="${list:+$list,}$FANOUT_NET.$octet"
    done
    printf '%s' "$list"
}

# ---------------------------------------------------------------- sets

set_public() {
    load_policy "$EGRESS_DIR/examples/public.json"
    start_resolver "$EGRESS_DIR/examples/public.json" "$WORK/resolver-public.log"
    start_knocker
    start_capture public "(dst host $METADATA_IP and tcp) or dst host $SPIKE_NGINX_IP"

    run_probes public --set public \
        --helper-ip "$HELPER_IP" --uplink-ip "$HELPER_UPLINK_IP" \
        --gateway "$HOST_IP" --gateway-port "$GATEWAY_PORT" \
        --public-name "$PUBLIC_NAME" --rfc1918-name "$RFC1918_NAME" \
        --short-ttl-name "$SHORT_TTL_NAME" \
        --nginx-ip "$SPIKE_NGINX_IP" --metadata-ip "$METADATA_IP" \
        --ipv6-ip "$IPV6_IP" --spoof-source "$SPOOF_SOURCE" \
        --inbound-port "$INBOUND_PORT" --ttl-port "$TTL_TARGET_PORT" $SLOW

    sudo pkill -f spike-eg-knocker 2>/dev/null || true
    assert_capture_empty public
    assert_counter forward anti-spoof
    assert_counter forward baseline
    assert_counter forward smtp
    assert_counter forward tcp-udp-only
    assert_counter forward forward-drop
    assert_counter egress_accept resolved
    assert_counter input tap-dns
    assert_counter input input-drop
    assert_counter output no-inbound
    say "--- resolver log (public)"
    sed 's/^/      /' "$WORK/resolver-public.log"
    sudo pkill -f 'flow-sandbox-resolver --policy' 2>/dev/null || true
}

set_none() {
    load_policy "$EGRESS_DIR/examples/none.json"
    start_capture none ip

    run_probes none --set none \
        --public-name "$PUBLIC_NAME" --nginx-ip "$SPIKE_NGINX_IP" \
        --metadata-ip "$METADATA_IP" --timeout 15

    assert_capture_empty none
    assert_counter input input-drop
    assert_counter forward forward-drop
}

set_declared() {
    load_policy "$EGRESS_DIR/examples/declared.json"
    start_resolver "$EGRESS_DIR/examples/declared.json" "$WORK/resolver-declared.log"
    start_capture declared \
        "dst host $UNDECLARED_IP or (dst host $SPIKE_NGINX_IP and tcp dst port 80)"

    run_probes declared --set declared \
        --nginx-ip "$SPIKE_NGINX_IP" --undeclared-ip "$UNDECLARED_IP"

    assert_capture_empty declared
    assert_counter egress_accept declared
    assert_counter forward forward-drop
    sudo pkill -f 'flow-sandbox-resolver --policy' 2>/dev/null || true
}

set_ratelimit() {
    load_policy "$EGRESS_DIR/examples/ratelimit.json"
    start_resolver "$EGRESS_DIR/examples/ratelimit.json" "$WORK/resolver-ratelimit.log"

    run_probes ratelimit --set ratelimit \
        --fanout-ips "$(fanout_ips)" --fanout-port 443 \
        --dest-limit 5 --rate-limit 60 --rate-attempts 80 --timeout 2 $SLOW

    assert_counter forward fan-out
    assert_counter forward rate-limit
    assert_counter forward forward-drop
    sudo pkill -f 'flow-sandbox-resolver --policy' 2>/dev/null || true
}

# The resolver spawns one `nft` per answered query; the runtime will not, so
# the report needs the number it is trading away.
measure_nft_cost() {
    load_policy "$EGRESS_DIR/examples/public.json"
    # Timed inside the namespace, so the number is nft's and not `ip netns exec`'s.
    in_helper bash -c '
started=$(date +%s%N)
for octet in $(seq 1 20); do
    nft add element inet flow_sandbox resolved "{ 203.0.113.$octet timeout 90s }"
done
elapsed=$(( ($(date +%s%N) - started) / 1000000 ))
printf "nft add element x20: %sms total, %sms each, %s\n" \
    "$elapsed" "$((elapsed / 20))" "$(nft --version)"
'
}

# ---------------------------------------------------------------- main

bash "$SPIKE_TASKS_DIR/egress-build.sh"
say "topology: helper=$NS_HELPER guest=$NS_GUEST uplink=$UPLINK_SUBNET tap=192.0.2.0/30"
say "upstream nameserver: testnet.py on $HOST_IP:$TESTNET_DNS_PORT, forwarding to $(real_nameserver)"
say "rfc1918 name: $RFC1918_NAME (this box, resolved at run time)"
setup

if ! sudo podman ps --format '{{.Names}}' | grep -qx "$SPIKE_NGINX_NAME"; then
    fail "$SPIKE_NGINX_NAME is not running; the declared set needs it (spike/tasks/env-setup.sh)"
fi

case "${ONLY_SET:-public}" in
public | none | declared | ratelimit) ;;
*) echo "unknown probe set ${ONLY_SET}" >&2 && exit 2 ;;
esac

for name in public none declared ratelimit; do
    if [ -n "$ONLY_SET" ] && [ "$ONLY_SET" != "$name" ]; then
        continue
    fi
    say ""
    say "=== set: $name"
    "set_$name"
done
say ""
measure_nft_cost

say ""
if [ "$FAILURES" -eq 0 ]; then
    say "PASS: every probe and every counter assertion"
else
    say "FAIL: $FAILURES check(s)"
fi
exit $((FAILURES > 0))
