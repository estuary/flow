#!/usr/bin/env bash
# Experiment 6: the egress rules proven from inside a real guest.
#
# WP02 proved the ruleset in a veth namespace pair. This runs the same probe
# suite as the connector's own user, inside a libkrun guest, across the real tap,
# behind the real helper - the only arrangement in which "the guest cannot reach
# X" is a claim about the thing we intend to ship.
#
# Three passes:
#   public/user  the whole `public` set as the image's user (nobody), including
#                the TTL probes that wait out the 90 second clamp floor.
#   public/root  the same set with --run-as-root, which adds the two probes that
#                need raw sockets: ICMP, and a UDP datagram with a forged source
#                from inside the tap's /30. Guest root is still just the guest.
#   spoof/nft    the forged-source probe again, from a source outside the /30 and
#                with rp_filter relaxed on the helper: the only configuration in
#                which nft's anti-spoof rule can be observed firing at all, and
#                deliberately not the one production runs. See report/exp6.md.
#   declared     declaredCidrs: the declared port reachable without resolving,
#                any other port and any other address not.
#
# Pass: every probe passes in every pass, and no forbidden packet reaches the
# helper's uplink.
#
#   spike/tasks/exp6-egress.sh
set -euo pipefail
source "$(dirname "${BASH_SOURCE[0]}")/exp6-common.sh"

trap eg_fixtures_down EXIT

# The public set's must-fail destinations, as seen on the bridge: everything
# sourced from the helper's uplink (so the sibling container's own traffic and
# the replies coming back are not counted), plus anything still sourced from the
# tap, which would mean a guest packet that escaped the masquerade. The TTL
# target (203.0.113.5) is deliberately absent - the guest is supposed to reach
# it, once it has resolved the name.
PUBLIC_FILTER="(src host $EG_HELPER_UPLINK_IP and ( \
tcp dst port 25 \
or (dst host $EG_METADATA_IP and tcp) \
or dst host $SPIKE_NGINX_IP \
or (dst host $EG_GATEWAY and tcp dst port $EG_GATEWAY_PORT) \
or icmp)) \
or (ip6 and tcp) or src net 192.0.2.0/30"

# The forged datagram, whether it escaped masqueraded or as it was built, and
# the ICMP echo that `--root-probes only` runs alongside it.
SPOOF_FILTER="(src host $EG_HELPER_UPLINK_IP and (dst host $SPIKE_NGINX_IP or icmp)) \
or src net 192.0.2.0/30 or src host $EG_SPOOF_SOURCE_OFFNET"

DECLARED_FILTER="(src host $EG_HELPER_UPLINK_IP and ( \
dst host $EG_UNDECLARED_IP \
or (dst host $SPIKE_NGINX_IP and tcp dst port 80))) \
or src net 192.0.2.0/30"

public_probe_args=(
    --set public
    --helper-ip "$EG_HELPER_IP" --uplink-ip "$EG_HELPER_UPLINK_IP"
    --gateway "$EG_GATEWAY" --gateway-port "$EG_GATEWAY_PORT"
    --public-name "$EG_PUBLIC_NAME" --rfc1918-name "$EG_RFC1918_NAME"
    --short-ttl-name "$EG_SHORT_TTL_NAME" --ttl-port "$EG_TTL_TARGET_PORT"
    --nginx-ip "$SPIKE_NGINX_IP" --metadata-ip "$EG_METADATA_IP"
    --ipv6-ip "$EG_IPV6_IP" --spoof-source "$EG_SPOOF_SOURCE"
    --inbound-port "$EG_INBOUND_PORT" --inbound-wait 5
)

say "experiment 6: egress from inside the guest"
say "  guest image:  $EG_GUEST_IMAGE"
say "  helper:       $SPIKE_HELPER_IMAGE on $SPIKE_NET_CONNECTORS at $EG_HELPER_UPLINK_IP"
say "  resolver:     testnet.py on $EG_GATEWAY:$EG_TESTNET_DNS_PORT -> aardvark-dns on $EG_GATEWAY:53"
say "  short TTL:    $EG_SHORT_TTL_NAME -> $EG_TTL_TARGET_IP:$EG_TTL_TARGET_PORT, TTL 5s"
eg_fixtures_up

# ------------------------------------------------------- public, as the image's user

step "public, as the connector image's own user"
EG_HELPER_ARGS=(--resolver-upstream "$EG_GATEWAY:$EG_TESTNET_DNS_PORT")
EG_PROBE_ARGS=("${public_probe_args[@]}" --slow)
EG_KNOCK=1
eg_run public-user "$EG_EGRESS_DIR/examples/public.json" "$PUBLIC_FILTER" || true
unset EG_KNOCK

eg_assert_capture_empty public-user
eg_assert_no_inbound public-user
eg_assert_counter public-user forward baseline
eg_assert_counter public-user forward smtp
eg_assert_counter public-user forward forward-drop
eg_assert_counter public-user egress_accept resolved
eg_assert_counter public-user input tap-dns
eg_assert_counter public-user input input-drop
eg_assert_counter public-user output no-inbound

# The forward chain's `no-ipv6` rule is a backstop that never fires, and that is
# the result rather than a gap in it: flow-init turns IPv6 off in the guest, so
# the socket cannot be created at all and no packet is ever offered to the tap.
# Two independent controls, and the outer one is not reached.
ipv6_result="$(eg_probe_result public-user connect-ipv6)"
if [ "$ipv6_result" = "error:EADDRNOTAVAIL" ]; then
    ok "public-user: IPv6 stopped in the guest ($ipv6_result); forward/no-ipv6 = $(eg_counter_of public-user forward no-ipv6), never reached"
else
    fail "public-user: connect-ipv6 gave $ipv6_result, expected the guest's own IPv6 to be off"
fi

# ------------------------------------------------------- public, as guest root

step "public, as guest root (adds the two raw-socket probes)"
EG_HELPER_ARGS=(--resolver-upstream "$EG_GATEWAY:$EG_TESTNET_DNS_PORT" --run-as-root)
EG_PROBE_ARGS=("${public_probe_args[@]}")
eg_run public-root "$EG_EGRESS_DIR/examples/public.json" "$PUBLIC_FILTER" || true

eg_assert_capture_empty public-root
eg_assert_counter public-root forward tcp-udp-only
eg_report_rp_filter public-root

# The spoofed source sits inside the tap's /30 so that rp_filter, whatever the
# helper inherited, cannot pre-empt the rule under test. If it is strict anyway,
# a zero counter is the kernel's doing and not a failure - the claim that holds
# either way is the empty capture above (master's answer to WP02).
rp_filter="$(cat "$EG_WORK/public-root.rp_filter" 2>/dev/null || echo unknown)"
anti_spoof="$(eg_counter_of public-root forward anti-spoof)"
if [ "$rp_filter" = "0" ]; then
    eg_assert_counter public-root forward anti-spoof
elif [ "${anti_spoof:-0}" -gt 0 ]; then
    ok "public-root: counter forward/anti-spoof = $anti_spoof (rp_filter=$rp_filter, and nft still saw it)"
else
    ok "public-root: forward/anti-spoof = ${anti_spoof:-absent} with rp_filter=$rp_filter; the kernel dropped it first"
fi

# ------------------------------------ the anti-spoof rule, made observable

# Two things stop a forged source before nft in the configuration above, and
# both are the kernel's: 192.0.2.3 is the /30's broadcast address, which is a
# martian source whatever rp_filter says, and any source outside the /30 fails
# the strict rp_filter the helper inherits from the host. So the nft rule is a
# third line of defence that production never reaches. This pass relaxes
# rp_filter at container creation (podman mounts /proc/sys read-only, so it
# cannot be done later) and forges an ordinary off-net source, which is the only
# way to see the rule do its job.
step "anti-spoof, with rp_filter relaxed so nft is the thing under test"
EG_PODMAN_ARGS=(--sysctl net.ipv4.conf.all.rp_filter=0 --sysctl net.ipv4.conf.default.rp_filter=0)
EG_HELPER_ARGS=(--run-as-root)
EG_PROBE_ARGS=("${public_probe_args[@]}" --root-probes only --spoof-source "$EG_SPOOF_SOURCE_OFFNET")
eg_run spoof-nft "$EG_EGRESS_DIR/examples/public.json" "$SPOOF_FILTER" || true
EG_PODMAN_ARGS=()

eg_assert_capture_empty spoof-nft
eg_report_rp_filter spoof-nft
eg_assert_counter spoof-nft forward anti-spoof
eg_assert_counter spoof-nft forward tcp-udp-only

# ------------------------------------------------------- declared CIDRs

step "declaredCidrs"
EG_HELPER_ARGS=()
EG_PROBE_ARGS=(
    --set declared
    --nginx-ip "$SPIKE_NGINX_IP" --nginx-port 443 --nginx-other-port 80
    --undeclared-ip "$EG_UNDECLARED_IP"
)
eg_run declared "$EG_EGRESS_DIR/examples/declared.json" "$DECLARED_FILTER" || true

eg_assert_capture_empty declared
eg_assert_counter declared egress_accept declared
eg_assert_counter declared forward forward-drop

step "raw probe data"
eg_write_csv "$SPIKE_DIR/report/data/exp6-probes.csv" public-user public-root spoof-nft declared
say "  wrote $SPIKE_DIR/report/data/exp6-probes.csv"

eg_finish exp6-egress.sh
