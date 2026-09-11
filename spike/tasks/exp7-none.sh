#!/usr/bin/env bash
# Experiment 7: `egress: none` from inside a real guest.
#
# The whole policy is "the guest has no network", and the point of measuring it
# is the cost: every failure is a drop, so nothing is refused and the guest pays
# its resolver's and its kernel's full retry budget before it finds out. Those
# seconds are an accepted cost of the design and belong in the report.
#
# Pass: the DNS lookup and both connects fail, and no IP packet at all leaves the
# helper's uplink for the duration.
#
#   spike/tasks/exp7-none.sh
set -euo pipefail
source "$(dirname "${BASH_SOURCE[0]}")/exp6-common.sh"

trap eg_fixtures_down EXIT

# Nothing whatsoever that could be the guest's: with no resolver running the
# helper has no traffic of its own either, so the claim is the strongest one
# available - not one IP packet from the helper's uplink address, and nothing
# still sourced from the tap. IPv6 is narrowed to globally routable destinations
# on purpose: the bridge carries a steady trickle of link-local ICMPv6 (router
# solicitations, MLD reports) from veth devices appearing and disappearing as
# containers come and go, which is the host's housekeeping and not egress.
NONE_FILTER="(ip and (src host $EG_HELPER_UPLINK_IP or src net 192.0.2.0/30)) \
or (ip6 and dst net 2000::/3)"

# The cap the probes give a connect. A dropped SYN is not refused, so the kernel
# would take ~127 s (tcp_syn_retries=6) to give up; what the probe reports is
# therefore a lower bound, and the report says so.
CONNECT_CAP=15

say "experiment 7: egress none"
say "  guest image:  $EG_GUEST_IMAGE"
say "  helper:       $SPIKE_HELPER_IMAGE on $SPIKE_NET_CONNECTORS at $EG_HELPER_UPLINK_IP"
say "  connect cap:  ${CONNECT_CAP}s"
eg_fixtures_up

step "egress: none"
EG_HELPER_ARGS=()
EG_PROBE_ARGS=(
    --set none
    --public-name "$EG_PUBLIC_NAME"
    --nginx-ip "$SPIKE_NGINX_IP" --nginx-port 443
    --metadata-ip "$EG_METADATA_IP"
    --timeout "$CONNECT_CAP"
)
eg_run none "$EG_EGRESS_DIR/examples/none.json" "$NONE_FILTER" || true

eg_assert_capture_empty none
eg_assert_counter none input input-drop
eg_assert_counter none forward forward-drop

# The resolver is the one thing `none` removes outright, so its absence is worth
# asserting rather than assuming: a resolver that had started would have written
# a listening line to the helper's stderr.
if grep -q 'flow-sandbox-resolver' "$EG_WORK/none.err"; then
    fail "none: a resolver started; CONTRACTS says none runs without one"
else
    ok "none: no resolver started"
fi

step "what each failure cost the guest"
python3 - "$EG_WORK/none.err" <<'PY'
import json, sys

for line in open(sys.argv[1]):
    if not line.startswith('{"probe"'):
        continue
    row = json.loads(line)
    print("      %-16s %-10s %-14s %6.1f s" % (
        row["probe"], row["expect"], row["result"], row["ms"] / 1000.0))
PY

step "raw probe data"
eg_write_csv "$SPIKE_DIR/report/data/exp7-probes.csv" none
say "  wrote $SPIKE_DIR/report/data/exp7-probes.csv"

eg_finish exp7-none.sh
