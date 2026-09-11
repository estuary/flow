#!/usr/bin/env bash
# Experiment 8: the rate and fan-out limits, from inside a real guest.
#
# The two limits fail in opposite ways, which is the whole finding:
#
#   connectionsPerMinute        `limit rate over` drops the SYN. TCP retransmits
#                               it a second later into a bucket that has refilled
#                               by then, so the guest sees SLOW connections, not
#                               errors. What is asserted is the sustained rate.
#   distinctDestinationsPerMinute  a dynamic set with `size N` and a timeout. The
#                               N+1st destination cannot be added, the rule's
#                               verdict never runs, and the packet falls to the
#                               chain's drop policy: a connect timeout.
#
# Twenty fan-out destinations are TEST-NET-3 /32s the host owns and listens on
# none of, declared as 203.0.113.0/24 in examples/ratelimit.json: a SYN that
# arrives is refused in ~2 ms, so "reached" is unambiguous and costs nothing.
#
# Pass: the first five destinations are reached and the rest are not, the
# sustained rate is at the configured limit, and both recover after the window.
#
#   spike/tasks/exp8-limits.sh
set -euo pipefail
source "$(dirname "${BASH_SOURCE[0]}")/exp6-common.sh"

trap eg_fixtures_down EXIT

DEST_LIMIT=5
RATE_LIMIT=60
RATE_ATTEMPTS=80
# Index 18 of the twenty: past the fan-out limit, and the one destination no
# recovery probe revisits after the window, so an empty capture is unambiguous.
BLOCKED_DEST=203.0.113.29
FANOUT_FILTER="src host $EG_HELPER_UPLINK_IP and dst host $BLOCKED_DEST"

say "experiment 8: rate and fan-out limits"
say "  guest image:  $EG_GUEST_IMAGE"
say "  policy:       connectionsPerMinute=$RATE_LIMIT distinctDestinationsPerMinute=$DEST_LIMIT"
say "  fan-out:      203.0.113.$EG_FANOUT_FIRST-$EG_FANOUT_LAST, declared 203.0.113.0/24:443"
eg_fixtures_up

step "both limits"
EG_HELPER_ARGS=()
EG_PROBE_ARGS=(
    --set ratelimit
    --fanout-ips "$(eg_fanout_ips)" --fanout-port 443
    --dest-limit "$DEST_LIMIT" --rate-limit "$RATE_LIMIT" --rate-attempts "$RATE_ATTEMPTS"
    --timeout 2 --slow
)
eg_run ratelimit "$EG_EGRESS_DIR/examples/ratelimit.json" "$FANOUT_FILTER" || true

eg_assert_capture_empty ratelimit
eg_assert_counter ratelimit forward fan-out
eg_assert_counter ratelimit forward rate-limit
eg_assert_counter ratelimit forward forward-drop

step "the nft constructs, verbatim, as the kernel holds them"
sed -n '/chain forward/,/^	}/p' "$EG_WORK/ratelimit.ruleset" | sed 's/^/      /'
say ""
sed -n '/set dests/,/^	}/p' "$EG_WORK/ratelimit.ruleset" | sed 's/^/      /'

step "what the guest measured"
python3 - "$EG_WORK/ratelimit.err" "$DEST_LIMIT" <<'PY'
import json, re, sys

rows = [json.loads(line) for line in open(sys.argv[1])
        if line.startswith('{"probe"')]
limit = int(sys.argv[2])

# `fanout-NN`, not `fanout-recovers`, which is the probe after the window.
fanout = [row for row in rows if re.fullmatch(r"fanout-\d+", row["probe"])]
reached = [row for row in fanout if row["result"] in ("connected", "refused")]
print("      fan-out: %d of %d destinations reached, limit %d"
      % (len(reached), len(fanout), limit))
print("      reached: %s" % ", ".join(r["probe"] for r in reached))
for name in ("rate-limit", "rate-recovers", "fanout-recovers"):
    for row in rows:
        if row["probe"] == name:
            print("      %-16s %-12s %s (%.1f s)"
                  % (name, row["expect"], row["result"], row["ms"] / 1000.0))
PY

step "raw probe data"
eg_write_csv "$SPIKE_DIR/report/data/exp8-probes.csv" ratelimit
say "  wrote $SPIKE_DIR/report/data/exp8-probes.csv"

eg_finish exp8-limits.sh
