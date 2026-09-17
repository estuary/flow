#!/usr/bin/env bash
# POC gates. G1 to G5 are regressions on what experiment 4 established through
# `flowctl preview`; G6 and G7 are the evidence only a real reactor can produce,
# and are the reason the POC exists.
#
# Run under mise, after poc-up.sh:
#   mise exec -- spike/tasks/poc.sh
#
# Not quick and not read-only: G6 idles the derivation for POC_IDLE_SECS
# (default 600) by stopping its source, and G7 restarts the reactor.
set -euo pipefail
source "$(dirname "${BASH_SOURCE[0]}")/poc-common.sh"

POC_FAILURES=0
OUT="$(mktemp -d)"
trap 'rm -rf "$OUT"' EXIT

PS_FMT='{{.Names}} {{index .Labels "task-name"}}'

read_derived()  { poc_flowctl collections read --collection "$POC_DERIVATION"; }
derived_count() { read_derived 2>/dev/null | grep -c . || true; }
grew_past()     { [ "$(derived_count)" -gt "$1" ]; }
running_fs()    { sudo podman ps --filter 'name=^fs_' --format '{{.Names}}' | sort; }

step "G1: two guests, no plain connector containers"
sudo podman ps --format "$PS_FMT" >"$OUT/ps"
sed 's/^/  /' "$OUT/ps"
for task in "$POC_CAPTURE" "$POC_DERIVATION"; do
    name="$(awk -v t="$task" '$2 == t {print $1}' "$OUT/ps")"
    case "$name" in
    fs_*) ok "$task is a sandbox helper ($name)" ;;
    fc_*) fail "$task is an unsandboxed container ($name); the switch is off" ;;
    "") fail "$task has no container" ;;
    *) fail "$task has an unrecognized container ($name)" ;;
    esac
done
# An fs_ container for any other task belongs to something we did not launch.
strays="$(awk -v c="$POC_CAPTURE" -v d="$POC_DERIVATION" \
    '$1 ~ /^fs_/ && $2 != c && $2 != d {print $1}' "$OUT/ps" | tr '\n' ' ')"
if [ -z "$strays" ]; then
    ok "no stray helpers"
else
    fail "stray helpers: $strays"
fi

step "G2: the derivation produces documents"
read_derived >"$OUT/docs" 2>"$OUT/docs.err" || true
if jq -se 'length > 0 and all(has("words") and has("characters"))' \
    <"$OUT/docs" >/dev/null 2>&1; then
    ok "$(grep -c . "$OUT/docs") documents carrying pandas-computed fields"
    head -3 "$OUT/docs" | sed 's/^/  /'
else
    fail "no usable documents from $POC_DERIVATION"
    head -10 "$OUT/docs.err" | sed 's/^/  /'
fi

step "G3: pandas was fetched and imported inside the guest"
poc_flowctl logs --task "$POC_DERIVATION" --since 2h >"$OUT/derive.log" 2>/dev/null || true
footprint="$(grep -ao 'scratch-footprint: [^"]*' "$OUT/derive.log" | head -1 || true)"
case "$footprint" in
*"no /scratch (unsandboxed)"*)
    fail "the module ran with no scratch disk; this shard is not the guest" ;;
*pandas=*)
    ok "$footprint" ;;
*)
    fail "no scratch-footprint line; the module never imported" ;;
esac

step "G4: the runtime reports the guest's address"
ip="$(jq -r 'select(.message == "started connector container")
             | .fields.container.ipAddr' <"$OUT/derive.log" 2>/dev/null |
    grep -v '^null$' | tail -1 || true)"
if [ "$ip" = 192.0.2.2 ]; then
    ok "ContainerStarted reports $ip"
else
    fail "ContainerStarted reports '${ip:-<none>}', want 192.0.2.2"
fi

step "G5: the ruleset is live inside each helper"
# allowAll adds one accept at the head of egress_accept and leaves the
# anti-spoof, baseline, IPv6 and tcp/25 drops in front of it. A ruleset missing
# those would be an unenforced network, not a permissive one.
bridge="$(poc_bridge_subnet)"
for task in "$POC_CAPTURE" "$POC_DERIVATION"; do
    name="$(poc_container "$task")"
    if [ -z "$name" ]; then
        continue # G1 already reported the absence.
    fi
    if ! sudo podman exec "$name" nft list table inet flow_sandbox >"$OUT/nft" 2>&1; then
        fail "$task: no inet flow_sandbox table"
        continue
    fi
    missing=""
    for want in 'comment "anti-spoof"' 'comment "no-ipv6"' 'comment "smtp"' \
        'comment "allow-all"' '169.254.0.0/16' '192.0.2.0/30'; do
        if ! grep -qF -- "$want" "$OUT/nft"; then
            missing="$missing [$want]"
        fi
    done
    if [ -n "$missing" ]; then
        fail "$task: ruleset is missing:$missing"
        continue
    fi
    # The bridge is not named literally. `baseline` holds RFC1918 aggregates with
    # auto-merge, so 10.89.0.0/24 arrives as part of 10.0.0.0/8; asserting the
    # literal CIDR asserts a string the generator never emits. What matters is
    # that the helper's own bridge is covered, so check containment.
    if python3 - "$bridge" "$OUT/nft" <<'PY'
import ipaddress, re, sys
bridge = ipaddress.ip_network(sys.argv[1])
body = open(sys.argv[2]).read()
block = re.search(r"set baseline \{(.*?)\}", body, re.S)
nets = [ipaddress.ip_network(m) for m in
        re.findall(r"\d+\.\d+\.\d+\.\d+/\d+", block.group(1) if block else "")]
sys.exit(0 if any(bridge.subnet_of(n) for n in nets) else 1)
PY
    then
        ok "$task: ruleset loaded, baseline covers the $bridge bridge"
    else
        fail "$task: baseline does not cover the $bridge bridge"
    fi
done

step "G6: the guest survives ${POC_IDLE_SECS}s with no input"
# The keep-alive interval and timeout in spike.rs were chosen blind and never
# exercised past a single preview session. Stopping the capture is the only way
# to starve the derivation without editing the module under test.
before_helper="$(poc_container "$POC_DERIVATION")"
poc_capture_disable true
echo "  capture disabled; idling ${POC_IDLE_SECS}s"
sleep "$POC_IDLE_SECS"
after_helper="$(poc_container "$POC_DERIVATION")"
if [ -n "$after_helper" ] && [ "$after_helper" = "$before_helper" ]; then
    ok "helper $after_helper survived the idle"
else
    fail "helper was ${before_helper:-<none>}, now ${after_helper:-<gone>}"
fi

idle_count="$(derived_count)"
poc_capture_disable false
if poc_wait 300 grew_past "$idle_count"; then
    ok "documents resumed after the idle"
else
    fail "no new documents within 300s of re-enabling the capture"
fi

step "G7: a reactor restart tears helpers down and relaunches them"
read_derived 2>/dev/null | sort >"$OUT/before" || true
before_count="$(grep -c . "$OUT/before" || true)"
mapfile -t old < <(running_fs)
# Per-task, so the assertions below can say which helper should have died.
declare -A was
for task in "$POC_CAPTURE" "$POC_DERIVATION"; do
    was[$task]="$(poc_container "$task")"
done
sudo systemctl restart "$POC_ROOT_UNIT"

survivors=""
for _ in $(seq 1 30); do
    survivors=""
    now="$(running_fs)"
    for name in "${old[@]}"; do
        if grep -qx -- "$name" <<<"$now"; then
            survivors="$survivors $name"
        fi
    done
    [ -z "$survivors" ] && break
    sleep 2
done
if [ -z "$survivors" ]; then
    ok "no helper from before the restart survived it"
else
    fail "orphaned helpers:$survivors"
fi

# Every pre-restart helper's directory must be gone: the runtime creates <id>/
# before a launch and its DirGuard removes it after. Checked against the
# pre-restart ids rather than against "directories with no running helper",
# which passes vacuously for exactly the helpers that leaked -- they are still
# running, so their directories look legitimate.
dirs="$(sudo find "$SPIKE_REACTOR_DIR" -mindepth 1 -maxdepth 1 -name 'fs_*' -printf '%f\n')"
leaked=""
for name in "${old[@]}"; do
    [ -n "$name" ] || continue
    grep -qx -- "$name" <<<"$dirs" && leaked="$leaked $name"
done
if [ -z "$leaked" ]; then
    ok "every pre-restart reactor directory was removed"
else
    fail "reactor directories outlived the restart:$leaked"
fi

for task in "$POC_CAPTURE" "$POC_DERIVATION"; do
    if ! poc_wait 300 poc_has_container "$task"; then
        fail "$task did not relaunch within 300s"
        continue
    fi
    # The helper that served this task before the restart must be gone, and a
    # different one must be serving it now. A surviving old helper is the
    # failure this gate exists to catch: it keeps a guest, a tap and its cgroup
    # reservation alive with nothing driving it.
    mapfile -t serving < <(sudo podman ps --filter "label=task-name=$task" \
        --format '{{.Names}}')
    if [ -n "${was[$task]}" ] &&
        printf '%s\n' "${serving[@]}" | grep -qx -- "${was[$task]}"; then
        others="$(printf '%s\n' "${serving[@]}" | grep -vx -- "${was[$task]}" | tr '\n' ' ')"
        fail "$task: pre-restart helper ${was[$task]} still running, alongside ${others:-nothing}"
    elif [ "${#serving[@]}" -eq 1 ]; then
        ok "$task relaunched as ${serving[0]}, old helper gone"
    else
        fail "$task has ${#serving[@]} helpers: ${serving[*]}"
    fi
done

if poc_wait 300 grew_past "$before_count"; then
    read_derived 2>/dev/null | sort >"$OUT/after"
    lost="$(comm -23 "$OUT/before" "$OUT/after" | grep -c . || true)"
    if [ "$lost" -eq 0 ]; then
        ok "the collection grew and lost nothing across the restart"
    else
        fail "$lost documents present before the restart are gone after it"
    fi
else
    fail "the collection did not grow within 300s of the restart"
fi

printf '\n'
if [ "$POC_FAILURES" -ne 0 ]; then
    printf 'poc.sh: %d failure(s)\n' "$POC_FAILURES" >&2
    exit 1
fi
echo "poc.sh: ok"
