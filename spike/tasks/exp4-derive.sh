#!/usr/bin/env bash
# Experiment 4: derive-python end to end with a permissive ruleset. Previews a
# derivation whose only dependency is pandas, so `uv` has to resolve a name and
# fetch from PyPI from inside the guest before the module can even be imported.
#
# Pass: the derivation produces documents. That single sentence covers Python
# running under libkrun, the tap and its masquerade, DNS, TLS out to PyPI, uv
# writing a 180 MiB environment onto the scratch disk, and connector-init
# carrying the derive protocol over vsock - any one of which failing produces no
# documents at all.
#
# The switch-off arm is a control, not a second gate: it proves the sandboxed
# derivation computed the same answers as the unsandboxed one.
#
# The `/scratch` footprint is recorded for information. It does not size
# `diskMib`: production guests receive a prebuilt dependency image on /dev/vdb
# (experiment 5b) and will not run uv at all.
#
# `allowAll: true` is not an unenforced network: it adds one accept at the head of
# egress_accept and leaves the anti-spoof, baseline, IPv6, non-TCP/UDP and tcp/25
# drops in front of it, and the real resolver still gates every answer. What it
# removes is the requirement that a destination be named. See report/exp4.md.
#
# Run under mise: mise exec -- spike/tasks/exp4-derive.sh
set -euo pipefail
source "$(dirname "${BASH_SOURCE[0]}")/preview-common.sh"

SPEC=derive-pandas.flow.yaml
POLICY=policy-allow-all.json
FIXTURE=derive-fixture.ndjson
DOCUMENTS=4

OUT="$(mktemp -d)"
trap 'rm -rf "$OUT"' EXIT

step() { printf '\n== %s\n' "$*"; }
fails=0
ok() { printf 'ok    %s\n' "$*"; }
fail() {
    printf 'FAIL  %s\n' "$*"
    fails=$((fails + 1))
}

spike_stage_catalog
SPIKE_PREVIEW_GUEST_ENV=(RUST_LOG=info)

for arm in on off; do
    policy="$POLICY"
    [ "$arm" = off ] && policy=""

    step "$arm: preview the derivation"
    spike_preview "$policy" "$SPEC" \
        --fixture "$SPIKE_WORK_DIR/$FIXTURE" --sessions 1 \
        >"$OUT/$arm.docs" 2>"$OUT/$arm.log" ||
        fail "$arm: preview exited non-zero"
done

step "documents"
cat "$OUT/on.docs"

count=$(grep -c . "$OUT/on.docs" || true)
[ "$count" -eq "$DOCUMENTS" ] &&
    ok "the derivation produced $count documents inside the guest" ||
    fail "the derivation produced $count documents, want $DOCUMENTS"

# The connector stamps each document with a placeholder UUID, so the two arms
# are directly comparable with no masking at all.
if diff -u "$OUT/off.docs" "$OUT/on.docs" >"$OUT/docs.diff"; then
    ok "sandboxed documents are byte-identical to unsandboxed"
else
    fail "sandboxed and unsandboxed documents differ"
    head -20 "$OUT/docs.diff"
fi

step "pandas reached the guest"
version=$(sed 's/\x1b\[[0-9;]*m//g' "$OUT/on.log" |
    grep -ao 'pandas=[0-9][^ ]*' | head -1)
[ -n "$version" ] &&
    ok "uv fetched and imported $version from PyPI inside the guest" ||
    fail "no pandas version reported; the module never imported"

step "/scratch footprint, for information"
sed 's/\x1b\[[0-9;]*m//g' "$OUT/on.log" |
    grep -ao 'scratch-footprint: .*' | sed 's/ fields={}$//' | head -1 |
    tr ' ' '\n' | sed 's/^/  /'

printf '\n'
if [ "$fails" -ne 0 ]; then
    printf 'exp4-derive.sh: %d failure(s)\n' "$fails" >&2
    exit 1
fi
echo "exp4-derive.sh: ok"
