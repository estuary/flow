#!/usr/bin/env bash
# Experiment 3: protocol parity with Go connectors. Previews the Go capture and
# the Go materialization with the switch on and off, and diffs what came out.
#
# Three comparisons per task, each a diff that must be empty:
#   documents   `flowctl preview` stdout. The capture emits five; the
#               materialization emits none and commits to its endpoint instead.
#   connector   every log line the connector produced, as the decoder rendered
#               it. This is the pass condition PLAN states: the connector's own
#               output crossing connector-init, the codec, and the log decoder.
#               The materialization's `connector applied` line carries the DDL
#               it ran, which is the richest connector-authored payload the two
#               Go connectors produce.
#   stats       the runtime's per-transaction accounting: documents and bytes in
#               and out. Equality here is what says the transactions themselves
#               were the same, not just the chatter around them.
#
# Deliberately NOT diffed: `started connector container`, whose `ipAddr`,
# `mappedHostPorts` and `networkPorts` differ by contract (CONTRACTS
# "runtime-next spike switch": ip_addr is the guest, network_ports is empty).
# That is the launcher changing, which is the point of the spike, not the
# protocol changing. The report records the exact difference.
#
# Scope: `flowctl preview` validates the catalog through the LEGACY `runtime`
# crate before runtime-next drives anything, so Spec and Validate run in
# unsandboxed `fc_*` containers in BOTH arms. What this proves is parity on the
# Open-and-documents path, which is where the connector actually runs.
#
# Run under mise: mise exec -- spike/tasks/exp3-parity.sh
set -euo pipefail
source "$(dirname "${BASH_SOURCE[0]}")/preview-common.sh"

POLICY=policy-egress-none.json
CAPTURE=capture-hello-world.flow.yaml
MATERIALIZE=materialize-sqlite.flow.yaml
FIXTURE=materialize-fixture.ndjson

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

# INFO is where the connector's own lines and the transaction stats live; at the
# default level a materialization preview says nothing at all.
SPIKE_PREVIEW_GUEST_ENV=(RUST_LOG=info)

for arm in off on; do
    policy="$POLICY"
    [ "$arm" = off ] && policy=""

    step "$arm: capture"
    spike_preview "$policy" "$CAPTURE" --sessions 3,2 \
        >"$OUT/capture.$arm.docs" 2>"$OUT/capture.$arm.log"

    step "$arm: materialization"
    spike_preview "$policy" "$MATERIALIZE" \
        --fixture "$SPIKE_WORK_DIR/$FIXTURE" --sessions 1,-1 \
        >"$OUT/materialize.$arm.docs" 2>"$OUT/materialize.$arm.log"
done

step "mask"
python3 - "$OUT" <<'PY'
import json, pathlib, re, sys

out = pathlib.Path(sys.argv[1])

# Everything that moves between two runs of the same task, whether or not the
# switch is on: clocks, container identities, addresses, and temporary paths.
MASKS = [
    (re.compile(r"\x1b\[[0-9;]*m"), ""),
    (re.compile(r"^\d{4}-\d\d-\d\dT[\d:.]+Z\s+"), ""),
    (re.compile(r"handler\{id=\d+"), "handler{id=N"),
    (re.compile(r"\b(?:fc|fs)_[0-9a-f]{16}\b"), "<container>"),
    (re.compile(r"\b(?:\d{1,3}\.){3}\d{1,3}:\d+\b"), "<addr:port>"),
    (re.compile(r"\b(?:\d{1,3}\.){3}\d{1,3}\b"), "<addr>"),
    (re.compile(r"/var/tmp/[^\s\"]+"), "<tmp>"),
    (re.compile(r"/tmp/[^\s\"]+"), "<tmp>"),
    (re.compile(r'"(ts|lastPublishedAt|lastSourcePublishedAt)":"[^"]*"'), r'"\1":"<ts>"'),
    (re.compile(r'"openSecondsTotal":[-\d.e+]+'), '"openSecondsTotal":<s>'),
    # The capture stamps each document with a wall clock whose serialization is
    # sometimes a byte shorter (a trailing zero in the nanoseconds is trimmed),
    # so byte totals differ between two runs of the SAME arm and carry no
    # signal. Document and transaction counts, which do, are left alone.
    (re.compile(r'"bytesTotal":\d+'), '"bytesTotal":<n>'),
]


def mask(line):
    for pattern, replacement in MASKS:
        line = pattern.sub(replacement, line)
    return line


# A connector log line is one the decoder rendered from the connector's stderr.
# The launcher's own container events are not connector output, and the helper's
# diagnostics are not either.
NOT_CONNECTOR = ("started connector container", "stopped connector container",
                 "flow-sandbox-helper:", "flow-init:")


def select(path, kind):
    lines = []
    for line in path.read_text(errors="replace").splitlines():
        line = mask(line)
        if kind == "connector":
            if " ops: " not in line or any(s in line for s in NOT_CONNECTOR):
                continue
        elif "transaction stats" not in line:
            continue
        lines.append(line)
    return lines


for task in ("capture", "materialize"):
    for arm in ("off", "on"):
        docs = (out / f"{task}.{arm}.docs").read_text().splitlines()
        # The capture stamps each document with the wall clock it ran at.
        docs = [json.dumps([d[0], {**d[1], "ts": "<ts>"}], sort_keys=True)
                if isinstance(d := json.loads(line), list) else line for line in docs]
        (out / f"{task}.{arm}.documents.masked").write_text("\n".join(docs) + "\n")
        for kind in ("connector", "stats"):
            (out / f"{task}.{arm}.{kind}.masked").write_text(
                "\n".join(select(out / f"{task}.{arm}.log", kind)) + "\n")
PY

step "results"
for task in capture materialize; do
    printf '%s: %s documents\n' "$task" "$(wc -l <"$OUT/$task.off.docs")"
    for kind in documents connector stats; do
        lines=$(grep -c . "$OUT/$task.off.$kind.masked" || true)
        if diff -u "$OUT/$task.off.$kind.masked" "$OUT/$task.on.$kind.masked" \
            >"$OUT/$task.$kind.diff"; then
            ok "$task $kind: identical ($lines lines)"
        else
            fail "$task $kind: differs"
            head -30 "$OUT/$task.$kind.diff"
        fi
    done
done

step "the connector's own output, as both arms rendered it"
sed -n '1,3p' "$OUT/materialize.on.connector.masked"
sed -n '1p' "$OUT/capture.off.documents.masked"

step "the launcher difference, which is by contract and not diffed"
for arm in off on; do
    # The last one: with the switch on, the first two are the legacy validation
    # path's unsandboxed containers and only the last is the helper.
    printf '%-4s %s\n' "$arm" "$(sed 's/\x1b\[[0-9;]*m//g' "$OUT/capture.$arm.log" |
        grep -a 'started connector container' | tail -1 | sed 's/.*fields=//' |
        jq -c '.container | {ipAddr, networkPorts, mappedHostPorts}')"
done

printf '\n'
if [ "$fails" -ne 0 ]; then
    printf 'exp3-parity.sh: %d failure(s)\n' "$fails" >&2
    exit 1
fi
echo "exp3-parity.sh: ok"
