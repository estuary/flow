#!/usr/bin/env bash
# Experiment 13: helper crash and cleanup (gate).
#
# Two ways for a sandbox to die badly, and what each leaves behind:
#   sigkill  the shim is killed mid-transaction while the runtime is driving a
#            real materialization. The runtime must see the socket close and
#            report the container's exit; the container, its tap, its netns and
#            its scratch space must all be gone; and the runtime must remove the
#            per-connector directory it created.
#   panic    the guest kernel panics under OOM. The guest has no sysrq, so the
#            panic is armed with vm.panic_on_oom. PLAN's gate asks for a prompt
#            non-zero exit: it is prompt, and it is zero. See report/exp13.md.
#
# Pass: every check below prints ok.
#
# Run under mise: mise exec -- spike/tasks/exp13-crash.sh
set -euo pipefail

# The panic pass needs an interpreter that can allocate in a loop; the SIGKILL
# pass takes its image from the catalog spec and ignores this.
XP_GUEST_IMAGE=ghcr.io/estuary/derive-python:dev
source "$(dirname "${BASH_SOURCE[0]}")/exp11-common.sh"
source "$(dirname "${BASH_SOURCE[0]}")/preview-common.sh"

# The FIFO writer spins until something closes the pipe, so it goes with the
# script however the script ends.
writer_pid=""
FIFO=""
stop_writer() {
    [ -n "$writer_pid" ] && kill "$writer_pid" 2>/dev/null
    [ -n "$FIFO" ] && rm -f "$FIFO"
    return 0
}
trap 'stop_writer; xp_cleanup' EXIT

POLICY=policy-egress-none.json
MATERIALIZE=materialize-sqlite.flow.yaml
FIXTURE=materialize-fixture.ndjson

say "experiment 13: helper crash and cleanup"
say "  spec:         $MATERIALIZE, driven by flowctl preview through the fake reactor"
say "  helper:       $SPIKE_HELPER_IMAGE"

# --------------------------------------------------- SIGKILL mid-transaction

step "SIGKILL to the shim while the runtime is driving a materialization"
spike_stage_catalog

netns_before="$(ip netns list 2>/dev/null | wc -l)"
fs_before="$(xp_fs_used)"

SPIKE_PREVIEW_GUEST_ENV=(RUST_LOG=info)

# A FIFO fixture, so the kill lands mid-transaction rather than after the run.
# `flowctl preview` streams a named pipe as one unbounded session, feeding
# transactions as the writer emits them, so the connector stays open and busy
# for as long as the writer keeps going. A regular file is read eagerly and the
# eight documents would be committed before the helper finished booting.
FIFO="$SPIKE_WORK_DIR/fixture.fifo"
rm -f "$FIFO"
mkfifo "$FIFO"
(while :; do cat "$SPIKE_WORK_DIR/$FIXTURE"; sleep 0.5; done >"$FIFO") 2>/dev/null &
writer_pid=$!

spike_preview "$POLICY" "$MATERIALIZE" --fixture "$FIFO" \
    >"$XP_WORK/preview.docs" 2>"$XP_WORK/preview.log" &
preview_pid=$!

# One preview starts the connector TWICE - the shard validates, then opens - and
# the first helper is gone within a couple of seconds (WP06). Waiting for the
# first committed transaction is what makes "the helper" the session's own, and
# "mid-transaction" true rather than aspirational.
for _ in $(seq 900); do
    if grep -q 'transaction stats' "$XP_WORK/preview.log" 2>/dev/null &&
        [ "$(sudo podman ps --format '{{.Names}}' | grep -c '^fs_' || true)" -eq 1 ]; then
        break
    fi
    kill -0 "$preview_pid" 2>/dev/null || break
    sleep 0.1
done
running_helpers="$(sudo podman ps --format '{{.Names}}' | grep '^fs_' | tr '\n' ' ' || true)"
helper="$(sudo podman ps --format '{{.Names}}' | grep '^fs_' | head -1 || true)"

if [ -z "$helper" ]; then
    fail "sigkill: no helper was running once the materialization had committed"
    sed 's/^/      /' "$XP_WORK/preview.log" | tail -20
    stop_writer
    wait "$preview_pid" || true
else
    txns_before_kill="$(grep -c 'transaction stats' "$XP_WORK/preview.log" || true)"
    fs_running="$(xp_fs_used)"
    netns_running="$(ip netns list 2>/dev/null | wc -l)"
    tap_in_helper="$(sudo podman exec "$helper" ip -o link show tap0 2>/dev/null | wc -l)"

    # Only lines AFTER this point are the runtime's account of the death; the
    # log already says "started connector container" and would match anything
    # looked for naively.
    log_at_kill="$(wc -l <"$XP_WORK/preview.log")"
    killed_at="$(date +%s.%N)"
    sudo podman kill -s KILL "$helper" >/dev/null 2>&1 || true

    # The kill is only the thing under test if it landed on the helper the
    # runtime is driving, and that helper then goes away.
    gone=0
    for _ in $(seq 100); do
        sudo podman ps --format '{{.Names}}' | grep -q "^$helper$" || { gone=1; break; }
        sleep 0.1
    done
    [ "$gone" -eq 1 ] || fail "sigkill: $helper was still running 10s after SIGKILL"

    # Give the runtime a window to notice on its own before the fixture is
    # closed: EOF is also a reason to stop, and a stop that arrives first would
    # leave the log saying nothing about the connector at all.
    for _ in $(seq 150); do
        kill -0 "$preview_pid" 2>/dev/null || break
        tail -n "+$((log_at_kill + 1))" "$XP_WORK/preview.log" |
            grep -q 'error=\|Error:\|transport\|broadcasting Stopped' && break
        sleep 0.1
    done
    # Without EOF the preview would feed transactions for ever, and a runtime
    # that recovers by relaunching would outlive anything this script asserts.
    stop_writer

    rc=0
    alive=1
    for _ in $(seq 600); do
        kill -0 "$preview_pid" 2>/dev/null || { alive=0; break; }
        sleep 0.1
    done
    if [ "$alive" -eq 1 ]; then
        fail "sigkill: flowctl was still running 60s after its connector was killed"
        kill "$preview_pid" 2>/dev/null || true
    fi
    wait "$preview_pid" || rc=$?
    exited_at="$(date +%s.%N)"

    # The runtime's own account of the death, which is what a task operator
    # sees. Kept whole in the report's data, because the wording of these lines
    # is the deliverable: transaction stats are the noise around it.
    RUNTIME_LOG="$SPIKE_DIR/report/data/exp13-runtime.log"
    {
        sed 's/\x1b\[[0-9;]*m//g' "$XP_WORK/preview.log" | grep -v 'transaction stats' |
            head -n "$log_at_kill"
        printf -- '--- SIGKILL to the shim here ---\n'
        sed 's/\x1b\[[0-9;]*m//g' "$XP_WORK/preview.log" | tail -n "+$((log_at_kill + 1))"
    } >"$RUNTIME_LOG"
    report="$(sed 's/\x1b\[[0-9;]*m//g' "$XP_WORK/preview.log" | tail -n "+$((log_at_kill + 1))" |
        grep -oE 'error reading a body from connection|expected connector response|stopped connector container|transport error[^,]*|connection (closed|reset)[^,]*' |
        head -1 || true)"

    # podman removes a container, its netns and its layers after the client that
    # asked has already exited, so "afterwards" means "once it has settled".
    settled=0
    for _ in $(seq 600); do
        if [ "$(sudo podman ps --format '{{.Names}}' | grep -cE '^f[sc]_' || true)" -eq 0 ] &&
            [ "$(ip netns list 2>/dev/null | wc -l)" -le "$netns_before" ]; then
            settled=1
            break
        fi
        sleep 0.1
    done
    settled_at="$(date +%s.%N)"
    fs_after="$(xp_fs_used)"
    netns_after="$(ip netns list 2>/dev/null | wc -l)"
    [ "$settled" -eq 1 ] || fail "sigkill: containers or netns were still up 60s after flowctl exited"

    note "helper:       $helper, killed after $txns_before_kill committed transaction(s)"
    note "fs_ running at the kill: $running_helpers"
    note "helper boots in the run: $(grep -c 'flow-sandbox-helper: timing stage=start' "$XP_WORK/preview.log" || true); transactions after the kill: $(tail -n "+$((log_at_kill + 1))" "$XP_WORK/preview.log" | grep -c 'transaction stats' || true)"
    note "tap0 inside the helper while it ran: $tap_in_helper line(s) from ip link"
    note "named netns:  $netns_before -> $netns_running -> $netns_after"
    note "df(reactor):  $((fs_before / 1024)) MiB -> $((fs_running / 1024)) MiB -> $((fs_after / 1024)) MiB"
    note "flowctl:      exit $rc, $(python3 -c "import sys; print('%.1f' % (float(sys.argv[1]) - float(sys.argv[2])))" "$exited_at" "$killed_at")s after the kill; podman settled $(python3 -c "import sys; print('%.1f' % (float(sys.argv[1]) - float(sys.argv[2])))" "$settled_at" "$exited_at")s later"
    note "runtime log:  $RUNTIME_LOG"

    if [ "${txns_before_kill:-0}" -lt 1 ] || [ "$tap_in_helper" -ne 1 ]; then
        fail "sigkill: the helper was not mid-transaction with a tap when it was killed; the teardown checks prove nothing"
    else
        ok "sigkill: the helper was killed mid-transaction, with tap0 up"
    fi
    if [ -n "$report" ]; then
        ok "sigkill: the runtime reported the connector's death: $report"
    else
        fail "sigkill: nothing in the runtime's log says the connector went away"
        sed 's/^/      /' "$XP_WORK/preview.log" | tail -20
    fi
    if sudo podman ps --format '{{.Names}}' | grep -q "^$helper$"; then
        fail "sigkill: $helper is still running"
    else
        ok "sigkill: no helper container remains"
    fi
    if [ "$netns_after" -ne "$netns_before" ]; then
        fail "sigkill: $((netns_after - netns_before)) netns left behind"
    elif [ "$netns_running" -gt "$netns_before" ]; then
        ok "sigkill: the helper's netns is gone, and tap0 with it"
    else
        # podman's netns for a container is listed under /run/netns only while
        # netavark holds it open by name; on this box it is not, so the tap's
        # disappearance rests on the container's.
        ok "sigkill: no netns left behind; tap0 lived in the container's own netns, which is gone with it"
    fi
    # The scratch disk's own return is measured at scale in experiment 11; what
    # this pass adds is that a SIGKILL returns it too. fs_before is taken before
    # the preview's Spec and Validate run, and those write connector-init into
    # TMPDIR under the reactor directory, so the level the helper is measured
    # against is the one it was launched from.
    if [ "$fs_after" -le "$fs_running" ]; then
        ok "sigkill: the reactor filesystem is back to $(((fs_running - fs_after) / 1024)) MiB below where it stood with the helper alive"
    else
        fail "sigkill: $(((fs_after - fs_running) / 1024)) MiB more held than while the helper was running"
    fi
    if sudo test -e "$SPIKE_REACTOR_DIR/$helper"; then
        fail "sigkill: the runtime left $SPIKE_REACTOR_DIR/$helper behind"
        sudo ls -R "$SPIKE_REACTOR_DIR/$helper" | sed 's/^/      /' | head -10
    else
        ok "sigkill: the runtime removed $helper/"
    fi
fi

# ------------------------------------------------------------ guest kernel panic

# Extra room outside the guest's RAM so the HOST cgroup is not what kills the
# helper: the pass is only a test of the guest's panic path if the guest is the
# one that runs out first.
XP_OVERHEAD_MIB=512

step "guest kernel panic under OOM"
id="$(xp_new_sandbox)"
# The guest has no sysrq, so the panic is armed with a sysctl and then provoked
# by allocating past the guest's RAM. --debug tees the kernel console to stderr,
# which is the only place the panic line appears.
started="$(date +%s.%N)"
rc=0
xp_run panic "$id" --debug --run-as-root --exec /bin/sh -c \
    'echo 1 > /proc/sys/vm/panic_on_oom; /usr/local/bin/python -c "a=[]; [a.append(bytearray(64<<20)) for _ in range(1000)]"' || rc=$?
elapsed="$(python3 -c "import sys; print('%.1f' % (float(sys.argv[1]) - float(sys.argv[2])))" "$(date +%s.%N)" "$started")"

# The console arrives on stdout unless --debug is on, in which case it is also
# teed to stderr with a `kernel: ` prefix. Both are kept whole: the kernel's own
# last words are the deliverable here.
# --debug also raises libkrun's own log level, and its per-interrupt DEBUG lines
# outnumber everything else about two hundred to one; they are dropped here so
# the artifact is the guest's last words and the helper's, which is what the
# report quotes.
PANIC_CONSOLE="$SPIKE_DIR/report/data/exp13-panic-console.log"
{
    printf -- '--- helper stdout (kernel console and workload stdout)\n'
    tr -d '\r' <"$XP_WORK/panic.out" | grep -v 'DEBUG krun_' || true
    printf -- '\n--- helper stderr (workload stderr, plus the console under --debug)\n'
    tr -d '\r' <"$XP_WORK/panic.err" | grep -v 'DEBUG krun_' || true
} >"$PANIC_CONSOLE"

panic_line="$(grep -hoE 'Kernel panic.*' "$PANIC_CONSOLE" | head -1 || true)"
oom_line="$(grep -hoE 'Out of memory:.*|invoked oom-killer.*|oom-kill:.*' "$PANIC_CONSOLE" | head -1 || true)"
after_panic="$(grep -A3 -hE 'Kernel panic' "$PANIC_CONSOLE" | tail -n +2 | head -3 | tr '\n' '|' || true)"
note "helper exit:  $rc after ${elapsed}s"
note "oom:          ${oom_line:-<none>}"
note "panic:        ${panic_line:-<none>}"
note "after panic:  ${after_panic:-<nothing further>}"
note "console:      $PANIC_CONSOLE"

if [ -n "$panic_line" ]; then
    ok "panic: the guest kernel panicked"
else
    fail "panic: no kernel panic on the console"
    tail -5 "$XP_WORK/panic.err" | sed 's/^/      /'
fi
# PLAN's gate has two halves. "Promptly rather than hanging" holds. "Non-zero"
# does not, and the reason is libkrun's: a guest that panics and reboots never
# reaches init's exit-code report, so the VMM falls back to the vcpu/i8042 path,
# which is FC_EXIT_CODE_OK (src/vmm/src/lib.rs:405-430). The runtime therefore
# cannot tell a panicked guest from a clean one by exit code; it learns of the
# death from the socket, as the SIGKILL pass above shows. exp13.md carries this
# as the gate's one open item.
if [ "$(python3 -c "import sys; print(1 if float(sys.argv[1]) < 60 else 0)" "$elapsed")" -eq 1 ]; then
    ok "panic: the helper exited promptly, ${elapsed}s after the workload started, rather than hanging"
else
    fail "panic: the helper took ${elapsed}s to exit"
fi
if [ "$rc" -ne 0 ]; then
    ok "panic: the helper exited $rc, non-zero"
else
    fail "panic: the helper exited 0; a guest kernel panic is indistinguishable from a clean exit by exit code alone"
fi

xp_finish exp13-crash.sh
