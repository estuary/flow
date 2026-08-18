#!/usr/bin/env bash
#
# Start and stop the etcd, Gazette broker, and disk daemon that the examples need.
# An example starts nothing itself, and only sends RPCs.
#
#   ./demo-services.sh start    # start all three, and print how to run an example
#   ./demo-services.sh stop     # stop all three, and remove what they made
#   ./demo-services.sh status   # report what is running
#
# etcd and the broker run as you. Only the daemon needs privilege, so only it runs
# under `sudo`. The ports are away from the defaults, so this disturbs neither a
# local Flow stack nor a system etcd. Override any of:
#
#   ETCD_PORT=22379 BROKER_PORT=28080 DAEMON=/path/to/flow-disk-daemon
#
# Install what it starts, and load `ublk_drv` with `sudo modprobe ublk_drv`:
#
#   go install go.gazette.dev/core/cmd/gazette@v0.103.1-0.20260722193110-e54beb5c6e64
#   cargo build -p disk-daemon --bin flow-disk-daemon
#   etcd: https://etcd.io/docs/latest/install/

set -euo pipefail

ETCD_PORT=${ETCD_PORT:-22379}
ETCD_PEER_PORT=${ETCD_PEER_PORT:-22380}
BROKER_PORT=${BROKER_PORT:-28080}

# What the examples fall back to. Keep these equal to the defaults above, so an
# example of these services needs no variable.
DEMO_UDS_DEFAULT="${TMPDIR:-/tmp}/disk-daemon-demo/disk.sock"
DEMO_ENDPOINT_DEFAULT="http://127.0.0.1:28080"

# Every file of a run lives here. The name is fixed so `stop` finds what `start`
# left, and short enough to hold a Unix socket address.
STATE=${DEMO_STATE:-${TMPDIR:-/tmp}/disk-daemon-demo}
UDS="${STATE}/disk.sock"

here=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
root=$(cd "${here}/../../.." && pwd)

# The built daemon of this checkout, unless you name another.
DAEMON=${DAEMON:-"${CARGO_TARGET_DIR:-${root}/target}/debug/flow-disk-daemon"}

# True while something accepts connections on a localhost port.
listening() { (exec 3<>"/dev/tcp/127.0.0.1/$1") 2>/dev/null; }

# Wait for a service to answer. `target` is a port or the path of a socket.
wait_ready() {
    local name=$1 target=$2 log=$3 attempt

    for attempt in $(seq 1 200); do
        if [[ ${target} == /* ]]; then
            [[ -S ${target} ]] && return 0
        else
            listening "${target}" && return 0
        fi
        sleep 0.1
    done

    echo "error: ${name} did not become ready. Its last output was:" >&2
    tail -n 20 "${log}" >&2
    return 1
}

# Wait for every process matching a command line to exit, then insist.
await_gone() {
    local pattern=$1 privileged=${2:-} attempt

    for attempt in $(seq 1 100); do
        pgrep -f "${pattern}" >/dev/null || return 0
        sleep 0.1
    done
    echo "warning: ${pattern} did not stop, so killing it" >&2
    ${privileged} pkill -KILL -f "${pattern}" 2>/dev/null || true
}

start() {
    [[ -e ${STATE} ]] &&
        { echo "error: ${STATE} exists. Run '$0 stop' first." >&2; exit 1; }

    for tool in etcd gazette; do
        command -v "${tool}" >/dev/null ||
            { echo "error: ${tool} is not on PATH; see the top of this script" >&2; exit 1; }
    done
    [[ -x ${DAEMON} ]] ||
        { echo "error: no daemon binary at ${DAEMON}; build it or set DAEMON=" >&2; exit 1; }
    [[ -e /sys/module/ublk_drv ]] ||
        { echo "error: ublk_drv is not loaded; run 'sudo modprobe ublk_drv'" >&2; exit 1; }

    for port in "${ETCD_PORT}" "${ETCD_PEER_PORT}" "${BROKER_PORT}"; do
        listening "${port}" &&
            { echo "error: something already listens on port ${port}" >&2; exit 1; }
    done

    mkdir -p "${STATE}/fragments" "${STATE}/images" "${STATE}/mounts"

    # A partial start leaves nothing running.
    trap 'echo "start failed, so cleaning up" >&2; stop >/dev/null 2>&1 || true' ERR

    nohup etcd --data-dir "${STATE}/etcd" \
        --listen-client-urls "http://127.0.0.1:${ETCD_PORT}" \
        --advertise-client-urls "http://127.0.0.1:${ETCD_PORT}" \
        --listen-peer-urls "http://127.0.0.1:${ETCD_PEER_PORT}" \
        --log-level error >"${STATE}/etcd.log" 2>&1 &
    wait_ready etcd "${ETCD_PORT}" "${STATE}/etcd.log"

    # `file-only` keeps every fragment on this host. `broker.host` makes the broker
    # advertise an address the daemon can reach.
    nohup gazette serve \
        --broker.host 127.0.0.1 \
        --broker.port "${BROKER_PORT}" \
        --broker.file-only \
        --broker.file-root "${STATE}/fragments" \
        --etcd.address "http://127.0.0.1:${ETCD_PORT}" \
        --log.level warn >"${STATE}/gazette.log" 2>&1 &
    wait_ready gazette "${BROKER_PORT}" "${STATE}/gazette.log"

    # The daemon needs CAP_SYS_ADMIN to serve a ublk device and to mount a
    # filesystem. It gives each mount to the client which opened the disk, so an
    # example needs no privilege.
    nohup sudo "${DAEMON}" serve \
        --uds-path "${UDS}" \
        --image-dir "${STATE}/images" \
        --mount-dir "${STATE}/mounts" >"${STATE}/daemon.log" 2>&1 &
    wait_ready "the disk daemon" "${UDS}" "${STATE}/daemon.log"

    trap - ERR

    # What an example reads. `stop` finds the services by their command lines, not
    # by this file, so losing it strands nothing.
    cat >"${STATE}/services.env" <<EOF
UDS_PATH=${UDS}
BROKER_ENDPOINT=http://127.0.0.1:${BROKER_PORT}
EOF

    echo "etcd, one broker, and the disk daemon are running. Their logs are in ${STATE}."

    # An export left over from an earlier run would send an example elsewhere.
    for expected in "UDS_PATH:${UDS}" "BROKER_ENDPOINT:http://127.0.0.1:${BROKER_PORT}"; do
        local name=${expected%%:*} want=${expected#*:} have
        have=${!name:-}

        if [[ -n ${have} && ${have} != "${want}" ]]; then
            echo >&2
            echo "warning: ${name}=${have} is exported, but these services are at ${want}." >&2
            echo "         Run 'unset ${name}', or export the value below." >&2
        fi
    done
    echo

    # Tell an example only what it cannot assume.
    if [[ ${UDS} != "${DEMO_UDS_DEFAULT}" ]]; then
        echo "    export UDS_PATH=${UDS}"
    fi
    if [[ http://127.0.0.1:${BROKER_PORT} != "${DEMO_ENDPOINT_DEFAULT}" ]]; then
        echo "    export BROKER_ENDPOINT=http://127.0.0.1:${BROKER_PORT}"
    fi
    echo "    cargo run -p disk-daemon --example basic"
    echo "    cargo run -p disk-daemon --example two_phase_commit"
    echo
    echo "Stop them with '$0 stop'."
}

stop() {
    if [[ ! -d ${STATE} ]]; then
        echo "nothing to stop: ${STATE} does not exist"
        return 0
    fi
    # Each service is matched by the state directory in its command line, which is
    # unique to this run, so a lost `services.env` strands nothing.
    #
    # Only root can signal the daemon. SIGTERM drains it, so it unmounts each disk
    # and deletes each device before it exits.
    sudo pkill -TERM -f "flow-disk-daemon serve --uds-path ${UDS}" 2>/dev/null || true
    await_gone "flow-disk-daemon serve --uds-path ${UDS}" sudo

    for pattern in "gazette serve .*--broker.file-root ${STATE}/fragments" \
        "etcd --data-dir ${STATE}/etcd"; do
        pkill -TERM -f "${pattern}" 2>/dev/null || true
        await_gone "${pattern}"
    done

    # The daemon removes each mount directory it made, so anything left here
    # outlived its session.
    local left
    left=$(ls -A "${STATE}/mounts" 2>/dev/null || true)
    [[ -n ${left} ]] && echo "warning: these mounts were left behind: ${left}" >&2

    sudo rm -rf "${STATE}"
    echo "stopped, and removed ${STATE}"
}

status() {
    if [[ ! -f ${STATE}/services.env ]]; then
        echo "not started: no ${STATE}/services.env"
        return 0
    fi
    local UDS_PATH='' BROKER_ENDPOINT=''
    # shellcheck source=/dev/null
    source "${STATE}/services.env"

    for named in "etcd:etcd --data-dir ${STATE}/etcd" \
        "gazette:gazette serve .*--broker.file-root ${STATE}/fragments" \
        "the disk daemon:flow-disk-daemon serve --uds-path ${UDS}"; do
        local name=${named%%:*} pattern=${named#*:} pids

        pids=$(pgrep -d' ' -f "${pattern}" || true)
        echo "${name}: ${pids:-NOT running}"
    done
    echo "UDS_PATH=${UDS_PATH} BROKER_ENDPOINT=${BROKER_ENDPOINT}"
}

case "${1:-}" in
start) start ;;
stop) stop ;;
status) status ;;
*)
    echo "usage: $0 {start|stop|status}" >&2
    exit 2
    ;;
esac
