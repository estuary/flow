#!/usr/bin/env bash
# Record resources before creating them so failed tests can clean up.
set -euo pipefail

TEST=$(dirname "$(readlink -f "$0")")
readonly TEST
readonly RESOURCES="${TEST%/*}/resources"

# The launcher's own arguments, before any control below changes them.
printf '%s\n' "$*" >>"${TEST}/calls"

# Check before sudo, which may otherwise hide a leak from the launcher.
if [[ -e "${TEST}/require-clean-environment" ]]; then
    for name in CONSUMER_AUTH_KEYS BROKER_AUTH_KEYS SOPS_AGE_KEY FLOW_AUTH_TOKEN; do
        if [[ -v "${name}" ]]; then
            echo "podman.sh: unexpected platform authority: ${name}" >&2
            exit 125
        fi
    done
    printf '%s\n' "$*" >>"${TEST}/clean-environment"
fi

# A test-created tag exists only in local storage, so pulling it would ask
# the registry for an image the test owns on this host.
if [[ "${1:-}" == pull && -e "${TEST}/local-image" && "${2:-}" == "$(<"${TEST}/local-image")" ]]; then
    exit 0
fi

# An owner in a PID namespace of its own still drives the host's podman, in
# the host's: podman records container PIDs as it sees them. No process can
# join an ancestor PID namespace, so systemd runs it, with this script's
# stdin, stdout and stderr.
podman=(sudo -n podman)
if [[ -n "${CONNECTOR_VMM_TESTS_HOST_PID:-}" ]]; then
    podman=(sudo -n systemd-run --quiet --pipe --wait --collect --service-type=exec -- podman)
fi

step=
case "${1:-} ${2:-}" in
"network create")
    printf 'network %s\n' "${!#}" >>"${RESOURCES}"
    step=network-create
    ;;
"network ls")
    step=network-ls
    ;;
"network rm")
    step=network-rm
    ;;
"rm "*)
    step=rm
    ;;
"create "*)
    for arg in "$@"; do
        if [[ "${arg}" == --name=* ]]; then
            printf 'container %s\n' "${arg#--name=}" >>"${RESOURCES}"
        fi
    done
    step=create
    ;;
"start --attach")
    step=start
    ;;
"ps "*)
    step=ps
    ;;
"run "*)
    if [[ $# -ge 2 && "${*: -2:1}" == boundary && "${*: -1}" == verify ]]; then
        step=verify
    fi
    ;;
esac

if [[ -n "${step}" && -e "${TEST}/fail-${step}" ]]; then
    fault=$(<"${TEST}/fail-${step}")
    if [[ -z "${fault}" || "$*" == *"${fault}"* ]]; then
        echo "podman.sh: failing ${step}, as the test asks" >&2
        exit 125
    fi
fi

# Stalled before podman runs, as by a wedged engine, until the control goes
# or the launcher gives up on it: every call of the step whose arguments
# contain a line of the control (every call, if it's empty), where a hold
# catches one. The mark names this script, when it stalled and the call, and
# appears whole.
if [[ -n "${step}" && -e "${TEST}/stall-${step}" ]]; then
    mapfile -t namings <"${TEST}/stall-${step}"
    stall=$((${#namings[@]} == 0))
    for naming in "${namings[@]}"; do
        if [[ -n "${naming}" && "$*" == *"${naming}"* ]]; then
            stall=1
        fi
    done
    if [[ "${stall}" -eq 1 ]]; then
        printf '%s %s %s\n' "$$" "${EPOCHREALTIME}" "$*" >"${TEST}/.stalled-${step}-$$"
        mv "${TEST}/.stalled-${step}-$$" "${TEST}/stalled-${step}-$$"
        while [[ -e "${TEST}/stall-${step}" ]]; do
            sleep 0.05
        done
    fi
fi

# Test-only VMM flags, one per line, follow the launcher's own arguments to
# the VMM's `run`, where a reference line's caller appends them.
if [[ "${step}" == create && -e "${TEST}/vmm-flags" ]]; then
    mapfile -t flags <"${TEST}/vmm-flags"
    set -- "$@" "${flags[@]}"
fi

# A container without KVM, as a launch on a host which cannot pass it one.
if [[ "${step}" == create && -e "${TEST}/no-kvm" ]]; then
    args=()
    for arg in "$@"; do
        if [[ "${arg}" != --device=/dev/kvm ]]; then
            args+=("${arg}")
        fi
    done
    set -- "${args[@]}"
fi

# Verification of the tables in the named network namespace, which the test
# owns, in place of the host's.
if [[ "${step}" == verify && -e "${TEST}/verify-netns" ]]; then
    netns=$(<"${TEST}/verify-netns")
    args=()
    for arg in "$@"; do
        if [[ "${arg}" == --network=host ]]; then
            arg="--network=ns:/run/netns/${netns}"
        fi
        args+=("${arg}")
    done
    set -- "${args[@]}"
fi

# Whether this call is the one a hold of its step catches: only the Nth while
# the hold is in place, N being what the hold holds or else 1, so that a hold
# never catches a launch other than the one the test awaits. Each call claims
# the next ordinal by exclusive creation.
claim() {
    [[ -n "${step}" && -e "${TEST}/$1-${step}" ]] || return 1
    local nth ordinal=1
    nth=$(<"${TEST}/$1-${step}")
    while ! mkdir "${TEST}/taken-$1-${step}-${ordinal}" 2>/dev/null; do
        if [[ ! -d "${TEST}/taken-$1-${step}-${ordinal}" ]]; then
            echo "podman.sh: cannot claim ${step}'s ordinal ${ordinal} for $1" >&2
            exit 125
        fi
        ordinal=$((ordinal + 1))
    done
    [[ "${ordinal}" -eq "${nth:-1}" ]]
}

# Held as root, after sudo, so that what kills this script and its sudo, but
# can't signal root's processes, leaves the command to run once released,
# holding its stdin, a launch's fence, all the while. The mark names the
# root shell, which becomes podman, and appears whole.
if claim root-hold; then
    exec sudo -n bash -c '
        printf "%s\n" "$$" >"$1.tmp"
        mv "$1.tmp" "$1"
        while [[ -e "$2" ]]; do sleep 0.05; done
        shift 2
        exec "$@"' root-hold "${TEST}/held-${step}" "${TEST}/root-hold-${step}" "${podman[@]:2}" "$@"
fi

# The mark names this script, and appears whole.
if claim hold; then
    printf '%s\n' "$$" >"${TEST}/.held-${step}"
    mv "${TEST}/.held-${step}" "${TEST}/held-${step}"
    while [[ -e "${TEST}/hold-${step}" ]]; do
        sleep 0.05
    done
fi

# Held once the command has finished and before the launcher hears of it, so
# that a test sees what it did, and may kill the launcher, with nothing left
# for the command to change. The mark names this script and the command's
# exit status, and appears whole.
if claim hold-after; then
    status=0
    "${podman[@]}" "$@" || status=$?
    exec 0<&-
    printf '%s %s\n' "$$" "${status}" >"${TEST}/.held-after-${step}"
    mv "${TEST}/.held-after-${step}" "${TEST}/held-after-${step}"
    while [[ -e "${TEST}/hold-after-${step}" ]]; do
        sleep 0.05
    done
    exit "${status}"
fi

if [[ "${step}" == create && -e "${TEST}/hold-created" ]] && mkdir "${TEST}/taken-hold-created-1" 2>/dev/null; then
    # Made, and its fence let go with the command, but not yet reported.
    status=0
    "${podman[@]}" "$@" || status=$?
    exec 0<&-
    : >"${TEST}/held-created"
    while [[ -e "${TEST}/hold-created" ]]; do
        sleep 0.05
    done
    exit "${status}"
fi

if [[ "${step}" == start && -e "${TEST}/gate-readiness" ]]; then
    # Redirections apply in order, so the gate starts while fd 3 is still open.
    exec 3>&2
    exec "${podman[@]}" "$@" 2> >(exec python3 "${TEST}/gate.py" "${TEST}/held-readiness" "$(<"${TEST}/gate-readiness")" >&3 3>&-) 3>&-
fi
exec "${podman[@]}" "$@"
