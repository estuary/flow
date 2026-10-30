#!/usr/bin/env bash
# Record resources before creating them so failed tests can clean up.
set -euo pipefail

TEST=$(dirname "$(readlink -f "$0")")
readonly TEST
readonly RESOURCES="${TEST%/*}/resources"
readonly CALL="$*"

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
"volume create")
    printf 'volume %s\n' "${!#}" >>"${RESOURCES}"
    step=volume-create
    ;;
"volume ls")
    step=volume-ls
    ;;
"volume rm")
    step=volume-rm
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
    if [[ -d "${TEST}/hooks" ]]; then
        set -- "--hooks-dir=${TEST}/hooks" "$@"
    fi
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

# Verify the launcher's completed mount before podman can use it or teardown
# can remove it. The platform test supplies the expected static binary.
if [[ "${step}" == create && -e "${TEST}/expected-connector-init" ]]; then
    mount= name=
    for arg in "$@"; do
        case "${arg}" in
        --env=CONNECTOR_MOUNT=*) mount=${arg#--env=CONNECTOR_MOUNT=} ;;
        --name=*) name=${arg#--name=} ;;
        esac
    done
    [[ -n "${mount}" && -n "${name}" ]]
    cmp -- "$(<"${TEST}/expected-connector-init")" "${mount}/flow-connector-init" >&2
    printf '%s\n' "${name}" >>"${TEST}/checked-mounts"
fi

# Whether this call is the one a hold of its step catches: only the Nth while
# the hold is in place, N being what the hold holds or else 1, so that a hold
# never catches a launch other than the one the test awaits. Each call claims
# the next ordinal by exclusive creation.
claim() {
    [[ -n "${step}" && -e "${TEST}/$1-${step}" ]] || return 1
    if [[ -e "${TEST}/$1-${step}-naming" ]]; then
        local naming matched=0
        while IFS= read -r naming; do
            if [[ -n "${naming}" && "${CALL}" == *"${naming}"* ]]; then
                matched=1
            fi
        done <"${TEST}/$1-${step}-naming"
        [[ "${matched}" -eq 1 ]] || return 1
    fi
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
    printf '%s %s\n' "$$" "${status}" >"${TEST}/.held-after-${step}"
    mv "${TEST}/.held-after-${step}" "${TEST}/held-after-${step}"
    while [[ -e "${TEST}/hold-after-${step}" ]]; do
        sleep 0.05
    done
    exit "${status}"
fi

if [[ "${step}" == create && -e "${TEST}/hold-created" ]] && mkdir "${TEST}/taken-hold-created-1" 2>/dev/null; then
    # Made, but not yet reported.
    status=0
    "${podman[@]}" "$@" || status=$?
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
