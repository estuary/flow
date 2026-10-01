#!/usr/bin/env bash
# Record resources before creating them so failed tests can clean up.
set -euo pipefail

TEST=$(dirname "$(readlink -f "$0")")
readonly TEST
readonly RESOURCES="${TEST%/*}/resources"

printf '%s\n' "$*" >>"${TEST}/calls"

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
esac

if [[ -n "${step}" && -e "${TEST}/fail-${step}" ]]; then
    fault=$(<"${TEST}/fail-${step}")
    if [[ -z "${fault}" || "$*" == *"${fault}"* ]]; then
        echo "podman.sh: failing ${step}, as the test asks" >&2
        exit 125
    fi
fi

# Test-only VMM flags, one per line, follow the launcher's own arguments to
# the VMM's `run`, where a reference line's caller appends them.
if [[ "${step}" == create && -e "${TEST}/vmm-flags" ]]; then
    mapfile -t flags <"${TEST}/vmm-flags"
    set -- "$@" "${flags[@]}"
fi

if [[ -n "${step}" && -e "${TEST}/hold-${step}" ]]; then
    : >"${TEST}/held-${step}"
    while [[ -e "${TEST}/hold-${step}" ]]; do
        sleep 0.05
    done
fi

if [[ "${step}" == create && -e "${TEST}/hold-created" ]]; then
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
    exec "${podman[@]}" "$@" 2> >(exec python3 "${TEST}/gate.py" "${TEST}/held-readiness" >&3 3>&-) 3>&-
fi
exec "${podman[@]}" "$@"
