#!/bin/bash
# Shard surgery for the consistency suite, over `gazctl`.
#
# These operations are not part of `flowctl` on purpose. Unassigning a shard and joining
# a task's shards are local test affordances, not things a connector author or an
# operator needs from the CLI, and the suite is their only caller. `gazctl` already does
# both; all that was missing was a way to point it at a Flow data plane, which
# `flowctl raw gazctl-env` provides.
#
# Usage:
#   shard-tools.sh unassign <task> [--failed]
#   shard-tools.sh join     <task> [--dry-run]
set -euo pipefail

usage() {
    echo "usage: $(basename "$0") {unassign|join} <task> [options]" >&2
    exit 2
}

[ $# -ge 2 ] || usage
COMMAND=$1
TASK=$2
shift 2

# Resolved rather than assumed: the suite spawns this with a minimal environment, so
# neither tool is necessarily on PATH. `go install` puts gazctl under GOPATH/bin, which
# is where a checkout that has run `mise run local:stack` will have it.
resolve() {
    local var=$1 name=$2
    if [ -n "${!var:-}" ]; then echo "${!var}"; return; fi
    if command -v "${name}" >/dev/null 2>&1; then command -v "${name}"; return; fi
    local candidate
    for candidate in "$(go env GOPATH 2>/dev/null)/bin/${name}" "${HOME}/go/bin/${name}"; do
        [ -x "${candidate}" ] && { echo "${candidate}"; return; }
    done
    echo "error: ${name} not found; set ${var} to its path" >&2
    exit 1
}

FLOWCTL=$(resolve FLOWCTL flowctl)
GAZCTL=$(resolve GAZCTL gazctl)
SELECTOR="estuary.dev/task-name=${TASK}"

# Authorize gazctl against the task's data plane. `--name` resolves the data plane from
# the catalog name, so the task must still exist; `--admin` is required because both
# operations mutate.
authorize() {
    local env
    env=$("${FLOWCTL}" raw gazctl-env --name "${TASK}" --admin)
    eval "${env}"
    export BROKER_ADDRESS BROKER_AUTH_TOKEN CONSUMER_ADDRESS CONSUMER_AUTH_TOKEN
}

case "${COMMAND}" in
unassign)
    authorize
    # `--failed` limits it to shards the allocator has given up on, which is the usual
    # case after an injected crash. Without it, every shard is rescheduled.
    "${GAZCTL}" shards unassign --selector "${SELECTOR}" "$@"
    ;;

join)
    authorize
    SPECS=$(mktemp -t shard-join-XXXXXX.yaml)
    trap 'rm -f "${SPECS}"' EXIT

    "${GAZCTL}" shards list --selector "${SELECTOR}" -o yaml >"${SPECS}.orig"

    DRY_RUN=false
    for arg in "$@"; do
        [ "${arg}" = "--dry-run" ] && DRY_RUN=true
    done

    python3 "$(dirname "$0")/join-shards.py" "${SPECS}.orig" "${SPECS}"
    rm -f "${SPECS}.orig"

    if [ "${DRY_RUN}" = true ]; then
        cat "${SPECS}"
        exit 0
    fi
    "${GAZCTL}" shards apply --specs "${SPECS}"
    ;;

*)
    usage
    ;;
esac
