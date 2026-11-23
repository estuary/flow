#!/bin/sh
# Scripted podman for readiness tests; start is controlled through a FIFO.
set -eu
dir=$READINESS_TEST_DIR
printf '%s\n' "$*" >>"$dir/calls"
# The ID of a kept ingress grant's container, which a test writes as `grant`.
grant_id=0000000000000000000000000000000000000000000000000000000000000002

# A command matching the pattern in `hang` never returns: only a kill ends it.
if [ -e "$dir/hang" ]; then
    case "$*" in
    $(cat "$dir/hang"))
        printf '%s\n' "$@" >"$dir/hung-args"
        exec 3<>"$dir/hung"
        echo hung >>"$dir/events"
        read -r _ <&3
        exit 47
        ;;
    esac
fi

case "$1 ${2:-}" in
"pull "*) ;;
"image inspect")
    # The VMM image's ID, once pulled if a test has it missing.
    if [ "${3:-}" = --format ]; then
        if [ -e "$dir/vmm-missing" ]; then rm "$dir/vmm-missing"; exit 125; fi
        echo 00000000000000000000000000000000000000000000000000000000000000aa
        exit 0
    fi
    if [ -e "$dir/inspection" ]; then cat "$dir/inspection"; exit 0; fi
    echo '[{"Id":"synthetic-image","Created":"2026-01-01T00:00:00Z","Config":{"Env":[],"Labels":{"FLOW_RUNTIME_PROTOCOL":"derive"}}}]'
    ;;
"run "*)
    for arg; do
        case "$arg" in
        verify)
            exit "$(cat "$dir/verify-status" 2>/dev/null || echo 0)"
            ;;
        grant) touch "$dir/ingress"; exit 0 ;;
        revoke)
            [ ! -e "$dir/refuse-revoke" ] || exit 44
            rm -f "$dir/ingress"; exit 0 ;;
        esac
    done
    exit 43
    ;;
"inspect --format")
    echo 10.89.0.2
    ;;
"network create")
    for arg; do name=$arg; done
    if [ -e "$dir/hold-network" ]; then
        exec 3<>"$dir/control"
        echo network-create >>"$dir/events"
        read -r command <&3
        [ "$command" = release-network ] || exit 46
    fi
    if [ ! -e "$dir/network-error" ] || [ -e "$dir/partial-network" ]; then
        printf '%s\n' "$name" >"$dir/network"
    fi
    if [ -e "$dir/network-error" ]; then cat "$dir/network-error" >&2; exit 125; fi
    ;;
"network ls")
    if [ -e "$dir/network" ]; then cat "$dir/network"; fi
    ;;
"network rm")
    rm "$dir/network"
    ;;
"volume create")
    if [ -e "$dir/volume-error" ]; then cat "$dir/volume-error" >&2; exit 125; fi
    for arg; do name=$arg; done
    printf '%s\n' "$name" >"$dir/volume"
    ;;
"volume ls")
    if [ -e "$dir/volume" ]; then cat "$dir/volume"; fi
    ;;
"volume rm")
    rm "$dir/volume"
    ;;
"create --rm")
    for arg; do
        case "$arg" in
        --name=*) printf '%s\n' "${arg#--name=}" >"$dir/container-name" ;;
        --mount=type=bind,source=*,target=/sock)
            sock=${arg#--mount=type=bind,source=}
            printf '%s/init.sock' "${sock%,target=/sock}" >"$dir/socket"
            ;;
        esac
    done
    echo 0000000000000000000000000000000000000000000000000000000000000001 | tee "$dir/container"
    ;;
"start --attach")
    # Opened before announcing, so that no command is lost.
    exec 3<>"$dir/control"
    echo attached >>"$dir/events"
    while read -r command status <&3; do
        case "$command" in
        stderr)
            cat "$dir/stderr" >&2
            echo written >>"$dir/events"
            ;;
        close-stderr)
            exec 2>&-
            ;;
        exit | rm)
            echo "$command" >"$dir/ended"
            exit "$status"
            ;;
        esac
    done
    ;;
"ps --all")
    case "$*" in
    *'{{.Names}}'*)
        if [ -e "$dir/container" ]; then cat "$dir/container-name"; fi
        if [ -e "$dir/grant" ]; then cat "$dir/grant-name"; fi
        ;;
    *)
        if [ -e "$dir/container" ]; then cat "$dir/container"; fi
        if [ -e "$dir/grant" ]; then echo "$grant_id"; fi
        ;;
    esac
    ;;
"container inspect")
    cat "$dir/grant"
    ;;
"rm --force")
    # Opened read-write, which never blocks, whether or not it's still there.
    rm -f "$dir/container" "$dir/container-name"
    for arg; do
        if [ "$arg" = "$grant_id" ]; then rm -f "$dir/grant" "$dir/grant-name"; fi
    done
    echo 'rm 137' 1<>"$dir/control"
    ;;
*)
    echo "unexpected engine command: $*" >&2
    exit 45
    ;;
esac
