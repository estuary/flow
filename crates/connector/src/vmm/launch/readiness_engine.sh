#!/bin/sh
# Scripted podman for readiness tests; start is controlled through a FIFO.
set -eu
dir=$READINESS_TEST_DIR
printf '%s\n' "$*" >>"$dir/calls"

case "$1 ${2:-}" in
"pull "*) ;;
"image inspect")
    if [ -e "$dir/inspection" ]; then cat "$dir/inspection"; exit 0; fi
    echo '[{"Id":"synthetic-image","Created":"2026-01-01T00:00:00Z","Config":{"Env":[],"Labels":{"FLOW_RUNTIME_PROTOCOL":"derive"}}}]'
    ;;
"run --rm")
    for arg; do
        case "$arg" in
        verify) exit 0 ;;
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
    printf '%s\n' "$name" >"$dir/network"
    ;;
"network ls")
    if [ -e "$dir/network" ]; then cat "$dir/network"; fi
    ;;
"network rm")
    rm "$dir/network"
    ;;
"volume create")
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
        exit | rm)
            echo "$command" >"$dir/ended"
            exit "$status"
            ;;
        esac
    done
    ;;
"ps --all")
    if [ -e "$dir/container" ]; then cat "$dir/container"; fi
    ;;
"rm --force")
    # Opened read-write, which never blocks, whether or not it's still there.
    rm -f "$dir/container"
    echo 'rm 137' 1<>"$dir/control"
    ;;
*)
    echo "unexpected engine command: $*" >&2
    exit 45
    ;;
esac
