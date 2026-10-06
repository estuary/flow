#!/usr/bin/env bash
#MISE hide=true
#MISE description="(library) ci:connector-vmm-kvm's resources list and its release - not runnable"
#
# Sourced by mise/tasks/ci/connector-vmm-kvm once it has set ROOT and TABLE,
# and by crates/connector-vmm-tests/tests/release.rs, which runs `release`
# against stand-ins for the commands it calls.

# Everything a run creates, the tests' containers and directories included,
# is appended to ${ROOT}/resources before it exists, and removed exactly from
# that list, newest first. Nothing is ever removed by age or by a name pattern.
record() {
    printf '%s %s\n' "$1" "$2" >>"${ROOT}/resources"
}

# `flow-connector-vmm boundary ACTION` from VMM image $1, in the host's network
# namespace. Must match `launch::boundary` in crates/connector-vmm-tests.
boundary() {
    sudo -n podman run --rm --network=host --log-driver=none --read-only \
        --cap-drop=all --cap-add=CAP_NET_ADMIN "$1" boundary "$2"
}

# The running units of stack $1, whose data plane is $2, scoped as local:stop
# scopes them.
stack_units() {
    systemctl --user list-units --no-legend --plain \
        --state=active,activating,deactivating,reloading,failed \
        "flow-*@$1.service" "flow-*@$1.target" \
        "flow-*@$2.service" "flow-*@$2.target" \
        "flow-*@$2-*.service" "flow-*@$2-*.target"
}

# Stops the stack recorded as "<name> <data plane> <checkout>", and shows it
# stopped: local:stop carries on past units which fail to stop, so its status
# alone proves nothing.
stop_stack() {
    local name cluster checkout units
    read -r name cluster checkout <<<"$1"
    if [ -z "${checkout}" ]; then
        echo "ci:connector-vmm-kvm: a stack is recorded as '$1', which names no checkout" >&2
        return 1
    fi
    if ! (cd "${checkout}" && mise run local:stop); then
        echo "ci:connector-vmm-kvm: local:stop of stack '${name}' failed" >&2
        return 1
    fi
    if ! units=$(stack_units "${name}" "${cluster}"); then
        echo "ci:connector-vmm-kvm: could not list stack '${name}''s systemd units" >&2
        return 1
    fi
    if [ -n "${units}" ]; then
        echo "ci:connector-vmm-kvm: stack '${name}' still has units running:" >&2
        echo "${units}" >&2
        return 1
    fi
}

# Whether no launch beneath a listed platform or launcher directory has an
# owner. A live launcher holds its record's lock, and so does a podman command
# it fenced, which outlives it and may still create a network or container.
launches_unowned() {
    local kind value records record status unowned=true
    while read -r kind value; do
        if [ "${kind}" != dir ]; then
            continue
        fi
        case "${value}" in
        "${ROOT}"/platform-* | "${ROOT}"/launcher-*) ;;
        *) continue ;;
        esac
        if [ ! -e "${value}" ]; then
            continue
        fi
        if ! records=$(find "${value}" -mindepth 2 -maxdepth 2 -path "${value}/state/fv_*.owner"); then
            echo "ci:connector-vmm-kvm: could not look for records beneath ${value}" >&2
            unowned=false
            continue
        fi
        while read -r record; do
            if [ -z "${record}" ]; then
                continue
            fi
            status=0
            flock -n -E 75 "${record}" true || status=$?
            case "${status}" in
            0) ;;
            75)
                echo "ci:connector-vmm-kvm: a launch still owns ${record}" >&2
                unowned=false
                ;;
            *)
                echo "ci:connector-vmm-kvm: could not try the lock of ${record}" >&2
                unowned=false
                ;;
            esac
        done <<<"${records}"
    done < <(tac "${ROOT}/resources")
    [ "${unowned}" = true ]
}

# Runs the rest of the arguments, which remove resource $1 $2, if a listing
# which succeeded shows it exists; fails if the listing failed. Not by the
# commands' own lookups: they exit 1 for something absent, as sudo and ip do
# when they themselves fail.
remove_listed() {
    local listing
    case "$1" in
    network) listing=$(sudo -n podman network ls --format '{{.Name}}') ;;
    builder) listing=$(docker buildx ls --format '{{.Name}}') ;;
    # "<name>" or "<name> (id: <n>)".
    netns) listing=$(ip netns list | cut -d' ' -f1) ;;
    # "<index>: <name>[@<peer>]: ...".
    link) listing=$(ip -o link show | cut -d' ' -f2 | sed 's/@.*//; s/:$//') ;;
    # ip ends each route with a space.
    route) listing=$(ip route show type blackhole | sed 's/ *$//') ;;
    esac || {
        echo "ci:connector-vmm-kvm: could not tell whether $1 $2 exists" >&2
        return 1
    }
    if grep -qxF -- "$2" <<<"${listing}"; then
        "${@:3}"
    fi
}

# Removes everything the list holds, unless a stack in it can't be shown
# stopped or a launch beneath it is still owned: a launcher still running,
# idle or before it has made its network, could make a VMM network after the
# boundary had gone. Then nothing is removed, the list is kept whole for a
# later run, and the release fails.
#
# Otherwise each entry, newest first, is removed or shown already gone, or is
# kept in the list for a later run and the release fails. A VMM, network or
# boundary which is kept may still use what was made before it, so that is
# kept untried: state directories and the ownership records in them, the
# boundary around VMM bridges, and images, among them the one a kept
# boundary's removal runs. Images and the buildx builder only take space:
# failing to remove them is reported, and nothing more.
release() {
    # One removal failing must not stop the rest.
    local - kind value stopped=true status kept=() standing=false
    set +e -o pipefail
    while read -r kind value; do
        if [ "${kind}" = stack ] && ! stop_stack "${value}"; then
            stopped=false
        fi
    done < <(tac "${ROOT}/resources")
    if [ "${stopped}" = true ] && ! launches_unowned; then
        stopped=false
    fi
    if [ "${stopped}" = false ]; then
        echo "ci:connector-vmm-kvm: kept everything in ${ROOT}/resources, the host boundary" \
            "included, since a launcher may still be running. Stop it, then run this task" \
            "again to release:" >&2
        sed 's/^/  /' "${ROOT}/resources" >&2
        return 1
    fi

    while read -r kind value; do
        if [ "${standing}" = true ]; then
            case "${kind}" in
            dir | boundary | image)
                kept+=("${kind} ${value}")
                continue
                ;;
            esac
        fi
        status=0
        case "${kind}" in
        stack) ;;
        dropin)
            case "${value}" in
            "${HOME}"/.config/systemd/user/flow-reactor@*.service.d/connector-vmm.conf)
                rm -f "${value}" && systemctl --user daemon-reload || status=$?
                ;;
            *) echo "Not removing ${value}: it is not a connector VMM drop-in" >&2 ;;
            esac
            ;;
        # --ignore excuses only a container which is already gone.
        container) sudo -n podman rm -f --time 0 --ignore "${value}" >/dev/null || status=$? ;;
        image) sudo -n podman image rm --ignore "${value}" >/dev/null || echo "Left image ${value}" >&2 ;;
        builder)
            remove_listed builder "${value}" docker buildx rm "${value}" >/dev/null \
                || echo "Left buildx builder ${value}" >&2
            ;;
        # Refused while a container still uses it.
        network) remove_listed network "${value}" sudo -n podman network rm "${value}" >/dev/null || status=$? ;;
        netns) remove_listed netns "${value}" sudo -n ip netns del "${value}" || status=$? ;;
        # Refused while any VMM bridge remains, this run's or another owner's.
        # Tables already gone are no failure.
        boundary) boundary "${value}" remove || status=$? ;;
        link) remove_listed link "${value}" sudo -n ip link del "${value}" || status=$? ;;
        # "blackhole <prefix> metric <n>", deliberately split.
        route) remove_listed route "${value}" sudo -n ip route del ${value} || status=$? ;;
        dir)
            case "${value}" in
            "${ROOT}"/state/fv_* | "${ROOT}"/connector-mounts-0/mount-* | "${ROOT}"/launcher-* | "${ROOT}"/platform-*)
                sudo -n rm -rf --one-file-system "${value}" || status=$?
                ;;
            *) echo "Not removing ${value}: it is outside ${ROOT}" >&2 ;;
            esac
            ;;
        *) echo "Not removing unknown resource: ${kind} ${value}" >&2 ;;
        esac
        if [ "${status}" -ne 0 ]; then
            echo "ci:connector-vmm-kvm: could not remove ${kind} ${value}" >&2
            kept+=("${kind} ${value}")
            case "${kind}" in
            container | network | boundary) standing=true ;;
            esac
        fi
    done < <(tac "${ROOT}/resources")

    if [ "${#kept[@]}" -eq 0 ]; then
        : >"${ROOT}/resources"
        return
    fi
    printf '%s\n' "${kept[@]}" | tac >"${ROOT}/resources"
    echo "ci:connector-vmm-kvm: kept in ${ROOT}/resources what could not be shown removed," \
        "and what it may still use. Put right what failed, then run this task again to release:" >&2
    sed 's/^/  /' "${ROOT}/resources" >&2
    return 1
}
