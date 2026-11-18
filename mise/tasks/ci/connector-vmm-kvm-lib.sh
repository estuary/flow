#!/usr/bin/env bash
#MISE hide=true
#MISE description="(library) ci:connector-vmm-kvm's root, its resources list and their release - not runnable"
#
# Sourced by mise/tasks/ci/connector-vmm-kvm once it has set ROOT, TABLE,
# CHECKOUT and the endpoints' NETNS, LINK, FENCE and DECOY_FENCE, and by
# crates/connector-vmm-tests/tests/release.rs, which runs `claim` and
# `release` against stand-ins for the commands they call.
#
# The task trusts only the invoking user, this checkout and root. ROOT is a
# fixed path in /var/tmp, where any user may create it first, so `claim`
# takes ROOT only if it is the invoking user's private directory.
#
# Two files in ROOT track what a run made. Its producers (`record`, and
# `run::record` and fixtures/podman.sh in crates/connector-vmm-tests) append
# each entry to `members`, then to `resources`, the list, and only then make
# the resource. `members` also vouches for the run's own VMM images, by
# name. release acts on an entry of the list only if `members` holds it too,
# and it matches what this run knows apart from both files: see `bound`.

# "<type> <owner> <mode> <links>" of $1 itself, from one lstat, or "absent".
# Type is d, f or l, or o for anything else, and mode its permission bits in
# octal. Tests of an entry made one by one could each see a different object
# where others can replace it. "absent" also covers what this user cannot
# see, beneath a directory they cannot search.
inspect() {
    local info type owner mode links
    if ! info=$(LC_ALL=C stat -c '%F|%u|%a|%h' -- "$1" 2>/dev/null); then
        if [ ! -e "$1" ] && [ ! -L "$1" ]; then
            echo absent
            return 0
        fi
        echo "ci:connector-vmm-kvm: could not inspect $1" >&2
        return 1
    fi
    IFS='|' read -r type owner mode links <<<"${info}"
    case "${type}" in
    directory) type=d ;;
    "regular file" | "regular empty file") type=f ;;
    "symbolic link") type=l ;;
    *) type=o ;;
    esac
    echo "${type} ${owner} ${mode} ${links}"
}

# Whether lstat result $1 is of a directory which only user $2 and root can
# change: owned by one of them, and writable by no one else. With a third
# argument, others may write it if it is sticky, as /var/tmp is: they may
# add entries to it, but not rename or remove another user's.
trusted() {
    local type owner mode links
    read -r type owner mode links <<<"$1"
    [ "${type}" = d ] && { [ "${owner}" = "$2" ] || [ "${owner}" = 0 ]; } \
        && { ! ((8#${mode} & 8#022)) || { [ -n "${3:-}" ] && ((8#${mode} & 8#1000)); }; }
}

# Whether each directory of relative path $2 beneath directory $1, as far as
# any exists, is `trusted` by user $3 (with $4, if sticky). What is removed
# at the end of such a path is reached through no link.
trusted_path() {
    local at=${1%/} component components info
    IFS=/ read -r -a components <<<"$2"
    for component in "${components[@]}"; do
        at+=/${component}
        info=$(inspect "${at}") || return 1
        if [ "${info}" = absent ]; then
            return 0
        fi
        if ! trusted "${info}" "$3" "${4:-}"; then
            echo "ci:connector-vmm-kvm: refusing ${at} (${info}): it is not a directory only you (UID $3) and root can change" >&2
            return 1
        fi
    done
}

# Whether $1 is absent, or a regular file of user $2's, writable by no one
# else, with no other name: another name could reach it from elsewhere.
private_file() {
    local info type owner mode links
    info=$(inspect "$1") || return 1
    read -r type owner mode links <<<"${info}"
    if [ "${type}" = absent ] \
        || { [ "${type}" = f ] && [ "${owner}" = "$2" ] && ! ((8#${mode} & 8#022)) && [ "${links}" = 1 ]; }; then
        return 0
    fi
    echo "ci:connector-vmm-kvm: refusing $1 (${info}): it must be a regular file of yours (UID $2)," \
        "writable by no one else, with no other name" >&2
    return 1
}

# Takes ROOT for this run, making it if it is absent, and holds its lock on
# fd 9 until the run ends.
#
# Each directory above ROOT must be `trusted`, sticky allowed. mkdir then
# makes ROOT with mode 0700, or fails if anything is there, and never follows
# a link. Whoever made ROOT, one lstat must show it a directory of the
# invoking user's with mode 0700. Its sticky parent then lets no one else
# rename or replace it, and nothing in it is anyone else's to change. A ROOT
# of root's is an earlier version of this task's, and is refused rather than
# adopted: see crates/connector-vmm-tests/README.md.
#
# The lock is made once and never replaced, so every run locks the same file.
# Concurrent claims may each make ROOT or open the lock. Only the one which
# holds it goes on to check and make the files in ROOT.
claim() {
    local uid parent info type owner mode links mask file
    uid=$(id -u) || return 1
    parent=${ROOT%/*}
    trusted_path / "${parent#/}" "${uid}" sticky || return 1

    (umask 077 && mkdir -m 0700 -- "${ROOT}" 2>/dev/null) || true
    info=$(inspect "${ROOT}") || return 1
    read -r type owner mode links <<<"${info}"
    if [ "${type}" = d ] && [ "${owner}" = 0 ] && [ "${uid}" != 0 ]; then
        echo "ci:connector-vmm-kvm: refusing ${ROOT}: it is root's, as an earlier version of this task" \
            "made it; see crates/connector-vmm-tests/README.md to release and remove it" >&2
        return 1
    fi
    if [ "${type}" != d ] || [ "${owner}" != "${uid}" ] || [ "${mode}" != 700 ]; then
        echo "ci:connector-vmm-kvm: refusing ${ROOT} (${info}): it must be a directory of yours (UID ${uid})" \
            "with mode 0700" >&2
        return 1
    fi

    private_file "${ROOT}/lock" "${uid}" || return 1
    mask=$(umask)
    umask 077
    exec 9>>"${ROOT}/lock"
    umask "${mask}"
    if ! flock -n 9; then
        echo "ci:connector-vmm-kvm: another run holds ${ROOT}/lock" >&2
        return 1
    fi
    for file in resources members run.json platform.json; do
        private_file "${ROOT}/${file}" "${uid}" || return 1
    done
    # Nothing listed: nothing an earlier run vouched for remains.
    if [ ! -s "${ROOT}/resources" ]; then
        replace resources </dev/null && replace members </dev/null
    fi
}

# local:stack's one reactor, which counts down from the top of its block.
reactor_unit() {
    echo "flow-reactor@${FLOW_CLUSTER}-$((FLOW_PLANE_BASE + 99)).service"
}

# Where, beneath HOME, the platform run's drop-in for that reactor goes.
dropin_dir() {
    echo ".config/systemd/user/$(reactor_unit).d"
}

# Vouches in members for "$1 $2": an entry release may act on, or, with a
# kind no entry has, a value which entries must match.
vouch() {
    printf '%s %s\n' "$1" "$2" >>"${ROOT}/members"
}

# Everything a run creates, the tests' containers and directories included,
# is vouched for and appended to ${ROOT}/resources before it exists, and
# removed exactly from that list, newest first. Nothing is ever removed by
# age or by a name pattern.
record() {
    vouch "$1" "$2"
    printf '%s %s\n' "$1" "$2" >>"${ROOT}/resources"
}

# Replaces ROOT/$1 with stdin. A new file is renamed over the old, so that a
# reader sees one or the other whole, and a link there is replaced rather
# than followed.
replace() {
    local staged=
    if staged=$(mktemp "${ROOT}/.$1.XXXXXX") && cat >"${staged}" && mv -f -- "${staged}" "${ROOT}/$1"; then
        return 0
    fi
    if [ -n "${staged}" ]; then
        rm -f -- "${staged}"
    fi
    echo "ci:connector-vmm-kvm: could not replace ${ROOT}/$1, which is unchanged" >&2
    return 1
}

# Reads ROOT/$1 whole, once, into array $2, a line an element. Fails rather
# than yield less if it cannot be read, its last line is unterminated, or it
# holds a NUL byte, which bash would drop.
snapshot() {
    local -n into=$2
    local content
    # No entry holds byte 0x01, so a NUL read as one is seen.
    if ! content=$(tr '\000' '\001' <"${ROOT}/$1" && echo .); then
        echo "ci:connector-vmm-kvm: could not read ${ROOT}/$1" >&2
        return 1
    fi
    content=${content%.}
    if [[ "${content}" == *$'\001'* ]]; then
        echo "ci:connector-vmm-kvm: refusing ${ROOT}/$1: it holds a NUL byte, or byte 0x01" >&2
        return 1
    fi
    into=()
    if [ -z "${content}" ]; then
        return 0
    fi
    if [ "${content: -1}" != $'\n' ]; then
        echo "ci:connector-vmm-kvm: refusing ${ROOT}/$1: its last line is unterminated" >&2
        return 1
    fi
    mapfile -t into <<<"${content%$'\n'}"
}

# Whether ROOT/$1 still holds what array $2 snapshot from it.
unchanged() {
    local -n was=$2
    local now=()
    snapshot "$1" now || return 1
    [ "${#now[@]}" -eq "${#was[@]}" ] && [ "$(printf '%s\n' "${now[@]}")" = "$(printf '%s\n' "${was[@]}")" ]
}

# `flow-connector-vmm boundary ACTION` from VMM image $1, in the host's network
# namespace, with the options of `connector::vmm_argv::host_boundary`. Never
# pulled, so that it runs an image this host already holds, or fails.
boundary() {
    sudo -n podman run --rm --pull=never --network=host --log-driver=none --read-only \
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
# alone proves nothing. `bound` has shown it is this checkout's stack.
stop_stack() {
    local name cluster checkout units
    read -r name cluster checkout <<<"$1"
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

# Whether $1 is one of the arguments which follow it.
among() {
    local item
    for item in "${@:2}"; do
        if [ "${item}" = "$1" ]; then
            return 0
        fi
    done
    return 1
}

# Check the entry against its producer's naming rules and run configuration.
bound() {
    local kind=$1 value=$2 uid=$3 rest
    local image='^(sha256:[0-9a-f]{64}|localhost/connector-vmm-test-controls:kvm-[0-9a-f]{8}'
    image+='|ghcr\.io/estuary/(capture|derive|materialize)-python:local'
    image+='|localhost/connector-vmm-test-guest(-root)?:[0-9a-f]{16}'
    # Must match the images the KVM and launcher tests build and tag.
    image+='|localhost/connector-vmm-kvm-(linked-accounts|no-new-privs|no-new-privs-root|environment'
    image+='|production-guest|restrictive-acmesvc|restrictive-root):[0-9a-f]{16}'
    image+='|localhost/connector-vmm-kvm-uv-cache:[0-9a-f]{16}-(custom|absent)'
    image+='|localhost/connector-vmm-kvm-working-dir:[0-9a-f]{16}-(root|tmp|var-tmp|etc|work|covered)'
    image+='|localhost/connector-vmm-tests-grant:[0-9a-f]{16}|ghcr\.io/estuary/derive-python:vmm-[0-9a-f]{16})$'
    local grant='^localhost/connector-vmm-test-controls:kvm-[0-9a-f]{8} [0-9a-f]{32}$'
    local network='^(fv_|vmm-api-)[0-9a-f]{16}$'
    local container='^(fv_[0-9a-f]{16}(-ingress)?|vmm-api-[0-9a-f]{16})$'
    local beneath='^(state/fv_[0-9a-f]{16}|connector-mounts-0/mount-[0-9a-f]{16}|launcher-[0-9a-f]{16}|platform-[0-9a-f]{8})$'

    case "${kind}" in
    stack)
        [ -n "${FLOW_STACK_NAME:-}" ] && [ -n "${FLOW_CLUSTER:-}" ] && [ -n "${CHECKOUT:-}" ] \
            && [ "${value}" = "${FLOW_STACK_NAME} ${FLOW_CLUSTER} ${CHECKOUT}" ]
        ;;
    dropin)
        [ -n "${FLOW_CLUSTER:-}" ] && [[ "${FLOW_PLANE_BASE:-}" =~ ^[0-9]+$ ]] \
            && [ "${value}" = "${HOME}/$(dropin_dir)/connector-vmm.conf" ] \
            && trusted_path "${HOME}" "$(dropin_dir)" "${uid}"
        ;;
    # As the task records the fences.
    route) [ "${value}" = "blackhole ${FENCE} metric 4000" ] || [ "${value}" = "blackhole ${DECOY_FENCE} metric 4000" ] ;;
    netns) [ "${value}" = "${NETNS}" ] || [[ "${value}" =~ ^connector-vmm-kvm-[0-9a-f]{16}$ ]] ;;
    link) [ "${value}" = "${LINK}" ] ;;
    builder) [[ "${value}" =~ ^connector-vmm-kvm-[0-9a-f]{8}$ ]] ;;
    image)
        [[ "${value}" =~ ${image} ]] || return 1
        case "${value}" in
        sha256:*) among "vmm-image ${value}" "${vouched[@]}" ;;
        localhost/connector-vmm-test-controls:*) among "test-vmm-image ${value}" "${vouched[@]}" ;;
        esac
        ;;
    boundary) [[ "${value}" =~ ^sha256:[0-9a-f]{64}$ ]] && among "vmm-image ${value}" "${vouched[@]}" ;;
    ingress) [[ "${value}" =~ ${grant} ]] && among "test-vmm-image ${value% *}" "${vouched[@]}" ;;
    network) [[ "${value}" =~ ${network} ]] ;;
    volume) [[ "${value}" =~ ^fv_[0-9a-f]{16}$ ]] ;;
    container) [[ "${value}" =~ ${container} ]] ;;
    dir)
        rest=${value#"${ROOT}"/}
        [ "${value}" = "${ROOT}/${rest}" ] && [[ "${rest}" =~ ${beneath} ]] \
            && trusted_path "${ROOT}" "${rest}" "${uid}"
        ;;
    *) false ;;
    esac
}

# Whether every entry of `listed`, release's snapshot of the list, is written
# exactly as producers write it, is held by `vouched` too, and is `bound`.
# release checks the whole list before it acts on any entry, so that a list
# with an entry it refuses changes nothing.
preflight() {
    local uid line kind value refused=false
    uid=$(id -u) || return 1
    for line in "${listed[@]}"; do
        kind=${line%% *}
        value=${line#* }
        if [ "${line}" = "${kind} ${value}" ] && among "${line}" "${vouched[@]}" \
            && bound "${kind}" "${value}" "${uid}"; then
            continue
        fi
        echo "ci:connector-vmm-kvm: refusing ${line@Q}: this run vouched for no such entry, or cannot show it is its own" >&2
        refused=true
    done
    [ "${refused}" = false ]
}

# Whether no launch beneath a platform or launcher directory of `listed` has
# an owner. A live launcher holds its record's lock. The state directory and
# its records must be the invoking user's and not links, the directory
# writable by no one else, and each record of no other name: anything else
# there could hide an owner, or have flock open something not a record.
launches_unowned() {
    local line value uid entries type owner mode links path expected status unowned=true
    uid=$(id -u) || return 1
    for line in "${listed[@]}"; do
        value=${line#dir }
        case "${line}" in
        "dir ${ROOT}"/platform-* | "dir ${ROOT}"/launcher-*) ;;
        *) continue ;;
        esac
        if [ ! -e "${value}" ]; then
            continue
        fi
        if ! entries=$(find "${value}" -mindepth 1 -maxdepth 2 \
            \( -path "${value}/state" -o -path "${value}/state/fv_*.owner" \) -printf '%y %U %m %n %p\n'); then
            echo "ci:connector-vmm-kvm: could not look for records beneath ${value}" >&2
            unowned=false
            continue
        fi
        while read -r type owner mode links path; do
            if [ -z "${path}" ]; then
                continue
            fi
            expected=f
            if [ "${path}" = "${value}/state" ]; then
                expected=d
            fi
            if [ "${type}" != "${expected}" ] || [ "${owner}" != "${uid}" ] \
                || { [ "${expected}" = d ] && ((8#${mode} & 8#022)); } \
                || { [ "${expected}" = f ] && [ "${links}" != 1 ]; }; then
                echo "ci:connector-vmm-kvm: refusing ${path}: a launch's state directory and records must be" \
                    "yours and not links, the directory writable by no one else, and a record of one name" >&2
                unowned=false
                continue
            fi
            if [ "${expected}" = d ]; then
                continue
            fi
            status=0
            flock -n -E 75 "${path}" true || status=$?
            case "${status}" in
            0) ;;
            75)
                echo "ci:connector-vmm-kvm: a launch still owns ${path}" >&2
                unowned=false
                ;;
            *)
                echo "ci:connector-vmm-kvm: could not try the lock of ${path}" >&2
                unowned=false
                ;;
            esac
        done <<<"${entries}"
    done
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
    # An unfiltered listing can fail while Podman inspects an unrelated
    # volume which its owner removes concurrently.
    volume) listing=$(sudo -n podman volume ls --filter "name=^${2}$" --format '{{.Name}}') ;;
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

# Removes everything the list holds, as release read it and members. Nothing
# is removed, the list is kept whole for a later run, and the release fails,
# if either can't be read, preflight refuses an entry, a stack in the list
# can't be shown stopped, a launch beneath it is still owned, or either file
# has changed since it was read: a launcher still running, idle or before it
# has made its network, could make a VMM network after the boundary had gone.
#
# Otherwise each entry, newest first, is removed or shown already gone, or is
# kept in the list for a later run and the release fails. A VMM, network,
# grant or boundary which is kept may still use what was made before it, so
# that is kept untried: state directories and the ownership records in them,
# the boundary around VMM bridges, and images, among them the one a kept
# boundary's removal or grant's revoke runs.
#
# The list is rewritten by `replace`. A release interrupted before then
# leaves the list as it was, and the next finds what this one removed gone.
# Once the list is empty, so is members.
release() {
    # One removal failing must not stop the rest.
    local - line kind value i stopped=true status standing=false
    local listed=() vouched=() kept=()
    set +e -o pipefail
    if ! snapshot resources listed || ! snapshot members vouched; then
        echo "ci:connector-vmm-kvm: kept ${ROOT}/resources as it is, and released none of it" >&2
        return 1
    fi
    if ! preflight; then
        echo "ci:connector-vmm-kvm: kept everything in ${ROOT}/resources, and released none of it." \
            "Release by hand what it refused and remove it from the list, then run this task again to release:" >&2
        printf '  %s\n' "${listed[@]}" >&2
        return 1
    fi
    for ((i = ${#listed[@]} - 1; i >= 0; i--)); do
        line=${listed[i]}
        if [ "${line%% *}" = stack ] && ! stop_stack "${line#* }"; then
            stopped=false
        fi
    done
    if [ "${stopped}" = true ] && ! launches_unowned; then
        stopped=false
    fi
    if [ "${stopped}" = false ]; then
        echo "ci:connector-vmm-kvm: kept everything in ${ROOT}/resources, the host boundary" \
            "included, since a launcher may still be running. Stop it, then run this task" \
            "again to release:" >&2
        printf '  %s\n' "${listed[@]}" >&2
        return 1
    fi
    # A launcher of the stack may have listed more as it stopped. A rewrite of
    # what was read would lose that.
    if ! unchanged resources listed || ! unchanged members vouched; then
        echo "ci:connector-vmm-kvm: ${ROOT}/resources or members changed as the stack stopped, or could not" \
            "be read again. Kept both as they are, and released none of it; run this task again to release" >&2
        return 1
    fi

    for ((i = ${#listed[@]} - 1; i >= 0; i--)); do
        line=${listed[i]}
        kind=${line%% *}
        value=${line#* }
        if [ "${standing}" = true ]; then
            case "${kind}" in
            dir | boundary | image)
                kept=("${line}" "${kept[@]}")
                continue
                ;;
            esac
        fi
        status=0
        case "${kind}" in
        stack) ;;
        dropin) rm -f "${value}" && systemctl --user daemon-reload || status=$? ;;
        # --ignore excuses only a container which is already gone.
        container) sudo -n podman rm -f --time 0 --ignore "${value}" >/dev/null || status=$? ;;
        # --no-prune: removing a derived image must not take an untagged parent,
        # such as the loaded VMM image, which a later entry still uses.
        image) sudo -n podman image rm --ignore --no-prune "${value}" >/dev/null || status=$? ;;
        builder) remove_listed builder "${value}" docker buildx rm "${value}" >/dev/null || status=$? ;;
        # Refused while a container still uses it.
        network) remove_listed network "${value}" sudo -n podman network rm "${value}" >/dev/null || status=$? ;;
        volume) remove_listed volume "${value}" sudo -n podman volume rm "${value}" >/dev/null || status=$? ;;
        netns) remove_listed netns "${value}" sudo -n ip netns del "${value}" || status=$? ;;
        # Refused while any VMM bridge remains, this run's or another owner's.
        # Tables already gone are no failure.
        boundary) boundary "${value}" remove || status=$? ;;
        ingress)
            local ingress_image ingress_owner
            read -r ingress_image ingress_owner <<<"${value}"
            sudo -n podman run --rm --pull=never --network=host --log-driver=none --read-only \
                --cap-drop=all --cap-add=CAP_NET_ADMIN "${ingress_image}" \
                boundary revoke "--owner=${ingress_owner}" || status=$?
            ;;
        link) remove_listed link "${value}" sudo -n ip link del "${value}" || status=$? ;;
        # "blackhole <prefix> metric <n>", deliberately split.
        route) remove_listed route "${value}" sudo -n ip route del ${value} || status=$? ;;
        dir) sudo -n rm -rf --one-file-system "${value}" || status=$? ;;
        esac
        if [ "${status}" -ne 0 ]; then
            echo "ci:connector-vmm-kvm: could not remove ${line}" >&2
            kept=("${line}" "${kept[@]}")
            case "${kind}" in
            container | network | volume | boundary | ingress) standing=true ;;
            esac
        fi
    done

    if [ "${#kept[@]}" -eq 0 ]; then
        replace resources </dev/null && replace members </dev/null
        return
    fi
    printf '%s\n' "${kept[@]}" | replace resources || return 1
    echo "ci:connector-vmm-kvm: kept in ${ROOT}/resources what could not be shown removed," \
        "and what it may still use. Put right what failed, then run this task again to release:" >&2
    printf '  %s\n' "${kept[@]}" >&2
    return 1
}
