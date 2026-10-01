#!/usr/bin/env bash
# Nothing here writes outside /rootfs and /sock, because the container root is
# read-only; in particular, no here-documents, which bash may back with /tmp.
set -euo pipefail

# flow-connector-vmm's code for a failure before the VM would have started.
readonly EXIT_FAILED=2
readonly PORT=49092
readonly SOCK=/sock/init.sock
readonly ROOTFS=/rootfs

# Every line is prefixed: the launcher reads a leading space on stderr as
# connector-init's readiness byte.
log() {
    local message="$*"
    printf 'connector-vmm-fake: %s\n' "${message//$'\n'/$'\n'connector-vmm-fake: }" >&2
}

fail() {
    log "$@"
    exit "${EXIT_FAILED}"
}

# The host boundary is the real binary's, so that a launcher verifying with
# the fake verifies exactly as it would with the real image.
if [[ "${1:-}" == boundary ]]; then
    exec /usr/local/libexec/flow-connector-vmm "$@"
fi
[[ "${1:-}" == run ]] || fail "expected the run or boundary subcommand; the fake implements nothing else"
shift

declare -A args=()
while (($# > 0)); do
    flag="${1%%=*}"
    case "${flag}" in
    --policy | --connector-mount | --memory-mib | --vcpus | --disk-mib) ;;
    --persistent-disk)
        fail "--persistent-disk is unsupported: the fake cannot mount a share inside ${ROOTFS}"
        ;;
    --run-as-root | --as-root-exec | --debug | --resolver-upstream | --exec)
        fail "${flag} is test-only and needs the real VMM"
        ;;
    *)
        fail "unexpected argument: $1"
        ;;
    esac

    if [[ -v "args[${flag}]" ]]; then
        fail "${flag} given more than once"
    fi
    if [[ "$1" == *=* ]]; then
        args["${flag}"]="${1#*=}"
        shift
    elif (($# >= 2)); then
        args["${flag}"]="$2"
        shift 2
    else
        fail "${flag} requires a value"
    fi
done

for flag in --policy --connector-mount --memory-mib --vcpus --disk-mib; do
    [[ -v "args[${flag}]" ]] || fail "${flag} is required"
done
for flag in --memory-mib --vcpus --disk-mib; do
    [[ "${args[${flag}]}" =~ ^[0-9]+$ ]] || fail "${flag} expects a number, not ${args[${flag}]}"
done

readonly MOUNT="${args[--connector-mount]}"
[[ "${MOUNT}" == /* ]] || fail "--connector-mount expects an absolute path"
[[ ! "${MOUNT}" =~ ^/+$ ]] || fail "--connector-mount: the guest root is not a mount point for this share"
[[ -d "${MOUNT}" ]] || fail "${MOUNT} is not a directory"

# `-w` is false on a read-only mount even for root.
if [[ -w / ]]; then
    fail "/ is writable; the VMM container must be launched with \`--read-only\`"
fi
if [[ -w "${MOUNT}" ]]; then
    fail "${MOUNT} is writable; the VMM container must be launched with \`--mount type=bind,source=M,target=M,readonly\`"
fi
[[ -r "${args[--policy]}" ]] || fail "${args[--policy]} is not readable"

ipv6=/proc/sys/net/ipv6/conf/default/disable_ipv6
if [[ -e "${ipv6}" && "$(<"${ipv6}")" != 1 ]]; then
    fail "IPv6 is enabled; the VMM container must be launched with --sysctl net.ipv6.conf.default.disable_ipv6=1"
fi
[[ -r "${MOUNT}/image-inspect.json" ]] || fail "${MOUNT}/image-inspect.json is not readable"

# The connector sees the mount at its own path inside the chroot, which only a
# copy can give it: a bind mount needs CAP_SYS_ADMIN, and a symlink resolves
# inside the chroot. Symlinks already in the image resolve against this
# container's root rather than the chroot's, so one on the staging path would
# put the copy outside /rootfs, as would a dot component.
IFS=/ read -r -a components < <(printf '%s\n' "${MOUNT#/}")
staged="${ROOTFS}"
for component in "${components[@]}"; do
    [[ -n "${component}" ]] || continue
    if [[ "${component}" == . || "${component}" == .. ]]; then
        fail "--connector-mount must not contain . or .. components"
    fi
    staged="${staged}/${component}"
    if [[ -L "${staged}" ]]; then
        fail "${staged#"${ROOTFS}"} is a symlink in the connector image; the fake cannot stage ${MOUNT} beneath it"
    fi
done
if ! output=$(mkdir -p "${staged}" 2>&1 && cp -a "${MOUNT}/." "${staged}/" 2>&1); then
    fail "staging ${MOUNT} into ${ROOTFS}: ${output}"
fi

# The runtime re-mints task-update.json by renaming a new file over it, and a
# connector must re-read it at each use, so follow it into the copy. A first
# pass that recopies it closes the gap after the copy above.
follow_task_update() {
    local source="${MOUNT}/task-update.json"
    local target="${staged}/task-update.json"
    local next="${staged}/.task-update.json.follow"
    local seen="" current

    while sleep 1; do
        current=$(stat -c '%i %.9Y %s' "${source}" 2>/dev/null) || continue
        [[ "${current}" != "${seen}" ]] || continue

        if output=$(cp -p "${source}" "${next}" 2>&1 && mv -f "${next}" "${target}" 2>&1); then
            seen="${current}"
        else
            log "following ${source}: ${output}"
        fi
    done
}
follow_task_update &

# libkrun binds this socket in the real VMM, under umask 0 so an unprivileged
# launcher can dial it; the launcher's mode on /sock is the access control.
# It refuses one left by an earlier run, and so does this: the wait below
# would otherwise mistake that leftover for socat's listener.
if [[ -e "${SOCK}" || -L "${SOCK}" ]]; then
    fail "binding ${SOCK}: File exists"
fi
(
    umask 0
    exec socat "UNIX-LISTEN:${SOCK},fork" "TCP:127.0.0.1:${PORT}"
) 2> >(sed -u 's/^/connector-vmm-fake: socat: /' >&2) &

for _ in $(seq 50); do
    [[ -S "${SOCK}" ]] && break
    sleep 0.1
done
[[ -S "${SOCK}" ]] || fail "socat did not create ${SOCK}"

export CONNECTOR_MOUNT="${MOUNT}"

exec chroot "${ROOTFS}" "${MOUNT}/flow-connector-init" \
    --image-inspect-json-path="${MOUNT}/image-inspect.json" \
    --port="${PORT}"
