#!/usr/bin/env bash
# Nothing here writes outside /scratch-backing and /sock, because the container
# root and /rootfs are read-only; in particular, no here-documents, which bash
# may back with /tmp.
set -euo pipefail

# flow-connector-vmm's code for a failure before the VM would have started.
readonly EXIT_FAILED=2
# Keep socat's automatic source ports separate from init's listener.
readonly PORT=19092
readonly SOCK=/sock/init.sock
readonly ROOTFS=/rootfs
readonly ROOT=/scratch-backing/root
readonly GUEST_MOUNT=/connector-mount

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

# Share argument parsing and launch prerequisites with the real VMM.
/usr/local/libexec/flow-connector-vmm check-launch "$@" || exit

# Decode validated arguments and refuse fake-specific unsupported flags.
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
    # check-launch has printed run's help.
    -h | --help)
        exit 0
        ;;
    *)
        fail "${flag} is unsupported: the fake does not implement it"
        ;;
    esac

    if [[ "$1" == *=* ]]; then
        args["${flag}"]="${1#*=}"
        shift
    else
        args["${flag}"]="$2"
        shift 2
    fi
done

readonly MOUNT="${args[--connector-mount]}"

# A bind mount needs CAP_SYS_ADMIN, so the read-only image and connector mount
# are copied into the chroot. Give the launcher ownership and write access so
# it can remove the copy unprivileged. --no-dereference keeps chown from
# following image links into this container's root.
staged="${ROOT}${GUEST_MOUNT}"
owner=$(stat -c %u:%g /scratch-backing)
if ! output=$(mkdir "${ROOT}" 2>&1 \
    && cp -a "${ROOTFS}/." "${ROOT}/" 2>&1 \
    && rm -rf "${staged}" 2>&1 \
    && mkdir "${staged}" 2>&1 \
    && cp -a "${MOUNT}/." "${staged}/" 2>&1 \
    && chown -R --no-dereference "${owner}" "${ROOT}" 2>&1 \
    && chmod -R u+w "${ROOT}" 2>&1); then
    fail "staging ${ROOTFS} and ${MOUNT} into ${ROOT}: ${output}"
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

export CONNECTOR_MOUNT="${GUEST_MOUNT}"

exec chroot "${ROOT}" "${GUEST_MOUNT}/flow-connector-init" \
    --image-inspect-json-path="${GUEST_MOUNT}/image-inspect.json" \
    --port="${PORT}"
