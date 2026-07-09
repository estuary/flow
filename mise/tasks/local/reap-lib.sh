#!/usr/bin/env bash
#MISE hide=true
#MISE description="(library) shared local-stack reaper — not runnable"
#
# The garbage collector for per-checkout stacks. Sourced by both
# mise/tasks/local/stack-env (which reaps while allocating a slot) and
# mise/tasks/local/stack-prune (the idempotent task run at lifecycle joints).
#
# ONE signal, and only one, releases a stack's slot and artifacts: its
# registered checkout directory no longer exists. Being *stopped* releases
# nothing — a stopped-but-present checkout keeps its index AND its (expensive,
# tens-of-GB) build artifacts, so restarting it is instant. You release a stack
# by deleting its worktree; the reaper then reclaims it in the background.
#
# These functions assume ${HOME}/flow-local is the registry home (identical to
# stack-env's `_se_flow_local`). `reap_gone_entries` does NOT lock — its caller
# owns the flock (stack-env already holds it while allocating). `reap_gone_locked`
# takes the lock itself, for callers that are not already inside it.

# Fully tear down a stack by NAME. Its checkout is gone, so nothing about it is
# ambient; every path is derived from the name, exactly as local:stop scopes a
# stack: control-plane units are instanced on the raw name, data-plane units on
# the `local-<name>-cluster[-N]` family. Idempotent — globs that match nothing
# and rm of absent paths are no-ops. Guarded so an empty or short name can never
# escalate into an rm of $HOME or a shared directory.
reap_teardown_stack() {
    local name="$1"
    [ -n "${name}" ] || return 0
    local cluster="local-${name}-cluster"
    local unit_dir="${HOME}/.config/systemd/user"
    local env_dir="${HOME}/flow-local/env"

    # Stop the now-orphaned units FIRST: deleting a worktree does not stop its
    # `systemctl --user` units, and freeing the index while they still hold their
    # ports would collide when a new checkout reuses that index.
    systemctl --user stop \
        "flow-*@${name}.service" "flow-*@${name}.target" \
        "flow-*@${cluster}.service" "flow-*@${cluster}.target" \
        "flow-*@${cluster}-*.service" "flow-*@${cluster}-*.target" 2>/dev/null || true

    rm -rf "${unit_dir}/flow-"*"@${name}.service.d" \
           "${unit_dir}/flow-"*"@${name}.target.d" \
           "${unit_dir}/flow-"*"@${cluster}.service.d" \
           "${unit_dir}/flow-"*"@${cluster}.target.d" \
           "${unit_dir}/flow-"*"@${cluster}-"*.service.d \
           "${unit_dir}/flow-"*"@${cluster}-"*.target.d 2>/dev/null || true
    rm -f "${env_dir}"/*"-${name}.env" \
          "${env_dir}"/*"-${cluster}.env" \
          "${env_dir}"/*"-${cluster}-"*.env 2>/dev/null || true

    systemctl --user daemon-reload 2>/dev/null || true
    systemctl --user reset-failed \
        "flow-*@${name}.service" "flow-*@${name}.target" \
        "flow-*@${cluster}.service" "flow-*@${cluster}.target" \
        "flow-*@${cluster}-*.service" "flow-*@${cluster}-*.target" 2>/dev/null || true

    # Reclaim disk: build artifacts (the big item, regenerated on the next
    # build), the private state dir, and Supabase volumes/network.
    local target="${HOME}/cargo-target/${name}"
    if [ "${target}" != "${HOME}/cargo-target" ] && [ -d "${target}" ]; then
        rm -rf "${target}"
    fi
    local statedir="${HOME}/flow-local/${name}"
    if [ "${statedir}" != "${HOME}/flow-local" ] && [ -d "${statedir}" ]; then
        rm -rf "${statedir}"
    fi

    # Volumes are `supabase_<component>_<id>` with id == the stack name; names
    # never contain '_' (stack-env sanitizes to [a-z0-9-]), so the segment after
    # the final '_' is exactly the id — match it precisely so 'flow' can't reap
    # 'flow-2'.
    if command -v docker >/dev/null 2>&1; then
        local vol
        while IFS= read -r vol; do
            [ "${vol##*_}" = "${name}" ] || continue
            docker volume rm "${vol}" >/dev/null 2>&1 || true
        done < <(docker volume ls --format '{{.Name}}' 2>/dev/null | grep '^supabase_' || true)
        docker network rm "supabase_network_${name}" >/dev/null 2>&1 || true
    fi
}

# Reap every registry entry whose checkout directory is gone, rewriting the
# registry to keep only entries whose root still exists. Prints one line per
# reaped stack on stderr. The caller MUST already hold the stacks.lock flock.
reap_gone_entries() {
    local registry="$1"
    [ -f "${registry}" ] || return 0

    local -a survivors=()
    local reaped=0 idx name root
    while IFS=$'\t' read -r idx name root; do
        [ -n "${root}" ] || continue
        if [ -d "${root}" ]; then
            survivors+=("${idx}"$'\t'"${name}"$'\t'"${root}")
            continue
        fi
        reap_teardown_stack "${name}"
        echo "reap: reclaimed '${name}' (index ${idx}); checkout ${root} is gone." >&2
        reaped=$((reaped + 1))
    done < "${registry}"

    [ "${reaped}" -gt 0 ] || return 0
    local tmp
    tmp="$(mktemp "${registry}.XXXXXX")"
    if [ "${#survivors[@]}" -gt 0 ]; then
        printf '%s\n' "${survivors[@]}" > "${tmp}"
    else
        : > "${tmp}"
    fi
    mv "${tmp}" "${registry}"
}

# reap_gone_entries wrapped in the registry lock, for callers not already inside
# it (the stack-prune task and lifecycle-joint hooks). NEVER call this from
# stack-env's allocation, which already holds fd 9 on the same lock — re-locking
# the same file in the same process would deadlock.
reap_gone_locked() {
    local registry="$1" lock="$2"
    [ -f "${registry}" ] || return 0
    (
        flock 9 || exit 0
        reap_gone_entries "${registry}"
    ) 9>"${lock}"
}
