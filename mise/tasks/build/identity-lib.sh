#!/usr/bin/env bash
#MISE hide=true
#MISE description="(library) the source identity a build embeds - not runnable"
#
# Sourced by mise/tasks/local/stack-env, which makes FLOW_BUILD_IDENTITY
# ambient for every mise invocation, and by build/local-rust, which the local
# stack's systemd units run outside mise. VMM launchers and images embed it to
# pair with each other (see crates/connector's README). Release workflows and
# ci:package source it for require_flow_build.

# Print what differs in checkout $1 from its HEAD, as porcelain status lines:
# a tracked file or submodule changed, or a file git does not ignore
# untracked, such as new source. The flags override what configuration may
# hide (status.showUntrackedFiles, diff.ignoreSubmodules,
# submodule.<name>.ignore). --no-optional-locks: refreshing the index here
# must never contend with a user's own git command.
flow_build_changes() {
    git -C "$1" --no-optional-locks status --porcelain \
        --untracked-files=normal --ignore-submodules=none 2>/dev/null
}

# Print the identity of checkout $1: HEAD's commit, suffixed -dirty while
# flow_build_changes lists anything. Fails, printing nothing, outside a git
# checkout or before its first commit.
flow_build_identity() {
    local commit changes
    commit=$(git -C "$1" rev-parse --verify --quiet HEAD 2>/dev/null) || return
    changes=$(flow_build_changes "$1") || return
    printf '%s%s\n' "${commit}" "${changes:+-dirty}"
}

# Require checkout $1 to be unchanged at full commit ID $2.
require_flow_build() {
    local identity
    if [[ ! "$2" =~ ^[0-9a-f]{40}$ ]]; then
        echo "the expected commit '$2' is not a full commit ID" >&2
        return 1
    fi
    identity=$(flow_build_identity "$1") || identity=
    if [[ "${identity}" != "$2" ]]; then
        echo "checkout $1 is ${identity:-not a git commit}, not the expected commit $2" >&2
        flow_build_changes "$1" | sed 's/^/  /' >&2 || true
        return 1
    fi
}

# Export FLOW_BUILD_IDENTITY for checkout $1, or empty, which builds embed as
# no identity: a value inherited from another tree must not stand for this one.
export_flow_build_identity() {
    FLOW_BUILD_IDENTITY=$(flow_build_identity "$1") || FLOW_BUILD_IDENTITY=
    export FLOW_BUILD_IDENTITY
}
