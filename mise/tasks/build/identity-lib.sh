#!/usr/bin/env bash
#MISE hide=true
#MISE description="(library) the source identity a build embeds - not runnable"
#
# Sourced by mise/tasks/local/stack-env, which makes FLOW_BUILD_IDENTITY
# ambient for every mise invocation, and by build/local-rust, which the local
# stack's systemd units run outside mise. VMM launchers and images embed it to
# pair with each other (see crates/connector's README).

# Print the identity of checkout $1: HEAD's commit, suffixed -dirty while a
# tracked file or submodule differs from it, or a file git does not ignore is
# untracked, such as new source. Fails, printing nothing, outside a git
# checkout or before its first commit. The status flags override what
# configuration may hide (status.showUntrackedFiles, diff.ignoreSubmodules,
# submodule.<name>.ignore). --no-optional-locks: refreshing the index here
# must never contend with a user's own git command.
flow_build_identity() {
    local commit changes
    commit=$(git -C "$1" rev-parse --verify --quiet HEAD 2>/dev/null) || return
    changes=$(git -C "$1" --no-optional-locks status --porcelain \
        --untracked-files=normal --ignore-submodules=none 2>/dev/null) || return
    printf '%s%s\n' "${commit}" "${changes:+-dirty}"
}

# Export FLOW_BUILD_IDENTITY for checkout $1, or empty, which builds embed as
# no identity: a value inherited from another tree must not stand for this one.
export_flow_build_identity() {
    FLOW_BUILD_IDENTITY=$(flow_build_identity "$1") || FLOW_BUILD_IDENTITY=
    export FLOW_BUILD_IDENTITY
}
