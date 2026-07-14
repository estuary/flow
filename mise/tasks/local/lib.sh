#!/usr/bin/env bash
#MISE hide=true
#MISE description="(library) shared helpers for local:* tasks — not runnable"
#
# Sourceable helpers shared by mise/tasks/local/* task scripts. This file is not
# a task; source it near the top of a task with:
#
#     source "$(dirname "$0")/lib.sh"
#     ensure_stack_env
#
# It REQUIRES the per-stack ambient environment provided by stack-env (via
# mise.toml `[env] _.source`), which is present for anything run through mise.
# `ensure_stack_env` asserts that — direct execution outside a mise context is
# unsupported (mise is mandatory; see local/README.md).

# Absolute checkout root, resilient to being called from a subdirectory.
flow_repo_root() {
    git rev-parse --show-toplevel 2>/dev/null || pwd
}

# Assert the per-stack ambient env (from stack-env via mise) is present. It is
# NOT re-derived here: mise is the single entry point, so a missing
# FLOW_STACK_NAME means the task was run outside `mise run`, which is
# unsupported. Fail with a re-education message rather than guessing an env.
ensure_stack_env() {
    if [ -z "${FLOW_STACK_NAME:-}" ]; then
        echo "error: FLOW_STACK_NAME is unset — run this via 'mise run <task>';" >&2
        echo "       direct execution is unsupported; see local/README.md" >&2
        exit 1
    fi
    # Common derived paths used across tasks.
    FLOW_LOCAL="${HOME}/flow-local"
    FLOW_ENV_DIR="${FLOW_LOCAL}/env"
    export FLOW_LOCAL FLOW_ENV_DIR
}

# Ensure the shared/flat directories every task relies on exist: the flat env
# dir (env files for all stacks), the systemd user unit dir, and this stack's
# private state dir.
prepare_dirs() {
    mkdir -p "${HOME}/flow-local/env"
    mkdir -p "${HOME}/.config/systemd/user"
    mkdir -p "${FLOW_STACK_DIR}"
}

# Force-(re)link a systemd unit template from the *invoking* checkout into the
# user unit dir. Unit templates are branch-stable but last-writer-wins across
# branches (systemd's unit namespace is machine-global); always relinking at
# stack start ensures a freshly started stack pins its own checkout's templates.
# Callers should `systemctl --user daemon-reload` once after linking a batch.
link_unit() {
    local name="$1"
    local root
    root="$(flow_repo_root)"
    ln -sf "${root}/local/systemd/${name}" "${HOME}/.config/systemd/user/${name}"
}

# Emit the common environment block (which the old scheme kept in a shared
# ~/flow-local/env/common.env) to stdout, for inlining into a per-instance
# systemd EnvironmentFile. Duplicating these across the generated per-instance
# files is intentional and free — every file is regenerated on stack start, and
# a single env file per unit removes the brittle two-file load order.
emit_common_vars() {
    local _root
    _root="$(flow_repo_root)"
    cat <<EOF
# Common build/runtime environment (generated; inlined per instance).
CARGO_TARGET_DIR="${CARGO_TARGET_DIR}"
CGO_CFLAGS="${CGO_CFLAGS}"
CGO_CPPFLAGS="${CGO_CPPFLAGS}"
CGO_LDFLAGS="${CGO_LDFLAGS}"
FLOW_ROOT="${_root}"
GOBIN="${GOBIN}"
JEMALLOC_OVERRIDE="${JEMALLOC_OVERRIDE}"
PATH="${PATH}:${CARGO_TARGET_DIR}/$(uname -m)-unknown-linux-musl/debug/:${CARGO_TARGET_DIR}/debug/:${GOBIN}"
ROCKSDB_INCLUDE_DIR="${ROCKSDB_INCLUDE_DIR}"
ROCKSDB_LIB_DIR="${ROCKSDB_LIB_DIR}"
ROCKSDB_VERSION="${ROCKSDB_VERSION}"
RUSTFLAGS="${RUSTFLAGS}"
SNAPPY_LIB_DIR="${SNAPPY_LIB_DIR}"
SNAPPY_STATIC="${SNAPPY_STATIC}"
EOF
}

# Print the compact stack "card": the one-screen summary of this stack's
# identity and go-to commands. Shared by local:stack (printed on completion) and
# local:stack-info (which prints it as the short form beneath its full detail).
# Deliberately makes NO claim about run state — stack-info shows the card even
# when nothing is running; callers announce state themselves.
print_stack_card() {
    echo "Stack '${FLOW_STACK_NAME}' — index ${FLOW_STACK_INDEX}, ports ${FLOW_STACK_BASE}-$((FLOW_STACK_BASE + 999))."
    echo "  flowctl catalog list          # FLOWCTL_PROFILE=${FLOW_STACK_NAME} is ambient in this checkout"
    echo "  psql \"\$FLOW_PG_URL\"           # Supabase Postgres  localhost:${FLOW_PORT_SUPABASE_DB}"
    echo "  mise run local:stack-info     # full port map, units, and commands"
    echo "  mise run local:test-tenant    # provision tenant credentials"
}
