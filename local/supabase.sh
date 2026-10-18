#!/usr/bin/env bash
set -euo pipefail

cd "${FLOW_ROOT}"
registry_file="${FLOW_STACK_DIR}/.supabase-registry"

if [ "${1:-}" = "start" ]; then
    # The pinned CLI retries one registry but cannot fail over between mirrors.
    for registry in ghcr.io public.ecr.aws; do
        echo "Starting Supabase with image registry ${registry}" >&2
        if SUPABASE_INTERNAL_IMAGE_REGISTRY="${registry}" supabase "$@"; then
            mkdir -p "${FLOW_STACK_DIR}"
            printf '%s\n' "${registry}" > "${registry_file}"
            exit 0
        else
            status=$?
        fi
        echo "Supabase startup with ${registry} failed (exit ${status})." >&2
    done
    exit "${status}"
fi

# Reset and edge functions must use the registry that successfully started the stack.
export SUPABASE_INTERNAL_IMAGE_REGISTRY
SUPABASE_INTERNAL_IMAGE_REGISTRY=$(cat "${registry_file}")
exec supabase "$@"
