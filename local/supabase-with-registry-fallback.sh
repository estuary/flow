#!/usr/bin/env bash
set -euo pipefail

# Keep registry failures independent of the CLI's JWT signing defaults. An
# explicit override is authoritative (for example, a private registry mirror).
if [[ -n "${SUPABASE_INTERNAL_IMAGE_REGISTRY:-}" ]]; then
    exec supabase "$@"
fi

log_dir=$(mktemp -d)
trap 'rm -rf "$log_dir"' EXIT
log_file="$log_dir/stderr"
mkfifo "$log_dir/stream"

for registry in public.ecr.aws ghcr.io docker.io; do
    # Preserve stdout and stream stderr, but retain only its tail: functions
    # serve runs indefinitely and must not accumulate an unbounded log file.
    tail -n 100 < "$log_dir/stream" > "$log_file" &
    collector=$!
    if SUPABASE_INTERNAL_IMAGE_REGISTRY="$registry" supabase "$@" \
        2>&1 1>&3 | tee "$log_dir/stream" >&2; then
        wait "$collector"
        exit 0
    else
        status=${PIPESTATUS[0]}
    fi
    wait "$collector"

    # Match the pinned CLI's DockerImagePull errors, including errors returned
    # inside Docker's streamed pull response. Never retry migrations, unhealthy
    # containers, or configuration failures just because another registry exists.
    if [[ "$status" -eq 0 ]]; then
        exit 1 # A logging stage failed, rather than Supabase itself.
    fi
    # Earlier pull attempts may have recovered before a different failure.
    # The CLI prints its final error followed by an optional debug suggestion.
    final_error=$(awk '
        { gsub(/\033\[[0-9;]*m/, "") }
        NF && $0 !~ /^Try rerunning the command with --debug/ { last = $0 }
        END { print last }
    ' "$log_file")
    if [[ "$status" -ge 128 ]] ||
        [[ "$final_error" != *"failed to pull docker image:"* &&
           "$final_error" != *"failed to display json stream:"* ]]; then
        exit "$status"
    fi
    if [[ "$registry" != docker.io ]]; then
        echo "Supabase image pull from $registry failed; retrying with the next registry." >&2
    fi
done 3>&1

exit "$status"
