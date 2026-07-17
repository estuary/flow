#!/usr/bin/env bash
set -euo pipefail

# Keep credentials on the host: callers provide an SSH session with agent
# forwarding, and this command uses it only while cloning the repository.
curl https://mise.run | sh
export PATH="${HOME}/.local/bin:${PATH}"
mkdir -p "${HOME}/estuary"
cd "${HOME}/estuary"
GIT_SSH_COMMAND="ssh -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null" \
  git clone git@github.com:estuary/flow.git
cd flow

mise trust
mise run vm:create-post
