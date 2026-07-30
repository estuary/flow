#!/bin/bash

set -o errexit
set -o pipefail
set -o nounset

MODE="${1:?Usage: data-plane-controller-entrypoint.sh <job|service> [args...]}"

# Place the database CA certificate, needed by both modes.
printf '%s\n' "${CONTROL_PLANE_DB_CA_CERT}" > /etc/db-ca.crt

# GCP Service Account JSON credentials path.
export GOOGLE_APPLICATION_CREDENTIALS=/etc/data_plane_controller.json
printf '%s\n' "${DPC_SERVICE_ACCOUNT}" > ${GOOGLE_APPLICATION_CREDENTIALS}

# The service mode requires infrastructure credentials for running
# Pulumi, Ansible, git operations, and cloud provider interactions.
if [[ "${MODE}" == "service" ]]; then
    # AWS profile to expect in ~/.aws/credentials
    export AWS_PROFILE=data-plane-ops

    mkdir -p /root/.aws
    printf '%s\n' "${DPC_IAM_CREDENTIALS}" > /root/.aws/credentials

    # Select git authentication for cloning our private repos. This is the
    # cutover switch from the SSH machine user to a GitHub App: it defaults to
    # `ssh` so behavior is unchanged until we flip it to `github-app`.
    if [[ "${DPC_GIT_AUTH_MODE:-ssh}" == "github-app" ]]; then
        # Rewrite the SSH-style remotes baked into the controller to HTTPS, and
        # hand git a credential helper that mints and caches short-lived App
        # installation tokens on demand. Auth stays out of the worker code,
        # just as ssh-agent did.
        git config --global url."https://github.com/".insteadOf "git@github.com:"
        git config --global credential."https://github.com".helper \
            "!/usr/local/bin/data-plane-controller git-credential"
    else
        # SSH machine-user path. Disable host-key checks when cloning our repo.
        export GIT_SSH_COMMAND="ssh -o StrictHostKeyChecking=no"
        printf '%s\n' "${DPC_GITHUB_SSH_KEY}" > /root/ssh_key
        chmod 0400 /root/ssh_key
        eval "$(ssh-agent -s)"
        ssh-add /root/ssh_key
    fi
fi

# Log out the IP from which we're running.
echo "Current egress IP:"
curl -s -S http://icanhazip.com

# Start data-plane-controller with the given arguments.
data-plane-controller "$@" &
DPC_PID=$!

# In job mode, send SIGINT after one hour to gracefully wind down.
if [[ "${MODE}" == "job" ]]; then
    (sleep 3600; kill -INT ${DPC_PID} 2>/dev/null || true) &
fi

# Wait for data-plane-controller to exit and surface its status.
set +o errexit
wait ${DPC_PID}
DPC_STATUS=${?}

echo "data-plane-controller exited with status ${DPC_STATUS}"
exit ${DPC_STATUS}
