#!/bin/bash

set -o errexit
set -o pipefail
set -o nounset

MODE="${1:?Usage: data-plane-controller-entrypoint.sh <job|service> [args...]}"

# Place the database CA certificate, needed by both modes.
printf '%s\n' "${CONTROL_PLANE_DB_CA_CERT}" > /etc/db-ca.crt

# The service mode requires infrastructure credentials for running
# Pulumi, Ansible, git operations, and cloud provider interactions.
if [[ "${MODE}" == "service" ]]; then
    mkdir -p /root/.aws
    printf '%s\n' "${DPC_GITHUB_SSH_KEY}" > /root/ssh_key
    printf '%s\n' "${DPC_IAM_CREDENTIALS}" > /root/.aws/credentials
    printf '%s\n' "${DPC_SERVICE_ACCOUNT}" > ${GOOGLE_APPLICATION_CREDENTIALS}

    chmod 0400 /root/ssh_key
    eval "$(ssh-agent -s)"
    ssh-add /root/ssh_key
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