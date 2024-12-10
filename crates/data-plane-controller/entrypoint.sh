#!/bin/bash

set -o errexit
set -o pipefail
set -o nounset

# Place secrets into expected file locations.
# The SSH key in particular requires a trailing newline.
mkdir /root/.aws
printf '%s\n' "${CONTROL_PLANE_DB_CA_CERT}" > /etc/db-ca.crt
printf '%s\n' "${DPC_GITHUB_SSH_KEY}" > /root/ssh_key
printf '%s\n' "${DPC_IAM_CREDENTIALS}" > /root/.aws/credentials
printf '%s\n' "${DPC_SERVICE_ACCOUNT}" > ${GOOGLE_APPLICATION_CREDENTIALS}

# Start background ssh-agent, evaluate output to set variables, and add SSH key.
chmod 0400 /root/ssh_key
eval "$(ssh-agent -s)"
ssh-add /root/ssh_key

# Log out the IP from which we're running.
echo "Current egress IP:"
curl -s -S http://icanhazip.com

# Start data-plane-controller in the background
data-plane-controller &
DPC_PID=$!

# Start a background timer to send SIGINT after one hour.
(
  sleep 3600
  kill -INT ${DPC_PID} 2>/dev/null || true
) &

# Wait for data-plane-controller to exit and surface it's status.
set +o errexit
wait ${DPC_PID}
DPC_STATUS=${?}

echo "data-plane-controller exited with status ${DPC_STATUS}"
exit ${DPC_STATUS}