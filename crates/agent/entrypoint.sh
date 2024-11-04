#!/bin/bash

set -o errexit
set -o pipefail
set -o nounset

# Place secrets into expected file locations.
mkdir /root/.aws
printf '%s\n' "${CONTROL_PLANE_DB_CA_CERT}" > /etc/db-ca.crt

exec agent --allow-origin=https://dashboard.estuary.dev --allow-origin=http://localhost:3000