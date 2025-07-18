#!/bin/bash
set -e

# Place database CA certificate in expected location
printf '%s\n' "${CONTROL_PLANE_DB_CA_CERT}" > /etc/db-ca.crt

# Start the OIDC discovery server
exec /usr/local/bin/oidc-discovery-server