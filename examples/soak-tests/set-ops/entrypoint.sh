#!/bin/bash
# This script is the entrypoint for the Job that runs soak tests within kubernetes.
set -ex

# We need to copy the files out of the directory where the configmap is mounted because that
# directory is readonly.
cp /soak-test/* ./
go mod download
go run ./ test \
    --postgres-uri "postgresql://${POSTGRES_USER}:${POSTGRES_PASSWORD}@postgres.flow.svc.cluster.local:5432/postgres" \
    --ingester-address 'ws://flow-ingester.flow.svc.cluster.local:8080'

