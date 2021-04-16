#!/bin/bash
# This script is the entrypoint for the Job that runs soak tests within kubernetes.
set -ex

: ${STREAMS:=100}
: ${OPS_PER_SECOND:=1000}

cp /soak-test/* ./
go mod download
go test ./ -v \
    -timeout 0 \
    -streams 100 \
    -ops-per-second 1000 \
    -postgres-uri "postgresql://${POSTGRES_USER}:${POSTGRES_PASSWORD}@postgres.flow.svc.cluster.local:5432/postgres" \
    -ingester-address 'ws://flow-ingester.flow.svc.cluster.local:8080'

