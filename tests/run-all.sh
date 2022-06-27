#!/bin/sh

# fail fast
set -e

ROOTDIR="$(realpath $(git rev-parse --show-toplevel))"

echo "running source-test-no-state"
${ROOTDIR}/tests/run-end-to-end.sh source-test-no-state
echo
echo "running citi-bike-success"
${ROOTDIR}/tests/run-end-to-end.sh citi-bike-success
echo
echo "running source-test-exit-status"
${ROOTDIR}/tests/run-end-to-end.sh source-test-exit-status
echo
echo "running push-capture"
${ROOTDIR}/tests/run-end-to-end.sh push-capture

