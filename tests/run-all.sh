#!/bin/sh

# fail fast
set -e

ROOTDIR="$(realpath $(git rev-parse --show-toplevel))"

echo "running source-test"
${ROOTDIR}/tests/run-end-to-end.sh source-test
echo
echo "running source-test-fail"
${ROOTDIR}/tests/run-end-to-end.sh source-test-fail
echo
echo "running citi-bike-success"
${ROOTDIR}/tests/run-end-to-end.sh citi-bike-success
