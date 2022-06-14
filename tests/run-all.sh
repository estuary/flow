#!/bin/sh

# fail fast
set -e

echo "running source-test-no-state"
./run-end-to-end.sh source-test-no-state
echo
echo "running citi-bike-success"
./run-end-to-end.sh citi-bike-success
echo
echo "running source-test-exit-status"
./run-end-to-end.sh source-test-exit-status
echo
echo "running push-capture"
./run-end-to-end.sh push-capture

