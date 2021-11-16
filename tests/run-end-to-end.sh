#!/bin/bash
#
# This script runs the examples catalog to completion using a temp-data-plane in --poll mode,
# and outputs selected materializations.

# -e causes the script to exit on encountering an error
# -m turns on job management, required for our use of `fg` below.
set -em

ROOTDIR="$(realpath $(git rev-parse --show-toplevel))"
cd "${ROOTDIR}"

function bail() {
    echo "$@" 1>&2
    exit 1
}

# Temporary test directory into which we'll build our test database,
# and stage temporary data plane files.
TESTDIR="$(mktemp -d -t flow-end-to-end-XXXXXXXXXX)"

# SQLite database into which the catalog materializes.
OUTPUT_DB="${ROOTDIR}/examples/examples.db"
# Actual materialization output scraped from ${OUTPUT_DB}.
ACTUAL="${TESTDIR}/actual_test_results.txt"
# Expected materialization output.
EXPECTED="${ROOTDIR}/tests/end-to-end.expected"

# `flowctl` commands look for a BUILDS_ROOT environment variable which roots
# build databases known to the data plane.
export BUILDS_ROOT="file://${TESTDIR}/build/"
# `flowctl` commands which interact with the data plane look for *_ADDRESS
# variables, which are used by the temp-data-plane we're about to start.
export BROKER_ADDRESS=unix://localhost${TESTDIR}/gazette.sock
export CONSUMER_ADDRESS=unix://localhost${TESTDIR}/consumer.sock

# Start an empty local data plane within our TESTDIR as a background job.
# --poll so that connectors are polled rather than continuously tailed.
# --sigterm to verify we cleanly tear down the test catalog (otherwise it hangs).
# --tempdir to use our known TESTDIR rather than creating a new temporary directory.
# --unix-sockets to create UDS socket files in TESTDIR in well-known locations.
flowctl temp-data-plane \
    --poll \
    --sigterm \
    --tempdir ${TESTDIR} \
    --unix-sockets \
    &
DATA_PLANE_PID=$!
# Arrange to stop the data plane on exit and remove the temporary directory.
trap "kill -s SIGTERM ${DATA_PLANE_PID} && wait ${DATA_PLANE_PID} && rm -r ${TESTDIR}" EXIT

BUILD_ID=run-end-to-end

# Build the catalog. Arrange for it to be removed on exit.
flowctl api build --directory ${TESTDIR}/build --build-id ${BUILD_ID} --source ${ROOTDIR}/tests/end-to-end.flow.yaml --ts-package || bail "Build failed."
# Activate the catalog.
flowctl api activate --build-id ${BUILD_ID} --all || bail "Activate failed."
# Wait for polling pass to finish.
flowctl api await --build-id ${BUILD_ID} || bail "Await failed."

# Read out materialization results.
#
# TODO(johnny): relocation-related statistics are not stable due to
# mis-orderings of the source ride data, which cause allowable variations
# depending on how the capture is chunked up into transactions.
sqlite3 ${OUTPUT_DB} 'SELECT count, message FROM greetings ORDER BY count;' >> ${ACTUAL}
sqlite3 ${OUTPUT_DB} 'SELECT id, name, "arrival/ride", "departure/ride" FROM citi_stations;' >> ${ACTUAL}
sqlite3 ${OUTPUT_DB} 'SELECT bike_id, "last/station/name", "last/timestamp" FROM citi_last_seen;' >> ${ACTUAL}

# Clean up the activated catalog.
flowctl api delete --build-id ${BUILD_ID} --all || bail "Delete failed."

# Uncomment me to update the expectation from a current run.
# cp ${ACTUAL} ${EXPECTED}

# Verify actual vs expected results. `diff` will exit 1 if files are different
diff --suppress-common-lines ${ACTUAL} ${EXPECTED} || bail "Test Failed"

echo "Test Passed"