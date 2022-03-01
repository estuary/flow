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

# Docker compose file for starting / stopping the testing SSH server and PSQL DB.
SSH_PSQL_DOCKER_COMPOSE="${ROOTDIR}/tests/sshforwarding/sshd-configs/docker-compose.yaml"
function startTestInfra() {
  docker-compose --file ${SSH_PSQL_DOCKER_COMPOSE} up --detach
  # Allow postgres to be prepared.
  sleep 2
}
function stopTestInfra() {
  docker-compose --file ${SSH_PSQL_DOCKER_COMPOSE} down
}

function cleanupDataIfPassed() {
    if [[ -z "$TESTS_PASSED" ]]; then
        echo "Tests failed, retaining data dir: $TESTDIR"
    else
        echo "Tests passed, deleting data dir: $TESTDIR"
        rm -r "$TESTDIR"
    fi
}

# Start local ssh server and postgres database.
startTestInfra

# SQLite database into which the catalog materializes.
OUTPUT_DB="${ROOTDIR}/examples/examples.db"
# Actual materialization output scraped from ${OUTPUT_DB}.
ACTUAL="${TESTDIR}/actual_test_results.txt"
# Expected materialization output.
EXPECTED="${ROOTDIR}/tests/end-to-end.expected"

# `flowctl` commands which interact with the data plane look for *_ADDRESS
# variables, which are used by the temp-data-plane we're about to start.
export BROKER_ADDRESS=unix://localhost${TESTDIR}/gazette.sock
export CONSUMER_ADDRESS=unix://localhost${TESTDIR}/consumer.sock
export FLOW_BINARY_DIR=${ROOTDIR}/.build/package/bin

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

# `flowctl temp-data-plane` always uses ./builds/ of --tempdir as its --flow.builds-root.
# See cmd-temp-data-plane.go.
export BUILDS_ROOT=${TESTDIR}/builds

# Arrange to stop the data plane on exit and remove the temporary directory.
trap "kill -s SIGTERM ${DATA_PLANE_PID} && wait ${DATA_PLANE_PID} && stopTestInfra && cleanupDataIfPassed" EXIT

BUILD_ID=run-end-to-end

# Build the catalog. Arrange for it to be removed on exit.
flowctl api build \
    --directory ${TESTDIR}/catalog-build \
    --build-id ${BUILD_ID} \
    --source ${ROOTDIR}/tests/end-to-end.flow.yaml \
    --ts-package \
    || bail "Catalog build failed."

# Move the built database to the data plane's builds root.
mv ${TESTDIR}/catalog-build/${BUILD_ID} ${BUILDS_ROOT}/
# Activate the catalog.
flowctl api activate --build-id ${BUILD_ID} --all || bail "Activate failed."
# Wait for polling pass to finish.
flowctl api await --build-id ${BUILD_ID} || bail "Await failed."
# Read out materialization results.
#
# TODO(johnny): relocation-related statistics are not stable due to
# mis-orderings of the source ride data, which cause allowable variations
# depending on how the capture is chunked up into transactions.
echo 'greetings:' >> ${ACTUAL}
sqlite3 ${OUTPUT_DB} 'SELECT count, message FROM greetings ORDER BY count;' >> ${ACTUAL}
echo 'citi_stations:' >> ${ACTUAL}
sqlite3 ${OUTPUT_DB} 'SELECT id, name, "arrival/ride", "departure/ride" FROM citi_stations;' >> ${ACTUAL}
echo 'citi_last_seen:' >> ${ACTUAL}
sqlite3 ${OUTPUT_DB} 'SELECT bike_id, "last/station/name", "last/timestamp" FROM citi_last_seen;' >> ${ACTUAL}
# Assert that each task produced at least one log message, which was able to be materialized.
echo 'flow_logs:' >> ${ACTUAL}
sqlite3 ${OUTPUT_DB} 'SELECT DISTINCT name FROM flow_logs;' | sort >> ${ACTUAL}
# We can't really make precise assertions on the stats that have been materialized because they
# vary from run to run. So this is basically asserting that we've materialized some stats on at
# least one transaction for each expected task.
echo 'flow_stats:' >> ${ACTUAL}
sqlite3 ${OUTPUT_DB}\
    'SELECT distinct kind, name FROM flow_stats where txnCount >= 1 AND openSecondsTotal > 0;'\
    | sort >> ${ACTUAL}
# We _can_ make a precise assertion on the number of documents output from the hello-world capture
# because it's configured to output a specific number of documents. So this value should match the
# `greetings` config in that capture.
echo 'flow_stats (greetings docsTotal):' >> ${ACTUAL}
sqlite3 ${OUTPUT_DB} >> ${ACTUAL} <<EOF
    select
        sum(json_extract(flow_document,
            '$.capture.examples/greetings.right.docsTotal'
        )) as right_docs_total,
        sum(json_extract(flow_document,
            '$.capture.examples/greetings.out.docsTotal'
        )) as out_docs_total
        from flow_stats
    where
        name = 'examples/source-hello-world'
EOF
echo 'greetings from psql:' >> ${ACTUAL}
docker-compose --file ${SSH_PSQL_DOCKER_COMPOSE} exec -T -e PGPASSWORD=flow postgres psql -w -U flow -d flow -c 'SELECT message, count FROM greetings ORDER BY count;' --csv -P pager=off >> ${ACTUAL}

# Clean up the activated catalog.
flowctl api delete --build-id ${BUILD_ID} --all || bail "Delete failed."

# Uncomment me to update the expectation from a current run.
# cp ${ACTUAL} ${EXPECTED}

# Verify actual vs expected results. `diff` will exit 1 if files are different
diff --suppress-common-lines ${ACTUAL} ${EXPECTED} || bail "Test Failed"

# Setting this to true will cause TESTDIR to be cleaned up on exit
TESTS_PASSED=true
