#!/bin/bash
#
# This script runs the examples catalog to completion using a temp-data-plane,
# and outputs selected materializations.

# -m turns on job management, required for our use of `fg` below.
set -m

ROOTDIR="$(realpath $(git rev-parse --show-toplevel))"
FLOWCTL=${ROOTDIR}/.build/package/bin/flowctl-go

# The test to run, a folder name relative to `tests` directory
TEST=$1
# Root of the running test
TEST_ROOT="${ROOTDIR}/tests/${TEST}"

###############################################################################
# Create test directory into which we'll stage temporary data plane files.
TESTDIR="$(mktemp -d -t flow-end-to-end-XXXXXXXXXX)"

# Move sshd configs to the temp dir, which will be removed after execution.
cp -r "${ROOTDIR}/tests/sshforwarding/sshd-configs" "${TESTDIR}"
DOCKER_COMPOSE="${TESTDIR}/sshd-configs/docker-compose.yaml"

echo "Created temporary test directory: $TESTDIR"

###############################################################################
# Start local ssh server and postgres database.

# Docker compose file for starting / stopping the testing SSH server and PSQL DB.
docker compose --file ${DOCKER_COMPOSE} up --detach
# Allow postgres to start.
sleep 10

function stopTestInfra() {
  docker compose --file ${DOCKER_COMPOSE} down
}
function cleanup() {
    stopTestInfra
}

echo "Started Postgres and SSH server"

###############################################################################
# Build the catalog under test.

BUILD_ID=1122334455667788

# Build the catalog.
# `flowctl temp-data-plane` always uses ./builds/ of --tempdir as its --flow.builds-root.
# See cmd-temp-data-plane.go. Use this as --build-db to effectively build in-place.
mkdir ${TESTDIR}/builds

${FLOWCTL} api build \
    --build-id ${BUILD_ID} \
    --build-db ${TESTDIR}/builds/${BUILD_ID} \
    --log.level info \
    --network flow-test-network \
    --source ${TEST_ROOT}/flow.yaml
    1>$TESTDIR/build.out 2>&1

if [ $? -ne 0 ]; then
    echo "flowctl api build exited with an error:"
    cat $TESTDIR/build.out

    cleanup
    echo "FAIL ${TEST}"
    exit 1
fi

echo "Successfully built catalog ${BUILD_ID}"

###############################################################################
# Start a temporary data plane.

# `flowctl` commands which interact with the data plane look for *_ADDRESS
# variables, which are used by the temp-data-plane we're about to start.
export BROKER_ADDRESS=unix://localhost${TESTDIR}/gazette.sock
export CONSUMER_ADDRESS=unix://localhost${TESTDIR}/consumer.sock

# Start an empty local data plane within our TESTDIR as a background job.
# --sigterm to verify we cleanly tear down the test catalog (otherwise it hangs).
# --tempdir to use our known TESTDIR rather than creating a new temporary directory.
# --unix-sockets to create UDS socket files in TESTDIR in well-known locations.
${FLOWCTL} temp-data-plane \
    --log.level info \
    --network flow-test-network \
    --sigterm \
    --tempdir ${TESTDIR} \
    --unix-sockets \
    1>$TESTDIR/data-plane.out 2>&1 \
    &
DATA_PLANE_PID=$!

function stopDataPlane() {
  kill -s SIGTERM ${DATA_PLANE_PID}
}
function cleanup() {
    stopDataPlane
    stopTestInfra
}

echo "Started temp-data-plane"

###############################################################################
# Activate built catalog into our data plane.

${FLOWCTL} api activate \
    --all \
    --build-id ${BUILD_ID} \
    --log.level=info \
    --network flow-test-network \
    1>$TESTDIR/activate.out 2>&1

if [ $? -ne 0 ]; then
    echo "flowctl api activate exited with an error:"
    cat $TESTDIR/activate.out

    cleanup
    echo "FAIL ${TEST}"
    exit 1
fi

echo "Activated catalog into temp-data-plane"

###############################################################################
# Await test execution.

${FLOWCTL} api await \
    --build-id ${BUILD_ID} \
    --log.level info \
    1>$TESTDIR/await.out 2>&1
AWAIT_STATUS=$?

echo "Awaited catalog in temp-data-plane (code ${AWAIT_STATUS})"

if [ $AWAIT_STATUS -ne 0 ]; then
    echo "flowctl api await exited with an error (which may be expected by the test):"
    cat $TESTDIR/await.out
    echo
    echo "Sleeping for 5s to allow the materialization to finish."
    sleep 5
fi

###############################################################################
# Compare stdout / stderr to expected fixtures.

if [ -f $TEST_ROOT/data-plane.out.expect ]; then
    n_actual=$( cat $TESTDIR/data-plane.out | grep --count --file=$TEST_ROOT/data-plane.out.expect )
    n_expect=$( cat $TEST_ROOT/data-plane.out.expect | wc -l)

    if [ $n_actual -ne $n_expect ]; then
        echo "Expected data-plane output was not matched ($n_actual actual vs $n_expect expected)"
        echo "Expected to see data-plane output:"
        cat $TEST_ROOT/data-plane.out.expect
        echo
        echo "Actual data-plane output:"
        cat $TESTDIR/data-plane.out

        cleanup
        echo "FAIL ${TEST}"
        exit 1
    fi

    echo "Data-plane output matched the expectation"
fi

###############################################################################
# Compare stdout / stderr to expected fixtures.

function psql_exec() {
    docker compose --file ${DOCKER_COMPOSE} exec -T -e PGPASSWORD=flow postgres psql -w -U flow -d flow "$@"
}

# Glob patterns which match nothing should expand to nothing, rather than themselves.
shopt -s nullglob

for table_expected in ${TEST_ROOT}/*.rows; do
    table_id=$(basename $table_expected .rows)
    actual=${TESTDIR}/${table_id}.rows

    columns=$(head -n 1 $table_expected | sed 's/,/","/g')
    first_column=$(echo $columns | sed 's/",.*//')

    psql_exec -c "SELECT \"$columns\" FROM $table_id ORDER BY \"$first_column\";" --csv -P pager=off >> $actual
    diff --suppress-common-lines $actual $table_expected

    if [ $? -ne 0 ]; then
        echo "Expected and actual table rows differed."
        echo
        echo "Data-plane ouptut:"
        cat $TESTDIR/data-plane.out

        cleanup
        echo "FAIL ${TEST}"
        exit 1
    fi

    echo "Table ${table_id} matches expected rows"
done

# TODO(johnny): These are super flaky. Figure something else out.
#
# Logs from connector. In this case we don't do a full diff between all lines, we just check
# that the expected logs exist among all the logs from the connector.
#for table_expected in ${TEST_ROOT}/logs; do
#    table_id="flow_logs"
#    table_actual=${TESTDIR}/${table_id}
#    columns=$(head -n 1 $table_expected | sed 's/,/","/g')
#    psql_exec -c "SELECT \"$columns\" FROM $table_id;" --csv -P pager=off >> $table_actual
#
#    n_actual=$( cat $table_actual | grep --count --file=$table_expected )
#    n_expect=$( cat $table_expected | wc -l )
#
#    if [ $n_actual -ne $n_expect ]; then
#        echo "Expected ops logs were not matched ($n_actual actual vs $n_expect expected)"
#        echo "Expected to see log rows:"
#        cat $table_expected
#        echo
#        echo "Actually saw log rows:"
#        cat $table_actual
#        echo
#        echo "Data-plane ouptut:"
#        cat $TESTDIR/data-plane.out
#
#        cleanup
#        echo "FAIL ${TEST}"
#        exit 1
#    fi
#
#    echo "Test logs ${table_expected} match the expectation"
#done

###############################################################################
# Delete built catalog from our data plane.

${FLOWCTL} api delete \
    --all \
    --build-id ${BUILD_ID} \
    --log.level=info \
    --network flow-test-network \
    1>$TESTDIR/delete.out 2>&1

if [ $? -ne 0 ]; then
    echo "flowctl api delete exited with an error"
    cat $TESTDIR/delete.out

    cleanup
    echo "FAIL ${TEST}"
    exit 1
fi

echo "Deleted catalog from temp-data-plane"

###############################################################################
# Remove temp directory.

echo "PASS ${TEST}"

cleanup

# Need sudo and force deletion to delete some read-only temp files owned by root,
# which are generated by the test infra of openssh server during testing.
sudo rm -rf "$TESTDIR"
