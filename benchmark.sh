#!/bin/bash
#
# This script runs a throughput benchmark of the data plane.
# It runs a high-volume dummy capture (source-gigahello)
# for a constant length of time, then uses the resulting
# output data to establish the average throughput.

# -m turns on job management, required for our use of `fg` below.
set -m
set -o errexit
set -o pipefail
set -o nounset

BENCHMARK_MESSAGE="Hello ---------------------------------------------------------------------------------------------------- world!"

# Use binaries from the local build
PATH="$(dirname $(realpath $0))/.build/package/bin:$PATH"

command -v flowctl-go >/dev/null 2>&1 || {
  echo >&2 "flowctl-go must be available via PATH, aborting."
  exit 1
}

function bail() {
  echo "$@" 1>&2
  exit 1
}

# Connector image to use. Export to make it available to `envsubst`
export CONNECTOR_IMAGE="ghcr.io/estuary/source-gigahello:b7deea5"

# Directory under which the test runs.
export TMPDIR="/tmp/flow_benchmark"
if [[ -d "${TMPDIR}" ]]; then
  rm -r ${TMPDIR}
fi
mkdir -p "${TMPDIR}"

# `flowctl-go` commands which interact with the data plane look for *_ADDRESS
# variables, which are created by the temp-data-plane we're about to start.
echo "export BROKER_ADDRESS=unix://localhost${TMPDIR}/gazette.sock"
echo "export CONSUMER_ADDRESS=unix://localhost${TMPDIR}/consumer.sock"
export BROKER_ADDRESS=unix://localhost${TMPDIR}/gazette.sock
export CONSUMER_ADDRESS=unix://localhost${TMPDIR}/consumer.sock

# Start an empty local data plane within our TMPDIR as a background job.
# --sigterm to verify we cleanly tear down the test catalog (otherwise it hangs).
# --tempdir to use our known TMPDIR rather than creating a new temporary directory.
# --unix-sockets to create UDS socket files in TMPDIR in well-known locations.
flowctl-go temp-data-plane \
  --log.level info \
  --sigterm \
  --network "flow-test" \
  --tempdir ${TMPDIR} \
  --unix-sockets \
  &
DATA_PLANE_PID=$!

# Arrange to stop the data plane on exit.
trap "kill -s SIGTERM ${DATA_PLANE_PID} && wait ${DATA_PLANE_PID}" EXIT

# Write out the task definition
CATALOG_SOURCE="${TMPDIR}/benchmark.flow.yaml"
cat >"$CATALOG_SOURCE" <<EOF
collections:
  tests/benchmark/greetings:
    schema:
      properties:
        seq: {type: integer}
        message: {type: string}
      required: [seq, message]
      type: object
    key: [/seq]
captures:
  tests/benchmark/source-gigahello:
    endpoint:
      connector:
        image: "${CONNECTOR_IMAGE}"
        config:
          rate: -1
    bindings:
      - resource:
          name: greetings
          message: "${BENCHMARK_MESSAGE}"
        target: tests/benchmark/greetings
EOF

# Build and activate the catalog.
function publish {
    CATALOG_SOURCE=$1
    BUILD_ID=$2
    flowctl-go api build \
      --build-id ${BUILD_ID} \
      --build-db ${TMPDIR}/builds/${BUILD_ID} \
      --source ${CATALOG_SOURCE} \
      --network "flow-test" ||
      bail "Build failed."
    flowctl-go api activate --build-id ${BUILD_ID} --all --network "flow-test" --log.level info || bail "Activate failed."
}
BUILD_ID=1111111111111111
publish ${CATALOG_SOURCE} ${BUILD_ID}

# Give it 60 seconds to run
echo "Running for 60s ..." && sleep 10
echo "Running for 50s ..." && sleep 10
echo "Running for 40s ..." && sleep 10
echo "Running for 30s ..." && sleep 10
echo "Running for 20s ..." && sleep 10
echo "Running for 10s ..." && sleep 10
echo "...Complete!"

# Disable the task
echo "    shards:" >> ${CATALOG_SOURCE}
echo "      disable: true" >> ${CATALOG_SOURCE}
BUILD_ID=2222222222222222
publish ${CATALOG_SOURCE} ${BUILD_ID}
sleep 5

# Read all the messages out of the data plane into a file
MESSAGES_FILE="$TMPDIR/messages.jsonl"
flowctl-go journals read -l name=tests/benchmark/greetings/ffffffffffffffff/pivot=00 > ${MESSAGES_FILE}

# Clean up the activated catalog
flowctl-go api delete --build-id ${BUILD_ID} --all --log.level info || bail "Delete failed."

# Shut down the data plane
kill -s SIGTERM ${DATA_PLANE_PID} && wait ${DATA_PLANE_PID}
trap - EXIT

# Analyze the results
FIRST_TS="$(head -n1 $MESSAGES_FILE  | jq -r '._meta.uuid' | uuidparse -J | jq -r '.uuids[0].time')"
FINAL_TS="$(tail -n1 $MESSAGES_FILE  | jq -r '._meta.uuid' | uuidparse -J | jq -r '.uuids[0].time')"
FIRST_NS="$(date -d "$FIRST_TS" +%s%N)"
FINAL_NS="$(date -d "$FINAL_TS" +%s%N)"
DIFF_SECS=$(echo "scale=9; ($FINAL_NS - $FIRST_NS) / 1000000000" | bc)
FILE_DOCS="$(wc -l $MESSAGES_FILE | awk '{print $1}')"
FILE_BYTES="$(wc -c $MESSAGES_FILE | awk '{print $1}')"
BYTES_PER_DOC=$(echo "scale=2; $FILE_BYTES / $FILE_DOCS" | bc)
DOCS_PER_SEC=$(echo "scale=2; $FILE_DOCS / $DIFF_SECS" | bc)
MBYTES_PER_SEC=$(echo "scale=2; ($FILE_BYTES / $DIFF_SECS) / 1000000" | bc)
echo ""
echo "=== BENCHMARK COMPLETE ==="
echo "Capture Time: ${DIFF_SECS}s"
echo "Documents: ${FILE_DOCS}"
echo "Total Bytes: ${FILE_BYTES}"
echo "${BYTES_PER_DOC} bytes/doc"
echo "${DOCS_PER_SEC} docs/s"
echo "${MBYTES_PER_SEC} MBps"
echo "=== BENCHMARK COMPLETE ==="
