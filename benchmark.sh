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

export CONNECTOR_IMAGE="ghcr.io/estuary/source-gigahello:342b2e5"

# Benchmark configuration: small, medium, or large
BENCHMARK_SIZE=${BENCHMARK_SIZE:-medium}

SHORT_MESSAGE="Hello ---------------------------------------------------------------------------------------------------- world!"
LONG_MESSAGE="Hello ---------------------------------------------------------------------------------------------------- world! Lorem ipsum Lorem ipsum Lorem ipsum Lorem ipsum Lorem ipsum Lorem ipsum Lorem ipsum Lorem ipsum Lorem ipsum Lorem ipsum Lorem ipsum Lorem ipsum Lorem ipsum Lorem ipsum Lorem ipsum Lorem ipsum Lorem ipsum Lorem ipsum Lorem ipsum Lorem ipsum Lorem ipsum Lorem ipsum Lorem ipsum Lorem ipsum Lorem ipsum Lorem ipsum Lorem ipsum Lorem ipsum Lorem ipsum Lorem ipsum Lorem ipsum Lorem ipsum Lorem ipsum Lorem ipsum Lorem ipsum Lorem ipsum Lorem ipsum Lorem ipsum Lorem ipsum Lorem ipsum Lorem ipsum Lorem ipsum Lorem ipsum Lorem ipsum Lorem ipsum Lorem ipsum Lorem ipsum Lorem ipsum Lorem ipsum Lorem ipsum Lorem ipsum Lorem ipsum Lorem ipsum Lorem ipsum Lorem ipsum Lorem ipsum Lorem ipsum Lorem ipsum Lorem ipsum Lorem ipsum Lorem ipsum Lorem ipsum Lorem ipsum Lorem ipsum Lorem ipsum Lorem ipsum Lorem ipsum Lorem ipsum Lorem ipsum Lorem ipsum Lorem ipsum Lorem ipsum Lorem ipsum Lorem ipsum Lorem ipsum Lorem ipsum Lorem ipsum Lorem ipsum Lorem ipsum Lorem ipsum Lorem ipsum Lorem ipsum Lorem ipsum Lorem ipsum Lorem ipsum Lorem ipsum Lorem ipsum Lorem ipsum Lorem ipsum Lorem ipsum Lorem ipsum Lorem ipsum Lorem ipsum Lorem ipsum Lorem ipsum Lorem ipsum Lorem ipsum Lorem ipsum Lorem ipsum Lorem ipsum Lorem ipsum Lorem ipsum Lorem ipsum Lorem ipsum Lorem ipsum Lorem ipsum Lorem ipsum Lorem ipsum Lorem ipsum Lorem ipsum Lorem ipsum Lorem ipsum Lorem ipsum Lorem ipsum Lorem ipsum Lorem ipsum Lorem ipsum Lorem ipsum Lorem ipsum Lorem ipsum Lorem ipsum Lorem ipsum Lorem ipsum Lorem ipsum Lorem ipsum Lorem ipsum Lorem ipsum Lorem ipsum Lorem ipsum Lorem ipsum Lorem ipsum Lorem ipsum Lorem ipsum Lorem ipsum Lorem ipsum Lorem ipsum Lorem ipsum Lorem ipsum Lorem ipsum Lorem ipsum Lorem ipsum Lorem ipsum Lorem ipsum Lorem ipsum Lorem ipsum Lorem ipsum Lorem ipsum Lorem ipsum Lorem ipsum Lorem ipsum Lorem ipsum Lorem ipsum Lorem ipsum Lorem ipsum Lorem ipsum Lorem ipsum Lorem ipsum Lorem ipsum Lorem ipsum Lorem ipsum Lorem ipsum Lorem ipsum Lorem ipsum Lorem ipsum Lorem ipsum Lorem ipsum Lorem ipsum Lorem ipsum Lorem ipsum Lorem ipsum Lorem ipsum Lorem ipsum Lorem ipsum Lorem ipsum Lorem ipsum Lorem ipsum Lorem ipsum Lorem ipsum Lorem ipsum Lorem ipsum Lorem ipsum Lorem ipsum Lorem ipsum Lorem ipsum Lorem ipsum Lorem ipsum Lorem ipsum Lorem ipsum Lorem ipsum Lorem ipsum Lorem ipsum Lorem ipsum Lorem ipsum Lorem ipsum Lorem ipsum Lorem ipsum Lorem ipsum Lorem ipsum Lorem ipsum Lorem ipsum"

RAW_PAYLOAD='{"user_id":12345,"order_id":987654321,"status_code":200,"retry_count":3,"priority_level":7,"username":"john.doe.example","email":"john.doe@company.example.com","department":"Engineering","product_name":"Advanced Benchmarking Platform","category":"Enterprise Software","created_at":"2024-08-06T21:46:55.543442804Z","updated_at":"2024-08-06T22:15:30.128945623Z","expires_at":"2024-12-31T23:59:59.999999999Z","description":"Lorem ipsum dolor sit amet, consectetur adipiscing elit, sed do eiusmod tempor incididunt ut labore et dolore magna aliqua. Ut enim ad minim veniam, quis nostrud exercitation ullamco laboris nisi ut aliquip ex ea commodo consequat. Duis aute irure dolor in reprehenderit in voluptate velit esse cillum dolore eu fugiat nulla pariatur. Excepteur sint occaecat cupidatat non proident, sunt in culpa qui officia deserunt mollit anim id est laborum. Sed ut perspiciatis unde omnis iste natus error sit voluptatem accusantium doloremque laudantium, totam rem aperiam, eaque ipsa quae ab illo inventore veritatis et quasi architecto beatae vitae dicta sunt explicabo. Nemo enim ipsam voluptatem quia voluptas sit aspernatur aut odit aut fugit, sed quia consequuntur magni dolores eos qui ratione voluptatem sequi nesciunt. Neque porro quisquam est, qui dolorem ipsum quia dolor sit amet, consectetur, adipisci velit, sed quia non numquam eius modi tempora incidunt ut labore et dolore magnam aliquam quaerat voluptatem. Ut enim ad minima veniam, quis nostrum exercitationem ullam corporis suscipit laboriosam, nisi ut aliquid ex ea commodi consequatur."}'

# Configure benchmark parameters based on size
case $BENCHMARK_SIZE in
  small)
    BENCHMARK_MESSAGE="$SHORT_MESSAGE"
    BENCHMARK_RAW="{}"
    ;;
  medium)
    BENCHMARK_MESSAGE="$SHORT_MESSAGE"
    BENCHMARK_RAW="$RAW_PAYLOAD"
    ;;
  large)
    BENCHMARK_MESSAGE="$LONG_MESSAGE"
    BENCHMARK_RAW="$RAW_PAYLOAD"
    ;;
  *)
    echo "Error: BENCHMARK_SIZE must be 'small', 'medium', or 'large'"
    exit 1
    ;;
esac

echo "Running benchmark with size: $BENCHMARK_SIZE"

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

# Check if perf_event_paranoid is set to allow non-root perf recording
check_perf_paranoid() {
  local paranoid_value
  paranoid_value=$(cat /proc/sys/kernel/perf_event_paranoid 2>/dev/null || echo "unknown")

  if [[ "$paranoid_value" != "1" && "$paranoid_value" != "0" && "$paranoid_value" != "-1" ]]; then
    echo "Error: perf_event_paranoid is set to '$paranoid_value', which prevents non-root perf recording."
    echo "Run this command first: echo '1' | sudo tee /proc/sys/kernel/perf_event_paranoid"
    exit 1
  fi
  echo "perf_event_paranoid check passed (value: $paranoid_value)"
}

# Check perf_event_paranoid before proceeding
check_perf_paranoid

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
    journals:
      fragments:
        flushInterval: 1m
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
          raw: "${BENCHMARK_RAW//\"/\\\"}"
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

echo "Waiting 10s..."
sleep 10
echo "Splitting output journal"
flowctl-go journals split --splits=32 --journal tests/benchmark/greetings/ffffffffffffffff/pivot=00

# Begin a perf recording
echo "Locating flowctl-go consumer process..."
CONSUMER_PID=""
for i in {1..30}; do
  CONSUMER_PID=$(pgrep -f "flowctl-go.*consumer" || echo "")
  if [[ -n "$CONSUMER_PID" ]]; then
    echo "Found consumer process (PID: $CONSUMER_PID)"
    break
  fi
  sleep 1
done

if [[ -z "$CONSUMER_PID" ]]; then
  bail "Could not find flowctl-go consumer process after 30 seconds"
fi

# Start perf recording in the background
echo "Starting perf record on consumer process..."
perf record -g -p "$CONSUMER_PID" -o "${BENCHMARK_PREFIX}consumer-perf.data" &
PERF_PID=$!

# Update trap to also kill perf recording
trap "kill $PERF_PID 2>/dev/null || true; kill -s SIGTERM ${DATA_PLANE_PID} && wait ${DATA_PLANE_PID}" EXIT

# Capture CPU profile
echo "===================="
echo "Starting CPU profile of Flow consumer..."
echo "===================="
curl -o "${BENCHMARK_PREFIX}consumer-cpu.prof" "http://localhost:6060/debug/pprof/profile?seconds=180"
echo "===================="
echo "CPU profile completed!"
echo "===================="

# Capture allocations profile
echo "Capturing allocations profile of Flow consumer..."
curl -o "${BENCHMARK_PREFIX}consumer-allocs.prof" "http://localhost:6060/debug/pprof/allocs"
echo "Allocations profile completed!"

# Stop perf recording
echo "Stopping perf recording..."
kill $PERF_PID 2>/dev/null || true
wait $PERF_PID 2>/dev/null || true

# Disable the task
echo "    shards:" >> ${CATALOG_SOURCE}
echo "      disable: true" >> ${CATALOG_SOURCE}
BUILD_ID=2222222222222222
publish ${CATALOG_SOURCE} ${BUILD_ID}

# Wait longer than the configured flushInterval
sleep 65

# Read all the messages out of the data plane into a file
MESSAGES_FILE="$TMPDIR/messages.jsonl"
CHECKPOINTS_FILE="$TMPDIR/checkpoints.jsonl"
flowctl-go journals read -l name=tests/benchmark/greetings/ffffffffffffffff/pivot=00 > ${MESSAGES_FILE}
flowctl-go journals read -l name=recovery/capture/tests/benchmark/source-gigahello/ffffffffffffffff/00000000-00000000 | strings | grep bindingStateV1 | sed 's/[^{]*{/{/' > ${CHECKPOINTS_FILE}

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
echo "Output Interval:  ${DIFF_SECS}s"
echo "Output Documents: ${FILE_DOCS}"
echo "Output Bytes:     ${FILE_BYTES}"
echo "Output: ${BYTES_PER_DOC} bytes/doc"
echo "Output: ${DOCS_PER_SEC} docs/s"
echo "Output: ${MBYTES_PER_SEC} MBps"

SOURCE_TIME="$(tail -n1 $CHECKPOINTS_FILE | jq '(.LatestTime - .StartTime) / 1000000000')"
SOURCE_DOCS="$(tail -n1 $CHECKPOINTS_FILE | jq '.bindingStateV1.greetings.counter')"
SOURCE_BYTES="$(tail -n1 $CHECKPOINTS_FILE | jq '.TotalBytes')"
SOURCE_BYTES_PER_DOC=$(echo "scale=2; $SOURCE_BYTES / $SOURCE_DOCS" | bc)
SOURCE_DOCS_PER_SEC=$(echo "scale=2; $SOURCE_DOCS / $SOURCE_TIME" | bc)
SOURCE_MBYTES_PER_SEC=$(echo "scale=2; ($SOURCE_BYTES / $SOURCE_TIME) / 1000000" | bc)
echo ""
echo "Source Interval:  ${SOURCE_TIME}s"
echo "Source Documents: ${SOURCE_DOCS}"
echo "Source Bytes:     ${SOURCE_BYTES}"
echo "Source: ${SOURCE_BYTES_PER_DOC} bytes/doc"
echo "Source: ${SOURCE_DOCS_PER_SEC} docs/s"
echo "Source: ${SOURCE_MBYTES_PER_SEC} MBps"
echo "=== BENCHMARK COMPLETE ==="

echo ""
echo "Wrote profiling results to ${BENCHMARK_PREFIX}<foo>"
