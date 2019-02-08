#!/usr/bin/env bash
set -Eeux -o pipefail

readonly V2DIR="$(CDPATH= cd "$(dirname "$0")/.." && pwd)"
readonly USAGE="Usage: $0 kube-context kube-namespace optional-broker-namespace"
readonly NAMESPACE="${2?Kubernetes namespace is required. ${USAGE}}"
readonly BK_NAMESPACE="${3:-${NAMESPACE}}"

. "${V2DIR}/test/lib.sh"
configure_environment "${1?Kubernetes context is required. ${USAGE}}"

# GAZCTL runs gazctl in an ephemeral docker container which has direct access to
# the kubernetes networking space. The alternative is to run gazctl on the host
# and port-forward broker or consumer pods as required to expose the service (yuck).
readonly GAZCTL="${DOCKER} run \
  --rm \
  --interactive \
  --env BROKER_ADDRESS \
  --env CONSUMER_ADDRESS \
  liveramp/gazette:latest \
  gazctl"

# Create all test journals. Use `sed` to replace the MINIO_RELEASE token with the
# correct Minio service address.
sed -e "s/MINIO_RELEASE/$(helm_release ${BK_NAMESPACE} minio).${BK_NAMESPACE}/g" ${V2DIR}/test/examples.journalspace.yaml | \
  BROKER_ADDRESS=$(release_address $(helm_release ${BK_NAMESPACE} gazette) gazette) ${GAZCTL} journals apply --specs /dev/stdin

# Install a test "gazette-zonemap" ConfigMap in the namespace,
# if one doesn't already exist.
install_zonemap ${NAMESPACE}

# Install the "stream-sum" chart, first updating dependencies and blocking until release is complete.
${HELM} dependency update ${V2DIR}/charts/examples/stream-sum
${HELM} install --namespace ${NAMESPACE} --wait ${V2DIR}/charts/examples/stream-sum --values /dev/stdin << EOF
summer:
  etcd:
    endpoint: http://$(helm_release ${BK_NAMESPACE} etcd)-etcd.${BK_NAMESPACE}:2379
  gazette:
    endpoint: http://$(helm_release ${BK_NAMESPACE} gazette)-gazette.${BK_NAMESPACE}:80
EOF

# Enumerate all stream-sum shards, one for each journal of the topic.
function stream_sum_shards {
  cat<<EOF
# Create one shard for each journal & recoverylog hard-coded in test/examples.journalspace.yaml
# Compare to ShardSpec for field definitions.
common:
  recovery_log_prefix: examples/stream-sum/recovery-logs
  hint_prefix:         /gazette/hints/examples/stream-sum
  hint_backups:        2
  max_txn_duration:    1s
  hot_standbys:        1
shards:
EOF
  for i in $(seq -f "%03g" 0 7); do
     cat<<EOF
- id: chunks-part-${i}
  sources: [journal: examples/stream-sum/chunks/part-${i}]
EOF
    done
}

# Create shards by piping the enumeration to `gazctl shards apply` with the stream-sum service address.
stream_sum_shards | \
  CONSUMER_ADDRESS=$(release_address $(helm_release ${NAMESPACE} stream-sum) summer) ${GAZCTL} shards apply --specs /dev/stdin

# Be a jerk and delete all gazette & stream-sum consumer pods a few times while
# verification jobs are still running. Expect this breaks nothing, and all
# jobs run to completion.
for i in {1..3}; do
  ${KUBECTL} --namespace ${BK_NAMESPACE} delete pod -l "app.kubernetes.io/name = gazette" & # Run in background.
  ${KUBECTL} --namespace ${NAMESPACE} delete pod -l "app.kubernetes.io/name = summer"   # Run synchronously.
  wait $! # Wait for first job to complete.
done

# Update dependencies and install the "word-count" chart.
${HELM} dependency update ${V2DIR}/charts/examples/word-count
${HELM} install --namespace ${NAMESPACE} --wait ${V2DIR}/charts/examples/word-count --values /dev/stdin << EOF
counter:
  etcd:
    endpoint: http://$(helm_release ${BK_NAMESPACE} etcd)-etcd.${BK_NAMESPACE}:2379
  gazette:
    endpoint: http://$(helm_release ${BK_NAMESPACE} gazette)-gazette.${BK_NAMESPACE}:80
EOF

# Enumerate all word-count shards.
function word_count_shards {
  cat<<EOF
# Create one shard for journals & recoverylog hard-coded in test/examples.journalspace.yaml
# Compare to ShardSpec for field definitions.
common:
  recovery_log_prefix: examples/word-count/recovery-logs
  hint_prefix:         /gazette/hints/examples/word-count
  hint_backups:        2
  max_txn_duration:    1s
  hot_standbys:        1
shards:
EOF
  for i in $(seq -f "%03g" 0 3); do
     cat<<EOF
- id: shard-${i}
  sources: [journal: examples/word-count/deltas/part-${i}]
EOF
    done
}

# Create shards by piping the enumeration to `gazctl shards apply` with the word-count service address.
word_count_shards | \
  CONSUMER_ADDRESS=$(release_address $(helm_release ${NAMESPACE} word-count) counter) ${GAZCTL} shards apply --specs /dev/stdin
