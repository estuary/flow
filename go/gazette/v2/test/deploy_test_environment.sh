#!/usr/bin/env bash
set -Eeu -o pipefail

# Base V2 directory which parents this script.
V2DIR="$(cd "$(dirname "$0")/.." && pwd)"

# Defaults for NAMESPACE & CONTEXT. The latter is conditioned on whether minikube is installed.
NAMESPACE="default"
if [[ -x "$(which minikube)" ]] ; then
  CONTEXT="minikube"
else
  CONTEXT="docker-for-desktop"
fi

# Parse explicit CONTEXT & NAMESPACE options.
usage() { echo "Usage: $0 [ -c kubernetes-context ] [ -n kubernetes-namespace ]" 1>&2; exit 1; }

while getopts ":c:n:" opt; do
  case "${opt}" in
    c)   CONTEXT=${OPTARG} ;;
    n)   NAMESPACE=${OPTARG} ;;
    \? ) usage ;;
  esac
done

echo "Using context \"$CONTEXT\" & namespace \"$NAMESPACE\""

# Alias `kubectl` and `helm` to use the proper context & namespace.
KUBECTL="kubectl --context ${CONTEXT} --namespace ${NAMESPACE}"
HELM="helm --kube-context ${CONTEXT}"

# lastHelmRelease retrieves the name (eg, "oily-wombat") created by the last `helm install`.
function lastHelmRelease {
  $HELM list --date --reverse --output json | jq -r '.Releases[0].Name'
}

# releaseAddress returns the endpoint (eg, "http://172.12.0.10:8080") of the named release.
# Endpoints are raw IPs, and must have access to the Kubernetes network namespace, but do
# not require access to kube-DNS (eg can be run on the Linux host, or a docker container).
# It requires that Services are using Kubernetes recommended labels:
#   https://kubernetes.io/docs/concepts/overview/working-with-objects/common-labels/
# Note that Gazette, and many public Helm charts do so, but not all (particularly charts generated prior to Helm V2).
function releaseSvcAddress {
    host=$($KUBECTL get svc -l app.kubernetes.io/instance=$1 -o jsonpath={.items[0].spec.clusterIP})
    port=$($KUBECTL get svc -l app.kubernetes.io/instance=$1 -o jsonpath={.items[0].spec.ports[0].port})
    echo "http://$host:$port"
}

# Install the "incubator/etcd" chart (https://github.com/helm/charts/tree/master/incubator/etcd).
# Despite being in incubator, this chart is recommended over etcd-operator due to its use of
# PersistentVolumeClaims and StatefulSets.
$HELM install --namespace $NAMESPACE --wait incubator/etcd --values /dev/stdin << EOF
replicas: 3
image:
  repository: quay.io/coreos/etcd
  tag: v3.3.9
persistentVolume:
  enabled: true
  storage: 256Mi
EOF
ETCD_RELEASE=$(lastHelmRelease)

# Install the "minio" chart, which provides an S3-compatible cloud filesystem.
$HELM install --namespace $NAMESPACE --wait stable/minio --values /dev/stdin << EOF
defaultBucket:
  enabled: true   # Ask the minio chart to create a bucket.
  name: examples
  policy: public  # One of none|download|upload|public.
persistence:
  enabled: false  # Bucket data is ephemeral, and lives only with the minio pod.
EOF
MINIO_RELEASE=$(lastHelmRelease)

# Create a Secret holding AWS credentials.
function minioCredentialsB64 {
    jq -rRs @base64 <<EOF
[minio]
# These are the default example credentials which Minio starts with.
aws_access_key_id=AKIAIOSFODNN7EXAMPLE
aws_secret_access_key=wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY
EOF
}
function minioConfigB64 {
    # Note that ~/.aws/config uses "[profile my-name]",
    # where ~/.aws/credentials uses just "[my-name]".
    jq -rRs @base64 <<EOF
[profile minio]
region=us-east-1
EOF
}

$KUBECTL apply -f - << EOF
apiVersion: v1
kind: Secret
metadata:
  name: gazette-aws-credentials
data:
  credentials: $(minioCredentialsB64)
  config: $(minioConfigB64)
EOF

# Create a testing gazette-zonemap ConfigMap, which will return a random zone on each invocation.
$V2DIR/charts/generate-zonemap-testing.sh | $KUBECTL apply -f -

# Install the "gazette" chart, configured to use our Etcd release.
$HELM install --namespace $NAMESPACE --wait $V2DIR/charts/gazette  --values /dev/stdin << EOF
etcd:
  endpoint: http://${ETCD_RELEASE}-etcd:2379
EOF
GAZETTE_RELEASE=$(lastHelmRelease)

# We'll run gazctl in an ephemeral docker container which has direct access to
# the kubernetes networking space, and mounts this "test" directory. The
# alternative is to run gazctl on the host and port-forward broker or consumer
# pods as required to expose the service (yuck).
GAZCTL="docker run \
    --rm \
    --interactive \
    --env BROKER_ADDRESS \
    --env CONSUMER_ADDRESS \
    liveramp/gazette:latest \
    gazctl"

# Create all test journals. Use `sed` to replace the MINIO_ENDPOINT token with the
# correct, URL-encoded Minio service address.
sed -e "s/MINIO_ENDPOINT/http%3A\/\/${MINIO_RELEASE}%3A9000/g" $V2DIR/test/journalspace.yaml | \
  BROKER_ADDRESS=$(releaseSvcAddress $GAZETTE_RELEASE) $GAZCTL journals apply --specs /dev/stdin

# Install the "stream-sum" chart, first updating dependencies and blocking until release is complete.
$HELM install --namespace $NAMESPACE --dep-up --wait $V2DIR/charts/examples/stream-sum --values /dev/stdin << EOF
consumer:
  etcd:
    endpoint: http://${ETCD_RELEASE}-etcd:2379
  gazette:
    endpoint: http://${GAZETTE_RELEASE}-gazette:80
EOF
STREAM_SUM_RELEASE=$(lastHelmRelease)

# Enumerate all stream-sum shards, one for each journal of the topic.
function streamSumShards {
    # Create one shard for each journal & recoverylog hard-coded in test/journalspace.yaml
    # TODO(johnny): This is hacky, but works until we can define better tooling.
    for i in $(seq -f "%03g" 0 7); do
       cat<<EOF
# Define ShardSpec in YAML format. Compare to ShardSpec for field definitions.
- id: chunks-part-$i
  sources:
  - journal: examples/stream-sum/chunks/part-$i
  recovery_log: examples/stream-sum/recovery-logs/shard-chunks-$i
  hint_keys:
  - /gazette/hints/examples/streams-sum/part-$i.recorded
  - /gazette/hints/examples/streams-sum/part-$i.recovered-1
  - /gazette/hints/examples/streams-sum/part-$i.recovered-2
  max_txn_duration: 1s
  disable: false
  hot_standbys: 1
EOF
    done
}

# Create shards by piping the enumeration to `gazctl shards apply` with the stream-sum service address.
streamSumShards | \
  CONSUMER_ADDRESS=$(releaseSvcAddress $STREAM_SUM_RELEASE) $GAZCTL shards apply --specs /dev/stdin

# Be a jerk and delete all gazette & stream-sum consumer pods a few times while
# verification jobs are still running. Expect this breaks nothing, and all
# jobs run to completion.
for i in {1..3}; do
  $KUBECTL delete pod -l "app.kubernetes.io/name in (gazette, stream-sum)"
done

# Install the "word-count" chart.
$HELM install --namespace $NAMESPACE --dep-up --wait $V2DIR/charts/examples/word-count --values /dev/stdin << EOF
consumer:
  etcd:
    endpoint: http://${ETCD_RELEASE}-etcd:2379
  gazette:
    endpoint: http://${GAZETTE_RELEASE}-gazette:80
EOF
WORD_COUNT_RELEASE=$(lastHelmRelease)

# Enumerate all word-count shards.
function wordCountShards {
    # Create one shard for journals & recoverylog hard-coded in test/journalspace.yaml
    for i in $(seq -f "%03g" 0 3); do
       cat<<EOF
# Define ShardSpec in YAML format. Compare to ShardSpec for field definitions.
- id: shard-$i
  sources:
  - journal: examples/word-count/deltas/part-$i
  - journal: examples/word-count/relocations/part-$i
  recovery_log: examples/word-count/recovery-logs/shard-$i
  hint_keys:
  - /gazette/hints/examples/word-count/part-$i.recorded
  - /gazette/hints/examples/word-count/part-$i.recovered-1
  - /gazette/hints/examples/word-count/part-$i.recovered-2
  max_txn_duration: 1s
  disable: false
  hot_standbys: 1
EOF
    done
}

# Create shards by piping the enumeration to `gazctl shards apply` with the word-count service address.
wordCountShards | \
  CONSUMER_ADDRESS=$(releaseSvcAddress $WORD_COUNT_RELEASE) $GAZCTL shards apply --specs /dev/stdin
