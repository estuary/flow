#!/usr/bin/env bash
set -Eeux -o pipefail

readonly V2DIR="$(CDPATH= cd "$(dirname "$0")/.." && pwd)"
readonly USAGE="Usage: $0 kube-context kube-namespace"
readonly NAMESPACE="${2?Kubernetes namespace is required. ${USAGE}}"
readonly REPOSITORY="${REPOSITORY:-liveramp}"

. "${V2DIR}/test/lib.sh"
configure_environment "${1?Kubernetes context is required. ${USAGE}}"


# Install the "incubator/etcd" chart (https://github.com/helm/charts/tree/master/incubator/etcd).
# Despite being in incubator, this chart is recommended over etcd-operator due to its use of
# PersistentVolumeClaims and StatefulSets.
${HELM} install --namespace ${NAMESPACE} --wait incubator/etcd --values /dev/stdin << EOF
replicas: 3
image:
  repository: quay.io/coreos/etcd
  tag: v3.3.9
persistentVolume:
  enabled: true
  storage: 256Mi
EOF

# Install the "minio" chart, which provides an S3-compatible, local, ephemeral filesystem.
${HELM} install --namespace ${NAMESPACE} --wait stable/minio --values /dev/stdin << EOF
defaultBucket:
  enabled: true   # Ask the minio chart to create a bucket.
  name: examples
  policy: public  # One of none|download|upload|public.
persistence:
  enabled: false  # Bucket data is ephemeral, and lives only with the minio pod.
EOF

# minio_credentials_b64 emits base64 ~/.aws/credentials content for use with Minio.
function minio_credentials_b64 {
  jq -rRs @base64 <<EOF
[minio]
# These are the default example credentials which Minio starts with.
aws_access_key_id=AKIAIOSFODNN7EXAMPLE
aws_secret_access_key=wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY
EOF
}

# minio_config_b64 emits base64 ~/.aws/config content for use with Minio.
function minio_config_b64 {
  # Note that ~/.aws/config uses "[profile my-name]",
  # where ~/.aws/credentials uses just "[my-name]".
  jq -rRs @base64 <<EOF
[profile minio]
region=us-east-1
EOF
}

${KUBECTL} --namespace ${NAMESPACE} apply -f - << EOF
apiVersion: v1
kind: Secret
metadata:
  name: gazette-aws-credentials
data:
  credentials: $(minio_credentials_b64)
  config: $(minio_config_b64)
EOF

# Install a test "gazette-zonemap" ConfigMap.
install_zonemap ${NAMESPACE}

# Install the "gazette" chart, configured to use our Etcd release.
${HELM} install --namespace ${NAMESPACE} --wait ${V2DIR}/charts/gazette  --values /dev/stdin << EOF
image:
  repository: ${REPOSITORY}/gazette
etcd:
  endpoint: http://$(helm_release ${NAMESPACE} etcd)-etcd.${NAMESPACE}:2379
EOF
