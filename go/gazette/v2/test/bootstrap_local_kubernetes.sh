#!/usr/bin/env bash
set -Eeu -o pipefail

readonly USAGE="Usage: $0 kube-context"
readonly CONTEXT="${1?Kubernetes context is required. $USAGE}"

# KUBECTL defaults to `kubectl`, and uses an explicit --context.
readonly KUBECTL="${KUBECTL:-kubectl} --context ${CONTEXT}"

# HELM defaults to `helm`, and uses an explicit --context and --kubeconfig.
# KUBECTL may use a non-standard location for its config (eg, microk8s.kubectl
# manages kubernetes config under its /snap directory). We make configuration
# explicit by copying into a tempfile and passing to helm by flag.
readonly TMPKUBECONFIG=$(mktemp)
readonly HELM="${HELM:-helm} --kube-context ${CONTEXT} --kubeconfig ${TMPKUBECONFIG}"
trap "{ rm -f ${TMPKUBECONFIG}; }" EXIT
${KUBECTL} config view --raw > ${TMPKUBECONFIG}

# Create a service account for helm's server component "tiller" to use, with the cluster-admin role.
${KUBECTL} create serviceaccount -n kube-system tiller || true
${KUBECTL} create clusterrolebinding tiller-binding --clusterrole=cluster-admin --serviceaccount kube-system:tiller || true

# Install tiller into the Kubernetes cluster, and wait for its pod to startup.
${HELM} init --service-account tiller --wait

# Add the "incubator" repository, which is relied on for the "incubator/etcd" chart.
${HELM} repo add incubator http://storage.googleapis.com/kubernetes-charts-incubator
${HELM} repo update
