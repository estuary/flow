#!/usr/bin/env bash
set -Eeu -o pipefail

readonly V2DIR="$(CDPATH= cd "$(dirname "$0")/.." && pwd)"
readonly USAGE="Usage: $0 kube-context"

. "${V2DIR}/test/lib.sh"
configure_environment "${1?Kubernetes context is required. $USAGE}"

# Create a service account for helm's server component "tiller" to use, with the cluster-admin role.
${KUBECTL} create serviceaccount -n kube-system tiller || true
${KUBECTL} create clusterrolebinding tiller-binding --clusterrole=cluster-admin --serviceaccount kube-system:tiller || true

# Install tiller into the Kubernetes cluster, and wait for its pod to startup.
${HELM} init --service-account tiller --upgrade --wait

# Add the "incubator" repository, which is relied on for the "incubator/etcd" chart.
${HELM} repo add incubator http://storage.googleapis.com/kubernetes-charts-incubator
${HELM} repo update
