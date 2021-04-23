#!/bin/bash
# Script to run the set-ops soak test as a kubernetes Job.
# Creates a ConfigMap with the entrypoint script and the couple of go files that we need.
# Then creates the Job that actually runs the test.

set -e

: ${TMPDIR:=/tmp}
kube_ctx="$(kubectl config current-context)"
cm_temp="${TMPDIR}/soak-set-ops-config.yaml"

dir=examples/soak-tests/set-ops

kubectl create configmap set-ops-sources --dry-run=client \
    --from-file $dir/entrypoint.sh \
    --from-file go.mod \
    --from-file $dir/main.go \
    --from-file $dir/generator.go -o yaml > "$cm_temp"

# Confirm first, since this uses the current kubernetes context, and it's easy to accidentally
# modify the wrong environment.
read -p "Confirm before applying configmap and job to context '${kube_ctx}'?(y/n) " yn

if [[ "$yn" == "y" ]]; then
  kubectl apply -f "$cm_temp"
  kubectl apply -f "$dir/k8s-job.yaml"
fi


