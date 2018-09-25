#!/usr/bin/env bash
set -Eeu -o pipefail

# Default to the "docker-for-desktop" Kubernetes context provided by Docker CE.
CONTEXT="docker-for-desktop"

if kubectl config get-contexts -o name ${CONTEXT} &> /dev/null ; then
  echo "Using Kubernetes context ${CONTEXT}."
elif ! which minikube &> /dev/null ; then
  echo "Cannot bootstrap Kubernetes (neither ${CONTEXT} nor minikube are available)."
  exit 1
else
  # Use minikube to boostrap a native Kubernetes instance (without a VM driver),
  # and with the roles-based auth plugin.
  CONTEXT="minikube"
  FLAGS="--vm-driver=none --extra-config=apiserver.authorization-mode=RBAC"

  # NOTE(johnny): At the moment, minikube requires docker-ce <= 18.06,
  # which can be installed or downgraded to via:
  #
  #   $ sudo apt install docker-ce=18.06.1~ce~3-0~ubuntu
  #
  # This is annoying, and there is no current minikube work-around. See:
  #  - https://github.com/kubernetes/minikube/issues/3320
  #  - https://github.com/kubernetes/minikube/issues/3323

  # TODO(johnny): Remove with https://github.com/kubernetes/kubeadm/issues/845 (K8s v1.11).
  SYSTEMD_RESOLVD_CONF="/run/systemd/resolve/resolv.conf"
  if [[ -f ${SYSTEMD_RESOLVD_CONF} ]]; then
    FLAGS="$FLAGS --extra-config=kubelet.resolv-conf=${SYSTEMD_RESOLVD_CONF}"
  fi

  sudo \
    MINIKUBE_WANTKUBECTLDOWNLOADMSG=false \
    MINIKUBE_WANTNONEDRIVERWARNING=false \
    CHANGE_MINIKUBE_NONE_USER=true \
    minikube start ${FLAGS}
fi

KUBECTL="kubectl --context=${CONTEXT}"
HELM="helm --kube-context=${CONTEXT}"

# Create a service account for helm's server component "tiller" to use, with the cluster-admin role.
$KUBECTL create serviceaccount -n kube-system tiller
$KUBECTL create clusterrolebinding tiller-binding --clusterrole=cluster-admin --serviceaccount kube-system:tiller
$KUBECTL create clusterrolebinding fixDNS --clusterrole=cluster-admin --serviceaccount=kube-system:kube-dns

# Install tiller into the Kubernetes cluster, and wait for its pod to startup.
$HELM init --service-account tiller --wait

# Add the "incubator" repository, which is relied on for the "incubator/etcd" chart.
$HELM repo add incubator http://storage.googleapis.com/kubernetes-charts-incubator
$HELM repo update

echo "Kubernetes and Tiller are ready. Use context \"$CONTEXT.\""
