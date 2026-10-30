#!/bin/sh -ex

readonly ETCD_SERVICE_NAME="${ETCD_SERVICE_NAME?ETCD_SERVICE_NAME is required}"
readonly MIN_REPLICAS="${MIN_REPLICAS?MIN_REPLICAS is required}"
readonly AUTH_FLAGS="${AUTH_FLAGS?AUTH_FLAGS is required}"

# Compose endpoint list of the MIN_REPLICAS seed cluster.
seed_endpoints() {
  lst=""
  for i in $(seq 0 $((MIN_REPLICAS - 1))); do
    lst="${lst}${lst:+,}http://${ETCD_SERVICE_NAME}-${i}.${ETCD_SERVICE_NAME}:2379"
  done
  echo "${lst}"
}

# Compose peer list of the MIN_REPLICAS seed cluster.
seed_peers() {
  lst=""
  for i in $(seq 0 $((MIN_REPLICAS - 1))); do
    lst="${lst}${lst:+,}${ETCD_SERVICE_NAME}-${i}=http://${ETCD_SERVICE_NAME}-${i}.${ETCD_SERVICE_NAME}:2380"
  done
  echo "${lst}"
}

# Select the member hash ID of this host from amoung the current member list.
member_hash() {
  etcdctl ${AUTH_FLAGS} member list | grep "http://$(hostname).${ETCD_SERVICE_NAME}:2380" | cut -d',' -f1
}

# Persist member hash ID into the persistent volume for future member restart.
persist_member_hash() {
  while ! etcdctl ${AUTH_FLAGS} member list; do sleep 1; done
  member_hash >/var/run/etcd/member_id
}

member_index() {
  readonly h=$(hostname)
  echo ${h##*[^0-9]}
}
