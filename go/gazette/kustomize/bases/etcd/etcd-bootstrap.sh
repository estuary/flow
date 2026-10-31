#!/bin/sh -ex

. /opt/etcd/etcd-lib.sh
export ETCDCTL_ENDPOINTS="$(seed_endpoints)"

# Wait for other pods to be up before trying to join, otherwise we
# get "no such host" errors when trying to resolve other members.
for i in $(seq 0 $((MIN_REPLICAS - 1))); do
  while true; do
    echo "Waiting for ${ETCD_SERVICE_NAME}-${i}.${ETCD_SERVICE_NAME} to come up"
    ping -W 1 -c 1 ${ETCD_SERVICE_NAME}-${i}.${ETCD_SERVICE_NAME} >/dev/null && break
    sleep 1s
  done
done

# Re-joining after failure?
if [ -f /var/run/etcd/member_id ]; then
  echo "Re-joining etcd member"
  member_id=$(cat /var/run/etcd/member_id)

  # Re-join member.
  exec etcd \
    --advertise-client-urls http://$(hostname).${ETCD_SERVICE_NAME}:2379 \
    --data-dir /var/run/etcd/default.etcd \
    --listen-client-urls http://0.0.0.0:2379 \
    --listen-peer-urls http://0.0.0.0:2380 \
    --logger zap \
    --name $(hostname)
fi

# This member is being bootstrapped.
persist_member_hash &

# Adding a seed member?
if [ $(member_index) -lt ${MIN_REPLICAS} ]; then
  exec etcd \
    --advertise-client-urls http://$(hostname).${ETCD_SERVICE_NAME}:2379 \
    --data-dir /var/run/etcd/default.etcd \
    --initial-advertise-peer-urls http://$(hostname).${ETCD_SERVICE_NAME}:2380 \
    --initial-cluster $(seed_peers) \
    --initial-cluster-state new \
    --initial-cluster-token etcd-cluster-1 \
    --listen-client-urls http://0.0.0.0:2379 \
    --listen-peer-urls http://0.0.0.0:2380 \
    --logger zap \
    --name $(hostname)
fi

# We're adding a new member to an existing cluster.
# Requires other members are healthy and available.

echo "Adding new member"
etcdctl ${AUTH_FLAGS} member add $(hostname) \
  --peer-urls http://$(hostname).${ETCD_SERVICE_NAME}:2380 \
  --command-timeout=300s \
  --debug |
  grep "^ETCD_" >/var/run/etcd/new_member_envs

if [ $? -ne 0 ]; then
  echo "Failed to add member."
  exit 1
fi

cat /var/run/etcd/new_member_envs
. /var/run/etcd/new_member_envs

exec etcd \
  --advertise-client-urls http://$(hostname).${ETCD_SERVICE_NAME}:2379 \
  --data-dir /var/run/etcd/default.etcd \
  --initial-advertise-peer-urls http://$(hostname).${ETCD_SERVICE_NAME}:2380 \
  --initial-cluster ${ETCD_INITIAL_CLUSTER} \
  --initial-cluster-state ${ETCD_INITIAL_CLUSTER_STATE} \
  --listen-client-urls http://0.0.0.0:2379 \
  --listen-peer-urls http://0.0.0.0:2380 \
  --logger zap \
  --name $(hostname)
