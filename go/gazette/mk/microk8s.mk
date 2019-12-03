
# Running microk8s.reset can take quite a while when many pods are running.
# This command nukes microk8s from orbit, by deleting its Etcd backing store
# and restarting, then spinning up Helm. In practice it's much faster for
# tearing down and re-initializing a local cluster.
microk8s-reset:
	sudo microk8s.stop
	sudo rm -r /var/snap/microk8s/common/var/run/etcd/member
	sleep 1
	sudo microk8s.start || true
	microk8s.enable dns storage registry

.PHONY: microk8s-reset
