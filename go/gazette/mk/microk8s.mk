
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
	KUBECTL=microk8s.kubectl \
	${ROOTDIR}/test/bootstrap_local_kubernetes.sh microk8s

# Push the broker image to a local MicroK8s registry.
microk8s-push-broker:
	docker tag gazette/broker:latest   localhost:32000/broker:latest
	docker push localhost:32000/broker:latest

# Push the exmamples image to a local MicroK8s registry.
microk8s-push-examples:
	docker tag gazette/examples:latest localhost:32000/examples:latest
	docker push localhost:32000/examples:latest

# Deploy brokers to the local MicroK8s cluster.
microk8s-deploy-brokers: microk8s-push-broker
	REPOSITORY=localhost:32000 \
	KUBECTL=microk8s.kubectl \
	${ROOTDIR}/test/deploy_brokers.sh microk8s default

# Deploy examples to the local MicroK8s cluster in a continuous soak-testing
# configuration (`CHUNKER_STREAMS=-1` instructs chunker jobs to run forever).
microk8s-deploy-soak: microk8s-push-examples
	REPOSITORY=localhost:32000 \
	KUBECTL=microk8s.kubectl \
	CHUNKER_STREAMS=-1 \
	CHUNKER_JOBS=$(shell nproc --ignore=6) \
	${ROOTDIR}/test/deploy_examples.sh microk8s default

.PHONY: microk8s-reset microk8s-push-broker microk8s-push-examples microk8s-deploy-brokers microk8s-deploy-soak
