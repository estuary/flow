
This directory contains (older) Kustomize-based deployment
resources for managing Gazette in a Kubernetes environment.

We're actually moving away from Kustomize, and use Tanka ourselves to manage deployments.
(We plan to eventually switch this out with Jsonnet modules for dynamic operations
configuration, plus some generated manifests for quickly spinning up a test cluster).

Soak Tests
==========

Gazette is distributed with soak test automation, designed to run on a (local) Kubernetes cluster.

Soak tests run the stream-sum example continuously.
That example performs SHA-summing over generated random chunks of data,
and actively looks for dropped, duplicated, or modified data
(as compared to a ground-truth value known to the stream `chunker`).

Meanwhile, soak tests run a `crash-test-runner` which actively introduces
network partitions, single pod failures, and full pod deletions. Basically
any error we can think of which Gazette and applications should be able to
tolerate gracefully (so hard-killing all brokers is out of scope, of course).

Run them using Kind, a Kubernetes-in-Docker tool, as:

```console
# Build Gazette.
$ make as-ci target=go-install
# Package gazette/broker:latest image.
$ make as-ci target=ci-release-gazette-broker
# Package gazette/examples:latest image.
$ make as-ci target=ci-release-gazette-examples

# Install kind (if needed).
$ go get sigs.k8s.io/kind
$ kind create cluster

# Apply the complete soak test, running in namespace stream-sum.
$ kubectl apply -k kustomize/test/deploy-stream-sum-with-crash-tests/

# Watch logs of the crash-test job, which will run a number of rounds of crash tests.
$ kubectl -nstream-sum logs -f -l app.kubernetes.io/name=crash-test

# Watch logs of the summer application (optional).
$ ~/go/bin/kail -nstream-sum --since=1h -l app.kubernetes.io/name=summer

# Cleanup.
$ kind delete cluster
```