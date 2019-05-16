=====================================================
Deploying and Testing Gazette on a Kubernetes cluster
=====================================================

Obtain a Kubernetes Cluster
===========================

Deploying a test Gazette environment requires a Kubernetes cluster with
`Helm <http://helm.sh/>`_ installed. See
`bootstrap_local_kubernetes.sh <bootstrap_local_kubernetes.sh/>`_ for run-able,
prescriptive documentation of bootstrapping a local Kubernetes cluster with Helm.

Gazette has been tested with several providers of local Kubernetes environments:

  - `Docker for Desktop <https://www.docker.com/products/docker-desktop/>`_ (with Kubernetes enabled).
  - `MicroK8s <https://microk8s.io/>`_.
  - `Minikube <https://kubernetes.io/docs/setup/minikube/>`_.

Cloud providers, such as Google Container Engine, may also be used.

Depending on your provider, you may wish to declare ``KUBECTL`` and ``DOCKER``
environment variables (which otherwise default to ``kubectl`` and ``docker``,
respectively). For example, when using MicroK8s:

.. code-block:: console

  # Export DOCKER to use the MicroK8s's docker binary and daemon socket.
  # At present, microk8s.docker cannot be used directly due to github.com/ubuntu/microk8s/issues/272
  #
  # It may also be necessary to disable iptables within the MicroK8s's docker daemon,
  # which can otherwise interfere with inter-pod ICMP used by the "incubator/etcd" chart.
  # This is accomplished by adding "--iptables=false" to /var/snap/microk8s/current/args/dockerd.
  # See: github.com/ubuntu/microk8s/issues/266
  $ export DOCKER="/snap/microk8s/current/usr/bin/docker -H unix:///var/snap/microk8s/current/docker.sock"

  # Export KUBECTL to use the MicroK8s's kubectl binary (and its kubeconfig).
  $ export KUBECTL=microk8s.kubectl

  # Gazette scripts now use these variables. Install Tiller to the "microk8s" context.
  $ v2/test/bootstrap_local_kubernetes.sh microk8s

Deploying Brokers
=================

The `deploy_brokers.sh <deploy_brokers.sh/>`_ script requires a Kubernetes
context and namespace, and deploys a complete Gazette installation
into the named cluster & namespace, including:

  - An Etcd V3 cluster, used by Gazette brokers.
  - `Minio <https://www.minio.io/>`_, which provides an ephemeral S3-compatible fragment store.
  - Gazette brokers.

While that script can be run directly, it's recommended to first try running it
manually to better understand the steps and what's happening with each.

Note that Minio is started with (only) the ``examples`` bucket. This is sufficient
for the bundled example JournalSpec fixtures, but if desired additional buckets
can be created:

.. code-block:: console

  # Port-forward to the running Minio pod.
  $ alias minio_pod="kubectl get pod -l app=minio -o jsonpath='{.items[0].metadata.name}'"
  $ kubectl port-forward $(minio_pod) 9000

  # Access the Minio web UI.
  $ browse http://localhost:9000

  # Login with the configured access & secret key. This defaults to:
  # Access Key:  AKIAIOSFODNN7EXAMPLE
  # Secret Key:  wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY

Example Applications
--------------------

A collection of example applications are started by `deploy_examples.sh <deploy_examples.sh/>`_,
which like ``deploy_brokers.sh`` requires a Kubernetes context and namespace. The namespace
may differ from that of the brokers. ``deploy_examples.sh`` applies a collection of example
JournalSpecs using the Minio fragment store, starts the ``stream-sum`` and ``word-count``
applications, and performs a number of disruptive broker and application Pod deletions to
verify correctness and fast fail-over.

StreamSum
~~~~~~~~~

The ``stream-sum`` example is an integration test in disguise. It consists of a
*chunker* Job which generates a number of randomized streams, emitting small chunks
of each stream in an arbitrary fashion. The *summer* consumer collects and sums each
stream chunk, and upon stream completion emits a final SHA-sum to a
``examples/stream-sums/sums`` journal.

The chunker job, which also knows the expected SHA-sum of the stream, reads and
verifies that a correct sum was computed for every stream it completed. The
example should complete successfully in spite of arbitrary broker or summer
process failures or restarts.

WordCount
~~~~~~~~~

The ``word-count`` chart and example application builds a distributed, on-line
N-gram model of published text, and presents a gRPC API for performing point and
range lookups of the model. It may be interacted with using the ``wordcountctl``
CLI. For example:

.. code-block:: console

    # Create an interactive shell into a wordcount consumer node. 
    # This pod will come with all the required CLI tools needed for the demo.
    $ alias word_count_pod="kubectl get pod -l app.kubernetes.io/name=word-count -o jsonpath='{.items[0].metadata.name}'"
    $ kubectl exec -it $(word_count_pod) /bin/sh

    # Download some test text, "A Tale of Two Cities":
    $ wget http://www.textfiles.com/etext/AUTHORS/DICKENS/dickens-tale-126.txt

    # Verify the consumer is operational:
    $ gazctl shards list --primary
    +-----------+---------+-----------------------------------------------------+
    |    ID     | STATUS  |                       PRIMARY                       |
    +-----------+---------+-----------------------------------------------------+
    | shard-000 | PRIMARY | limping-crocodile-word-count-9f95cb85-x62t6:PRIMARY |
    | shard-001 | PRIMARY | limping-crocodile-word-count-9f95cb85-x62t6:PRIMARY |
    | shard-002 | PRIMARY | limping-crocodile-word-count-9f95cb85-x62t6:PRIMARY |
    | shard-003 | PRIMARY | limping-crocodile-word-count-9f95cb85-x62t6:PRIMARY |
    +-----------+---------+-----------------------------------------------------+

    # Publish text to the application:
    $ wordcountctl publish --file dickens-tale-126.txt

    # Query a specific N-Gram (this assumes N = 2):
    $ wordcountctl query --prefix "best of"
    INFO[0000] gram  count=10 gram="best of"

    # Query a range of N-Grams from specific shards:
    $ wordcountctl query --prefix "best" --shard shard-001
    INFO[0000] gram  count=1 gram="best is"
    INFO[0000] gram  count=1 gram="best patriots"
    INFO[0000] gram  count=1 gram="best short"
    INFO[0000] gram  count=1 gram="best still"
    INFO[0000] gram  count=1 gram="bestowal of"
    INFO[0000] gram  count=1 gram="bestrewn with"

    $ wordcountctl query --prefix "best" --shard shard-002
    INFO[0000] gram  count=3 gram="best and"
    INFO[0000] gram  count=1 gram="best authority"
    INFO[0000] gram  count=1 gram="best condition"
    INFO[0000] gram  count=1 gram="best he"
    INFO[0000] gram  count=1 gram="best mr"
    INFO[0000] gram  count=2 gram="best not"
    INFO[0000] gram  count=1 gram="best room"

