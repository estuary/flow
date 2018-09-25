============================================
Notes on setting up and testing via Minikube
============================================

Obtain a Kubernetes Cluster
===========================

Deploying a test Gazette environment requires a Kubernetes cluster with
`Helm <http://helm.sh/>`_ installed. See
`bootstrap_local_kubernetes.sh <bootstrap_local_kubernetes.sh/>`_ for run-able,
prescriptive documentation of bootstrapping a local Kubernetes cluster with Helm.

Other providers, such as Google Container Engine, may also be used.

Deploying A Test Environment
============================

The `deploy_test_environment.sh <deploy_test_environment.sh/>`_ script accepts
a Kubernetes context and namespace, and deploys a complete Gazette installation
into the named cluster & namespace, including:

  - An Etcd V3 cluster, used by Gazette brokers and example applications.
  - `Minio <https://www.minio.io/>`_, which provides an ephemeral S3-compatible fragment store.
  - Gazette brokers, initialized with a collection of test JournalSpecs using the Minio fragment store.
  - Example applications.

While that script can be run directly, it's recommended to first try running it
incrementally and manually at first, to better understand the steps and what's
happening with each.

Note that Minio is started with (only) the ``examples`` bucket. This is sufficient
for test JournalSpec fixtures, but if needed additional buckets can be created:

.. code-block:: console

  # Port-forward to the running Minio pod.
  $ alias minio_pod="kubectl get pod -l release=minio -o jsonpath={.items[0].metadata.name}"
  $ kubectl port-forward $(minio_pod) 9000

  # Access the Minio web UI.
  $ browse http://localhost:9000

  # Login with the configured access & secret key. This defaults to:
  # Access Key:  AKIAIOSFODNN7EXAMPLE
  # Secret Key:  wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY

Example Applications
--------------------

A collection of example applications are started by ``deploy_test_environment.sh``.

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

    # Download some test text, "A Tale of Two Cities":
    $ wget http://www.textfiles.com/etext/AUTHORS/DICKENS/dickens-tale-126.txt

    # Port-forward to a consumer pod:
    $ alias word_count_pod="kubectl get pod -l app.kubernetes.io/name=word-count -o jsonpath={.items[0].metadata.name}"
    $ kubectl port-forward $(word_count_pod) 8080

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

