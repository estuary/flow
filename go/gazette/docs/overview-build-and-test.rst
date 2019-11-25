Build and Test 
===============

Most binaries and packages are "pure" Go and can be directly go-installed and go-tested:

.. code-block:: console

   $ export GO111MODULE=on

   $ go install go.gazette.dev/core/cmd/gazette
   $ go install go.gazette.dev/core/cmd/gazctl
   $ go test go.gazette.dev/core/broker/...
   $ go test go.gazette.dev/core/consumer

Certain packages used by consumer applications, like ``go.gazette.dev/core/consumer/store-rocksdb``,
require CGO to build and also require appropriate development libraries for RocksDB.
Standard Linux packages are insufficient, as run-time type information must be enabled
in the RocksDB build (and it's turned off in the standard Debian package, for example).


Continuous Integration
-----------------------

Gazette uses a `Make-based build system`__ which pulls down and stages
development dependencies into a ``.build`` sub-directory of the repository root:

.. code-block:: console

   $ make go-install
   $ make go-test-fast

__ ../mk/build.mk

Continuous integration builds of Gazette run tests 15 times, with race detection enabled:

.. code-block:: console

   $ make go-test-ci

The Make build system offers fully hermetic builds using a Docker-in-Docker
builder. Run CI tests in a hermetic build environment:

.. code-block:: console

   $ make as-ci target=go-test-ci

Package release Docker images for the Gazette broker and examples:

.. code-block:: console

   $ make as-ci target=ci-release-broker
   $ make as-ci target=ci-release-examples

Deploy Gazette's continuous soak test to a Kubernetes cluster (which can be
Docker for Desktop or Minikube). Soak tests run with ``latest`` images.

.. code-block:: console

   $ kubectl apply -k ./kustomize/test/deploy-stream-sum-with-crash-tests/

The ``kustomize`` directory also has a `helper manifest`_ for using a local
registry (eg, for development builds)

.. _`helper manifest`: ../kustomize/test/run-with-local-registry/kustomization.yaml
