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

Gazette uses a :githubsource:`Make-based build system<Makefile>` which pulls down and stages
development dependencies into a ``.build`` sub-directory of the repository root.

The Make build system offers fully hermetic builds using a Docker-in-Docker
builder. Run CI tests in a hermetic build environment:

.. code-block:: console

   $ make as-ci target=go-test-ci

Continuous integration builds of Gazette run tests 15 times, with race detection enabled.

Package release Docker images for the Gazette broker and examples,
as ``gazette-broker:latest`` and ``gazette-examples:latest``.

.. code-block:: console

   $ make as-ci target=ci-release-gazette-broker
   $ make as-ci target=ci-release-gazette-examples

Deploy Gazette's continuous soak test to a Kubernetes cluster (which can be
Docker for Desktop or Minikube). Soak tests run with ``latest`` images.

.. code-block:: console

   $ kubectl apply -k ./kustomize/test/deploy-stream-sum-with-crash-tests/

Push images to a registry. If ``registry`` is not specified, it defaults to ``localhost:32000`` (which is used by MicroK8s):

.. code-block:: console

   $ make push-to-registry registry=my.registry.com

The ``kustomize`` directory also has a
:githubsource:`helper manifest<kustomize/test/run-with-local-registry/kustomization.yaml>` for using
a local registry (eg, for development builds).


Dependencies
-------------

It's recommended to use the docker-based build targets described above in most situations, since the
docker image will have all the required dependencies. If you want to execute build targets directly
instead of using ``as-ci``, then the following dependencies are required:

* Compression library development headers ("-dev" packages):

    * ``libbz2``
    * ``libsnappy``
    * ``liblz4``
    * ``libzstd``
    * ``libbz2``

* Protobuf compiler:

    * ``libprotobuf`` (development headers)
    * ``protobuf-compiler``

* Etcd: The deb/rpm packages provided in many repositories are likely too old to work with
  Gazette. Gazette requires version 3.3 or later. Version 3.4.x is recommended, since that is used 
  in Gazette CI.

* Sqlite

    * ``libsqlite3`` (development headers)
    * It's also probably useful to have the sqlite3 CLI for debugging

* RocksDB: On linux systems, this will be downloaded and built automatically. You'll need to have a
  few things in order for this to work. Most systems will already have this stuff, but it's listed
  here anyway just for the sake of being thorough

    * A C compiler toolchain (on debian-based distros, the ``build-essential`` package will have you covered)
    * ``curl``
    * ``ca-certificates`` (so that curl can validate the certificate of the rocksdb download server)
    * ``tar``

Other Build Targets
--------------------

If you execute these targets directly, then you'll need to have all of the above dependencies installed.

.. code-block:: console

    $ make go-install
    $ make go-test-fast


.. code-block:: console

    $ make go-test-ci

Building the Docs
------------------

To build these docs locally, you'll need a few more dependencies. To start with, you'll need
``python`` and ``pip``. Note that on some systems, these may be called ``python3`` and ``pip3``.
Next you'll need to install the following python packages using ``pip install --user <package>``.

* ``sphinx``
* ``sphinxcontrib-programoutput``
* ``sphinx_rtd_theme``

Once you have all those installed, you can change directory into ``docs/`` and run ``make html``.
This will write the output to ``docs/_build``, and then you can open any of the html files in your
browser.

