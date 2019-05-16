Containerized Gazette Build
===========================

Overview
~~~~~~~~

Gazette releases are built via a multi-stage `Dockerfile <Dockerfile>`_
with multiple named targets:

``base``
  Includes RocksDB and its runtime dependencies.

``vendor``
  Adds Go source of vendored dependencies onto the ``base`` target.

``build``
  Builds and tests Gazette.

``gazette``
  Plucks Gazette binaries onto the ``base`` target.

``examples``
  Plucks Gazette example binaries onto the ``gazette`` target.

See ``docker_build_all`` in `lib.sh <lib.sh>`_ for run-able documentation of
building each target.

Build QuickStart
~~~~~~~~~~~~~~~~

To build the world from a clean local checkout:

.. code-block:: console

    # Perform a containerized build and test of the project and examples (requires Docker).
    $ v2/build/all .
