===========================
Containerized Gazette Build
===========================

Gazette releases are built via a multi-stage `Dockerfile <Dockerfile>`_
with multiple named targets:

 1) Debian ``base`` target which includes RocksDB and its runtime dependencies.
 2) A ``builder`` target which builds and tests Gazette.
 3) ``gazette`` target which plucks binaries onto the ``base`` image.
 4) ``gazette-examples`` target which plucks example binaries onto the ``gazette`` image.

See `all.sh <all.sh>`_ for run-able documentation of building each target.
`push.sh <push.sh>`_ pushes built Docker image artifacts to Docker Hub.
