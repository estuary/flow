Containerized Gazette Build
===========================

From the top-level repository directory:

Pull a base runtime image with compiled RocksDB libraries and a protobuf
compiler. This image is used both for building gazette itself, and also for
multi-stage Dockerfiles which pick out compiled binaries of the gazette build
but still require the RocksDB runtime::

  docker pull liveramp/gazette-base:1.0.0

Build and test Gazette. This image includes all Gazette source, vendored
dependencies, compiled packages and binaries, and only completes after
all tests pass::

  docker build . -f build/Dockerfile.gazette-build --tag gazette-build

Create the ``gazette`` command container, which plucks the ``gazette`` binary
onto a ``gazette-base`` image. Other containerized binaries use a similar
pattern::

  docker build . -f build/cmd/Dockerfile.gazette --tag gazette

Gazette examples also have Dockerized build targets. Eg, ``word-count``
demonstrates building and package consumer plugins, and is utilized by the
included ``word-count`` Helm chart::

  docker build . -f build/examples/Dockerfile.word-count --tag word-count


Publish The Base Runtime Image
==============================

Build a new base image::

    docker build . -f build/Dockerfile.gazette-base --tag gazette-base

Test it locally by temporarily changing the references in
``build/Dockerfile.gazette-build`` and ``build/cmd/Dockerfile.gazette`` from
``liveramp/gazette-base:X.Y.Z`` [*]_ to ``gazette-base``. Then run the ``docker
build`` commands given above.

Once the image has been tested and is ready to publish, pick an appropriate
`semantic version number`_ [*]_ then tag and push the image::

  docker tag gazette-base liveramp/gazette-base:latest
  docker tag gazette-base liveramp/gazette-base:X.Y.Z
  docker push liveramp/gazette-base:latest
  docker push liveramp/gazette-base:X.Y.Z

It may be necessary to log in to Docker Hub before ``docker push``::

  docker login  # Interactively enter username and password

.. _semantic version number: https://semver.org

.. [*] Note that this project's convention is to not prefix the version number
       with "v".
.. [*] For example, changing major versions of Go would be a major version
       bump, installing an additional tool would be a minor version bump, and
       fixing a bug in a ``RUN`` command would be a patch version bump.
