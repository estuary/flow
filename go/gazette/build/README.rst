Containerized Gazette Build
===========================

From the top-level repository directory:

Build a base runtime image with compiled RocksDB libraries and a protobuf
compiler. This image is used both for building gazette itself, and also for
multi-stage Dockerfiles which pick out compiled binaries of the gazette build
but still require the RocksDB runtime::

  docker build . -f build/Dockerfile.gazette-base --tag gazette-base

Build and test Gazette. This image includes all Gazette source, vendored
dependencies, compiled packages and binaries, and only completes after
all tests pass::

  docker build . -f build/Dockerfile.gazette-build --tag gazette-build

Create the `gazette` command container, which plucks the `gazette` binary onto
a `gazette-base` image. Other containerized binaries use a similar pattern::

  docker build . -f build/cmd/Dockerfile.gazette --tag gazette

Gazette examples also have Dockerized build targets. Eg, `word-count`
demonstrates building and package consumer plugins, and is utilized by the
included `word-count` Helm chart::

  docker build . -f build/examples/Dockerfile.word-count --tag word-count
