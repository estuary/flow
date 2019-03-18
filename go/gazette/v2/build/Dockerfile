# State 1: Create a base image which includes the Go toolchain,
# RocksDB library, its tools, and dependencies.
FROM golang:1.11 AS base

ARG ROCKSDB_VERSION=5.17.2
ARG ZSTD_VERSION=1.3.5

# Install dependencies for building & running RocksDB.
RUN apt-get update -y \
 && apt-get install --no-install-recommends -y \
      build-essential \
      curl \
      libbz2-dev \
      libgflags-dev \
      libjemalloc-dev \
      liblz4-dev \
      libsnappy-dev \
      unzip \
      zlib1g-dev \
 && rm -rf /var/lib/apt/lists/*

# Build zstd. The packaged version (libzstd-dev) is currently 1.1.2 on Debian stretch.
# We need at least 1.3
RUN curl -L -o /opt/zstd.tgz \
      https://github.com/facebook/zstd/archive/v${ZSTD_VERSION}.tar.gz \
 && mkdir -p /opt/zstd \
 && tar xzf /opt/zstd.tgz -C /opt/zstd --strip-components=1 \
 && rm /opt/zstd.tgz \
 && cd /opt/zstd \
 && make install -j$(nproc) \
 && rm -r /opt/zstd

# Install RocksDB and the "ldb" & "sst_dump" tools.
RUN curl -L -o /opt/rocksdb.tgz \
      https://github.com/facebook/rocksdb/archive/v${ROCKSDB_VERSION}.tar.gz \
 && mkdir -p /opt/rocksdb \
 && tar xzf /opt/rocksdb.tgz -C /opt/rocksdb --strip-components=1 \
 && rm /opt/rocksdb.tgz \
 && cd /opt/rocksdb \
 && USE_SSE=1 DEBUG_LEVEL=0 USE_RTTI=1 make shared_lib tools -j$(nproc) \
 && make install-shared -j$(nproc) \
 && cp ldb sst_dump /usr/local/bin \
 && rm -r /opt/rocksdb \
 && ldconfig


# Stage 2: Create an image with the vendored dependencies, suitable for using
# as a cache to speed up CI build times.
FROM base as vendor

ARG DEP_VERSION=v0.5.0

RUN curl -fsSL -o /usr/local/bin/dep \
    https://github.com/golang/dep/releases/download/${DEP_VERSION}/dep-linux-amd64 \
 && chmod +x /usr/local/bin/dep

COPY Gopkg.toml Gopkg.lock /go/src/github.com/LiveRamp/gazette/
RUN cd /go/src/github.com/LiveRamp/gazette/ \
 && dep ensure -vendor-only \
 && rm -rf "$GOPATH/pkg/dep"


# Stage 3: Create a build image with source, which is built and tested.
FROM vendor as build

# Copy, install, and test library and main packages.
COPY v2/pkg /go/src/github.com/LiveRamp/gazette/v2/pkg
RUN go install -race -tags rocksdb github.com/LiveRamp/gazette/v2/pkg/...
RUN go test -race -tags rocksdb github.com/LiveRamp/gazette/v2/pkg/...
RUN go install -tags rocksdb github.com/LiveRamp/gazette/v2/pkg/...

COPY v2/cmd /go/src/github.com/LiveRamp/gazette/v2/cmd
RUN go install github.com/LiveRamp/gazette/v2/cmd/...
RUN go test github.com/LiveRamp/gazette/v2/cmd/...

# Build & test examples.
COPY v2/examples /go/src/github.com/LiveRamp/gazette/v2/examples
RUN go install -tags rocksdb github.com/LiveRamp/gazette/v2/examples/...
RUN go test -tags rocksdb github.com/LiveRamp/gazette/v2/examples/...

# Stage 4: Pluck gazette binaries onto base.
FROM base as gazette

# Install vim in gazette image as it is required for some gazctl commands
RUN apt-get update && apt-get install -y vim --no-install-recommends

COPY --from=build \
    /go/bin/gazctl \
    /go/bin/gazette \
    /go/bin/


# Stage 5: Pluck example binaries onto gazette.
FROM gazette as examples

COPY --from=build \
        /go/bin/chunker \
        /go/bin/counter \
        /go/bin/summer \
        /go/bin/wordcountctl \
    /go/bin/

