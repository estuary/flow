# State 1: Create a base image which includes the Go toolchain,
# RocksDB library, its tools, and dependencies.
FROM golang:1.11 AS base

ARG ROCKSDB_VERSION=5.14.2
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


# Stage 2: Create a build image with dependencies & source, which is built and tested.
FROM base as build

ARG DEP_VERSION=v0.5.0

RUN curl -fsSL -o /usr/local/bin/dep \
    https://github.com/golang/dep/releases/download/${DEP_VERSION}/dep-linux-amd64 \
 && chmod +x /usr/local/bin/dep

COPY Gopkg.toml Gopkg.lock /go/src/github.com/LiveRamp/gazette/
RUN cd /go/src/github.com/LiveRamp/gazette/ && dep ensure -vendor-only

# Copy, install, and test library and main packages.
COPY v2/pkg /go/src/github.com/LiveRamp/gazette/v2/pkg
RUN go install -race github.com/LiveRamp/gazette/v2/pkg/...
RUN go test -race github.com/LiveRamp/gazette/v2/pkg/...
RUN go install github.com/LiveRamp/gazette/v2/pkg/...

COPY v2/cmd /go/src/github.com/LiveRamp/gazette/v2/cmd
RUN go install github.com/LiveRamp/gazette/v2/cmd/...
RUN go test github.com/LiveRamp/gazette/v2/cmd/...

# Build & test examples.
COPY v2/examples /go/src/github.com/LiveRamp/gazette/v2/examples
RUN go install github.com/LiveRamp/gazette/v2/examples/...
RUN go test github.com/LiveRamp/gazette/v2/examples/...

# Build the stream-sum consumer plugin.
RUN go build \
    --buildmode=plugin \
    -o /go/bin/stream-sum-summer.so \
    github.com/LiveRamp/gazette/v2/examples/stream-sum/summer

# Build the word-count consumer plugin.
RUN go build \
    --buildmode=plugin \
    -o /go/bin/word-count-counter.so \
    github.com/LiveRamp/gazette/v2/examples/word-count/counter


# Stage 3: Pluck gazette binaries onto base.
FROM base as gazette

COPY --from=build \
    /go/bin/gazctl \
    /go/bin/gazette \
    /go/bin/run-consumer \
    /go/bin/


# Stage 4: Pluck example binaries onto gazette.
FROM gazette as examples

COPY --from=build \
        /go/bin/chunker \
        /go/bin/word-count-counter.so \
        /go/bin/stream-sum-summer.so \
        /go/bin/wordcountctl \
    /go/bin/

